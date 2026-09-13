use canonical_normalizer::CheckedValue;
use indexer_types::{EnvelopeView, SourceCursor, Stream};
use replay_domain::{
    ConditionalMarketPrice, ContractOrientation, DecimalScale, InstrumentId, Level, PositiveQty,
    Px, Qty, SegmentEvent, Side,
};
use serde_json::{Map, Value};

use crate::{
    Config,
    error::Reject,
    event::{RelativeDelta, Snapshot, Trade},
    value::{CheckedKalshiValue, CheckedObject, numeric_code},
};

type Failure = Reject;

pub(crate) enum MessageOutcome {
    Events(Vec<SegmentEvent>),
    Ignored(&'static str),
}

pub(crate) fn normalize_message(
    envelope: &EnvelopeView<'_>,
    value: &Value,
    config: Config,
    batched: bool,
) -> Result<MessageOutcome, Reject> {
    let object = value
        .checked_object()
        .map_err(|_| Reject::new("message_not_object"))?;
    let kind = object
        .checked_required("type")?
        .checked_text()
        .map_err(|_| Reject::new("invalid_message_type"))?;
    match kind {
        "orderbook_snapshot" => snapshot(envelope, object, config, batched),
        "orderbook_delta" => delta(envelope, object, config, batched),
        "trade" => trade(envelope, object, config, batched),
        "ticker" => ticker(envelope, object, config, batched),
        "subscribed" => subscribed(envelope, object),
        "unsubscribed" => unsubscribed(envelope, object, batched),
        "ok" => ok_response(envelope, object, batched),
        "error" => error_response(envelope, object, batched),
        _ => Err(Reject::new("unsupported_message_type")),
    }
}

fn snapshot(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    config: Config,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    outer.checked_fields(&["type", "sid", "seq", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    sequence(outer, envelope, batched)?;
    outer
        .checked_required("sid")?
        .checked_positive_u64()
        .map_err(|_| Reject::new("invalid_sid"))?;
    let msg = outer
        .checked_required("msg")?
        .checked_object()
        .map_err(|_| Reject::new("invalid_snapshot_msg"))?;
    let modern = msg.contains_key("yes_dollars_fp") || msg.contains_key("no_dollars_fp");
    let legacy = msg.contains_key("yes") || msg.contains_key("no");
    if modern && legacy {
        return Err(Failure::new("mixed_snapshot_schema"));
    }
    let instrument = instrument(msg)?;
    if modern {
        msg.checked_fields(&[
            "market_ticker",
            "market_id",
            "yes_dollars_fp",
            "no_dollars_fp",
        ])?;
    } else {
        msg.checked_fields(&["market_ticker", "market_id", "yes", "no"])?;
    }
    optional_nonempty_text(msg.get("market_id"), "invalid_market_id")?;
    let (yes, no) = if modern {
        (
            levels(msg.get("yes_dollars_fp"), config, false, &instrument)?,
            levels(msg.get("no_dollars_fp"), config, false, &instrument)?,
        )
    } else {
        (
            levels(msg.get("yes"), config, true, &instrument)?,
            levels(msg.get("no"), config, true, &instrument)?,
        )
    };
    Snapshot {
        instrument,
        yes,
        no,
    }
    .try_into()
}

fn delta(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    config: Config,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    outer.checked_fields(&["type", "sid", "seq", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    sequence(outer, envelope, batched)?;
    outer
        .checked_required("sid")?
        .checked_positive_u64()
        .map_err(|_| Reject::new("invalid_sid"))?;
    let msg = outer
        .checked_required("msg")?
        .checked_object()
        .map_err(|_| Reject::new("invalid_delta_msg"))?;
    msg.checked_fields(&[
        "market_ticker",
        "market_id",
        "price_dollars",
        "delta_fp",
        "side",
        "client_order_id",
        "subaccount",
        "ts",
        "ts_ms",
    ])?;
    let instrument = instrument(msg)?;
    optional_nonempty_text(msg.get("market_id"), "invalid_market_id")?;
    optional_nonempty_text(msg.get("client_order_id"), "invalid_client_order_id")?;
    optional_nonnegative_u64(msg.get("subaccount"), "invalid_subaccount")?;
    optional_nonempty_text(msg.get("ts"), "invalid_source_time")?;
    optional_positive_u64(msg.get("ts_ms"), "invalid_source_time")?;
    let orientation = match msg
        .checked_required("side")?
        .checked_text()
        .map_err(|_| Reject::new("invalid_side"))?
    {
        "yes" => ContractOrientation::Outcome,
        "no" => ContractOrientation::Complement,
        _ => return Err(Failure::for_instrument("invalid_side", instrument)),
    };
    let price = msg
        .checked_required("price_dollars")?
        .checked_price(config.price_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    let change = msg
        .checked_required("delta_fp")?
        .checked_level_change(config.quantity_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    RelativeDelta {
        instrument,
        orientation,
        price,
        change,
    }
    .try_into()
}

fn trade(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    config: Config,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    outer.checked_fields(&["type", "sid", "seq", "msg"])?;
    if !batched {
        expect_stream(envelope, Stream::PublicTrade)?;
    }
    sequence(outer, envelope, batched)?;
    outer
        .checked_required("sid")?
        .checked_positive_u64()
        .map_err(|_| Reject::new("invalid_sid"))?;
    let msg = outer
        .checked_required("msg")?
        .checked_object()
        .map_err(|_| Reject::new("invalid_trade_msg"))?;
    msg.checked_fields(&[
        "trade_id",
        "market_ticker",
        "yes_price_dollars",
        "no_price_dollars",
        "count_fp",
        "taker_side",
        "taker_outcome_side",
        "taker_book_side",
        "is_block_trade",
        "ts",
        "ts_ms",
    ])?;
    let instrument = instrument(msg)?;
    msg.checked_required("trade_id")?
        .checked_nonempty_text()
        .map_err(|_| Reject::new("invalid_trade_id"))?;
    msg.checked_required("no_price_dollars")?
        .checked_price(config.price_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    if let Some(block) = msg.get("is_block_trade") {
        block
            .checked_bool()
            .map_err(|_| Reject::new("invalid_block_trade"))?;
    }
    msg.checked_required("ts")?
        .checked_positive_u64()
        .map_err(|_| Reject::new("invalid_source_time"))?;
    msg.checked_required("ts_ms")?
        .checked_positive_u64()
        .map_err(|_| Reject::new("invalid_source_time"))?;
    let outcome = msg
        .checked_required("taker_outcome_side")?
        .checked_text()
        .map_err(|_| Reject::new("invalid_trade_direction"))?;
    let legacy = msg
        .checked_required("taker_side")?
        .checked_text()
        .map_err(|_| Reject::new("invalid_trade_direction"))?;
    let book = msg
        .checked_required("taker_book_side")?
        .checked_text()
        .map_err(|_| Reject::new("invalid_trade_direction"))?;
    let (expected_outcome, aggressor) = match book {
        "bid" => ("yes", Side::Bid),
        "ask" => ("no", Side::Ask),
        _ => {
            return Err(Failure::for_instrument(
                "invalid_trade_direction",
                instrument,
            ));
        }
    };
    if outcome != expected_outcome || legacy != outcome {
        return Err(Failure::for_instrument(
            "inconsistent_trade_direction",
            instrument,
        ));
    }
    let price = msg
        .checked_required("yes_price_dollars")?
        .checked_price(config.price_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    let quantity = msg
        .checked_required("count_fp")?
        .checked_positive_quantity(config.quantity_scale)
        .map_err(|code| {
            let code = if code == "zero_quantity" {
                "non_positive_trade_quantity"
            } else {
                code
            };
            Failure::for_instrument(code, instrument.clone())
        })?;
    Trade {
        instrument,
        price,
        quantity,
        aggressor,
    }
    .try_into()
}

fn ticker(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    config: Config,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    outer.checked_fields(&["type", "sid", "msg"])?;
    if !batched {
        expect_stream(envelope, Stream::PublicQuote)?;
    }
    expect_unsequenced(envelope)?;
    outer
        .checked_required("sid")?
        .checked_positive_u64()
        .map_err(|_| Reject::new("invalid_sid"))?;
    let msg = outer
        .checked_required("msg")?
        .checked_object()
        .map_err(|_| Reject::new("invalid_ticker_msg"))?;
    msg.checked_fields(&[
        "market_ticker",
        "market_id",
        "price_dollars",
        "yes_bid_dollars",
        "yes_ask_dollars",
        "yes_bid_size_fp",
        "yes_ask_size_fp",
        "last_trade_size_fp",
        "volume_fp",
        "open_interest_fp",
        "dollar_volume",
        "dollar_open_interest",
        "ts",
        "ts_ms",
        "time",
    ])?;
    let instrument = instrument(msg)?;
    msg.checked_required("market_id")?
        .checked_nonempty_text()
        .map_err(|_| Reject::new("invalid_market_id"))?;
    for field in ["price_dollars", "yes_bid_dollars", "yes_ask_dollars"] {
        msg.checked_required(field)?
            .checked_price(config.price_scale)
            .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    }
    for field in [
        "yes_bid_size_fp",
        "yes_ask_size_fp",
        "last_trade_size_fp",
        "volume_fp",
        "open_interest_fp",
    ] {
        msg.checked_required(field)?
            .checked_quantity(config.quantity_scale)
            .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    }
    for field in ["dollar_volume", "dollar_open_interest"] {
        msg.checked_required(field)?
            .checked_nonnegative_u64()
            .map_err(|_| Reject::new("invalid_ticker_integer"))?;
    }
    for field in ["ts", "ts_ms"] {
        msg.checked_required(field)?
            .checked_positive_u64()
            .map_err(|_| Reject::new("invalid_source_time"))?;
    }
    msg.checked_required("time")?
        .checked_nonempty_text()
        .map_err(|_| Reject::new("invalid_source_time"))?;
    Ok(MessageOutcome::Ignored("ticker_not_in_replay_domain"))
}

fn subscribed(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
) -> Result<MessageOutcome, Failure> {
    outer.checked_fields(&["id", "type", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    expect_unsequenced(envelope)?;
    optional_positive_u64(outer.get("id"), "invalid_command_id")?;
    let msg = outer
        .checked_required("msg")?
        .checked_object()
        .map_err(|_| Reject::new("invalid_subscribed_msg"))?;
    msg.checked_fields(&["channel", "sid"])?;
    msg.checked_required("channel")?
        .checked_nonempty_text()
        .map_err(|_| Reject::new("invalid_channel"))?;
    msg.checked_required("sid")?
        .checked_positive_u64()
        .map_err(|_| Reject::new("invalid_sid"))?;
    Ok(MessageOutcome::Ignored(
        "venue_control_not_in_replay_domain",
    ))
}

fn unsubscribed(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    outer.checked_fields(&["id", "sid", "seq", "type"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    sequence(outer, envelope, batched)?;
    optional_positive_u64(outer.get("id"), "invalid_command_id")?;
    outer
        .checked_required("sid")?
        .checked_positive_u64()
        .map_err(|_| Reject::new("invalid_sid"))?;
    Ok(MessageOutcome::Ignored(
        "venue_control_not_in_replay_domain",
    ))
}

fn ok_response(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    outer.checked_fields(&["id", "sid", "seq", "type", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    optional_positive_u64(outer.get("id"), "invalid_command_id")?;
    optional_positive_u64(outer.get("sid"), "invalid_sid")?;
    if outer.contains_key("seq") {
        sequence(outer, envelope, batched)?;
    } else {
        expect_unsequenced(envelope)?;
    }
    if let Some(value) = outer.get("msg") {
        match value {
            Value::Object(msg) => {
                msg.checked_fields(&["market_tickers", "market_ids"])?;
                optional_text_array(msg.get("market_tickers"), "invalid_ok_msg")?;
                optional_text_array(msg.get("market_ids"), "invalid_ok_msg")?;
            }
            Value::Array(items) => {
                for item in items {
                    let subscription = item
                        .checked_object()
                        .map_err(|_| Reject::new("invalid_ok_msg"))?;
                    subscription.checked_fields(&["channel", "sid"])?;
                    subscription
                        .checked_required("channel")?
                        .checked_nonempty_text()
                        .map_err(|_| Reject::new("invalid_channel"))?;
                    subscription
                        .checked_required("sid")?
                        .checked_positive_u64()
                        .map_err(|_| Reject::new("invalid_sid"))?;
                }
            }
            _ => return Err(Failure::new("invalid_ok_msg")),
        }
    }
    Ok(MessageOutcome::Ignored(
        "venue_control_not_in_replay_domain",
    ))
}

fn error_response(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    outer.checked_fields(&["id", "sid", "seq", "type", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    optional_positive_u64(outer.get("id"), "invalid_command_id")?;
    optional_positive_u64(outer.get("sid"), "invalid_sid")?;
    if outer.contains_key("seq") {
        sequence(outer, envelope, batched)?;
    } else {
        expect_unsequenced(envelope)?;
    }
    let msg = outer
        .checked_required("msg")?
        .checked_object()
        .map_err(|_| Reject::new("invalid_error_msg"))?;
    msg.checked_fields(&["code", "msg"])?;
    msg.checked_required("code")?
        .checked_nonnegative_i64()
        .map_err(|_| Reject::new("invalid_error_code"))?;
    msg.checked_required("msg")?
        .checked_nonempty_text()
        .map_err(|_| Reject::new("invalid_error_message"))?;
    Ok(MessageOutcome::Ignored(
        "venue_control_not_in_replay_domain",
    ))
}

fn levels(
    value: Option<&Value>,
    config: Config,
    legacy: bool,
    instrument: &InstrumentId,
) -> Result<Vec<Level>, Failure> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let rows = value
        .as_array()
        .ok_or_else(|| Failure::for_instrument("invalid_snapshot_levels", instrument.clone()))?;
    rows.iter()
        .map(|row| {
            let pair = row
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| {
                    Failure::for_instrument("invalid_snapshot_level", instrument.clone())
                })?;
            let (price, quantity) = if legacy {
                let cents = pair[0]
                    .checked_nonnegative_i64()
                    .map_err(|_| Failure::for_instrument("invalid_price", instrument.clone()))?;
                let contracts = pair[1]
                    .checked_nonnegative_i64()
                    .map_err(|_| Failure::for_instrument("invalid_quantity", instrument.clone()))?;
                let cents_scale = DecimalScale::new(2).expect("constant scale");
                let contracts_scale = DecimalScale::new(0).expect("constant scale");
                (
                    Px::from_atoms(cents, cents_scale)
                        .and_then(|price| price.checked_rescale(config.price_scale))
                        .and_then(ConditionalMarketPrice::try_from)
                        .map_err(|error| {
                            Failure::for_instrument(
                                numeric_code(error, "price"),
                                instrument.clone(),
                            )
                        })?,
                    Qty::from_atoms(contracts as u64, contracts_scale)
                        .and_then(PositiveQty::new)
                        .and_then(|quantity| quantity.checked_rescale(config.quantity_scale))
                        .map_err(|error| {
                            Failure::for_instrument(
                                numeric_code(error, "quantity"),
                                instrument.clone(),
                            )
                        })?,
                )
            } else {
                (
                    pair[0]
                        .checked_price(config.price_scale)
                        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?,
                    pair[1]
                        .checked_positive_quantity(config.quantity_scale)
                        .map_err(|code| {
                            let code = if code == "zero_quantity" {
                                "non_positive_snapshot_quantity"
                            } else {
                                code
                            };
                            Failure::for_instrument(code, instrument.clone())
                        })?,
                )
            };
            Ok(Level::new(price, quantity))
        })
        .collect()
}

fn instrument(msg: &Map<String, Value>) -> Result<InstrumentId, Failure> {
    let ticker = msg
        .checked_required("market_ticker")?
        .checked_nonempty_text()
        .map_err(|_| Failure::new("invalid_market_ticker"))?;
    InstrumentId::new(format!("kalshi:{ticker}")).map_err(|_| Failure::new("invalid_market_ticker"))
}

fn sequence(
    outer: &Map<String, Value>,
    envelope: &EnvelopeView<'_>,
    batched: bool,
) -> Result<(), Failure> {
    let seq = outer
        .checked_required("seq")?
        .checked_positive_u64()
        .map_err(|_| Failure::new("invalid_sequence"))?;
    if batched {
        return expect_unsequenced(envelope).map_err(|_| Failure::new("batch_cursor_mismatch"));
    }
    match envelope.source_cursor {
        Some(SourceCursor::UpdateRange {
            first,
            last,
            previous_last,
        }) if first == seq && last == seq && previous_last == seq - 1 => Ok(()),
        _ => Err(Failure::new("sequence_cursor_mismatch")),
    }
}

fn expect_unsequenced(envelope: &EnvelopeView<'_>) -> Result<(), Failure> {
    if matches!(
        envelope.source_cursor,
        Some(SourceCursor::Unsequenced { .. })
    ) {
        Ok(())
    } else {
        Err(Failure::new("unexpected_sequence_cursor"))
    }
}

fn expect_stream(envelope: &EnvelopeView<'_>, expected: Stream) -> Result<(), Failure> {
    if envelope.stream == expected {
        Ok(())
    } else {
        Err(Failure::new("message_stream_mismatch"))
    }
}

fn optional_nonempty_text(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => value
            .checked_nonempty_text()
            .map(|_| ())
            .map_err(|_| Failure::new(code)),
        None => Ok(()),
    }
}

fn optional_text_array(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    let Some(value) = value else {
        return Ok(());
    };
    let values = value.as_array().ok_or_else(|| Failure::new(code))?;
    for value in values {
        value
            .checked_nonempty_text()
            .map_err(|_| Failure::new(code))?;
    }
    Ok(())
}

fn optional_nonnegative_u64(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => value
            .checked_nonnegative_u64()
            .map(|_| ())
            .map_err(|_| Failure::new(code)),
        None => Ok(()),
    }
}

fn optional_positive_u64(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => value
            .checked_positive_u64()
            .map(|_| ())
            .map_err(|_| Failure::new(code)),
        None => Ok(()),
    }
}
