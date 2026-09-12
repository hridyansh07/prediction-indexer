use std::collections::BTreeSet;

use indexer_types::{EnvelopeView, RecordKind, SourceCursor, Stream};
use replay_domain::{
    ConditionalMarketPrice, ContractOrientation, ControlEvent, DecimalScale, InstrumentId, Level,
    PositiveQty, Px, Qty, SegmentEvent, Side,
};
use serde_json::{Map, Value};

use crate::{
    Config,
    error::Reject,
    event::{RelativeDelta, Snapshot, Trade},
    value::{CheckedObject, CheckedValue, numeric_code},
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
    let object = value.checked_object("message_not_object")?;
    let kind = object
        .checked_required("type")?
        .checked_text("invalid_message_type")?;
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
        .checked_positive_u64("invalid_sid")?;
    let msg = outer
        .checked_required("msg")?
        .checked_object("invalid_snapshot_msg")?;
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
        .checked_positive_u64("invalid_sid")?;
    let msg = outer
        .checked_required("msg")?
        .checked_object("invalid_delta_msg")?;
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
    let orientation = match msg.checked_required("side")?.checked_text("invalid_side")? {
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
        .checked_positive_u64("invalid_sid")?;
    let msg = outer
        .checked_required("msg")?
        .checked_object("invalid_trade_msg")?;
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
        .checked_nonempty_text("invalid_trade_id")?;
    msg.checked_required("no_price_dollars")?
        .checked_price(config.price_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    if let Some(block) = msg.get("is_block_trade") {
        block.checked_bool("invalid_block_trade")?;
    }
    msg.checked_required("ts")?
        .checked_positive_u64("invalid_source_time")?;
    msg.checked_required("ts_ms")?
        .checked_positive_u64("invalid_source_time")?;
    let outcome = msg
        .checked_required("taker_outcome_side")?
        .checked_text("invalid_trade_direction")?;
    let legacy = msg
        .checked_required("taker_side")?
        .checked_text("invalid_trade_direction")?;
    let book = msg
        .checked_required("taker_book_side")?
        .checked_text("invalid_trade_direction")?;
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
        .checked_positive_u64("invalid_sid")?;
    let msg = outer
        .checked_required("msg")?
        .checked_object("invalid_ticker_msg")?;
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
        .checked_nonempty_text("invalid_market_id")?;
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
            .checked_nonnegative_u64("invalid_ticker_integer")?;
    }
    for field in ["ts", "ts_ms"] {
        msg.checked_required(field)?
            .checked_positive_u64("invalid_source_time")?;
    }
    msg.checked_required("time")?
        .checked_nonempty_text("invalid_source_time")?;
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
        .checked_object("invalid_subscribed_msg")?;
    msg.checked_fields(&["channel", "sid"])?;
    msg.checked_required("channel")?
        .checked_nonempty_text("invalid_channel")?;
    msg.checked_required("sid")?
        .checked_positive_u64("invalid_sid")?;
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
        .checked_positive_u64("invalid_sid")?;
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
                    let subscription = item.checked_object("invalid_ok_msg")?;
                    subscription.checked_fields(&["channel", "sid"])?;
                    subscription
                        .checked_required("channel")?
                        .checked_nonempty_text("invalid_channel")?;
                    subscription
                        .checked_required("sid")?
                        .checked_positive_u64("invalid_sid")?;
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
        .checked_object("invalid_error_msg")?;
    msg.checked_fields(&["code", "msg"])?;
    msg.checked_required("code")?
        .checked_nonnegative_i64("invalid_error_code")?;
    msg.checked_required("msg")?
        .checked_nonempty_text("invalid_error_message")?;
    Ok(MessageOutcome::Ignored(
        "venue_control_not_in_replay_domain",
    ))
}

pub(crate) enum ProcessOutcome {
    Event(SegmentEvent),
    Ignored(&'static str),
}

pub(crate) fn normalize_process(
    envelope: &EnvelopeView<'_>,
    value: &Value,
) -> Result<ProcessOutcome, &'static str> {
    if !matches!(envelope.kind, RecordKind::Control | RecordKind::Fault) {
        return Err("unexpected_record_kind");
    }
    let object = value.as_object().ok_or("control_not_object")?;
    let event = object
        .get("event")
        .and_then(Value::as_str)
        .ok_or("invalid_control_event")?;
    let epoch = envelope.connection_epoch.as_str().to_owned();
    match event {
        "connection_opened" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "target_digest",
                    "target_count",
                    "asset_ids",
                    "targets_path",
                    "target_metadata_digest",
                    "target_metadata_path",
                    "delivers_deltas",
                    "fsync_interval_seconds",
                    "repaired_bytes_on_start",
                    "clock_scope",
                    "url",
                    "channels",
                    "send_initial_snapshot",
                    "verified_against_live_socket",
                    "snapshot_sweep_seconds",
                    "snapshot_max_age_seconds",
                    "snapshot_request_cooldown_seconds",
                    "key_id",
                ],
            )?;
            let assets = object
                .get("asset_ids")
                .and_then(Value::as_array)
                .ok_or("invalid_control_asset_ids")?;
            let mut seen = BTreeSet::new();
            let mut instruments = Vec::with_capacity(assets.len());
            for asset in assets {
                let text = asset
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .ok_or("invalid_control_asset_ids")?;
                if !seen.insert(text) {
                    return Err("duplicate_control_asset_id");
                }
                instruments.push(
                    InstrumentId::new(format!("kalshi:{text}"))
                        .map_err(|_| "invalid_control_asset_ids")?,
                );
            }
            object
                .get("target_count")
                .ok_or("invalid_control_target_count")?
                .checked_nonnegative_u64("invalid_control_target_count")
                .map_err(|_| "invalid_control_target_count")
                .and_then(|count| {
                    (count == assets.len() as u64)
                        .then_some(())
                        .ok_or("control_target_count_mismatch")
                })?;
            required_text_field(object, "targets_path")?;
            optional_text_field(object, "target_metadata_digest")?;
            optional_text_field(object, "target_metadata_path")?;
            for field in [
                "fsync_interval_seconds",
                "snapshot_sweep_seconds",
                "snapshot_max_age_seconds",
                "snapshot_request_cooldown_seconds",
            ] {
                required_nonnegative_number(object, field)?;
            }
            required_nonnegative_u64(object, "repaired_bytes_on_start")?;
            validate_clock_scope(object.get("clock_scope"))?;
            required_text_field(object, "url")?;
            required_text_array(object.get("channels"), "invalid_control_channels")?;
            required_bool(object, "send_initial_snapshot")?;
            required_bool(object, "verified_against_live_socket")?;
            optional_text_field(object, "key_id")?;
            let delivers_deltas = object
                .get("delivers_deltas")
                .and_then(Value::as_bool)
                .ok_or("invalid_control_delivers_deltas")?;
            let target_digest = optional_text_field(object, "target_digest")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionOpened {
                    epoch,
                    instruments,
                    delivers_deltas,
                    target_digest,
                },
            )))
        }
        "connection_closed" => {
            exact_field_names(object, &["event", "seconds_open", "records_this_epoch"])?;
            required_nonnegative_number(object, "seconds_open")?;
            required_nonnegative_u64(object, "records_this_epoch")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionClosed { epoch },
            )))
        }
        "connection_failed" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "error_type",
                    "error",
                    "seconds_open",
                    "frames_this_epoch",
                ],
            )?;
            let reason = required_text_field(object, "error")?;
            required_text_field(object, "error_type")?;
            required_nonnegative_number(object, "seconds_open")?;
            required_nonnegative_u64(object, "frames_this_epoch")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionFailed { epoch, reason },
            )))
        }
        "subscription_changed" => {
            exact_field_names(
                object,
                &["event", "from_digest", "to_digest", "added", "removed"],
            )?;
            required_text_array(object.get("added"), "invalid_control_asset_ids")?;
            required_text_array(object.get("removed"), "invalid_control_asset_ids")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::SubscriptionChanged {
                    from: optional_text_field(object, "from_digest")?,
                    to: required_text_field(object, "to_digest")?,
                },
            )))
        }
        "target_metadata_changed" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "target_digest",
                    "from_metadata_digest",
                    "to_metadata_digest",
                    "metadata_path",
                ],
            )?;
            required_text_field(object, "target_digest")?;
            optional_text_field(object, "metadata_path")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::MetadataChanged {
                    from: optional_text_field(object, "from_metadata_digest")?,
                    to: required_text_field(object, "to_metadata_digest")?,
                },
            )))
        }
        "subscription_sent" => {
            exact_field_names(object, &["event", "target_digest", "target_count"])?;
            required_text_field(object, "target_digest")?;
            required_nonnegative_u64(object, "target_count")?;
            Ok(ProcessOutcome::Ignored("subscription_sent"))
        }
        "connection_closing" => {
            exact_field_names(object, &["event", "reason"])?;
            required_text_field(object, "reason")?;
            Ok(ProcessOutcome::Ignored("connection_closing"))
        }
        "orderbook_reconciliation_request" => {
            exact_field_names(
                object,
                &["event", "sid", "command_id", "market_tickers", "reason"],
            )?;
            required_positive_u64(object, "sid")?;
            required_positive_u64(object, "command_id")?;
            required_text_array(object.get("market_tickers"), "invalid_control_asset_ids")?;
            required_text_field(object, "reason")?;
            Ok(ProcessOutcome::Ignored("reconciliation_request"))
        }
        "orderbook_reconciliation_disabled" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "reason",
                    "channel",
                    "command_id",
                    "error_type",
                    "error",
                    "code",
                    "detail",
                ],
            )?;
            required_text_field(object, "reason")?;
            optional_text_field(object, "channel")?;
            optional_positive_u64_field(object, "command_id")?;
            optional_text_field(object, "error_type")?;
            optional_text_field(object, "error")?;
            optional_code(object.get("code"))?;
            optional_text_field(object, "detail")?;
            Ok(ProcessOutcome::Ignored("reconciliation_disabled"))
        }
        "orderbook_reconciliation_backoff" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "command_id",
                    "code",
                    "from_sweep_seconds",
                    "to_sweep_seconds",
                    "detail",
                ],
            )?;
            required_positive_u64(object, "command_id")?;
            optional_code(object.get("code"))?;
            required_nonnegative_number(object, "from_sweep_seconds")?;
            required_nonnegative_number(object, "to_sweep_seconds")?;
            optional_text_field(object, "detail")?;
            Ok(ProcessOutcome::Ignored("reconciliation_backoff"))
        }
        "targets_unreadable" => {
            exact_field_names(object, &["event", "error"])?;
            required_text_field(object, "error")?;
            Ok(ProcessOutcome::Ignored("targets_unreadable"))
        }
        "frame_not_utf8" => {
            exact_field_names(object, &["event", "bytes"])?;
            required_positive_u64(object, "bytes")?;
            Ok(ProcessOutcome::Ignored("frame_not_utf8"))
        }
        _ => Err("unsupported_control_event"),
    }
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
                let cents = pair[0].checked_nonnegative_i64("invalid_price")?;
                let contracts = pair[1].checked_nonnegative_i64("invalid_quantity")?;
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
        .checked_nonempty_text("invalid_market_ticker")?;
    InstrumentId::new(format!("kalshi:{ticker}")).map_err(|_| Failure::new("invalid_market_ticker"))
}

fn sequence(
    outer: &Map<String, Value>,
    envelope: &EnvelopeView<'_>,
    batched: bool,
) -> Result<(), Failure> {
    let seq = outer
        .checked_required("seq")?
        .checked_positive_u64("invalid_sequence")?;
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

fn exact_field_names(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), &'static str> {
    object.checked_fields(allowed).map_err(|reject| reject.code)
}

fn optional_nonempty_text(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => value.checked_nonempty_text(code).map(|_| ()),
        None => Ok(()),
    }
}

fn optional_text_array(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    let Some(value) = value else {
        return Ok(());
    };
    let values = value.as_array().ok_or_else(|| Failure::new(code))?;
    for value in values {
        value.checked_nonempty_text(code)?;
    }
    Ok(())
}

fn required_text_field(object: &Map<String, Value>, field: &str) -> Result<String, &'static str> {
    object.checked_required_text(field)
}

fn optional_text_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<String>, &'static str> {
    object.checked_optional_text(field)
}

fn optional_nonnegative_u64(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => value.checked_nonnegative_u64(code).map(|_| ()),
        None => Ok(()),
    }
}

fn required_nonnegative_u64(object: &Map<String, Value>, field: &str) -> Result<(), &'static str> {
    object
        .get(field)
        .ok_or("missing_control_field")?
        .as_u64()
        .map(|_| ())
        .ok_or("invalid_control_integer")
}

fn required_positive_u64(object: &Map<String, Value>, field: &str) -> Result<(), &'static str> {
    object
        .get(field)
        .ok_or("missing_control_field")?
        .as_u64()
        .filter(|value| *value > 0)
        .map(|_| ())
        .ok_or("invalid_control_integer")
}

fn optional_positive_u64_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), &'static str> {
    match object.get(field) {
        None => Ok(()),
        Some(value) => value
            .as_u64()
            .filter(|value| *value > 0)
            .map(|_| ())
            .ok_or("invalid_control_integer"),
    }
}

fn required_nonnegative_number(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), &'static str> {
    object
        .get(field)
        .ok_or("missing_control_field")?
        .checked_nonnegative_number("invalid_control_number")
        .map(|_| ())
        .map_err(|_| "invalid_control_number")
}

fn required_bool(object: &Map<String, Value>, field: &str) -> Result<(), &'static str> {
    object
        .get(field)
        .and_then(Value::as_bool)
        .map(|_| ())
        .ok_or("invalid_control_flag")
}

fn required_text_array(value: Option<&Value>, code: &'static str) -> Result<(), &'static str> {
    let values = value.and_then(Value::as_array).ok_or(code)?;
    for value in values {
        value
            .as_str()
            .filter(|text| !text.is_empty() && !text.chars().any(char::is_control))
            .ok_or(code)?;
    }
    Ok(())
}

fn optional_code(value: Option<&Value>) -> Result<(), &'static str> {
    match value {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(value)) if !value.is_empty() => Ok(()),
        Some(Value::Number(value)) if value.as_u64().is_some() => Ok(()),
        _ => Err("invalid_control_code"),
    }
}

fn validate_clock_scope(value: Option<&Value>) -> Result<(), &'static str> {
    let object = value
        .and_then(Value::as_object)
        .ok_or("invalid_clock_scope")?;
    exact_field_names(
        object,
        &[
            "lane",
            "clock",
            "scope",
            "scope_id",
            "comparable_across_processes",
            "platform",
        ],
    )?;
    for field in ["lane", "clock", "scope", "scope_id", "platform"] {
        required_text_field(object, field)?;
    }
    required_bool(object, "comparable_across_processes")
}

fn optional_positive_u64(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => value.checked_positive_u64(code).map(|_| ()),
        None => Ok(()),
    }
}
