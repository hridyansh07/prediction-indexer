use canonical_normalizer::CheckedValue;
use indexer_types::{EnvelopeView, SourceCursor, Stream};
use replay_domain::SegmentEvent;
use serde_json::{Map, Value};

use crate::{
    Config,
    error::Reject,
    event::{RelativeDelta, Snapshot, Trade, instrument},
    value::{CheckedKalshiValue, CheckedObject},
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
    let event = Snapshot::parse(outer.checked_required("msg")?, config)?;
    Ok(MessageOutcome::Events(event.into()))
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
    let event = RelativeDelta::parse(outer.checked_required("msg")?, config)?;
    Ok(MessageOutcome::Events(event.into()))
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
    let event = Trade::parse(outer.checked_required("msg")?, config)?;
    Ok(MessageOutcome::Events(event.into()))
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

fn optional_positive_u64(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => value
            .checked_positive_u64()
            .map(|_| ())
            .map_err(|_| Failure::new(code)),
        None => Ok(()),
    }
}
