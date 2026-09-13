use std::collections::BTreeSet;

use chrono::DateTime;
use indexer_types::{EnvelopeView, RecordKind, SourceCursor};
use replay_domain::{
    BookEvent, ConditionalMarketPrice, ContractOrientation, ControlEvent, DecimalScale, FullBook,
    InstrumentId, Level, SegmentEvent,
};
use serde_json::{Map, Value};

use crate::{
    Config,
    error::Reject,
    value::{
        CheckedObject, CheckedValue, quantity_from_raw_atoms, validate_decimal_text_price,
        validate_derived_price,
    },
};

type Failure = Reject;

const PUBLIC_EVENTS: [&str; 6] = [
    "newPriceData",
    "orderbookUpdate",
    "marketCreated",
    "marketResolved",
    "system",
    "exception",
];

pub(crate) enum MessageOutcome {
    Events(Vec<SegmentEvent>),
    Ignored(&'static str),
}

pub(crate) fn normalize_message(
    envelope: &EnvelopeView<'_>,
    value: &Value,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    let wrapper = value.checked_object("message_not_object")?;
    wrapper.checked_fields(&["event", "data"])?;
    let name = wrapper
        .checked_required("event")?
        .checked_nonempty_text("invalid_message_type")?;
    let data = wrapper.checked_required("data")?;
    match name {
        "orderbookUpdate" => orderbook_update(envelope, data, config),
        "newPriceData" => new_price_data(data, config),
        "marketCreated" => market_created(data),
        "marketResolved" => market_resolved(data),
        "system" => system(data),
        "exception" => Err(Failure::new("venue_exception")),
        _ => Err(Failure::new("unsupported_message_type")),
    }
}

fn orderbook_update(
    envelope: &EnvelopeView<'_>,
    value: &Value,
    config: Config,
) -> Result<MessageOutcome, Failure> {
    let data = value.checked_object("invalid_orderbook_update")?;
    data.checked_fields(&["marketSlug", "orderbook", "version", "timestamp"])?;
    let instrument = instrument(
        data.checked_required("marketSlug")?
            .checked_nonempty_text("invalid_market_slug")?,
    )?;
    let version = data
        .checked_required("version")?
        .checked_u64("invalid_version")?;
    match envelope.source_cursor {
        Some(SourceCursor::SnapshotId { last_update_id }) if last_update_id == version => {}
        _ => {
            return Err(Failure::for_instrument(
                "version_cursor_mismatch",
                instrument,
            ));
        }
    }
    let source_observed_ns = source_time_ns(data.checked_required("timestamp")?)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    let book = data
        .checked_required("orderbook")?
        .checked_object("invalid_orderbook")?;
    let rich = book.keys().any(|field| {
        matches!(
            field.as_str(),
            "adjustedMidpoint" | "maxSpread" | "midpoint" | "minSize" | "tokenId"
        )
    });
    if rich {
        book.checked_fields(&[
            "adjustedMidpoint",
            "asks",
            "bids",
            "maxSpread",
            "midpoint",
            "minSize",
            "tokenId",
        ])?;
        for field in ["adjustedMidpoint", "midpoint", "maxSpread"] {
            validate_derived_price(book.checked_required(field)?)
                .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
        }
        book.checked_required("minSize")?
            .checked_u64("invalid_min_size")?;
        let token = book
            .checked_required("tokenId")?
            .checked_nonempty_text("invalid_token_id")?;
        if !token.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Failure::for_instrument("invalid_token_id", instrument));
        }
    } else {
        book.checked_fields(&["asks", "bids"])?;
    }
    let bids = levels(
        book.checked_required("bids")?,
        true,
        rich,
        config,
        &instrument,
    )?;
    let asks = levels(
        book.checked_required("asks")?,
        false,
        rich,
        config,
        &instrument,
    )?;
    let full = FullBook::new(
        instrument.clone(),
        ContractOrientation::Outcome,
        bids,
        asks,
        None,
        Some(source_observed_ns),
    )
    .map_err(|_| Failure::for_instrument("invalid_orderbook_levels", instrument))?;
    Ok(MessageOutcome::Events(vec![SegmentEvent::Book(
        BookEvent::Full(full),
    )]))
}

fn levels(
    value: &Value,
    bids: bool,
    rich: bool,
    config: Config,
    instrument: &InstrumentId,
) -> Result<Vec<Level>, Failure> {
    let rows = value
        .as_array()
        .ok_or_else(|| Failure::for_instrument("invalid_orderbook_levels", instrument.clone()))?;
    rows.iter()
        .map(|row| {
            let level = row.checked_object("invalid_orderbook_level")?;
            if rich {
                level.checked_fields(&["price", "side", "size"])?;
                let side = level
                    .checked_required("side")?
                    .checked_nonempty_text("invalid_level_side")?;
                if side != if bids { "BUY" } else { "SELL" } {
                    return Err(Failure::for_instrument(
                        "invalid_level_side",
                        instrument.clone(),
                    ));
                }
            } else {
                level.checked_fields(&["price", "size"])?;
            }
            let price = level_price(level.checked_required("price")?, config.price_scale)
                .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
            let quantity =
                quantity_from_raw_atoms(level.checked_required("size")?, config.quantity_scale)
                    .map_err(|code| {
                        let code = if code == "zero_quantity" {
                            "non_positive_level_quantity"
                        } else {
                            code
                        };
                        Failure::for_instrument(code, instrument.clone())
                    })?;
            Ok(Level::new(price, quantity))
        })
        .collect()
}

fn level_price(
    value: &Value,
    output_scale: DecimalScale,
) -> Result<ConditionalMarketPrice, &'static str> {
    let tick_scale = DecimalScale::new(3).expect("constant scale");
    let tick_price = value.checked_price(tick_scale)?;
    // The public order-creation API currently advertises 0.01..=0.99, but the
    // retained public book contains resting 0.005 and 0.998 levels. A replay
    // parser validates the observed CLOB tick/range rather than deleting valid
    // venue state based on a narrower order-entry rule.
    if !(1..=999).contains(&tick_price.atoms()) {
        return Err("price_out_of_venue_range");
    }
    value.checked_price(output_scale)
}

fn new_price_data(value: &Value, config: Config) -> Result<MessageOutcome, Failure> {
    let data = value.checked_object("invalid_price_data")?;
    data.checked_fields(&["marketAddress", "updatedPrices", "blockNumber", "timestamp"])?;
    let instrument = instrument(
        data.checked_required("marketAddress")?
            .checked_nonempty_text("invalid_market_address")?,
    )?;
    let prices = data
        .checked_required("updatedPrices")?
        .checked_object("invalid_updated_prices")?;
    prices.checked_fields(&["yes", "no"])?;
    for field in ["yes", "no"] {
        validate_decimal_text_price(prices.checked_required(field)?, config.price_scale)
            .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    }
    data.checked_required("blockNumber")?
        .checked_u64("invalid_block_number")?;
    source_time_ns(data.checked_required("timestamp")?)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    Err(Failure::for_instrument(
        "amm_price_state_not_in_replay_domain",
        instrument,
    ))
}

fn market_created(value: &Value) -> Result<MessageOutcome, Failure> {
    let data = value.checked_object("invalid_market_created")?;
    data.checked_fields(&[
        "slug",
        "title",
        "type",
        "groupSlug",
        "categoryIds",
        "createdAt",
    ])?;
    let instrument = instrument(
        data.checked_required("slug")?
            .checked_nonempty_text("invalid_market_slug")?,
    )?;
    data.checked_required("title")?
        .checked_nonempty_text("invalid_market_title")?;
    market_type(data.checked_required("type")?)?;
    if let Some(group) = data.get("groupSlug") {
        group.checked_nonempty_text("invalid_group_slug")?;
    }
    if let Some(categories) = data.get("categoryIds") {
        let categories = categories
            .as_array()
            .ok_or_else(|| Failure::new("invalid_category_ids"))?;
        for category in categories {
            category.checked_u64("invalid_category_ids")?;
        }
    }
    source_time_ns(data.checked_required("createdAt")?)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    Err(Failure::for_instrument(
        "market_created_not_in_replay_domain",
        instrument,
    ))
}

fn market_resolved(value: &Value) -> Result<MessageOutcome, Failure> {
    let data = value.checked_object("invalid_market_resolved")?;
    data.checked_fields(&[
        "slug",
        "type",
        "winningOutcome",
        "winningIndex",
        "resolutionDate",
    ])?;
    let instrument = instrument(
        data.checked_required("slug")?
            .checked_nonempty_text("invalid_market_slug")?,
    )?;
    market_type(data.checked_required("type")?)?;
    let outcome = data
        .checked_required("winningOutcome")?
        .checked_nonempty_text("invalid_winning_outcome")?;
    let index = data
        .checked_required("winningIndex")?
        .checked_u64("invalid_winning_index")?;
    if !matches!((outcome, index), ("YES", 0) | ("NO", 1)) {
        return Err(Failure::for_instrument(
            "inconsistent_winning_outcome",
            instrument,
        ));
    }
    source_time_ns(data.checked_required("resolutionDate")?)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    Err(Failure::for_instrument(
        "market_resolved_not_in_replay_domain",
        instrument,
    ))
}

fn system(value: &Value) -> Result<MessageOutcome, Failure> {
    if let Some(message) = value.as_str() {
        if message.is_empty() || message.chars().any(char::is_control) {
            return Err(Failure::new("invalid_system_message"));
        }
        return Ok(MessageOutcome::Ignored("venue_system_control"));
    }
    let data = value.checked_object("invalid_system_message")?;
    data.checked_fields(&["message", "markets"])?;
    data.checked_required("message")?
        .checked_nonempty_text("invalid_system_message")?;
    if let Some(markets) = data.get("markets") {
        text_array(markets, "invalid_system_markets")?;
    }
    Ok(MessageOutcome::Ignored("venue_system_control"))
}

fn market_type(value: &Value) -> Result<(), Failure> {
    match value.checked_nonempty_text("invalid_market_type")? {
        "AMM" | "CLOB" => Ok(()),
        _ => Err(Failure::new("invalid_market_type")),
    }
}

fn instrument(native: &str) -> Result<InstrumentId, Failure> {
    InstrumentId::new(format!("limitless:{native}")).map_err(|_| Failure::new("invalid_instrument"))
}

fn source_time_ns(value: &Value) -> Result<u64, &'static str> {
    let text = value
        .as_str()
        .filter(|text| !text.is_empty())
        .ok_or("invalid_source_time")?;
    let parsed = DateTime::parse_from_rfc3339(text).map_err(|_| "invalid_source_time")?;
    let nanos = parsed.timestamp_nanos_opt().ok_or("invalid_source_time")?;
    u64::try_from(nanos).map_err(|_| "invalid_source_time")
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
            exact_fields(
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
                    "namespace",
                    "events",
                ],
            )?;
            let assets = required_text_array(object.get("asset_ids"), "invalid_control_asset_ids")?;
            if object.get("target_count").and_then(Value::as_u64)
                != Some(u64::try_from(assets.len()).map_err(|_| "invalid_control_target_count")?)
            {
                return Err("control_target_count_mismatch");
            }
            let mut instruments = Vec::with_capacity(assets.len());
            let mut seen = BTreeSet::new();
            for asset in assets {
                if !seen.insert(asset) {
                    return Err("duplicate_control_asset_id");
                }
                instruments.push(
                    InstrumentId::new(format!("limitless:{asset}"))
                        .map_err(|_| "invalid_control_asset_ids")?,
                );
            }
            required_text(object, "targets_path")?;
            optional_text(object, "target_metadata_digest")?;
            optional_text(object, "target_metadata_path")?;
            required_nonnegative_number(object, "fsync_interval_seconds")?;
            required_u64(object, "repaired_bytes_on_start")?;
            validate_clock_scope(object.get("clock_scope"))?;
            required_text(object, "url")?;
            required_text(object, "namespace")?;
            let events = required_text_array(object.get("events"), "invalid_control_events")?;
            if events.as_slice() != PUBLIC_EVENTS {
                return Err("unsupported_control_events");
            }
            let delivers_deltas = object
                .get("delivers_deltas")
                .and_then(Value::as_bool)
                .ok_or("invalid_control_delivers_deltas")?;
            if delivers_deltas {
                return Err("invalid_control_delivers_deltas");
            }
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionOpened {
                    epoch,
                    instruments,
                    delivers_deltas,
                    target_digest: optional_text(object, "target_digest")?,
                },
            )))
        }
        "connection_closed" => {
            exact_fields(object, &["event", "seconds_open", "records_this_epoch"])?;
            required_nonnegative_number(object, "seconds_open")?;
            required_u64(object, "records_this_epoch")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionClosed { epoch },
            )))
        }
        "connection_failed" => {
            exact_fields(
                object,
                &[
                    "event",
                    "error_type",
                    "error",
                    "seconds_open",
                    "frames_this_epoch",
                ],
            )?;
            let error_type = required_text(object, "error_type")?;
            let error = required_diagnostic(object, "error")?;
            let reason = format!("{error_type}:{}", error.escape_default());
            required_nonnegative_number(object, "seconds_open")?;
            required_u64(object, "frames_this_epoch")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionFailed { epoch, reason },
            )))
        }
        "subscription_changed" => {
            exact_fields(
                object,
                &["event", "from_digest", "to_digest", "added", "removed"],
            )?;
            required_text_array(object.get("added"), "invalid_control_asset_ids")?;
            required_text_array(object.get("removed"), "invalid_control_asset_ids")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::SubscriptionChanged {
                    from: optional_text(object, "from_digest")?,
                    to: required_text(object, "to_digest")?,
                },
            )))
        }
        "target_metadata_changed" => {
            exact_fields(
                object,
                &[
                    "event",
                    "target_digest",
                    "from_metadata_digest",
                    "to_metadata_digest",
                    "metadata_path",
                ],
            )?;
            required_text(object, "target_digest")?;
            optional_text(object, "metadata_path")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::MetadataChanged {
                    from: optional_text(object, "from_metadata_digest")?,
                    to: required_text(object, "to_metadata_digest")?,
                },
            )))
        }
        "subscription_sent" => {
            exact_fields(object, &["event", "target_digest", "target_count"])?;
            required_text(object, "target_digest")?;
            required_u64(object, "target_count")?;
            Ok(ProcessOutcome::Ignored("subscription_sent"))
        }
        "connection_closing" => {
            exact_fields(object, &["event", "reason"])?;
            required_text(object, "reason")?;
            Ok(ProcessOutcome::Ignored("connection_closing"))
        }
        "targets_unreadable" => {
            exact_fields(object, &["event", "error"])?;
            required_diagnostic(object, "error")?;
            Ok(ProcessOutcome::Ignored("targets_unreadable"))
        }
        "frame_not_utf8" => {
            exact_fields(object, &["event", "bytes"])?;
            if required_u64(object, "bytes")? == 0 {
                return Err("invalid_control_integer");
            }
            Ok(ProcessOutcome::Ignored("frame_not_utf8"))
        }
        _ => Err("unsupported_control_event"),
    }
}

fn exact_fields(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), &'static str> {
    object.checked_fields(allowed).map_err(|reject| reject.code)
}

fn required_text(object: &Map<String, Value>, field: &str) -> Result<String, &'static str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .map(str::to_owned)
        .ok_or("invalid_control_field")
}

fn optional_text(object: &Map<String, Value>, field: &str) -> Result<Option<String>, &'static str> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() && !value.chars().any(char::is_control) => {
            Ok(Some(value.clone()))
        }
        _ => Err("invalid_control_field"),
    }
}

fn required_diagnostic<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, &'static str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or("invalid_control_field")
}

fn required_text_array<'a>(
    value: Option<&'a Value>,
    code: &'static str,
) -> Result<Vec<&'a str>, &'static str> {
    let values = value.and_then(Value::as_array).ok_or(code)?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|text| !text.is_empty() && !text.chars().any(char::is_control))
                .ok_or(code)
        })
        .collect()
}

fn text_array(value: &Value, code: &'static str) -> Result<(), Failure> {
    required_text_array(Some(value), code)
        .map(|_| ())
        .map_err(Failure::new)
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<u64, &'static str> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .ok_or("invalid_control_integer")
}

fn required_nonnegative_number(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), &'static str> {
    let value = object
        .get(field)
        .and_then(Value::as_number)
        .ok_or("invalid_control_number")?;
    if value.as_str().starts_with('-') {
        Err("invalid_control_number")
    } else {
        Ok(())
    }
}

fn validate_clock_scope(value: Option<&Value>) -> Result<(), &'static str> {
    let object = value
        .and_then(Value::as_object)
        .ok_or("invalid_clock_scope")?;
    exact_fields(
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
        required_text(object, field)?;
    }
    object
        .get("comparable_across_processes")
        .and_then(Value::as_bool)
        .ok_or("invalid_clock_scope")?;
    Ok(())
}
