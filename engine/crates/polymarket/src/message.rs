use std::collections::BTreeSet;

use canonical_normalizer::{CheckedObject as _, CheckedValue as _};
use indexer_types::{EnvelopeView, RecordKind, SourceCursor, Stream};
use replay_domain::{ControlEvent, SegmentEvent};
use serde_json::{Map, Value};

use crate::{
    Config,
    error::Reject,
    event::{AuditSnapshot, PriceChange, Snapshot, Trade, instrument, timestamp, validate_market},
    value::{CheckedObject, CheckedValue},
};

pub(crate) enum MessageOutcome {
    Events(Vec<SegmentEvent>),
}

pub(crate) enum ProcessOutcome {
    Event(SegmentEvent),
    Ignored(&'static str),
}

pub(crate) fn normalize_message(
    envelope: &EnvelopeView<'_>,
    value: &Value,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    let object = value.object("message_not_object")?;
    let event_type = object
        .required("event_type")?
        .nonempty_text("invalid_message_type")?;
    match event_type {
        "book" => {
            expect_stream(envelope, Stream::PublicBook)?;
            // Prefix validation must precede cursor checks for stable rejects.
            Snapshot::prefix(object, config)?;
            expect_unsequenced(envelope)?;
            Ok(MessageOutcome::Events(
                Snapshot::parse(object, config)?.into(),
            ))
        }
        "price_change" => {
            expect_public_book(envelope)?;
            Ok(MessageOutcome::Events(
                PriceChange::parse(object, config)?.into(),
            ))
        }
        "last_trade_price" => {
            expect_public_book(envelope)?;
            Ok(MessageOutcome::Events(Trade::parse(object, config)?.into()))
        }
        "tick_size_change" => unsupported_tick_size(envelope, object, config),
        "best_bid_ask" => unsupported_best_bid_ask(envelope, object, config),
        "new_market" => unsupported_new_market(envelope, object, config),
        "market_resolved" => unsupported_market_resolved(envelope, object, config),
        _ => Err(Reject::new("unsupported_message_type")),
    }
}

pub(crate) fn normalize_rest_snapshot(
    envelope: &EnvelopeView<'_>,
    value: &Value,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    let object = value.object("snapshot_not_object")?;
    expect_stream(envelope, Stream::PublicSnapshot)?;
    let (instrument, timestamp_ms) = AuditSnapshot::prefix(object, config)?;
    if !matches!(envelope.source_cursor, Some(SourceCursor::SnapshotTime { source_time_ms }) if source_time_ms == timestamp_ms)
    {
        return Err(Reject::for_instrument(
            "snapshot_cursor_mismatch",
            instrument,
        ));
    }
    Ok(MessageOutcome::Events(
        AuditSnapshot::parse(object, config)?.into(),
    ))
}

fn unsupported_tick_size(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    expect_public_book(envelope)?;
    object.fields(
        &[
            "event_type",
            "asset_id",
            "market",
            "old_tick_size",
            "new_tick_size",
            "timestamp",
        ],
        config.accept_additive_fields,
    )?;
    let instrument = instrument(object.required("asset_id")?)?;
    validate_market(object.required("market")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    timestamp(object.required("timestamp")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    for field in ["old_tick_size", "new_tick_size"] {
        object
            .required(field)?
            .price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    }
    Err(Reject::for_instrument(
        "unsupported_tick_size_change",
        instrument,
    ))
}

fn unsupported_best_bid_ask(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    expect_public_book(envelope)?;
    object.fields(
        &[
            "event_type",
            "market",
            "asset_id",
            "best_bid",
            "best_ask",
            "spread",
            "timestamp",
        ],
        config.accept_additive_fields,
    )?;
    let instrument = instrument(object.required("asset_id")?)?;
    validate_market(object.required("market")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    timestamp(object.required("timestamp")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    for field in ["best_bid", "best_ask", "spread"] {
        object
            .required(field)?
            .price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    }
    Err(Reject::for_instrument(
        "unsupported_best_bid_ask",
        instrument,
    ))
}

fn unsupported_new_market(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    expect_public_book(envelope)?;
    object.fields(
        &[
            "event_type",
            "id",
            "question",
            "market",
            "slug",
            "assets_ids",
            "outcomes",
            "timestamp",
            "description",
            "event_message",
            "tags",
            "condition_id",
            "active",
            "clob_token_ids",
            "sports_market_type",
            "line",
            "game_start_time",
            "order_price_min_tick_size",
            "group_item_title",
        ],
        config.accept_additive_fields,
    )?;
    for field in ["id", "question", "slug"] {
        object
            .required(field)?
            .nonempty_text("invalid_market_metadata")?;
    }
    validate_market(object.required("market")?).map_err(Reject::new)?;
    timestamp(object.required("timestamp")?).map_err(Reject::new)?;
    validate_asset_array(object.required("assets_ids")?)?;
    validate_text_array(object.required("outcomes")?, "invalid_outcomes")?;
    Err(Reject::new("unsupported_new_market"))
}

fn unsupported_market_resolved(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    expect_public_book(envelope)?;
    object.fields(
        &[
            "event_type",
            "id",
            "market",
            "assets_ids",
            "winning_asset_id",
            "winning_outcome",
            "timestamp",
            "event_message",
            "tags",
            "question",
            "slug",
            "description",
            "outcomes",
        ],
        config.accept_additive_fields,
    )?;
    object
        .required("id")?
        .nonempty_text("invalid_market_metadata")?;
    validate_market(object.required("market")?).map_err(Reject::new)?;
    timestamp(object.required("timestamp")?).map_err(Reject::new)?;
    validate_asset_array(object.required("assets_ids")?)?;
    instrument(object.required("winning_asset_id")?)?;
    object
        .required("winning_outcome")?
        .nonempty_text("invalid_winning_outcome")?;
    Err(Reject::new("unsupported_market_resolved"))
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
            process_fields(
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
                    "feed",
                    "poll_seconds",
                    "batch_size",
                ],
            )?;
            let assets = object
                .get("asset_ids")
                .and_then(Value::as_array)
                .ok_or("invalid_control_asset_ids")?;
            let mut seen = BTreeSet::new();
            let mut instruments = Vec::with_capacity(assets.len());
            for asset in assets {
                let instrument = instrument(asset).map_err(|_| "invalid_control_asset_ids")?;
                if !seen.insert(instrument.as_str().to_owned()) {
                    return Err("duplicate_control_asset_id");
                }
                instruments.push(instrument);
            }
            let count = object
                .get("target_count")
                .and_then(Value::as_u64)
                .ok_or("invalid_control_target_count")?;
            if count != assets.len() as u64 {
                return Err("control_target_count_mismatch");
            }
            required_text(object, "targets_path")?;
            optional_text(object, "target_digest")?;
            optional_text(object, "target_metadata_digest")?;
            optional_text(object, "target_metadata_path")?;
            required_nonnegative_number(object, "fsync_interval_seconds")?;
            object
                .get("repaired_bytes_on_start")
                .and_then(Value::as_u64)
                .ok_or("invalid_control_integer")?;
            validate_clock_scope(object.get("clock_scope"))?;
            required_text(object, "url")?;
            optional_text(object, "feed")?;
            optional_nonnegative_number(object, "poll_seconds")?;
            optional_positive_u64(object, "batch_size")?;
            let delivers_deltas = object
                .get("delivers_deltas")
                .and_then(Value::as_bool)
                .ok_or("invalid_control_delivers_deltas")?;
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
            process_fields(object, &["event", "seconds_open", "records_this_epoch"])?;
            required_nonnegative_number(object, "seconds_open")?;
            required_u64(object, "records_this_epoch")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionClosed { epoch },
            )))
        }
        "connection_failed" => {
            process_fields(
                object,
                &[
                    "event",
                    "error_type",
                    "error",
                    "seconds_open",
                    "frames_this_epoch",
                ],
            )?;
            required_text(object, "error_type")?;
            let reason = required_text(object, "error")?;
            required_nonnegative_number(object, "seconds_open")?;
            required_u64(object, "frames_this_epoch")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionFailed { epoch, reason },
            )))
        }
        "subscription_changed" => {
            process_fields(
                object,
                &["event", "from_digest", "to_digest", "added", "removed"],
            )?;
            validate_asset_array(object.get("added").ok_or("missing_control_field")?)
                .map_err(|_| "invalid_control_asset_ids")?;
            validate_asset_array(object.get("removed").ok_or("missing_control_field")?)
                .map_err(|_| "invalid_control_asset_ids")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::SubscriptionChanged {
                    from: optional_text(object, "from_digest")?,
                    to: required_text(object, "to_digest")?,
                },
            )))
        }
        "target_metadata_changed" => {
            process_fields(
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
            process_fields(object, &["event", "target_digest", "target_count"])?;
            required_text(object, "target_digest")?;
            required_u64(object, "target_count")?;
            Ok(ProcessOutcome::Ignored("subscription_sent"))
        }
        "connection_closing" => {
            process_fields(object, &["event", "reason"])?;
            required_text(object, "reason")?;
            Ok(ProcessOutcome::Ignored("connection_closing"))
        }
        "targets_unreadable" => {
            process_fields(object, &["event", "error"])?;
            required_text(object, "error")?;
            Ok(ProcessOutcome::Ignored("targets_unreadable"))
        }
        "frame_not_utf8" => {
            process_fields(object, &["event", "bytes"])?;
            object
                .get("bytes")
                .and_then(Value::as_u64)
                .filter(|value| *value > 0)
                .ok_or("invalid_control_integer")?;
            Ok(ProcessOutcome::Ignored("frame_not_utf8"))
        }
        _ => Err("unsupported_control_event"),
    }
}

fn expect_public_book(envelope: &EnvelopeView<'_>) -> Result<(), Reject> {
    expect_stream(envelope, Stream::PublicBook)?;
    expect_unsequenced(envelope)
}

fn expect_stream(envelope: &EnvelopeView<'_>, expected: Stream) -> Result<(), Reject> {
    if envelope.kind != RecordKind::VenueFrame {
        Err(Reject::new("unexpected_record_kind"))
    } else if envelope.stream != expected {
        Err(Reject::new("message_stream_mismatch"))
    } else {
        Ok(())
    }
}

fn expect_unsequenced(envelope: &EnvelopeView<'_>) -> Result<(), Reject> {
    if matches!(
        envelope.source_cursor,
        Some(SourceCursor::Unsequenced { .. })
    ) {
        Ok(())
    } else {
        Err(Reject::new("unexpected_sequence_cursor"))
    }
}

fn validate_asset_array(value: &Value) -> Result<(), Reject> {
    let values = value
        .as_array()
        .ok_or_else(|| Reject::new("invalid_asset_ids"))?;
    let mut seen = BTreeSet::new();
    for value in values {
        let instrument = instrument(value)?;
        if !seen.insert(instrument.as_str().to_owned()) {
            return Err(Reject::new("duplicate_asset_id"));
        }
    }
    Ok(())
}

fn validate_text_array(value: &Value, code: &'static str) -> Result<(), Reject> {
    let values = value.as_array().ok_or_else(|| Reject::new(code))?;
    for value in values {
        value.nonempty_text(code)?;
    }
    Ok(())
}

fn process_fields(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), &'static str> {
    object.checked_fields(allowed).map_err(|_| "unknown_field")
}

fn required_text(object: &Map<String, Value>, field: &str) -> Result<String, &'static str> {
    object
        .checked_required(field)
        .map_err(|_| "invalid_control_field")?
        .checked_nonempty_text()
        .map(str::to_owned)
        .map_err(|_| "invalid_control_field")
}

fn optional_text(object: &Map<String, Value>, field: &str) -> Result<Option<String>, &'static str> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .checked_nonempty_text()
            .map(|value| Some(value.to_owned()))
            .map_err(|_| "invalid_control_field"),
    }
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<(), &'static str> {
    object
        .checked_required(field)
        .map_err(|_| "invalid_control_integer")?
        .checked_nonnegative_u64()
        .map(|_| ())
        .map_err(|_| "invalid_control_integer")
}

fn optional_positive_u64(object: &Map<String, Value>, field: &str) -> Result<(), &'static str> {
    match object.get(field) {
        None => Ok(()),
        Some(value) => value
            .checked_positive_u64()
            .map(|_| ())
            .map_err(|_| "invalid_control_integer"),
    }
}

fn required_nonnegative_number(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), &'static str> {
    object
        .checked_required(field)
        .map_err(|_| "invalid_control_number")?
        .checked_nonnegative_number()
        .map(|_| ())
        .map_err(|_| "invalid_control_number")
}

fn optional_nonnegative_number(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), &'static str> {
    if object.contains_key(field) {
        required_nonnegative_number(object, field)
    } else {
        Ok(())
    }
}

fn validate_clock_scope(value: Option<&Value>) -> Result<(), &'static str> {
    let object = value
        .and_then(Value::as_object)
        .ok_or("invalid_control_clock_scope")?;
    process_fields(
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
        .ok_or("invalid_control_clock_scope")?;
    Ok(())
}
