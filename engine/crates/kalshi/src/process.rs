use std::collections::HashSet;

use indexer_types::{EnvelopeView, RecordKind};
use replay_domain::{ControlEvent, InstrumentId, SegmentEvent};
use serde::Deserialize;
use serde_json::Value;

pub(crate) enum ProcessOutcome {
    Event(SegmentEvent),
    Ignored(&'static str),
}

#[derive(Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
enum ProcessControl {
    ConnectionOpened(Box<ConnectionOpened>),
    ConnectionClosed {
        seconds_open: f64,
        records_this_epoch: u64,
    },
    ConnectionFailed {
        error_type: String,
        error: String,
        seconds_open: f64,
        frames_this_epoch: u64,
    },
    SubscriptionChanged {
        from_digest: Option<String>,
        to_digest: String,
        added: Vec<String>,
        removed: Vec<String>,
    },
    TargetMetadataChanged {
        target_digest: String,
        from_metadata_digest: Option<String>,
        to_metadata_digest: String,
        metadata_path: Option<String>,
    },
    SubscriptionSent {
        target_digest: String,
        target_count: u64,
    },
    ConnectionClosing {
        reason: String,
    },
    OrderbookReconciliationRequest {
        sid: u64,
        command_id: u64,
        market_tickers: Vec<String>,
        reason: String,
    },
    OrderbookReconciliationDisabled {
        reason: String,
        channel: Option<String>,
        command_id: Option<u64>,
        error_type: Option<String>,
        error: Option<String>,
        code: Option<Value>,
        detail: Option<String>,
    },
    OrderbookReconciliationBackoff {
        command_id: u64,
        code: Option<Value>,
        from_sweep_seconds: f64,
        to_sweep_seconds: f64,
        detail: Option<String>,
    },
    TargetsUnreadable {
        error: String,
    },
    FrameNotUtf8 {
        bytes: u64,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionOpened {
    target_digest: Option<String>,
    target_count: u64,
    asset_ids: Vec<String>,
    targets_path: String,
    target_metadata_digest: Option<String>,
    target_metadata_path: Option<String>,
    delivers_deltas: bool,
    fsync_interval_seconds: f64,
    repaired_bytes_on_start: u64,
    clock_scope: ClockScope,
    url: String,
    channels: Vec<String>,
    send_initial_snapshot: bool,
    verified_against_live_socket: bool,
    snapshot_sweep_seconds: f64,
    snapshot_max_age_seconds: f64,
    snapshot_request_cooldown_seconds: f64,
    key_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClockScope {
    lane: String,
    clock: String,
    scope: String,
    scope_id: String,
    comparable_across_processes: bool,
    platform: String,
}

pub(crate) fn normalize_process(
    envelope: &EnvelopeView<'_>,
    value: &Value,
) -> Result<ProcessOutcome, &'static str> {
    if !matches!(envelope.kind, RecordKind::Control | RecordKind::Fault) {
        return Err("unexpected_record_kind");
    }
    let control = ProcessControl::deserialize(value)
        .map_err(|error| classify_decode_error(&error.to_string()))?;
    let epoch = envelope.connection_epoch.as_str().to_owned();
    match control {
        ProcessControl::ConnectionOpened(opened) => {
            let ConnectionOpened {
                target_digest,
                target_count,
                asset_ids,
                targets_path,
                target_metadata_digest,
                target_metadata_path,
                delivers_deltas,
                fsync_interval_seconds,
                repaired_bytes_on_start,
                clock_scope,
                url,
                channels,
                send_initial_snapshot,
                verified_against_live_socket,
                snapshot_sweep_seconds,
                snapshot_max_age_seconds,
                snapshot_request_cooldown_seconds,
                key_id,
            } = *opened;
            validate_optional_text(target_digest.as_deref())?;
            validate_text(&targets_path)?;
            validate_optional_text(target_metadata_digest.as_deref())?;
            validate_optional_text(target_metadata_path.as_deref())?;
            validate_text(&url)?;
            validate_texts(&channels)?;
            validate_optional_text(key_id.as_deref())?;
            validate_clock_scope(&clock_scope)?;
            let _ = (
                repaired_bytes_on_start,
                send_initial_snapshot,
                verified_against_live_socket,
            );
            for duration in [
                fsync_interval_seconds,
                snapshot_sweep_seconds,
                snapshot_max_age_seconds,
                snapshot_request_cooldown_seconds,
            ] {
                validate_duration(duration)?;
            }
            if target_count != asset_ids.len() as u64 {
                return Err("control_target_count_mismatch");
            }
            let mut seen = HashSet::with_capacity(asset_ids.len());
            for asset in &asset_ids {
                validate_text(asset).map_err(|_| "invalid_control_asset_ids")?;
                if !seen.insert(asset.as_str()) {
                    return Err("duplicate_control_asset_id");
                }
            }
            drop(seen);
            let mut instruments = Vec::with_capacity(asset_ids.len());
            for asset in asset_ids {
                instruments.push(
                    InstrumentId::new(format!("kalshi:{asset}"))
                        .map_err(|_| "invalid_control_asset_ids")?,
                );
            }
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionOpened {
                    epoch,
                    instruments,
                    delivers_deltas,
                    target_digest,
                },
            )))
        }
        ProcessControl::ConnectionClosed {
            seconds_open,
            records_this_epoch,
        } => {
            validate_duration(seconds_open)?;
            let _ = records_this_epoch;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionClosed { epoch },
            )))
        }
        ProcessControl::ConnectionFailed {
            error_type,
            error,
            seconds_open,
            frames_this_epoch,
        } => {
            validate_text(&error_type)?;
            validate_text(&error)?;
            validate_duration(seconds_open)?;
            let _ = frames_this_epoch;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionFailed {
                    epoch,
                    reason: error,
                },
            )))
        }
        ProcessControl::SubscriptionChanged {
            from_digest,
            to_digest,
            added,
            removed,
        } => {
            validate_optional_text(from_digest.as_deref())?;
            validate_text(&to_digest)?;
            validate_texts(&added).map_err(|_| "invalid_control_asset_ids")?;
            validate_texts(&removed).map_err(|_| "invalid_control_asset_ids")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::SubscriptionChanged {
                    from: from_digest,
                    to: to_digest,
                },
            )))
        }
        ProcessControl::TargetMetadataChanged {
            target_digest,
            from_metadata_digest,
            to_metadata_digest,
            metadata_path,
        } => {
            validate_text(&target_digest)?;
            validate_optional_text(from_metadata_digest.as_deref())?;
            validate_text(&to_metadata_digest)?;
            validate_optional_text(metadata_path.as_deref())?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::MetadataChanged {
                    from: from_metadata_digest,
                    to: to_metadata_digest,
                },
            )))
        }
        ProcessControl::SubscriptionSent {
            target_digest,
            target_count,
        } => {
            validate_text(&target_digest)?;
            let _ = target_count;
            Ok(ProcessOutcome::Ignored("subscription_sent"))
        }
        ProcessControl::ConnectionClosing { reason } => {
            validate_text(&reason)?;
            Ok(ProcessOutcome::Ignored("connection_closing"))
        }
        ProcessControl::OrderbookReconciliationRequest {
            sid,
            command_id,
            market_tickers,
            reason,
        } => {
            validate_positive(sid)?;
            validate_positive(command_id)?;
            validate_texts(&market_tickers).map_err(|_| "invalid_control_asset_ids")?;
            validate_text(&reason)?;
            Ok(ProcessOutcome::Ignored("reconciliation_request"))
        }
        ProcessControl::OrderbookReconciliationDisabled {
            reason,
            channel,
            command_id,
            error_type,
            error,
            code,
            detail,
        } => {
            validate_text(&reason)?;
            validate_optional_text(channel.as_deref())?;
            if let Some(command_id) = command_id {
                validate_positive(command_id)?;
            }
            validate_optional_text(error_type.as_deref())?;
            validate_optional_text(error.as_deref())?;
            validate_code(code.as_ref())?;
            validate_optional_text(detail.as_deref())?;
            Ok(ProcessOutcome::Ignored("reconciliation_disabled"))
        }
        ProcessControl::OrderbookReconciliationBackoff {
            command_id,
            code,
            from_sweep_seconds,
            to_sweep_seconds,
            detail,
        } => {
            validate_positive(command_id)?;
            validate_code(code.as_ref())?;
            validate_duration(from_sweep_seconds)?;
            validate_duration(to_sweep_seconds)?;
            validate_optional_text(detail.as_deref())?;
            Ok(ProcessOutcome::Ignored("reconciliation_backoff"))
        }
        ProcessControl::TargetsUnreadable { error } => {
            validate_text(&error)?;
            Ok(ProcessOutcome::Ignored("targets_unreadable"))
        }
        ProcessControl::FrameNotUtf8 { bytes } => {
            validate_positive(bytes)?;
            Ok(ProcessOutcome::Ignored("frame_not_utf8"))
        }
    }
}

fn classify_decode_error(message: &str) -> &'static str {
    if message.contains("unknown field") {
        "unknown_field"
    } else if message.contains("unknown variant") {
        "unsupported_control_event"
    } else if message.contains("expected a non-negative integer") {
        "invalid_control_integer"
    } else {
        "invalid_control_event"
    }
}

fn validate_text(value: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.chars().any(char::is_control) {
        Err("invalid_control_field")
    } else {
        Ok(())
    }
}

fn validate_optional_text(value: Option<&str>) -> Result<(), &'static str> {
    value.map_or(Ok(()), validate_text)
}

fn validate_texts(values: &[String]) -> Result<(), &'static str> {
    values.iter().try_for_each(|value| validate_text(value))
}

fn validate_duration(value: f64) -> Result<(), &'static str> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err("invalid_control_number")
    }
}

fn validate_positive(value: u64) -> Result<(), &'static str> {
    if value > 0 {
        Ok(())
    } else {
        Err("invalid_control_integer")
    }
}

fn validate_code(value: Option<&Value>) -> Result<(), &'static str> {
    match value {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(value)) if !value.is_empty() => Ok(()),
        Some(Value::Number(value)) if value.as_u64().is_some() => Ok(()),
        _ => Err("invalid_control_code"),
    }
}

fn validate_clock_scope(scope: &ClockScope) -> Result<(), &'static str> {
    for value in [
        &scope.lane,
        &scope.clock,
        &scope.scope,
        &scope.scope_id,
        &scope.platform,
    ] {
        validate_text(value)?;
    }
    let _ = scope.comparable_across_processes;
    Ok(())
}
