//! Annotation failures are operator diagnostics, never rejected book operations.
use chrono::DateTime;
use replay_domain::{EventKind, Resolution, VenueTime};
use serde_json::{Map, Value};

#[derive(Debug)]
pub(super) struct Annotation {
    pub time: Option<VenueTime>,
    pub diagnostics: Vec<&'static str>,
}

pub(super) fn extract(
    kind: &str,
    msg: Option<&Map<String, Value>>,
    sent_ms: Option<u64>,
) -> Annotation {
    let mut diagnostics = Vec::new();
    let sent_ns = sent_ms.and_then(|ms| match ms.checked_mul(1_000_000) {
        Some(ns) => Some(ns),
        None => {
            diagnostics.push("sending_time_overflow");
            None
        }
    });
    let event = match (kind, msg) {
        ("orderbook_delta", Some(msg)) => delta(msg),
        ("trade", Some(msg)) => trade(msg),
        _ => Ok(None),
    };
    let event = match event {
        Ok(event) => event,
        Err(code) => {
            diagnostics.push(code);
            None
        }
    };
    let time = if event.is_some() || sent_ns.is_some() {
        Some(
            VenueTime::new(
                event.map(|v| v.0),
                event.map(|v| v.1),
                event.map(|_| EventKind::ExchangeEvent),
                sent_ns,
            )
            .expect("validated annotation"),
        )
    } else {
        None
    };
    Annotation { time, diagnostics }
}

fn delta(msg: &Map<String, Value>) -> Result<Option<(u64, Resolution)>, &'static str> {
    let ms = msg.get("ts_ms").and_then(Value::as_u64);
    if let Some(ts) = msg.get("ts").and_then(Value::as_str) {
        let (ns, resolution) = iso(ts)?;
        if ms.is_some_and(|ms| ns / 1_000_000 != ms) {
            return Err("inconsistent_event_time");
        }
        Ok(Some((ns, resolution)))
    } else {
        ms.map(|ms| {
            ms.checked_mul(1_000_000)
                .map(|ns| (ns, Resolution::Millisecond))
                .ok_or("event_time_overflow")
        })
        .transpose()
    }
}

fn trade(msg: &Map<String, Value>) -> Result<Option<(u64, Resolution)>, &'static str> {
    let Some(ms) = msg.get("ts_ms").and_then(Value::as_u64) else {
        return Ok(None);
    };
    if msg
        .get("ts")
        .and_then(Value::as_u64)
        .is_some_and(|seconds| seconds != ms / 1_000)
    {
        return Err("inconsistent_event_time");
    }
    ms.checked_mul(1_000_000)
        .map(|ns| Some((ns, Resolution::Millisecond)))
        .ok_or("event_time_overflow")
}

fn iso(ts: &str) -> Result<(u64, Resolution), &'static str> {
    let fraction = ts.split_once('.').map_or(0, |(_, suffix)| {
        suffix.bytes().take_while(u8::is_ascii_digit).count()
    });
    // Chrono accepts and truncates fractions beyond nanoseconds. Never do that
    // to persisted venue evidence; sub-microsecond input is outside this profile.
    if fraction > 9 {
        return Err("unsupported_event_time_precision");
    }
    let value = DateTime::parse_from_rfc3339(ts).map_err(|_| "invalid_event_time_iso")?;
    let nanos = value.timestamp_subsec_nanos();
    if nanos >= 1_000_000_000 || nanos % 1_000 != 0 {
        return Err("unsupported_event_time_precision");
    }
    let seconds = u64::try_from(value.timestamp()).map_err(|_| "event_time_overflow")?;
    let ns = seconds
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(u64::from(nanos)))
        .ok_or("event_time_overflow")?;
    Ok((
        ns,
        if fraction > 3 {
            Resolution::Microsecond
        } else {
            Resolution::Millisecond
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn inconsistency_retains_send_time_and_has_an_operator_diagnostic() {
        for (kind, msg) in [
            (
                "orderbook_delta",
                json!({"ts":"2022-11-22T20:44:01.123456Z", "ts_ms":1669149841124_u64}),
            ),
            (
                "trade",
                json!({"ts":1669149842_u64, "ts_ms":1669149841123_u64}),
            ),
        ] {
            let annotation = extract(kind, msg.as_object(), Some(1669149841130));
            assert_eq!(annotation.diagnostics, ["inconsistent_event_time"]);
            let time = annotation.time.unwrap();
            assert_eq!(time.event_ns(), None);
            assert_eq!(time.event_resolution(), None);
            assert_eq!(time.event_kind(), None);
            assert_eq!(time.sent_ns(), Some(1669149841130000000));
            assert_eq!(extract(kind, msg.as_object(), None).time, None);
        }
    }
    #[test]
    fn invalid_or_unrepresentable_annotation_never_guesses_or_truncates() {
        for ts in [
            "bad",
            "2022-11-22T20:44:01.1234567Z",
            "2022-11-22T20:44:01.1234560000Z",
            "1969-12-31T23:59:59Z",
            "2016-12-31T23:59:60Z",
            "3000-01-01T00:00:00Z",
        ] {
            let msg = json!({"ts":ts});
            let annotation = extract("orderbook_delta", msg.as_object(), None);
            assert_eq!(annotation.time, None, "{ts}");
            assert_eq!(annotation.diagnostics.len(), 1, "{ts}");
        }
        let msg = json!({"ts_ms":u64::MAX});
        let annotation = extract("orderbook_delta", msg.as_object(), Some(u64::MAX));
        assert_eq!(annotation.time, None);
        assert_eq!(
            annotation.diagnostics,
            ["sending_time_overflow", "event_time_overflow"]
        );
    }
}
