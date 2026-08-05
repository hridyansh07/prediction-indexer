//! Envelope parsing: one exact delivered line in, a typed view out.
//!
//! Each version's field set is closed. Legacy v1 records have the original ten
//! fields and no explicit version; v2 adds `envelope_version` and
//! `monotonic_ns`. An unknown field or mixed-version shape rejects the whole line,
//! because a parser that ignored extras would let a splice write something the
//! tape claims to carry and no reader ever sees — silently, and for as long as
//! nobody checks. Failing loudly on the first record is the cheaper failure by a
//! wide margin.
//!
//! Identifiers are *borrowed* out of the caller's line rather than unescaped into
//! owned strings. That is why the splice is required to emit plain ASCII for
//! `record_id` and `connection_epoch`: the parser hands back the bytes between the
//! quotes, so an escape sequence there would silently mean something other than it
//! appears to.

use serde::de::{MapAccess, Visitor};
use serde_json::value::RawValue;

use crate::error::EnvelopeError;
use crate::identity::{EpochId, LogicalTime, RecordId, RecordKind, SourceCursor, Stream, Venue};

/// One parsed envelope, borrowing from the line it was read out of.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvelopeView<'a> {
    /// Absent on the wire means the original v1 shape.
    pub envelope_version: u64,
    /// The splice's own delivery order. Its claim, not our authority — the store
    /// assigns the global `EvidenceSeq` and the two are compared, not conflated.
    pub delivery_index: u64,
    pub record_id: RecordId<'a>,
    pub visible_ns: LogicalTime,
    /// Present in v2. This is meaningful only within the clock scope recorded by
    /// `connection_opened`; it is never a Unix timestamp.
    pub monotonic_ns: Option<LogicalTime>,
    pub venue: Venue,
    pub stream: Stream,
    pub connection_epoch: EpochId<'a>,
    pub local_counter: u64,
    pub source_cursor: Option<SourceCursor>,
    pub kind: RecordKind,
    /// The venue frame verbatim, unescaped by serde. The exact transport line is
    /// separate evidence and is hashed independently.
    pub raw_payload: String,
}

impl<'a> EnvelopeView<'a> {
    pub fn parse(line: &'a [u8]) -> Result<Self, EnvelopeError> {
        let text =
            std::str::from_utf8(strip_newline(line)).map_err(|_| EnvelopeError::InvalidUtf8)?;
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let view = serde::Deserializer::deserialize_map(&mut deserializer, EnvelopeVisitor)
            .map_err(not_json)??;
        deserializer.end().map_err(not_json)?;
        Ok(view)
    }
}

/// Strips one framing newline and only that. Any other trailing byte is content.
pub fn strip_newline(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

fn not_json(error: serde_json::Error) -> EnvelopeError {
    EnvelopeError::NotJson(error.to_string())
}

#[derive(Default)]
struct Slots<'a> {
    envelope_version: Option<u64>,
    delivery_index: Option<u64>,
    record_id: Option<&'a str>,
    visible_ns: Option<LogicalTime>,
    monotonic_ns: Option<LogicalTime>,
    venue: Option<Venue>,
    stream: Option<Stream>,
    connection_epoch: Option<&'a str>,
    local_counter: Option<u64>,
    // Doubly wrapped on purpose: the outer Option is "was the field present",
    // the inner is "was it null". `source_cursor: null` is legal; an absent
    // `source_cursor` is not, and the two must not collapse.
    source_cursor: Option<Option<SourceCursor>>,
    kind: Option<RecordKind>,
    raw_payload: Option<String>,
}

impl<'a> Slots<'a> {
    fn finish(self) -> Result<EnvelopeView<'a>, EnvelopeError> {
        let (envelope_version, monotonic_ns) = match (self.envelope_version, self.monotonic_ns) {
            (None, None) => (1, None),
            (Some(2), Some(monotonic_ns)) => (2, Some(monotonic_ns)),
            (Some(version), _) if version != 2 => {
                return Err(EnvelopeError::UnsupportedEnvelopeVersion(version));
            }
            _ => return Err(EnvelopeError::InvalidEnvelopeVersionShape),
        };
        Ok(EnvelopeView {
            envelope_version,
            delivery_index: self
                .delivery_index
                .ok_or(EnvelopeError::MissingField("delivery_index"))?,
            record_id: RecordId::new(
                self.record_id
                    .ok_or(EnvelopeError::MissingField("record_id"))?,
            ),
            visible_ns: self
                .visible_ns
                .ok_or(EnvelopeError::MissingField("visible_ns"))?,
            monotonic_ns,
            venue: self.venue.ok_or(EnvelopeError::MissingField("venue"))?,
            stream: self.stream.ok_or(EnvelopeError::MissingField("stream"))?,
            connection_epoch: EpochId::new(
                self.connection_epoch
                    .ok_or(EnvelopeError::MissingField("connection_epoch"))?,
            ),
            local_counter: self
                .local_counter
                .ok_or(EnvelopeError::MissingField("local_counter"))?,
            source_cursor: self
                .source_cursor
                .ok_or(EnvelopeError::MissingField("source_cursor"))?,
            kind: self.kind.ok_or(EnvelopeError::MissingField("kind"))?,
            raw_payload: self
                .raw_payload
                .ok_or(EnvelopeError::MissingField("raw_payload"))?,
        })
    }
}

struct EnvelopeVisitor;

impl<'de> Visitor<'de> for EnvelopeVisitor {
    type Value = Result<EnvelopeView<'de>, EnvelopeError>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a capture envelope object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut slots = Slots::default();
        let mut rejected: Option<EnvelopeError> = None;

        while let Some((key, value)) = map.next_entry::<&str, &RawValue>()? {
            let lexeme = value.get().as_bytes();
            let outcome = match key {
                "envelope_version" => uint(lexeme, "envelope_version")
                    .and_then(|v| set_once(&mut slots.envelope_version, v, "envelope_version")),
                "delivery_index" => uint(lexeme, "delivery_index")
                    .and_then(|v| set_once(&mut slots.delivery_index, v, "delivery_index")),
                "record_id" => identifier(lexeme, "record_id")
                    .and_then(|v| set_once(&mut slots.record_id, v, "record_id")),
                "visible_ns" => uint(lexeme, "visible_ns").and_then(|v| {
                    set_once(&mut slots.visible_ns, LogicalTime::from_ns(v), "visible_ns")
                }),
                "monotonic_ns" => uint(lexeme, "monotonic_ns").and_then(|v| {
                    set_once(
                        &mut slots.monotonic_ns,
                        LogicalTime::from_ns(v),
                        "monotonic_ns",
                    )
                }),
                "venue" => plain_string(lexeme, "venue")
                    .and_then(|t| {
                        Venue::from_wire(t).ok_or_else(|| EnvelopeError::UnknownVenue(t.to_owned()))
                    })
                    .and_then(|v| set_once(&mut slots.venue, v, "venue")),
                "stream" => plain_string(lexeme, "stream")
                    .and_then(|t| {
                        Stream::from_wire(t)
                            .ok_or_else(|| EnvelopeError::UnknownStream(t.to_owned()))
                    })
                    .and_then(|v| set_once(&mut slots.stream, v, "stream")),
                "connection_epoch" => identifier(lexeme, "connection_epoch")
                    .and_then(|v| set_once(&mut slots.connection_epoch, v, "connection_epoch")),
                "local_counter" => uint(lexeme, "local_counter")
                    .and_then(|v| set_once(&mut slots.local_counter, v, "local_counter")),
                "source_cursor" => (if lexeme == b"null" {
                    Ok(None)
                } else {
                    parse_cursor(value).map(Some)
                })
                .and_then(|v| set_once(&mut slots.source_cursor, v, "source_cursor")),
                "kind" => plain_string(lexeme, "kind")
                    .and_then(|t| {
                        RecordKind::from_wire(t)
                            .ok_or_else(|| EnvelopeError::UnknownRecordKind(t.to_owned()))
                    })
                    .and_then(|v| set_once(&mut slots.kind, v, "kind")),
                "raw_payload" => match serde_json::from_str::<String>(value.get()) {
                    Ok(text) => set_once(&mut slots.raw_payload, text, "raw_payload"),
                    Err(_) => Err(EnvelopeError::NotJson(
                        "raw_payload is not a string".to_owned(),
                    )),
                },
                other => Err(EnvelopeError::UnknownField(other.to_owned())),
            };
            if let Err(error) = outcome {
                rejected = Some(error);
            }
        }

        Ok(match rejected {
            Some(error) => Err(error),
            None => slots.finish(),
        })
    }
}

/// Parses an unsigned integer straight from its ASCII lexeme.
///
/// Kept away from serde deliberately: `-1`, `1.0` and `1e2` must be typed field
/// errors rather than silent coercions, and a counter that arrived as a float is a
/// bug in the splice worth hearing about immediately.
fn parse_uint(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut value: u64 = 0;
    for &byte in bytes {
        value = value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
    }
    Some(value)
}

fn uint(lexeme: &[u8], field: &'static str) -> Result<u64, EnvelopeError> {
    parse_uint(lexeme).ok_or(EnvelopeError::InvalidInteger { field })
}

fn plain_string<'a>(lexeme: &'a [u8], field: &'static str) -> Result<&'a str, EnvelopeError> {
    if lexeme.len() < 2
        || lexeme.first() != Some(&b'"')
        || lexeme.last() != Some(&b'"')
        || lexeme[1..lexeme.len() - 1].contains(&b'\\')
    {
        return Err(EnvelopeError::UnsupportedIdentifier { field });
    }
    std::str::from_utf8(&lexeme[1..lexeme.len() - 1]).map_err(|_| EnvelopeError::InvalidUtf8)
}

fn identifier<'a>(lexeme: &'a [u8], field: &'static str) -> Result<&'a str, EnvelopeError> {
    let text = plain_string(lexeme, field)?;
    if text.is_empty() || !text.is_ascii() || text.bytes().any(|b| b.is_ascii_control()) {
        return Err(EnvelopeError::UnsupportedIdentifier { field });
    }
    Ok(text)
}

fn set_once<T>(slot: &mut Option<T>, value: T, field: &'static str) -> Result<(), EnvelopeError> {
    if slot.replace(value).is_some() {
        Err(EnvelopeError::DuplicateField(field))
    } else {
        Ok(())
    }
}

fn parse_cursor(value: &RawValue) -> Result<SourceCursor, EnvelopeError> {
    #[derive(serde::Deserialize)]
    struct Wire {
        #[serde(rename = "type")]
        kind: String,
        counter: Option<u64>,
        last_update_id: Option<u64>,
        source_time_ms: Option<u64>,
        first: Option<u64>,
        last: Option<u64>,
        previous_last: Option<u64>,
    }

    let wire: Wire =
        serde_json::from_str(value.get()).map_err(|_| EnvelopeError::InvalidSourceCursor)?;
    match (
        wire.kind.as_str(),
        wire.counter,
        wire.last_update_id,
        wire.source_time_ms,
        wire.first,
        wire.last,
        wire.previous_last,
    ) {
        ("unsequenced", Some(counter), None, None, None, None, None) => {
            Ok(SourceCursor::Unsequenced { counter })
        }
        ("snapshot", None, Some(last_update_id), None, None, None, None) => {
            Ok(SourceCursor::SnapshotId { last_update_id })
        }
        ("snapshot", None, None, Some(source_time_ms), None, None, None) => {
            Ok(SourceCursor::SnapshotTime { source_time_ms })
        }
        ("update_range", None, None, None, Some(first), Some(last), Some(previous_last)) => {
            Ok(SourceCursor::UpdateRange {
                first,
                last,
                previous_last,
            })
        }
        _ => Err(EnvelopeError::InvalidSourceCursor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real line, copied verbatim from a Polymarket capture run.
    const POLYMARKET: &[u8] = br#"{"delivery_index":3,"record_id":"pm-427f40aa-3","visible_ns":1785267959274886000,"venue":"polymarket","stream":"public_book","connection_epoch":"427f40aa","local_counter":3,"source_cursor":{"type":"unsequenced","counter":3},"kind":"venue_frame","raw_payload":"[{\"event_type\":\"book\"}]"}"#;

    #[test]
    fn parses_a_real_polymarket_line() {
        let view = EnvelopeView::parse(POLYMARKET).expect("parses");
        assert_eq!(view.envelope_version, 1);
        assert_eq!(view.delivery_index, 3);
        assert_eq!(view.monotonic_ns, None);
        assert_eq!(view.venue, Venue::Polymarket);
        assert_eq!(view.kind, RecordKind::VenueFrame);
        assert_eq!(
            view.source_cursor,
            Some(SourceCursor::Unsequenced { counter: 3 })
        );
        // Unescaped by serde: the payload is the venue's bytes, not ours.
        assert_eq!(view.raw_payload, r#"[{"event_type":"book"}]"#);
    }

    #[test]
    fn parses_a_v2_line_with_monotonic_time() {
        let line = br#"{"envelope_version":2,"delivery_index":3,"record_id":"pm-427f40aa-3","visible_ns":1785267959274886000,"monotonic_ns":918273645,"venue":"polymarket","stream":"public_book","connection_epoch":"427f40aa","local_counter":3,"source_cursor":{"type":"unsequenced","counter":3},"kind":"venue_frame","raw_payload":"[]"}"#;
        let view = EnvelopeView::parse(line).expect("parses");
        assert_eq!(view.envelope_version, 2);
        assert_eq!(view.monotonic_ns, Some(LogicalTime::from_ns(918_273_645)));
    }

    #[test]
    fn rejects_mixed_or_unknown_version_shapes() {
        let missing_monotonic = br#"{"envelope_version":2,"delivery_index":1,"record_id":"pm-a-1","visible_ns":1,"venue":"polymarket","stream":"process","connection_epoch":"a","local_counter":1,"source_cursor":null,"kind":"control","raw_payload":"{}"}"#;
        assert_eq!(
            EnvelopeView::parse(missing_monotonic),
            Err(EnvelopeError::InvalidEnvelopeVersionShape)
        );

        let explicit_v1 = br#"{"envelope_version":1,"delivery_index":1,"record_id":"pm-a-1","visible_ns":1,"monotonic_ns":1,"venue":"polymarket","stream":"process","connection_epoch":"a","local_counter":1,"source_cursor":null,"kind":"control","raw_payload":"{}"}"#;
        assert_eq!(
            EnvelopeView::parse(explicit_v1),
            Err(EnvelopeError::UnsupportedEnvelopeVersion(1))
        );
    }

    #[test]
    fn parses_a_limitless_version_cursor() {
        let line = br#"{"delivery_index":5,"record_id":"lm-abc-5","visible_ns":1,"venue":"limitless","stream":"public_book","connection_epoch":"abc","local_counter":5,"source_cursor":{"type":"snapshot","last_update_id":958621772},"kind":"venue_frame","raw_payload":"{}"}"#;
        let view = EnvelopeView::parse(line).expect("parses");
        assert_eq!(view.venue, Venue::Limitless);
        assert_eq!(
            view.source_cursor,
            Some(SourceCursor::SnapshotId {
                last_update_id: 958_621_772
            })
        );
    }

    #[test]
    fn null_cursor_is_legal_but_an_absent_one_is_not() {
        let with_null = br#"{"delivery_index":1,"record_id":"pm-a-1","visible_ns":1,"venue":"polymarket","stream":"process","connection_epoch":"a","local_counter":1,"source_cursor":null,"kind":"control","raw_payload":"{}"}"#;
        assert_eq!(EnvelopeView::parse(with_null).unwrap().source_cursor, None);

        let absent = br#"{"delivery_index":1,"record_id":"pm-a-1","visible_ns":1,"venue":"polymarket","stream":"process","connection_epoch":"a","local_counter":1,"kind":"control","raw_payload":"{}"}"#;
        assert_eq!(
            EnvelopeView::parse(absent),
            Err(EnvelopeError::MissingField("source_cursor"))
        );
    }

    #[test]
    fn an_unknown_field_rejects_the_whole_line() {
        let line = br#"{"delivery_index":1,"record_id":"pm-a-1","visible_ns":1,"venue":"polymarket","stream":"process","connection_epoch":"a","local_counter":1,"source_cursor":null,"kind":"control","raw_payload":"{}","extra":1}"#;
        assert_eq!(
            EnvelopeView::parse(line),
            Err(EnvelopeError::UnknownField("extra".to_owned()))
        );
    }

    #[test]
    fn counters_must_be_plain_digits() {
        for bad in [r#""1""#, "1.0", "1e2", "-1", "true"] {
            let line = format!(
                r#"{{"delivery_index":{bad},"record_id":"pm-a-1","visible_ns":1,"venue":"polymarket","stream":"process","connection_epoch":"a","local_counter":1,"source_cursor":null,"kind":"control","raw_payload":"{{}}"}}"#
            );
            assert!(
                matches!(
                    EnvelopeView::parse(line.as_bytes()),
                    Err(EnvelopeError::InvalidInteger {
                        field: "delivery_index"
                    })
                ),
                "accepted {bad}"
            );
        }
    }

    #[test]
    fn escaped_identifiers_are_refused() {
        // `\u002D` is a perfectly valid JSON spelling of '-', and serde would
        // decode it to the same string. It is refused anyway: the parser borrows
        // the bytes between the quotes without unescaping, so accepting this
        // would mean the stored identifier is not what the borrow says it is.
        let line = br#"{"delivery_index":1,"record_id":"pm\u002Da\u002D1","visible_ns":1,"venue":"polymarket","stream":"process","connection_epoch":"a","local_counter":1,"source_cursor":null,"kind":"control","raw_payload":"{}"}"#;
        assert_eq!(
            EnvelopeView::parse(line),
            Err(EnvelopeError::UnsupportedIdentifier { field: "record_id" })
        );
    }

    #[test]
    fn non_ascii_identifiers_are_refused() {
        let line = "{\"delivery_index\":1,\"record_id\":\"pm-é-1\",\"visible_ns\":1,\"venue\":\"polymarket\",\"stream\":\"process\",\"connection_epoch\":\"a\",\"local_counter\":1,\"source_cursor\":null,\"kind\":\"control\",\"raw_payload\":\"{}\"}";
        assert_eq!(
            EnvelopeView::parse(line.as_bytes()),
            Err(EnvelopeError::UnsupportedIdentifier { field: "record_id" })
        );
    }

    #[test]
    fn an_unknown_venue_names_the_value() {
        let line = br#"{"delivery_index":1,"record_id":"a-1","visible_ns":1,"venue":"binance","stream":"process","connection_epoch":"a","local_counter":1,"source_cursor":null,"kind":"control","raw_payload":"{}"}"#;
        assert_eq!(
            EnvelopeView::parse(line),
            Err(EnvelopeError::UnknownVenue("binance".to_owned()))
        );
    }

    #[test]
    fn trailing_newline_is_framing_not_content() {
        let mut line = POLYMARKET.to_vec();
        line.push(b'\n');
        assert_eq!(EnvelopeView::parse(&line).unwrap().delivery_index, 3);
    }
}
