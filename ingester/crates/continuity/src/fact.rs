//! The committed record of what was decided about one delivery.
//!
//! A fact is durable and replayable: it carries the transition, so state can be
//! rebuilt by folding facts alone without re-reading a single raw line. Its
//! canonical encoding is written by hand rather than derived, because these bytes
//! are hashed and stored — a serde attribute changing field order in a future
//! version would silently invalidate every hash already on disk.

use std::{fmt, io};

use indexer_types::{
    ContentHash, EnvelopeView, FactSeq, RecordKind, SinkError, Sinkable, Stream, Venue,
};

use crate::state::{EpochKey, IdentityVerdict};

/// What one delivery meant for its stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContinuityCause {
    /// Our own narration — `connection_opened`, a fault, a subscription change.
    /// Carries no cursor and is never measured for gaps.
    Lifecycle,
    /// First frame on this epoch. Nothing to compare against yet.
    Bootstrap,
    /// The venue publishes no ordering at all. Polymarket.
    UnsequencedVenue,
    /// Venue key moved forward, but density was never promised so no gap is
    /// implied. Limitless.
    SparseMonotonic,
    /// A true delta stream with every id accounted for.
    Continuous {
        first: u64,
        last: u64,
    },
    /// A hole in a stream that promised density. The only cause that *proves* loss.
    GapProven {
        previous: u64,
        first: u64,
    },
    /// An older state arrived after a newer one.
    CursorWentBackwards {
        previous: u64,
        observed: u64,
    },
    /// Our own dense per-connection counter skipped. A splice bug or a torn spool,
    /// never a venue behaviour.
    LocalCounterBroken {
        expected: u64,
        observed: u64,
    },
    Duplicate {
        original: FactSeq,
    },
    Conflict {
        original: FactSeq,
    },
}

impl ContinuityCause {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Lifecycle => "lifecycle",
            Self::Bootstrap => "bootstrap",
            Self::UnsequencedVenue => "unsequenced_venue",
            Self::SparseMonotonic => "sparse_monotonic",
            Self::Continuous { .. } => "continuous",
            Self::GapProven { .. } => "gap_proven",
            Self::CursorWentBackwards { .. } => "cursor_went_backwards",
            Self::LocalCounterBroken { .. } => "local_counter_broken",
            Self::Duplicate { .. } => "duplicate",
            Self::Conflict { .. } => "conflict",
        }
    }

    /// Whether this cause means the book on this epoch can no longer be trusted.
    pub fn stales_the_epoch(&self) -> bool {
        matches!(
            self,
            Self::GapProven { .. }
                | Self::CursorWentBackwards { .. }
                | Self::LocalCounterBroken { .. }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinuityFact {
    pub seq: FactSeq,
    pub record_id: String,
    pub key: EpochKey,
    pub kind: RecordKind,
    pub local_counter: u64,
    pub delivery_index: u64,
    pub content: ContentHash,
    pub monotonic_key: Option<u64>,
    pub cause: ContinuityCause,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FactDecodeError(String);

impl FactDecodeError {
    fn invalid(detail: impl Into<String>) -> Self {
        Self(detail.into())
    }
}

impl fmt::Display for FactDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for FactDecodeError {}

impl ContinuityFact {
    pub(crate) fn accepted(
        seq: FactSeq,
        envelope: &EnvelopeView<'_>,
        key: EpochKey,
        content: ContentHash,
        cause: ContinuityCause,
    ) -> Self {
        Self {
            seq,
            record_id: envelope.record_id.as_str().to_owned(),
            key,
            kind: envelope.kind,
            local_counter: envelope.local_counter,
            delivery_index: envelope.delivery_index,
            content,
            monotonic_key: envelope
                .source_cursor
                .and_then(|cursor| cursor.monotonic_key()),
            cause,
        }
    }

    pub(crate) fn duplicate(
        seq: FactSeq,
        envelope: &EnvelopeView<'_>,
        key: EpochKey,
        content: ContentHash,
        verdict: IdentityVerdict,
    ) -> Self {
        let cause = match verdict {
            IdentityVerdict::Duplicate { original } => ContinuityCause::Duplicate { original },
            IdentityVerdict::Conflict { original, .. } => ContinuityCause::Conflict { original },
            IdentityVerdict::Unseen => unreachable!("only called for a seen record"),
        };
        Self::accepted(seq, envelope, key, content, cause)
    }

    /// Decodes one previously committed canonical fact for state recovery.
    ///
    /// Recovery folds the fact log; it does not reinterpret raw venue payloads.
    /// The store verifies the canonical bytes against `fact_hash` before calling
    /// this function.
    pub fn from_canonical_bytes(
        expected_seq: FactSeq,
        bytes: &[u8],
    ) -> Result<Self, FactDecodeError> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            v: u64,
            seq: i64,
            record_id: String,
            venue: String,
            stream: String,
            epoch: String,
            kind: String,
            local_counter: u64,
            delivery_index: u64,
            content: String,
            monotonic_key: Option<u64>,
            cause: String,
            first: Option<u64>,
            last: Option<u64>,
            previous: Option<u64>,
            observed: Option<u64>,
            expected: Option<u64>,
            original: Option<i64>,
        }

        let wire: Wire = serde_json::from_slice(bytes).map_err(|error| {
            FactDecodeError::invalid(format!("invalid canonical JSON: {error}"))
        })?;
        if wire.v != 1 {
            return Err(FactDecodeError::invalid(format!(
                "unsupported continuity fact version {}",
                wire.v
            )));
        }
        if wire.seq != expected_seq.get() {
            return Err(FactDecodeError::invalid(format!(
                "canonical seq {} does not match row seq {}",
                wire.seq, expected_seq
            )));
        }

        let no_detail = (
            wire.first,
            wire.last,
            wire.previous,
            wire.observed,
            wire.expected,
            wire.original,
        );
        let cause = match (wire.cause.as_str(), no_detail) {
            ("lifecycle", (None, None, None, None, None, None)) => ContinuityCause::Lifecycle,
            ("bootstrap", (None, None, None, None, None, None)) => ContinuityCause::Bootstrap,
            ("unsequenced_venue", (None, None, None, None, None, None)) => {
                ContinuityCause::UnsequencedVenue
            }
            ("sparse_monotonic", (None, None, None, None, None, None)) => {
                ContinuityCause::SparseMonotonic
            }
            ("continuous", (Some(first), Some(last), None, None, None, None)) => {
                ContinuityCause::Continuous { first, last }
            }
            ("gap_proven", (Some(first), None, Some(previous), None, None, None)) => {
                ContinuityCause::GapProven { previous, first }
            }
            ("cursor_went_backwards", (None, None, Some(previous), Some(observed), None, None)) => {
                ContinuityCause::CursorWentBackwards { previous, observed }
            }
            ("local_counter_broken", (None, None, None, Some(observed), Some(expected), None)) => {
                ContinuityCause::LocalCounterBroken { expected, observed }
            }
            ("duplicate", (None, None, None, None, None, Some(original))) => {
                ContinuityCause::Duplicate {
                    original: FactSeq::new(original)
                        .ok_or_else(|| FactDecodeError::invalid("invalid duplicate origin"))?,
                }
            }
            ("conflict", (None, None, None, None, None, Some(original))) => {
                ContinuityCause::Conflict {
                    original: FactSeq::new(original)
                        .ok_or_else(|| FactDecodeError::invalid("invalid conflict origin"))?,
                }
            }
            _ => {
                return Err(FactDecodeError::invalid(format!(
                    "cause {:?} has an invalid detail shape",
                    wire.cause
                )));
            }
        };

        let venue = Venue::from_wire(&wire.venue)
            .ok_or_else(|| FactDecodeError::invalid(format!("unknown venue {:?}", wire.venue)))?;
        let stream = Stream::from_wire(&wire.stream)
            .ok_or_else(|| FactDecodeError::invalid(format!("unknown stream {:?}", wire.stream)))?;
        let kind = RecordKind::from_wire(&wire.kind)
            .ok_or_else(|| FactDecodeError::invalid(format!("unknown kind {:?}", wire.kind)))?;
        let content = ContentHash::from_hex(&wire.content)
            .ok_or_else(|| FactDecodeError::invalid("invalid content hash"))?;

        Ok(Self {
            seq: expected_seq,
            record_id: wire.record_id,
            key: EpochKey::new(venue, stream, wire.epoch),
            kind,
            local_counter: wire.local_counter,
            delivery_index: wire.delivery_index,
            content,
            monotonic_key: wire.monotonic_key,
            cause,
        })
    }
}

impl Sinkable for ContinuityFact {
    /// Canonical bytes, written by hand and versioned.
    ///
    /// One line, fixed field order, no whitespace. Hand-written because these
    /// bytes are hashed and persisted: a derived serialiser would let a future
    /// refactor reorder a field and invalidate every hash already on disk without
    /// anything failing to compile.
    fn write_to(&self, writer: &mut dyn io::Write) -> Result<(), SinkError> {
        let detail = match &self.cause {
            ContinuityCause::Continuous { first, last } => {
                format!(r#","first":{first},"last":{last}"#)
            }
            ContinuityCause::GapProven { previous, first } => {
                format!(r#","previous":{previous},"first":{first}"#)
            }
            ContinuityCause::CursorWentBackwards { previous, observed } => {
                format!(r#","previous":{previous},"observed":{observed}"#)
            }
            ContinuityCause::LocalCounterBroken { expected, observed } => {
                format!(r#","expected":{expected},"observed":{observed}"#)
            }
            ContinuityCause::Duplicate { original } | ContinuityCause::Conflict { original } => {
                format!(r#","original":{original}"#)
            }
            _ => String::new(),
        };
        let monotonic = match self.monotonic_key {
            Some(value) => value.to_string(),
            None => "null".to_owned(),
        };
        write!(
            writer,
            concat!(
                r#"{{"v":1,"seq":{},"record_id":"{}","venue":"{}","stream":"{}","epoch":"{}","#,
                r#""kind":"{}","local_counter":{},"delivery_index":{},"content":"{}","#,
                r#""monotonic_key":{},"cause":"{}"{}}}"#
            ),
            self.seq,
            self.record_id,
            self.key.lane.venue,
            self.key.lane.stream,
            self.key.epoch,
            self.kind,
            self.local_counter,
            self.delivery_index,
            self.content.to_hex(),
            monotonic,
            self.cause.label(),
            detail,
        )
        .map_err(SinkError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_fact_round_trips_for_recovery() {
        let fact = ContinuityFact {
            seq: FactSeq::first(),
            record_id: "pm-epoch-3".to_owned(),
            key: EpochKey::new(Venue::Polymarket, Stream::PublicBook, "epoch".to_owned()),
            kind: RecordKind::VenueFrame,
            local_counter: 3,
            delivery_index: 2,
            content: ContentHash::hash(b"payload"),
            monotonic_key: None,
            cause: ContinuityCause::LocalCounterBroken {
                expected: 2,
                observed: 3,
            },
        };
        let bytes = fact.to_canonical_bytes().expect("canonical bytes");
        assert_eq!(
            ContinuityFact::from_canonical_bytes(FactSeq::first(), &bytes).expect("decode"),
            fact
        );
    }
}
