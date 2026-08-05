//! The wire vocabulary: who sent a record, on what stream, and what it is.

use core::fmt;

use crate::error::DecodeError;

macro_rules! wire_enum {
    ($name:ident, $field:literal, { $($variant:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name { $($variant),+ }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire),+ }
            }

            pub fn from_wire(input: &str) -> Option<Self> {
                match input { $($wire => Some(Self::$variant),)+ _ => None }
            }

            pub fn decode(input: &str) -> Result<Self, DecodeError> {
                Self::from_wire(input).ok_or(DecodeError::InvalidField { field: $field })
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

wire_enum!(Venue, "venue", {
    Polymarket => "polymarket",
    Kalshi     => "kalshi",
    Limitless  => "limitless",
    Internal   => "internal",
});

wire_enum!(Stream, "stream", {
    PublicBook     => "public_book",
    PublicSnapshot => "public_snapshot",
    PublicTrade    => "public_trade",
    PublicQuote    => "public_quote",
    ReferenceEvent => "reference_event",
    Process        => "process",
});

wire_enum!(RecordKind, "kind", {
    VenueFrame => "venue_frame",
    Control    => "control",
    Fault      => "fault",
});

/// What the venue said about its own continuity — never an ordering.
///
/// Replay walks our `EvidenceSeq`. This is evidence *about* a venue, and how much
/// it is worth differs sharply between them:
///
/// * `Unsequenced` — the venue numbers nothing. Polymarket. Our counter is the
///   only order that exists, and a dropped frame leaves no trace.
/// * `SnapshotId` — a snapshot carrying an id. **Density is not implied.**
///   Limitless's `version` is monotonic per market but sparse, with ranges
///   overlapping between markets, so it dates and orders a book without ever
///   revealing a missing one.
/// * `SnapshotTime` — a snapshot carrying only a time. Staleness, nothing more.
/// * `UpdateRange` — a real delta stream where every id is accounted for. The only
///   variant from which a gap can be *proved*. Kalshi is expected to land here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SourceCursor {
    Unsequenced {
        counter: u64,
    },
    SnapshotId {
        last_update_id: u64,
    },
    SnapshotTime {
        source_time_ms: u64,
    },
    UpdateRange {
        first: u64,
        last: u64,
        previous_last: u64,
    },
}

impl SourceCursor {
    /// Whether a hole in this cursor's numbering proves a lost message.
    ///
    /// Only `UpdateRange` says yes. Treating `SnapshotId` as dense — which is what
    /// a Binance-shaped classifier would do — reports a gap on nearly every
    /// Limitless message, because that venue's ids jump by thousands between
    /// consecutive updates to the same book.
    pub const fn proves_gaps(self) -> bool {
        matches!(self, Self::UpdateRange { .. })
    }

    /// The value a later cursor must not fall below, where the venue offers one.
    pub const fn monotonic_key(self) -> Option<u64> {
        match self {
            Self::SnapshotId { last_update_id } => Some(last_update_id),
            Self::SnapshotTime { source_time_ms } => Some(source_time_ms),
            Self::UpdateRange { last, .. } => Some(last),
            Self::Unsequenced { .. } => None,
        }
    }
}

/// A borrowed record identifier, read straight out of the raw line.
///
/// The parser takes the bytes between the quotes without unescaping them, so the
/// splice is required to emit plain ASCII with no backslash, quote, or control
/// character. Anything else would make the identifier something other than what
/// it appears to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordId<'a>(&'a str);

impl<'a> RecordId<'a> {
    pub const fn new(value: &'a str) -> Self {
        Self(value)
    }
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

impl fmt::Display for RecordId<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// One connection's identity. A reconnect always mints a new one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EpochId<'a>(&'a str);

impl<'a> EpochId<'a> {
    pub const fn new(value: &'a str) -> Self {
        Self(value)
    }
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

impl fmt::Display for EpochId<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// The splice's receive clock, in nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogicalTime(u64);

impl LogicalTime {
    pub const fn from_ns(value: u64) -> Self {
        Self(value)
    }
    pub const fn ns(self) -> u64 {
        self.0
    }
}

impl fmt::Display for LogicalTime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_spellings_round_trip() {
        for venue in [
            Venue::Polymarket,
            Venue::Kalshi,
            Venue::Limitless,
            Venue::Internal,
        ] {
            assert_eq!(Venue::from_wire(venue.as_str()), Some(venue));
        }
        assert_eq!(Venue::from_wire("binance"), None);
    }

    #[test]
    fn only_update_range_proves_a_gap() {
        assert!(
            SourceCursor::UpdateRange {
                first: 1,
                last: 2,
                previous_last: 0
            }
            .proves_gaps()
        );
        // Limitless: monotonic but sparse. A hole here is not evidence of loss.
        assert!(
            !SourceCursor::SnapshotId {
                last_update_id: 958_621_772
            }
            .proves_gaps()
        );
        assert!(!SourceCursor::SnapshotTime { source_time_ms: 1 }.proves_gaps());
        assert!(!SourceCursor::Unsequenced { counter: 1 }.proves_gaps());
    }

    #[test]
    fn unsequenced_offers_no_monotonic_key() {
        assert_eq!(
            SourceCursor::Unsequenced { counter: 7 }.monotonic_key(),
            None
        );
        assert_eq!(
            SourceCursor::SnapshotId { last_update_id: 7 }.monotonic_key(),
            Some(7)
        );
    }
}
