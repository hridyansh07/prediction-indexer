//! Durable positions.
//!
//! A sequence is a positive integer with a name. It carries no capability and
//! proves nothing on its own — the store still validates every position inside
//! the transaction that assigns it — but the two stay distinct types so an
//! evidence position cannot be passed where a fact position is expected. That
//! would be a silent error rather than a loud one, and the two are dense in
//! different ways.

macro_rules! sequence {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(i64);

        impl $name {
            /// Positions start at one, so zero or negative is refused rather than
            /// stored. A non-positive value in the database means it was edited
            /// outside this program.
            pub const fn new(value: i64) -> Option<Self> {
                if value > 0 { Some(Self(value)) } else { None }
            }

            pub const fn get(self) -> i64 {
                self.0
            }

            pub const fn next(self) -> Self {
                Self(self.0 + 1)
            }

            pub const fn first() -> Self {
                Self(1)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}", self.0)
            }
        }
    };
}

sequence!(
    EvidenceSeq,
    "Global position of one captured line across every lane, in the order this \
     process read it — `file_order`. A total, reproducible order over the tape, \
     and **not event order**: whole files are consumed atomically, so a record \
     received between two records of another lane sequences after both. Never \
     read it as time; sort on `visible_ns` for that. \
     `docs/SEALED_CAPTURE_PIPELINE_V1.md` §5 defines the merged order that \
     replaces it."
);

sequence!(
    CanonicalSeq,
    "Global position of one captured line in the **merged** order — the \
     `EvidenceSeq` of `docs/SEALED_CAPTURE_PIPELINE_V1.md` §5, assigned by the \
     finalizer from `(visible_ns, lane_rank, delivery_index)` and equal to the \
     line ordinal of the canonical evidence file. \
     \
     A separate type from `EvidenceSeq` on purpose. That name is already taken \
     by this repository's `file_order`, and the two are different global orders \
     over the same bytes; sharing an identifier would let one be passed where \
     the other is meant and the mistake would be silent. `LaneAuthority` carries \
     the same warning about `LaneId`. \
     \
     It is capture observation order at one host, **not venue event order**. Two \
     venues may act on the same world event milliseconds apart and be recorded \
     in the opposite order by routing and stamping alone."
);

sequence!(
    FactSeq,
    "Position of one committed classification. Always equal to its evidence \
     sequence: exactly one fact is committed per delivery, so the two counters \
     move together and a divergence means a bug rather than a design."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_start_at_one() {
        assert!(EvidenceSeq::new(0).is_none());
        assert!(EvidenceSeq::new(-1).is_none());
        assert_eq!(EvidenceSeq::new(1).unwrap().get(), 1);
        assert_eq!(EvidenceSeq::first().get(), 1);
    }

    #[test]
    fn next_advances_by_one() {
        assert_eq!(EvidenceSeq::first().next().get(), 2);
    }
}
