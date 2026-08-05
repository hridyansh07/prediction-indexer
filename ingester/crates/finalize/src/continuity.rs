//! Classifying merged records, and the state that has to survive a window.
//!
//! ## Two receipts, one rule
//!
//! `indexer_continuity` gates classification on a [`Positioned`] receipt and
//! state movement on a [`Committed`] one — the rule that state
//! moves only after commit. The store issues those receipts from a SQLite
//! transaction; here the canonical file is the sink, so the receipts are issued
//! by the writer once a line's bytes are down and its position assigned. Same
//! rule, same order:
//!
//! ```text
//! write evidence line   ->  WrittenLine       bytes down, position decided
//! classify(view, &written)                    takes &self, moves nothing
//! write provenance line with the verdict
//! apply(&ClassifiedLine)                      retained state moves only now
//! ```
//!
//! ## What survives a window, and what does not
//!
//! **Ordering state persists; identity does not.**
//!
//! Ordering has to persist or the verdicts would be wrong *and* non-deterministic:
//! reset the per-connection counter and per-epoch cursor each run, and the first
//! Kalshi record of a resumed window reads `bootstrap` where it should read
//! `continuous` — so provenance bytes would depend on where a run happened to
//! stop, which breaks §8's retry-after-crash guarantee. Seeded from the
//! watermark, the state is a function of committed history rather than of run
//! boundaries.
//!
//! Identity — the record-id map behind duplicate and conflict detection — starts
//! empty for every window, and that is a real limit stated plainly. Carrying it
//! would mean holding every record id in the retention window, which at the
//! measured 70M records/day over 5–7 days is hundreds of millions of entries;
//! walking it back on demand would need a queryable store of ids that does not
//! exist. Window-scoped detection still catches what duplicates actually come
//! from — reconnect retransmission, which resolves in seconds inside a
//! thirty-minute window — and `indexer-ingest`'s fact log keeps the global view.
//!
//! `EpochState::bootstrapped` is not carried: `classify_cause` never consults it.

use std::collections::{BTreeMap, BTreeSet};

use indexer_continuity::{ClassifierState, ContinuityFact, EpochKey, EpochState, LaneId};
use indexer_types::{CanonicalSeq, Committed, FactSeq, Positioned, Stream, Venue};
use serde::{Deserialize, Serialize};

/// Proof that one record's line is in the open canonical file at a known position.
///
/// Holds a [`CanonicalSeq`], which is the finalizer's own global order and a
/// different thing from the store's `EvidenceSeq`. The conversion to `FactSeq`
/// happens here and nowhere else: a fact's `seq` means "the position in whichever
/// sink issued this receipt", so the narrowing is a deliberate boundary rather
/// than the two orders quietly sharing a type.
pub struct WrittenLine {
    position: CanonicalSeq,
}

impl WrittenLine {
    pub fn new(position: CanonicalSeq) -> Self {
        Self { position }
    }

    pub fn canonical_seq(&self) -> CanonicalSeq {
        self.position
    }
}

impl Positioned for WrittenLine {
    fn position(&self) -> FactSeq {
        FactSeq::new(self.position.get()).expect("CanonicalSeq is positive by construction")
    }
}

/// Proof that a record's verdict is in the open provenance file beside it.
pub struct ClassifiedLine {
    position: FactSeq,
    fact: ContinuityFact,
}

impl ClassifiedLine {
    pub fn new(position: FactSeq, fact: ContinuityFact) -> Self {
        Self { position, fact }
    }
}

impl Committed<ContinuityFact> for ClassifiedLine {
    fn position(&self) -> FactSeq {
        self.position
    }
    fn value(&self) -> &ContinuityFact {
        &self.fact
    }
}

/// One connection's last delivered counter.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConnectionCarry {
    /// The capture lane, so an entry can be superseded when *that* lane
    /// reconnects rather than when any lane of the same venue does.
    pub lane: String,
    pub venue: String,
    pub epoch: String,
    pub local_counter: u64,
}

/// One stream-epoch's last venue-supplied ordering key.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EpochCarry {
    pub lane: String,
    pub venue: String,
    pub stream: String,
    pub epoch: String,
    pub monotonic_key: u64,
}

/// The bounded ordering state that crosses a window boundary.
///
/// Sorted vectors rather than maps so the JSON is deterministic — the watermark
/// is compared byte-for-byte after a rebuild.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct OrderingState {
    pub connections: Vec<ConnectionCarry>,
    pub epochs: Vec<EpochCarry>,
}

impl OrderingState {
    /// A classifier state carrying this ordering history and no identity history.
    pub fn seed(&self) -> ClassifierState {
        let mut state = ClassifierState::default();
        for entry in &self.connections {
            let Some(venue) = Venue::from_wire(&entry.venue) else {
                continue;
            };
            state
                .connections
                .insert((venue, entry.epoch.clone()), entry.local_counter);
        }
        for entry in &self.epochs {
            let (Some(venue), Some(stream)) = (
                Venue::from_wire(&entry.venue),
                Stream::from_wire(&entry.stream),
            ) else {
                continue;
            };
            let key = EpochKey {
                lane: LaneId { venue, stream },
                epoch: entry.epoch.clone(),
            };
            state.epochs.insert(
                key,
                EpochState {
                    monotonic_key: Some(entry.monotonic_key),
                    ..EpochState::default()
                },
            );
        }
        state
    }

    /// The ordering history to hand to the next window.
    ///
    /// **A lane's entry survives its own silence.** An epoch is retired when that
    /// lane produces a *different* one, not when it produces nothing: a quiet
    /// lane emits an empty sealed segment by design (§3) and its connection is
    /// still open, so dropping it would make the window after the silence read
    /// `bootstrap` where it should read `continuous` — and would hide a
    /// `local_counter` break straddling the quiet window.
    ///
    /// That keeps it bounded all the same. One connection is live per lane at a
    /// time, so the carry holds one entry per lane plus one per lane-stream, and
    /// a reconnect replaces rather than appends.
    pub fn capture(
        previous: &Self,
        state: &ClassifierState,
        seen: &BTreeMap<String, BTreeSet<String>>,
    ) -> Self {
        let spoke = |lane: &str| seen.contains_key(lane);
        let live = |lane: &str, epoch: &String| {
            seen.get(lane).is_some_and(|epochs| epochs.contains(epoch))
        };

        let mut connections: Vec<ConnectionCarry> = previous
            .connections
            .iter()
            .filter(|entry| !spoke(&entry.lane))
            .cloned()
            .collect();
        for (lane, epochs) in seen {
            for epoch in epochs {
                for ((venue, held), counter) in &state.connections {
                    if held == epoch && live(lane, held) {
                        connections.push(ConnectionCarry {
                            lane: lane.clone(),
                            venue: venue.as_str().to_owned(),
                            epoch: held.clone(),
                            local_counter: *counter,
                        });
                    }
                }
            }
        }
        connections.sort_by(|left, right| {
            (&left.lane, &left.venue, &left.epoch).cmp(&(&right.lane, &right.venue, &right.epoch))
        });
        connections.dedup_by(|left, right| {
            (&left.lane, &left.venue, &left.epoch) == (&right.lane, &right.venue, &right.epoch)
        });

        let mut epochs: Vec<EpochCarry> = previous
            .epochs
            .iter()
            .filter(|entry| !spoke(&entry.lane))
            .cloned()
            .collect();
        for lane in seen.keys() {
            for (key, epoch) in &state.epochs {
                let Some(monotonic_key) = epoch.monotonic_key else {
                    continue;
                };
                if !live(lane, &key.epoch) {
                    continue;
                }
                epochs.push(EpochCarry {
                    lane: lane.clone(),
                    venue: key.lane.venue.as_str().to_owned(),
                    stream: key.lane.stream.as_str().to_owned(),
                    epoch: key.epoch.clone(),
                    monotonic_key,
                });
            }
        }
        epochs.sort_by(|left, right| {
            (&left.lane, &left.venue, &left.stream, &left.epoch).cmp(&(
                &right.lane,
                &right.venue,
                &right.stream,
                &right.epoch,
            ))
        });
        epochs.dedup_by(|left, right| {
            (&left.lane, &left.venue, &left.stream, &left.epoch)
                == (&right.lane, &right.venue, &right.stream, &right.epoch)
        });

        Self {
            connections,
            epochs,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.connections.is_empty() && self.epochs.is_empty()
    }
}

/// Which epochs each lane produced in this window.
///
/// Keyed by *capture lane* rather than venue: Polymarket runs four lanes, each
/// with its own connection, so one lane reconnecting must not retire the other
/// three's counters.
#[derive(Default)]
pub struct SeenEpochs(BTreeMap<String, BTreeSet<String>>);

impl SeenEpochs {
    pub fn observe(&mut self, lane: &str, fact: &ContinuityFact) {
        self.0
            .entry(lane.to_owned())
            .or_default()
            .insert(fact.key.epoch.clone());
    }

    pub fn into_map(self) -> BTreeMap<String, BTreeSet<String>> {
        self.0
    }
}

/// Per-lane first and last receive time, for the cross-window boundary check.
#[derive(Default)]
pub struct LaneClocks(BTreeMap<String, (u64, u64)>);

impl LaneClocks {
    pub fn observe(&mut self, lane: &str, visible_ns: u64) {
        self.0
            .entry(lane.to_owned())
            .and_modify(|(_, last)| *last = visible_ns)
            .or_insert((visible_ns, visible_ns));
    }

    pub fn first(&self, lane: &str) -> Option<u64> {
        self.0.get(lane).map(|(first, _)| *first)
    }

    /// Each lane's last receive time, for the next window to check against.
    pub fn last_by_lane(&self) -> BTreeMap<String, u64> {
        self.0
            .iter()
            .map(|(lane, (_, last))| (lane.clone(), *last))
            .collect()
    }
}
