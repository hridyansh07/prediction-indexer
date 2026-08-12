//! Retained classifier state.
//!
//! Every map is a `BTreeMap`/`BTreeSet`. Iteration order becomes observable the
//! moment a report or a hash walks this state, and a `HashMap` would make that
//! order depend on a random seed — so ordered containers are a determinism
//! requirement here, not a style preference.

use std::collections::BTreeMap;

use indexer_types::{ContentHash, FactSeq, Stream, Venue};

use crate::fact::{ContinuityCause, ContinuityFact};

/// How a delivery relates to what has been seen before.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityVerdict {
    Unseen,
    /// Same id, same bytes: a retransmission. Expected after a reconnect.
    Duplicate {
        original: FactSeq,
    },
    /// Same id, *different* bytes. The venue is contradicting itself, which is
    /// worth surfacing loudly rather than silently keeping the newer copy.
    Conflict {
        original: FactSeq,
        original_content: ContentHash,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityEntry {
    pub content: ContentHash,
    pub first_seen: FactSeq,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IdentityState {
    pub records: BTreeMap<String, IdentityEntry>,
}

impl IdentityState {
    pub fn verdict(&self, record_id: &str, content: ContentHash) -> IdentityVerdict {
        match self.records.get(record_id) {
            None => IdentityVerdict::Unseen,
            Some(entry) if entry.content == content => IdentityVerdict::Duplicate {
                original: entry.first_seen,
            },
            Some(entry) => IdentityVerdict::Conflict {
                original: entry.first_seen,
                original_content: entry.content,
            },
        }
    }

    /// Records the first observation. A later delivery never overwrites it —
    /// otherwise a conflict would quietly become the new truth.
    pub fn remember(&mut self, record_id: &str, content: ContentHash, at: FactSeq) {
        self.records
            .entry(record_id.to_owned())
            .or_insert(IdentityEntry {
                content,
                first_seen: at,
            });
    }
}

/// One venue stream, independent of which connection currently owns it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LaneId {
    pub venue: Venue,
    pub stream: Stream,
}

/// One connection on one lane — the key continuity state is filed under.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EpochKey {
    pub lane: LaneId,
    pub epoch: String,
}

impl EpochKey {
    pub fn new(venue: Venue, stream: Stream, epoch: String) -> Self {
        Self {
            lane: LaneId { venue, stream },
            epoch,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochHealth {
    /// Connected, but no book has been proven yet.
    AwaitingBootstrap,
    Healthy,
    /// Something arrived that the book cannot be trusted through — a proven gap
    /// or a cursor that went backwards. Recoverable by a fresh snapshot.
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochState {
    pub health: EpochHealth,
    /// Greatest of *our* per-connection counters seen. Dense by construction.
    pub observed_counter: Option<u64>,
    /// Greatest venue-supplied ordering key, where the venue supplies one.
    pub monotonic_key: Option<u64>,
    pub frames: u64,
    pub proven_gaps: u64,
    pub backwards: u64,
    pub local_breaks: u64,
    /// Instruments that have had their own accepted snapshot inside this epoch.
    ///
    /// Tracked per instrument, not per connection, and that distinction is
    /// load-bearing: on a multi-instrument stream the first instrument's snapshot
    /// would otherwise mark the whole lane healthy, and a sibling's delta would
    /// fold onto a book carried over from the connection before it. That yields a
    /// corrupt book rather than an error. Polymarket subscriptions are multi-asset
    /// by construction, so this is not hypothetical.
    pub bootstrapped: std::collections::BTreeSet<String>,
}

impl Default for EpochState {
    fn default() -> Self {
        Self {
            health: EpochHealth::AwaitingBootstrap,
            observed_counter: None,
            monotonic_key: None,
            frames: 0,
            proven_gaps: 0,
            backwards: 0,
            local_breaks: 0,
            bootstrapped: std::collections::BTreeSet::new(),
        }
    }
}

/// Which epoch currently owns a lane, and when it took over.
///
/// Retained as an explicit transition rather than derived by ordering epoch
/// identifiers. Deriving authority by taking the lexicographically greatest epoch
/// id and treating the rest as retired would fence a live connection whenever a
/// lane reconnected from `epoch-z` to `epoch-a`. Authority moves in
/// `FactSeq` order and nowhere else — epoch ids are identities, not counters.
///
/// `FactSeq` currently tracks `file_order`, so a finalizer that reorders records
/// changes which epoch ends up authoritative — this is a last-writer-wins over
/// the observation sequence, not a derivation from anything intrinsic. Note also
/// that "lane" here means `(venue, stream)`, which is *not* the capture lane the
/// sealed-pipeline design ranks; the two names collide and the types must not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneAuthority {
    pub active_epoch: String,
    pub established_at: FactSeq,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClassifierState {
    pub identity: IdentityState,
    pub epochs: BTreeMap<EpochKey, EpochState>,
    pub lanes: BTreeMap<LaneId, LaneAuthority>,
    /// Last `local_counter` seen per **connection**, keyed by `(venue, epoch)`
    /// and deliberately not by stream.
    ///
    /// A splice mints one counter per connection and spends it across every
    /// stream that connection carries — a Polymarket epoch interleaves `process`
    /// lifecycle records with `public_book` frames out of the same sequence.
    /// Tracking it per lane instead makes every switch between the two look like
    /// a dropped record, which is exactly what the first ingest run reported:
    /// 3,306 phantom breaks out of 3,823 frames.
    pub connections: BTreeMap<(Venue, String), u64>,
    pub duplicates: u64,
    pub conflicts: u64,
}

impl ClassifierState {
    pub fn observe(&mut self, fact: &ContinuityFact, retain_identity_history: bool) {
        match &fact.cause {
            ContinuityCause::Duplicate { .. } => {
                self.duplicates += 1;
                return;
            }
            ContinuityCause::Conflict { .. } => {
                self.conflicts += 1;
                return;
            }
            _ => {}
        }

        if retain_identity_history {
            self.identity
                .remember(&fact.record_id, fact.content, fact.seq);
        }

        let lane = fact.key.lane.clone();
        self.lanes
            .entry(lane)
            .and_modify(|authority| {
                if authority.active_epoch != fact.key.epoch {
                    authority.active_epoch = fact.key.epoch.clone();
                    authority.established_at = fact.seq;
                }
            })
            .or_insert(LaneAuthority {
                active_epoch: fact.key.epoch.clone(),
                established_at: fact.seq,
            });

        self.connections.insert(
            (fact.key.lane.venue, fact.key.epoch.clone()),
            fact.local_counter,
        );

        let epoch = self.epochs.entry(fact.key.clone()).or_default();
        epoch.observed_counter = Some(fact.local_counter);
        if let Some(key) = fact.monotonic_key {
            epoch.monotonic_key = Some(key.max(epoch.monotonic_key.unwrap_or(0)));
        }

        match &fact.cause {
            ContinuityCause::Lifecycle => {}
            ContinuityCause::GapProven { .. } => {
                epoch.frames += 1;
                epoch.proven_gaps += 1;
                epoch.health = EpochHealth::Stale;
            }
            ContinuityCause::CursorWentBackwards { .. } => {
                epoch.frames += 1;
                epoch.backwards += 1;
                epoch.health = EpochHealth::Stale;
            }
            ContinuityCause::LocalCounterBroken { .. } => {
                epoch.frames += 1;
                epoch.local_breaks += 1;
                epoch.health = EpochHealth::Stale;
            }
            _ => {
                epoch.frames += 1;
                if epoch.health == EpochHealth::AwaitingBootstrap {
                    epoch.health = EpochHealth::Healthy;
                }
            }
        }
    }
}
