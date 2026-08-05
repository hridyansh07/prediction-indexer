//! What a delivery means for continuity: identity, epoch health, cursor order.
//!
//! Two rules shape the whole crate:
//!
//! 1. **Identity is decided before continuity.** A retransmission must not be able
//!    to move a counter or stale a stream, so duplicate detection short-circuits
//!    everything else.
//! 2. **State moves only after commit.** `classify` takes `&self` and previews the
//!    transition into the fact; `apply` requires a store receipt. A crash between
//!    them loses nothing, because the committed fact carries the transition.
//!
//! Cursor handling does not assume a dense snapshot id, and the reason is our
//! venues. Treating a snapshot id as dense is right for Binance and wrong for
//! Limitless: its `version` is monotonic per market but jumps by thousands between
//! consecutive updates to the same book, with ranges overlapping across markets.
//! Applying a density assumption there would report a gap on nearly every message.
//! So sparse-monotonic is a first-class class here, and only `UpdateRange` can ever
//! *prove* a loss.

pub mod fact;
pub mod state;

pub use fact::{ContinuityCause, ContinuityFact};
pub use state::{ClassifierState, EpochHealth, EpochKey, EpochState, IdentityVerdict, LaneId};

use indexer_types::{Committed, ContentHash, EnvelopeView, Positioned, RecordKind, SourceCursor};

/// Previews and applies continuity transitions.
#[derive(Default)]
pub struct Classifier {
    state: ClassifierState,
}

impl Classifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> &ClassifierState {
        &self.state
    }

    /// Previews what one delivery means. Takes `&self` — nothing moves here.
    ///
    /// Gated on a [`Positioned`] receipt rather than on one concrete store type:
    /// the rule is that the bytes are already down and the position decided, and
    /// two sinks satisfy it. `indexer-ingest` passes the row it captured;
    /// `indexer-finalize` passes the line it wrote into the canonical file. That
    /// is also why this crate depends on neither of them.
    pub fn classify(
        &self,
        envelope: &EnvelopeView<'_>,
        captured: &impl Positioned,
    ) -> ContinuityFact {
        let fact_seq = captured.position();
        let content = ContentHash::hash(envelope.raw_payload.as_bytes());
        let key = EpochKey::new(
            envelope.venue,
            envelope.stream,
            envelope.connection_epoch.as_str().to_owned(),
        );

        // Rule 1: identity first. A retransmission is not evidence of anything
        // about the stream's continuity, so it must not be allowed to touch it.
        let verdict = self
            .state
            .identity
            .verdict(envelope.record_id.as_str(), content);
        if !matches!(verdict, IdentityVerdict::Unseen) {
            return ContinuityFact::duplicate(fact_seq, envelope, key, content, verdict);
        }

        // The local counter belongs to the connection, not the lane: one splice
        // connection spends a single counter across every stream it carries.
        let previous_counter = self
            .state
            .connections
            .get(&(
                envelope.venue,
                envelope.connection_epoch.as_str().to_owned(),
            ))
            .copied();
        let previous_key = self
            .state
            .epochs
            .get(&key)
            .and_then(|state| state.monotonic_key);

        let cause = classify_cause(envelope, previous_counter, previous_key);
        ContinuityFact::accepted(fact_seq, envelope, key, content, cause)
    }

    /// Moves retained state. Requires a commit receipt, so a crash between
    /// `classify` and here costs nothing — replay re-derives from the fact.
    pub fn apply(&mut self, committed: &impl Committed<ContinuityFact>) {
        let fact = committed.value();
        self.state.observe(fact);
    }

    /// Replaces the retained state wholesale.
    ///
    /// For a reader that recovers ordering state from somewhere other than a
    /// fact log — the finalizer seeds it from the watermark, because a window's
    /// verdicts have to continue the previous window's counters rather than
    /// start over. Deliberately not a mutable borrow of `state`: the caller
    /// supplies a whole coherent state rather than reaching in.
    pub fn restore(&mut self, state: ClassifierState) {
        self.state = state;
    }
}

/// What this delivery says about its stream's continuity.
fn classify_cause(
    envelope: &EnvelopeView<'_>,
    previous_counter: Option<u64>,
    previous_key: Option<u64>,
) -> ContinuityCause {
    // Our own per-connection counter is dense by construction, and every record
    // consumes one — lifecycle records included. So this is checked before the
    // kind is considered: a break inside a run of control records is just as much
    // a torn spool as a break between frames, and skipping the check for them
    // would leave a blind spot exactly where reconnects cluster.
    if let Some(previous) = previous_counter {
        if envelope.local_counter != previous + 1 {
            return ContinuityCause::LocalCounterBroken {
                expected: previous + 1,
                observed: envelope.local_counter,
            };
        }
    }

    // A control or fault record is our own narration of the connection, not a
    // venue message. It carries no cursor and must not be measured against the
    // venue's continuity, or every `connection_opened` would read as a gap.
    if envelope.kind != RecordKind::VenueFrame {
        return ContinuityCause::Lifecycle;
    }

    let Some(cursor) = envelope.source_cursor else {
        return ContinuityCause::Bootstrap;
    };

    match cursor {
        // The venue numbers nothing. Our order is the only order, and no claim
        // about the venue's continuity is available at capture time. Polymarket's
        // book hash makes a claim possible later, in the analysis layer, from the
        // payload we stored verbatim.
        SourceCursor::Unsequenced { .. } => ContinuityCause::UnsequencedVenue,

        // Monotonic per *instrument*, and the ingester cannot see instruments.
        //
        // A lane multiplexes every market on one connection, so comparing a
        // snapshot id against the lane-wide previous value compares two different
        // books. Limitless makes this concrete: its `version` behaves like a
        // server-wide counter sampled per book, so consecutive frames for
        // different markets legitimately move backwards relative to each other.
        // The first ingest run reported 7 such "faults" over 451 frames, none of
        // them real.
        //
        // Identifying the instrument would mean parsing the venue payload, which
        // is normalisation, which this component deliberately does not do. So the
        // key is recorded on the fact and instrument-level continuity is left to
        // the analysis layer, which does parse and can group correctly. The
        // classification here stays honest about what a connection-level view can
        // actually establish.
        SourceCursor::SnapshotId { .. } | SourceCursor::SnapshotTime { .. } => {
            ContinuityCause::SparseMonotonic
        }

        // The only variant where a hole can prove a loss — and only because a
        // range carries its own predecessor, so continuity is checkable without
        // knowing which instrument it belongs to.
        //
        // Kalshi is expected to land here and its real shape is unverified. If its
        // deltas turn out to be numbered per connection rather than per market,
        // this holds; if per market, it needs the same treatment as the snapshot
        // variants above.
        SourceCursor::UpdateRange {
            first,
            last,
            previous_last,
        } => match previous_key {
            None => ContinuityCause::Bootstrap,
            Some(previous) if previous_last == previous || first == previous + 1 => {
                ContinuityCause::Continuous { first, last }
            }
            Some(previous) if last <= previous => ContinuityCause::CursorWentBackwards {
                previous,
                observed: last,
            },
            Some(previous) => ContinuityCause::GapProven { previous, first },
        },
    }
}
