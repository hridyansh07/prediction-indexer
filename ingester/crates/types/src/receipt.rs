//! Proof that a record's bytes are down and its position is decided.
//!
//! Two sinks in this system make that claim, and they make the same one:
//!
//! ```text
//! indexer-ingest    Store::capture_raw   the row is in SQLite, EvidenceSeq assigned
//! indexer-finalize  the canonical writer the line is in decoded evidence.ndjson.zst, CanonicalSeq assigned
//! ```
//!
//! The classifier is gated on holding one, enforcing the rule that
//! **state moves only after commit** — a crash between deciding and committing
//! must lose nothing. That rule is about the write having happened, not about
//! which sink accepted it, so the contract lives here rather than being spelled
//! as one concrete store type. Without this the finalizer could not classify at
//! all: `CapturedRecord`'s fields are private and only `capture_raw` mints one.
//!
//! Keeping it in `indexer-types` is what lets `indexer-continuity` depend on
//! neither store nor finalizer — so the finalizer classifies without linking
//! SQLite, and the store keeps its receipts unforgeable outside the transaction
//! that issues them.

use crate::sequence::FactSeq;

/// A record whose bytes are durable and whose position has been assigned.
pub trait Positioned {
    fn position(&self) -> FactSeq;
}

/// A durable classification, carrying its value onward to the reducer.
///
/// Separate from [`Positioned`] because the two gate different steps: a position
/// lets a fact be *decided*, a commit lets retained state *move*.
pub trait Committed<T> {
    fn position(&self) -> FactSeq;
    fn value(&self) -> &T;
}
