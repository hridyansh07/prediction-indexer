//! Shared vocabulary: positions, identities, canonical bytes, envelope parsing.
//!
//! Nothing here touches a network, a clock, or a database. The crate exists so
//! that the store and the classifier agree on what a record *is* before either of
//! them decides what to do about it.
//!
//! No financial/economic-event layer. Normalising venue frames into typed
//! economic events at ingest is what a system needs when it has to act on them
//! immediately; this one does not, so `Money`, `Qty`, `Price` and the whole
//! fixed-point stack have no consumer here. Capture stores bytes.
//! Interpretation happens later, where it can be revised.

pub mod envelope;
pub mod error;
pub mod hash;
pub mod identity;
pub mod receipt;
pub mod sequence;
pub mod sink;

pub use envelope::EnvelopeView;
pub use error::{DecodeError, EnvelopeError};
pub use hash::{ContentHash, Digest32, EvidenceHash, FactHash, StreamHasher};
pub use identity::{EpochId, LogicalTime, RecordId, RecordKind, SourceCursor, Stream, Venue};
pub use receipt::{Committed, Positioned};
pub use sequence::{CanonicalSeq, EvidenceSeq, FactSeq};
pub use sink::{SinkError, Sinkable};
