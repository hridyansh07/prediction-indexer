//! Stable, venue-independent Replay values.
//!
//! This crate starts after `indexer-finalize` has verified and joined canonical
//! evidence. It deliberately does not reproduce finalizer receipts or audit
//! capabilities. A later tape adapter converts each finished
//! `JoinedCanonicalRecord` into these owned values while preserving every field.

mod event;
mod mutation;
mod numeric;

pub use event::{
    AuditAnchor, BookDelta, BookEvent, CanonicalProvenance, ContinuityVerdict, ContractOrientation,
    ControlEvent, DomainError, EventAddress, EventHeader, FaultImpact, FullBook, InstrumentId,
    LaneId, Level, LevelSize, LevelSizeMode, NormalizationFault, SegmentEvent, SegmentRecord, Side,
    TradeEvent,
};
pub use mutation::{
    ApplyError, MutationReceipt, PrepareError, PreparedMutation, Revision, Revisioned,
};
pub use numeric::{DecimalScale, NumericError, PriceUnit, Px, Qty, QuantityUnit};

/// Canonical persisted schema emitted and consumed by Replay S2.
pub const SEGMENT_SCHEMA_VERSION: u16 = 1;
