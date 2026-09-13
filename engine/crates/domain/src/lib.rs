//! Stable, venue-independent Replay values.
//!
//! This crate starts after `indexer-finalize` has verified and joined canonical
//! evidence. It deliberately does not reproduce finalizer receipts or audit
//! capabilities. A later tape adapter converts each finished
//! `JoinedCanonicalRecord` into these owned values while preserving every field.

mod event;
mod hash;
mod mutation;
mod numeric;

pub use event::{
    AuditAnchor, BookDelta, BookEvent, CanonicalProvenance, ContinuityVerdict, ContractOrientation,
    ControlEvent, DomainError, EventAddress, EventHeader, FaultImpact, FullBook, InstrumentId,
    LaneId, Level, LevelChange, NormalizationFault, SegmentEvent, SegmentRecord, Side, TradeEvent,
};
pub use hash::{BookStateHash, Sha1, Sha1Error};
pub use indexer_types::Sha256;
pub use mutation::{
    ApplyError, MutationReceipt, PrepareError, PreparedMutation, Revision, Revisioned,
};
pub use numeric::{
    ConditionalMarketPrice, DecimalScale, MAX_QUANTITY_ATOMS, Magnitude, NumericError, PositiveQty,
    PriceUnit, Px, Qty, QuantityUnit,
};

/// Canonical persisted schema emitted and consumed by the Replay engine.
pub const SEGMENT_SCHEMA_VERSION: u16 = 3;
