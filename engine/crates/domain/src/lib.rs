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
    AuditAnchor, BookDelta, BookEvent, BookKey, CanonicalProvenance, ContinuityVerdict,
    ContractOrientation, ControlEvent, DomainError, EventAddress, EventHeader, FaultImpact,
    FullBook, InstrumentId, LaneId, Level, LevelChange, NormalizationFault, SegmentEvent,
    SegmentRecord, Side, TradeEvent,
};
pub use hash::{BookStateHash, Sha1, Sha1Error};
pub use indexer_types::Sha256;
pub use mutation::{
    ApplyError, MutationReceipt, PrepareError, PreparedMutation, Revision, Revisioned,
};
pub use numeric::{
    ConditionalMarketPrice, DecimalScale, MAX_DECIMAL_SCALE, MAX_QUANTITY_ATOMS, Magnitude,
    NumericError, PositiveQty, PriceUnit, Px, Qty, QuantityUnit,
};

/// Frozen reader schema. Its types/serialization must remain available when a
/// future writer schema is introduced; never retarget this constant.
pub const SEGMENT_SCHEMA_V3: u16 = 3;
/// Current writer selection, independent of historical reader dispatch.
pub const SEGMENT_SCHEMA_VERSION: u16 = SEGMENT_SCHEMA_V3;
