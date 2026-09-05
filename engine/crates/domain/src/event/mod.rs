//! Closed normalized events, grouped by domain responsibility.

mod book;
mod control;
mod error;
mod fault;
mod identity;
mod provenance;
mod record;
mod trade;

pub use book::{
    AuditAnchor, BookDelta, BookEvent, ContractOrientation, FullBook, Level, LevelSize,
    LevelSizeMode, Side,
};
pub use control::ControlEvent;
pub use error::DomainError;
pub use fault::{FaultImpact, NormalizationFault};
pub use identity::{InstrumentId, LaneId};
pub use provenance::{CanonicalProvenance, ContinuityVerdict, EventAddress, EventHeader};
pub use record::{SegmentEvent, SegmentRecord};
pub use trade::TradeEvent;

fn validate_text(value: &str, field: &'static str) -> Result<(), DomainError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        Err(DomainError::Empty(field))
    } else {
        Ok(())
    }
}

fn validate_optional_text(value: &Option<String>, field: &'static str) -> Result<(), DomainError> {
    match value {
        Some(value) => validate_text(value, field),
        None => Ok(()),
    }
}
