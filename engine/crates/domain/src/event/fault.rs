use serde::{Deserialize, Serialize};

use super::{DomainError, InstrumentId, LaneId, validate_text};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FaultImpact {
    Instrument(InstrumentId),
    RequestedVenueBooks(String),
    AuditCoverageOnly(String),
    UnattributedLane(LaneId),
}

impl FaultImpact {
    fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::RequestedVenueBooks(venue) | Self::AuditCoverageOnly(venue) => {
                validate_text(venue, "venue")
            }
            Self::Instrument(_) | Self::UnattributedLane(_) => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizationFault {
    reject_id: String,
    impact: FaultImpact,
}

impl NormalizationFault {
    pub fn new(reject_id: impl Into<String>, impact: FaultImpact) -> Result<Self, DomainError> {
        let reject_id = reject_id.into();
        validate_text(&reject_id, "reject_id")?;
        impact.validate()?;
        Ok(Self { reject_id, impact })
    }

    pub fn reject_id(&self) -> &str {
        &self.reject_id
    }

    pub fn impact(&self) -> &FaultImpact {
        &self.impact
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        validate_text(&self.reject_id, "reject_id")?;
        self.impact.validate()
    }
}
