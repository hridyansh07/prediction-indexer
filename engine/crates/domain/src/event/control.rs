use serde::{Deserialize, Serialize};

use super::{DomainError, InstrumentId, validate_optional_text, validate_text};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ControlEvent {
    ConnectionOpened {
        epoch: String,
        instruments: Vec<InstrumentId>,
        delivers_deltas: bool,
        target_digest: Option<String>,
    },
    ConnectionClosed {
        epoch: String,
    },
    ConnectionFailed {
        epoch: String,
        reason: String,
    },
    SubscriptionChanged {
        from: Option<String>,
        to: String,
    },
    MetadataChanged {
        from: Option<String>,
        to: String,
    },
}

impl ControlEvent {
    pub(super) fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::ConnectionOpened {
                epoch,
                target_digest,
                ..
            } => {
                validate_text(epoch, "epoch")?;
                validate_optional_text(target_digest, "target_digest")
            }
            Self::ConnectionClosed { epoch } => validate_text(epoch, "epoch"),
            Self::ConnectionFailed { epoch, reason } => {
                validate_text(epoch, "epoch")?;
                validate_text(reason, "reason")
            }
            Self::SubscriptionChanged { from, to } | Self::MetadataChanged { from, to } => {
                validate_optional_text(from, "from")?;
                validate_text(to, "to")
            }
        }
    }
}
