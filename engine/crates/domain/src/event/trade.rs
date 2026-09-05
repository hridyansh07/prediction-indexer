use serde::{Deserialize, Serialize};

use crate::{Px, Qty};

use super::{ContractOrientation, DomainError, InstrumentId, Side};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeEvent {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    price: Px,
    quantity: Qty,
    aggressor: Option<Side>,
}

impl TradeEvent {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        price: Px,
        quantity: Qty,
        aggressor: Option<Side>,
    ) -> Result<Self, DomainError> {
        if quantity.atoms() <= 0 {
            return Err(DomainError::NonPositiveLevel);
        }
        Ok(Self {
            instrument,
            orientation,
            price,
            quantity,
            aggressor,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub const fn price(&self) -> Px {
        self.price
    }

    pub const fn quantity(&self) -> Qty {
        self.quantity
    }

    pub const fn aggressor(&self) -> Option<Side> {
        self.aggressor
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.quantity.atoms() <= 0 {
            Err(DomainError::NonPositiveLevel)
        } else {
            Ok(())
        }
    }
}
