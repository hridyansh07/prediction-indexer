use serde::{Deserialize, Serialize};

use crate::{ConditionalMarketPrice, PositiveQty};

use super::{ContractOrientation, InstrumentId, Side};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeEvent {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    price: ConditionalMarketPrice,
    quantity: PositiveQty,
    aggressor: Option<Side>,
}

impl TradeEvent {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        price: ConditionalMarketPrice,
        quantity: PositiveQty,
        aggressor: Option<Side>,
    ) -> Self {
        Self {
            instrument,
            orientation,
            price,
            quantity,
            aggressor,
        }
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub const fn price(&self) -> ConditionalMarketPrice {
        self.price
    }

    pub const fn quantity(&self) -> PositiveQty {
        self.quantity
    }

    pub const fn aggressor(&self) -> Option<Side> {
        self.aggressor
    }
}
