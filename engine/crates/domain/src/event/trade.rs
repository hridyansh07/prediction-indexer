use serde::{Deserialize, Serialize};

use crate::{ConditionalMarketPrice, PositiveQty};

use super::{ContractOrientation, InstrumentId, Side, VenueTime};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeEvent {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    price: ConditionalMarketPrice,
    quantity: PositiveQty,
    aggressor: Option<Side>,
    #[serde(deserialize_with = "super::time::required_option")]
    venue_time: Option<VenueTime>,
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
            venue_time: None,
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

    pub fn with_venue_time(mut self, time: Option<VenueTime>) -> Self {
        self.venue_time = time;
        self
    }

    pub const fn venue_time(&self) -> Option<VenueTime> {
        self.venue_time
    }
}
