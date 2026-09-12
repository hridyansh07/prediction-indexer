use replay_domain::{
    BookDelta, BookEvent, ConditionalMarketPrice, ContractOrientation, FullBook, InstrumentId,
    Level, LevelChange, PositiveQty, SegmentEvent, Side, TradeEvent,
};

use crate::{error::Reject, message::MessageOutcome};

pub(crate) struct Snapshot {
    pub(crate) instrument: InstrumentId,
    pub(crate) yes: Vec<Level>,
    pub(crate) no: Vec<Level>,
}

impl TryFrom<Snapshot> for MessageOutcome {
    type Error = Reject;

    fn try_from(snapshot: Snapshot) -> Result<Self, Self::Error> {
        let outcome = FullBook::new(
            snapshot.instrument.clone(),
            ContractOrientation::Outcome,
            snapshot.yes,
            Vec::new(),
            None,
            None,
        )
        .map_err(|_| {
            Reject::for_instrument("invalid_snapshot_levels", snapshot.instrument.clone())
        })?;
        let complement = FullBook::new(
            snapshot.instrument.clone(),
            ContractOrientation::Complement,
            snapshot.no,
            Vec::new(),
            None,
            None,
        )
        .map_err(|_| Reject::for_instrument("invalid_snapshot_levels", snapshot.instrument))?;
        Ok(Self::Events(vec![
            SegmentEvent::Book(BookEvent::Full(outcome)),
            SegmentEvent::Book(BookEvent::Full(complement)),
        ]))
    }
}

pub(crate) struct RelativeDelta {
    pub(crate) instrument: InstrumentId,
    pub(crate) orientation: ContractOrientation,
    pub(crate) price: ConditionalMarketPrice,
    pub(crate) change: LevelChange,
}

impl TryFrom<RelativeDelta> for MessageOutcome {
    type Error = Reject;

    fn try_from(delta: RelativeDelta) -> Result<Self, Self::Error> {
        let event = BookDelta::new(
            delta.instrument.clone(),
            delta.orientation,
            Side::Bid,
            delta.price,
            delta.change,
            None,
        )
        .map_err(|_| Reject::for_instrument("invalid_delta", delta.instrument))?;
        Ok(Self::Events(vec![SegmentEvent::Book(BookEvent::Delta(
            event,
        ))]))
    }
}

pub(crate) struct Trade {
    pub(crate) instrument: InstrumentId,
    pub(crate) price: ConditionalMarketPrice,
    pub(crate) quantity: PositiveQty,
    pub(crate) aggressor: Side,
}

impl TryFrom<Trade> for MessageOutcome {
    type Error = Reject;

    fn try_from(trade: Trade) -> Result<Self, Self::Error> {
        let event = TradeEvent::new(
            trade.instrument.clone(),
            ContractOrientation::Outcome,
            trade.price,
            trade.quantity,
            Some(trade.aggressor),
        );
        Ok(Self::Events(vec![SegmentEvent::Trade(event)]))
    }
}
