//! Frozen schema-3 wire shapes. Decode/encode old records without inventing a
//! venue_time field or relaxing their closed schemas.
use super::{
    AuditAnchor, BookDelta, BookEvent, ContractOrientation, ControlEvent, DomainError, FullBook,
    InstrumentId, Level, LevelChange, NormalizationFault, SegmentEvent, Side, TradeEvent,
};
use crate::{BookStateHash, ConditionalMarketPrice, PositiveQty};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(super) enum Event {
    Control(ControlEvent),
    Book(Book),
    AuditAnchor(AuditAnchor),
    NormalizationFault(NormalizationFault),
    Trade(Trade),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(super) enum Book {
    Full(Full),
    Delta(Delta),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Full {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    bids: Vec<Level>,
    asks: Vec<Level>,
    snapshot_hash: Option<BookStateHash>,
    source_observed_ns: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Delta {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    side: Side,
    price: ConditionalMarketPrice,
    change: LevelChange,
    book_hash: Option<BookStateHash>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Trade {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    price: ConditionalMarketPrice,
    quantity: PositiveQty,
    aggressor: Option<Side>,
}

impl TryFrom<Event> for SegmentEvent {
    type Error = DomainError;
    fn try_from(event: Event) -> Result<Self, Self::Error> {
        Ok(match event {
            Event::Control(v) => Self::Control(v),
            Event::AuditAnchor(v) => Self::AuditAnchor(v),
            Event::NormalizationFault(v) => Self::NormalizationFault(v),
            Event::Trade(v) => Self::Trade(TradeEvent::new(
                v.instrument,
                v.orientation,
                v.price,
                v.quantity,
                v.aggressor,
            )),
            Event::Book(Book::Delta(v)) => Self::Book(BookEvent::Delta(BookDelta::new(
                v.instrument,
                v.orientation,
                v.side,
                v.price,
                v.change,
                v.book_hash,
            )?)),
            Event::Book(Book::Full(v)) => {
                // Constructors sort new books; persisted readers must reject
                // noncanonical ordering before constructing an old book.
                super::book::validate_level_scales(&v.bids, &v.asks)?;
                super::book::validate_order(&v.bids, Side::Bid)?;
                super::book::validate_order(&v.asks, Side::Ask)?;
                Self::Book(BookEvent::Full(FullBook::new(
                    v.instrument,
                    v.orientation,
                    v.bids,
                    v.asks,
                    v.snapshot_hash,
                    v.source_observed_ns,
                )?))
            }
        })
    }
}

impl From<&SegmentEvent> for Event {
    fn from(event: &SegmentEvent) -> Self {
        match event {
            SegmentEvent::Control(v) => Self::Control(v.clone()),
            SegmentEvent::AuditAnchor(v) => Self::AuditAnchor(v.clone()),
            SegmentEvent::NormalizationFault(v) => Self::NormalizationFault(v.clone()),
            SegmentEvent::Trade(v) => Self::Trade(Trade {
                instrument: v.instrument().clone(),
                orientation: v.orientation(),
                price: v.price(),
                quantity: v.quantity(),
                aggressor: v.aggressor(),
            }),
            SegmentEvent::Book(BookEvent::Delta(v)) => Self::Book(Book::Delta(Delta {
                instrument: v.instrument().clone(),
                orientation: v.orientation(),
                side: v.side(),
                price: v.price(),
                change: v.change(),
                book_hash: v.book_hash(),
            })),
            SegmentEvent::Book(BookEvent::Full(v)) => Self::Book(Book::Full(Full {
                instrument: v.instrument().clone(),
                orientation: v.orientation(),
                bids: v.bids().to_vec(),
                asks: v.asks().to_vec(),
                snapshot_hash: v.snapshot_hash(),
                source_observed_ns: v.source_observed_ns(),
            })),
        }
    }
}
