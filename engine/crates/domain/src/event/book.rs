use serde::{Deserialize, Serialize};

use crate::{ConditionalMarketPrice, PositiveQty};

use super::{DomainError, InstrumentId, validate_optional_text, validate_text};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

/// Whether prices refer to the named outcome or its logical complement.
/// Conversion between orientations is never implicit at this boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractOrientation {
    Outcome,
    Complement,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Level {
    price: ConditionalMarketPrice,
    quantity: PositiveQty,
}

impl Level {
    pub const fn new(price: ConditionalMarketPrice, quantity: PositiveQty) -> Self {
        Self { price, quantity }
    }

    pub const fn price(self) -> ConditionalMarketPrice {
        self.price
    }

    pub const fn quantity(self) -> PositiveQty {
        self.quantity
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullBook {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    bids: Vec<Level>,
    asks: Vec<Level>,
    snapshot_hash: Option<String>,
    source_observed_ns: Option<u64>,
}

impl FullBook {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        mut bids: Vec<Level>,
        mut asks: Vec<Level>,
        snapshot_hash: Option<String>,
        source_observed_ns: Option<u64>,
    ) -> Result<Self, DomainError> {
        validate_optional_text(&snapshot_hash, "snapshot_hash")?;
        validate_level_scales(&bids, &asks)?;
        bids.sort_by_key(|level| core::cmp::Reverse(level.price.atoms()));
        asks.sort_by_key(|level| level.price.atoms());
        reject_duplicate_prices(&bids)?;
        reject_duplicate_prices(&asks)?;
        Ok(Self {
            instrument,
            orientation,
            bids,
            asks,
            snapshot_hash,
            source_observed_ns,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub fn bids(&self) -> &[Level] {
        &self.bids
    }

    pub fn asks(&self) -> &[Level] {
        &self.asks
    }

    pub fn snapshot_hash(&self) -> Option<&str> {
        self.snapshot_hash.as_deref()
    }

    pub const fn source_observed_ns(&self) -> Option<u64> {
        self.source_observed_ns
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        validate_optional_text(&self.snapshot_hash, "snapshot_hash")?;
        validate_level_scales(&self.bids, &self.asks)?;
        validate_order(&self.bids, Side::Bid)?;
        validate_order(&self.asks, Side::Ask)
    }
}

/// A complete, direction-explicit instruction for one book level.
///
/// Sign never carries operation semantics. Set and relative changes require a
/// positive quantity, while deletion is represented by its own variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum LevelChange {
    Set(PositiveQty),
    Delete,
    Increase(PositiveQty),
    Decrease(PositiveQty),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookDelta {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    side: Side,
    price: ConditionalMarketPrice,
    change: LevelChange,
    book_hash: Option<String>,
}

impl BookDelta {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        side: Side,
        price: ConditionalMarketPrice,
        change: LevelChange,
        book_hash: Option<String>,
    ) -> Result<Self, DomainError> {
        validate_optional_text(&book_hash, "book_hash")?;
        Ok(Self {
            instrument,
            orientation,
            side,
            price,
            change,
            book_hash,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub const fn side(&self) -> Side {
        self.side
    }

    pub const fn price(&self) -> ConditionalMarketPrice {
        self.price
    }

    pub const fn change(&self) -> LevelChange {
        self.change
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        validate_optional_text(&self.book_hash, "book_hash")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum BookEvent {
    Full(FullBook),
    Delta(BookDelta),
}

impl BookEvent {
    pub(super) fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::Full(book) => book.validate(),
            Self::Delta(delta) => delta.validate(),
        }
    }
}

/// Independently observed full-book evidence. It is not a current-state reset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditAnchor {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    bids: Vec<Level>,
    asks: Vec<Level>,
    snapshot_hash: String,
    source_observed_ns: Option<u64>,
}

impl AuditAnchor {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        mut bids: Vec<Level>,
        mut asks: Vec<Level>,
        snapshot_hash: impl Into<String>,
        source_observed_ns: Option<u64>,
    ) -> Result<Self, DomainError> {
        let snapshot_hash = snapshot_hash.into();
        validate_text(&snapshot_hash, "snapshot_hash")?;
        validate_level_scales(&bids, &asks)?;
        bids.sort_by_key(|level| core::cmp::Reverse(level.price.atoms()));
        asks.sort_by_key(|level| level.price.atoms());
        reject_duplicate_prices(&bids)?;
        reject_duplicate_prices(&asks)?;
        Ok(Self {
            instrument,
            orientation,
            bids,
            asks,
            snapshot_hash,
            source_observed_ns,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub fn bids(&self) -> &[Level] {
        &self.bids
    }

    pub fn asks(&self) -> &[Level] {
        &self.asks
    }

    pub fn snapshot_hash(&self) -> &str {
        &self.snapshot_hash
    }

    pub const fn source_observed_ns(&self) -> Option<u64> {
        self.source_observed_ns
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        validate_text(&self.snapshot_hash, "snapshot_hash")?;
        validate_level_scales(&self.bids, &self.asks)?;
        validate_order(&self.bids, Side::Bid)?;
        validate_order(&self.asks, Side::Ask)
    }
}

fn validate_level_scales(bids: &[Level], asks: &[Level]) -> Result<(), DomainError> {
    let mut levels = bids.iter().chain(asks);
    let Some(first) = levels.next() else {
        return Ok(());
    };
    for level in levels {
        if level.price.scale() != first.price.scale()
            || level.quantity.scale() != first.quantity.scale()
        {
            return Err(DomainError::ScaleMismatch);
        }
    }
    Ok(())
}

fn reject_duplicate_prices(levels: &[Level]) -> Result<(), DomainError> {
    if levels
        .windows(2)
        .any(|pair| pair[0].price.atoms() == pair[1].price.atoms())
    {
        Err(DomainError::DuplicatePrice)
    } else {
        Ok(())
    }
}

fn validate_order(levels: &[Level], side: Side) -> Result<(), DomainError> {
    reject_duplicate_prices(levels)?;
    if levels.windows(2).any(|pair| match side {
        Side::Bid => pair[0].price.atoms() <= pair[1].price.atoms(),
        Side::Ask => pair[0].price.atoms() >= pair[1].price.atoms(),
    }) {
        Err(DomainError::NonCanonicalLevelOrder)
    } else {
        Ok(())
    }
}
