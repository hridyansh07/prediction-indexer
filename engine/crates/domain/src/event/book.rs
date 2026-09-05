use serde::{Deserialize, Serialize};

use crate::{Px, Qty};

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
    price: Px,
    quantity: Qty,
}

impl Level {
    pub fn new(price: Px, quantity: Qty) -> Result<Self, DomainError> {
        if quantity.atoms() <= 0 {
            return Err(DomainError::NonPositiveLevel);
        }
        Ok(Self { price, quantity })
    }

    pub const fn price(self) -> Px {
        self.price
    }

    pub const fn quantity(self) -> Qty {
        self.quantity
    }

    fn validate(&self) -> Result<(), DomainError> {
        if self.quantity.atoms() <= 0 {
            Err(DomainError::NonPositiveLevel)
        } else {
            Ok(())
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LevelSizeMode {
    Absolute,
    Relative,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LevelSize {
    mode: LevelSizeMode,
    quantity: Qty,
}

impl LevelSize {
    pub fn absolute(quantity: Qty) -> Result<Self, DomainError> {
        if quantity.atoms() < 0 {
            Err(DomainError::NegativeAbsoluteLevel)
        } else {
            Ok(Self {
                mode: LevelSizeMode::Absolute,
                quantity,
            })
        }
    }

    pub fn relative(quantity: Qty) -> Result<Self, DomainError> {
        if quantity.atoms() == 0 {
            Err(DomainError::ZeroRelativeLevel)
        } else {
            Ok(Self {
                mode: LevelSizeMode::Relative,
                quantity,
            })
        }
    }

    pub const fn mode(self) -> LevelSizeMode {
        self.mode
    }

    pub const fn quantity(self) -> Qty {
        self.quantity
    }

    fn validate(self) -> Result<(), DomainError> {
        match self.mode {
            LevelSizeMode::Absolute if self.quantity.atoms() < 0 => {
                Err(DomainError::NegativeAbsoluteLevel)
            }
            LevelSizeMode::Relative if self.quantity.atoms() == 0 => {
                Err(DomainError::ZeroRelativeLevel)
            }
            _ => Ok(()),
        }
    }
}

impl<'de> Deserialize<'de> for LevelSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            mode: LevelSizeMode,
            quantity: Qty,
        }

        let wire = Wire::deserialize(deserializer)?;
        match wire.mode {
            LevelSizeMode::Absolute => Self::absolute(wire.quantity),
            LevelSizeMode::Relative => Self::relative(wire.quantity),
        }
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookDelta {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    side: Side,
    price: Px,
    size: LevelSize,
    book_hash: Option<String>,
}

impl BookDelta {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        side: Side,
        price: Px,
        size: LevelSize,
        book_hash: Option<String>,
    ) -> Result<Self, DomainError> {
        size.validate()?;
        validate_optional_text(&book_hash, "book_hash")?;
        Ok(Self {
            instrument,
            orientation,
            side,
            price,
            size,
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

    pub const fn price(&self) -> Px {
        self.price
    }

    pub const fn size(&self) -> LevelSize {
        self.size
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        self.size.validate()?;
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
    first.validate()?;
    for level in levels {
        level.validate()?;
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
