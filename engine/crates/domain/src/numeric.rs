use core::fmt;

use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

/// Maximum decimal exponent supported by the fixed-point boundary.
pub const MAX_DECIMAL_SCALE: u8 = 18;

/// Replay quantities use unsigned storage but deliberately retain the current
/// signed-64-bit logical width until a persisted schema explicitly widens it.
pub const MAX_QUANTITY_ATOMS: u64 = i64::MAX as u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumericError {
    Empty,
    InvalidSyntax,
    NegativePrice,
    NegativeQuantity,
    ZeroQuantity,
    ConditionalPriceOutOfRange,
    LogicalMaximumExceeded,
    ScaleOutOfRange,
    Overflow,
    /// A non-zero value is smaller than one unit at the requested coarser scale.
    Underflow,
    /// Rescaling would discard a non-zero remainder.
    InexactRescale,
}

impl fmt::Display for NumericError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "decimal is empty",
            Self::InvalidSyntax => "decimal syntax is invalid",
            Self::NegativePrice => "price cannot be negative",
            Self::NegativeQuantity => "quantity cannot be negative",
            Self::ZeroQuantity => "quantity must be positive",
            Self::ConditionalPriceOutOfRange => {
                "conditional-market price must be between zero and one inclusive"
            }
            Self::LogicalMaximumExceeded => "quantity exceeds the Replay logical maximum",
            Self::ScaleOutOfRange => "decimal scale exceeds 18",
            Self::Overflow => "fixed-point value overflows its storage width",
            Self::Underflow => "value is below one unit at the requested scale",
            Self::InexactRescale => "rescale would discard a non-zero remainder",
        })
    }
}

impl std::error::Error for NumericError {}

/// Number of decimal digits represented by one integer atom.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct DecimalScale(u8);

impl DecimalScale {
    pub const fn new(exponent: u8) -> Result<Self, NumericError> {
        if exponent <= MAX_DECIMAL_SCALE {
            Ok(Self(exponent))
        } else {
            Err(NumericError::ScaleOutOfRange)
        }
    }

    pub const fn exponent(self) -> u8 {
        self.0
    }
}

impl<'de> Deserialize<'de> for DecimalScale {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let exponent = u8::deserialize(deserializer)?;
        Self::new(exponent).map_err(serde::de::Error::custom)
    }
}

/// Unit-free, unsigned exact arithmetic. `Magnitude` has no financial meaning
/// and is never persisted on its own; typed values wrap it at domain boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Magnitude {
    atoms: u64,
    scale: DecimalScale,
}

impl Magnitude {
    pub fn parse(text: &str, scale: DecimalScale) -> Result<Self, NumericError> {
        let atoms = u64::try_from(parse_unsigned_decimal(text, scale)?)
            .map_err(|_| NumericError::Overflow)?;
        Ok(Self { atoms, scale })
    }

    pub const fn from_atoms(atoms: u64, scale: DecimalScale) -> Self {
        Self { atoms, scale }
    }

    pub const fn atoms(self) -> u64 {
        self.atoms
    }

    pub const fn scale(self) -> DecimalScale {
        self.scale
    }

    pub fn checked_rescale(self, scale: DecimalScale) -> Result<Self, NumericError> {
        Ok(Self::from_atoms(
            rescale_u64(self.atoms, self.scale, scale)?,
            scale,
        ))
    }

    pub fn checked_add(self, other: Self) -> Result<Self, NumericError> {
        if self.scale != other.scale {
            return Err(NumericError::InexactRescale);
        }
        Ok(Self::from_atoms(
            self.atoms
                .checked_add(other.atoms)
                .ok_or(NumericError::Overflow)?,
            self.scale,
        ))
    }
}

/// Unit of a price atom. Currency identity and conversion are intentionally not
/// part of the numeric domain; a segment manifest will bind the quote currency later.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceUnit {
    QuotePerContract,
}

/// Unit of a quantity atom.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantityUnit {
    Contracts,
}

/// Exact nonnegative price in `10^-scale` quote units per contract.
///
/// `Px` deliberately has no market-specific upper bound. Domain refinements
/// such as [`ConditionalMarketPrice`] enforce those constraints.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Px {
    atoms: i64,
    scale: DecimalScale,
    unit: PriceUnit,
}

impl Px {
    pub fn parse(text: &str, scale: DecimalScale) -> Result<Self, NumericError> {
        if text.starts_with('-') {
            return Err(NumericError::NegativePrice);
        }
        let atoms = i64::try_from(parse_unsigned_decimal(text, scale)?)
            .map_err(|_| NumericError::Overflow)?;
        Ok(Self {
            atoms,
            scale,
            unit: PriceUnit::QuotePerContract,
        })
    }

    pub const fn from_atoms(atoms: i64, scale: DecimalScale) -> Result<Self, NumericError> {
        if atoms < 0 {
            return Err(NumericError::NegativePrice);
        }
        Ok(Self {
            atoms,
            scale,
            unit: PriceUnit::QuotePerContract,
        })
    }

    pub const fn atoms(self) -> i64 {
        self.atoms
    }

    pub const fn scale(self) -> DecimalScale {
        self.scale
    }

    pub const fn unit(self) -> PriceUnit {
        self.unit
    }

    pub fn checked_rescale(self, scale: DecimalScale) -> Result<Self, NumericError> {
        let atoms = rescale_i64(self.atoms, self.scale, scale)?;
        Self::from_atoms(atoms, scale)
    }
}

impl<'de> Deserialize<'de> for Px {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            atoms: i64,
            scale: DecimalScale,
            unit: PriceUnit,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.unit != PriceUnit::QuotePerContract {
            return Err(serde::de::Error::custom("unsupported price unit"));
        }
        Self::from_atoms(wire.atoms, wire.scale).map_err(serde::de::Error::custom)
    }
}

/// Exact conditional-market price in the inclusive interval `[0, 1]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct ConditionalMarketPrice(Px);

impl ConditionalMarketPrice {
    pub fn parse(text: &str, scale: DecimalScale) -> Result<Self, NumericError> {
        Self::try_from(Px::parse(text, scale)?)
    }

    pub fn from_atoms(atoms: i64, scale: DecimalScale) -> Result<Self, NumericError> {
        Self::try_from(Px::from_atoms(atoms, scale)?)
    }

    pub const fn as_px(self) -> Px {
        self.0
    }

    pub const fn atoms(self) -> i64 {
        self.0.atoms()
    }

    pub const fn scale(self) -> DecimalScale {
        self.0.scale()
    }

    pub const fn unit(self) -> PriceUnit {
        self.0.unit()
    }

    pub fn checked_rescale(self, scale: DecimalScale) -> Result<Self, NumericError> {
        Self::try_from(self.0.checked_rescale(scale)?)
    }
}

impl TryFrom<Px> for ConditionalMarketPrice {
    type Error = NumericError;

    fn try_from(price: Px) -> Result<Self, Self::Error> {
        let one = 10_i64.pow(u32::from(price.scale().exponent()));
        if price.atoms() > one {
            Err(NumericError::ConditionalPriceOutOfRange)
        } else {
            Ok(Self(price))
        }
    }
}

impl<'de> Deserialize<'de> for ConditionalMarketPrice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::try_from(Px::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Exact nonnegative quantity in `10^-scale` contracts.
///
/// Storage is unsigned, while construction enforces [`MAX_QUANTITY_ATOMS`] as
/// the current persisted Replay logical maximum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Qty {
    magnitude: Magnitude,
    unit: QuantityUnit,
}

impl Qty {
    pub fn parse(text: &str, scale: DecimalScale) -> Result<Self, NumericError> {
        if text.starts_with('-') {
            return Err(NumericError::NegativeQuantity);
        }
        Self::from_magnitude(Magnitude::parse(text, scale)?)
    }

    pub const fn from_atoms(atoms: u64, scale: DecimalScale) -> Result<Self, NumericError> {
        Self::from_magnitude(Magnitude::from_atoms(atoms, scale))
    }

    pub const fn from_magnitude(magnitude: Magnitude) -> Result<Self, NumericError> {
        if magnitude.atoms() > MAX_QUANTITY_ATOMS {
            return Err(NumericError::LogicalMaximumExceeded);
        }
        Ok(Self {
            magnitude,
            unit: QuantityUnit::Contracts,
        })
    }

    pub const fn magnitude(self) -> Magnitude {
        self.magnitude
    }

    pub const fn atoms(self) -> u64 {
        self.magnitude.atoms()
    }

    pub const fn scale(self) -> DecimalScale {
        self.magnitude.scale()
    }

    pub const fn unit(self) -> QuantityUnit {
        self.unit
    }

    pub fn checked_rescale(self, scale: DecimalScale) -> Result<Self, NumericError> {
        Self::from_magnitude(self.magnitude.checked_rescale(scale)?)
    }

    pub fn checked_add(self, other: Self) -> Result<Self, NumericError> {
        if self.unit != other.unit {
            return Err(NumericError::InexactRescale);
        }
        Self::from_magnitude(self.magnitude.checked_add(other.magnitude)?)
    }
}

impl Serialize for Qty {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut wire = serializer.serialize_struct("Qty", 3)?;
        wire.serialize_field("atoms", &self.atoms())?;
        wire.serialize_field("scale", &self.scale())?;
        wire.serialize_field("unit", &self.unit)?;
        wire.end()
    }
}

impl<'de> Deserialize<'de> for Qty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            atoms: u64,
            scale: DecimalScale,
            unit: QuantityUnit,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.unit != QuantityUnit::Contracts {
            return Err(serde::de::Error::custom("unsupported quantity unit"));
        }
        Self::from_atoms(wire.atoms, wire.scale).map_err(serde::de::Error::custom)
    }
}

/// A quantity proven nonzero at construction and deserialization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct PositiveQty(Qty);

impl PositiveQty {
    pub const fn new(quantity: Qty) -> Result<Self, NumericError> {
        if quantity.atoms() == 0 {
            Err(NumericError::ZeroQuantity)
        } else {
            Ok(Self(quantity))
        }
    }

    pub fn parse(text: &str, scale: DecimalScale) -> Result<Self, NumericError> {
        Self::new(Qty::parse(text, scale)?)
    }

    pub const fn as_qty(self) -> Qty {
        self.0
    }

    pub const fn atoms(self) -> u64 {
        self.0.atoms()
    }

    pub const fn scale(self) -> DecimalScale {
        self.0.scale()
    }

    pub const fn unit(self) -> QuantityUnit {
        self.0.unit()
    }

    pub fn checked_rescale(self, scale: DecimalScale) -> Result<Self, NumericError> {
        Self::new(self.0.checked_rescale(scale)?)
    }
}

impl<'de> Deserialize<'de> for PositiveQty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(Qty::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

fn power_of_ten(exponent: u8) -> u128 {
    10_u128.pow(u32::from(exponent))
}

fn parse_digits(text: &str) -> Result<u128, NumericError> {
    text.bytes().try_fold(0_u128, |value, byte| {
        if !byte.is_ascii_digit() {
            return Err(NumericError::InvalidSyntax);
        }
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u128::from(byte - b'0')))
            .ok_or(NumericError::Overflow)
    })
}

fn parse_unsigned_decimal(text: &str, scale: DecimalScale) -> Result<u128, NumericError> {
    if text.is_empty() {
        return Err(NumericError::Empty);
    }
    if text.starts_with(['+', '-']) {
        return Err(NumericError::InvalidSyntax);
    }
    let mut parts = text.split('.');
    let whole = parts.next().expect("split always has one part");
    let fraction = parts.next();
    if parts.next().is_some() || whole.is_empty() || fraction.is_some_and(str::is_empty) {
        return Err(NumericError::InvalidSyntax);
    }
    let mut fractional = fraction.unwrap_or("");
    let scale_len = usize::from(scale.exponent());
    if fractional.len() > scale_len {
        let (kept, discarded) = fractional.split_at(scale_len);
        if discarded.bytes().any(|byte| byte != b'0') {
            return Err(NumericError::InexactRescale);
        }
        fractional = kept;
    }
    let whole = parse_digits(whole)?;
    let fraction = parse_digits(fractional)?;
    let padding = scale_len - fractional.len();
    whole
        .checked_mul(power_of_ten(scale.exponent()))
        .and_then(|value| value.checked_add(fraction * power_of_ten(padding as u8)))
        .ok_or(NumericError::Overflow)
}

fn rescale_i64(
    atoms: i64,
    source: DecimalScale,
    destination: DecimalScale,
) -> Result<i64, NumericError> {
    match destination.exponent().cmp(&source.exponent()) {
        core::cmp::Ordering::Equal => Ok(atoms),
        core::cmp::Ordering::Greater => {
            let factor = power_of_ten(destination.exponent() - source.exponent());
            i64::try_from((atoms as u128) * factor).map_err(|_| NumericError::Overflow)
        }
        core::cmp::Ordering::Less => {
            let divisor = power_of_ten(source.exponent() - destination.exponent());
            let value = atoms as u128;
            if value % divisor != 0 {
                if value != 0 && value < divisor {
                    Err(NumericError::Underflow)
                } else {
                    Err(NumericError::InexactRescale)
                }
            } else {
                i64::try_from(value / divisor).map_err(|_| NumericError::Overflow)
            }
        }
    }
}

fn rescale_u64(
    atoms: u64,
    source: DecimalScale,
    destination: DecimalScale,
) -> Result<u64, NumericError> {
    match destination.exponent().cmp(&source.exponent()) {
        core::cmp::Ordering::Equal => Ok(atoms),
        core::cmp::Ordering::Greater => {
            let factor = power_of_ten(destination.exponent() - source.exponent());
            u64::try_from(u128::from(atoms) * factor).map_err(|_| NumericError::Overflow)
        }
        core::cmp::Ordering::Less => {
            let divisor = power_of_ten(source.exponent() - destination.exponent());
            let value = u128::from(atoms);
            if value % divisor != 0 {
                if value != 0 && value < divisor {
                    Err(NumericError::Underflow)
                } else {
                    Err(NumericError::InexactRescale)
                }
            } else {
                u64::try_from(value / divisor).map_err(|_| NumericError::Overflow)
            }
        }
    }
}
