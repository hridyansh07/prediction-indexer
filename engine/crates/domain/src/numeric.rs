use core::fmt;

use serde::{Deserialize, Serialize};

/// Maximum decimal exponent supported by the fixed-point boundary.
pub const MAX_DECIMAL_SCALE: u8 = 18;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumericError {
    Empty,
    InvalidSyntax,
    NegativePrice,
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
            Self::ScaleOutOfRange => "decimal scale exceeds 18",
            Self::Overflow => "fixed-point value overflows i64",
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

/// Unit of a price atom. Currency identity and conversion are intentionally not
/// part of S2; a segment manifest will bind the quote currency later.
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

/// Exact price in `10^-scale` quote units per contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Px {
    atoms: i64,
    scale: DecimalScale,
    unit: PriceUnit,
}

impl Px {
    pub fn parse(text: &str, scale: DecimalScale) -> Result<Self, NumericError> {
        let atoms = parse_decimal(text, scale, false)?;
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
        let atoms = rescale(self.atoms, self.scale, scale)?;
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

/// Exact signed quantity in `10^-scale` contracts.
///
/// Signed values exist because a relative level mutation may remove size. Full
/// books and absolute levels validate non-negative/positive quantities at their
/// own constructors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Qty {
    atoms: i64,
    scale: DecimalScale,
    unit: QuantityUnit,
}

impl Qty {
    pub fn parse(text: &str, scale: DecimalScale) -> Result<Self, NumericError> {
        Ok(Self {
            atoms: parse_decimal(text, scale, true)?,
            scale,
            unit: QuantityUnit::Contracts,
        })
    }

    pub const fn from_atoms(atoms: i64, scale: DecimalScale) -> Self {
        Self {
            atoms,
            scale,
            unit: QuantityUnit::Contracts,
        }
    }

    pub const fn atoms(self) -> i64 {
        self.atoms
    }

    pub const fn scale(self) -> DecimalScale {
        self.scale
    }

    pub const fn unit(self) -> QuantityUnit {
        self.unit
    }

    pub fn checked_rescale(self, scale: DecimalScale) -> Result<Self, NumericError> {
        Ok(Self::from_atoms(
            rescale(self.atoms, self.scale, scale)?,
            scale,
        ))
    }

    pub fn checked_add(self, other: Self) -> Result<Self, NumericError> {
        if self.scale != other.scale || self.unit != other.unit {
            return Err(NumericError::InexactRescale);
        }
        let atoms = self
            .atoms
            .checked_add(other.atoms)
            .ok_or(NumericError::Overflow)?;
        Ok(Self::from_atoms(atoms, self.scale))
    }
}

fn power_of_ten(exponent: u8) -> i128 {
    10_i128.pow(u32::from(exponent))
}

fn parse_digits(text: &str) -> Result<i128, NumericError> {
    text.bytes().try_fold(0_i128, |value, byte| {
        if !byte.is_ascii_digit() {
            return Err(NumericError::InvalidSyntax);
        }
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(byte - b'0')))
            .ok_or(NumericError::Overflow)
    })
}

fn parse_decimal(
    text: &str,
    scale: DecimalScale,
    allow_negative: bool,
) -> Result<i64, NumericError> {
    if text.is_empty() {
        return Err(NumericError::Empty);
    }
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) if allow_negative => (true, rest),
        Some(_) => return Err(NumericError::NegativePrice),
        None => (false, text),
    };
    if unsigned.is_empty() || unsigned.starts_with('+') {
        return Err(NumericError::InvalidSyntax);
    }
    let mut parts = unsigned.split('.');
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
    let magnitude = whole
        .checked_mul(power_of_ten(scale.exponent()))
        .and_then(|value| value.checked_add(fraction * power_of_ten(padding as u8)))
        .ok_or(NumericError::Overflow)?;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed).map_err(|_| NumericError::Overflow)
}

fn rescale(
    atoms: i64,
    source: DecimalScale,
    destination: DecimalScale,
) -> Result<i64, NumericError> {
    match destination.exponent().cmp(&source.exponent()) {
        core::cmp::Ordering::Equal => Ok(atoms),
        core::cmp::Ordering::Greater => {
            let factor = power_of_ten(destination.exponent() - source.exponent());
            i64::try_from(i128::from(atoms) * factor).map_err(|_| NumericError::Overflow)
        }
        core::cmp::Ordering::Less => {
            let divisor = power_of_ten(source.exponent() - destination.exponent());
            let value = i128::from(atoms);
            if value % divisor != 0 {
                if value != 0 && value.unsigned_abs() < divisor as u128 {
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
