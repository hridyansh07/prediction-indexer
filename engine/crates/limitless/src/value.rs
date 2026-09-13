use canonical_normalizer::{CheckedDecimal, DecimalError};
use replay_domain::{ConditionalMarketPrice, DecimalScale, NumericError, PositiveQty, Qty};
use serde_json::{Map, Number, Value};

use crate::error::Reject;

pub(crate) use canonical_normalizer::CheckedObject;

// Only Limitless's stable error mapping belongs here, not JSON validation.
pub(crate) trait CheckedLimitlessValue {
    fn checked_object(&self, code: &'static str) -> Result<&Map<String, Value>, Reject>;
    fn checked_nonempty_text(&self, code: &'static str) -> Result<&str, Reject>;
    fn checked_u64(&self, code: &'static str) -> Result<u64, Reject>;
    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str>;
}

impl CheckedLimitlessValue for Value {
    fn checked_object(&self, code: &'static str) -> Result<&Map<String, Value>, Reject> {
        canonical_normalizer::CheckedValue::checked_object(self).map_err(|_| Reject::new(code))
    }

    fn checked_nonempty_text(&self, code: &'static str) -> Result<&str, Reject> {
        canonical_normalizer::CheckedValue::checked_nonempty_text(self)
            .map_err(|_| Reject::new(code))
    }

    fn checked_u64(&self, code: &'static str) -> Result<u64, Reject> {
        canonical_normalizer::CheckedValue::checked_nonnegative_u64(self)
            .map_err(|_| Reject::new(code))
    }

    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str> {
        let number = self.as_number().ok_or("invalid_price")?;
        parse_price_number(number, scale)
    }
}

pub(crate) fn quantity_from_raw_atoms(
    value: &Value,
    output_scale: DecimalScale,
) -> Result<PositiveQty, &'static str> {
    let atoms = value.as_u64().ok_or("invalid_quantity")?;
    let wire_scale = DecimalScale::new(6).expect("constant scale");
    Qty::from_atoms(atoms, wire_scale)
        .and_then(PositiveQty::new)
        .and_then(|quantity| quantity.checked_rescale(output_scale))
        .map_err(|error| numeric_code(error, "quantity"))
}

pub(crate) fn validate_derived_price(value: &Value) -> Result<(), &'static str> {
    let scale = DecimalScale::new(18).expect("constant scale");
    CheckedLimitlessValue::checked_price(value, scale).map(|_| ())
}

pub(crate) fn validate_decimal_text_price(
    value: &Value,
    scale: DecimalScale,
) -> Result<(), &'static str> {
    CheckedDecimal::checked_price(value, scale)
        .map(|_| ())
        .map_err(|error| match error {
            DecimalError::Numeric(error) => numeric_code(error, "price"),
            _ => "invalid_price",
        })
}

fn parse_price_number(
    value: &Number,
    scale: DecimalScale,
) -> Result<ConditionalMarketPrice, &'static str> {
    // `engine` enables serde_json's arbitrary-precision representation so this
    // is the captured decimal lexeme, never an f64 round-trip.
    ConditionalMarketPrice::parse(value.as_str(), scale)
        .map_err(|error| numeric_code(error, "price"))
}

pub(crate) fn numeric_code(error: NumericError, field: &'static str) -> &'static str {
    match (error, field) {
        (NumericError::Overflow, "price") => "price_overflow",
        (NumericError::Underflow, "price") => "price_underflow",
        (NumericError::InexactRescale, "price") => "inexact_price",
        (NumericError::ConditionalPriceOutOfRange, "price") => "price_out_of_range",
        (NumericError::Overflow, "quantity")
        | (NumericError::LogicalMaximumExceeded, "quantity") => "quantity_overflow",
        (NumericError::Underflow, "quantity") => "quantity_underflow",
        (NumericError::InexactRescale, "quantity") => "inexact_quantity",
        (NumericError::ZeroQuantity, "quantity") => "zero_quantity",
        (_, "price") => "invalid_price",
        _ => "invalid_quantity",
    }
}
