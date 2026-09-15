use canonical_normalizer::{CheckedDecimal, DecimalError};
use replay_domain::{
    ConditionalMarketPrice, DecimalScale, LevelChange, NumericError, PositiveQty, Qty,
};
use serde_json::Value;

pub(crate) use canonical_normalizer::CheckedObject;

pub(crate) trait CheckedKalshiValue {
    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str>;
    fn checked_quantity(&self, scale: DecimalScale) -> Result<Qty, &'static str>;
    fn checked_positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, &'static str>;
    fn checked_level_change(&self, scale: DecimalScale) -> Result<LevelChange, &'static str>;
}

impl CheckedKalshiValue for Value {
    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str> {
        CheckedDecimal::checked_price(self, scale).map_err(|error| decimal_code(error, "price"))
    }

    fn checked_quantity(&self, scale: DecimalScale) -> Result<Qty, &'static str> {
        CheckedDecimal::checked_quantity(self, scale)
            .map_err(|error| decimal_code(error, "quantity"))
    }

    fn checked_positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, &'static str> {
        CheckedDecimal::checked_positive_quantity(self, scale)
            .map_err(|error| decimal_code(error, "quantity"))
    }

    fn checked_level_change(&self, scale: DecimalScale) -> Result<LevelChange, &'static str> {
        CheckedDecimal::checked_level_change(self, scale)
            .map_err(|error| decimal_code(error, "quantity"))
    }
}

fn decimal_code(error: DecimalError, field: &'static str) -> &'static str {
    match error {
        DecimalError::Numeric(error) => numeric_code(error, field),
        DecimalError::ZeroQuantity => "zero_quantity",
        DecimalError::ZeroRelativeDelta => "zero_relative_delta",
        DecimalError::WrongType if field == "price" => "invalid_price",
        DecimalError::WrongType => "invalid_quantity",
    }
}

pub(crate) fn numeric_code(error: NumericError, field: &'static str) -> &'static str {
    match (error, field) {
        (NumericError::Overflow, "price") => "price_overflow",
        (NumericError::Underflow, "price") => "price_underflow",
        (NumericError::InexactRescale, "price") => "inexact_price",
        (NumericError::ConditionalPriceOutOfRange, "price") => "price_out_of_range",
        (NumericError::Overflow, "quantity") => "quantity_overflow",
        (NumericError::LogicalMaximumExceeded, "quantity") => "quantity_overflow",
        (NumericError::Underflow, "quantity") => "quantity_underflow",
        (NumericError::InexactRescale, "quantity") => "inexact_quantity",
        (_, "price") => "invalid_price",
        _ => "invalid_quantity",
    }
}
