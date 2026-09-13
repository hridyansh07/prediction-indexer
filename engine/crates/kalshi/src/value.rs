use replay_domain::{
    ConditionalMarketPrice, DecimalScale, LevelChange, NumericError, PositiveQty, Qty,
};
use serde_json::{Map, Value};

use crate::error::Reject;

pub(crate) trait CheckedKalshiValue {
    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str>;
    fn checked_quantity(&self, scale: DecimalScale) -> Result<Qty, &'static str>;
    fn checked_positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, &'static str>;
    fn checked_level_change(&self, scale: DecimalScale) -> Result<LevelChange, &'static str>;
}

impl CheckedKalshiValue for Value {
    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str> {
        let text = self.as_str().ok_or("invalid_price")?;
        ConditionalMarketPrice::parse(text, scale).map_err(|error| numeric_code(error, "price"))
    }

    fn checked_quantity(&self, scale: DecimalScale) -> Result<Qty, &'static str> {
        let text = self.as_str().ok_or("invalid_quantity")?;
        Qty::parse(text, scale).map_err(|error| numeric_code(error, "quantity"))
    }

    fn checked_positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, &'static str> {
        PositiveQty::new(self.checked_quantity(scale)?).map_err(|_| "zero_quantity")
    }

    fn checked_level_change(&self, scale: DecimalScale) -> Result<LevelChange, &'static str> {
        let text = self.as_str().ok_or("invalid_quantity")?;
        let (decrease, magnitude) = match text.strip_prefix('-') {
            Some(magnitude) => (true, magnitude),
            None => (false, text),
        };
        let quantity =
            Qty::parse(magnitude, scale).map_err(|error| numeric_code(error, "quantity"))?;
        let quantity = PositiveQty::new(quantity).map_err(|_| "zero_relative_delta")?;
        Ok(if decrease {
            LevelChange::Decrease(quantity)
        } else {
            LevelChange::Increase(quantity)
        })
    }
}

pub(crate) trait CheckedObject {
    fn checked_fields(&self, allowed: &[&str]) -> Result<(), Reject>;
    fn checked_required(&self, field: &str) -> Result<&Value, Reject>;
}

impl CheckedObject for Map<String, Value> {
    fn checked_fields(&self, allowed: &[&str]) -> Result<(), Reject> {
        if self.keys().any(|key| !allowed.contains(&key.as_str())) {
            Err(Reject::new("unknown_field"))
        } else {
            Ok(())
        }
    }

    fn checked_required(&self, field: &str) -> Result<&Value, Reject> {
        self.get(field)
            .ok_or_else(|| Reject::new("missing_required_field"))
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
