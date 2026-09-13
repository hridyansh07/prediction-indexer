use replay_domain::{ConditionalMarketPrice, DecimalScale, NumericError, PositiveQty, Qty};
use serde_json::{Map, Value};

use crate::error::Reject;

pub(crate) trait CheckedValue {
    fn object(&self, code: &'static str) -> Result<&Map<String, Value>, Reject>;
    fn text(&self, code: &'static str) -> Result<&str, Reject>;
    fn nonempty_text(&self, code: &'static str) -> Result<&str, Reject>;
    fn price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str>;
    fn quantity(&self, scale: DecimalScale) -> Result<Qty, &'static str>;
    fn positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, &'static str>;
}

impl CheckedValue for Value {
    fn object(&self, code: &'static str) -> Result<&Map<String, Value>, Reject> {
        self.as_object().ok_or_else(|| Reject::new(code))
    }

    fn text(&self, code: &'static str) -> Result<&str, Reject> {
        self.as_str().ok_or_else(|| Reject::new(code))
    }

    fn nonempty_text(&self, code: &'static str) -> Result<&str, Reject> {
        self.text(code).and_then(|value| {
            if value.is_empty() || value.chars().any(char::is_control) {
                Err(Reject::new(code))
            } else {
                Ok(value)
            }
        })
    }

    fn price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str> {
        let text = self.as_str().ok_or("invalid_price")?;
        ConditionalMarketPrice::parse(text, scale).map_err(|error| numeric_code(error, "price"))
    }

    fn quantity(&self, scale: DecimalScale) -> Result<Qty, &'static str> {
        let text = self.as_str().ok_or("invalid_quantity")?;
        Qty::parse(text, scale).map_err(|error| numeric_code(error, "quantity"))
    }

    fn positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, &'static str> {
        PositiveQty::new(self.quantity(scale)?).map_err(|_| "zero_quantity")
    }
}

pub(crate) trait CheckedObject {
    fn required(&self, field: &str) -> Result<&Value, Reject>;
    fn fields(&self, allowed: &[&str], additive: bool) -> Result<(), Reject>;
}

impl CheckedObject for Map<String, Value> {
    fn required(&self, field: &str) -> Result<&Value, Reject> {
        self.get(field)
            .ok_or_else(|| Reject::new("missing_required_field"))
    }

    fn fields(&self, allowed: &[&str], additive: bool) -> Result<(), Reject> {
        if !additive && self.keys().any(|key| !allowed.contains(&key.as_str())) {
            Err(Reject::new("unknown_field"))
        } else {
            Ok(())
        }
    }
}

pub(crate) fn numeric_code(error: NumericError, field: &'static str) -> &'static str {
    match (error, field) {
        (NumericError::Overflow, "price") => "price_overflow",
        (NumericError::Underflow, "price") => "price_underflow",
        (NumericError::InexactRescale, "price") => "inexact_price",
        (NumericError::ConditionalPriceOutOfRange, "price") => "price_out_of_range",
        (NumericError::Overflow | NumericError::LogicalMaximumExceeded, "quantity") => {
            "quantity_overflow"
        }
        (NumericError::Underflow, "quantity") => "quantity_underflow",
        (NumericError::InexactRescale, "quantity") => "inexact_quantity",
        (_, "price") => "invalid_price",
        _ => "invalid_quantity",
    }
}
