use canonical_normalizer::{CheckedDecimal, CheckedObject as _, CheckedValue as _, DecimalError};
use replay_domain::{ConditionalMarketPrice, DecimalScale, NumericError, PositiveQty, Qty};
use serde_json::{Map, Value};

use super::error::Reject;

// Venue error taxonomy only; the shared normalizer owns all JSON and decimal
// validation. These methods preserve the adapter's established reject codes.
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
        self.checked_object().map_err(|_| Reject::new(code))
    }

    fn text(&self, code: &'static str) -> Result<&str, Reject> {
        self.checked_text().map_err(|_| Reject::new(code))
    }

    fn nonempty_text(&self, code: &'static str) -> Result<&str, Reject> {
        self.checked_nonempty_text().map_err(|_| Reject::new(code))
    }

    fn price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str> {
        self.checked_price(scale)
            .map_err(|error| decimal_code(error, "price"))
    }

    fn quantity(&self, scale: DecimalScale) -> Result<Qty, &'static str> {
        self.checked_quantity(scale)
            .map_err(|error| decimal_code(error, "quantity"))
    }

    fn positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, &'static str> {
        self.checked_positive_quantity(scale)
            .map_err(|error| decimal_code(error, "quantity"))
    }
}

pub(crate) trait CheckedObject {
    fn required(&self, field: &str) -> Result<&Value, Reject>;
    fn fields(&self, allowed: &[&str], additive: bool) -> Result<(), Reject>;
}

impl CheckedObject for Map<String, Value> {
    fn required(&self, field: &str) -> Result<&Value, Reject> {
        self.checked_required(field)
            .map_err(|_| Reject::new("missing_required_field"))
    }

    fn fields(&self, allowed: &[&str], additive: bool) -> Result<(), Reject> {
        if additive {
            Ok(())
        } else {
            self.checked_fields(allowed)
                .map_err(|_| Reject::new("unknown_field"))
        }
    }
}

fn decimal_code(error: DecimalError, field: &'static str) -> &'static str {
    match error {
        DecimalError::Numeric(error) => numeric_code(error, field),
        DecimalError::ZeroQuantity => "zero_quantity",
        DecimalError::ZeroRelativeDelta => unreachable!("Polymarket uses absolute quantities"),
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
        (NumericError::Overflow | NumericError::LogicalMaximumExceeded, "quantity") => {
            "quantity_overflow"
        }
        (NumericError::Underflow, "quantity") => "quantity_underflow",
        (NumericError::InexactRescale, "quantity") => "inexact_quantity",
        (_, "price") => "invalid_price",
        _ => "invalid_quantity",
    }
}
