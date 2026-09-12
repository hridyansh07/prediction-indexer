use replay_domain::{
    ConditionalMarketPrice, DecimalScale, LevelChange, NumericError, PositiveQty, Qty,
};
use serde_json::{Map, Value};

use crate::error::Reject;

pub(crate) trait CheckedValue {
    fn checked_object(&self, code: &'static str) -> Result<&Map<String, Value>, Reject>;
    fn checked_text(&self, code: &'static str) -> Result<&str, Reject>;
    fn checked_nonempty_text(&self, code: &'static str) -> Result<&str, Reject>;
    fn checked_i64(&self, code: &'static str) -> Result<i64, Reject>;
    fn checked_nonnegative_i64(&self, code: &'static str) -> Result<i64, Reject>;
    fn checked_nonnegative_u64(&self, code: &'static str) -> Result<u64, Reject>;
    fn checked_positive_u64(&self, code: &'static str) -> Result<u64, Reject>;
    fn checked_nonnegative_number(&self, code: &'static str) -> Result<f64, Reject>;
    fn checked_bool(&self, code: &'static str) -> Result<bool, Reject>;
    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, &'static str>;
    fn checked_quantity(&self, scale: DecimalScale) -> Result<Qty, &'static str>;
    fn checked_positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, &'static str>;
    fn checked_level_change(&self, scale: DecimalScale) -> Result<LevelChange, &'static str>;
}

impl CheckedValue for Value {
    fn checked_object(&self, code: &'static str) -> Result<&Map<String, Value>, Reject> {
        self.as_object().ok_or_else(|| Reject::new(code))
    }

    fn checked_text(&self, code: &'static str) -> Result<&str, Reject> {
        self.as_str().ok_or_else(|| Reject::new(code))
    }

    fn checked_nonempty_text(&self, code: &'static str) -> Result<&str, Reject> {
        self.checked_text(code).and_then(|value| {
            if value.is_empty() || value.chars().any(char::is_control) {
                Err(Reject::new(code))
            } else {
                Ok(value)
            }
        })
    }

    fn checked_i64(&self, code: &'static str) -> Result<i64, Reject> {
        self.as_i64().ok_or_else(|| Reject::new(code))
    }

    fn checked_nonnegative_i64(&self, code: &'static str) -> Result<i64, Reject> {
        self.checked_i64(code).and_then(|value| {
            if value < 0 {
                Err(Reject::new(code))
            } else {
                Ok(value)
            }
        })
    }

    fn checked_nonnegative_u64(&self, code: &'static str) -> Result<u64, Reject> {
        self.as_u64().ok_or_else(|| Reject::new(code))
    }

    fn checked_positive_u64(&self, code: &'static str) -> Result<u64, Reject> {
        self.as_u64()
            .filter(|value| *value > 0)
            .ok_or_else(|| Reject::new(code))
    }

    fn checked_nonnegative_number(&self, code: &'static str) -> Result<f64, Reject> {
        self.as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .ok_or_else(|| Reject::new(code))
    }

    fn checked_bool(&self, code: &'static str) -> Result<bool, Reject> {
        self.as_bool().ok_or_else(|| Reject::new(code))
    }

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
    fn checked_required_text(&self, field: &str) -> Result<String, &'static str>;
    fn checked_optional_text(&self, field: &str) -> Result<Option<String>, &'static str>;
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

    fn checked_required_text(&self, field: &str) -> Result<String, &'static str> {
        self.get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
            .map(str::to_owned)
            .ok_or("invalid_control_field")
    }

    fn checked_optional_text(&self, field: &str) -> Result<Option<String>, &'static str> {
        match self.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value))
                if !value.is_empty() && !value.chars().any(char::is_control) =>
            {
                Ok(Some(value.clone()))
            }
            _ => Err("invalid_control_field"),
        }
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
