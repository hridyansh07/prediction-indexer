use replay_domain::{
    ConditionalMarketPrice, DecimalScale, LevelChange, NumericError, PositiveQty, Qty,
};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedValueError {
    WrongType,
    EmptyOrControlText,
    Negative,
    Zero,
    NonFinite,
}

/// Structural JSON conversions shared by venue adapters. Venue code maps these
/// neutral boundary failures to its stable reject taxonomy.
pub trait CheckedValue {
    fn checked_object(&self) -> Result<&Map<String, Value>, CheckedValueError>;
    fn checked_text(&self) -> Result<&str, CheckedValueError>;
    fn checked_nonempty_text(&self) -> Result<&str, CheckedValueError>;
    fn checked_i64(&self) -> Result<i64, CheckedValueError>;
    fn checked_nonnegative_i64(&self) -> Result<i64, CheckedValueError>;
    fn checked_nonnegative_u64(&self) -> Result<u64, CheckedValueError>;
    fn checked_positive_u64(&self) -> Result<u64, CheckedValueError>;
    fn checked_nonnegative_number(&self) -> Result<f64, CheckedValueError>;
    fn checked_bool(&self) -> Result<bool, CheckedValueError>;
}

impl CheckedValue for Value {
    fn checked_object(&self) -> Result<&Map<String, Value>, CheckedValueError> {
        self.as_object().ok_or(CheckedValueError::WrongType)
    }

    fn checked_text(&self) -> Result<&str, CheckedValueError> {
        self.as_str().ok_or(CheckedValueError::WrongType)
    }

    fn checked_nonempty_text(&self) -> Result<&str, CheckedValueError> {
        self.checked_text().and_then(|value| {
            if value.is_empty() || value.chars().any(char::is_control) {
                Err(CheckedValueError::EmptyOrControlText)
            } else {
                Ok(value)
            }
        })
    }

    fn checked_i64(&self) -> Result<i64, CheckedValueError> {
        self.as_i64().ok_or(CheckedValueError::WrongType)
    }

    fn checked_nonnegative_i64(&self) -> Result<i64, CheckedValueError> {
        self.checked_i64().and_then(|value| {
            if value < 0 {
                Err(CheckedValueError::Negative)
            } else {
                Ok(value)
            }
        })
    }

    fn checked_nonnegative_u64(&self) -> Result<u64, CheckedValueError> {
        self.as_u64().ok_or(CheckedValueError::Negative)
    }

    fn checked_positive_u64(&self) -> Result<u64, CheckedValueError> {
        self.checked_nonnegative_u64().and_then(|value| {
            if value == 0 {
                Err(CheckedValueError::Zero)
            } else {
                Ok(value)
            }
        })
    }

    fn checked_nonnegative_number(&self) -> Result<f64, CheckedValueError> {
        self.as_f64()
            .ok_or(CheckedValueError::WrongType)
            .and_then(|value| {
                if !value.is_finite() {
                    Err(CheckedValueError::NonFinite)
                } else if value < 0.0 {
                    Err(CheckedValueError::Negative)
                } else {
                    Ok(value)
                }
            })
    }

    fn checked_bool(&self) -> Result<bool, CheckedValueError> {
        self.as_bool().ok_or(CheckedValueError::WrongType)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectError {
    UnknownField,
    MissingRequiredField,
}

pub trait CheckedObject {
    fn checked_fields(&self, allowed: &[&str]) -> Result<(), ObjectError>;
    fn checked_required(&self, field: &str) -> Result<&Value, ObjectError>;
}

impl CheckedObject for Map<String, Value> {
    fn checked_fields(&self, allowed: &[&str]) -> Result<(), ObjectError> {
        if self.keys().any(|key| !allowed.contains(&key.as_str())) {
            Err(ObjectError::UnknownField)
        } else {
            Ok(())
        }
    }

    fn checked_required(&self, field: &str) -> Result<&Value, ObjectError> {
        self.get(field).ok_or(ObjectError::MissingRequiredField)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecimalError {
    WrongType,
    Numeric(NumericError),
    ZeroQuantity,
    ZeroRelativeDelta,
}

pub trait CheckedDecimal {
    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, DecimalError>;
    fn checked_quantity(&self, scale: DecimalScale) -> Result<Qty, DecimalError>;
    fn checked_positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, DecimalError>;
    fn checked_level_change(&self, scale: DecimalScale) -> Result<LevelChange, DecimalError>;
}

impl CheckedDecimal for Value {
    fn checked_price(&self, scale: DecimalScale) -> Result<ConditionalMarketPrice, DecimalError> {
        let text = self.as_str().ok_or(DecimalError::WrongType)?;
        ConditionalMarketPrice::parse(text, scale).map_err(DecimalError::Numeric)
    }

    fn checked_quantity(&self, scale: DecimalScale) -> Result<Qty, DecimalError> {
        let text = self.as_str().ok_or(DecimalError::WrongType)?;
        Qty::parse(text, scale).map_err(DecimalError::Numeric)
    }

    fn checked_positive_quantity(&self, scale: DecimalScale) -> Result<PositiveQty, DecimalError> {
        PositiveQty::new(self.checked_quantity(scale)?).map_err(|_| DecimalError::ZeroQuantity)
    }

    fn checked_level_change(&self, scale: DecimalScale) -> Result<LevelChange, DecimalError> {
        let text = self.as_str().ok_or(DecimalError::WrongType)?;
        let (decrease, magnitude) = match text.strip_prefix('-') {
            Some(magnitude) => (true, magnitude),
            None => (false, text),
        };
        let quantity = Qty::parse(magnitude, scale).map_err(DecimalError::Numeric)?;
        let quantity = PositiveQty::new(quantity).map_err(|_| DecimalError::ZeroRelativeDelta)?;
        Ok(if decrease {
            LevelChange::Decrease(quantity)
        } else {
            LevelChange::Increase(quantity)
        })
    }
}

#[cfg(test)]
mod checked_value_tests {
    use serde_json::json;

    use super::{CheckedValue, CheckedValueError};

    #[test]
    fn integer_sign_constraints_are_enforced_at_the_shared_boundary() {
        assert_eq!(json!(0).checked_nonnegative_u64(), Ok(0));
        assert_eq!(json!(1).checked_positive_u64(), Ok(1));
        assert_eq!(
            json!(-1).checked_nonnegative_u64(),
            Err(CheckedValueError::Negative)
        );
        assert_eq!(
            json!(0).checked_positive_u64(),
            Err(CheckedValueError::Zero)
        );
    }

    #[test]
    fn checked_text_rejects_empty_and_control_characters() {
        assert_eq!(
            json!("").checked_nonempty_text(),
            Err(CheckedValueError::EmptyOrControlText)
        );
        assert_eq!(
            json!("bad\nvalue").checked_nonempty_text(),
            Err(CheckedValueError::EmptyOrControlText)
        );
        assert_eq!(
            json!("open-world-name").checked_nonempty_text(),
            Ok("open-world-name")
        );
    }
}

#[cfg(test)]
mod checked_decimal_tests {
    use replay_domain::{DecimalScale, LevelChange, NumericError};
    use serde_json::json;

    use super::{CheckedDecimal, DecimalError};

    #[test]
    fn level_change_consumes_only_one_leading_negative_sign() {
        let scale = DecimalScale::new(2).unwrap();
        assert!(matches!(
            json!("-1.25").checked_level_change(scale),
            Ok(LevelChange::Decrease(_))
        ));
        assert!(matches!(
            json!("1.25").checked_level_change(scale),
            Ok(LevelChange::Increase(_))
        ));
        assert_eq!(
            json!("--1.25").checked_level_change(scale),
            Err(DecimalError::Numeric(NumericError::NegativeQuantity))
        );
        assert_eq!(
            json!("-0").checked_level_change(scale),
            Err(DecimalError::ZeroRelativeDelta)
        );
    }

    #[test]
    fn decimal_helpers_preserve_exact_fixed_point_parsing() {
        let scale = DecimalScale::new(2).unwrap();
        assert!(json!("0.500").checked_price(scale).is_ok());
        assert_eq!(
            json!("0.501").checked_price(scale),
            Err(DecimalError::Numeric(NumericError::InexactRescale))
        );
        assert_eq!(
            json!("1.001").checked_quantity(scale),
            Err(DecimalError::Numeric(NumericError::InexactRescale))
        );
    }
}
