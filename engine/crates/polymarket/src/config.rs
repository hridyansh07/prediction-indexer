use std::collections::BTreeMap;

use canonical_normalizer::{ConfigValue, NormalizerConfigIdentity};
use replay_domain::DecimalScale;
use serde::{Deserialize, Serialize};

pub const DEFAULT_PRICE_SCALE: u8 = 4;
pub const DEFAULT_QUANTITY_SCALE: u8 = 6;
pub const DEFAULT_ACCEPT_ADDITIVE_FIELDS: bool = true;

/// Every runtime choice that can change normalized output or rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub price_scale: DecimalScale,
    pub quantity_scale: DecimalScale,
    /// Polymarket's public schemas and non-exhaustive SDK types permit additive
    /// object fields. Known fields remain strictly typed in either mode.
    pub accept_additive_fields: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            price_scale: DecimalScale::new(DEFAULT_PRICE_SCALE).expect("constant scale"),
            quantity_scale: DecimalScale::new(DEFAULT_QUANTITY_SCALE).expect("constant scale"),
            accept_additive_fields: DEFAULT_ACCEPT_ADDITIVE_FIELDS,
        }
    }
}

impl Config {
    pub(crate) fn identity(self) -> NormalizerConfigIdentity {
        NormalizerConfigIdentity {
            schema_version: 1,
            variables: BTreeMap::from([
                (
                    "accept_additive_fields".to_owned(),
                    ConfigValue::Boolean(self.accept_additive_fields),
                ),
                (
                    "price_scale".to_owned(),
                    ConfigValue::Unsigned(u64::from(self.price_scale.exponent())),
                ),
                (
                    "quantity_scale".to_owned(),
                    ConfigValue::Unsigned(u64::from(self.quantity_scale.exponent())),
                ),
            ]),
        }
    }
}
