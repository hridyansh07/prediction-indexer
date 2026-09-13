use canonical_normalizer::{ConfigValue, NormalizerConfigIdentity};
use replay_domain::DecimalScale;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DEFAULT_PRICE_SCALE: u8 = 4;
pub const DEFAULT_QUANTITY_SCALE: u8 = 2;

/// Runtime-controlled Kalshi normalization variables. The canonical serialized
/// structure is hashed directly; no hand-built config string exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub price_scale: DecimalScale,
    pub quantity_scale: DecimalScale,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            price_scale: DecimalScale::new(DEFAULT_PRICE_SCALE).expect("constant scale"),
            quantity_scale: DecimalScale::new(DEFAULT_QUANTITY_SCALE).expect("constant scale"),
        }
    }
}

impl Config {
    pub(crate) fn identity(self) -> NormalizerConfigIdentity {
        NormalizerConfigIdentity {
            schema_version: 2,
            variables: BTreeMap::from([
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
