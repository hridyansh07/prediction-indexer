use canonical_normalizer::{ConfigValue, NormalizerConfigIdentity};
use replay_domain::DecimalScale;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DEFAULT_PRICE_SCALE: u8 = 3;
pub const DEFAULT_QUANTITY_SCALE: u8 = 6;

/// Every runtime variable that can change normalized Limitless values. Full-book
/// replacement, YES orientation, and unsupported-event policy are bundle
/// semantics and therefore belong to `NORMALIZER_BUNDLE_ID`, not runtime config.
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
            schema_version: 1,
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
