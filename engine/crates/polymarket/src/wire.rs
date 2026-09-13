//! Closed wire shapes. Additive vendor fields are projected out explicitly at
//! the adapter boundary; optional fields preserve absence and reject null.
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

pub(crate) const BOOK_FIELDS: &[&str] = &[
    "event_type",
    "market",
    "asset_id",
    "timestamp",
    "hash",
    "bids",
    "asks",
    "tick_size",
    "last_trade_price",
    "min_order_size",
    "neg_risk",
];
pub(crate) const REST_FIELDS: &[&str] = &[
    "market",
    "asset_id",
    "timestamp",
    "hash",
    "bids",
    "asks",
    "tick_size",
    "last_trade_price",
    "min_order_size",
    "neg_risk",
];
pub(crate) const CHANGE_FIELDS: &[&str] = &["event_type", "market", "price_changes", "timestamp"];
pub(crate) const CHILD_FIELDS: &[&str] = &[
    "asset_id", "price", "size", "side", "hash", "best_bid", "best_ask",
];
pub(crate) const TRADE_FIELDS: &[&str] = &[
    "event_type",
    "market",
    "asset_id",
    "price",
    "size",
    "fee_rate_bps",
    "side",
    "timestamp",
    "transaction_hash",
];

fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Level {
    price: String,
    size: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Book {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    event_type: Option<String>,
    market: String,
    asset_id: String,
    timestamp: String,
    bids: Vec<Level>,
    asks: Vec<Level>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    hash: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    tick_size: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    last_trade_price: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    min_order_size: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    neg_risk: Option<bool>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PriceChange {
    event_type: String,
    market: String,
    timestamp: String,
    price_changes: Vec<Change>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Change {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    hash: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    best_bid: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    best_ask: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Trade {
    event_type: String,
    market: String,
    asset_id: String,
    price: String,
    size: String,
    side: String,
    timestamp: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    fee_rate_bps: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    transaction_hash: Option<String>,
}

pub(crate) fn project(object: &Map<String, Value>, fields: &[&str], additive: bool) -> Value {
    let mut value = Value::Object(object.clone());
    if additive {
        retain(&mut value, fields);
        for field in ["bids", "asks", "price_changes"] {
            if let Some(Value::Array(rows)) = value.get_mut(field) {
                for row in rows {
                    retain(
                        row,
                        if field == "price_changes" {
                            CHILD_FIELDS
                        } else {
                            &["price", "size"]
                        },
                    );
                }
            }
        }
    }
    value
}

fn retain(value: &mut Value, fields: &[&str]) {
    if let Value::Object(object) = value {
        object.retain(|field, _| fields.contains(&field.as_str()));
    }
}
