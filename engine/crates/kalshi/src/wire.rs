//! Closed, unnormalized payload schemas. Optional means absent, never JSON null.
use serde::{Deserialize, Deserializer, Serialize};

fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum Snapshot {
    Legacy(LegacySnapshot),
    FixedPoint(FixedPointSnapshot),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacySnapshot {
    market_ticker: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    market_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    yes: Option<Vec<(i64, i64)>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    no: Option<Vec<(i64, i64)>>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixedPointSnapshot {
    market_ticker: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    market_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    yes_dollars_fp: Option<Vec<(String, String)>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    no_dollars_fp: Option<Vec<(String, String)>>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RelativeDelta {
    market_ticker: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    market_id: Option<String>,
    price_dollars: String,
    delta_fp: String,
    side: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    client_order_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    subaccount: Option<u64>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    ts: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    ts_ms: Option<u64>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Trade {
    trade_id: String,
    market_ticker: String,
    yes_price_dollars: String,
    no_price_dollars: String,
    count_fp: String,
    taker_side: String,
    taker_outcome_side: String,
    taker_book_side: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    is_block_trade: Option<bool>,
    ts: u64,
    ts_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn snapshot_variants_are_closed_and_primitive_typed() {
        for value in [
            json!({"market_ticker":"X", "yes":[], "no_dollars_fp":[]}),
            json!({"market_ticker":"X", "yes":null}),
            json!({"market_ticker":"X", "yes":[["51",1]]}),
            json!({"market_ticker":"X", "yes_dollars_fp":[[0.51,"1"]]}),
            json!({"market_ticker":"X", "future":true}),
            json!({"yes":[]}),
        ] {
            assert!(Snapshot::deserialize(&value).is_err(), "{value}");
        }
    }

    #[test]
    fn optional_flag_does_not_accept_null_or_coerce_other_primitives() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/trade.json")).unwrap();
        for invalid in [Value::Null, json!(0), json!("false"), json!([])] {
            let mut value = fixture["msg"].clone();
            value["is_block_trade"] = invalid;
            assert!(Trade::deserialize(&value).is_err());
        }
        for flag in [false, true] {
            let mut value = fixture["msg"].clone();
            value["is_block_trade"] = json!(flag);
            let wire = Trade::deserialize(&value).unwrap();
            assert_eq!(wire.is_block_trade, Some(flag));
        }
    }
}
