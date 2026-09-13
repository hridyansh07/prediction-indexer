//! Validated venue events. Delivery and envelope checks belong in message.rs.
use replay_domain::{
    AuditAnchor, BookDelta, BookEvent, BookStateHash, ContractOrientation, FullBook, InstrumentId,
    Level, LevelChange, SegmentEvent, Sha1, Side, TradeEvent,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    Config,
    error::Reject,
    value::{CheckedObject, CheckedValue},
    wire,
};

const U256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";

pub(crate) struct Snapshot {
    event: FullBook,
}

impl Snapshot {
    pub(crate) fn parse(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
        let value = wire::project(object, wire::BOOK_FIELDS, config.accept_additive_fields);
        // Serde owns the closed shape; its error text is not the reject API.
        // Recover the established ordered diagnostic on shape failure only.
        match wire::Snapshot::deserialize(&value) {
            Ok(wire) => Self::try_from((wire, config)),
            Err(_) => match Self::validate(object, config) {
                Err(reject) => Err(reject),
                Ok(_) => panic!("Polymarket snapshot wire schema and validator disagree"),
            },
        }
    }

    // The message boundary also validates this prefix before checking the
    // cursor. Direct constructors repeat it and cannot bypass validation.
    pub(crate) fn prefix(
        object: &Map<String, Value>,
        config: Config,
    ) -> Result<(InstrumentId, u64), Reject> {
        object.fields(wire::BOOK_FIELDS, config.accept_additive_fields)?;
        expect_event_type(object, "book")?;
        book_identity(object)
    }

    fn validate(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
        let (instrument, timestamp_ms) = Self::prefix(object, config)?;
        optional_price(object.get("tick_size"), config, &instrument)?;
        optional_price(object.get("last_trade_price"), config, &instrument)?;
        optional_positive_quantity(object.get("min_order_size"), config, &instrument)?;
        if object
            .get("neg_risk")
            .is_some_and(|value| !value.is_boolean())
        {
            return Err(Reject::for_instrument("invalid_neg_risk", instrument));
        }
        let hash = object
            .get("hash")
            .map(state_hash)
            .transpose()
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        let bids = levels(object.required("bids")?, config, &instrument)?;
        let asks = levels(object.required("asks")?, config, &instrument)?;
        let observed_ns = timestamp_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| Reject::for_instrument("source_time_overflow", instrument.clone()))?;
        let event = FullBook::new(
            instrument.clone(),
            ContractOrientation::Outcome,
            bids,
            asks,
            hash,
            Some(observed_ns),
        )
        .map_err(|_| Reject::for_instrument("invalid_snapshot_levels", instrument))?;
        Ok(Self { event })
    }
}

impl TryFrom<(wire::Snapshot, Config)> for Snapshot {
    type Error = Reject;

    fn try_from((wire, config): (wire::Snapshot, Config)) -> Result<Self, Reject> {
        let value = serde_json::to_value(wire).expect("snapshot wire must serialize");
        Self::validate(value.as_object().expect("snapshot object"), config)
    }
}

impl From<Snapshot> for Vec<SegmentEvent> {
    fn from(snapshot: Snapshot) -> Self {
        vec![SegmentEvent::Book(BookEvent::Full(snapshot.event))]
    }
}

pub(crate) struct AuditSnapshot {
    event: AuditAnchor,
}

impl AuditSnapshot {
    pub(crate) fn parse(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
        let value = wire::project(object, wire::REST_FIELDS, config.accept_additive_fields);
        match wire::AuditSnapshot::deserialize(&value) {
            Ok(wire) => Self::try_from((wire, config)),
            Err(_) => match Self::validate(object, config) {
                Err(reject) => Err(reject),
                Ok(_) => panic!("Polymarket audit snapshot wire schema and validator disagree"),
            },
        }
    }

    pub(crate) fn prefix(
        object: &Map<String, Value>,
        config: Config,
    ) -> Result<(InstrumentId, u64), Reject> {
        object.fields(wire::REST_FIELDS, config.accept_additive_fields)?;
        book_identity(object)
    }

    fn validate(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
        let (instrument, timestamp_ms) = Self::prefix(object, config)?;
        object
            .required("min_order_size")?
            .positive_quantity(config.quantity_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        object
            .required("tick_size")?
            .price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        object
            .required("neg_risk")?
            .as_bool()
            .ok_or_else(|| Reject::for_instrument("invalid_neg_risk", instrument.clone()))?;
        object
            .required("last_trade_price")?
            .price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        let hash = state_hash(object.required("hash")?)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        let bids = levels(object.required("bids")?, config, &instrument)?;
        let asks = levels(object.required("asks")?, config, &instrument)?;
        let observed_ns = timestamp_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| Reject::for_instrument("source_time_overflow", instrument.clone()))?;
        let event = AuditAnchor::new(
            instrument.clone(),
            ContractOrientation::Outcome,
            bids,
            asks,
            hash,
            Some(observed_ns),
        )
        .map_err(|_| Reject::for_instrument("invalid_snapshot_levels", instrument))?;
        Ok(Self { event })
    }
}

impl TryFrom<(wire::AuditSnapshot, Config)> for AuditSnapshot {
    type Error = Reject;

    fn try_from((wire, config): (wire::AuditSnapshot, Config)) -> Result<Self, Reject> {
        let value = serde_json::to_value(wire).expect("audit snapshot wire must serialize");
        Self::validate(value.as_object().expect("audit snapshot object"), config)
    }
}

impl From<AuditSnapshot> for Vec<SegmentEvent> {
    fn from(snapshot: AuditSnapshot) -> Self {
        vec![SegmentEvent::AuditAnchor(snapshot.event)]
    }
}

fn book_identity(object: &Map<String, Value>) -> Result<(InstrumentId, u64), Reject> {
    let instrument = instrument(object.required("asset_id")?)?;
    validate_market(object.required("market")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    let timestamp_ms = timestamp(object.required("timestamp")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    Ok((instrument, timestamp_ms))
}

pub(crate) struct PriceChange {
    events: Vec<BookDelta>,
}

impl PriceChange {
    pub(crate) fn parse(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
        let value = wire::project(object, wire::CHANGE_FIELDS, config.accept_additive_fields);
        match wire::PriceChange::deserialize(&value) {
            Ok(wire) => Self::try_from((wire, config)),
            Err(_) => match Self::validate(object, config) {
                Err(reject) => Err(reject),
                Ok(_) => panic!("Polymarket price-change wire schema and validator disagree"),
            },
        }
    }

    fn validate(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
        object.fields(wire::CHANGE_FIELDS, config.accept_additive_fields)?;
        expect_event_type(object, "price_change")?;
        validate_market(object.required("market")?).map_err(Reject::new)?;
        timestamp(object.required("timestamp")?).map_err(Reject::new)?;
        let changes = object
            .required("price_changes")?
            .as_array()
            .ok_or_else(|| Reject::new("invalid_price_changes"))?;
        let mut events = Vec::with_capacity(changes.len());
        for value in changes {
            let change = value.object("price_change_not_object")?;
            change.fields(wire::CHILD_FIELDS, config.accept_additive_fields)?;
            let instrument = instrument(change.required("asset_id")?)?;
            let price = change
                .required("price")?
                .price(config.price_scale)
                .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
            let quantity = change
                .required("size")?
                .quantity(config.quantity_scale)
                .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
            let side = match change.required("side")?.text("invalid_side")? {
                "BUY" => Side::Bid,
                "SELL" => Side::Ask,
                _ => return Err(Reject::for_instrument("invalid_side", instrument)),
            };
            let hash = state_hash(change.required("hash")?)
                .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
            optional_price(change.get("best_bid"), config, &instrument)?;
            optional_price(change.get("best_ask"), config, &instrument)?;
            let level_change = if quantity.atoms() == 0 {
                LevelChange::Delete
            } else {
                LevelChange::Set(
                    replay_domain::PositiveQty::new(quantity)
                        .expect("nonzero quantity checked immediately above"),
                )
            };
            events.push(
                BookDelta::new(
                    instrument.clone(),
                    ContractOrientation::Outcome,
                    side,
                    price,
                    level_change,
                    Some(hash),
                )
                .map_err(|_| Reject::for_instrument("invalid_delta", instrument))?,
            );
        }
        Ok(Self { events })
    }
}

impl TryFrom<(wire::PriceChange, Config)> for PriceChange {
    type Error = Reject;

    fn try_from((wire, config): (wire::PriceChange, Config)) -> Result<Self, Reject> {
        let value = serde_json::to_value(wire).expect("price-change wire must serialize");
        Self::validate(value.as_object().expect("price-change object"), config)
    }
}

impl From<PriceChange> for Vec<SegmentEvent> {
    fn from(change: PriceChange) -> Self {
        change
            .events
            .into_iter()
            .map(|event| SegmentEvent::Book(BookEvent::Delta(event)))
            .collect()
    }
}

pub(crate) struct Trade {
    event: TradeEvent,
}

impl Trade {
    pub(crate) fn parse(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
        let value = wire::project(object, wire::TRADE_FIELDS, config.accept_additive_fields);
        match wire::Trade::deserialize(&value) {
            Ok(wire) => Self::try_from((wire, config)),
            Err(_) => match Self::validate(object, config) {
                Err(reject) => Err(reject),
                Ok(_) => panic!("Polymarket trade wire schema and validator disagree"),
            },
        }
    }

    fn validate(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
        object.fields(wire::TRADE_FIELDS, config.accept_additive_fields)?;
        expect_event_type(object, "last_trade_price")?;
        let instrument = instrument(object.required("asset_id")?)?;
        validate_market(object.required("market")?)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        timestamp(object.required("timestamp")?)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        optional_unsigned_decimal(object.get("fee_rate_bps"), "invalid_fee_rate")?;
        optional_hash256(object.get("transaction_hash"), "invalid_transaction_hash")?;
        let aggressor = match object.required("side")?.text("invalid_side")? {
            "BUY" => Side::Bid,
            "SELL" => Side::Ask,
            _ => return Err(Reject::for_instrument("invalid_side", instrument)),
        };
        let price = object
            .required("price")?
            .price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        let quantity = object
            .required("size")?
            .positive_quantity(config.quantity_scale)
            .map_err(|code| {
                Reject::for_instrument(
                    if code == "zero_quantity" {
                        "non_positive_trade_quantity"
                    } else {
                        code
                    },
                    instrument.clone(),
                )
            })?;
        Ok(Self {
            event: TradeEvent::new(
                instrument,
                ContractOrientation::Outcome,
                price,
                quantity,
                Some(aggressor),
            ),
        })
    }
}

impl TryFrom<(wire::Trade, Config)> for Trade {
    type Error = Reject;

    fn try_from((wire, config): (wire::Trade, Config)) -> Result<Self, Reject> {
        let value = serde_json::to_value(wire).expect("trade wire must serialize");
        Self::validate(value.as_object().expect("trade object"), config)
    }
}

impl From<Trade> for Vec<SegmentEvent> {
    fn from(trade: Trade) -> Self {
        vec![SegmentEvent::Trade(trade.event)]
    }
}

fn expect_event_type(object: &Map<String, Value>, expected: &str) -> Result<(), Reject> {
    if object
        .required("event_type")?
        .text("invalid_message_type")?
        != expected
    {
        Err(Reject::new("invalid_message_type"))
    } else {
        Ok(())
    }
}

fn levels(value: &Value, config: Config, instrument: &InstrumentId) -> Result<Vec<Level>, Reject> {
    let values = value
        .as_array()
        .ok_or_else(|| Reject::for_instrument("invalid_snapshot_levels", instrument.clone()))?;
    values
        .iter()
        .map(|value| {
            let object = value.object("invalid_snapshot_level")?;
            object.fields(&["price", "size"], config.accept_additive_fields)?;
            let price = object
                .required("price")?
                .price(config.price_scale)
                .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
            let quantity = object
                .required("size")?
                .positive_quantity(config.quantity_scale)
                .map_err(|code| {
                    Reject::for_instrument(
                        if code == "zero_quantity" {
                            "non_positive_snapshot_quantity"
                        } else {
                            code
                        },
                        instrument.clone(),
                    )
                })?;
            Ok(Level::new(price, quantity))
        })
        .collect()
}

pub(crate) fn instrument(value: &Value) -> Result<InstrumentId, Reject> {
    let asset = value.nonempty_text("invalid_asset_id")?;
    if asset.len() > U256_MAX.len()
        || !asset.bytes().all(|byte| byte.is_ascii_digit())
        || (asset.len() > 1 && asset.starts_with('0'))
        || (asset.len() == U256_MAX.len() && asset > U256_MAX)
    {
        return Err(Reject::new("invalid_asset_id"));
    }
    InstrumentId::new(format!("polymarket:{asset}")).map_err(|_| Reject::new("invalid_asset_id"))
}

fn state_hash(value: &Value) -> Result<BookStateHash, &'static str> {
    let text = value.as_str().ok_or("invalid_state_sha1")?;
    Sha1::from_hex(text)
        .map(BookStateHash::Sha1)
        .map_err(|_| "invalid_state_sha1")
}

pub(crate) fn validate_market(value: &Value) -> Result<(), &'static str> {
    let text = value.as_str().ok_or("invalid_market_id")?;
    if text.len() == 66
        && text.starts_with("0x")
        && text[2..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err("invalid_market_id")
    }
}

pub(crate) fn timestamp(value: &Value) -> Result<u64, &'static str> {
    let text = value.as_str().ok_or("invalid_source_time")?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid_source_time");
    }
    text.parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or("invalid_source_time")
}

fn optional_price(
    value: Option<&Value>,
    config: Config,
    instrument: &InstrumentId,
) -> Result<(), Reject> {
    if let Some(value) = value {
        value
            .price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    }
    Ok(())
}

fn optional_positive_quantity(
    value: Option<&Value>,
    config: Config,
    instrument: &InstrumentId,
) -> Result<(), Reject> {
    if let Some(value) = value {
        value
            .positive_quantity(config.quantity_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    }
    Ok(())
}

fn optional_unsigned_decimal(value: Option<&Value>, code: &'static str) -> Result<(), Reject> {
    let Some(value) = value else {
        return Ok(());
    };
    let text = value.as_str().ok_or_else(|| Reject::new(code))?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Reject::new(code));
    }
    Ok(())
}

fn optional_hash256(value: Option<&Value>, code: &'static str) -> Result<(), Reject> {
    let Some(value) = value else {
        return Ok(());
    };
    let text = value.as_str().ok_or_else(|| Reject::new(code))?;
    let digest = text.strip_prefix("0x").unwrap_or(text);
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Reject::new(code));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn book() -> Value {
        json!({"event_type":"book", "market":format!("0x{}", "a".repeat(64)),
            "asset_id":"17", "timestamp":"123", "bids":[
                {"price":"0.21", "size":"3.7"}, {"price":"0.63", "size":"1.9"}],
            "asks":[{"price":"0.87", "size":"4.1"}, {"price":"0.72", "size":"2.3"}]})
    }

    fn typed_book(value: &Value, audit: bool) -> Result<Vec<SegmentEvent>, Reject> {
        if audit {
            AuditSnapshot::try_from((
                wire::AuditSnapshot::deserialize(value).unwrap(),
                Config::default(),
            ))
            .map(Into::into)
        } else {
            Snapshot::try_from((
                wire::Snapshot::deserialize(value).unwrap(),
                Config::default(),
            ))
            .map(Into::into)
        }
    }

    #[test]
    fn typed_books_finish_domain_validation_before_conversion() {
        for audit in [false, true] {
            let mut value = book();
            if audit {
                value.as_object_mut().unwrap().remove("event_type");
                value["hash"] = json!("a".repeat(40));
                value["min_order_size"] = json!("1");
                value["tick_size"] = json!("0.01");
                value["last_trade_price"] = json!("0.41");
                value["neg_risk"] = json!(false);
            }
            let events = typed_book(&value, audit).unwrap();
            let (bids, asks, time) = match &events[0] {
                SegmentEvent::Book(BookEvent::Full(book)) if !audit => {
                    (book.bids(), book.asks(), book.source_observed_ns())
                }
                SegmentEvent::AuditAnchor(book) if audit => {
                    (book.bids(), book.asks(), book.source_observed_ns())
                }
                other => panic!("wrong book semantics: {other:?}"),
            };
            assert_eq!(
                bids.iter().map(|l| l.price().atoms()).collect::<Vec<_>>(),
                [6300, 2100]
            );
            assert_eq!(
                asks.iter().map(|l| l.price().atoms()).collect::<Vec<_>>(),
                [7200, 8700]
            );
            assert_eq!(bids[0].quantity().atoms(), 1_900_000);
            assert_eq!(time, Some(123_000_000));
            value["bids"][1]["price"] = json!("0.2100");
            assert_eq!(
                typed_book(&value, audit).unwrap_err().code,
                "invalid_snapshot_levels"
            );
            // Malformed asks precede domain-level duplicate bid validation.
            value["asks"][0]["size"] = json!("bad");
            assert_eq!(
                typed_book(&value, audit).unwrap_err().code,
                "invalid_quantity"
            );
        }
    }

    #[test]
    fn typed_delta_and_trade_constructors_enforce_semantics() {
        let mut value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/price_change.json")).unwrap();
        let wire = wire::PriceChange::deserialize(&value).unwrap();
        let events: Vec<SegmentEvent> = PriceChange::try_from((wire, Config::default()))
            .unwrap()
            .into();
        assert_eq!(events.len(), 2);
        let SegmentEvent::Book(BookEvent::Delta(first)) = &events[0] else {
            panic!("delta")
        };
        assert_eq!(first.side(), Side::Bid);
        assert!(matches!(first.change(), LevelChange::Set(q) if q.atoms() == 12_366_000_000));
        let SegmentEvent::Book(BookEvent::Delta(second)) = &events[1] else {
            panic!("delta")
        };
        assert_eq!(second.side(), Side::Ask);
        assert_eq!(second.change(), LevelChange::Delete);
        value["price_changes"][1]["size"] = json!("-1");
        let wire = wire::PriceChange::deserialize(&value).unwrap();
        assert_eq!(
            PriceChange::try_from((wire, Config::default()))
                .err()
                .unwrap()
                .code,
            "invalid_quantity"
        );

        let mut value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/last_trade_price.json")).unwrap();
        let wire = wire::Trade::deserialize(&value).unwrap();
        let events: Vec<SegmentEvent> = Trade::try_from((wire, Config::default())).unwrap().into();
        assert!(
            matches!(&events[0], SegmentEvent::Trade(trade) if trade.price().atoms() == 4700 && trade.quantity().atoms() == 5_000_000 && trade.aggressor() == Some(Side::Ask))
        );
        value["size"] = json!("0");
        let wire = wire::Trade::deserialize(&value).unwrap();
        assert_eq!(
            Trade::try_from((wire, Config::default()))
                .err()
                .unwrap()
                .code,
            "non_positive_trade_quantity"
        );
    }

    #[test]
    fn closed_wire_preserves_omission_and_rejects_unknown_or_null_fields() {
        let value = book();
        let wire = wire::Snapshot::deserialize(&value).unwrap();
        let roundtrip = serde_json::to_value(wire).unwrap();
        for optional in [
            "hash",
            "tick_size",
            "min_order_size",
            "last_trade_price",
            "neg_risk",
        ] {
            assert!(roundtrip.get(optional).is_none());
            let mut invalid = value.clone();
            invalid[optional] = Value::Null;
            assert!(wire::Snapshot::deserialize(&invalid).is_err());
        }
        let mut unknown = value.clone();
        unknown["future"] = json!(true);
        assert!(wire::Snapshot::deserialize(&unknown).is_err());
        let mut nested = value;
        nested["bids"][0]["future"] = json!(true);
        assert!(wire::Snapshot::deserialize(&nested).is_err());
    }

    #[test]
    fn rest_constructor_requires_independent_evidence_and_time_conversion() {
        let rest: Value =
            serde_json::from_str(include_str!("../tests/fixtures/rest_book.json")).unwrap();
        for field in [
            "hash",
            "tick_size",
            "min_order_size",
            "last_trade_price",
            "neg_risk",
        ] {
            let mut value = rest.clone();
            value.as_object_mut().unwrap().remove(field);
            assert!(wire::AuditSnapshot::deserialize(&value).is_err());
            assert_eq!(
                AuditSnapshot::parse(value.as_object().unwrap(), Config::default())
                    .err()
                    .unwrap()
                    .code,
                "missing_required_field"
            );
        }
        let mut invalid = rest.clone();
        invalid["hash"] = json!("bad");
        assert_eq!(
            typed_book(&invalid, true).unwrap_err().code,
            "invalid_state_sha1"
        );
        for (mut value, audit) in [(book(), false), (rest, true)] {
            value["timestamp"] = json!(u64::MAX.to_string());
            assert_eq!(
                typed_book(&value, audit).unwrap_err().code,
                "source_time_overflow"
            );
        }
    }
}
