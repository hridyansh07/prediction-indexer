use canonical_normalizer::CheckedValue;
use replay_domain::{
    BookDelta, BookEvent, ConditionalMarketPrice, ContractOrientation, DecimalScale, FullBook,
    InstrumentId, Level, PositiveQty, Px, Qty, SegmentEvent, Side, TradeEvent,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    Config,
    error::Reject,
    value::{CheckedKalshiValue, CheckedObject, numeric_code},
    wire,
};

pub(crate) struct Snapshot {
    outcome: FullBook,
    complement: FullBook,
}

impl Snapshot {
    pub(crate) fn parse(value: &Value, config: Config) -> Result<Self, Reject> {
        // Strict serde owns the wire shape, but its diagnostics are deliberately
        // not public API. On shape failure, the ordered validator is run only to
        // recover the established Reject; success would prove an internal bug.
        match wire::Snapshot::deserialize(value) {
            Ok(wire) => Self::try_from((wire, config)),
            Err(_) => match Self::validate(value, config) {
                Err(reject) => Err(reject),
                Ok(_) => panic!("Kalshi snapshot wire schema and validator disagree"),
            },
        }
    }

    fn validate(value: &Value, config: Config) -> Result<Self, Reject> {
        let msg = value
            .checked_object()
            .map_err(|_| Reject::new("invalid_snapshot_msg"))?;
        let modern = msg.contains_key("yes_dollars_fp") || msg.contains_key("no_dollars_fp");
        let legacy = msg.contains_key("yes") || msg.contains_key("no");
        if modern && legacy {
            return Err(Reject::new("mixed_snapshot_schema"));
        }
        let instrument = instrument(msg)?;
        if modern {
            msg.checked_fields(&[
                "market_ticker",
                "market_id",
                "yes_dollars_fp",
                "no_dollars_fp",
            ])?;
        } else {
            msg.checked_fields(&["market_ticker", "market_id", "yes", "no"])?;
        }
        optional_nonempty_text(msg.get("market_id"), "invalid_market_id")?;
        let (yes, no) = if modern {
            (
                levels(msg.get("yes_dollars_fp"), config, false, &instrument)?,
                levels(msg.get("no_dollars_fp"), config, false, &instrument)?,
            )
        } else {
            (
                levels(msg.get("yes"), config, true, &instrument)?,
                levels(msg.get("no"), config, true, &instrument)?,
            )
        };

        // Parse both sides before constructing either book: malformed `no` levels
        // must retain precedence over semantic failures while constructing `yes`.
        let outcome = FullBook::new(
            instrument.clone(),
            ContractOrientation::Outcome,
            yes,
            Vec::new(),
            None,
            None,
        )
        .map_err(|_| Reject::for_instrument("invalid_snapshot_levels", instrument.clone()))?;
        let complement = FullBook::new(
            instrument.clone(),
            ContractOrientation::Complement,
            no,
            Vec::new(),
            None,
            None,
        )
        .map_err(|_| Reject::for_instrument("invalid_snapshot_levels", instrument))?;
        Ok(Self {
            outcome,
            complement,
        })
    }
}

impl TryFrom<(wire::Snapshot, Config)> for Snapshot {
    type Error = Reject;

    fn try_from((wire, config): (wire::Snapshot, Config)) -> Result<Self, Self::Error> {
        // This intentional roundtrip gives typed callers exactly the same ordered
        // semantic validator and stable rejection taxonomy as JSON callers.
        let value = serde_json::to_value(wire).expect("Kalshi snapshot wire must serialize");
        Self::validate(&value, config)
    }
}

impl From<Snapshot> for Vec<SegmentEvent> {
    fn from(snapshot: Snapshot) -> Self {
        vec![
            SegmentEvent::Book(BookEvent::Full(snapshot.outcome)),
            SegmentEvent::Book(BookEvent::Full(snapshot.complement)),
        ]
    }
}

pub(crate) struct RelativeDelta {
    event: BookDelta,
}

impl RelativeDelta {
    pub(crate) fn parse(value: &Value, config: Config) -> Result<Self, Reject> {
        match wire::RelativeDelta::deserialize(value) {
            Ok(wire) => Self::try_from((wire, config)),
            Err(_) => match Self::validate(value, config) {
                Err(reject) => Err(reject),
                Ok(_) => panic!("Kalshi delta wire schema and validator disagree"),
            },
        }
    }

    fn validate(value: &Value, config: Config) -> Result<Self, Reject> {
        let msg = value
            .checked_object()
            .map_err(|_| Reject::new("invalid_delta_msg"))?;
        msg.checked_fields(&[
            "market_ticker",
            "market_id",
            "price_dollars",
            "delta_fp",
            "side",
            "client_order_id",
            "subaccount",
            "ts",
            "ts_ms",
        ])?;
        let instrument = instrument(msg)?;
        optional_nonempty_text(msg.get("market_id"), "invalid_market_id")?;
        optional_nonempty_text(msg.get("client_order_id"), "invalid_client_order_id")?;
        optional_nonnegative_u64(msg.get("subaccount"), "invalid_subaccount")?;
        optional_nonempty_text(msg.get("ts"), "invalid_source_time")?;
        optional_positive_u64(msg.get("ts_ms"), "invalid_source_time")?;
        let orientation = match msg
            .checked_required("side")?
            .checked_text()
            .map_err(|_| Reject::new("invalid_side"))?
        {
            "yes" => ContractOrientation::Outcome,
            "no" => ContractOrientation::Complement,
            _ => return Err(Reject::for_instrument("invalid_side", instrument)),
        };
        let price = msg
            .checked_required("price_dollars")?
            .checked_price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        let change = msg
            .checked_required("delta_fp")?
            .checked_level_change(config.quantity_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        let event = BookDelta::new(
            instrument.clone(),
            orientation,
            Side::Bid,
            price,
            change,
            None,
        )
        .map_err(|_| Reject::for_instrument("invalid_delta", instrument))?;
        Ok(Self { event })
    }
}

impl TryFrom<(wire::RelativeDelta, Config)> for RelativeDelta {
    type Error = Reject;

    fn try_from((wire, config): (wire::RelativeDelta, Config)) -> Result<Self, Self::Error> {
        let value = serde_json::to_value(wire).expect("Kalshi delta wire must serialize");
        Self::validate(&value, config)
    }
}

impl From<RelativeDelta> for Vec<SegmentEvent> {
    fn from(delta: RelativeDelta) -> Self {
        vec![SegmentEvent::Book(BookEvent::Delta(delta.event))]
    }
}

pub(crate) struct Trade {
    event: TradeEvent,
}

impl Trade {
    pub(crate) fn parse(value: &Value, config: Config) -> Result<Self, Reject> {
        match wire::Trade::deserialize(value) {
            Ok(wire) => Self::try_from((wire, config)),
            Err(_) => match Self::validate(value, config) {
                Err(reject) => Err(reject),
                Ok(_) => panic!("Kalshi trade wire schema and validator disagree"),
            },
        }
    }

    fn validate(value: &Value, config: Config) -> Result<Self, Reject> {
        let msg = value
            .checked_object()
            .map_err(|_| Reject::new("invalid_trade_msg"))?;
        msg.checked_fields(&[
            "trade_id",
            "market_ticker",
            "yes_price_dollars",
            "no_price_dollars",
            "count_fp",
            "taker_side",
            "taker_outcome_side",
            "taker_book_side",
            "is_block_trade",
            "ts",
            "ts_ms",
        ])?;
        let instrument = instrument(msg)?;
        msg.checked_required("trade_id")?
            .checked_nonempty_text()
            .map_err(|_| Reject::new("invalid_trade_id"))?;
        msg.checked_required("no_price_dollars")?
            .checked_price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        if let Some(block) = msg.get("is_block_trade") {
            block
                .checked_bool()
                .map_err(|_| Reject::new("invalid_block_trade"))?;
        }
        msg.checked_required("ts")?
            .checked_positive_u64()
            .map_err(|_| Reject::new("invalid_source_time"))?;
        msg.checked_required("ts_ms")?
            .checked_positive_u64()
            .map_err(|_| Reject::new("invalid_source_time"))?;
        let outcome = msg
            .checked_required("taker_outcome_side")?
            .checked_text()
            .map_err(|_| Reject::new("invalid_trade_direction"))?;
        let legacy = msg
            .checked_required("taker_side")?
            .checked_text()
            .map_err(|_| Reject::new("invalid_trade_direction"))?;
        let book = msg
            .checked_required("taker_book_side")?
            .checked_text()
            .map_err(|_| Reject::new("invalid_trade_direction"))?;
        let (expected_outcome, aggressor) = match book {
            "bid" => ("yes", Side::Bid),
            "ask" => ("no", Side::Ask),
            _ => {
                return Err(Reject::for_instrument(
                    "invalid_trade_direction",
                    instrument,
                ));
            }
        };
        if outcome != expected_outcome || legacy != outcome {
            return Err(Reject::for_instrument(
                "inconsistent_trade_direction",
                instrument,
            ));
        }
        let price = msg
            .checked_required("yes_price_dollars")?
            .checked_price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
        let quantity = msg
            .checked_required("count_fp")?
            .checked_positive_quantity(config.quantity_scale)
            .map_err(|code| {
                let code = if code == "zero_quantity" {
                    "non_positive_trade_quantity"
                } else {
                    code
                };
                Reject::for_instrument(code, instrument.clone())
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

    fn try_from((wire, config): (wire::Trade, Config)) -> Result<Self, Self::Error> {
        let value = serde_json::to_value(wire).expect("Kalshi trade wire must serialize");
        Self::validate(&value, config)
    }
}

impl From<Trade> for Vec<SegmentEvent> {
    fn from(trade: Trade) -> Self {
        vec![SegmentEvent::Trade(trade.event)]
    }
}

pub(crate) fn instrument(msg: &Map<String, Value>) -> Result<InstrumentId, Reject> {
    let ticker = msg
        .checked_required("market_ticker")?
        .checked_nonempty_text()
        .map_err(|_| Reject::new("invalid_market_ticker"))?;
    InstrumentId::new(format!("kalshi:{ticker}")).map_err(|_| Reject::new("invalid_market_ticker"))
}

fn levels(
    value: Option<&Value>,
    config: Config,
    legacy: bool,
    instrument: &InstrumentId,
) -> Result<Vec<Level>, Reject> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let rows = value
        .as_array()
        .ok_or_else(|| Reject::for_instrument("invalid_snapshot_levels", instrument.clone()))?;
    rows.iter()
        .map(|row| {
            let pair = row
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| {
                    Reject::for_instrument("invalid_snapshot_level", instrument.clone())
                })?;
            let (price, quantity) = if legacy {
                let cents = pair[0]
                    .checked_nonnegative_i64()
                    .map_err(|_| Reject::for_instrument("invalid_price", instrument.clone()))?;
                let contracts = pair[1]
                    .checked_nonnegative_i64()
                    .map_err(|_| Reject::for_instrument("invalid_quantity", instrument.clone()))?;
                let cents_scale = DecimalScale::new(2).expect("constant scale");
                let contracts_scale = DecimalScale::new(0).expect("constant scale");
                (
                    Px::from_atoms(cents, cents_scale)
                        .and_then(|price| price.checked_rescale(config.price_scale))
                        .and_then(ConditionalMarketPrice::try_from)
                        .map_err(|error| {
                            Reject::for_instrument(numeric_code(error, "price"), instrument.clone())
                        })?,
                    Qty::from_atoms(contracts as u64, contracts_scale)
                        .and_then(PositiveQty::new)
                        .and_then(|quantity| quantity.checked_rescale(config.quantity_scale))
                        .map_err(|error| {
                            Reject::for_instrument(
                                numeric_code(error, "quantity"),
                                instrument.clone(),
                            )
                        })?,
                )
            } else {
                (
                    pair[0]
                        .checked_price(config.price_scale)
                        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?,
                    pair[1]
                        .checked_positive_quantity(config.quantity_scale)
                        .map_err(|code| {
                            let code = if code == "zero_quantity" {
                                "non_positive_snapshot_quantity"
                            } else {
                                code
                            };
                            Reject::for_instrument(code, instrument.clone())
                        })?,
                )
            };
            Ok(Level::new(price, quantity))
        })
        .collect()
}

fn optional_nonempty_text(value: Option<&Value>, code: &'static str) -> Result<(), Reject> {
    match value {
        Some(value) => value
            .checked_nonempty_text()
            .map(|_| ())
            .map_err(|_| Reject::new(code)),
        None => Ok(()),
    }
}

fn optional_nonnegative_u64(value: Option<&Value>, code: &'static str) -> Result<(), Reject> {
    match value {
        Some(value) => value
            .checked_nonnegative_u64()
            .map(|_| ())
            .map_err(|_| Reject::new(code)),
        None => Ok(()),
    }
}

fn optional_positive_u64(value: Option<&Value>, code: &'static str) -> Result<(), Reject> {
    match value {
        Some(value) => value
            .checked_positive_u64()
            .map(|_| ())
            .map_err(|_| Reject::new(code)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn typed_semantically_bad_wire_rejects() {
        let wire: wire::RelativeDelta = serde_json::from_value(json!({
            "market_ticker": "TEST", "price_dollars": "0.5000",
            "delta_fp": "0.00", "side": "yes"
        }))
        .unwrap();
        let reject = RelativeDelta::try_from((wire, Config::default()))
            .err()
            .unwrap();
        assert_eq!(reject.code, "zero_relative_delta");
    }

    #[test]
    fn successful_event_conversion_is_infallible() {
        let event = Trade::parse(
            &json!({
                "trade_id": "t", "market_ticker": "TEST",
                "yes_price_dollars": "0.5000", "no_price_dollars": "0.5000",
                "count_fp": "1.00", "taker_side": "yes",
                "taker_outcome_side": "yes", "taker_book_side": "bid",
                "ts": 1, "ts_ms": 1
            }),
            Config::default(),
        )
        .unwrap();
        let events: Vec<SegmentEvent> = event.into();
        assert!(matches!(events.as_slice(), [SegmentEvent::Trade(_)]));
    }

    #[test]
    fn absent_optional_block_stays_absent_through_wire_roundtrip() {
        let value = json!({
            "trade_id": "t", "market_ticker": "TEST",
            "yes_price_dollars": "0.5000", "no_price_dollars": "0.5000",
            "count_fp": "1.00", "taker_side": "yes", "taker_outcome_side": "yes",
            "taker_book_side": "bid", "ts": 1, "ts_ms": 1
        });
        let wire: wire::Trade = serde_json::from_value(value).unwrap();
        let roundtrip = serde_json::to_value(wire).unwrap();
        assert!(roundtrip.get("is_block_trade").is_none());
    }

    #[test]
    fn duplicate_snapshot_levels_are_rejected() {
        let wire: wire::Snapshot = serde_json::from_value(json!({
            "market_ticker": "TEST",
            "yes_dollars_fp": [["0.5000", "1.00"], ["0.5000", "2.00"]],
            "no_dollars_fp": []
        }))
        .unwrap();
        let reject = Snapshot::try_from((wire, Config::default())).err().unwrap();
        assert_eq!(reject.code, "invalid_snapshot_levels");
    }

    #[test]
    fn all_typed_constructors_finish_validation_before_infallible_conversion() {
        let snapshot: wire::Snapshot = serde_json::from_value(json!({
            "market_ticker": "TEST", "yes": [[31, 7]], "no": [[62, 9]]
        }))
        .unwrap();
        let events: Vec<SegmentEvent> = Snapshot::try_from((snapshot, Config::default()))
            .unwrap()
            .into();
        assert_eq!(events.len(), 2);
        for (event, orientation, price, quantity) in [
            (&events[0], ContractOrientation::Outcome, 3100, 700),
            (&events[1], ContractOrientation::Complement, 6200, 900),
        ] {
            let SegmentEvent::Book(BookEvent::Full(book)) = event else {
                panic!("full book")
            };
            assert_eq!(book.orientation(), orientation);
            assert_eq!(book.bids()[0].price().atoms(), price);
            assert_eq!(book.bids()[0].quantity().atoms(), quantity);
        }
        for (signed, decrease) in [("-2.37", true), ("4.19", false)] {
            let delta: wire::RelativeDelta = serde_json::from_value(json!({
                "market_ticker": "TEST", "price_dollars": "0.6200",
                "delta_fp": signed, "side": "no"
            }))
            .unwrap();
            let events: Vec<SegmentEvent> = RelativeDelta::try_from((delta, Config::default()))
                .unwrap()
                .into();
            let SegmentEvent::Book(BookEvent::Delta(delta)) = &events[0] else {
                panic!("delta")
            };
            assert_eq!(delta.orientation(), ContractOrientation::Complement);
            match delta.change() {
                replay_domain::LevelChange::Decrease(qty) if decrease => {
                    assert_eq!(qty.atoms(), 237)
                }
                replay_domain::LevelChange::Increase(qty) if !decrease => {
                    assert_eq!(qty.atoms(), 419)
                }
                other => panic!("incorrect sign: {other:?}"),
            }
        }
        let mut trade: Value =
            serde_json::from_str(include_str!("../tests/fixtures/trade.json")).unwrap();
        trade["msg"]["taker_side"] = json!("yes");
        let wire: wire::Trade = serde_json::from_value(trade["msg"].clone()).unwrap();
        assert_eq!(
            Trade::try_from((wire, Config::default()))
                .err()
                .unwrap()
                .code,
            "inconsistent_trade_direction"
        );
    }

    #[test]
    fn wire_shape_failures_preserve_ordered_diagnostics() {
        // Exercise every field independently and pairwise. This checks that the
        // strict schema and diagnostic path agree, including missing vs null,
        // without using serde's error text as part of the contract.
        type Parse = fn(&Value, Config) -> Result<Vec<SegmentEvent>, Reject>;
        for (fixture, parse, validate) in [
            (
                include_str!("../tests/fixtures/orderbook_snapshot.json"),
                (|v, c| Snapshot::parse(v, c).map(Into::into)) as Parse,
                (|v, c| Snapshot::validate(v, c).map(Into::into)) as Parse,
            ),
            (
                include_str!("../tests/fixtures/orderbook_delta.json"),
                (|v, c| RelativeDelta::parse(v, c).map(Into::into)) as Parse,
                (|v, c| RelativeDelta::validate(v, c).map(Into::into)) as Parse,
            ),
            (
                include_str!("../tests/fixtures/trade.json"),
                (|v, c| Trade::parse(v, c).map(Into::into)) as Parse,
                (|v, c| Trade::validate(v, c).map(Into::into)) as Parse,
            ),
        ] {
            let fixture: Value = serde_json::from_str(fixture).unwrap();
            let original = fixture["msg"].as_object().unwrap();
            let fields: Vec<_> = original.keys().collect();
            for field in &fields {
                for replacement in [
                    None,
                    Some(Value::Null),
                    Some(json!(false)),
                    Some(json!(-1)),
                    Some(json!(0)),
                    Some(json!(1.5)),
                    Some(json!("")),
                    Some(json!("bad")),
                    Some(json!([])),
                    Some(json!({})),
                ] {
                    let mut value = Value::Object(original.clone());
                    match replacement {
                        Some(replacement) => {
                            value[*field] = replacement;
                        }
                        None => {
                            value.as_object_mut().unwrap().remove(*field);
                        }
                    }
                    for second in &fields {
                        let mut value = value.clone();
                        if second != field {
                            value[*second] = Value::Null;
                        }
                        let actual =
                            parse(&value, Config::default()).map_err(|r| (r.code, r.instrument));
                        let expected =
                            validate(&value, Config::default()).map_err(|r| (r.code, r.instrument));
                        assert_eq!(actual, expected, "{field}/{second}: {value}");
                    }
                }
            }
        }
    }
}
