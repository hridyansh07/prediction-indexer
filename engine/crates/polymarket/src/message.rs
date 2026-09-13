use std::collections::BTreeSet;

use canonical_normalizer::{CheckedObject as _, CheckedValue as _};
use indexer_types::{EnvelopeView, RecordKind, SourceCursor, Stream};
use replay_domain::{
    AuditAnchor, BookDelta, BookEvent, BookStateHash, ContractOrientation, ControlEvent,
    InstrumentId, Level, LevelChange, SegmentEvent, Sha1, Side, TradeEvent,
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

pub(crate) enum MessageOutcome {
    Events(Vec<SegmentEvent>),
}

pub(crate) enum ProcessOutcome {
    Event(SegmentEvent),
    Ignored(&'static str),
}

pub(crate) fn normalize_message(
    envelope: &EnvelopeView<'_>,
    value: &Value,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    let object = value.object("message_not_object")?;
    let event_type = object
        .required("event_type")?
        .nonempty_text("invalid_message_type")?;
    match event_type {
        "book" => full_book(envelope, object, config, false),
        "price_change" => {
            expect_public_book(envelope)?;
            Ok(MessageOutcome::Events(
                PriceChange::parse(object, config)?.into(),
            ))
        }
        "last_trade_price" => {
            expect_public_book(envelope)?;
            Ok(MessageOutcome::Events(Trade::parse(object, config)?.into()))
        }
        "tick_size_change" => unsupported_tick_size(envelope, object, config),
        "best_bid_ask" => unsupported_best_bid_ask(envelope, object, config),
        "new_market" => unsupported_new_market(envelope, object, config),
        "market_resolved" => unsupported_market_resolved(envelope, object, config),
        _ => Err(Reject::new("unsupported_message_type")),
    }
}

pub(crate) fn normalize_rest_snapshot(
    envelope: &EnvelopeView<'_>,
    value: &Value,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    full_book(envelope, value.object("snapshot_not_object")?, config, true)
}

fn full_book(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
    independent: bool,
) -> Result<MessageOutcome, Reject> {
    expect_stream(
        envelope,
        if independent {
            Stream::PublicSnapshot
        } else {
            Stream::PublicBook
        },
    )?;
    // Prefix validation preserves the historical cursor-failure precedence.
    // Event construction below is independent of delivery/envelope state.
    let (instrument, timestamp_ms) = book_prefix(object, config, independent)?;
    if independent {
        if !matches!(envelope.source_cursor, Some(SourceCursor::SnapshotTime { source_time_ms }) if source_time_ms == timestamp_ms)
        {
            return Err(Reject::for_instrument(
                "snapshot_cursor_mismatch",
                instrument,
            ));
        }
    } else {
        expect_unsequenced(envelope)?;
    }
    Ok(MessageOutcome::Events(
        Snapshot::parse(object, config, independent)?.into(),
    ))
}

fn book_prefix(
    object: &Map<String, Value>,
    config: Config,
    independent: bool,
) -> Result<(InstrumentId, u64), Reject> {
    object.fields(
        if independent {
            wire::REST_FIELDS
        } else {
            wire::BOOK_FIELDS
        },
        config.accept_additive_fields,
    )?;
    if !independent
        && object
            .required("event_type")?
            .text("invalid_message_type")?
            != "book"
    {
        return Err(Reject::new("invalid_message_type"));
    }
    let instrument = instrument(object.required("asset_id")?)?;
    validate_market(object.required("market")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    let timestamp_ms = timestamp(object.required("timestamp")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    Ok((instrument, timestamp_ms))
}

struct Snapshot {
    event: SnapshotEvent,
}

enum SnapshotEvent {
    Full(replay_domain::FullBook),
    Audit(AuditAnchor),
}

impl Snapshot {
    fn parse(
        object: &Map<String, Value>,
        config: Config,
        independent: bool,
    ) -> Result<Self, Reject> {
        let value = wire::project(
            object,
            if independent {
                wire::REST_FIELDS
            } else {
                wire::BOOK_FIELDS
            },
            config.accept_additive_fields,
        );
        match wire::Book::deserialize(&value) {
            Ok(wire) => Self::try_from((wire, config, independent)),
            Err(_) => match Self::validate(object, config, independent) {
                Err(reject) => Err(reject),
                Ok(_) => panic!("Polymarket book wire schema and validator disagree"),
            },
        }
    }

    fn validate(
        object: &Map<String, Value>,
        config: Config,
        independent: bool,
    ) -> Result<Self, Reject> {
        let (instrument, timestamp_ms) = book_prefix(object, config, independent)?;
        if independent {
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
        } else {
            optional_price(object.get("tick_size"), config, &instrument)?;
            optional_price(object.get("last_trade_price"), config, &instrument)?;
            optional_positive_quantity(object.get("min_order_size"), config, &instrument)?;
            if object
                .get("neg_risk")
                .is_some_and(|value| !value.is_boolean())
            {
                return Err(Reject::for_instrument("invalid_neg_risk", instrument));
            }
        }
        let hash = if independent {
            Some(
                state_hash(object.required("hash")?)
                    .map_err(|code| Reject::for_instrument(code, instrument.clone()))?,
            )
        } else {
            object
                .get("hash")
                .map(state_hash)
                .transpose()
                .map_err(|code| Reject::for_instrument(code, instrument.clone()))?
        };
        let bids = levels(object.required("bids")?, config, &instrument)?;
        let asks = levels(object.required("asks")?, config, &instrument)?;
        let observed_ns = timestamp_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| Reject::for_instrument("source_time_overflow", instrument.clone()))?;
        let event = if independent {
            SnapshotEvent::Audit(
                AuditAnchor::new(
                    instrument.clone(),
                    ContractOrientation::Outcome,
                    bids,
                    asks,
                    hash.expect("independent snapshot hash is required above"),
                    Some(observed_ns),
                )
                .map_err(|_| Reject::for_instrument("invalid_snapshot_levels", instrument))?,
            )
        } else {
            SnapshotEvent::Full(
                replay_domain::FullBook::new(
                    instrument.clone(),
                    ContractOrientation::Outcome,
                    bids,
                    asks,
                    hash,
                    Some(observed_ns),
                )
                .map_err(|_| Reject::for_instrument("invalid_snapshot_levels", instrument))?,
            )
        };
        Ok(Self { event })
    }
}

impl TryFrom<(wire::Book, Config, bool)> for Snapshot {
    type Error = Reject;

    fn try_from((wire, config, independent): (wire::Book, Config, bool)) -> Result<Self, Reject> {
        let value = serde_json::to_value(wire).expect("book wire must serialize");
        Self::validate(value.as_object().expect("book object"), config, independent)
    }
}

impl From<Snapshot> for Vec<SegmentEvent> {
    fn from(snapshot: Snapshot) -> Self {
        vec![match snapshot.event {
            SnapshotEvent::Full(book) => SegmentEvent::Book(BookEvent::Full(book)),
            SnapshotEvent::Audit(anchor) => SegmentEvent::AuditAnchor(anchor),
        }]
    }
}

struct PriceChange {
    events: Vec<BookDelta>,
}

impl PriceChange {
    fn parse(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
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

struct Trade {
    event: TradeEvent,
}

impl Trade {
    fn parse(object: &Map<String, Value>, config: Config) -> Result<Self, Reject> {
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

fn unsupported_tick_size(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    expect_public_book(envelope)?;
    object.fields(
        &[
            "event_type",
            "asset_id",
            "market",
            "old_tick_size",
            "new_tick_size",
            "timestamp",
        ],
        config.accept_additive_fields,
    )?;
    let instrument = instrument(object.required("asset_id")?)?;
    validate_market(object.required("market")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    timestamp(object.required("timestamp")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    for field in ["old_tick_size", "new_tick_size"] {
        object
            .required(field)?
            .price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    }
    Err(Reject::for_instrument(
        "unsupported_tick_size_change",
        instrument,
    ))
}

fn unsupported_best_bid_ask(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    expect_public_book(envelope)?;
    object.fields(
        &[
            "event_type",
            "market",
            "asset_id",
            "best_bid",
            "best_ask",
            "spread",
            "timestamp",
        ],
        config.accept_additive_fields,
    )?;
    let instrument = instrument(object.required("asset_id")?)?;
    validate_market(object.required("market")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    timestamp(object.required("timestamp")?)
        .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    for field in ["best_bid", "best_ask", "spread"] {
        object
            .required(field)?
            .price(config.price_scale)
            .map_err(|code| Reject::for_instrument(code, instrument.clone()))?;
    }
    Err(Reject::for_instrument(
        "unsupported_best_bid_ask",
        instrument,
    ))
}

fn unsupported_new_market(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    expect_public_book(envelope)?;
    object.fields(
        &[
            "event_type",
            "id",
            "question",
            "market",
            "slug",
            "assets_ids",
            "outcomes",
            "timestamp",
            "description",
            "event_message",
            "tags",
            "condition_id",
            "active",
            "clob_token_ids",
            "sports_market_type",
            "line",
            "game_start_time",
            "order_price_min_tick_size",
            "group_item_title",
        ],
        config.accept_additive_fields,
    )?;
    for field in ["id", "question", "slug"] {
        object
            .required(field)?
            .nonempty_text("invalid_market_metadata")?;
    }
    validate_market(object.required("market")?).map_err(Reject::new)?;
    timestamp(object.required("timestamp")?).map_err(Reject::new)?;
    validate_asset_array(object.required("assets_ids")?)?;
    validate_text_array(object.required("outcomes")?, "invalid_outcomes")?;
    Err(Reject::new("unsupported_new_market"))
}

fn unsupported_market_resolved(
    envelope: &EnvelopeView<'_>,
    object: &Map<String, Value>,
    config: Config,
) -> Result<MessageOutcome, Reject> {
    expect_public_book(envelope)?;
    object.fields(
        &[
            "event_type",
            "id",
            "market",
            "assets_ids",
            "winning_asset_id",
            "winning_outcome",
            "timestamp",
            "event_message",
            "tags",
            "question",
            "slug",
            "description",
            "outcomes",
        ],
        config.accept_additive_fields,
    )?;
    object
        .required("id")?
        .nonempty_text("invalid_market_metadata")?;
    validate_market(object.required("market")?).map_err(Reject::new)?;
    timestamp(object.required("timestamp")?).map_err(Reject::new)?;
    validate_asset_array(object.required("assets_ids")?)?;
    instrument(object.required("winning_asset_id")?)?;
    object
        .required("winning_outcome")?
        .nonempty_text("invalid_winning_outcome")?;
    Err(Reject::new("unsupported_market_resolved"))
}

pub(crate) fn normalize_process(
    envelope: &EnvelopeView<'_>,
    value: &Value,
) -> Result<ProcessOutcome, &'static str> {
    if !matches!(envelope.kind, RecordKind::Control | RecordKind::Fault) {
        return Err("unexpected_record_kind");
    }
    let object = value.as_object().ok_or("control_not_object")?;
    let event = object
        .get("event")
        .and_then(Value::as_str)
        .ok_or("invalid_control_event")?;
    let epoch = envelope.connection_epoch.as_str().to_owned();
    match event {
        "connection_opened" => {
            process_fields(
                object,
                &[
                    "event",
                    "target_digest",
                    "target_count",
                    "asset_ids",
                    "targets_path",
                    "target_metadata_digest",
                    "target_metadata_path",
                    "delivers_deltas",
                    "fsync_interval_seconds",
                    "repaired_bytes_on_start",
                    "clock_scope",
                    "url",
                    "feed",
                    "poll_seconds",
                    "batch_size",
                ],
            )?;
            let assets = object
                .get("asset_ids")
                .and_then(Value::as_array)
                .ok_or("invalid_control_asset_ids")?;
            let mut seen = BTreeSet::new();
            let mut instruments = Vec::with_capacity(assets.len());
            for asset in assets {
                let instrument = instrument(asset).map_err(|_| "invalid_control_asset_ids")?;
                if !seen.insert(instrument.as_str().to_owned()) {
                    return Err("duplicate_control_asset_id");
                }
                instruments.push(instrument);
            }
            let count = object
                .get("target_count")
                .and_then(Value::as_u64)
                .ok_or("invalid_control_target_count")?;
            if count != assets.len() as u64 {
                return Err("control_target_count_mismatch");
            }
            required_text(object, "targets_path")?;
            optional_text(object, "target_digest")?;
            optional_text(object, "target_metadata_digest")?;
            optional_text(object, "target_metadata_path")?;
            required_nonnegative_number(object, "fsync_interval_seconds")?;
            object
                .get("repaired_bytes_on_start")
                .and_then(Value::as_u64)
                .ok_or("invalid_control_integer")?;
            validate_clock_scope(object.get("clock_scope"))?;
            required_text(object, "url")?;
            optional_text(object, "feed")?;
            optional_nonnegative_number(object, "poll_seconds")?;
            optional_positive_u64(object, "batch_size")?;
            let delivers_deltas = object
                .get("delivers_deltas")
                .and_then(Value::as_bool)
                .ok_or("invalid_control_delivers_deltas")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionOpened {
                    epoch,
                    instruments,
                    delivers_deltas,
                    target_digest: optional_text(object, "target_digest")?,
                },
            )))
        }
        "connection_closed" => {
            process_fields(object, &["event", "seconds_open", "records_this_epoch"])?;
            required_nonnegative_number(object, "seconds_open")?;
            required_u64(object, "records_this_epoch")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionClosed { epoch },
            )))
        }
        "connection_failed" => {
            process_fields(
                object,
                &[
                    "event",
                    "error_type",
                    "error",
                    "seconds_open",
                    "frames_this_epoch",
                ],
            )?;
            required_text(object, "error_type")?;
            let reason = required_text(object, "error")?;
            required_nonnegative_number(object, "seconds_open")?;
            required_u64(object, "frames_this_epoch")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionFailed { epoch, reason },
            )))
        }
        "subscription_changed" => {
            process_fields(
                object,
                &["event", "from_digest", "to_digest", "added", "removed"],
            )?;
            validate_asset_array(object.get("added").ok_or("missing_control_field")?)
                .map_err(|_| "invalid_control_asset_ids")?;
            validate_asset_array(object.get("removed").ok_or("missing_control_field")?)
                .map_err(|_| "invalid_control_asset_ids")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::SubscriptionChanged {
                    from: optional_text(object, "from_digest")?,
                    to: required_text(object, "to_digest")?,
                },
            )))
        }
        "target_metadata_changed" => {
            process_fields(
                object,
                &[
                    "event",
                    "target_digest",
                    "from_metadata_digest",
                    "to_metadata_digest",
                    "metadata_path",
                ],
            )?;
            required_text(object, "target_digest")?;
            optional_text(object, "metadata_path")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::MetadataChanged {
                    from: optional_text(object, "from_metadata_digest")?,
                    to: required_text(object, "to_metadata_digest")?,
                },
            )))
        }
        "subscription_sent" => {
            process_fields(object, &["event", "target_digest", "target_count"])?;
            required_text(object, "target_digest")?;
            required_u64(object, "target_count")?;
            Ok(ProcessOutcome::Ignored("subscription_sent"))
        }
        "connection_closing" => {
            process_fields(object, &["event", "reason"])?;
            required_text(object, "reason")?;
            Ok(ProcessOutcome::Ignored("connection_closing"))
        }
        "targets_unreadable" => {
            process_fields(object, &["event", "error"])?;
            required_text(object, "error")?;
            Ok(ProcessOutcome::Ignored("targets_unreadable"))
        }
        "frame_not_utf8" => {
            process_fields(object, &["event", "bytes"])?;
            object
                .get("bytes")
                .and_then(Value::as_u64)
                .filter(|value| *value > 0)
                .ok_or("invalid_control_integer")?;
            Ok(ProcessOutcome::Ignored("frame_not_utf8"))
        }
        _ => Err("unsupported_control_event"),
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

fn instrument(value: &Value) -> Result<InstrumentId, Reject> {
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

fn validate_market(value: &Value) -> Result<(), &'static str> {
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

fn timestamp(value: &Value) -> Result<u64, &'static str> {
    let text = value.as_str().ok_or("invalid_source_time")?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid_source_time");
    }
    text.parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or("invalid_source_time")
}

fn expect_public_book(envelope: &EnvelopeView<'_>) -> Result<(), Reject> {
    expect_stream(envelope, Stream::PublicBook)?;
    expect_unsequenced(envelope)
}

fn expect_stream(envelope: &EnvelopeView<'_>, expected: Stream) -> Result<(), Reject> {
    if envelope.kind != RecordKind::VenueFrame {
        Err(Reject::new("unexpected_record_kind"))
    } else if envelope.stream != expected {
        Err(Reject::new("message_stream_mismatch"))
    } else {
        Ok(())
    }
}

fn expect_unsequenced(envelope: &EnvelopeView<'_>) -> Result<(), Reject> {
    if matches!(
        envelope.source_cursor,
        Some(SourceCursor::Unsequenced { .. })
    ) {
        Ok(())
    } else {
        Err(Reject::new("unexpected_sequence_cursor"))
    }
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

fn validate_asset_array(value: &Value) -> Result<(), Reject> {
    let values = value
        .as_array()
        .ok_or_else(|| Reject::new("invalid_asset_ids"))?;
    let mut seen = BTreeSet::new();
    for value in values {
        let instrument = instrument(value)?;
        if !seen.insert(instrument.as_str().to_owned()) {
            return Err(Reject::new("duplicate_asset_id"));
        }
    }
    Ok(())
}

fn validate_text_array(value: &Value, code: &'static str) -> Result<(), Reject> {
    let values = value.as_array().ok_or_else(|| Reject::new(code))?;
    for value in values {
        value.nonempty_text(code)?;
    }
    Ok(())
}

fn process_fields(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), &'static str> {
    object.checked_fields(allowed).map_err(|_| "unknown_field")
}

fn required_text(object: &Map<String, Value>, field: &str) -> Result<String, &'static str> {
    object
        .checked_required(field)
        .map_err(|_| "invalid_control_field")?
        .checked_nonempty_text()
        .map(str::to_owned)
        .map_err(|_| "invalid_control_field")
}

fn optional_text(object: &Map<String, Value>, field: &str) -> Result<Option<String>, &'static str> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .checked_nonempty_text()
            .map(|value| Some(value.to_owned()))
            .map_err(|_| "invalid_control_field"),
    }
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<(), &'static str> {
    object
        .checked_required(field)
        .map_err(|_| "invalid_control_integer")?
        .checked_nonnegative_u64()
        .map(|_| ())
        .map_err(|_| "invalid_control_integer")
}

fn optional_positive_u64(object: &Map<String, Value>, field: &str) -> Result<(), &'static str> {
    match object.get(field) {
        None => Ok(()),
        Some(value) => value
            .checked_positive_u64()
            .map(|_| ())
            .map_err(|_| "invalid_control_integer"),
    }
}

fn required_nonnegative_number(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), &'static str> {
    object
        .checked_required(field)
        .map_err(|_| "invalid_control_number")?
        .checked_nonnegative_number()
        .map(|_| ())
        .map_err(|_| "invalid_control_number")
}

fn optional_nonnegative_number(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), &'static str> {
    if object.contains_key(field) {
        required_nonnegative_number(object, field)
    } else {
        Ok(())
    }
}

fn validate_clock_scope(value: Option<&Value>) -> Result<(), &'static str> {
    let object = value
        .and_then(Value::as_object)
        .ok_or("invalid_control_clock_scope")?;
    process_fields(
        object,
        &[
            "lane",
            "clock",
            "scope",
            "scope_id",
            "comparable_across_processes",
            "platform",
        ],
    )?;
    for field in ["lane", "clock", "scope", "scope_id", "platform"] {
        required_text(object, field)?;
    }
    object
        .get("comparable_across_processes")
        .and_then(Value::as_bool)
        .ok_or("invalid_control_clock_scope")?;
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

    #[test]
    fn typed_books_finish_domain_validation_before_conversion() {
        for independent in [false, true] {
            let mut value = book();
            if independent {
                value.as_object_mut().unwrap().remove("event_type");
                value["hash"] = json!("a".repeat(40));
                value["min_order_size"] = json!("1");
                value["tick_size"] = json!("0.01");
                value["last_trade_price"] = json!("0.41");
                value["neg_risk"] = json!(false);
            }
            let wire = wire::Book::deserialize(&value).unwrap();
            let events: Vec<SegmentEvent> =
                Snapshot::try_from((wire, Config::default(), independent))
                    .unwrap()
                    .into();
            let (bids, asks, time) = match &events[0] {
                SegmentEvent::Book(BookEvent::Full(book)) if !independent => {
                    (book.bids(), book.asks(), book.source_observed_ns())
                }
                SegmentEvent::AuditAnchor(book) if independent => {
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
            let wire = wire::Book::deserialize(&value).unwrap();
            assert_eq!(
                Snapshot::try_from((wire, Config::default(), independent))
                    .err()
                    .unwrap()
                    .code,
                "invalid_snapshot_levels"
            );
            // A malformed ask still wins over duplicate bid validation.
            value["asks"][0]["size"] = json!("bad");
            let wire = wire::Book::deserialize(&value).unwrap();
            assert_eq!(
                Snapshot::try_from((wire, Config::default(), independent))
                    .err()
                    .unwrap()
                    .code,
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
        let wire = wire::Book::deserialize(&value).unwrap();
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
            assert!(wire::Book::deserialize(&invalid).is_err());
        }
        let mut unknown = value.clone();
        unknown["future"] = json!(true);
        assert!(wire::Book::deserialize(&unknown).is_err());
        let mut nested = value;
        nested["bids"][0]["future"] = json!(true);
        assert!(wire::Book::deserialize(&nested).is_err());
    }

    #[test]
    fn rest_constructor_requires_independent_evidence_and_time_conversion() {
        let mut value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/rest_book.json")).unwrap();
        value.as_object_mut().unwrap().remove("hash");
        let wire = wire::Book::deserialize(&value).unwrap();
        assert_eq!(
            Snapshot::try_from((wire, Config::default(), true))
                .err()
                .unwrap()
                .code,
            "missing_required_field"
        );
        let mut value = book();
        value["timestamp"] = json!(u64::MAX.to_string());
        let wire = wire::Book::deserialize(&value).unwrap();
        assert_eq!(
            Snapshot::try_from((wire, Config::default(), false))
                .err()
                .unwrap()
                .code,
            "source_time_overflow"
        );
    }
}
