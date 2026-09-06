//! Kalshi WebSocket evidence to the closed Replay S2 domain.
//!
//! Kalshi publishes independent YES and NO bid books. This adapter never
//! complements prices: it emits `Outcome` and `Complement` full books and marks
//! both as bid-side evidence. State application remains a future book concern.

use std::collections::BTreeSet;

use indexer_finalize::JoinedCanonicalRecord;
use indexer_types::{EnvelopeView, RecordKind, SourceCursor, Stream, Venue};
use replay_domain::{
    BookDelta, BookEvent, ContractOrientation, ControlEvent, DecimalScale, FaultImpact, FullBook,
    InstrumentId, LaneId, Level, LevelSize, Px, Qty, SegmentEvent, Sha256, Side, TradeEvent,
};
use replay_normalize::{
    Normalization, Normalizer, NormalizerDescriptor, NormalizerError, ParseReject,
};
use serde_json::{Map, Value};

pub const PARSER_VERSION: u32 = 1;
pub const ADAPTER_BUNDLE_ID: &str = "prediction-indexer/replay-kalshi/v1";
pub const DEFAULT_CONFIG_ID: &str =
    "kalshi-normalizer-config-v1;price_scale=4;quantity_scale=2;use_yes_price=false";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KalshiConfig {
    pub price_scale: DecimalScale,
    pub quantity_scale: DecimalScale,
    /// Must match the capture subscription. V1 supports the splice's current
    /// legacy per-outcome pricing (`use_yes_price` omitted/false) only.
    pub use_yes_price: bool,
}

impl Default for KalshiConfig {
    fn default() -> Self {
        Self {
            price_scale: DecimalScale::new(4).expect("constant scale"),
            quantity_scale: DecimalScale::new(2).expect("constant scale"),
            use_yes_price: false,
        }
    }
}

impl KalshiConfig {
    fn identity(self) -> String {
        format!(
            "kalshi-normalizer-config-v1;price_scale={};quantity_scale={};use_yes_price={}",
            self.price_scale.exponent(),
            self.quantity_scale.exponent(),
            self.use_yes_price
        )
    }
}

pub struct KalshiNormalizer {
    config: KalshiConfig,
    descriptor: NormalizerDescriptor,
}

impl Default for KalshiNormalizer {
    fn default() -> Self {
        Self::new(KalshiConfig::default()).expect("default Kalshi config is supported")
    }
}

impl KalshiNormalizer {
    pub fn new(config: KalshiConfig) -> Result<Self, NormalizerError> {
        if config.use_yes_price {
            return Err(NormalizerError::new(
                "Kalshi v1 does not support use_yes_price=true captures",
            ));
        }
        let descriptor = NormalizerDescriptor {
            bundle_sha256: Sha256::digest(ADAPTER_BUNDLE_ID.as_bytes()),
            config_sha256: Sha256::digest(config.identity().as_bytes()),
        };
        Ok(Self { config, descriptor })
    }

    fn reject(
        &self,
        source: &JoinedCanonicalRecord,
        code: &'static str,
        instrument: Option<InstrumentId>,
    ) -> Normalization {
        let impact = instrument
            .clone()
            .map(FaultImpact::Instrument)
            .unwrap_or_else(|| {
                FaultImpact::UnattributedLane(
                    LaneId::new(source.event_address.lane_id.clone())
                        .expect("audited lane is non-empty"),
                )
            });
        Normalization::Reject(ParseReject {
            parser_version: PARSER_VERSION,
            error_code: code.to_owned(),
            instrument_hint: instrument,
            impact,
        })
    }
}

impl Normalizer for KalshiNormalizer {
    fn descriptor(&self) -> &NormalizerDescriptor {
        &self.descriptor
    }

    fn normalize(
        &mut self,
        source: &JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError> {
        let envelope = EnvelopeView::parse(&source.envelope).map_err(|error| {
            NormalizerError::new(format!(
                "audited canonical envelope became invalid: {error}"
            ))
        })?;
        if envelope.venue != Venue::Kalshi {
            return Ok(Normalization::Ignored {
                reason_code: "not_kalshi".to_owned(),
            });
        }
        let payload: Value = match serde_json::from_str(&envelope.raw_payload) {
            Ok(value) => value,
            Err(_) => return Ok(self.reject(source, "invalid_json", None)),
        };
        if envelope.stream == Stream::Process {
            return Ok(match normalize_process(&envelope, &payload) {
                Ok(ProcessOutcome::Event(event)) => Normalization::Events(vec![event]),
                Ok(ProcessOutcome::Ignored(reason)) => Normalization::Ignored {
                    reason_code: reason.to_owned(),
                },
                Err(code) => self.reject(source, code, None),
            });
        }
        if envelope.kind != RecordKind::VenueFrame {
            return Ok(self.reject(source, "unexpected_record_kind", None));
        }

        let (messages, batched) = match payload {
            Value::Array(values) => (values, true),
            value => (vec![value], false),
        };
        if batched
            && !matches!(
                envelope.source_cursor,
                Some(SourceCursor::Unsequenced { .. })
            )
        {
            return Ok(self.reject(source, "batch_cursor_mismatch", None));
        }
        if messages.is_empty() {
            return Ok(Normalization::Events(Vec::new()));
        }
        let mut events = Vec::new();
        let mut ignored = BTreeSet::new();
        for value in messages {
            match normalize_message(&envelope, &value, self.config, batched) {
                Ok(MessageOutcome::Events(mut children)) => events.append(&mut children),
                Ok(MessageOutcome::Ignored(reason)) => {
                    ignored.insert(reason);
                }
                Err(failure) => {
                    return Ok(self.reject(source, failure.code, failure.instrument));
                }
            }
        }
        if events.is_empty() && !ignored.is_empty() {
            Ok(Normalization::Ignored {
                reason_code: if ignored.len() == 1 {
                    ignored.into_iter().next().expect("non-empty").to_owned()
                } else {
                    "supported_non_domain_messages".to_owned()
                },
            })
        } else {
            Ok(Normalization::Events(events))
        }
    }

    fn finish(&mut self) -> Result<(), NormalizerError> {
        Ok(())
    }
}

struct Failure {
    code: &'static str,
    instrument: Option<InstrumentId>,
}

impl Failure {
    const fn new(code: &'static str) -> Self {
        Self {
            code,
            instrument: None,
        }
    }

    fn for_instrument(code: &'static str, instrument: InstrumentId) -> Self {
        Self {
            code,
            instrument: Some(instrument),
        }
    }
}

enum MessageOutcome {
    Events(Vec<SegmentEvent>),
    Ignored(&'static str),
}

fn normalize_message(
    envelope: &EnvelopeView<'_>,
    value: &Value,
    config: KalshiConfig,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    let object = object(value, "message_not_object")?;
    let kind = text(required(object, "type")?, "invalid_message_type")?;
    match kind {
        "orderbook_snapshot" => snapshot(envelope, object, config, batched),
        "orderbook_delta" => delta(envelope, object, config, batched),
        "trade" => trade(envelope, object, config, batched),
        "ticker" => ticker(envelope, object, config),
        "subscribed" => subscribed(envelope, object),
        "unsubscribed" => unsubscribed(envelope, object, batched),
        "ok" => ok_response(envelope, object, batched),
        "error" => error_response(envelope, object, batched),
        _ => Err(Failure::new("unsupported_message_type")),
    }
}

fn snapshot(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    config: KalshiConfig,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    exact_fields(outer, &["type", "sid", "seq", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    sequence(outer, envelope, batched)?;
    positive_u64(required(outer, "sid")?, "invalid_sid")?;
    let msg = object(required(outer, "msg")?, "invalid_snapshot_msg")?;
    let modern = msg.contains_key("yes_dollars_fp") || msg.contains_key("no_dollars_fp");
    let legacy = msg.contains_key("yes") || msg.contains_key("no");
    if modern && legacy {
        return Err(Failure::new("mixed_snapshot_schema"));
    }
    let instrument = instrument(msg)?;
    if modern {
        exact_fields(
            msg,
            &[
                "market_ticker",
                "market_id",
                "yes_dollars_fp",
                "no_dollars_fp",
            ],
        )?;
    } else {
        exact_fields(msg, &["market_ticker", "market_id", "yes", "no"])?;
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
    let outcome = FullBook::new(
        instrument.clone(),
        ContractOrientation::Outcome,
        yes,
        Vec::new(),
        None,
        None,
    )
    .map_err(|_| Failure::for_instrument("invalid_snapshot_levels", instrument.clone()))?;
    let complement = FullBook::new(
        instrument.clone(),
        ContractOrientation::Complement,
        no,
        Vec::new(),
        None,
        None,
    )
    .map_err(|_| Failure::for_instrument("invalid_snapshot_levels", instrument))?;
    Ok(MessageOutcome::Events(vec![
        SegmentEvent::Book(BookEvent::Full(outcome)),
        SegmentEvent::Book(BookEvent::Full(complement)),
    ]))
}

fn delta(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    config: KalshiConfig,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    exact_fields(outer, &["type", "sid", "seq", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    sequence(outer, envelope, batched)?;
    positive_u64(required(outer, "sid")?, "invalid_sid")?;
    let msg = object(required(outer, "msg")?, "invalid_delta_msg")?;
    exact_fields(
        msg,
        &[
            "market_ticker",
            "market_id",
            "price_dollars",
            "delta_fp",
            "side",
            "client_order_id",
            "subaccount",
            "ts",
            "ts_ms",
        ],
    )?;
    let instrument = instrument(msg)?;
    optional_nonempty_text(msg.get("market_id"), "invalid_market_id")?;
    optional_nonempty_text(msg.get("client_order_id"), "invalid_client_order_id")?;
    optional_i64(msg.get("subaccount"), "invalid_subaccount")?;
    optional_nonempty_text(msg.get("ts"), "invalid_source_time")?;
    optional_i64(msg.get("ts_ms"), "invalid_source_time")?;
    let orientation = match text(required(msg, "side")?, "invalid_side")? {
        "yes" => ContractOrientation::Outcome,
        "no" => ContractOrientation::Complement,
        _ => return Err(Failure::for_instrument("invalid_side", instrument)),
    };
    let price = parse_px(required(msg, "price_dollars")?, config.price_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    let quantity = parse_qty(required(msg, "delta_fp")?, config.quantity_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    let size = LevelSize::relative(quantity)
        .map_err(|_| Failure::for_instrument("zero_relative_delta", instrument.clone()))?;
    let event = BookDelta::new(
        instrument.clone(),
        orientation,
        Side::Bid,
        price,
        size,
        None,
    )
    .map_err(|_| Failure::for_instrument("invalid_delta", instrument))?;
    Ok(MessageOutcome::Events(vec![SegmentEvent::Book(
        BookEvent::Delta(event),
    )]))
}

fn trade(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    config: KalshiConfig,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    exact_fields(outer, &["type", "sid", "seq", "msg"])?;
    expect_stream(envelope, Stream::PublicTrade)?;
    sequence(outer, envelope, batched)?;
    positive_u64(required(outer, "sid")?, "invalid_sid")?;
    let msg = object(required(outer, "msg")?, "invalid_trade_msg")?;
    exact_fields(
        msg,
        &[
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
        ],
    )?;
    let instrument = instrument(msg)?;
    nonempty_text(required(msg, "trade_id")?, "invalid_trade_id")?;
    parse_px(required(msg, "no_price_dollars")?, config.price_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    bool_value(required(msg, "is_block_trade")?, "invalid_block_trade")?;
    i64_value(required(msg, "ts")?, "invalid_source_time")?;
    i64_value(required(msg, "ts_ms")?, "invalid_source_time")?;
    let outcome = text(
        required(msg, "taker_outcome_side")?,
        "invalid_trade_direction",
    )?;
    let legacy = text(required(msg, "taker_side")?, "invalid_trade_direction")?;
    let book = text(required(msg, "taker_book_side")?, "invalid_trade_direction")?;
    let (expected_outcome, aggressor) = match book {
        "bid" => ("yes", Side::Bid),
        "ask" => ("no", Side::Ask),
        _ => {
            return Err(Failure::for_instrument(
                "invalid_trade_direction",
                instrument,
            ));
        }
    };
    if outcome != expected_outcome || legacy != outcome {
        return Err(Failure::for_instrument(
            "inconsistent_trade_direction",
            instrument,
        ));
    }
    let price = parse_px(required(msg, "yes_price_dollars")?, config.price_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    let quantity = parse_qty(required(msg, "count_fp")?, config.quantity_scale)
        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    let event = TradeEvent::new(
        instrument.clone(),
        ContractOrientation::Outcome,
        price,
        quantity,
        Some(aggressor),
    )
    .map_err(|_| Failure::for_instrument("non_positive_trade_quantity", instrument))?;
    Ok(MessageOutcome::Events(vec![SegmentEvent::Trade(event)]))
}

fn ticker(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    config: KalshiConfig,
) -> Result<MessageOutcome, Failure> {
    exact_fields(outer, &["type", "sid", "msg"])?;
    expect_stream(envelope, Stream::PublicQuote)?;
    expect_unsequenced(envelope)?;
    positive_u64(required(outer, "sid")?, "invalid_sid")?;
    let msg = object(required(outer, "msg")?, "invalid_ticker_msg")?;
    exact_fields(
        msg,
        &[
            "market_ticker",
            "market_id",
            "price_dollars",
            "yes_bid_dollars",
            "yes_ask_dollars",
            "yes_bid_size_fp",
            "yes_ask_size_fp",
            "last_trade_size_fp",
            "volume_fp",
            "open_interest_fp",
            "dollar_volume",
            "dollar_open_interest",
            "ts",
            "ts_ms",
            "time",
        ],
    )?;
    let instrument = instrument(msg)?;
    nonempty_text(required(msg, "market_id")?, "invalid_market_id")?;
    for field in ["price_dollars", "yes_bid_dollars", "yes_ask_dollars"] {
        parse_px(required(msg, field)?, config.price_scale)
            .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    }
    for field in [
        "yes_bid_size_fp",
        "yes_ask_size_fp",
        "last_trade_size_fp",
        "volume_fp",
        "open_interest_fp",
    ] {
        parse_qty(required(msg, field)?, config.quantity_scale)
            .map_err(|code| Failure::for_instrument(code, instrument.clone()))?;
    }
    for field in ["dollar_volume", "dollar_open_interest", "ts", "ts_ms"] {
        nonnegative_i64(required(msg, field)?, "invalid_ticker_integer")?;
    }
    nonempty_text(required(msg, "time")?, "invalid_source_time")?;
    Ok(MessageOutcome::Ignored("ticker_not_in_s2"))
}

fn subscribed(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
) -> Result<MessageOutcome, Failure> {
    exact_fields(outer, &["id", "type", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    expect_unsequenced(envelope)?;
    optional_nonnegative_u64(outer.get("id"), "invalid_command_id")?;
    let msg = object(required(outer, "msg")?, "invalid_subscribed_msg")?;
    exact_fields(msg, &["channel", "sid"])?;
    nonempty_text(required(msg, "channel")?, "invalid_channel")?;
    positive_u64(required(msg, "sid")?, "invalid_sid")?;
    Ok(MessageOutcome::Ignored("venue_control_not_in_s2"))
}

fn unsubscribed(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    exact_fields(outer, &["id", "sid", "seq", "type"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    sequence(outer, envelope, batched)?;
    optional_nonnegative_u64(outer.get("id"), "invalid_command_id")?;
    positive_u64(required(outer, "sid")?, "invalid_sid")?;
    Ok(MessageOutcome::Ignored("venue_control_not_in_s2"))
}

fn ok_response(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    exact_fields(outer, &["id", "sid", "seq", "type", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    optional_nonnegative_u64(outer.get("id"), "invalid_command_id")?;
    optional_positive_u64(outer.get("sid"), "invalid_sid")?;
    if outer.contains_key("seq") {
        sequence(outer, envelope, batched)?;
    } else {
        expect_unsequenced(envelope)?;
    }
    if let Some(value) = outer.get("msg") {
        match value {
            Value::Object(msg) => {
                exact_fields(msg, &["market_tickers", "market_ids"])?;
                optional_text_array(msg.get("market_tickers"), "invalid_ok_msg")?;
                optional_text_array(msg.get("market_ids"), "invalid_ok_msg")?;
            }
            Value::Array(items) => {
                for item in items {
                    let subscription = object(item, "invalid_ok_msg")?;
                    exact_fields(subscription, &["channel", "sid"])?;
                    nonempty_text(required(subscription, "channel")?, "invalid_channel")?;
                    positive_u64(required(subscription, "sid")?, "invalid_sid")?;
                }
            }
            _ => return Err(Failure::new("invalid_ok_msg")),
        }
    }
    Ok(MessageOutcome::Ignored("venue_control_not_in_s2"))
}

fn error_response(
    envelope: &EnvelopeView<'_>,
    outer: &Map<String, Value>,
    batched: bool,
) -> Result<MessageOutcome, Failure> {
    exact_fields(outer, &["id", "sid", "seq", "type", "msg"])?;
    expect_stream(envelope, Stream::PublicBook)?;
    optional_nonnegative_u64(outer.get("id"), "invalid_command_id")?;
    optional_positive_u64(outer.get("sid"), "invalid_sid")?;
    if outer.contains_key("seq") {
        sequence(outer, envelope, batched)?;
    } else {
        expect_unsequenced(envelope)?;
    }
    let msg = object(required(outer, "msg")?, "invalid_error_msg")?;
    exact_fields(msg, &["code", "msg"])?;
    nonnegative_i64(required(msg, "code")?, "invalid_error_code")?;
    nonempty_text(required(msg, "msg")?, "invalid_error_message")?;
    Ok(MessageOutcome::Ignored("venue_control_not_in_s2"))
}

enum ProcessOutcome {
    Event(SegmentEvent),
    Ignored(&'static str),
}

fn normalize_process(
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
            exact_field_names(
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
                    "channels",
                    "send_initial_snapshot",
                    "verified_against_live_socket",
                    "snapshot_sweep_seconds",
                    "snapshot_max_age_seconds",
                    "snapshot_request_cooldown_seconds",
                    "key_id",
                ],
            )?;
            let assets = object
                .get("asset_ids")
                .and_then(Value::as_array)
                .ok_or("invalid_control_asset_ids")?;
            let mut seen = BTreeSet::new();
            let mut instruments = Vec::with_capacity(assets.len());
            for asset in assets {
                let text = asset
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .ok_or("invalid_control_asset_ids")?;
                if !seen.insert(text) {
                    return Err("duplicate_control_asset_id");
                }
                instruments.push(
                    InstrumentId::new(format!("kalshi:{text}"))
                        .map_err(|_| "invalid_control_asset_ids")?,
                );
            }
            let delivers_deltas = object
                .get("delivers_deltas")
                .and_then(Value::as_bool)
                .ok_or("invalid_control_delivers_deltas")?;
            let target_digest = optional_text_field(object, "target_digest")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionOpened {
                    epoch,
                    instruments,
                    delivers_deltas,
                    target_digest,
                },
            )))
        }
        "connection_closed" => {
            exact_field_names(object, &["event", "seconds_open", "records_this_epoch"])?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionClosed { epoch },
            )))
        }
        "connection_failed" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "error_type",
                    "error",
                    "seconds_open",
                    "frames_this_epoch",
                ],
            )?;
            let reason = required_text_field(object, "error")?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::ConnectionFailed { epoch, reason },
            )))
        }
        "subscription_changed" => {
            exact_field_names(
                object,
                &["event", "from_digest", "to_digest", "added", "removed"],
            )?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::SubscriptionChanged {
                    from: optional_text_field(object, "from_digest")?,
                    to: required_text_field(object, "to_digest")?,
                },
            )))
        }
        "target_metadata_changed" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "target_digest",
                    "from_metadata_digest",
                    "to_metadata_digest",
                    "metadata_path",
                ],
            )?;
            Ok(ProcessOutcome::Event(SegmentEvent::Control(
                ControlEvent::MetadataChanged {
                    from: optional_text_field(object, "from_metadata_digest")?,
                    to: required_text_field(object, "to_metadata_digest")?,
                },
            )))
        }
        "subscription_sent" => {
            exact_field_names(object, &["event", "target_digest", "target_count"])?;
            Ok(ProcessOutcome::Ignored("subscription_sent"))
        }
        "connection_closing" => {
            exact_field_names(object, &["event", "reason"])?;
            Ok(ProcessOutcome::Ignored("connection_closing"))
        }
        "orderbook_reconciliation_request" => {
            exact_field_names(
                object,
                &["event", "sid", "command_id", "market_tickers", "reason"],
            )?;
            Ok(ProcessOutcome::Ignored("reconciliation_request"))
        }
        "orderbook_reconciliation_disabled" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "reason",
                    "channel",
                    "command_id",
                    "error_type",
                    "error",
                    "code",
                    "detail",
                ],
            )?;
            Ok(ProcessOutcome::Ignored("reconciliation_disabled"))
        }
        "orderbook_reconciliation_backoff" => {
            exact_field_names(
                object,
                &[
                    "event",
                    "command_id",
                    "code",
                    "from_sweep_seconds",
                    "to_sweep_seconds",
                    "detail",
                ],
            )?;
            Ok(ProcessOutcome::Ignored("reconciliation_backoff"))
        }
        "targets_unreadable" => {
            exact_field_names(object, &["event", "error"])?;
            Ok(ProcessOutcome::Ignored("targets_unreadable"))
        }
        "frame_not_utf8" => {
            exact_field_names(object, &["event", "bytes"])?;
            Ok(ProcessOutcome::Ignored("frame_not_utf8"))
        }
        _ => Err("unsupported_control_event"),
    }
}

fn levels(
    value: Option<&Value>,
    config: KalshiConfig,
    legacy: bool,
    instrument: &InstrumentId,
) -> Result<Vec<Level>, Failure> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let rows = value
        .as_array()
        .ok_or_else(|| Failure::for_instrument("invalid_snapshot_levels", instrument.clone()))?;
    rows.iter()
        .map(|row| {
            let pair = row
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| {
                    Failure::for_instrument("invalid_snapshot_level", instrument.clone())
                })?;
            let (price, quantity) = if legacy {
                let cents = nonnegative_i64(&pair[0], "invalid_price")?;
                let contracts = nonnegative_i64(&pair[1], "invalid_quantity")?;
                let cents_scale = DecimalScale::new(2).expect("constant scale");
                let contracts_scale = DecimalScale::new(0).expect("constant scale");
                (
                    Px::from_atoms(cents, cents_scale)
                        .and_then(|price| price.checked_rescale(config.price_scale))
                        .map_err(|error| {
                            Failure::for_instrument(
                                numeric_code(error, "price"),
                                instrument.clone(),
                            )
                        })?,
                    Qty::from_atoms(contracts, contracts_scale)
                        .checked_rescale(config.quantity_scale)
                        .map_err(|error| {
                            Failure::for_instrument(
                                numeric_code(error, "quantity"),
                                instrument.clone(),
                            )
                        })?,
                )
            } else {
                (
                    parse_px(&pair[0], config.price_scale)
                        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?,
                    parse_qty(&pair[1], config.quantity_scale)
                        .map_err(|code| Failure::for_instrument(code, instrument.clone()))?,
                )
            };
            Level::new(price, quantity).map_err(|_| {
                Failure::for_instrument("non_positive_snapshot_quantity", instrument.clone())
            })
        })
        .collect()
}

fn instrument(msg: &Map<String, Value>) -> Result<InstrumentId, Failure> {
    let ticker = nonempty_text(required(msg, "market_ticker")?, "invalid_market_ticker")?;
    InstrumentId::new(format!("kalshi:{ticker}")).map_err(|_| Failure::new("invalid_market_ticker"))
}

fn sequence(
    outer: &Map<String, Value>,
    envelope: &EnvelopeView<'_>,
    batched: bool,
) -> Result<(), Failure> {
    let seq = positive_u64(required(outer, "seq")?, "invalid_sequence")?;
    if batched {
        return expect_unsequenced(envelope).map_err(|_| Failure::new("batch_cursor_mismatch"));
    }
    match envelope.source_cursor {
        Some(SourceCursor::UpdateRange {
            first,
            last,
            previous_last,
        }) if first == seq && last == seq && previous_last == seq - 1 => Ok(()),
        _ => Err(Failure::new("sequence_cursor_mismatch")),
    }
}

fn expect_unsequenced(envelope: &EnvelopeView<'_>) -> Result<(), Failure> {
    if matches!(
        envelope.source_cursor,
        Some(SourceCursor::Unsequenced { .. })
    ) {
        Ok(())
    } else {
        Err(Failure::new("unexpected_sequence_cursor"))
    }
}

fn expect_stream(envelope: &EnvelopeView<'_>, expected: Stream) -> Result<(), Failure> {
    if envelope.stream == expected {
        Ok(())
    } else {
        Err(Failure::new("message_stream_mismatch"))
    }
}

fn exact_fields(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), Failure> {
    exact_field_names(object, allowed).map_err(Failure::new)
}

fn exact_field_names(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), &'static str> {
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        Err("unknown_field")
    } else {
        Ok(())
    }
}

fn object<'a>(value: &'a Value, code: &'static str) -> Result<&'a Map<String, Value>, Failure> {
    value.as_object().ok_or_else(|| Failure::new(code))
}

fn required<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a Value, Failure> {
    object
        .get(field)
        .ok_or_else(|| Failure::new("missing_required_field"))
}

fn text<'a>(value: &'a Value, code: &'static str) -> Result<&'a str, Failure> {
    value.as_str().ok_or_else(|| Failure::new(code))
}

fn nonempty_text<'a>(value: &'a Value, code: &'static str) -> Result<&'a str, Failure> {
    text(value, code).and_then(|value| {
        if value.is_empty() || value.chars().any(char::is_control) {
            Err(Failure::new(code))
        } else {
            Ok(value)
        }
    })
}

fn optional_nonempty_text(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => nonempty_text(value, code).map(|_| ()),
        None => Ok(()),
    }
}

fn optional_text_array(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    let Some(value) = value else {
        return Ok(());
    };
    let values = value.as_array().ok_or_else(|| Failure::new(code))?;
    for value in values {
        nonempty_text(value, code)?;
    }
    Ok(())
}

fn required_text_field(object: &Map<String, Value>, field: &str) -> Result<String, &'static str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or("invalid_control_field")
}

fn optional_text_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<String>, &'static str> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        _ => Err("invalid_control_field"),
    }
}

fn bool_value(value: &Value, code: &'static str) -> Result<bool, Failure> {
    value.as_bool().ok_or_else(|| Failure::new(code))
}

fn i64_value(value: &Value, code: &'static str) -> Result<i64, Failure> {
    value.as_i64().ok_or_else(|| Failure::new(code))
}

fn nonnegative_i64(value: &Value, code: &'static str) -> Result<i64, Failure> {
    i64_value(value, code).and_then(|value| {
        if value < 0 {
            Err(Failure::new(code))
        } else {
            Ok(value)
        }
    })
}

fn positive_u64(value: &Value, code: &'static str) -> Result<u64, Failure> {
    value
        .as_u64()
        .filter(|value| *value > 0)
        .ok_or_else(|| Failure::new(code))
}

fn optional_i64(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => i64_value(value, code).map(|_| ()),
        None => Ok(()),
    }
}

fn optional_nonnegative_u64(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => value.as_u64().map(|_| ()).ok_or_else(|| Failure::new(code)),
        None => Ok(()),
    }
}

fn optional_positive_u64(value: Option<&Value>, code: &'static str) -> Result<(), Failure> {
    match value {
        Some(value) => positive_u64(value, code).map(|_| ()),
        None => Ok(()),
    }
}

fn parse_px(value: &Value, scale: DecimalScale) -> Result<Px, &'static str> {
    let text = value.as_str().ok_or("invalid_price")?;
    Px::parse(text, scale).map_err(|error| numeric_code(error, "price"))
}

fn parse_qty(value: &Value, scale: DecimalScale) -> Result<Qty, &'static str> {
    let text = value.as_str().ok_or("invalid_quantity")?;
    Qty::parse(text, scale).map_err(|error| numeric_code(error, "quantity"))
}

fn numeric_code(error: replay_domain::NumericError, field: &'static str) -> &'static str {
    match (error, field) {
        (replay_domain::NumericError::Overflow, "price") => "price_overflow",
        (replay_domain::NumericError::Underflow, "price") => "price_underflow",
        (replay_domain::NumericError::InexactRescale, "price") => "inexact_price",
        (replay_domain::NumericError::Overflow, "quantity") => "quantity_overflow",
        (replay_domain::NumericError::Underflow, "quantity") => "quantity_underflow",
        (replay_domain::NumericError::InexactRescale, "quantity") => "inexact_quantity",
        (_, "price") => "invalid_price",
        _ => "invalid_quantity",
    }
}
