use canonical_normalizer::{Normalization, Normalize, Normalizer, segment_record};
use indexer_finalize::{ContinuityVerdict, EventAddress, JoinedCanonicalRecord};
use indexer_types::{ContentHash, Sha256};
use kalshi_normalizer::{Config, Kalshi, NORMALIZER_BUNDLE_ID};
use replay_domain::{BookEvent, ContractOrientation, LevelChange, SegmentEvent, Side};
use serde_json::{Value, json};

const SNAPSHOT: &str = include_str!("fixtures/orderbook_snapshot.json");
const DELTA: &str = include_str!("fixtures/orderbook_delta.json");
const TRADE: &str = include_str!("fixtures/trade.json");
const TICKER: &str = include_str!("fixtures/ticker.json");

fn source(payload: &str, stream: &str, cursor: Value) -> JoinedCanonicalRecord {
    let envelope = format!(
        "{}\n",
        json!({
            "envelope_version": 2,
            "delivery_index": 41,
            "record_id": "kx-epoch-41",
            "visible_ns": 700,
            "monotonic_ns": 600,
            "venue": "kalshi",
            "stream": stream,
            "connection_epoch": "epoch",
            "local_counter": 9,
            "source_cursor": cursor,
            "kind": if stream == "process" { "control" } else { "venue_frame" },
            "raw_payload": payload,
        })
    )
    .into_bytes();
    JoinedCanonicalRecord {
        envelope,
        canonical_seq: 17,
        order_ns: 700,
        visible_ns: 700,
        visible_tie_group: Some(4),
        event_address: EventAddress {
            canonical_seq: 17,
            lane_id: "kalshi".to_owned(),
            delivery_index: 41,
        },
        record_id: "kx-epoch-41".to_owned(),
        source_segment_sha256: Sha256::digest(b"source-segment"),
        source_line_number: 6,
        content_hash: Sha256::from_bytes(*ContentHash::hash(payload.as_bytes()).as_bytes()),
        continuity: ContinuityVerdict::Continuous,
    }
}

fn sequenced(payload: &str, stream: &str, seq: u64) -> JoinedCanonicalRecord {
    source(
        payload,
        stream,
        json!({"type":"update_range","first":seq,"last":seq,"previous_last":seq-1}),
    )
}

fn normalize(source: &JoinedCanonicalRecord) -> Normalization {
    Normalizer::new(Kalshi::default())
        .unwrap()
        .normalize(source)
        .unwrap()
}

fn events(value: Normalization) -> Vec<SegmentEvent> {
    match value {
        Normalization::Events(events) => events,
        other => panic!("expected events, got {other:?}"),
    }
}

fn reject_code(value: Normalization) -> String {
    match value {
        Normalization::Reject(reject) => reject.error_code,
        other => panic!("expected reject, got {other:?}"),
    }
}

#[test]
fn descriptor_is_versioned_and_config_changes_identity() {
    let default = Normalizer::new(Kalshi::default()).unwrap();
    let default_config = serde_json::to_vec(&json!({
        "schema_version": 2,
        "variables": {
            "price_scale": {"type":"unsigned", "value":4},
            "quantity_scale": {"type":"unsigned", "value":2}
        }
    }))
    .unwrap();
    assert_eq!(
        default.descriptor().config_sha256,
        Sha256::digest(&default_config)
    );
    assert_eq!(
        default.descriptor().bundle_sha256,
        Sha256::digest(NORMALIZER_BUNDLE_ID.as_bytes())
    );
    assert_eq!(
        default.descriptor().config_sha256,
        Sha256::digest(&default_config)
    );
    for changed_config in [
        Config {
            price_scale: replay_domain::DecimalScale::new(3).unwrap(),
            ..Config::default()
        },
        Config {
            quantity_scale: replay_domain::DecimalScale::new(3).unwrap(),
            ..Config::default()
        },
    ] {
        assert_ne!(
            Normalizer::new(Kalshi::from(changed_config))
                .unwrap()
                .descriptor()
                .config_sha256,
            default.descriptor().config_sha256
        );
    }
}

#[test]
fn authoritative_snapshot_is_two_explicit_sorted_bid_books() {
    let normalized = events(normalize(&sequenced(SNAPSHOT.trim(), "public_book", 2)));
    assert_eq!(normalized.len(), 2);
    let books = normalized
        .iter()
        .map(|event| match event {
            SegmentEvent::Book(BookEvent::Full(book)) => book,
            _ => panic!("expected full book"),
        })
        .collect::<Vec<_>>();
    assert_eq!(books[0].orientation(), ContractOrientation::Outcome);
    assert_eq!(books[1].orientation(), ContractOrientation::Complement);
    assert!(books.iter().all(|book| book.asks().is_empty()));
    assert_eq!(
        books[0]
            .bids()
            .iter()
            .map(|level| (level.price().atoms(), level.quantity().atoms()))
            .collect::<Vec<_>>(),
        [(2200, 33_300), (800, 30_000)]
    );
    assert_eq!(
        books[1]
            .bids()
            .iter()
            .map(|level| level.price().atoms())
            .collect::<Vec<_>>(),
        [5600, 5400]
    );
}

#[test]
fn retained_integer_snapshot_schema_rescales_exactly() {
    let payload = json!({
        "type":"orderbook_snapshot","sid":1,"seq":1,
        "msg":{"market_ticker":"KX-TEST","yes":[[51,300]],"no":[[48,120]]}
    })
    .to_string();
    let normalized = events(normalize(&sequenced(&payload, "public_book", 1)));
    let SegmentEvent::Book(BookEvent::Full(yes)) = &normalized[0] else {
        panic!("expected full book")
    };
    assert_eq!(yes.bids()[0].price().atoms(), 5100);
    assert_eq!(yes.bids()[0].quantity().atoms(), 30_000);
}

#[test]
fn relative_delta_is_typed_without_requiring_initialized_state() {
    let normalized = events(normalize(&sequenced(DELTA.trim(), "public_book", 3)));
    let SegmentEvent::Book(BookEvent::Delta(delta)) = &normalized[0] else {
        panic!("expected delta")
    };
    assert_eq!(delta.orientation(), ContractOrientation::Outcome);
    assert_eq!(delta.side(), Side::Bid);
    assert_eq!(delta.price().atoms(), 9600);
    let LevelChange::Decrease(quantity) = delta.change() else {
        panic!("expected decreasing level change")
    };
    assert_eq!(quantity.atoms(), 5400);
}

#[test]
fn delta_sign_is_consumed_once_and_magnitude_stays_positive() {
    let with_delta = |delta: &str| {
        let mut payload: Value = serde_json::from_str(DELTA).unwrap();
        payload["msg"]["delta_fp"] = json!(delta);
        payload.to_string()
    };

    let increased = events(normalize(&sequenced(
        &with_delta("92233720368547758.07"),
        "public_book",
        3,
    )));
    let SegmentEvent::Book(BookEvent::Delta(delta)) = &increased[0] else {
        panic!("expected delta")
    };
    let LevelChange::Increase(quantity) = delta.change() else {
        panic!("expected increasing level change")
    };
    assert_eq!(quantity.atoms(), i64::MAX as u64);

    for (lexeme, code) in [
        ("0.00", "zero_relative_delta"),
        ("-0.00", "zero_relative_delta"),
        ("+1.00", "invalid_quantity"),
        ("1e2", "invalid_quantity"),
        (" 1.00", "invalid_quantity"),
        ("1.001", "inexact_quantity"),
        ("92233720368547758.08", "quantity_overflow"),
        ("-92233720368547758.08", "quantity_overflow"),
    ] {
        assert_eq!(
            reject_code(normalize(
                &sequenced(&with_delta(lexeme), "public_book", 3,)
            )),
            code,
            "unexpected classification for {lexeme:?}"
        );
    }
}

#[test]
fn trade_uses_exact_yes_price_and_validated_direction() {
    let normalized = events(normalize(&sequenced(TRADE.trim(), "public_trade", 2)));
    let SegmentEvent::Trade(trade) = &normalized[0] else {
        panic!("expected trade")
    };
    assert_eq!(trade.orientation(), ContractOrientation::Outcome);
    assert_eq!(trade.price().atoms(), 3600);
    assert_eq!(trade.quantity().atoms(), 13_600);
    assert_eq!(trade.aggressor(), Some(Side::Ask));
}

#[test]
fn historical_trade_without_block_flag_is_unknown_not_false() {
    let mut trade: Value = serde_json::from_str(TRADE).unwrap();
    trade["msg"]
        .as_object_mut()
        .unwrap()
        .remove("is_block_trade");
    assert_eq!(
        events(normalize(&sequenced(&trade.to_string(), "public_trade", 2))).len(),
        1
    );

    trade["msg"]["is_block_trade"] = json!(0);
    assert_eq!(
        reject_code(normalize(&sequenced(&trade.to_string(), "public_trade", 2))),
        "invalid_block_trade"
    );
}

#[test]
fn timestamps_ids_and_counts_reject_negative_or_zero_where_required() {
    let mut trade: Value = serde_json::from_str(TRADE).unwrap();
    for (field, value) in [("ts", json!(-1)), ("ts_ms", json!(0))] {
        let original = trade["msg"][field].clone();
        trade["msg"][field] = value;
        assert_eq!(
            reject_code(normalize(&sequenced(&trade.to_string(), "public_trade", 2))),
            "invalid_source_time"
        );
        trade["msg"][field] = original;
    }

    let mut ticker: Value = serde_json::from_str(TICKER).unwrap();
    ticker["msg"]["dollar_volume"] = json!(-1);
    assert_eq!(
        reject_code(normalize(&source(
            &ticker.to_string(),
            "public_quote",
            json!({"type":"unsequenced","counter":9}),
        ))),
        "invalid_ticker_integer"
    );
}

#[test]
fn snapshot_schema_families_are_explicit_and_empty_current_snapshot_is_valid() {
    let empty = json!({
        "type":"orderbook_snapshot","sid":1,"seq":1,
        "msg":{"market_ticker":"KX-EMPTY","market_id":"id"}
    });
    let normalized = events(normalize(&sequenced(&empty.to_string(), "public_book", 1)));
    assert_eq!(normalized.len(), 2);

    let mut mixed: Value = serde_json::from_str(SNAPSHOT).unwrap();
    mixed["msg"]["yes"] = json!([[51, 1]]);
    assert_eq!(
        reject_code(normalize(&sequenced(&mixed.to_string(), "public_book", 2))),
        "mixed_snapshot_schema"
    );
}

#[test]
fn snapshot_and_trade_quantities_are_positive_at_the_parse_boundary() {
    let mut snapshot: Value = serde_json::from_str(SNAPSHOT).unwrap();
    for (quantity, code) in [
        ("0.00", "non_positive_snapshot_quantity"),
        ("-1.00", "invalid_quantity"),
        ("92233720368547758.08", "quantity_overflow"),
    ] {
        snapshot["msg"]["yes_dollars_fp"][0][1] = json!(quantity);
        assert_eq!(
            reject_code(normalize(&sequenced(
                &snapshot.to_string(),
                "public_book",
                2,
            ))),
            code
        );
    }

    let mut trade: Value = serde_json::from_str(TRADE).unwrap();
    for (quantity, code) in [
        ("0.00", "non_positive_trade_quantity"),
        ("-1.00", "invalid_quantity"),
        ("92233720368547758.08", "quantity_overflow"),
    ] {
        trade["msg"]["count_fp"] = json!(quantity);
        assert_eq!(
            reject_code(normalize(
                &sequenced(&trade.to_string(), "public_trade", 2,)
            )),
            code
        );
    }
}

#[test]
fn ticker_is_strictly_validated_then_explicitly_ignored() {
    let ticker_source = source(
        TICKER.trim(),
        "public_quote",
        json!({"type":"unsequenced","counter":9}),
    );
    assert_eq!(
        normalize(&ticker_source),
        Normalization::Ignored {
            reason_code: "ticker_not_in_replay_domain".to_owned()
        }
    );
}

#[test]
fn venue_controls_validate_nested_values_and_sequence_edges() {
    let subscribed = r#"{"id":1,"type":"subscribed","msg":{"channel":"trade","sid":2}}"#;
    assert_eq!(
        normalize(&source(
            subscribed,
            "public_book",
            json!({"type":"unsequenced","counter":9}),
        )),
        Normalization::Ignored {
            reason_code: "venue_control_not_in_replay_domain".to_owned()
        }
    );
    let unsubscribed = r#"{"id":2,"sid":2,"seq":4,"type":"unsubscribed"}"#;
    assert_eq!(
        normalize(&sequenced(unsubscribed, "public_book", 4)),
        Normalization::Ignored {
            reason_code: "venue_control_not_in_replay_domain".to_owned()
        }
    );
    let malformed_error = r#"{"id":2,"type":"error","msg":{"code":"27","msg":"slow down"}}"#;
    assert_eq!(
        reject_code(normalize(&source(
            malformed_error,
            "public_book",
            json!({"type":"unsequenced","counter":9}),
        ))),
        "invalid_error_code"
    );
    let malformed_ok = r#"{"id":2,"type":"ok","msg":{"market_tickers":[1]}}"#;
    assert_eq!(
        reject_code(normalize(&source(
            malformed_ok,
            "public_book",
            json!({"type":"unsequenced","counter":9}),
        ))),
        "invalid_ok_msg"
    );
}

#[test]
fn arrays_flatten_in_source_order_and_empty_array_has_zero_children() {
    let snapshot: Value = serde_json::from_str(SNAPSHOT).unwrap();
    let mut delta: Value = serde_json::from_str(DELTA).unwrap();
    delta["seq"] = json!(3);
    let batch = json!([snapshot, delta]).to_string();
    let batch_source = source(
        &batch,
        "public_book",
        json!({"type":"unsequenced","counter":9}),
    );
    let normalized = events(normalize(&batch_source));
    assert_eq!(normalized.len(), 3);
    assert!(matches!(
        normalized[0],
        SegmentEvent::Book(BookEvent::Full(_))
    ));
    assert!(matches!(
        normalized[1],
        SegmentEvent::Book(BookEvent::Full(_))
    ));
    assert!(matches!(
        normalized[2],
        SegmentEvent::Book(BookEvent::Delta(_))
    ));
    assert!(
        events(normalize(&source(
            "[]",
            "public_book",
            json!({"type":"unsequenced","counter":9}),
        )))
        .is_empty()
    );
    assert_eq!(
        reject_code(normalize(&sequenced(&batch, "public_book", 2))),
        "batch_cursor_mismatch"
    );
}

#[test]
fn captured_arrays_route_children_by_type_and_reject_as_one_delivery() {
    let trade: Value = serde_json::from_str(TRADE).unwrap();
    let ticker: Value = serde_json::from_str(TICKER).unwrap();
    let batch_source = source(
        &json!([trade, ticker]).to_string(),
        "public_book",
        json!({"type":"unsequenced","counter":9}),
    );
    let normalized = events(normalize(&batch_source));
    assert_eq!(normalized.len(), 1);
    assert!(matches!(normalized[0], SegmentEvent::Trade(_)));

    let mut valid: Value = serde_json::from_str(DELTA).unwrap();
    valid["msg"]["market_ticker"] = json!("KX-A");
    let mut invalid = valid.clone();
    invalid["msg"]["market_ticker"] = json!("KX-B");
    invalid["msg"]["delta_fp"] = json!("bad");
    let rejected = normalize(&source(
        &json!([valid, invalid]).to_string(),
        "public_book",
        json!({"type":"unsequenced","counter":9}),
    ));
    let Normalization::Reject(reject) = rejected else {
        panic!("expected atomic batch reject")
    };
    assert!(matches!(
        reject.impact,
        replay_domain::FaultImpact::UnattributedLane(_)
    ));
    assert!(reject.instrument_hint.is_none());
}

#[test]
fn malformed_unicode_decimal_is_a_stable_reject_not_a_panic() {
    let mut delta: Value = serde_json::from_str(DELTA).unwrap();
    for value in ["0.000é", "1.0é"] {
        delta["msg"]["delta_fp"] = json!(value);
        assert_eq!(
            reject_code(normalize(&sequenced(&delta.to_string(), "public_book", 3))),
            "invalid_quantity"
        );
    }
}

#[test]
fn malformed_unknown_and_ambiguous_shapes_have_stable_reject_codes() {
    let cases = [
        ("not-json", "invalid_json"),
        (r#"{"type":"future_shape"}"#, "unsupported_message_type"),
        (
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"extra":0,"msg":{}}"#,
            "unknown_field",
        ),
        (
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"KX-X","price_dollars":0.5,"delta_fp":"1.00","side":"yes"}}"#,
            "invalid_price",
        ),
        (
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"KX-X","price_dollars":"0.50001","delta_fp":"1.00","side":"yes"}}"#,
            "inexact_price",
        ),
        (
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"KX-X","price_dollars":"1.0001","delta_fp":"1.00","side":"yes"}}"#,
            "price_out_of_range",
        ),
        (
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"KX-X","price_dollars":"0.5000","delta_fp":"0.00","side":"yes"}}"#,
            "zero_relative_delta",
        ),
    ];
    for (payload, expected) in cases {
        assert_eq!(
            reject_code(normalize(&sequenced(payload, "public_book", 1))),
            expected
        );
    }
    assert_eq!(
        reject_code(normalize(&sequenced(DELTA.trim(), "public_book", 4))),
        "sequence_cursor_mismatch"
    );
}

#[test]
fn process_lifecycle_is_typed_and_unknown_control_fields_reject() {
    let opened = json!({
        "event":"connection_opened",
        "target_count":2,
        "asset_ids":["KX-A","KX-B"],
        "delivers_deltas":true,
        "target_digest":"digest",
        "targets_path":"targets.json",
        "target_metadata_digest":"metadata",
        "target_metadata_path":"run.json",
        "fsync_interval_seconds":1.0,
        "repaired_bytes_on_start":0,
        "clock_scope":{"lane":"kalshi","clock":"monotonic","scope":"boot","scope_id":"id","comparable_across_processes":true,"platform":"linux"},
        "url":"wss://example.invalid",
        "channels":["orderbook_delta","trade"],
        "send_initial_snapshot":true,
        "verified_against_live_socket":false,
        "snapshot_sweep_seconds":30.0,
        "snapshot_max_age_seconds":600.0,
        "snapshot_request_cooldown_seconds":60.0,
        "key_id":null
    })
    .to_string();
    let opened_source = source(&opened, "process", Value::Null);
    let normalized = events(normalize(&opened_source));
    let SegmentEvent::Control(replay_domain::ControlEvent::ConnectionOpened {
        epoch,
        instruments,
        delivers_deltas,
        target_digest,
    }) = &normalized[0]
    else {
        panic!("expected connection opened")
    };
    assert_eq!(epoch, "epoch");
    assert_eq!(instruments[0].as_str(), "kalshi:KX-A");
    assert!(*delivers_deltas);
    assert_eq!(target_digest.as_deref(), Some("digest"));

    let malformed = json!({
        "event":"connection_closed","seconds_open":1,"records_this_epoch":2,"new":true
    })
    .to_string();
    assert_eq!(
        reject_code(normalize(&source(&malformed, "process", Value::Null))),
        "unknown_field"
    );
}

#[test]
fn process_control_values_are_validated_and_channels_are_open_world() {
    let valid_unknown_channel =
        json!({"id":1,"type":"subscribed","msg":{"channel":"future_channel","sid":2}});
    assert!(matches!(
        normalize(&source(
            &valid_unknown_channel.to_string(),
            "public_book",
            json!({"type":"unsequenced","counter":9}),
        )),
        Normalization::Ignored { .. }
    ));

    for payload in [
        json!({"event":"connection_closed","seconds_open":-0.1,"records_this_epoch":2}),
        json!({"event":"connection_failed","error_type":"IOError","error":"x","seconds_open":1,"frames_this_epoch":-1}),
        json!({"event":"subscription_sent","target_digest":"digest","target_count":-1}),
        json!({"event":"frame_not_utf8","bytes":0}),
        json!({"event":"orderbook_reconciliation_request","sid":1,"command_id":0,"market_tickers":["KX"],"reason":"stale"}),
    ] {
        assert!(matches!(
            normalize(&source(&payload.to_string(), "process", Value::Null)),
            Normalization::Reject(_)
        ));
    }
}

#[test]
fn canonical_record_retains_every_address_and_provenance_coordinate() {
    let source = sequenced(DELTA.trim(), "public_book", 3);
    let event = events(normalize(&source)).pop().unwrap();
    let record = segment_record(&source, 7, event).unwrap();
    let decoded =
        replay_domain::SegmentRecord::from_canonical_json(&record.to_canonical_json()).unwrap();
    let header = decoded.header();
    assert_eq!(header.address().canonical_seq(), 17);
    assert_eq!(header.address().lane().as_str(), "kalshi");
    assert_eq!(header.address().delivery_index(), 41);
    assert_eq!(header.address().event_index(), 7);
    assert_eq!(header.visible_tie_group(), Some(4));
    assert_eq!(header.record_id(), "kx-epoch-41");
    assert_eq!(header.provenance().source_line_number(), 6);
    assert_eq!(
        header.provenance().source_segment_sha256(),
        &source.source_segment_sha256
    );
    assert_eq!(header.provenance().content_hash(), &source.content_hash);
}
