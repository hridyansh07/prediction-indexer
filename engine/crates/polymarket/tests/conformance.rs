use canonical_normalizer::{Normalization, Normalize, Normalizer, segment_record};
use indexer_finalize::{ContinuityVerdict, EventAddress, JoinedCanonicalRecord};
use indexer_types::{ContentHash, Sha256};
use polymarket_normalizer::{Config, NORMALIZER_BUNDLE_ID, Polymarket};
use replay_domain::{
    BookEvent, BookStateHash, ContractOrientation, FaultImpact, LevelChange, SegmentEvent, Side,
};
use serde_json::{Value, json};

const BOOK: &str = include_str!("fixtures/book.json");
const PRICE_CHANGE: &str = include_str!("fixtures/price_change.json");
const TRADE: &str = include_str!("fixtures/last_trade_price.json");
const REST_BOOK: &str = include_str!("fixtures/rest_book.json");
const TICK_SIZE: &str = include_str!("fixtures/tick_size_change.json");

fn source(payload: &str, stream: &str, cursor: Value, kind: &str) -> JoinedCanonicalRecord {
    let envelope = format!(
        "{}\n",
        json!({
            "envelope_version":2,
            "delivery_index":41,
            "record_id":"pm-epoch-41",
            "visible_ns":700,
            "monotonic_ns":600,
            "venue":"polymarket",
            "stream":stream,
            "connection_epoch":"epoch",
            "local_counter":9,
            "source_cursor":cursor,
            "kind":kind,
            "raw_payload":payload,
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
            lane_id: if stream == "public_snapshot" {
                "polymarket_snapshots".to_owned()
            } else {
                "polymarket".to_owned()
            },
            delivery_index: 41,
        },
        record_id: "pm-epoch-41".to_owned(),
        source_segment_sha256: Sha256::digest(b"source-segment"),
        source_line_number: 6,
        content_hash: Sha256::from_bytes(*ContentHash::hash(payload.as_bytes()).as_bytes()),
        continuity: ContinuityVerdict::UnsequencedVenue,
    }
}

fn ws(payload: &str) -> JoinedCanonicalRecord {
    source(
        payload,
        "public_book",
        json!({"type":"unsequenced","counter":9}),
        "venue_frame",
    )
}

fn rest(payload: &str) -> JoinedCanonicalRecord {
    let timestamp = serde_json::from_str::<Value>(payload).unwrap()["timestamp"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    source(
        payload,
        "public_snapshot",
        json!({"type":"snapshot","source_time_ms":timestamp}),
        "venue_frame",
    )
}

fn normalize_with(config: Config, source: &JoinedCanonicalRecord) -> Normalization {
    Normalizer::new(Polymarket::new(config))
        .unwrap()
        .normalize(source)
        .unwrap()
}

fn normalize(source: &JoinedCanonicalRecord) -> Normalization {
    normalize_with(Config::default(), source)
}

fn events(value: Normalization) -> Vec<SegmentEvent> {
    match value {
        Normalization::Events(events) => events,
        other => panic!("expected events, got {other:?}"),
    }
}

fn reject(value: Normalization) -> canonical_normalizer::ParseReject {
    match value {
        Normalization::Reject(reject) => reject,
        other => panic!("expected reject, got {other:?}"),
    }
}

#[test]
fn descriptor_binds_every_behavior_variable_and_bundle_version() {
    let default = Normalizer::new(Polymarket::default()).unwrap();
    let canonical = serde_json::to_vec(&json!({
        "schema_version":1,
        "variables":{
            "accept_additive_fields":{"type":"boolean","value":true},
            "price_scale":{"type":"unsigned","value":4},
            "quantity_scale":{"type":"unsigned","value":6}
        }
    }))
    .unwrap();
    assert_eq!(
        default.descriptor().config_sha256,
        Sha256::digest(&canonical)
    );
    assert_eq!(
        default.descriptor().bundle_sha256,
        Sha256::digest(NORMALIZER_BUNDLE_ID.as_bytes())
    );
    for config in [
        Config {
            price_scale: replay_domain::DecimalScale::new(3).unwrap(),
            ..Config::default()
        },
        Config {
            quantity_scale: replay_domain::DecimalScale::new(5).unwrap(),
            ..Config::default()
        },
        Config {
            accept_additive_fields: false,
            ..Config::default()
        },
    ] {
        assert_ne!(
            Normalizer::new(Polymarket::new(config))
                .unwrap()
                .descriptor()
                .config_sha256,
            default.descriptor().config_sha256
        );
    }
}

#[test]
fn retained_live_ws_book_is_a_sorted_outcome_book_without_invented_hash() {
    let normalized = events(normalize(&ws(BOOK.trim())));
    let SegmentEvent::Book(BookEvent::Full(book)) = &normalized[0] else {
        panic!("expected full book")
    };
    assert_eq!(book.orientation(), ContractOrientation::Outcome);
    assert_eq!(
        book.instrument().as_str(),
        "polymarket:5615282760875985231868508008056959876238536896643315063916840237042205273721"
    );
    assert_eq!(book.bids()[0].price().atoms(), 4800);
    assert_eq!(book.asks()[0].price().atoms(), 4900);
    assert_eq!(book.bids()[1].quantity().atoms(), 1_136_439_700_000);
    assert_eq!(book.snapshot_hash(), None);
    assert_eq!(book.source_observed_ns(), Some(1_788_532_340_847_000_000));
}

#[test]
fn batched_price_changes_preserve_source_order_and_absolute_set_delete_semantics() {
    let normalized = events(normalize(&ws(PRICE_CHANGE.trim())));
    assert_eq!(normalized.len(), 2);
    let SegmentEvent::Book(BookEvent::Delta(first)) = &normalized[0] else {
        panic!("expected first delta")
    };
    let SegmentEvent::Book(BookEvent::Delta(second)) = &normalized[1] else {
        panic!("expected second delta")
    };
    assert_eq!(first.side(), Side::Bid);
    assert_eq!(first.price().atoms(), 3000);
    let LevelChange::Set(quantity) = first.change() else {
        panic!("expected absolute set")
    };
    assert_eq!(quantity.atoms(), 12_366_000_000);
    assert_eq!(second.side(), Side::Ask);
    assert_eq!(second.change(), LevelChange::Delete);
    assert_ne!(first.book_hash(), second.book_hash());
    assert!(matches!(first.book_hash(), Some(BookStateHash::Sha1(_))));
}

#[test]
fn repeated_hash_deliveries_remain_separately_addressed_events() {
    let source_one = ws(PRICE_CHANGE.trim());
    let source_two = JoinedCanonicalRecord {
        canonical_seq: 18,
        event_address: EventAddress {
            canonical_seq: 18,
            lane_id: "polymarket".to_owned(),
            delivery_index: 42,
        },
        ..ws(PRICE_CHANGE.trim())
    };
    let first = segment_record(&source_one, 0, events(normalize(&source_one)).remove(0)).unwrap();
    let second = segment_record(&source_two, 0, events(normalize(&source_two)).remove(0)).unwrap();
    assert_eq!(
        match first.event() {
            SegmentEvent::Book(BookEvent::Delta(delta)) => delta.book_hash(),
            _ => None,
        },
        match second.event() {
            SegmentEvent::Book(BookEvent::Delta(delta)) => delta.book_hash(),
            _ => None,
        }
    );
    assert_eq!(first.header().address().delivery_index(), 41);
    assert_eq!(second.header().address().delivery_index(), 42);
    assert_ne!(
        first.header().address().canonical_seq(),
        second.header().address().canonical_seq()
    );
}

#[test]
fn retained_trade_is_exact_without_fee_or_complement_derivation() {
    let normalized = events(normalize(&ws(TRADE.trim())));
    let SegmentEvent::Trade(trade) = &normalized[0] else {
        panic!("expected trade")
    };
    assert_eq!(trade.orientation(), ContractOrientation::Outcome);
    assert_eq!(trade.price().atoms(), 4700);
    assert_eq!(trade.quantity().atoms(), 5_000_000);
    assert_eq!(trade.aggressor(), Some(Side::Ask));
}

#[test]
fn rest_book_is_typed_audit_evidence_and_never_a_current_book_reset() {
    let normalized = events(normalize(&rest(REST_BOOK.trim())));
    assert_eq!(normalized.len(), 1);
    let SegmentEvent::AuditAnchor(anchor) = &normalized[0] else {
        panic!("REST book must be an audit anchor")
    };
    assert_eq!(anchor.bids()[0].price().atoms(), 30);
    assert_eq!(anchor.asks()[0].price().atoms(), 50);
    assert!(matches!(anchor.snapshot_hash(), BookStateHash::Sha1(_)));
}

#[test]
fn state_hash_is_strict_sha1_not_sha256_or_ambiguous_text() {
    let mut delta: Value = serde_json::from_str(PRICE_CHANGE).unwrap();
    for value in ["abc", &"a".repeat(64), &"A".repeat(40)] {
        delta["price_changes"][0]["hash"] = json!(value);
        assert_eq!(
            reject(normalize(&ws(&delta.to_string()))).error_code,
            "invalid_state_sha1"
        );
    }
}

#[test]
fn exact_fixed_point_boundary_rejects_float_rounding_and_invalid_quantities() {
    let mut delta: Value = serde_json::from_str(PRICE_CHANGE).unwrap();
    for (value, code) in [
        (json!(0.3), "invalid_price"),
        (json!("0.30001"), "inexact_price"),
        (json!("1.0001"), "price_out_of_range"),
        (json!("1e-2"), "invalid_price"),
    ] {
        delta["price_changes"][0]["price"] = value;
        assert_eq!(reject(normalize(&ws(&delta.to_string()))).error_code, code);
    }
    delta = serde_json::from_str(PRICE_CHANGE).unwrap();
    delta["price_changes"][0]["size"] = json!("9223372036854.775808");
    assert_eq!(
        reject(normalize(&ws(&delta.to_string()))).error_code,
        "quantity_overflow"
    );
}

#[test]
fn official_additive_policy_is_explicit_and_known_fields_remain_strict() {
    let mut book: Value = serde_json::from_str(BOOK).unwrap();
    book["future_metadata"] = json!({"opaque":true});
    assert_eq!(events(normalize(&ws(&book.to_string()))).len(), 1);
    assert_eq!(
        reject(normalize_with(
            Config {
                accept_additive_fields: false,
                ..Config::default()
            },
            &ws(&book.to_string()),
        ))
        .error_code,
        "unknown_field"
    );
    book["timestamp"] = json!(1788532340847_u64);
    assert_eq!(
        reject(normalize(&ws(&book.to_string()))).error_code,
        "invalid_source_time"
    );
}

#[test]
fn unsupported_state_bearing_messages_are_validated_then_fault_visible() {
    let tick = reject(normalize(&ws(TICK_SIZE.trim())));
    assert_eq!(tick.error_code, "unsupported_tick_size_change");
    assert!(matches!(tick.impact, FaultImpact::Instrument(_)));

    let mut malformed: Value = serde_json::from_str(TICK_SIZE).unwrap();
    malformed["new_tick_size"] = json!("0.00001");
    assert_eq!(
        reject(normalize(&ws(&malformed.to_string()))).error_code,
        "inexact_price"
    );
    assert_eq!(
        reject(normalize(&ws(r#"{"event_type":"future_state"}"#))).error_code,
        "unsupported_message_type"
    );

    let best = json!({
        "event_type":"best_bid_ask", "market":format!("0x{}", "a".repeat(64)),
        "asset_id":"1", "best_bid":"0.40", "best_ask":"0.60",
        "spread":"0.20", "timestamp":"1788532355000"
    });
    assert_eq!(
        reject(normalize(&ws(&best.to_string()))).error_code,
        "unsupported_best_bid_ask"
    );

    let new_market = json!({
        "event_type":"new_market", "id":"event-1", "question":"Will it happen?",
        "market":format!("0x{}", "b".repeat(64)), "slug":"will-it-happen",
        "assets_ids":["1", "2"], "outcomes":["Yes", "No"],
        "timestamp":"1788532356000"
    });
    assert_eq!(
        reject(normalize(&ws(&new_market.to_string()))).error_code,
        "unsupported_new_market"
    );

    let resolved = json!({
        "event_type":"market_resolved", "id":"event-1",
        "market":format!("0x{}", "b".repeat(64)), "assets_ids":["1", "2"],
        "winning_asset_id":"1", "winning_outcome":"Yes",
        "timestamp":"1788532357000"
    });
    assert_eq!(
        reject(normalize(&ws(&resolved.to_string()))).error_code,
        "unsupported_market_resolved"
    );
}

#[test]
fn malformed_child_rejects_the_whole_delivery_without_partial_acceptance() {
    let valid: Value = serde_json::from_str(BOOK).unwrap();
    let mut invalid: Value = serde_json::from_str(PRICE_CHANGE).unwrap();
    invalid["price_changes"][0]["size"] = json!("bad");
    let rejection = reject(normalize(&ws(&json!([valid, invalid]).to_string())));
    assert_eq!(rejection.error_code, "invalid_quantity");
    assert!(rejection.instrument_hint.is_none());
    assert!(matches!(rejection.impact, FaultImpact::UnattributedLane(_)));
}

#[test]
fn malformed_nested_price_change_batch_has_lane_wide_impact() {
    let mut invalid: Value = serde_json::from_str(PRICE_CHANGE).unwrap();
    invalid["price_changes"][1]["size"] = json!("bad");
    let rejection = reject(normalize(&ws(&invalid.to_string())));
    assert_eq!(rejection.error_code, "invalid_quantity");
    assert!(rejection.instrument_hint.is_none());
    assert!(matches!(rejection.impact, FaultImpact::UnattributedLane(_)));
}

#[test]
fn heartbeat_and_process_lifecycle_are_intentional_or_typed() {
    assert_eq!(
        normalize(&ws("PONG")),
        Normalization::Ignored {
            reason_code: "application_heartbeat".to_owned()
        }
    );
    let opened = json!({
        "event":"connection_opened","target_digest":"digest","target_count":1,
        "asset_ids":["5615282760875985231868508008056959876238536896643315063916840237042205273721"],
        "targets_path":"targets.json","target_metadata_digest":"metadata",
        "target_metadata_path":"run.json","delivers_deltas":true,
        "fsync_interval_seconds":1.0,"repaired_bytes_on_start":0,
        "clock_scope":{"lane":"polymarket","clock":"monotonic","scope":"boot","scope_id":"id","comparable_across_processes":true,"platform":"linux"},
        "url":"wss://example.invalid"
    }).to_string();
    let normalized = events(normalize(&source(
        &opened,
        "process",
        Value::Null,
        "control",
    )));
    let SegmentEvent::Control(replay_domain::ControlEvent::ConnectionOpened {
        instruments,
        delivers_deltas,
        ..
    }) = &normalized[0]
    else {
        panic!("expected connection opened")
    };
    assert_eq!(instruments.len(), 1);
    assert!(*delivers_deltas);
}

#[test]
fn child_records_retain_complete_phase_zero_provenance_and_addresses() {
    let source = ws(PRICE_CHANGE.trim());
    let normalized = events(normalize(&source));
    for (index, event) in normalized.into_iter().enumerate() {
        let record = segment_record(&source, u32::try_from(index).unwrap(), event).unwrap();
        let header = record.header();
        assert_eq!(header.address().canonical_seq(), 17);
        assert_eq!(header.address().lane().as_str(), "polymarket");
        assert_eq!(header.address().delivery_index(), 41);
        assert_eq!(header.address().event_index(), index as u32);
        assert_eq!(header.visible_tie_group(), Some(4));
        assert_eq!(header.record_id(), "pm-epoch-41");
        assert_eq!(header.provenance().source_line_number(), 6);
        assert_eq!(
            header.provenance().source_segment_sha256(),
            &source.source_segment_sha256
        );
        assert_eq!(header.provenance().content_hash(), &source.content_hash);
    }
}
