use canonical_normalizer::{Normalization, Normalize, Normalizer, segment_record};
use indexer_finalize::{ContinuityVerdict, EventAddress, JoinedCanonicalRecord};
use indexer_types::{ContentHash, Sha256};
use limitless_normalizer::{Config, Limitless, NORMALIZER_BUNDLE_ID};
use replay_domain::{BookEvent, FaultImpact, SegmentEvent};
use serde_json::{Value, json};

const CAPTURED_BOOK: &str = include_str!("fixtures/orderbook_update_live_2026_09_12.json");
const DOCUMENTED_BOOK: &str = include_str!("fixtures/orderbook_update_documented.json");
const PRICE_DATA: &str = include_str!("fixtures/new_price_data_documented.json");
const CREATED: &str = include_str!("fixtures/market_created_documented.json");
const RESOLVED: &str = include_str!("fixtures/market_resolved_documented.json");
const SYSTEM: &str = include_str!("fixtures/system_live_2026_09_12.json");

fn source(payload: &str, stream: &str, cursor: Value) -> JoinedCanonicalRecord {
    source_with_kind(
        payload,
        stream,
        cursor,
        if stream == "process" {
            "control"
        } else {
            "venue_frame"
        },
    )
}

fn source_with_kind(
    payload: &str,
    stream: &str,
    cursor: Value,
    kind: &str,
) -> JoinedCanonicalRecord {
    let envelope = format!(
        "{}\n",
        json!({
            "envelope_version":2,"delivery_index":41,"record_id":"lm-epoch-41",
            "visible_ns":1_789_240_000_000_000_000_u64,"monotonic_ns":600,
            "venue":"limitless","stream":stream,"connection_epoch":"epoch",
            "local_counter":9,"source_cursor":cursor,
            "kind":kind,
            "raw_payload":payload,
        })
    )
    .into_bytes();
    JoinedCanonicalRecord {
        envelope,
        canonical_seq: 17,
        order_ns: 1_789_240_000_000_000_000,
        visible_ns: 1_789_240_000_000_000_000,
        visible_tie_group: Some(4),
        event_address: EventAddress {
            canonical_seq: 17,
            lane_id: "limitless".to_owned(),
            delivery_index: 41,
        },
        record_id: "lm-epoch-41".to_owned(),
        source_segment_sha256: Sha256::digest(b"source-segment"),
        source_line_number: 6,
        content_hash: Sha256::from_bytes(*ContentHash::hash(payload.as_bytes()).as_bytes()),
        continuity: ContinuityVerdict::SparseMonotonic,
    }
}

fn book_source(payload: &str) -> JoinedCanonicalRecord {
    let value: Value = serde_json::from_str(payload).unwrap();
    source(
        payload.trim(),
        "public_book",
        json!({"type":"snapshot","last_update_id":value["data"]["version"]}),
    )
}

fn normalize(source: &JoinedCanonicalRecord) -> Normalization {
    Normalizer::new(Limitless::default())
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

fn reject(value: Normalization) -> canonical_normalizer::ParseReject {
    match value {
        Normalization::Reject(reject) => reject,
        other => panic!("expected reject, got {other:?}"),
    }
}

#[test]
fn descriptor_binds_bundle_and_every_config_variable() {
    let default = Normalizer::new(Limitless::default()).unwrap();
    let identity = serde_json::to_vec(&json!({
        "schema_version":1,
        "variables":{
            "price_scale":{"type":"unsigned","value":3},
            "quantity_scale":{"type":"unsigned","value":6}
        }
    }))
    .unwrap();
    assert_eq!(
        default.descriptor().config_sha256,
        Sha256::digest(&identity)
    );
    assert_eq!(
        default.descriptor().bundle_sha256,
        Sha256::digest(NORMALIZER_BUNDLE_ID.as_bytes())
    );
    let changed = Config {
        quantity_scale: replay_domain::DecimalScale::new(7).unwrap(),
        ..Config::default()
    };
    assert_ne!(
        Normalizer::new(Limitless::try_from(changed).unwrap())
            .unwrap()
            .descriptor()
            .config_sha256,
        default.descriptor().config_sha256
    );
    assert!(
        Limitless::try_from(Config {
            price_scale: replay_domain::DecimalScale::new(2).unwrap(),
            ..Config::default()
        })
        .is_err()
    );
}

#[test]
fn retained_fixture_is_one_sorted_replacement_book() {
    let normalized = events(normalize(&book_source(CAPTURED_BOOK)));
    let [SegmentEvent::Book(BookEvent::Full(book))] = normalized.as_slice() else {
        panic!("expected one full book")
    };
    assert_eq!(
        book.instrument().as_str(),
        "limitless:btc-up-or-down-5-min-1789242900"
    );
    assert_eq!(book.bids()[0].price().atoms(), 555);
    assert_eq!(book.bids()[0].quantity().atoms(), 20_000_000);
    assert_eq!(
        book.asks()
            .iter()
            .map(|level| level.price().atoms())
            .collect::<Vec<_>>(),
        [657, 989, 995, 998]
    );
    assert_eq!(book.source_observed_ns(), Some(1_789_243_055_873_000_000));
    assert!(book.snapshot_hash().is_none());
}

#[test]
fn compact_fixture_preserves_raw_six_decimal_quantity_atoms() {
    let normalized = events(normalize(&book_source(DOCUMENTED_BOOK)));
    let SegmentEvent::Book(BookEvent::Full(book)) = &normalized[0] else {
        panic!("expected full book")
    };
    assert_eq!(book.bids()[0].quantity().atoms(), 100_000_000);
    assert_eq!(book.bids()[1].quantity().atoms(), i64::MAX as u64);
}

#[test]
fn sparse_versions_are_cursor_bound_but_never_become_gap_proof_or_anchors() {
    let mut later: Value = serde_json::from_str(CAPTURED_BOOK).unwrap();
    later["data"]["version"] = json!(9_000_001);
    later["data"]["orderbook"]["bids"] = json!([]);
    let later = later.to_string();
    let later_source = book_source(&later);
    let event = events(normalize(&later_source)).pop().unwrap();
    assert!(matches!(event, SegmentEvent::Book(BookEvent::Full(_))));
    let record = segment_record(&later_source, 0, event).unwrap();
    assert_eq!(
        record.header().provenance().continuity(),
        replay_domain::ContinuityVerdict::SparseMonotonic
    );
    assert!(!matches!(record.event(), SegmentEvent::AuditAnchor(_)));

    let mismatch = source(
        CAPTURED_BOOK.trim(),
        "public_book",
        json!({"type":"snapshot","last_update_id":4_151_380}),
    );
    let rejected = reject(normalize(&mismatch));
    assert_eq!(rejected.error_code, "version_cursor_mismatch");
    assert!(matches!(rejected.impact, FaultImpact::Instrument(_)));
}

#[test]
fn financial_values_never_round_or_pass_through_binary_float() {
    let altered = |price: Value, size: Value| {
        let mut value: Value = serde_json::from_str(CAPTURED_BOOK).unwrap();
        value["data"]["orderbook"]["bids"][0]["price"] = price;
        value["data"]["orderbook"]["bids"][0]["size"] = size;
        value.to_string()
    };
    for (price, size, code) in [
        (json!(0.5551), json!(20_000_000), "inexact_price"),
        (json!(0.0), json!(20_000_000), "price_out_of_venue_range"),
        (json!(1.0), json!(20_000_000), "price_out_of_venue_range"),
        (json!(0.555), json!(0), "non_positive_level_quantity"),
        (json!(0.555), json!(1.5), "invalid_quantity"),
        (
            json!(0.555),
            json!(9_223_372_036_854_775_808_u64),
            "quantity_overflow",
        ),
    ] {
        assert_eq!(
            reject(normalize(&book_source(&altered(price, size)))).error_code,
            code
        );
    }
    let exponent = CAPTURED_BOOK.replacen("0.555", "5.55e-1", 1);
    assert_eq!(
        reject(normalize(&book_source(&exponent))).error_code,
        "inexact_price"
    );

    for (field, value) in [("price", "0.555"), ("size", "20000000")] {
        for marker in ["Number", "RawValue"] {
            let payload = CAPTURED_BOOK.replacen(
                value,
                &format!(r#"{{"$serde_json::private::{marker}":"{value}"}}"#),
                1,
            );
            assert_eq!(
                reject(normalize(&book_source(&payload))).error_code,
                "reserved_json_value_key",
                "{field} accepted serde_json's private {marker} object",
            );
        }
    }
    let nested = CAPTURED_BOOK.replacen(
        "0.555",
        r#"{"$serde_json::private::RawValue":"{\"$serde_json::private::Number\":\"0.555\"}"}"#,
        1,
    );
    assert_eq!(
        reject(normalize(&book_source(&nested))).error_code,
        "reserved_json_value_key"
    );
}

#[test]
fn closed_shapes_reject_unknown_fields_bad_sides_duplicates_and_batches() {
    let mut value: Value = serde_json::from_str(CAPTURED_BOOK).unwrap();
    value["data"]["new"] = json!(true);
    assert_eq!(
        reject(normalize(&book_source(&value.to_string()))).error_code,
        "unknown_field"
    );

    let mut value: Value = serde_json::from_str(CAPTURED_BOOK).unwrap();
    value["data"]["orderbook"]["bids"][0]["side"] = json!("SELL");
    assert_eq!(
        reject(normalize(&book_source(&value.to_string()))).error_code,
        "invalid_level_side"
    );

    let mut value: Value = serde_json::from_str(CAPTURED_BOOK).unwrap();
    let first = value["data"]["orderbook"]["asks"][0].clone();
    value["data"]["orderbook"]["asks"]
        .as_array_mut()
        .unwrap()
        .push(first);
    assert_eq!(
        reject(normalize(&book_source(&value.to_string()))).error_code,
        "invalid_orderbook_levels"
    );

    for (payload, code) in [
        ("[]", "message_not_object"),
        (
            r#"{"event":"orderbookUpdate","data":[]}"#,
            "invalid_orderbook_update",
        ),
        (
            r#"{"event":"futureState","data":{}}"#,
            "unsupported_message_type",
        ),
    ] {
        assert_eq!(
            reject(normalize(&source(
                payload,
                "public_book",
                json!({"type":"unsequenced","counter":9})
            )))
            .error_code,
            code
        );
    }
}

#[test]
fn unsupported_state_is_validated_then_emitted_as_an_instrument_fault() {
    for (payload, code, instrument) in [
        (
            PRICE_DATA,
            "amm_price_state_not_in_replay_domain",
            "limitless:0x1234",
        ),
        (
            CREATED,
            "market_created_not_in_replay_domain",
            "limitless:btc-above-110k-apr-2026",
        ),
        (
            RESOLVED,
            "market_resolved_not_in_replay_domain",
            "limitless:btc-above-110k-apr-2026",
        ),
    ] {
        let rejected = reject(normalize(&source(
            payload.trim(),
            "public_book",
            json!({"type":"unsequenced","counter":9}),
        )));
        assert_eq!(rejected.error_code, code);
        assert_eq!(rejected.instrument_hint.unwrap().as_str(), instrument);
    }
    let mut malformed: Value = serde_json::from_str(RESOLVED).unwrap();
    malformed["data"]["winningIndex"] = json!(1);
    let rejected = reject(normalize(&source(
        &malformed.to_string(),
        "public_book",
        json!({"type":"unsequenced","counter":9}),
    )));
    assert_eq!(rejected.error_code, "inconsistent_winning_outcome");
}

#[test]
fn system_is_a_visible_ignore_while_exception_is_a_fault() {
    let unsequenced = json!({"type":"unsequenced","counter":9});
    assert_eq!(
        normalize(&source(SYSTEM.trim(), "public_book", unsequenced.clone())),
        Normalization::Ignored {
            reason_code: "venue_system_control".to_owned()
        }
    );
    assert_eq!(
        reject(normalize(&source(
            r#"{"event":"exception","data":{"future":"opaque"}}"#,
            "public_book",
            unsequenced,
        )))
        .error_code,
        "venue_exception"
    );
}

#[test]
fn process_connection_and_phase_zero_coordinates_are_preserved() {
    let mut opened = json!({
        "event":"connection_opened","target_digest":"digest","target_count":2,
        "asset_ids":["market-a","market-b"],"targets_path":"targets.json",
        "target_metadata_digest":"metadata","target_metadata_path":"run.json",
        "delivers_deltas":false,"fsync_interval_seconds":1.0,"repaired_bytes_on_start":0,
        "clock_scope":{"lane":"limitless","clock":"monotonic","scope":"boot","scope_id":"id","comparable_across_processes":true,"platform":"linux"},
        "url":"wss://ws.limitless.exchange","namespace":"/markets",
        "events":["newPriceData","orderbookUpdate","marketCreated","marketResolved","system","exception"]
    });
    let normalized = events(normalize(&source(
        &opened.to_string(),
        "process",
        Value::Null,
    )));
    let SegmentEvent::Control(replay_domain::ControlEvent::ConnectionOpened {
        instruments,
        delivers_deltas,
        ..
    }) = &normalized[0]
    else {
        panic!("expected connection opened")
    };
    assert_eq!(instruments[0].as_str(), "limitless:market-a");
    assert!(!delivers_deltas);
    opened["delivers_deltas"] = json!(true);
    assert_eq!(
        reject(normalize(&source(
            &opened.to_string(),
            "process",
            Value::Null
        )))
        .error_code,
        "invalid_control_delivers_deltas"
    );

    let source = book_source(CAPTURED_BOOK);
    let event = events(normalize(&source)).pop().unwrap();
    let record = segment_record(&source, 0, event).unwrap();
    let header = record.header();
    assert_eq!(header.address().canonical_seq(), 17);
    assert_eq!(header.address().lane().as_str(), "limitless");
    assert_eq!(header.address().delivery_index(), 41);
    assert_eq!(header.address().event_index(), 0);
    assert_eq!(header.visible_tie_group(), Some(4));
    assert_eq!(header.provenance().source_line_number(), 6);
    assert_eq!(
        header.provenance().source_segment_sha256(),
        &source.source_segment_sha256
    );
    assert_eq!(header.provenance().content_hash(), &source.content_hash);
}

#[test]
fn every_captured_process_control_is_typed_or_explicitly_ignored() {
    let cursor = Value::Null;
    for (payload, expected) in [
        (
            json!({"event":"connection_closed","seconds_open":1.25,"records_this_epoch":8}),
            "connection_closed",
        ),
        (
            json!({"event":"connection_failed","error_type":"TimeoutError","error":"timed out","seconds_open":2.5,"frames_this_epoch":3}),
            "connection_failed",
        ),
        (
            json!({"event":"subscription_changed","from_digest":null,"to_digest":"next","added":["market-b"],"removed":["market-a"]}),
            "subscription_changed",
        ),
        (
            json!({"event":"target_metadata_changed","target_digest":"targets","from_metadata_digest":null,"to_metadata_digest":"metadata","metadata_path":null}),
            "metadata_changed",
        ),
    ] {
        let normalized = events(normalize(&source(
            &payload.to_string(),
            "process",
            cursor.clone(),
        )));
        let name = match &normalized[0] {
            SegmentEvent::Control(replay_domain::ControlEvent::ConnectionClosed { .. }) => {
                "connection_closed"
            }
            SegmentEvent::Control(replay_domain::ControlEvent::ConnectionFailed { .. }) => {
                "connection_failed"
            }
            SegmentEvent::Control(replay_domain::ControlEvent::SubscriptionChanged { .. }) => {
                "subscription_changed"
            }
            SegmentEvent::Control(replay_domain::ControlEvent::MetadataChanged { .. }) => {
                "metadata_changed"
            }
            other => panic!("unexpected control event: {other:?}"),
        };
        assert_eq!(name, expected);
    }

    for (payload, reason) in [
        (
            json!({"event":"subscription_sent","target_digest":"targets","target_count":2}),
            "subscription_sent",
        ),
        (
            json!({"event":"connection_closing","reason":"time_limit"}),
            "connection_closing",
        ),
        (
            json!({"event":"targets_unreadable","error":"invalid target file"}),
            "targets_unreadable",
        ),
        (
            json!({"event":"frame_not_utf8","bytes":12}),
            "frame_not_utf8",
        ),
    ] {
        assert_eq!(
            normalize(&source(&payload.to_string(), "process", cursor.clone())),
            Normalization::Ignored {
                reason_code: reason.to_owned(),
            }
        );
    }

    assert_eq!(
        reject(normalize(&source(
            r#"{"event":"future_process_state","value":1}"#,
            "process",
            cursor,
        )))
        .error_code,
        "unsupported_control_event"
    );

    for error in ["", "first line\nsecond line\0\u{7f}"] {
        let payload = json!({
            "event":"connection_failed","error_type":"RuntimeError","error":error,
            "seconds_open":2.5,"frames_this_epoch":3
        });
        let normalized = events(normalize(&source_with_kind(
            &payload.to_string(),
            "process",
            Value::Null,
            "fault",
        )));
        let SegmentEvent::Control(replay_domain::ControlEvent::ConnectionFailed { reason, .. }) =
            &normalized[0]
        else {
            panic!("expected connection failure")
        };
        assert!(reason.starts_with("RuntimeError:"));
        assert!(!reason.chars().any(char::is_control));

        let unreadable = json!({"event":"targets_unreadable","error":error});
        assert_eq!(
            normalize(&source_with_kind(
                &unreadable.to_string(),
                "process",
                Value::Null,
                "fault",
            )),
            Normalization::Ignored {
                reason_code: "targets_unreadable".to_owned(),
            }
        );
    }

    let marker_text = json!({
        "event":"targets_unreadable",
        "error":"literal $serde_json::private::Number: text is diagnostic data"
    });
    assert_eq!(
        normalize(&source_with_kind(
            &marker_text.to_string(),
            "process",
            Value::Null,
            "fault",
        )),
        Normalization::Ignored {
            reason_code: "targets_unreadable".to_owned(),
        }
    );
}
