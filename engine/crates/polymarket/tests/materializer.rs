use std::fs;
use std::io::{BufRead, BufReader, Cursor};

use canonical_normalizer::{Normalize, Normalizer};
use indexer_finalize::{
    CanonicalOutput, CompressionContract as CanonicalCompression, DecodedIdentity, InputSegment,
    Receipt as CanonicalReceipt, StoredIdentity as CanonicalStored, window_directory,
};
use indexer_types::{ContentHash, EnvelopeView, Sha256};
use polymarket_normalizer::Polymarket;
use prediction_encoder::{
    DEFAULT_ZSTD_LEVEL, LogicalIdentity as CodecLogical, StoredIdentity as CodecStored,
    StreamingDecoder, encode_stream, encoder_version,
};
use replay_domain::{FaultImpact, SEGMENT_SCHEMA_VERSION, SegmentEvent, SegmentRecord};
use replay_materialize::{
    BuildDisposition, DerivativeSpec, NormalizationPolicy, build_window, verify_derivative,
};
use serde_json::{Value, json};
use tempdir::TempDir;

const PM_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SNAPSHOT_SHA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BOOK: &str = include_str!("fixtures/book.json");
const PRICE_CHANGE: &str = include_str!("fixtures/price_change.json");
const REST_BOOK: &str = include_str!("fixtures/rest_book.json");
const TICK_SIZE: &str = include_str!("fixtures/tick_size_change.json");

fn envelope(seq: u64, delivery_index: u64, lane: &str, stream: &str, payload: &str) -> Vec<u8> {
    let cursor = if stream == "public_snapshot" {
        let source_time_ms = serde_json::from_str::<Value>(payload).unwrap()["timestamp"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        json!({"type":"snapshot","source_time_ms":source_time_ms})
    } else {
        json!({"type":"unsequenced","counter":delivery_index})
    };
    format!(
        "{}\n",
        json!({
            "envelope_version":2,
            "delivery_index":delivery_index,
            "record_id":format!("pm-{lane}-{delivery_index}"),
            "visible_ns":seq,
            "monotonic_ns":seq,
            "venue":"polymarket",
            "stream":stream,
            "connection_epoch":"e",
            "local_counter":delivery_index,
            "source_cursor":cursor,
            "kind":"venue_frame",
            "raw_payload":payload,
        })
    )
    .into_bytes()
}

fn encoded_output(directory: &std::path::Path, name: &str, logical: &[u8]) -> CanonicalOutput {
    let mut stored = Vec::new();
    let encoded = encode_stream(Cursor::new(logical), &mut stored, DEFAULT_ZSTD_LEVEL).unwrap();
    fs::write(directory.join(name), stored).unwrap();
    CanonicalOutput {
        file: name.to_owned(),
        content_encoding: "zstd".to_owned(),
        decoded: DecodedIdentity {
            byte_length: encoded.logical.byte_length,
            line_count: encoded.logical.line_count,
            sha256: encoded.logical.sha256,
        },
        stored: CanonicalStored {
            byte_length: encoded.stored.byte_length,
            sha256: encoded.stored.sha256,
        },
        compression: CanonicalCompression {
            algorithm: "zstd".to_owned(),
            level: DEFAULT_ZSTD_LEVEL,
            frame_checksum: true,
            dictionary: None,
            frame_count: 1,
            encoder: encoder_version(),
        },
    }
}

fn canonical_fixture(root: &std::path::Path) {
    let directory = window_directory(root, 0);
    fs::create_dir_all(&directory).unwrap();
    let mut malformed_price_change: Value = serde_json::from_str(PRICE_CHANGE).unwrap();
    malformed_price_change["price_changes"][1]["size"] = json!("bad");
    let malformed_price_change = malformed_price_change.to_string();
    let rows = [
        ("polymarket", "public_book", BOOK.trim(), PM_SHA, 1_u64),
        ("polymarket", "public_book", PRICE_CHANGE.trim(), PM_SHA, 2),
        ("polymarket", "public_book", PRICE_CHANGE.trim(), PM_SHA, 3),
        (
            "polymarket",
            "public_book",
            malformed_price_change.as_str(),
            PM_SHA,
            4,
        ),
        (
            "polymarket_snapshots",
            "public_snapshot",
            REST_BOOK.trim(),
            SNAPSHOT_SHA,
            1,
        ),
        ("polymarket", "public_book", TICK_SIZE.trim(), PM_SHA, 5),
        ("polymarket", "public_book", "PONG", PM_SHA, 6),
    ];
    let mut evidence = Vec::new();
    let mut provenance = Vec::new();
    for (offset, (lane, stream, payload, source_sha, source_line)) in rows.iter().enumerate() {
        let seq = u64::try_from(offset + 1).unwrap();
        let encoded = envelope(seq, *source_line, lane, stream, payload);
        let view = EnvelopeView::parse(&encoded).unwrap();
        evidence.extend_from_slice(&encoded);
        provenance.extend_from_slice(
            format!(
                "{}\n",
                json!({
                    "canonical_seq":seq,
                    "lane_id":lane,
                    "source_segment_sha256":source_sha,
                    "source_line_number":source_line,
                    "record_id":view.record_id.as_str(),
                    "content_hash":ContentHash::hash(view.raw_payload.as_bytes()).to_hex(),
                    "continuity_verdict":if *stream == "public_snapshot" {"sparse_monotonic"} else {"unsequenced_venue"},
                    "visible_tie_group":null,
                })
            )
            .as_bytes(),
        );
    }
    let receipt = CanonicalReceipt {
        receipt_version: 1,
        window_start_ns: 0,
        window_end_ns: 10,
        completeness: "complete".to_owned(),
        certified: true,
        expected_lanes: vec!["polymarket".to_owned(), "polymarket_snapshots".to_owned()],
        present_lanes: vec!["polymarket".to_owned(), "polymarket_snapshots".to_owned()],
        unexpected_lanes: Vec::new(),
        missing_lanes: Vec::new(),
        invalid_lanes: Vec::new(),
        finalization_deadline_seconds: 300,
        deadline_expired: false,
        finalized_at_ns: 10,
        inputs: vec![
            InputSegment {
                lane: "polymarket".to_owned(),
                data_file: "pm.ndjson".to_owned(),
                segment_index: 0,
                line_count: 6,
                sha256: PM_SHA.to_owned(),
                first_delivery_index: Some(1),
                last_delivery_index: Some(6),
            },
            InputSegment {
                lane: "polymarket_snapshots".to_owned(),
                data_file: "snapshot.ndjson".to_owned(),
                segment_index: 0,
                line_count: 1,
                sha256: SNAPSHOT_SHA.to_owned(),
                first_delivery_index: Some(1),
                last_delivery_index: Some(1),
            },
        ],
        evidence: encoded_output(&directory, "evidence.ndjson.zst", &evidence),
        provenance: encoded_output(&directory, "provenance.ndjson.zst", &provenance),
        first_canonical_seq: Some(1),
        last_canonical_seq: Some(7),
        carried: Default::default(),
        clock_faults: Vec::new(),
        finalizer_version: 1,
    };
    let mut bytes = serde_json::to_vec_pretty(&receipt).unwrap();
    bytes.push(b'\n');
    fs::write(directory.join("receipt.json"), bytes).unwrap();
}

fn read_events(derivative: &replay_materialize::VerifiedDerivative) -> Vec<SegmentRecord> {
    let output = &derivative.manifest.events;
    let file = fs::File::open(derivative.directory.join(&output.file)).unwrap();
    let logical = CodecLogical {
        sha256: output.logical.sha256.as_hex(),
        byte_length: output.logical.byte_length,
        line_count: output.logical.line_count,
    };
    let stored = CodecStored {
        sha256: output.stored.sha256.as_hex(),
        byte_length: output.stored.byte_length,
    };
    let decoder =
        StreamingDecoder::new(file, &logical, Some(&stored), Some(logical.byte_length)).unwrap();
    let mut reader = BufReader::new(decoder);
    let mut records = Vec::new();
    let mut line = Vec::new();
    while reader.read_until(b'\n', &mut line).unwrap() != 0 {
        records.push(SegmentRecord::from_canonical_json(&line[..line.len() - 1]).unwrap());
        line.clear();
    }
    reader.into_inner().finish().unwrap();
    records
}

fn spec(normalizer: &Normalizer<Polymarket>) -> DerivativeSpec {
    DerivativeSpec {
        normalized_schema_version: SEGMENT_SCHEMA_VERSION,
        normalizer_bundle_sha256: normalizer.descriptor().bundle_sha256,
        normalizer_config_sha256: normalizer.descriptor().config_sha256,
        policy: NormalizationPolicy {
            policy_sha256: Sha256::digest(b"polymarket-normalization-policy-v1"),
            effective_from_ns: 0,
            effective_until_ns: Some(10),
        },
    }
}

#[test]
fn materializes_verifies_and_idempotently_retries_polymarket_derivative() {
    let canonical = TempDir::new("polymarket-canonical").unwrap();
    let output = TempDir::new("polymarket-normalized").unwrap();
    canonical_fixture(canonical.path());
    let mut first_normalizer = Normalizer::new(Polymarket::default()).unwrap();
    let spec = spec(&first_normalizer);
    let first = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec,
        &mut first_normalizer,
    )
    .unwrap();
    assert_eq!(first.disposition, BuildDisposition::Committed);
    assert_eq!(first.derivative.manifest.counts.input_records, 7);
    assert_eq!(first.derivative.manifest.counts.accepted_events, 6);
    assert_eq!(first.derivative.manifest.counts.rejected_source_records, 2);
    assert_eq!(
        first.derivative.manifest.counts.normalization_fault_events,
        2
    );
    assert_eq!(
        first
            .derivative
            .manifest
            .counts
            .intentionally_ignored_records,
        1
    );
    let verified = verify_derivative(&first.derivative.directory).unwrap();
    assert_eq!(verified.pin, first.derivative.pin);
    let records = read_events(&verified);
    let malformed_fault = records
        .iter()
        .find(|record| record.header().address().canonical_seq() == 4)
        .expect("malformed price-change fault must be persisted");
    assert!(matches!(
        malformed_fault.event(),
        SegmentEvent::NormalizationFault(fault)
            if matches!(fault.impact(), FaultImpact::UnattributedLane(_))
    ));

    let retry = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec,
        &mut Normalizer::new(Polymarket::default()).unwrap(),
    )
    .unwrap();
    assert_eq!(retry.disposition, BuildDisposition::VerifiedNoOp);
    assert_eq!(retry.derivative.pin, first.derivative.pin);
}
