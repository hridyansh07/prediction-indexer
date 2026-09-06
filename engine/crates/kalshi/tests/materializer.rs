use std::fs;
use std::io::Cursor;

use indexer_finalize::{
    CanonicalOutput, CompressionContract as CanonicalCompression, DecodedIdentity, InputSegment,
    Receipt as CanonicalReceipt, StoredIdentity as CanonicalStored, window_directory,
};
use indexer_types::{ContentHash, EnvelopeView, Sha256};
use prediction_encoder::{DEFAULT_ZSTD_LEVEL, encode_stream, encoder_version};
use replay_domain::SEGMENT_SCHEMA_VERSION;
use replay_kalshi::Kalshi;
use replay_materialize::{
    BuildDisposition, DerivativeSpec, NormalizationPolicy, build_window, verify_derivative,
};
use replay_normalize::{Normalize, Normalizer};
use serde_json::json;
use tempdir::TempDir;

const SOURCE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SNAPSHOT: &str = include_str!("fixtures/orderbook_snapshot.json");

fn envelope(seq: u64, payload: &str) -> Vec<u8> {
    let cursor = if payload == "[]" {
        json!({"type":"unsequenced","counter":seq})
    } else {
        json!({"type":"update_range","first":seq,"last":seq,"previous_last":seq-1})
    };
    format!(
        "{}\n",
        json!({
            "envelope_version":2,
            "delivery_index":seq,
            "record_id":format!("kx-e-{seq}"),
            "visible_ns":seq,
            "monotonic_ns":seq,
            "venue":"kalshi",
            "stream":"public_book",
            "connection_epoch":"e",
            "local_counter":seq,
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
    let mut snapshot: serde_json::Value = serde_json::from_str(SNAPSHOT).unwrap();
    snapshot["seq"] = json!(1);
    let payloads = [
        snapshot.to_string(),
        r#"{"type":"unknown"}"#.to_owned(),
        "[]".to_owned(),
    ];
    let mut evidence = Vec::new();
    let mut provenance = Vec::new();
    for (offset, payload) in payloads.iter().enumerate() {
        let seq = u64::try_from(offset + 1).unwrap();
        let encoded = envelope(seq, payload);
        let view = EnvelopeView::parse(&encoded).unwrap();
        evidence.extend_from_slice(&encoded);
        provenance.extend_from_slice(
            format!(
                "{}\n",
                json!({
                    "canonical_seq":seq,
                    "lane_id":"kalshi",
                    "source_segment_sha256":SOURCE_SHA,
                    "source_line_number":seq,
                    "record_id":view.record_id.as_str(),
                    "content_hash":ContentHash::hash(view.raw_payload.as_bytes()).to_hex(),
                    "continuity_verdict":"continuous",
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
        expected_lanes: vec!["kalshi".to_owned()],
        present_lanes: vec!["kalshi".to_owned()],
        unexpected_lanes: Vec::new(),
        missing_lanes: Vec::new(),
        invalid_lanes: Vec::new(),
        finalization_deadline_seconds: 300,
        deadline_expired: false,
        finalized_at_ns: 10,
        inputs: vec![InputSegment {
            lane: "kalshi".to_owned(),
            data_file: "source.ndjson".to_owned(),
            segment_index: 0,
            line_count: 3,
            sha256: SOURCE_SHA.to_owned(),
            first_delivery_index: Some(1),
            last_delivery_index: Some(3),
        }],
        evidence: encoded_output(&directory, "evidence.ndjson.zst", &evidence),
        provenance: encoded_output(&directory, "provenance.ndjson.zst", &provenance),
        first_canonical_seq: Some(1),
        last_canonical_seq: Some(3),
        carried: Default::default(),
        clock_faults: Vec::new(),
        finalizer_version: 1,
    };
    let mut bytes = serde_json::to_vec_pretty(&receipt).unwrap();
    bytes.push(b'\n');
    fs::write(directory.join("receipt.json"), bytes).unwrap();
}

fn spec(normalizer: &Normalizer<Kalshi>) -> DerivativeSpec {
    DerivativeSpec {
        normalized_schema_version: SEGMENT_SCHEMA_VERSION,
        normalizer_bundle_sha256: normalizer.descriptor().bundle_sha256,
        normalizer_config_sha256: normalizer.descriptor().config_sha256,
        policy: NormalizationPolicy {
            policy_sha256: Sha256::digest(b"kalshi-normalization-policy-v1"),
            effective_from_ns: 0,
            effective_until_ns: Some(10),
        },
    }
}

#[test]
fn materializes_verifies_and_idempotently_retries_kalshi_derivative() {
    let canonical = TempDir::new("kalshi-canonical").unwrap();
    let output = TempDir::new("kalshi-normalized").unwrap();
    canonical_fixture(canonical.path());
    let mut first_normalizer = Normalizer::new(Kalshi::default()).unwrap();
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
    assert_eq!(first.derivative.manifest.counts.input_records, 3);
    assert_eq!(first.derivative.manifest.counts.accepted_events, 2);
    assert_eq!(first.derivative.manifest.counts.rejected_source_records, 1);
    assert_eq!(
        first.derivative.manifest.counts.normalization_fault_events,
        1
    );
    assert_eq!(
        first
            .derivative
            .manifest
            .counts
            .intentionally_ignored_records,
        1
    );
    let independently_verified = verify_derivative(&first.derivative.directory).unwrap();
    assert_eq!(independently_verified.pin, first.derivative.pin);

    let retry = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec,
        &mut Normalizer::new(Kalshi::default()).unwrap(),
    )
    .unwrap();
    assert_eq!(retry.disposition, BuildDisposition::VerifiedNoOp);
    assert_eq!(retry.derivative.pin, first.derivative.pin);
}
