use std::fs;
use std::io::Cursor;

use canonical_normalizer::{Normalize, Normalizer};
use indexer_finalize::{
    CanonicalOutput, CompressionContract as CanonicalCompression, DecodedIdentity, InputSegment,
    Receipt as CanonicalReceipt, StoredIdentity as CanonicalStored, window_directory,
};
use indexer_types::{ContentHash, EnvelopeView, Sha256};
use limitless_normalizer::Limitless;
use prediction_encoder::{DEFAULT_ZSTD_LEVEL, encode_stream, encoder_version};
use replay_domain::{BookEvent, SEGMENT_SCHEMA_VERSION, SegmentEvent, SegmentRecord};
use replay_materialize::{
    BuildDisposition, DerivativeSpec, NormalizationPolicy, build_window, verify_derivative,
};
use serde_json::{Value, json};
use tempdir::TempDir;

const SOURCE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BOOK: &str = include_str!("fixtures/orderbook_update_live_2026_09_12.json");
const CREATED: &str = include_str!("fixtures/market_created_documented.json");
const SYSTEM: &str = include_str!("fixtures/system_live_2026_09_12.json");

fn envelope(seq: u64, payload: &str) -> Vec<u8> {
    let payload_value: Value = serde_json::from_str(payload).unwrap();
    let cursor = payload_value["data"]["version"].as_u64().map_or_else(
        || json!({"type":"unsequenced","counter":seq}),
        |version| json!({"type":"snapshot","last_update_id":version}),
    );
    format!(
        "{}\n",
        json!({
            "envelope_version":2,"delivery_index":seq,"record_id":format!("lm-e-{seq}"),
            "visible_ns":seq,"monotonic_ns":seq,"venue":"limitless",
            "stream":"public_book","connection_epoch":"e","local_counter":seq,
            "source_cursor":cursor,"kind":"venue_frame","raw_payload":payload,
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
    let payloads = [BOOK.trim(), CREATED.trim(), SYSTEM.trim()];
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
                    "canonical_seq":seq,"lane_id":"limitless",
                    "source_segment_sha256":SOURCE_SHA,"source_line_number":seq,
                    "record_id":view.record_id.as_str(),
                    "content_hash":ContentHash::hash(view.raw_payload.as_bytes()).to_hex(),
                    "continuity_verdict":if seq == 1 { "sparse_monotonic" } else { "unsequenced_venue" },
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
        expected_lanes: vec!["limitless".to_owned()],
        present_lanes: vec!["limitless".to_owned()],
        unexpected_lanes: Vec::new(),
        missing_lanes: Vec::new(),
        invalid_lanes: Vec::new(),
        finalization_deadline_seconds: 300,
        deadline_expired: false,
        finalized_at_ns: 10,
        inputs: vec![InputSegment {
            lane: "limitless".to_owned(),
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

fn spec(normalizer: &Normalizer<Limitless>) -> DerivativeSpec {
    DerivativeSpec {
        normalized_schema_version: SEGMENT_SCHEMA_VERSION,
        normalizer_bundle_sha256: normalizer.descriptor().bundle_sha256,
        normalizer_config_sha256: normalizer.descriptor().config_sha256,
        policy: NormalizationPolicy {
            policy_sha256: Sha256::digest(b"limitless-normalization-policy-v1"),
            effective_from_ns: 0,
            effective_until_ns: Some(10),
        },
    }
}

#[test]
fn materializes_verifies_and_retries_limitless_derivative_end_to_end() {
    let canonical = TempDir::new("limitless-canonical").unwrap();
    let output = TempDir::new("limitless-normalized").unwrap();
    canonical_fixture(canonical.path());
    let mut normalizer = Normalizer::new(Limitless::default()).unwrap();
    let spec = spec(&normalizer);
    let built = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec,
        &mut normalizer,
    )
    .unwrap();
    assert_eq!(built.disposition, BuildDisposition::Committed);
    assert_eq!(built.derivative.manifest.counts.input_records, 3);
    assert_eq!(built.derivative.manifest.counts.accepted_events, 1);
    assert_eq!(built.derivative.manifest.counts.rejected_source_records, 1);
    assert_eq!(
        built.derivative.manifest.counts.normalization_fault_events,
        1
    );
    assert_eq!(
        built
            .derivative
            .manifest
            .counts
            .intentionally_ignored_records,
        1
    );
    let verified = verify_derivative(&built.derivative.directory).unwrap();
    assert_eq!(verified.pin, built.derivative.pin);

    let mut logical = Vec::new();
    let output_identity = &built.derivative.manifest.events;
    prediction_encoder::decode_stream(
        fs::File::open(built.derivative.directory.join("events.ndjson.zst")).unwrap(),
        &mut logical,
        &prediction_encoder::LogicalIdentity {
            sha256: output_identity.logical.sha256.to_string(),
            byte_length: output_identity.logical.byte_length,
            line_count: output_identity.logical.line_count,
        },
        Some(&prediction_encoder::StoredIdentity {
            sha256: output_identity.stored.sha256.to_string(),
            byte_length: output_identity.stored.byte_length,
        }),
        None,
    )
    .unwrap();
    let rows = logical
        .split(|byte| *byte == b'\n')
        .filter(|row| !row.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    let first = SegmentRecord::from_canonical_json(rows[0]).unwrap();
    let second = SegmentRecord::from_canonical_json(rows[1]).unwrap();
    assert!(matches!(
        first.event(),
        SegmentEvent::Book(BookEvent::Full(_))
    ));
    assert!(matches!(
        second.event(),
        SegmentEvent::NormalizationFault(_)
    ));
    assert_eq!(first.header().address().event_index(), 0);
    assert_eq!(second.header().address().event_index(), 0);

    let retry = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec,
        &mut Normalizer::new(Limitless::default()).unwrap(),
    )
    .unwrap();
    assert_eq!(retry.disposition, BuildDisposition::VerifiedNoOp);
    assert_eq!(retry.derivative.pin, built.derivative.pin);
}
