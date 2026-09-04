use std::fs;
use std::io::Cursor;
use std::path::Path;

use indexer_finalize::{
    CanonicalOutput, CertifiedPolicy, CompressionContract, DecodedIdentity, InputSegment,
    LowerBoundPolicy, Receipt, SelectionPolicy, StoredIdentity, select_canonical_windows,
    window_directory,
};
use prediction_encoder::{DEFAULT_ZSTD_LEVEL, encode_stream, encoder_version};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempdir::TempDir;

const SOURCE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn envelope(seq: i64, visible_ns: u64) -> Vec<u8> {
    format!(
        "{{\"delivery_index\":{seq},\"record_id\":\"pm-e-{seq}\",\"visible_ns\":{visible_ns},\"venue\":\"polymarket\",\"stream\":\"public_book\",\"connection_epoch\":\"e\",\"local_counter\":{seq},\"source_cursor\":{{\"type\":\"unsequenced\",\"counter\":{seq}}},\"kind\":\"venue_frame\",\"raw_payload\":\"{{}}\"}}\n"
    )
    .into_bytes()
}

fn encoded_output(directory: &Path, name: &str, logical: &[u8]) -> CanonicalOutput {
    let mut stored = Vec::new();
    let result = encode_stream(Cursor::new(logical), &mut stored, DEFAULT_ZSTD_LEVEL)
        .expect("encode fixture");
    fs::write(directory.join(name), stored).expect("write fixture object");
    CanonicalOutput {
        file: name.to_owned(),
        content_encoding: "zstd".to_owned(),
        decoded: DecodedIdentity {
            byte_length: result.logical.byte_length,
            line_count: result.logical.line_count,
            sha256: result.logical.sha256,
        },
        stored: StoredIdentity {
            byte_length: result.stored.byte_length,
            sha256: result.stored.sha256,
        },
        compression: CompressionContract {
            algorithm: "zstd".to_owned(),
            level: DEFAULT_ZSTD_LEVEL,
            frame_checksum: true,
            dictionary: None,
            frame_count: 1,
            encoder: encoder_version(),
        },
    }
}

fn add_window(root: &Path, start: u64, end: u64, records: &[(i64, u64)], certified: bool) {
    let directory = window_directory(root, start);
    fs::create_dir_all(&directory).expect("window directory");
    let mut evidence = Vec::new();
    let mut provenance = Vec::new();
    for (line, (seq, visible_ns)) in records.iter().enumerate() {
        let encoded = envelope(*seq, *visible_ns);
        let view = indexer_types::EnvelopeView::parse(&encoded).expect("fixture envelope");
        evidence.extend_from_slice(&encoded);
        provenance.extend_from_slice(
            format!(
                "{}\n",
                json!({
                    "canonical_seq": seq,
                    "lane_id": "polymarket",
                    "source_segment_sha256": SOURCE_SHA,
                    "source_line_number": line + 1,
                    "record_id": view.record_id.as_str(),
                    "content_hash": indexer_types::ContentHash::hash(view.raw_payload.as_bytes()).to_hex(),
                    "continuity_verdict": "unsequenced_venue",
                    "visible_tie_group": null,
                })
            )
            .as_bytes(),
        );
    }
    let evidence_output = encoded_output(&directory, "evidence.ndjson.zst", &evidence);
    let provenance_output = encoded_output(&directory, "provenance.ndjson.zst", &provenance);
    let receipt = Receipt {
        receipt_version: 1,
        window_start_ns: start,
        window_end_ns: end,
        completeness: "complete".to_owned(),
        certified,
        expected_lanes: vec!["polymarket".to_owned()],
        present_lanes: vec!["polymarket".to_owned()],
        unexpected_lanes: Vec::new(),
        missing_lanes: Vec::new(),
        invalid_lanes: Vec::new(),
        finalization_deadline_seconds: 300,
        deadline_expired: false,
        finalized_at_ns: end,
        inputs: vec![InputSegment {
            lane: "polymarket".to_owned(),
            data_file: "source.ndjson".to_owned(),
            segment_index: 0,
            line_count: records.len() as u64,
            sha256: SOURCE_SHA.to_owned(),
            first_delivery_index: records.first().map(|(seq, _)| *seq as u64),
            last_delivery_index: records.last().map(|(seq, _)| *seq as u64),
        }],
        evidence: evidence_output,
        provenance: provenance_output,
        first_canonical_seq: records.first().map(|(seq, _)| *seq),
        last_canonical_seq: records.last().map(|(seq, _)| *seq),
        carried: Default::default(),
        clock_faults: Vec::new(),
        finalizer_version: 1,
    };
    write_receipt(&directory, &receipt);
}

fn write_receipt(directory: &Path, receipt: &Receipt) {
    let mut encoded = serde_json::to_vec_pretty(receipt).expect("receipt JSON");
    encoded.push(b'\n');
    fs::write(directory.join("receipt.json"), encoded).expect("write receipt");
}

fn mutate_receipt(root: &Path, start: u64, change: impl FnOnce(&mut Value)) {
    let path = window_directory(root, start).join("receipt.json");
    let mut receipt: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    change(&mut receipt);
    fs::write(
        path,
        format!("{}\n", serde_json::to_string_pretty(&receipt).unwrap()),
    )
    .unwrap();
}

fn replace_logical(root: &Path, start: u64, name: &str, logical: &[u8], update_receipt: bool) {
    let directory = window_directory(root, start);
    let output = encoded_output(&directory, name, logical);
    if update_receipt {
        mutate_receipt(root, start, |receipt| {
            receipt[name.strip_suffix(".ndjson.zst").unwrap()] =
                serde_json::to_value(output).unwrap();
        });
    }
}

fn read_all(
    root: &Path,
    start: u64,
    end: u64,
    policy: SelectionPolicy,
) -> Result<(Vec<i64>, indexer_finalize::AuditedCanonicalSelection), String> {
    let selection = select_canonical_windows(root, start, end, policy)?;
    let mut reader = selection.open()?;
    let mut seqs = Vec::new();
    while let Some(record) = reader.next_record()? {
        assert_eq!(record.order_ns, record.visible_ns);
        assert_eq!(record.canonical_seq, record.event_address.canonical_seq);
        assert_eq!(record.record_id, format!("pm-e-{}", record.canonical_seq));
        assert_eq!(record.source_segment_sha256, SOURCE_SHA);
        seqs.push(record.canonical_seq);
    }
    Ok((seqs, reader.finish()?))
}

#[test]
fn minimal_exact_adjacent_selection_streams_in_finalizer_order_and_retains_receipts() {
    let temp = TempDir::new("audited-selection").unwrap();
    add_window(temp.path(), 0, 10, &[(1, 1)], true);
    add_window(temp.path(), 10, 20, &[(2, 11)], true);
    add_window(temp.path(), 20, 30, &[(3, 21)], true);

    let (seqs, audited) = read_all(temp.path(), 10, 30, SelectionPolicy::default()).unwrap();
    assert_eq!(seqs, [2, 3]);
    assert_eq!(audited.requested_interval(), (10, 30));
    assert_eq!(audited.effective_interval(), (10, 30));
    assert_eq!(audited.receipt_identities().len(), 2);
    assert!(
        audited
            .receipt_identities()
            .iter()
            .all(|id| id.sha256.len() == 64)
    );
    assert_eq!(audited.records_verified(), 2);
}

#[test]
fn gaps_and_overlaps_fail_closed() {
    let gap = TempDir::new("selection-gap").unwrap();
    add_window(gap.path(), 0, 10, &[(1, 1)], true);
    add_window(gap.path(), 11, 20, &[(2, 12)], true);
    assert!(
        select_canonical_windows(gap.path(), 0, 20, SelectionPolicy::default())
            .unwrap_err()
            .contains("gap")
    );

    let overlap = TempDir::new("selection-overlap").unwrap();
    add_window(overlap.path(), 0, 11, &[(1, 1)], true);
    add_window(overlap.path(), 10, 20, &[(2, 12)], true);
    assert!(
        select_canonical_windows(overlap.path(), 0, 20, SelectionPolicy::default())
            .unwrap_err()
            .contains("overlap")
    );
}

#[test]
fn missing_receipt_object_invalid_marker_bounds_and_schema_fail_closed() {
    let missing_receipt = TempDir::new("missing-receipt").unwrap();
    add_window(missing_receipt.path(), 0, 10, &[(1, 1)], true);
    fs::remove_file(window_directory(missing_receipt.path(), 0).join("receipt.json")).unwrap();
    assert!(select_canonical_windows(missing_receipt.path(), 0, 10, Default::default()).is_err());

    let missing_object = TempDir::new("missing-object").unwrap();
    add_window(missing_object.path(), 0, 10, &[(1, 1)], true);
    fs::remove_file(window_directory(missing_object.path(), 0).join("evidence.ndjson.zst"))
        .unwrap();
    assert!(select_canonical_windows(missing_object.path(), 0, 10, Default::default()).is_err());

    let invalid = TempDir::new("invalid-marker").unwrap();
    add_window(invalid.path(), 0, 10, &[(1, 1)], true);
    fs::write(
        window_directory(invalid.path(), 0).join("receipt.json"),
        b"{}\n",
    )
    .unwrap();
    assert!(select_canonical_windows(invalid.path(), 0, 10, Default::default()).is_err());

    let bounds = TempDir::new("wrong-bounds").unwrap();
    add_window(bounds.path(), 0, 10, &[(1, 10)], true);
    assert!(read_all(bounds.path(), 0, 10, Default::default()).is_err());

    let schema = TempDir::new("wrong-schema").unwrap();
    add_window(schema.path(), 0, 10, &[(1, 1)], true);
    mutate_receipt(schema.path(), 0, |receipt| receipt["unknown"] = json!(true));
    assert!(select_canonical_windows(schema.path(), 0, 10, Default::default()).is_err());
}

#[test]
fn stored_and_decoded_identity_fail_independently() {
    let stored = TempDir::new("stored-identity").unwrap();
    add_window(stored.path(), 0, 10, &[(1, 1)], true);
    mutate_receipt(stored.path(), 0, |receipt| {
        receipt["evidence"]["stored"]["sha256"] = json!("b".repeat(64));
    });
    let error = read_all(stored.path(), 0, 10, Default::default()).unwrap_err();
    assert!(error.contains("stored sha256"), "{error}");

    let decoded = TempDir::new("decoded-identity").unwrap();
    add_window(decoded.path(), 0, 10, &[(1, 1)], true);
    mutate_receipt(decoded.path(), 0, |receipt| {
        receipt["evidence"]["decoded"]["sha256"] = json!("b".repeat(64));
    });
    let error = read_all(decoded.path(), 0, 10, Default::default()).unwrap_err();
    assert!(error.contains("decoded sha256"), "{error}");
}

#[test]
fn finish_is_required_and_eof_corruption_cannot_mint_a_capability() {
    let temp = TempDir::new("finish-required").unwrap();
    add_window(temp.path(), 0, 10, &[(1, 1)], true);
    let selection = select_canonical_windows(temp.path(), 0, 10, Default::default()).unwrap();
    let mut reader = selection.open().unwrap();
    assert!(reader.next_record().unwrap().is_some());
    assert!(reader.finish().unwrap_err().contains("consumed through"));

    let truncated = TempDir::new("truncated-eof").unwrap();
    add_window(truncated.path(), 0, 10, &[(1, 1)], true);
    let directory = window_directory(truncated.path(), 0);
    let object = directory.join("evidence.ndjson.zst");
    let mut bytes = fs::read(&object).unwrap();
    bytes.pop();
    fs::write(&object, &bytes).unwrap();
    mutate_receipt(truncated.path(), 0, |receipt| {
        receipt["evidence"]["stored"]["byte_length"] = json!(bytes.len());
        receipt["evidence"]["stored"]["sha256"] = json!(format!("{:x}", Sha256::digest(&bytes)));
    });
    assert!(read_all(truncated.path(), 0, 10, Default::default()).is_err());
}

#[test]
fn provenance_binding_sequence_continuity_and_labels_are_strict() {
    let binding = TempDir::new("provenance-binding").unwrap();
    add_window(binding.path(), 0, 10, &[(1, 1)], true);
    let bad = format!(
        "{}\n",
        json!({
            "canonical_seq": 1, "lane_id": "kalshi",
            "source_segment_sha256": SOURCE_SHA, "source_line_number": 1,
            "record_id": "pm-e-1",
            "content_hash": indexer_types::ContentHash::hash(b"{}").to_hex(),
            "continuity_verdict": "continuous", "visible_tie_group": null
        })
    );
    replace_logical(
        binding.path(),
        0,
        "provenance.ndjson.zst",
        bad.as_bytes(),
        true,
    );
    let selection = select_canonical_windows(binding.path(), 0, 10, Default::default()).unwrap();
    let mut reader = selection.open().unwrap();
    assert!(reader.next_record().is_err());
    assert!(
        reader.next_record().is_err(),
        "a line error poisons the stream"
    );
    assert!(
        reader.finish().is_err(),
        "a poisoned stream mints no capability"
    );

    let sequence = TempDir::new("sequence-boundary").unwrap();
    add_window(sequence.path(), 0, 10, &[(1, 1)], true);
    add_window(sequence.path(), 10, 20, &[(3, 11)], true);
    assert!(select_canonical_windows(sequence.path(), 0, 20, Default::default()).is_err());

    let label = TempDir::new("unknown-continuity").unwrap();
    add_window(label.path(), 0, 10, &[(1, 1)], true);
    let bad = format!(
        "{}\n",
        json!({
            "canonical_seq": 1, "lane_id": "polymarket",
            "source_segment_sha256": SOURCE_SHA, "source_line_number": 1,
            "record_id": "pm-e-1",
            "content_hash": indexer_types::ContentHash::hash(b"{}").to_hex(),
            "continuity_verdict": "maybe", "visible_tie_group": null
        })
    );
    replace_logical(
        label.path(),
        0,
        "provenance.ndjson.zst",
        bad.as_bytes(),
        true,
    );
    assert!(read_all(label.path(), 0, 10, Default::default()).is_err());

    for known in [
        "lifecycle",
        "bootstrap",
        "unsequenced_venue",
        "sparse_monotonic",
        "continuous",
        "gap_proven",
        "cursor_went_backwards",
        "local_counter_broken",
        "duplicate",
        "conflict",
    ] {
        let encoded = json!({
            "canonical_seq": 1, "lane_id": "polymarket",
            "source_segment_sha256": SOURCE_SHA, "source_line_number": 1,
            "record_id": "pm-e-1", "content_hash": "b".repeat(64),
            "continuity_verdict": known, "visible_tie_group": null
        });
        serde_json::from_value::<indexer_finalize::CanonicalProvenance>(encoded)
            .unwrap_or_else(|error| panic!("known continuity label {known} failed: {error}"));
    }
}

#[test]
fn empty_windows_preserve_sequence_adjacency_and_interval_clipping() {
    let temp = TempDir::new("empty-adjacency").unwrap();
    add_window(temp.path(), 0, 10, &[], true);
    add_window(temp.path(), 10, 20, &[(1, 11), (2, 18)], true);
    add_window(temp.path(), 20, 30, &[], true);
    add_window(temp.path(), 30, 40, &[(3, 31)], true);
    let policy = SelectionPolicy {
        lower_bound: LowerBoundPolicy::Clip,
        ..Default::default()
    };
    let (seqs, audited) = read_all(temp.path(), 12, 35, policy).unwrap();
    assert_eq!(seqs, [2, 3]);
    assert_eq!(audited.records_verified(), 3);
    assert_eq!(audited.receipt_identities().len(), 3);

    assert!(select_canonical_windows(temp.path(), 12, 35, Default::default()).is_err());
    let expanded = SelectionPolicy {
        lower_bound: LowerBoundPolicy::ExpandToWindowStart,
        ..Default::default()
    };
    let (seqs, audited) = read_all(temp.path(), 12, 35, expanded).unwrap();
    assert_eq!(seqs, [1, 2, 3]);
    assert_eq!(audited.effective_interval(), (10, 35));
}

#[test]
fn uncertified_windows_require_an_explicit_policy() {
    let temp = TempDir::new("uncertified-policy").unwrap();
    add_window(temp.path(), 0, 10, &[(1, 1)], false);
    assert!(select_canonical_windows(temp.path(), 0, 10, Default::default()).is_err());
    let policy = SelectionPolicy {
        certified: CertifiedPolicy::AllowUncertified,
        ..Default::default()
    };
    let (_, audited) = read_all(temp.path(), 0, 10, policy).unwrap();
    assert!(!audited.receipt_identities()[0].certified);
}
