use std::fs;

use indexer_finalize::{
    CanonicalOutputsState, canonical_outputs_state, read_receipt, receipt_path,
};
use sha2::{Digest, Sha256};
use tempdir::TempDir;

#[test]
fn a_provider_neutral_gcs_archive_receipt_is_a_valid_tombstone() {
    let root = TempDir::new("gcs-canonical-tombstone").expect("temporary directory");
    let canonical = root.path().join("canonical");
    let path = receipt_path(&canonical, 0);
    let directory = path.parent().expect("window directory");
    fs::create_dir_all(directory).expect("create window");

    let zero_sha = format!("{:064x}", 0);
    let receipt = serde_json::json!({
        "receipt_version": 1,
        "window_start_ns": 0,
        "window_end_ns": 1_800_000_000_000_u64,
        "completeness": "complete",
        "certified": true,
        "expected_lanes": [],
        "present_lanes": [],
        "unexpected_lanes": [],
        "missing_lanes": [],
        "invalid_lanes": [],
        "finalization_deadline_seconds": 300,
        "deadline_expired": false,
        "finalized_at_ns": 1_800_000_000_000_u64,
        "inputs": [],
        "evidence": canonical_output("evidence.ndjson.zst", &zero_sha),
        "provenance": canonical_output("provenance.ndjson.zst", &zero_sha),
        "first_canonical_seq": null,
        "last_canonical_seq": null,
        "finalizer_version": 1,
    });
    let receipt_bytes = serde_json::to_vec(&receipt).expect("serialize receipt");
    fs::write(&path, &receipt_bytes).expect("write canonical receipt");

    let base = "canonical/date=1970-01-01/window=0";
    let marker = serde_json::json!({
        "canonical_archive_receipt_version": 2,
        "store": {"provider": "gcs", "location": "archive-bucket"},
        "window_start_ns": 0,
        "window_end_ns": 1_800_000_000_000_u64,
        "evidence": archive_object(
            "evidence.ndjson.zst",
            &format!("{base}/evidence.ndjson.zst"),
            0,
            &zero_sha,
            "application/x-ndjson",
            Some("zstd"),
        ),
        "provenance": archive_object(
            "provenance.ndjson.zst",
            &format!("{base}/provenance.ndjson.zst"),
            0,
            &zero_sha,
            "application/x-ndjson",
            Some("zstd"),
        ),
        "canonical_receipt": archive_object(
            "receipt.json",
            &format!("{base}/receipt.json"),
            receipt_bytes.len() as u64,
            &format!("{:x}", Sha256::digest(&receipt_bytes)),
            "application/json",
            None,
        ),
        "verified_at_ns": 1_800_000_000_000_u64,
        "archiver_version": 1,
    });
    fs::write(
        directory.join("canonical_archive_receipt.json"),
        serde_json::to_vec(&marker).expect("serialize archive receipt"),
    )
    .expect("write archive receipt");

    let receipt = read_receipt(&canonical, 0)
        .expect("read receipt")
        .expect("committed receipt");
    assert_eq!(
        canonical_outputs_state(&canonical, &receipt).expect("validate tombstone"),
        CanonicalOutputsState::Archived
    );
}

fn canonical_output(file: &str, sha256: &str) -> serde_json::Value {
    serde_json::json!({
        "file": file,
        "content_encoding": "zstd",
        "decoded": {"byte_length": 0, "line_count": 0, "sha256": sha256},
        "stored": {"byte_length": 0, "sha256": sha256},
        "compression": {
            "algorithm": "zstd",
            "level": 3,
            "frame_checksum": true,
            "dictionary": null,
            "frame_count": 1,
            "encoder": "fixture"
        }
    })
}

fn archive_object(
    file: &str,
    key: &str,
    byte_length: u64,
    sha256: &str,
    content_type: &str,
    content_encoding: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({
        "file": file,
        "key": key,
        "byte_length": byte_length,
        "sha256": sha256,
        "content_type": content_type,
        "content_encoding": content_encoding,
        "provider_checksum": "AAAAAA==",
        "provider_checksum_algorithm": "CRC32C"
    })
}
