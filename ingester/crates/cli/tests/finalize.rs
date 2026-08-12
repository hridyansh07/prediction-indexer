//! What `indexer-finalize` produces, over the same bytes `ordering.rs` uses.
//!
//! The pair is the point. `ordering.rs` pins `FILE_ORDER = [100, 300, 200]` —
//! `indexer-ingest` consuming whole files atomically, so a record received
//! between two records of another lane sequences after both. This file asserts
//! `MERGED_ORDER = [100, 200, 300]` over the identical fixture. Two binaries,
//! one set of bytes, two honestly-labelled global orders (§5, §8.3).

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use common::{
    MERGED_ORDER, WINDOW_NS, current_window, ended_window, envelope, interleaved_fixture,
    receipt_paths, seal_segment_in_window, seal_segment_with, segment_name, segment_path,
    write_lane, write_lane_in_window,
};
use prediction_encoder::{LogicalIdentity, StoredIdentity, decode_stream};
use serde_json::Value;
use tempdir::TempDir;

const POLYMARKET_AND_KALSHI: [&str; 2] = ["polymarket", "kalshi"];

fn finalize_raw(spool: &Path, canonical: &Path, expected: &[&str], deadline: u64) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_indexer-finalize"));
    command.arg(spool).arg(canonical);
    for lane in expected {
        command.arg("--expect-lane").arg(lane);
    }
    command
        .arg("--finalization-deadline-seconds")
        .arg(deadline.to_string());
    command.output().expect("run indexer-finalize")
}

#[test]
fn a_periodic_finalizer_runs_repeated_sweeps_and_releases_its_lease_on_sigterm() {
    let root = TempDir::new("periodic-finalizer").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    fs::create_dir_all(&spool).expect("spool");
    let child = Command::new(env!("CARGO_BIN_EXE_indexer-finalize"))
        .arg(&spool)
        .arg(&canonical)
        .arg("--expect-lane")
        .arg("polymarket")
        .arg("--interval-seconds")
        .arg("1")
        .stdout(Stdio::piped())
        .spawn()
        .expect("start periodic finalizer");
    std::thread::sleep(Duration::from_millis(1300));
    let signal = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(signal.success());
    let output = child.wait_with_output().expect("periodic finalizer exits");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .matches("\"spool_root\"")
            .count()
            >= 2,
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(!canonical.join(".finalize.lease").exists());
}

#[test]
fn a_zero_periodic_interval_is_refused() {
    let output = Command::new(env!("CARGO_BIN_EXE_indexer-finalize"))
        .arg("spool")
        .arg("canonical")
        .arg("--expect-lane")
        .arg("polymarket")
        .arg("--interval-seconds")
        .arg("0")
        .output()
        .expect("run finalizer");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must be positive"));
}

#[test]
fn a_fatal_sweep_persists_its_failure_report() {
    let root = TempDir::new("finalizer-failure-report").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let corrupt = canonical.join("date=1970-01-01/window=0");
    fs::create_dir_all(&spool).expect("spool");
    fs::create_dir_all(&corrupt).expect("canonical window");
    fs::write(corrupt.join("receipt.json"), b"{}\n").expect("corrupt receipt");
    let report = canonical.join("ops/last_finalizer_sweep.json");
    let output = Command::new(env!("CARGO_BIN_EXE_indexer-finalize"))
        .arg(&spool)
        .arg(&canonical)
        .arg("--expect-lane")
        .arg("polymarket")
        .arg("--report")
        .arg(&report)
        .output()
        .expect("run finalizer");
    assert!(!output.status.success());
    let document: Value =
        serde_json::from_slice(&fs::read(report).expect("failure report")).expect("report JSON");
    assert_eq!(document["status"], "error");
    assert!(
        document["error"]
            .as_str()
            .unwrap()
            .contains("unreadable receipt")
    );
}

fn finalize(spool: &Path, canonical: &Path, expected: &[&str], deadline: u64) -> Value {
    let output = finalize_raw(spool, canonical, expected, deadline);
    assert!(
        output.status.success(),
        "finalize failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("JSON report")
}

fn audit_raw(canonical: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_indexer-canonical-audit"))
        .arg(canonical)
        .output()
        .expect("run indexer-canonical-audit")
}

/// A window's output directory, found by walking rather than by assuming a date.
///
/// The `date=` partition comes from the window start, so a fixture placed
/// relative to the current clock lands under today's date and not 1970's.
fn window_directory(canonical: &Path, start_ns: u64) -> PathBuf {
    let wanted = format!("window={start_ns}");
    for date in fs::read_dir(canonical)
        .expect("canonical root")
        .filter_map(Result::ok)
    {
        let candidate = date.path().join(&wanted);
        if candidate.is_dir() {
            return candidate;
        }
    }
    panic!("no output directory for window {start_ns}");
}

fn read_lines(path: &Path) -> Vec<Value> {
    String::from_utf8(read_decoded(path))
        .expect("decoded output is UTF-8")
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("JSON line"))
        .collect()
}

fn read_decoded(path: &Path) -> Vec<u8> {
    let document: Value = serde_json::from_slice(
        &fs::read(path.parent().expect("window").join("receipt.json")).expect("receipt"),
    )
    .expect("receipt JSON");
    let field = if path.file_name().unwrap() == "evidence.ndjson.zst" {
        "evidence"
    } else {
        "provenance"
    };
    let output = &document[field];
    let logical = LogicalIdentity {
        sha256: output["decoded"]["sha256"].as_str().unwrap().to_owned(),
        byte_length: output["decoded"]["byte_length"].as_u64().unwrap(),
        line_count: output["decoded"]["line_count"].as_u64().unwrap(),
    };
    let stored = StoredIdentity {
        sha256: output["stored"]["sha256"].as_str().unwrap().to_owned(),
        byte_length: output["stored"]["byte_length"].as_u64().unwrap(),
    };
    let mut decoded = Vec::new();
    decode_stream(
        std::io::Cursor::new(fs::read(path).expect("compressed output")),
        &mut decoded,
        &logical,
        Some(&stored),
        Some(logical.byte_length),
    )
    .expect("verified decode");
    decoded
}

/// The receive times of the canonical file, in the order it stores them.
fn visible_ns_in_canonical_order(canonical: &Path) -> Vec<u64> {
    read_lines(&window_directory(canonical, 0).join("evidence.ndjson.zst"))
        .into_iter()
        .map(|line| line["visible_ns"].as_u64().expect("visible_ns"))
        .collect()
}

fn receipt(canonical: &Path, start_ns: u64) -> Value {
    serde_json::from_slice(
        &fs::read(window_directory(canonical, start_ns).join("receipt.json")).expect("receipt"),
    )
    .expect("receipt JSON")
}

// -- the headline claim ----------------------------------------------------

#[test]
fn canonical_order_is_receive_order_across_lanes() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert_eq!(report["windows_finalized"], 1);

    assert_eq!(
        visible_ns_in_canonical_order(&canonical),
        MERGED_ORDER,
        "the kalshi record was received between the two polymarket records and \
         must be sequenced between them"
    );
}

#[test]
fn finalizer_retains_no_record_identity_history() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert_eq!(
        report["max_identity_records_in_memory"], 0,
        "record identity must be exact but disk-backed; retaining one entry per \
         canonical record makes finalizer memory O(records in the window)"
    );
}

#[test]
fn the_two_binaries_disagree_about_identical_bytes() {
    // Stated as one assertion, because this is the whole reason the finalizer
    // exists. Both binaries read the same spool; neither rewrites it.
    let (root, spool, store) = interleaved_fixture();
    let canonical = root.path().join("canonical");

    let ingest = Command::new(env!("CARGO_BIN_EXE_indexer-ingest"))
        .arg(&spool)
        .arg(&store)
        .output()
        .expect("run indexer-ingest");
    assert!(ingest.status.success());
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let connection = rusqlite::Connection::open(store.join("store.db")).expect("open store");
    let mut statement = connection
        .prepare("SELECT raw_line FROM evidence ORDER BY seq")
        .expect("prepare");
    let file_order: Vec<u64> = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .expect("query")
        .map(|row| {
            let parsed: Value = serde_json::from_slice(&row.expect("row")).expect("envelope");
            parsed["visible_ns"].as_u64().expect("visible_ns")
        })
        .collect();

    assert_ne!(
        file_order,
        visible_ns_in_canonical_order(&canonical),
        "if these agreed the fixture would prove nothing"
    );
}

// -- byte fidelity ---------------------------------------------------------

#[test]
fn canonical_evidence_is_the_input_lines_byte_for_byte() {
    // §5: canonical evidence is "the original envelope lines, copied
    // byte-for-byte into global order" — a reordering of the tape, not an
    // interpretation of it. Re-encoding would quietly make the canonical file
    // lossy in exactly the layer we expect to revise later.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let mut source: Vec<String> = Vec::new();
    for lane in ["polymarket", "kalshi"] {
        let directory = spool.join(format!("lane={lane}")).join("date=2026-07-30");
        for entry in fs::read_dir(&directory).expect("lane directory") {
            let path = entry.expect("entry").path();
            if path
                .extension()
                .is_some_and(|extension| extension == "ndjson")
            {
                source.extend(
                    fs::read_to_string(&path)
                        .expect("segment")
                        .lines()
                        .map(str::to_owned),
                );
            }
        }
    }
    let mut canonical_lines: Vec<String> = String::from_utf8(read_decoded(
        &window_directory(&canonical, 0).join("evidence.ndjson.zst"),
    ))
    .expect("canonical evidence is UTF-8")
    .lines()
    .map(str::to_owned)
    .collect();

    source.sort();
    canonical_lines.sort();
    assert_eq!(
        source, canonical_lines,
        "same multiset of lines, not re-encoded"
    );
}

#[test]
fn provenance_carries_one_line_per_canonical_position() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let evidence = read_lines(&window_directory(&canonical, 0).join("evidence.ndjson.zst"));
    let provenance = read_lines(&window_directory(&canonical, 0).join("provenance.ndjson.zst"));
    assert_eq!(evidence.len(), provenance.len());

    for (index, (record, entry)) in evidence.iter().zip(&provenance).enumerate() {
        let position = index as u64 + 1;
        assert_eq!(entry["canonical_seq"].as_u64(), Some(position));
        assert_eq!(
            entry["record_id"], record["record_id"],
            "at position {position}"
        );
        assert!(
            entry["source_segment_sha256"]
                .as_str()
                .is_some_and(|hex| hex.len() == 64)
        );
        assert!(
            entry["source_line_number"]
                .as_u64()
                .is_some_and(|line| line >= 1)
        );
        // Distinct timestamps throughout this fixture, so nothing is a tie.
        assert!(entry["visible_tie_group"].is_null());
    }
}

#[test]
fn receipt_digests_verify_against_the_files_they_describe() {
    use sha2::{Digest, Sha256};

    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let receipt = receipt(&canonical, 0);
    for name in ["evidence", "provenance"] {
        let file = window_directory(&canonical, 0)
            .join(receipt[name]["file"].as_str().expect("file name"));
        let bytes = fs::read(&file).expect("output file");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            receipt[name]["stored"]["sha256"]
                .as_str()
                .expect("stored sha256"),
            "{name} stored digest"
        );
        assert_eq!(
            bytes.len() as u64,
            receipt[name]["stored"]["byte_length"]
                .as_u64()
                .expect("stored length")
        );
        let decoded = read_decoded(&file);
        assert_eq!(
            format!("{:x}", Sha256::digest(&decoded)),
            receipt[name]["decoded"]["sha256"]
                .as_str()
                .expect("decoded sha256")
        );
    }
    assert_eq!(receipt["completeness"], "complete");
    assert_eq!(receipt["certified"], true);
    assert_eq!(receipt["evidence"]["content_encoding"], "zstd");
    assert_eq!(receipt["evidence"]["compression"]["frame_checksum"], true);
    assert_eq!(receipt["evidence"]["compression"]["frame_count"], 1);
    assert!(
        !window_directory(&canonical, 0)
            .join("evidence.ndjson")
            .exists()
    );
    assert!(
        !window_directory(&canonical, 0)
            .join("provenance.ndjson")
            .exists()
    );
}

#[test]
fn canonical_audit_verifies_every_compressed_record() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let output = audit_raw(&canonical);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).expect("audit report");
    assert_eq!(report["windows_verified"], 1);
    assert_eq!(report["evidence_records_verified"], 3);
}

#[test]
fn canonical_audit_rejects_same_length_compressed_corruption() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    let path = window_directory(&canonical, 0).join("evidence.ndjson.zst");
    let mut bytes = fs::read(&path).expect("frame");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(&path, bytes).expect("corrupt frame");

    let output = audit_raw(&canonical);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("verifying evidence.ndjson.zst")
            || String::from_utf8_lossy(&output.stderr).contains("decoding evidence.ndjson.zst"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn a_changed_byte_after_finalization_fails_the_recorded_digest() {
    // §8. The receipt is what makes the canonical file evidence; if the bytes
    // can drift out from under it without detection, it asserts nothing.
    use sha2::{Digest, Sha256};

    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let evidence = window_directory(&canonical, 0).join("evidence.ndjson.zst");
    let mut bytes = fs::read(&evidence).expect("evidence");
    let position = bytes
        .iter()
        .position(|byte| *byte == b'1')
        .expect("a mutable byte");
    bytes[position] = b'2';
    fs::write(&evidence, &bytes).expect("same-length corruption");

    assert_ne!(
        format!("{:x}", Sha256::digest(&bytes)),
        receipt(&canonical, 0)["evidence"]["stored"]["sha256"]
            .as_str()
            .expect("sha256"),
    );
}

// -- completeness and the deadline -----------------------------------------

#[test]
fn a_missing_expected_lane_produces_an_incomplete_receipt_naming_it() {
    // §5: the deadline is finite, and an expired one commits what is available
    // rather than stalling. One wedged splice must not halt finalization for
    // every healthy venue.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");

    let report = finalize(
        &spool,
        &canonical,
        &["polymarket", "kalshi", "limitless"],
        0,
    );
    assert_eq!(report["windows_finalized"], 1);

    let receipt = receipt(&canonical, 0);
    assert_eq!(receipt["completeness"], "incomplete");
    assert_eq!(receipt["certified"], false);
    assert_eq!(receipt["deadline_expired"], true);
    assert_eq!(receipt["missing_lanes"][0]["lane"], "limitless");
    assert_eq!(receipt["missing_lanes"][0]["reason"], "lane_missing");
    // The records that did arrive are still committed and still usable.
    assert_eq!(receipt["evidence"]["decoded"]["line_count"], 3);
    assert_eq!(
        receipt["expected_lanes"],
        serde_json::json!(["polymarket", "kalshi", "limitless"]),
        "recorded exactly as supplied, so the verdict can be read against it"
    );
}

#[test]
fn a_window_inside_its_deadline_is_not_finalized_at_all() {
    // §7. Committing early would assign positions to a window that may still
    // gain a lane, and a committed window is immutable.
    //
    // The window is placed a minute in the past against the real clock: the
    // deadline runs from a window's *end* in wall time, so the 1970 fixture is
    // permanently past every deadline and cannot express "still waiting".
    let root = TempDir::new("finalize-waiting").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let (start, end) = ended_window(0);

    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "pmepoch", 1, 1, start + 100)],
        start,
        end,
    );

    let report = finalize(&spool, &canonical, &["polymarket", "kalshi"], 86_400);
    assert_eq!(report["windows_finalized"], 0);
    assert_eq!(
        report["waiting"][0]["missing"],
        serde_json::json!(["kalshi"])
    );
    assert!(
        receipt_paths(&canonical).is_empty(),
        "nothing may be committed yet"
    );
}

#[test]
fn a_window_that_has_not_ended_is_never_finalized() {
    // Every expected lane has sealed, but the window is still open. That happens
    // for real: a crash mid-window leaves a recovery seal and the restarted
    // splice opens a second segment for the same window. Committing on the
    // strength of the first seal would push everything after the restart onto
    // the late-arrival path, which §5 forbids from entering canonical evidence.
    let root = TempDir::new("finalize-open-window").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let (start, end) = current_window();

    for lane in POLYMARKET_AND_KALSHI {
        write_lane_in_window(
            &spool,
            lane,
            lane,
            &[envelope(lane, "e", 1, 1, start + 100)],
            start,
            end,
        );
    }

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert_eq!(
        report["windows_finalized"], 0,
        "a window that has not reached its boundary is still being written to"
    );
    assert!(receipt_paths(&canonical).is_empty());
}

#[test]
fn an_earlier_waiting_window_holds_back_a_later_complete_one() {
    // §7 forbids finalizing a later window while an earlier one is still inside
    // its seal-wait deadline. Both windows below have ended; the later one is
    // complete on its own and must still not overtake the earlier.
    let root = TempDir::new("finalize-ordering").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let (first_start, first_end) = ended_window(1);
    let (second_start, second_end) = ended_window(0);

    // Window 0: polymarket only, so it waits for kalshi.
    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "a", 1, 1, first_start + 100)],
        first_start,
        first_end,
    );
    // Window 1: both lanes, so complete in isolation.
    for lane in POLYMARKET_AND_KALSHI {
        let delivery = if lane == "polymarket" { 2 } else { 1 };
        write_lane_in_window(
            &spool,
            lane,
            "b",
            &[envelope(
                lane,
                if lane == "polymarket" { "a" } else { "c" },
                delivery,
                delivery,
                second_start + 100,
            )],
            second_start,
            second_end,
        );
    }

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 86_400);
    assert_eq!(
        report["windows_finalized"], 0,
        "the later window is complete, but §7 forbids finalizing past an earlier one \
         still inside its deadline"
    );
    assert!(receipt_paths(&canonical).is_empty());
}

// -- immutability ----------------------------------------------------------

#[test]
fn finalizing_twice_neither_renumbers_nor_rewrites() {
    // §8's retry-after-crash guarantee in its simplest form. A committed window
    // is immutable: re-running must not renumber positions or rewrite bytes a
    // reader may already have taken as evidence.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    let first = receipt(&canonical, 0);
    let evidence =
        fs::read(window_directory(&canonical, 0).join("evidence.ndjson.zst")).expect("read");

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert_eq!(report["windows_finalized"], 0);
    assert_eq!(report["windows_already_committed"], 1);

    assert_eq!(receipt(&canonical, 0), first, "the receipt is immutable");
    assert_eq!(
        fs::read(window_directory(&canonical, 0).join("evidence.ndjson.zst")).expect("read"),
        evidence
    );
}

#[test]
fn unreceipted_compressed_outputs_are_rebuilt_deterministically() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    let directory = canonical.join("date=1970-01-01").join("window=0");
    fs::create_dir_all(&directory).expect("crash-state directory");
    fs::write(directory.join("evidence.ndjson.zst"), b"partial-frame")
        .expect("unreceipted evidence");

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    let output = audit_raw(&canonical);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(visible_ns_in_canonical_order(&canonical), MERGED_ORDER);
    assert!(directory.join("receipt.json").is_file());
}

#[test]
fn positions_continue_across_windows() {
    // The canonical sequence is global, not per window.
    let root = TempDir::new("finalize-sequence").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    for (index, first_delivery) in [(0u64, 1u64), (1, 3)] {
        let start = index * WINDOW_NS;
        write_lane_in_window(
            &spool,
            "polymarket",
            "e",
            &[
                envelope(
                    "polymarket",
                    "e",
                    first_delivery,
                    first_delivery,
                    start + 10,
                ),
                envelope(
                    "polymarket",
                    "e",
                    first_delivery + 1,
                    first_delivery + 1,
                    start + 20,
                ),
            ],
            start,
            start + WINDOW_NS,
        );
    }

    let report = finalize(&spool, &canonical, &["polymarket"], 0);
    assert_eq!(report["windows_finalized"], 2);
    assert_eq!(receipt(&canonical, 0)["last_canonical_seq"], 2);
    assert_eq!(receipt(&canonical, WINDOW_NS)["first_canonical_seq"], 3);
    assert_eq!(report["next_canonical_seq"], 5);
}

#[test]
fn a_delivery_gap_between_adjacent_valid_windows_invalidates_the_lane() {
    // `delivery_index` is lane-lifetime dense, not merely segment- or
    // window-local. Resetting the comparison at each Window used to let both of
    // these windows commit `complete` even though Polymarket delivery 2 is
    // provably absent.
    let root = TempDir::new("finalize-cross-window-gap").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "a", 1, 1, 100)],
        0,
        WINDOW_NS,
    );
    write_lane_in_window(
        &spool,
        "kalshi",
        "b",
        &[envelope("kalshi", "b", 1, 1, 200)],
        0,
        WINDOW_NS,
    );
    // Commit the first boundary in a separate process invocation. The next run
    // must recover continuity from receipts rather than from transient memory.
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    write_lane_in_window(
        &spool,
        "polymarket",
        "c",
        &[envelope("polymarket", "c", 3, 3, WINDOW_NS + 100)],
        WINDOW_NS,
        WINDOW_NS * 2,
    );
    write_lane_in_window(
        &spool,
        "kalshi",
        "d",
        &[envelope("kalshi", "d", 2, 2, WINDOW_NS + 200)],
        WINDOW_NS,
        WINDOW_NS * 2,
    );

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    assert_eq!(receipt(&canonical, 0)["completeness"], "complete");
    let second = receipt(&canonical, WINDOW_NS);
    assert_eq!(second["completeness"], "incomplete");
    assert_eq!(second["invalid_lanes"][0]["lane"], "polymarket");
    assert!(
        second["invalid_lanes"][0]["detail"]
            .as_str()
            .expect("detail")
            .contains("1 -> 3"),
        "{:?}",
        second["invalid_lanes"][0]["detail"]
    );
    assert_eq!(second["present_lanes"], serde_json::json!(["kalshi"]));
}

// -- lane faults -----------------------------------------------------------

#[test]
fn a_lane_missing_a_segment_is_invalid_while_the_rest_commit() {
    // A `delivery_index` gap means one of that lane's segments never reached the
    // window. Merging across it would publish evidence that omits records while
    // the receipt called the lane present.
    let root = TempDir::new("finalize-gap").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    // polymarket wrote indexes 1 and 3 — index 2's segment is gone.
    let broken = segment_path(&spool, "polymarket", 0, "a");
    fs::create_dir_all(broken.parent().expect("parent")).expect("create");
    fs::write(&broken, envelope("polymarket", "a", 1, 1, 100)).expect("write");
    seal_segment_in_window(&broken, "polymarket", 0, 1_800_000_000_000);

    // A second segment of the same window, so the gap is between them.
    let later = broken.with_file_name(segment_name(0, 1, "a"));
    fs::write(&later, envelope("polymarket", "a", 3, 3, 300)).expect("write");
    seal_segment_with(&later, "polymarket", 0, 1_800_000_000_000, |seal| {
        seal["segment_index"] = serde_json::json!(1);
    });

    write_lane(
        &spool,
        "kalshi",
        "b",
        &[envelope("kalshi", "kxepoch", 1, 1, 200)],
    );

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    let receipt = receipt(&canonical, 0);

    assert_eq!(receipt["completeness"], "incomplete");
    assert_eq!(receipt["invalid_lanes"][0]["lane"], "polymarket");
    assert_eq!(receipt["invalid_lanes"][0]["reason"], "lane_invalid");
    assert!(
        receipt["invalid_lanes"][0]["detail"]
            .as_str()
            .expect("detail")
            .contains("missing from the window"),
        "{:?}",
        receipt["invalid_lanes"][0]["detail"]
    );
    // The healthy lane still commits, which is the whole point of a per-lane verdict.
    assert_eq!(receipt["present_lanes"], serde_json::json!(["kalshi"]));
    assert_eq!(receipt["evidence"]["decoded"]["line_count"], 1);
}

#[test]
fn a_corrupt_segment_makes_its_lane_invalid_not_the_window_fatal() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");

    let segment = segment_path(&spool, "polymarket", 0, "a");
    let mut bytes = fs::read(&segment).expect("segment");
    let position = bytes
        .iter()
        .position(|byte| *byte == b'1')
        .expect("mutable byte");
    bytes[position] = b'2';
    fs::write(&segment, bytes).expect("same-length corruption");

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    let receipt = receipt(&canonical, 0);

    assert_eq!(receipt["invalid_lanes"][0]["lane"], "polymarket");
    assert!(
        receipt["invalid_lanes"][0]["detail"]
            .as_str()
            .expect("detail")
            .contains("sha256"),
        "the digest failure is named: {:?}",
        receipt["invalid_lanes"][0]["detail"]
    );
    assert_eq!(receipt["present_lanes"], serde_json::json!(["kalshi"]));
}

// -- the command surface ---------------------------------------------------

#[test]
fn an_undeclared_expectation_is_refused() {
    // Without a declared expectation `lane_missing` cannot mean anything: a lane
    // that dies permanently would simply stop being expected.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");

    let output = Command::new(env!("CARGO_BIN_EXE_indexer-finalize"))
        .arg(&spool)
        .arg(&canonical)
        .output()
        .expect("run indexer-finalize");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--expect-lane"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn an_unrankable_expected_lane_is_refused() {
    // A typo would otherwise expect a lane that can never arrive, so every
    // window would sit out its deadline and commit incomplete forever.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    let output = finalize_raw(&spool, &canonical, &["polymarket", "polymrket"], 0);

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("polymrket"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn lane_ranks_are_printable_for_the_parity_check() {
    let output = Command::new(env!("CARGO_BIN_EXE_indexer-finalize"))
        .arg("--print-lane-ranks")
        .output()
        .expect("run indexer-finalize");
    assert!(output.status.success());

    let ranks: Value = serde_json::from_slice(&output.stdout).expect("JSON ranks");
    assert_eq!(ranks["polymarket"], 0);
    assert_eq!(ranks["kalshi"], 10);
    assert_eq!(ranks["limitless"], 20);
}

// -- lane isolation and window integrity -----------------------------------
//
// Eight defects an independent review of 3c/3d found, each reproduced by probe
// before it was fixed. Two themes run through them: a fault attributable to one
// lane must not cost the window, and anything that cannot be established must
// fail closed rather than be guessed past.

#[test]
fn a_malformed_record_invalidates_its_lane_without_losing_the_window() {
    // Seals cannot reveal this. A segment whose digest, length and line count
    // all check out can still hold a line the envelope parser refuses, and that
    // is only found by parsing during the merge. It used to abort the run: one
    // bad Polymarket line meant a healthy Kalshi lane never committed at all.
    let root = TempDir::new("finalize-schema").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    // Valid JSON with real ordering fields, so every seal check passes — but
    // carrying an unknown field, which the closed schema rejects.
    let unknown_field = serde_json::json!({
        "delivery_index": 2, "record_id": "polymarket-pmepoch-2", "visible_ns": 300,
        "venue": "polymarket", "stream": "public_book", "connection_epoch": "pmepoch",
        "local_counter": 2, "source_cursor": {"type": "unsequenced", "counter": 2},
        "kind": "venue_frame", "raw_payload": "{}", "extra_field": 1,
    });
    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[
            envelope("polymarket", "pmepoch", 1, 1, 100),
            format!(
                "{}\n",
                serde_json::to_string(&unknown_field).expect("encode")
            ),
        ],
        0,
        1_800_000_000_000,
    );
    write_lane(
        &spool,
        "kalshi",
        "b",
        &[envelope("kalshi", "kxepoch", 1, 1, 200)],
    );

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    let receipt = receipt(&canonical, 0);

    assert_eq!(receipt["invalid_lanes"][0]["lane"], "polymarket");
    assert_eq!(receipt["invalid_lanes"][0]["reason"], "lane_invalid");
    assert_eq!(
        receipt["present_lanes"],
        serde_json::json!(["kalshi"]),
        "the healthy lane still commits"
    );
    assert_eq!(receipt["evidence"]["decoded"]["line_count"], 1);
    assert_eq!(receipt["completeness"], "incomplete");
}

#[test]
fn a_clock_regressed_lane_is_excluded_as_invalid() {
    // §2 step 3: "Exclude that lane from the certified merge for the affected
    // window and record it as `lane_invalid`." A regressed clock breaks the one
    // precondition a k-way merge has — that each input is already sorted by the
    // merge key — so it is exclusion, not a note attached to a merged lane.
    let root = TempDir::new("finalize-clock").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    let path = segment_path(&spool, "polymarket", 0, "a");
    fs::create_dir_all(path.parent().expect("parent")).expect("create");
    fs::write(&path, envelope("polymarket", "pmepoch", 1, 1, 100)).expect("write");
    seal_segment_with(&path, "polymarket", 0, 1_800_000_000_000, |seal| {
        seal["visible_non_decreasing"] = serde_json::json!(false);
        seal["ordering_status"] = serde_json::json!("visible_clock_regression");
    });
    write_lane(
        &spool,
        "kalshi",
        "b",
        &[envelope("kalshi", "kxepoch", 1, 1, 200)],
    );

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    let receipt = receipt(&canonical, 0);

    assert_eq!(receipt["invalid_lanes"][0]["lane"], "polymarket");
    assert!(
        receipt["invalid_lanes"][0]["detail"]
            .as_str()
            .expect("detail")
            .contains("visible_clock_regression"),
        "{:?}",
        receipt["invalid_lanes"][0]["detail"]
    );
    assert_eq!(receipt["present_lanes"], serde_json::json!(["kalshi"]));
    assert_eq!(receipt["certified"], false);
    // Not silently reordered into the window — §2's closing requirement.
    assert_eq!(receipt["evidence"]["decoded"]["line_count"], 1);
}

#[test]
fn an_invalid_lane_does_not_satisfy_the_deadline_gate() {
    // §5 step 1 waits for "one **valid** seal from every expected lane". The
    // gate used to check only that a lane key existed, so a corrupt segment made
    // the window complete instantly — skipping the wait during which that lane's
    // next segment could still arrive.
    let root = TempDir::new("finalize-gate").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let (start, end) = ended_window(0);

    let corrupt = segment_path(&spool, "polymarket", start, "a");
    fs::create_dir_all(corrupt.parent().expect("parent")).expect("create");
    fs::write(
        &corrupt,
        envelope("polymarket", "pmepoch", 1, 1, start + 100),
    )
    .expect("write");
    seal_segment_in_window(&corrupt, "polymarket", start, end);
    let mut bytes = fs::read(&corrupt).expect("segment");
    let position = bytes
        .iter()
        .position(|byte| *byte == b'1')
        .expect("mutable byte");
    bytes[position] = b'2';
    fs::write(&corrupt, bytes).expect("same-length corruption");

    write_lane_in_window(
        &spool,
        "kalshi",
        "b",
        &[envelope("kalshi", "kxepoch", 1, 1, start + 200)],
        start,
        end,
    );

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 86_400);
    assert_eq!(
        report["windows_finalized"], 0,
        "a hash-invalid lane has not delivered a valid seal, so the window waits"
    );
    assert!(receipt_paths(&canonical).is_empty());
}

#[test]
fn a_seal_declaring_the_wrong_window_cannot_redefine_it() {
    // Canonical output is keyed by window start, so two seals claiming one start
    // with different ends used to produce two window keys writing to the same
    // directory — the second commit silently replacing the first. Bounds are now
    // computed from the configured period and the aligned start, and a seal that
    // disagrees faults its own lane instead of redefining the window.
    let root = TempDir::new("finalize-bounds").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "pmepoch", 1, 1, 100)],
        0,
        WINDOW_NS,
    );
    // A kalshi seal claiming a double-length window, under a window-0 filename.
    let liar = segment_path(&spool, "kalshi", 0, "b");
    fs::create_dir_all(liar.parent().expect("parent")).expect("create");
    fs::write(&liar, envelope("kalshi", "kxepoch", 1, 1, 200)).expect("write");
    seal_segment_with(&liar, "kalshi", 0, WINDOW_NS * 2, |_| {});

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    let receipts = receipt_paths(&canonical);
    assert_eq!(receipts.len(), 1, "one window start, one output directory");

    let receipt = receipt(&canonical, 0);
    assert_eq!(
        receipt["window_end_ns"], WINDOW_NS,
        "the period decides, not the seal"
    );
    assert_eq!(receipt["invalid_lanes"][0]["lane"], "kalshi");
    assert!(
        receipt["invalid_lanes"][0]["detail"]
            .as_str()
            .expect("detail")
            .contains("configured period"),
        "{:?}",
        receipt["invalid_lanes"][0]["detail"]
    );
    assert_eq!(receipt["present_lanes"], serde_json::json!(["polymarket"]));
}

#[test]
fn a_segment_whose_records_fall_outside_its_window_is_refused() {
    // A seal naming a window its own records were never received in used to
    // validate and certify: every digest checked out, and nothing compared the
    // record bounds against the window they claimed.
    let root = TempDir::new("finalize-outside").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    let path = segment_path(&spool, "polymarket", 0, "a");
    fs::create_dir_all(path.parent().expect("parent")).expect("create");
    // A record from the *second* window, sealed into the first.
    fs::write(&path, envelope("polymarket", "a", 1, 1, WINDOW_NS + 100)).expect("write");
    seal_segment_in_window(&path, "polymarket", 0, WINDOW_NS);

    finalize(&spool, &canonical, &["polymarket"], 0);
    let receipt = receipt(&canonical, 0);

    assert_eq!(receipt["certified"], false);
    assert_eq!(receipt["invalid_lanes"][0]["lane"], "polymarket");
    assert!(
        receipt["invalid_lanes"][0]["detail"]
            .as_str()
            .expect("detail")
            .contains("outside the declared window"),
        "{:?}",
        receipt["invalid_lanes"][0]["detail"]
    );
}

#[test]
fn a_window_with_no_segments_at_all_still_gets_a_receipt() {
    // A total capture outage produces no seal, and a scan of seals cannot see a
    // window that has none. Left out, the outage leaves no trace anywhere: the
    // next window with data commits and the sequence moves on.
    let root = TempDir::new("finalize-absent").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let window = 1_800_000_000_000u64;

    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "a", 1, 1, 100)],
        0,
        window,
    );
    finalize(&spool, &canonical, &["polymarket"], 0);

    // Window 1 never existed. Window 2 now arrives.
    write_lane_in_window(
        &spool,
        "polymarket",
        "c",
        &[envelope("polymarket", "c", 2, 2, window * 2 + 100)],
        window * 2,
        window * 3,
    );
    let report = finalize(&spool, &canonical, &["polymarket"], 0);
    assert_eq!(report["windows_synthesized"], 1);

    let receipt = receipt(&canonical, window);
    assert_eq!(receipt["completeness"], "incomplete");
    assert_eq!(receipt["missing_lanes"][0]["lane"], "polymarket");
    assert_eq!(receipt["missing_lanes"][0]["reason"], "lane_missing");
    assert_eq!(receipt["evidence"]["decoded"]["line_count"], 0);
}

#[test]
fn a_total_outage_after_observed_data_materializes_expired_empty_windows() {
    // Using the newest observed seal as the tiling horizon only revealed a total
    // outage after capture eventually resumed. The wall clock already tells us
    // which windows have passed their finite deadline, so those windows must be
    // receipted while the outage is still happening.
    let root = TempDir::new("finalize-trailing-outage").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let (start, end) = ended_window(2);

    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "a", 1, 1, start + 100)],
        start,
        end,
    );

    // The first receipt anchors this deployment. Historical first-run backfills
    // must not synthesize every window between their oldest fixture and today.
    finalize(&spool, &canonical, &["polymarket"], 0);
    let report = finalize(&spool, &canonical, &["polymarket"], 0);
    assert!(
        report["windows_synthesized"].as_u64().expect("count") >= 1,
        "at least the next fully elapsed window must be made explicit: {report:?}"
    );
    let missing = receipt(&canonical, end);
    assert_eq!(missing["completeness"], "incomplete");
    assert_eq!(missing["missing_lanes"][0]["lane"], "polymarket");
}

#[test]
fn a_torn_seal_is_charged_to_its_own_window_only() {
    // The window is exactly what an unreadable seal fails to say, so it is
    // recovered from the filename. Attaching the fault to every window the lane
    // appears in — the obvious shortcut — made a torn seal for a *later* window
    // mark an earlier healthy one invalid.
    let root = TempDir::new("finalize-torn").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let window = 1_800_000_000_000u64;

    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "a", 1, 1, 100)],
        0,
        window,
    );
    write_lane_in_window(
        &spool,
        "kalshi",
        "b",
        &[envelope("kalshi", "b", 1, 1, 200)],
        0,
        window,
    );
    // A later window whose seal is torn. Its filename still names the window.
    let later = segment_path(&spool, "polymarket", window, "c");
    fs::write(&later, envelope("polymarket", "c", 2, 2, window + 100)).expect("write");
    fs::write(
        later.with_file_name(segment_name(window, 0, "c").replace(".ndjson", ".seal.json")),
        "{ not json",
    )
    .expect("torn seal");

    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let first = receipt(&canonical, 0);
    assert_eq!(
        first["completeness"], "complete",
        "an earlier healthy window must not be damaged by a later torn seal"
    );
    assert_eq!(first["invalid_lanes"], serde_json::json!([]));

    // And the torn seal's own window charges it to the right lane, as
    // `lane_invalid` rather than `lane_missing` — the segment did arrive.
    let second = receipt(&canonical, window);
    assert_eq!(second["invalid_lanes"][0]["lane"], "polymarket");
    assert_eq!(second["invalid_lanes"][0]["reason"], "lane_invalid");
}

#[test]
fn a_corrupt_receipt_stops_the_run_rather_than_reopening_the_window() {
    // The receipt is the commit marker. Skipping one that cannot be read
    // silently reclassified a committed window as open, so the next run
    // re-finalized it — renumbering positions, overwriting bytes a reader may
    // hold, and folding in any segment that arrived meanwhile.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let receipt_file = receipt_paths(&canonical).remove(0);
    fs::write(&receipt_file, "{ corrupt").expect("corrupt the receipt");

    let output = finalize_raw(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert!(
        !output.status.success(),
        "a receipt that cannot be read must fail closed"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("unreadable receipt"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn the_expected_lane_list_is_recorded_exactly_as_supplied() {
    // The receipt claims to record the expectation verbatim, and a verdict is
    // read against it. Sorting made the receipt describe an invocation that
    // never happened.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &["polymarket", "kalshi"], 0);

    assert_eq!(
        receipt(&canonical, 0)["expected_lanes"],
        serde_json::json!(["polymarket", "kalshi"]),
    );
}

// -- deadline, receipts, and durability -------------------------------------
//
// A second review round. The theme: every one of these was a place where
// something *looked* established without having been checked.

#[test]
fn a_record_level_fault_reopens_the_deadline_question() {
    // The gap the record-level retry opened. A lane passes seal and digest
    // validation, so the window reads `Complete` and the deadline is never
    // consulted; the merge then finds a malformed envelope and drops that lane.
    // Committing there publishes an incomplete window while the deployment still
    // had a day in which that lane's next segment could arrive — §5 waits for a
    // *valid* seal, and validity was not established until the records were read.
    let root = TempDir::new("finalize-defer").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    let (start, end) = ended_window(0);

    let unknown_field = serde_json::json!({
        "delivery_index": 2, "record_id": "polymarket-e-2", "visible_ns": start + 200,
        "venue": "polymarket", "stream": "public_book", "connection_epoch": "e",
        "local_counter": 2, "source_cursor": {"type": "unsequenced", "counter": 2},
        "kind": "venue_frame", "raw_payload": "{}", "extra_field": 1,
    });
    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[
            envelope("polymarket", "e", 1, 1, start + 100),
            format!(
                "{}\n",
                serde_json::to_string(&unknown_field).expect("encode")
            ),
        ],
        start,
        end,
    );
    write_lane_in_window(
        &spool,
        "kalshi",
        "b",
        &[envelope("kalshi", "e", 1, 1, start + 150)],
        start,
        end,
    );

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 86_400);
    assert_eq!(report["windows_finalized"], 0);
    assert!(
        receipt_paths(&canonical).is_empty(),
        "nothing partial may survive"
    );
    assert_eq!(
        report["waiting"][0]["unsatisfied"],
        serde_json::json!(["polymarket"])
    );

    // And once the deadline has expired, the same input commits without it.
    let expired = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert_eq!(expired["windows_finalized"], 1);
    let receipt = receipt(&canonical, start);
    assert_eq!(receipt["completeness"], "incomplete");
    assert_eq!(receipt["present_lanes"], serde_json::json!(["kalshi"]));
    assert_eq!(
        receipt["deadline_expired"], true,
        "the post-merge lane fault committed only because the deadline had expired"
    );
}

#[test]
fn a_lone_torn_seal_still_produces_coherent_window_bounds() {
    // The placeholder used to carry no end at all, so a window whose only seal
    // was torn committed a receipt with `window_start_ns > window_end_ns`.
    let root = TempDir::new("finalize-torn-only").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    let path = segment_path(&spool, "polymarket", 0, "a");
    fs::create_dir_all(path.parent().expect("parent")).expect("create");
    fs::write(&path, envelope("polymarket", "a", 1, 1, 100)).expect("write");
    fs::write(
        path.with_file_name(segment_name(0, 0, "a").replace(".ndjson", ".seal.json")),
        "{ not json",
    )
    .expect("torn seal");

    finalize(&spool, &canonical, &["polymarket"], 0);
    let receipt = receipt(&canonical, 0);

    assert_eq!(receipt["window_start_ns"], 0);
    assert_eq!(
        receipt["window_end_ns"], WINDOW_NS,
        "bounds come from the period"
    );
    assert_eq!(receipt["invalid_lanes"][0]["lane"], "polymarket");
}

#[test]
fn a_stray_period_cannot_hide_a_real_window() {
    // Bounds used to be inferred from whichever period appeared most often
    // across observed seals, so a single double-length seal could shift the
    // tiling and skip an entirely absent window between two present ones.
    let root = TempDir::new("finalize-period").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "a", 1, 1, 100)],
        0,
        WINDOW_NS,
    );
    // A segment two windows later, declaring a double-length window.
    let stray = segment_path(&spool, "polymarket", WINDOW_NS * 2, "c");
    fs::create_dir_all(stray.parent().expect("parent")).expect("create");
    fs::write(
        &stray,
        envelope("polymarket", "c", 2, 2, WINDOW_NS * 2 + 100),
    )
    .expect("write");
    seal_segment_with(&stray, "polymarket", WINDOW_NS * 2, WINDOW_NS * 4, |_| {});

    finalize(&spool, &canonical, &["polymarket"], 0);

    let starts: Vec<u64> = receipt_paths(&canonical)
        .iter()
        .map(|path| {
            let receipt: Value =
                serde_json::from_slice(&fs::read(path).expect("receipt")).expect("JSON");
            receipt["window_start_ns"].as_u64().expect("start")
        })
        .collect();
    assert!(
        starts.contains(&WINDOW_NS),
        "the empty middle window must still get a receipt: {starts:?}"
    );
}

#[test]
fn a_committed_window_is_not_revalidated_and_its_late_seals_do_not_block() {
    // Committed windows used to enter the readiness plan and be validated before
    // being skipped — rehashing the whole retained spool every run — and a late
    // segment landing on one could fault it and stop every later window.
    let root = TempDir::new("finalize-late").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "polymarket",
        "a",
        &[envelope("polymarket", "a", 1, 1, 100)],
        0,
        WINDOW_NS,
    );
    finalize(&spool, &canonical, &["polymarket"], 0);
    let committed = receipt(&canonical, 0);

    // A late arrival for the already-committed window, plus a fresh window.
    write_lane_in_window(
        &spool,
        "kalshi",
        "z",
        &[envelope("kalshi", "z", 1, 1, 150)],
        0,
        WINDOW_NS,
    );
    write_lane_in_window(
        &spool,
        "polymarket",
        "b",
        &[envelope("polymarket", "b", 2, 2, WINDOW_NS + 100)],
        WINDOW_NS,
        WINDOW_NS * 2,
    );

    let report = finalize(&spool, &canonical, &["polymarket"], 0);
    assert_eq!(
        report["windows_finalized"], 1,
        "the later window still commits"
    );
    assert_eq!(
        receipt(&canonical, 0),
        committed,
        "§5: a late seal must not change a committed window's canonical hash or positions"
    );
    let late = report["late_after_finalization"].as_array().expect("array");
    assert_eq!(
        late.len(),
        1,
        "and it is reported rather than merged: {late:?}"
    );
}

#[test]
fn an_empty_late_segment_is_not_conflated_with_another_lanes_empty_input() {
    // Every empty file has SHA-256(empty). Hash-only late detection therefore
    // treated a late empty Kalshi segment as though it were the already consumed
    // empty Polymarket segment and omitted the late-arrival label entirely.
    let root = TempDir::new("finalize-empty-late").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane(&spool, "polymarket", "a", &[]);
    finalize(&spool, &canonical, &["polymarket"], 0);

    write_lane(&spool, "kalshi", "b", &[]);
    let report = finalize(&spool, &canonical, &["polymarket"], 0);
    let late = report["late_after_finalization"].as_array().expect("array");

    assert_eq!(
        late.len(),
        1,
        "the segment identity is more than its digest: {late:?}"
    );
    assert!(late[0].as_str().expect("late path").starts_with("kalshi/"));
}

#[test]
fn an_empty_object_is_not_a_receipt() {
    // `{}` parsed as JSON and was accepted as proof a window was committed,
    // which is the entire claim a receipt exists to make.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    fs::write(receipt_paths(&canonical).remove(0), "{}\n").expect("blank the receipt");
    let output = finalize_raw(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("unreadable receipt"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn a_receipt_whose_evidence_is_gone_is_not_trusted() {
    // A structurally valid receipt naming a file that no longer exists was
    // accepted as a watermark, so the sequence advanced past evidence that was
    // not there.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let evidence = receipt_paths(&canonical)
        .remove(0)
        .with_file_name("evidence.ndjson.zst");
    fs::remove_file(&evidence).expect("delete the evidence");

    let output = finalize_raw(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert!(
        !output.status.success(),
        "a receipt is only as good as what it names"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("evidence.ndjson"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn a_window_period_that_does_not_tile_the_day_is_refused() {
    // The finalizer's period has to match the writer's, and the writer refuses
    // anything that does not divide a UTC day — otherwise windows drift across
    // midnight and the `date=` partition disagrees with the records inside it.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");

    let output = Command::new(env!("CARGO_BIN_EXE_indexer-finalize"))
        .arg(&spool)
        .arg(&canonical)
        .arg("--expect-lane")
        .arg("polymarket")
        .arg("--window-seconds")
        .arg("700")
        .output()
        .expect("run indexer-finalize");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("divisor of 86400"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// -- continuity verdicts and the watermark ----------------------------------

/// A Kalshi-shaped envelope: `update_range` is the only cursor whose verdict
/// depends on the record before it, so it is the only one that can show whether
/// state crossed a window boundary.
fn ranged(delivery: u64, visible_ns: u64, first: u64) -> String {
    let record = serde_json::json!({
        "delivery_index": delivery,
        "record_id": format!("kalshi-e-{delivery}"),
        "visible_ns": visible_ns,
        "venue": "kalshi",
        "stream": "public_book",
        "connection_epoch": "e",
        "local_counter": delivery,
        "source_cursor": {"type": "update_range", "first": first, "last": first + 9,
                          "previous_last": first - 1},
        "kind": "venue_frame",
        "raw_payload": "{}",
    });
    format!(
        "{}\n",
        serde_json::to_string(&record).expect("encode envelope")
    )
}

fn verdicts(canonical: &Path, start_ns: u64) -> Vec<String> {
    read_lines(&window_directory(canonical, start_ns).join("provenance.ndjson.zst"))
        .into_iter()
        .map(|line| {
            line["continuity_verdict"]
                .as_str()
                .expect("verdict")
                .to_owned()
        })
        .collect()
}

fn envelope_with_identity(
    venue: &str,
    delivery: u64,
    visible_ns: u64,
    record_id: &str,
    raw_payload: &str,
) -> String {
    let record = serde_json::json!({
        "delivery_index": delivery,
        "record_id": record_id,
        "visible_ns": visible_ns,
        "venue": venue,
        "stream": "public_book",
        "connection_epoch": "identity-epoch",
        "local_counter": delivery,
        "source_cursor": {"type": "unsequenced", "counter": delivery},
        "kind": "venue_frame",
        "raw_payload": raw_payload,
    });
    format!(
        "{}\n",
        serde_json::to_string(&record).expect("encode identity envelope")
    )
}

#[test]
fn provenance_carries_the_continuity_verdict() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    // Polymarket and Kalshi both publish unsequenced cursors in this fixture, so
    // every record is `unsequenced_venue` — our counter is the only order there
    // is, which is exactly what the label says.
    assert!(
        verdicts(&canonical, 0)
            .iter()
            .all(|verdict| verdict == "unsequenced_venue"),
        "{:?}",
        verdicts(&canonical, 0)
    );
}

#[test]
fn disk_backed_identity_preserves_exact_duplicate_and_conflict_verdicts() {
    let root = TempDir::new("finalize-disk-identity").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");
    write_lane(
        &spool,
        "polymarket",
        "identity",
        &[
            envelope_with_identity("polymarket", 1, 100, "same-id", "same-content"),
            envelope_with_identity("polymarket", 2, 200, "same-id", "same-content"),
            envelope_with_identity("polymarket", 3, 300, "same-id", "changed-content"),
        ],
    );

    let report = finalize(&spool, &canonical, &["polymarket"], 0);
    assert_eq!(report["max_identity_records_in_memory"], 0);
    assert_eq!(
        verdicts(&canonical, 0),
        vec!["unsequenced_venue", "duplicate", "conflict"]
    );
    assert!(
        !window_directory(&canonical, 0)
            .join(".record-identity.sqlite.open")
            .exists(),
        "the per-attempt index is not a durable canonical artifact"
    );
}

#[test]
fn lane_retry_does_not_carry_excluded_identity_into_the_surviving_merge() {
    let root = TempDir::new("finalize-identity-retry").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    let malformed = serde_json::json!({
        "delivery_index": 3,
        "record_id": "malformed",
        "visible_ns": 300,
        "venue": "polymarket",
        "stream": "public_book",
        "connection_epoch": "identity-epoch",
        "local_counter": 3,
        "source_cursor": {"type": "unsequenced", "counter": 3},
        "kind": "venue_frame",
        "raw_payload": "{}",
        "unknown": true,
    });
    write_lane(
        &spool,
        "polymarket",
        "faulting",
        &[
            envelope_with_identity("polymarket", 1, 100, "cross-lane-id", "same-content"),
            envelope_with_identity("polymarket", 2, 150, "polymarket-second", "other"),
            format!(
                "{}\n",
                serde_json::to_string(&malformed).expect("encode malformed envelope")
            ),
        ],
    );
    write_lane(
        &spool,
        "kalshi",
        "surviving",
        &[envelope_with_identity(
            "kalshi",
            1,
            200,
            "cross-lane-id",
            "same-content",
        )],
    );

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert_eq!(report["max_identity_records_in_memory"], 0);
    assert_eq!(verdicts(&canonical, 0), vec!["unsequenced_venue"]);
    assert_eq!(
        receipt(&canonical, 0)["invalid_lanes"][0]["lane"],
        "polymarket"
    );
}

#[test]
fn ordering_state_crosses_a_window_boundary() {
    // The property the whole carried-state design exists for. Without it the
    // first record of the second window reads `bootstrap` — "nothing to compare
    // against" — when there plainly was something.
    let root = TempDir::new("finalize-carry").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "kalshi",
        "a",
        &[ranged(1, 10, 100), ranged(2, 20, 110)],
        0,
        WINDOW_NS,
    );
    write_lane_in_window(
        &spool,
        "kalshi",
        "b",
        &[ranged(3, WINDOW_NS + 10, 120)],
        WINDOW_NS,
        WINDOW_NS * 2,
    );

    finalize(&spool, &canonical, &["kalshi"], 0);

    assert_eq!(verdicts(&canonical, 0), vec!["bootstrap", "continuous"]);
    assert_eq!(
        verdicts(&canonical, WINDOW_NS),
        vec!["continuous"],
        "the second window continues the first rather than restarting"
    );
}

#[test]
fn a_lane_that_goes_quiet_keeps_its_place() {
    // A quiet lane emits an empty sealed segment by design (§3) and its
    // connection is still open. Retiring its epoch on silence rather than on
    // reconnect made the window after the silence read `bootstrap`, and would
    // have hidden a `local_counter` break straddling the quiet window.
    let root = TempDir::new("finalize-quiet").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "kalshi",
        "a",
        &[ranged(1, 10, 100), ranged(2, 20, 110)],
        0,
        WINDOW_NS,
    );
    write_lane_in_window(&spool, "kalshi", "b", &[], WINDOW_NS, WINDOW_NS * 2);
    write_lane_in_window(
        &spool,
        "kalshi",
        "c",
        &[ranged(3, WINDOW_NS * 2 + 10, 120)],
        WINDOW_NS * 2,
        WINDOW_NS * 3,
    );

    finalize(&spool, &canonical, &["kalshi"], 0);

    assert!(
        verdicts(&canonical, WINDOW_NS).is_empty(),
        "the quiet window holds no records"
    );
    assert_eq!(
        verdicts(&canonical, WINDOW_NS * 2),
        vec!["continuous"],
        "an epoch is retired when its lane reconnects, not when it falls silent"
    );
}

#[test]
fn provenance_is_identical_however_the_run_was_split() {
    // §8's retry-after-crash guarantee reaches provenance too. A classifier
    // reset per run would make the second window's verdicts depend on whether
    // the first was finalized in the same invocation.
    let build = |spool: &Path, windows: u64| {
        for index in 0..windows {
            let start = index * WINDOW_NS;
            let delivery = index * 2 + 1;
            write_lane_in_window(
                spool,
                "kalshi",
                &format!("s{index}"),
                &[
                    ranged(delivery, start + 10, 100 + index * 20),
                    ranged(delivery + 1, start + 20, 110 + index * 20),
                ],
                start,
                start + WINDOW_NS,
            );
        }
    };

    let root = TempDir::new("finalize-split").expect("temporary directory");
    let (one_spool, one) = (
        root.path().join("one/spool"),
        root.path().join("one/canonical"),
    );
    build(&one_spool, 2);
    finalize(&one_spool, &one, &["kalshi"], 0);

    let (split_spool, split) = (
        root.path().join("split/spool"),
        root.path().join("split/canonical"),
    );
    build(&split_spool, 1);
    finalize(&split_spool, &split, &["kalshi"], 0);
    build(&split_spool, 2);
    finalize(&split_spool, &split, &["kalshi"], 0);

    for start in [0, WINDOW_NS] {
        for name in ["evidence.ndjson.zst", "provenance.ndjson.zst"] {
            assert_eq!(
                fs::read(window_directory(&one, start).join(name)).expect("one run"),
                fs::read(window_directory(&split, start).join(name)).expect("split run"),
                "{name} for window {start} depends on where the run stopped"
            );
        }
    }
}

#[test]
fn the_watermark_records_where_finalization_reached() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let mark: Value =
        serde_json::from_slice(&fs::read(canonical.join("watermark.json")).expect("watermark"))
            .expect("watermark JSON");
    let receipt = receipt(&canonical, 0);

    assert_eq!(mark["last_window_start_ns"], 0);
    assert_eq!(mark["last_canonical_seq"], receipt["last_canonical_seq"]);
    assert_eq!(
        mark["evidence_sha256"],
        receipt["evidence"]["decoded"]["sha256"]
    );
    assert_eq!(mark["certified"], true);
    assert_eq!(mark["quarantined"], serde_json::json!([]));
}

#[test]
fn a_deleted_watermark_rebuilds_from_the_receipts() {
    // It is a derived index, not a second authority — the same relationship a
    // seal has to the tape. Losing it must cost a rebuild, never a re-finalize.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    let original = fs::read(canonical.join("watermark.json")).expect("watermark");
    let receipt_before = receipt(&canonical, 0);
    fs::remove_file(canonical.join("watermark.json")).expect("delete watermark");

    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert_eq!(
        report["windows_finalized"], 0,
        "a lost watermark must not re-finalize"
    );
    assert_eq!(
        fs::read(canonical.join("watermark.json")).expect("rebuilt watermark"),
        original,
        "the rebuild reproduces it exactly, not approximately"
    );
    assert_eq!(receipt(&canonical, 0), receipt_before);
}

#[test]
fn the_window_boundary_invariant_is_enforced_before_the_merge() {
    // §7 asks for the visible-time boundary to be checked before the watermark
    // advances. It is — but upstream of the watermark and more strictly than a
    // flag would be: `validate_sealed_segment` refuses a segment whose records
    // fall outside the window it declares, so a window holds only instants
    // inside its own bounds and consecutive windows cannot overlap. A lane whose
    // clock stepped back is `lane_invalid` and never enters the merge.
    //
    // `watermark::clock_faults` keeps the comparison as defence in depth and is
    // unit-tested directly, because the binary cannot produce a state where it
    // fires.
    let root = TempDir::new("finalize-boundary").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "kalshi",
        "a",
        &[ranged(1, WINDOW_NS - 10, 100)],
        0,
        WINDOW_NS,
    );
    // A second-window segment carrying a first-window instant.
    let backwards = segment_path(&spool, "kalshi", WINDOW_NS, "b");
    fs::create_dir_all(backwards.parent().expect("parent")).expect("create");
    fs::write(&backwards, ranged(2, WINDOW_NS - 5, 110)).expect("write");
    seal_segment_in_window(&backwards, "kalshi", WINDOW_NS, WINDOW_NS * 2);

    finalize(&spool, &canonical, &["kalshi"], 0);

    assert_eq!(receipt(&canonical, 0)["certified"], true);
    let regressed = receipt(&canonical, WINDOW_NS);
    assert_eq!(regressed["invalid_lanes"][0]["lane"], "kalshi");
    assert!(
        regressed["invalid_lanes"][0]["detail"]
            .as_str()
            .expect("detail")
            .contains("outside the declared window"),
        "{:?}",
        regressed["invalid_lanes"][0]["detail"]
    );
    assert_eq!(regressed["certified"], false);
    assert_eq!(
        regressed["evidence"]["decoded"]["line_count"], 0,
        "the record is not silently reordered into the window (§2)"
    );
}

#[test]
fn the_watermark_advances_past_an_uncertified_window() {
    // Quarantine rather than stop: one bad window must not freeze canonical
    // output for every healthy lane behind it.
    let root = TempDir::new("finalize-advance").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(&spool, "kalshi", "a", &[ranged(1, 10, 100)], 0, WINDOW_NS);
    // An empty middle window with an expected lane missing leaves it incomplete.
    write_lane_in_window(
        &spool,
        "kalshi",
        "c",
        &[ranged(2, WINDOW_NS * 2 + 10, 110)],
        WINDOW_NS * 2,
        WINDOW_NS * 3,
    );

    finalize(&spool, &canonical, &["kalshi"], 0);

    assert_eq!(receipt(&canonical, WINDOW_NS)["completeness"], "incomplete");
    let mark: Value =
        serde_json::from_slice(&fs::read(canonical.join("watermark.json")).expect("watermark"))
            .expect("watermark JSON");
    assert_eq!(
        mark["last_window_start_ns"],
        WINDOW_NS * 2,
        "an incomplete window does not hold the watermark back"
    );
}

#[test]
fn a_quiet_window_does_not_hand_out_committed_positions_again() {
    // The watermark used to take its position from the newest receipt. An empty
    // window assigns none, so the next window with records restarted at 1 —
    // handing out positions that were already committed elsewhere.
    let root = TempDir::new("finalize-seq-reset").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "kalshi",
        "a",
        &[ranged(1, 10, 100), ranged(2, 20, 110)],
        0,
        WINDOW_NS,
    );
    write_lane_in_window(&spool, "kalshi", "b", &[], WINDOW_NS, WINDOW_NS * 2);
    finalize(&spool, &canonical, &["kalshi"], 0);

    let mark: Value =
        serde_json::from_slice(&fs::read(canonical.join("watermark.json")).expect("watermark"))
            .expect("watermark JSON");
    assert_eq!(
        mark["last_canonical_seq"], 2,
        "the empty window leaves the count where it was"
    );

    // A later window must continue, not collide.
    write_lane_in_window(
        &spool,
        "kalshi",
        "c",
        &[ranged(3, WINDOW_NS * 2 + 10, 120)],
        WINDOW_NS * 2,
        WINDOW_NS * 3,
    );
    finalize(&spool, &canonical, &["kalshi"], 0);
    assert_eq!(receipt(&canonical, WINDOW_NS * 2)["first_canonical_seq"], 3);
}

// -- the seal is a claim, not a fact ----------------------------------------

#[test]
fn a_seal_that_misreports_its_own_records_faults_its_lane() {
    // The digest proves the bytes have not changed. It proves nothing about
    // whether the seal's *summary* of them is true — and only the summary was
    // being checked against the window. A segment whose record sat past the
    // window end, under a seal claiming an in-window value, committed into a
    // `complete`, `certified` window.
    let root = TempDir::new("finalize-lying-seal").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    let path = segment_path(&spool, "kalshi", 0, "a");
    fs::create_dir_all(path.parent().expect("parent")).expect("create");
    // Ten nanoseconds past the window end.
    fs::write(&path, ranged(1, WINDOW_NS + 10, 100)).expect("write");
    seal_segment_with(&path, "kalshi", 0, WINDOW_NS, |seal| {
        seal["first_visible_ns"] = serde_json::json!(100);
        seal["last_visible_ns"] = serde_json::json!(100);
    });

    finalize(&spool, &canonical, &["kalshi"], 0);
    let receipt = receipt(&canonical, 0);

    assert_eq!(receipt["completeness"], "incomplete");
    assert_eq!(receipt["certified"], false);
    assert_eq!(receipt["invalid_lanes"][0]["lane"], "kalshi");
    assert_eq!(
        receipt["evidence"]["decoded"]["line_count"], 0,
        "the out-of-window record must not reach canonical evidence"
    );
}

#[test]
fn a_seal_that_misreports_its_delivery_bounds_faults_its_lane() {
    let root = TempDir::new("finalize-lying-delivery").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    let path = segment_path(&spool, "kalshi", 0, "a");
    fs::create_dir_all(path.parent().expect("parent")).expect("create");
    fs::write(
        &path,
        format!("{}{}", ranged(1, 10, 100), ranged(2, 20, 110)),
    )
    .expect("write");
    seal_segment_with(&path, "kalshi", 0, WINDOW_NS, |seal| {
        // Claims the segment ends one record later than it does.
        seal["last_delivery_index"] = serde_json::json!(3);
        seal["line_count"] = serde_json::json!(3);
    });

    finalize(&spool, &canonical, &["kalshi"], 0);
    let receipt = receipt(&canonical, 0);
    assert_eq!(receipt["invalid_lanes"][0]["lane"], "kalshi");
    assert_eq!(receipt["evidence"]["decoded"]["line_count"], 0);
}

#[test]
fn a_window_behind_the_watermark_is_refused_before_anything_is_written() {
    // Every position after it is already assigned, so committing it runs the
    // canonical sequence backwards in visible time. It used to publish the
    // receipt first and report the contradiction afterwards — the one order that
    // cannot be undone. §5's correction policy owns this case.
    let root = TempDir::new("finalize-historical").expect("temporary directory");
    let spool = root.path().join("spool");
    let canonical = root.path().join("canonical");

    write_lane_in_window(
        &spool,
        "kalshi",
        "b",
        &[ranged(1, WINDOW_NS + 10, 100)],
        WINDOW_NS,
        WINDOW_NS * 2,
    );
    finalize(&spool, &canonical, &["kalshi"], 0);
    let committed = receipt_paths(&canonical);

    // An older window turns up afterwards.
    write_lane_in_window(&spool, "kalshi", "a", &[ranged(2, 10, 110)], 0, WINDOW_NS);
    let output = finalize_raw(&spool, &canonical, &["kalshi"], 0);

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("before the watermark"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        receipt_paths(&canonical),
        committed,
        "nothing may be written for a window that cannot be positioned"
    );
}

#[test]
fn a_second_finalizer_on_one_root_is_refused() {
    // Two runs share every intermediate name and race over the same receipts.
    // The lease is held for the whole run; the message names the holder, because
    // a killed finalizer leaves the file behind.
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    fs::create_dir_all(&canonical).expect("create canonical root");
    fs::write(
        canonical.join(".finalize.lease"),
        "{\"pid\": 1, \"acquired_ns\": 0}\n",
    )
    .expect("pre-existing lease");

    let output = finalize_raw(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("another finalizer holds"), "{stderr}");
    assert!(receipt_paths(&canonical).is_empty());
}

#[test]
fn the_lease_is_released_when_the_run_ends() {
    let (root, spool, _) = interleaved_fixture();
    let canonical = root.path().join("canonical");
    finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);

    assert!(
        !canonical.join(".finalize.lease").exists(),
        "a finished run holds nothing"
    );
    // And the next run proceeds normally.
    let report = finalize(&spool, &canonical, &POLYMARKET_AND_KALSHI, 0);
    assert_eq!(report["windows_already_committed"], 1);
}
