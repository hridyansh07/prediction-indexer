#![allow(dead_code)]
//! Shared fixtures. Both ordering tests build on `interleaved_fixture` so the
//! two global orders are demonstrably derived from one identical set of bytes.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use sha2::{Digest, Sha256};
use tempdir::TempDir;

/// The window every fixture segment declares, unless a test says otherwise.
pub const WINDOW_START_NS: u64 = 0;
pub const WINDOW_END_NS: u64 = 1_800_000_000_000;

pub fn seal_segment(path: &Path, lane: &str) {
    seal_segment_in_window(path, lane, WINDOW_START_NS, WINDOW_END_NS);
}

pub fn seal_segment_in_window(path: &Path, lane: &str, window_start_ns: u64, window_end_ns: u64) {
    seal_segment_with(path, lane, window_start_ns, window_end_ns, |_| {});
}

/// Seals a segment, letting a test bend one field of the sidecar.
///
/// The mutation runs on the finished document, so a fixture can express "a seal
/// that is internally consistent but declares a clock regression" without
/// rebuilding the whole thing.
pub fn seal_segment_with(
    path: &Path,
    lane: &str,
    window_start_ns: u64,
    window_end_ns: u64,
    bend: impl FnOnce(&mut Value),
) {
    let bytes = fs::read(path).expect("read segment for seal");
    let records: Vec<Value> = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("fixture envelope"))
        .collect();
    let first = records.first();
    let last = records.last();
    let mut epochs = Vec::new();
    for record in &records {
        let epoch = record["connection_epoch"]
            .as_str()
            .expect("connection_epoch");
        if epochs.last().is_none_or(|previous| *previous != epoch) {
            epochs.push(epoch);
        }
    }
    let data_file = path.file_name().expect("data filename").to_string_lossy();
    let stem = data_file.strip_suffix(".ndjson").expect("ndjson suffix");
    let seal_path = path.with_file_name(format!("{stem}.seal.json"));
    let seal = serde_json::json!({
        "seal_version": 1,
        "lane_id": lane,
        "window_start_ns": window_start_ns,
        "window_end_ns": window_end_ns,
        "data_file": data_file,
        "byte_length": bytes.len(),
        "line_count": records.len(),
        "sha256": format!("{:x}", Sha256::digest(&bytes)),
        "first_delivery_index": first.and_then(|record| record["delivery_index"].as_u64()),
        "last_delivery_index": last.and_then(|record| record["delivery_index"].as_u64()),
        "first_visible_ns": first.and_then(|record| record["visible_ns"].as_u64()),
        "last_visible_ns": last.and_then(|record| record["visible_ns"].as_u64()),
        "visible_non_decreasing": true,
        "delivery_index_dense": true,
        "segment_id": "fixture",
        "segment_index": 0,
        "seal_reason": "test",
        "ordering_status": "ok",
        "epochs": epochs,
        "repaired_bytes": 0,
        "created_ns": 0,
        "writer_version": 1
    });
    let mut seal = seal;
    bend(&mut seal);
    fs::write(
        seal_path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&seal).expect("encode seal")
        ),
    )
    .expect("write seal");
}

pub fn write_sealed(path: &Path, lane: &str, content: impl AsRef<[u8]>) {
    fs::create_dir_all(path.parent().expect("segment parent")).expect("create segment parent");
    fs::write(path, content).expect("write segment");
    seal_segment(path, lane);
}

/// One envelope with an explicit receive time, so a fixture can make file order
/// and clock order disagree on purpose.
pub fn envelope(venue: &str, epoch: &str, delivery: u64, counter: u64, visible_ns: u64) -> String {
    let record = serde_json::json!({
        "delivery_index": delivery,
        "record_id": format!("{venue}-{epoch}-{counter}"),
        "visible_ns": visible_ns,
        "venue": venue,
        "stream": "public_book",
        "connection_epoch": epoch,
        "local_counter": counter,
        "source_cursor": {"type": "unsequenced", "counter": counter},
        "kind": "venue_frame",
        "raw_payload": format!(r#"{{"visible_ns":{visible_ns}}}"#),
    });
    format!(
        "{}\n",
        serde_json::to_string(&record).expect("encode envelope")
    )
}

/// A segment filename the writer would actually produce.
///
/// The stamp encodes the window start, and the finalizer places a segment by its
/// filename — so a fixture whose name and seal disagree is not a fixture of
/// anything real. `segment_id` decides order between two lanes in one window,
/// which is the whole of what `file_order` has to go on across lanes.
pub fn segment_name(window_start_ns: u64, segment_index: u64, segment_id: &str) -> String {
    format!(
        "{}-{segment_index:03}-{segment_id}.ndjson",
        indexer_finalize::segment_stamp(window_start_ns)
    )
}

pub fn write_lane(spool: &Path, lane: &str, segment_id: &str, lines: &[String]) {
    write_lane_in_window(
        spool,
        lane,
        segment_id,
        lines,
        WINDOW_START_NS,
        WINDOW_END_NS,
    );
}

/// What `indexer-ingest` produces over `interleaved_fixture`.
pub const FILE_ORDER: [u64; 3] = [100, 300, 200];

/// What `indexer-finalize` produces over the identical bytes, merging on
/// `(visible_ns, lane_rank, delivery_index)`.
pub const MERGED_ORDER: [u64; 3] = [100, 200, 300];

/// Two lanes whose files sort one way while their clocks demand another.
///
/// `polymarket` opened first, so its filename sorts first, and the reader
/// consumes it to completion before opening the kalshi file. But the kalshi
/// record was received *between* the two polymarket records.
///
/// ```text
/// file order   polymarket@100, polymarket@300, kalshi@200
/// clock order  polymarket@100, kalshi@200,     polymarket@300
/// ```
pub fn interleaved_fixture() -> (TempDir, PathBuf, PathBuf) {
    let root = TempDir::new("indexer-ordering").expect("temporary directory");
    let spool = root.path().join("spool");
    let store = root.path().join("store");

    // Both lanes are in one window, so their stamps are identical and the
    // segment id decides which file the reader opens first — `a` before `b`.
    // That is all `file_order` has to go on across lanes, which is the point.
    write_lane(
        &spool,
        "polymarket",
        "a",
        &[
            envelope("polymarket", "pmepoch", 1, 1, 100),
            envelope("polymarket", "pmepoch", 2, 2, 300),
        ],
    );
    // Carries a record received at 200 — between the two above.
    write_lane(
        &spool,
        "kalshi",
        "b",
        &[envelope("kalshi", "kxepoch", 1, 1, 200)],
    );

    (root, spool, store)
}

/// Now, in nanoseconds since the epoch.
///
/// Deadline behaviour is measured against wall time from a window's *end*, so a
/// test about waiting has to place its window relative to the real clock. The
/// fixture above sits in 1970 and is therefore always long past any deadline,
/// which is exactly what makes it useful for the other tests.
pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos() as u64
}

pub const SECOND_NS: u64 = 1_000_000_000;

/// One lane's segment, sealed into an explicit window.
pub fn write_lane_in_window(
    spool: &Path,
    lane: &str,
    segment_id: &str,
    lines: &[String],
    window_start_ns: u64,
    window_end_ns: u64,
) -> PathBuf {
    let path = segment_path(spool, lane, window_start_ns, segment_id);
    fs::create_dir_all(path.parent().expect("segment parent")).expect("create segment parent");
    fs::write(&path, lines.concat()).expect("write segment");
    seal_segment_in_window(&path, lane, window_start_ns, window_end_ns);
    path
}

pub fn segment_path(spool: &Path, lane: &str, window_start_ns: u64, segment_id: &str) -> PathBuf {
    spool
        .join(format!("lane={lane}"))
        .join("date=2026-07-30")
        .join(segment_name(window_start_ns, 0, segment_id))
}

/// Every committed receipt under a canonical root, oldest window first.
///
/// Found by walking rather than by building a path: a window's `date=` partition
/// comes from its start, so a fixture placed relative to the current clock lands
/// under today's date rather than a fixed one.
pub fn receipt_paths(canonical: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(dates) = fs::read_dir(canonical) else {
        return found;
    };
    for date in dates.filter_map(Result::ok) {
        let Ok(windows) = fs::read_dir(date.path()) else {
            continue;
        };
        for window in windows.filter_map(Result::ok) {
            let receipt = window.path().join("receipt.json");
            if receipt.is_file() {
                found.push(receipt);
            }
        }
    }
    found.sort();
    found
}

/// The window period every fixture uses, matching the finalizer's default.
pub const WINDOW_NS: u64 = 1800 * SECOND_NS;

/// The window currently being written to: it has not ended yet.
pub fn current_window() -> (u64, u64) {
    let start = (now_ns() / WINDOW_NS) * WINDOW_NS;
    (start, start + WINDOW_NS)
}

/// An aligned window that has already ended, `windows_back` before the current
/// one (0 = the one that just closed).
///
/// Windows tile the UTC day from the epoch, and the finalizer now rejects a
/// start that is not on that grid — so a test about deadlines has to sit on the
/// grid too, not merely at "now minus a minute".
pub fn ended_window(windows_back: u64) -> (u64, u64) {
    let end = (now_ns() / WINDOW_NS) * WINDOW_NS - windows_back * WINDOW_NS;
    (end - WINDOW_NS, end)
}
