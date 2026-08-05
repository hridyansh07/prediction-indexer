//! What `EvidenceSeq` currently means, pinned so that changing it is deliberate.
//!
//! This is a characterisation test. It asserts the ingester's **present, wrong**
//! cross-lane order, and it exists because nothing else in either language
//! constrains ordering at all — the sealed-pipeline work could have silently
//! changed the global sequence with every suite still green.
//!
//! Phase 1 of `docs/SEALED_CAPTURE_PIPELINE_V1.md` §8 requires the defect be
//! demonstrated before the fix is accepted. Demonstrating it is pinning it: a
//! test left failing until the phase 3 finalizer lands would sit red through the
//! whole of phase 2 and hide anything else that broke in the meantime.
//!
//! **These assertions are permanent, not temporary.** The finalizer does not
//! replace this path — §8.3 puts the k-way merge "behind a separate
//! command/mode", and §5 keeps the filename ordering available as `file_order`.
//! So what is pinned here is a real and lasting property of *this* binary, and
//! phase 3 adds its own test asserting the merged order over the same fixture
//! rather than inverting these.
//!
//! `interleaved_fixture` exists to be reused verbatim by that test, so the two
//! orders are demonstrably derived from one identical set of bytes.

mod common;

use std::path::Path;
use std::process::Command;

use common::{FILE_ORDER, MERGED_ORDER, envelope, interleaved_fixture, write_lane};
use rusqlite::Connection;
use serde_json::Value;
use tempdir::TempDir;

fn ingest(spool: &Path, store: &Path) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_indexer-ingest"))
        .arg(spool)
        .arg(store)
        .output()
        .expect("run indexer-ingest");
    assert!(
        output.status.success(),
        "ingest failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("JSON report")
}

/// The receive times of every stored line, in the global order the store assigned.
fn visible_ns_in_evidence_order(store: &Path) -> Vec<u64> {
    let connection = Connection::open(store.join("store.db")).expect("open store");
    let mut statement = connection
        .prepare("SELECT raw_line FROM evidence ORDER BY seq")
        .expect("prepare evidence scan");
    let rows = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .expect("query evidence");
    rows.map(|row| {
        let line = row.expect("evidence row");
        let parsed: Value = serde_json::from_slice(&line).expect("stored line is an envelope");
        parsed["visible_ns"].as_u64().expect("visible_ns")
    })
    .collect()
}

#[test]
fn evidence_seq_is_file_order_and_not_receive_order() {
    let (_root, spool, store) = interleaved_fixture();
    let report = ingest(&spool, &store);
    assert_eq!(report["facts_committed"], 3);

    // The finalizer, over these same bytes, must produce `MERGED_ORDER`. It does
    // so in its own mode and its own test; this one keeps describing file order.
    assert_eq!(
        visible_ns_in_evidence_order(&store),
        FILE_ORDER,
        "EvidenceSeq is assigned in filename order, so a record received between \
         two others in another lane is sequenced after both"
    );
    assert_ne!(
        FILE_ORDER, MERGED_ORDER,
        "the fixture must actually distinguish the two orders, or it proves nothing"
    );
}

#[test]
fn a_later_lane_file_cannot_precede_an_earlier_one_however_early_its_records() {
    // The defect stated as the property that actually bites: no record in the
    // second file, however early its receive time, can be sequenced before a
    // record in the first. Whole files are consumed atomically.
    let (_root, spool, store) = interleaved_fixture();
    ingest(&spool, &store);

    let connection = Connection::open(store.join("store.db")).expect("open store");
    let kalshi_first: i64 = connection
        .query_row(
            "SELECT MIN(seq) FROM evidence WHERE venue = 'kalshi'",
            [],
            |row| row.get(0),
        )
        .expect("kalshi position");
    let polymarket_last: i64 = connection
        .query_row(
            "SELECT MAX(seq) FROM evidence WHERE venue = 'polymarket'",
            [],
            |row| row.get(0),
        )
        .expect("polymarket position");

    // Permanent for this binary. The finalizer's mode inverts it over the same
    // bytes, which is the entire reason that mode exists.
    assert!(
        kalshi_first > polymarket_last,
        "file order sequences the whole polymarket file ahead of the kalshi one \
         ({kalshi_first} vs {polymarket_last})"
    );
}

#[test]
fn within_one_lane_file_order_is_already_receive_order() {
    // The half that is correct today, pinned so the fix is not credited with it
    // and cannot regress it: a splice writes one lane in receive order, so
    // reading a single file front to back is right and stays right.
    let root = TempDir::new("indexer-ordering-single").expect("temporary directory");
    let spool = root.path().join("spool");
    let store = root.path().join("store");
    write_lane(
        &spool,
        "polymarket",
        "a",
        &[
            envelope("polymarket", "pmepoch", 1, 1, 100),
            envelope("polymarket", "pmepoch", 2, 2, 200),
            envelope("polymarket", "pmepoch", 3, 3, 300),
        ],
    );

    ingest(&spool, &store);
    assert_eq!(visible_ns_in_evidence_order(&store), vec![100, 200, 300]);
}
