mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use common::{seal_segment, write_sealed};
use rusqlite::Connection;
use serde_json::Value;
use tempdir::TempDir;

fn envelope(delivery: u64, counter: u64) -> String {
    envelope_with_identity(delivery, counter, &format!("pm-epoch-{counter}"), counter)
}

fn envelope_with_identity(
    delivery: u64,
    counter: u64,
    record_id: &str,
    payload_counter: u64,
) -> String {
    let record = serde_json::json!({
        "delivery_index": delivery,
        "record_id": record_id,
        "visible_ns": delivery,
        "venue": "polymarket",
        "stream": "public_book",
        "connection_epoch": "epoch",
        "local_counter": counter,
        "source_cursor": {"type": "unsequenced", "counter": counter},
        "kind": "venue_frame",
        "raw_payload": format!(r#"{{"counter":{payload_counter}}}"#),
    });
    format!(
        "{}\n",
        serde_json::to_string(&record).expect("encode envelope")
    )
}

fn fixture() -> (TempDir, PathBuf, PathBuf, PathBuf) {
    let root = TempDir::new("indexer-cli-regression").expect("temporary directory");
    let spool = root.path().join("spool");
    let store = root.path().join("store");
    let file = spool
        .join("lane=polymarket")
        .join("date=2026-07-29")
        .join("20260729T000000000000-epoch.ndjson");
    fs::create_dir_all(file.parent().expect("spool parent")).expect("create spool");
    (root, spool, store, file)
}

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

#[test]
fn a_schema_migration_reports_its_duration_once() {
    let (_root, spool, store, _file) = fixture();
    fs::create_dir_all(&store).expect("store directory");
    let connection = Connection::open(store.join("store.db")).expect("create v1 database");
    connection
        .execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta (key, value) VALUES ('schema_version', '1');",
        )
        .expect("schema-v1 marker");
    drop(connection);

    let output = Command::new(env!("CARGO_BIN_EXE_indexer-ingest"))
        .arg(&spool)
        .arg(&store)
        .output()
        .expect("run migrating indexer-ingest");
    assert!(
        output.status.success(),
        "migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).expect("JSON report");
    assert_eq!(report["store_migration"]["from_schema"], 1);
    assert_eq!(report["store_migration"]["to_schema"], 3);
    assert_eq!(report["store_migration"]["identity_records"], 0);
    assert!(report["store_migration"]["elapsed_seconds"].is_number());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("migrated store schema 1 -> 3"),
        "the blocking migration must be visible in service logs"
    );

    let reopened = ingest(&spool, &store);
    assert!(
        reopened["store_migration"].is_null(),
        "the report names work performed by this startup, not migration history"
    );
}

#[test]
fn continuity_state_survives_a_process_restart() {
    let (_root, spool, store, file) = fixture();
    write_sealed(&file, "polymarket", envelope(1, 1));
    let first = ingest(&spool, &store);
    assert_eq!(first["facts_committed"], 1);

    let second = file.with_file_name("20260729T000001000000-epoch.ndjson");
    write_sealed(&second, "polymarket", envelope(2, 3));

    let resumed = ingest(&spool, &store);
    assert_eq!(
        resumed["causes"]["local_counter_broken"], 1,
        "counter 1 followed by counter 3 must remain a proven local hole across restart"
    );
}

#[test]
fn partition_identity_survives_restart_without_retaining_record_history_in_memory() {
    let (_root, spool, store, file) = fixture();
    let record_id = "pm-epoch-original";
    write_sealed(
        &file,
        "polymarket",
        envelope_with_identity(1, 1, record_id, 7),
    );
    let first = ingest(&spool, &store);
    assert_eq!(first["facts_committed"], 1);
    assert_eq!(first["identity_records_in_memory"], 0);

    let retransmission = file.with_file_name("20260729T000001000000-epoch.ndjson");
    write_sealed(
        &retransmission,
        "polymarket",
        envelope_with_identity(2, 2, record_id, 7),
    );

    let resumed = ingest(&spool, &store);
    assert_eq!(resumed["causes"]["duplicate"], 1);
    assert_eq!(resumed["duplicates"], 1);
    assert_eq!(
        resumed["identity_records_in_memory"], 0,
        "the active partition must answer exact identity while the process retains only bounded \
         ordering state"
    );

    let contradiction = file.with_file_name("20260729T000002000000-epoch.ndjson");
    write_sealed(
        &contradiction,
        "polymarket",
        envelope_with_identity(3, 3, record_id, 99),
    );
    let conflicted = ingest(&spool, &store);
    assert_eq!(conflicted["causes"]["conflict"], 1);
    assert_eq!(conflicted["conflicts"], 1);
    assert_eq!(conflicted["identity_records_in_memory"], 0);
}

#[test]
fn an_unsealed_torn_tail_never_becomes_eligible() {
    let (_root, spool, store, file) = fixture();
    let complete = envelope(1, 1);
    let partial = br#"{"delivery_index":2"#;
    let mut bytes = complete.as_bytes().to_vec();
    bytes.extend_from_slice(partial);
    fs::write(&file, bytes).expect("unsealed spool with torn tail");

    let report = ingest(&spool, &store);
    assert_eq!(report["facts_committed"], 0);
    assert_eq!(
        report["evidence_rows"], 0,
        "an unsealed torn segment is not eligible even when it has a complete prefix"
    );
}

#[test]
fn equivalent_spool_paths_share_one_cursor_identity() {
    let (_root, spool, store, file) = fixture();
    write_sealed(&file, "polymarket", envelope(1, 1));
    let first = ingest(&spool, &store);
    assert_eq!(first["facts_committed"], 1);

    let detour = spool.join("detour");
    fs::create_dir(&detour).expect("path detour");
    let equivalent = detour.join("..");
    let resumed = ingest(&equivalent, &store);

    assert_eq!(
        resumed["facts_committed"], 0,
        "the same spool reached through an equivalent path must not be ingested twice"
    );
    assert_eq!(resumed["evidence_rows"], 1);
}

#[test]
fn a_resumed_pass_does_not_rescan_the_committed_prefix() {
    let (_root, spool, store, file) = fixture();
    let records = (1..=64)
        .map(|counter| envelope(counter, counter))
        .collect::<String>();
    write_sealed(&file, "polymarket", records);

    let first = ingest(&spool, &store);
    assert_eq!(first["facts_committed"], 64);

    let resumed = ingest(&spool, &store);
    assert_eq!(resumed["facts_committed"], 0);
    assert_eq!(
        resumed["already_ingested"], 0,
        "a polling ingester must seek to its durable byte cursor instead of \
         rereading every committed line on every pass"
    );
}

#[test]
fn an_unsealed_ndjson_is_not_eligible_for_ingestion() {
    let (_root, spool, store, file) = fixture();
    fs::write(&file, envelope(1, 1)).expect("complete but unsealed segment");

    let report = ingest(&spool, &store);

    assert_eq!(
        report["facts_committed"], 0,
        "the .ndjson suffix is not a commit marker; a matching valid seal is required"
    );
    assert_eq!(report["evidence_rows"], 0);
}

#[test]
fn a_changed_byte_after_sealing_fails_ingestion() {
    let (_root, spool, store, file) = fixture();
    write_sealed(&file, "polymarket", envelope(1, 1));
    let mut changed = fs::read(&file).expect("sealed data");
    let position = changed
        .iter()
        .position(|byte| *byte == b'1')
        .expect("mutable byte");
    changed[position] = b'2';
    fs::write(&file, changed).expect("same-length corruption");

    let output = Command::new(env!("CARGO_BIN_EXE_indexer-ingest"))
        .arg(&spool)
        .arg(&store)
        .output()
        .expect("run indexer-ingest");

    assert!(
        !output.status.success(),
        "changed sealed bytes must stop ingestion"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("sha256"),
        "the failure should identify the invalid digest: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn a_seal_for_another_data_file_is_rejected() {
    let (_root, spool, store, file) = fixture();
    write_sealed(&file, "polymarket", envelope(1, 1));
    let seal_path = file.with_file_name(format!(
        "{}.seal.json",
        file.file_name()
            .expect("filename")
            .to_string_lossy()
            .strip_suffix(".ndjson")
            .expect("ndjson suffix")
    ));
    let mut seal: Value =
        serde_json::from_slice(&fs::read(&seal_path).expect("seal")).expect("parse seal");
    seal["data_file"] = Value::String("different.ndjson".to_owned());
    fs::write(
        &seal_path,
        serde_json::to_vec(&seal).expect("encode changed seal"),
    )
    .expect("change seal");

    let output = Command::new(env!("CARGO_BIN_EXE_indexer-ingest"))
        .arg(&spool)
        .arg(&store)
        .output()
        .expect("run indexer-ingest");

    assert!(
        !output.status.success(),
        "a mismatched seal must stop ingestion"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("data_file"));
}

#[test]
fn a_valid_seal_makes_the_segment_eligible() {
    let (_root, spool, store, file) = fixture();
    fs::write(&file, envelope(1, 1)).expect("segment");
    seal_segment(&file, "polymarket");

    assert_eq!(ingest(&spool, &store)["facts_committed"], 1);
}
