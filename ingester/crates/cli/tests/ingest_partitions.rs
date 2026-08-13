use std::path::Path;

use indexer_cli::ingest_partition::{
    ACTIVE_FILE, DATABASE_FILE, OPEN_DATABASE_FILE, OpenPartition, RECEIPT_FILE, read_receipts,
};
use indexer_cli::ingest_reaper::{MIN_RETENTION_HOURS, ReapMode, sweep};
use indexer_continuity::{Classifier, IdentityVerdict};
use indexer_types::EnvelopeView;
use sha2::Digest;
use tempdir::TempDir;

const DAY_NS: u64 = 86_400_000_000_000;
const DAY_ONE_NS: u64 = 1_785_412_800_000_000_000; // 2026-07-30T00:00:00Z
const DAY_TWO_NS: u64 = DAY_ONE_NS + DAY_NS;
const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn commit_segment(partition: &mut OpenPartition, path: &str, sha256: &str) -> i64 {
    let line = br#"{"delivery_index":1,"record_id":"record-1","visible_ns":1,"venue":"polymarket","stream":"public_book","connection_epoch":"epoch","local_counter":1,"source_cursor":{"type":"unsequenced","counter":1},"kind":"venue_frame","raw_payload":"{}"}
"#;
    let envelope = EnvelopeView::parse(line).expect("test envelope");
    let classifier = Classifier::without_identity_history();
    partition
        .store
        .transaction(|store| {
            let captured = store.capture_raw("polymarket", path, 0, line, b"{}")?;
            let fact =
                classifier.classify_with_identity(&envelope, &captured, IdentityVerdict::Unseen);
            let committed = store.commit_fact(&captured, fact)?;
            store.remember_record_identity("record-1", captured.content_hash(), committed.seq())?;
            store.advance_spool_cursor(path, "polymarket", line.len() as u64, 1)?;
            store.complete_spool_segment(path, "polymarket", sha256, line.len() as u64, 1)?;
            Ok(captured.seq().get())
        })
        .expect("commit segment")
}

fn commit_envelope(
    partition: &mut OpenPartition,
    path: &str,
    record_id: &str,
    local_counter: u64,
) -> &'static str {
    let line = format!(
        "{{\"delivery_index\":{local_counter},\"record_id\":\"{record_id}\",\
         \"visible_ns\":{local_counter},\"venue\":\"polymarket\",\
         \"stream\":\"public_book\",\"connection_epoch\":\"epoch\",\
         \"local_counter\":{local_counter},\"source_cursor\":{{\"type\":\
         \"unsequenced\",\"counter\":{local_counter}}},\"kind\":\"venue_frame\",\
         \"raw_payload\":\"{{}}\"}}\n"
    );
    let envelope = EnvelopeView::parse(line.as_bytes()).expect("test envelope");
    partition.tracker.observe_envelope(&envelope);
    let classifier = &mut partition.classifier;
    partition
        .store
        .transaction(|store| {
            let captured = store.capture_raw("polymarket", path, 0, line.as_bytes(), b"{}")?;
            let fact =
                classifier.classify_with_identity(&envelope, &captured, IdentityVerdict::Unseen);
            let committed = store.commit_fact(&captured, fact)?;
            store.remember_record_identity(record_id, captured.content_hash(), committed.seq())?;
            classifier.apply(&committed);
            store.advance_spool_cursor(path, "polymarket", line.len() as u64, 1)?;
            store.complete_spool_segment(path, "polymarket", SHA_A, line.len() as u64, 1)?;
            Ok(committed.value().cause.label())
        })
        .expect("commit envelope")
}

fn partition(root: &Path, date: &str) -> std::path::PathBuf {
    root.join(format!("date={date}"))
}

#[test]
fn rollover_continues_sequence_resets_identity_and_retains_the_skip_ledger() {
    let temporary = TempDir::new("ingest-partitions").expect("temporary directory");
    let root = temporary.path();
    let segment = "lane=polymarket/date=2026-07-30/segment.ndjson";
    let mut first = OpenPartition::open(root, DAY_ONE_NS).expect("open first partition");
    assert_eq!(commit_segment(&mut first, segment, SHA_A), 1);

    let second = first.rotate(DAY_TWO_NS).expect("rotate partition");
    let closed = partition(root, "2026-07-30");
    assert!(closed.join(DATABASE_FILE).is_file());
    assert!(closed.join(RECEIPT_FILE).is_file());
    assert!(!closed.join(OPEN_DATABASE_FILE).exists());
    assert!(!closed.join(ACTIVE_FILE).exists());
    assert_eq!(second.store.next_evidence_seq(), 2);
    assert!(
        second
            .store
            .record_identity("record-1")
            .expect("identity lookup")
            .is_none(),
        "record identity deliberately starts empty in the new ingest partition"
    );

    std::fs::remove_file(closed.join(DATABASE_FILE)).expect("simulate reaped database");
    drop(second);
    let reopened = OpenPartition::open(root, DAY_TWO_NS).expect("reopen current partition");
    assert!(
        reopened
            .consumed
            .contains(segment, SHA_A)
            .expect("skip lookup"),
        "the retained receipt must prevent old spool bytes from being re-ingested"
    );
    assert!(
        reopened
            .consumed
            .contains(segment, &"b".repeat(64))
            .is_err()
    );
    assert!(closed.join(RECEIPT_FILE).is_file());
}

#[test]
fn rollover_carries_the_counter_needed_to_classify_the_next_segment() {
    let temporary = TempDir::new("ingest-partition-carry").expect("temporary directory");
    let root = temporary.path();
    let mut first = OpenPartition::open(root, DAY_ONE_NS).expect("open first partition");
    assert_eq!(
        commit_envelope(
            &mut first,
            "lane=polymarket/date=2026-07-30/first.ndjson",
            "record-1",
            1,
        ),
        "unsequenced_venue"
    );

    let mut second = first.rotate(DAY_TWO_NS).expect("rotate partition");
    assert_eq!(
        commit_envelope(
            &mut second,
            "lane=polymarket/date=2026-07-31/second.ndjson",
            "record-2",
            3,
        ),
        "local_counter_broken",
        "counter continuity crosses the database boundary even though identity does not"
    );
}

#[test]
fn restart_recovers_both_open_and_renamed_but_unreceipted_databases() {
    for remove_marker in [false, true] {
        let temporary = TempDir::new("ingest-partition-recovery").expect("temporary directory");
        let root = temporary.path();
        let mut open = OpenPartition::open(root, DAY_ONE_NS).expect("open partition");
        commit_segment(
            &mut open,
            "lane=polymarket/date=2026-07-30/recovery.ndjson",
            SHA_A,
        );
        drop(open);

        let directory = partition(root, "2026-07-30");
        std::fs::rename(
            directory.join(OPEN_DATABASE_FILE),
            directory.join(DATABASE_FILE),
        )
        .expect("simulate interrupted close rename");
        if remove_marker {
            std::fs::remove_file(directory.join(ACTIVE_FILE))
                .expect("simulate interrupted marker removal");
        }

        let recovered = OpenPartition::open(root, DAY_ONE_NS).expect("recover partition");
        assert_eq!(recovered.store.evidence_count().expect("evidence count"), 1);
        assert!(directory.join(OPEN_DATABASE_FILE).is_file());
        assert!(!directory.join(DATABASE_FILE).exists());
    }
}

#[test]
fn reaper_is_audit_first_and_deletes_at_the_exact_twenty_four_hour_boundary() {
    let temporary = TempDir::new("ingest-reaper").expect("temporary directory");
    let root = temporary.path();
    let first = OpenPartition::open(root, DAY_ONE_NS).expect("open first partition");
    let second = first.rotate(DAY_TWO_NS).expect("close first partition");
    let closed = partition(root, "2026-07-30");
    let active = partition(root, "2026-07-31");

    let before = sweep(
        root,
        DAY_TWO_NS + DAY_NS - 1,
        MIN_RETENTION_HOURS,
        ReapMode::Delete,
    )
    .expect("pre-boundary sweep");
    assert!(closed.join(DATABASE_FILE).is_file());
    assert!(before.decisions.iter().any(|decision| {
        decision.partition_date == "2026-07-30" && decision.reason == "retention_floor"
    }));

    let audit = sweep(
        root,
        DAY_TWO_NS + DAY_NS,
        MIN_RETENTION_HOURS,
        ReapMode::Audit,
    )
    .expect("audit sweep");
    assert_eq!(audit.counts.reapable, 1);
    assert_eq!(audit.counts.reaped, 0);
    assert!(closed.join(DATABASE_FILE).is_file());

    let deleted = sweep(
        root,
        DAY_TWO_NS + DAY_NS,
        MIN_RETENTION_HOURS,
        ReapMode::Delete,
    )
    .expect("delete sweep");
    assert_eq!(deleted.counts.reaped, 1);
    assert!(!closed.join(DATABASE_FILE).exists());
    assert!(closed.join(RECEIPT_FILE).is_file());
    assert!(active.join(OPEN_DATABASE_FILE).is_file());
    assert!(active.join(ACTIVE_FILE).is_file());

    drop(second);
    let repeated = sweep(
        root,
        DAY_TWO_NS + DAY_NS,
        MIN_RETENTION_HOURS,
        ReapMode::Delete,
    )
    .expect("idempotent sweep");
    assert_eq!(repeated.counts.already_reaped, 1);
}

#[test]
fn reaper_retains_active_unreceipted_and_changed_databases() {
    let temporary = TempDir::new("ingest-reaper-retention").expect("temporary directory");
    let root = temporary.path();
    let first = OpenPartition::open(root, DAY_ONE_NS).expect("open first partition");
    let _second = first.rotate(DAY_TWO_NS).expect("close first partition");
    let closed = partition(root, "2026-07-30").join(DATABASE_FILE);
    let mut bytes = std::fs::read(&closed).expect("closed database");
    bytes.push(0);
    std::fs::write(&closed, bytes).expect("change closed database");

    let unreceipted = partition(root, "2026-07-29");
    std::fs::create_dir_all(&unreceipted).expect("unreceipted partition");
    std::fs::write(unreceipted.join(DATABASE_FILE), b"derived").expect("unreceipted database");

    let report = sweep(
        root,
        DAY_TWO_NS + DAY_NS,
        MIN_RETENTION_HOURS,
        ReapMode::Delete,
    )
    .expect("retention sweep");
    assert!(report.decisions.iter().any(|decision| {
        decision.partition_date == "2026-07-30" && decision.reason == "database_identity_mismatch"
    }));
    assert!(report.decisions.iter().any(|decision| {
        decision.partition_date == "2026-07-29" && decision.reason == "receipt_missing"
    }));
    assert!(report.decisions.iter().any(|decision| {
        decision.partition_date == "2026-07-31" && decision.reason == "active_partition"
    }));
    assert!(closed.is_file());
    assert!(unreceipted.join(DATABASE_FILE).is_file());

    assert!(
        sweep(root, DAY_TWO_NS + DAY_NS, 23, ReapMode::Audit).is_err(),
        "the command must refuse a retention floor shorter than one day"
    );
}

#[test]
fn receipt_records_the_exact_closed_database_identity() {
    let temporary = TempDir::new("ingest-receipt-identity").expect("temporary directory");
    let root = temporary.path();
    let first = OpenPartition::open(root, DAY_ONE_NS).expect("open first partition");
    let _second = first.rotate(DAY_TWO_NS).expect("close first partition");
    let receipt = read_receipts(root)
        .expect("read receipts")
        .remove(0)
        .receipt;
    let database =
        std::fs::read(partition(root, "2026-07-30").join(DATABASE_FILE)).expect("closed database");

    assert_eq!(receipt.database_byte_length, database.len() as u64);
    assert_eq!(
        receipt.database_sha256,
        format!("{:x}", sha2::Sha256::digest(database))
    );
}
