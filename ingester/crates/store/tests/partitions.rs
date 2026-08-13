use indexer_store::Store;
use indexer_types::{ContentHash, Sinkable};
use tempdir::TempDir;

#[derive(Clone)]
struct Fact(&'static [u8]);

impl Sinkable for Fact {
    fn write_to(&self, writer: &mut dyn std::io::Write) -> Result<(), indexer_types::SinkError> {
        writer
            .write_all(self.0)
            .map_err(indexer_types::SinkError::from)
    }
}

#[test]
fn a_partition_continues_the_global_sequence_without_requiring_older_rows() {
    let directory = TempDir::new("partition-sequence").expect("temporary directory");
    let mut store = Store::open_with_first_seq(directory.path(), 42).expect("open partition");

    let captured = store
        .capture_raw("polymarket", "segment", 0, b"line\n", b"payload")
        .expect("capture");
    let committed = store
        .commit_fact(&captured, Fact(b"fact"))
        .expect("commit fact");

    assert_eq!(captured.seq().get(), 42);
    assert_eq!(committed.seq().get(), 42);
    assert_eq!(store.first_evidence_seq(), 42);
    assert_eq!(store.next_evidence_seq(), 43);
    assert!(store.check_integrity().expect("integrity").is_clean());
}

#[test]
fn consumed_segment_identity_commits_and_rolls_back_with_the_cursor() {
    let directory = TempDir::new("partition-segments").expect("temporary directory");
    let mut store = Store::open(directory.path()).expect("open store");
    let sha256 = "a".repeat(64);

    let failed: Result<(), indexer_store::StoreError> = store.transaction(|store| {
        store.advance_spool_cursor("lane=x/date=y/segment.ndjson", "polymarket", 5, 1)?;
        store.complete_spool_segment(
            "lane=x/date=y/segment.ndjson",
            "polymarket",
            &sha256,
            5,
            1,
        )?;
        Err(indexer_store::StoreError::Encoding)
    });
    assert!(failed.is_err());
    assert!(store.consumed_segments().expect("segments").is_empty());

    store
        .transaction(|store| {
            let captured = store.capture_raw(
                "polymarket",
                "lane=x/date=y/segment.ndjson",
                0,
                b"line\n",
                b"payload",
            )?;
            let fact = store.commit_fact(&captured, Fact(b"fact"))?;
            store.remember_record_identity(
                "record-1",
                ContentHash::hash(b"payload"),
                fact.seq(),
            )?;
            store.advance_spool_cursor("lane=x/date=y/segment.ndjson", "polymarket", 5, 1)?;
            store.complete_spool_segment(
                "lane=x/date=y/segment.ndjson",
                "polymarket",
                &sha256,
                5,
                1,
            )
        })
        .expect("commit segment");

    let segments = store.consumed_segments().expect("segments");
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].spool_file, "lane=x/date=y/segment.ndjson");
    assert_eq!(segments[0].source_sha256, sha256);
    assert_eq!(segments[0].byte_length, 5);
    assert_eq!(segments[0].line_count, 1);
}
