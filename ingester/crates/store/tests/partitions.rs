use indexer_store::{SCHEMA_VERSION, Store};
use indexer_types::{ContentHash, Sinkable};
use rusqlite::Connection;
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
fn fresh_partition_metadata_initialization_is_atomic_and_retryable() {
    let directory = TempDir::new("partition-metadata-atomicity").expect("temporary directory");
    let database = directory.path().join("store.db.open");
    {
        let connection = Connection::open(&database).expect("create database");
        connection
            .execute_batch(
                "
                CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TRIGGER reject_first_evidence_seq
                BEFORE INSERT ON meta
                WHEN NEW.key = 'first_evidence_seq'
                BEGIN
                    SELECT RAISE(ABORT, 'injected metadata failure');
                END;
                ",
            )
            .expect("install metadata failure");
    }

    assert!(
        Store::open_partition(directory.path(), 42).is_err(),
        "the injected second metadata write must fail store creation"
    );
    let connection = Connection::open(&database).expect("inspect failed initialization");
    let version_rows: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("schema version count");
    assert_eq!(
        version_rows, 0,
        "a failed fresh-store initialization must not leave a schema version without its sequence origin"
    );
    connection
        .execute_batch("DROP TRIGGER reject_first_evidence_seq;")
        .expect("remove metadata failure");
    drop(connection);

    let store =
        Store::open_partition(directory.path(), 42).expect("retry fresh-store initialization");
    assert_eq!(store.first_evidence_seq(), 42);
    drop(store);

    let connection = Connection::open(database).expect("inspect initialized store");
    let metadata: Vec<(String, String)> = connection
        .prepare("SELECT key, value FROM meta ORDER BY key")
        .expect("prepare metadata query")
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query metadata")
        .collect::<Result<_, _>>()
        .expect("read metadata");
    assert_eq!(
        metadata,
        vec![
            ("first_evidence_seq".to_owned(), "42".to_owned()),
            ("schema_version".to_owned(), SCHEMA_VERSION.to_string()),
        ]
    );
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
