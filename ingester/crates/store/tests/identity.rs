use std::io::Write;

use indexer_store::{SCHEMA_VERSION, Store, StoreError};
use indexer_types::{ContentHash, SinkError, Sinkable};
use rusqlite::Connection;
use tempdir::TempDir;

struct TestFact(Vec<u8>);

impl Sinkable for TestFact {
    fn write_to(&self, writer: &mut dyn Write) -> Result<(), SinkError> {
        writer.write_all(&self.0).map_err(SinkError::from)
    }
}

#[test]
fn record_identity_is_exact_and_survives_reopen() {
    let directory = TempDir::new("indexer-store-identity").expect("temporary directory");
    let content = ContentHash::hash(b"first payload");
    {
        let mut store = Store::open(directory.path()).expect("open store");
        let captured = store
            .capture_raw("polymarket", "segment", 0, b"line\n", b"first payload")
            .expect("capture");
        let committed = store
            .commit_fact(&captured, TestFact(b"fact".to_vec()))
            .expect("commit fact");
        store
            .remember_record_identity("record-1", content, committed.seq())
            .expect("remember identity");
    }

    let store = Store::open(directory.path()).expect("reopen store");
    let identity = store
        .record_identity("record-1")
        .expect("read identity")
        .expect("identity exists");
    assert_eq!(identity.content, content);
    assert_eq!(identity.first_seen.get(), 1);
    assert!(store.record_identity("unseen").expect("lookup").is_none());
}

#[test]
fn record_identity_rolls_back_with_its_fact() {
    let directory = TempDir::new("indexer-store-identity-rollback").expect("temporary directory");
    let mut store = Store::open(directory.path()).expect("open store");
    let content = ContentHash::hash(b"payload");

    let result: Result<(), StoreError> = store.transaction(|store| {
        let captured = store.capture_raw("kalshi", "segment", 0, b"line\n", b"payload")?;
        let committed = store.commit_fact(&captured, TestFact(b"fact".to_vec()))?;
        store.remember_record_identity("rolled-back", content, committed.seq())?;
        Err(StoreError::Encoding)
    });

    assert!(matches!(result, Err(StoreError::Encoding)));
    assert!(
        store
            .record_identity("rolled-back")
            .expect("lookup")
            .is_none(),
        "an identity cannot survive a transaction whose fact and cursor rolled back"
    );
    assert_eq!(store.evidence_count().expect("evidence count"), 0);
    assert_eq!(store.fact_count().expect("fact count"), 0);
}

#[test]
fn schema_one_migrates_identity_from_facts_without_an_in_memory_rebuild() {
    let directory = TempDir::new("indexer-store-v1-migration").expect("temporary directory");
    let database = directory.path().join("store.db");
    let content = ContentHash::hash(b"historical payload");
    {
        let connection = Connection::open(&database).expect("create v1 database");
        connection
            .execute_batch(
                "
                CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE evidence (
                    seq INTEGER PRIMARY KEY,
                    venue TEXT NOT NULL,
                    spool_file TEXT NOT NULL,
                    byte_offset INTEGER NOT NULL,
                    evidence_hash TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    raw_line BLOB NOT NULL
                );
                CREATE TABLE facts (
                    seq INTEGER PRIMARY KEY,
                    evidence_seq INTEGER NOT NULL UNIQUE,
                    fact_hash TEXT NOT NULL,
                    canonical BLOB NOT NULL
                );
                CREATE TABLE spool_cursor (
                    spool_file TEXT PRIMARY KEY,
                    venue TEXT NOT NULL,
                    bytes_consumed INTEGER NOT NULL,
                    records INTEGER NOT NULL
                );
                INSERT INTO meta (key, value) VALUES ('schema_version', '1');
                ",
            )
            .expect("v1 schema");
        let canonical = format!(
            "{{\"record_id\":\"historical-record\",\"content\":\"{}\"}}",
            content.to_hex()
        );
        connection
            .execute(
                "INSERT INTO facts (seq, evidence_seq, fact_hash, canonical) \
                 VALUES (1, 1, 'unused', ?1)",
                [canonical.as_bytes()],
            )
            .expect("historical fact");
    }

    let store = Store::open(directory.path()).expect("migrate v1 store");
    let migration = store.migration_report().expect("migration report");
    assert_eq!(migration.from_schema, 1);
    assert_eq!(migration.to_schema, SCHEMA_VERSION);
    assert_eq!(migration.identity_records, 1);
    let identity = store
        .record_identity("historical-record")
        .expect("read migrated identity")
        .expect("migrated identity exists");
    assert_eq!(identity.content, content);
    assert_eq!(identity.first_seen.get(), 1);
    drop(store);

    let connection = Connection::open(database).expect("inspect migrated store");
    let version: String = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("schema version");
    assert_eq!(version, SCHEMA_VERSION.to_string());
}

#[test]
fn schema_two_identity_rejects_a_null_record_id() {
    let directory = TempDir::new("indexer-store-null-identity").expect("temporary directory");
    drop(Store::open(directory.path()).expect("create store"));
    let connection = Connection::open(directory.path().join("store.db")).expect("inspect store");

    let result = connection.execute(
        "INSERT INTO record_identity (record_id, content_hash, first_seen) VALUES (NULL, ?1, 1)",
        [ContentHash::hash(b"payload").as_bytes().as_slice()],
    );
    assert!(
        result.is_err(),
        "SQLite rowid tables permit NULL in a TEXT PRIMARY KEY unless the column \
         is explicitly NOT NULL; the closed identity schema must reject it"
    );
}

#[test]
fn schema_one_migration_rolls_back_when_a_fact_has_no_record_id() {
    let directory = TempDir::new("indexer-store-null-migration").expect("temporary directory");
    let database = directory.path().join("store.db");
    let connection = Connection::open(&database).expect("create v1 database");
    connection
        .execute_batch(
            "
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE evidence (
                seq INTEGER PRIMARY KEY,
                venue TEXT NOT NULL,
                spool_file TEXT NOT NULL,
                byte_offset INTEGER NOT NULL,
                evidence_hash TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                raw_line BLOB NOT NULL
            );
            CREATE TABLE facts (
                seq INTEGER PRIMARY KEY,
                evidence_seq INTEGER NOT NULL UNIQUE,
                fact_hash TEXT NOT NULL,
                canonical BLOB NOT NULL
            );
            CREATE TABLE spool_cursor (
                spool_file TEXT PRIMARY KEY,
                venue TEXT NOT NULL,
                bytes_consumed INTEGER NOT NULL,
                records INTEGER NOT NULL
            );
            INSERT INTO meta (key, value) VALUES ('schema_version', '1');
            ",
        )
        .expect("v1 schema");
    let canonical = format!(
        "{{\"content\":\"{}\"}}",
        ContentHash::hash(b"historical payload").to_hex()
    );
    connection
        .execute(
            "INSERT INTO facts (seq, evidence_seq, fact_hash, canonical) \
             VALUES (1, 1, 'unused', ?1)",
            [canonical.as_bytes()],
        )
        .expect("malformed historical fact");
    drop(connection);

    assert!(
        Store::open(directory.path()).is_err(),
        "a missing record_id must fail the migration rather than materializing a NULL identity"
    );
    let connection = Connection::open(database).expect("inspect rolled-back migration");
    let version: String = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("schema version");
    assert_eq!(version, "1");
    let identity_table: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'record_identity'",
            [],
            |row| row.get(0),
        )
        .expect("identity table count");
    assert_eq!(identity_table, 0, "the failed migration is atomic");
}
