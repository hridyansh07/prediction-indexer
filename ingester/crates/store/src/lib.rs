//! The durable log: evidence, facts, and the spool cursor.
//!
//! SQLite owns atomicity and crash rollback; this crate owns ordering, hashes,
//! and the gates that decide when later work may begin. Nothing here decodes,
//! classifies, or interprets a payload — callers hand over bytes and get back
//! proof that those bytes are durable.
//!
//! ```text
//! capture_raw  -> CapturedRecord    evidence is durable, parsing may begin
//! commit_fact  -> CommittedFact<T>  the fact is durable, projections may move
//! ```
//!
//! Each receipt can only be produced by its own commit, so the ordering shows up
//! in the signatures a caller has to satisfy rather than in a comment asking them
//! to be careful. That is a strong hint and not a proof — the store re-validates
//! every position *inside* the transaction, because a constructor cannot know
//! whether an `fsync` reached the platter.
//!
//! Two rules this crate holds to, worth stating plainly:
//!
//! 1. **The raw line is durable before anything parses it.** If the parse then
//!    fails, the bytes still exist and the schema can be revised. Parsing first
//!    would let a misread discard evidence that cannot be re-collected.
//! 2. **One encode, one hash, one write.** `Sinkable::to_canonical_bytes` produces
//!    the buffer that is both hashed and persisted. Encoding separately for each
//!    would let them disagree, and the disagreement surfaces as corruption years
//!    of data later.

pub mod error;

use std::path::Path;
use std::time::{Duration, Instant};

use indexer_types::{
    Committed, ContentHash, EvidenceHash, EvidenceSeq, FactHash, FactSeq, Positioned, Sinkable,
};
use rusqlite::{Connection, OptionalExtension, params};

pub use error::StoreError;

/// The schema this build reads and writes.
pub const SCHEMA_VERSION: i64 = 3;

const BASE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence (
    seq           INTEGER PRIMARY KEY CHECK (seq > 0),
    venue         TEXT    NOT NULL,
    spool_file    TEXT    NOT NULL,
    byte_offset   INTEGER NOT NULL CHECK (byte_offset >= 0),
    evidence_hash TEXT    NOT NULL,
    content_hash  TEXT    NOT NULL,
    raw_line      BLOB    NOT NULL
);
CREATE INDEX IF NOT EXISTS evidence_by_file ON evidence (spool_file, byte_offset);
CREATE TABLE IF NOT EXISTS facts (
    seq          INTEGER PRIMARY KEY CHECK (seq > 0),
    evidence_seq INTEGER NOT NULL UNIQUE REFERENCES evidence (seq),
    fact_hash    TEXT    NOT NULL,
    canonical    BLOB    NOT NULL
);
CREATE TABLE IF NOT EXISTS spool_cursor (
    spool_file     TEXT PRIMARY KEY,
    venue          TEXT    NOT NULL,
    bytes_consumed INTEGER NOT NULL CHECK (bytes_consumed >= 0),
    records        INTEGER NOT NULL CHECK (records >= 0)
);
CREATE TABLE IF NOT EXISTS consumed_segment (
    spool_file    TEXT    PRIMARY KEY,
    lane          TEXT    NOT NULL,
    source_sha256 TEXT    NOT NULL CHECK (length(source_sha256) = 64),
    byte_length   INTEGER NOT NULL CHECK (byte_length >= 0),
    line_count    INTEGER NOT NULL CHECK (line_count >= 0)
);
";

/// The exact identity index used within one ingest-store database.
///
/// This is derived from the fact log but durable so the process does not need a
/// `BTreeMap` entry for every record ever captured. `first_seen` is the original
/// fact position returned for duplicate/conflict provenance.
const IDENTITY_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS record_identity (
    record_id    TEXT    NOT NULL PRIMARY KEY,
    content_hash BLOB    NOT NULL
                         CHECK (
                             typeof(content_hash) = 'blob'
                             AND length(content_hash) = 32
                         ),
    first_seen   INTEGER NOT NULL CHECK (first_seen > 0)
) WITHOUT ROWID;
";

/// V1 retained global identity only in the classifier's unbounded in-memory map.
/// Its facts already carry the exact record ID, content hash, and first position,
/// so V2 materializes that projection with a streaming SQLite statement. The
/// primary-key conflict keeps the earliest row because input is explicitly in
/// sequence order; malformed JSON or a missing/invalid field violates the closed
/// V2 table schema and rolls the migration back rather than silently omitting an
/// identity.
const MIGRATE_V1_TO_V2: &str = "
BEGIN IMMEDIATE;
CREATE TABLE record_identity (
    record_id    TEXT    NOT NULL PRIMARY KEY,
    content_hash BLOB    NOT NULL
                         CHECK (
                             typeof(content_hash) = 'blob'
                             AND length(content_hash) = 32
                         ),
    first_seen   INTEGER NOT NULL CHECK (first_seen > 0)
) WITHOUT ROWID;
INSERT INTO record_identity (record_id, content_hash, first_seen)
SELECT
    json_extract(CAST(canonical AS TEXT), '$.record_id'),
    unhex(json_extract(CAST(canonical AS TEXT), '$.content')),
    seq
FROM facts
WHERE true
ORDER BY seq
ON CONFLICT(record_id) DO NOTHING;
UPDATE meta SET value = '2' WHERE key = 'schema_version';
COMMIT;
";

/// V3 makes global sequence continuation explicit so one UTC ingest partition
/// can begin where the previous partition ended without retaining its rows.
const MIGRATE_V2_TO_V3: &str = "
BEGIN IMMEDIATE;
INSERT INTO meta (key, value)
VALUES (
    'first_evidence_seq',
    CAST(COALESCE((SELECT MIN(seq) FROM evidence), 1) AS TEXT)
);
UPDATE meta SET value = '3' WHERE key = 'schema_version';
COMMIT;
";

// ---------------------------------------------------------------------------
// Commit receipts
// ---------------------------------------------------------------------------

/// Proof that an exact line is durable. Only `capture_raw` produces one.
#[derive(Debug, Clone)]
pub struct CapturedRecord {
    seq: EvidenceSeq,
    evidence_hash: EvidenceHash,
    content_hash: ContentHash,
    raw_line: Vec<u8>,
}

impl CapturedRecord {
    pub fn seq(&self) -> EvidenceSeq {
        self.seq
    }
    pub fn evidence_hash(&self) -> EvidenceHash {
        self.evidence_hash
    }
    pub fn content_hash(&self) -> ContentHash {
        self.content_hash
    }
    /// The exact delivered bytes, including a trailing newline where one arrived.
    pub fn raw_line(&self) -> &[u8] {
        &self.raw_line
    }
}

impl Positioned for CapturedRecord {
    /// `FactSeq` equals `EvidenceSeq` here by construction — exactly one fact per
    /// delivery — which is what lets a captured row gate classification.
    fn position(&self) -> FactSeq {
        FactSeq::new(self.seq.get()).expect("capture_raw assigns positive positions")
    }
}

/// Proof that a fact is durable, carrying the value onward to the reducer.
#[derive(Debug, Clone)]
pub struct CommittedFact<T> {
    seq: FactSeq,
    value: T,
}

impl<T> CommittedFact<T> {
    pub fn seq(&self) -> FactSeq {
        self.seq
    }
    pub fn value(&self) -> &T {
        &self.value
    }
    pub fn into_value(self) -> T {
        self.value
    }
}

impl<T> Committed<T> for CommittedFact<T> {
    fn position(&self) -> FactSeq {
        self.seq
    }
    fn value(&self) -> &T {
        &self.value
    }
}

/// Where ingestion of one spool file has reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolCursor {
    pub spool_file: String,
    pub venue: String,
    pub bytes_consumed: u64,
    pub records: u64,
}

/// One sealed segment fully consumed by this partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedSegment {
    pub spool_file: String,
    pub lane: String,
    pub source_sha256: String,
    pub byte_length: u64,
    pub line_count: u64,
}

/// The first exact content committed under one record ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordIdentity {
    pub content: ContentHash,
    pub first_seen: FactSeq,
}

/// One schema migration completed while opening the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationReport {
    pub from_schema: i64,
    pub to_schema: i64,
    pub identity_records: u64,
    pub elapsed: Duration,
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

pub struct Store {
    connection: Connection,
    first_evidence: i64,
    next_evidence: i64,
    migration: Option<MigrationReport>,
}

impl Store {
    /// Opens or creates a store directory.
    pub fn open(directory: &Path) -> Result<Self, StoreError> {
        Self::open_internal(&directory.join("store.db"), None)
    }

    /// Opens or creates one partition at an explicit first global position.
    ///
    /// Existing partitions must agree with `first_seq`; a mismatch means the
    /// active marker and database came from different rollover attempts.
    pub fn open_with_first_seq(directory: &Path, first_seq: i64) -> Result<Self, StoreError> {
        if first_seq <= 0 {
            return Err(StoreError::CorruptSequence);
        }
        Self::open_internal(&directory.join("store.db"), Some(first_seq))
    }

    /// Opens or creates the writable database for one ingest partition.
    ///
    /// The `.open` suffix is part of the closure protocol. Rollover checkpoints
    /// and closes this file, renames it to `store.db`, fsyncs the directory, and
    /// only then publishes the partition receipt.
    pub fn open_partition(directory: &Path, first_seq: i64) -> Result<Self, StoreError> {
        if first_seq <= 0 {
            return Err(StoreError::CorruptSequence);
        }
        Self::open_internal(&directory.join("store.db.open"), Some(first_seq))
    }

    fn open_internal(database: &Path, requested_first: Option<i64>) -> Result<Self, StoreError> {
        let directory = database.parent().ok_or_else(|| {
            StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("database path {} has no parent", database.display()),
            ))
        })?;
        std::fs::create_dir_all(directory)?;
        let mut connection = Connection::open(database)?;
        // WAL keeps a reader from blocking the ingest writer. `NORMAL` is the
        // right durability point here because the spool — not this database — is
        // the irreversible copy: a lost transaction is re-ingested from bytes we
        // still hold, so a full fsync per commit would buy nothing.
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(BASE_SCHEMA)?;

        let stored: Option<String> = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let migration = match stored {
            None => {
                let first = requested_first.unwrap_or(1);
                let transaction = connection.transaction()?;
                transaction.execute_batch(IDENTITY_SCHEMA)?;
                transaction.execute(
                    "INSERT INTO meta (key, value) VALUES ('schema_version', ?1)",
                    params![SCHEMA_VERSION.to_string()],
                )?;
                transaction.execute(
                    "INSERT INTO meta (key, value) VALUES ('first_evidence_seq', ?1)",
                    params![first.to_string()],
                )?;
                transaction.commit()?;
                None
            }
            Some(value) if value == "1" => {
                let started = Instant::now();
                connection.execute_batch(MIGRATE_V1_TO_V2)?;
                connection.execute_batch(MIGRATE_V2_TO_V3)?;
                let identity_records =
                    connection.query_row("SELECT COUNT(*) FROM record_identity", [], |row| {
                        row.get::<_, i64>(0)
                    })? as u64;
                Some(MigrationReport {
                    from_schema: 1,
                    to_schema: SCHEMA_VERSION,
                    identity_records,
                    elapsed: started.elapsed(),
                })
            }
            Some(value) if value == "2" => {
                let started = Instant::now();
                connection.execute_batch(MIGRATE_V2_TO_V3)?;
                let identity_records =
                    connection.query_row("SELECT COUNT(*) FROM record_identity", [], |row| {
                        row.get::<_, i64>(0)
                    })? as u64;
                Some(MigrationReport {
                    from_schema: 2,
                    to_schema: SCHEMA_VERSION,
                    identity_records,
                    elapsed: started.elapsed(),
                })
            }
            Some(value) if value == SCHEMA_VERSION.to_string() => None,
            Some(value) => return Err(StoreError::SchemaMismatch { found: value }),
        };

        let first_evidence: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'first_evidence_seq'",
            [],
            |row| row.get(0),
        )?;
        if first_evidence <= 0 {
            return Err(StoreError::CorruptSequence);
        }
        if let Some(expected) = requested_first {
            if expected != first_evidence {
                return Err(StoreError::PartitionSequenceMismatch {
                    expected,
                    found: first_evidence,
                });
            }
        }
        let highest: i64 =
            connection.query_row("SELECT COALESCE(MAX(seq), 0) FROM evidence", [], |row| {
                row.get(0)
            })?;
        Ok(Self {
            connection,
            first_evidence,
            next_evidence: if highest == 0 {
                first_evidence
            } else {
                highest + 1
            },
            migration,
        })
    }

    /// Reports a migration performed by this call to [`open`](Self::open).
    pub fn migration_report(&self) -> Option<MigrationReport> {
        self.migration
    }

    /// Makes one delivered line durable and assigns its global position.
    ///
    /// The only place `EvidenceSeq` is minted, and it is minted in the order
    /// lines are read. That is `file_order`: capture order *within* a lane,
    /// because spool filenames are timestamp-prefixed, but not across lanes,
    /// because each file is consumed whole before the next is opened.
    ///
    /// The sealed-window finalizer displaces this call site — it must assign
    /// positions from a merge over sealed segments rather than from read order.
    /// See `docs/SEALED_CAPTURE_PIPELINE_V1.md` §5.
    pub fn capture_raw(
        &mut self,
        venue: &str,
        spool_file: &str,
        byte_offset: u64,
        raw_line: &[u8],
        content: &[u8],
    ) -> Result<CapturedRecord, StoreError> {
        let seq = EvidenceSeq::new(self.next_evidence).ok_or(StoreError::CorruptSequence)?;
        let evidence_hash = EvidenceHash::hash(raw_line);
        let content_hash = ContentHash::hash(content);
        self.connection.execute(
            "INSERT INTO evidence \
             (seq, venue, spool_file, byte_offset, evidence_hash, content_hash, raw_line) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                seq.get(),
                venue,
                spool_file,
                byte_offset as i64,
                evidence_hash.to_hex(),
                content_hash.to_hex(),
                raw_line,
            ],
        )?;
        self.next_evidence += 1;
        Ok(CapturedRecord {
            seq,
            evidence_hash,
            content_hash,
            raw_line: raw_line.to_vec(),
        })
    }

    /// Commits one classification against a captured line.
    ///
    /// Requires the `CapturedRecord`, so a fact cannot be committed for evidence
    /// that is not already durable. `FactSeq` equals `EvidenceSeq` by construction
    /// — exactly one fact per delivery.
    pub fn commit_fact<T: Sinkable>(
        &mut self,
        captured: &CapturedRecord,
        value: T,
    ) -> Result<CommittedFact<T>, StoreError> {
        let bytes = value
            .to_canonical_bytes()
            .map_err(|_| StoreError::Encoding)?;
        let hex = FactHash::hash(&bytes).to_hex();
        let seq = FactSeq::new(captured.seq.get()).ok_or(StoreError::CorruptSequence)?;
        self.connection.execute(
            "INSERT INTO facts (seq, evidence_seq, fact_hash, canonical) VALUES (?1, ?2, ?3, ?4)",
            params![seq.get(), captured.seq.get(), hex, bytes],
        )?;
        Ok(CommittedFact { seq, value })
    }

    // -- partition record identity ----------------------------------------

    /// Returns the first content committed for `record_id`, if any.
    ///
    /// This indexed lookup replaces the classifier's unbounded process-local
    /// identity map. It remains exact within this database — no Bloom-filter
    /// false positives or LRU expiry can change a verdict before rollover.
    pub fn record_identity(&self, record_id: &str) -> Result<Option<RecordIdentity>, StoreError> {
        let row: Option<(Vec<u8>, i64)> = self
            .connection
            .query_row(
                "SELECT content_hash, first_seen FROM record_identity WHERE record_id = ?1",
                params![record_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((raw_content, raw_first_seen)) = row else {
            return Ok(None);
        };
        let content = ContentHash::from_raw(raw_content.try_into().map_err(|_| {
            StoreError::CorruptRecordIdentity {
                record_id: record_id.to_owned(),
            }
        })?);
        let first_seen =
            FactSeq::new(raw_first_seen).ok_or_else(|| StoreError::CorruptRecordIdentity {
                record_id: record_id.to_owned(),
            })?;
        Ok(Some(RecordIdentity {
            content,
            first_seen,
        }))
    }

    /// Commits the first identity after its fact is down.
    ///
    /// Called inside the same transaction as `commit_fact` and the spool cursor
    /// advance. A rollback therefore removes all three, while a successful
    /// cursor can never get ahead of the identity required to classify a replay.
    pub fn remember_record_identity(
        &mut self,
        record_id: &str,
        content: ContentHash,
        first_seen: FactSeq,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO record_identity (record_id, content_hash, first_seen) \
             VALUES (?1, ?2, ?3)",
            params![record_id, content.as_bytes().as_slice(), first_seen.get()],
        )?;
        Ok(())
    }

    // -- spool cursors -----------------------------------------------------

    pub fn spool_cursor(&self, spool_file: &str) -> Result<Option<SpoolCursor>, StoreError> {
        let exact = self
            .connection
            .query_row(
                "SELECT spool_file, venue, bytes_consumed, records \
                 FROM spool_cursor WHERE spool_file = ?1",
                params![spool_file],
                |row| {
                    Ok(SpoolCursor {
                        spool_file: row.get(0)?,
                        venue: row.get(1)?,
                        bytes_consumed: row.get::<_, i64>(2)? as u64,
                        records: row.get::<_, i64>(3)? as u64,
                    })
                },
            )
            .optional()?;
        if exact.is_some() {
            return Ok(exact);
        }

        // Schema v1 originally keyed cursors by the caller's path spelling.
        // New callers use canonical paths, but an existing store may still contain
        // `data/spool/...`. Capture filenames end in a timestamp plus connection
        // UUID, so the basename is the stable compatibility identity.
        let Some(filename) = Path::new(spool_file)
            .file_name()
            .and_then(|name| name.to_str())
        else {
            return Ok(None);
        };
        let suffix = format!("%/{filename}");
        Ok(self
            .connection
            .query_row(
                "SELECT spool_file, venue, bytes_consumed, records \
                 FROM spool_cursor WHERE spool_file LIKE ?1 \
                 ORDER BY bytes_consumed DESC LIMIT 1",
                params![suffix],
                |row| {
                    Ok(SpoolCursor {
                        spool_file: row.get(0)?,
                        venue: row.get(1)?,
                        bytes_consumed: row.get::<_, i64>(2)? as u64,
                        records: row.get::<_, i64>(3)? as u64,
                    })
                },
            )
            .optional()?)
    }

    pub fn advance_spool_cursor(
        &mut self,
        spool_file: &str,
        venue: &str,
        bytes_consumed: u64,
        records: u64,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO spool_cursor (spool_file, venue, bytes_consumed, records) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (spool_file) DO UPDATE SET bytes_consumed = ?3, records = ?4",
            params![spool_file, venue, bytes_consumed as i64, records as i64],
        )?;
        Ok(())
    }

    /// Marks a sealed segment fully consumed by this partition.
    ///
    /// Callers place this in the same transaction as the final cursor advance.
    /// The durable identity is copied into the partition receipt at rollover, so
    /// a retained raw segment is not re-ingested after this database is reaped.
    pub fn complete_spool_segment(
        &mut self,
        spool_file: &str,
        lane: &str,
        source_sha256: &str,
        byte_length: u64,
        line_count: u64,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO consumed_segment \
             (spool_file, lane, source_sha256, byte_length, line_count) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(spool_file) DO NOTHING",
            params![
                spool_file,
                lane,
                source_sha256,
                byte_length as i64,
                line_count as i64,
            ],
        )?;
        let recorded: (String, String, i64, i64) = self.connection.query_row(
            "SELECT lane, source_sha256, byte_length, line_count \
             FROM consumed_segment WHERE spool_file = ?1",
            params![spool_file],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if recorded
            != (
                lane.to_owned(),
                source_sha256.to_owned(),
                byte_length as i64,
                line_count as i64,
            )
        {
            return Err(StoreError::ConsumedSegmentConflict {
                spool_file: spool_file.to_owned(),
            });
        }
        Ok(())
    }

    /// Runs a closure inside one transaction.
    ///
    /// Ingest batches a run of records together with the cursor advance that
    /// covers them, so a crash mid-batch rolls back to a boundary where cursor and
    /// evidence agree. A cursor ahead of its evidence would silently skip records
    /// on the next run; behind, it would duplicate them.
    pub fn transaction<T>(
        &mut self,
        work: impl FnOnce(&mut Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let checkpoint = self.next_evidence;
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        match work(self) {
            Ok(value) => {
                self.connection.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK");
                // The in-memory counter has to walk back with the rows, or the
                // next capture would leave a hole that `check_integrity` reads as
                // a deleted row.
                self.next_evidence = checkpoint;
                Err(error)
            }
        }
    }

    // -- reads -------------------------------------------------------------

    pub fn evidence_count(&self) -> Result<u64, StoreError> {
        Ok(self
            .connection
            .query_row("SELECT COUNT(*) FROM evidence", [], |row| {
                row.get::<_, i64>(0)
            })? as u64)
    }

    pub fn fact_count(&self) -> Result<u64, StoreError> {
        Ok(self
            .connection
            .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get::<_, i64>(0))?
            as u64)
    }

    pub fn next_evidence_seq(&self) -> i64 {
        self.next_evidence
    }

    pub fn first_evidence_seq(&self) -> i64 {
        self.first_evidence
    }

    pub fn consumed_segments(&self) -> Result<Vec<ConsumedSegment>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT spool_file, lane, source_sha256, byte_length, line_count \
             FROM consumed_segment ORDER BY spool_file",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(ConsumedSegment {
                spool_file: row.get(0)?,
                lane: row.get(1)?,
                source_sha256: row.get(2)?,
                byte_length: row.get::<_, i64>(3)? as u64,
                line_count: row.get::<_, i64>(4)? as u64,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Checkpoints every WAL frame and closes the partition database.
    ///
    /// A closed partition is immutable and may later be removed as one file, so
    /// no state may remain only in `store.db-wal` when its receipt is published.
    pub fn checkpoint_and_close(self) -> Result<(), StoreError> {
        let (busy, log_frames, checkpointed): (i64, i64, i64) =
            self.connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
        if busy != 0 || checkpointed != log_frames {
            return Err(StoreError::CheckpointBusy {
                remaining_frames: log_frames.saturating_sub(checkpointed),
            });
        }
        self.connection
            .close()
            .map_err(|(_, error)| StoreError::Sqlite(error))
    }

    /// Folds every durable fact in sequence order into caller-owned state.
    ///
    /// This is recovery from the fact log, not replay from raw venue bytes. The
    /// committed hash is checked before decoding, and the callback receives the
    /// same receipt type produced by `commit_fact`, so recovered state does not
    /// bypass the durability gate.
    pub fn recover_facts<T, E>(
        &self,
        mut decode: impl FnMut(FactSeq, &[u8]) -> Result<T, E>,
        mut apply: impl FnMut(CommittedFact<T>),
    ) -> Result<u64, StoreError>
    where
        E: std::fmt::Display,
    {
        let mut statement = self
            .connection
            .prepare("SELECT seq, fact_hash, canonical FROM facts ORDER BY seq")?;
        let mut rows = statement.query([])?;
        let mut recovered = 0u64;
        while let Some(row) = rows.next()? {
            let raw_seq: i64 = row.get(0)?;
            let recorded_hash: String = row.get(1)?;
            let canonical: Vec<u8> = row.get(2)?;
            let seq = FactSeq::new(raw_seq).ok_or(StoreError::CorruptSequence)?;
            if FactHash::hash(&canonical).to_hex() != recorded_hash {
                return Err(StoreError::FactHashMismatch { seq: raw_seq });
            }
            let value = decode(seq, &canonical).map_err(|error| StoreError::Decoding {
                seq: raw_seq,
                detail: error.to_string(),
            })?;
            apply(CommittedFact { seq, value });
            recovered += 1;
        }
        Ok(recovered)
    }

    /// Re-hashes every stored line and compares against the recorded digest.
    ///
    /// A torn row must never become authority. This reports a position rather than
    /// a vague failure, and it is the closest thing this iteration has to the
    /// `verify` command that lands next.
    pub fn check_integrity(&self) -> Result<IntegrityReport, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT seq, evidence_hash, raw_line FROM evidence ORDER BY seq")?;
        let mut checked = 0u64;
        let mut mismatches = Vec::new();
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let seq: i64 = row.get(0)?;
            let recorded: String = row.get(1)?;
            let raw: Vec<u8> = row.get(2)?;
            checked += 1;
            if EvidenceHash::hash(&raw).to_hex() != recorded {
                mismatches.push(seq);
            }
        }

        // Dense from this partition's first global position: older positions
        // live in older partitions and need not remain on disk.
        let (lowest, highest): (i64, i64) = self.connection.query_row(
            "SELECT COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0) FROM evidence",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let dense = if checked == 0 {
            true
        } else {
            lowest == self.first_evidence && highest - lowest + 1 == checked as i64
        };

        Ok(IntegrityReport {
            checked,
            mismatches,
            dense,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityReport {
    pub checked: u64,
    pub mismatches: Vec<i64>,
    pub dense: bool,
}

impl IntegrityReport {
    pub fn is_clean(&self) -> bool {
        self.mismatches.is_empty() && self.dense
    }
}
