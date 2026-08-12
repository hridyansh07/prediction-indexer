//! Exact, disposable identity state for one finalization attempt.
//!
//! Canonicalization may have to restart a window after attributing a malformed
//! record to one lane. Identity must restart with it: a record from the excluded
//! lane cannot make a surviving record look duplicated. This index therefore
//! belongs to one merge attempt, is never a commit marker, and is deleted when
//! the attempt succeeds, faults, defers, or otherwise unwinds.

use std::path::{Path, PathBuf};

use indexer_continuity::IdentityVerdict;
use indexer_types::{ContentHash, FactSeq};
use rusqlite::{Connection, OptionalExtension, params};

const SCRATCH_FILE: &str = ".record-identity.sqlite.open";

pub(crate) struct AttemptIdentity {
    connection: Option<Connection>,
    path: PathBuf,
}

impl AttemptIdentity {
    pub(crate) fn create(directory: &Path) -> Result<Self, String> {
        let path = directory.join(SCRATCH_FILE);
        remove_scratch(&path)?;

        let connection = Connection::open(&path)
            .map_err(|error| format!("creating identity scratch {}: {error}", path.display()))?;
        let identity = Self {
            connection: Some(connection),
            path,
        };
        // This database is an uncommitted derivative of the sealed inputs. A
        // crash discards it and restarts the complete merge attempt, so journaling
        // and fsync would spend I/O without making any authoritative state safer.
        // The page cache is explicitly bounded; overflow goes to the scratch file.
        identity
            .connection()
            .execute_batch(
                "
                PRAGMA journal_mode = OFF;
                PRAGMA synchronous = OFF;
                PRAGMA temp_store = FILE;
                PRAGMA cache_size = -2048;
                PRAGMA locking_mode = EXCLUSIVE;
                CREATE TABLE record_identity (
                    record_id   TEXT PRIMARY KEY,
                    content     BLOB    NOT NULL CHECK (length(content) = 32),
                    first_seen  INTEGER NOT NULL CHECK (first_seen > 0)
                ) WITHOUT ROWID;
                BEGIN IMMEDIATE;
                ",
            )
            .map_err(|error| {
                format!(
                    "initializing identity scratch {}: {error}",
                    identity.path.display()
                )
            })?;
        Ok(identity)
    }

    pub(crate) fn verdict(
        &self,
        record_id: &str,
        content: ContentHash,
    ) -> Result<IdentityVerdict, String> {
        let mut statement = self
            .connection()
            .prepare_cached("SELECT content, first_seen FROM record_identity WHERE record_id = ?1")
            .map_err(|error| format!("preparing record identity lookup: {error}"))?;
        let row: Option<(Vec<u8>, i64)> = statement
            .query_row(params![record_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
            .map_err(|error| format!("looking up record identity {record_id:?}: {error}"))?;
        let Some((raw_content, raw_first_seen)) = row else {
            return Ok(IdentityVerdict::Unseen);
        };
        let digest: [u8; 32] = raw_content
            .try_into()
            .map_err(|_| format!("record identity {record_id:?} has an invalid content digest"))?;
        let original_content = ContentHash::from_raw(digest);
        let original = FactSeq::new(raw_first_seen)
            .ok_or_else(|| format!("record identity {record_id:?} has an invalid position"))?;
        if original_content == content {
            Ok(IdentityVerdict::Duplicate { original })
        } else {
            Ok(IdentityVerdict::Conflict {
                original,
                original_content,
            })
        }
    }

    pub(crate) fn remember(
        &mut self,
        record_id: &str,
        content: ContentHash,
        first_seen: FactSeq,
    ) -> Result<(), String> {
        let mut statement = self
            .connection()
            .prepare_cached(
                "INSERT INTO record_identity (record_id, content, first_seen) VALUES (?1, ?2, ?3)",
            )
            .map_err(|error| format!("preparing record identity insert: {error}"))?;
        statement
            .execute(params![
                record_id,
                content.as_bytes().as_slice(),
                first_seen.get()
            ])
            .map_err(|error| format!("remembering record identity {record_id:?}: {error}"))?;
        Ok(())
    }

    fn connection(&self) -> &Connection {
        self.connection
            .as_ref()
            .expect("identity connection exists until drop")
    }
}

impl Drop for AttemptIdentity {
    fn drop(&mut self) {
        self.connection.take();
        let _ = remove_scratch(&self.path);
    }
}

fn remove_scratch(path: &Path) -> Result<(), String> {
    for candidate in [
        path.to_path_buf(),
        with_suffix(path, "-journal"),
        with_suffix(path, "-wal"),
        with_suffix(path, "-shm"),
    ] {
        match std::fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "removing identity scratch {}: {error}",
                    candidate.display()
                ));
            }
        }
    }
    Ok(())
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn identity_is_exact_and_scratch_is_disposable() {
        let directory = TempDir::new("finalizer-identity").expect("temporary directory");
        let first = ContentHash::hash(b"first");
        let changed = ContentHash::hash(b"changed");
        let position = FactSeq::new(17).expect("positive position");
        {
            let mut identity = AttemptIdentity::create(directory.path()).expect("identity index");
            assert_eq!(
                identity.verdict("record", first).expect("unseen verdict"),
                IdentityVerdict::Unseen
            );
            identity
                .remember("record", first, position)
                .expect("remember first observation");
            assert_eq!(
                identity
                    .verdict("record", first)
                    .expect("duplicate verdict"),
                IdentityVerdict::Duplicate { original: position }
            );
            assert_eq!(
                identity
                    .verdict("record", changed)
                    .expect("conflict verdict"),
                IdentityVerdict::Conflict {
                    original: position,
                    original_content: first,
                }
            );
        }
        assert!(!directory.path().join(SCRATCH_FILE).exists());

        let identity = AttemptIdentity::create(directory.path()).expect("new attempt");
        assert_eq!(
            identity.verdict("record", first).expect("fresh verdict"),
            IdentityVerdict::Unseen,
            "a lane-retry attempt must not inherit identities from the excluded attempt"
        );
    }
}
