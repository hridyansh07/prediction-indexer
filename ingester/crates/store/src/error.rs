use core::fmt;

#[derive(Debug)]
pub enum StoreError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    /// The database was written by a different build of this program.
    SchemaMismatch {
        found: String,
    },
    /// A position outside the schema's `CHECK (seq > 0)`, which means the database
    /// was edited outside this program.
    CorruptSequence,
    /// Canonical fact bytes no longer match the digest committed beside them.
    FactHashMismatch {
        seq: i64,
    },
    /// A durable fact cannot be decoded by this build.
    Decoding {
        seq: i64,
        detail: String,
    },
    /// An indexed identity row contains a value outside the closed store schema.
    CorruptRecordIdentity {
        record_id: String,
    },
    Encoding,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io: {error}"),
            Self::Sqlite(error) => write!(formatter, "sqlite: {error}"),
            Self::SchemaMismatch { found } => write!(
                formatter,
                "store was written by schema version {found}; this build expects {}",
                crate::SCHEMA_VERSION
            ),
            Self::CorruptSequence => formatter.write_str("non-positive sequence in the store"),
            Self::FactHashMismatch { seq } => {
                write!(formatter, "fact {seq} does not match its committed hash")
            }
            Self::Decoding { seq, detail } => {
                write!(formatter, "fact {seq} could not be decoded: {detail}")
            }
            Self::CorruptRecordIdentity { record_id } => {
                write!(formatter, "record identity {record_id:?} is corrupt")
            }
            Self::Encoding => formatter.write_str("value could not produce canonical bytes"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<std::io::Error> for StoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}
