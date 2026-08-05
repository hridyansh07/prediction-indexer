//! The canonical encoding boundary between a value and any store that persists it.
//!
//! One contract makes a store trustworthy: **`write_to` is the single source of
//! the bytes.** A store encodes once, hashes that buffer, and persists that same
//! buffer. Encoding separately for storage and for hashing would let the two
//! disagree, and the disagreement would only surface as a corruption report years
//! of data later.
//!
//! This is also what keeps `replay` and `verify` reachable. Those commands are not
//! in this iteration, but the property they check has to hold from the first
//! commit — it cannot be retrofitted onto rows whose bytes were never pinned.

use std::io;

#[derive(Debug, PartialEq, Eq)]
pub enum SinkError {
    Io { kind: io::ErrorKind },
}

impl From<io::Error> for SinkError {
    fn from(error: io::Error) -> Self {
        Self::Io { kind: error.kind() }
    }
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { kind } => write!(formatter, "canonical encoding failed: {kind}"),
        }
    }
}

impl std::error::Error for SinkError {}

/// A value that can produce its exact, versioned canonical bytes.
///
/// The implementing type owns its version tag, field order, and collection order.
/// It knows nothing about where the bytes are stored. There is deliberately no
/// blanket implementation: when persistence needs a wrapper around a domain value,
/// the owning stage defines a named versioned wrapper and implements this on that
/// wrapper, so the persisted shape is always something a person named on purpose.
pub trait Sinkable {
    fn write_to(&self, writer: &mut dyn io::Write) -> Result<(), SinkError>;

    fn to_canonical_bytes(&self) -> Result<Vec<u8>, SinkError> {
        let mut buffer = Vec::new();
        self.write_to(&mut buffer)?;
        Ok(buffer)
    }
}
