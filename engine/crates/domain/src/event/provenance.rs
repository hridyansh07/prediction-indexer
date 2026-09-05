use serde::{Deserialize, Serialize};

use super::{DomainError, LaneId, validate_text};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
struct Sha256Digest(String);

impl Sha256Digest {
    fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DomainError::InvalidDigest);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityVerdict {
    Lifecycle,
    Bootstrap,
    UnsequencedVenue,
    SparseMonotonic,
    Continuous,
    GapProven,
    CursorWentBackwards,
    LocalCounterBroken,
    Duplicate,
    Conflict,
}

/// Replay-owned downstream provenance. The future Phase-0 adapter converts from
/// `indexer_finalize::CanonicalProvenance`; no finalizer receipt type is copied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalProvenance {
    source_segment_sha256: Sha256Digest,
    source_line_number: u64,
    content_hash: Sha256Digest,
    continuity: ContinuityVerdict,
}

impl CanonicalProvenance {
    pub fn new(
        source_segment_sha256: impl Into<String>,
        source_line_number: u64,
        content_hash: impl Into<String>,
        continuity: ContinuityVerdict,
    ) -> Result<Self, DomainError> {
        if source_line_number == 0 {
            return Err(DomainError::InvalidSourceLine);
        }
        Ok(Self {
            source_segment_sha256: Sha256Digest::new(source_segment_sha256)?,
            source_line_number,
            content_hash: Sha256Digest::new(content_hash)?,
            continuity,
        })
    }

    pub fn source_segment_sha256(&self) -> &str {
        self.source_segment_sha256.as_str()
    }

    pub const fn source_line_number(&self) -> u64 {
        self.source_line_number
    }

    pub fn content_hash(&self) -> &str {
        self.content_hash.as_str()
    }

    pub const fn continuity(&self) -> ContinuityVerdict {
        self.continuity
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.source_line_number == 0 {
            Err(DomainError::InvalidSourceLine)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventAddress {
    canonical_seq: i64,
    lane: LaneId,
    delivery_index: u64,
    event_index: u32,
}

impl EventAddress {
    pub fn new(
        canonical_seq: i64,
        lane: LaneId,
        delivery_index: u64,
        event_index: u32,
    ) -> Result<Self, DomainError> {
        if canonical_seq <= 0 {
            return Err(DomainError::InvalidCanonicalSequence);
        }
        Ok(Self {
            canonical_seq,
            lane,
            delivery_index,
            event_index,
        })
    }

    /// Derives the deterministic zero-based address of a normalized child while
    /// retaining the canonical delivery address exactly.
    pub fn child(&self, event_index: u32) -> Self {
        Self {
            event_index,
            ..self.clone()
        }
    }

    pub const fn canonical_seq(&self) -> i64 {
        self.canonical_seq
    }

    pub const fn delivery_index(&self) -> u64 {
        self.delivery_index
    }

    pub const fn event_index(&self) -> u32 {
        self.event_index
    }

    pub fn lane(&self) -> &LaneId {
        &self.lane
    }

    fn validate(&self) -> Result<(), DomainError> {
        if self.canonical_seq <= 0 {
            Err(DomainError::InvalidCanonicalSequence)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventHeader {
    order_ns: u64,
    visible_ns: u64,
    visible_tie_group: Option<u64>,
    address: EventAddress,
    record_id: String,
    provenance: CanonicalProvenance,
}

impl EventHeader {
    pub fn new(
        order_ns: u64,
        visible_ns: u64,
        visible_tie_group: Option<u64>,
        address: EventAddress,
        record_id: impl Into<String>,
        provenance: CanonicalProvenance,
    ) -> Result<Self, DomainError> {
        if order_ns != visible_ns {
            return Err(DomainError::OrderClockMismatch);
        }
        let record_id = record_id.into();
        validate_text(&record_id, "record_id")?;
        Ok(Self {
            order_ns,
            visible_ns,
            visible_tie_group,
            address,
            record_id,
            provenance,
        })
    }

    pub const fn order_ns(&self) -> u64 {
        self.order_ns
    }

    pub const fn visible_ns(&self) -> u64 {
        self.visible_ns
    }

    pub const fn visible_tie_group(&self) -> Option<u64> {
        self.visible_tie_group
    }

    pub fn address(&self) -> &EventAddress {
        &self.address
    }

    pub fn record_id(&self) -> &str {
        &self.record_id
    }

    pub fn provenance(&self) -> &CanonicalProvenance {
        &self.provenance
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.order_ns != self.visible_ns {
            return Err(DomainError::OrderClockMismatch);
        }
        validate_text(&self.record_id, "record_id")?;
        self.address.validate()?;
        self.provenance.validate()
    }
}
