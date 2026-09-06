//! Generic boundary between audited canonical records and Replay domain events.
//!
//! Implementations interpret payloads, but this crate contains no venue schema.
//! It only preserves the Phase 0 address/provenance and assigns child indexes.

use std::fmt;

use indexer_finalize::JoinedCanonicalRecord;
use replay_domain::{
    CanonicalProvenance, ContinuityVerdict, EventAddress, EventHeader, FaultImpact, InstrumentId,
    LaneId, SegmentEvent, SegmentRecord, Sha256,
};

/// One deterministic decision for one canonical source record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Normalization {
    /// Zero or more children in source-defined order. Zero children are an
    /// intentional ignore and remain provenance-visible in the sidecar.
    Events(Vec<SegmentEvent>),
    /// An explicit state-neutral decision. The materializer preserves its stable
    /// reason and complete source provenance in the sidecar.
    Ignored { reason_code: String },
    /// Expected malformed venue/control data. Materialization records the exact
    /// source and emits one closed `NormalizationFault` event.
    Reject(ParseReject),
}

/// Stable expected-reject facts. Exact source bytes and provenance are attached
/// by the materializer, so adapters cannot accidentally alter them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseReject {
    pub parser_version: u32,
    pub error_code: String,
    pub instrument_hint: Option<InstrumentId>,
    pub impact: FaultImpact,
}

impl ParseReject {
    pub fn validate(&self) -> Result<(), NormalizerError> {
        if self.parser_version == 0 {
            return Err(NormalizerError::new("parser_version must be positive"));
        }
        validate_code(&self.error_code, "error_code")
    }
}

/// A fatal adapter defect or failed final consistency check. This is never a
/// persisted reject because doing so would mislabel an internal failure as data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizerError(String);

impl NormalizerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for NormalizerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for NormalizerError {}

/// Stateful only within one privately staged derivative build. Implementations
/// must be deterministic for their pinned bundle/config identities.
pub trait Normalizer {
    /// Immutable semantic identity checked against the derivative address.
    fn descriptor(&self) -> &NormalizerDescriptor;

    fn normalize(
        &mut self,
        source: &JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError>;

    /// Called only after Phase 0 returned `Ok(None)`. A failure is fatal and
    /// prevents frame finalization and receipt publication.
    fn finish(&mut self) -> Result<(), NormalizerError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizerDescriptor {
    pub bundle_sha256: Sha256,
    pub config_sha256: Sha256,
}

/// Wraps a normalized child in the closed S2 schema while preserving every
/// Phase 0 provenance/address field and assigning its zero-based child index.
pub fn segment_record(
    source: &JoinedCanonicalRecord,
    event_index: u32,
    event: SegmentEvent,
) -> Result<SegmentRecord, NormalizerError> {
    let header = event_header(source, event_index)?;
    SegmentRecord::new(header, event).map_err(|error| NormalizerError::new(error.to_string()))
}

pub fn event_header(
    source: &JoinedCanonicalRecord,
    event_index: u32,
) -> Result<EventHeader, NormalizerError> {
    let lane = LaneId::new(source.event_address.lane_id.clone())
        .map_err(|error| NormalizerError::new(error.to_string()))?;
    let address = EventAddress::new(
        source.canonical_seq,
        lane,
        source.event_address.delivery_index,
        event_index,
    )
    .map_err(|error| NormalizerError::new(error.to_string()))?;
    let provenance = CanonicalProvenance::new(
        source.source_segment_sha256,
        source.source_line_number,
        source.content_hash,
        continuity(source.continuity),
    )
    .map_err(|error| NormalizerError::new(error.to_string()))?;
    EventHeader::new(
        source.order_ns,
        source.visible_ns,
        source.visible_tie_group,
        address,
        source.record_id.clone(),
        provenance,
    )
    .map_err(|error| NormalizerError::new(error.to_string()))
}

fn continuity(value: indexer_finalize::ContinuityVerdict) -> ContinuityVerdict {
    match value {
        indexer_finalize::ContinuityVerdict::Lifecycle => ContinuityVerdict::Lifecycle,
        indexer_finalize::ContinuityVerdict::Bootstrap => ContinuityVerdict::Bootstrap,
        indexer_finalize::ContinuityVerdict::UnsequencedVenue => {
            ContinuityVerdict::UnsequencedVenue
        }
        indexer_finalize::ContinuityVerdict::SparseMonotonic => ContinuityVerdict::SparseMonotonic,
        indexer_finalize::ContinuityVerdict::Continuous => ContinuityVerdict::Continuous,
        indexer_finalize::ContinuityVerdict::GapProven => ContinuityVerdict::GapProven,
        indexer_finalize::ContinuityVerdict::CursorWentBackwards => {
            ContinuityVerdict::CursorWentBackwards
        }
        indexer_finalize::ContinuityVerdict::LocalCounterBroken => {
            ContinuityVerdict::LocalCounterBroken
        }
        indexer_finalize::ContinuityVerdict::Duplicate => ContinuityVerdict::Duplicate,
        indexer_finalize::ContinuityVerdict::Conflict => ContinuityVerdict::Conflict,
    }
}

pub fn validate_code(value: &str, field: &'static str) -> Result<(), NormalizerError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(NormalizerError::new(format!(
            "{field} must be non-empty lowercase snake case"
        )));
    }
    Ok(())
}
