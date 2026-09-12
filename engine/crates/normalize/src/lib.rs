//! One normalization boundary extended by venue adapters.
//!
//! `Normalizer` owns canonical-envelope decoding, raw JSON decoding, descriptor
//! identity, and venue routing. Adapters own only their venue schema and its
//! conversion into Replay domain events.

use std::collections::BTreeMap;
use std::fmt;

use indexer_finalize::JoinedCanonicalRecord;
use indexer_types::{EnvelopeView, Venue};
use replay_domain::{
    CanonicalProvenance, ContinuityVerdict, EventAddress, EventHeader, FaultImpact, InstrumentId,
    LaneId, SegmentEvent, SegmentRecord, Sha256,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

    pub fn for_source(
        parser_version: u32,
        error_code: &'static str,
        source: &JoinedCanonicalRecord,
        instrument: Option<InstrumentId>,
    ) -> Normalization {
        let impact = instrument
            .clone()
            .map(FaultImpact::Instrument)
            .unwrap_or_else(|| {
                FaultImpact::UnattributedLane(
                    LaneId::new(source.event_address.lane_id.clone())
                        .expect("audited lane is non-empty"),
                )
            });
        Normalization::Reject(Self {
            parser_version,
            error_code: error_code.to_owned(),
            instrument_hint: instrument,
            impact,
        })
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

/// The materializer-facing behavior implemented once by [`Normalizer`].
pub trait Normalize {
    /// Immutable semantic identity checked against the derivative address.
    fn descriptor(&self) -> &NormalizerDescriptor;

    fn normalize(
        &mut self,
        source: &JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError>;

    /// Called only after the audited canonical reader returned `Ok(None)`. A
    /// failure is fatal and prevents frame finalization and receipt publication.
    fn finish(&mut self) -> Result<(), NormalizerError>;
}

/// Venue-specific extension point. Implementations receive only a fully decoded
/// canonical envelope and JSON payload; shared seam failures cannot be handled
/// differently by each venue.
pub trait VenueAdapter {
    const VENUE: Venue;
    const PARSER_VERSION: u32;
    const BUNDLE_ID: &'static str;

    fn config_identity(&self) -> NormalizerConfigIdentity;

    fn normalize(&mut self, input: CanonicalEnvelope<'_>)
    -> Result<Normalization, NormalizerError>;

    /// Optional handling for venue text controls that are not JSON. Returning
    /// `None` preserves the shared `invalid_json` reject behavior.
    fn normalize_non_json(
        &mut self,
        _source: &JoinedCanonicalRecord,
        _envelope: &EnvelopeView<'_>,
    ) -> Result<Option<Normalization>, NormalizerError> {
        Ok(None)
    }

    fn finish(&mut self) -> Result<(), NormalizerError> {
        Ok(())
    }
}

/// Closed, canonically ordered variables that participate in derivative identity.
/// Adapters cannot supply an insertion-ordered map or a custom serializer whose
/// bytes vary while its runtime meaning stays the same.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizerConfigIdentity {
    pub schema_version: u16,
    pub variables: BTreeMap<String, ConfigValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ConfigValue {
    Boolean(bool),
    Unsigned(u64),
    Text(String),
}

/// The single reusable normalizer. A venue is an adapter parameter, not a
/// separately reimplemented normalization lifecycle.
pub struct Normalizer<A> {
    adapter: A,
    descriptor: NormalizerDescriptor,
}

impl<A: VenueAdapter> Normalizer<A> {
    pub fn new(adapter: A) -> Result<Self, NormalizerError> {
        let config = serde_json::to_vec(&adapter.config_identity()).map_err(|error| {
            NormalizerError::new(format!(
                "normalizer config identity is not serializable: {error}"
            ))
        })?;
        Ok(Self {
            adapter,
            descriptor: NormalizerDescriptor {
                bundle_sha256: Sha256::digest(A::BUNDLE_ID.as_bytes()),
                config_sha256: Sha256::digest(&config),
            },
        })
    }

    pub const fn adapter(&self) -> &A {
        &self.adapter
    }
}

impl<A: VenueAdapter> Normalize for Normalizer<A> {
    fn descriptor(&self) -> &NormalizerDescriptor {
        &self.descriptor
    }

    fn normalize(
        &mut self,
        source: &JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError> {
        let envelope = EnvelopeView::parse(&source.envelope).map_err(|error| {
            NormalizerError::new(format!(
                "audited canonical envelope became invalid: {error}"
            ))
        })?;
        if envelope.venue != A::VENUE {
            return Ok(Normalization::Ignored {
                reason_code: "different_venue".to_owned(),
            });
        }
        let payload = match serde_json::from_str(&envelope.raw_payload) {
            Ok(payload) => payload,
            Err(_) => {
                if let Some(normalized) = self.adapter.normalize_non_json(source, &envelope)? {
                    return Ok(normalized);
                }
                return Ok(ParseReject::for_source(
                    A::PARSER_VERSION,
                    "invalid_json",
                    source,
                    None,
                ));
            }
        };
        self.adapter.normalize(CanonicalEnvelope {
            source,
            envelope,
            payload,
        })
    }

    fn finish(&mut self) -> Result<(), NormalizerError> {
        self.adapter.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizerDescriptor {
    pub bundle_sha256: Sha256,
    pub config_sha256: Sha256,
}

/// A canonical source record whose envelope and raw JSON have passed the shared
/// normalization seam. Venue adapters consume this value into events or a
/// rejection; they never reparse canonical bytes themselves.
pub struct CanonicalEnvelope<'a> {
    source: &'a JoinedCanonicalRecord,
    envelope: EnvelopeView<'a>,
    payload: Value,
}

impl<'a> CanonicalEnvelope<'a> {
    pub const fn source(&self) -> &'a JoinedCanonicalRecord {
        self.source
    }

    pub const fn envelope(&self) -> &EnvelopeView<'a> {
        &self.envelope
    }

    pub const fn payload(&self) -> &Value {
        &self.payload
    }

    pub fn into_payload(self) -> Value {
        self.payload
    }

    pub fn reject(
        &self,
        parser_version: u32,
        error_code: &'static str,
        instrument: Option<InstrumentId>,
    ) -> Normalization {
        ParseReject::for_source(parser_version, error_code, self.source, instrument)
    }
}

/// Wraps a normalized child in the closed Replay domain, preserves every
/// provenance/address field from the audited canonical record, and assigns its
/// zero-based child index.
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
