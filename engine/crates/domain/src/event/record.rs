use serde::{Deserialize, Serialize};

use crate::SEGMENT_SCHEMA_VERSION;

use super::{
    AuditAnchor, BookEvent, ControlEvent, DomainError, EventHeader, NormalizationFault, TradeEvent,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SegmentEvent {
    Control(ControlEvent),
    Book(BookEvent),
    AuditAnchor(AuditAnchor),
    NormalizationFault(NormalizationFault),
    Trade(TradeEvent),
}

impl SegmentEvent {
    fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::Control(control) => control.validate(),
            Self::Book(book) => book.validate(),
            Self::AuditAnchor(anchor) => anchor.validate(),
            Self::NormalizationFault(fault) => fault.validate(),
            Self::Trade(trade) => trade.validate(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRecord {
    schema_version: u16,
    header: EventHeader,
    event: SegmentEvent,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentRecordWire {
    schema_version: u16,
    header: EventHeader,
    event: SegmentEvent,
}

impl SegmentRecord {
    pub fn new(header: EventHeader, event: SegmentEvent) -> Result<Self, DomainError> {
        header.validate()?;
        event.validate()?;
        Ok(Self {
            schema_version: SEGMENT_SCHEMA_VERSION,
            header,
            event,
        })
    }

    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    pub fn header(&self) -> &EventHeader {
        &self.header
    }

    pub fn event(&self) -> &SegmentEvent {
        &self.event
    }

    /// Compact UTF-8 JSON with stable struct-field and variant ordering.
    pub fn to_canonical_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("validated Replay domain values always serialize")
    }

    /// Decodes only the exact canonical representation. This rejects unknown
    /// fields/variants, unsupported versions, invalid states, alternate field
    /// order, and insignificant whitespace.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, DomainError> {
        let wire: SegmentRecordWire =
            serde_json::from_slice(bytes).map_err(|error| DomainError::Json(error.to_string()))?;
        let decoded = Self::from_wire(wire)?;
        if decoded.to_canonical_json() != bytes {
            return Err(DomainError::NonCanonicalEncoding);
        }
        Ok(decoded)
    }

    fn from_wire(wire: SegmentRecordWire) -> Result<Self, DomainError> {
        if wire.schema_version != SEGMENT_SCHEMA_VERSION {
            return Err(DomainError::UnsupportedSchemaVersion(wire.schema_version));
        }
        wire.header.validate()?;
        wire.event.validate()?;
        Ok(Self {
            schema_version: wire.schema_version,
            header: wire.header,
            event: wire.event,
        })
    }
}

impl<'de> Deserialize<'de> for SegmentRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::from_wire(SegmentRecordWire::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
