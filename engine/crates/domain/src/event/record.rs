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
            Self::Trade(_) => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRecord {
    schema_version: u16,
    header: EventHeader,
    event: SegmentEvent,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentRecordWire {
    schema_version: u16,
    header: Box<serde_json::value::RawValue>,
    event: Box<serde_json::value::RawValue>,
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

    /// Single-pass typed decode for bytes whose exact encoding is proven at
    /// another boundary (the Replay pinned-derivative read, whose files are
    /// bound to their receipt by SHA-256 at install). Header and event are
    /// deserialized directly into their final types from one parse; there is
    /// no intermediate raw value, second parse, or re-encode comparison.
    ///
    /// It still rejects malformed JSON, trailing data, unknown, duplicate,
    /// missing or reordered top-level fields, unknown nested fields and
    /// variants, and every state the domain types reject. `schema_version`
    /// must come first and is checked before the header or event is
    /// interpreted. It does not reject insignificant whitespace or alternate
    /// number/string spellings: use [`Self::from_canonical_json`] wherever the
    /// canonical encoding itself must be proven (writers, audits, tests).
    pub fn from_json(bytes: &[u8]) -> Result<Self, DomainError> {
        let mut unsupported = None;
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let decoded = serde::de::DeserializeSeed::deserialize(
            SinglePass {
                unsupported: &mut unsupported,
            },
            &mut deserializer,
        )
        .and_then(|record| deserializer.end().map(|()| record));
        let (schema_version, header, event) = match (decoded, unsupported) {
            (_, Some(version)) => return Err(DomainError::UnsupportedSchemaVersion(version)),
            (Err(error), None) => return Err(DomainError::Json(error.to_string())),
            (Ok(fields), None) => fields,
        };
        header.validate()?;
        event.validate()?;
        Ok(Self {
            schema_version,
            header,
            event,
        })
    }

    fn from_wire(wire: SegmentRecordWire) -> Result<Self, DomainError> {
        if !supported_schema(wire.schema_version) {
            return Err(DomainError::UnsupportedSchemaVersion(wire.schema_version));
        }
        let header: EventHeader = serde_json::from_str(wire.header.get())
            .map_err(|error| DomainError::Json(error.to_string()))?;
        let mut deserializer = serde_json::Deserializer::from_str(wire.event.get());
        let event = serde::de::DeserializeSeed::deserialize(
            EventSeed(wire.schema_version),
            &mut deserializer,
        )
        .map_err(|error| DomainError::Json(error.to_string()))?;
        header.validate()?;
        event.validate()?;
        Ok(Self {
            schema_version: wire.schema_version,
            header,
            event,
        })
    }
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(field_identifier, rename_all = "snake_case")]
enum SegmentRecordField {
    SchemaVersion,
    Header,
    Event,
}

/// `SegmentRecord::from_json`'s one-pass visitor. Fields must arrive in schema
/// order, so the version is known before any header or event byte is decoded.
struct SinglePass<'a> {
    unsupported: &'a mut Option<u16>,
}

impl<'de> serde::de::DeserializeSeed<'de> for SinglePass<'_> {
    type Value = (u16, EventHeader, SegmentEvent);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_struct(
            "SegmentRecord",
            &["schema_version", "header", "event"],
            self,
        )
    }
}

impl<'de> serde::de::Visitor<'de> for SinglePass<'_> {
    type Value = (u16, EventHeader, SegmentEvent);

    fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("a segment record object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        use serde::de::Error;
        next_field(
            &mut map,
            SegmentRecordField::SchemaVersion,
            "schema_version",
        )?;
        let schema_version: u16 = map.next_value()?;
        if !supported_schema(schema_version) {
            *self.unsupported = Some(schema_version);
            return Err(A::Error::custom("unsupported segment schema version"));
        }
        next_field(&mut map, SegmentRecordField::Header, "header")?;
        let header: EventHeader = map.next_value()?;
        next_field(&mut map, SegmentRecordField::Event, "event")?;
        let event = map.next_value_seed(EventSeed(schema_version))?;
        if map.next_key::<SegmentRecordField>()?.is_some() {
            return Err(A::Error::custom("duplicate segment record field"));
        }
        Ok((schema_version, header, event))
    }
}

/// Requires the next top-level key to be exactly `expected`.
fn next_field<'de, A>(
    map: &mut A,
    expected: SegmentRecordField,
    name: &'static str,
) -> Result<(), A::Error>
where
    A: serde::de::MapAccess<'de>,
{
    use serde::de::Error;
    match map.next_key::<SegmentRecordField>()? {
        Some(found) if found == expected => Ok(()),
        Some(_) => Err(A::Error::custom(format!(
            "segment record field `{name}` is missing or out of schema order"
        ))),
        None => Err(A::Error::missing_field(name)),
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

fn supported_schema(version: u16) -> bool {
    matches!(version, crate::SEGMENT_SCHEMA_V3 | crate::SEGMENT_SCHEMA_V4)
}

struct EventSeed(u16);
impl<'de> serde::de::DeserializeSeed<'de> for EventSeed {
    type Value = SegmentEvent;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        if self.0 == crate::SEGMENT_SCHEMA_V3 {
            super::schema3::Event::deserialize(deserializer)?
                .try_into()
                .map_err(serde::de::Error::custom)
        } else {
            SegmentEvent::deserialize(deserializer)
        }
    }
}

impl Serialize for SegmentRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut record = serializer.serialize_struct("SegmentRecord", 3)?;
        record.serialize_field("schema_version", &self.schema_version)?;
        record.serialize_field("header", &self.header)?;
        if self.schema_version == crate::SEGMENT_SCHEMA_V3 {
            record.serialize_field("event", &super::schema3::Event::from(&self.event))?;
        } else {
            record.serialize_field("event", &self.event)?;
        }
        record.end()
    }
}
