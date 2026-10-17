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
        let (schema_version, header, event) = parse_json(bytes, EventSeed)?;
        event.validate()?;
        Ok(Self {
            schema_version,
            header,
            event,
        })
    }

    /// Single-pass selected-body decode for the pinned scoped reader. Unlike
    /// the unscoped decoder, this also rejects unknown fields on the event,
    /// book and control wrappers, as the scoped selection projection does.
    /// On failure the reader must use its projection fallback to preserve the
    /// original validation order and diagnostic text.
    pub fn from_json_scoped(bytes: &[u8]) -> Result<Self, DomainError> {
        let (schema_version, header, event) = parse_json(bytes, ScopedEventSeed)?;
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

/// Validated version/header with a borrowed, syntactically validated JSON body.
/// Only a receipt-bound reader may skip body schema validation after selection;
/// writers and independent audits must decode the complete record instead.
pub struct RawSegmentRecord<'a> {
    schema_version: u16,
    header: EventHeader,
    event: &'a serde_json::value::RawValue,
}

impl<'a> RawSegmentRecord<'a> {
    pub fn from_json(bytes: &'a [u8]) -> Result<Self, DomainError> {
        let (schema_version, header, event) = parse_json(bytes, |_| {
            core::marker::PhantomData::<&serde_json::value::RawValue>
        })?;
        Ok(Self {
            schema_version,
            header,
            event,
        })
    }

    pub fn header(&self) -> &EventHeader {
        &self.header
    }

    pub fn schema_version(&self) -> u16 {
        self.schema_version
    }

    pub fn event_json(&self) -> &str {
        self.event.get()
    }

    /// Retain evidence without copying it or constructing an excluded body.
    pub fn into_header(self) -> EventHeader {
        self.header
    }

    pub fn into_record(self) -> Result<SegmentRecord, DomainError> {
        let mut deserializer = serde_json::Deserializer::from_str(self.event.get());
        let event = serde::de::DeserializeSeed::deserialize(
            EventSeed(self.schema_version),
            &mut deserializer,
        )
        .and_then(|event| deserializer.end().map(|()| event))
        .map_err(|error| DomainError::Json(error.to_string()))?;
        event.validate()?;
        Ok(SegmentRecord {
            schema_version: self.schema_version,
            header: self.header,
            event,
        })
    }
}

fn parse_json<'de, F, S>(
    bytes: &'de [u8],
    event_seed: F,
) -> Result<(u16, EventHeader, S::Value), DomainError>
where
    F: FnOnce(u16) -> S,
    S: serde::de::DeserializeSeed<'de>,
{
    let mut unsupported = None;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let decoded = serde::de::DeserializeSeed::deserialize(
        SinglePass {
            unsupported: &mut unsupported,
            event_seed,
        },
        &mut deserializer,
    )
    .and_then(|record| deserializer.end().map(|()| record));
    let fields = match (decoded, unsupported) {
        (_, Some(version)) => return Err(DomainError::UnsupportedSchemaVersion(version)),
        (Err(error), None) => return Err(DomainError::Json(error.to_string())),
        (Ok(fields), None) => fields,
    };
    fields.1.validate()?;
    Ok(fields)
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
struct SinglePass<'a, F> {
    unsupported: &'a mut Option<u16>,
    event_seed: F,
}

impl<'de, F, S> serde::de::DeserializeSeed<'de> for SinglePass<'_, F>
where
    F: FnOnce(u16) -> S,
    S: serde::de::DeserializeSeed<'de>,
{
    type Value = (u16, EventHeader, S::Value);

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

impl<'de, F, S> serde::de::Visitor<'de> for SinglePass<'_, F>
where
    F: FnOnce(u16) -> S,
    S: serde::de::DeserializeSeed<'de>,
{
    type Value = (u16, EventHeader, S::Value);

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
        let event = map.next_value_seed((self.event_seed)(schema_version))?;
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

// Reuse the existing payload types (including frozen schema 3); only wrapper
// validation differs from the unscoped decoder. No RawValue or Value tree.
#[derive(Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ScopedEvent<B, T> {
    Control(ClosedWrapper<ControlEvent>),
    Book(ClosedWrapper<B>),
    AuditAnchor(AuditAnchor),
    NormalizationFault(NormalizationFault),
    Trade(T),
}

struct ScopedEventSeed(u16);
impl<'de> serde::de::DeserializeSeed<'de> for ScopedEventSeed {
    type Value = SegmentEvent;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        if self.0 == crate::SEGMENT_SCHEMA_V3 {
            use super::schema3;
            let event =
                match ScopedEvent::<schema3::Book, schema3::Trade>::deserialize(deserializer)? {
                    ScopedEvent::Control(v) => schema3::Event::Control(v.0),
                    ScopedEvent::Book(v) => schema3::Event::Book(v.0),
                    ScopedEvent::AuditAnchor(v) => schema3::Event::AuditAnchor(v),
                    ScopedEvent::NormalizationFault(v) => schema3::Event::NormalizationFault(v),
                    ScopedEvent::Trade(v) => schema3::Event::Trade(v),
                };
            event.try_into().map_err(serde::de::Error::custom)
        } else {
            Ok(
                match ScopedEvent::<BookEvent, TradeEvent>::deserialize(deserializer)? {
                    ScopedEvent::Control(v) => SegmentEvent::Control(v.0),
                    ScopedEvent::Book(v) => SegmentEvent::Book(v.0),
                    ScopedEvent::AuditAnchor(v) => SegmentEvent::AuditAnchor(v),
                    ScopedEvent::NormalizationFault(v) => SegmentEvent::NormalizationFault(v),
                    ScopedEvent::Trade(v) => SegmentEvent::Trade(v),
                },
            )
        }
    }
}

struct ClosedWrapper<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for ClosedWrapper<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct WrapperVisitor<T>(core::marker::PhantomData<T>);

        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for WrapperVisitor<T> {
            type Value = ClosedWrapper<T>;

            fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str("a closed kind/value wrapper")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(WrapperMap(
                    map,
                )))
                .map(ClosedWrapper)
            }
        }

        deserializer.deserialize_map(WrapperVisitor(core::marker::PhantomData))
    }
}

// Guard only this wrapper's keys. The domain's existing decoder still owns
// duplicates, variants and payload validation, with no stricter leaf schema.
struct WrapperMap<A>(A);

impl<'de, A: serde::de::MapAccess<'de>> serde::de::MapAccess<'de> for WrapperMap<A> {
    type Error = A::Error;

    fn next_key_seed<K: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Kind,
            Value,
        }

        self.0
            .next_key::<Field>()?
            .map(|field| {
                let key = match field {
                    Field::Kind => "kind",
                    Field::Value => "value",
                };
                seed.deserialize(serde::de::value::StrDeserializer::<A::Error>::new(key))
            })
            .transpose()
    }

    fn next_value_seed<V: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        self.0.next_value_seed(seed)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CanonicalProvenance, ContinuityVerdict, ContractOrientation, EventAddress, FullBook,
        InstrumentId, LaneId, Sha256,
    };

    #[test]
    fn scoped_decode_closes_wrappers_without_changing_unscoped_decode() {
        let header = EventHeader::new(
            1,
            1,
            None,
            EventAddress::new(1, LaneId::new("x").unwrap(), 1, 0).unwrap(),
            "record-1",
            CanonicalProvenance::new(
                Sha256::digest(b"source"),
                1,
                Sha256::digest(b"content"),
                ContinuityVerdict::Continuous,
            )
            .unwrap(),
        )
        .unwrap();
        for event in [
            SegmentEvent::Book(BookEvent::Full(
                FullBook::new(
                    InstrumentId::new("kalshi:A").unwrap(),
                    ContractOrientation::Outcome,
                    vec![],
                    vec![],
                    None,
                    None,
                )
                .unwrap(),
            )),
            SegmentEvent::Control(ControlEvent::ConnectionClosed { epoch: "e".into() }),
        ] {
            let record = SegmentRecord::new(header.clone(), event).unwrap();
            let canonical = String::from_utf8(record.to_canonical_json()).unwrap();
            for version in [crate::SEGMENT_SCHEMA_V3, crate::SEGMENT_SCHEMA_V4] {
                let line = if version == crate::SEGMENT_SCHEMA_V3 {
                    canonical
                        .replacen("\"schema_version\":4", "\"schema_version\":3", 1)
                        .replace(",\"venue_time\":null", "")
                } else {
                    canonical.clone()
                };
                let decoded = SegmentRecord::from_json_scoped(line.as_bytes()).unwrap();
                assert_eq!(decoded, SegmentRecord::from_json(line.as_bytes()).unwrap());
                assert_eq!(decoded.to_canonical_json(), line.as_bytes());
                // An extra key after the value keeps the canonical selection
                // prefix intact. Both wrapper depths must still reject it.
                for depth in [2, 3] {
                    let invalid = format!(
                        "{},\"extra\":{{}}{}",
                        &line[..line.len() - depth],
                        "}".repeat(depth)
                    );
                    assert!(SegmentRecord::from_json(invalid.as_bytes()).is_ok());
                    assert!(SegmentRecord::from_json_scoped(invalid.as_bytes()).is_err());
                }
            }
        }
    }
}
