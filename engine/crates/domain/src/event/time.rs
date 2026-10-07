use serde::{Deserialize, Serialize};

use super::DomainError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Resolution {
    Millisecond,
    Microsecond,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    ExchangeEvent,
    BookUpdate,
    TradeReport,
    BookAsOf,
}

/// Venue clock evidence only; never an ordering or book-validity input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct VenueTime {
    event_ns: Option<u64>,
    event_resolution: Option<Resolution>,
    event_kind: Option<EventKind>,
    sent_ns: Option<u64>,
}

impl VenueTime {
    pub fn new(
        event_ns: Option<u64>,
        event_resolution: Option<Resolution>,
        event_kind: Option<EventKind>,
        sent_ns: Option<u64>,
    ) -> Result<Self, DomainError> {
        if event_ns.is_some() != event_resolution.is_some()
            || event_ns.is_some() != event_kind.is_some()
            || (event_ns.is_none() && sent_ns.is_none())
        {
            return Err(DomainError::InvalidVenueTime);
        }
        Ok(Self {
            event_ns,
            event_resolution,
            event_kind,
            sent_ns,
        })
    }

    pub const fn event_ns(self) -> Option<u64> {
        self.event_ns
    }
    pub const fn event_resolution(self) -> Option<Resolution> {
        self.event_resolution
    }
    pub const fn event_kind(self) -> Option<EventKind> {
        self.event_kind
    }
    pub const fn sent_ns(self) -> Option<u64> {
        self.sent_ns
    }
}

impl<'de> Deserialize<'de> for VenueTime {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "required_option")]
            event_ns: Option<u64>,
            #[serde(deserialize_with = "required_option")]
            event_resolution: Option<Resolution>,
            #[serde(deserialize_with = "required_option")]
            event_kind: Option<EventKind>,
            #[serde(deserialize_with = "required_option")]
            sent_ns: Option<u64>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.event_ns,
            wire.event_resolution,
            wire.event_kind,
            wire.sent_ns,
        )
        .map_err(serde::de::Error::custom)
    }
}

// Explicitly nullable, but required in the closed schema (serde otherwise
// silently supplies None when an Option field is missing).
pub(super) fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}
