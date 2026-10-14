//! Scope decisions shared by the event reader and walker. Excluded bodies stay
//! borrowed JSON: the build/audit and receipt-bound stored SHA-256 prove their
//! domain schema. Syntax, version, header, kind and selection evidence are still
//! validated on every read. No event bytes bypass VerifiedLines or its decoder.
use std::collections::BTreeSet;

use replay_domain::{BookEvent, FaultImpact, InstrumentId, LaneId, SegmentEvent, SegmentRecord};
use serde::Deserialize;

/// Explicit resolver inputs. No inference from lane names or prior messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeFilter {
    pub instruments: BTreeSet<InstrumentId>,
    pub lanes: BTreeSet<LaneId>,
}

impl ScopeFilter {
    pub fn impact(&self, impact: &FaultImpact) -> bool {
        match impact {
            FaultImpact::Instrument(id) => self.instruments.contains(id),
            FaultImpact::RequestedVenueBooks(venue) | FaultImpact::AuditCoverageOnly(venue) => self
                .instruments
                .iter()
                .any(|id| id.as_str().split_once(':').unwrap().0 == venue),
            FaultImpact::UnattributedLane(_) => true,
        }
    }

    pub fn event(&self, record: &SegmentRecord) -> bool {
        match record.event() {
            SegmentEvent::Book(BookEvent::Full(book)) => {
                self.instruments.contains(book.instrument())
            }
            SegmentEvent::Book(BookEvent::Delta(book)) => {
                self.instruments.contains(book.instrument())
            }
            SegmentEvent::Trade(trade) => self.instruments.contains(trade.instrument()),
            SegmentEvent::AuditAnchor(anchor) => self.instruments.contains(anchor.instrument()),
            SegmentEvent::Control(_) => self.lanes.contains(record.header().address().lane()),
            SegmentEvent::NormalizationFault(fault) => self.impact(fault.impact()),
        }
    }

    pub(crate) fn projected_event(&self, event: &EventProjection, lane: &LaneId) -> bool {
        match event {
            EventProjection::Book(BookProjection::Full(book) | BookProjection::Delta(book))
            | EventProjection::Trade(book)
            | EventProjection::AuditAnchor(book) => self.instruments.contains(&book.instrument),
            EventProjection::Control(_control) => self.lanes.contains(lane),
            EventProjection::NormalizationFault(fault) => self.impact(&fault.impact),
        }
    }
}

// These are selection/evidence fields, not typed event bodies. RawValue checks
// all body syntax without allocating book levels, trades or control payloads.
#[derive(Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum EventProjection<'a> {
    Book(BookProjection),
    Trade(InstrumentProjection),
    AuditAnchor(InstrumentProjection),
    NormalizationFault(FaultProjection),
    #[serde(borrow)]
    Control(ControlProjection<'a>),
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum BookProjection {
    Full(InstrumentProjection),
    Delta(InstrumentProjection),
}

#[derive(Deserialize)]
pub(crate) struct InstrumentProjection {
    instrument: InstrumentId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FaultProjection {
    pub(crate) reject_id: String,
    pub(crate) impact: FaultImpact,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlProjection<'a> {
    #[serde(rename = "kind")]
    _kind: ControlKind,
    #[serde(rename = "value", borrow)]
    _value: &'a serde_json::value::RawValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ControlKind {
    ConnectionOpened,
    ConnectionClosed,
    ConnectionFailed,
    SubscriptionChanged,
    MetadataChanged,
}
