//! Pull traversal of pinned normalized derivatives, never canonical or venue JSON.
//! Groups are immutable evidence, not book mutations. A future cursor must apply
//! each complete group before a strategy can inspect books or skip evaluation.
use std::collections::{BTreeMap, BTreeSet};

use replay_domain::{
    BookEvent, BookKey, ContinuityVerdict, EventAddress, EventHeader, FaultImpact, InstrumentId,
    LaneId, SegmentEvent, SegmentRecord,
};
pub use replay_materialize::{
    CoverageEvidence, LaneCoverage, LaneState, SourceFault, SourceFaultReason,
};
pub use replay_materialize::{DerivativeMetadata, DerivativePin, PinnedDerivative, ReadLimits};
use replay_materialize::{
    RejectDisposition, SourceDelivery, VerifiedWindowReader, inspect_pinned, open_pinned,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LowerBoundPolicy {
    Clip,
    ExpandToWindowStart,
    RequireWindowBoundary,
}

/// Explicit resolver inputs. No inference from lane names or prior messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeFilter {
    pub instruments: BTreeSet<InstrumentId>,
    pub lanes: BTreeSet<LaneId>,
}

impl ScopeFilter {
    fn impact(&self, impact: &FaultImpact) -> bool {
        match impact {
            FaultImpact::Instrument(id) => self.instruments.contains(id),
            FaultImpact::RequestedVenueBooks(venue) | FaultImpact::AuditCoverageOnly(venue) => self
                .instruments
                .iter()
                .any(|id| id.as_str().split_once(':').unwrap().0 == venue),
            FaultImpact::UnattributedLane(_) => true,
        }
    }
    fn event(&self, record: &SegmentRecord) -> bool {
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalkRequest {
    pub start_ns: u64,
    pub end_ns: u64,
    pub lower_bound: LowerBoundPolicy,
    pub scope: ScopeFilter,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalkCounts {
    pub source_deliveries: u64,
    pub included_sources: u64,
    pub scope_excluded_sources: u64,
    pub interval_excluded_sources: u64,
    pub included_events: u64,
    pub excluded_events: u64,
    pub rejected_sources: u64,
    pub ignored_sources: u64,
    pub groups: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FilteredDelivery {
    header: EventHeader,
    connection_epoch: Option<String>,
    records: Vec<SegmentRecord>,
    disposition: Option<RejectDisposition>,
}
impl FilteredDelivery {
    /// Exact splice connection identity, or None for the frozen old profile.
    pub fn connection_epoch(&self) -> Option<&str> {
        self.connection_epoch.as_deref()
    }
    pub fn header(&self) -> &EventHeader {
        &self.header
    }
    pub fn records(&self) -> &[SegmentRecord] {
        &self.records
    }
    pub fn disposition(&self) -> Option<&RejectDisposition> {
        self.disposition.as_ref()
    }
}

/// Original unfiltered source span; children keep original event indexes.
#[derive(Debug, PartialEq, Eq)]
pub struct AtomicGroup {
    pin: DerivativePin,
    first: EventAddress,
    last: EventAddress,
    visible_ns: u64,
    tie: Option<u64>,
    deliveries: Vec<FilteredDelivery>,
}
impl AtomicGroup {
    pub fn pin(&self) -> &DerivativePin {
        &self.pin
    }
    pub fn first(&self) -> &EventAddress {
        &self.first
    }
    pub fn last(&self) -> &EventAddress {
        &self.last
    }
    pub fn visible_ns(&self) -> u64 {
        self.visible_ns
    }
    pub fn visible_tie_group(&self) -> Option<u64> {
        self.tie
    }
    pub fn deliveries(&self) -> &[FilteredDelivery] {
        &self.deliveries
    }
    /// Keys named by actual book events, not a claim that they were mutated.
    /// Faults and controls have separate, possibly wider impact semantics.
    pub fn book_keys(&self) -> impl Iterator<Item = BookKey> + '_ {
        self.deliveries
            .iter()
            .flat_map(|d| d.records.iter())
            .filter_map(|r| match r.event() {
                SegmentEvent::Book(BookEvent::Full(book)) => Some(book.book_key()),
                SegmentEvent::Book(BookEvent::Delta(book)) => Some(book.book_key()),
                _ => None,
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoverageDetails {
    NotRecordedInDerivativeV1,
    ReceiptBoundV2,
}

#[derive(Debug)]
pub struct WindowStatus {
    metadata: DerivativeMetadata,
}
impl WindowStatus {
    pub fn metadata(&self) -> &DerivativeMetadata {
        &self.metadata
    }
    pub fn coverage_details(&self) -> CoverageDetails {
        if self.metadata.supports_source_evidence() {
            CoverageDetails::ReceiptBoundV2
        } else {
            CoverageDetails::NotRecordedInDerivativeV1
        }
    }
    pub fn coverage(&self) -> Option<&CoverageEvidence> {
        self.metadata.coverage()
    }
}

#[derive(Debug)]
pub enum WalkItem {
    WindowStatus(Box<WindowStatus>),
    Group(AtomicGroup),
}

#[derive(Debug)]
pub struct FinishedWalk {
    request: WalkRequest,
    effective_start_ns: u64,
    pins: Vec<DerivativePin>,
    counts: WalkCounts,
    source_evidence_complete: bool,
}
impl FinishedWalk {
    /// True only when every pinned window, including empty ones, has profile-2 evidence.
    pub fn supports_source_evidence(&self) -> bool {
        self.source_evidence_complete
    }
    pub fn request(&self) -> &WalkRequest {
        &self.request
    }
    pub fn effective_interval(&self) -> (u64, u64) {
        (self.effective_start_ns, self.request.end_ns)
    }
    pub fn pins(&self) -> &[DerivativePin] {
        &self.pins
    }
    pub fn counts(&self) -> &WalkCounts {
        &self.counts
    }
}

pub struct DerivativeWalker {
    windows: Vec<(PinnedDerivative, DerivativeMetadata)>,
    request: WalkRequest,
    effective_start_ns: u64,
    limits: ReadLimits,
    index: usize,
    reader: Option<VerifiedWindowReader>,
    pending: Option<SourceDelivery>,
    previous_seq: Option<i64>,
    lane_deliveries: BTreeMap<LaneId, u64>,
    lane_bytes: u64,
    counts: WalkCounts,
    poisoned: bool,
    exhausted: bool,
}

impl DerivativeWalker {
    pub fn open(
        inputs: Vec<PinnedDerivative>,
        request: WalkRequest,
        limits: ReadLimits,
    ) -> Result<Self, String> {
        limits.validate()?;
        if inputs.is_empty() || inputs.len() > limits.max_windows {
            return Err("invalid selected window count".into());
        }
        if request.start_ns >= request.end_ns {
            return Err("invalid requested interval".into());
        }
        if request
            .scope
            .instruments
            .len()
            .saturating_add(request.scope.lanes.len())
            > limits.max_scope_entries
        {
            return Err("scope exceeds entry limit".into());
        }
        let mut windows = Vec::new();
        let mut metadata_bytes = 0u64;
        for id in &request.scope.instruments {
            add(
                &mut metadata_bytes,
                id.as_str().len() as u64,
                limits.max_metadata_bytes,
                "scope metadata bytes",
            )?;
        }
        for id in &request.scope.lanes {
            add(
                &mut metadata_bytes,
                id.as_str().len() as u64,
                limits.max_metadata_bytes,
                "scope metadata bytes",
            )?;
        }
        for input in inputs {
            add(
                &mut metadata_bytes,
                input.directory.as_os_str().len() as u64,
                limits.max_metadata_bytes,
                "path metadata bytes",
            )?;
            let metadata = inspect_pinned(&input, &limits)?;
            add(
                &mut metadata_bytes,
                metadata.metadata_bytes(),
                limits.max_metadata_bytes,
                "window metadata bytes",
            )?;
            windows.push((input, metadata));
        }
        windows.sort_by_key(|(_, m)| m.manifest().effective_start_ns);
        let mut end = None;
        for (_, metadata) in &windows {
            let m = metadata.manifest();
            if m.effective_end_ns <= request.start_ns || m.effective_start_ns >= request.end_ns {
                return Err("redundant window outside requested interval".into());
            }
            if end.is_some_and(|end| m.effective_start_ns != end) {
                return Err("derivative windows are not uniquely adjacent".into());
            }
            end = Some(m.effective_end_ns);
        }
        let start = windows[0].1.manifest().effective_start_ns;
        if start > request.start_ns || end.unwrap() < request.end_ns {
            return Err("derivatives do not cover requested interval".into());
        }
        let effective_start_ns = match request.lower_bound {
            LowerBoundPolicy::Clip => request.start_ns,
            LowerBoundPolicy::ExpandToWindowStart => start,
            LowerBoundPolicy::RequireWindowBoundary if start == request.start_ns => start,
            LowerBoundPolicy::RequireWindowBoundary => {
                return Err("requested start is not a window boundary".into());
            }
        };
        Ok(Self {
            windows,
            request,
            effective_start_ns,
            limits,
            index: 0,
            reader: None,
            pending: None,
            previous_seq: None,
            lane_deliveries: BTreeMap::new(),
            lane_bytes: 0,
            counts: WalkCounts::default(),
            poisoned: false,
            exhausted: false,
        })
    }

    pub fn next_item(&mut self) -> Result<Option<WalkItem>, String> {
        if self.poisoned {
            return Err("derivative walker is poisoned".into());
        }
        if self.exhausted {
            return Ok(None);
        }
        let result = self.next_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn source(&mut self) -> Result<Option<SourceDelivery>, String> {
        let delivery = self.reader.as_mut().unwrap().next_delivery()?;
        if let Some(d) = &delivery {
            let address = d.header().address();
            if !self.lane_deliveries.contains_key(address.lane()) {
                if self.lane_deliveries.len() == self.limits.max_lanes {
                    return Err("lane count exceeds walk limit".into());
                }
                add(
                    &mut self.lane_bytes,
                    address.lane().as_str().len() as u64,
                    self.limits.max_metadata_bytes,
                    "lane metadata bytes",
                )?;
            }
            if self
                .lane_deliveries
                .insert(address.lane().clone(), address.delivery_index())
                .is_some_and(|previous| previous >= address.delivery_index())
            {
                return Err("source delivery index repeats or decreases across windows".into());
            }
            let seq = d.header().address().canonical_seq();
            if self
                .previous_seq
                .is_some_and(|p| p.checked_add(1) != Some(seq))
            {
                return Err("source canonical sequence is not adjacent across windows".into());
            }
            self.previous_seq = Some(seq);
        }
        Ok(delivery)
    }

    fn next_inner(&mut self) -> Result<Option<WalkItem>, String> {
        loop {
            if self.reader.is_none() {
                if self.index == self.windows.len() {
                    self.exhausted = true;
                    return Ok(None);
                }
                let (input, expected) = &self.windows[self.index];
                let reader = open_pinned(input, &self.limits)?;
                if reader.metadata() != expected {
                    return Err("derivative metadata changed since planning".into());
                }
                let status = WindowStatus {
                    metadata: reader.metadata().clone(),
                };
                self.reader = Some(reader);
                return Ok(Some(WalkItem::WindowStatus(Box::new(status))));
            }
            let first = match self.pending.take() {
                Some(d) => Some(d),
                None => self.source()?,
            };
            let Some(first) = first else {
                self.reader.take().unwrap().finish()?;
                self.index += 1;
                continue;
            };
            let mut group = AtomicGroup {
                pin: self.windows[self.index].0.pin.clone(),
                first: first.header().address().clone(),
                last: first.header().address().clone(),
                visible_ns: first.header().visible_ns(),
                tie: first.header().visible_tie_group(),
                deliveries: Vec::new(),
            };
            let mut bytes = 0;
            let mut records = 0;
            let mut current = first;
            loop {
                add(
                    &mut bytes,
                    current.logical_bytes(),
                    self.limits.max_group_bytes,
                    "group bytes",
                )?;
                add(
                    &mut records,
                    current.record_count(),
                    self.limits.max_group_records,
                    "group records",
                )?;
                group.last = current
                    .records()
                    .last()
                    .map(|r| r.header().address())
                    .unwrap_or(current.header().address())
                    .clone();
                if let Some(delivery) = self.filter(&current)? {
                    group.deliveries.push(delivery);
                }
                let next = self.source()?;
                if group.tie.is_some()
                    && next
                        .as_ref()
                        .is_some_and(|d| d.header().visible_tie_group() == group.tie)
                {
                    current = next.unwrap();
                } else {
                    self.pending = next;
                    break;
                }
            }
            if !group.deliveries.is_empty() {
                add(&mut self.counts.groups, 1, u64::MAX, "groups")?;
                return Ok(Some(WalkItem::Group(group)));
            }
        }
    }

    fn filter(&mut self, delivery: &SourceDelivery) -> Result<Option<FilteredDelivery>, String> {
        add(
            &mut self.counts.source_deliveries,
            1,
            u64::MAX,
            "source deliveries",
        )?;
        match delivery.disposition() {
            Some(RejectDisposition::ParseReject { .. }) => add(
                &mut self.counts.rejected_sources,
                1,
                u64::MAX,
                "rejected sources",
            )?,
            Some(RejectDisposition::IntentionallyIgnored { .. }) => add(
                &mut self.counts.ignored_sources,
                1,
                u64::MAX,
                "ignored sources",
            )?,
            None => {}
        }
        let h = delivery.header();
        if h.visible_ns() < self.effective_start_ns || h.visible_ns() >= self.request.end_ns {
            add(
                &mut self.counts.interval_excluded_sources,
                1,
                u64::MAX,
                "interval excluded sources",
            )?;
            add(
                &mut self.counts.excluded_events,
                delivery.records().len() as u64,
                u64::MAX,
                "excluded events",
            )?;
            return Ok(None);
        }
        let scope = &self.request.scope;
        let records: Vec<_> = delivery
            .records()
            .iter()
            .filter(|r| scope.event(r))
            .cloned()
            .collect();
        let disposition = delivery
            .disposition()
            .filter(|d| match d {
                RejectDisposition::ParseReject { impact, .. } => scope.impact(impact),
                RejectDisposition::IntentionallyIgnored { .. } => {
                    scope.lanes.contains(h.address().lane())
                }
            })
            .cloned();
        let lane_fault = scope.lanes.contains(h.address().lane())
            && matches!(
                h.provenance().continuity(),
                ContinuityVerdict::Conflict
                    | ContinuityVerdict::GapProven
                    | ContinuityVerdict::CursorWentBackwards
                    | ContinuityVerdict::LocalCounterBroken
            );
        add(
            &mut self.counts.included_events,
            records.len() as u64,
            u64::MAX,
            "included events",
        )?;
        add(
            &mut self.counts.excluded_events,
            (delivery.records().len() - records.len()) as u64,
            u64::MAX,
            "excluded events",
        )?;
        // Profile-2 source epochs are state-bearing even when every child is
        // outside instrument scope. Keep source-only evidence on planned lanes;
        // never restore excluded market events or sidecar payloads.
        let source_epoch =
            delivery.connection_epoch().is_some() && scope.lanes.contains(h.address().lane());
        if records.is_empty() && disposition.is_none() && !lane_fault && !source_epoch {
            add(
                &mut self.counts.scope_excluded_sources,
                1,
                u64::MAX,
                "scope excluded sources",
            )?;
            return Ok(None);
        }
        add(
            &mut self.counts.included_sources,
            1,
            u64::MAX,
            "included sources",
        )?;
        Ok(Some(FilteredDelivery {
            header: h.clone(),
            connection_epoch: delivery.connection_epoch().map(str::to_owned),
            records,
            disposition,
        }))
    }

    pub fn finish(self) -> Result<FinishedWalk, String> {
        if self.poisoned || !self.exhausted {
            return Err("walker requires verified EOF before finish".into());
        }
        Ok(FinishedWalk {
            source_evidence_complete: self
                .windows
                .iter()
                .all(|(_, metadata)| metadata.supports_source_evidence()),
            request: self.request,
            effective_start_ns: self.effective_start_ns,
            pins: self
                .windows
                .into_iter()
                .map(|(input, _)| input.pin)
                .collect(),
            counts: self.counts,
        })
    }
}

fn add(total: &mut u64, count: u64, maximum: u64, label: &'static str) -> Result<(), String> {
    *total = total
        .checked_add(count)
        .filter(|n| *n <= maximum)
        .ok_or_else(|| format!("walk resource limit exceeded: {label}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::add;

    #[test]
    fn budget_errors_name_the_limit_and_preserve_the_counter() {
        let mut total = 7;
        add(&mut total, 2, 9, "group bytes").unwrap();
        assert_eq!(
            add(&mut total, 1, 9, "group bytes").unwrap_err(),
            "walk resource limit exceeded: group bytes"
        );
        assert_eq!(total, 9);
        total = u64::MAX;
        assert_eq!(
            add(&mut total, 1, u64::MAX, "included events").unwrap_err(),
            "walk resource limit exceeded: included events"
        );
        assert_eq!(total, u64::MAX);
    }
}
