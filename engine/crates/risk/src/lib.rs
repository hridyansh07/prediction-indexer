//! Deterministic reconstruction decisions, not execution risk or completeness certification.
use std::collections::BTreeMap;
use std::sync::Arc;

use replay_domain::*;
use replay_tape::{
    AtomicGroup, DerivativePin, DerivativeWalker, FinishedWalk, LaneState, LowerBoundPolicy,
    PinnedDerivative, ReadLimits, ScopeFilter, SourceFaultReason, WalkItem, WalkRequest,
    WindowStatus,
};

/// Authority is independent of BookKey. No role is inferred from lane spelling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookPlan {
    pub key: BookKey,
    pub lane: LaneId,
    pub venue: String,
    pub price_scale: DecimalScale,
    pub quantity_scale: DecimalScale,
}

#[derive(Clone, Debug)]
pub struct RiskLimits {
    pub max_books: usize,
    pub max_levels_per_book: usize,
    pub max_plan_bytes: usize,
    pub read: ReadLimits,
}
impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_books: 1024,
            max_levels_per_book: 100_000,
            max_plan_bytes: 1_048_576,
            read: ReadLimits::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reason {
    MissingInitialization,
    EpochChanged,
    ConnectionOpened,
    ConnectionClosed,
    ConnectionFailed,
    SubscriptionChanged,
    MetadataChanged,
    Continuity(ContinuityVerdict),
    UnsupportedState,
    ScaleMismatch,
    QuantityUnderflow,
    QuantityOverflow,
    LaneNotExpected,
    Interval(SourceFaultReason),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Validity {
    NotInitialized,
    Usable,
    Unusable(Reason),
}

/// Exact atoms at the plan's scales. Ascending price maps; bids are read in reverse.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ladder {
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
}
impl Ladder {
    pub fn bids(&self) -> &BTreeMap<i64, u64> {
        &self.bids
    }
    pub fn asks(&self) -> &BTreeMap<i64, u64> {
        &self.asks
    }
    pub fn locked(&self) -> bool {
        self.top().is_some_and(|(b, a)| b == a)
    }
    pub fn crossed(&self) -> bool {
        self.top().is_some_and(|(b, a)| b > a)
    }
    fn top(&self) -> Option<(i64, i64)> {
        Some((
            *self.bids.last_key_value()?.0,
            *self.asks.first_key_value()?.0,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reference {
    pub pin: DerivativePin,
    pub address: EventAddress,
    pub visible_ns: u64,
    pub order_ns: u64,
}
impl Reference {
    fn new(pin: &DerivativePin, h: &EventHeader) -> Self {
        Self {
            pin: pin.clone(),
            address: h.address().clone(),
            visible_ns: h.visible_ns(),
            order_ns: h.order_ns(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dependency {
    pub epoch: String,
    pub anchor: Reference,
    pub through: Reference,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookView {
    revision: u64,
    validity: Validity,
    ladder: Option<Ladder>,
    dependency: Option<Dependency>,
    as_of: Option<CutOrigin>,
}
impl BookView {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn validity(&self) -> &Validity {
        &self.validity
    }
    pub fn ladder(&self) -> Option<&Ladder> {
        self.ladder.as_ref()
    }
    pub fn dependency(&self) -> Option<&Dependency> {
        self.dependency.as_ref()
    }
    /// Atomic source span, or receipt interval, at which this revision was decided.
    pub fn as_of(&self) -> Option<&CutOrigin> {
        self.as_of.as_ref()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Includes initialization/recovery and any group containing a Full.
    Snapshot(Ladder),
    Operations(Vec<BookDelta>),
    Invalidation(Reason),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookTransition {
    pub key: BookKey,
    pub previous_revision: u64,
    pub view: Arc<BookView>,
    pub decision: Decision,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    Observed,
    Applied,
    Duplicate,
    NotAuthority,
    Invalidated,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarketEvent {
    Book(BookEvent),
    Trade(TradeEvent),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketRecord {
    pub reference: Reference,
    pub event: MarketEvent,
    pub disposition: Disposition,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CutOrigin {
    Window {
        pin: DerivativePin,
        start_ns: u64,
        end_ns: u64,
    },
    Group {
        pin: DerivativePin,
        first: EventAddress,
        last: EventAddress,
        visible_ns: u64,
    },
}
/// Owned and immutable via read-only accessors; only affected books are published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RiskCut {
    sequence: u64,
    origin: CutOrigin,
    market_events: Vec<MarketRecord>,
    book_transitions: Vec<BookTransition>,
}
impl RiskCut {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn origin(&self) -> &CutOrigin {
        &self.origin
    }
    pub fn market_events(&self) -> &[MarketRecord] {
        &self.market_events
    }
    pub fn book_transitions(&self) -> &[BookTransition] {
        &self.book_transitions
    }
}

pub struct FinishedRisk {
    walk: FinishedWalk,
    cuts: u64,
    plans: BTreeMap<BookKey, BookPlan>,
}
impl FinishedRisk {
    pub fn walk(&self) -> &FinishedWalk {
        &self.walk
    }
    pub fn cuts(&self) -> u64 {
        self.cuts
    }
    pub fn plans(&self) -> &BTreeMap<BookKey, BookPlan> {
        &self.plans
    }
}

/// Owns the walker from open to EOF. There is intentionally no apply/skip/seek API.
pub struct RiskEngine {
    walker: DerivativeWalker,
    plans: BTreeMap<BookKey, BookPlan>,
    books: BTreeMap<BookKey, Arc<BookView>>,
    epochs: BTreeMap<LaneId, String>,
    blocked: BTreeMap<LaneId, Reason>,
    limits: RiskLimits,
    sequence: u64,
    poisoned: bool,
    eof: bool,
}
impl RiskEngine {
    pub fn open(
        inputs: Vec<PinnedDerivative>,
        start_ns: u64,
        end_ns: u64,
        lower_bound: LowerBoundPolicy,
        plans: Vec<BookPlan>,
        limits: RiskLimits,
    ) -> Result<Self, String> {
        limits.read.validate()?;
        if inputs.is_empty() || inputs.len() > limits.read.max_windows {
            return Err("invalid selected window count".into());
        }
        if plans.is_empty() || plans.len() > limits.max_books || limits.max_levels_per_book == 0 {
            return Err("invalid risk limits or plan size".into());
        }
        let mut bytes = 0usize;
        let mut map = BTreeMap::new();
        for plan in plans {
            bytes = bytes
                .checked_add(plan.key.instrument.as_str().len())
                .and_then(|v| v.checked_add(plan.lane.as_str().len()))
                .and_then(|v| v.checked_add(plan.venue.len()))
                .ok_or("plan size overflow")?;
            if bytes > limits.max_plan_bytes
                || plan.key.instrument.as_str().split_once(':').unwrap().0 != plan.venue
            {
                return Err("invalid risk authority or plan limit".into());
            }
            if map.insert(plan.key.clone(), plan).is_some() {
                return Err("duplicate planned book".into());
            }
        }
        let scope = ScopeFilter {
            instruments: map.keys().map(|k| k.instrument.clone()).collect(),
            lanes: map.values().map(|p| p.lane.clone()).collect(),
        };
        // Inspect every pin up front, including empty and later windows. The walker
        // independently verifies again; these planning reads mint no capability.
        for input in &inputs {
            if !replay_materialize::inspect_pinned(input, &limits.read)?.supports_source_evidence()
            {
                return Err("strong risk requires source-evidence profile 2".into());
            }
        }
        let walker = DerivativeWalker::open(
            inputs,
            WalkRequest {
                start_ns,
                end_ns,
                lower_bound,
                scope,
            },
            limits.read.clone(),
        )?;
        let books = map
            .keys()
            .map(|k| {
                (
                    k.clone(),
                    Arc::new(BookView {
                        revision: 0,
                        validity: Validity::NotInitialized,
                        ladder: None,
                        dependency: None,
                        as_of: None,
                    }),
                )
            })
            .collect();
        Ok(Self {
            walker,
            plans: map,
            books,
            epochs: BTreeMap::new(),
            blocked: BTreeMap::new(),
            limits,
            sequence: 0,
            poisoned: false,
            eof: false,
        })
    }
    pub fn view(&self, key: &BookKey) -> Option<Arc<BookView>> {
        self.books.get(key).cloned()
    }
    pub fn plans(&self) -> &BTreeMap<BookKey, BookPlan> {
        &self.plans
    }
    pub fn next_cut(&mut self) -> Result<Option<RiskCut>, String> {
        if self.poisoned {
            return Err("risk attempt is poisoned".into());
        }
        let result = self.next_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn next_inner(&mut self) -> Result<Option<RiskCut>, String> {
        let Some(item) = self.walker.next_item()? else {
            self.eof = true;
            return Ok(None);
        };
        let cut = match item {
            WalkItem::WindowStatus(status) => self.window(&status)?,
            WalkItem::Group(group) => self.group(&group)?,
        };
        Ok(Some(cut))
    }
    pub fn finish(self) -> Result<FinishedRisk, String> {
        if self.poisoned || !self.eof {
            return Err("risk requires clean EOF before finish".into());
        }
        let walk = self.walker.finish()?;
        if !walk.supports_source_evidence() {
            return Err("missing source evidence".into());
        }
        Ok(FinishedRisk {
            walk,
            cuts: self.sequence,
            plans: self.plans,
        })
    }
    fn window(&mut self, status: &WindowStatus) -> Result<RiskCut, String> {
        let coverage = status.coverage().ok_or("missing source evidence")?;
        self.blocked.clear();
        for p in self.plans.values() {
            let lane = coverage.lane(&p.lane);
            let reason = if !lane.expected {
                Some(Reason::LaneNotExpected)
            } else {
                match lane.state {
                    LaneState::Present { .. } => None,
                    LaneState::Missing => Some(Reason::Interval(SourceFaultReason::LaneMissing)),
                    LaneState::Invalid { detail } => {
                        Some(Reason::Interval(SourceFaultReason::LaneInvalid { detail }))
                    }
                    LaneState::NotExpected => Some(Reason::LaneNotExpected),
                }
            };
            if let Some(reason) = reason {
                self.blocked.insert(p.lane.clone(), reason);
            }
            if let Some(f) = coverage.faults().iter().find(|f| f.lane == p.lane) {
                self.blocked
                    .insert(p.lane.clone(), Reason::Interval(f.reason.clone()));
            }
        }
        let mut staged = BTreeMap::new();
        for (key, p) in &self.plans {
            if let Some(reason) = self.blocked.get(&p.lane) {
                staged.insert(key.clone(), Work::invalid(&self.books[key], reason.clone()));
            }
        }
        let m = status.metadata();
        self.commit(
            CutOrigin::Window {
                pin: m.pin().clone(),
                start_ns: m.manifest().effective_start_ns,
                end_ns: m.manifest().effective_end_ns,
            },
            vec![],
            staged,
        )
    }
    fn group(&mut self, group: &AtomicGroup) -> Result<RiskCut, String> {
        let mut staged = BTreeMap::<BookKey, Work>::new();
        let mut events = vec![];
        for delivery in group.deliveries() {
            let h = delivery.header();
            let lane = h.address().lane();
            let duplicate = h.provenance().continuity() == ContinuityVerdict::Duplicate;
            let relevant: Vec<_> = self
                .plans
                .iter()
                .filter(|(_, p)| &p.lane == lane)
                .map(|(k, _)| k.clone())
                .collect();
            let epoch = delivery.connection_epoch().ok_or("missing source epoch")?;
            if !duplicate && !relevant.is_empty() {
                if self
                    .epochs
                    .insert(lane.clone(), epoch.into())
                    .is_some_and(|old| old != epoch)
                {
                    invalidate(
                        &mut staged,
                        &self.books,
                        &relevant,
                        Reason::EpochChanged,
                        false,
                    );
                }
                let broken = match h.provenance().continuity() {
                    ContinuityVerdict::GapProven
                    | ContinuityVerdict::CursorWentBackwards
                    | ContinuityVerdict::LocalCounterBroken
                    | ContinuityVerdict::Conflict => true,
                    ContinuityVerdict::Lifecycle
                    | ContinuityVerdict::Bootstrap
                    | ContinuityVerdict::UnsequencedVenue
                    | ContinuityVerdict::SparseMonotonic
                    | ContinuityVerdict::Continuous
                    | ContinuityVerdict::Duplicate => false,
                };
                if broken {
                    invalidate(
                        &mut staged,
                        &self.books,
                        &relevant,
                        Reason::Continuity(h.provenance().continuity()),
                        true,
                    );
                }
            }
            for record in delivery.records() {
                let reference = Reference::new(group.pin(), record.header());
                let market = match record.event() {
                    SegmentEvent::Book(b) => Some(MarketEvent::Book(b.clone())),
                    SegmentEvent::Trade(t) => Some(MarketEvent::Trade(t.clone())),
                    _ => None,
                };
                if let Some(event) = market {
                    events.push(MarketRecord {
                        reference: reference.clone(),
                        event,
                        disposition: if duplicate {
                            Disposition::Duplicate
                        } else {
                            Disposition::Observed
                        },
                    });
                }
                if duplicate {
                    continue;
                }
                match record.event() {
                    SegmentEvent::Book(book) => {
                        let key = book_key(book);
                        let Some(plan) = self.plans.get(&key).filter(|p| &p.lane == lane) else {
                            events.last_mut().unwrap().disposition = Disposition::NotAuthority;
                            continue;
                        };
                        let w = staged
                            .entry(key.clone())
                            .or_insert_with(|| Work::new(&self.books[&key]));
                        if let Some(reason) = self.blocked.get(lane) {
                            w.fail(reason.clone(), true);
                        }
                        if !w.latched {
                            match apply(&mut w.view, plan, book, &reference, epoch) {
                                Ok(()) => match book {
                                    BookEvent::Delta(d) => w.operations.push(d.clone()),
                                    BookEvent::Full(_) => w.full = true,
                                },
                                Err(reason) => w.fail(reason, true),
                            }
                        }
                        if w.view.ladder.as_ref().is_some_and(|l| {
                            l.bids.len().saturating_add(l.asks.len())
                                > self.limits.max_levels_per_book
                        }) {
                            return Err("risk level limit exceeded".into());
                        }
                        events.last_mut().unwrap().disposition =
                            if w.view.validity == Validity::Usable {
                                Disposition::Applied
                            } else {
                                Disposition::Invalidated
                            };
                    }
                    SegmentEvent::NormalizationFault(f) if !relevant.is_empty() => {
                        let keys: Vec<_> = relevant
                            .iter()
                            .filter(|k| match f.impact() {
                                FaultImpact::Instrument(i) => &k.instrument == i,
                                FaultImpact::RequestedVenueBooks(v) => self.plans[*k].venue == *v,
                                FaultImpact::UnattributedLane(l) => l == lane,
                                FaultImpact::AuditCoverageOnly(_) => false,
                            })
                            .cloned()
                            .collect();
                        invalidate(
                            &mut staged,
                            &self.books,
                            &keys,
                            Reason::UnsupportedState,
                            true,
                        );
                    }
                    SegmentEvent::Control(control) if !relevant.is_empty() => {
                        let (reason, latch) = match control {
                            ControlEvent::ConnectionOpened { .. } => {
                                (Reason::ConnectionOpened, false)
                            }
                            ControlEvent::ConnectionClosed { .. } => {
                                (Reason::ConnectionClosed, true)
                            }
                            ControlEvent::ConnectionFailed { .. } => {
                                (Reason::ConnectionFailed, true)
                            }
                            ControlEvent::SubscriptionChanged { .. } => {
                                (Reason::SubscriptionChanged, false)
                            }
                            ControlEvent::MetadataChanged { .. } => (Reason::MetadataChanged, true),
                        };
                        invalidate(&mut staged, &self.books, &relevant, reason, latch);
                    }
                    _ => {}
                }
            }
        }
        // Earlier accepted operations in a failed key's group are rolled back,
        // and their analytics disposition must describe that final decision.
        for e in &mut events {
            if let MarketEvent::Book(b) = &e.event {
                if e.disposition == Disposition::Applied
                    && staged
                        .get(&book_key(b))
                        .is_some_and(|w| w.view.validity != Validity::Usable)
                {
                    e.disposition = Disposition::Invalidated;
                }
            }
        }
        self.commit(
            CutOrigin::Group {
                pin: group.pin().clone(),
                first: group.first().clone(),
                last: group.last().clone(),
                visible_ns: group.visible_ns(),
            },
            events,
            staged,
        )
    }
    fn commit(
        &mut self,
        origin: CutOrigin,
        market_events: Vec<MarketRecord>,
        staged: BTreeMap<BookKey, Work>,
    ) -> Result<RiskCut, String> {
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or("cut sequence overflow")?;
        let mut transitions = vec![];
        for (key, mut w) in staged {
            let previous = self.books[&key].revision;
            w.view.revision = previous.checked_add(1).ok_or("book revision overflow")?;
            w.view.as_of = Some(origin.clone());
            let decision = match &w.view.validity {
                Validity::Usable if w.full => Decision::Snapshot(w.view.ladder.clone().unwrap()),
                Validity::Usable => Decision::Operations(w.operations),
                Validity::Unusable(reason) => Decision::Invalidation(reason.clone()),
                Validity::NotInitialized => return Err("internal uninitialized transition".into()),
            };
            transitions.push(BookTransition {
                key,
                previous_revision: previous,
                view: Arc::new(w.view),
                decision,
            });
        }
        for t in &transitions {
            self.books.insert(t.key.clone(), t.view.clone());
        }
        self.sequence = sequence;
        Ok(RiskCut {
            sequence,
            origin,
            market_events,
            book_transitions: transitions,
        })
    }
}

struct Work {
    view: BookView,
    operations: Vec<BookDelta>,
    full: bool,
    latched: bool,
}
impl Work {
    fn new(view: &BookView) -> Self {
        Self {
            view: view.clone(),
            operations: vec![],
            full: false,
            latched: false,
        }
    }
    fn invalid(view: &BookView, reason: Reason) -> Self {
        let mut w = Self::new(view);
        w.fail(reason, true);
        w
    }
    fn fail(&mut self, reason: Reason, latch: bool) {
        if self.latched {
            return;
        }
        self.view.validity = Validity::Unusable(reason);
        self.view.ladder = None;
        self.view.dependency = None;
        self.operations.clear();
        self.full = false;
        self.latched = latch;
    }
}
fn invalidate(
    staged: &mut BTreeMap<BookKey, Work>,
    books: &BTreeMap<BookKey, Arc<BookView>>,
    keys: &[BookKey],
    reason: Reason,
    latch: bool,
) {
    for key in keys {
        staged
            .entry(key.clone())
            .or_insert_with(|| Work::new(&books[key]))
            .fail(reason.clone(), latch);
    }
}
fn book_key(book: &BookEvent) -> BookKey {
    match book {
        BookEvent::Full(b) => b.book_key(),
        BookEvent::Delta(d) => d.book_key(),
    }
}
fn apply(
    view: &mut BookView,
    plan: &BookPlan,
    book: &BookEvent,
    reference: &Reference,
    epoch: &str,
) -> Result<(), Reason> {
    match book {
        BookEvent::Full(full) => {
            let mut ladder = Ladder::default();
            for (side, levels) in [
                (&mut ladder.bids, full.bids()),
                (&mut ladder.asks, full.asks()),
            ] {
                for level in levels {
                    if level.price().scale() != plan.price_scale
                        || level.quantity().scale() != plan.quantity_scale
                    {
                        return Err(Reason::ScaleMismatch);
                    }
                    side.insert(level.price().atoms(), level.quantity().atoms());
                }
            }
            view.ladder = Some(ladder);
            view.validity = Validity::Usable;
            view.dependency = Some(Dependency {
                epoch: epoch.into(),
                anchor: reference.clone(),
                through: reference.clone(),
            });
        }
        BookEvent::Delta(delta) => {
            match &view.validity {
                Validity::NotInitialized => return Err(Reason::MissingInitialization),
                Validity::Unusable(reason) => return Err(reason.clone()),
                Validity::Usable => {}
            }
            if delta.price().scale() != plan.price_scale {
                return Err(Reason::ScaleMismatch);
            }
            let q = match delta.change() {
                LevelChange::Delete => None,
                LevelChange::Set(q) | LevelChange::Increase(q) | LevelChange::Decrease(q) => {
                    Some(q)
                }
            };
            if q.is_some_and(|q| q.scale() != plan.quantity_scale) {
                return Err(Reason::ScaleMismatch);
            }
            let ladder = view.ladder.as_mut().unwrap();
            let side = match delta.side() {
                Side::Bid => &mut ladder.bids,
                Side::Ask => &mut ladder.asks,
            };
            let p = delta.price().atoms();
            let old = side.get(&p).copied().unwrap_or(0);
            let new = match delta.change() {
                LevelChange::Delete => 0,
                LevelChange::Set(q) => q.atoms(),
                LevelChange::Increase(q) => old
                    .checked_add(q.atoms())
                    .filter(|v| *v <= MAX_QUANTITY_ATOMS)
                    .ok_or(Reason::QuantityOverflow)?,
                LevelChange::Decrease(q) => old
                    .checked_sub(q.atoms())
                    .ok_or(Reason::QuantityUnderflow)?,
            };
            if new == 0 {
                side.remove(&p);
            } else {
                side.insert(p, new);
            }
            view.dependency.as_mut().unwrap().through = reference.clone();
        }
    }
    Ok(())
}
