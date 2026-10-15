//! Wire V4 uses JSON strings for every integer, including domain integers.
use replay_domain::{BookKey, ContractOrientation};
use replay_risk::{
    CutOrigin, Decision, Disposition, MarketEvent, Reason, Reference, RiskCut, RiskEngine, Validity,
};
use replay_tape::{DerivativePin, SourceFaultReason};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

// Keys at this boundary are emitted in serde_json::Value's sorted order.
// Only schema literals are written raw; all borrowed text uses serde's escaping.
macro_rules! object {
    ($out:ident, $($key:literal => $value:expr),* $(,)?) => {{
        $out.raw("{");
        let mut separator = "";
        $(
            $out.raw(separator);
            $out.raw(concat!("\"", $key, "\":"));
            $value;
            separator = ",";
        )*
        let _ = separator;
        $out.raw("}");
    }};
}

/// One section buffer, borrowing Risk data for the duration of encoding only.
/// No domain serialization, numeric conversion or section extraction builds a
/// JSON tree. The existing entry/queue limits still charge the finished fields.
#[derive(Default)]
struct Section(Vec<u8>);

impl Section {
    fn raw(&mut self, value: &str) {
        self.0.extend_from_slice(value.as_bytes());
    }
    fn value(&mut self, value: impl serde::Serialize) {
        serde_json::to_writer(&mut self.0, &value).unwrap();
    }
    fn integer(&mut self, value: impl std::fmt::Display) {
        use std::io::Write;
        write!(&mut self.0, "\"{value}\"").unwrap();
    }
    fn array<T>(
        &mut self,
        values: impl IntoIterator<Item = T>,
        mut emit: impl FnMut(&mut Self, T),
    ) {
        self.raw("[");
        for (index, value) in values.into_iter().enumerate() {
            if index != 0 {
                self.raw(",");
            }
            emit(self, value);
        }
        self.raw("]");
    }
    fn optional<T>(&mut self, value: Option<T>, emit: impl FnOnce(&mut Self, T)) {
        if let Some(value) = value {
            emit(self, value);
        } else {
            self.raw("null");
        }
    }
    fn finish(self) -> String {
        String::from_utf8(self.0).unwrap()
    }
    fn pin(&mut self, p: &DerivativePin) {
        object!(self,
            "derivative_address" => self.value(&p.derivative_address),
            "receipt_sha256" => self.value(p.receipt_sha256),
        );
    }
    fn address(&mut self, a: &replay_domain::EventAddress) {
        object!(self,
            "canonical_seq" => self.integer(a.canonical_seq()),
            "delivery_index" => self.integer(a.delivery_index()),
            "event_index" => self.integer(a.event_index()),
            "lane" => self.value(a.lane()),
        );
    }
    fn reference(&mut self, r: &Reference) {
        object!(self,
            "address" => self.address(&r.address),
            "order_ns" => self.integer(r.order_ns),
            "pin" => self.pin(&r.pin),
            "visible_ns" => self.integer(r.visible_ns),
        );
    }
    fn origin(&mut self, origin: &CutOrigin) {
        match origin {
            CutOrigin::Window {
                pin,
                start_ns,
                end_ns,
            } => object!(self,
                "end_ns" => self.integer(end_ns),
                "kind" => self.value("window"),
                "pin" => self.pin(pin),
                "start_ns" => self.integer(start_ns),
            ),
            CutOrigin::Group {
                pin,
                first,
                last,
                visible_ns,
            } => object!(self,
                "first" => self.address(first),
                "kind" => self.value("group"),
                "last" => self.address(last),
                "pin" => self.pin(pin),
                "visible_ns" => self.integer(visible_ns),
            ),
        }
    }
    fn price(&mut self, p: replay_domain::ConditionalMarketPrice) {
        object!(self,
            "atoms" => self.integer(p.atoms()),
            "scale" => self.integer(p.scale().exponent()),
            "unit" => self.value(p.unit()),
        );
    }
    fn quantity(&mut self, q: replay_domain::PositiveQty) {
        object!(self,
            "atoms" => self.integer(q.atoms()),
            "scale" => self.integer(q.scale().exponent()),
            "unit" => self.value(q.unit()),
        );
    }
    fn level(&mut self, l: &replay_domain::Level) {
        object!(self,
            "price" => self.price(l.price()),
            "quantity" => self.quantity(l.quantity()),
        );
    }
    fn venue_time(&mut self, time: Option<replay_domain::VenueTime>) {
        self.optional(time, |out, t| {
            object!(out,
                "event_kind" => out.value(t.event_kind()),
                "event_ns" => out.optional(t.event_ns(), Self::integer),
                "event_resolution" => out.value(t.event_resolution()),
                "sent_ns" => out.optional(t.sent_ns(), Self::integer),
            )
        });
    }
    fn change(&mut self, c: replay_domain::LevelChange) {
        use replay_domain::LevelChange;
        let (kind, quantity) = match c {
            LevelChange::Set(q) => ("set", Some(q)),
            LevelChange::Delete => ("delete", None),
            LevelChange::Increase(q) => ("increase", Some(q)),
            LevelChange::Decrease(q) => ("decrease", Some(q)),
        };
        self.raw("{\"kind\":");
        self.value(kind);
        if let Some(quantity) = quantity {
            self.raw(",\"value\":");
            self.quantity(quantity);
        }
        self.raw("}");
    }
    fn delta(&mut self, d: &replay_domain::BookDelta, observation: bool) {
        // Observations have venue_time; transition operations deliberately do not.
        object!(self,
            "book_hash" => self.value(d.book_hash()),
            "change" => self.change(d.change()),
            "instrument" => self.value(d.instrument()),
            "orientation" => self.value(d.orientation()),
            "price" => self.price(d.price()),
            "side" => self.value(d.side()),
        );
        // Reopen the object only to append the optional observation field.
        self.0.pop();
        if observation {
            self.raw(",\"venue_time\":");
            self.venue_time(d.venue_time());
        }
        self.raw("}");
    }
    fn market(&mut self, event: &MarketEvent) {
        use replay_domain::BookEvent;
        match event {
            MarketEvent::Book(book) => {
                self.raw("{\"kind\":\"book\",\"value\":{\"kind\":");
                self.value(match book {
                    BookEvent::Full(_) => "full",
                    BookEvent::Delta(_) => "delta",
                });
                self.raw(",\"value\":");
                match book {
                    BookEvent::Full(b) => object!(self,
                        "asks" => self.array(b.asks(), Self::level),
                        "bids" => self.array(b.bids(), Self::level),
                        "instrument" => self.value(b.instrument()),
                        "orientation" => self.value(b.orientation()),
                        "snapshot_hash" => self.value(b.snapshot_hash()),
                        "source_observed_ns" => self.optional(b.source_observed_ns(), Self::integer),
                        "venue_time" => self.venue_time(b.venue_time()),
                    ),
                    BookEvent::Delta(d) => self.delta(d, true),
                }
                self.raw("}}");
            }
            MarketEvent::Trade(t) => {
                self.raw("{\"kind\":\"trade\",\"value\":");
                object!(self,
                    "aggressor" => self.value(t.aggressor()),
                    "instrument" => self.value(t.instrument()),
                    "orientation" => self.value(t.orientation()),
                    "price" => self.price(t.price()),
                    "quantity" => self.quantity(t.quantity()),
                    "venue_time" => self.venue_time(t.venue_time()),
                );
                self.raw("}");
            }
        }
    }
    fn reason(&mut self, r: &Reason) {
        let kind = match r {
            Reason::MissingInitialization => "missing_initialization",
            Reason::EpochChanged => "epoch_changed",
            Reason::ConnectionOpened => "connection_opened",
            Reason::ConnectionClosed => "connection_closed",
            Reason::ConnectionFailed => "connection_failed",
            Reason::SubscriptionChanged => "subscription_changed",
            Reason::UnsupportedState => "unsupported_state",
            Reason::ScaleMismatch => "scale_mismatch",
            Reason::QuantityUnderflow => "quantity_underflow",
            Reason::QuantityOverflow => "quantity_overflow",
            Reason::LaneNotExpected => "lane_not_expected",
            Reason::Interval(SourceFaultReason::LaneMissing) => "lane_missing",
            Reason::Continuity(v) => {
                return object!(self,
                    "kind" => self.value("continuity"),
                    "verdict" => self.value(v),
                );
            }
            Reason::Interval(SourceFaultReason::LaneInvalid { detail }) => {
                return object!(self,
                    "detail" => self.value(detail),
                    "kind" => self.value("lane_invalid"),
                );
            }
            Reason::Interval(SourceFaultReason::VisibleClockRegression {
                previous_visible_ns,
                observed_visible_ns,
            }) => {
                return object!(self,
                    "kind" => self.value("visible_clock_regression"),
                    "observed_visible_ns" => self.integer(observed_visible_ns),
                    "previous_visible_ns" => self.integer(previous_visible_ns),
                );
            }
        };
        object!(self, "kind" => self.value(kind));
    }
    fn decision(&mut self, d: &Decision) {
        match d {
            Decision::Snapshot(l) => object!(self,
                "asks" => self.array(l.asks(), |out, (p, q)| { out.raw("["); out.integer(p); out.raw(","); out.integer(q); out.raw("]"); }),
                "bids" => self.array(l.bids(), |out, (p, q)| { out.raw("["); out.integer(p); out.raw(","); out.integer(q); out.raw("]"); }),
                "kind" => self.value("snapshot"),
            ),
            Decision::Operations(ops) => object!(self,
                "kind" => self.value("operations"),
                "operations" => self.array(ops, |out, d| out.delta(d, false)),
            ),
            Decision::Invalidation(r) => object!(self,
                "kind" => self.value("invalidation"),
                "reason" => self.reason(r),
            ),
        }
    }
}

/// One Redis entry. Limits count the sum of field values, not just `record`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fields {
    pub record: String,
    pub events: Option<String>,
    pub deps: Option<String>,
}

impl Fields {
    /// Direct borrowed encoding, called only after the retention decision.
    pub fn cut(c: &RiskCut, run_id: &str, attempt_id: &str, sequence: u64) -> Self {
        let mut record = Section::default();
        object!(record,
            "attempt_id" => record.value(attempt_id),
            "body" => object!(record,
                "book_transitions" => record.array(c.book_transitions(), |out, t| object!(out,
                    "decision" => out.decision(&t.decision),
                    "key" => object!(out,
                        "instrument" => out.value(&t.key.instrument),
                        "orientation" => out.value(t.key.orientation),
                    ),
                    "previous_revision" => out.integer(t.previous_revision),
                    "revision" => out.integer(t.view.revision()),
                )),
                "origin" => record.origin(c.origin()),
                "risk_sequence" => record.integer(c.sequence()),
            ),
            "kind" => record.value("cut"),
            "run_id" => record.value(run_id),
            "sequence" => record.integer(sequence),
            "version" => record.value("4"),
        );
        let mut events = Section::default();
        events.raw("{");
        if !c.control_events().is_empty() {
            events.raw("\"control_events\":");
            events.array(c.control_events(), |out, r| {
                object!(out,
                    "event" => object!(out,
                        "from" => out.value(&r.from),
                        "kind" => out.value("metadata_changed"),
                        "to" => out.value(&r.to),
                    ),
                    "reference" => out.reference(&r.reference),
                )
            });
            events.raw(",");
        }
        events.raw("\"market_events\":");
        events.array(c.market_events(), |out, r| {
            object!(out,
                "disposition" => out.value(match r.disposition {
                    Disposition::Observed => "observed",
                    Disposition::Applied => "applied",
                    Disposition::Duplicate => "duplicate",
                    Disposition::NotAuthority => "not_authority",
                    Disposition::Invalidated => "invalidated",
                }),
                "event" => out.market(&r.event),
                "reference" => out.reference(&r.reference),
            )
        });
        events.raw("}");
        let mut deps = Section::default();
        deps.array(c.book_transitions(), |out, t| {
            out.optional(t.view.dependency(), |out, d| {
                object!(out,
                    "anchor" => out.reference(&d.anchor),
                    "epoch" => out.value(&d.epoch),
                    "through" => out.reference(&d.through),
                )
            })
        });
        Self {
            record: record.finish(),
            events: Some(events.finish()),
            deps: Some(deps.finish()),
        }
    }

    /// Encode an already constructed envelope (also the reference path for cut
    /// fixtures). Production cuts use the borrowed encoder above after retention.
    pub fn encode(mut envelope: Value) -> Self {
        let (events, deps) = if envelope["kind"] == "cut" {
            let body = envelope["body"].as_object_mut().unwrap();
            let mut events = serde_json::Map::new();
            for name in ["market_events", "control_events"] {
                if let Some(value) = body.remove(name) {
                    events.insert(name.into(), value);
                }
            }
            let deps: Vec<_> = body["book_transitions"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .map(|t| t.as_object_mut().unwrap().remove("dependency").unwrap())
                .collect();
            (
                (!events.is_empty()).then(|| Value::Object(events).to_string()),
                Some(Value::Array(deps).to_string()),
            )
        } else {
            (None, None)
        };
        Self {
            record: envelope.to_string(),
            events,
            deps,
        }
    }

    pub fn len(&self) -> usize {
        self.record.len()
            + self.events.as_ref().map_or(0, String::len)
            + self.deps.as_ref().map_or(0, String::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn exact(mut value: Value) -> Value {
    match &mut value {
        Value::Number(n) => value = Value::String(n.to_string()),
        Value::Array(a) => a.iter_mut().for_each(|v| *v = exact(v.take())),
        Value::Object(o) => o.values_mut().for_each(|v| *v = exact(v.take())),
        _ => {}
    }
    value
}
pub fn key(k: &BookKey) -> Value {
    json!({"instrument": k.instrument, "orientation": k.orientation})
}

// Transition operations retain their existing wire schema. Venue clocks are
// observations attached to market_events, not reconstruction instructions.
fn operation(delta: &replay_domain::BookDelta) -> Value {
    json!({"instrument":delta.instrument(), "orientation":delta.orientation(),
        "side":delta.side(), "price":delta.price(), "change":delta.change(),
        "book_hash":delta.book_hash()})
}
pub fn pin(p: &DerivativePin) -> Value {
    json!({"derivative_address": p.derivative_address, "receipt_sha256": p.receipt_sha256})
}
fn reference(r: &Reference) -> Value {
    json!({"pin": pin(&r.pin), "address": r.address, "visible_ns": r.visible_ns, "order_ns": r.order_ns})
}
fn origin(o: &CutOrigin) -> Value {
    match o {
        CutOrigin::Window {
            pin: p,
            start_ns,
            end_ns,
        } => json!({"kind":"window", "pin":pin(p), "start_ns":start_ns, "end_ns":end_ns}),
        CutOrigin::Group {
            pin: p,
            first,
            last,
            visible_ns,
        } => {
            json!({"kind":"group", "pin":pin(p), "first":first, "last":last, "visible_ns":visible_ns})
        }
    }
}
fn reason(r: &Reason) -> Value {
    let kind = match r {
        Reason::MissingInitialization => "missing_initialization",
        Reason::EpochChanged => "epoch_changed",
        Reason::ConnectionOpened => "connection_opened",
        Reason::ConnectionClosed => "connection_closed",
        Reason::ConnectionFailed => "connection_failed",
        Reason::SubscriptionChanged => "subscription_changed",
        Reason::UnsupportedState => "unsupported_state",
        Reason::ScaleMismatch => "scale_mismatch",
        Reason::QuantityUnderflow => "quantity_underflow",
        Reason::QuantityOverflow => "quantity_overflow",
        Reason::LaneNotExpected => "lane_not_expected",
        Reason::Continuity(v) => return json!({"kind":"continuity", "verdict":v}),
        Reason::Interval(SourceFaultReason::LaneMissing) => "lane_missing",
        Reason::Interval(SourceFaultReason::LaneInvalid { detail }) => {
            return json!({"kind":"lane_invalid", "detail":detail});
        }
        Reason::Interval(SourceFaultReason::VisibleClockRegression {
            previous_visible_ns,
            observed_visible_ns,
        }) => {
            return json!({"kind":"visible_clock_regression", "previous_visible_ns":previous_visible_ns, "observed_visible_ns":observed_visible_ns});
        }
    };
    json!({"kind":kind})
}
pub fn cut(c: &RiskCut) -> Value {
    let events: Vec<_> = c
        .market_events()
        .iter()
        .map(|r| {
            let event = match &r.event {
                MarketEvent::Book(b) => json!({"kind":"book", "value":b}),
                MarketEvent::Trade(t) => json!({"kind":"trade", "value":t}),
            };
            let disposition = match r.disposition {
                Disposition::Observed => "observed",
                Disposition::Applied => "applied",
                Disposition::Duplicate => "duplicate",
                Disposition::NotAuthority => "not_authority",
                Disposition::Invalidated => "invalidated",
            };
            json!({"reference":reference(&r.reference), "event":event, "disposition":disposition})
        })
        .collect();
    let controls: Vec<_> = c
        .control_events()
        .iter()
        .map(|r| {
            json!({
                "reference": reference(&r.reference),
                "event": {"kind":"metadata_changed", "from":r.from, "to":r.to},
            })
        })
        .collect();
    let transitions: Vec<_> = c.book_transitions().iter().map(|t| {
        let decision = match &t.decision {
            Decision::Snapshot(l) => json!({"kind":"snapshot", "bids":l.bids().iter().collect::<Vec<_>>(), "asks":l.asks().iter().collect::<Vec<_>>()}),
            Decision::Operations(ops) => json!({"kind":"operations", "operations":ops.iter().map(operation).collect::<Vec<_>>()}),
            Decision::Invalidation(r) => json!({"kind":"invalidation", "reason":reason(r)}),
        };
        let dependency = t.view.dependency().map(|d| json!({"epoch":d.epoch,"anchor":reference(&d.anchor),"through":reference(&d.through)}));
        json!({"key":key(&t.key),"previous_revision":t.previous_revision,"revision":t.view.revision(),"dependency":dependency,"decision":decision})
    }).collect();
    let mut value = json!({"origin":origin(c.origin()),"risk_sequence":c.sequence(),"market_events":events,"book_transitions":transitions});
    if !controls.is_empty() {
        value
            .as_object_mut()
            .unwrap()
            .insert("control_events".into(), Value::Array(controls));
    }
    exact(value)
}

/// Terminal digest of every planned book's final Risk state (read-only).
///
/// SHA-256 over UTF-8 lines, one per planned book sorted by instrument bytes,
/// then orientation spelling: `instrument TAB orientation TAB revision TAB
/// validity TAB bids TAB asks LF`. Validity is `not_initialized`, `usable` or
/// `unusable`; bids (descending price) and asks (ascending) are comma-joined
/// `price:quantity` decimal atoms and are empty unless the book is usable.
/// Python `replay.streams.protocol.books_sha256` computes the same bytes.
pub fn books_sha256(engine: &RiskEngine) -> String {
    let mut rows: Vec<_> = engine
        .plans()
        .keys()
        .filter_map(|key| {
            let orientation = match key.orientation {
                ContractOrientation::Outcome => "outcome",
                ContractOrientation::Complement => "complement",
            };
            engine
                .view(key)
                .map(|view| (key.instrument.as_str().to_owned(), orientation, view))
        })
        .collect();
    rows.sort_by(|a, b| (a.0.as_bytes(), a.1).cmp(&(b.0.as_bytes(), b.1)));
    let mut digest = Sha256::new();
    for (instrument, orientation, view) in rows {
        let validity = match view.validity() {
            Validity::NotInitialized => "not_initialized",
            Validity::Usable => "usable",
            Validity::Unusable(_) => "unusable",
        };
        let (bids, asks) = match (view.validity(), view.ladder()) {
            (Validity::Usable, Some(ladder)) => {
                (join(ladder.bids().iter().rev()), join(ladder.asks().iter()))
            }
            _ => (String::new(), String::new()),
        };
        digest.update(
            format!(
                "{instrument}\t{orientation}\t{}\t{validity}\t{bids}\t{asks}\n",
                view.revision()
            )
            .as_bytes(),
        );
    }
    format!("{:x}", digest.finalize())
}
fn join<'a>(levels: impl Iterator<Item = (&'a i64, &'a u64)>) -> String {
    levels
        .map(|(price, quantity)| format!("{price}:{quantity}"))
        .collect::<Vec<_>>()
        .join(",")
}
/// Closed terminal body: published and Risk cut counts and final book digest.
pub fn terminal(cuts: u64, risk_cuts: u64, books_sha256: String) -> Value {
    json!({"cuts": cuts.to_string(), "risk_cuts":risk_cuts.to_string(), "books_sha256": books_sha256})
}
