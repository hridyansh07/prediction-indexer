//! Wire V1 uses JSON strings for every integer, including domain integers.
use replay_domain::BookKey;
use replay_risk::{CutOrigin, Decision, Disposition, MarketEvent, Reason, Reference, RiskCut};
use replay_tape::{DerivativePin, SourceFaultReason};
use serde_json::{Value, json};

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
        Reason::MetadataChanged => "metadata_changed",
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
    let transitions: Vec<_> = c.book_transitions().iter().map(|t| {
        let decision = match &t.decision {
            Decision::Snapshot(l) => json!({"kind":"snapshot", "bids":l.bids().iter().collect::<Vec<_>>(), "asks":l.asks().iter().collect::<Vec<_>>()}),
            Decision::Operations(ops) => json!({"kind":"operations", "operations":ops}),
            Decision::Invalidation(r) => json!({"kind":"invalidation", "reason":reason(r)}),
        };
        let dependency = t.view.dependency().map(|d| json!({"epoch":d.epoch,"anchor":reference(&d.anchor),"through":reference(&d.through)}));
        json!({"key":key(&t.key),"previous_revision":t.previous_revision,"revision":t.view.revision(),"dependency":dependency,"decision":decision})
    }).collect();
    exact(
        json!({"origin":origin(c.origin()),"market_events":events,"book_transitions":transitions}),
    )
}
