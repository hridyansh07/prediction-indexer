//! Wire V1 uses JSON strings for every integer, including domain integers.
use replay_domain::{BookKey, ContractOrientation};
use replay_risk::{
    CutOrigin, Decision, Disposition, MarketEvent, Reason, Reference, RiskCut, RiskEngine, Validity,
};
use replay_tape::{DerivativePin, SourceFaultReason};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

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
            Decision::Operations(ops) => json!({"kind":"operations", "operations":ops}),
            Decision::Invalidation(r) => json!({"kind":"invalidation", "reason":reason(r)}),
        };
        let dependency = t.view.dependency().map(|d| json!({"epoch":d.epoch,"anchor":reference(&d.anchor),"through":reference(&d.through)}));
        json!({"key":key(&t.key),"previous_revision":t.previous_revision,"revision":t.view.revision(),"dependency":dependency,"decision":decision})
    }).collect();
    let mut value =
        json!({"origin":origin(c.origin()),"market_events":events,"book_transitions":transitions});
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
/// Closed terminal body: the cut count and the final book digest.
pub fn terminal(cuts: u64, books_sha256: String) -> Value {
    json!({"cuts": cuts.to_string(), "books_sha256": books_sha256})
}
