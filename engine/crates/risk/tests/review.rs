//! Characterizations for deferred review decisions; these do not prescribe new policy.
#[allow(dead_code)]
mod support;
use canonical_normalizer::{Normalization, ParseReject};
use replay_domain::*;
use replay_risk::*;
use replay_tape::{DerivativeWalker, LowerBoundPolicy, ReadLimits, ScopeFilter, WalkRequest};
use support::*;

#[test]
fn full_level_budget_combines_sides_and_preserves_fault_precedence() {
    for (limit, mismatch, latched) in [
        (3, false, false),
        (2, false, false),
        (2, true, false),
        (2, false, true),
    ] {
        let mut events = vec![];
        if latched {
            events.push(SegmentEvent::Control(ControlEvent::ConnectionClosed {
                epoch: "e1".into(),
            }));
        }
        events.push(full("kalshi:A", &[(37, 11), (17, 3)], &[(83, 2)]));
        let f = Fixture::new(0, 100, 1, vec![row("x", 1, events)], |_| {});
        let mut p = plan("kalshi:A", "x");
        if mismatch {
            p.price_scale = scale(3);
        }
        let mut e = RiskEngine::open(
            vec![f.pin.clone()],
            0,
            100,
            LowerBoundPolicy::Clip,
            vec![p],
            RiskLimits {
                max_levels_per_book: limit,
                ..RiskLimits::default()
            },
        )
        .unwrap();
        e.next_cut().unwrap();
        if limit == 2 && !mismatch && !latched {
            assert_eq!(e.next_cut().unwrap_err(), "risk level limit exceeded");
            assert_eq!(e.view(&key("kalshi:A")).unwrap().revision(), 0);
            assert!(e.next_cut().unwrap_err().contains("poisoned"));
        } else {
            let cut = e.next_cut().unwrap().unwrap();
            assert_eq!(
                cut.book_transitions()[0].view.validity(),
                &if mismatch {
                    Validity::Unusable(Reason::ScaleMismatch)
                } else if latched {
                    Validity::Unusable(Reason::ConnectionClosed)
                } else {
                    Validity::Usable
                }
            );
            assert!(e.next_cut().unwrap().is_none());
            assert!(e.finish().is_ok());
        }
    }
}

#[test]
fn walk_disposition_counts_include_clipped_sources() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            ignored("x", 1),
            Row {
                result: Normalization::Reject(ParseReject {
                    parser_version: 1,
                    error_code: "unsupported".into(),
                    instrument_hint: None,
                    impact: FaultImpact::Instrument(id("kalshi:A")),
                }),
                ..row("x", 2, vec![])
            },
            row("x", 3, vec![full("kalshi:A", &[], &[])]),
        ],
        |_| {},
    );
    let mut walker = DerivativeWalker::open(
        vec![f.pin.clone()],
        WalkRequest {
            start_ns: 3,
            end_ns: 100,
            lower_bound: LowerBoundPolicy::Clip,
            scope: ScopeFilter {
                instruments: [id("kalshi:A")].into(),
                lanes: [LaneId::new("x").unwrap()].into(),
            },
        },
        ReadLimits::default(),
    )
    .unwrap();
    while walker.next_item().unwrap().is_some() {}
    let finished = walker.finish().unwrap();
    let counts = finished.counts();
    assert_eq!(counts.source_deliveries, 3);
    assert_eq!(counts.included_sources, 1);
    assert_eq!(counts.interval_excluded_sources, 2);
    assert_eq!(counts.rejected_sources, 1);
    assert_eq!(counts.ignored_sources, 1);
    assert_eq!(counts.included_events, 1);
    assert_eq!(counts.excluded_events, 1);
}

#[test]
fn interval_reason_can_be_replaced_without_restoring_usability() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            Row {
                continuity: "gap_proven",
                ..row("x", 3, vec![full("kalshi:A", &[], &[])])
            },
            row("x", 4, vec![full("kalshi:A", &[], &[])]),
        ],
        |r| {
            r.certified = false;
            r.clock_faults.push(indexer_finalize::ClockFault {
                window_start_ns: 0,
                lane: "x".into(),
                previous_visible_ns: 8,
                observed_visible_ns: 3,
            });
        },
    );
    let mut e = RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        vec![plan("kalshi:A", "x")],
        RiskLimits::default(),
    )
    .unwrap();
    let first = e.next_cut().unwrap().unwrap();
    assert!(matches!(
        first.book_transitions()[0].decision,
        Decision::Invalidation(Reason::Interval(_))
    ));
    let gap = e.next_cut().unwrap().unwrap();
    assert_eq!(
        gap.book_transitions()[0].decision,
        Decision::Invalidation(Reason::Continuity(ContinuityVerdict::GapProven))
    );
    assert!(gap.book_transitions()[0].view.ladder().is_none());
    let full = e.next_cut().unwrap().unwrap();
    assert!(matches!(
        full.book_transitions()[0].decision,
        Decision::Invalidation(Reason::Interval(_))
    ));
    assert!(e.next_cut().unwrap().is_none());
    assert!(e.finish().is_ok());
}

#[test]
fn differently_named_unattributed_fault_is_accepted_but_has_no_authority() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row("x", 1, vec![full("kalshi:A", &[(37, 11)], &[])]),
            Row {
                result: Normalization::Reject(ParseReject {
                    parser_version: 1,
                    error_code: "unsupported".into(),
                    instrument_hint: None,
                    impact: FaultImpact::UnattributedLane(LaneId::new("other").unwrap()),
                }),
                ..row("x", 2, vec![])
            },
        ],
        |_| {},
    );
    let mut e = RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        vec![plan("kalshi:A", "x")],
        RiskLimits::default(),
    )
    .unwrap();
    e.next_cut().unwrap();
    e.next_cut().unwrap();
    let fault = e.next_cut().unwrap().unwrap();
    assert!(fault.book_transitions().is_empty());
    assert!(fault.market_events().is_empty());
    assert_eq!(
        e.view(&key("kalshi:A")).unwrap().validity(),
        &Validity::Usable
    );
    assert!(e.next_cut().unwrap().is_none());
    assert!(e.finish().is_ok());
}

#[test]
fn metadata_budget_limits_empty_windows_before_window_count() {
    let fixtures: Vec<_> = (0..170)
        .map(|n| Fixture::new(n * 100, (n + 1) * 100, 1, vec![], |_| {}))
        .collect();
    let limits = ReadLimits::default();
    let mut bytes = 0;
    let mut admitted = 0;
    for f in &fixtures {
        let m = replay_materialize::inspect_pinned(&f.pin, &limits).unwrap();
        bytes += m.metadata_bytes() + f.pin.directory.as_os_str().len() as u64;
        if bytes <= limits.max_metadata_bytes {
            admitted += 1;
        }
    }
    let request = |count| WalkRequest {
        start_ns: 0,
        end_ns: count * 100,
        lower_bound: LowerBoundPolicy::Clip,
        scope: ScopeFilter {
            instruments: Default::default(),
            lanes: Default::default(),
        },
    };
    assert!(
        DerivativeWalker::open(
            fixtures[..admitted].iter().map(|f| f.pin.clone()).collect(),
            request(admitted as u64),
            limits.clone()
        )
        .is_ok()
    );
    let error = DerivativeWalker::open(
        fixtures[..=admitted]
            .iter()
            .map(|f| f.pin.clone())
            .collect(),
        request(admitted as u64 + 1),
        limits.clone(),
    )
    .err()
    .unwrap();
    assert!(error.contains("walk resource limit"));
    println!(
        "empty profile2 metadata={} bytes; default admits {admitted} windows including paths; max_windows={}",
        replay_materialize::inspect_pinned(&fixtures[0].pin, &limits)
            .unwrap()
            .metadata_bytes(),
        limits.max_windows
    );
}

#[test]
#[ignore = "release-mode microbenchmark; not an end-to-end throughput claim"]
fn lane_lookup_cost() {
    use std::{collections::BTreeMap, hint::black_box, time::Instant};
    for (books, lanes) in [(32, 4), (1024, 4), (1024, 1024)] {
        let plans: BTreeMap<_, _> = (0..books)
            .map(|n| {
                let p = plan(&format!("kalshi:{n}"), &format!("lane-{}", n % lanes));
                (p.key.clone(), p)
            })
            .collect();
        let mut index = BTreeMap::<LaneId, Vec<BookKey>>::new();
        for (key, p) in &plans {
            index.entry(p.lane.clone()).or_default().push(key.clone());
        }
        let lane = LaneId::new("lane-0").unwrap();
        let start = Instant::now();
        for _ in 0..100_000 {
            black_box(
                plans
                    .iter()
                    .filter(|(_, p)| &p.lane == black_box(&lane))
                    .map(|(k, _)| k.clone())
                    .collect::<Vec<_>>(),
            );
        }
        let scan = start.elapsed();
        let start = Instant::now();
        for _ in 0..100_000 {
            black_box(index.get(black_box(&lane)).unwrap().as_slice());
        }
        println!(
            "books={books} lanes={lanes} 100k lookups scan+clone={scan:?} borrowed-index={:?} key headers={}B plus strings/map/vector allocations",
            start.elapsed(),
            books * std::mem::size_of::<BookKey>()
        );
    }
}
