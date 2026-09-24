mod support;
use canonical_normalizer::{Normalization, ParseReject};
use replay_domain::*;
use replay_risk::*;
use replay_tape::{LowerBoundPolicy, PinnedDerivative};
use support::*;

fn open(f: &Fixture, plans: Vec<BookPlan>) -> RiskEngine {
    RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        plans,
        RiskLimits::default(),
    )
    .unwrap()
}
fn collect(mut e: RiskEngine) -> Vec<RiskCut> {
    let mut cuts = vec![];
    while let Some(c) = e.next_cut().unwrap() {
        cuts.push(c);
    }
    assert_eq!(e.finish().unwrap().cuts(), cuts.len() as u64);
    cuts
}
fn bid(c: &RiskCut, name: &str, p: i64) -> u64 {
    c.book_transitions()
        .iter()
        .find(|t| t.key == key(name))
        .unwrap()
        .view
        .ladder()
        .unwrap()
        .bids()[&p]
}
fn trade(name: &str, quantity: u64) -> SegmentEvent {
    SegmentEvent::Trade(TradeEvent::new(
        id(name),
        ContractOrientation::Outcome,
        ConditionalMarketPrice::from_atoms(37, scale(2)).unwrap(),
        qty(quantity),
        Some(Side::Ask),
    ))
}
fn reject(lane: &'static str, time: u64, impact: FaultImpact) -> Row {
    Row {
        result: Normalization::Reject(ParseReject {
            parser_version: 1,
            error_code: "unsupported".into(),
            instrument_hint: None,
            impact,
        }),
        ..row(lane, time, vec![])
    }
}

#[test]
fn whole_deliveries_ties_ordered_updates_trades_views_and_repeatability() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row(
                "x",
                1,
                vec![
                    full("kalshi:A", &[(37, 11)], &[]),
                    full("kalshi:B", &[(21, 6)], &[]),
                ],
            ),
            row(
                "x",
                2,
                vec![
                    delta("kalshi:A", 37, LevelChange::Increase(qty(3))),
                    trade("kalshi:A", 5),
                    delta("kalshi:A", 37, LevelChange::Decrease(qty(7))),
                    trade("kalshi:A", 9),
                ],
            ),
            row("y", 2, vec![full("polymarket:T", &[], &[(83, 2)])]),
            row(
                "x",
                3,
                vec![delta("kalshi:B", 21, LevelChange::Set(qty(19)))],
            ),
        ],
        |_| {},
    );
    let plans = vec![
        plan("kalshi:A", "x"),
        plan("kalshi:B", "x"),
        plan("polymarket:T", "y"),
    ];
    let mut e = open(&f, plans.clone());
    assert_eq!(
        e.view(&key("kalshi:A")).unwrap().validity(),
        &Validity::NotInitialized
    );
    e.next_cut().unwrap(); // coverage
    let first = e.next_cut().unwrap().unwrap();
    assert_eq!(first.book_transitions().len(), 2);
    let retained = e.view(&key("kalshi:A")).unwrap();
    let tied = e.next_cut().unwrap().unwrap();
    assert_eq!(tied.book_transitions().len(), 2);
    assert_eq!(bid(&tied, "kalshi:A", 37), 7);
    assert_eq!(retained.ladder().unwrap().bids()[&37], 11);
    assert_eq!(tied.market_events().len(), 5);
    assert_eq!(
        tied.market_events()
            .iter()
            .map(|m| m.reference.address.event_index())
            .collect::<Vec<_>>(),
        [0, 1, 2, 3, 0]
    );
    let quantities = tied
        .market_events()
        .iter()
        .filter_map(|m| match &m.event {
            MarketEvent::Trade(t) => Some(t.quantity().atoms()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(quantities, [5, 9]);
    let a = &tied.book_transitions()[0];
    assert_eq!(a.previous_revision, 1);
    assert_eq!(a.view.revision(), 2);
    assert!(matches!(&a.decision,Decision::Operations(ops) if ops.len() == 2));
    let dep = a.view.dependency().unwrap();
    assert_eq!(dep.anchor.address.canonical_seq(), 1);
    assert_eq!(dep.through.address.event_index(), 2);
    assert_eq!(e.next_cut().unwrap().unwrap().book_transitions().len(), 1);
    assert!(e.next_cut().unwrap().is_none());
    assert!(e.finish().unwrap().walk().supports_source_evidence());
    assert_eq!(collect(open(&f, plans.clone())), collect(open(&f, plans)));
}

#[test]
fn failure_rolls_back_only_affected_key_and_latches_until_later_full() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row(
                "x",
                1,
                vec![
                    full("kalshi:A", &[(37, 11)], &[]),
                    full("kalshi:B", &[(21, 6)], &[]),
                ],
            ),
            row(
                "x",
                2,
                vec![
                    delta("kalshi:A", 37, LevelChange::Increase(qty(3))),
                    delta("kalshi:A", 37, LevelChange::Decrease(qty(15))),
                    full("kalshi:A", &[(37, 99)], &[]),
                    delta("kalshi:B", 21, LevelChange::Set(qty(19))),
                ],
            ),
            row(
                "x",
                3,
                vec![delta("kalshi:A", 37, LevelChange::Set(qty(8)))],
            ),
            row(
                "x",
                4,
                vec![
                    full("kalshi:A", &[], &[]),
                    delta("kalshi:A", 37, LevelChange::Increase(qty(2))),
                ],
            ),
        ],
        |_| {},
    );
    let c = collect(open(&f, vec![plan("kalshi:A", "x"), plan("kalshi:B", "x")]));
    assert_eq!(
        c[2].book_transitions()[0].view.validity(),
        &Validity::Unusable(Reason::QuantityUnderflow)
    );
    assert!(c[2].book_transitions()[0].view.ladder().is_none());
    assert!(
        c[2].market_events()[..3]
            .iter()
            .all(|e| e.disposition == Disposition::Invalidated)
    );
    assert_eq!(bid(&c[2], "kalshi:B", 21), 19);
    assert!(c[3].book_transitions()[0].view.ladder().is_none());
    assert_eq!(
        c[3].book_transitions()[0].decision,
        Decision::Invalidation(Reason::QuantityUnderflow)
    );
    assert_eq!(bid(&c[4], "kalshi:A", 37), 2);
    assert!(matches!(
        c[4].book_transitions()[0].decision,
        Decision::Snapshot(_)
    ));
}

#[test]
fn exact_numeric_boundaries_full_replacement_and_nonsticky_crossing() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row(
                "x",
                1,
                vec![full(
                    "limitless:A",
                    &[(37, MAX_QUANTITY_ATOMS - 3)],
                    &[(36, 2)],
                )],
            ),
            row(
                "x",
                2,
                vec![delta("limitless:A", 37, LevelChange::Increase(qty(3)))],
            ),
            row(
                "x",
                3,
                vec![delta("limitless:A", 37, LevelChange::Increase(qty(1)))],
            ),
            row("x", 4, vec![full("limitless:A", &[(12, 7)], &[(12, 9)])]),
            row(
                "x",
                5,
                vec![delta("limitless:A", 12, LevelChange::Decrease(qty(7)))],
            ),
            row("x", 6, vec![delta("limitless:A", 99, LevelChange::Delete)]),
            row("x", 7, vec![full("limitless:A", &[], &[])]),
            row(
                "x",
                8,
                vec![delta("limitless:A", 99, LevelChange::Decrease(qty(1)))],
            ),
        ],
        |_| {},
    );
    let c = collect(open(&f, vec![plan("limitless:A", "x")]));
    assert!(c[1].book_transitions()[0].view.ladder().unwrap().crossed());
    assert_eq!(bid(&c[2], "limitless:A", 37), MAX_QUANTITY_ATOMS);
    assert_eq!(
        c[3].book_transitions()[0].decision,
        Decision::Invalidation(Reason::QuantityOverflow)
    );
    let l = c[4].book_transitions()[0].view.ladder().unwrap();
    assert!(l.locked());
    assert!(!l.crossed());
    assert_eq!(l.bids().len(), 1);
    let l = c[5].book_transitions()[0].view.ladder().unwrap();
    assert!(!l.locked());
    assert!(l.bids().is_empty());
    assert_eq!(l.asks()[&12], 9);
    assert_eq!(
        c[7].book_transitions()[0].view.validity(),
        &Validity::Usable
    );
    assert_eq!(
        c[8].book_transitions()[0].decision,
        Decision::Invalidation(Reason::QuantityUnderflow)
    );
}

#[test]
fn epoch_evidence_from_filtered_ignored_rejected_and_duplicate_sources() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row("x", 1, vec![full("kalshi:A", &[(37, 11)], &[])]),
            Row {
                epoch: "e2",
                ..row("x", 2, vec![full("kalshi:excluded", &[], &[])])
            },
            Row {
                epoch: "e2",
                ..row("x", 3, vec![full("kalshi:A", &[(37, 4)], &[])])
            },
            Row {
                epoch: "e1",
                continuity: "duplicate",
                ..row(
                    "x",
                    4,
                    vec![
                        delta("kalshi:A", 37, LevelChange::Increase(qty(99))),
                        trade("kalshi:A", 7),
                    ],
                )
            },
            Row {
                epoch: "e2",
                ..row(
                    "x",
                    5,
                    vec![delta("kalshi:A", 37, LevelChange::Increase(qty(2)))],
                )
            },
            Row {
                epoch: "e3",
                ..ignored("x", 6)
            },
            Row {
                epoch: "e4",
                ..reject("x", 7, FaultImpact::Instrument(id("kalshi:excluded")))
            },
            Row {
                epoch: "e4",
                ..row("x", 8, vec![full("kalshi:A", &[(37, 5)], &[])])
            },
        ],
        |_| {},
    );
    let c = collect(open(&f, vec![plan("kalshi:A", "x")]));
    for i in [2, 6, 7] {
        assert!(c[i].market_events().is_empty());
        assert_eq!(
            c[i].book_transitions()[0].decision,
            Decision::Invalidation(Reason::EpochChanged)
        );
    }
    assert!(c[4].book_transitions().is_empty());
    assert!(
        c[4].market_events()
            .iter()
            .all(|e| e.disposition == Disposition::Duplicate)
    );
    assert_eq!(bid(&c[5], "kalshi:A", 37), 6);
    assert_eq!(
        c[5].book_transitions()[0].view.dependency().unwrap().epoch,
        "e2"
    );
    assert_eq!(bid(&c[8], "kalshi:A", 37), 5);
}

#[test]
fn all_continuity_classes_and_control_classes() {
    for (verdict, fault) in [
        ("lifecycle", None),
        ("bootstrap", None),
        ("unsequenced_venue", None),
        ("sparse_monotonic", None),
        ("continuous", None),
        ("gap_proven", Some(ContinuityVerdict::GapProven)),
        (
            "cursor_went_backwards",
            Some(ContinuityVerdict::CursorWentBackwards),
        ),
        (
            "local_counter_broken",
            Some(ContinuityVerdict::LocalCounterBroken),
        ),
        ("conflict", Some(ContinuityVerdict::Conflict)),
    ] {
        let f = Fixture::new(
            0,
            100,
            1,
            vec![
                row("x", 1, vec![full("kalshi:A", &[(37, 11)], &[])]),
                Row {
                    continuity: verdict,
                    ..row("x", 2, vec![full("kalshi:A", &[(37, 3)], &[])])
                },
                row("x", 3, vec![full("kalshi:A", &[], &[])]),
            ],
            |_| {},
        );
        let c = collect(open(&f, vec![plan("kalshi:A", "x")]));
        if let Some(v) = fault {
            assert_eq!(
                c[2].book_transitions()[0].decision,
                Decision::Invalidation(Reason::Continuity(v))
            );
        } else {
            assert_eq!(bid(&c[2], "kalshi:A", 37), 3);
        }
        assert_eq!(
            c[3].book_transitions()[0].view.validity(),
            &Validity::Usable
        );
    }
    for (control, reason, recover) in [
        (
            ControlEvent::ConnectionOpened {
                epoch: "e1".into(),
                instruments: vec![id("kalshi:A")],
                delivers_deltas: true,
                target_digest: None,
            },
            Reason::ConnectionOpened,
            true,
        ),
        (
            ControlEvent::ConnectionClosed { epoch: "e1".into() },
            Reason::ConnectionClosed,
            false,
        ),
        (
            ControlEvent::ConnectionFailed {
                epoch: "e1".into(),
                reason: "failed".into(),
            },
            Reason::ConnectionFailed,
            false,
        ),
        (
            ControlEvent::SubscriptionChanged {
                from: None,
                to: "next".into(),
            },
            Reason::SubscriptionChanged,
            true,
        ),
        (
            ControlEvent::MetadataChanged {
                from: None,
                to: "next".into(),
            },
            Reason::MetadataChanged,
            false,
        ),
    ] {
        let f = Fixture::new(
            0,
            100,
            1,
            vec![
                row("x", 1, vec![full("kalshi:A", &[(37, 11)], &[])]),
                row(
                    "x",
                    2,
                    vec![
                        SegmentEvent::Control(control),
                        full("kalshi:A", &[(37, 3)], &[]),
                    ],
                ),
                row("x", 3, vec![full("kalshi:A", &[], &[])]),
            ],
            |_| {},
        );
        let c = collect(open(&f, vec![plan("kalshi:A", "x")]));
        if recover {
            assert_eq!(bid(&c[2], "kalshi:A", 37), 3);
        } else {
            assert_eq!(
                c[2].book_transitions()[0].decision,
                Decision::Invalidation(reason)
            );
        }
        assert_eq!(
            c[3].book_transitions()[0].view.validity(),
            &Validity::Usable
        );
    }
}

#[test]
fn orientations_fault_scopes_and_audit_lanes_are_separate() {
    let no = SegmentEvent::Book(BookEvent::Full(
        FullBook::new(
            id("kalshi:A"),
            ContractOrientation::Complement,
            vec![Level::new(
                ConditionalMarketPrice::from_atoms(63, scale(2)).unwrap(),
                qty(19),
            )],
            vec![],
            None,
            None,
        )
        .unwrap(),
    ));
    let mut no_plan = plan("kalshi:A", "x");
    no_plan.key.orientation = ContractOrientation::Complement;
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row(
                "x",
                1,
                vec![
                    full("kalshi:A", &[(31, 7)], &[]),
                    no,
                    full("kalshi:B", &[], &[]),
                ],
            ),
            reject(
                "audit",
                2,
                FaultImpact::RequestedVenueBooks("kalshi".into()),
            ),
            reject("x", 3, FaultImpact::AuditCoverageOnly("kalshi".into())),
            reject("x", 4, FaultImpact::Instrument(id("kalshi:A"))),
            reject(
                "x",
                5,
                FaultImpact::UnattributedLane(LaneId::new("x").unwrap()),
            ),
        ],
        |_| {},
    );
    let c = collect(open(
        &f,
        vec![plan("kalshi:A", "x"), no_plan, plan("kalshi:B", "x")],
    ));
    assert_eq!(
        c[1].book_transitions()[0].view.ladder().unwrap().bids()[&31],
        7
    );
    assert_eq!(
        c[1].book_transitions()[1].view.ladder().unwrap().bids()[&63],
        19
    );
    assert!(
        c[1].book_transitions()
            .iter()
            .all(|t| t.view.ladder().unwrap().asks().is_empty())
    );
    assert!(c[2].book_transitions().is_empty());
    assert!(c[3].book_transitions().is_empty());
    assert_eq!(c[4].book_transitions().len(), 2);
    assert!(
        c[4].book_transitions()
            .iter()
            .all(|t| t.key.instrument == id("kalshi:A"))
    );
    assert_eq!(c[5].book_transitions().len(), 3);
}

#[test]
fn planned_absent_lane_is_fault_even_when_certified_and_audit_missing_is_inert() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![row("x", 1, vec![full("kalshi:A", &[], &[])])],
        |_| {},
    );
    let c = collect(open(
        &f,
        vec![plan("kalshi:A", "x"), plan("kalshi:B", "absent")],
    ));
    assert_eq!(
        c[0].book_transitions()[0].decision,
        Decision::Invalidation(Reason::LaneNotExpected)
    );
    assert_eq!(
        c[1].book_transitions()[0].view.validity(),
        &Validity::Usable
    );
    let f = Fixture::new(
        0,
        100,
        1,
        vec![row("x", 1, vec![full("kalshi:A", &[], &[])])],
        |r| {
            r.certified = false;
            r.completeness = "incomplete".into();
            r.deadline_expired = true;
            r.expected_lanes.push("audit".into());
            r.missing_lanes.push(indexer_finalize::LaneFault {
                lane: "audit".into(),
                reason: "lane_missing".into(),
                detail: None,
            });
        },
    );
    let c = collect(open(&f, vec![plan("kalshi:A", "x")]));
    assert!(c[0].book_transitions().is_empty());
    assert_eq!(
        c[1].book_transitions()[0].view.validity(),
        &Validity::Usable
    );
}

#[test]
fn empty_fault_windows_and_clock_intervals_require_later_clean_full() {
    let a = Fixture::new(
        0,
        10,
        1,
        vec![row("x", 1, vec![full("kalshi:A", &[(37, 4)], &[])])],
        |_| {},
    );
    for invalid in [false, true] {
        let b = Fixture::new(10, 20, 2, vec![], |r| {
            r.certified = false;
            r.completeness = "incomplete".into();
            r.deadline_expired = true;
            r.expected_lanes.push("x".into());
            let fault = indexer_finalize::LaneFault {
                lane: "x".into(),
                reason: if invalid {
                    "lane_invalid"
                } else {
                    "lane_missing"
                }
                .into(),
                detail: invalid.then(|| "digest".into()),
            };
            if invalid {
                r.invalid_lanes.push(fault);
            } else {
                r.missing_lanes.push(fault);
            }
        });
        let c = Fixture::new(
            20,
            30,
            2,
            vec![
                row(
                    "x",
                    21,
                    vec![delta("kalshi:A", 37, LevelChange::Set(qty(9)))],
                ),
                row("x", 22, vec![full("kalshi:A", &[(37, 6)], &[])]),
            ],
            |_| {},
        );
        let cuts = collect(
            RiskEngine::open(
                vec![a.pin.clone(), b.pin, c.pin],
                0,
                30,
                LowerBoundPolicy::Clip,
                vec![plan("kalshi:A", "x")],
                RiskLimits::default(),
            )
            .unwrap(),
        );
        assert!(matches!(
            cuts[2].book_transitions()[0].decision,
            Decision::Invalidation(Reason::Interval(_))
        ));
        assert!(cuts[3].book_transitions().is_empty());
        assert!(cuts[4].book_transitions()[0].view.ladder().is_none());
        assert_eq!(bid(&cuts[5], "kalshi:A", 37), 6);
    }
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row("x", 3, vec![full("kalshi:A", &[(37, 8)], &[])]),
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
    let cuts = collect(open(&f, vec![plan("kalshi:A", "x")]));
    for c in cuts {
        assert!(matches!(
            c.book_transitions()[0].decision,
            Decision::Invalidation(Reason::Interval(_))
        ));
    }
}

#[test]
fn scale_mismatch_missing_initialization_and_resource_poison() {
    for change in [
        LevelChange::Delete,
        LevelChange::Set(qty(3)),
        LevelChange::Increase(qty(2)),
        LevelChange::Decrease(qty(1)),
    ] {
        let f = Fixture::new(
            0,
            100,
            1,
            vec![row(
                "x",
                1,
                vec![delta("kalshi:A", 37, change), full("kalshi:A", &[], &[])],
            )],
            |_| {},
        );
        let c = collect(open(&f, vec![plan("kalshi:A", "x")]));
        assert_eq!(
            c[1].book_transitions()[0].decision,
            Decision::Invalidation(Reason::MissingInitialization)
        );
    }
    let f = Fixture::new(
        0,
        100,
        1,
        vec![row(
            "x",
            1,
            vec![full("kalshi:A", &[(31, 7), (27, 2)], &[])],
        )],
        |_| {},
    );
    for price in [false, true] {
        let mut p = plan("kalshi:A", "x");
        if price {
            p.price_scale = scale(3);
        } else {
            p.quantity_scale = scale(1);
        }
        let c = collect(open(&f, vec![p]));
        assert_eq!(
            c[1].book_transitions()[0].decision,
            Decision::Invalidation(Reason::ScaleMismatch)
        );
    }
    let limits = RiskLimits {
        max_levels_per_book: 1,
        ..RiskLimits::default()
    };
    let mut e = RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        vec![plan("kalshi:A", "x")],
        limits,
    )
    .unwrap();
    e.next_cut().unwrap();
    assert!(e.next_cut().unwrap_err().contains("level limit"));
    assert_eq!(e.view(&key("kalshi:A")).unwrap().revision(), 0);
    assert!(e.next_cut().unwrap_err().contains("poisoned"));
    assert!(e.finish().is_err());
    let mut limits = RiskLimits::default();
    limits.read.max_group_records = 1;
    let f = Fixture::new(
        0,
        100,
        1,
        vec![row(
            "x",
            1,
            vec![full("kalshi:A", &[], &[]), full("kalshi:A", &[], &[])],
        )],
        |_| {},
    );
    let mut e = RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        vec![plan("kalshi:A", "x")],
        limits,
    )
    .unwrap();
    while let Ok(Some(_)) = e.next_cut() {}
    assert!(e.finish().is_err());
}

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
fn prefix_and_later_corruption_cannot_finish_and_profile1_cannot_open() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![row("x", 1, vec![full("kalshi:A", &[], &[])])],
        |_| {},
    );
    let mut e = open(&f, vec![plan("kalshi:A", "x")]);
    e.next_cut().unwrap();
    assert!(e.finish().is_err());
    let a = Fixture::new(
        0,
        10,
        1,
        vec![row("x", 1, vec![full("kalshi:A", &[], &[])])],
        |_| {},
    );
    let b = Fixture::new(
        10,
        20,
        2,
        vec![row("x", 11, vec![full("kalshi:A", &[], &[])])],
        |_| {},
    );
    let mut e = RiskEngine::open(
        vec![a.pin.clone(), b.pin.clone()],
        0,
        20,
        LowerBoundPolicy::Clip,
        vec![plan("kalshi:A", "x")],
        RiskLimits::default(),
    )
    .unwrap();
    e.next_cut().unwrap();
    e.next_cut().unwrap();
    std::fs::write(b.pin.directory.join("sources.ndjson.zst"), b"corrupt").unwrap();
    assert!(e.next_cut().is_err());
    assert!(e.finish().is_err());
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../materialize/tests/fixtures/profile1");
    let temp = tempdir::TempDir::new("risk-profile1").unwrap();
    let address = "5b2a4358be376144d898f0c0671b25c23dc2f8de946d16cc049b3b9f92685ea4";
    let directory = temp.path().join(address);
    std::fs::create_dir(&directory).unwrap();
    for name in [
        "receipt.json",
        "manifest.json",
        "events.ndjson.zst",
        "rejects.ndjson.zst",
    ] {
        std::fs::copy(fixture.join(name), directory.join(name)).unwrap();
    }
    let old = PinnedDerivative {
        directory,
        pin: replay_tape::DerivativePin {
            derivative_address: address.into(),
            receipt_sha256: Sha256::from_hex(
                "4ecc6f2b25224e2988cb2879575fa4d8800a5c5d4dc7470dbbaba518d29fe5b8",
            )
            .unwrap(),
        },
    };
    assert!(
        RiskEngine::open(
            vec![old],
            0,
            10,
            LowerBoundPolicy::Clip,
            vec![plan("kalshi:A", "x")],
            RiskLimits::default()
        )
        .err()
        .unwrap()
        .contains("profile 2")
    );
}

#[test]
fn cross_lane_fault_commits_healthy_sibling_and_tokens_stay_distinct() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row(
                "x",
                1,
                vec![
                    full("polymarket:YES", &[(23, 7)], &[]),
                    full("polymarket:NO", &[(64, 13)], &[]),
                ],
            ),
            row("y", 1, vec![full("limitless:A", &[(37, 11)], &[])]),
            reject(
                "x",
                2,
                FaultImpact::RequestedVenueBooks("polymarket".into()),
            ),
            row(
                "y",
                2,
                vec![delta("limitless:A", 37, LevelChange::Decrease(qty(8)))],
            ),
            row(
                "audit",
                3,
                vec![
                    full("limitless:A", &[(37, 999)], &[]),
                    SegmentEvent::AuditAnchor(
                        AuditAnchor::new(
                            id("limitless:A"),
                            ContractOrientation::Outcome,
                            vec![],
                            vec![],
                            BookStateHash::Sha1(
                                Sha1::from_hex("0000000000000000000000000000000000000000").unwrap(),
                            ),
                            None,
                        )
                        .unwrap(),
                    ),
                ],
            ),
        ],
        |_| {},
    );
    let c = collect(open(
        &f,
        vec![
            plan("polymarket:YES", "x"),
            plan("polymarket:NO", "x"),
            plan("limitless:A", "y"),
        ],
    ));
    assert_eq!(c[1].book_transitions().len(), 3);
    assert_eq!(bid(&c[1], "polymarket:YES", 23), 7);
    assert_eq!(bid(&c[1], "polymarket:NO", 64), 13);
    assert_eq!(c[2].book_transitions().len(), 3);
    assert_eq!(bid(&c[2], "limitless:A", 37), 3);
    assert_eq!(
        c[2].book_transitions()
            .iter()
            .filter(|t| t.view.validity() == &Validity::Unusable(Reason::UnsupportedState))
            .count(),
        2
    );
    assert!(c[3].book_transitions().is_empty());
    assert_eq!(c[3].market_events().len(), 1);
    assert_eq!(
        c[3].market_events()[0].disposition,
        Disposition::NotAuthority
    );
}

#[test]
fn epoch_change_without_control_full_recovers_but_delta_cannot_and_duplicate_control_is_inert() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row("x", 1, vec![full("kalshi:A", &[(37, 11)], &[])]),
            Row {
                epoch: "e2",
                ..row("x", 2, vec![full("kalshi:A", &[(37, 3)], &[])])
            },
            Row {
                epoch: "e1",
                continuity: "duplicate",
                ..row(
                    "x",
                    3,
                    vec![SegmentEvent::Control(ControlEvent::ConnectionClosed {
                        epoch: "e1".into(),
                    })],
                )
            },
            Row {
                epoch: "e3",
                ..row(
                    "x",
                    4,
                    vec![delta("kalshi:A", 37, LevelChange::Increase(qty(2)))],
                )
            },
            Row {
                epoch: "e3",
                ..row("x", 5, vec![full("kalshi:A", &[], &[])])
            },
        ],
        |_| {},
    );
    let c = collect(open(&f, vec![plan("kalshi:A", "x")]));
    assert_eq!(bid(&c[2], "kalshi:A", 37), 3);
    assert_eq!(
        c[2].book_transitions()[0].view.dependency().unwrap().epoch,
        "e2"
    );
    assert!(c[3].book_transitions().is_empty());
    assert!(c[4].book_transitions()[0].view.ladder().is_none());
    assert_eq!(
        c[5].book_transitions()[0].view.dependency().unwrap().epoch,
        "e3"
    );
    assert_eq!(c[5].book_transitions()[0].view.as_of(), Some(c[5].origin()));
}

#[test]
fn scale_mismatch_on_delta_both_sides_and_delete_existing() {
    for price in [false, true] {
        let d = SegmentEvent::Book(BookEvent::Delta(
            BookDelta::new(
                id("kalshi:A"),
                ContractOrientation::Outcome,
                Side::Ask,
                ConditionalMarketPrice::from_atoms(
                    if price { 830 } else { 83 },
                    scale(if price { 3 } else { 2 }),
                )
                .unwrap(),
                LevelChange::Set(
                    PositiveQty::new(Qty::from_atoms(7, scale(if price { 0 } else { 1 })).unwrap())
                        .unwrap(),
                ),
                None,
            )
            .unwrap(),
        ));
        let f = Fixture::new(
            0,
            100,
            1,
            vec![
                row("x", 1, vec![full("kalshi:A", &[], &[(83, 2)])]),
                row("x", 2, vec![d]),
            ],
            |_| {},
        );
        let c = collect(open(&f, vec![plan("kalshi:A", "x")]));
        assert_eq!(
            c[2].book_transitions()[0].decision,
            Decision::Invalidation(Reason::ScaleMismatch)
        );
    }
    let ask = |change| {
        SegmentEvent::Book(BookEvent::Delta(
            BookDelta::new(
                id("kalshi:A"),
                ContractOrientation::Outcome,
                Side::Ask,
                ConditionalMarketPrice::from_atoms(83, scale(2)).unwrap(),
                change,
                None,
            )
            .unwrap(),
        ))
    };
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row("x", 1, vec![full("kalshi:A", &[(17, 4)], &[(83, 2)])]),
            row(
                "x",
                2,
                vec![
                    ask(LevelChange::Increase(qty(7))),
                    ask(LevelChange::Decrease(qty(3))),
                ],
            ),
            row(
                "x",
                3,
                vec![
                    ask(LevelChange::Delete),
                    delta("kalshi:A", 17, LevelChange::Delete),
                ],
            ),
        ],
        |_| {},
    );
    let c = collect(open(&f, vec![plan("kalshi:A", "x")]));
    assert_eq!(
        c[2].book_transitions()[0].view.ladder().unwrap().asks()[&83],
        6
    );
    assert_eq!(
        c[2].book_transitions()[0].view.ladder().unwrap().bids()[&17],
        4
    );
    assert_eq!(
        c[3].book_transitions()[0].view.ladder(),
        Some(&Ladder::default())
    );
}

#[test]
fn coverage_faults_are_lane_scoped_and_clean_empty_windows_do_not_resurrect() {
    for clock in [false, true] {
        let rows = if clock {
            vec![
                row("x", 3, vec![full("kalshi:A", &[], &[])]),
                row("y", 4, vec![full("limitless:B", &[(29, 17)], &[])]),
            ]
        } else {
            vec![row("y", 4, vec![full("limitless:B", &[(29, 17)], &[])])]
        };
        let f = Fixture::new(0, 100, 1, rows, |r| {
            r.certified = false;
            if clock {
                r.clock_faults.push(indexer_finalize::ClockFault {
                    window_start_ns: 0,
                    lane: "x".into(),
                    previous_visible_ns: 8,
                    observed_visible_ns: 3,
                });
            } else {
                r.expected_lanes.push("x".into());
                r.completeness = "incomplete".into();
                r.invalid_lanes.push(indexer_finalize::LaneFault {
                    lane: "x".into(),
                    reason: "lane_invalid".into(),
                    detail: Some("seal".into()),
                });
            }
        });
        let c = collect(open(
            &f,
            vec![plan("kalshi:A", "x"), plan("limitless:B", "y")],
        ));
        assert_eq!(c[0].book_transitions().len(), 1);
        assert_eq!(c[0].book_transitions()[0].key, key("kalshi:A"));
        assert_eq!(bid(c.last().unwrap(), "limitless:B", 29), 17);
    }
    let f = Fixture::new(0, 100, 1, vec![], |r| {
        r.expected_lanes.push("x".into());
        r.present_lanes.push("x".into());
        r.inputs.push(indexer_finalize::InputSegment {
            lane: "x".into(),
            data_file: "empty.ndjson".into(),
            segment_index: 0,
            line_count: 0,
            sha256: Sha256::digest(b"").as_hex(),
            first_delivery_index: None,
            last_delivery_index: None,
        });
    });
    let mut e = open(&f, vec![plan("kalshi:A", "x")]);
    assert!(e.next_cut().unwrap().unwrap().book_transitions().is_empty());
    assert_eq!(
        e.view(&key("kalshi:A")).unwrap().validity(),
        &Validity::NotInitialized
    );
    assert!(e.next_cut().unwrap().is_none());
    assert!(e.finish().is_ok());
}

#[test]
fn invalid_configuration_and_retained_state_limits_abort_instead_of_drop() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![row("x", 1, vec![full("kalshi:A", &[], &[])])],
        |_| {},
    );
    let mut wrong = plan("kalshi:A", "x");
    wrong.venue = "polymarket".into();
    for plans in [
        vec![],
        vec![wrong],
        vec![plan("kalshi:A", "x"), plan("kalshi:A", "x")],
    ] {
        assert!(
            RiskEngine::open(
                vec![f.pin.clone()],
                0,
                100,
                LowerBoundPolicy::Clip,
                plans,
                RiskLimits::default()
            )
            .is_err()
        );
    }
    for limits in [
        RiskLimits {
            max_plan_bytes: 1,
            ..RiskLimits::default()
        },
        RiskLimits {
            max_books: 0,
            ..RiskLimits::default()
        },
    ] {
        assert!(
            RiskEngine::open(
                vec![f.pin.clone()],
                0,
                100,
                LowerBoundPolicy::Clip,
                vec![plan("kalshi:A", "x")],
                limits
            )
            .is_err()
        );
    }
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row("x", 1, vec![full("kalshi:A", &[(12, 1)], &[])]),
            row(
                "x",
                2,
                vec![delta("kalshi:A", 13, LevelChange::Set(qty(2)))],
            ),
        ],
        |_| {},
    );
    let mut e = RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        vec![plan("kalshi:A", "x")],
        RiskLimits {
            max_levels_per_book: 1,
            ..RiskLimits::default()
        },
    )
    .unwrap();
    e.next_cut().unwrap();
    e.next_cut().unwrap();
    let before = e.view(&key("kalshi:A")).unwrap();
    assert!(e.next_cut().is_err());
    assert_eq!(e.view(&key("kalshi:A")).unwrap(), before);
    assert!(e.finish().is_err());
}
