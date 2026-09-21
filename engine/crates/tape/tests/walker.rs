use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Cursor;
use std::path::Path;

use canonical_normalizer::{
    Normalization, Normalize, Normalizer, NormalizerDescriptor, NormalizerError, ParseReject,
};
use indexer_finalize::{
    CanonicalOutput, CompressionContract, DecodedIdentity, InputSegment, Receipt, StoredIdentity,
    window_directory,
};
use indexer_types::{ContentHash, Sha256};
use prediction_encoder::{encode_stream, encoder_version};
use replay_domain::{
    BookEvent, ContractOrientation, ControlEvent, FaultImpact, FullBook, InstrumentId, LaneId,
    SEGMENT_SCHEMA_VERSION, SegmentEvent,
};
use replay_materialize::{DerivativeSpec, NormalizationPolicy, build_window, open_pinned};
use replay_tape::*;
use serde_json::json;
use tempdir::TempDir;

struct Row<'a> {
    lane: &'a str,
    time: u64,
    payload: &'a str,
    continuity: &'a str,
    epoch: &'a str,
}
fn row<'a>(lane: &'a str, time: u64, payload: &'static str) -> Row<'a> {
    Row {
        lane,
        time,
        payload,
        continuity: "continuous",
        epoch: "e",
    }
}

fn output(directory: &Path, name: &str, bytes: &[u8]) -> CanonicalOutput {
    let result = encode_stream(
        Cursor::new(bytes),
        fs::File::create(directory.join(name)).unwrap(),
        3,
    )
    .unwrap();
    CanonicalOutput {
        file: name.into(),
        content_encoding: "zstd".into(),
        decoded: DecodedIdentity {
            sha256: result.logical.sha256,
            byte_length: result.logical.byte_length,
            line_count: result.logical.line_count,
        },
        stored: StoredIdentity {
            sha256: result.stored.sha256,
            byte_length: result.stored.byte_length,
        },
        compression: CompressionContract {
            algorithm: "zstd".into(),
            level: 3,
            frame_checksum: true,
            dictionary: None,
            frame_count: 1,
            encoder: encoder_version(),
        },
    }
}

fn canonical(root: &Path, start: u64, end: u64, first: i64, rows: &[Row<'_>], certified: bool) {
    let directory = window_directory(root, start);
    fs::create_dir_all(&directory).unwrap();
    let mut evidence = Vec::new();
    let mut provenance = Vec::new();
    let mut lanes = BTreeMap::<&str, u64>::new();
    for (index, row) in rows.iter().enumerate() {
        let seq = first + index as i64;
        let local = lanes.entry(row.lane).or_default();
        *local += 1;
        let id = format!("{}-{seq}", row.lane);
        let source = Sha256::digest(row.lane.as_bytes()).as_hex();
        let payload: serde_json::Value = serde_json::from_str(row.payload).unwrap_or(json!(null));
        let cursor = if row.lane == "kalshi" && payload.get("seq").is_some() {
            let seq = payload["seq"].as_u64().unwrap();
            json!({"type":"update_range","first":seq,"last":seq,"previous_last":seq-1})
        } else if row.lane == "limitless" && payload["data"].get("version").is_some() {
            json!({"type":"snapshot","last_update_id":payload["data"]["version"]})
        } else {
            json!({"type":"unsequenced","counter":seq})
        };
        let envelope = json!({"envelope_version":2,"delivery_index":first as u64 + *local - 1,"record_id":id,"visible_ns":row.time,"monotonic_ns":row.time,
            "venue":row.lane,"stream":"public_book","connection_epoch":row.epoch,"local_counter":local,"source_cursor":cursor,
            "kind":"venue_frame","raw_payload":row.payload});
        evidence.extend_from_slice(format!("{envelope}\n").as_bytes());
        let tie = rows
            .iter()
            .any(|other| other.time == row.time && other.lane != row.lane)
            .then_some(row.time);
        provenance.extend_from_slice(format!("{}\n", json!({"canonical_seq":seq,"lane_id":row.lane,"source_segment_sha256":source,
            "source_line_number":local,"record_id":id,"content_hash":ContentHash::hash(row.payload.as_bytes()).to_hex(),
            "continuity_verdict":row.continuity,"visible_tie_group":tie})).as_bytes());
    }
    let names: Vec<_> = lanes.keys().map(|s| s.to_string()).collect();
    let inputs = lanes
        .into_iter()
        .map(|(lane, count)| InputSegment {
            lane: lane.into(),
            data_file: "source.ndjson".into(),
            segment_index: 0,
            line_count: count,
            sha256: Sha256::digest(lane.as_bytes()).as_hex(),
            first_delivery_index: Some(first as u64),
            last_delivery_index: Some(first as u64 + count - 1),
        })
        .collect();
    let receipt = Receipt {
        receipt_version: 1,
        window_start_ns: start,
        window_end_ns: end,
        completeness: if certified { "complete" } else { "incomplete" }.into(),
        certified,
        expected_lanes: names
            .iter()
            .cloned()
            .chain((!certified).then(|| "missing-lane".into()))
            .collect(),
        present_lanes: names,
        unexpected_lanes: vec![],
        missing_lanes: if certified {
            vec![]
        } else {
            vec![indexer_finalize::LaneFault {
                lane: "missing-lane".into(),
                reason: "lane_missing".into(),
                detail: None,
            }]
        },
        invalid_lanes: vec![],
        finalization_deadline_seconds: 300,
        deadline_expired: !certified,
        finalized_at_ns: end,
        inputs,
        evidence: output(&directory, "evidence.ndjson.zst", &evidence),
        provenance: output(&directory, "provenance.ndjson.zst", &provenance),
        first_canonical_seq: (!rows.is_empty()).then_some(first),
        last_canonical_seq: (!rows.is_empty()).then_some(first + rows.len() as i64 - 1),
        carried: Default::default(),
        clock_faults: vec![],
        finalizer_version: 1,
    };
    fs::write(
        directory.join("receipt.json"),
        format!("{}\n", serde_json::to_string_pretty(&receipt).unwrap()),
    )
    .unwrap();
}

struct Fake {
    descriptor: NormalizerDescriptor,
}
impl Fake {
    fn new() -> Self {
        Self {
            descriptor: NormalizerDescriptor {
                bundle_sha256: Sha256::digest(b"fake"),
                config_sha256: Sha256::digest(b"config"),
            },
        }
    }
}
impl Normalize for Fake {
    fn descriptor(&self) -> &NormalizerDescriptor {
        &self.descriptor
    }
    fn normalize(
        &mut self,
        source: &indexer_finalize::JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError> {
        let envelope = indexer_types::EnvelopeView::parse(&source.envelope).unwrap();
        let lane = &source.event_address.lane_id;
        let instrument = InstrumentId::new(format!("{lane}:A")).unwrap();
        Ok(match envelope.raw_payload.as_str() {
            "ignore" => Normalization::Ignored {
                reason_code: "neutral".into(),
            },
            "reject" => Normalization::Reject(ParseReject {
                parser_version: 1,
                error_code: "unsupported_state".into(),
                instrument_hint: None,
                impact: FaultImpact::RequestedVenueBooks(lane.clone()),
            }),
            "unknown" => Normalization::Reject(ParseReject {
                parser_version: 1,
                error_code: "unknown_lane".into(),
                instrument_hint: None,
                impact: FaultImpact::UnattributedLane(LaneId::new(lane.clone()).unwrap()),
            }),
            "control" => Normalization::Events(vec![SegmentEvent::Control(
                ControlEvent::ConnectionFailed {
                    epoch: "e".into(),
                    reason: "closed".into(),
                },
            )]),
            _ => Normalization::Events(
                [
                    ContractOrientation::Outcome,
                    ContractOrientation::Complement,
                ]
                .into_iter()
                .map(|orientation| {
                    SegmentEvent::Book(BookEvent::Full(
                        FullBook::new(instrument.clone(), orientation, vec![], vec![], None, None)
                            .unwrap(),
                    ))
                })
                .collect(),
            ),
        })
    }
    fn finish(&mut self) -> Result<(), NormalizerError> {
        Ok(())
    }
}

fn build(
    root: &Path,
    out: &Path,
    start: u64,
    end: u64,
    normalizer: &mut dyn Normalize,
) -> PinnedDerivative {
    let spec = DerivativeSpec {
        normalized_schema_version: SEGMENT_SCHEMA_VERSION,
        normalizer_bundle_sha256: normalizer.descriptor().bundle_sha256,
        normalizer_config_sha256: normalizer.descriptor().config_sha256,
        policy: NormalizationPolicy {
            policy_sha256: Sha256::digest(b"policy"),
            effective_from_ns: 0,
            effective_until_ns: None,
        },
    };
    struct Borrowed<'a>(&'a mut dyn Normalize);
    impl Normalize for Borrowed<'_> {
        fn descriptor(&self) -> &NormalizerDescriptor {
            self.0.descriptor()
        }
        fn normalize(
            &mut self,
            source: &indexer_finalize::JoinedCanonicalRecord,
        ) -> Result<Normalization, NormalizerError> {
            self.0.normalize(source)
        }
        fn finish(&mut self) -> Result<(), NormalizerError> {
            self.0.finish()
        }
    }
    let built = build_window(root, out, start, end, &spec, &mut Borrowed(normalizer)).unwrap();
    PinnedDerivative {
        directory: built.derivative.directory,
        pin: built.derivative.pin,
    }
}

fn request(start: u64, end: u64) -> WalkRequest {
    WalkRequest {
        start_ns: start,
        end_ns: end,
        lower_bound: LowerBoundPolicy::Clip,
        scope: ScopeFilter {
            instruments: ["kalshi:A", "polymarket:A", "limitless:A"]
                .into_iter()
                .map(|s| InstrumentId::new(s).unwrap())
                .collect(),
            lanes: ["kalshi", "polymarket", "limitless"]
                .into_iter()
                .map(|s| LaneId::new(s).unwrap())
                .collect(),
        },
    }
}
fn drain(mut walker: DerivativeWalker) -> (Vec<AtomicGroup>, Vec<WindowStatus>, FinishedWalk) {
    let mut groups = vec![];
    let mut statuses = vec![];
    while let Some(item) = walker.next_item().unwrap() {
        match item {
            WalkItem::Group(g) => groups.push(g),
            WalkItem::WindowStatus(s) => statuses.push(*s),
        }
    }
    assert!(walker.next_item().unwrap().is_none());
    (groups, statuses, walker.finish().unwrap())
}

#[test]
fn multi_window_ties_orientation_ignored_rejected_and_empty_coverage() {
    let canonical_root = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(
        canonical_root.path(),
        0,
        10,
        40,
        &[
            row("polymarket", 2, "control"),
            row("polymarket", 5, "book"),
            row("kalshi", 5, "ignore"),
            row("limitless", 5, "book"),
            row("kalshi", 9, "reject"),
        ],
        true,
    );
    canonical(canonical_root.path(), 10, 20, 45, &[], false);
    canonical(
        canonical_root.path(),
        20,
        30,
        45,
        &[row("kalshi", 21, "book")],
        true,
    );
    let inputs: Vec<_> = [20, 10, 0]
        .into_iter()
        .map(|start| {
            build(
                canonical_root.path(),
                out.path(),
                start,
                start + 10,
                &mut Fake::new(),
            )
        })
        .collect();
    let (groups, statuses, finished) =
        drain(DerivativeWalker::open(inputs, request(0, 30), ReadLimits::default()).unwrap());
    assert_eq!(
        groups
            .iter()
            .map(|g| (
                g.first().canonical_seq(),
                g.last().canonical_seq(),
                g.visible_tie_group()
            ))
            .collect::<Vec<_>>(),
        [
            (40, 40, None),
            (41, 43, Some(5)),
            (44, 44, None),
            (45, 45, None)
        ]
    );
    assert_eq!(groups[1].deliveries().len(), 3);
    assert_eq!(groups[1].book_keys().count(), 4);
    assert_eq!(
        groups[3]
            .book_keys()
            .map(|k| k.orientation)
            .collect::<Vec<_>>(),
        [
            ContractOrientation::Outcome,
            ContractOrientation::Complement
        ]
    );
    assert_eq!(statuses.len(), 3);
    assert!(!statuses[1].metadata().manifest().source_receipt.certified);
    assert_eq!(
        statuses[1].coverage_details(),
        CoverageDetails::ReceiptBoundV2
    );
    assert_eq!(finished.counts().source_deliveries, 6);
    assert_eq!(finished.counts().ignored_sources, 1);
    assert_eq!(finished.counts().rejected_sources, 1);
    assert_eq!(finished.pins().len(), 3);
}

#[test]
fn clipping_is_group_atomic_and_never_imputes_a_bootstrap() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(
        source.path(),
        0,
        10,
        1,
        &[
            row("kalshi", 2, "book"),
            row("polymarket", 5, "book"),
            row("kalshi", 5, "book"),
            row("kalshi", 8, "book"),
        ],
        true,
    );
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    for (start, end, policy, expected, effective) in [
        (5, 8, LowerBoundPolicy::Clip, vec![5], 5),
        (6, 9, LowerBoundPolicy::Clip, vec![8], 6),
        (5, 8, LowerBoundPolicy::ExpandToWindowStart, vec![2, 5], 0),
        (0, 5, LowerBoundPolicy::RequireWindowBoundary, vec![2], 0),
    ] {
        let mut req = request(start, end);
        req.lower_bound = policy;
        let (groups, _, done) =
            drain(DerivativeWalker::open(vec![input.clone()], req, ReadLimits::default()).unwrap());
        assert_eq!(
            groups
                .iter()
                .map(AtomicGroup::visible_ns)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(done.effective_interval(), (effective, end));
        assert_eq!(done.counts().source_deliveries, 4);
    }
    let mut req = request(1, 9);
    req.lower_bound = LowerBoundPolicy::RequireWindowBoundary;
    assert!(DerivativeWalker::open(vec![input], req, ReadLimits::default()).is_err());
}

#[test]
fn scope_keeps_faults_controls_and_original_coordinates_without_collapsing_orientation() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(
        source.path(),
        0,
        10,
        1,
        &[
            row("kalshi", 1, "control"),
            row("polymarket", 2, "book"),
            row("kalshi", 2, "book"),
            Row {
                continuity: "gap_proven",
                ..row("limitless", 3, "book")
            },
            row("kalshi", 4, "reject"),
            row("polymarket", 5, "unknown"),
        ],
        true,
    );
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    let mut req = request(0, 10);
    req.scope.instruments = [InstrumentId::new("kalshi:A").unwrap()]
        .into_iter()
        .collect();
    let (groups, _, done) =
        drain(DerivativeWalker::open(vec![input], req, ReadLimits::default()).unwrap());
    assert_eq!(groups.len(), 5);
    assert_eq!(groups[1].first().canonical_seq(), 2); // filtered PM source still bounds tie
    assert_eq!(groups[1].last().canonical_seq(), 3);
    assert_eq!(
        groups[1].deliveries()[0].header().address().canonical_seq(),
        3
    );
    assert_eq!(groups[2].deliveries()[0].records().len(), 0);
    assert_eq!(
        groups[2].deliveries()[0].header().provenance().continuity(),
        replay_domain::ContinuityVerdict::GapProven
    );
    assert_eq!(done.counts().scope_excluded_sources, 1);
    assert_eq!(done.counts().excluded_events, 4);
}

#[test]
fn missing_overlapping_duplicate_and_nonadjacent_windows_fail_closed() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    for (start, end, seq) in [(0, 10, 1), (10, 20, 3), (20, 30, 4)] {
        canonical(
            source.path(),
            start,
            end,
            seq,
            &[row("kalshi", start + 1, "book")],
            true,
        );
    }
    let a = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    let b = build(source.path(), out.path(), 10, 20, &mut Fake::new());
    let c = build(source.path(), out.path(), 20, 30, &mut Fake::new());
    let other = TempDir::new("overlap-source").unwrap();
    canonical(other.path(), 5, 15, 7, &[row("kalshi", 6, "book")], true);
    let overlap = build(other.path(), out.path(), 5, 15, &mut Fake::new());
    for inputs in [
        vec![a.clone(), a.clone()],
        vec![a.clone(), c],
        vec![a.clone(), overlap],
    ] {
        assert!(DerivativeWalker::open(inputs, request(0, 30), ReadLimits::default()).is_err());
    }
    let mut walker =
        DerivativeWalker::open(vec![a, b], request(0, 20), ReadLimits::default()).unwrap();
    assert!(walker.next_item().is_ok());
    assert!(walker.next_item().is_ok());
    assert!(walker.next_item().is_ok());
    assert!(walker.next_item().unwrap_err().contains("not adjacent"));
    assert!(walker.next_item().unwrap_err().contains("poisoned"));
    assert!(walker.finish().is_err());
}

#[test]
fn pinned_open_survives_source_replacement_and_requires_explicit_eof() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(source.path(), 0, 10, 1, &[row("kalshi", 1, "book")], true);
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    assert!(
        open_pinned(&input, &ReadLimits::default())
            .unwrap()
            .finish()
            .is_err()
    );
    let mut reader = open_pinned(&input, &ReadLimits::default()).unwrap();
    fs::write(input.directory.join("events.ndjson.zst"), b"changed").unwrap();
    fs::remove_file(input.directory.join("receipt.json")).unwrap();
    let delivery = reader.next_delivery().unwrap().unwrap();
    assert_eq!(delivery.records().len(), 2);
    assert!(reader.next_delivery().unwrap().is_none());
    assert!(reader.next_delivery().unwrap().is_none());
    assert_eq!(reader.finish().unwrap().metadata().pin(), &input.pin);
    assert!(open_pinned(&input, &ReadLimits::default()).is_err());
}

#[test]
fn lazy_open_checks_replacement_and_poisoning_even_for_empty_scope() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(source.path(), 0, 10, 1, &[row("kalshi", 9, "book")], true);
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    let mut req = request(0, 1);
    req.scope.instruments.clear();
    req.scope.lanes.clear();
    let mut walker =
        DerivativeWalker::open(vec![input.clone()], req, ReadLimits::default()).unwrap();
    fs::write(input.directory.join("rejects.ndjson.zst"), b"corrupt").unwrap();
    assert!(walker.next_item().is_err());
    assert!(walker.next_item().unwrap_err().contains("poisoned"));
    assert!(walker.finish().is_err());
}

#[test]
fn limits_apply_before_filtering_and_same_lane_equal_times_do_not_buffer_a_run() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(
        source.path(),
        0,
        10,
        1,
        &[
            row("kalshi", 1, "book"),
            row("kalshi", 1, "book"),
            row("polymarket", 3, "book"),
            row("kalshi", 3, "book"),
        ],
        true,
    );
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    let mut req = request(0, 1);
    req.scope.instruments.clear();
    for limits in [
        ReadLimits {
            max_line_bytes: 5,
            ..ReadLimits::default()
        },
        ReadLimits {
            max_group_records: 3,
            ..ReadLimits::default()
        },
        ReadLimits {
            max_snapshot_bytes: 10,
            ..ReadLimits::default()
        },
    ] {
        let mut walker = DerivativeWalker::open(vec![input.clone()], req.clone(), limits).unwrap();
        assert!(walker.next_item().is_err());
    }
    let (groups, _, _) =
        drain(DerivativeWalker::open(vec![input], request(0, 10), ReadLimits::default()).unwrap());
    assert_eq!(
        groups
            .iter()
            .map(|g| g.deliveries().len())
            .collect::<Vec<_>>(),
        [1, 1, 2]
    );
}

#[test]
fn all_empty_windows_are_visible_and_finish_normally() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    let inputs: Vec<_> = [0, 10]
        .into_iter()
        .map(|start| {
            canonical(source.path(), start, start + 10, 1, &[], false);
            build(
                source.path(),
                out.path(),
                start,
                start + 10,
                &mut Fake::new(),
            )
        })
        .collect();
    let (groups, statuses, done) =
        drain(DerivativeWalker::open(inputs, request(1, 19), ReadLimits::default()).unwrap());
    assert!(groups.is_empty());
    assert_eq!(statuses.len(), 2);
    assert_eq!(done.counts().source_deliveries, 0);
}

#[test]
fn actual_three_venue_derivatives_keep_native_books_and_exact_pins() {
    let fixtures = [
        (
            "kalshi",
            include_str!("../../kalshi/tests/fixtures/orderbook_snapshot.json"),
        ),
        (
            "polymarket",
            include_str!("../../polymarket/tests/fixtures/book.json"),
        ),
        (
            "limitless",
            include_str!("../../limitless/tests/fixtures/orderbook_update_live_2026_09_12.json"),
        ),
    ];
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    let mut normalizers: Vec<Box<dyn Normalize>> = vec![
        Box::new(Normalizer::new(kalshi_normalizer::Kalshi::default()).unwrap()),
        Box::new(Normalizer::new(polymarket_normalizer::Polymarket::default()).unwrap()),
        Box::new(Normalizer::new(limitless_normalizer::Limitless::default()).unwrap()),
    ];
    let mut inputs = vec![];
    let mut instruments = BTreeSet::new();
    let mut expected = vec![];
    for (index, ((lane, payload), normalizer)) in
        fixtures.into_iter().zip(normalizers.iter_mut()).enumerate()
    {
        let start = index as u64 * 10;
        canonical(
            source.path(),
            start,
            start + 10,
            index as i64 + 1,
            &[Row {
                lane,
                time: start + 1,
                payload,
                continuity: "continuous",
                epoch: "e",
            }],
            true,
        );
        let input = build(
            source.path(),
            out.path(),
            start,
            start + 10,
            normalizer.as_mut(),
        );
        let mut reader = open_pinned(&input, &ReadLimits::default()).unwrap();
        let delivery = reader.next_delivery().unwrap().unwrap();
        assert!(
            delivery.disposition().is_none(),
            "{lane}: {:?}",
            delivery.disposition()
        );
        for record in delivery.records() {
            if let SegmentEvent::Book(BookEvent::Full(book)) = record.event() {
                instruments.insert(book.instrument().clone());
            }
        }
        expected.extend(delivery.records().iter().cloned());
        assert!(reader.next_delivery().unwrap().is_none());
        reader.finish().unwrap();
        inputs.push(input);
    }
    let pins: Vec<_> = inputs.iter().map(|i| i.pin.clone()).collect();
    let mut req = request(0, 30);
    req.scope.instruments = instruments;
    let (groups, _, done) =
        drain(DerivativeWalker::open(inputs, req, ReadLimits::default()).unwrap());
    let actual: Vec<_> = groups
        .iter()
        .flat_map(|g| g.deliveries())
        .flat_map(|d| d.records())
        .cloned()
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(done.pins(), pins);
    assert_eq!(groups[0].book_keys().count(), 2);
    let books: Vec<_> = groups[0].deliveries()[0]
        .records()
        .iter()
        .map(|r| match r.event() {
            SegmentEvent::Book(BookEvent::Full(b)) => b,
            _ => panic!(),
        })
        .collect();
    assert_eq!(books[0].instrument(), books[1].instrument());
    assert_ne!(books[0].orientation(), books[1].orientation());
    assert!(books.iter().all(|b| b.asks().is_empty()));
    assert_ne!(books[0].bids(), books[1].bids());
}

// Corruption fixtures rewrite every outer hash, so semantic failures cannot
// accidentally pass merely because the compressed digest no longer matches.
fn rewrite(input: &mut PinnedDerivative, file: &str, change: impl FnOnce(&mut String)) {
    use std::io::Read;
    let mut receipt: replay_materialize::DerivativeReceipt =
        serde_json::from_slice(&fs::read(input.directory.join("receipt.json")).unwrap()).unwrap();
    let mut manifest: replay_materialize::DerivativeManifest =
        serde_json::from_slice(&fs::read(input.directory.join("manifest.json")).unwrap()).unwrap();
    let object = match file {
        "events.ndjson.zst" => &manifest.events,
        "rejects.ndjson.zst" => &manifest.rejects,
        "sources.ndjson.zst" => manifest.sources.as_ref().unwrap(),
        _ => panic!("unknown output"),
    };
    let logical = prediction_encoder::LogicalIdentity {
        sha256: object.logical.sha256.as_hex(),
        byte_length: object.logical.byte_length,
        line_count: object.logical.line_count,
    };
    let stored = prediction_encoder::StoredIdentity {
        sha256: object.stored.sha256.as_hex(),
        byte_length: object.stored.byte_length,
    };
    let mut decoder = prediction_encoder::StreamingDecoder::new(
        fs::File::open(input.directory.join(file)).unwrap(),
        &logical,
        Some(&stored),
        Some(logical.byte_length),
    )
    .unwrap();
    let mut text = String::new();
    decoder.read_to_string(&mut text).unwrap();
    decoder.finish().unwrap();
    change(&mut text);
    let result = encode_stream(
        Cursor::new(text),
        fs::File::create(input.directory.join(file)).unwrap(),
        3,
    )
    .unwrap();
    let object = match file {
        "events.ndjson.zst" => &mut manifest.events,
        "rejects.ndjson.zst" => &mut manifest.rejects,
        "sources.ndjson.zst" => manifest.sources.as_mut().unwrap(),
        _ => panic!("unknown output"),
    };
    object.logical = replay_materialize::LogicalIdentity {
        sha256: Sha256::from_hex(&result.logical.sha256).unwrap(),
        byte_length: result.logical.byte_length,
        line_count: result.logical.line_count,
    };
    object.stored = replay_materialize::StoredIdentity {
        sha256: Sha256::from_hex(&result.stored.sha256).unwrap(),
        byte_length: result.stored.byte_length,
    };
    receipt.events = manifest.events.clone();
    receipt.rejects = manifest.rejects.clone();
    receipt.sources = manifest.sources.clone();
    let bytes = format!("{}\n", serde_json::to_string(&manifest).unwrap()).into_bytes();
    receipt.manifest.sha256 = Sha256::digest(&bytes);
    receipt.manifest.byte_length = bytes.len() as u64;
    fs::write(input.directory.join("manifest.json"), bytes).unwrap();
    let bytes = format!("{}\n", serde_json::to_string(&receipt).unwrap()).into_bytes();
    input.pin.receipt_sha256 = Sha256::digest(&bytes);
    fs::write(input.directory.join("receipt.json"), bytes).unwrap();
}

#[test]
fn rehashed_semantic_corruption_fails_before_window_status() {
    for mutation in [
        "child_header",
        "child_gap",
        "source_gap",
        "time_outside",
        "tie_none",
        "tie_wrong",
        "version",
        "continuity",
        "unknown_field",
    ] {
        let source = TempDir::new("source").unwrap();
        let out = TempDir::new("out").unwrap();
        canonical(
            source.path(),
            0,
            10,
            1,
            &[row("polymarket", 5, "book"), row("kalshi", 5, "book")],
            true,
        );
        let mut input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
        rewrite(&mut input, "events.ndjson.zst", |text| {
            *text = match mutation {
                "child_header" => text.replacen(
                    "\"record_id\":\"polymarket-1\"",
                    "\"record_id\":\"changed\"",
                    1,
                ),
                "child_gap" => text.replacen("\"event_index\":1", "\"event_index\":2", 1),
                "source_gap" => text.replace("\"canonical_seq\":2", "\"canonical_seq\":3"),
                "time_outside" => text.replace(
                    "\"order_ns\":5,\"visible_ns\":5",
                    "\"order_ns\":10,\"visible_ns\":10",
                ),
                "tie_none" => {
                    text.replacen("\"visible_tie_group\":5", "\"visible_tie_group\":null", 2)
                }
                "tie_wrong" => text.replace("\"visible_tie_group\":5", "\"visible_tie_group\":4"),
                "version" => text.replace("\"schema_version\":3", "\"schema_version\":99"),
                "continuity" => text.replace(
                    "\"continuity\":\"continuous\"",
                    "\"continuity\":\"guessed\"",
                ),
                "unknown_field" => text.replacen(
                    "{\"schema_version\":3",
                    "{\"unknown\":true,\"schema_version\":3",
                    1,
                ),
                _ => unreachable!(),
            };
        });
        let mut walker =
            DerivativeWalker::open(vec![input], request(0, 1), ReadLimits::default()).unwrap();
        assert!(walker.next_item().is_err(), "{mutation}");
        assert!(walker.next_item().unwrap_err().contains("poisoned"));
        assert!(walker.finish().is_err());
    }
}

#[test]
fn codec_and_pin_corruption_never_becomes_eof() {
    for mutation in [
        "truncated",
        "trailing",
        "concatenated",
        "checksum",
        "missing_receipt",
        "bad_pin_address",
        "bad_pin_hash",
    ] {
        let source = TempDir::new("source").unwrap();
        let out = TempDir::new("out").unwrap();
        canonical(source.path(), 0, 10, 1, &[row("kalshi", 1, "book")], true);
        let mut input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
        let path = input.directory.join("events.ndjson.zst");
        let mut bytes = fs::read(&path).unwrap();
        match mutation {
            "truncated" => {
                bytes.pop();
            }
            "trailing" => bytes.push(0),
            "concatenated" => bytes.extend(bytes.clone()),
            "checksum" => {
                let n = bytes.len();
                bytes[n - 1] ^= 1;
            }
            "missing_receipt" => fs::remove_file(input.directory.join("receipt.json")).unwrap(),
            "bad_pin_address" => input.pin.derivative_address = "f".repeat(64),
            "bad_pin_hash" => input.pin.receipt_sha256 = Sha256::digest(b"bad"),
            _ => unreachable!(),
        }
        fs::write(path, bytes).unwrap();
        assert!(
            open_pinned(&input, &ReadLimits::default()).is_err(),
            "{mutation}"
        );
    }
}

#[test]
fn ignored_sources_cannot_hide_sequence_gaps_or_nonzero_indexes() {
    for mutation in ["gap", "index"] {
        let source = TempDir::new("source").unwrap();
        let out = TempDir::new("out").unwrap();
        canonical(
            source.path(),
            0,
            10,
            1,
            &[row("kalshi", 1, "ignore"), row("kalshi", 2, "ignore")],
            true,
        );
        let mut input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
        rewrite(&mut input, "rejects.ndjson.zst", |text| {
            *text = if mutation == "gap" {
                text.replace("\"canonical_seq\":2", "\"canonical_seq\":3")
            } else {
                text.replacen("\"event_index\":0", "\"event_index\":1", 1)
            };
        });
        assert!(
            open_pinned(&input, &ReadLimits::default()).is_err(),
            "{mutation}"
        );
    }
}

#[test]
fn evaluation_cadence_does_not_skip_delivery_consumption() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(
        source.path(),
        0,
        10,
        1,
        &[
            row("kalshi", 1, "book"),
            Row {
                continuity: "duplicate",
                ..row("kalshi", 2, "book")
            },
            row("kalshi", 3, "book"),
        ],
        true,
    );
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    let mut applications = vec![];
    let mut evaluations = vec![];
    for cadence in [1, 2] {
        let mut walker =
            DerivativeWalker::open(vec![input.clone()], request(0, 10), ReadLimits::default())
                .unwrap();
        let mut applied = vec![];
        let mut evaluated = vec![];
        while let Some(item) = walker.next_item().unwrap() {
            if let WalkItem::Group(group) = item {
                // Inert sink: no book algorithm. Receives the complete pair first.
                assert_eq!(group.deliveries()[0].records().len(), 2);
                applied.push(group.first().canonical_seq());
                if applied.len() % cadence == 0 {
                    evaluated.push(group.last().canonical_seq());
                }
            }
        }
        walker.finish().unwrap();
        applications.push(applied);
        evaluations.push(evaluated);
    }
    assert_eq!(applications, [vec![1, 2, 3], vec![1, 2, 3]]);
    assert_eq!(evaluations, [vec![1, 2, 3], vec![2]]);
}

#[test]
fn distinct_bundle_versions_keep_old_pins_readable_and_unknown_profiles_fail() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(source.path(), 0, 10, 1, &[row("kalshi", 1, "book")], true);
    let old = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    let mut newer = Fake::new();
    newer.descriptor.bundle_sha256 = Sha256::digest(b"new implementation");
    let new = build(source.path(), out.path(), 0, 10, &mut newer);
    assert_ne!(old.pin, new.pin);
    assert!(open_pinned(&old, &ReadLimits::default()).is_ok());
    assert!(open_pinned(&new, &ReadLimits::default()).is_ok());
    for field in [
        "receipt_version",
        "materializer_version",
        "normalized_schema_version",
    ] {
        let path = old.directory.join("receipt.json");
        let original = fs::read(&path).unwrap();
        let text = String::from_utf8(original.clone()).unwrap();
        let value = if field == "normalized_schema_version" {
            3
        } else {
            2
        };
        let bytes = text
            .replacen(
                &format!("\"{field}\":{value}"),
                &format!("\"{field}\":99"),
                1,
            )
            .into_bytes();
        let mut input = old.clone();
        input.pin.receipt_sha256 = Sha256::digest(&bytes);
        fs::write(&path, bytes).unwrap();
        assert!(open_pinned(&input, &ReadLimits::default()).is_err());
        fs::write(path, original).unwrap();
    }
    assert!(open_pinned(&old, &ReadLimits::default()).is_ok());
}

#[test]
fn metadata_scope_limits_and_prefix_finish_are_explicit() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(source.path(), 0, 10, 1, &[row("kalshi", 1, "book")], true);
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    for limits in [
        ReadLimits {
            max_metadata_bytes: 100,
            ..ReadLimits::default()
        },
        ReadLimits {
            max_scope_entries: 1,
            ..ReadLimits::default()
        },
    ] {
        assert!(DerivativeWalker::open(vec![input.clone()], request(0, 10), limits).is_err());
    }
    assert!(
        DerivativeWalker::open(
            vec![input.clone(), input.clone()],
            request(0, 10),
            ReadLimits {
                max_windows: 1,
                ..ReadLimits::default()
            }
        )
        .is_err()
    );
    let mut walker =
        DerivativeWalker::open(vec![input], request(0, 10), ReadLimits::default()).unwrap();
    walker.next_item().unwrap();
    walker.next_item().unwrap();
    assert!(walker.finish().is_err()); // last group is not the explicit EOF pull
}

#[test]
fn repeated_source_delivery_index_cannot_hide_behind_dense_canonical_sequence() {
    let source = TempDir::new("source").unwrap();
    let out = TempDir::new("out").unwrap();
    canonical(
        source.path(),
        0,
        10,
        1,
        &[row("kalshi", 1, "book"), row("kalshi", 2, "book")],
        true,
    );
    let mut input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    rewrite(&mut input, "events.ndjson.zst", |text| {
        *text = text.replace("\"delivery_index\":2", "\"delivery_index\":1");
    });
    assert!(open_pinned(&input, &ReadLimits::default()).is_err());
}

#[test]
#[cfg(target_os = "linux")]
fn bounded_memory_subprocess() {
    let Ok(path) = std::env::var("REPLAY_WALK_TEST_INPUT") else {
        return;
    };
    let receipt = fs::read(Path::new(&path).join("receipt.json")).unwrap();
    let pin = DerivativePin {
        derivative_address: Path::new(&path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .into(),
        receipt_sha256: Sha256::digest(&receipt),
    };
    let metadata: replay_materialize::DerivativeReceipt = serde_json::from_slice(&receipt).unwrap();
    let manifest: replay_materialize::DerivativeManifest =
        serde_json::from_slice(&fs::read(Path::new(&path).join(&metadata.manifest.file)).unwrap())
            .unwrap();
    let mut walker = DerivativeWalker::open(
        vec![PinnedDerivative {
            directory: path.into(),
            pin,
        }],
        request(0, manifest.effective_end_ns),
        ReadLimits {
            max_group_records: 2,
            ..ReadLimits::default()
        },
    )
    .unwrap();
    let mut groups = 0;
    while let Some(item) = walker.next_item().unwrap() {
        if let WalkItem::Group(group) = item {
            assert!(group.deliveries().len() <= 1);
            groups += 1;
        }
    }
    assert_eq!(groups, manifest.counts.input_records);
    walker.finish().unwrap();
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let peak = status
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap();
    println!("WALK_PEAK_KIB={peak}");
}

#[test]
#[cfg(target_os = "linux")]
fn memory_is_bounded_when_tape_grows_twentyfold() {
    use std::process::Command;
    for payload in ["book", "reject", "ignore"] {
        let mut peaks = vec![];
        for count in [400, 8000] {
            let source = TempDir::new("memory-source").unwrap();
            let out = TempDir::new("memory-out").unwrap();
            // All deliveries have the same lane/time; they are NOT one atomic tie.
            let rows: Vec<_> = (0..count).map(|_| row("kalshi", 1, payload)).collect();
            canonical(source.path(), 0, 10, 1, &rows, true);
            let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "bounded_memory_subprocess", "--nocapture"])
                .env("REPLAY_WALK_TEST_INPUT", input.directory)
                .env("MALLOC_ARENA_MAX", "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            let peak: u64 = stdout
                .lines()
                .find_map(|l| l.strip_prefix("WALK_PEAK_KIB="))
                .unwrap()
                .parse()
                .unwrap();
            peaks.push(peak);
        }
        println!("walker peak RSS KiB, {payload} 400/8000 deliveries: {peaks:?}");
        // Whole-window Vec<SegmentRecord> retention costs substantially more than
        // this allowance. Fixed codec windows and allocator noise are permitted.
        assert!(
            peaks[1] <= peaks[0] + 8 * 1024,
            "peak grew with tape: {peaks:?}"
        );
    }
}

#[test]
fn epochs_without_open_controls_survive_dispositions_duplicates_filter_and_clipping() {
    let source = TempDir::new("epochs").unwrap();
    let out = TempDir::new("epochs-out").unwrap();
    canonical(
        source.path(),
        0,
        10,
        1,
        &[
            row("kalshi", 1, "book"),
            Row {
                epoch: "second-connection",
                ..row("kalshi", 2, "book")
            },
            Row {
                epoch: "second-connection",
                ..row("kalshi", 3, "reject")
            },
            Row {
                epoch: "third-connection",
                ..row("kalshi", 4, "ignore")
            },
            Row {
                epoch: "third-connection",
                continuity: "duplicate",
                ..row("kalshi", 5, "book")
            },
            Row {
                epoch: "fourth-connection",
                ..row("kalshi", 6, "book")
            },
        ],
        true,
    );
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    let (groups, _, done) = drain(
        DerivativeWalker::open(vec![input.clone()], request(2, 6), ReadLimits::default()).unwrap(),
    );
    assert_eq!(
        groups
            .iter()
            .map(|g| (
                g.first().canonical_seq(),
                g.deliveries()[0].connection_epoch().unwrap()
            ))
            .collect::<Vec<_>>(),
        [
            (2, "second-connection"),
            (3, "second-connection"),
            (4, "third-connection"),
            (5, "third-connection")
        ]
    );
    assert_eq!(
        groups
            .iter()
            .map(|g| g.deliveries()[0].records().len())
            .collect::<Vec<_>>(),
        [2, 1, 0, 2]
    );
    assert_eq!(
        groups[3].deliveries()[0].header().provenance().continuity(),
        replay_domain::ContinuityVerdict::Duplicate
    );
    assert!(done.supports_source_evidence());
    let mut req = request(2, 6);
    req.scope.instruments.clear();
    let (filtered, _, _) =
        drain(DerivativeWalker::open(vec![input], req, ReadLimits::default()).unwrap());
    assert_eq!(filtered.len(), 1); // relevant-lane ignore survives, venue fault does not
    assert_eq!(
        filtered[0].deliveries()[0].connection_epoch(),
        Some("third-connection")
    );
}

fn amend_receipt(root: &Path, change: impl FnOnce(&mut Receipt)) {
    let path = window_directory(root, 0).join("receipt.json");
    let mut receipt: Receipt = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    change(&mut receipt);
    fs::write(
        path,
        format!("{}\n", serde_json::to_string_pretty(&receipt).unwrap()),
    )
    .unwrap();
}

#[test]
fn upstream_lane_and_clock_facts_precede_groups_even_when_empty_or_unrelated() {
    for empty in [false, true] {
        let source = TempDir::new("coverage").unwrap();
        let out = TempDir::new("coverage-out").unwrap();
        let rows = if empty {
            vec![]
        } else {
            vec![row("kalshi", 3, "book")]
        };
        canonical(source.path(), 0, 10, 1, &rows, true);
        amend_receipt(source.path(), |r| {
            r.certified = false;
            r.completeness = "incomplete".into();
            r.expected_lanes
                .extend(["book-feed".into(), "audit-feed".into(), "quiet".into()]);
            r.present_lanes.push("quiet".into());
            r.inputs.push(InputSegment {
                lane: "quiet".into(),
                data_file: "empty.ndjson".into(),
                segment_index: 0,
                line_count: 0,
                sha256: Sha256::digest(b"").as_hex(),
                first_delivery_index: None,
                last_delivery_index: None,
            });
            r.missing_lanes = ["book-feed", "audit-feed"]
                .map(|lane| indexer_finalize::LaneFault {
                    lane: lane.into(),
                    reason: "lane_missing".into(),
                    detail: None,
                })
                .to_vec();
            r.unexpected_lanes.push("unrelated".into());
            r.invalid_lanes.push(indexer_finalize::LaneFault {
                lane: "unrelated".into(),
                reason: "lane_invalid".into(),
                detail: Some("seal digest mismatch".into()),
            });
            r.deadline_expired = true;
            if !empty {
                r.clock_faults.push(indexer_finalize::ClockFault {
                    window_start_ns: 0,
                    lane: "kalshi".into(),
                    previous_visible_ns: 8,
                    observed_visible_ns: 3,
                });
            }
        });
        let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
        let mut walker =
            DerivativeWalker::open(vec![input], request(1, 9), ReadLimits::default()).unwrap();
        let Some(WalkItem::WindowStatus(status)) = walker.next_item().unwrap() else {
            panic!("fault status must precede groups")
        };
        let coverage = status.coverage().unwrap();
        assert_eq!(
            coverage.lane(&LaneId::new("quiet").unwrap()).state,
            LaneState::Present { records: 0 }
        );
        assert_eq!(
            coverage.lane(&LaneId::new("disabled").unwrap()),
            LaneCoverage {
                expected: false,
                state: LaneState::NotExpected
            }
        );
        for lane in ["book-feed", "audit-feed"] {
            assert_eq!(
                coverage.lane(&LaneId::new(lane).unwrap()),
                LaneCoverage {
                    expected: true,
                    state: LaneState::Missing
                }
            );
        }
        assert_eq!(
            coverage.lane(&LaneId::new("unrelated").unwrap()),
            LaneCoverage {
                expected: false,
                state: LaneState::Invalid {
                    detail: Some("seal digest mismatch".into())
                }
            }
        );
        assert_eq!(coverage.faults().len(), if empty { 3 } else { 4 });
        assert!(
            coverage
                .faults()
                .iter()
                .all(|f| (f.start_ns, f.end_ns) == (0, 10))
        );
        if !empty {
            assert!(coverage.faults().iter().any(|f| f.reason
                == SourceFaultReason::VisibleClockRegression {
                    previous_visible_ns: 8,
                    observed_visible_ns: 3
                }));
        }
        assert_eq!(coverage.deadline_seconds(), 300);
        assert!(coverage.deadline_expired());
        let (groups, statuses, done) = drain(walker);
        assert_eq!(groups.len(), usize::from(!empty));
        assert!(statuses.is_empty());
        assert!(done.supports_source_evidence());
    }
}

#[test]
fn rehashed_source_metadata_fails_before_status_and_poisons() {
    for mutation in [
        "epoch",
        "empty_epoch",
        "header",
        "missing",
        "extra",
        "unknown",
    ] {
        let source = TempDir::new("source-corrupt").unwrap();
        let out = TempDir::new("source-corrupt-out").unwrap();
        canonical(
            source.path(),
            0,
            10,
            1,
            &[
                row("kalshi", 1, "book"),
                row("kalshi", 2, "ignore"),
                row("kalshi", 3, "reject"),
            ],
            true,
        );
        let mut input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
        rewrite(&mut input, "sources.ndjson.zst", |text| {
            *text = match mutation {
                "epoch" => text.replace(
                    "\"connection_epoch\":\"e\"",
                    "\"connection_epoch\":\"wrong\"",
                ),
                "empty_epoch" => {
                    text.replace("\"connection_epoch\":\"e\"", "\"connection_epoch\":\"\"")
                }
                "header" => {
                    text.replacen("\"record_id\":\"kalshi-1\"", "\"record_id\":\"wrong\"", 1)
                }
                "missing" => text.split_once('\n').unwrap().1.to_owned(),
                "extra" => format!("{text}{}\n", text.lines().last().unwrap()),
                "unknown" => text.replace(
                    "\"source_version\":1",
                    "\"source_version\":1,\"extra\":true",
                ),
                _ => unreachable!(),
            };
        });
        let opened = DerivativeWalker::open(vec![input], request(8, 9), ReadLimits::default());
        if let Ok(mut walker) = opened {
            assert!(walker.next_item().is_err(), "{mutation}");
            assert_eq!(
                walker.next_item().unwrap_err(),
                "derivative walker is poisoned"
            );
            assert!(walker.finish().is_err());
        }
    }
}

#[test]
fn mixed_profiles_preserve_pins_but_cannot_mint_complete_source_capability() {
    let source = TempDir::new("mixed-source").unwrap();
    let out = TempDir::new("mixed-out").unwrap();
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../materialize/tests/fixtures/profile1");
    let address = "5b2a4358be376144d898f0c0671b25c23dc2f8de946d16cc049b3b9f92685ea4";
    let directory = out.path().join(address);
    fs::create_dir(&directory).unwrap();
    for name in [
        "receipt.json",
        "manifest.json",
        "events.ndjson.zst",
        "rejects.ndjson.zst",
    ] {
        fs::copy(fixture.join(name), directory.join(name)).unwrap();
    }
    let old = PinnedDerivative {
        directory,
        pin: DerivativePin {
            derivative_address: address.into(),
            receipt_sha256: Sha256::from_hex(
                "4ecc6f2b25224e2988cb2879575fa4d8800a5c5d4dc7470dbbaba518d29fe5b8",
            )
            .unwrap(),
        },
    };
    canonical(source.path(), 10, 20, 4, &[row("kalshi", 12, "book")], true);
    let new = build(source.path(), out.path(), 10, 20, &mut Fake::new());
    let (groups, statuses, done) = drain(
        DerivativeWalker::open(
            vec![new.clone(), old.clone()],
            request(0, 20),
            ReadLimits::default(),
        )
        .unwrap(),
    );
    assert_eq!(
        statuses[0].coverage_details(),
        CoverageDetails::NotRecordedInDerivativeV1
    );
    assert!(statuses[0].coverage().is_none());
    assert_eq!(groups[0].deliveries()[0].connection_epoch(), None);
    assert_eq!(
        groups.last().unwrap().deliveries()[0].connection_epoch(),
        Some("e")
    );
    assert!(!done.supports_source_evidence());
    assert_eq!(done.pins(), [old.pin, new.pin]);
}

#[test]
fn source_sidecar_codec_corruption_cannot_hide_in_clipped_tail() {
    for mutation in ["truncated", "trailing", "concatenated", "checksum"] {
        let source = TempDir::new("codec-source").unwrap();
        let out = TempDir::new("codec-out").unwrap();
        canonical(source.path(), 0, 10, 1, &[row("kalshi", 8, "book")], true);
        let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
        let path = input.directory.join("sources.ndjson.zst");
        let mut bytes = fs::read(&path).unwrap();
        match mutation {
            "truncated" => {
                bytes.pop();
            }
            "trailing" => bytes.push(0),
            "concatenated" => bytes.extend(bytes.clone()),
            "checksum" => {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }
            _ => unreachable!(),
        }
        fs::write(path, bytes).unwrap();
        let mut walker =
            DerivativeWalker::open(vec![input], request(0, 1), ReadLimits::default()).unwrap();
        assert!(walker.next_item().is_err(), "{mutation}");
        assert_eq!(
            walker.next_item().unwrap_err(),
            "derivative walker is poisoned"
        );
        assert!(walker.finish().is_err());
    }
}

#[test]
fn certified_empty_lane_is_evidence_not_missing_coverage() {
    let source = TempDir::new("quiet-source").unwrap();
    let out = TempDir::new("quiet-output").unwrap();
    canonical(source.path(), 0, 10, 1, &[], true);
    amend_receipt(source.path(), |r| {
        r.expected_lanes.push("quiet".into());
        r.present_lanes.push("quiet".into());
        r.inputs.push(InputSegment {
            lane: "quiet".into(),
            data_file: "empty.ndjson".into(),
            segment_index: 0,
            line_count: 0,
            sha256: Sha256::digest(b"").as_hex(),
            first_delivery_index: None,
            last_delivery_index: None,
        });
    });
    let input = build(source.path(), out.path(), 0, 10, &mut Fake::new());
    let (groups, statuses, done) =
        drain(DerivativeWalker::open(vec![input], request(2, 8), ReadLimits::default()).unwrap());
    assert!(groups.is_empty());
    assert_eq!(statuses.len(), 1);
    assert!(statuses[0].metadata().manifest().source_receipt.certified);
    let coverage = statuses[0].coverage().unwrap();
    assert_eq!(
        coverage.lane(&LaneId::new("quiet").unwrap()),
        LaneCoverage {
            expected: true,
            state: LaneState::Present { records: 0 }
        }
    );
    assert!(coverage.faults().is_empty());
    assert!(done.supports_source_evidence());
}
