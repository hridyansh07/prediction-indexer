//! The merge order §8 requires, and the inputs it must refuse.
//!
//! These are the "required false/failure tests" of
//! `docs/SEALED_CAPTURE_PIPELINE_V1.md` §8, written against the merge itself
//! rather than the binary, so a failure names the comparison that went wrong
//! instead of a diff between two large files.
//!
//! `crates/cli/tests/ordering.rs` asserts the *other* order over an equivalent
//! fixture — `file_order`, which sequences a record received between two records
//! of another lane after both. The two live side by side on purpose.

use std::fs;

use indexer_finalize::{
    LaneReader, LaneRecord, LaneStream, MergedRecord, SegmentInput, SourceRef, merged,
    read_receipt, receipt_path,
};
use tempdir::TempDir;

/// One record, with only the fields the merge actually keys on.
fn record(lane: u16, delivery_index: u64, visible_ns: u64) -> LaneRecord {
    LaneRecord {
        visible_ns,
        delivery_index,
        source: SourceRef {
            lane,
            segment: 0,
            line_number: delivery_index,
        },
        record_id: format!("lane{lane}-{delivery_index}"),
        content_hash: format!("{lane:064x}"),
        line: format!("{{\"lane\":{lane},\"visible_ns\":{visible_ns}}}\n").into_bytes(),
    }
}

fn stream(records: Vec<LaneRecord>) -> LaneStream<'static> {
    Box::new(records.into_iter().map(Ok))
}

/// The merged records, or the first fault the merge raised, rendered.
///
/// A `MergeFault` names its lane structurally so the finalizer can exclude that
/// lane and retry; these tests only need the message, so it is flattened here.
fn run(lanes: Vec<(&str, Vec<LaneRecord>)>) -> Result<Vec<MergedRecord>, String> {
    let built = lanes
        .into_iter()
        .map(|(lane, records)| (lane.to_owned(), stream(records)))
        .collect();
    merged(built)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|fault| fault.to_string())
}

fn order(results: &[MergedRecord]) -> Vec<(u16, u64)> {
    results
        .iter()
        .map(|item| (item.record.source.lane, item.record.visible_ns))
        .collect()
}

// -- the interleave --------------------------------------------------------

#[test]
fn a_record_received_between_two_others_merges_between_them() {
    // The defect the whole design exists to fix, stated positively. In
    // `file_order` the kalshi record lands after both polymarket records because
    // whole files are consumed atomically.
    let results = run(vec![
        ("polymarket", vec![record(0, 1, 100), record(0, 2, 300)]),
        ("kalshi", vec![record(1, 1, 200)]),
    ])
    .expect("merge");

    assert_eq!(order(&results), vec![(0, 100), (1, 200), (0, 300)]);
    assert!(
        results.iter().all(|item| item.visible_tie_group.is_none()),
        "distinct timestamps are not a tie"
    );
}

// -- ties ------------------------------------------------------------------

#[test]
fn identical_timestamps_resolve_polymarket_then_kalshi_then_limitless() {
    // §8. Note the lanes are supplied in the opposite order to prove the result
    // comes from the rank table and not from construction order.
    let results = run(vec![
        ("limitless", vec![record(0, 1, 500)]),
        ("kalshi", vec![record(1, 1, 500)]),
        ("polymarket", vec![record(2, 1, 500)]),
    ])
    .expect("merge");

    let lanes: Vec<u16> = results.iter().map(|item| item.record.source.lane).collect();
    assert_eq!(
        lanes,
        vec![2, 1, 0],
        "polymarket, then kalshi, then limitless"
    );
}

#[test]
fn a_cross_lane_tie_is_marked_as_one_group() {
    // §8: analysis must treat that deterministic ordering as a tie, not lead-lag.
    // The rank decided the bytes; `visible_tie_group` is what tells the reader
    // the order it sees carries no timing information.
    let results = run(vec![
        ("polymarket", vec![record(0, 1, 500)]),
        ("kalshi", vec![record(1, 1, 500)]),
    ])
    .expect("merge");

    let groups: Vec<Option<u64>> = results.iter().map(|item| item.visible_tie_group).collect();
    assert_eq!(
        groups,
        vec![Some(500), Some(500)],
        "one group, shared by both lanes"
    );
}

#[test]
fn two_lanes_of_one_venue_tie_by_rank_and_stay_one_group() {
    // §8's same-venue case. `polymarket` and `polymarket_snapshots` both carry
    // `venue: polymarket` in their envelopes, so the venue field cannot separate
    // them — only the lane rank can.
    let results = run(vec![
        ("polymarket_snapshots", vec![record(0, 1, 700)]),
        ("polymarket", vec![record(1, 1, 700)]),
    ])
    .expect("merge");

    assert_eq!(
        order(&results),
        vec![(1, 700), (0, 700)],
        "polymarket (rank 0) precedes polymarket_snapshots (rank 1)"
    );
    assert!(
        results
            .iter()
            .all(|item| item.visible_tie_group == Some(700)),
        "two lanes of one venue are still one analytical tie group"
    );
}

#[test]
fn one_lane_sharing_a_timestamp_is_not_a_tie_group() {
    // The distinction that keeps the field meaningful: these two are genuinely
    // ordered by `delivery_index`, because one splice observed them in that
    // order. Marking them as simultaneous would discard real information.
    let results = run(vec![(
        "polymarket",
        vec![record(0, 1, 900), record(0, 2, 900)],
    )])
    .expect("merge");

    assert_eq!(order(&results), vec![(0, 900), (0, 900)]);
    assert!(results.iter().all(|item| item.visible_tie_group.is_none()));
}

#[test]
fn a_tie_group_closes_before_the_next_instant() {
    // A run must not absorb the record that ends it.
    let results = run(vec![
        ("polymarket", vec![record(0, 1, 100), record(0, 2, 200)]),
        ("kalshi", vec![record(1, 1, 100)]),
    ])
    .expect("merge");

    let groups: Vec<Option<u64>> = results.iter().map(|item| item.visible_tie_group).collect();
    assert_eq!(groups, vec![Some(100), Some(100), None]);
}

// -- determinism -----------------------------------------------------------

#[test]
fn merged_order_does_not_depend_on_which_lane_was_discovered_first() {
    // Discovery order is a filesystem accident. If it reached the output, two
    // runs over identical bytes would produce different canonical hashes and the
    // §8 retry-after-crash guarantee would be unprovable.
    let build = || {
        vec![
            ("polymarket", vec![record(0, 1, 100), record(0, 2, 400)]),
            ("kalshi", vec![record(1, 1, 200), record(1, 2, 400)]),
            ("limitless", vec![record(2, 1, 300)]),
        ]
    };
    let forward = run(build()).expect("merge");
    let mut reversed_input = build();
    reversed_input.reverse();
    let reversed = run(reversed_input).expect("merge");

    assert_eq!(order(&forward), order(&reversed));
    assert_eq!(
        order(&forward),
        vec![(0, 100), (1, 200), (2, 300), (0, 400), (1, 400)],
        "and the 400 tie falls to polymarket before kalshi"
    );
}

// -- invalid input ---------------------------------------------------------

#[test]
fn a_repeated_delivery_index_within_a_lane_is_refused() {
    // §1: "A repeated or decreasing value within a lane is invalid input, not
    // another tie to resolve." Resolving it as a tie would copy a record into
    // canonical evidence twice.
    let error = run(vec![(
        "polymarket",
        vec![record(0, 7, 100), record(0, 7, 200)],
    )])
    .expect_err("a repeat must be refused");
    assert!(error.contains("delivery_index"), "{error}");
    assert!(error.contains("not a tie"), "{error}");
}

#[test]
fn a_decreasing_delivery_index_within_a_lane_is_refused() {
    let error = run(vec![(
        "polymarket",
        vec![record(0, 9, 100), record(0, 4, 200)],
    )])
    .expect_err("a decrease must be refused");
    assert!(error.contains("delivery_index"), "{error}");
}

#[test]
fn a_lane_whose_visible_ns_runs_backwards_is_refused() {
    // The seal flags this per segment and the window classifies that lane
    // invalid. If one reaches the merge anyway, emitting it would write a
    // canonical file whose own ordering key decreases.
    let error = run(vec![(
        "polymarket",
        vec![record(0, 1, 500), record(0, 2, 400)],
    )])
    .expect_err("a backwards clock must be refused");
    assert!(error.contains("visible_ns"), "{error}");
}

#[test]
fn a_reader_error_surfaces_rather_than_truncating_the_merge() {
    let lanes: Vec<(String, LaneStream<'static>)> = vec![
        ("polymarket".to_owned(), stream(vec![record(0, 1, 100)])),
        (
            "kalshi".to_owned(),
            Box::new(std::iter::once(Err("torn segment".to_owned()))),
        ),
    ];
    let outcome: Result<Vec<MergedRecord>, _> = merged(lanes).expect("distinct lanes").collect();
    let fault = outcome.expect_err("the failing lane must not be silently skipped");
    assert_eq!(
        fault.lane, "kalshi",
        "the fault names its lane structurally"
    );
    assert!(fault.detail.contains("torn segment"), "{}", fault.detail);
}

#[test]
fn a_receipt_cannot_resolve_an_output_outside_its_window() {
    let root = TempDir::new("canonical-receipt-path").expect("temporary directory");
    let canonical = root.path().join("canonical");
    let path = receipt_path(&canonical, 0);
    fs::create_dir_all(path.parent().expect("window directory")).expect("create window");
    fs::write(root.path().join("outside-evidence.ndjson.zst"), b"x").expect("outside evidence");
    let receipt = serde_json::json!({
        "receipt_version": 1,
        "window_start_ns": 0,
        "window_end_ns": 1_800_000_000_000_u64,
        "completeness": "complete",
        "certified": true,
        "expected_lanes": [],
        "present_lanes": [],
        "unexpected_lanes": [],
        "missing_lanes": [],
        "invalid_lanes": [],
        "finalization_deadline_seconds": 300,
        "deadline_expired": false,
        "finalized_at_ns": 1_800_000_000_000_u64,
        "inputs": [],
        "evidence": {
            "file": "../../../outside-evidence.ndjson.zst",
            "content_encoding": "zstd",
            "decoded": {"byte_length": 0, "line_count": 0, "sha256": format!("{:064x}", 0)},
            "stored": {"byte_length": 1, "sha256": format!("{:064x}", 0)},
            "compression": {
                "algorithm": "zstd", "level": 3, "frame_checksum": true,
                "dictionary": null, "frame_count": 1, "encoder": "fixture"
            }
        },
        "provenance": {
            "file": "provenance.ndjson.zst",
            "content_encoding": "zstd",
            "decoded": {"byte_length": 0, "line_count": 0, "sha256": format!("{:064x}", 0)},
            "stored": {"byte_length": 0, "sha256": format!("{:064x}", 0)},
            "compression": {
                "algorithm": "zstd", "level": 3, "frame_checksum": true,
                "dictionary": null, "frame_count": 1, "encoder": "fixture"
            }
        },
        "first_canonical_seq": null,
        "last_canonical_seq": null,
        "finalizer_version": 1,
    });
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(&receipt).unwrap()),
    )
    .expect("receipt");

    let error = read_receipt(&canonical, 0)
        .expect_err("an output path outside the window must be rejected");
    assert!(error.contains("evidence"), "{error}");
}

// -- against real envelope bytes -------------------------------------------

fn envelope(venue: &str, delivery: u64, visible_ns: u64, monotonic_ns: u64) -> String {
    let record = format!(
        r#"{{"envelope_version":2,"delivery_index":{delivery},"record_id":"{venue}-e-{delivery}","visible_ns":{visible_ns},"monotonic_ns":{monotonic_ns},"venue":"{venue}","stream":"public_book","connection_epoch":"e","local_counter":{delivery},"source_cursor":{{"type":"unsequenced","counter":{delivery}}},"kind":"venue_frame","raw_payload":"{{}}"}}"#
    );
    format!("{record}\n")
}

#[test]
fn a_monotonic_clock_reset_does_not_disturb_a_valid_visible_merge() {
    // §8. `monotonic_ns` resets to a new boot-relative origin across a reboot, so
    // ordering on it would put the post-reboot records first. V1 orders on
    // `visible_ns`, which is why the reset is merely recorded and not obeyed.
    let root = TempDir::new("finalize-monotonic").expect("temporary directory");

    // Polymarket rebooted between its two records: monotonic falls from a large
    // value to a small one while wall time moves forward normally.
    let polymarket = root.path().join("polymarket.ndjson");
    fs::write(
        &polymarket,
        format!(
            "{}{}",
            envelope("polymarket", 1, 100, 9_000_000_000),
            envelope("polymarket", 2, 300, 12_000)
        ),
    )
    .expect("write polymarket segment");

    let kalshi = root.path().join("kalshi.ndjson");
    fs::write(&kalshi, envelope("kalshi", 1, 200, 5_000_000_000)).expect("write kalshi segment");

    let lanes: Vec<(String, LaneStream<'_>)> = vec![
        (
            "polymarket".to_owned(),
            Box::new(LaneReader::new(
                0,
                vec![SegmentInput {
                    path: polymarket,
                    segment: 0,
                    claims: None,
                }],
            )),
        ),
        (
            "kalshi".to_owned(),
            Box::new(LaneReader::new(
                1,
                vec![SegmentInput {
                    path: kalshi,
                    segment: 1,
                    claims: None,
                }],
            )),
        ),
    ];
    let results: Vec<MergedRecord> = merged(lanes)
        .expect("distinct lanes")
        .collect::<Result<_, _>>()
        .expect("merge");

    assert_eq!(
        order(&results),
        vec![(0, 100), (1, 200), (0, 300)],
        "wall receive time decides; the monotonic reset changes nothing"
    );
    // And the bytes are carried through untouched.
    assert_eq!(
        results[1].record.line,
        envelope("kalshi", 1, 200, 5_000_000_000).into_bytes()
    );
}

#[test]
fn a_lane_concatenates_its_segments_in_order() {
    let root = TempDir::new("finalize-segments").expect("temporary directory");
    let first = root.path().join("first.ndjson");
    let second = root.path().join("second.ndjson");
    fs::write(&first, envelope("polymarket", 1, 100, 1)).expect("write first");
    fs::write(&second, envelope("polymarket", 2, 200, 2)).expect("write second");

    let reader = LaneReader::new(
        0,
        vec![
            SegmentInput {
                path: first,
                segment: 0,
                claims: None,
            },
            SegmentInput {
                path: second,
                segment: 1,
                claims: None,
            },
        ],
    );
    let records: Vec<LaneRecord> = reader.collect::<Result<_, _>>().expect("read");

    assert_eq!(records.len(), 2);
    // Line numbers restart per segment, and the segment index says which file.
    assert_eq!(
        records[0].source,
        SourceRef {
            lane: 0,
            segment: 0,
            line_number: 1
        }
    );
    assert_eq!(
        records[1].source,
        SourceRef {
            lane: 0,
            segment: 1,
            line_number: 1
        }
    );
}

#[test]
fn a_malformed_envelope_fails_the_lane_rather_than_being_skipped() {
    // §5 validates closed envelope schemas before a window is finalized. A line
    // the parser refuses makes its lane invalid for the window; it must not drop
    // out of canonical evidence while the rest of the lane is committed.
    let root = TempDir::new("finalize-malformed").expect("temporary directory");
    let path = root.path().join("segment.ndjson");
    fs::write(
        &path,
        format!(
            "{}{}",
            envelope("polymarket", 1, 100, 1),
            "{\"delivery_index\":2}\n"
        ),
    )
    .expect("write segment");

    let reader = LaneReader::new(
        0,
        vec![SegmentInput {
            path,
            segment: 0,
            claims: None,
        }],
    );
    let outcome: Result<Vec<LaneRecord>, String> = reader.collect();
    let error = outcome.expect_err("a malformed line must fail the lane");
    assert!(error.contains(":2:"), "the failure names the line: {error}");
}

// -- lane-set and density regressions --------------------------------------
//
// Three defects an independent review of 3b found, each reproduced by a direct
// probe before it was fixed. All three shared a shape: the merge trusted a
// property of its input that nothing established.

#[test]
fn a_gap_in_delivery_index_within_a_lane_is_refused() {
    // Found accepting `[1, 3]` and emitting two records. `delivery_index` is
    // assigned by the splice before the record is written and continues across
    // restarts, so within a window it cannot legitimately skip — a gap means one
    // of that lane's segments is missing from the window. Merging across it
    // publishes canonical evidence that omits records while its receipt says
    // the lane was present, which is the failure mode seals exist to prevent.
    let error = run(vec![(
        "polymarket",
        vec![record(0, 1, 100), record(0, 3, 200)],
    )])
    .expect_err("a gap must be refused");
    assert!(error.contains("delivery_index"), "{error}");
    assert!(error.contains("missing from the window"), "{error}");
}

#[test]
fn two_unranked_lanes_do_not_let_discovery_order_decide_the_bytes() {
    // Found reversing its output when the inputs were reversed. Both lanes are
    // absent from the rank table, so both carry `UNRANKED_LANE_RANK` and, at one
    // instant with one index, the key fell through to construction order — a
    // filesystem accident. Two runs over identical bytes would then produce
    // different canonical hashes, making §8's retry-after-crash guarantee false.
    let forward = run(vec![
        ("zulu", vec![record(0, 1, 100)]),
        ("alpha", vec![record(1, 1, 100)]),
    ])
    .expect("merge");
    let reversed = run(vec![
        ("alpha", vec![record(1, 1, 100)]),
        ("zulu", vec![record(0, 1, 100)]),
    ])
    .expect("merge");

    assert_eq!(
        order(&forward),
        order(&reversed),
        "input order must not reach the output"
    );
    assert_eq!(
        order(&forward),
        vec![(1, 100), (0, 100)],
        "the lane name is the last resort, so `alpha` precedes `zulu` either way"
    );
}

#[test]
fn an_unranked_lane_still_sorts_after_every_ranked_one() {
    let results = run(vec![
        ("alpha", vec![record(0, 1, 100)]),
        ("limitless", vec![record(1, 1, 100)]),
    ])
    .expect("merge");
    assert_eq!(
        order(&results),
        vec![(1, 100), (0, 100)],
        "an unranked lane does not jump ahead of the table by sorting alphabetically"
    );
}

#[test]
fn the_same_lane_supplied_twice_is_refused() {
    // Found accepting two streams both named `polymarket`, each with
    // `delivery_index` 1. A lane is one splice running one counter; two cursors
    // under one name each keep their own `last_delivery_index`, so the density
    // and uniqueness checks above pass independently and the same index enters
    // canonical evidence twice.
    let lanes: Vec<(String, LaneStream<'static>)> = vec![
        ("polymarket".to_owned(), stream(vec![record(0, 1, 100)])),
        ("polymarket".to_owned(), stream(vec![record(1, 1, 200)])),
    ];
    let error = merged(lanes)
        .err()
        .expect("a repeated lane name must be refused");
    assert!(error.contains("polymarket"), "{error}");
    assert!(error.contains("twice"), "{error}");
}

#[test]
fn the_rank_table_is_not_the_expected_lane_set() {
    // §3: "The deployment manifest defines which lanes are expected; a disabled
    // Kalshi profile is not waited on." `compose.yaml` puts splice-kalshi,
    // splice-polymarket-sports and splice-polymarket-rtds behind opt-in
    // profiles, so the default deployment runs three of these six. Reading this
    // table as the expectation would mark every default window incomplete with
    // three phantom `lane_missing` entries.
    assert_eq!(indexer_finalize::supported_lanes().len(), 6);
    // And ranking must still cover a lane nobody is waiting for, because a lane
    // can be present without being expected.
    assert!(indexer_finalize::lane_rank("kalshi") < indexer_finalize::UNRANKED_LANE_RANK);
}
