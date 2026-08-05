//! Continuity classification, including regressions for the two bugs the first
//! ingest over real spools exposed.

use indexer_continuity::{Classifier, EpochHealth};
use indexer_store::Store;
use indexer_types::EnvelopeView;

fn temporary_store() -> (tempdir::TempDir, Store) {
    let directory = tempdir::TempDir::new("indexer-test").expect("temp dir");
    let store = Store::open(directory.path()).expect("open store");
    (directory, store)
}

fn line(
    delivery: u64,
    counter: u64,
    stream: &str,
    kind: &str,
    cursor: &str,
    venue: &str,
    epoch: &str,
) -> String {
    format!(
        r#"{{"delivery_index":{delivery},"record_id":"r-{epoch}-{counter}","visible_ns":{delivery},"venue":"{venue}","stream":"{stream}","connection_epoch":"{epoch}","local_counter":{counter},"source_cursor":{cursor},"kind":"{kind}","raw_payload":"{{\"n\":{counter}}}"}}"#
    )
}

fn frame(delivery: u64, counter: u64) -> String {
    line(
        delivery,
        counter,
        "public_book",
        "venue_frame",
        &format!(r#"{{"type":"unsequenced","counter":{counter}}}"#),
        "polymarket",
        "aaaa",
    )
}

fn control(delivery: u64, counter: u64) -> String {
    line(
        delivery,
        counter,
        "process",
        "control",
        "null",
        "polymarket",
        "aaaa",
    )
}

/// Feeds lines through the whole pipeline and returns each record's cause label.
fn run(lines: &[String]) -> (Classifier, Vec<String>) {
    let (_guard, mut store) = temporary_store();
    let mut classifier = Classifier::new();
    let mut causes = Vec::new();
    for (index, text) in lines.iter().enumerate() {
        let bytes = text.as_bytes();
        let view = EnvelopeView::parse(bytes).expect("parses");
        let captured = store
            .capture_raw(
                "polymarket",
                "test",
                index as u64,
                bytes,
                view.raw_payload.as_bytes(),
            )
            .expect("capture");
        let fact = classifier.classify(&view, &captured);
        let committed = store.commit_fact(&captured, fact).expect("commit");
        causes.push(committed.value().cause.label().to_owned());
        classifier.apply(&committed);
    }
    (classifier, causes)
}

/// Regression: the first ingest reported 3,306 phantom counter breaks out of
/// 3,823 frames, because `local_counter` was tracked per lane. A splice mints one
/// counter per *connection* and spends it across every stream, so a Polymarket
/// epoch interleaves `process` lifecycle records with `public_book` frames out of
/// the same sequence.
#[test]
fn local_counter_spans_streams_within_one_connection() {
    let lines = vec![
        control(1, 1),
        control(2, 2),
        frame(3, 3),
        frame(4, 4),
        control(5, 5),
        frame(6, 6),
        frame(7, 7),
    ];
    let (classifier, causes) = run(&lines);

    assert!(
        !causes.iter().any(|cause| cause == "local_counter_broken"),
        "stream interleaving must not read as a dropped record: {causes:?}"
    );
    for epoch in classifier.state().epochs.values() {
        assert_eq!(epoch.local_breaks, 0);
    }
}

#[test]
fn a_genuine_counter_hole_is_caught() {
    let (_classifier, causes) = run(&[frame(1, 1), frame(2, 2), frame(3, 4)]);
    assert_eq!(causes.last().unwrap(), "local_counter_broken");
}

/// Regression: comparing a snapshot id against a lane-wide previous value
/// compares two different books. Limitless's `version` behaves like a server-wide
/// counter sampled per market, so consecutive frames for different markets move
/// backwards relative to each other — 7 such "faults" in 451 real frames, none of
/// them real. Instrument-level continuity needs the payload, which this component
/// deliberately does not parse.
#[test]
fn interleaved_snapshot_ids_are_not_faults() {
    let descending = |delivery: u64, counter: u64, version: u64| {
        line(
            delivery,
            counter,
            "public_book",
            "venue_frame",
            &format!(r#"{{"type":"snapshot","last_update_id":{version}}}"#),
            "limitless",
            "bbbb",
        )
    };
    let lines = vec![
        descending(1, 1, 958_621_772),
        descending(2, 2, 958_624_794), // market A moves forward
        descending(3, 3, 958_621_816), // market B, lower id, still legitimate
        descending(4, 4, 958_625_186),
    ];
    let (classifier, causes) = run(&lines);

    assert!(
        !causes.iter().any(|cause| cause == "cursor_went_backwards"),
        "a multiplexed lane cannot judge per-instrument order: {causes:?}"
    );
    assert!(
        classifier
            .state()
            .epochs
            .values()
            .all(|epoch| epoch.health == EpochHealth::Healthy)
    );
}

#[test]
fn a_proven_gap_stales_the_epoch() {
    let ranged = |delivery: u64, counter: u64, first: u64, last: u64, previous: u64| {
        line(
            delivery,
            counter,
            "public_book",
            "venue_frame",
            &format!(
                r#"{{"type":"update_range","first":{first},"last":{last},"previous_last":{previous}}}"#
            ),
            "kalshi",
            "cccc",
        )
    };
    // 1-2, then 3-4 continuing, then 9-10 with a hole behind it.
    let lines = vec![
        ranged(1, 1, 1, 2, 0),
        ranged(2, 2, 3, 4, 2),
        ranged(3, 3, 9, 10, 8),
    ];
    let (classifier, causes) = run(&lines);

    assert_eq!(causes, vec!["bootstrap", "continuous", "gap_proven"]);
    let epoch = classifier
        .state()
        .epochs
        .values()
        .next()
        .expect("one epoch");
    assert_eq!(epoch.proven_gaps, 1);
    assert_eq!(epoch.health, EpochHealth::Stale);
}

#[test]
fn a_retransmission_is_a_duplicate_and_moves_nothing() {
    let first = frame(1, 1);
    let (classifier, causes) = run(&[first.clone(), frame(2, 2), first]);

    assert_eq!(
        causes,
        vec!["unsequenced_venue", "unsequenced_venue", "duplicate"]
    );
    assert_eq!(classifier.state().duplicates, 1);
    // Identity is decided before continuity, so the replay must not have rewound
    // the counter to 1.
    let epoch = classifier
        .state()
        .epochs
        .values()
        .next()
        .expect("one epoch");
    assert_eq!(epoch.observed_counter, Some(2));
}

/// The same id carrying different bytes is a venue contradicting itself, and it
/// must not silently become the new truth.
#[test]
fn the_same_id_with_different_bytes_is_a_conflict() {
    let original = frame(1, 1);
    let altered = original.replace(r#"{\"n\":1}"#, r#"{\"n\":99}"#);
    assert_ne!(original, altered);

    let (classifier, causes) = run(&[original, altered]);
    assert_eq!(causes, vec!["unsequenced_venue", "conflict"]);
    assert_eq!(classifier.state().conflicts, 1);
}

#[test]
fn lifecycle_records_are_never_measured_against_venue_continuity() {
    let (_classifier, causes) = run(&[control(1, 1), control(2, 2)]);
    assert_eq!(causes, vec!["lifecycle", "lifecycle"]);
}

#[test]
fn a_reconnect_starts_a_fresh_epoch_without_faulting() {
    let second_epoch = line(
        3,
        1,
        "public_book",
        "venue_frame",
        r#"{"type":"unsequenced","counter":1}"#,
        "polymarket",
        "dddd",
    );
    let (classifier, causes) = run(&[frame(1, 1), frame(2, 2), second_epoch]);

    assert!(!causes.iter().any(|cause| cause == "local_counter_broken"));
    assert_eq!(
        classifier.state().epochs.len(),
        2,
        "each connection gets its own epoch"
    );
}
