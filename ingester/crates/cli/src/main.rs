//! `indexer-ingest` — reads the venue spools, assigns the global order, classifies
//! continuity, and writes the fact log.
//!
//! ```text
//! indexer-ingest <spool-root> <store-dir> [--check-integrity]
//! indexer-ingest <spool-root> <store-dir> --watch-interval-seconds 5
//! ```
//!
//! Resumable and idempotent. Only an immutable `.ndjson` with a matching valid
//! seal is eligible. Each file still carries a byte cursor so a process crash
//! during ingestion resumes at its last committed batch rather than rescanning
//! the segment. Records and their cursor advance commit in one transaction: a
//! cursor ahead of its evidence would silently skip records on the next run, and
//! behind would duplicate them.
//!
//! Watch mode keeps the recovered classifier and open store in one process while
//! polling for newly sealed files. A segment's length and SHA-256 are validated
//! once, immediately before its first ingest in that process; hashing every old
//! immutable segment on every five-second poll would make validation an
//! ever-growing disk scan.
//!
//! **On ordering: what this binary assigns is `file_order`, not event order.**
//!
//! `EvidenceSeq` is assigned in the order files are read — globally by
//! timestamp-prefixed filename, then by position within the file. A splice opens
//! a file when a connection opens, so that is chronological *between* connections
//! but not interleaved *within* two that overlap. Whole files are consumed
//! atomically, so a record received between two records of another lane is
//! sequenced after both. `tests/ordering.rs` pins this.
//!
//! **Never use `EvidenceSeq` as time.** Cross-venue analysis must sort on
//! `visible_ns`, which every record carries.
//!
//! `docs/SEALED_CAPTURE_PIPELINE_V1.md` is the design that replaces this: sealed
//! UTC-aligned lane segments merged on `(visible_ns, lane_rank, delivery_index)`.
//! Its §1 also settles a question this comment previously got wrong — it
//! recommended `monotonic_ns` for lead-lag, and V1 orders on `visible_ns`
//! instead, with `monotonic_ns` kept for diagnostics. Monotonic time resets per
//! boot and is comparable across processes only within one Linux boot scope,
//! which makes it the worse authoritative key even though it never steps.
//!
//! Until the finalizer lands, this ordering remains available and must stay
//! labelled `file_order` wherever it is surfaced.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use indexer_cli::ingest_partition::{
    CarryTracker, IDENTITY_SCOPE, OpenPartition, relative_segment_path,
};
use indexer_continuity::{Classifier, IdentityVerdict};
use indexer_segment::{SegmentSeal, discover_segments, read_seal, validate_sealed_segment};
use indexer_store::{IntegrityReport, Store, StoreError};
use indexer_types::EnvelopeView;

/// Records committed per transaction. Large enough that commit cost amortises,
/// small enough that a crash re-reads seconds rather than minutes of spool.
const BATCH: usize = 512;

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            std::process::ExitCode::from(1)
        }
    }
}

struct Arguments {
    spool_root: PathBuf,
    store_dir: PathBuf,
    check_integrity: bool,
    watch_interval: Option<Duration>,
}

const USAGE: &str = "usage: indexer-ingest <spool-root> <store-dir> \
                     [--check-integrity] [--watch-interval-seconds <seconds>]";

fn parse_arguments() -> Result<Arguments, String> {
    let mut positional = Vec::new();
    let mut check_integrity = false;
    let mut watch_interval = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--check-integrity" => check_integrity = true,
            "--watch-interval-seconds" => {
                let raw = arguments
                    .next()
                    .ok_or_else(|| format!("--watch-interval-seconds needs a value\n{USAGE}"))?;
                let seconds = raw.parse::<f64>().map_err(|_| {
                    format!("invalid --watch-interval-seconds value: {raw:?}\n{USAGE}")
                })?;
                if !seconds.is_finite() || seconds <= 0.0 {
                    return Err(format!(
                        "--watch-interval-seconds must be a finite number greater than zero\n{USAGE}"
                    ));
                }
                watch_interval = Some(Duration::from_secs_f64(seconds));
            }
            "-h" | "--help" => return Err(USAGE.to_owned()),
            other if other.starts_with("--") => {
                return Err(format!("unknown flag: {other}\n{USAGE}"));
            }
            other => positional.push(other.to_owned()),
        }
    }
    if positional.len() != 2 {
        return Err(USAGE.to_owned());
    }
    Ok(Arguments {
        spool_root: PathBuf::from(&positional[0]),
        store_dir: PathBuf::from(&positional[1]),
        check_integrity,
        watch_interval,
    })
}

#[derive(Default)]
struct Tally {
    read: u64,
    captured: u64,
    skipped_resumed: u64,
    skipped_segments: u64,
    rejected: u64,
    causes: BTreeMap<&'static str, u64>,
    first_rejections: Vec<String>,
}

struct IngestState<'a> {
    store: &'a mut Store,
    classifier: &'a mut Classifier,
    tracker: &'a mut CarryTracker,
}

fn run() -> Result<(), String> {
    let arguments = parse_arguments()?;
    if arguments.watch_interval.is_some() {
        std::fs::create_dir_all(&arguments.spool_root)
            .map_err(|error| format!("creating spool root: {error}"))?;
    }
    // Cursor and receipt identities are relative to this canonical root, so a
    // different spelling of the same path cannot ingest a segment twice.
    let spool_root = std::fs::canonicalize(&arguments.spool_root)
        .map_err(|error| format!("resolving spool root: {error}"))?;
    let mut partition = OpenPartition::open(&arguments.store_dir, unix_now_ns()?)?;
    if let Some(migration) = partition.store.migration_report() {
        eprintln!(
            "migrated store schema {} -> {}: {} identity records in {:.3} seconds",
            migration.from_schema,
            migration.to_schema,
            migration.identity_records,
            migration.elapsed.as_secs_f64(),
        );
    }
    // Hashing a sealed segment is deliberately a once-per-process operation.
    // Watch mode polls every few seconds and production segments can be close to
    // a gigabyte; rereading every old file on every poll would turn validation
    // into an unbounded disk scan. A segment is immutable after its seal appears,
    // so validation immediately before its first ingest is sufficient.
    let mut validated_segments = BTreeSet::new();

    loop {
        // A restart after midnight must close yesterday before it consumes any
        // newly discovered work. The same check runs between segments below, so
        // rollover never bisects a sealed segment even when ingestion spans UTC
        // midnight.
        let rollover_time = unix_now_ns()?;
        if partition.should_rotate(rollover_time)? {
            partition = partition.rotate(rollover_time)?;
            validated_segments.clear();
        }

        // Discovery returns segments sorted by filename. Reading them in that
        // order, each to completion, is what makes `EvidenceSeq` `file_order` —
        // the claim belongs here, at the call site that acts on it, not to
        // discovery itself. See the module header.
        let files = discover_segments(&spool_root)
            .map_err(|error| format!("reading spool root: {error}"))?;
        let mut tally = Tally::default();

        for (lane, path) in &files {
            let rollover_time = unix_now_ns()?;
            if partition.should_rotate(rollover_time)? {
                partition = partition.rotate(rollover_time)?;
                validated_segments.clear();
            }
            let key = relative_segment_path(&spool_root, path)?;
            let declared = read_seal(path)?;
            if partition.consumed.contains(&key, &declared.sha256)? {
                tally.skipped_segments += 1;
                continue;
            }
            if !validated_segments.contains(path) {
                validate_sealed_segment(lane, path)?;
                validated_segments.insert(path.clone());
            }
            let mut state = IngestState {
                store: &mut partition.store,
                classifier: &mut partition.classifier,
                tracker: &mut partition.tracker,
            };
            ingest_file(&mut state, &spool_root, lane, path, &declared, &mut tally)
                .map_err(|error| format!("{}: {error}", path.display()))?;
            partition.consumed.remember(&key, &declared.sha256)?;
        }

        let integrity = if arguments.check_integrity {
            Some(
                partition
                    .store
                    .check_integrity()
                    .map_err(|error| error.to_string())?,
            )
        } else {
            None
        };
        println!(
            "{}",
            render_report(&arguments, &files, &tally, &partition, integrity.as_ref(),)
        );
        std::io::stdout()
            .flush()
            .map_err(|error| format!("flushing report: {error}"))?;

        let rollover_time = unix_now_ns()?;
        if partition.should_rotate(rollover_time)? {
            partition = partition.rotate(rollover_time)?;
            validated_segments.clear();
        }

        match arguments.watch_interval {
            Some(interval) => std::thread::sleep(interval),
            None => break,
        }
    }
    Ok(())
}

fn ingest_file(
    state: &mut IngestState<'_>,
    spool_root: &Path,
    lane: &str,
    path: &Path,
    seal: &SegmentSeal,
    tally: &mut Tally,
) -> Result<(), String> {
    let key = relative_segment_path(spool_root, path)?;
    let cursor = state
        .store
        .spool_cursor(&key)
        .map_err(|error| error.to_string())?;
    let already = cursor
        .as_ref()
        .map(|entry| entry.bytes_consumed)
        .unwrap_or(0);
    let mut records = cursor.as_ref().map(|entry| entry.records).unwrap_or(0);

    let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut reader = BufReader::new(file);
    let file_length = reader
        .get_ref()
        .metadata()
        .map_err(|error| error.to_string())?
        .len();
    if already > file_length {
        return Err(format!(
            "durable cursor {already} is beyond current file length {file_length}"
        ));
    }
    reader
        .seek(SeekFrom::Start(already))
        .map_err(|error| format!("seeking to durable cursor {already}: {error}"))?;
    let mut offset = already;
    let mut durable_offset = already;
    let mut batch: Vec<(u64, Vec<u8>)> = Vec::with_capacity(BATCH);

    loop {
        let mut line = Vec::new();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        let start = offset;
        offset += read as u64;

        // A final line with no newline is a torn write: the splice was interrupted
        // mid-record. It is not durable, so it is not ingested — the splice's own
        // recovery truncates it on restart, and ingesting it here would make a
        // partial record permanent.
        if !line.ends_with(b"\n") {
            break;
        }
        durable_offset = offset;
        tally.read += 1;
        batch.push((start, line));

        if batch.len() >= BATCH {
            records += commit_batch(state, lane, &key, &batch, records, tally)?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        records += commit_batch(state, lane, &key, &batch, records, tally)?;
    }
    // A cursor at EOF and the completed-segment identity commit together. If a
    // crash happens after the final record batch but before this small
    // transaction, restart seeks to EOF and commits only this marker.
    state
        .store
        .transaction(|store| {
            store.advance_spool_cursor(&key, lane, durable_offset, records)?;
            store.complete_spool_segment(
                &key,
                lane,
                &seal.sha256,
                seal.byte_length,
                seal.line_count,
            )
        })
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn commit_batch(
    state: &mut IngestState<'_>,
    lane: &str,
    key: &str,
    batch: &[(u64, Vec<u8>)],
    records_before: u64,
    tally: &mut Tally,
) -> Result<u64, String> {
    let offset = batch
        .last()
        .map(|(start, line)| start + line.len() as u64)
        .expect("commit_batch is never called empty");
    let mut rejected = Vec::new();
    let mut accepted = 0u64;

    // Classification state advances *inside* the transaction, immediately after
    // each `commit_fact`.
    //
    // The rule this bends is the classifier's "state moves only after commit", which
    // exists so a crash between classifying and committing loses nothing. Holding
    // to it literally across a batch broke correctness instead: every record after
    // the first compared against the state as of the batch start, so a 512-record
    // batch reported 511 phantom counter breaks. The first ingest run over real
    // spools showed 3,306 of them out of 3,823 frames.
    //
    // Applying inside the transaction is safe here because a rollback aborts the
    // whole run — the in-memory classifier is discarded with the process and the
    // next run rebuilds it from the spool cursor, which rolled back with the rows.
    // In-memory state that moved past a failed commit is therefore never observed.
    let committed = state
        .store
        .transaction(|store| {
            let mut facts = Vec::with_capacity(batch.len());
            for (start, line) in batch {
                // The exact bytes go down *before* anything parses them. If the
                // parse then fails the evidence still exists and the schema can be
                // revised; parsing first would let a misread discard a frame that
                // cannot be re-collected.
                let parsed = EnvelopeView::parse(line);
                let content: Vec<u8> = match &parsed {
                    Ok(view) => view.raw_payload.as_bytes().to_vec(),
                    Err(_) => line.clone(),
                };
                let captured = store.capture_raw(lane, key, *start, line, &content)?;

                match parsed {
                    Ok(view) => {
                        state.tracker.observe_envelope(&view);
                        // Identity is exact within this UTC ingest partition and
                        // disk-backed. It resets only at a committed rollover;
                        // lookup and first insert share this transaction with the
                        // fact and spool cursor.
                        let identity = store.record_identity(view.record_id.as_str())?;
                        let verdict = match identity {
                            None => IdentityVerdict::Unseen,
                            Some(original) if original.content == captured.content_hash() => {
                                IdentityVerdict::Duplicate {
                                    original: original.first_seen,
                                }
                            }
                            Some(original) => IdentityVerdict::Conflict {
                                original: original.first_seen,
                                original_content: original.content,
                            },
                        };
                        let first_observation = matches!(verdict, IdentityVerdict::Unseen);
                        let fact = state
                            .classifier
                            .classify_with_identity(&view, &captured, verdict);
                        let committed = store.commit_fact(&captured, fact)?;
                        if first_observation {
                            store.remember_record_identity(
                                view.record_id.as_str(),
                                captured.content_hash(),
                                committed.seq(),
                            )?;
                        }
                        state.classifier.apply(&committed);
                        facts.push(committed);
                        accepted += 1;
                    }
                    Err(error) => {
                        // Recorded, not dropped. A line we cannot parse is still
                        // the only copy that will ever exist, and the reason it
                        // failed is usually a schema question, not corruption.
                        rejected.push(format!("{key}@{start}: {error}"));
                    }
                }
            }
            store.advance_spool_cursor(key, lane, offset, records_before + accepted)?;
            Ok(facts)
        })
        .map_err(|error: StoreError| error.to_string())?;

    for fact in &committed {
        *tally.causes.entry(fact.value().cause.label()).or_insert(0) += 1;
        tally.captured += 1;
    }
    tally.rejected += rejected.len() as u64;
    for line in rejected {
        if tally.first_rejections.len() < 5 {
            tally.first_rejections.push(line);
        }
    }
    Ok(accepted)
}

fn render_report(
    arguments: &Arguments,
    files: &[(String, PathBuf)],
    tally: &Tally,
    partition: &OpenPartition,
    integrity: Option<&IntegrityReport>,
) -> String {
    let state = partition.classifier.state();

    let causes: Vec<String> = tally
        .causes
        .iter()
        .map(|(name, count)| format!("    \"{name}\": {count}"))
        .collect();

    let epochs: Vec<String> = state
        .epochs
        .iter()
        .map(|(key, epoch)| {
            format!(
                "    {{\"venue\": \"{}\", \"stream\": \"{}\", \"epoch\": \"{}\", \
                 \"health\": \"{:?}\", \"frames\": {}, \"proven_gaps\": {}, \
                 \"backwards\": {}, \"local_breaks\": {}}}",
                key.lane.venue,
                key.lane.stream,
                key.epoch,
                epoch.health,
                epoch.frames,
                epoch.proven_gaps,
                epoch.backwards,
                epoch.local_breaks,
            )
        })
        .collect();

    let rejections: Vec<String> = tally
        .first_rejections
        .iter()
        .map(|line| format!("    {line:?}"))
        .collect();

    let integrity_line = match integrity {
        Some(report) => format!(
            ",\n  \"integrity\": {{\"checked\": {}, \"clean\": {}, \"dense\": {}, \
             \"mismatches\": {:?}}}",
            report.checked,
            report.is_clean(),
            report.dense,
            report.mismatches
        ),
        None => String::new(),
    };
    let migration = partition
        .store
        .migration_report()
        .map(|migration| {
            format!(
                "{{\"from_schema\": {}, \"to_schema\": {}, \"identity_records\": {}, \
                 \"elapsed_seconds\": {:.3}}}",
                migration.from_schema,
                migration.to_schema,
                migration.identity_records,
                migration.elapsed.as_secs_f64(),
            )
        })
        .unwrap_or_else(|| "null".to_owned());

    format!(
        "{{\n  \"spool_root\": {:?},\n  \"store\": {:?},\n  \"partition_date\": {:?},\n  \
         \"closed_partitions\": {},\n  \"identity_scope\": {:?},\n  \"spool_files\": {},\n  \
         \"segments_already_ingested\": {},\n  \"lines_read\": {},\n  \"already_ingested\": {},\n  \
         \"facts_committed\": {},\n  \
         \"unparseable_recorded\": {},\n  \"evidence_rows\": {},\n  \"fact_rows\": {},\n  \
         \"duplicates\": {},\n  \"conflicts\": {},\n  \"identity_records_in_memory\": {},\n  \
         \"store_migration\": {},\n  \
         \"causes\": {{\n{}\n  }},\n  \
         \"epochs\": [\n{}\n  ],\n  \"rejections\": [\n{}\n  ]{}\n}}",
        arguments.spool_root.display().to_string(),
        arguments.store_dir.display().to_string(),
        partition.date(),
        partition.receipt_count(),
        IDENTITY_SCOPE,
        files.len(),
        tally.skipped_segments,
        tally.read,
        tally.skipped_resumed,
        tally.captured,
        tally.rejected,
        partition.store.evidence_count().unwrap_or(0),
        partition.store.fact_count().unwrap_or(0),
        state.duplicates,
        state.conflicts,
        partition.classifier.retained_identity_count(),
        migration,
        causes.join(",\n"),
        epochs.join(",\n"),
        rejections.join(",\n"),
        integrity_line,
    )
}

fn unix_now_ns() -> Result<u64, String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("reading UTC clock: {error}"))?
        .as_nanos();
    u64::try_from(nanos).map_err(|_| "UTC clock is outside the supported range".to_owned())
}
