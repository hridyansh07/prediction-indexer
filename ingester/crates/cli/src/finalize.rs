//! `indexer-finalize` — merges sealed windows into canonical evidence.
//!
//! ```text
//! indexer-finalize <spool-root> <canonical-root> --expect-lane polymarket [--expect-lane ...]
//! indexer-finalize --print-lane-ranks
//! ```
//!
//! A **separate command** from `indexer-ingest`, per
//! `docs/SEALED_CAPTURE_PIPELINE_V1.md` §8.3. The two assign different global
//! orders over the same bytes and both remain available:
//!
//! ```text
//! indexer-ingest    file_order      filename order, whole files consumed atomically
//! indexer-finalize  EvidenceSeq     (visible_ns, lane_rank, delivery_index)
//! ```
//!
//! Neither is venue event order. `EvidenceSeq` is capture observation order at
//! one host: two venues may act on the same world event milliseconds apart and
//! be recorded in the opposite order by routing and stamping alone.
//!
//! `--expect-lane` is required and is the deployment's declaration, not this
//! build's opinion. §3: "The deployment manifest defines which lanes are
//! expected; a disabled Kalshi profile is not waited on." Without a declared
//! expectation, `lane_missing` cannot mean anything — a lane that dies
//! permanently would simply stop being expected, and every window after its
//! death would read complete.

use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use std::collections::BTreeMap;

use indexer_finalize::{
    RootLease, WindowOutcome, advance_delivery_continuity, assemble, committed_windows,
    create_dir_all_durable, delivery_continuity, finalize_window, late_segments, ranks_as_json,
    ready_windows, supported_lanes,
    watermark::{self, Carried, Watermark},
    window::{tile_absent_windows, validate_window_seconds},
};

const USAGE: &str = "usage: indexer-finalize <spool-root> <canonical-root> \
                     --expect-lane <lane> [--expect-lane <lane>]... \
                     [--window-seconds <seconds>] \
                     [--finalization-deadline-seconds <seconds>] \
                     [--interval-seconds <seconds>] [--report <path>]\n       \
                     indexer-finalize --print-lane-ranks";

/// The window period, which must match the splices' `SEGMENT_SECONDS`.
///
/// **The authority for every window's bounds.** Seals declare their own, but a
/// declaration is not an authority: a torn one leaves no end at all, a stray
/// 60-minute seal can redefine the tiling and hide a real 30-minute window, and
/// a seal naming a window its own records fall outside of still validates. With
/// the period configured, bounds are computed from the aligned start and every
/// seal is checked against them.
const DEFAULT_WINDOW_SECONDS: u64 = 1800;

/// How long a window waits for a lane that has not sealed yet.
///
/// Finite by design. §5 commits the available lanes when it expires and marks
/// the receipt incomplete rather than stalling: one wedged splice must not halt
/// finalization for every healthy venue.
const DEFAULT_DEADLINE_SECONDS: u64 = 300;

struct Arguments {
    spool_root: PathBuf,
    canonical_root: PathBuf,
    expected: Vec<String>,
    deadline_seconds: u64,
    period_ns: u64,
    interval_seconds: Option<u64>,
    report: Option<PathBuf>,
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            std::process::ExitCode::from(1)
        }
    }
}

fn parse_arguments() -> Result<Option<Arguments>, String> {
    let mut positional = Vec::new();
    let mut expected: Vec<String> = Vec::new();
    let mut deadline_seconds = DEFAULT_DEADLINE_SECONDS;
    let mut window_seconds = DEFAULT_WINDOW_SECONDS;
    let mut interval_seconds = None;
    let mut report = None;
    let mut arguments = std::env::args().skip(1);

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--print-lane-ranks" => {
                println!("{}", ranks_as_json());
                return Ok(None);
            }
            "--expect-lane" => {
                let lane = arguments
                    .next()
                    .ok_or_else(|| format!("--expect-lane needs a value\n{USAGE}"))?;
                if expected.contains(&lane) {
                    return Err(format!("--expect-lane {lane:?} was given twice\n{USAGE}"));
                }
                expected.push(lane);
            }
            "--window-seconds" => {
                let raw = arguments
                    .next()
                    .ok_or_else(|| format!("--window-seconds needs a value\n{USAGE}"))?;
                window_seconds = raw
                    .parse::<u64>()
                    .map_err(|_| format!("invalid --window-seconds value: {raw:?}\n{USAGE}"))?;
            }
            "--finalization-deadline-seconds" => {
                let raw = arguments.next().ok_or_else(|| {
                    format!("--finalization-deadline-seconds needs a value\n{USAGE}")
                })?;
                deadline_seconds = raw.parse::<u64>().map_err(|_| {
                    format!("invalid --finalization-deadline-seconds value: {raw:?}\n{USAGE}")
                })?;
            }
            "--interval-seconds" => {
                let raw = arguments
                    .next()
                    .ok_or_else(|| format!("--interval-seconds needs a value\n{USAGE}"))?;
                let value = raw
                    .parse::<u64>()
                    .map_err(|_| format!("invalid --interval-seconds value: {raw:?}\n{USAGE}"))?;
                if value == 0 {
                    return Err(format!("--interval-seconds must be positive\n{USAGE}"));
                }
                interval_seconds = Some(value);
            }
            "--report" => {
                let raw = arguments
                    .next()
                    .ok_or_else(|| format!("--report needs a value\n{USAGE}"))?;
                report = Some(PathBuf::from(raw));
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
    if expected.is_empty() {
        return Err(format!(
            "at least one --expect-lane is required: a completeness verdict is only \
             meaningful against a declared expectation\n{USAGE}"
        ));
    }
    // A typo here would silently expect a lane that can never arrive, so every
    // window would sit out its deadline and commit incomplete forever.
    let supported = supported_lanes();
    if let Some(unknown) = expected
        .iter()
        .find(|lane| !supported.contains(&lane.as_str()))
    {
        return Err(format!(
            "--expect-lane {unknown:?} is not a lane this build can rank; known lanes are {}\n{USAGE}",
            supported.join(", ")
        ));
    }

    // Deliberately not sorted. The receipt records this list verbatim, and a
    // verdict is read against the expectation that produced it — reordering it
    // would mean the receipt describes an invocation that never happened.
    Ok(Some(Arguments {
        spool_root: PathBuf::from(&positional[0]),
        canonical_root: PathBuf::from(&positional[1]),
        expected,
        deadline_seconds,
        period_ns: validate_window_seconds(window_seconds)?,
        interval_seconds,
        report,
    }))
}

fn run() -> Result<(), String> {
    let Some(arguments) = parse_arguments()? else {
        return Ok(());
    };

    // Durable from the root down. `create_dir_all` leaves the link to a new
    // directory in its parent's page cache, so on a first deployment a crash
    // could take the whole canonical tree — receipts included — while every
    // fsync inside it had succeeded.
    create_dir_all_durable(&arguments.canonical_root)?;

    // Held for the whole run and released on the way out, including on the error
    // paths below — two finalizers over one root race over the same files.
    let _lease = RootLease::acquire(&arguments.canonical_root)?;

    let Some(interval_seconds) = arguments.interval_seconds else {
        return recorded_sweep(&arguments);
    };
    let stopping = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&stopping))
        .map_err(|error| format!("installing SIGTERM handler: {error}"))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&stopping))
        .map_err(|error| format!("installing SIGINT handler: {error}"))?;
    while !stopping.load(Ordering::Relaxed) {
        recorded_sweep(&arguments)?;
        let deadline = Instant::now() + Duration::from_secs(interval_seconds);
        while !stopping.load(Ordering::Relaxed) && Instant::now() < deadline {
            thread::sleep(
                Duration::from_secs(1).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
    Ok(())
}

fn recorded_sweep(arguments: &Arguments) -> Result<(), String> {
    match sweep_once(arguments) {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(path) = &arguments.report {
                let failed_at_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_nanos() as u64)
                    .unwrap_or(0);
                let failure = serde_json::json!({
                    "status": "error",
                    "spool_root": arguments.spool_root.display().to_string(),
                    "canonical_root": arguments.canonical_root.display().to_string(),
                    "failed_at_ns": failed_at_ns,
                    "error": error,
                });
                if let Err(report_error) = write_report(path, &failure) {
                    return Err(format!(
                        "{error}; additionally failed to write failure report: {report_error}"
                    ));
                }
            }
            Err(error)
        }
    }
}

fn write_report(path: &std::path::Path, value: &serde_json::Value) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("report path {} has no parent", path.display()))?;
    create_dir_all_durable(parent)?;
    let name = path
        .file_name()
        .ok_or_else(|| format!("report path {} has no file name", path.display()))?
        .to_string_lossy();
    indexer_finalize::canonical::write_json_durable(parent, &name, value)
}

fn sweep_once(arguments: &Arguments) -> Result<(), String> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("reading the clock: {error}"))?
        .as_nanos() as u64;

    let assembly = assemble(&arguments.spool_root, arguments.period_ns)?;
    let mut windows = assembly.windows;

    // The watermark is derived state checked against the receipts it derives
    // from; where they disagree it is rebuilt, because the receipts are the
    // authority. Loading it verifies every receipt, so a corrupt one still stops
    // the run here.
    let mut mark = watermark::load_or_rebuild(&arguments.canonical_root)?;
    let committed = committed_windows(&arguments.canonical_root)?;
    let committed_starts: std::collections::BTreeSet<u64> = committed.keys().copied().collect();
    let resume_after_ns = mark.as_ref().map(|mark| mark.last_window_end_ns);
    let mut delivery_continuity = delivery_continuity(&committed)?;

    // §5: a seal arriving after its window was committed is archived and
    // labelled, never merged in — it must not renumber committed positions.
    let mut late: Vec<String> = Vec::new();
    for (start, receipt) in &committed {
        if let Some(window) = windows
            .values()
            .find(|window| window.key.start_ns == *start)
        {
            late.extend(late_segments(window, receipt));
        }
    }

    // A window in which every lane was down produces no seal, and a scan of
    // seals therefore cannot see it. Tiling makes the hole explicit so a total
    // outage leaves an incomplete receipt rather than no trace at all.
    let synthesized = tile_absent_windows(
        &mut windows,
        arguments.period_ns,
        resume_after_ns,
        now_ns,
        arguments.deadline_seconds,
    )?;

    // A window whose start precedes the watermark cannot be given a coherent
    // position: every position after it is already assigned, so committing it
    // would run the canonical sequence backwards in visible time. It is removed
    // **before** anything is written — the previous behaviour published its
    // receipt first and reported the contradiction afterwards, which is the one
    // order that cannot be undone. §5's correction policy owns this case: a
    // versioned correction dataset with an explicit parent manifest, never an
    // insertion into a committed sequence.
    let behind: Vec<u64> = match &mark {
        Some(mark) => windows
            .keys()
            .map(|key| key.start_ns)
            .filter(|start| *start < mark.last_window_end_ns && !committed_starts.contains(start))
            .collect(),
        None => Vec::new(),
    };
    windows.retain(|key, _| !behind.contains(&key.start_ns));

    let mut validation_cache = BTreeMap::new();
    let ready = ready_windows(
        &windows,
        &committed_starts,
        &arguments.expected,
        now_ns,
        arguments.deadline_seconds,
        &mut validation_cache,
    );

    // Positions continue across windows and across restarts: the canonical
    // sequence is global, not per window.
    let mut next_seq = indexer_types::CanonicalSeq::new(
        mark.as_ref()
            .map(Watermark::next_canonical_seq)
            .unwrap_or(1),
    )
    .ok_or_else(|| "canonical sequence is not positive".to_owned())?;
    // Ordering history and per-lane boundaries travel window to window. Seeded
    // from the watermark so a restart continues rather than starting over —
    // without that, provenance bytes would depend on where a run stopped.
    let mut carried: Carried = mark
        .as_ref()
        .map(|mark| mark.carried.clone())
        .unwrap_or_default();

    let mut finalized = Vec::new();
    let mut deferred: Vec<String> = Vec::new();

    for (key, verdict, status) in &ready {
        let window = &windows[key];
        let mut status = status.clone();
        for (lane, detail) in delivery_continuity.check_window(window, &status) {
            status.invalidate(&lane, detail);
        }
        let outcome = finalize_window(
            &arguments.canonical_root,
            window,
            &status,
            &arguments.expected,
            verdict,
            arguments.deadline_seconds,
            next_seq,
            now_ns,
            &carried,
        )?;
        match outcome {
            WindowOutcome::Committed(window) => {
                advance_delivery_continuity(&mut delivery_continuity, &window.receipt)?;
                if let Some(last) = window.receipt.last_canonical_seq {
                    next_seq = indexer_types::CanonicalSeq::new(last + 1)
                        .ok_or_else(|| "canonical sequence is not positive".to_owned())?;
                }
                carried = window.receipt.carried.clone();
                // The watermark advances only after the window's own receipt is
                // durable, so it can never name a window that is not committed.
                let advanced = Watermark::advance(mark.as_ref(), &window);
                watermark::write(&arguments.canonical_root, &advanced)?;
                mark = Some(advanced);
                finalized.push(window.receipt);
            }
            WindowOutcome::Deferred {
                unsatisfied,
                until_ns,
            } => {
                // Nothing was written, and a window still inside its deadline
                // blocks every later one (§7).
                deferred.push(format!(
                    "    {{\"window_start_ns\": {}, \"unsatisfied\": {:?}, \"until_ns\": {until_ns}}}",
                    key.start_ns, unsatisfied
                ));
                break;
            }
        }
    }

    let waiting: Vec<String> = windows
        .iter()
        .filter(|(key, _)| {
            !committed_starts.contains(&key.start_ns)
                && !ready.iter().any(|(ready_key, _, _)| ready_key == *key)
        })
        .map(|(key, window)| {
            format!(
                "    {{\"window_start_ns\": {}, \"missing\": {:?}}}",
                key.start_ns,
                window.missing(&arguments.expected)
            )
        })
        .chain(deferred)
        .collect();

    // `{:?}` on an Option renders `Some(1)`, which is Rust and not JSON. The
    // report is consumed by tooling, so it has to be the latter.
    let json_number = |value: Option<i64>| {
        value
            .map(|number| number.to_string())
            .unwrap_or_else(|| "null".to_owned())
    };
    let committed_lines: Vec<String> = finalized
        .iter()
        .map(|receipt| {
            format!(
                "    {{\"window_start_ns\": {}, \"completeness\": {:?}, \"certified\": {}, \
                 \"records\": {}, \"first_canonical_seq\": {}, \"last_canonical_seq\": {}, \
                 \"present\": {:?}, \"missing\": {:?}, \"invalid\": {:?}, \
                 \"quarantined\": {}}}",
                receipt.window_start_ns,
                receipt.completeness,
                receipt.certified,
                receipt.evidence.decoded.line_count,
                json_number(receipt.first_canonical_seq),
                json_number(receipt.last_canonical_seq),
                receipt.present_lanes,
                receipt
                    .missing_lanes
                    .iter()
                    .map(|f| &f.lane)
                    .collect::<Vec<_>>(),
                receipt
                    .invalid_lanes
                    .iter()
                    .map(|f| &f.lane)
                    .collect::<Vec<_>>(),
                !receipt.clock_faults.is_empty(),
            )
        })
        .collect();

    let report = format!(
        "{{\n  \"spool_root\": {:?},\n  \"canonical_root\": {:?},\n  \
         \"expected_lanes\": {:?},\n  \"finalization_deadline_seconds\": {},\n  \
         \"windows_seen\": {},\n  \"windows_synthesized\": {},\n  \
         \"windows_finalized\": {},\n  \"windows_already_committed\": {},\n  \
         \"next_canonical_seq\": {},\n  \"quarantined_windows\": {},\n  \
         \"behind_watermark\": {:?},\n  \
         \"unplaceable_seals\": {:?},\n  \
         \"late_after_finalization\": {:?},\n  \
         \"finalized\": [\n{}\n  ],\n  \"waiting\": [\n{}\n  ]\n}}",
        arguments.spool_root.display().to_string(),
        arguments.canonical_root.display().to_string(),
        arguments.expected,
        arguments.deadline_seconds,
        windows.len(),
        synthesized,
        finalized.len(),
        committed_starts.len(),
        next_seq.get(),
        mark.as_ref()
            .map(|mark| mark.quarantined.len())
            .unwrap_or(0),
        behind,
        assembly.unplaceable_seals,
        late,
        committed_lines.join(",\n"),
        waiting.join(",\n"),
    );
    println!("{report}");
    std::io::Write::flush(&mut std::io::stdout())
        .map_err(|error| format!("flushing report: {error}"))?;
    if let Some(path) = &arguments.report {
        let value: serde_json::Value = serde_json::from_str(&report)
            .map_err(|error| format!("validating finalizer report JSON: {error}"))?;
        write_report(path, &value)?;
    }

    if !behind.is_empty() {
        return Err(format!(
            "{} window(s) start before the watermark at {}: {behind:?}; nothing was \
             written for them. A window behind the sequence cannot be inserted into \
             it — see the correction policy in SEALED_CAPTURE_PIPELINE_V1 §5.",
            behind.len(),
            mark.as_ref()
                .map(|mark| mark.last_window_end_ns)
                .unwrap_or(0),
        ));
    }
    if !assembly.unplaceable_seals.is_empty() {
        return Err(format!(
            "{} seal(s) could not be read or placed in a window: {:?}",
            assembly.unplaceable_seals.len(),
            assembly.unplaceable_seals
        ));
    }
    Ok(())
}
