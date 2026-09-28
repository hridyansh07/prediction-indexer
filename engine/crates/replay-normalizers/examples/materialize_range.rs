//! Private Stage-A operator helper. This is deliberately an example target,
//! not a supported installed CLI.

use canonical_normalizer::Normalize;
use indexer_finalize::{
    CertifiedPolicy, LowerBoundPolicy, SelectionPolicy, select_canonical_windows,
};
use indexer_types::Sha256;
use replay_domain::SEGMENT_SCHEMA_VERSION;
use replay_materialize::{
    DerivativeSpec, NormalizationPolicy, PinnedDerivative, ReadLimits, build_window,
    inspect_pinned, verify_derivative,
};
use replay_normalizers::{CanonicalNormalizer, CanonicalNormalizerIdentity};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

const MAX_WINDOWS: usize = 4096;
/// Matches the usual main-thread stack so a worker has the same headroom the
/// sequential build had.
const WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;
const POLICY_DOMAIN: &[u8] = b"prediction-indexer/replay-normalizers/materialize-range-policy/v1";

fn enforce_window_limit(count: usize) -> Result<(), String> {
    if count > MAX_WINDOWS {
        Err(format!(
            "selected {count} canonical windows; maximum is {MAX_WINDOWS}"
        ))
    } else {
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u16,
    canonical_root: PathBuf,
    output_root: PathBuf,
    start_ns: u64,
    end_ns: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Response {
    version: u16,
    normalizer: CanonicalNormalizerIdentity,
    derivatives: Vec<OutputPin>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OutputPin {
    window_start_ns: u64,
    window_end_ns: u64,
    derivative_address: String,
    receipt_sha256: Sha256,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectRequest {
    version: u16,
    directory: PathBuf,
    derivative_address: String,
    receipt_sha256: String,
}

fn describe() -> Result<String, String> {
    let normalizer = CanonicalNormalizer::default();
    serde_json::to_string(&json!({
        "materialization_policy_sha256": Sha256::digest(POLICY_DOMAIN),
        "materializer_version": 2,
        "normalized_schema_version": SEGMENT_SCHEMA_VERSION,
        "normalizer": normalizer.identity(),
        "producer_identity_version": 1,
    }))
    .map_err(|error| format!("serializing producer: {error}"))
}

fn inspect(input: &str) -> Result<String, String> {
    if input.len() > 1_048_576 {
        return Err("inspection request exceeds 1 MiB".into());
    }
    let request: InspectRequest = serde_json::from_str(input)
        .map_err(|error| format!("invalid inspection request: {error}"))?;
    if request.version != 1 {
        return Err("unsupported inspection request version".into());
    }
    let receipt_sha256 = Sha256::from_hex(&request.receipt_sha256)
        .map_err(|error| format!("invalid receipt_sha256: {error}"))?;
    let expected = BTreeSet::from([
        "events.ndjson.zst".to_owned(),
        "rejects.ndjson.zst".to_owned(),
        "sources.ndjson.zst".to_owned(),
        "manifest.json".to_owned(),
        "receipt.json".to_owned(),
    ]);
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(&request.directory).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if !entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_file()
        {
            return Err("derivative directory contains a non-regular entry".into());
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "derivative filename is not UTF-8")?;
        actual.insert(name);
    }
    if actual != expected {
        return Err("derivative directory does not contain the exact file allowlist".into());
    }
    let verified = verify_derivative(&request.directory)?;
    if verified.pin.derivative_address != request.derivative_address
        || verified.pin.receipt_sha256 != receipt_sha256
    {
        return Err("verified derivative does not match requested pin".into());
    }
    serde_json::to_string(&json!({
        "derivative_address": verified.pin.derivative_address,
        "receipt_sha256": verified.pin.receipt_sha256,
        "version": 1,
    }))
    .map_err(|error| format!("serializing inspection response: {error}"))
}

/// Builds, verifies, and pins one exact canonical window with a fresh
/// normalizer. Nothing carries across windows, so windows are independent.
fn build_one(
    canonical_root: &Path,
    output_root: &Path,
    start: u64,
    end: u64,
) -> Result<OutputPin, String> {
    let mut normalizer = CanonicalNormalizer::default();
    let descriptor = normalizer.descriptor().clone();
    let spec = DerivativeSpec {
        normalized_schema_version: SEGMENT_SCHEMA_VERSION,
        normalizer_bundle_sha256: descriptor.bundle_sha256,
        normalizer_config_sha256: descriptor.config_sha256,
        policy: NormalizationPolicy {
            policy_sha256: Sha256::digest(POLICY_DOMAIN),
            effective_from_ns: 0,
            effective_until_ns: None,
        },
    };
    let built = build_window(
        canonical_root,
        output_root,
        start,
        end,
        &spec,
        &mut normalizer,
    )
    .map_err(|error| error.to_string())?;
    let input = PinnedDerivative {
        directory: built.derivative.directory.clone(),
        pin: built.derivative.pin.clone(),
    };
    let inspected = inspect_pinned(&input, &ReadLimits::default())?;
    if !inspected.supports_source_evidence()
        || inspected.manifest().materializer_version != 2
        || inspected.manifest().normalizer_bundle_sha256 != descriptor.bundle_sha256
        || inspected.manifest().normalizer_config_sha256 != descriptor.config_sha256
    {
        return Err("materialized pin is not profile 2 with the composite descriptor".into());
    }
    Ok(OutputPin {
        window_start_ns: start,
        window_end_ns: end,
        derivative_address: input.pin.derivative_address,
        receipt_sha256: input.pin.receipt_sha256,
    })
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// Builds `windows` on up to `workers` threads and returns their pins in
/// window order. Workers claim windows in ascending order from a shared index,
/// so when any window fails, every lower window has already been claimed and
/// runs to completion: returning the lowest failing index reports exactly the
/// error a sequential build would have reported first. After a failure no
/// worker claims another window.
fn build_windows(
    canonical_root: &Path,
    output_root: &Path,
    windows: &[(u64, u64)],
    workers: usize,
) -> Result<Vec<OutputPin>, String> {
    build_windows_with(windows, workers, |start, end| {
        build_one(canonical_root, output_root, start, end)
    })
}

/// The scheduling core of [`build_windows`], generic over the per-window build
/// so tests can inject failures and panics.
fn build_windows_with<T, F>(
    windows: &[(u64, u64)],
    workers: usize,
    build: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(u64, u64) -> Result<T, String> + Sync,
{
    let workers = workers.clamp(1, windows.len().max(1));
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let run = |index: usize| -> Result<T, String> {
        let (start, end) = windows[index];
        panic::catch_unwind(AssertUnwindSafe(|| build(start, end))).unwrap_or_else(|payload| {
            Err(format!(
                "materializing window {start}..{end} panicked: {}",
                panic_message(payload.as_ref())
            ))
        })
    };
    let worker = || {
        let mut results = Vec::new();
        loop {
            if failed.load(Ordering::SeqCst) {
                break;
            }
            let index = next.fetch_add(1, Ordering::SeqCst);
            if index >= windows.len() {
                break;
            }
            let result = run(index);
            if result.is_err() {
                failed.store(true, Ordering::SeqCst);
            }
            results.push((index, result));
        }
        results
    };
    let mut slots: Vec<Option<Result<T, String>>> = std::iter::repeat_with(|| None)
        .take(windows.len())
        .collect();
    thread::scope(|scope| -> Result<(), String> {
        let mut handles = Vec::with_capacity(workers);
        for number in 0..workers {
            let handle = thread::Builder::new()
                .name(format!("materialize-{number}"))
                .stack_size(WORKER_STACK_BYTES)
                .spawn_scoped(scope, worker);
            match handle {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    // Stop the workers already running; they finish in flight.
                    failed.store(true, Ordering::SeqCst);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(format!("spawning materializer worker: {error}"));
                }
            }
        }
        let mut joined = Ok(());
        for handle in handles {
            match handle.join() {
                Ok(results) => {
                    for (index, result) in results {
                        slots[index] = Some(result);
                    }
                }
                Err(payload) => {
                    joined = Err(format!(
                        "materializer worker panicked: {}",
                        panic_message(payload.as_ref())
                    ));
                }
            }
        }
        joined
    })?;
    // Slots are in window order, so the first error is the earliest window's.
    if let Some(error) = slots.iter().flatten().find_map(|slot| slot.as_ref().err()) {
        return Err(error.clone());
    }
    slots
        .into_iter()
        .map(|slot| match slot {
            Some(Ok(pin)) => Ok(pin),
            _ => Err("materializer worker left a window unbuilt".to_owned()),
        })
        .collect()
}

fn execute(input: &str) -> Result<String, String> {
    // Honors cgroup CPU quotas on Linux; clamped to the window count later.
    let workers = thread::available_parallelism().map_or(1, usize::from);
    execute_with_workers(input, workers)
}

fn execute_with_workers(input: &str, workers: usize) -> Result<String, String> {
    if input.len() > 1_048_576 {
        return Err("request exceeds 1 MiB".into());
    }
    let request: Request =
        serde_json::from_str(input).map_err(|error| format!("invalid request: {error}"))?;
    if request.version != 1 {
        return Err("unsupported request version".to_owned());
    }
    if request.start_ns >= request.end_ns {
        return Err("start_ns must be less than end_ns".to_owned());
    }
    let selection = select_canonical_windows(
        &request.canonical_root,
        request.start_ns,
        request.end_ns,
        SelectionPolicy {
            certified: CertifiedPolicy::AllowUncertified,
            lower_bound: LowerBoundPolicy::Clip,
        },
    )?;
    let windows = selection
        .receipt_identities()
        .map(|receipt| (receipt.window_start_ns, receipt.window_end_ns))
        .collect::<Vec<_>>();
    enforce_window_limit(windows.len())?;

    let identity = CanonicalNormalizer::default().identity().clone();
    let pins = build_windows(
        &request.canonical_root,
        &request.output_root,
        &windows,
        workers,
    )?;
    let response = Response {
        version: 1,
        normalizer: identity,
        derivatives: pins,
    };
    let value =
        serde_json::to_value(response).map_err(|error| format!("serializing response: {error}"))?;
    serde_json::to_string(&value).map_err(|error| format!("serializing response: {error}"))
}

fn main() {
    let mode = std::env::args().nth(1);
    if mode.as_deref() == Some("--describe") {
        match describe() {
            Ok(output) => {
                println!("{output}");
                return;
            }
            Err(error) => {
                eprintln!("materialize_range: {}", error.replace(['\n', '\r'], " "));
                std::process::exit(1);
            }
        }
    }
    let mut input = String::new();
    let read = io::stdin()
        .take(1_048_577)
        .read_to_string(&mut input)
        .map_err(|error| format!("reading stdin: {error}"));
    let result = read.and_then(|_| match mode.as_deref() {
        None => execute(&input),
        Some("--inspect-pin") => inspect(&input),
        Some(_) => Err("unknown argument".into()),
    });
    match result {
        Ok(output) => println!("{output}"),
        Err(error) => {
            let diagnostic = error.replace(['\n', '\r'], " ");
            eprintln!("materialize_range: {diagnostic}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_finalize::{
        CanonicalOutput, CompressionContract, DecodedIdentity, Receipt, StoredIdentity,
        window_directory,
    };
    use prediction_encoder::{DEFAULT_ZSTD_LEVEL, encode_stream, encoder_version};
    use std::fs;
    use std::io::Cursor;
    use std::path::Path;
    use tempdir::TempDir;

    fn output(directory: &Path, name: &str) -> CanonicalOutput {
        let mut stored = Vec::new();
        let encoded = encode_stream(Cursor::new([]), &mut stored, DEFAULT_ZSTD_LEVEL).unwrap();
        fs::write(directory.join(name), stored).unwrap();
        CanonicalOutput {
            file: name.to_owned(),
            content_encoding: "zstd".to_owned(),
            decoded: DecodedIdentity {
                byte_length: encoded.logical.byte_length,
                line_count: encoded.logical.line_count,
                sha256: encoded.logical.sha256,
            },
            stored: StoredIdentity {
                byte_length: encoded.stored.byte_length,
                sha256: encoded.stored.sha256,
            },
            compression: CompressionContract {
                algorithm: "zstd".to_owned(),
                level: DEFAULT_ZSTD_LEVEL,
                frame_checksum: true,
                dictionary: None,
                frame_count: 1,
                encoder: encoder_version(),
            },
        }
    }

    fn canonical_window(root: &Path, start: u64, end: u64) {
        let directory = window_directory(root, start);
        fs::create_dir_all(&directory).unwrap();
        let receipt = Receipt {
            receipt_version: 1,
            window_start_ns: start,
            window_end_ns: end,
            completeness: "complete".to_owned(),
            certified: true,
            expected_lanes: vec![],
            present_lanes: vec![],
            unexpected_lanes: vec![],
            missing_lanes: vec![],
            invalid_lanes: vec![],
            finalization_deadline_seconds: 300,
            deadline_expired: false,
            finalized_at_ns: end,
            inputs: vec![],
            evidence: output(&directory, "evidence.ndjson.zst"),
            provenance: output(&directory, "provenance.ndjson.zst"),
            first_canonical_seq: None,
            last_canonical_seq: None,
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

    fn request(canonical: &Path, output: &Path, start: u64, end: u64) -> String {
        serde_json::json!({
            "version":1,"canonical_root":canonical,"output_root":output,
            "start_ns":start,"end_ns":end,
        })
        .to_string()
    }

    fn snapshot(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut files = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    pending.push(entry.path());
                } else {
                    files.push((
                        entry.path().strip_prefix(root).unwrap().to_path_buf(),
                        fs::read(entry.path()).unwrap(),
                    ));
                }
            }
        }
        files.sort_by(|left, right| left.0.cmp(&right.0));
        files
    }

    #[test]
    fn request_is_closed_and_window_limit_keeps_the_existing_boundary() {
        assert_eq!(
            execute(&" ".repeat(1_048_577)).unwrap_err(),
            "request exceeds 1 MiB"
        );
        assert!(
            serde_json::from_str::<Request>(
                r#"{"version":1,"canonical_root":"a","output_root":"b","start_ns":0,"end_ns":1}"#
            )
            .is_ok()
        );
        assert!(serde_json::from_str::<Request>(
            r#"{"version":1,"canonical_root":"a","output_root":"b","start_ns":0,"end_ns":1,"extra":true}"#
        )
        .is_err());
        assert!(enforce_window_limit(4096).is_ok());
        assert_eq!(
            enforce_window_limit(4097).unwrap_err(),
            "selected 4097 canonical windows; maximum is 4096"
        );
        assert_eq!(
            execute(
                r#"{"version":1,"canonical_root":"a","output_root":"b","start_ns":1,"end_ns":1}"#
            )
            .unwrap_err(),
            "start_ns must be less than end_ns"
        );
    }

    #[test]
    fn describe_is_exact_canonical_producer_json() {
        let output = describe().unwrap();
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(serde_json::to_string(&value).unwrap(), output);
        assert_eq!(value["producer_identity_version"], 1);
        assert_eq!(value["normalized_schema_version"], SEGMENT_SCHEMA_VERSION);
        assert_eq!(value["materializer_version"], 2);
        assert_eq!(
            value["materialization_policy_sha256"],
            Sha256::digest(POLICY_DOMAIN).as_hex()
        );
        assert_eq!(
            value["normalizer"],
            json!(CanonicalNormalizer::default().identity())
        );
    }

    #[test]
    fn minimal_adjacent_selection_is_verified_ordered_noop_and_source_read_only() {
        let canonical = TempDir::new("helper-canonical").unwrap();
        let output = TempDir::new("helper-output").unwrap();
        for (start, end) in [(0, 10), (10, 20), (20, 30)] {
            canonical_window(canonical.path(), start, end);
        }
        let before = snapshot(canonical.path());
        let input = request(canonical.path(), output.path(), 5, 25);
        let first = execute(&input).unwrap();
        let response: Response = serde_json::from_str(&first).unwrap();
        assert_eq!(
            response
                .derivatives
                .iter()
                .map(|pin| (pin.window_start_ns, pin.window_end_ns))
                .collect::<Vec<_>>(),
            [(0, 10), (10, 20), (20, 30)]
        );
        assert_eq!(execute(&input).unwrap(), first);
        let maximum = format!("{input}{}", " ".repeat(1_048_576 - input.len()));
        assert_eq!(execute(&maximum).unwrap(), first);
        assert_eq!(
            execute(&(maximum + " ")).unwrap_err(),
            "request exceeds 1 MiB"
        );
        assert_eq!(snapshot(canonical.path()), before);

        let pin = &response.derivatives[0];
        let inspection = json!({
            "version": 1,
            "directory": output.path().join(format!("window={}", pin.window_start_ns)).join(&pin.derivative_address),
            "derivative_address": pin.derivative_address,
            "receipt_sha256": pin.receipt_sha256,
        })
        .to_string();
        let inspected: serde_json::Value =
            serde_json::from_str(&inspect(&inspection).unwrap()).unwrap();
        assert_eq!(inspected["derivative_address"], pin.derivative_address);
        assert_eq!(inspected["receipt_sha256"], pin.receipt_sha256.as_hex());
        assert!(inspect(&inspection.replace(&pin.derivative_address, &"f".repeat(64))).is_err());

        let directory = output
            .path()
            .join(format!("window={}", pin.window_start_ns))
            .join(&pin.derivative_address);
        let events_path = directory.join("events.ndjson.zst");
        let events = fs::read(&events_path).unwrap();
        fs::write(&events_path, [events.as_slice(), b"trailing"].concat()).unwrap();
        assert!(inspect(&inspection).is_err());
        fs::write(&events_path, events).unwrap();

        let receipt_path = directory.join("receipt.json");
        let receipt = fs::read(&receipt_path).unwrap();
        let noncanonical = [b" ".as_slice(), receipt.as_slice()].concat();
        fs::write(&receipt_path, &noncanonical).unwrap();
        let changed_hash = Sha256::digest(&noncanonical).as_hex();
        let changed_request = inspection.replace(&pin.receipt_sha256.as_hex(), &changed_hash);
        assert!(inspect(&changed_request).is_err());
        fs::write(&receipt_path, receipt).unwrap();
    }

    #[test]
    fn absence_gap_overlap_and_missing_local_object_fail_before_stdout_value() {
        let output = TempDir::new("helper-output").unwrap();

        let absent = TempDir::new("helper-absent").unwrap();
        assert!(execute(&request(absent.path(), output.path(), 0, 10)).is_err());

        let gap = TempDir::new("helper-gap").unwrap();
        canonical_window(gap.path(), 0, 10);
        canonical_window(gap.path(), 20, 30);
        assert!(execute(&request(gap.path(), output.path(), 0, 30)).is_err());

        let overlap = TempDir::new("helper-overlap").unwrap();
        canonical_window(overlap.path(), 0, 15);
        canonical_window(overlap.path(), 10, 20);
        assert!(execute(&request(overlap.path(), output.path(), 0, 20)).is_err());

        let missing = TempDir::new("helper-missing-object").unwrap();
        canonical_window(missing.path(), 0, 10);
        fs::remove_file(window_directory(missing.path(), 0).join("evidence.ndjson.zst")).unwrap();
        assert!(execute(&request(missing.path(), output.path(), 0, 10)).is_err());
    }

    const PARALLEL_WINDOWS: [(u64, u64); 5] = [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50)];

    fn parallel_canonical() -> TempDir {
        let canonical = TempDir::new("helper-parallel-canonical").unwrap();
        for (start, end) in PARALLEL_WINDOWS {
            canonical_window(canonical.path(), start, end);
        }
        canonical
    }

    fn corrupt(canonical: &Path, start: u64, name: &str) -> Vec<u8> {
        let path = window_directory(canonical, start).join(name);
        let original = fs::read(&path).unwrap();
        fs::write(&path, vec![0xa5; original.len()]).unwrap();
        original
    }

    fn committed(output: &Path, start: u64) -> bool {
        let window = output.join(format!("window={start}"));
        window.is_dir()
            && fs::read_dir(window)
                .unwrap()
                .map(|entry| entry.unwrap().path().join("receipt.json"))
                .any(|receipt| receipt.is_file())
    }

    #[test]
    fn parallel_build_is_byte_identical_to_sequential_build() {
        let canonical = parallel_canonical();
        let before = snapshot(canonical.path());
        let sequential_output = TempDir::new("helper-sequential-output").unwrap();
        let sequential = execute_with_workers(
            &request(canonical.path(), sequential_output.path(), 0, 50),
            1,
        )
        .unwrap();
        for workers in [2, 4, 64] {
            let parallel_output = TempDir::new("helper-parallel-output").unwrap();
            let input = request(canonical.path(), parallel_output.path(), 0, 50);
            let parallel = execute_with_workers(&input, workers).unwrap();
            assert_eq!(parallel, sequential, "{workers} workers");
            assert_eq!(
                snapshot(parallel_output.path()),
                snapshot(sequential_output.path()),
                "{workers} workers"
            );
            // A parallel retry over committed windows is the verify/no-op path.
            assert_eq!(execute_with_workers(&input, workers).unwrap(), sequential);
        }
        let response: Response = serde_json::from_str(&sequential).unwrap();
        assert_eq!(
            response
                .derivatives
                .iter()
                .map(|pin| (pin.window_start_ns, pin.window_end_ns))
                .collect::<Vec<_>>(),
            PARALLEL_WINDOWS
        );
        assert_eq!(snapshot(canonical.path()), before);
    }

    #[test]
    fn parallel_failure_reports_the_earliest_failing_window() {
        let clean = parallel_canonical();
        let clean_output = TempDir::new("helper-clean-output").unwrap();
        let expected =
            execute_with_workers(&request(clean.path(), clean_output.path(), 0, 50), 1).unwrap();

        // Window 10 and window 30 both fail, with distinguishable errors.
        let failing = [(10, "evidence.ndjson.zst"), (30, "provenance.ndjson.zst")];
        let canonical = parallel_canonical();
        let originals =
            failing.map(|(start, name)| (start, name, corrupt(canonical.path(), start, name)));
        let errors = failing.map(|(start, name)| {
            let only = parallel_canonical();
            corrupt(only.path(), start, name);
            let output = TempDir::new("helper-single-failure").unwrap();
            execute_with_workers(&request(only.path(), output.path(), 0, 50), 1).unwrap_err()
        });
        assert_ne!(errors[0], errors[1]);
        let sequential_output = TempDir::new("helper-sequential-failure").unwrap();
        assert_eq!(
            execute_with_workers(
                &request(canonical.path(), sequential_output.path(), 0, 50),
                1
            )
            .unwrap_err(),
            errors[0]
        );

        for workers in [2, 5] {
            let output = TempDir::new("helper-parallel-failure").unwrap();
            let input = request(canonical.path(), output.path(), 0, 50);
            assert_eq!(
                execute_with_workers(&input, workers).unwrap_err(),
                errors[0],
                "{workers} workers"
            );
            // Lower windows were claimed first and ran to completion; failing
            // windows publish no receipt.
            assert!(committed(output.path(), 0), "{workers} workers");
            assert!(!committed(output.path(), 10), "{workers} workers");
            assert!(!committed(output.path(), 30), "{workers} workers");
        }

        // Repair, then retry into the partially committed root: committed
        // windows verify/no-op and the response matches a clean build.
        let output = TempDir::new("helper-parallel-retry").unwrap();
        let input = request(canonical.path(), output.path(), 0, 50);
        execute_with_workers(&input, 5).unwrap_err();
        for (start, name, bytes) in originals {
            fs::write(window_directory(canonical.path(), start).join(name), bytes).unwrap();
        }
        assert_eq!(execute_with_workers(&input, 5).unwrap(), expected);
        assert_eq!(snapshot(output.path()), snapshot(clean_output.path()));
    }

    #[test]
    fn scheduler_keeps_order_stops_after_failure_and_surfaces_panics() {
        let windows: Vec<(u64, u64)> = (0..32).map(|index| (index * 10, index * 10 + 10)).collect();
        for workers in [1, 3, 8, 64] {
            let built = build_windows_with(&windows, workers, |start, end| {
                thread::sleep(std::time::Duration::from_millis((start / 10) % 3));
                Ok((start, end))
            })
            .unwrap();
            assert_eq!(built, windows);

            let attempted = AtomicUsize::new(0);
            let error = build_windows_with(&windows, workers, |start, _| {
                attempted.fetch_add(1, Ordering::SeqCst);
                match start {
                    // The later failure finishes first; the earlier one still wins.
                    50 => {
                        thread::sleep(std::time::Duration::from_millis(20));
                        Err("window 50 failed".to_owned())
                    }
                    70 => Err("window 70 failed".to_owned()),
                    _ => Ok(start),
                }
            })
            .unwrap_err();
            assert_eq!(error, "window 50 failed", "{workers} workers");
            // No window is started once a failure is observed: at most the
            // failing window plus the windows in flight on other workers.
            assert!(
                attempted.load(Ordering::SeqCst) <= 8 + workers,
                "{workers} workers"
            );

            let error = build_windows_with(&windows, workers, |start, _| {
                if start == 40 {
                    panic!("injected panic");
                }
                Ok(start)
            })
            .unwrap_err();
            assert_eq!(
                error, "materializing window 40..50 panicked: injected panic",
                "{workers} workers"
            );
        }
        assert!(
            build_windows_with(&[], 8, |_, _| Ok(()))
                .unwrap()
                .is_empty()
        );
    }
}
