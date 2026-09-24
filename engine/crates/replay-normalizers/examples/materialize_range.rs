//! Private Stage-A operator helper. This is deliberately an example target,
//! not a supported installed CLI.

use canonical_normalizer::Normalize;
use indexer_finalize::{
    CertifiedPolicy, LowerBoundPolicy, SelectionPolicy, select_canonical_windows,
};
use indexer_types::Sha256;
use replay_domain::SEGMENT_SCHEMA_VERSION;
use replay_materialize::{
    DerivativeSpec, NormalizationPolicy, PinnedDerivative, ReadLimits, build_window, inspect_pinned,
};
use replay_normalizers::{CanonicalNormalizer, CanonicalNormalizerIdentity};
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use std::path::PathBuf;

const MAX_WINDOWS: usize = 4096;

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

fn execute(input: &str) -> Result<String, String> {
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
    let mut pins = Vec::with_capacity(windows.len());
    for (start, end) in windows {
        let mut normalizer = CanonicalNormalizer::default();
        let descriptor = normalizer.descriptor().clone();
        let spec = DerivativeSpec {
            normalized_schema_version: SEGMENT_SCHEMA_VERSION,
            normalizer_bundle_sha256: descriptor.bundle_sha256,
            normalizer_config_sha256: descriptor.config_sha256,
            policy: NormalizationPolicy {
                policy_sha256: Sha256::digest(
                    b"prediction-indexer/replay-normalizers/materialize-range-policy/v1",
                ),
                effective_from_ns: 0,
                effective_until_ns: None,
            },
        };
        let built = build_window(
            &request.canonical_root,
            &request.output_root,
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
        pins.push(OutputPin {
            window_start_ns: start,
            window_end_ns: end,
            derivative_address: input.pin.derivative_address,
            receipt_sha256: input.pin.receipt_sha256,
        });
    }
    serde_json::to_string(&Response {
        version: 1,
        normalizer: identity,
        derivatives: pins,
    })
    .map_err(|error| format!("serializing response: {error}"))
}

fn main() {
    let mut input = String::new();
    let result = io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| format!("reading stdin: {error}"))
        .and_then(|_| execute(&input));
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
        assert_eq!(snapshot(canonical.path()), before);
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
}
