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

#[derive(Serialize)]
struct Response {
    version: u16,
    normalizer: CanonicalNormalizerIdentity,
    derivatives: Vec<OutputPin>,
}

#[derive(Serialize)]
struct OutputPin {
    window_start_ns: u64,
    window_end_ns: u64,
    derivative_address: String,
    receipt_sha256: Sha256,
}

fn run() -> Result<String, String> {
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| format!("reading stdin: {error}"))?;
    let request: Request =
        serde_json::from_str(&input).map_err(|error| format!("invalid request: {error}"))?;
    if request.version != 1 {
        return Err("unsupported request version".to_owned());
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
    match run() {
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
    }
}
