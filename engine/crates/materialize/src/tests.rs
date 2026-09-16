use std::fs;
use std::io::{BufRead, BufReader, Cursor};
use std::panic;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

use canonical_normalizer::{
    Normalization, Normalize, NormalizerDescriptor, NormalizerError, ParseReject,
};
use indexer_finalize::{
    CanonicalOutput, CompressionContract as CanonicalCompression, DecodedIdentity, InputSegment,
    Receipt as CanonicalReceipt, StoredIdentity as CanonicalStored, window_directory,
};
use prediction_encoder::{
    DEFAULT_ZSTD_LEVEL, LogicalIdentity as CodecLogical, StoredIdentity as CodecStored,
    StreamingDecoder, encode_stream, encoder_version,
};
use replay_domain::{ControlEvent, FaultImpact, LaneId, SEGMENT_SCHEMA_VERSION, SegmentEvent};
use serde_json::{Value, json};
use tempdir::TempDir;

use super::*;

const SOURCE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn digest(byte: char) -> Sha256 {
    Sha256::from_hex(&byte.to_string().repeat(64)).unwrap()
}

#[derive(Clone)]
enum FakeMode {
    Normal,
    Divergent,
    FailRecord,
    Panic,
    FailFinish,
    UnpairedFault,
    ControlLineBytes(usize),
}

struct FakeNormalizer {
    mode: FakeMode,
    finished: bool,
    descriptor: NormalizerDescriptor,
}

struct IgnoreAllNormalizer {
    descriptor: NormalizerDescriptor,
}

struct RejectAllNormalizer {
    descriptor: NormalizerDescriptor,
}

impl RejectAllNormalizer {
    fn new() -> Self {
        Self {
            descriptor: NormalizerDescriptor {
                bundle_sha256: digest('b'),
                config_sha256: digest('c'),
            },
        }
    }
}

impl Normalize for RejectAllNormalizer {
    fn descriptor(&self) -> &NormalizerDescriptor {
        &self.descriptor
    }

    fn normalize(
        &mut self,
        _source: &indexer_finalize::JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError> {
        Ok(Normalization::Reject(ParseReject {
            parser_version: 1,
            error_code: "synthetic_reject".to_owned(),
            instrument_hint: None,
            impact: FaultImpact::UnattributedLane(LaneId::new("polymarket").unwrap()),
        }))
    }

    fn finish(&mut self) -> Result<(), NormalizerError> {
        Ok(())
    }
}

impl IgnoreAllNormalizer {
    fn new() -> Self {
        Self {
            descriptor: NormalizerDescriptor {
                bundle_sha256: digest('b'),
                config_sha256: digest('c'),
            },
        }
    }
}

impl Normalize for IgnoreAllNormalizer {
    fn descriptor(&self) -> &NormalizerDescriptor {
        &self.descriptor
    }

    fn normalize(
        &mut self,
        _source: &indexer_finalize::JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError> {
        Ok(Normalization::Ignored {
            reason_code: "test_ignored".to_owned(),
        })
    }

    fn finish(&mut self) -> Result<(), NormalizerError> {
        Ok(())
    }
}

impl FakeNormalizer {
    fn new(mode: FakeMode) -> Self {
        Self {
            mode,
            finished: false,
            descriptor: NormalizerDescriptor {
                bundle_sha256: digest('b'),
                config_sha256: digest('c'),
            },
        }
    }
}

impl Normalize for FakeNormalizer {
    fn descriptor(&self) -> &NormalizerDescriptor {
        &self.descriptor
    }

    fn normalize(
        &mut self,
        source: &indexer_finalize::JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError> {
        match self.mode {
            FakeMode::FailRecord => return Err(NormalizerError::new("internal defect")),
            FakeMode::Panic => panic!("adapter panic"),
            FakeMode::Normal | FakeMode::Divergent | FakeMode::FailFinish => {}
            FakeMode::ControlLineBytes(length) => {
                let header = event_header(source, 0).unwrap();
                let small =
                    SegmentEvent::Control(ControlEvent::ConnectionClosed { epoch: "x".into() });
                let overhead = replay_domain::SegmentRecord::new(header, small)
                    .unwrap()
                    .to_canonical_json()
                    .len();
                // One existing epoch byte is replaced by padding; the LF adds
                // one byte, cancelling that removed byte.
                return Ok(Normalization::Events(vec![SegmentEvent::Control(
                    ControlEvent::ConnectionClosed {
                        epoch: "x".repeat(length - overhead),
                    },
                )]));
            }
            FakeMode::UnpairedFault => {
                return Ok(Normalization::Events(vec![
                    SegmentEvent::NormalizationFault(
                        NormalizationFault::new(
                            "a".repeat(64),
                            FaultImpact::UnattributedLane(LaneId::new("polymarket").unwrap()),
                        )
                        .unwrap(),
                    ),
                ]));
            }
        }
        match source.canonical_seq {
            1 => {
                let count = if matches!(self.mode, FakeMode::Divergent) {
                    1
                } else {
                    2
                };
                Ok(Normalization::Events(
                    (0..count)
                        .map(|index| {
                            SegmentEvent::Control(ControlEvent::ConnectionClosed {
                                epoch: format!("epoch-{index}"),
                            })
                        })
                        .collect(),
                ))
            }
            2 => Ok(Normalization::Events(Vec::new())),
            3 => Ok(Normalization::Reject(ParseReject {
                parser_version: 7,
                error_code: "malformed_payload".to_owned(),
                instrument_hint: None,
                impact: FaultImpact::UnattributedLane(LaneId::new("polymarket").unwrap()),
            })),
            _ => unreachable!(),
        }
    }

    fn finish(&mut self) -> Result<(), NormalizerError> {
        self.finished = true;
        if matches!(self.mode, FakeMode::FailFinish) {
            Err(NormalizerError::new("finish invariant"))
        } else {
            Ok(())
        }
    }
}

fn spec() -> DerivativeSpec {
    DerivativeSpec {
        normalized_schema_version: SEGMENT_SCHEMA_VERSION,
        normalizer_bundle_sha256: digest('b'),
        normalizer_config_sha256: digest('c'),
        policy: NormalizationPolicy {
            policy_sha256: digest('d'),
            effective_from_ns: 0,
            effective_until_ns: Some(10),
        },
    }
}

fn envelope(seq: i64) -> Vec<u8> {
    format!(
        "{{\"delivery_index\":{seq},\"record_id\":\"record-{seq}\",\"visible_ns\":{seq},\"venue\":\"polymarket\",\"stream\":\"public_book\",\"connection_epoch\":\"epoch\",\"local_counter\":{seq},\"source_cursor\":{{\"type\":\"unsequenced\",\"counter\":{seq}}},\"kind\":\"venue_frame\",\"raw_payload\":\"{{}}\"}}\n"
    )
    .into_bytes()
}

fn encoded_output(directory: &Path, name: &str, logical: &[u8]) -> CanonicalOutput {
    let mut stored = Vec::new();
    let result = encode_stream(Cursor::new(logical), &mut stored, DEFAULT_ZSTD_LEVEL).unwrap();
    fs::write(directory.join(name), stored).unwrap();
    CanonicalOutput {
        file: name.to_owned(),
        content_encoding: "zstd".to_owned(),
        decoded: DecodedIdentity {
            byte_length: result.logical.byte_length,
            line_count: result.logical.line_count,
            sha256: result.logical.sha256,
        },
        stored: CanonicalStored {
            byte_length: result.stored.byte_length,
            sha256: result.stored.sha256,
        },
        compression: CanonicalCompression {
            algorithm: "zstd".to_owned(),
            level: DEFAULT_ZSTD_LEVEL,
            frame_checksum: true,
            dictionary: None,
            frame_count: 1,
            encoder: encoder_version(),
        },
    }
}

fn canonical_fixture(root: &Path) {
    canonical_fixture_count(root, 3, 10);
}

fn canonical_fixture_count(root: &Path, count: i64, window_end_ns: u64) {
    let directory = window_directory(root, 0);
    fs::create_dir_all(&directory).unwrap();
    let mut evidence = Vec::new();
    let mut provenance = Vec::new();
    for seq in 1..=count {
        let encoded = envelope(seq);
        let view = indexer_types::EnvelopeView::parse(&encoded).unwrap();
        evidence.extend_from_slice(&encoded);
        provenance.extend_from_slice(
            format!(
                "{}\n",
                json!({
                    "canonical_seq": seq,
                    "lane_id": "polymarket",
                    "source_segment_sha256": SOURCE_SHA,
                    "source_line_number": seq,
                    "record_id": view.record_id.as_str(),
                    "content_hash": indexer_types::ContentHash::hash(view.raw_payload.as_bytes()).to_hex(),
                    "continuity_verdict": "unsequenced_venue",
                    "visible_tie_group": null,
                })
            )
            .as_bytes(),
        );
    }
    let receipt = CanonicalReceipt {
        receipt_version: 1,
        window_start_ns: 0,
        window_end_ns,
        completeness: "complete".to_owned(),
        certified: true,
        expected_lanes: vec!["polymarket".to_owned()],
        present_lanes: vec!["polymarket".to_owned()],
        unexpected_lanes: Vec::new(),
        missing_lanes: Vec::new(),
        invalid_lanes: Vec::new(),
        finalization_deadline_seconds: 300,
        deadline_expired: false,
        finalized_at_ns: window_end_ns,
        inputs: vec![InputSegment {
            lane: "polymarket".to_owned(),
            data_file: "source.ndjson".to_owned(),
            segment_index: 0,
            line_count: count as u64,
            sha256: SOURCE_SHA.to_owned(),
            first_delivery_index: Some(1),
            last_delivery_index: Some(count as u64),
        }],
        evidence: encoded_output(&directory, "evidence.ndjson.zst", &evidence),
        provenance: encoded_output(&directory, "provenance.ndjson.zst", &provenance),
        first_canonical_seq: Some(1),
        last_canonical_seq: Some(count),
        carried: Default::default(),
        clock_faults: Vec::new(),
        finalizer_version: 1,
    };
    let mut bytes = serde_json::to_vec_pretty(&receipt).unwrap();
    bytes.push(b'\n');
    fs::write(directory.join("receipt.json"), bytes).unwrap();
}

fn output_directories(root: &Path) -> Vec<std::path::PathBuf> {
    let window = root.join("window=0");
    if !window.is_dir() {
        return Vec::new();
    }
    fs::read_dir(window)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
        .map(|entry| entry.path())
        .collect()
}

fn event_indexes(derivative: &VerifiedDerivative) -> Vec<(i64, u32)> {
    let output = &derivative.manifest.events;
    let file = fs::File::open(derivative.directory.join("events.ndjson.zst")).unwrap();
    let logical = CodecLogical {
        sha256: output.logical.sha256.as_hex(),
        byte_length: output.logical.byte_length,
        line_count: output.logical.line_count,
    };
    let stored = CodecStored {
        sha256: output.stored.sha256.as_hex(),
        byte_length: output.stored.byte_length,
    };
    let decoder =
        StreamingDecoder::new(file, &logical, Some(&stored), Some(logical.byte_length)).unwrap();
    let mut reader = BufReader::new(decoder);
    let mut result = Vec::new();
    let mut line = Vec::new();
    while reader.read_until(b'\n', &mut line).unwrap() != 0 {
        let record =
            replay_domain::SegmentRecord::from_canonical_json(&line[..line.len() - 1]).unwrap();
        result.push((
            record.header().address().canonical_seq(),
            record.header().address().event_index(),
        ));
        line.clear();
    }
    reader.into_inner().finish().unwrap();
    result
}

#[test]
fn verifier_requires_zero_based_contiguous_child_indexes() {
    assert!(super::verify::verify_event_order(None, (1, 0)).is_ok());
    assert!(super::verify::verify_event_order(Some((1, 0)), (1, 1)).is_ok());
    assert!(super::verify::verify_event_order(Some((1, 1)), (3, 0)).is_ok());
    assert!(super::verify::verify_event_order(None, (1, 1)).is_err());
    assert!(super::verify::verify_event_order(Some((1, 0)), (2, 1)).is_err());
    assert!(super::verify::verify_event_order(Some((2, 0)), (1, 0)).is_err());
}

fn first_reject_json(derivative: &VerifiedDerivative) -> Vec<u8> {
    let output = &derivative.manifest.rejects;
    let file = fs::File::open(derivative.directory.join("rejects.ndjson.zst")).unwrap();
    let logical = CodecLogical {
        sha256: output.logical.sha256.as_hex(),
        byte_length: output.logical.byte_length,
        line_count: output.logical.line_count,
    };
    let stored = CodecStored {
        sha256: output.stored.sha256.as_hex(),
        byte_length: output.stored.byte_length,
    };
    let decoder =
        StreamingDecoder::new(file, &logical, Some(&stored), Some(logical.byte_length)).unwrap();
    let mut reader = BufReader::new(decoder);
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).unwrap();
    line.pop();
    line
}

#[test]
fn streams_zero_many_and_rejects_then_commits_a_verified_derivative() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let mut normalizer = FakeNormalizer::new(FakeMode::Normal);
    let selection = indexer_finalize::select_canonical_windows(
        canonical.path(),
        0,
        10,
        indexer_finalize::SelectionPolicy {
            certified: indexer_finalize::CertifiedPolicy::AllowUncertified,
            lower_bound: indexer_finalize::LowerBoundPolicy::RequireWindowBoundary,
        },
    )
    .unwrap();
    let built = materialize_window(selection, output.path(), &spec(), &mut normalizer).unwrap();

    assert_eq!(built.disposition, BuildDisposition::Committed);
    assert!(normalizer.finished);
    assert_eq!(
        built.derivative.manifest.counts,
        DerivativeCounts {
            input_records: 3,
            accepted_source_records: 1,
            accepted_events: 2,
            rejected_source_records: 1,
            normalization_fault_events: 1,
            intentionally_ignored_records: 1,
        }
    );
    assert_eq!(event_indexes(&built.derivative), [(1, 0), (1, 1), (3, 0)]);
    assert_eq!(built.derivative.pin.derivative_address.len(), 64);
    assert_eq!(built.derivative.pin.receipt_sha256.as_bytes().len(), 32);
    assert!(built.derivative.directory.join("receipt.json").is_file());
}

#[test]
fn verifier_rejects_rehashed_inflated_input_and_accepted_source_counts() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let built = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();

    let manifest_path = built.derivative.directory.join("manifest.json");
    let mut manifest = built.derivative.manifest.clone();
    manifest.counts.input_records += 1;
    manifest.counts.accepted_source_records += 1;
    let mut manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    manifest_bytes.push(b'\n');
    fs::write(&manifest_path, &manifest_bytes).unwrap();

    let receipt_path = built.derivative.directory.join("receipt.json");
    let mut receipt = built.derivative.receipt.clone();
    receipt.manifest.byte_length = manifest_bytes.len() as u64;
    receipt.manifest.sha256 = Sha256::digest(&manifest_bytes);
    let mut receipt_bytes = serde_json::to_vec(&receipt).unwrap();
    receipt_bytes.push(b'\n');
    fs::write(receipt_path, receipt_bytes).unwrap();

    let error = verify_derivative(&built.derivative.directory).unwrap_err();
    assert_eq!(
        error,
        "verified derivative lines disagree with manifest counts"
    );
}

#[test]
fn verifier_rejects_mismatched_child_source_header() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let built = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    let directory = &built.derivative.directory;
    let output = &built.derivative.manifest.events;
    let logical = CodecLogical {
        sha256: output.logical.sha256.as_hex(),
        byte_length: output.logical.byte_length,
        line_count: output.logical.line_count,
    };
    let stored = CodecStored {
        sha256: output.stored.sha256.as_hex(),
        byte_length: output.stored.byte_length,
    };
    let decoder = StreamingDecoder::new(
        File::open(directory.join("events.ndjson.zst")).unwrap(),
        &logical,
        Some(&stored),
        Some(logical.byte_length),
    )
    .unwrap();
    let mut lines = BufReader::new(decoder);
    let mut payload = Vec::new();
    let mut index = 0;
    let mut line = String::new();
    while lines.read_line(&mut line).unwrap() != 0 {
        let text = line.trim_end_matches('\n').to_owned();
        let text = if index == 1 {
            text.replace("\"lane\":\"polymarket\"", "\"lane\":\"other\"")
        } else {
            text
        };
        payload.extend_from_slice(text.as_bytes());
        payload.push(b'\n');
        index += 1;
        line.clear();
    }
    lines.into_inner().finish().unwrap();
    let result = encode_stream(
        Cursor::new(payload),
        File::create(directory.join("events.ndjson.zst")).unwrap(),
        DEFAULT_ZSTD_LEVEL,
    )
    .unwrap();
    let events = compressed_output("events.ndjson.zst", result).unwrap();
    let mut manifest = built.derivative.manifest.clone();
    manifest.events = events.clone();
    let bytes = canonical_document(&manifest).unwrap();
    fs::write(directory.join("manifest.json"), &bytes).unwrap();
    let mut receipt = built.derivative.receipt.clone();
    receipt.events = events;
    receipt.manifest.sha256 = Sha256::digest(&bytes);
    receipt.manifest.byte_length = bytes.len() as u64;
    fs::write(
        directory.join("receipt.json"),
        canonical_document(&receipt).unwrap(),
    )
    .unwrap();
    assert!(
        verify_derivative(directory).is_err(),
        "different source lanes within one delivery must fail"
    );
}

#[test]
fn identical_retry_is_noop_and_divergent_same_address_conflicts() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let first = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    let retry = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    assert_eq!(retry.disposition, BuildDisposition::VerifiedNoOp);
    assert_eq!(first.derivative.pin, retry.derivative.pin);

    let error = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Divergent),
    )
    .unwrap_err();
    assert!(matches!(error, BuildError::Conflict(_)));
}

#[test]
fn fatal_normalizer_failures_and_panics_commit_nothing() {
    for mode in [FakeMode::FailRecord, FakeMode::Panic, FakeMode::FailFinish] {
        let canonical = TempDir::new("canonical").unwrap();
        let output = TempDir::new("normalized").unwrap();
        canonical_fixture(canonical.path());
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            build_window(
                canonical.path(),
                output.path(),
                0,
                10,
                &spec(),
                &mut FakeNormalizer::new(mode),
            )
        }))
        .expect("materializer catches adapter panic");
        assert!(result.is_err());
        assert!(output_directories(output.path()).is_empty());
    }
}

#[test]
fn invalid_candidate_is_verified_before_receipt_publication() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let error = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::UnpairedFault),
    )
    .unwrap_err();
    assert!(matches!(error, BuildError::Verification(_)));
    assert!(output_directories(output.path()).is_empty());
}

#[test]
fn mismatched_normalizer_descriptor_commits_nothing() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let mut normalizer = FakeNormalizer::new(FakeMode::Normal);
    normalizer.descriptor.config_sha256 = digest('e');
    let error = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut normalizer,
    )
    .unwrap_err();
    assert!(matches!(error, BuildError::InvalidSpec(_)));
    assert!(output_directories(output.path()).is_empty());
}

#[test]
fn audit_truncation_commits_nothing() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let path = window_directory(canonical.path(), 0).join("evidence.ndjson.zst");
    let mut bytes = fs::read(&path).unwrap();
    bytes.pop();
    fs::write(path, bytes).unwrap();
    let error = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap_err();
    assert!(matches!(error, BuildError::Audit(_)));
    assert!(output_directories(output.path()).is_empty());
}

#[test]
fn address_binds_every_version_and_policy_input() {
    let source = SourceReceipt {
        window_start_ns: 0,
        window_end_ns: 10,
        byte_length: 100,
        sha256: digest('a'),
        certified: true,
    };
    let base = spec();
    let base_address = derivative_address(&source, &base).unwrap();
    assert_eq!(
        base_address,
        "71df204dafc347cee916dcf38a9ffcc4b765f2bd612e5cbc65300885f35268b0"
    );
    // A newer writer selection must not retarget historical address verification.
    assert_eq!(
        derivative_address_versions(&source, &base, 1, 1, 1).unwrap(),
        base_address
    );
    for versions in [(2, 1, 1), (1, 2, 1), (1, 1, 2)] {
        assert_ne!(
            derivative_address_versions(&source, &base, versions.0, versions.1, versions.2)
                .unwrap(),
            base_address,
        );
    }
    let mut variants = Vec::new();
    let mut changed = base.clone();
    changed.normalized_schema_version += 1;
    variants.push(derivative_address(&source, &changed).unwrap());
    changed = base.clone();
    changed.normalizer_bundle_sha256 = digest('e');
    variants.push(derivative_address(&source, &changed).unwrap());
    changed = base.clone();
    changed.normalizer_config_sha256 = digest('e');
    variants.push(derivative_address(&source, &changed).unwrap());
    changed = base.clone();
    changed.policy.policy_sha256 = digest('e');
    variants.push(derivative_address(&source, &changed).unwrap());
    changed = base.clone();
    changed.policy.effective_until_ns = Some(11);
    variants.push(derivative_address(&source, &changed).unwrap());
    let mut changed_source = source;
    changed_source.sha256 = digest('f');
    variants.push(derivative_address(&changed_source, &base).unwrap());
    assert!(variants.iter().all(|address| address != &base_address));
    assert_eq!(
        variants
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        variants.len()
    );
}

#[test]
fn crash_boundaries_recover_and_receipt_rename_is_commit_point() {
    for point in [
        Checkpoint::FramesFinished,
        Checkpoint::FilesSynced,
        Checkpoint::BeforeManifestSerialization,
        Checkpoint::CandidateVerified,
        Checkpoint::DirectoryPublished,
        Checkpoint::ReceiptSynced,
        Checkpoint::ReceiptRenamed,
    ] {
        let canonical = TempDir::new("canonical").unwrap();
        let output = TempDir::new("normalized").unwrap();
        canonical_fixture(canonical.path());
        let mut fired = false;
        let selection = indexer_finalize::select_canonical_windows(
            canonical.path(),
            0,
            10,
            indexer_finalize::SelectionPolicy {
                certified: indexer_finalize::CertifiedPolicy::AllowUncertified,
                lower_bound: indexer_finalize::LowerBoundPolicy::RequireWindowBoundary,
            },
        )
        .unwrap();
        let result = build_window_inner(
            selection,
            output.path(),
            &spec(),
            &mut FakeNormalizer::new(FakeMode::Normal),
            |current| {
                if current == point {
                    fired = true;
                    return Err(if current == Checkpoint::BeforeManifestSerialization {
                        BuildError::Serialization("injected".to_owned())
                    } else {
                        BuildError::Io("injected crash".to_owned())
                    });
                }
                Ok(())
            },
        );
        assert!(fired);
        assert!(result.is_err());
        let committed = output_directories(output.path());
        if point == Checkpoint::ReceiptRenamed {
            assert_eq!(committed.len(), 1);
            verify_derivative(&committed[0]).unwrap();
        } else {
            assert!(
                committed.is_empty() || !committed[0].join("receipt.json").exists(),
                "no pre-marker checkpoint may commit"
            );
        }
        let recovered = build_window(
            canonical.path(),
            output.path(),
            0,
            10,
            &spec(),
            &mut FakeNormalizer::new(FakeMode::Normal),
        )
        .unwrap();
        verify_derivative(&recovered.derivative.directory).unwrap();
    }
}

#[test]
fn startup_prunes_only_abandoned_stages_across_windows() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let old_window = output.path().join("window=99");
    fs::create_dir(&old_window).unwrap();
    let address = "a".repeat(64);
    let orphan = old_window.join(format!(".{address}.123.0.open"));
    fs::create_dir(&orphan).unwrap();
    fs::write(
        orphan.join("events.ndjson.zst.open"),
        b"torn frame, not decodable",
    )
    .unwrap();

    let active_address = "b".repeat(64);
    let active = old_window.join(format!(".{active_address}.456.0.open"));
    fs::create_dir(&active).unwrap();
    let active_lock = acquire_lock(&old_window, &active_address).unwrap();
    let committed = old_window.join(&address);
    let marked_stage = old_window.join(format!(".{address}.123.1.open"));
    let unrelated = old_window.join(".unrelated.open");
    for directory in [&active, &committed, &marked_stage, &unrelated] {
        fs::create_dir_all(directory).unwrap();
        fs::write(directory.join("keep"), b"unchanged").unwrap();
    }
    fs::write(committed.join("receipt.json"), b"commit marker").unwrap();
    fs::write(marked_stage.join("receipt.json"), b"never delete a marker").unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            &unrelated,
            old_window.join(format!(".{address}.123.2.open")),
        )
        .unwrap();
        std::os::unix::fs::symlink(&old_window, output.path().join("window=100")).unwrap();
    }

    let built = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    assert!(
        !orphan.exists(),
        "startup must prune the abandoned private stage"
    );
    for directory in [&active, &committed, &marked_stage, &unrelated] {
        assert_eq!(fs::read(directory.join("keep")).unwrap(), b"unchanged");
    }
    verify_derivative(&built.derivative.directory).unwrap();
    drop(active_lock);
    build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    assert!(
        !active.exists(),
        "released ownership makes the old stage eligible"
    );
    assert!(old_window.join(format!(".{active_address}.lock")).exists());
}

#[test]
fn hard_killed_builder_leaves_stage_that_next_run_prunes() {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    const CHILD_ROOT: &str = "REPLAY_MATERIALIZE_CRASH_TEST_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        let selection = select_canonical_windows(
            &root.join("canonical"),
            0,
            10,
            SelectionPolicy {
                certified: CertifiedPolicy::AllowUncertified,
                lower_bound: LowerBoundPolicy::RequireWindowBoundary,
            },
        )
        .unwrap();
        build_window_inner(
            selection,
            &root.join("normalized"),
            &spec(),
            &mut FakeNormalizer::new(FakeMode::Normal),
            |point| {
                if point == Checkpoint::FramesFinished {
                    fs::write(root.join("ready"), b"ready").unwrap();
                    thread::sleep(Duration::from_secs(30));
                    panic!("parent did not kill paused builder");
                }
                Ok(())
            },
        )
        .unwrap();
        panic!("child unexpectedly finished");
    }
    let root = TempDir::new("hard-kill").unwrap();
    let canonical = root.path().join("canonical");
    let output = root.path().join("normalized");
    fs::create_dir_all(&canonical).unwrap();
    canonical_fixture(&canonical);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::hard_killed_builder_leaves_stage_that_next_run_prunes",
            "--nocapture",
        ])
        .env(CHILD_ROOT, root.path())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !root.path().join("ready").exists() && start.elapsed() < Duration::from_secs(10) {
        assert!(
            child.try_wait().unwrap().is_none(),
            "builder exited before checkpoint"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let ready = root.path().join("ready").exists();
    let window = output.join("window=0");
    if ready {
        let stages = sorted_directories(&window).unwrap();
        assert_eq!(stages.len(), 1);
        prune_abandoned_stages(&output, None).unwrap();
        assert!(stages[0].exists(), "a live builder must retain its stage");
    }
    child.kill().unwrap(); // SIGKILL on Unix: no Rust destructors run.
    assert!(!child.wait().unwrap().success());
    assert!(ready, "builder did not reach checkpoint before timeout");
    let stages = sorted_directories(&window).unwrap();
    assert_eq!(stages.len(), 1);
    assert!(stages[0].join("events.ndjson.zst.open").is_file());
    let built = build_window(
        &canonical,
        &output,
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    assert!(!stages[0].exists());
    verify_derivative(&built.derivative.directory).unwrap();
}

#[test]
fn concurrent_identical_builders_publish_one_derivative() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let canonical_path = canonical.path().to_path_buf();
    let output_path = output.path().to_path_buf();
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let canonical_path = canonical_path.clone();
            let output_path = output_path.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                build_window(
                    &canonical_path,
                    &output_path,
                    0,
                    10,
                    &spec(),
                    &mut FakeNormalizer::new(FakeMode::Normal),
                )
                .unwrap()
                .disposition
            })
        })
        .collect::<Vec<_>>();
    let dispositions = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert!(dispositions.contains(&BuildDisposition::Committed));
    assert!(dispositions.contains(&BuildDisposition::VerifiedNoOp));
    assert_eq!(output_directories(output.path()).len(), 1);
}

#[test]
fn strict_reader_rejects_unknown_versions_fields_and_corrupt_frames() {
    let canonical = TempDir::new("canonical").unwrap();
    canonical_fixture(canonical.path());
    for mutation in ["version", "field"] {
        let output = TempDir::new("normalized").unwrap();
        let built = build_window(
            canonical.path(),
            output.path(),
            0,
            10,
            &spec(),
            &mut FakeNormalizer::new(FakeMode::Normal),
        )
        .unwrap();
        let path = built.derivative.directory.join("receipt.json");
        let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        if mutation == "version" {
            value["receipt_version"] = json!(999);
        } else {
            value["unknown"] = json!(true);
        }
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&value).unwrap()),
        )
        .unwrap();
        assert!(verify_derivative(&built.derivative.directory).is_err());
    }

    let output = TempDir::new("normalized").unwrap();
    let built = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    let reject_json = first_reject_json(&built.derivative);
    let valid_reject = String::from_utf8(reject_json.clone()).unwrap();
    assert!(RejectRecord::from_canonical_json(valid_reject.as_bytes()).is_ok());
    for invalid_id in ["", "bad\\nrecord"] {
        let invalid_reject = valid_reject.replacen(
            "\"record_id\":\"record-2\"",
            &format!("\"record_id\":\"{invalid_id}\""),
            1,
        );
        assert_ne!(invalid_reject, valid_reject);
        assert!(serde_json::from_str::<RejectRecord>(&invalid_reject).is_err());
        assert!(RejectRecord::from_canonical_json(invalid_reject.as_bytes()).is_err());
    }
    let mut reject: Value = serde_json::from_slice(&reject_json).unwrap();
    reject["unknown"] = json!(true);
    assert!(
        RejectRecord::from_canonical_json(serde_json::to_string(&reject).unwrap().as_bytes())
            .is_err()
    );
    reject.as_object_mut().unwrap().remove("unknown");
    reject["reject_version"] = json!(999);
    assert!(
        RejectRecord::from_canonical_json(serde_json::to_string(&reject).unwrap().as_bytes())
            .is_err()
    );
    let mut manifest = serde_json::to_value(&built.derivative.manifest).unwrap();
    manifest["unknown"] = json!(true);
    assert!(serde_json::from_value::<DerivativeManifest>(manifest).is_err());
    let mut manifest = serde_json::to_value(&built.derivative.manifest).unwrap();
    manifest["normalizer_bundle_sha256"] = json!("B".repeat(64));
    assert!(serde_json::from_value::<DerivativeManifest>(manifest).is_err());

    let frame = built.derivative.directory.join("events.ndjson.zst");
    let mut bytes = fs::read(&frame).unwrap();
    bytes.push(0);
    fs::write(frame, bytes).unwrap();
    assert!(verify_derivative(&built.derivative.directory).is_err());
}

#[test]
fn changed_source_receipt_and_normalizer_versions_coexist() {
    let canonical = TempDir::new("canonical").unwrap();
    let output = TempDir::new("normalized").unwrap();
    canonical_fixture(canonical.path());
    let first = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    let mut newer = spec();
    newer.normalizer_bundle_sha256 = digest('e');
    let mut newer_normalizer = FakeNormalizer::new(FakeMode::Normal);
    newer_normalizer.descriptor.bundle_sha256 = digest('e');
    let second = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &newer,
        &mut newer_normalizer,
    )
    .unwrap();
    assert_ne!(first.derivative.pin, second.derivative.pin);
    assert_eq!(output_directories(output.path()).len(), 2);

    let receipt_path = window_directory(canonical.path(), 0).join("receipt.json");
    let mut receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    receipt["finalized_at_ns"] = json!(11);
    fs::write(
        receipt_path,
        format!("{}\n", serde_json::to_string_pretty(&receipt).unwrap()),
    )
    .unwrap();
    let third = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    assert_ne!(
        first.derivative.pin.derivative_address,
        third.derivative.pin.derivative_address
    );
    assert_eq!(output_directories(output.path()).len(), 3);
}

#[test]
fn large_synthetic_window_uses_the_same_streaming_path() {
    const RECORDS: i64 = 10_000;
    const END_NS: u64 = RECORDS as u64 + 1;
    let canonical = TempDir::new("canonical-large").unwrap();
    let output = TempDir::new("normalized-large").unwrap();
    canonical_fixture_count(canonical.path(), RECORDS, END_NS);
    let mut large_spec = spec();
    large_spec.policy.effective_until_ns = Some(END_NS);

    let built = build_window(
        canonical.path(),
        output.path(),
        0,
        END_NS,
        &large_spec,
        &mut IgnoreAllNormalizer::new(),
    )
    .unwrap();

    assert_eq!(
        built.derivative.manifest.counts.input_records,
        RECORDS as u64
    );
    assert_eq!(
        built
            .derivative
            .manifest
            .counts
            .intentionally_ignored_records,
        RECORDS as u64
    );
    assert_eq!(built.derivative.manifest.events.logical.line_count, 0);
    assert_eq!(
        built.derivative.manifest.rejects.logical.line_count,
        RECORDS as u64
    );
}

#[test]
fn large_reject_window_pairs_faults_in_stream_order() {
    const RECORDS: i64 = 10_000;
    const END_NS: u64 = RECORDS as u64 + 1;
    let canonical = TempDir::new("canonical-rejects").unwrap();
    let output = TempDir::new("normalized-rejects").unwrap();
    canonical_fixture_count(canonical.path(), RECORDS, END_NS);
    let mut large_spec = spec();
    large_spec.policy.effective_until_ns = Some(END_NS);

    let built = build_window(
        canonical.path(),
        output.path(),
        0,
        END_NS,
        &large_spec,
        &mut RejectAllNormalizer::new(),
    )
    .unwrap();

    assert_eq!(
        built.derivative.manifest.counts.rejected_source_records,
        RECORDS as u64
    );
    assert_eq!(
        built.derivative.manifest.counts.normalization_fault_events,
        RECORDS as u64
    );
    verify_derivative(&built.derivative.directory).unwrap();
}

#[test]
fn pinned_snapshot_races_and_post_verification_io_errors_fail_closed() {
    use crate::reader::{ReadCheckpoint, open_pinned_with_checkpoint};

    for stage in [
        ReadCheckpoint::MetadataCaptured,
        ReadCheckpoint::SnapshotCopied,
        ReadCheckpoint::ReaderOpened,
    ] {
        let canonical = TempDir::new("read-race-source").unwrap();
        let output = TempDir::new("read-race-output").unwrap();
        canonical_fixture(canonical.path());
        let built = build_window(
            canonical.path(),
            output.path(),
            0,
            10,
            &spec(),
            &mut FakeNormalizer::new(FakeMode::Normal),
        )
        .unwrap();
        let input = PinnedDerivative {
            directory: built.derivative.directory,
            pin: built.derivative.pin,
        };
        let original = fs::read(input.directory.join("events.ndjson.zst")).unwrap();
        let result =
            open_pinned_with_checkpoint(&input, &ReadLimits::default(), |checkpoint, path| {
                if checkpoint == stage {
                    // After copy, destroy the ORIGINAL. After decoder open, inject
                    // a private-file I/O failure before its first delivery read.
                    let directory = if stage == ReadCheckpoint::SnapshotCopied {
                        &input.directory
                    } else {
                        path
                    };
                    fs::write(directory.join("events.ndjson.zst"), []).unwrap();
                }
            });
        if stage == ReadCheckpoint::MetadataCaptured {
            assert!(result.is_err());
        } else {
            let mut reader = result.unwrap();
            if stage == ReadCheckpoint::ReaderOpened {
                assert!(reader.next_delivery().is_err());
                assert_eq!(
                    reader.next_delivery().unwrap_err(),
                    "derivative reader is poisoned"
                );
                assert!(reader.finish().is_err());
            } else {
                let mut sequences = Vec::new();
                while let Some(delivery) = reader.next_delivery().unwrap() {
                    sequences.push(delivery.header().address().canonical_seq());
                }
                assert_eq!(sequences, [1, 2, 3]);
                assert_eq!(reader.finish().unwrap().metadata().pin(), &input.pin);
            }
        }
        fs::write(input.directory.join("events.ndjson.zst"), original).unwrap();
        let mut retry = open_pinned(&input, &ReadLimits::default()).unwrap();
        let mut sources = 0;
        while retry.next_delivery().unwrap().is_some() {
            sources += 1;
        }
        assert_eq!(sources, 3);
        assert_eq!(retry.finish().unwrap().metadata().pin(), &input.pin);
    }
}

#[test]
fn snapshot_root_is_used_without_fallback_and_cleaned_on_drop_or_failure() {
    use crate::reader::{ReadCheckpoint, open_pinned_with_checkpoint};
    let canonical = TempDir::new("scratch-source").unwrap();
    let output = TempDir::new("scratch-output").unwrap();
    let scratch = tempfile::tempdir().unwrap();
    canonical_fixture(canonical.path());
    let built = build_window(
        canonical.path(),
        output.path(),
        0,
        10,
        &spec(),
        &mut FakeNormalizer::new(FakeMode::Normal),
    )
    .unwrap();
    let input = PinnedDerivative {
        directory: built.derivative.directory,
        pin: built.derivative.pin,
    };
    let mut limits = ReadLimits {
        snapshot_root: Some(scratch.path().to_path_buf()),
        ..ReadLimits::default()
    };
    let reader = open_pinned_with_checkpoint(&input, &limits, |stage, path| {
        if stage == ReadCheckpoint::SnapshotCopied {
            assert_eq!(path.parent(), Some(scratch.path()));
            assert!(path.join("events.ndjson.zst").is_file());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(path).unwrap().permissions().mode() & 0o777,
                    0o700
                );
            }
        }
    })
    .unwrap();
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 1);
    drop(reader);
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
    assert!(
        open_pinned_with_checkpoint(&input, &limits, |stage, path| {
            if stage == ReadCheckpoint::SnapshotCopied {
                fs::write(path.join("events.ndjson.zst"), []).unwrap();
            }
        })
        .is_err()
    );
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
    limits.snapshot_root = Some(scratch.path().join("missing-volume"));
    assert!(open_pinned(&input, &limits).is_err());
    assert!(!limits.snapshot_root.as_ref().unwrap().exists());
    limits.snapshot_root = Some(input.directory.join("receipt.json"));
    assert!(open_pinned(&input, &limits).is_err());
}

#[test]
fn default_build_line_limit_includes_lf_and_fails_before_commit() {
    for extra in [0, 1] {
        let canonical = TempDir::new("line-limit-source").unwrap();
        let output = TempDir::new("line-limit-output").unwrap();
        canonical_fixture_count(canonical.path(), 1, 10);
        let result = build_window(
            canonical.path(),
            output.path(),
            0,
            10,
            &spec(),
            &mut FakeNormalizer::new(FakeMode::ControlLineBytes(
                ReadLimits::default().max_line_bytes as usize + extra,
            )),
        );
        if extra == 0 {
            let built = result.unwrap();
            assert_eq!(
                built.derivative.manifest.events.logical.byte_length,
                ReadLimits::default().max_line_bytes
            );
            verify_derivative(&built.derivative.directory).unwrap();
        } else {
            assert!(
                matches!(result, Err(BuildError::Verification(ref message)) if message == "normalized line exceeds read limit")
            );
            assert!(output_directories(output.path()).is_empty());
        }
    }
}
