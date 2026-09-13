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
