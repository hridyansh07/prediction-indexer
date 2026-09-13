//! Immutable normalized derivatives built from one audited canonical window.
//!
//! The receipt is the sole commit marker. Events and expected rejects are staged
//! privately while Phase 0 is consumed, and cannot be published until verified
//! EOF, normalizer `finish`, frame finish, and file fsync all succeed.

mod schema;
mod verify;

#[cfg(test)]
mod tests;

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use canonical_normalizer::{Normalization, Normalize, event_header, segment_record, validate_code};
use fs2::FileExt;
use indexer_finalize::{
    CanonicalSelection, CertifiedPolicy, LowerBoundPolicy, SelectionPolicy, create_dir_all_durable,
    select_canonical_windows,
};
pub use indexer_types::Sha256;
use prediction_encoder::{
    CODEC_VERSION, DEFAULT_ZSTD_LEVEL, EncodeResult, StreamingEncoder, encoder_version,
};
use replay_domain::{NormalizationFault, SEGMENT_SCHEMA_VERSION, SegmentEvent, SegmentRecord};
use serde::Serialize;
use sha2::{Digest, Sha256 as Sha256Hasher};

pub use schema::{
    CompressedOutput, CompressionContract, DerivativeCounts, DerivativeManifest, DerivativeReceipt,
    DerivativeSpec, LogicalIdentity, NormalizationPolicy, PlainOutput, RejectDisposition,
    RejectRecord, SourceReceipt, StoredIdentity,
};
pub use verify::{DerivativePin, VerifiedDerivative, verify_derivative};

const ADDRESS_DOMAIN: &[u8] = b"prediction-indexer/replay-normalized-derivative/v1";
const REJECT_DOMAIN: &[u8] = b"prediction-indexer/replay-normalization-reject/v1";
const DERIVATIVE_EVENTS_FILE: &str = "events.ndjson.zst";
const DERIVATIVE_REJECTS_FILE: &str = "rejects.ndjson.zst";
const DERIVATIVE_MANIFEST_FILE: &str = "manifest.json";
const DERIVATIVE_RECEIPT_FILE: &str = "receipt.json";
static STAGE_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildDisposition {
    Committed,
    VerifiedNoOp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildOutcome {
    pub disposition: BuildDisposition,
    pub derivative: VerifiedDerivative,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildError {
    InvalidSpec(String),
    Audit(String),
    Normalizer(String),
    NormalizerPanic,
    Serialization(String),
    Io(String),
    Conflict(String),
    Verification(String),
}

impl fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSpec(error) => write!(formatter, "invalid derivative spec: {error}"),
            Self::Audit(error) => write!(formatter, "canonical audit failed: {error}"),
            Self::Normalizer(error) => write!(formatter, "normalizer failed: {error}"),
            Self::NormalizerPanic => formatter.write_str("normalizer panicked"),
            Self::Serialization(error) => write!(formatter, "serialization failed: {error}"),
            Self::Io(error) => write!(formatter, "derivative I/O failed: {error}"),
            Self::Conflict(error) => write!(formatter, "immutable derivative conflict: {error}"),
            Self::Verification(error) => {
                write!(formatter, "derivative verification failed: {error}")
            }
        }
    }
}

impl std::error::Error for BuildError {}

/// Builds exactly one derivative for exactly one canonical window.
///
/// The exact-window restriction is compositional with Phase 0: no API change or
/// second read is needed, and each derivative binds one canonical receipt.
pub fn build_window<N: Normalize>(
    canonical_root: &Path,
    output_root: &Path,
    window_start_ns: u64,
    window_end_ns: u64,
    spec: &DerivativeSpec,
    normalizer: &mut N,
) -> Result<BuildOutcome, BuildError> {
    let policy = SelectionPolicy {
        certified: CertifiedPolicy::AllowUncertified,
        lower_bound: LowerBoundPolicy::RequireWindowBoundary,
    };
    let selection =
        select_canonical_windows(canonical_root, window_start_ns, window_end_ns, policy)
            .map_err(BuildError::Audit)?;
    build_window_inner(selection, output_root, spec, normalizer, |_| Ok(()))
}

/// Materializes a caller-selected Phase 0 input. The selection must contain one
/// exact canonical window; the materializer consumes it and owns publication.
pub fn materialize_window<N: Normalize>(
    selection: CanonicalSelection,
    output_root: &Path,
    spec: &DerivativeSpec,
    normalizer: &mut N,
) -> Result<BuildOutcome, BuildError> {
    build_window_inner(selection, output_root, spec, normalizer, |_| Ok(()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Checkpoint {
    FramesFinished,
    FilesSynced,
    BeforeManifestSerialization,
    CandidateVerified,
    DirectoryPublished,
    ReceiptSynced,
    ReceiptRenamed,
}

fn build_window_inner<N, H>(
    selection: CanonicalSelection,
    output_root: &Path,
    spec: &DerivativeSpec,
    normalizer: &mut N,
    mut checkpoint: H,
) -> Result<BuildOutcome, BuildError>
where
    N: Normalize,
    H: FnMut(Checkpoint) -> Result<(), BuildError>,
{
    let (window_start_ns, window_end_ns) = selection.requested_interval();
    spec.validate_for(window_start_ns, window_end_ns)
        .map_err(BuildError::InvalidSpec)?;
    if normalizer.descriptor().bundle_sha256 != spec.normalizer_bundle_sha256
        || normalizer.descriptor().config_sha256 != spec.normalizer_config_sha256
    {
        return Err(BuildError::InvalidSpec(
            "normalizer descriptor does not match the addressed bundle/config".to_owned(),
        ));
    }
    let window_root = output_root.join(format!("window={window_start_ns}"));
    create_dir_all_durable(&window_root).map_err(BuildError::Io)?;

    if selection.receipt_identities().len() != 1 {
        return Err(BuildError::InvalidSpec(
            "build_window requires bounds matching exactly one canonical receipt".to_owned(),
        ));
    }
    let selected = selection
        .receipt_identities()
        .next()
        .expect("length checked above");
    if selected.window_start_ns != window_start_ns || selected.window_end_ns != window_end_ns {
        return Err(BuildError::InvalidSpec(
            "requested bounds do not match one canonical receipt".to_owned(),
        ));
    }
    let source_receipt = SourceReceipt::from(selected);
    let address = derivative_address(&source_receipt, spec)?;
    let stage = stage_path(&window_root, &address);
    create_dir_all_durable(&stage).map_err(BuildError::Io)?;
    let mut stage_guard = StageGuard::new(stage.clone());

    let events_file = create_file(&stage.join(format!("{DERIVATIVE_EVENTS_FILE}.open")))?;
    let rejects_file = create_file(&stage.join(format!("{DERIVATIVE_REJECTS_FILE}.open")))?;
    let mut events = StreamingEncoder::new(events_file, DEFAULT_ZSTD_LEVEL)
        .map_err(|error| BuildError::Io(error.to_string()))?;
    let mut rejects = StreamingEncoder::new(rejects_file, DEFAULT_ZSTD_LEVEL)
        .map_err(|error| BuildError::Io(error.to_string()))?;
    let mut counts = DerivativeCounts::default();
    let mut reader = selection.open().map_err(BuildError::Audit)?;

    loop {
        let Some(source) = reader.next_record().map_err(BuildError::Audit)? else {
            break;
        };
        counts.input_records = checked_add(counts.input_records, 1, "input_records")?;
        let normalized = catch_unwind(AssertUnwindSafe(|| normalizer.normalize(&source)))
            .map_err(|_| BuildError::NormalizerPanic)?
            .map_err(|error| BuildError::Normalizer(error.to_string()))?;
        match normalized {
            Normalization::Events(children) if children.is_empty() => {
                let header = event_header(&source, 0)
                    .map_err(|error| BuildError::Normalizer(error.to_string()))?;
                let envelope = String::from_utf8(source.envelope)
                    .map_err(|error| BuildError::Serialization(error.to_string()))?;
                let ignored = RejectRecord::ignored(header, envelope, "zero_children".to_owned())
                    .map_err(BuildError::Serialization)?;
                let encoded = ignored
                    .to_canonical_json()
                    .map_err(BuildError::Serialization)?;
                write_line(&mut rejects, &encoded, DERIVATIVE_REJECTS_FILE)?;
                counts.intentionally_ignored_records = checked_add(
                    counts.intentionally_ignored_records,
                    1,
                    "intentionally_ignored_records",
                )?;
            }
            Normalization::Events(children) => {
                counts.accepted_source_records =
                    checked_add(counts.accepted_source_records, 1, "accepted_source_records")?;
                for (index, event) in children.into_iter().enumerate() {
                    let index = u32::try_from(index).map_err(|_| {
                        BuildError::Normalizer(
                            "one source produced more than u32::MAX children".to_owned(),
                        )
                    })?;
                    let record = segment_record(&source, index, event)
                        .map_err(|error| BuildError::Normalizer(error.to_string()))?;
                    write_line(
                        &mut events,
                        &record.to_canonical_json(),
                        DERIVATIVE_EVENTS_FILE,
                    )?;
                    counts.accepted_events =
                        checked_add(counts.accepted_events, 1, "accepted_events")?;
                }
            }
            Normalization::Ignored { reason_code } => {
                validate_code(&reason_code, "reason_code")
                    .map_err(|error| BuildError::Normalizer(error.to_string()))?;
                let header = event_header(&source, 0)
                    .map_err(|error| BuildError::Normalizer(error.to_string()))?;
                let envelope = String::from_utf8(source.envelope)
                    .map_err(|error| BuildError::Serialization(error.to_string()))?;
                let ignored = RejectRecord::ignored(header, envelope, reason_code)
                    .map_err(BuildError::Serialization)?;
                let encoded = ignored
                    .to_canonical_json()
                    .map_err(BuildError::Serialization)?;
                write_line(&mut rejects, &encoded, DERIVATIVE_REJECTS_FILE)?;
                counts.intentionally_ignored_records = checked_add(
                    counts.intentionally_ignored_records,
                    1,
                    "intentionally_ignored_records",
                )?;
            }
            Normalization::Reject(reject) => {
                reject
                    .validate()
                    .map_err(|error| BuildError::Normalizer(error.to_string()))?;
                let reject_id =
                    reject_id(&address, &source, reject.parser_version, &reject.error_code);
                let header = event_header(&source, 0)
                    .map_err(|error| BuildError::Normalizer(error.to_string()))?;
                // A reject produces both a sidecar record and a domain fault.
                // Duplicate their small typed header, then consume the sole raw
                // envelope allocation into the sidecar.
                let fault_header = header.clone();
                let envelope = String::from_utf8(source.envelope)
                    .map_err(|error| BuildError::Serialization(error.to_string()))?;
                let sidecar = RejectRecord::parse_reject(
                    header,
                    envelope,
                    RejectDisposition::ParseReject {
                        reject_id: reject_id.clone(),
                        parser_version: reject.parser_version,
                        error_code: reject.error_code,
                        instrument_hint: reject.instrument_hint,
                        impact: reject.impact.clone(),
                        normalizer_bundle_sha256: spec.normalizer_bundle_sha256,
                        normalizer_config_sha256: spec.normalizer_config_sha256,
                    },
                )
                .map_err(BuildError::Serialization)?;
                let encoded = sidecar
                    .to_canonical_json()
                    .map_err(BuildError::Serialization)?;
                write_line(&mut rejects, &encoded, DERIVATIVE_REJECTS_FILE)?;
                let fault = NormalizationFault::new(reject_id, reject.impact)
                    .map_err(|error| BuildError::Normalizer(error.to_string()))?;
                let record =
                    SegmentRecord::new(fault_header, SegmentEvent::NormalizationFault(fault))
                        .map_err(|error| BuildError::Normalizer(error.to_string()))?;
                write_line(
                    &mut events,
                    &record.to_canonical_json(),
                    DERIVATIVE_EVENTS_FILE,
                )?;
                counts.rejected_source_records =
                    checked_add(counts.rejected_source_records, 1, "rejected_source_records")?;
                counts.normalization_fault_events = checked_add(
                    counts.normalization_fault_events,
                    1,
                    "normalization_fault_events",
                )?;
            }
        }
    }

    catch_unwind(AssertUnwindSafe(|| normalizer.finish()))
        .map_err(|_| BuildError::NormalizerPanic)?
        .map_err(|error| BuildError::Normalizer(error.to_string()))?;
    let audited = reader.finish().map_err(BuildError::Audit)?;
    if audited.receipt_identities().len() != 1
        || audited.receipt_identities()[0].sha256 != source_receipt.sha256
        || audited.effective_interval() != (window_start_ns, window_end_ns)
        || audited.records_verified() != counts.input_records
    {
        return Err(BuildError::Audit(
            "finished audit capability disagrees with selected source".to_owned(),
        ));
    }

    let (events_file, events_result) = events
        .finish()
        .map_err(|error| BuildError::Io(error.to_string()))?;
    let (rejects_file, rejects_result) = rejects
        .finish()
        .map_err(|error| BuildError::Io(error.to_string()))?;
    checkpoint(Checkpoint::FramesFinished)?;
    events_file
        .sync_all()
        .map_err(io_error("fsyncing events"))?;
    rejects_file
        .sync_all()
        .map_err(io_error("fsyncing rejects"))?;
    drop(events_file);
    drop(rejects_file);
    rename_in_stage(
        &stage,
        &format!("{DERIVATIVE_EVENTS_FILE}.open"),
        DERIVATIVE_EVENTS_FILE,
    )?;
    rename_in_stage(
        &stage,
        &format!("{DERIVATIVE_REJECTS_FILE}.open"),
        DERIVATIVE_REJECTS_FILE,
    )?;
    sync_directory(&stage)?;
    checkpoint(Checkpoint::FilesSynced)?;

    let events_output = compressed_output(DERIVATIVE_EVENTS_FILE, events_result)?;
    let rejects_output = compressed_output(DERIVATIVE_REJECTS_FILE, rejects_result)?;
    checkpoint(Checkpoint::BeforeManifestSerialization)?;
    let manifest = DerivativeManifest {
        manifest_version: schema::MANIFEST_VERSION,
        derivative_address: address.clone(),
        source_receipt: source_receipt.clone(),
        requested_start_ns: window_start_ns,
        requested_end_ns: window_end_ns,
        effective_start_ns: audited.effective_interval().0,
        effective_end_ns: audited.effective_interval().1,
        normalized_schema_version: spec.normalized_schema_version,
        event_serialization_version: schema::EVENT_SERIALIZATION_VERSION,
        reject_serialization_version: schema::REJECT_SERIALIZATION_VERSION,
        materializer_version: schema::MATERIALIZER_VERSION,
        normalizer_bundle_sha256: spec.normalizer_bundle_sha256,
        normalizer_config_sha256: spec.normalizer_config_sha256,
        policy: spec.policy.clone(),
        counts,
        events: events_output.clone(),
        rejects: rejects_output.clone(),
    };
    manifest.validate().map_err(BuildError::Serialization)?;
    let manifest_bytes = canonical_document(&manifest)?;
    let manifest_open = format!("{DERIVATIVE_MANIFEST_FILE}.open");
    write_synced(&stage.join(&manifest_open), &manifest_bytes)?;
    rename_in_stage(&stage, &manifest_open, DERIVATIVE_MANIFEST_FILE)?;
    sync_directory(&stage)?;
    let manifest_output = PlainOutput {
        file: DERIVATIVE_MANIFEST_FILE.to_owned(),
        sha256: sha256(&manifest_bytes),
        byte_length: manifest_bytes.len() as u64,
    };
    let receipt = DerivativeReceipt {
        receipt_version: schema::RECEIPT_VERSION,
        derivative_address: address.clone(),
        source_receipt_sha256: source_receipt.sha256,
        normalized_schema_version: SEGMENT_SCHEMA_VERSION,
        materializer_version: schema::MATERIALIZER_VERSION,
        normalizer_bundle_sha256: spec.normalizer_bundle_sha256,
        normalizer_config_sha256: spec.normalizer_config_sha256,
        policy_sha256: spec.policy.policy_sha256,
        manifest: manifest_output,
        events: events_output,
        rejects: rejects_output,
    };
    receipt.validate().map_err(BuildError::Serialization)?;
    let receipt_bytes = canonical_document(&receipt)?;
    verify::verify_candidate(&stage, &receipt_bytes).map_err(BuildError::Verification)?;
    checkpoint(Checkpoint::CandidateVerified)?;

    let lock = acquire_lock(&window_root, &address)?;
    let final_directory = window_root.join(&address);
    if final_directory.join(DERIVATIVE_RECEIPT_FILE).is_file() {
        let existing = verify_derivative(&final_directory).map_err(|error| {
            BuildError::Conflict(format!(
                "address {address} has an invalid committed derivative: {error}"
            ))
        })?;
        if existing.receipt_bytes() == receipt_bytes {
            drop(lock);
            return Ok(BuildOutcome {
                disposition: BuildDisposition::VerifiedNoOp,
                derivative: existing,
            });
        }
        return Err(BuildError::Conflict(format!(
            "address {address} is already committed with different bytes"
        )));
    }
    if final_directory.exists() {
        std::fs::remove_dir_all(&final_directory)
            .map_err(io_error("removing uncommitted derivative"))?;
        sync_directory(&window_root)?;
    }
    std::fs::rename(&stage, &final_directory)
        .map_err(io_error("publishing derivative directory"))?;
    stage_guard.published = true;
    sync_directory(&window_root)?;
    checkpoint(Checkpoint::DirectoryPublished)?;
    write_receipt_last(&final_directory, &receipt_bytes, &mut checkpoint)?;
    drop(lock);
    let derivative = verify_derivative(&final_directory).map_err(BuildError::Verification)?;
    Ok(BuildOutcome {
        disposition: BuildDisposition::Committed,
        derivative,
    })
}

fn derivative_address(source: &SourceReceipt, spec: &DerivativeSpec) -> Result<String, BuildError> {
    let mut digest = Sha256Hasher::new();
    digest.update(ADDRESS_DOMAIN);
    for bytes in [canonical_value(source)?, canonical_value(spec)?] {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    digest.update(schema::EVENT_SERIALIZATION_VERSION.to_be_bytes());
    digest.update(schema::REJECT_SERIALIZATION_VERSION.to_be_bytes());
    digest.update(schema::MATERIALIZER_VERSION.to_be_bytes());
    Ok(format!("{:x}", digest.finalize()))
}

fn reject_id(
    address: &str,
    source: &indexer_finalize::JoinedCanonicalRecord,
    parser_version: u32,
    error_code: &str,
) -> String {
    let header = event_header(source, 0).expect("Phase 0 source was validated before reject id");
    reject_id_from_header(address, &header, parser_version, error_code)
}

fn reject_id_from_header(
    address: &str,
    header: &replay_domain::EventHeader,
    parser_version: u32,
    error_code: &str,
) -> String {
    let mut digest = Sha256Hasher::new();
    digest.update(REJECT_DOMAIN);
    for bytes in [
        address.as_bytes(),
        &header.address().canonical_seq().to_be_bytes(),
        header.address().lane().as_str().as_bytes(),
        &header.address().delivery_index().to_be_bytes(),
        &parser_version.to_be_bytes(),
        error_code.as_bytes(),
    ] {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    format!("{:x}", digest.finalize())
}

fn compressed_output(file: &str, result: EncodeResult) -> Result<CompressedOutput, BuildError> {
    Ok(CompressedOutput {
        file: file.to_owned(),
        content_encoding: "zstd".to_owned(),
        logical: LogicalIdentity {
            sha256: parse_codec_sha256(&result.logical.sha256)?,
            byte_length: result.logical.byte_length,
            line_count: result.logical.line_count,
        },
        stored: StoredIdentity {
            sha256: parse_codec_sha256(&result.stored.sha256)?,
            byte_length: result.stored.byte_length,
        },
        compression: CompressionContract {
            algorithm: "zstd".to_owned(),
            level: DEFAULT_ZSTD_LEVEL,
            frame_checksum: true,
            dictionary: None,
            frame_count: 1,
            encoder: format!(
                "prediction-encoder-rust/{CODEC_VERSION}; {}",
                encoder_version()
            ),
        },
    })
}

fn canonical_document<T: Serialize>(value: &T) -> Result<Vec<u8>, BuildError> {
    let mut bytes = canonical_value(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn canonical_value<T: Serialize>(value: &T) -> Result<Vec<u8>, BuildError> {
    serde_json::to_vec(value).map_err(|error| BuildError::Serialization(error.to_string()))
}

fn write_line<W: Write>(sink: &mut W, bytes: &[u8], name: &str) -> Result<(), BuildError> {
    sink.write_all(bytes)
        .and_then(|()| sink.write_all(b"\n"))
        .map_err(io_error(&format!("writing {name}")))
}

fn create_file(path: &Path) -> Result<File, BuildError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_error(&format!("creating {}", path.display())))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), BuildError> {
    let mut file = create_file(path)?;
    file.write_all(bytes)
        .map_err(io_error(&format!("writing {}", path.display())))?;
    file.sync_all()
        .map_err(io_error(&format!("fsyncing {}", path.display())))
}

fn rename_in_stage(stage: &Path, from: &str, to: &str) -> Result<(), BuildError> {
    std::fs::rename(stage.join(from), stage.join(to))
        .map_err(io_error(&format!("renaming {from} to {to}")))
}

fn write_receipt_last<H>(
    directory: &Path,
    bytes: &[u8],
    checkpoint: &mut H,
) -> Result<(), BuildError>
where
    H: FnMut(Checkpoint) -> Result<(), BuildError>,
{
    let temporary = directory.join(format!("{DERIVATIVE_RECEIPT_FILE}.open"));
    write_synced(&temporary, bytes)?;
    checkpoint(Checkpoint::ReceiptSynced)?;
    std::fs::rename(&temporary, directory.join(DERIVATIVE_RECEIPT_FILE))
        .map_err(io_error("renaming derivative receipt"))?;
    checkpoint(Checkpoint::ReceiptRenamed)?;
    sync_directory(directory)
}

fn acquire_lock(output_root: &Path, address: &str) -> Result<File, BuildError> {
    let path = output_root.join(format!(".{address}.lock"));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(io_error("opening derivative publication lock"))?;
    file.lock_exclusive()
        .map_err(io_error("locking derivative publication"))?;
    Ok(file)
}

fn sync_directory(path: &Path) -> Result<(), BuildError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_error(&format!("fsyncing directory {}", path.display())))
}

fn stage_path(root: &Path, address: &str) -> PathBuf {
    let nonce = STAGE_NONCE.fetch_add(1, Ordering::Relaxed);
    root.join(format!(".{address}.{}.{}.open", std::process::id(), nonce))
}

fn checked_add(value: u64, amount: u64, field: &str) -> Result<u64, BuildError> {
    value
        .checked_add(amount)
        .ok_or_else(|| BuildError::Serialization(format!("{field} overflows u64")))
}

fn sha256(bytes: &[u8]) -> Sha256 {
    Sha256::digest(bytes)
}

fn parse_codec_sha256(hex: &str) -> Result<Sha256, BuildError> {
    Sha256::from_hex(hex).map_err(|error| {
        BuildError::Serialization(format!("encoder returned invalid SHA-256: {error}"))
    })
}

fn io_error(context: &str) -> impl FnOnce(std::io::Error) -> BuildError + '_ {
    move |error| BuildError::Io(format!("{context}: {error}"))
}

struct StageGuard {
    path: PathBuf,
    published: bool,
}

impl StageGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            published: false,
        }
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
