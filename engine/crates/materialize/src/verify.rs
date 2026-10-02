use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use indexer_types::EnvelopeView;
use indexer_types::Sha256;
use prediction_encoder::{
    LogicalIdentity as CodecLogicalIdentity, StoredIdentity as CodecStoredIdentity,
    StreamingDecoder, StructuralDecoder, StructuralIdentity,
};
use replay_domain::{SegmentEvent, SegmentRecord};
use serde::{Serialize, de::DeserializeOwned};

use super::schema::{DerivativeManifest, DerivativeReceipt, RejectDisposition, RejectRecord};
use super::{DERIVATIVE_MANIFEST_FILE, DERIVATIVE_RECEIPT_FILE, DerivativeSpec};

#[derive(Default)]
pub(crate) struct VerificationCounts {
    input_records: u64,
    accepted_source_records: u64,
    event_lines: u64,
    faults: u64,
    reject_lines: u64,
    parse_rejects: u64,
    ignored: u64,
}

struct Binding {
    reject_id: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivativePin {
    pub derivative_address: String,
    pub receipt_sha256: Sha256,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedDerivative {
    pub directory: PathBuf,
    pub manifest: DerivativeManifest,
    pub receipt: DerivativeReceipt,
    pub pin: DerivativePin,
    receipt_bytes: Vec<u8>,
}

impl VerifiedDerivative {
    pub(crate) fn receipt_bytes(&self) -> &[u8] {
        &self.receipt_bytes
    }
}

/// Independently verifies the marker, strict schemas, canonical JSON, both
/// Zstandard identities, one-frame EOF, line schemas, and reject/fault pairing.
/// It drains the same single-pass stream a pinned read uses, in
/// [`ReadMode::Audit`] (every SHA-256 identity and canonical re-encode
/// equality), and returns only after that stream's clean, fully checked EOF:
/// a standalone verdict. It is the on-demand audit behind `--inspect-pin`.
pub fn verify_derivative(directory: &Path) -> Result<VerifiedDerivative, String> {
    let receipt_path = directory.join(DERIVATIVE_RECEIPT_FILE);
    if !receipt_path.is_file() {
        return Err("derivative has no receipt commit marker".to_owned());
    }
    let receipt_bytes = super::reader::read_bounded(
        &receipt_path,
        super::ReadLimits::default().max_metadata_bytes,
    )?;
    let (receipt, receipt_bytes) = decode_receipt_document(&receipt_bytes, &receipt_path)?;
    verify_contents(directory, receipt, receipt_bytes, true)
}

pub(crate) fn inspect_contents(
    directory: &Path,
    receipt: DerivativeReceipt,
    receipt_bytes: Vec<u8>,
    require_addressed_directory: bool,
    manifest_bytes: &[u8],
) -> Result<VerifiedDerivative, String> {
    receipt.validate()?;
    if require_addressed_directory {
        let directory_name = directory
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| "derivative directory has no UTF-8 address".to_owned())?;
        if directory_name != receipt.derivative_address {
            return Err("derivative directory does not match receipt address".to_owned());
        }
    }

    let manifest_path = directory.join(DERIVATIVE_MANIFEST_FILE);
    if manifest_bytes.len() as u64 != receipt.manifest.byte_length
        || sha256(manifest_bytes) != receipt.manifest.sha256
    {
        return Err("manifest identity disagrees with receipt".to_owned());
    }
    let (manifest, canonical_manifest_bytes) = if receipt.materializer_version == 1 {
        let (wire, bytes) =
            decode_canonical_document::<crate::profile1::Manifest>(manifest_bytes, &manifest_path)?;
        (DerivativeManifest::from(wire), bytes)
    } else {
        decode_canonical_document::<DerivativeManifest>(manifest_bytes, &manifest_path)?
    };
    if canonical_manifest_bytes != manifest_bytes {
        return Err("manifest is not canonically encoded".to_owned());
    }
    manifest.validate()?;
    if manifest.derivative_address != receipt.derivative_address
        || manifest.source_receipt.sha256 != receipt.source_receipt_sha256
        || manifest.normalized_schema_version != receipt.normalized_schema_version
        || manifest.materializer_version != receipt.materializer_version
        || manifest.normalizer_bundle_sha256 != receipt.normalizer_bundle_sha256
        || manifest.normalizer_config_sha256 != receipt.normalizer_config_sha256
        || manifest.policy.policy_sha256 != receipt.policy_sha256
        || manifest.events != receipt.events
        || manifest.rejects != receipt.rejects
        || manifest.sources != receipt.sources
    {
        return Err("manifest and receipt bindings disagree".to_owned());
    }
    let spec = DerivativeSpec {
        normalized_schema_version: manifest.normalized_schema_version,
        normalizer_bundle_sha256: manifest.normalizer_bundle_sha256,
        normalizer_config_sha256: manifest.normalizer_config_sha256,
        policy: manifest.policy.clone(),
    };
    let expected_address = super::derivative_address_versions(
        &manifest.source_receipt,
        &spec,
        manifest.event_serialization_version,
        manifest.reject_serialization_version,
        manifest.materializer_version,
    )
    .map_err(|error| error.to_string())?;
    if expected_address != receipt.derivative_address {
        return Err("derivative address does not match its domain-separated inputs".to_owned());
    }

    Ok(VerifiedDerivative {
        directory: directory.to_path_buf(),
        manifest,
        receipt: receipt.clone(),
        pin: DerivativePin {
            derivative_address: receipt.derivative_address,
            receipt_sha256: sha256(&receipt_bytes),
        },
        receipt_bytes,
    })
}

fn verify_contents(
    directory: &Path,
    receipt: DerivativeReceipt,
    receipt_bytes: Vec<u8>,
    addressed: bool,
) -> Result<VerifiedDerivative, String> {
    let manifest_bytes = super::reader::read_bounded(
        &directory.join(DERIVATIVE_MANIFEST_FILE),
        super::ReadLimits::default().max_metadata_bytes,
    )?;
    let verified = inspect_contents(
        directory,
        receipt,
        receipt_bytes,
        addressed,
        &manifest_bytes,
    )?;
    verify_data(directory, &verified.manifest, &super::ReadLimits::default())?;
    Ok(verified)
}

/// The audit form of the single verification pass shared with every pinned
/// read: each output is decoded once and each line parsed once, with the
/// per-record, per-delivery and end-of-stream checks applied as it streams.
pub(crate) fn verify_data(
    directory: &Path,
    manifest: &DerivativeManifest,
    limits: &super::ReadLimits,
) -> Result<(), String> {
    super::reader::verify_stream(directory, manifest, limits, ReadMode::Audit)
}

/// How much of a derivative's byte identity one read re-proves.
///
/// Every mode applies the same semantic, ordering, count, limit and Zstandard
/// frame checks (one checksummed frame, no truncation or trailing bytes, the
/// stored length, decoded length and LF count). They differ only in work that
/// re-proves bytes another boundary already bound by SHA-256.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadMode {
    /// `verify_derivative`, and through it `--inspect-pin`, the existing
    /// address no-op check and every other audit: both SHA-256 identities of
    /// every output and decode/re-encode equality of every line. This is the
    /// independent verdict for bytes nobody has hash-bound yet.
    Audit,
    /// `open_pinned`, the Replay walker's hot read. The installed directory
    /// was bound to the pin by SHA-256 at install, and `open_pinned` binds the
    /// receipt and manifest to the pin again by SHA-256. Data bytes are not
    /// re-hashed and lines are decoded once without re-encoding them.
    PinnedReplay,
}

/// Parses one normalized event line and checks canonical child order. `Audit`
/// also proves the line's canonical encoding; `PinnedReplay` decodes it in a
/// single typed pass with the same closed-schema and domain validation.
pub(crate) fn parse_event(
    line: &[u8],
    previous: &mut Option<(i64, u32)>,
    counts: &mut VerificationCounts,
    mode: ReadMode,
) -> Result<SegmentRecord, String> {
    let record = match mode {
        ReadMode::Audit => {
            #[cfg(test)]
            probe::canonical_check();
            SegmentRecord::from_canonical_json(line)
        }
        ReadMode::PinnedReplay => SegmentRecord::from_json(line),
    }
    .map_err(|error| format!("invalid normalized event: {error}"))?;
    let current = (
        record.header().address().canonical_seq(),
        record.header().address().event_index(),
    );
    verify_event_order(*previous, current)?;
    *previous = Some(current);
    counts.event_lines += 1;
    if matches!(record.event(), SegmentEvent::NormalizationFault(_)) {
        counts.faults += 1;
    }
    Ok(record)
}

/// Parses one sidecar line: exact envelope provenance, source order, and, for
/// a parse reject, the normalizer identity and reject_id binding. Only `Audit`
/// re-encodes the line to prove its canonical encoding.
pub(crate) fn parse_reject(
    line: &[u8],
    previous_seq: &mut Option<i64>,
    counts: &mut VerificationCounts,
    manifest: &DerivativeManifest,
    mode: ReadMode,
) -> Result<RejectRecord, String> {
    let record = match mode {
        ReadMode::Audit => {
            #[cfg(test)]
            probe::canonical_check();
            RejectRecord::from_canonical_json(line)?
        }
        ReadMode::PinnedReplay => RejectRecord::from_json(line)?,
    };
    verify_reject_source(&record)?;
    let current_seq = record.header().address().canonical_seq();
    if previous_seq.is_some_and(|previous| current_seq <= previous) {
        return Err("reject records are not in canonical source order".to_owned());
    }
    *previous_seq = Some(current_seq);
    counts.reject_lines += 1;
    match record.disposition() {
        RejectDisposition::ParseReject {
            reject_id,
            parser_version,
            error_code,
            normalizer_bundle_sha256,
            normalizer_config_sha256,
            ..
        } => {
            if normalizer_bundle_sha256 != &manifest.normalizer_bundle_sha256
                || normalizer_config_sha256 != &manifest.normalizer_config_sha256
            {
                return Err("parse reject names the wrong normalizer identity".to_owned());
            }
            let expected = super::reject_id_from_header(
                &manifest.derivative_address,
                record.header(),
                *parser_version,
                error_code,
            );
            if expected != *reject_id {
                return Err("parse reject_id does not bind its source and parser result".to_owned());
            }
            counts.parse_rejects += 1;
        }
        RejectDisposition::IntentionallyIgnored { .. } => counts.ignored += 1,
    }
    Ok(record)
}

/// Checks one source delivery's disposition: its normalization faults pair
/// exactly with its parse reject, and it is accepted, rejected or ignored, never
/// two of those. Faults and parse rejects are each in canonical order and pair
/// only on identical headers, so this per-delivery check is exactly the former
/// whole-file lockstep pairing of the fault stream against the reject stream.
pub(crate) fn verify_disposition(
    records: &[SegmentRecord],
    reject: Option<&RejectRecord>,
    counts: &mut VerificationCounts,
) -> Result<(), String> {
    let unpaired =
        || "normalization fault events do not pair exactly with parse rejects".to_owned();
    let mut faults = records.iter().filter_map(|record| match record.event() {
        SegmentEvent::NormalizationFault(fault) => Some((record, fault)),
        _ => None,
    });
    let fault = match faults.next() {
        Some((record, fault)) => Some(Binding {
            reject_id: fault.reject_id().to_owned(),
            bytes: serde_json::to_vec(&(record.header(), fault.impact()))
                .map_err(|error| format!("encoding normalization fault binding: {error}"))?,
        }),
        None => None,
    };
    if faults.next().is_some() {
        return Err(unpaired());
    }
    let parse = match reject {
        Some(record) => match record.disposition() {
            RejectDisposition::ParseReject {
                reject_id, impact, ..
            } => Some(Binding {
                reject_id: reject_id.clone(),
                bytes: serde_json::to_vec(&(record.header(), impact))
                    .map_err(|error| format!("encoding parse reject binding: {error}"))?,
            }),
            RejectDisposition::IntentionallyIgnored { .. } => None,
        },
        None => None,
    };
    match (fault, parse) {
        (None, None) => {}
        (Some(left), Some(right))
            if left.reject_id == right.reject_id && left.bytes == right.bytes => {}
        _ => return Err(unpaired()),
    }
    let accepted = records
        .iter()
        .any(|record| !matches!(record.event(), SegmentEvent::NormalizationFault(_)));
    if accepted && reject.is_some() {
        return Err("source has more than one normalization disposition".to_owned());
    }
    counts.input_records += 1;
    if accepted {
        counts.accepted_source_records += 1;
    }
    Ok(())
}

/// The end-of-stream count agreement, after every line has been read.
pub(crate) fn verify_counts(
    counts: &VerificationCounts,
    manifest: &DerivativeManifest,
) -> Result<(), String> {
    if counts.input_records != manifest.counts.input_records
        || counts.accepted_source_records != manifest.counts.accepted_source_records
        || counts.event_lines != manifest.events.logical.line_count
        || counts.reject_lines != manifest.rejects.logical.line_count
        || counts.faults != manifest.counts.normalization_fault_events
        || counts.parse_rejects != manifest.counts.rejected_source_records
        || counts.ignored != manifest.counts.intentionally_ignored_records
    {
        return Err("verified derivative lines disagree with manifest counts".to_owned());
    }
    Ok(())
}

pub(crate) fn verify_event_order(
    previous: Option<(i64, u32)>,
    current: (i64, u32),
) -> Result<(), String> {
    let valid = match previous {
        None => current.1 == 0,
        Some((previous_seq, previous_index)) if current.0 == previous_seq => {
            previous_index.checked_add(1) == Some(current.1)
        }
        Some((previous_seq, _)) => current.0 > previous_seq && current.1 == 0,
    };
    if valid {
        Ok(())
    } else {
        Err("normalized events are not in canonical child order".to_owned())
    }
}

fn verify_reject_source(record: &RejectRecord) -> Result<(), String> {
    let envelope = EnvelopeView::parse(record.canonical_envelope().as_bytes())
        .map_err(|error| format!("reject canonical envelope is invalid: {error}"))?;
    let header = record.header();
    if envelope.record_id.as_str() != header.record_id()
        || envelope.delivery_index != header.address().delivery_index()
        || envelope.visible_ns.ns() != header.visible_ns()
        || Sha256::from_bytes(
            *indexer_types::ContentHash::hash(envelope.raw_payload.as_bytes()).as_bytes(),
        ) != *header.provenance().content_hash()
    {
        return Err("reject header does not bind its exact canonical envelope".to_owned());
    }
    Ok(())
}

/// The shared codec's decoder for one output: identity-checking (both
/// SHA-256 digests) for `Audit`, stored-digest only for `PinnedReplay`.
/// Both enforce the identical frame, length and LF-count rules.
enum LineDecoder {
    Identity(StreamingDecoder<File>),
    Structural(StructuralDecoder<File>),
}

impl Read for LineDecoder {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Identity(decoder) => decoder.read(buffer),
            Self::Structural(decoder) => decoder.read(buffer),
        }
    }
}

impl LineDecoder {
    fn finish(self) -> Result<(), prediction_encoder::CodecError> {
        match self {
            Self::Identity(decoder) => decoder.finish().map(|_| ()),
            Self::Structural(decoder) => decoder.finish().map(|_| ()),
        }
    }
}

pub(crate) struct VerifiedLines {
    path: PathBuf,
    reader: Option<BufReader<LineDecoder>>,
    line: Vec<u8>,
    max_line_bytes: u64,
}

impl VerifiedLines {
    pub(crate) fn open_limit(
        path: &Path,
        output: &super::CompressedOutput,
        max_line_bytes: u64,
        mode: ReadMode,
    ) -> Result<Self, String> {
        let source =
            File::open(path).map_err(|error| format!("opening {}: {error}", path.display()))?;
        let decoder = match mode {
            ReadMode::Audit => {
                let logical = CodecLogicalIdentity {
                    sha256: output.logical.sha256.as_hex(),
                    byte_length: output.logical.byte_length,
                    line_count: output.logical.line_count,
                };
                let stored = CodecStoredIdentity {
                    sha256: output.stored.sha256.as_hex(),
                    byte_length: output.stored.byte_length,
                };
                StreamingDecoder::new(source, &logical, Some(&stored), Some(logical.byte_length))
                    .map(LineDecoder::Identity)
            }
            ReadMode::PinnedReplay => {
                let expected = StructuralIdentity {
                    logical_byte_length: output.logical.byte_length,
                    line_count: output.logical.line_count,
                    stored_byte_length: output.stored.byte_length,
                    stored_sha256: output.stored.sha256.as_hex(),
                };
                StructuralDecoder::new(source, &expected, Some(expected.logical_byte_length))
                    .map(LineDecoder::Structural)
            }
        }
        .map_err(|error| format!("opening {}: {error}", path.display()))?;
        #[cfg(test)]
        probe::opened(path, mode);
        Ok(Self {
            path: path.to_path_buf(),
            reader: Some(BufReader::new(decoder)),
            line: Vec::new(),
            max_line_bytes,
        })
    }

    pub(crate) fn next_line(&mut self) -> Result<Option<&[u8]>, String> {
        self.line.clear();
        let read = self
            .reader
            .as_mut()
            .expect("reader is open")
            .take(self.max_line_bytes.saturating_add(1))
            .read_until(b'\n', &mut self.line)
            .map_err(|error| format!("decoding {}: {error}", self.path.display()))?;
        if read as u64 > self.max_line_bytes {
            return Err("normalized line exceeds read limit".to_owned());
        }
        if read == 0 {
            return Ok(None);
        }
        if self.line.last() != Some(&b'\n') {
            return Err(format!(
                "{} contains a non-LF-terminated record",
                self.path.display()
            ));
        }
        #[cfg(test)]
        probe::line(&self.path);
        Ok(Some(&self.line[..self.line.len() - 1]))
    }

    pub(crate) fn finish(mut self) -> Result<(), String> {
        self.reader
            .take()
            .expect("reader is open")
            .into_inner()
            .finish()
            .map_err(|error| format!("verifying {}: {error}", self.path.display()))
    }
}

pub(crate) fn decode_receipt_document(
    bytes: &[u8],
    path: &Path,
) -> Result<(DerivativeReceipt, Vec<u8>), String> {
    // Select the frozen wire profile before interpreting its remaining fields.
    // Unknown fields are ignored ONLY by this discriminator; the selected
    // closed receipt decoder below rejects them and checks exact canonical bytes.
    #[derive(serde::Deserialize)]
    struct Profile {
        receipt_version: u16,
        normalized_schema_version: u16,
        materializer_version: u16,
    }
    let profile: Profile = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    match (
        profile.receipt_version,
        profile.normalized_schema_version,
        profile.materializer_version,
    ) {
        (1, replay_domain::SEGMENT_SCHEMA_V3, 1) => {
            let (wire, bytes) = decode_canonical_document::<crate::profile1::Receipt>(bytes, path)?;
            Ok((wire.into(), bytes))
        }
        (2, replay_domain::SEGMENT_SCHEMA_V3, 2) => decode_canonical_document(bytes, path),
        _ => Err("unsupported derivative receipt/schema/materializer profile".into()),
    }
}

pub(crate) fn decode_canonical_document<T>(
    bytes: &[u8],
    path: &Path,
) -> Result<(T, Vec<u8>), String>
where
    T: DeserializeOwned + Serialize,
{
    if bytes.last() != Some(&b'\n') {
        return Err(format!("{} does not end in LF", path.display()));
    }
    let value: T = serde_json::from_slice(&bytes[..bytes.len() - 1])
        .map_err(|error| format!("decoding {}: {error}", path.display()))?;
    let mut canonical = serde_json::to_vec(&value)
        .map_err(|error| format!("encoding {}: {error}", path.display()))?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(format!("{} is not canonically encoded", path.display()));
    }
    Ok((value, canonical))
}

fn sha256(bytes: &[u8]) -> Sha256 {
    Sha256::digest(bytes)
}

/// Test-only counters of decoder constructions and decoded lines, per output
/// file name, for the calling thread. They make "one decode pass per file" and
/// "no verification pass before the first record" structural assertions.
#[cfg(test)]
pub(crate) mod probe {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::path::Path;

    use super::ReadMode;

    thread_local! {
        static OPENS: RefCell<BTreeMap<String, u64>> = RefCell::default();
        static HASHED_OPENS: RefCell<BTreeMap<String, u64>> = RefCell::default();
        static LINES: RefCell<BTreeMap<String, u64>> = RefCell::default();
        static CANONICAL_CHECKS: RefCell<u64> = const { RefCell::new(0) };
    }

    fn name(path: &Path) -> String {
        path.file_name().unwrap().to_string_lossy().into_owned()
    }

    pub(crate) fn reset() {
        OPENS.with(|counts| counts.borrow_mut().clear());
        HASHED_OPENS.with(|counts| counts.borrow_mut().clear());
        LINES.with(|counts| counts.borrow_mut().clear());
        CANONICAL_CHECKS.with(|count| *count.borrow_mut() = 0);
    }

    pub(crate) fn opened(path: &Path, mode: ReadMode) {
        OPENS.with(|counts| *counts.borrow_mut().entry(name(path)).or_default() += 1);
        if mode == ReadMode::Audit {
            HASHED_OPENS.with(|counts| *counts.borrow_mut().entry(name(path)).or_default() += 1);
        }
    }

    /// One line decoded with a canonical re-encode equality check.
    pub(crate) fn canonical_check() {
        CANONICAL_CHECKS.with(|count| *count.borrow_mut() += 1);
    }

    /// Decoders that compute both SHA-256 identities over `file`.
    pub(crate) fn hashed_opens(file: &str) -> u64 {
        HASHED_OPENS.with(|counts| counts.borrow().get(file).copied().unwrap_or(0))
    }

    pub(crate) fn canonical_checks() -> u64 {
        CANONICAL_CHECKS.with(|count| *count.borrow())
    }

    pub(crate) fn line(path: &Path) {
        LINES.with(|counts| *counts.borrow_mut().entry(name(path)).or_default() += 1);
    }

    pub(crate) fn opens(file: &str) -> u64 {
        OPENS.with(|counts| counts.borrow().get(file).copied().unwrap_or(0))
    }

    pub(crate) fn lines(file: &str) -> u64 {
        LINES.with(|counts| counts.borrow().get(file).copied().unwrap_or(0))
    }
}
