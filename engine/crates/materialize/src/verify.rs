use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use indexer_types::EnvelopeView;
use indexer_types::Sha256;
use prediction_encoder::{
    LogicalIdentity as CodecLogicalIdentity, StoredIdentity as CodecStoredIdentity,
    StreamingDecoder,
};
use replay_domain::{SegmentEvent, SegmentRecord};
use serde::{Serialize, de::DeserializeOwned};

use super::schema::{DerivativeManifest, DerivativeReceipt, RejectDisposition, RejectRecord};
use super::{DerivativeSpec, EVENTS_FILE, MANIFEST_FILE, RECEIPT_FILE, REJECTS_FILE};

type FaultBindings = BTreeMap<String, Vec<u8>>;

struct VerifiedRejects {
    lines: u64,
    bindings: FaultBindings,
    ignored: u64,
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
pub fn verify_derivative(directory: &Path) -> Result<VerifiedDerivative, String> {
    let receipt_path = directory.join(RECEIPT_FILE);
    if !receipt_path.is_file() {
        return Err("derivative has no receipt commit marker".to_owned());
    }
    let (receipt, receipt_bytes) = read_canonical_document::<DerivativeReceipt>(&receipt_path)?;
    receipt.validate()?;
    let directory_name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "derivative directory has no UTF-8 address".to_owned())?;
    if directory_name != receipt.derivative_address {
        return Err("derivative directory does not match receipt address".to_owned());
    }

    let manifest_path = directory.join(MANIFEST_FILE);
    let manifest_bytes = std::fs::read(&manifest_path)
        .map_err(|error| format!("reading {}: {error}", manifest_path.display()))?;
    if manifest_bytes.len() as u64 != receipt.manifest.byte_length
        || sha256(&manifest_bytes) != receipt.manifest.sha256
    {
        return Err("manifest identity disagrees with receipt".to_owned());
    }
    let (manifest, canonical_manifest_bytes) =
        decode_canonical_document::<DerivativeManifest>(&manifest_bytes, &manifest_path)?;
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
    {
        return Err("manifest and receipt bindings disagree".to_owned());
    }
    let spec = DerivativeSpec {
        normalized_schema_version: manifest.normalized_schema_version,
        normalizer_bundle_sha256: manifest.normalizer_bundle_sha256,
        normalizer_config_sha256: manifest.normalizer_config_sha256,
        policy: manifest.policy.clone(),
    };
    let expected_address = super::derivative_address(&manifest.source_receipt, &spec)
        .map_err(|error| error.to_string())?;
    if expected_address != receipt.derivative_address {
        return Err("derivative address does not match its domain-separated inputs".to_owned());
    }

    let (event_lines, faults) = verify_events(directory, &manifest)?;
    let rejects = verify_rejects(directory, &manifest)?;
    if faults != rejects.bindings {
        return Err("normalization fault events do not pair exactly with parse rejects".to_owned());
    }
    if event_lines != manifest.events.logical.line_count
        || rejects.lines != manifest.rejects.logical.line_count
        || faults.len() as u64 != manifest.counts.normalization_fault_events
        || faults.len() as u64 != manifest.counts.rejected_source_records
        || rejects.ignored != manifest.counts.intentionally_ignored_records
    {
        return Err("verified derivative lines disagree with manifest counts".to_owned());
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

fn verify_events(
    directory: &Path,
    manifest: &DerivativeManifest,
) -> Result<(u64, FaultBindings), String> {
    let mut lines = 0_u64;
    let mut faults = BTreeMap::new();
    let mut previous = None;
    read_compressed_lines(&directory.join(EVENTS_FILE), &manifest.events, |line| {
        let record = SegmentRecord::from_canonical_json(line)
            .map_err(|error| format!("invalid normalized event: {error}"))?;
        let current = (
            record.header().address().canonical_seq(),
            record.header().address().event_index(),
        );
        verify_event_order(previous, current)?;
        previous = Some(current);
        if let SegmentEvent::NormalizationFault(fault) = record.event() {
            let binding = serde_json::to_vec(&(record.header(), fault.impact()))
                .map_err(|error| format!("encoding normalization fault binding: {error}"))?;
            if faults
                .insert(fault.reject_id().to_owned(), binding)
                .is_some()
            {
                return Err("duplicate normalization fault reject_id".to_owned());
            }
        }
        lines += 1;
        Ok(())
    })?;
    Ok((lines, faults))
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

fn verify_rejects(
    directory: &Path,
    manifest: &DerivativeManifest,
) -> Result<VerifiedRejects, String> {
    let mut lines = 0_u64;
    let mut ignored = 0_u64;
    let mut rejects = BTreeMap::new();
    let mut previous_seq = None;
    read_compressed_lines(&directory.join(REJECTS_FILE), &manifest.rejects, |line| {
        let record = RejectRecord::from_canonical_json(line)?;
        verify_reject_source(&record)?;
        let current_seq = record.header().address().canonical_seq();
        if previous_seq.is_some_and(|previous| current_seq <= previous) {
            return Err("reject records are not in canonical source order".to_owned());
        }
        previous_seq = Some(current_seq);
        match record.disposition() {
            RejectDisposition::ParseReject {
                reject_id,
                parser_version,
                error_code,
                impact,
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
                    return Err(
                        "parse reject_id does not bind its source and parser result".to_owned()
                    );
                }
                let binding = serde_json::to_vec(&(record.header(), impact))
                    .map_err(|error| format!("encoding parse reject binding: {error}"))?;
                if rejects.insert(reject_id.clone(), binding).is_some() {
                    return Err("duplicate parse reject_id".to_owned());
                }
            }
            RejectDisposition::IntentionallyIgnored { .. } => ignored += 1,
        }
        lines += 1;
        Ok(())
    })?;
    Ok(VerifiedRejects {
        lines,
        bindings: rejects,
        ignored,
    })
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

fn read_compressed_lines<F>(
    path: &Path,
    output: &super::CompressedOutput,
    mut consume: F,
) -> Result<(), String>
where
    F: FnMut(&[u8]) -> Result<(), String>,
{
    let source =
        File::open(path).map_err(|error| format!("opening {}: {error}", path.display()))?;
    let logical = CodecLogicalIdentity {
        sha256: output.logical.sha256.as_hex(),
        byte_length: output.logical.byte_length,
        line_count: output.logical.line_count,
    };
    let stored = CodecStoredIdentity {
        sha256: output.stored.sha256.as_hex(),
        byte_length: output.stored.byte_length,
    };
    let decoder = StreamingDecoder::new(source, &logical, Some(&stored), Some(logical.byte_length))
        .map_err(|error| format!("opening {}: {error}", path.display()))?;
    let mut reader = BufReader::new(decoder);
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("decoding {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            return Err(format!(
                "{} contains a non-LF-terminated record",
                path.display()
            ));
        }
        consume(&line[..line.len() - 1])?;
    }
    reader
        .into_inner()
        .finish()
        .map_err(|error| format!("verifying {}: {error}", path.display()))?;
    Ok(())
}

fn read_canonical_document<T>(path: &Path) -> Result<(T, Vec<u8>), String>
where
    T: DeserializeOwned + Serialize,
{
    let bytes =
        std::fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    decode_canonical_document(&bytes, path)
}

fn decode_canonical_document<T>(bytes: &[u8], path: &Path) -> Result<(T, Vec<u8>), String>
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
