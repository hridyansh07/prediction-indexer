//! Frozen profile-1 metadata wire types. Convert only after exact byte verification.
use crate::{CompressedOutput, DerivativeCounts, NormalizationPolicy, PlainOutput, Sha256};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceReceipt {
    window_start_ns: u64,
    window_end_ns: u64,
    byte_length: u64,
    sha256: Sha256,
    certified: bool,
}

impl From<SourceReceipt> for crate::SourceReceipt {
    fn from(s: SourceReceipt) -> Self {
        Self {
            window_start_ns: s.window_start_ns,
            window_end_ns: s.window_end_ns,
            byte_length: s.byte_length,
            sha256: s.sha256,
            certified: s.certified,
            document: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    manifest_version: u16,
    derivative_address: String,
    source_receipt: SourceReceipt,
    requested_start_ns: u64,
    requested_end_ns: u64,
    effective_start_ns: u64,
    effective_end_ns: u64,
    normalized_schema_version: u16,
    event_serialization_version: u16,
    reject_serialization_version: u16,
    materializer_version: u16,
    normalizer_bundle_sha256: Sha256,
    normalizer_config_sha256: Sha256,
    policy: NormalizationPolicy,
    counts: DerivativeCounts,
    events: CompressedOutput,
    rejects: CompressedOutput,
}

impl From<Manifest> for crate::DerivativeManifest {
    fn from(m: Manifest) -> Self {
        Self {
            manifest_version: m.manifest_version,
            derivative_address: m.derivative_address,
            source_receipt: m.source_receipt.into(),
            requested_start_ns: m.requested_start_ns,
            requested_end_ns: m.requested_end_ns,
            effective_start_ns: m.effective_start_ns,
            effective_end_ns: m.effective_end_ns,
            normalized_schema_version: m.normalized_schema_version,
            event_serialization_version: m.event_serialization_version,
            reject_serialization_version: m.reject_serialization_version,
            materializer_version: m.materializer_version,
            normalizer_bundle_sha256: m.normalizer_bundle_sha256,
            normalizer_config_sha256: m.normalizer_config_sha256,
            policy: m.policy,
            counts: m.counts,
            events: m.events,
            rejects: m.rejects,
            sources: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Receipt {
    receipt_version: u16,
    derivative_address: String,
    source_receipt_sha256: Sha256,
    normalized_schema_version: u16,
    materializer_version: u16,
    normalizer_bundle_sha256: Sha256,
    normalizer_config_sha256: Sha256,
    policy_sha256: Sha256,
    manifest: PlainOutput,
    events: CompressedOutput,
    rejects: CompressedOutput,
}

impl From<Receipt> for crate::DerivativeReceipt {
    fn from(r: Receipt) -> Self {
        Self {
            receipt_version: r.receipt_version,
            derivative_address: r.derivative_address,
            source_receipt_sha256: r.source_receipt_sha256,
            normalized_schema_version: r.normalized_schema_version,
            materializer_version: r.materializer_version,
            normalizer_bundle_sha256: r.normalizer_bundle_sha256,
            normalizer_config_sha256: r.normalizer_config_sha256,
            policy_sha256: r.policy_sha256,
            manifest: r.manifest,
            events: r.events,
            rejects: r.rejects,
            sources: None,
        }
    }
}
