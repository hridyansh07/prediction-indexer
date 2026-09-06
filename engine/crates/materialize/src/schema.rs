use replay_domain::{EventHeader, FaultImpact, InstrumentId, SEGMENT_SCHEMA_VERSION, Sha256};
use replay_normalize::validate_code;
use serde::{Deserialize, Serialize};

pub const MANIFEST_VERSION: u16 = 1;
pub const RECEIPT_VERSION: u16 = 1;
pub const REJECT_VERSION: u16 = 1;
pub const EVENT_SERIALIZATION_VERSION: u16 = 1;
pub const REJECT_SERIALIZATION_VERSION: u16 = 1;
pub const MATERIALIZER_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizationPolicy {
    pub policy_sha256: Sha256,
    pub effective_from_ns: u64,
    pub effective_until_ns: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivativeSpec {
    pub normalized_schema_version: u16,
    pub normalizer_bundle_sha256: Sha256,
    pub normalizer_config_sha256: Sha256,
    pub policy: NormalizationPolicy,
}

impl DerivativeSpec {
    pub fn validate_for(&self, start_ns: u64, end_ns: u64) -> Result<(), String> {
        if self.normalized_schema_version != SEGMENT_SCHEMA_VERSION {
            return Err(format!(
                "normalized schema version {} is unsupported; expected {SEGMENT_SCHEMA_VERSION}",
                self.normalized_schema_version
            ));
        }
        if self.policy.effective_from_ns > start_ns
            || self
                .policy
                .effective_until_ns
                .is_some_and(|until| until < end_ns)
        {
            return Err(
                "normalizer policy does not cover the complete canonical window".to_owned(),
            );
        }
        Ok(())
    }
}

pub type SourceReceipt = indexer_finalize::ReceiptIdentity;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalIdentity {
    pub sha256: Sha256,
    pub byte_length: u64,
    pub line_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredIdentity {
    pub sha256: Sha256,
    pub byte_length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompressionContract {
    pub algorithm: String,
    pub level: i32,
    pub frame_checksum: bool,
    pub dictionary: Option<String>,
    pub frame_count: u8,
    pub encoder: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompressedOutput {
    pub file: String,
    pub content_encoding: String,
    pub logical: LogicalIdentity,
    pub stored: StoredIdentity,
    pub compression: CompressionContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlainOutput {
    pub file: String,
    pub sha256: Sha256,
    pub byte_length: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivativeCounts {
    pub input_records: u64,
    pub accepted_source_records: u64,
    pub accepted_events: u64,
    pub rejected_source_records: u64,
    pub normalization_fault_events: u64,
    pub intentionally_ignored_records: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivativeManifest {
    pub manifest_version: u16,
    pub derivative_address: String,
    pub source_receipt: SourceReceipt,
    pub requested_start_ns: u64,
    pub requested_end_ns: u64,
    pub effective_start_ns: u64,
    pub effective_end_ns: u64,
    pub normalized_schema_version: u16,
    pub event_serialization_version: u16,
    pub reject_serialization_version: u16,
    pub materializer_version: u16,
    pub normalizer_bundle_sha256: Sha256,
    pub normalizer_config_sha256: Sha256,
    pub policy: NormalizationPolicy,
    pub counts: DerivativeCounts,
    pub events: CompressedOutput,
    pub rejects: CompressedOutput,
}

impl DerivativeManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.manifest_version != MANIFEST_VERSION {
            return Err(format!(
                "unsupported derivative manifest version {}",
                self.manifest_version
            ));
        }
        if self.event_serialization_version != EVENT_SERIALIZATION_VERSION
            || self.reject_serialization_version != REJECT_SERIALIZATION_VERSION
            || self.materializer_version != MATERIALIZER_VERSION
        {
            return Err("unsupported derivative serialization version".to_owned());
        }
        let spec = DerivativeSpec {
            normalized_schema_version: self.normalized_schema_version,
            normalizer_bundle_sha256: self.normalizer_bundle_sha256,
            normalizer_config_sha256: self.normalizer_config_sha256,
            policy: self.policy.clone(),
        };
        spec.validate_for(
            self.source_receipt.window_start_ns,
            self.source_receipt.window_end_ns,
        )?;
        validate_address(&self.derivative_address, "derivative_address")?;
        validate_output(&self.events, "events.ndjson.zst")?;
        validate_output(&self.rejects, "rejects.ndjson.zst")?;
        if self.requested_start_ns != self.source_receipt.window_start_ns
            || self.requested_end_ns != self.source_receipt.window_end_ns
            || self.effective_start_ns != self.requested_start_ns
            || self.effective_end_ns != self.requested_end_ns
        {
            return Err("derivative interval is not the exact source window".to_owned());
        }
        if self.counts.accepted_events + self.counts.normalization_fault_events
            != self.events.logical.line_count
            || self.counts.rejected_source_records + self.counts.intentionally_ignored_records
                != self.rejects.logical.line_count
            || self.counts.accepted_source_records
                + self.counts.rejected_source_records
                + self.counts.intentionally_ignored_records
                != self.counts.input_records
        {
            return Err("derivative counts disagree with output identities".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivativeReceipt {
    pub receipt_version: u16,
    pub derivative_address: String,
    pub source_receipt_sha256: Sha256,
    pub normalized_schema_version: u16,
    pub materializer_version: u16,
    pub normalizer_bundle_sha256: Sha256,
    pub normalizer_config_sha256: Sha256,
    pub policy_sha256: Sha256,
    pub manifest: PlainOutput,
    pub events: CompressedOutput,
    pub rejects: CompressedOutput,
}

impl DerivativeReceipt {
    pub fn validate(&self) -> Result<(), String> {
        if self.receipt_version != RECEIPT_VERSION {
            return Err(format!(
                "unsupported derivative receipt version {}",
                self.receipt_version
            ));
        }
        if self.normalized_schema_version != SEGMENT_SCHEMA_VERSION {
            return Err("unsupported normalized schema version in receipt".to_owned());
        }
        if self.materializer_version != MATERIALIZER_VERSION {
            return Err("unsupported materializer version in receipt".to_owned());
        }
        validate_address(&self.derivative_address, "derivative_address")?;
        if self.manifest.file != "manifest.json" {
            return Err("receipt names an unsupported manifest file".to_owned());
        }
        validate_output(&self.events, "events.ndjson.zst")?;
        validate_output(&self.rejects, "rejects.ndjson.zst")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RejectRecord {
    reject_version: u16,
    header: EventHeader,
    disposition: RejectDisposition,
    canonical_envelope: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RejectDisposition {
    ParseReject {
        reject_id: String,
        parser_version: u32,
        error_code: String,
        instrument_hint: Option<InstrumentId>,
        impact: FaultImpact,
        normalizer_bundle_sha256: Sha256,
        normalizer_config_sha256: Sha256,
    },
    IntentionallyIgnored {
        reason_code: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectRecordWire {
    reject_version: u16,
    header: EventHeader,
    disposition: RejectDisposition,
    canonical_envelope: String,
}

impl RejectRecord {
    pub fn parse_reject(
        header: EventHeader,
        canonical_envelope: String,
        disposition: RejectDisposition,
    ) -> Result<Self, String> {
        Self::from_wire(RejectRecordWire {
            reject_version: REJECT_VERSION,
            header,
            disposition,
            canonical_envelope,
        })
    }

    pub fn ignored(
        header: EventHeader,
        canonical_envelope: String,
        reason_code: String,
    ) -> Result<Self, String> {
        Self::from_wire(RejectRecordWire {
            reject_version: REJECT_VERSION,
            header,
            disposition: RejectDisposition::IntentionallyIgnored { reason_code },
            canonical_envelope,
        })
    }

    pub fn to_canonical_json(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|error| format!("serializing reject: {error}"))
    }

    pub fn header(&self) -> &EventHeader {
        &self.header
    }

    pub fn disposition(&self) -> &RejectDisposition {
        &self.disposition
    }

    pub fn canonical_envelope(&self) -> &str {
        &self.canonical_envelope
    }

    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, String> {
        let wire: RejectRecordWire = serde_json::from_slice(bytes)
            .map_err(|error| format!("decoding reject record: {error}"))?;
        let record = Self::from_wire(wire)?;
        if record.to_canonical_json()? != bytes {
            return Err("reject record is not canonically encoded".to_owned());
        }
        Ok(record)
    }

    fn from_wire(wire: RejectRecordWire) -> Result<Self, String> {
        if wire.reject_version != REJECT_VERSION {
            return Err(format!(
                "unsupported reject version {}",
                wire.reject_version
            ));
        }
        if !wire.canonical_envelope.ends_with('\n') {
            return Err("canonical reject envelope must end in LF".to_owned());
        }
        if wire.header.order_ns() != wire.header.visible_ns()
            || wire.header.address().canonical_seq() <= 0
            || wire.header.provenance().source_line_number() == 0
        {
            return Err("reject header contains an invalid normalized state".to_owned());
        }
        match &wire.disposition {
            RejectDisposition::ParseReject {
                reject_id,
                parser_version,
                error_code,
                ..
            } => {
                validate_address(reject_id, "reject_id")?;
                if *parser_version == 0 {
                    return Err("reject parser_version must be positive".to_owned());
                }
                validate_code(error_code, "error_code").map_err(|error| error.to_string())?;
            }
            RejectDisposition::IntentionallyIgnored { reason_code } => {
                validate_code(reason_code, "reason_code").map_err(|error| error.to_string())?;
            }
        }
        Ok(Self {
            reject_version: wire.reject_version,
            header: wire.header,
            disposition: wire.disposition,
            canonical_envelope: wire.canonical_envelope,
        })
    }
}

impl<'de> Deserialize<'de> for RejectRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::from_wire(RejectRecordWire::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

pub fn validate_address(value: &str, field: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{field} must be 64 lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

fn validate_output(output: &CompressedOutput, expected_file: &str) -> Result<(), String> {
    if output.file != expected_file || output.content_encoding != "zstd" {
        return Err(format!(
            "invalid compressed output contract for {expected_file}"
        ));
    }
    if output.compression.algorithm != "zstd"
        || output.compression.level != 3
        || !output.compression.frame_checksum
        || output.compression.dictionary.is_some()
        || output.compression.frame_count != 1
        || output.compression.encoder.is_empty()
    {
        return Err(format!("invalid compression profile for {expected_file}"));
    }
    Ok(())
}
