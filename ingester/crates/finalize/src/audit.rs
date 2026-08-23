//! Explicit, bounded-memory verification of committed canonical windows.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::Path;

use indexer_types::EnvelopeView;
use prediction_encoder::{
    LogicalIdentity, StoredIdentity as CodecStoredIdentity, StreamingDecoder,
};
use serde::{Deserialize, Serialize};

use crate::canonical::{
    CanonicalOutput, CanonicalOutputsState, Receipt, canonical_outputs_state, committed_windows,
    window_directory,
};

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct AuditReport {
    pub canonical_root: String,
    pub windows_verified: u64,
    pub windows_partially_reaped: u64,
    pub windows_archived_and_reaped: u64,
    pub evidence_records_verified: u64,
}

#[derive(Deserialize)]
struct ProvenanceLine {
    canonical_seq: i64,
    lane_id: String,
    source_segment_sha256: String,
    source_line_number: u64,
    record_id: String,
    content_hash: String,
}

pub fn audit_canonical_root(canonical_root: &Path) -> Result<AuditReport, String> {
    let committed = committed_windows(canonical_root)?;
    let mut records = 0_u64;
    let mut windows_verified = 0_u64;
    let mut windows_partially_reaped = 0_u64;
    let mut windows_archived_and_reaped = 0_u64;
    for receipt in committed.values() {
        match canonical_outputs_state(canonical_root, receipt)? {
            CanonicalOutputsState::Present => {}
            CanonicalOutputsState::PartiallyReaped => {
                windows_partially_reaped += 1;
                continue;
            }
            CanonicalOutputsState::Archived => {
                windows_archived_and_reaped += 1;
                continue;
            }
        }
        records = records
            .checked_add(audit_window(canonical_root, receipt)?)
            .ok_or_else(|| "canonical audit record count overflows u64".to_owned())?;
        windows_verified += 1;
    }
    Ok(AuditReport {
        canonical_root: canonical_root.display().to_string(),
        windows_verified,
        windows_partially_reaped,
        windows_archived_and_reaped,
        evidence_records_verified: records,
    })
}

fn audit_window(canonical_root: &Path, receipt: &Receipt) -> Result<u64, String> {
    let directory = window_directory(canonical_root, receipt.window_start_ns);
    let evidence = open_decoder(&directory, &receipt.evidence)?;
    let provenance = open_decoder(&directory, &receipt.provenance)?;
    let mut evidence = std::io::BufReader::new(evidence);
    let mut provenance = std::io::BufReader::new(provenance);

    let mut sources: BTreeMap<(String, String), Vec<u64>> = BTreeMap::new();
    for input in &receipt.inputs {
        let key = (input.lane.clone(), input.sha256.clone());
        sources.entry(key).or_default().push(input.line_count);
    }

    let mut evidence_line = Vec::new();
    let mut provenance_line = Vec::new();
    let mut count = 0_u64;
    loop {
        evidence_line.clear();
        provenance_line.clear();
        let evidence_read = evidence
            .read_until(b'\n', &mut evidence_line)
            .map_err(|error| format!("decoding {}: {error}", receipt.evidence.file))?;
        let provenance_read = provenance
            .read_until(b'\n', &mut provenance_line)
            .map_err(|error| format!("decoding {}: {error}", receipt.provenance.file))?;
        if evidence_read == 0 && provenance_read == 0 {
            break;
        }
        if evidence_read == 0 || provenance_read == 0 {
            return Err(format!(
                "window {} evidence and provenance end at different lines",
                receipt.window_start_ns
            ));
        }

        count += 1;
        let expected_seq = receipt.first_canonical_seq.ok_or_else(|| {
            format!(
                "window {} has records but no first sequence",
                receipt.window_start_ns
            )
        })? + count as i64
            - 1;
        let provenance: ProvenanceLine =
            serde_json::from_slice(&provenance_line).map_err(|error| {
                format!(
                    "window {} provenance line {count} is invalid: {error}",
                    receipt.window_start_ns
                )
            })?;
        if provenance.canonical_seq != expected_seq {
            return Err(format!(
                "window {} provenance line {count} has canonical_seq {}, expected {expected_seq}",
                receipt.window_start_ns, provenance.canonical_seq
            ));
        }

        let source_lines = sources
            .get(&(
                provenance.lane_id.clone(),
                provenance.source_segment_sha256.clone(),
            ))
            .ok_or_else(|| {
                format!(
                    "window {} provenance line {count} names no canonical input",
                    receipt.window_start_ns
                )
            })?;
        if provenance.source_line_number == 0
            || !source_lines
                .iter()
                .any(|line_count| provenance.source_line_number <= *line_count)
        {
            return Err(format!(
                "window {} provenance line {count} has source line {} outside its named input",
                receipt.window_start_ns, provenance.source_line_number
            ));
        }

        let view = EnvelopeView::parse(&evidence_line).map_err(|error| {
            format!(
                "window {} evidence line {count} is invalid: {error}",
                receipt.window_start_ns
            )
        })?;
        if view.record_id.as_str() != provenance.record_id {
            return Err(format!(
                "window {} line {count} record_id disagrees with provenance",
                receipt.window_start_ns
            ));
        }
        let content_hash = indexer_types::ContentHash::hash(view.raw_payload.as_bytes()).to_hex();
        if content_hash != provenance.content_hash {
            return Err(format!(
                "window {} line {count} content_hash disagrees with provenance",
                receipt.window_start_ns
            ));
        }
    }

    let evidence_decoder = evidence.into_inner();
    let provenance_decoder = provenance.into_inner();
    evidence_decoder
        .finish()
        .map_err(|error| format!("verifying {}: {error}", receipt.evidence.file))?;
    provenance_decoder
        .finish()
        .map_err(|error| format!("verifying {}: {error}", receipt.provenance.file))?;

    if count != receipt.evidence.decoded.line_count {
        return Err(format!(
            "window {} decoded {count} evidence lines, receipt records {}",
            receipt.window_start_ns, receipt.evidence.decoded.line_count
        ));
    }
    Ok(count)
}

fn open_decoder(
    directory: &Path,
    output: &CanonicalOutput,
) -> Result<StreamingDecoder<std::fs::File>, String> {
    let source = std::fs::File::open(directory.join(&output.file))
        .map_err(|error| format!("opening {}: {error}", output.file))?;
    let logical = LogicalIdentity {
        sha256: output.decoded.sha256.clone(),
        byte_length: output.decoded.byte_length,
        line_count: output.decoded.line_count,
    };
    let stored = CodecStoredIdentity {
        sha256: output.stored.sha256.clone(),
        byte_length: output.stored.byte_length,
    };
    StreamingDecoder::new(source, &logical, Some(&stored), Some(logical.byte_length))
        .map_err(|error| format!("opening Zstandard decoder for {}: {error}", output.file))
}
