//! Checks for a candidate this process has just written.
//!
//! The full strict verifier (`verify_derivative`) re-decodes and re-parses every
//! frame. For a fresh build that repeats what the writer already guaranteed while
//! producing each line, so the build instead enforces the verifier's operational
//! limits as it writes and, before the receipt, re-hashes the stored bytes on disk
//! against the identities it recorded. Derivatives this process did not write --
//! downloaded, cached, or pinned by a caller -- are still fully verified, and the
//! test suite runs the full verifier over writer output.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use replay_domain::{EventHeader, LaneId};
use sha2::{Digest, Sha256 as Sha256Hasher};

use crate::evidence::{CoverageEvidence, LaneState, SourceFaultReason};
use crate::reader::{ReadLimits, add_size};
use crate::schema::DerivativeReceipt;
use crate::{DERIVATIVE_MANIFEST_FILE, Sha256};

/// The delivery-level checks `reader::verify_deliveries` applies, run once per
/// source delivery as it is written. Accounting and messages match the reader,
/// so a build accepts and rejects exactly the inputs the default reader would.
pub(crate) struct CandidateLimits {
    limits: ReadLimits,
    previous: Option<EventHeader>,
    first_seq: Option<i64>,
    run_first: Option<EventHeader>,
    cross_lane: bool,
    run_bytes: u64,
    run_records: u64,
    lane_deliveries: BTreeMap<LaneId, u64>,
    lane_counts: BTreeMap<LaneId, u64>,
    first_visible: BTreeMap<LaneId, u64>,
    lane_bytes: u64,
    min_visible_ns: Option<u64>,
    max_visible_ns: Option<u64>,
}

impl CandidateLimits {
    pub(crate) fn new() -> Self {
        Self {
            limits: ReadLimits::default(),
            previous: None,
            first_seq: None,
            run_first: None,
            cross_lane: false,
            run_bytes: 0,
            run_records: 0,
            lane_deliveries: BTreeMap::new(),
            lane_counts: BTreeMap::new(),
            first_visible: BTreeMap::new(),
            lane_bytes: 0,
            min_visible_ns: None,
            max_visible_ns: None,
        }
    }

    /// The logical size a written line occupies, including its LF.
    pub(crate) fn line(&self, bytes: &[u8]) -> Result<u64, String> {
        let size = bytes.len() as u64 + 1;
        if size > self.limits.max_line_bytes {
            return Err("normalized line exceeds read limit".to_owned());
        }
        Ok(size)
    }

    /// Records one source delivery: its source-evidence, event, and reject lines.
    pub(crate) fn delivery(
        &mut self,
        h: &EventHeader,
        logical_bytes: u64,
        record_count: u64,
    ) -> Result<(), String> {
        // `write_counted` sums the whole delivery first, so this is one bound
        // check of the total rather than the reader's per-line accumulation.
        if logical_bytes > self.limits.max_group_bytes
            || record_count > self.limits.max_group_records
        {
            return Err("read resource limit exceeded".into());
        }

        self.first_seq.get_or_insert(h.address().canonical_seq());
        let lane = h.address().lane();
        let count = self.lane_counts.entry(lane.clone()).or_insert(0u64);
        *count = count.checked_add(1).ok_or("source lane count overflow")?;
        if !self.lane_deliveries.contains_key(lane) {
            if self.lane_deliveries.len() == self.limits.max_lanes {
                return Err("lane count exceeds read limit".into());
            }
            self.first_visible.insert(lane.clone(), h.visible_ns());
            add_size(
                &mut self.lane_bytes,
                lane.as_str().len() as u64,
                self.limits.max_metadata_bytes,
            )?;
        }
        if self
            .lane_deliveries
            .insert(lane.clone(), h.address().delivery_index())
            .is_some_and(|previous| previous >= h.address().delivery_index())
        {
            return Err("source delivery index repeats or decreases within lane".into());
        }
        self.min_visible_ns = Some(
            self.min_visible_ns
                .map_or(h.visible_ns(), |v| v.min(h.visible_ns())),
        );
        self.max_visible_ns = Some(
            self.max_visible_ns
                .map_or(h.visible_ns(), |v| v.max(h.visible_ns())),
        );
        if let Some(p) = &self.previous {
            if p.address().canonical_seq().checked_add(1) != Some(h.address().canonical_seq()) {
                return Err("source canonical sequence is not dense".into());
            }
            if h.visible_ns() < p.visible_ns() {
                return Err("source timestamps decrease".into());
            }
        }
        if self
            .run_first
            .as_ref()
            .is_some_and(|first| first.visible_ns() != h.visible_ns())
        {
            validate_tie(self.run_first.as_ref().unwrap(), self.cross_lane)?;
            self.run_first = None;
        }
        if let Some(first) = &self.run_first {
            if first.visible_tie_group() != h.visible_tie_group() {
                return Err("inconsistent equal-time tie tags".into());
            }
            self.cross_lane |= first.address().lane() != h.address().lane();
        } else {
            self.run_first = Some(h.clone());
            self.cross_lane = false;
            self.run_bytes = 0;
            self.run_records = 0;
        }
        if h.visible_tie_group().is_some() {
            add_size(
                &mut self.run_bytes,
                logical_bytes,
                self.limits.max_group_bytes,
            )?;
            add_size(
                &mut self.run_records,
                record_count,
                self.limits.max_group_records,
            )?;
        }
        self.previous = Some(h.clone());
        Ok(())
    }

    /// The end-of-window checks: the final tie run, the derivative window, and
    /// agreement with the upstream receipt's coverage claims.
    pub(crate) fn finish(
        &self,
        effective: (u64, u64),
        input_records: u64,
        coverage: &CoverageEvidence,
    ) -> Result<(), String> {
        if let Some(first) = &self.run_first {
            validate_tie(first, self.cross_lane)?;
        }
        if self.min_visible_ns.is_some_and(|v| v < effective.0)
            || self.max_visible_ns.is_some_and(|v| v >= effective.1)
        {
            return Err("source timestamp outside derivative window".into());
        }
        if coverage.sequence()
            != (
                self.first_seq,
                self.previous.as_ref().map(|h| h.address().canonical_seq()),
            )
        {
            return Err("source sequence disagrees with coverage".into());
        }
        if coverage.records() != input_records {
            return Err("source coverage count disagrees with derivative".into());
        }
        let mut lane_counts = self.lane_counts.clone();
        for (lane, status) in coverage.lanes() {
            if let LaneState::Present { records } = status.state {
                if lane_counts.remove(lane).unwrap_or(0) != records {
                    return Err("source lane count disagrees with coverage".into());
                }
            }
        }
        if !lane_counts.is_empty() {
            return Err("source delivery from excluded lane".into());
        }
        for fault in coverage.faults() {
            if let SourceFaultReason::VisibleClockRegression {
                observed_visible_ns,
                ..
            } = fault.reason
            {
                if self.first_visible.get(&fault.lane) != Some(&observed_visible_ns) {
                    return Err("source clock diagnosis disagrees with first delivery".into());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn coverage_lanes(&self, count: usize) -> Result<(), String> {
        if count > self.limits.max_lanes {
            return Err("coverage lane count exceeds read limit".to_owned());
        }
        Ok(())
    }

    /// Metadata documents must fit the reader's per-document limit and, for
    /// pinned inspection, their combined limit.
    pub(crate) fn metadata(&self, manifest: &[u8], receipt: &[u8]) -> Result<(), String> {
        let mut combined = 0;
        add_size(
            &mut combined,
            manifest.len() as u64,
            self.limits.max_metadata_bytes,
        )?;
        add_size(
            &mut combined,
            receipt.len() as u64,
            self.limits.max_metadata_bytes,
        )
    }
}

fn validate_tie(first: &EventHeader, cross_lane: bool) -> Result<(), String> {
    if first.visible_tie_group() != cross_lane.then_some(first.visible_ns()) {
        return Err("tie group disagrees with complete equal-time source run".into());
    }
    Ok(())
}

/// Re-hashes every staged file against the identity the receipt records, so
/// the bytes committed are exactly the bytes the writer produced.
pub(crate) fn verify_stored_identity(
    stage: &Path,
    receipt: &DerivativeReceipt,
    manifest_bytes: &[u8],
) -> Result<(), String> {
    let (manifest_sha256, manifest_length) = file_identity(&stage.join(DERIVATIVE_MANIFEST_FILE))?;
    if manifest_length != receipt.manifest.byte_length
        || manifest_sha256 != receipt.manifest.sha256
        || manifest_length != manifest_bytes.len() as u64
    {
        return Err("staged manifest disagrees with the receipt".to_owned());
    }
    let mut outputs = vec![&receipt.events, &receipt.rejects];
    outputs.extend(receipt.sources.as_ref());
    for output in outputs {
        let (sha256, length) = file_identity(&stage.join(&output.file))?;
        if length != output.stored.byte_length || sha256 != output.stored.sha256 {
            return Err(format!("staged {} disagrees with the receipt", output.file));
        }
    }
    Ok(())
}

fn file_identity(path: &Path) -> Result<(Sha256, u64), String> {
    let mut file =
        File::open(path).map_err(|error| format!("opening {}: {error}", path.display()))?;
    let mut hasher = Sha256Hasher::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut length = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("reading {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length += read as u64;
    }
    Ok((Sha256::from_bytes(hasher.finalize().into()), length))
}
