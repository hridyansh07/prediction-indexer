//! Audited, bounded-memory access to committed canonical windows.
//!
//! The capability returned by [`AuditedCanonicalReader::finish`] is the only
//! successful audit result. Records yielded before then remain untrusted: the
//! strict decoder verifies frame termination and both identities only at EOF.

use std::collections::{BTreeMap, BTreeSet};
use std::io::BufRead;
use std::path::{Path, PathBuf};

use indexer_types::EnvelopeView;
use prediction_encoder::{
    LogicalIdentity, StoredIdentity as CodecStoredIdentity, StreamingDecoder,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::canonical::{
    CanonicalOutput, CanonicalOutputsState, Receipt, canonical_outputs_state, committed_windows,
    receipt_path, window_directory,
};

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct AuditReport {
    pub canonical_root: String,
    pub windows_verified: u64,
    pub windows_partially_reaped: u64,
    pub windows_archived_and_reaped: u64,
    pub evidence_records_verified: u64,
}

/// Whether an uncertified receipt may enter a replay selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CertifiedPolicy {
    /// Fail closed. This is the default until replay defines how quarantined
    /// clock evidence affects downstream claims.
    #[default]
    RequireCertified,
    /// Admit it while retaining `certified: false` in the selected identity.
    AllowUncertified,
}

/// What to do when the requested lower bound falls inside the first window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LowerBoundPolicy {
    /// Fail closed rather than silently discard or add canonical evidence.
    #[default]
    RequireWindowBoundary,
    /// Audit the whole first window but yield only records at or after the bound.
    Clip,
    /// Yield the whole first window and record the expanded effective bound.
    ExpandToWindowStart,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SelectionPolicy {
    pub certified: CertifiedPolicy,
    pub lower_bound: LowerBoundPolicy,
}

/// Exact identity of one selected commit marker. These identities, rather than
/// paths, are suitable for a later segment manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptIdentity {
    pub window_start_ns: u64,
    pub window_end_ns: u64,
    pub byte_length: u64,
    pub sha256: String,
    pub certified: bool,
}

#[derive(Clone, Debug)]
struct SelectedWindow {
    receipt: Receipt,
    identity: ReceiptIdentity,
}

/// A non-forgeable plan for reading the minimal adjacent covering windows.
/// Construction is private; callers can only obtain one through
/// [`select_canonical_windows`]. It is not proof of an audit until consumed and
/// finished.
#[derive(Debug)]
pub struct CanonicalSelection {
    canonical_root: PathBuf,
    requested_start_ns: u64,
    requested_end_ns: u64,
    effective_start_ns: u64,
    windows: Vec<SelectedWindow>,
}

impl CanonicalSelection {
    pub fn requested_interval(&self) -> (u64, u64) {
        (self.requested_start_ns, self.requested_end_ns)
    }

    pub fn effective_interval(&self) -> (u64, u64) {
        (self.effective_start_ns, self.requested_end_ns)
    }

    pub fn receipt_identities(&self) -> impl ExactSizeIterator<Item = &ReceiptIdentity> {
        self.windows.iter().map(|window| &window.identity)
    }

    pub fn open(self) -> Result<AuditedCanonicalReader, String> {
        AuditedCanonicalReader::new(self)
    }
}

/// Selects the unique minimal adjacent receipt run covering `[start_ns, end_ns)`.
pub fn select_canonical_windows(
    canonical_root: &Path,
    start_ns: u64,
    end_ns: u64,
    policy: SelectionPolicy,
) -> Result<CanonicalSelection, String> {
    if start_ns >= end_ns {
        return Err("canonical selection requires start_ns < end_ns".to_owned());
    }
    let committed = committed_windows(canonical_root)?;
    for pair in committed.values().collect::<Vec<_>>().windows(2) {
        if pair[1].window_start_ns < pair[0].window_end_ns
            && pair[0].window_start_ns < end_ns
            && pair[1].window_end_ns > start_ns
        {
            return Err(format!(
                "committed canonical windows {} and {} overlap",
                pair[0].window_start_ns, pair[1].window_start_ns
            ));
        }
    }
    let containing = committed
        .values()
        .filter(|receipt| receipt.window_start_ns <= start_ns && start_ns < receipt.window_end_ns)
        .collect::<Vec<_>>();
    let first = match containing.as_slice() {
        [] => return Err(format!("no committed canonical window covers {start_ns}")),
        [receipt] => *receipt,
        _ => return Err(format!("committed canonical windows overlap at {start_ns}")),
    };
    if first.window_start_ns != start_ns
        && policy.lower_bound == LowerBoundPolicy::RequireWindowBoundary
    {
        return Err(format!(
            "requested lower bound {start_ns} falls inside window {}; choose clipping or recorded expansion explicitly",
            first.window_start_ns
        ));
    }

    let mut selected = Vec::new();
    let mut cursor = first.window_start_ns;
    let mut covered_until = start_ns;
    while covered_until < end_ns {
        let receipt = committed
            .get(&cursor)
            .ok_or_else(|| format!("gap in committed canonical windows at {covered_until}"))?;
        if receipt.window_start_ns > covered_until {
            return Err(format!(
                "gap in committed canonical windows at {covered_until}"
            ));
        }
        if receipt.window_start_ns < covered_until && !selected.is_empty() {
            return Err(format!(
                "committed canonical window {} overlaps the preceding window",
                receipt.window_start_ns
            ));
        }
        if !receipt.certified && policy.certified == CertifiedPolicy::RequireCertified {
            return Err(format!(
                "canonical window {} is not certified",
                receipt.window_start_ns
            ));
        }
        if canonical_outputs_state(canonical_root, receipt)? != CanonicalOutputsState::Present {
            return Err(format!(
                "canonical window {} does not have both local objects",
                receipt.window_start_ns
            ));
        }
        let identity = receipt_identity(canonical_root, receipt)?;
        covered_until = receipt.window_end_ns;
        cursor = covered_until;
        selected.push(SelectedWindow {
            receipt: receipt.clone(),
            identity,
        });
    }

    let mut expected_seq = None;
    for window in &selected {
        if let (Some(first), Some(last)) = (
            window.receipt.first_canonical_seq,
            window.receipt.last_canonical_seq,
        ) {
            if let Some(expected) = expected_seq {
                if first != expected {
                    return Err(format!(
                        "canonical sequence is not adjacent at window {}: found {first}, expected {expected}",
                        window.receipt.window_start_ns
                    ));
                }
            }
            expected_seq =
                Some(last.checked_add(1).ok_or_else(|| {
                    "canonical sequence overflows after selected window".to_owned()
                })?);
        }
    }

    let effective_start_ns = match policy.lower_bound {
        LowerBoundPolicy::ExpandToWindowStart => first.window_start_ns,
        LowerBoundPolicy::RequireWindowBoundary | LowerBoundPolicy::Clip => start_ns,
    };
    Ok(CanonicalSelection {
        canonical_root: canonical_root.to_path_buf(),
        requested_start_ns: start_ns,
        requested_end_ns: end_ns,
        effective_start_ns,
        windows: selected,
    })
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityVerdict {
    Lifecycle,
    Bootstrap,
    UnsequencedVenue,
    SparseMonotonic,
    Continuous,
    GapProven,
    CursorWentBackwards,
    LocalCounterBroken,
    Duplicate,
    Conflict,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CanonicalProvenance {
    pub canonical_seq: i64,
    pub lane_id: String,
    pub source_segment_sha256: String,
    pub source_line_number: u64,
    pub record_id: String,
    pub content_hash: String,
    pub continuity_verdict: ContinuityVerdict,
    pub visible_tie_group: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventAddress {
    pub canonical_seq: i64,
    pub lane_id: String,
    pub delivery_index: u64,
}

/// One exact canonical envelope joined to its audited provenance line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinedCanonicalRecord {
    pub envelope: Vec<u8>,
    pub canonical_seq: i64,
    /// V1's ordering clock. This is deliberately equal to `visible_ns`; it is
    /// named separately so later normalization does not substitute event time.
    pub order_ns: u64,
    pub visible_ns: u64,
    pub visible_tie_group: Option<u64>,
    pub event_address: EventAddress,
    pub record_id: String,
    pub source_segment_sha256: String,
    pub source_line_number: u64,
    pub content_hash: String,
    pub continuity: ContinuityVerdict,
}

/// Opaque proof that every selected byte and joined line reached verified EOF.
#[derive(Debug)]
pub struct AuditedCanonicalSelection {
    requested_start_ns: u64,
    requested_end_ns: u64,
    effective_start_ns: u64,
    receipt_identities: Vec<ReceiptIdentity>,
    records_verified: u64,
}

impl AuditedCanonicalSelection {
    pub fn requested_interval(&self) -> (u64, u64) {
        (self.requested_start_ns, self.requested_end_ns)
    }

    pub fn effective_interval(&self) -> (u64, u64) {
        (self.effective_start_ns, self.requested_end_ns)
    }

    pub fn receipt_identities(&self) -> &[ReceiptIdentity] {
        &self.receipt_identities
    }

    pub fn records_verified(&self) -> u64 {
        self.records_verified
    }
}

struct WindowReader {
    receipt: Receipt,
    evidence: std::io::BufReader<StreamingDecoder<std::fs::File>>,
    provenance: std::io::BufReader<StreamingDecoder<std::fs::File>>,
    sources: BTreeMap<(String, String), BTreeSet<u64>>,
    count: u64,
}

impl WindowReader {
    fn open(root: &Path, selected: &SelectedWindow) -> Result<Self, String> {
        let current = receipt_identity(root, &selected.receipt)?;
        if current != selected.identity {
            return Err(format!(
                "receipt for window {} changed after selection",
                selected.receipt.window_start_ns
            ));
        }
        if canonical_outputs_state(root, &selected.receipt)? != CanonicalOutputsState::Present {
            return Err(format!(
                "canonical window {} does not have both local objects",
                selected.receipt.window_start_ns
            ));
        }
        let directory = window_directory(root, selected.receipt.window_start_ns);
        let evidence =
            std::io::BufReader::new(open_decoder(&directory, &selected.receipt.evidence)?);
        let provenance =
            std::io::BufReader::new(open_decoder(&directory, &selected.receipt.provenance)?);
        let mut sources = BTreeMap::<_, BTreeSet<_>>::new();
        for input in &selected.receipt.inputs {
            sources
                .entry((input.lane.clone(), input.sha256.clone()))
                .or_default()
                .insert(input.line_count);
        }
        Ok(Self {
            receipt: selected.receipt.clone(),
            evidence,
            provenance,
            sources,
            count: 0,
        })
    }

    fn next(&mut self) -> Result<Option<JoinedCanonicalRecord>, String> {
        let mut envelope = Vec::new();
        let mut provenance_line = Vec::new();
        let evidence_read = self
            .evidence
            .read_until(b'\n', &mut envelope)
            .map_err(|error| format!("decoding {}: {error}", self.receipt.evidence.file))?;
        let provenance_read = self
            .provenance
            .read_until(b'\n', &mut provenance_line)
            .map_err(|error| format!("decoding {}: {error}", self.receipt.provenance.file))?;
        if evidence_read == 0 && provenance_read == 0 {
            return Ok(None);
        }
        if evidence_read == 0 || provenance_read == 0 {
            return Err(format!(
                "window {} evidence and provenance end at different lines",
                self.receipt.window_start_ns
            ));
        }

        self.count += 1;
        let expected_seq = self.receipt.first_canonical_seq.ok_or_else(|| {
            format!(
                "window {} has records but no first sequence",
                self.receipt.window_start_ns
            )
        })? + self.count as i64
            - 1;
        let provenance: CanonicalProvenance =
            serde_json::from_slice(&provenance_line).map_err(|error| {
                format!(
                    "window {} provenance line {} is invalid: {error}",
                    self.receipt.window_start_ns, self.count
                )
            })?;
        if provenance.canonical_seq != expected_seq {
            return Err(format!(
                "window {} provenance line {} has canonical_seq {}, expected {expected_seq}",
                self.receipt.window_start_ns, self.count, provenance.canonical_seq
            ));
        }
        let source_lines = self
            .sources
            .get(&(
                provenance.lane_id.clone(),
                provenance.source_segment_sha256.clone(),
            ))
            .ok_or_else(|| {
                format!(
                    "window {} provenance line {} names no canonical input",
                    self.receipt.window_start_ns, self.count
                )
            })?;
        if provenance.source_line_number == 0
            || !source_lines
                .iter()
                .any(|line_count| provenance.source_line_number <= *line_count)
        {
            return Err(format!(
                "window {} provenance line {} has source line {} outside its named input",
                self.receipt.window_start_ns, self.count, provenance.source_line_number
            ));
        }

        let view = EnvelopeView::parse(&envelope).map_err(|error| {
            format!(
                "window {} evidence line {} is invalid: {error}",
                self.receipt.window_start_ns, self.count
            )
        })?;
        let visible_ns = view.visible_ns.ns();
        if visible_ns < self.receipt.window_start_ns || visible_ns >= self.receipt.window_end_ns {
            return Err(format!(
                "window {} evidence line {} has visible_ns {visible_ns} outside [{}, {})",
                self.receipt.window_start_ns,
                self.count,
                self.receipt.window_start_ns,
                self.receipt.window_end_ns
            ));
        }
        if view.record_id.as_str() != provenance.record_id {
            return Err(format!(
                "window {} line {} record_id disagrees with provenance",
                self.receipt.window_start_ns, self.count
            ));
        }
        let content_hash = indexer_types::ContentHash::hash(view.raw_payload.as_bytes()).to_hex();
        if content_hash != provenance.content_hash {
            return Err(format!(
                "window {} line {} content_hash disagrees with provenance",
                self.receipt.window_start_ns, self.count
            ));
        }
        let delivery_index = view.delivery_index;
        let lane_id = provenance.lane_id.clone();
        Ok(Some(JoinedCanonicalRecord {
            envelope,
            canonical_seq: provenance.canonical_seq,
            order_ns: visible_ns,
            visible_ns,
            visible_tie_group: provenance.visible_tie_group,
            event_address: EventAddress {
                canonical_seq: provenance.canonical_seq,
                lane_id,
                delivery_index,
            },
            record_id: provenance.record_id,
            source_segment_sha256: provenance.source_segment_sha256,
            source_line_number: provenance.source_line_number,
            content_hash: provenance.content_hash,
            continuity: provenance.continuity_verdict,
        }))
    }

    fn finish(self) -> Result<u64, String> {
        self.evidence
            .into_inner()
            .finish()
            .map_err(|error| format!("verifying {}: {error}", self.receipt.evidence.file))?;
        self.provenance
            .into_inner()
            .finish()
            .map_err(|error| format!("verifying {}: {error}", self.receipt.provenance.file))?;
        if self.count != self.receipt.evidence.decoded.line_count {
            return Err(format!(
                "window {} decoded {} evidence lines, receipt records {}",
                self.receipt.window_start_ns, self.count, self.receipt.evidence.decoded.line_count
            ));
        }
        Ok(self.count)
    }
}

pub struct AuditedCanonicalReader {
    selection: CanonicalSelection,
    next_window: usize,
    current: Option<WindowReader>,
    records_verified: u64,
    last_seq: Option<i64>,
    exhausted: bool,
    failure: Option<String>,
}

impl AuditedCanonicalReader {
    fn new(selection: CanonicalSelection) -> Result<Self, String> {
        Ok(Self {
            selection,
            next_window: 0,
            current: None,
            records_verified: 0,
            last_seq: None,
            exhausted: false,
            failure: None,
        })
    }

    /// Streams records in finalizer order. Records outside the explicit clipping
    /// bounds are still audited, but are not yielded.
    pub fn next_record(&mut self) -> Result<Option<JoinedCanonicalRecord>, String> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.next_record_inner();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn next_record_inner(&mut self) -> Result<Option<JoinedCanonicalRecord>, String> {
        loop {
            if self.current.is_none() {
                if self.next_window == self.selection.windows.len() {
                    self.exhausted = true;
                    return Ok(None);
                }
                self.current = Some(WindowReader::open(
                    &self.selection.canonical_root,
                    &self.selection.windows[self.next_window],
                )?);
                self.next_window += 1;
            }
            let current = self.current.as_mut().expect("opened above");
            if let Some(record) = current.next()? {
                if let Some(previous) = self.last_seq {
                    if record.canonical_seq != previous + 1 {
                        return Err(format!(
                            "canonical sequence changed from {previous} to {}",
                            record.canonical_seq
                        ));
                    }
                }
                self.last_seq = Some(record.canonical_seq);
                if record.visible_ns >= self.selection.effective_start_ns
                    && record.visible_ns < self.selection.requested_end_ns
                {
                    return Ok(Some(record));
                }
                continue;
            }
            let finished = self
                .current
                .take()
                .expect("current window exists")
                .finish()?;
            self.records_verified = self
                .records_verified
                .checked_add(finished)
                .ok_or_else(|| "canonical audit record count overflows u64".to_owned())?;
        }
    }

    /// Returns the audited capability only after the caller consumed through the
    /// stream terminator. This makes accidentally publishing from a prefix a type
    /// error rather than a convention.
    pub fn finish(self) -> Result<AuditedCanonicalSelection, String> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if !self.exhausted || self.current.is_some() {
            return Err(
                "canonical reader must be consumed through next_record() == None before finish()"
                    .to_owned(),
            );
        }
        Ok(AuditedCanonicalSelection {
            requested_start_ns: self.selection.requested_start_ns,
            requested_end_ns: self.selection.requested_end_ns,
            effective_start_ns: self.selection.effective_start_ns,
            receipt_identities: self
                .selection
                .windows
                .into_iter()
                .map(|window| window.identity)
                .collect(),
            records_verified: self.records_verified,
        })
    }
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
        let selected = SelectedWindow {
            receipt: receipt.clone(),
            identity: receipt_identity(canonical_root, receipt)?,
        };
        let mut reader = WindowReader::open(canonical_root, &selected)?;
        while reader.next()?.is_some() {}
        records = records
            .checked_add(reader.finish()?)
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

fn receipt_identity(root: &Path, receipt: &Receipt) -> Result<ReceiptIdentity, String> {
    let path = receipt_path(root, receipt.window_start_ns);
    let bytes =
        std::fs::read(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    Ok(ReceiptIdentity {
        window_start_ns: receipt.window_start_ns,
        window_end_ns: receipt.window_end_ns,
        byte_length: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        certified: receipt.certified,
    })
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
