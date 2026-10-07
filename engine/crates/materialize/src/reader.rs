//! Pinned, privately snapshotted derivative reads. No canonical input is reopened.
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use replay_domain::{EventHeader, LaneId, SegmentRecord};
use tempfile::TempDir;

use crate::verify::{
    ReadMode, VerificationCounts, VerifiedLines, decode_receipt_document, inspect_contents,
    parse_event, parse_reject, verify_counts, verify_disposition,
};
use crate::{
    CompressedOutput, DerivativeManifest, DerivativePin, DerivativeReceipt, RejectDisposition,
    RejectRecord, Sha256,
};

#[derive(Clone, Debug)]
pub struct ReadLimits {
    /// Existing scratch directory on the intended data volume. None uses the
    /// system temporary directory. Failure never falls back to another volume.
    pub snapshot_root: Option<PathBuf>,
    pub max_metadata_bytes: u64,
    pub max_line_bytes: u64,
    pub max_snapshot_bytes: u64,
    pub max_group_bytes: u64,
    pub max_group_records: u64,
    pub max_windows: usize,
    pub max_scope_entries: usize,
    pub max_lanes: usize,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            snapshot_root: None,
            max_metadata_bytes: 1024 * 1024,
            max_line_bytes: 16 * 1024 * 1024,
            max_snapshot_bytes: 8 * 1024 * 1024 * 1024,
            max_group_bytes: 64 * 1024 * 1024,
            max_group_records: 100_000,
            max_windows: 4096,
            max_scope_entries: 100_000,
            max_lanes: 1024,
        }
    }
}

impl ReadLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_metadata_bytes == 0
            || self.max_line_bytes == 0
            || self.max_line_bytes == u64::MAX
            || self.max_metadata_bytes == u64::MAX
            || self.max_snapshot_bytes == 0
            || self.max_group_bytes == 0
            || self.max_group_records == 0
            || self.max_windows == 0
            || self.max_scope_entries == 0
            || self.max_lanes == 0
        {
            return Err("read limits must be positive and bounded".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PinnedDerivative {
    pub directory: PathBuf,
    pub pin: DerivativePin,
}

/// Verified metadata only; this is not a capability to read trusted events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivativeMetadata {
    pin: DerivativePin,
    manifest: DerivativeManifest,
    receipt: DerivativeReceipt,
    receipt_bytes: Vec<u8>,
    manifest_bytes: Vec<u8>,
    coverage: Option<crate::CoverageEvidence>,
}

impl DerivativeMetadata {
    pub fn pin(&self) -> &DerivativePin {
        &self.pin
    }
    pub fn manifest(&self) -> &DerivativeManifest {
        &self.manifest
    }
    pub fn metadata_bytes(&self) -> u64 {
        (self.receipt_bytes.len() + self.manifest_bytes.len()) as u64
    }
    pub fn coverage(&self) -> Option<&crate::CoverageEvidence> {
        self.coverage.as_ref()
    }
    pub fn supports_source_evidence(&self) -> bool {
        self.coverage.is_some()
    }
}

pub(crate) fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|e| e.to_string())?
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > maximum {
        return Err("metadata exceeds read limit".into());
    }
    Ok(bytes)
}

pub fn inspect_pinned(
    input: &PinnedDerivative,
    limits: &ReadLimits,
) -> Result<DerivativeMetadata, String> {
    limits.validate()?;
    crate::schema::validate_address(&input.pin.derivative_address, "pin address")?;
    let receipt_path = input.directory.join("receipt.json");
    let receipt_bytes = read_bounded(&receipt_path, limits.max_metadata_bytes)?;
    if Sha256::digest(&receipt_bytes) != input.pin.receipt_sha256 {
        return Err("receipt does not match pin".into());
    }
    let (receipt, _) = decode_receipt_document(&receipt_bytes, &receipt_path)?;
    if receipt.derivative_address != input.pin.derivative_address {
        return Err("receipt address does not match pin".into());
    }
    receipt.validate()?;
    let remaining = limits
        .max_metadata_bytes
        .checked_sub(receipt_bytes.len() as u64)
        .ok_or("metadata exceeds read limit")?;
    let manifest_bytes = read_bounded(&input.directory.join("manifest.json"), remaining)?;
    // Metadata parsing shares exactly the independent verifier's bindings. It
    // consumes captured bytes, not another read of the caller-controlled path.
    let verified = inspect_contents(
        &input.directory,
        receipt.clone(),
        receipt_bytes.clone(),
        true,
        &manifest_bytes,
    )?;
    let coverage = verified
        .manifest
        .source_receipt
        .document
        .as_ref()
        .map(|_| crate::CoverageEvidence::from_source(&verified.manifest.source_receipt))
        .transpose()?;
    if coverage
        .as_ref()
        .is_some_and(|c| c.lanes().len() > limits.max_lanes)
    {
        return Err("coverage lane count exceeds read limit".into());
    }
    Ok(DerivativeMetadata {
        pin: input.pin.clone(),
        coverage,
        manifest: verified.manifest,
        receipt,
        receipt_bytes,
        manifest_bytes,
    })
}

/// No raw rejected envelope crosses this boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceDelivery {
    header: EventHeader,
    connection_epoch: Option<String>,
    records: Vec<SegmentRecord>,
    disposition: Option<RejectDisposition>,
    logical_bytes: u64,
    record_count: u64,
}

impl SourceDelivery {
    /// None is an explicit missing profile-1 capability, never an inferred epoch.
    pub fn connection_epoch(&self) -> Option<&str> {
        self.connection_epoch.as_deref()
    }
    pub fn header(&self) -> &EventHeader {
        &self.header
    }
    pub fn records(&self) -> &[SegmentRecord] {
        &self.records
    }
    pub fn disposition(&self) -> Option<&RejectDisposition> {
        self.disposition.as_ref()
    }
    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }
    pub fn record_count(&self) -> u64 {
        self.record_count
    }
}

/// The single pass over one derivative. Each output is decoded once and each
/// line parsed once; that one parsed record feeds every check. Per-line checks
/// run as a line is read, per-delivery checks before its delivery is returned,
/// and whole-window checks in `finish`. A delivery returned here is therefore
/// checked as far as its own and its lookahead's lines go: a later violation
/// fails the stream, and only `finish` proves the whole window. `mode` selects
/// only whether byte identities are re-proved (see [`ReadMode`]).
struct Deliveries {
    events: VerifiedLines,
    rejects: VerifiedLines,
    sources: Option<VerifiedLines>,
    event: Option<(SegmentRecord, u64)>,
    reject: Option<(RejectRecord, u64)>,
    limits: ReadLimits,
    primed: bool,
    manifest: DerivativeManifest,
    counts: VerificationCounts,
    previous_event: Option<(i64, u32)>,
    previous_reject_seq: Option<i64>,
    checks: DeliveryChecks,
    mode: ReadMode,
}

impl Deliveries {
    fn open(
        directory: &Path,
        manifest: &DerivativeManifest,
        limits: &ReadLimits,
        mode: ReadMode,
    ) -> Result<Self, String> {
        Ok(Self {
            events: VerifiedLines::open_limit(
                &directory.join("events.ndjson.zst"),
                &manifest.events,
                limits.max_line_bytes,
                mode,
            )?,
            rejects: VerifiedLines::open_limit(
                &directory.join("rejects.ndjson.zst"),
                &manifest.rejects,
                limits.max_line_bytes,
                mode,
            )?,
            sources: manifest
                .sources
                .as_ref()
                .map(|output| {
                    VerifiedLines::open_limit(
                        &directory.join(&output.file),
                        output,
                        limits.max_line_bytes,
                        mode,
                    )
                })
                .transpose()?,
            event: None,
            reject: None,
            limits: limits.clone(),
            primed: false,
            manifest: manifest.clone(),
            counts: VerificationCounts::default(),
            previous_event: None,
            previous_reject_seq: None,
            checks: DeliveryChecks::default(),
            mode,
        })
    }
    fn read_event(&mut self) -> Result<(), String> {
        self.event = match self.events.next_line()? {
            Some(line) => Some((
                parse_event(
                    line,
                    &mut self.previous_event,
                    &mut self.counts,
                    self.mode,
                    self.manifest.normalized_schema_version,
                )?,
                line.len() as u64 + 1,
            )),
            None => None,
        };
        Ok(())
    }
    fn read_reject(&mut self) -> Result<(), String> {
        self.reject = match self.rejects.next_line()? {
            Some(line) => Some((
                parse_reject(
                    line,
                    &mut self.previous_reject_seq,
                    &mut self.counts,
                    &self.manifest,
                    self.mode,
                )?,
                line.len() as u64 + 1,
            )),
            None => None,
        };
        Ok(())
    }
    fn next(&mut self) -> Result<Option<SourceDelivery>, String> {
        if !self.primed {
            self.read_event()?;
            self.read_reject()?;
            self.primed = true;
        }
        let event_seq = self
            .event
            .as_ref()
            .map(|(r, _)| r.header().address().canonical_seq());
        let reject_seq = self
            .reject
            .as_ref()
            .map(|(r, _)| r.header().address().canonical_seq());
        let seq = match (event_seq, reject_seq) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => {
                if let Some(sources) = &mut self.sources {
                    if sources.next_line()?.is_some() {
                        return Err("extra source evidence record".into());
                    }
                }
                return Ok(None);
            }
        };
        let header = if event_seq == Some(seq) {
            self.event.as_ref().unwrap().0.header().clone()
        } else {
            self.reject.as_ref().unwrap().0.header().clone()
        };
        if header.address().event_index() != 0 {
            return Err("source must start at event index zero".into());
        }
        let mut delivery = SourceDelivery {
            header,
            connection_epoch: None,
            records: Vec::new(),
            disposition: None,
            logical_bytes: 0,
            record_count: 0,
        };
        if let Some(sources) = &mut self.sources {
            let bytes = sources
                .next_line()?
                .ok_or("missing source evidence record")?;
            let canonical = self.mode == ReadMode::Audit;
            #[cfg(test)]
            if canonical {
                crate::verify::probe::canonical_check();
            }
            let source = crate::evidence::SourceEvidence::decode(bytes, canonical)?;
            if source.header != delivery.header {
                return Err("source evidence header disagrees with delivery".into());
            }
            add_size(
                &mut delivery.logical_bytes,
                bytes.len() as u64 + 1,
                self.limits.max_group_bytes,
            )?;
            delivery.connection_epoch = Some(source.connection_epoch);
        }
        while self
            .event
            .as_ref()
            .is_some_and(|(r, _)| r.header().address().canonical_seq() == seq)
        {
            let (record, bytes) = self.event.take().unwrap();
            if !same_source(&delivery.header, record.header())
                || record.header().address().event_index() as usize != delivery.records.len()
            {
                return Err("normalized children disagree on source header or index".into());
            }
            add_size(
                &mut delivery.logical_bytes,
                bytes,
                self.limits.max_group_bytes,
            )?;
            add_size(&mut delivery.record_count, 1, self.limits.max_group_records)?;
            delivery.records.push(record);
            self.read_event()?;
        }
        let mut reject = None;
        if reject_seq == Some(seq) {
            let (record, bytes) = self.reject.take().unwrap();
            if record.header() != &delivery.header {
                return Err("reject source header disagrees with delivery".into());
            }
            if let Some(epoch) = delivery.connection_epoch() {
                let view =
                    indexer_types::EnvelopeView::parse(record.canonical_envelope().as_bytes())
                        .map_err(|e| e.to_string())?;
                if epoch != view.connection_epoch.as_str() {
                    return Err("source connection epoch disagrees with reject envelope".into());
                }
            }
            add_size(
                &mut delivery.logical_bytes,
                bytes,
                self.limits.max_group_bytes,
            )?;
            add_size(&mut delivery.record_count, 1, self.limits.max_group_records)?;
            self.read_reject()?;
            reject = Some(record);
        }
        verify_disposition(&delivery.records, reject.as_ref(), &mut self.counts)?;
        delivery.disposition = reject.map(|record| record.disposition().clone());
        self.checks
            .observe(&delivery, &self.manifest, &self.limits)?;
        Ok(Some(delivery))
    }
    /// The whole-window checks, valid only after `next` returned `None`: both
    /// frames' EOF and identities, line counts, the final tie run, coverage
    /// agreement, and the source sidecar's own EOF and identities.
    fn finish(self) -> Result<(), String> {
        self.events.finish()?;
        self.rejects.finish()?;
        verify_counts(&self.counts, &self.manifest)?;
        self.checks.finish(&self.manifest)?;
        if let Some(sources) = self.sources {
            sources.finish()?;
        }
        Ok(())
    }
}

fn same_source(a: &EventHeader, b: &EventHeader) -> bool {
    a.order_ns() == b.order_ns()
        && a.visible_ns() == b.visible_ns()
        && a.visible_tie_group() == b.visible_tie_group()
        && a.record_id() == b.record_id()
        && a.provenance() == b.provenance()
        && a.address().child(b.address().event_index()) == *b.address()
}

pub fn add_size(total: &mut u64, amount: u64, maximum: u64) -> Result<(), String> {
    *total = total
        .checked_add(amount)
        .filter(|sum| *sum <= maximum)
        .ok_or("read resource limit exceeded")?;
    Ok(())
}

/// Drains one derivative through the single pass: a full verdict in `mode`.
pub(crate) fn verify_stream(
    directory: &Path,
    manifest: &DerivativeManifest,
    limits: &ReadLimits,
    mode: ReadMode,
) -> Result<(), String> {
    let mut stream = Deliveries::open(directory, manifest, limits, mode)?;
    while stream.next()?.is_some() {}
    stream.finish()
}

/// Source-level ordering, window, lane, tie and coverage checks, kept as scalar
/// state across deliveries. Same-lane equal-time runs are not buffered.
#[derive(Default)]
struct DeliveryChecks {
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
}

impl DeliveryChecks {
    fn observe(
        &mut self,
        delivery: &SourceDelivery,
        manifest: &DerivativeManifest,
        limits: &ReadLimits,
    ) -> Result<(), String> {
        let h = delivery.header();
        self.first_seq.get_or_insert(h.address().canonical_seq());
        let lane = h.address().lane();
        let count = self.lane_counts.entry(lane.clone()).or_insert(0u64);
        *count = count.checked_add(1).ok_or("source lane count overflow")?;
        if !self.lane_deliveries.contains_key(lane) {
            if self.lane_deliveries.len() == limits.max_lanes {
                return Err("lane count exceeds read limit".into());
            }
            self.first_visible.insert(lane.clone(), h.visible_ns());
            add_size(
                &mut self.lane_bytes,
                lane.as_str().len() as u64,
                limits.max_metadata_bytes,
            )?;
        }
        if self
            .lane_deliveries
            .insert(lane.clone(), h.address().delivery_index())
            .is_some_and(|previous| previous >= h.address().delivery_index())
        {
            return Err("source delivery index repeats or decreases within lane".into());
        }
        if h.visible_ns() < manifest.effective_start_ns
            || h.visible_ns() >= manifest.effective_end_ns
        {
            return Err("source timestamp outside derivative window".into());
        }
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
            // A run that is already cross-lane can only be valid tagged with its
            // time. Failing now, not at the run's end, stops a streaming walker
            // from releasing an untagged member before its second lane is seen.
            if self.cross_lane {
                validate_tie(first, true)?;
            }
        } else {
            self.run_first = Some(h.clone());
            self.cross_lane = false;
            self.run_bytes = 0;
            self.run_records = 0;
        }
        if h.visible_tie_group().is_some() {
            add_size(
                &mut self.run_bytes,
                delivery.logical_bytes,
                limits.max_group_bytes,
            )?;
            add_size(
                &mut self.run_records,
                delivery.record_count,
                limits.max_group_records,
            )?;
        }
        self.previous = Some(h.clone());
        Ok(())
    }

    fn finish(mut self, manifest: &DerivativeManifest) -> Result<(), String> {
        if let Some(first) = &self.run_first {
            validate_tie(first, self.cross_lane)?;
        }
        if manifest.source_receipt.document.is_some() {
            let coverage = crate::CoverageEvidence::from_source(&manifest.source_receipt)?;
            if coverage.sequence()
                != (
                    self.first_seq,
                    self.previous.as_ref().map(|h| h.address().canonical_seq()),
                )
            {
                return Err("source sequence disagrees with coverage".into());
            }
            if coverage.records() != manifest.counts.input_records {
                return Err("source coverage count disagrees with derivative".into());
            }
            for (lane, status) in coverage.lanes() {
                if let crate::LaneState::Present { records } = status.state {
                    if self.lane_counts.remove(lane).unwrap_or(0) != records {
                        return Err("source lane count disagrees with coverage".into());
                    }
                }
            }
            if !self.lane_counts.is_empty() {
                return Err("source delivery from excluded lane".into());
            }
            for fault in coverage.faults() {
                if let crate::SourceFaultReason::VisibleClockRegression {
                    observed_visible_ns,
                    ..
                } = fault.reason
                {
                    if self.first_visible.get(&fault.lane) != Some(&observed_visible_ns) {
                        return Err("source clock diagnosis disagrees with first delivery".into());
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_tie(first: &EventHeader, cross_lane: bool) -> Result<(), String> {
    if first.visible_tie_group() != cross_lane.then_some(first.visible_ns()) {
        return Err("tie group disagrees with complete equal-time source run".into());
    }
    Ok(())
}

pub struct VerifiedWindowReader {
    stream: Option<Deliveries>,
    metadata: DerivativeMetadata,
    // Files are never reopened from the original directory after verified open.
    _snapshot: TempDir,
    poisoned: bool,
    exhausted: bool,
}

pub struct FinishedWindow {
    metadata: DerivativeMetadata,
}
impl FinishedWindow {
    pub fn metadata(&self) -> &DerivativeMetadata {
        &self.metadata
    }
}

pub fn open_pinned(
    input: &PinnedDerivative,
    limits: &ReadLimits,
) -> Result<VerifiedWindowReader, String> {
    open_pinned_with_checkpoint(input, limits, |_, _| {})
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadCheckpoint {
    MetadataCaptured,
    SnapshotCopied,
    ReaderOpened,
}

pub(crate) fn open_pinned_with_checkpoint(
    input: &PinnedDerivative,
    limits: &ReadLimits,
    mut checkpoint: impl FnMut(ReadCheckpoint, &Path),
) -> Result<VerifiedWindowReader, String> {
    let metadata = inspect_pinned(input, limits)?;
    checkpoint(ReadCheckpoint::MetadataCaptured, &input.directory);
    let mut total = metadata.metadata_bytes();
    for output in metadata.manifest.outputs() {
        add_size(
            &mut total,
            output.stored.byte_length,
            limits.max_snapshot_bytes,
        )?;
    }
    let mut builder = tempfile::Builder::new();
    builder.prefix("replay-verified-window-");
    let snapshot = match &limits.snapshot_root {
        Some(root) => builder.tempdir_in(root),
        None => builder.tempdir(),
    }
    .map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(snapshot.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(|e| e.to_string())?;
    }
    // The installed directory's exact bytes were bound to this pin by SHA-256
    // at install, and the receipt and manifest were just re-bound to it by
    // SHA-256 above. The copy re-proves only each data file's exact stored
    // length; the structural decode re-proves frame, checksum, length and LF
    // count. Data bytes are not re-hashed on this hot path.
    for output in metadata.manifest.outputs() {
        copy_bound(
            &input.directory.join(&output.file),
            &snapshot.path().join(&output.file),
            output,
        )?;
    }
    checkpoint(ReadCheckpoint::SnapshotCopied, snapshot.path());
    // No verification pass runs here: every semantic check runs once, while
    // the caller traverses, and only a clean, fully checked EOF can finish.
    let stream = Deliveries::open(
        snapshot.path(),
        &metadata.manifest,
        limits,
        ReadMode::PinnedReplay,
    )?;
    checkpoint(ReadCheckpoint::ReaderOpened, snapshot.path());
    Ok(VerifiedWindowReader {
        stream: Some(stream),
        metadata,
        _snapshot: snapshot,
        poisoned: false,
        exhausted: false,
    })
}

/// Copies one stored output into the private snapshot, bounded by and checked
/// against the receipt's exact stored length (with an extra-byte check). It
/// computes no digest: the pin's install-time SHA-256 binding covers content.
fn copy_bound(source: &Path, target: &Path, output: &CompressedOutput) -> Result<(), String> {
    let limit = output
        .stored
        .byte_length
        .checked_add(1)
        .ok_or("stored length overflow")?;
    let mut source = File::open(source).map_err(|e| e.to_string())?.take(limit);
    let mut sink = File::create(target).map_err(|e| e.to_string())?;
    let mut buffer = vec![0; 1024 * 1024];
    let mut count = 0u64;
    loop {
        let read = match source.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.to_string()),
        };
        sink.write_all(&buffer[..read]).map_err(|e| e.to_string())?;
        count += read as u64;
    }
    if count != output.stored.byte_length {
        return Err("snapshot stored length mismatch".into());
    }
    sink.flush().map_err(|e| e.to_string())
}

impl VerifiedWindowReader {
    pub fn metadata(&self) -> &DerivativeMetadata {
        &self.metadata
    }
    pub fn next_delivery(&mut self) -> Result<Option<SourceDelivery>, String> {
        if self.poisoned {
            return Err("derivative reader is poisoned".into());
        }
        if self.exhausted {
            return Ok(None);
        }
        let result = self.stream.as_mut().unwrap().next();
        match result {
            Ok(None) => {
                if let Err(error) = self.stream.take().unwrap().finish() {
                    self.poisoned = true;
                    return Err(error);
                }
                self.exhausted = true;
                Ok(None)
            }
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
            value => value,
        }
    }
    pub fn finish(self) -> Result<FinishedWindow, String> {
        if self.poisoned || !self.exhausted {
            return Err("derivative reader requires verified EOF before finish".into());
        }
        Ok(FinishedWindow {
            metadata: self.metadata.clone(),
        })
    }
}
