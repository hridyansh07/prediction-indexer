//! Pinned, privately snapshotted derivative reads. No canonical input is reopened.
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use replay_domain::{EventHeader, SegmentRecord};
use tempfile::TempDir;

use crate::verify::{VerifiedLines, decode_receipt_document, inspect_contents, verify_data};
use crate::{
    DerivativeManifest, DerivativePin, DerivativeReceipt, RejectDisposition, RejectRecord, Sha256,
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
    Ok(DerivativeMetadata {
        pin: input.pin.clone(),
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
    records: Vec<SegmentRecord>,
    disposition: Option<RejectDisposition>,
    logical_bytes: u64,
    record_count: u64,
}

impl SourceDelivery {
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

struct Deliveries {
    events: VerifiedLines,
    rejects: VerifiedLines,
    event: Option<(SegmentRecord, u64)>,
    reject: Option<(RejectRecord, u64)>,
    limits: ReadLimits,
    primed: bool,
}

impl Deliveries {
    fn open(
        directory: &Path,
        manifest: &DerivativeManifest,
        limits: &ReadLimits,
    ) -> Result<Self, String> {
        Ok(Self {
            events: VerifiedLines::open_limit(
                &directory.join("events.ndjson.zst"),
                &manifest.events,
                limits.max_line_bytes,
            )?,
            rejects: VerifiedLines::open_limit(
                &directory.join("rejects.ndjson.zst"),
                &manifest.rejects,
                limits.max_line_bytes,
            )?,
            event: None,
            reject: None,
            limits: limits.clone(),
            primed: false,
        })
    }
    fn read_event(&mut self) -> Result<(), String> {
        self.event = self
            .events
            .next_line()?
            .map(|line| {
                SegmentRecord::from_canonical_json(line)
                    .map(|r| (r, line.len() as u64 + 1))
                    .map_err(|e| e.to_string())
            })
            .transpose()?;
        Ok(())
    }
    fn read_reject(&mut self) -> Result<(), String> {
        self.reject = self
            .rejects
            .next_line()?
            .map(|line| RejectRecord::from_canonical_json(line).map(|r| (r, line.len() as u64 + 1)))
            .transpose()?;
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
            (None, None) => return Ok(None),
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
            records: Vec::new(),
            disposition: None,
            logical_bytes: 0,
            record_count: 0,
        };
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
        if reject_seq == Some(seq) {
            let (reject, bytes) = self.reject.take().unwrap();
            if reject.header() != &delivery.header {
                return Err("reject source header disagrees with delivery".into());
            }
            add_size(
                &mut delivery.logical_bytes,
                bytes,
                self.limits.max_group_bytes,
            )?;
            add_size(&mut delivery.record_count, 1, self.limits.max_group_records)?;
            delivery.disposition = Some(reject.disposition().clone());
            self.read_reject()?;
        }
        Ok(Some(delivery))
    }
    fn finish(self) -> Result<(), String> {
        self.events.finish()?;
        self.rejects.finish()
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

pub(crate) fn verify_deliveries(
    directory: &Path,
    manifest: &DerivativeManifest,
    limits: &ReadLimits,
) -> Result<(), String> {
    let mut stream = Deliveries::open(directory, manifest, limits)?;
    let mut previous: Option<EventHeader> = None;
    let mut run_first: Option<EventHeader> = None;
    let mut cross_lane = false;
    let mut run_bytes = 0;
    let mut run_records = 0;
    let mut lane_deliveries = BTreeMap::new();
    let mut lane_bytes = 0;
    while let Some(delivery) = stream.next()? {
        let h = delivery.header();
        let lane = h.address().lane();
        if !lane_deliveries.contains_key(lane) {
            if lane_deliveries.len() == limits.max_lanes {
                return Err("lane count exceeds read limit".into());
            }
            add_size(
                &mut lane_bytes,
                lane.as_str().len() as u64,
                limits.max_metadata_bytes,
            )?;
        }
        if lane_deliveries
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
        if let Some(p) = &previous {
            if p.address().canonical_seq().checked_add(1) != Some(h.address().canonical_seq()) {
                return Err("source canonical sequence is not dense".into());
            }
            if h.visible_ns() < p.visible_ns() {
                return Err("source timestamps decrease".into());
            }
        }
        if run_first
            .as_ref()
            .is_some_and(|first| first.visible_ns() != h.visible_ns())
        {
            validate_tie(run_first.as_ref().unwrap(), cross_lane)?;
            run_first = None;
        }
        if let Some(first) = &run_first {
            if first.visible_tie_group() != h.visible_tie_group() {
                return Err("inconsistent equal-time tie tags".into());
            }
            cross_lane |= first.address().lane() != h.address().lane();
        } else {
            run_first = Some(h.clone());
            cross_lane = false;
            run_bytes = 0;
            run_records = 0;
        }
        if h.visible_tie_group().is_some() {
            add_size(
                &mut run_bytes,
                delivery.logical_bytes,
                limits.max_group_bytes,
            )?;
            add_size(
                &mut run_records,
                delivery.record_count,
                limits.max_group_records,
            )?;
        }
        previous = Some(h.clone());
    }
    if let Some(first) = &run_first {
        validate_tie(first, cross_lane)?;
    }
    stream.finish()
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
    for output in [&metadata.manifest.events, &metadata.manifest.rejects] {
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
    for output in [&metadata.manifest.events, &metadata.manifest.rejects] {
        let source = File::open(input.directory.join(&output.file)).map_err(|e| e.to_string())?;
        let mut sink =
            File::create(snapshot.path().join(&output.file)).map_err(|e| e.to_string())?;
        let count = std::io::copy(
            &mut source.take(
                output
                    .stored
                    .byte_length
                    .checked_add(1)
                    .ok_or("stored length overflow")?,
            ),
            &mut sink,
        )
        .map_err(|e| e.to_string())?;
        if count != output.stored.byte_length {
            return Err("snapshot stored length mismatch".into());
        }
        sink.flush().map_err(|e| e.to_string())?;
    }
    checkpoint(ReadCheckpoint::SnapshotCopied, snapshot.path());
    verify_data(snapshot.path(), &metadata.manifest, limits)?;
    let stream = Deliveries::open(snapshot.path(), &metadata.manifest, limits)?;
    checkpoint(ReadCheckpoint::ReaderOpened, snapshot.path());
    Ok(VerifiedWindowReader {
        stream: Some(stream),
        metadata,
        _snapshot: snapshot,
        poisoned: false,
        exhausted: false,
    })
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
