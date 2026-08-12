//! Writing one finalized window: canonical evidence, provenance, and a receipt.
//!
//! ```text
//! canonical/date=<YYYY-MM-DD>/window=<start_ns>/
//!   evidence.ndjson.zst     one checksummed frame containing the original envelope lines
//!   provenance.ndjson.zst   one checksummed frame containing one line per position
//!   receipt.json        the commit marker
//! ```
//!
//! **The receipt is the commit marker**, exactly as a seal is for a segment. A
//! renamed `evidence.ndjson.zst` with no receipt is a crash between two steps, and
//! nothing may treat it as evidence until a receipt names its digest.
//!
//! Decoded `evidence.ndjson.zst` is the *original lines*, copied without re-encoding. §5 is
//! explicit that canonical evidence is not a normalized book event: re-encoding
//! would make the canonical file a lossy interpretation of the tape rather than
//! a reordering of it, and the interpretation is exactly the part we expect to
//! revise later.
//!
//! Digests are maintained while writing. At a gigabyte per window per lane, the
//! difference between hashing as bytes go past and hashing afterwards is a
//! second full pass over the disk — the same reason the segment writer keeps its
//! seal state incrementally.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use indexer_continuity::{Classifier, IdentityVerdict};
use indexer_types::{CanonicalSeq, EnvelopeView, Positioned};
use prediction_encoder::{
    CODEC_VERSION, DEFAULT_ZSTD_LEVEL, EncodeResult, StreamingEncoder, encoder_version,
};
use serde::{Deserialize, Serialize};

use crate::continuity::{ClassifiedLine, LaneClocks, OrderingState, SeenEpochs, WrittenLine};
use crate::identity::AttemptIdentity;
use crate::merge::LaneStream;
use crate::reader::{LaneReader, SegmentClaims, SegmentInput};
use crate::window::{
    DeliveryContinuity, Eligibility, LaneDeliverySpan, Window, WindowStatus, date_partition,
};
use crate::{lane_rank, merged};

pub const RECEIPT_VERSION: u64 = 1;
pub const FINALIZER_VERSION: u64 = 1;

pub const EVIDENCE_FILE: &str = "evidence.ndjson.zst";
pub const PROVENANCE_FILE: &str = "provenance.ndjson.zst";
const RECEIPT_FILE: &str = "receipt.json";
const LEASE_FILE: &str = ".finalize.lease";
const OPEN_SUFFIX: &str = ".open";

/// Identity of the decoded NDJSON carried by a compressed canonical object.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DecodedIdentity {
    pub byte_length: u64,
    pub line_count: u64,
    pub sha256: String,
}

/// Identity of the compressed bytes as stored.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct StoredIdentity {
    pub byte_length: u64,
    pub sha256: String,
}

/// The fixed V1 compression contract. The encoder string is diagnostic only.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CompressionContract {
    pub algorithm: String,
    pub level: i32,
    pub frame_checksum: bool,
    pub dictionary: Option<String>,
    pub frame_count: u64,
    pub encoder: String,
}

/// What one compressed output asserts about both representations.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CanonicalOutput {
    pub file: String,
    pub content_encoding: String,
    pub decoded: DecodedIdentity,
    pub stored: StoredIdentity,
    pub compression: CompressionContract,
}

/// One source segment a window consumed.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct InputSegment {
    pub lane: String,
    pub data_file: String,
    pub segment_index: u64,
    pub line_count: u64,
    pub sha256: String,
    pub first_delivery_index: Option<u64>,
    pub last_delivery_index: Option<u64>,
}

/// Why a lane is not contributing to this window.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LaneFault {
    pub lane: String,
    /// `lane_missing` or `lane_invalid` — §5's two verdicts.
    pub reason: String,
    pub detail: Option<String>,
}

/// The completion receipt (§5).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Receipt {
    pub receipt_version: u64,
    pub window_start_ns: u64,
    pub window_end_ns: u64,
    /// `complete` or `incomplete`. An incomplete window is usable evidence with
    /// explicit coverage limits, and must never be presented as a complete
    /// cross-venue window.
    pub completeness: String,
    /// False when something makes the window unfit to certify — §8 names a
    /// decreasing `visible_ns` as such a case.
    pub certified: bool,
    /// Recorded verbatim: a verdict is only readable against the expectation
    /// that produced it, and that expectation is deployment configuration.
    pub expected_lanes: Vec<String>,
    pub present_lanes: Vec<String>,
    /// Present but not expected. Merged like any other evidence, never counted
    /// toward completeness.
    pub unexpected_lanes: Vec<String>,
    pub missing_lanes: Vec<LaneFault>,
    pub invalid_lanes: Vec<LaneFault>,
    pub finalization_deadline_seconds: u64,
    pub deadline_expired: bool,
    pub finalized_at_ns: u64,
    pub inputs: Vec<InputSegment>,
    pub evidence: CanonicalOutput,
    pub provenance: CanonicalOutput,
    pub first_canonical_seq: Option<i64>,
    pub last_canonical_seq: Option<i64>,
    /// Handed to the next window: ordering history so its verdicts continue,
    /// and each lane's last receive time so its boundary can be checked.
    /// Recorded here, not only in the watermark, so a deleted watermark rebuilds
    /// exactly rather than approximately.
    #[serde(default)]
    pub carried: crate::watermark::Carried,
    /// Lanes whose first receive time in this window fell below their last in
    /// the previous one. Non-empty means the window is quarantined (§7).
    #[serde(default)]
    pub clock_faults: Vec<crate::watermark::ClockFault>,
    pub finalizer_version: u64,
}

impl Receipt {
    fn delivery_spans(&self) -> Vec<LaneDeliverySpan> {
        self.present_lanes
            .iter()
            .map(|lane| {
                let mut inputs = self
                    .inputs
                    .iter()
                    .filter(|input| &input.lane == lane)
                    .collect::<Vec<_>>();
                inputs.sort_by_key(|input| (input.segment_index, &input.data_file));
                LaneDeliverySpan {
                    lane: lane.clone(),
                    first: inputs.iter().find_map(|input| input.first_delivery_index),
                    last: inputs
                        .iter()
                        .rev()
                        .find_map(|input| input.last_delivery_index),
                }
            })
            .collect()
    }
}

/// Rebuilds the lane boundary carried by already committed windows.
///
/// Folding every receipt, rather than looking only at the newest one, preserves
/// a last delivery index across valid empty windows. It also fails closed if an
/// already committed adjacent pair contradicts the lane-lifetime counter.
pub fn delivery_continuity(
    committed: &BTreeMap<u64, Receipt>,
) -> Result<DeliveryContinuity, String> {
    let mut continuity = DeliveryContinuity::default();
    for receipt in committed.values() {
        continuity.advance(
            receipt.window_start_ns,
            receipt.window_end_ns,
            &receipt.delivery_spans(),
        )?;
    }
    Ok(continuity)
}

/// Advances continuity from the receipt that actually committed, after any
/// record-level lane exclusion changed the window's final status.
pub fn advance_delivery_continuity(
    continuity: &mut DeliveryContinuity,
    receipt: &Receipt,
) -> Result<(), String> {
    continuity.advance(
        receipt.window_start_ns,
        receipt.window_end_ns,
        &receipt.delivery_spans(),
    )
}

/// One line of the provenance index (§5).
#[derive(Serialize)]
struct ProvenanceLine<'a> {
    canonical_seq: i64,
    lane_id: &'a str,
    source_segment_sha256: &'a str,
    source_line_number: u64,
    record_id: &'a str,
    content_hash: &'a str,
    /// What this delivery meant for its stream: `continuous`, `gap_proven`,
    /// `duplicate`, and so on — the same vocabulary `indexer-ingest` commits.
    ///
    /// Duplicate and conflict are **window-scoped**: the exact scratch index
    /// starts empty for each window and is never carried in the watermark. See
    /// `continuity.rs`.
    continuity_verdict: &'a str,
    /// Non-null exactly when two or more distinct lanes share this `visible_ns`.
    /// §1 requires analysis to treat such records as simultaneous at capture
    /// resolution and forbids reading lead-lag from the order rank imposed.
    visible_tie_group: Option<u64>,
}

#[derive(Debug)]
pub struct FinalizedWindow {
    pub receipt: Receipt,
    pub directory: PathBuf,
    /// Operational proof that canonicalization did not retain per-record
    /// identity history in the classifier.
    pub identity_records_in_memory: usize,
}

/// What finalizing one window produced.
#[derive(Debug)]
pub enum WindowOutcome {
    Committed(Box<FinalizedWindow>),
    /// The merge excluded a lane that seals alone could not have faulted, and
    /// the window's deadline has not expired. Nothing was written.
    ///
    /// This closes the gap the record-level retry opened. A lane passes seal and
    /// digest validation, so the window reads `Complete` and the deadline is
    /// never consulted; the merge then finds a malformed envelope and drops that
    /// lane. Committing at that point publishes an incomplete window while the
    /// deployment still had hours in which that lane's next segment could
    /// arrive — §5 waits for a *valid* seal, and validity was not established
    /// until the records were read.
    Deferred {
        unsatisfied: Vec<String>,
        until_ns: u64,
    },
}

/// A merge attempt either failed outright or blamed one lane.
enum MergeAttemptError {
    /// Not attributable to a lane — a disk error, an encoding bug. No retry
    /// could help, and continuing would publish a partial window.
    Fatal(String),
    /// Attributable. That lane is excluded and the merge retried without it.
    Lane(crate::merge::MergeFault),
}

impl From<String> for MergeAttemptError {
    fn from(error: String) -> Self {
        Self::Fatal(error)
    }
}

/// An output file compressed and dual-hashed while logical records are written.
struct CompressedFile {
    encoder: Option<StreamingEncoder<std::fs::File>>,
    open_path: PathBuf,
    final_path: PathBuf,
    cleanup_on_drop: bool,
}

impl CompressedFile {
    fn create(directory: &Path, name: &str) -> Result<Self, String> {
        let final_path = directory.join(name);
        // Process-unique: the lease already makes concurrent runs an error, but
        // a shared intermediate is the thing that corrupts output rather than
        // merely failing, so it costs nothing to make it unshareable.
        let open_path = directory.join(format!("{name}.{}{OPEN_SUFFIX}", std::process::id()));
        let handle = std::fs::File::create(&open_path)
            .map_err(|error| format!("creating {}: {error}", open_path.display()))?;
        let encoder = StreamingEncoder::new(handle, DEFAULT_ZSTD_LEVEL).map_err(|error| {
            format!("creating Zstandard frame {}: {error}", open_path.display())
        })?;
        Ok(Self {
            encoder: Some(encoder),
            open_path,
            final_path,
            cleanup_on_drop: true,
        })
    }

    fn write_line(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.encoder
            .as_mut()
            .expect("encoder exists until finish")
            .write_all(bytes)
            .map_err(|error| format!("writing {}: {error}", self.open_path.display()))?;
        Ok(())
    }

    /// Finishes the one frame without publishing its final name. Both outputs
    /// must reach this state before either is renamed.
    fn finish(mut self) -> Result<FinishedCompressedFile, String> {
        let encoder = self.encoder.take().expect("encoder exists until finish");
        let (handle, result) = encoder
            .finish()
            .map_err(|error| format!("finishing {}: {error}", self.open_path.display()))?;
        self.cleanup_on_drop = false;
        Ok(FinishedCompressedFile {
            handle: Some(handle),
            open_path: self.open_path.clone(),
            final_path: self.final_path.clone(),
            output: output_from_result(&self.final_path, result),
            published: false,
        })
    }

    fn abandon(self) {
        drop(self);
    }
}

impl Drop for CompressedFile {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            self.encoder.take();
            let _ = std::fs::remove_file(&self.open_path);
        }
    }
}

struct FinishedCompressedFile {
    handle: Option<std::fs::File>,
    open_path: PathBuf,
    final_path: PathBuf,
    output: CanonicalOutput,
    published: bool,
}

impl FinishedCompressedFile {
    fn sync(&self) -> Result<(), String> {
        self.handle
            .as_ref()
            .expect("file remains open until publish")
            .sync_all()
            .map_err(|error| format!("fsyncing {}: {error}", self.open_path.display()))
    }

    fn publish(&mut self) -> Result<(), String> {
        self.handle.take();
        std::fs::rename(&self.open_path, &self.final_path)
            .map_err(|error| format!("renaming {}: {error}", self.open_path.display()))?;
        self.published = true;
        Ok(())
    }
}

impl Drop for FinishedCompressedFile {
    fn drop(&mut self) {
        self.handle.take();
        if !self.published {
            let _ = std::fs::remove_file(&self.open_path);
        }
    }
}

fn output_from_result(path: &Path, result: EncodeResult) -> CanonicalOutput {
    CanonicalOutput {
        file: path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        content_encoding: "zstd".to_owned(),
        decoded: DecodedIdentity {
            byte_length: result.logical.byte_length,
            line_count: result.logical.line_count,
            sha256: result.logical.sha256,
        },
        stored: StoredIdentity {
            byte_length: result.stored.byte_length,
            sha256: result.stored.sha256,
        },
        compression: CompressionContract {
            algorithm: "zstd".to_owned(),
            level: DEFAULT_ZSTD_LEVEL,
            frame_checksum: true,
            dictionary: None,
            frame_count: 1,
            encoder: format!(
                "prediction-encoder-rust/{}; {}",
                CODEC_VERSION,
                encoder_version()
            ),
        },
    }
}

/// fsyncs a directory so a rename inside it is durable.
///
/// Not optional and not best-effort. Without it a crash can lose the rename
/// while keeping the file, which is exactly the state the receipt is supposed to
/// rule out. An error here is a failed commit, never a warning behind a marker.
fn fsync_directory(directory: &Path) -> Result<(), String> {
    std::fs::File::open(directory)
        .and_then(|handle| handle.sync_all())
        .map_err(|error| format!("fsyncing directory {}: {error}", directory.display()))
}

/// Creates a directory chain, fsyncing each new directory into its parent.
///
/// `create_dir_all` alone leaves the *link* to a new directory sitting in its
/// parent's page cache. Every fsync inside `window=<start>` is then durable
/// within a directory that a crash can still take with it — receipt included —
/// so on Linux a committed window can vanish entirely. The entry has to be
/// pushed into each parent as the chain is built.
pub fn create_dir_all_durable(path: &Path) -> Result<(), String> {
    if path.is_dir() {
        return Ok(());
    }
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    // Depth-first: a directory's own entry lives in its parent, so the parent
    // must exist and be synced before this one can be linked into it durably.
    create_dir_all_durable(parent)?;
    match std::fs::create_dir(path) {
        Ok(()) => fsync_directory(parent),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(format!("creating {}: {error}", path.display())),
    }
}

/// An exclusive lease on a canonical root, held for the length of a run.
///
/// Two finalizers over one root are not safe. They share every intermediate
/// filename, so one can rename a file the other is still writing — the observed
/// failure was `renaming evidence.ndjson.zst.open: No such file or directory` — and
/// even with unique temporaries, two processes publishing receipts for the same
/// window race over which one the watermark ends up describing.
///
/// A lock file rather than an OS lock because the workspace denies `unsafe_code`
/// and `flock` needs it. The cost is that a killed finalizer leaves the file
/// behind; the contents name the process that took it so an operator can tell a
/// live holder from a corpse, and the error says so.
pub struct RootLease {
    path: PathBuf,
}

impl RootLease {
    pub fn acquire(canonical_root: &Path) -> Result<Self, String> {
        let path = canonical_root.join(LEASE_FILE);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut handle) => {
                let _ = writeln!(
                    handle,
                    "{{\"pid\": {}, \"acquired_ns\": {}}}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|since| since.as_nanos())
                        .unwrap_or(0),
                );
                let _ = handle.sync_all();
                Ok(Self { path })
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let holder = std::fs::read_to_string(&path).unwrap_or_default();
                Err(format!(
                    "another finalizer holds {}: {}. Two runs over one canonical root \
                     race over the same intermediate files and the same receipts. If no \
                     finalizer is running, that process died and the file can be removed.",
                    path.display(),
                    holder.trim()
                ))
            }
            Err(error) => Err(format!("taking {}: {error}", path.display())),
        }
    }
}

impl Drop for RootLease {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Where a window's canonical output lives.
pub fn window_directory(canonical_root: &Path, window_start_ns: u64) -> PathBuf {
    canonical_root
        .join(format!("date={}", date_partition(window_start_ns)))
        .join(format!("window={window_start_ns}"))
}

pub fn receipt_path(canonical_root: &Path, window_start_ns: u64) -> PathBuf {
    window_directory(canonical_root, window_start_ns).join(RECEIPT_FILE)
}

/// Validates, merges and commits one window.
///
/// Lane faults do not abort the window. §5 and finding R4 settled that a single
/// wedged or corrupt lane must not halt finalization for every healthy venue —
/// the window commits with the fault named in its receipt, which is strictly
/// more informative than no window at all.
#[allow(clippy::too_many_arguments)]
pub fn finalize_window(
    canonical_root: &Path,
    window: &Window,
    status: &WindowStatus,
    expected: &[String],
    verdict: &Eligibility,
    deadline_seconds: u64,
    first_canonical_seq: CanonicalSeq,
    now_ns: u64,
    // What the previous window handed over: ordering history so this window's
    // verdicts continue the counters, and each lane's last receive time so the
    // boundary can be checked. See `watermark.rs`.
    carried: &crate::watermark::Carried,
) -> Result<WindowOutcome, String> {
    // Lane verdicts were decided by `validate_window` before the deadline gate
    // was applied, so they are reused rather than recomputed — the same
    // classification must drive both "should this window wait" and "what does
    // its receipt say".
    let mut status = status.clone();

    // -- merge, excluding any lane a fault is attributable to ----------------
    //
    // Seals cannot reveal a record-level fault: a malformed envelope inside a
    // correctly-digested segment is only found by parsing it, which happens
    // during the merge. Letting that abort the run would mean one lane's bad
    // record destroys the whole window — a healthy Kalshi lane never commits
    // because Polymarket wrote one unparseable line. So a fault names its lane,
    // that lane is recorded `lane_invalid`, and the merge is retried without it.
    // Bounded: every attempt removes exactly one lane.
    let directory = window_directory(canonical_root, window.key.start_ns);
    create_dir_all_durable(&directory)?;

    let mut attempt = 0;
    let (
        evidence,
        provenance,
        inputs,
        lane_names,
        first_written,
        written,
        ordering,
        clocks,
        identity_records_in_memory,
    ) = loop {
        attempt += 1;
        if attempt > window.lanes.len() + 1 {
            return Err(format!(
                "window {} still faulted after excluding every lane",
                window.key.start_ns
            ));
        }

        let usable: Vec<&crate::window::LaneSegments> = window
            .lanes
            .values()
            .filter(|entry| status.is_valid(&entry.lane))
            .collect();

        let mut inputs: Vec<InputSegment> = Vec::new();
        let mut segment_digests: Vec<String> = Vec::new();
        let mut lane_names: Vec<String> = Vec::new();
        let mut streams: Vec<(String, LaneStream<'static>)> = Vec::new();

        for entry in &usable {
            let lane_slot = lane_names.len() as u16;
            lane_names.push(entry.lane.clone());
            let mut segments = Vec::new();
            for (path, seal) in &entry.segments {
                let slot = segment_digests.len() as u32;
                segment_digests.push(seal.sha256.clone());
                inputs.push(InputSegment {
                    lane: entry.lane.clone(),
                    data_file: seal.data_file.clone(),
                    segment_index: seal.segment_index,
                    line_count: seal.line_count,
                    sha256: seal.sha256.clone(),
                    first_delivery_index: seal.first_delivery_index,
                    last_delivery_index: seal.last_delivery_index,
                });
                segments.push(SegmentInput {
                    path: path.clone(),
                    segment: slot,
                    claims: Some(SegmentClaims::from_seal(seal)),
                });
            }
            streams.push((
                entry.lane.clone(),
                Box::new(LaneReader::new(lane_slot, segments)),
            ));
        }
        // Deterministic regardless of how the filesystem listed things.
        inputs.sort_by(|left, right| {
            (lane_rank(&left.lane), &left.lane, left.segment_index).cmp(&(
                lane_rank(&right.lane),
                &right.lane,
                right.segment_index,
            ))
        });

        let mut evidence = CompressedFile::create(&directory, EVIDENCE_FILE)?;
        let mut provenance = CompressedFile::create(&directory, PROVENANCE_FILE)?;
        let mut next_seq = first_canonical_seq;
        let mut first_written: Option<CanonicalSeq> = None;
        let mut written = 0u64;

        // Identity starts empty for every window and every lane-retry attempt;
        // ordering history continues from the watermark. The exact identity
        // index spills to a disposable SQLite file instead of retaining one
        // BTreeMap node per canonical record. See `continuity.rs`.
        let mut identity = AttemptIdentity::create(&directory)?;
        let mut classifier = Classifier::without_identity_history();
        classifier.restore(carried.ordering.seed());
        let mut seen = SeenEpochs::default();
        let mut clocks = LaneClocks::default();

        let outcome = (|| -> Result<(), MergeAttemptError> {
            for item in merged(streams).map_err(MergeAttemptError::Fatal)? {
                let item = item.map_err(MergeAttemptError::Lane)?;
                let record = &item.record;
                let lane = &lane_names[record.source.lane as usize];

                // Parsed before anything is written: a schema fault belongs to
                // its lane and must exclude it, not leave a half-written window.
                // The reader validated the same line, so reaching here means the
                // segment changed underneath us.
                let view = EnvelopeView::parse(&record.line).map_err(|error| {
                    MergeAttemptError::Lane(crate::merge::MergeFault {
                        lane: lane.clone(),
                        detail: format!("line {}: {error}", record.source.line_number),
                    })
                })?;
                let content_hash = indexer_types::ContentHash::hash(view.raw_payload.as_bytes());
                let identity_verdict = identity
                    .verdict(view.record_id.as_str(), content_hash)
                    .map_err(MergeAttemptError::Fatal)?;
                let first_observation = matches!(identity_verdict, IdentityVerdict::Unseen);

                // 1. bytes down, position assigned — the receipt.
                evidence
                    .write_line(&record.line)
                    .map_err(MergeAttemptError::Fatal)?;
                let written_line = WrittenLine::new(next_seq);

                // 2. decide, holding the receipt. Moves nothing.
                let fact =
                    classifier.classify_with_identity(&view, &written_line, identity_verdict);

                // 3. the verdict goes down beside the record it describes.
                let line = serde_json::to_vec(&ProvenanceLine {
                    canonical_seq: next_seq.get(),
                    lane_id: lane,
                    source_segment_sha256: &segment_digests[record.source.segment as usize],
                    source_line_number: record.source.line_number,
                    record_id: view.record_id.as_str(),
                    content_hash: &content_hash.to_hex(),
                    continuity_verdict: fact.cause.label(),
                    visible_tie_group: item.visible_tie_group,
                })
                .map_err(|error| {
                    MergeAttemptError::Fatal(format!("encoding provenance: {error}"))
                })?;
                provenance
                    .write_line(&line)
                    .map_err(MergeAttemptError::Fatal)?;
                provenance
                    .write_line(b"\n")
                    .map_err(MergeAttemptError::Fatal)?;

                // 4. only now does retained state move.
                seen.observe(lane, &fact);
                clocks.observe(lane, record.visible_ns);
                classifier.apply(&ClassifiedLine::new(
                    Positioned::position(&written_line),
                    fact,
                ));
                if first_observation {
                    identity
                        .remember(
                            view.record_id.as_str(),
                            content_hash,
                            Positioned::position(&written_line),
                        )
                        .map_err(MergeAttemptError::Fatal)?;
                }

                first_written.get_or_insert(next_seq);
                next_seq = next_seq.next();
                written += 1;
            }
            Ok(())
        })();

        match outcome {
            Ok(()) => {
                // Deliberately *not* committed yet. Excluding a lane during the
                // merge can make a window that read `Complete` incomplete, and
                // that has to be reconsidered against the deadline before any
                // rename makes it evidence.
                break (
                    evidence,
                    provenance,
                    inputs,
                    lane_names,
                    first_written,
                    written,
                    OrderingState::capture(&carried.ordering, classifier.state(), &seen.into_map()),
                    clocks,
                    classifier.retained_identity_count(),
                );
            }
            Err(fault) => {
                // Nothing partial survives to be mistaken for evidence.
                evidence.abandon();
                provenance.abandon();
                match fault {
                    MergeAttemptError::Fatal(error) => return Err(error),
                    MergeAttemptError::Lane(fault) => {
                        status.invalidate(&fault.lane, fault.detail);
                    }
                }
            }
        }
    };

    // §5 waits for a valid seal from every expected lane, and a record-level
    // fault is only discovered here. If dropping that lane leaves the window
    // short while its deadline still has time to run, nothing is published.
    //
    // Asked against the clock, not against `verdict`. The verdict was computed
    // before the merge ran, so it reads `Complete` precisely in the case this
    // check exists for — every lane looked valid until its records were parsed.
    let unsatisfied = window.unsatisfied(expected, &status);
    let until_ns = window
        .key
        .end_ns
        .saturating_add(deadline_seconds.saturating_mul(1_000_000_000));
    let post_merge_deadline_expired = !unsatisfied.is_empty() && now_ns >= until_ns;
    if now_ns < until_ns && !unsatisfied.is_empty() {
        evidence.abandon();
        provenance.abandon();
        return Ok(WindowOutcome::Deferred {
            unsatisfied: unsatisfied.into_iter().map(str::to_owned).collect(),
            until_ns,
        });
    }

    // §7's boundary check, run before anything claims to be certified: a lane's
    // first receive time in this window against its last in the previous one.
    let first_by_lane: BTreeMap<String, u64> = lane_names
        .iter()
        .filter_map(|lane| clocks.first(lane).map(|first| (lane.clone(), first)))
        .collect();
    let clock_faults = crate::watermark::clock_faults(window.key.start_ns, carried, &first_by_lane);

    // Close both frames before either name is published. Then make both byte
    // streams durable before the first rename, publish both names, and sync the
    // directory once for the pair. The receipt is committed separately below.
    let mut evidence = evidence.finish()?;
    let mut provenance = provenance.finish()?;
    evidence.sync()?;
    provenance.sync()?;
    evidence.publish()?;
    provenance.publish()?;
    fsync_directory(&directory)?;
    let evidence_digest = evidence.output.clone();
    let provenance_digest = provenance.output.clone();

    let invalid: Vec<LaneFault> = status
        .invalid_lanes()
        .into_iter()
        .map(|(lane, detail)| LaneFault {
            lane: lane.to_owned(),
            reason: "lane_invalid".to_owned(),
            detail: Some(detail.to_owned()),
        })
        .collect();

    // -- the receipt, which is what makes any of the above evidence ----------
    let missing: Vec<LaneFault> = window
        .missing(expected)
        .into_iter()
        .map(|lane| LaneFault {
            lane: lane.to_owned(),
            reason: "lane_missing".to_owned(),
            detail: None,
        })
        .collect();
    let invalid_names: Vec<&str> = invalid.iter().map(|fault| fault.lane.as_str()).collect();
    let complete = missing.is_empty()
        && !expected
            .iter()
            .any(|lane| invalid_names.contains(&lane.as_str()));

    let receipt = Receipt {
        receipt_version: RECEIPT_VERSION,
        window_start_ns: window.key.start_ns,
        window_end_ns: window.key.end_ns,
        completeness: if complete { "complete" } else { "incomplete" }.to_owned(),
        // §2 step 3 excludes a clock-regressed lane as `lane_invalid`, so an
        // uncertifiable window is already an incomplete one — except for a
        // *cross-window* boundary regression, which no lane's own seal can see.
        // That one quarantines the window it appeared in and nothing else.
        certified: complete && clock_faults.is_empty(),
        expected_lanes: expected.to_vec(),
        present_lanes: lane_names.clone(),
        unexpected_lanes: window
            .unexpected(expected)
            .into_iter()
            .map(str::to_owned)
            .collect(),
        missing_lanes: missing,
        invalid_lanes: invalid,
        finalization_deadline_seconds: deadline_seconds,
        deadline_expired: *verdict == Eligibility::DeadlineExpired || post_merge_deadline_expired,
        finalized_at_ns: now_ns,
        inputs,
        evidence: evidence_digest,
        provenance: provenance_digest,
        first_canonical_seq: first_written.map(CanonicalSeq::get),
        last_canonical_seq: (written > 0).then(|| first_canonical_seq.get() + written as i64 - 1),
        carried: crate::watermark::Carried {
            ordering,
            // A lane that said nothing this window keeps the boundary it had, so
            // the check still applies the next time it speaks.
            lane_visible_ns: {
                let mut carried_forward = carried.lane_visible_ns.clone();
                carried_forward.extend(clocks.last_by_lane());
                carried_forward
            },
        },
        clock_faults: clock_faults.clone(),
        finalizer_version: FINALIZER_VERSION,
    };

    write_receipt(&directory, &receipt)?;
    Ok(WindowOutcome::Committed(Box::new(FinalizedWindow {
        receipt,
        directory,
        identity_records_in_memory,
    })))
}

fn write_receipt(directory: &Path, receipt: &Receipt) -> Result<(), String> {
    write_json_durable(directory, RECEIPT_FILE, receipt)
}

/// Writes a JSON document through a temporary file and an atomic rename.
///
/// The same commit discipline a seal uses: the bytes are made durable, the name
/// is swapped in one step, and the directory entry is synced so the rename
/// itself survives a crash. Shared by the receipt and the watermark because
/// both are commit markers for what sits beside them.
pub fn write_json_durable<T: Serialize>(
    directory: &Path,
    name: &str,
    value: &T,
) -> Result<(), String> {
    let mut encoded =
        serde_json::to_vec_pretty(value).map_err(|error| format!("encoding {name}: {error}"))?;
    encoded.push(b'\n');

    let temporary = directory.join(format!("{name}.{}{OPEN_SUFFIX}", std::process::id()));
    let final_path = directory.join(name);
    {
        let mut handle = std::fs::File::create(&temporary)
            .map_err(|error| format!("creating {}: {error}", temporary.display()))?;
        handle
            .write_all(&encoded)
            .map_err(|error| format!("writing {}: {error}", temporary.display()))?;
        handle
            .sync_all()
            .map_err(|error| format!("fsyncing {}: {error}", temporary.display()))?;
    }
    std::fs::rename(&temporary, &final_path)
        .map_err(|error| format!("renaming {}: {error}", temporary.display()))?;
    fsync_directory(directory)
}

/// Reads and verifies a committed receipt, if the window has one.
///
/// **Structural validity is not enough.** An empty JSON object parses, and a
/// receipt whose `evidence.ndjson.zst` has been deleted still parses — both used to
/// be accepted as proof that a window was committed, which is exactly the claim
/// a receipt exists to make. So the document is deserialized into its real type,
/// its fields are checked against the window they claim, and the objects it
/// names are confirmed to exist at the length it recorded.
///
/// Lengths rather than digests: this runs over every retained window on every
/// run, and rehashing days of canonical output each time is the unbounded scan
/// the ingester already refuses. A length mismatch catches deletion and
/// truncation; full digest verification belongs in a deliberate audit pass.
pub fn read_receipt(
    canonical_root: &Path,
    window_start_ns: u64,
) -> Result<Option<Receipt>, String> {
    let directory = window_directory(canonical_root, window_start_ns);
    let path = directory.join(RECEIPT_FILE);
    if !path.is_file() {
        return Ok(None);
    }
    let encoded =
        std::fs::read(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    let receipt: Receipt = serde_json::from_slice(&encoded)
        .map_err(|error| format!("unreadable receipt {}: {error}", path.display()))?;

    let invalid = |detail: String| format!("unreadable receipt {}: {detail}", path.display());
    if receipt.receipt_version != RECEIPT_VERSION {
        return Err(invalid(format!(
            "unsupported receipt_version {}",
            receipt.receipt_version
        )));
    }
    if receipt.window_start_ns != window_start_ns {
        return Err(invalid(format!(
            "declares window_start_ns {} but sits under window={window_start_ns}",
            receipt.window_start_ns
        )));
    }
    if receipt.window_end_ns <= receipt.window_start_ns {
        return Err(invalid(
            "window_start_ns does not precede window_end_ns".to_owned(),
        ));
    }
    match (receipt.first_canonical_seq, receipt.last_canonical_seq) {
        (None, None) if receipt.evidence.decoded.line_count == 0 => {}
        (Some(first), Some(last)) if first >= 1 && last >= first => {
            if (last - first + 1) as u64 != receipt.evidence.decoded.line_count {
                return Err(invalid(
                    "canonical sequence range disagrees with line_count".to_owned(),
                ));
            }
        }
        _ => return Err(invalid("incoherent canonical sequence range".to_owned())),
    }
    if receipt.evidence.decoded.line_count != receipt.provenance.decoded.line_count {
        return Err(invalid(
            "evidence and provenance line counts disagree".to_owned(),
        ));
    }
    for input in &receipt.inputs {
        match (input.first_delivery_index, input.last_delivery_index) {
            (None, None) if input.line_count == 0 => {}
            (Some(first), Some(last)) if first <= last => {
                let span = last
                    .checked_sub(first)
                    .and_then(|distance| distance.checked_add(1))
                    .ok_or_else(|| invalid("input delivery range overflows".to_owned()))?;
                if span != input.line_count {
                    return Err(invalid(format!(
                        "input {} delivery bounds disagree with line_count",
                        input.data_file
                    )));
                }
            }
            _ => {
                return Err(invalid(format!(
                    "input {} has incoherent delivery bounds",
                    input.data_file
                )));
            }
        }
    }
    for (output, expected_file) in [
        (&receipt.evidence, EVIDENCE_FILE),
        (&receipt.provenance, PROVENANCE_FILE),
    ] {
        if output.file != expected_file {
            return Err(invalid(format!(
                "{} is not the canonical output {expected_file}",
                output.file
            )));
        }
        if output.content_encoding != "zstd"
            || output.compression.algorithm != "zstd"
            || output.compression.level != DEFAULT_ZSTD_LEVEL
            || !output.compression.frame_checksum
            || output.compression.dictionary.is_some()
            || output.compression.frame_count != 1
            || output.compression.encoder.is_empty()
        {
            return Err(invalid(format!(
                "{} does not declare the canonical V1 Zstandard contract",
                output.file
            )));
        }
        for (label, digest) in [
            ("decoded", output.decoded.sha256.as_str()),
            ("stored", output.stored.sha256.as_str()),
        ] {
            if !is_lower_sha256(digest) {
                return Err(invalid(format!(
                    "{} {label} sha256 is not 64 lowercase hexadecimal characters",
                    output.file
                )));
            }
        }
        let object = directory.join(&output.file);
        let metadata = std::fs::symlink_metadata(&object)
            .map_err(|error| invalid(format!("{}: {error}", output.file)))?;
        if !metadata.file_type().is_file() {
            return Err(invalid(format!("{} is not a regular file", output.file)));
        }
        if metadata.len() != output.stored.byte_length {
            return Err(invalid(format!(
                "{} is {} bytes, recorded as {}",
                output.file,
                metadata.len(),
                output.stored.byte_length
            )));
        }
    }
    Ok(Some(receipt))
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Every window that already carries a verified receipt.
///
/// **Fails closed** at every step, including directory traversal: an unreadable
/// entry used to be skipped, which silently reclassified a committed window as
/// open — so the next run would re-finalize it, renumber its positions and
/// overwrite bytes a reader may already hold.
pub fn committed_windows(canonical_root: &Path) -> Result<BTreeMap<u64, Receipt>, String> {
    let mut found = BTreeMap::new();
    if !canonical_root.is_dir() {
        return Ok(found);
    }
    let dates = std::fs::read_dir(canonical_root)
        .map_err(|error| format!("reading {}: {error}", canonical_root.display()))?;
    for date in dates {
        let date = date.map_err(|error| format!("reading canonical root: {error}"))?;
        if !date.path().is_dir() {
            continue;
        }
        let windows = std::fs::read_dir(date.path())
            .map_err(|error| format!("reading {}: {error}", date.path().display()))?;
        for entry in windows {
            let entry =
                entry.map_err(|error| format!("reading {}: {error}", date.path().display()))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(start) = name
                .strip_prefix("window=")
                .and_then(|v| v.parse::<u64>().ok())
            else {
                continue;
            };
            if let Some(receipt) = read_receipt(canonical_root, start)? {
                found.insert(start, receipt);
            }
        }
    }
    Ok(found)
}

/// Segments that arrived for a window after its receipt was committed.
///
/// §5: such a seal "is archived as raw evidence and labelled
/// `late_after_finalization`. It must not be inserted into the existing
/// canonical file or renumber already committed `EvidenceSeq`." Reported, never
/// merged.
pub fn late_segments(window: &Window, receipt: &Receipt) -> Vec<String> {
    let committed: std::collections::BTreeSet<(&str, &str, u64, &str)> = receipt
        .inputs
        .iter()
        .map(|input| {
            (
                input.lane.as_str(),
                input.data_file.as_str(),
                input.segment_index,
                input.sha256.as_str(),
            )
        })
        .collect();
    window
        .lanes
        .values()
        .flat_map(|entry| entry.segments.iter())
        .filter(|(_, seal)| {
            !committed.contains(&(
                seal.lane_id.as_str(),
                seal.data_file.as_str(),
                seal.segment_index,
                seal.sha256.as_str(),
            ))
        })
        .map(|(_, seal)| format!("{}/{}", seal.lane_id, seal.data_file))
        .collect()
}
