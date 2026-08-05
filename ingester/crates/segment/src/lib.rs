//! Sealed segments: discovery, seal decoding, and validation.
//!
//! A capture lane writes UTC-aligned segments and commits each one with a
//! checksummed sidecar. **The sidecar is the commit marker, not the `.ndjson`
//! suffix** — a renamed data file with no seal is a crash between two steps of
//! `docs/SEALED_CAPTURE_PIPELINE_V1.md` §3, and the writer's own recovery closes
//! that window. Until it does, the bytes are not evidence.
//!
//! This crate exists because two binaries need the same answers and must not
//! disagree about them. `indexer-ingest` consumes segments in filename order;
//! `indexer-finalize` merges them on `visible_ns`. Both have to decide what a
//! segment *is*, whether its seal is honest, and whether the bytes on disk are
//! still the bytes that were sealed. A second copy of that logic is how the two
//! would come to hold different opinions about the same file.
//!
//! Nothing here interprets a venue payload or assigns an order. It answers only:
//! which files are committed, and do they still say what they claimed.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

/// What a sealed segment asserts about itself.
///
/// Mirrors `splices.common.segment.Seal`. Every field is decoded even where this
/// crate has no use for it, so a truncated or wrong-version seal fails closed
/// rather than being read as a valid seal that happens to be missing fields.
#[derive(Clone, Debug, Deserialize)]
pub struct SegmentSeal {
    pub seal_version: u64,
    pub lane_id: String,
    pub window_start_ns: u64,
    pub window_end_ns: u64,
    pub data_file: String,
    pub byte_length: u64,
    pub line_count: u64,
    pub sha256: String,
    pub first_delivery_index: Option<u64>,
    pub last_delivery_index: Option<u64>,
    pub first_visible_ns: Option<u64>,
    pub last_visible_ns: Option<u64>,
    pub visible_non_decreasing: bool,
    pub delivery_index_dense: bool,
    pub segment_id: String,
    pub segment_index: u64,
    pub seal_reason: String,
    pub ordering_status: String,
    pub epochs: Vec<String>,
    pub repaired_bytes: u64,
    pub created_ns: u64,
    pub writer_version: u64,
}

/// The sidecar path for a segment, or `None` if the path is not a segment.
pub fn seal_path_for(data_path: &Path) -> Option<PathBuf> {
    let filename = data_path.file_name()?.to_str()?;
    let stem = filename.strip_suffix(".ndjson")?;
    Some(data_path.with_file_name(format!("{stem}.seal.json")))
}

/// Decodes a segment's seal without touching the data file.
///
/// Deliberately cheap: the finalizer groups hundreds of segments into windows
/// and decides which ones a run even needs before committing to hashing any of
/// them. A gigabyte-per-window digest is not something to spend on a segment
/// that belongs to a window this run will not finalize.
///
/// Decoding is *not* validation. Use [`validate_sealed_segment`] before treating
/// a segment's bytes as evidence.
pub fn read_seal(data_path: &Path) -> Result<SegmentSeal, String> {
    let seal_path = seal_path_for(data_path)
        .ok_or_else(|| format!("not a segment path: {}", data_path.display()))?;
    let encoded = std::fs::read(&seal_path)
        .map_err(|error| format!("reading seal {}: {error}", seal_path.display()))?;
    serde_json::from_slice(&encoded)
        .map_err(|error| format!("invalid seal {}: {error}", seal_path.display()))
}

/// Proves a sealed segment still says what it claimed, and returns its seal.
///
/// Every check that can falsify the seal against the bytes on disk: declared
/// length, recomputed SHA-256, line count, and the trailing newline that makes
/// the last record durable. Plus the internal consistency the writer promises —
/// an empty segment carries no record bounds, a non-empty one carries all of
/// them and at least one connection epoch, and `ordering_status` agrees with
/// `visible_non_decreasing` rather than being a free-text field.
///
/// Hashing is why callers cache the outcome. A segment is immutable once its
/// seal appears, so validating immediately before first use is sufficient and
/// re-validating on every poll would turn this into an unbounded disk scan.
pub fn validate_sealed_segment(lane: &str, data_path: &Path) -> Result<SegmentSeal, String> {
    let seal_path = seal_path_for(data_path)
        .ok_or_else(|| format!("not a segment path: {}", data_path.display()))?;
    let encoded = std::fs::read(&seal_path)
        .map_err(|error| format!("reading seal {}: {error}", seal_path.display()))?;
    let seal: SegmentSeal = serde_json::from_slice(&encoded)
        .map_err(|error| format!("invalid seal {}: {error}", seal_path.display()))?;
    let filename = data_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("non-UTF-8 segment filename: {}", data_path.display()))?;

    if seal.seal_version != 1 {
        return Err(format!(
            "invalid seal {}: unsupported seal_version {}",
            seal_path.display(),
            seal.seal_version
        ));
    }
    if seal.lane_id != lane {
        return Err(format!(
            "invalid seal {}: lane_id {:?} does not match lane {:?}",
            seal_path.display(),
            seal.lane_id,
            lane
        ));
    }
    if seal.data_file != filename {
        return Err(format!(
            "invalid seal {}: data_file {:?} does not name {:?}",
            seal_path.display(),
            seal.data_file,
            filename
        ));
    }
    if seal.window_start_ns >= seal.window_end_ns {
        return Err(format!(
            "invalid seal {}: window_start_ns must precede window_end_ns",
            seal_path.display()
        ));
    }
    if seal.segment_id.is_empty() || seal.seal_reason.is_empty() || seal.writer_version == 0 {
        return Err(format!(
            "invalid seal {}: segment_id, seal_reason and writer_version are required",
            seal_path.display()
        ));
    }
    if seal.ordering_status
        != if seal.visible_non_decreasing {
            "ok"
        } else {
            "visible_clock_regression"
        }
    {
        return Err(format!(
            "invalid seal {}: ordering_status disagrees with visible_non_decreasing",
            seal_path.display()
        ));
    }

    let metadata = std::fs::metadata(data_path)
        .map_err(|error| format!("reading metadata for {}: {error}", data_path.display()))?;
    if metadata.len() != seal.byte_length {
        return Err(format!(
            "invalid seal {}: byte_length {} does not match data length {}",
            seal_path.display(),
            seal.byte_length,
            metadata.len()
        ));
    }

    let mut data = std::fs::File::open(data_path)
        .map_err(|error| format!("opening sealed data {}: {error}", data_path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut line_count = 0u64;
    let mut last_byte = None;
    loop {
        let read = std::io::Read::read(&mut data, &mut buffer)
            .map_err(|error| format!("hashing sealed data {}: {error}", data_path.display()))?;
        if read == 0 {
            break;
        }
        let bytes = &buffer[..read];
        hasher.update(bytes);
        line_count += bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
        last_byte = bytes.last().copied();
    }
    let actual_sha256 = format!("{:x}", hasher.finalize());
    if actual_sha256 != seal.sha256 {
        return Err(format!(
            "invalid seal {}: sha256 {} does not match data sha256 {}",
            seal_path.display(),
            seal.sha256,
            actual_sha256
        ));
    }
    if line_count != seal.line_count {
        return Err(format!(
            "invalid seal {}: line_count {} does not match data line count {}",
            seal_path.display(),
            seal.line_count,
            line_count
        ));
    }
    if seal.byte_length > 0 && last_byte != Some(b'\n') {
        return Err(format!(
            "invalid seal {}: non-empty data does not end in a newline",
            seal_path.display()
        ));
    }

    let empty_bounds = seal.first_delivery_index.is_none()
        && seal.last_delivery_index.is_none()
        && seal.first_visible_ns.is_none()
        && seal.last_visible_ns.is_none();
    if seal.line_count == 0 {
        if seal.byte_length != 0 || !empty_bounds || !seal.epochs.is_empty() {
            return Err(format!(
                "invalid seal {}: empty segment carries record bounds",
                seal_path.display()
            ));
        }
    } else {
        let (Some(first_delivery), Some(last_delivery), Some(_), Some(_)) = (
            seal.first_delivery_index,
            seal.last_delivery_index,
            seal.first_visible_ns,
            seal.last_visible_ns,
        ) else {
            return Err(format!(
                "invalid seal {}: non-empty segment is missing record bounds",
                seal_path.display()
            ));
        };
        if seal.epochs.is_empty() {
            return Err(format!(
                "invalid seal {}: non-empty segment has no connection epochs",
                seal_path.display()
            ));
        }
        if first_delivery > last_delivery {
            return Err(format!(
                "invalid seal {}: delivery bounds are inverted",
                seal_path.display()
            ));
        }
        if seal.delivery_index_dense && last_delivery - first_delivery + 1 != seal.line_count {
            return Err(format!(
                "invalid seal {}: dense delivery bounds disagree with line_count",
                seal_path.display()
            ));
        }
        // A segment holds the records of its own window and no others. Without
        // this a seal can name a window its records never belonged to, and the
        // finalizer would merge them into it — the window bounds would be
        // fiction while every digest still checked out.
        if let (Some(first_visible), Some(last_visible)) =
            (seal.first_visible_ns, seal.last_visible_ns)
        {
            if first_visible < seal.window_start_ns || last_visible >= seal.window_end_ns {
                return Err(format!(
                    "invalid seal {}: records span {first_visible}..={last_visible}, outside the \
                     declared window {}..{}",
                    seal_path.display(),
                    seal.window_start_ns,
                    seal.window_end_ns
                ));
            }
        }
    }

    Ok(seal)
}

/// Every committed segment under a spool root, with the lane that wrote it.
///
/// Walks `lane=<lane>/date=<YYYY-MM-DD>/<segment>.ndjson`, taking only files
/// whose sidecar exists. The `lane=` partition names the *capture process*, not
/// the venue: Polymarket runs a market channel, a snapshot poller, a sports feed
/// and a price feed, and every record from all four says `venue: polymarket` in
/// its envelope.
///
/// The returned order is deterministic — by filename, then by full path — but it
/// carries no meaning on its own. What a caller makes of it is the caller's
/// claim: `indexer-ingest` treats it as `file_order`, and the finalizer discards
/// it in favour of the window bounds in each seal.
pub fn discover_segments(root: &Path) -> std::io::Result<Vec<(String, PathBuf)>> {
    let mut found = Vec::new();
    if !root.exists() {
        return Ok(found);
    }
    for lane_entry in read_sorted(root)? {
        if !lane_entry.is_dir() {
            continue;
        }
        let directory = lane_entry
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let Some(lane) = directory.strip_prefix("lane=") else {
            continue;
        };
        for date_entry in read_sorted(&lane_entry)? {
            if !date_entry.is_dir() {
                continue;
            }
            for file in read_sorted(&date_entry)? {
                if file
                    .extension()
                    .is_some_and(|extension| extension == "ndjson")
                    && seal_path_for(&file).is_some_and(|sidecar| sidecar.is_file())
                {
                    found.push((lane.to_owned(), file));
                }
            }
        }
    }
    found.sort_by(|left, right| {
        left.1
            .file_name()
            .cmp(&right.1.file_name())
            .then_with(|| left.1.cmp(&right.1))
    });
    Ok(found)
}

fn read_sorted(directory: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();
    entries.sort();
    Ok(entries)
}
