//! Reading one lane's records out of its sealed segments.
//!
//! A lane contributes its segments to a window in `(window_start_ns,
//! segment_index)` order and they concatenate: a splice writes one lane in
//! receive order, and `segment_index` exists precisely because a restart inside
//! a window opens a second segment for it. So a lane's stream is a flat
//! concatenation and needs no ordering of its own.
//!
//! Every line is parsed with the full envelope schema, not scanned for two
//! fields. §5 requires closed-schema validation before a window is finalized,
//! and the parser is the only thing that enforces it — an unknown field or a
//! mixed-version shape rejects the line rather than being quietly ignored.
//! Whatever is rejected here makes the lane invalid for the window; it does not
//! silently drop out of canonical evidence.
//!
//! ## The seal is a claim, and this is where it is falsified
//!
//! A seal's digest proves the *bytes* have not changed. It proves nothing about
//! whether the seal's summary of those bytes is true: `first_visible_ns`,
//! `last_delivery_index`, `delivery_index_dense` and the window bounds are
//! assertions the writer made, and a segment whose records sit outside the
//! window it declares hashes exactly as well as one whose records do not.
//!
//! Left unchecked that was a hole big enough to commit an out-of-window record
//! into a `complete`, `certified` window. So every record is reconciled against
//! the seal and the window **while it streams** — the only place the actual
//! values are in hand — and a disagreement faults the lane rather than being
//! carried into canonical evidence.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use indexer_types::{ContentHash, EnvelopeView};

use crate::merge::{LaneRecord, SourceRef};

/// What a seal asserts about the records inside its segment.
///
/// Checked against the records themselves as they stream. Every field here is
/// something the writer *said*; none of it is established by the digest.
#[derive(Clone, Debug, Default)]
pub struct SegmentClaims {
    pub window_start_ns: u64,
    pub window_end_ns: u64,
    pub line_count: u64,
    pub first_visible_ns: Option<u64>,
    pub last_visible_ns: Option<u64>,
    pub first_delivery_index: Option<u64>,
    pub last_delivery_index: Option<u64>,
    pub delivery_index_dense: bool,
    pub visible_non_decreasing: bool,
}

impl SegmentClaims {
    pub fn from_seal(seal: &indexer_segment::SegmentSeal) -> Self {
        Self {
            window_start_ns: seal.window_start_ns,
            window_end_ns: seal.window_end_ns,
            line_count: seal.line_count,
            first_visible_ns: seal.first_visible_ns,
            last_visible_ns: seal.last_visible_ns,
            first_delivery_index: seal.first_delivery_index,
            last_delivery_index: seal.last_delivery_index,
            delivery_index_dense: seal.delivery_index_dense,
            visible_non_decreasing: seal.visible_non_decreasing,
        }
    }
}

/// One sealed segment a lane contributes to a window.
#[derive(Clone, Debug)]
pub struct SegmentInput {
    pub path: PathBuf,
    /// Index into the caller's segment table, carried on every record's
    /// `SourceRef` so provenance can name the source digest without repeating it.
    pub segment: u32,
    /// What the seal asserts, to be checked against the records.
    ///
    /// `None` means there is no seal to falsify — fixtures and direct callers.
    /// Deliberately an `Option` rather than a permissive default, so a
    /// production path cannot end up asserting nothing by forgetting a field.
    pub claims: Option<SegmentClaims>,
}

/// Streams one lane's records across its segments, in order.
pub struct LaneReader {
    lane: u16,
    segments: std::vec::IntoIter<SegmentInput>,
    current: Option<(SegmentInput, BufReader<std::fs::File>)>,
    line_number: u64,
    finished: bool,
    /// The running reconciliation of the open segment against its seal.
    seen: SegmentTally,
}

/// What the records of one segment actually turned out to be.
#[derive(Default)]
struct SegmentTally {
    first_visible_ns: Option<u64>,
    last_visible_ns: Option<u64>,
    first_delivery_index: Option<u64>,
    last_delivery_index: Option<u64>,
    count: u64,
}

impl LaneReader {
    pub fn new(lane: u16, segments: Vec<SegmentInput>) -> Self {
        Self {
            lane,
            segments: segments.into_iter(),
            current: None,
            line_number: 0,
            finished: false,
            seen: SegmentTally::default(),
        }
    }

    fn open_next(&mut self) -> Result<bool, String> {
        let Some(segment) = self.segments.next() else {
            return Ok(false);
        };
        let file = std::fs::File::open(&segment.path)
            .map_err(|error| format!("opening {}: {error}", segment.path.display()))?;
        self.line_number = 0;
        self.seen = SegmentTally::default();
        self.current = Some((segment, BufReader::new(file)));
        Ok(true)
    }

    /// Reconciles what a segment actually held against what its seal claimed.
    ///
    /// Run when the segment's last line has been read, because the closing
    /// claims — `last_visible_ns`, `last_delivery_index`, `line_count` — are
    /// only falsifiable once there is nothing left.
    fn close_segment(segment: &SegmentInput, seen: &SegmentTally) -> Result<(), String> {
        let Some(claims) = &segment.claims else {
            return Ok(());
        };
        let name = segment
            .path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        let disagree = |detail: String| format!("{name}: {detail}");

        if seen.count != claims.line_count {
            return Err(disagree(format!(
                "holds {} records, sealed as {}",
                seen.count, claims.line_count
            )));
        }
        for (label, claimed, actual) in [
            (
                "first_visible_ns",
                claims.first_visible_ns,
                seen.first_visible_ns,
            ),
            (
                "last_visible_ns",
                claims.last_visible_ns,
                seen.last_visible_ns,
            ),
            (
                "first_delivery_index",
                claims.first_delivery_index,
                seen.first_delivery_index,
            ),
            (
                "last_delivery_index",
                claims.last_delivery_index,
                seen.last_delivery_index,
            ),
        ] {
            if claimed != actual {
                return Err(disagree(format!(
                    "seal claims {label} {claimed:?}, records say {actual:?}"
                )));
            }
        }
        Ok(())
    }

    fn read_line(&mut self) -> Result<Option<LaneRecord>, String> {
        loop {
            let Some((segment, reader)) = self.current.as_mut() else {
                if self.open_next()? {
                    continue;
                }
                return Ok(None);
            };

            let mut line = Vec::new();
            let read = reader
                .read_until(b'\n', &mut line)
                .map_err(|error| format!("reading {}: {error}", segment.path.display()))?;
            if read == 0 {
                Self::close_segment(segment, &self.seen)?;
                self.current = None;
                continue;
            }
            self.line_number += 1;
            // A sealed segment ends in a newline — `validate_sealed_segment`
            // refuses one that does not — so a line without one here means the
            // file changed after validation.
            if !line.ends_with(b"\n") {
                return Err(format!(
                    "{}:{}: sealed segment ends without a newline",
                    segment.path.display(),
                    self.line_number
                ));
            }

            let position = format!(
                "{}:{}",
                segment
                    .path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy(),
                self.line_number
            );
            let (visible_ns, delivery_index, record_id, content_hash) = {
                let view =
                    EnvelopeView::parse(&line).map_err(|error| format!("{position}: {error}"))?;
                (
                    view.visible_ns.ns(),
                    view.delivery_index,
                    view.record_id.as_str().to_owned(),
                    ContentHash::hash(view.raw_payload.as_bytes()).to_hex(),
                )
            };
            // The record against the window it claims to belong to. A seal can
            // say anything; this is the value that was actually captured.
            if let Some(claims) = &segment.claims {
                if visible_ns < claims.window_start_ns || visible_ns >= claims.window_end_ns {
                    return Err(format!(
                        "{position}: visible_ns {visible_ns} is outside the sealed window \
                     {}..{}",
                        claims.window_start_ns, claims.window_end_ns
                    ));
                }
                if claims.visible_non_decreasing {
                    if let Some(previous) = self.seen.last_visible_ns {
                        if visible_ns < previous {
                            return Err(format!(
                                "{position}: visible_ns {visible_ns} follows {previous}, but the seal \
                                 declares visible_non_decreasing"
                            ));
                        }
                    }
                }
                if claims.delivery_index_dense {
                    if let Some(previous) = self.seen.last_delivery_index {
                        if delivery_index != previous + 1 {
                            return Err(format!(
                                "{position}: delivery_index {delivery_index} does not follow {previous}, \
                                 but the seal declares delivery_index_dense"
                            ));
                        }
                    }
                }
                if self.seen.count + 1 > claims.line_count {
                    return Err(format!(
                        "{position}: more records than the seal's line_count of {}",
                        claims.line_count
                    ));
                }
            }
            if self.seen.count == 0 {
                self.seen.first_visible_ns = Some(visible_ns);
                self.seen.first_delivery_index = Some(delivery_index);
            }
            self.seen.last_visible_ns = Some(visible_ns);
            self.seen.last_delivery_index = Some(delivery_index);
            self.seen.count += 1;

            return Ok(Some(LaneRecord {
                visible_ns,
                delivery_index,
                source: SourceRef {
                    lane: self.lane,
                    segment: segment.segment,
                    line_number: self.line_number,
                },
                record_id,
                content_hash,
                line,
            }));
        }
    }
}

impl Iterator for LaneReader {
    type Item = Result<LaneRecord, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.read_line() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

/// Convenience for callers that already have paths rather than a segment table.
pub fn segment_inputs(paths: &[PathBuf]) -> Vec<SegmentInput> {
    paths
        .iter()
        .enumerate()
        .map(|(index, path)| SegmentInput {
            path: path.clone(),
            segment: index as u32,
            claims: None,
        })
        .collect()
}

/// Resolves a path to the lane partition that wrote it, mirroring
/// `replay/lanes.py::lane_of`.
pub fn lane_of(path: &Path) -> Option<String> {
    path.components()
        .filter_map(|part| part.as_os_str().to_str())
        .find_map(|part| part.strip_prefix("lane=").filter(|lane| !lane.is_empty()))
        .map(str::to_owned)
}
