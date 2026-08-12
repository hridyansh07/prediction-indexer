//! The durable watermark: §7's record of how far finalization has certified.
//!
//! **A derived index over receipts, not a second authority.** The same
//! relationship a seal has to the tape: it can be deleted and rebuilt, it is
//! checked against the newest receipt on load, and where they disagree the
//! receipts win. That is what lets it be fast without being trusted — it turns
//! "where do I resume, what is the next position, which windows are committed"
//! from a scan over the whole retention period into three field reads, while
//! last round's fail-closed receipt verification keeps its meaning.
//!
//! Everything it carries is also written into the receipt of the window it
//! describes, so a rebuild reproduces it exactly rather than approximately.
//!
//! ## The watermark advances past a bad window
//!
//! §7 asks for the visible-time boundary to be checked before the watermark
//! advances, and for a regression to stop the *certified* watermark. Read
//! strictly that halts all canonical output for every healthy lane over a host
//! clock step, which R4 already settled against: one fault must not stop the
//! pipeline. So a window that cannot be certified commits, carries
//! `certified: false`, and the watermark moves past it — the fault is confined
//! to the window it appeared in rather than freezing everything behind it.
//!
//! The boundary check itself lives in [`clock_faults`]. It cannot fire through
//! the binary, and the reason is worth stating exactly, because an earlier
//! version of this comment claimed the same thing while it was false: the
//! *reader* checks every record's `visible_ns` against its window as the record
//! streams. A seal's claim about its own bounds is not what establishes this —
//! a seal can say anything and still hash correctly, and while only the claim
//! was checked, an out-of-window record committed into a `certified` window.
//!
//! With the records themselves checked, window N holds only instants below
//! `N.end` and N+1 only instants at or above `N+1.start`, which are the same
//! instant. §7's invariant is therefore enforced upstream and more strictly than
//! a flag: a lane whose clock stepped back is `lane_invalid` and never enters
//! the merge. The comparison is kept as one per lane per window against that
//! rule being relaxed, and its unit tests construct the state directly.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::canonical::{FinalizedWindow, Receipt, committed_windows, write_json_durable};
use crate::continuity::OrderingState;

pub const WATERMARK_VERSION: u64 = 1;
pub const WATERMARK_FILE: &str = "watermark.json";

/// A window whose clock disagreed with the one before it.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ClockFault {
    pub window_start_ns: u64,
    pub lane: String,
    pub previous_visible_ns: u64,
    pub observed_visible_ns: u64,
}

/// What one window hands to the next.
///
/// Ordering history so verdicts continue rather than restart, and each lane's
/// last receive time so the boundary can be checked. Both are recorded in the
/// receipt, which is why a deleted watermark rebuilds exactly.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Carried {
    pub ordering: OrderingState,
    pub lane_visible_ns: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Watermark {
    pub watermark_version: u64,
    pub last_window_start_ns: u64,
    pub last_window_end_ns: u64,
    pub completeness: String,
    pub certified: bool,
    /// The highest canonical position assigned **anywhere so far**, not the last
    /// window's own last position.
    ///
    /// An empty window assigns none, so taking the newest receipt's value would
    /// reset the sequence to 1 and hand already-committed positions out a second
    /// time. A window with no records leaves the count where it was.
    pub last_canonical_seq: Option<i64>,
    /// §7's "canonical hashes" for the window this watermark stands at.
    pub evidence_sha256: String,
    pub provenance_sha256: String,
    /// §7's "all present source segment hashes".
    pub source_segment_sha256: Vec<String>,
    pub carried: Carried,
    /// Every window whose visible-time boundary regressed, oldest first.
    pub quarantined: Vec<ClockFault>,
}

impl Watermark {
    /// The watermark a freshly committed window leaves behind.
    pub fn advance(previous: Option<&Watermark>, window: &FinalizedWindow) -> Self {
        let mut quarantined = previous
            .map(|mark| mark.quarantined.clone())
            .unwrap_or_default();
        quarantined.extend(window.receipt.clock_faults.iter().cloned());
        let highest = window
            .receipt
            .last_canonical_seq
            .max(previous.and_then(|mark| mark.last_canonical_seq));
        Self::from_receipt(&window.receipt, quarantined, highest)
    }

    fn from_receipt(
        receipt: &Receipt,
        quarantined: Vec<ClockFault>,
        last_canonical_seq: Option<i64>,
    ) -> Self {
        Self {
            watermark_version: WATERMARK_VERSION,
            last_window_start_ns: receipt.window_start_ns,
            last_window_end_ns: receipt.window_end_ns,
            completeness: receipt.completeness.clone(),
            certified: receipt.certified,
            last_canonical_seq,
            evidence_sha256: receipt.evidence.decoded.sha256.clone(),
            provenance_sha256: receipt.provenance.decoded.sha256.clone(),
            source_segment_sha256: receipt
                .inputs
                .iter()
                .map(|input| input.sha256.clone())
                .collect(),
            carried: receipt.carried.clone(),
            quarantined,
        }
    }

    /// The next canonical position, continuing the global sequence.
    pub fn next_canonical_seq(&self) -> i64 {
        self.last_canonical_seq.unwrap_or(0) + 1
    }
}

pub fn watermark_path(canonical_root: &Path) -> PathBuf {
    canonical_root.join(WATERMARK_FILE)
}

pub fn write(canonical_root: &Path, watermark: &Watermark) -> Result<(), String> {
    write_json_durable(canonical_root, WATERMARK_FILE, watermark)
}

/// Loads the watermark, checking it against the newest receipt.
///
/// Returns `None` when there is nothing committed. Where the file is missing,
/// unreadable, or disagrees with the receipts, it is **rebuilt** rather than
/// trusted or refused — it is derived state, and the receipts it derives from
/// are already verified.
pub fn load_or_rebuild(canonical_root: &Path) -> Result<Option<Watermark>, String> {
    let committed = committed_windows(canonical_root)?;
    let Some((_, newest)) = committed.iter().next_back() else {
        return Ok(None);
    };

    let mut quarantined: Vec<ClockFault> = committed
        .values()
        .flat_map(|receipt| receipt.clock_faults.iter().cloned())
        .collect();
    quarantined.sort_by_key(|fault| (fault.window_start_ns, fault.lane.clone()));
    // Folded across every receipt, not read off the newest: the newest window may
    // be an empty one, which assigns no position at all.
    let highest = committed
        .values()
        .filter_map(|receipt| receipt.last_canonical_seq)
        .max();

    // The receipts must form one unbroken chain in window order. A rebuild that
    // accepted a gap or an overlap would hand back a watermark implying the
    // sequence is dense when it is not — and the sequence is the one thing every
    // reader indexes canonical evidence by.
    let mut previous: Option<(u64, i64)> = None;
    for (start, receipt) in &committed {
        let (Some(first), Some(last)) = (receipt.first_canonical_seq, receipt.last_canonical_seq)
        else {
            continue;
        };
        if let Some((previous_start, previous_last)) = previous {
            if first != previous_last + 1 {
                return Err(format!(
                    "canonical sequence is not a chain: window {previous_start} ends at \
                     {previous_last} and window {start} begins at {first}"
                ));
            }
        }
        previous = Some((*start, last));
    }

    let rebuilt = Watermark::from_receipt(newest, quarantined, highest);

    let path = watermark_path(canonical_root);
    let stored: Option<Watermark> = match std::fs::read(&path) {
        Ok(encoded) => serde_json::from_slice(&encoded).ok(),
        Err(_) => None,
    };
    match stored {
        Some(mark) if mark == rebuilt => Ok(Some(mark)),
        // Either absent, or describing a state the receipts do not agree with.
        // The receipts are the authority, so the file is replaced.
        _ => {
            write(canonical_root, &rebuilt)?;
            Ok(Some(rebuilt))
        }
    }
}

/// The boundary check: a lane's first receive time against its last in the
/// previous window.
///
/// A durable filesystem does not itself make time walk backwards (§2), so a
/// regression here is a real host-clock event and is recorded as one rather
/// than sorted away.
///
/// **Unreachable through the binary as it stands, and deliberately kept.**
/// `validate_sealed_segment` refuses a segment whose records fall outside the
/// window it declares, so window N holds only times below `N.end` and window
/// N+1 only times at or above `N+1.start` — which are the same instant. Windows
/// are then finalized in ascending order, so the comparison below cannot fail.
/// §7's invariant is therefore already enforced, and by something stronger than
/// this: a lane whose clock stepped back is rejected at seal validation as
/// `lane_invalid` rather than merged and flagged afterwards.
///
/// It stays because it is one comparison per lane per window and it is the
/// thing that notices if that upstream rule is ever relaxed. The unit test
/// below constructs the state directly, since the binary cannot produce it.
pub fn clock_faults(
    window_start_ns: u64,
    carried: &Carried,
    first_by_lane: &BTreeMap<String, u64>,
) -> Vec<ClockFault> {
    let mut faults: Vec<ClockFault> = first_by_lane
        .iter()
        .filter_map(|(lane, first)| {
            let previous = *carried.lane_visible_ns.get(lane)?;
            (*first < previous).then(|| ClockFault {
                window_start_ns,
                lane: lane.clone(),
                previous_visible_ns: previous,
                observed_visible_ns: *first,
            })
        })
        .collect();
    faults.sort_by(|left, right| left.lane.cmp(&right.lane));
    faults
}

#[cfg(test)]
mod tests {
    use super::*;

    fn carried(lane: &str, last: u64) -> Carried {
        Carried {
            ordering: OrderingState::default(),
            lane_visible_ns: [(lane.to_owned(), last)].into_iter().collect(),
        }
    }

    #[test]
    fn an_empty_window_does_not_reset_the_sequence() {
        // Taking the newest receipt's own `last_canonical_seq` would hand
        // position 1 out again after a quiet window, colliding with everything
        // already committed.
        let mut receipt = fixture_receipt();
        receipt.last_canonical_seq = None;
        receipt.first_canonical_seq = None;
        let previous = Watermark::from_receipt(&fixture_receipt(), Vec::new(), Some(16));
        let window = FinalizedWindow {
            receipt,
            directory: PathBuf::new(),
            identity_records_in_memory: 0,
        };

        let advanced = Watermark::advance(Some(&previous), &window);
        assert_eq!(advanced.last_canonical_seq, Some(16));
        assert_eq!(advanced.next_canonical_seq(), 17);
    }

    fn fixture_receipt() -> Receipt {
        Receipt {
            receipt_version: 1,
            window_start_ns: 0,
            window_end_ns: 1_800_000_000_000,
            completeness: "complete".to_owned(),
            certified: true,
            expected_lanes: Vec::new(),
            present_lanes: Vec::new(),
            unexpected_lanes: Vec::new(),
            missing_lanes: Vec::new(),
            invalid_lanes: Vec::new(),
            finalization_deadline_seconds: 0,
            deadline_expired: false,
            finalized_at_ns: 0,
            inputs: Vec::new(),
            evidence: fixture_output("evidence.ndjson.zst"),
            provenance: fixture_output("provenance.ndjson.zst"),
            first_canonical_seq: Some(1),
            last_canonical_seq: Some(16),
            carried: Carried::default(),
            clock_faults: Vec::new(),
            finalizer_version: 1,
        }
    }

    fn fixture_output(file: &str) -> crate::canonical::CanonicalOutput {
        crate::canonical::CanonicalOutput {
            file: file.to_owned(),
            content_encoding: "zstd".to_owned(),
            decoded: crate::canonical::DecodedIdentity {
                byte_length: 0,
                line_count: 0,
                sha256: "0".repeat(64),
            },
            stored: crate::canonical::StoredIdentity {
                byte_length: 0,
                sha256: "0".repeat(64),
            },
            compression: crate::canonical::CompressionContract {
                algorithm: "zstd".to_owned(),
                level: 3,
                frame_checksum: true,
                dictionary: None,
                frame_count: 1,
                encoder: "fixture".to_owned(),
            },
        }
    }

    #[test]
    fn a_lane_starting_below_where_it_ended_is_a_fault() {
        let faults = clock_faults(
            1_800_000_000_000,
            &carried("kalshi", 500),
            &[("kalshi".to_owned(), 400u64)].into_iter().collect(),
        );
        assert_eq!(
            faults,
            vec![ClockFault {
                window_start_ns: 1_800_000_000_000,
                lane: "kalshi".to_owned(),
                previous_visible_ns: 500,
                observed_visible_ns: 400,
            }]
        );
    }

    #[test]
    fn continuing_forward_is_not_a_fault() {
        assert!(
            clock_faults(
                1_800_000_000_000,
                &carried("kalshi", 500),
                &[("kalshi".to_owned(), 500u64)].into_iter().collect(),
            )
            .is_empty(),
            "equal instants are not a regression; the clock has resolution limits"
        );
    }

    #[test]
    fn a_lane_with_no_history_is_not_a_fault() {
        // Its first window, or its first appearance after the watermark was
        // rebuilt from a receipt that predates it.
        assert!(
            clock_faults(
                0,
                &Carried::default(),
                &[("kalshi".to_owned(), 400u64)].into_iter().collect(),
            )
            .is_empty()
        );
    }
}
