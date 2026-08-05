//! Grouping sealed segments into windows, and deciding when one may be finalized.
//!
//! ## Expected is not supported
//!
//! §3: "The deployment manifest defines which lanes are expected; a disabled
//! Kalshi profile is not waited on." The expected set is *supplied* — it is a
//! property of the deployment, not of this build — and it is recorded in every
//! receipt so a verdict can be read against the expectation that produced it.
//!
//! A lane can also be **present without being expected**: a profile enabled for
//! one run, or segments left behind by an earlier configuration. Those are
//! merged (their records are evidence like any other) and listed separately, so
//! they neither inflate completeness nor vanish.
//!
//! ## Waiting, then committing anyway
//!
//! A window is finalized once every expected lane has sealed, or once its
//! deadline expires — whichever comes first. The deadline is finite on purpose.
//! §5 and review finding R4 settled that one wedged splice must not halt
//! finalization for every healthy venue, so an expired deadline produces an
//! `incomplete` receipt naming what was missing rather than an indefinite stall.
//!
//! Windows are processed in ascending order and a run stops at the first one
//! still inside its deadline (§7). Skipping ahead would let a later window
//! commit positions that an earlier one still needs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use indexer_segment::{SegmentSeal, discover_segments, read_seal, validate_sealed_segment};

/// A UTC-aligned capture window, identified by its bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WindowKey {
    pub start_ns: u64,
    pub end_ns: u64,
}

/// One lane's contribution to one window.
#[derive(Clone, Debug)]
pub struct LaneSegments {
    pub lane: String,
    /// Sorted by `segment_index`: a restart inside a window opens a second
    /// segment, and they concatenate in the order they were opened.
    pub segments: Vec<(PathBuf, SegmentSeal)>,
    /// Set when a seal could not be read, or disagrees with its filename or the
    /// configured window period. The lane is still listed — a lane that failed
    /// validation is not the same as a lane that never arrived, and §5 gives
    /// them different verdicts.
    pub fault: Option<String>,
}

impl LaneSegments {
    /// Whether every segment declares a non-decreasing clock.
    ///
    /// §8 requires a decreasing `visible_ns` to prevent certification. The seal
    /// records it per segment as `ordering_status`, so this is a read rather
    /// than a rescan.
    pub fn clock_is_sound(&self) -> bool {
        self.segments
            .iter()
            .all(|(_, seal)| seal.visible_non_decreasing)
    }

    /// Whether the lane's `delivery_index` runs unbroken across its segments.
    ///
    /// Checked here, from seals alone, rather than left to the merge. The merge
    /// enforces the same invariant, but it discovers a break halfway through
    /// writing the window — too late to classify the lane and continue without
    /// it. The seals carry each segment's first and last index, so a missing
    /// segment is detectable before a byte is read.
    ///
    /// Empty segments are skipped: a quiet lane emits one by design (§3) and it
    /// carries no indices to chain.
    pub fn delivery_is_contiguous(&self) -> Result<(), String> {
        let mut previous_last: Option<u64> = None;
        for (path, seal) in &self.segments {
            let (Some(first), Some(last)) = (seal.first_delivery_index, seal.last_delivery_index)
            else {
                continue;
            };
            if !seal.delivery_index_dense {
                return Err(format!(
                    "{} declares delivery_index_dense false",
                    path.file_name().unwrap_or_default().to_string_lossy()
                ));
            }
            if let Some(previous) = previous_last {
                if first != previous + 1 {
                    return Err(format!(
                        "delivery_index jumps {previous} -> {first} at {}; a segment of this \
                         lane is missing from the window",
                        path.file_name().unwrap_or_default().to_string_lossy()
                    ));
                }
            }
            previous_last = Some(last);
        }
        Ok(())
    }

    pub fn line_count(&self) -> u64 {
        self.segments.iter().map(|(_, seal)| seal.line_count).sum()
    }

    pub fn first_delivery_index(&self) -> Option<u64> {
        self.segments
            .iter()
            .find_map(|(_, seal)| seal.first_delivery_index)
    }

    pub fn last_delivery_index(&self) -> Option<u64> {
        self.segments
            .iter()
            .rev()
            .find_map(|(_, seal)| seal.last_delivery_index)
    }

    pub fn first_visible_ns(&self) -> Option<u64> {
        self.segments
            .iter()
            .find_map(|(_, seal)| seal.first_visible_ns)
    }

    pub fn last_visible_ns(&self) -> Option<u64> {
        self.segments
            .iter()
            .rev()
            .find_map(|(_, seal)| seal.last_visible_ns)
    }
}

/// Every lane that put a segment in one window.
#[derive(Clone, Debug)]
pub struct Window {
    pub key: WindowKey,
    pub lanes: BTreeMap<String, LaneSegments>,
}

impl Window {
    /// Expected lanes with no segment at all — §5's `lane_missing`.
    ///
    /// Distinct from `lane_invalid`, which is a lane that arrived and failed
    /// validation. Both keep a window from being complete; §5 reports them
    /// separately because they mean different things about the deployment.
    pub fn missing<'a>(&self, expected: &'a [String]) -> Vec<&'a str> {
        expected
            .iter()
            .filter(|lane| !self.lanes.contains_key(lane.as_str()))
            .map(String::as_str)
            .collect()
    }

    /// Expected lanes with nothing mergeable — missing or invalid.
    ///
    /// This, not mere presence, is what the deadline waits on. §5 step 1 waits
    /// for "one **valid** seal from every expected lane"; counting a corrupt
    /// segment as arrival would declare the window complete the moment a broken
    /// seal appeared, skipping the wait that lets a good second segment land.
    pub fn unsatisfied<'a>(&self, expected: &'a [String], status: &WindowStatus) -> Vec<&'a str> {
        expected
            .iter()
            .filter(|lane| !status.is_valid(lane))
            .map(String::as_str)
            .collect()
    }

    /// Lanes that arrived without being expected. Merged, but never counted
    /// toward completeness.
    pub fn unexpected(&self, expected: &[String]) -> Vec<&str> {
        self.lanes
            .keys()
            .filter(|lane| !expected.iter().any(|want| want == *lane))
            .map(String::as_str)
            .collect()
    }
}

/// What a scan of the spool root found.
#[derive(Clone, Debug, Default)]
pub struct Assembly {
    pub windows: BTreeMap<WindowKey, Window>,
    /// Window starts with more than one declared end. Not finalizable: canonical
    /// output is keyed by start, so committing both would overwrite the first.
    pub conflicting_starts: BTreeMap<u64, Vec<u64>>,
    /// Seals that could not be read *and* whose filename gave no window. Surfaced
    /// rather than dropped: the segment exists, and something is wrong with it.
    pub unplaceable_seals: Vec<String>,
}

/// Whether one lane's contribution to a window can be merged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaneStatus {
    Valid,
    /// §5's `lane_invalid`: a seal or segment failed validation.
    Invalid {
        detail: String,
    },
}

/// Per-lane verdicts for one window, computed once and reused.
#[derive(Clone, Debug, Default)]
pub struct WindowStatus {
    pub lanes: BTreeMap<String, LaneStatus>,
}

impl WindowStatus {
    pub fn is_valid(&self, lane: &str) -> bool {
        matches!(self.lanes.get(lane), Some(LaneStatus::Valid))
    }

    pub fn valid_lanes(&self) -> Vec<&str> {
        self.lanes
            .iter()
            .filter(|(_, status)| **status == LaneStatus::Valid)
            .map(|(lane, _)| lane.as_str())
            .collect()
    }

    pub fn invalid_lanes(&self) -> Vec<(&str, &str)> {
        self.lanes
            .iter()
            .filter_map(|(lane, status)| match status {
                LaneStatus::Invalid { detail } => Some((lane.as_str(), detail.as_str())),
                LaneStatus::Valid => None,
            })
            .collect()
    }

    /// Records a lane as invalid after the fact — used when the merge attributes
    /// a record-level fault to a lane that its seals could not have revealed.
    pub fn invalidate(&mut self, lane: &str, detail: String) {
        self.lanes
            .insert(lane.to_owned(), LaneStatus::Invalid { detail });
    }
}

/// One valid lane's delivery bounds in a committed or candidate window.
///
/// Both values are `None` for a valid empty lane. Keeping that lane in the span
/// list matters: an empty window does not break a splice's lifetime counter, so
/// the last non-empty value must carry across it to the next adjacent window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneDeliverySpan {
    pub lane: String,
    pub first: Option<u64>,
    pub last: Option<u64>,
}

/// The last valid delivery position of every lane in the immediately preceding
/// window.
///
/// This is deliberately a window boundary rather than a global maximum. A
/// `lane_missing` or `lane_invalid` window explicitly breaks what can be proven;
/// when that lane later returns, its first counter is not blamed for a gap that
/// the intervening receipt has already recorded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeliveryContinuity {
    through_ns: Option<u64>,
    lanes: BTreeMap<String, Option<u64>>,
}

impl DeliveryContinuity {
    /// Cross-window faults visible before the merge starts.
    pub fn check_window(&self, window: &Window, status: &WindowStatus) -> Vec<(String, String)> {
        let spans = window
            .lanes
            .values()
            .filter(|entry| status.is_valid(&entry.lane))
            .map(|entry| LaneDeliverySpan {
                lane: entry.lane.clone(),
                first: entry.first_delivery_index(),
                last: entry.last_delivery_index(),
            })
            .collect::<Vec<_>>();
        self.check_spans(window.key.start_ns, &spans)
    }

    fn check_spans(
        &self,
        window_start_ns: u64,
        spans: &[LaneDeliverySpan],
    ) -> Vec<(String, String)> {
        if self.through_ns != Some(window_start_ns) {
            return Vec::new();
        }

        spans
            .iter()
            .filter_map(|span| {
                let previous = self.lanes.get(&span.lane).copied().flatten()?;
                let first = span.first?;
                let follows = previous.checked_add(1).is_some_and(|next| first == next);
                (!follows).then(|| {
                    (
                        span.lane.clone(),
                        format!(
                            "delivery_index jumps {previous} -> {first} across the window boundary \
                             at {window_start_ns}; the index is dense across the lane's lifetime"
                        ),
                    )
                })
            })
            .collect()
    }

    /// Advances the boundary after a window has actually committed.
    ///
    /// Callers pass only lanes the receipt names as present/valid. Missing and
    /// invalid lanes therefore disappear from the next boundary, while a valid
    /// empty lane retains its previous last value.
    pub fn advance(
        &mut self,
        window_start_ns: u64,
        window_end_ns: u64,
        spans: &[LaneDeliverySpan],
    ) -> Result<(), String> {
        if self
            .through_ns
            .is_some_and(|through| window_start_ns < through)
        {
            return Err(format!(
                "window {window_start_ns} overlaps the delivery boundary ending at {}",
                self.through_ns.unwrap_or_default()
            ));
        }

        let faults = self.check_spans(window_start_ns, spans);
        if let Some((lane, detail)) = faults.into_iter().next() {
            return Err(format!(
                "committed lane {lane} violates delivery continuity: {detail}"
            ));
        }

        let adjacent = self.through_ns == Some(window_start_ns);
        let mut next = BTreeMap::new();
        for span in spans {
            let previous = adjacent
                .then(|| self.lanes.get(&span.lane).copied().flatten())
                .flatten();
            next.insert(span.lane.clone(), span.last.or(previous));
        }
        self.through_ns = Some(window_end_ns);
        self.lanes = next;
        Ok(())
    }
}

/// Validates every lane of a window: seals, clock, contiguity, digests.
///
/// `cache` memoises per-segment digest results within a process. A waiting
/// window is re-examined on every run and segments are immutable once sealed, so
/// without it a long deadline would rehash the same gigabytes on every poll —
/// the unbounded scan the ingester already avoids.
pub fn validate_window(
    window: &Window,
    cache: &mut BTreeMap<PathBuf, Result<(), String>>,
) -> WindowStatus {
    let mut status = WindowStatus::default();
    for entry in window.lanes.values() {
        let verdict = lane_verdict(entry, cache);
        status.lanes.insert(entry.lane.clone(), verdict);
    }
    status
}

fn lane_verdict(
    entry: &LaneSegments,
    cache: &mut BTreeMap<PathBuf, Result<(), String>>,
) -> LaneStatus {
    if let Some(detail) = &entry.fault {
        return LaneStatus::Invalid {
            detail: detail.clone(),
        };
    }
    // §2 step 3: "Exclude that lane from the certified merge for the affected
    // window and record it as `lane_invalid`." A regressed clock breaks the one
    // precondition a k-way merge has — that each input is already sorted by the
    // merge key — so this is exclusion, not a note attached to a merged lane.
    if !entry.clock_is_sound() {
        return LaneStatus::Invalid {
            detail: format!(
                "lane {} sealed a segment with ordering_status visible_clock_regression",
                entry.lane
            ),
        };
    }
    if let Err(detail) = entry.delivery_is_contiguous() {
        return LaneStatus::Invalid { detail };
    }
    for (path, _) in &entry.segments {
        let outcome = cache
            .entry(path.clone())
            .or_insert_with(|| validate_sealed_segment(&entry.lane, path).map(|_| ()));
        if let Err(detail) = outcome {
            return LaneStatus::Invalid {
                detail: detail.clone(),
            };
        }
    }
    LaneStatus::Valid
}

/// Whether a window may be finalized yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Eligibility {
    /// Every expected lane produced a lane that merges.
    Complete,
    /// The deadline expired with lanes still outstanding.
    DeadlineExpired,
    /// Still inside the deadline. A run stops here rather than skipping ahead.
    Waiting { until_ns: u64 },
    /// Two seals disagree about this window's bounds. Nothing may be written.
    Conflicted { ends: Vec<u64> },
}

impl Eligibility {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Complete | Self::DeadlineExpired)
    }
}

/// Decides whether a window can be finalized now.
pub fn eligibility(
    window: &Window,
    status: &WindowStatus,
    expected: &[String],
    now_ns: u64,
    deadline_seconds: u64,
) -> Eligibility {
    // A window that has not ended is still being written to, however complete it
    // looks. "Every expected lane has sealed" is not proof the window is over: a
    // crash mid-window produces a recovery seal, and the restarted splice opens
    // a *second* segment for the same window. Finalizing on the strength of that
    // first seal would commit the window early and force every record after the
    // restart down the `late_after_finalization` path, where §5 forbids it from
    // ever entering canonical evidence. Waiting for the boundary costs nothing.
    if now_ns < window.key.end_ns {
        return Eligibility::Waiting {
            until_ns: window.key.end_ns,
        };
    }
    if window.unsatisfied(expected, status).is_empty() {
        return Eligibility::Complete;
    }
    let until_ns = window
        .key
        .end_ns
        .saturating_add(deadline_seconds.saturating_mul(1_000_000_000));
    if now_ns >= until_ns {
        Eligibility::DeadlineExpired
    } else {
        Eligibility::Waiting { until_ns }
    }
}

/// Groups every sealed segment under a spool root into its window.
///
/// Reads seals only. Grouping hundreds of segments must not cost a digest over
/// each of them — §5's full verification runs when a window is actually
/// finalized, against the segments that window will consume.
///
/// A seal that cannot be read does not abort the scan. It marks its lane
/// unreadable in the window it names if it names one, and otherwise is reported
/// against the lane alone; a torn sidecar on one lane must not hide five healthy
/// ones.
pub fn assemble(root: &Path, period_ns: u64) -> Result<Assembly, String> {
    let discovered =
        discover_segments(root).map_err(|error| format!("reading spool root: {error}"))?;

    let mut windows: BTreeMap<WindowKey, Window> = BTreeMap::new();
    let mut unplaceable: Vec<String> = Vec::new();

    for (lane, path) in discovered {
        // **The filename places the segment, and the seal is checked against
        // it.** Both are written by the same writer from the same window start,
        // so a disagreement means one is corrupt — and trusting the seal for
        // placement would let a corrupt one file records into a window they were
        // never received in, with every digest still checking out.
        let placement = window_start_from_filename(&path);
        let seal = read_seal(&path);

        let start_ns = match (placement, &seal) {
            (Some(start), _) => start,
            (None, Ok(seal)) => seal.window_start_ns,
            (None, Err(error)) => {
                unplaceable.push(format!("{}: {error}", path.display()));
                continue;
            }
        };
        // An unaligned start names no window this deployment produces — windows
        // tile the UTC day from the epoch. Materialising one anyway would invent
        // a window overlapping a real one and give it its own receipt.
        if start_ns % period_ns != 0 {
            unplaceable.push(format!(
                "{}: window start {start_ns} is not aligned to the {period_ns}ns period",
                path.display()
            ));
            continue;
        }
        let key = WindowKey {
            start_ns,
            end_ns: start_ns.saturating_add(period_ns),
        };

        match seal {
            Err(error) => {
                lane_entry(&mut windows, key, &lane).fault = Some(error);
            }
            Ok(seal) => {
                if let Err(detail) = window_bounds_agree(&seal, start_ns, period_ns) {
                    lane_entry(&mut windows, key, &lane).fault = Some(detail);
                    continue;
                }
                lane_entry(&mut windows, key, &lane)
                    .segments
                    .push((path, seal));
            }
        }
    }

    // Retained for the report. With the period configured and every seal checked
    // against it, two seals can no longer disagree about one window's bounds —
    // they are computed, not read.
    let conflicting_starts: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for window in windows.values_mut() {
        for entry in window.lanes.values_mut() {
            entry
                .segments
                .sort_by_key(|(path, seal)| (seal.segment_index, path.clone()));
        }
    }
    Ok(Assembly {
        windows,
        conflicting_starts,
        unplaceable_seals: unplaceable,
    })
}

/// Whether a seal's declared window matches where the segment actually sits.
///
/// Every window in a deployment is `period_ns` long and aligned to the Unix
/// epoch, because the writer refuses a period that does not divide a UTC day.
/// Checking that here is what gives window bounds a single authority: without
/// it, a lone torn seal produced a window with no end at all, a stray 60-minute
/// seal could redefine the tiling and skip a real 30-minute window, and a seal
/// naming a window its own records fall outside of still certified.
fn window_bounds_agree(seal: &SegmentSeal, start_ns: u64, period_ns: u64) -> Result<(), String> {
    if seal.window_start_ns != start_ns {
        return Err(format!(
            "seal declares window_start_ns {} but the segment filename places it at {start_ns}",
            seal.window_start_ns
        ));
    }
    if seal.window_end_ns != start_ns.saturating_add(period_ns) {
        return Err(format!(
            "seal declares window {}..{} but the configured period is {period_ns}ns",
            seal.window_start_ns, seal.window_end_ns
        ));
    }
    if start_ns % period_ns != 0 {
        return Err(format!(
            "window start {start_ns} is not aligned to the {period_ns}ns period"
        ));
    }
    Ok(())
}

/// Windows must tile the UTC day exactly, matching the writer's own rule.
pub fn validate_window_seconds(seconds: u64) -> Result<u64, String> {
    if seconds == 0 || 86_400 % seconds != 0 {
        return Err(format!(
            "--window-seconds must be a positive divisor of 86400, got {seconds}; \
             it has to match the splices' SEGMENT_SECONDS"
        ));
    }
    Ok(seconds * 1_000_000_000)
}

fn lane_entry<'a>(
    windows: &'a mut BTreeMap<WindowKey, Window>,
    key: WindowKey,
    lane: &str,
) -> &'a mut LaneSegments {
    windows
        .entry(key)
        .or_insert_with(|| Window {
            key,
            lanes: BTreeMap::new(),
        })
        .lanes
        .entry(lane.to_owned())
        .or_insert_with(|| LaneSegments {
            lane: lane.to_owned(),
            segments: Vec::new(),
            fault: None,
        })
}

/// The window start encoded in a segment filename.
///
/// `<%Y%m%dT%H%M%S%f>-<index>-<id>.ndjson`, written by
/// `splices.common.segment.segment_filename`. Used only to place a segment whose
/// seal cannot be read; a readable seal is always the authority.
pub fn window_start_from_filename(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let stamp = name.split('-').next()?;
    let (date, time) = stamp.split_once('T')?;
    if date.len() != 8
        || time.len() != 12
        || !stamp.bytes().all(|b| b.is_ascii_digit() || b == b'T')
    {
        return None;
    }
    let year: i64 = date.get(0..4)?.parse().ok()?;
    let month: u32 = date.get(4..6)?.parse().ok()?;
    let day: u32 = date.get(6..8)?.parse().ok()?;
    let hour: u64 = time.get(0..2)?.parse().ok()?;
    let minute: u64 = time.get(2..4)?.parse().ok()?;
    let second: u64 = time.get(4..6)?.parse().ok()?;
    let micros: u64 = time.get(6..12)?.parse().ok()?;
    if month == 0 || month > 12 || day == 0 || day > 31 || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    if days < 0 {
        return None;
    }
    Some(
        days as u64 * 86_400_000_000_000
            + (hour * 3600 + minute * 60 + second) * 1_000_000_000
            + micros * 1_000,
    )
}

/// The `%Y%m%dT%H%M%S%f` stamp a segment filename starts with.
///
/// The inverse of [`window_start_from_filename`], kept beside it so the two
/// cannot drift. Mirrors `splices.common.segment.segment_filename`.
pub fn segment_stamp(start_ns: u64) -> String {
    let (year, month, day) = civil_from_days((start_ns / 86_400_000_000_000) as i64);
    let within = start_ns % 86_400_000_000_000;
    let (hour, minute, second) = (
        within / 3_600_000_000_000,
        (within / 60_000_000_000) % 60,
        (within / 1_000_000_000) % 60,
    );
    let micros = (within % 1_000_000_000) / 1_000;
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}{micros:06}")
}

/// The inverse of `civil_from_days`, same source.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = (year - era * 400) as u64;
    let month = month as u64;
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day as u64 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era as i64 - 719_468
}

/// Ceiling on synthesized windows, so a long outage cannot make one run
/// materialise an unbounded number of receipts.
pub const MAX_SYNTHESIZED_WINDOWS: usize = 10_000;

/// Adds the windows that produced *no* segment from any lane.
///
/// A window with nothing in it is invisible to a scan of seals, because a scan
/// of seals is exactly what it has none of. Left out, a total capture outage
/// leaves no trace: the next window that does have data commits and the sequence
/// moves on with no `lane_missing` receipt anywhere. So the observed windows are
/// tiled from `resume_after_ns` — the end of the last committed window — and any
/// hole becomes an empty `Window` that finalizes to an incomplete receipt naming
/// every expected lane.
///
/// A durable watermark (§7) is the proper anchor and lands with it; until then
/// the last committed receipt serves, which covers every case except an outage
/// spanning the very first run.
pub fn tile_absent_windows(
    windows: &mut BTreeMap<WindowKey, Window>,
    period: u64,
    resume_after_ns: Option<u64>,
    now_ns: u64,
    deadline_seconds: u64,
) -> Result<usize, String> {
    let observed_newest = windows.keys().map(|key| key.start_ns).max();
    let deadline_ns = deadline_seconds.saturating_mul(1_000_000_000);
    let expired_newest = now_ns.checked_sub(deadline_ns).and_then(|cutoff| {
        let latest_end = (cutoff / period) * period;
        latest_end.checked_sub(period)
    });
    let Some(first) = resume_after_ns.or_else(|| windows.keys().map(|key| key.start_ns).min())
    else {
        return Ok(0);
    };

    // Wall time is a safe horizon only after a committed receipt anchors when
    // this deployment began. It is also bounded: a historical backfill with a
    // 1970 receipt must not be mistaken for a fifty-year live outage. Observed
    // seals still retain the old backlog-limit behaviour below; only the
    // wall-clock extension is declined when it exceeds one bounded run.
    let wall_horizon = if resume_after_ns.is_some() {
        expired_newest.filter(|expired| {
            *expired >= first && (*expired - first) / period <= MAX_SYNTHESIZED_WINDOWS as u64
        })
    } else {
        None
    };
    let newest = match (observed_newest, wall_horizon) {
        (Some(observed), Some(expired)) => Some(observed.max(expired)),
        (Some(observed), None) => Some(observed),
        (None, Some(expired)) => Some(expired),
        (None, None) => None,
    };
    let Some(newest) = newest else { return Ok(0) };

    if first > newest {
        return Ok(0);
    }

    if newest > first && (newest - first) / period > MAX_SYNTHESIZED_WINDOWS as u64 {
        return Err(format!(
            "the gap between the last committed window and {newest} spans more than              {MAX_SYNTHESIZED_WINDOWS} windows of {period}ns; finalize the backlog in              smaller ranges rather than materialising every empty window at once"
        ));
    }

    let mut added = 0;
    let mut start = first;
    loop {
        let end_ns = start
            .checked_add(period)
            .ok_or_else(|| format!("window starting at {start} overflows its {period}ns period"))?;
        let key = WindowKey {
            start_ns: start,
            end_ns,
        };
        if !windows.keys().any(|existing| existing.start_ns == start) {
            windows.insert(
                key,
                Window {
                    key,
                    lanes: BTreeMap::new(),
                },
            );
            added += 1;
        }
        if start >= newest {
            break;
        }
        start = end_ns;
    }
    Ok(added)
}

/// Windows ready to finalize, oldest first, stopping at the first still waiting.
///
/// Each returned window carries the validation it was judged on, so the caller
/// does not repeat the digest work that decided its verdict.
pub fn ready_windows(
    windows: &BTreeMap<WindowKey, Window>,
    committed: &std::collections::BTreeSet<u64>,
    expected: &[String],
    now_ns: u64,
    deadline_seconds: u64,
    cache: &mut BTreeMap<PathBuf, Result<(), String>>,
) -> Vec<(WindowKey, Eligibility, WindowStatus)> {
    let mut ready = Vec::new();
    for (key, window) in windows {
        // Committed windows leave the readiness plan before anything is read.
        //
        // They used to be validated here and skipped afterwards, which meant
        // every run rehashed the entire 5–7 day raw retention set — and a late
        // segment landing on an already-committed window could fault it and stop
        // every later window from finalizing. A committed window is immutable;
        // its late arrivals are reported separately under §5's
        // `late_after_finalization` policy and change nothing.
        if committed.contains(&key.start_ns) {
            continue;
        }
        let status = validate_window(window, cache);
        let verdict = eligibility(window, &status, expected, now_ns, deadline_seconds);
        if !verdict.is_ready() {
            // §7: an earlier window inside its deadline blocks every later one.
            // Committing out of order would assign a later window the positions
            // this one has not finished claiming.
            break;
        }
        ready.push((*key, verdict, status));
    }
    ready
}

/// The `date=YYYY-MM-DD` partition a window start belongs to.
///
/// Windows tile the UTC day exactly — the writer refuses a period that does not
/// divide 86,400 — so a window never straddles the boundary and this is a pure
/// function of its start.
pub fn date_partition(start_ns: u64) -> String {
    let (year, month, day) = civil_from_days((start_ns / 86_400_000_000_000) as i64);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Days since the Unix epoch to a civil date, by Howard Hinnant's algorithm.
///
/// Written out rather than pulling in a date crate: this is the only calendar
/// arithmetic the finalizer does, and the partition has to match the Python
/// writer's `datetime.utcfromtimestamp` exactly.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(lanes: &[&str]) -> Window {
        Window {
            key: WindowKey {
                start_ns: 0,
                end_ns: 1_800_000_000_000,
            },
            lanes: lanes
                .iter()
                .map(|lane| {
                    (
                        (*lane).to_owned(),
                        LaneSegments {
                            lane: (*lane).to_owned(),
                            segments: Vec::new(),
                            fault: None,
                        },
                    )
                })
                .collect(),
        }
    }

    fn expected(lanes: &[&str]) -> Vec<String> {
        lanes.iter().map(|lane| (*lane).to_owned()).collect()
    }

    /// Every present lane valid — the shape a healthy window has.
    fn sound(window: &Window) -> WindowStatus {
        WindowStatus {
            lanes: window
                .lanes
                .keys()
                .map(|lane| (lane.clone(), LaneStatus::Valid))
                .collect(),
        }
    }

    /// Any instant after the fixture window's end.
    const AFTER: u64 = 1_800_000_000_001;

    fn span(lane: &str, first: Option<u64>, last: Option<u64>) -> LaneDeliverySpan {
        LaneDeliverySpan {
            lane: lane.to_owned(),
            first,
            last,
        }
    }

    #[test]
    fn a_valid_empty_window_preserves_lane_delivery_continuity() {
        let mut continuity = DeliveryContinuity::default();
        continuity
            .advance(0, 10, &[span("polymarket", Some(1), Some(1))])
            .expect("first window");
        continuity
            .advance(10, 20, &[span("polymarket", None, None)])
            .expect("valid empty window");

        assert!(
            continuity
                .check_spans(20, &[span("polymarket", Some(2), Some(2))])
                .is_empty(),
            "quiet capture does not reset a lane-lifetime counter"
        );
    }

    #[test]
    fn an_explicitly_absent_lane_breaks_what_continuity_can_prove() {
        let mut continuity = DeliveryContinuity::default();
        continuity
            .advance(0, 10, &[span("polymarket", Some(1), Some(1))])
            .expect("first window");
        continuity
            .advance(10, 20, &[])
            .expect("lane_missing window");

        assert!(
            continuity
                .check_spans(20, &[span("polymarket", Some(100), Some(100))])
                .is_empty(),
            "a known missing interval owns the gap; the returning lane does not"
        );
    }

    #[test]
    fn a_window_with_every_expected_lane_is_complete_before_any_deadline() {
        let subject = window(&["polymarket", "kalshi"]);
        let verdict = eligibility(
            &subject,
            &sound(&subject),
            &expected(&["polymarket", "kalshi"]),
            AFTER,
            300,
        );
        assert_eq!(verdict, Eligibility::Complete);
    }

    #[test]
    fn a_window_that_has_not_ended_is_never_finalized_however_complete_it_looks() {
        // Every expected lane has sealed, but the window is still open. A
        // crash mid-window seals early and the restarted splice opens a second
        // segment for the same window; committing on the first seal would push
        // everything after the restart onto the late-arrival path, which §5
        // forbids from entering canonical evidence at all.
        let subject = window(&["polymarket", "kalshi"]);
        let verdict = eligibility(
            &subject,
            &sound(&subject),
            &expected(&["polymarket", "kalshi"]),
            1_799_999_999_999,
            0,
        );
        assert_eq!(
            verdict,
            Eligibility::Waiting {
                until_ns: 1_800_000_000_000
            }
        );
    }

    #[test]
    fn a_missing_lane_blocks_until_the_deadline_then_commits() {
        let subject = window(&["polymarket"]);
        let want = expected(&["polymarket", "kalshi"]);

        let waiting = eligibility(&subject, &sound(&subject), &want, 1_800_000_000_000, 300);
        assert_eq!(
            waiting,
            Eligibility::Waiting {
                until_ns: 1_800_000_000_000 + 300_000_000_000
            }
        );
        // One nanosecond past the deadline it commits anyway, rather than
        // letting one wedged splice halt every healthy venue.
        let expired = eligibility(
            &subject,
            &sound(&subject),
            &want,
            1_800_000_000_000 + 300_000_000_000,
            300,
        );
        assert_eq!(expired, Eligibility::DeadlineExpired);
        assert_eq!(subject.missing(&want), vec!["kalshi"]);
    }

    #[test]
    fn an_unexpected_lane_neither_completes_nor_disappears() {
        let subject = window(&["polymarket", "polymarket_rtds"]);
        let want = expected(&["polymarket", "kalshi"]);
        assert_eq!(subject.missing(&want), vec!["kalshi"]);
        assert_eq!(subject.unexpected(&want), vec!["polymarket_rtds"]);
        // Present-but-unexpected does not fill the hole left by a missing lane.
        assert!(!eligibility(&subject, &sound(&subject), &want, AFTER, 300).is_ready());
    }

    #[test]
    fn a_disabled_profile_is_simply_not_expected() {
        // §3's stated case. The default compose deployment runs three lanes; a
        // window containing exactly those is complete, not missing three.
        let running = &["polymarket", "polymarket_snapshots", "limitless"];
        let subject = window(running);
        let verdict = eligibility(&subject, &sound(&subject), &expected(running), AFTER, 300);
        assert_eq!(verdict, Eligibility::Complete);
    }

    #[test]
    fn an_earlier_waiting_window_blocks_every_later_one() {
        let mut windows = BTreeMap::new();
        for (index, lanes) in [vec!["polymarket"], vec!["polymarket", "kalshi"]]
            .into_iter()
            .enumerate()
        {
            let start = index as u64 * 1_800_000_000_000;
            let key = WindowKey {
                start_ns: start,
                end_ns: start + 1_800_000_000_000,
            };
            let mut subject = window(&lanes);
            subject.key = key;
            windows.insert(key, subject);
        }
        // Both windows have ended, and the second is complete. But the first
        // is missing a lane and still inside its (long) deadline, so nothing is
        // finalized — §7 forbids finalizing out of order.
        let ready = ready_windows(
            &windows,
            &std::collections::BTreeSet::new(),
            &expected(&["polymarket", "kalshi"]),
            3_600_000_000_001,
            7_200,
            &mut BTreeMap::new(),
        );
        assert!(
            ready.is_empty(),
            "a later complete window must not overtake an earlier one"
        );
    }

    #[test]
    fn a_segment_stamp_round_trips_through_its_window_start() {
        // The filename is the placement authority when a seal cannot be read, so
        // the two directions have to agree exactly.
        for start in [
            0u64,
            1_800_000_000_000,
            1_785_412_800_000_000_000,
            1_709_164_800_000_000_000,
        ] {
            let name = PathBuf::from(format!("{}-000-abcd.ndjson", segment_stamp(start)));
            assert_eq!(window_start_from_filename(&name), Some(start), "{start}");
        }
    }

    #[test]
    fn date_partitions_match_the_writers() {
        assert_eq!(date_partition(0), "1970-01-01");
        // 2026-07-30T12:00:00Z, the window the capture fixtures use.
        assert_eq!(date_partition(1_785_412_800_000_000_000), "2026-07-30");
        // A leap day, where an off-by-one calendar shows up.
        assert_eq!(date_partition(1_709_164_800_000_000_000), "2024-02-29");
    }
}
