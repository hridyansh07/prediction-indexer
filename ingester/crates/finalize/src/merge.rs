//! The k-way merge on `(visible_ns, lane_rank, delivery_index)`.
//!
//! One cursor per lane, a binary heap over their heads. Memory is O(lanes), not
//! O(records) — a window can approach a gigabyte per lane and is never held.
//!
//! Three invariants are enforced here rather than assumed, because each is
//! silently destructive if it fails and the merge is the last place any of them
//! is cheap to check:
//!
//! * **`delivery_index` is dense within a lane** — each value is exactly one
//!   more than the last. §1: "`delivery_index` is dense and unique within a
//!   lane. A repeated or decreasing value within a lane is invalid input, not
//!   another tie to resolve." Density matters as much as uniqueness: the counter
//!   is assigned by the splice before the record is written and continues across
//!   restarts, so within a window it cannot legitimately skip. A gap means a
//!   segment of that lane is **missing from this window**, and merging across it
//!   would publish canonical evidence that silently omits records while looking
//!   complete. A repeat would duplicate one.
//! * **`visible_ns` does not decrease within a lane.** The seal already asserts
//!   this per segment and the writer seeds each segment from the last one, so a
//!   violation reaching here means the input was assembled wrongly. Emitting it
//!   anyway would produce a canonical file whose own ordering key runs backwards.
//! * **Each lane appears exactly once.** Two cursors sharing a name each track
//!   their own `delivery_index`, which defeats both checks above — the same
//!   index passes twice, once per cursor.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::rank::lane_rank;

/// Where a record came from, interned.
///
/// Indices rather than strings: provenance needs the lane name and the source
/// segment's digest for every one of tens of millions of records a day, and
/// carrying a 64-character hex digest per record would cost more than the merge.
/// The caller keeps the tables and resolves these on the way out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceRef {
    pub lane: u16,
    pub segment: u32,
    /// 1-based, within the source segment.
    pub line_number: u64,
}

/// One record as the merge sees it: its key, its bytes, and where it came from.
///
/// `record_id` and `content_hash` are carried rather than recovered later. The
/// reader has already parsed the line to validate its schema, and provenance
/// needs both for every record; re-parsing in the writer would pay for the same
/// decode twice across tens of millions of records a day.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneRecord {
    pub visible_ns: u64,
    pub delivery_index: u64,
    pub source: SourceRef,
    pub record_id: String,
    /// Hash of the decoded venue payload, not of the transport line — identity
    /// is judged on content, which is the same rule the classifier applies.
    pub content_hash: String,
    /// The exact delivered line including its trailing newline. Copied
    /// byte-for-byte into canonical evidence — never re-encoded.
    pub line: Vec<u8>,
}

/// A merged record and whether it shares its instant with another lane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergedRecord {
    pub record: LaneRecord,
    /// Non-null exactly when two or more **distinct lanes** carry this
    /// `visible_ns`. The value is the shared `visible_ns` itself: it is already
    /// unique to the group, needs no counter to stay deterministic across a
    /// retry, and is globally unambiguous.
    ///
    /// The presence of the field is the information — §1 requires analysis to
    /// treat such records as a capture-time tie and forbids reading lead-lag out
    /// of the rank order that serialized them. Records of *one* lane sharing a
    /// timestamp are not a cross-lane tie; they are genuinely ordered by
    /// `delivery_index`, so they stay null.
    pub visible_tie_group: Option<u64>,
}

/// A lane's records, in receive order, as a fallible stream.
pub type LaneStream<'a> = Box<dyn Iterator<Item = Result<LaneRecord, String>> + 'a>;

/// A merge failure, carrying the lane responsible for it.
///
/// The lane is structural rather than embedded in a message because the caller
/// acts on it: §5 gives each lane its own verdict, so a fault attributable to
/// one lane must exclude *that* lane and let the window commit without it.
/// Recovering the name by parsing a string would make that isolation depend on
/// message formatting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeFault {
    pub lane: String,
    pub detail: String,
}

impl std::fmt::Display for MergeFault {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "lane {}: {}", self.lane, self.detail)
    }
}

struct Cursor<'a> {
    lane: String,
    rank: u32,
    /// Position of this lane's name among the merge's lane names, sorted. The
    /// final discriminator — see `Pending::key`.
    name_order: usize,
    stream: LaneStream<'a>,
    last_delivery_index: Option<u64>,
    last_visible_ns: Option<u64>,
}

/// A record waiting at the head of its lane, ordered by the merge key.
struct Pending {
    visible_ns: u64,
    rank: u32,
    delivery_index: u64,
    name_order: usize,
    /// Which cursor to pull from next. Deliberately *not* part of the key.
    slot: usize,
    record: LaneRecord,
}

impl Pending {
    /// `(visible_ns, lane_rank, delivery_index)` plus a last-resort discriminator.
    ///
    /// The discriminator is the lane's *name* order, never its position in the
    /// input. Two lanes absent from the rank table share `UNRANKED_LANE_RANK`,
    /// and if they also share an instant and an index the key would otherwise
    /// fall through to construction order — which is a filesystem accident, so
    /// the same bytes would canonicalize differently on two runs and the §8
    /// retry-after-crash guarantee would be false. Keying on the name makes the
    /// order a property of the data and the registry alone.
    fn key(&self) -> (u64, u32, u64, usize) {
        (
            self.visible_ns,
            self.rank,
            self.delivery_index,
            self.name_order,
        )
    }
}

impl Ord for Pending {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}
impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for Pending {}

/// Merges lane streams into one canonically ordered stream.
pub struct Merge<'a> {
    cursors: Vec<Cursor<'a>>,
    /// `Reverse` because `BinaryHeap` is a max-heap and the merge wants the
    /// smallest key next.
    heap: BinaryHeap<Reverse<Pending>>,
    primed: bool,
    failed: bool,
}

impl<'a> Merge<'a> {
    /// Builds a merge over one stream per lane.
    ///
    /// Lane order at construction does not affect the output: the heap is keyed
    /// entirely on record fields, the registry's rank, and lane names, so
    /// discovering segments in a different order produces identical bytes.
    ///
    /// Fails on a repeated lane name. A lane is one splice process running one
    /// counter, so two streams claiming the same name are not a lane — and each
    /// would carry its own `last_delivery_index`, letting the same index pass
    /// twice and defeating the density and uniqueness checks entirely.
    pub fn new(lanes: Vec<(String, LaneStream<'a>)>) -> Result<Self, String> {
        let mut names: Vec<String> = lanes.iter().map(|(lane, _)| lane.clone()).collect();
        names.sort();
        if let Some(repeated) = names.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(format!(
                "lane {:?} was supplied twice; one lane is one splice running one \
                 delivery_index counter, and two cursors under one name would each \
                 accept the same index",
                repeated[0]
            ));
        }
        let name_order: std::collections::BTreeMap<String, usize> = names
            .into_iter()
            .enumerate()
            .map(|(index, name)| (name, index))
            .collect();

        let cursors = lanes
            .into_iter()
            .map(|(lane, stream)| Cursor {
                rank: lane_rank(&lane),
                name_order: name_order[&lane],
                lane,
                stream,
                last_delivery_index: None,
                last_visible_ns: None,
            })
            .collect::<Vec<_>>();
        Ok(Self {
            heap: BinaryHeap::with_capacity(cursors.len()),
            cursors,
            primed: false,
            failed: false,
        })
    }

    /// The lane names, indexed by `SourceRef::lane`.
    pub fn lanes(&self) -> Vec<&str> {
        self.cursors
            .iter()
            .map(|cursor| cursor.lane.as_str())
            .collect()
    }

    /// Pulls the next record from one lane and pushes it, checking both per-lane
    /// invariants on the way through.
    fn advance(&mut self, slot: usize) -> Result<(), MergeFault> {
        let cursor = &mut self.cursors[slot];
        let fault = |detail: String| MergeFault {
            lane: cursor.lane.clone(),
            detail,
        };
        let Some(next) = cursor.stream.next() else {
            return Ok(());
        };
        let record = match next {
            Ok(record) => record,
            Err(detail) => {
                return Err(MergeFault {
                    lane: cursor.lane.clone(),
                    detail,
                });
            }
        };

        if let Some(previous) = cursor.last_delivery_index {
            if record.delivery_index != previous + 1 {
                let shape = if record.delivery_index > previous {
                    "a gap means a segment of this lane is missing from the window, and \
                     merging across it would publish evidence that omits records while \
                     looking complete"
                } else {
                    "a repeated or decreasing value within a lane is invalid input, not a tie"
                };
                return Err(fault(format!(
                    "delivery_index {} does not follow {previous}; the index is dense \
                     within a lane — {shape}",
                    record.delivery_index
                )));
            }
        }
        if let Some(previous) = cursor.last_visible_ns {
            if record.visible_ns < previous {
                return Err(fault(format!(
                    "visible_ns {} precedes {previous} at delivery_index {}; the merge \
                     key would run backwards",
                    record.visible_ns, record.delivery_index
                )));
            }
        }
        cursor.last_delivery_index = Some(record.delivery_index);
        cursor.last_visible_ns = Some(record.visible_ns);

        self.heap.push(Reverse(Pending {
            visible_ns: record.visible_ns,
            rank: cursor.rank,
            delivery_index: record.delivery_index,
            name_order: cursor.name_order,
            slot,
            record,
        }));
        Ok(())
    }
}

impl Iterator for Merge<'_> {
    type Item = Result<LaneRecord, MergeFault>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if !self.primed {
            self.primed = true;
            for slot in 0..self.cursors.len() {
                if let Err(error) = self.advance(slot) {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
        let Reverse(pending) = self.heap.pop()?;
        if let Err(error) = self.advance(pending.slot) {
            self.failed = true;
            return Some(Err(error));
        }
        Some(Ok(pending.record))
    }
}

/// Annotates merged records with their cross-lane tie group.
///
/// Buffers one run of equal `visible_ns` at a time, which is what deciding
/// "does this instant span more than one lane" requires — the answer is not
/// known until the run ends. The buffer is bounded by how many records share an
/// exact nanosecond: across 46,446 authenticated Kalshi records §1 measured zero
/// repeated values, and a run long enough to matter would mean the host clock had
/// stopped, which is a fault in its own right.
pub struct TieGroups<I> {
    inner: I,
    /// Records whose group is settled, waiting to be handed out.
    ready: std::collections::VecDeque<MergedRecord>,
    /// The open run: every record seen so far at `run_visible_ns`.
    run: Vec<LaneRecord>,
    done: bool,
}

impl<I> TieGroups<I> {
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            ready: std::collections::VecDeque::new(),
            run: Vec::new(),
            done: false,
        }
    }

    /// Closes the open run, deciding its group, and queues it for release.
    fn close_run(&mut self) {
        let Some(first) = self.run.first() else {
            return;
        };
        let cross_lane = self
            .run
            .iter()
            .any(|held| held.source.lane != first.source.lane);
        let group = cross_lane.then_some(first.visible_ns);
        self.ready
            .extend(self.run.drain(..).map(|record| MergedRecord {
                record,
                visible_tie_group: group,
            }));
    }
}

impl<I: Iterator<Item = Result<LaneRecord, MergeFault>>> Iterator for TieGroups<I> {
    type Item = Result<MergedRecord, MergeFault>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(settled) = self.ready.pop_front() {
                return Some(Ok(settled));
            }
            if self.done {
                return None;
            }
            match self.inner.next() {
                Some(Err(error)) => {
                    self.done = true;
                    self.run.clear();
                    return Some(Err(error));
                }
                None => {
                    self.done = true;
                    self.close_run();
                }
                Some(Ok(record)) => {
                    // The merge is sorted on `visible_ns`, so every record at one
                    // instant is contiguous and a different value ends the run.
                    if self
                        .run
                        .first()
                        .is_some_and(|open| open.visible_ns != record.visible_ns)
                    {
                        self.close_run();
                    }
                    self.run.push(record);
                }
            }
        }
    }
}
