//! Publisher-side buffering. `PublishBuffer` owns every stream-backend write
//! the publisher makes; the backend itself is a `StreamSink` adapter, so a
//! replacement for Redis is one more adapter, not a publisher change.
//!
//! Single-threaded semantics (REPLAY_STREAMS_V1.md, "Publisher buffering"):
//! entries are inserted in wire order; at `batch` buffered entries one
//! non-blocking drain appends as many as the queue accepts; if any remain, no
//! further drain is attempted until the hard capacity `ceil(1.25 × batch)`,
//! where a blocking drain appends everything with the queue-full backoff.
//! `flush` (initial and terminal) is the same blocking drain. Buffering never
//! changes delivered content, order, or sequence numbers.
use crate::{Error, Result};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

/// Queue-full backoff: 1 ms, doubling, capped at 50 ms, reset after any
/// progress. The publisher has no wait deadline of its own; the supervisor's
/// no-progress, stall and attempt deadlines bound it.
pub const FULL_BACKOFF_START: Duration = Duration::from_millis(1);
pub const FULL_BACKOFF_CAP: Duration = Duration::from_millis(50);
/// Closed bounds for the configurable batch size.
pub const MIN_BATCH_ENTRIES: usize = 1;
pub const MAX_BATCH_ENTRIES: usize = 1024;
pub const DEFAULT_BATCH_ENTRIES: usize = 100;

/// Hard buffer capacity for a batch size: `ceil(1.25 × batch)` (125 for 100).
pub fn hard_capacity(batch: usize) -> usize {
    batch + batch.div_ceil(4)
}

/// The stream backend behind the publisher. Every method is one fatal-on-error
/// operation: no adapter may retry an ambiguous command itself.
pub trait StreamSink {
    /// Creates the attempt's stream state. Fails if it already exists.
    fn setup(
        &mut self,
        groups: &[String],
        max_entry_bytes: usize,
        max_queue_bytes: usize,
    ) -> Result<()>;
    /// Appends the longest prefix of `entries` that fits under the queue byte
    /// limit and returns its length; 0 means the queue is full and nothing was
    /// written. `first` is the wire sequence of `entries[0]` and must directly
    /// follow the last appended entry. `terminal` marks the last element of
    /// `entries` as the terminal record. A failing call writes nothing.
    fn append(&mut self, first: u64, entries: &[String], terminal: bool) -> Result<usize>;
    /// Named progress fields (see the stream spec); never per-entry bookkeeping.
    fn progress(&mut self) -> Result<BTreeMap<String, String>>;
    /// Best-effort shared poison flag. Callers already treat the attempt as dead.
    fn poison(&mut self);
}

/// One encoded wire record awaiting publication.
pub struct Entry {
    pub sequence: u64,
    pub record: String,
    pub terminal: bool,
}

/// Diagnostic totals for queue-full waiting. Not persisted or part of the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueWaits {
    /// Short (FULL or partial) replies during a blocking drain; each was
    /// followed by exactly one backoff sleep.
    pub full_replies: u64,
    /// Total time spent sleeping before retrying a blocking drain.
    pub waited: Duration,
}

/// Diagnostic publication totals. Not persisted or part of the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublishStats {
    pub waits: QueueWaits,
    /// Append calls that wrote at least one entry.
    pub batches: u64,
    /// Entries written in total.
    pub entries: u64,
}

pub struct PublishBuffer<S: StreamSink> {
    sink: S,
    entries: Vec<String>,
    /// Wire sequence of `entries[0]` (equivalently, of the next append).
    first: u64,
    /// Wire sequence the next insert must carry.
    next: u64,
    terminal: bool,
    batch: usize,
    capacity: usize,
    max_entry_bytes: usize,
    /// Upper bound on one append call's payload. A call carrying more than
    /// the whole queue byte limit could never be accepted past that limit.
    max_call_bytes: usize,
    /// A non-blocking drain left entries behind; wait for the hard capacity.
    attempted: bool,
    stats: PublishStats,
    sleep: fn(Duration),
}

impl<S: StreamSink> PublishBuffer<S> {
    pub fn new(sink: S, batch: usize, max_entry_bytes: usize, max_queue_bytes: usize) -> Self {
        Self::with_sleep(
            sink,
            batch,
            max_entry_bytes,
            max_queue_bytes,
            std::thread::sleep,
        )
    }
    fn with_sleep(
        sink: S,
        batch: usize,
        max_entry_bytes: usize,
        max_queue_bytes: usize,
        sleep: fn(Duration),
    ) -> Self {
        let batch = batch.clamp(MIN_BATCH_ENTRIES, MAX_BATCH_ENTRIES);
        Self {
            sink,
            entries: Vec::with_capacity(hard_capacity(batch)),
            first: 0,
            next: 0,
            terminal: false,
            batch,
            capacity: hard_capacity(batch),
            max_entry_bytes,
            max_call_bytes: max_queue_bytes.max(max_entry_bytes),
            attempted: false,
            stats: PublishStats::default(),
            sleep,
        }
    }
    pub fn sink_mut(&mut self) -> &mut S {
        &mut self.sink
    }
    pub fn stats(&self) -> PublishStats {
        self.stats
    }
    /// Entries inserted but not yet appended.
    pub fn buffered(&self) -> usize {
        self.entries.len()
    }
    /// Buffers one entry. An entry above the per-entry cap can never be
    /// published and is fatal here, before any backend call.
    pub fn insert(&mut self, entry: Entry) -> Result<()> {
        if self.terminal || entry.sequence != self.next {
            return Err(Error::Protocol("buffer sequence".into()));
        }
        if entry.record.len() > self.max_entry_bytes {
            return Err(Error::Resource);
        }
        self.entries.push(entry.record);
        self.next = self.next.checked_add(1).ok_or(Error::Resource)?;
        self.terminal = entry.terminal;
        if self.entries.len() >= self.capacity {
            self.drain(true)
        } else if !self.attempted && self.entries.len() >= self.batch {
            self.drain(false)
        } else {
            Ok(())
        }
    }
    /// Blocking drain of everything buffered (setup/initial and terminal).
    pub fn flush(&mut self) -> Result<()> {
        self.drain(true)
    }
    /// Longest buffered prefix within `max_call_bytes`; at least one entry.
    fn call_len(&self) -> usize {
        let mut bytes = 0usize;
        for (index, entry) in self.entries.iter().enumerate() {
            bytes = bytes.saturating_add(entry.len());
            if bytes > self.max_call_bytes {
                return index.max(1);
            }
        }
        self.entries.len()
    }
    fn drain(&mut self, blocking: bool) -> Result<()> {
        let mut backoff = FULL_BACKOFF_START;
        while !self.entries.is_empty() {
            let sent = self.call_len();
            let terminal = self.terminal && sent == self.entries.len();
            let accepted = self
                .sink
                .append(self.first, &self.entries[..sent], terminal)?;
            if accepted > sent {
                return Err(Error::Protocol("append reply".into()));
            }
            if accepted > 0 {
                self.entries.drain(..accepted);
                self.first += accepted as u64;
                self.stats.batches += 1;
                self.stats.entries += accepted as u64;
                backoff = FULL_BACKOFF_START;
            }
            if accepted == sent {
                continue;
            }
            if !blocking {
                self.attempted = true;
                return Ok(());
            }
            self.stats.waits.full_replies += 1;
            let started = Instant::now();
            (self.sleep)(backoff);
            self.stats.waits.waited += started.elapsed();
            backoff = (backoff * 2).min(FULL_BACKOFF_CAP);
        }
        self.attempted = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    thread_local! {
        static SLEEPS: RefCell<Vec<Duration>> = const { RefCell::new(Vec::new()) };
    }
    fn record_sleep(duration: Duration) {
        SLEEPS.with(|s| s.borrow_mut().push(duration));
    }
    fn sleeps() -> Vec<Duration> {
        SLEEPS.with(|s| s.borrow_mut().drain(..).collect())
    }

    /// In-memory queue with the same contract as the Redis script: longest
    /// prefix under the byte limit, sequence and terminal checks, and a
    /// scripted per-call acceptance cap to model consumers freeing space.
    #[derive(Default)]
    struct Fake {
        limit: usize,
        entry_cap: usize,
        retained: usize,
        stream: Vec<String>,
        terminal: Option<u64>,
        /// Per-call caps consumed in order; `None` once exhausted = unlimited.
        caps: Vec<usize>,
        calls: Vec<(u64, usize, bool, usize)>,
        poisoned: bool,
    }
    impl Fake {
        fn new(limit: usize) -> Self {
            Self {
                limit,
                entry_cap: limit,
                ..Self::default()
            }
        }
        fn consume_all(&mut self) {
            self.retained = 0;
        }
    }
    impl StreamSink for Fake {
        fn setup(&mut self, _: &[String], _: usize, _: usize) -> Result<()> {
            Ok(())
        }
        fn append(&mut self, first: u64, entries: &[String], terminal: bool) -> Result<usize> {
            if self.poisoned {
                return Err(Error::Protocol("poisoned".into()));
            }
            if first != self.stream.len() as u64 || self.terminal.is_some() {
                return Err(Error::Protocol("sequence".into()));
            }
            if entries.iter().any(|e| e.len() > self.entry_cap) {
                return Err(Error::Resource);
            }
            let cap = if self.caps.is_empty() {
                usize::MAX
            } else {
                self.caps.remove(0)
            };
            let mut accepted = 0;
            for entry in entries.iter().take(cap) {
                if self.retained + entry.len() > self.limit {
                    break;
                }
                self.retained += entry.len();
                self.stream.push(entry.clone());
                accepted += 1;
            }
            if terminal && accepted == entries.len() {
                self.terminal = Some(self.stream.len() as u64 - 1);
            }
            self.calls.push((first, entries.len(), terminal, accepted));
            Ok(accepted)
        }
        fn progress(&mut self) -> Result<BTreeMap<String, String>> {
            Ok(BTreeMap::new())
        }
        fn poison(&mut self) {
            self.poisoned = true;
        }
    }

    fn entry(sequence: u64) -> Entry {
        Entry {
            sequence,
            record: format!("{sequence:04}"),
            terminal: false,
        }
    }
    fn buffer(fake: Fake, batch: usize) -> PublishBuffer<Fake> {
        let limit = fake.limit;
        PublishBuffer::with_sleep(fake, batch, 4, limit, record_sleep)
    }
    fn expected(n: u64) -> Vec<String> {
        (0..n).map(|s| format!("{s:04}")).collect()
    }

    #[test]
    fn hard_capacity_is_batch_plus_a_quarter_rounded_up() {
        assert_eq!(hard_capacity(100), 125);
        assert_eq!(hard_capacity(1), 2);
        assert_eq!(hard_capacity(4), 5);
        assert_eq!(hard_capacity(5), 7);
        assert_eq!(hard_capacity(1024), 1280);
    }

    #[test]
    fn drain_is_attempted_once_the_batch_fills() {
        let mut b = buffer(Fake::new(1 << 20), 100);
        for s in 0..99 {
            b.insert(entry(s)).unwrap();
        }
        assert!(b.sink.calls.is_empty(), "no backend call below the batch");
        b.insert(entry(99)).unwrap();
        assert_eq!(b.sink.calls, vec![(0, 100, false, 100)]);
        assert_eq!(b.buffered(), 0);
        for s in 100..200 {
            b.insert(entry(s)).unwrap();
        }
        assert_eq!(b.sink.calls[1], (100, 100, false, 100));
        assert_eq!(b.sink.stream, expected(200));
        assert_eq!(b.stats().batches, 2);
        assert_eq!(b.stats().entries, 200);
        assert!(sleeps().is_empty());
    }

    #[test]
    fn partial_acceptance_keeps_the_remainder_without_more_attempts() {
        let mut fake = Fake::new(1 << 20);
        fake.caps = vec![30];
        let mut b = buffer(fake, 100);
        for s in 0..100 {
            b.insert(entry(s)).unwrap();
        }
        assert_eq!(b.sink.calls, vec![(0, 100, false, 30)]);
        assert_eq!(b.buffered(), 70);
        // Back at the batch size again: still no attempt until hard capacity.
        for s in 100..154 {
            b.insert(entry(s)).unwrap();
        }
        assert_eq!(b.sink.calls.len(), 1);
        assert_eq!(b.buffered(), 124);
        assert!(sleeps().is_empty(), "a non-blocking drain never sleeps");
        b.insert(entry(154)).unwrap();
        assert_eq!(b.buffered(), 0);
        assert_eq!(b.sink.calls[1], (30, 125, false, 125));
        assert_eq!(b.sink.stream, expected(155));
    }

    #[test]
    fn full_queue_at_the_batch_defers_to_a_blocking_drain_at_hard_capacity() {
        // Queue holds exactly 100 four-byte entries and nothing is consumed:
        // the first drain fills it, the next attempt happens only at 125.
        let mut fake = Fake::new(400);
        fake.caps = vec![usize::MAX];
        let mut b = buffer(fake, 100);
        for s in 0..100 {
            b.insert(entry(s)).unwrap();
        }
        for s in 100..200 {
            b.insert(entry(s)).unwrap();
        }
        // 200 inserted: the second drain at 200 accepted 0 (FULL), so it is
        // deferred; a non-blocking FULL is not a wait.
        assert_eq!(b.sink.calls, vec![(0, 100, false, 100), (100, 100, false, 0)]);
        assert_eq!(b.stats().waits, QueueWaits::default());
        for s in 200..224 {
            b.insert(entry(s)).unwrap();
        }
        assert_eq!(b.sink.calls.len(), 2);
        assert_eq!(b.buffered(), 124);
        // Reaching 125 blocks. Model consumers freeing the queue only after
        // seven FULL replies; the drain then appends everything buffered.
        b.sink.caps = vec![0, 0, 0, 0, 0, 0, 0];
        b.sink.limit = usize::MAX;
        b.insert(entry(224)).unwrap();
        assert_eq!(b.buffered(), 0);
        assert_eq!(b.sink.stream, expected(225));
        let ms = |n| Duration::from_millis(n);
        // Backoff doubles from 1 ms to the 50 ms cap.
        assert_eq!(
            sleeps(),
            vec![ms(1), ms(2), ms(4), ms(8), ms(16), ms(32), ms(50)]
        );
        assert_eq!(b.stats().waits.full_replies, 7);
        assert_eq!(b.stats().entries, 225);
    }

    #[test]
    fn blocking_drain_resets_backoff_after_partial_progress() {
        let mut fake = Fake::new(1 << 20);
        fake.caps = vec![0, 0, 3, 0, 0];
        let mut b = buffer(fake, 4);
        for s in 0..5 {
            b.insert(entry(s)).unwrap();
        }
        // batch 4 → attempt (cap 0, deferred), capacity 5 → blocking.
        let ms = |n| Duration::from_millis(n);
        assert_eq!(sleeps(), vec![ms(1), ms(1), ms(2), ms(4)]);
        assert_eq!(b.sink.stream, expected(5));
    }

    #[test]
    fn terminal_is_flushed_completely_and_marked_on_the_last_call_only() {
        let mut fake = Fake::new(1 << 20);
        fake.caps = vec![2, 0, 1];
        let mut b = buffer(fake, 100);
        for s in 0..4 {
            b.insert(entry(s)).unwrap();
        }
        b.insert(Entry {
            terminal: true,
            ..entry(4)
        })
        .unwrap();
        assert!(b.sink.calls.is_empty());
        b.flush().unwrap();
        assert_eq!(b.buffered(), 0);
        assert_eq!(b.sink.stream, expected(5));
        assert_eq!(b.sink.terminal, Some(4));
        // Every call carried the tail, so each was flagged; the terminal was
        // set only once the terminal entry itself was appended.
        assert_eq!(
            b.sink.calls,
            vec![
                (0, 5, true, 2),
                (2, 3, true, 0),
                (2, 3, true, 1),
                (3, 2, true, 2)
            ]
        );
        assert!(matches!(b.insert(entry(5)), Err(Error::Protocol(_))));
        sleeps();
    }

    #[test]
    fn insert_rejects_gaps_duplicates_and_oversized_entries() {
        let mut b = buffer(Fake::new(1 << 20), 100);
        b.insert(entry(0)).unwrap();
        assert!(matches!(b.insert(entry(0)), Err(Error::Protocol(_))));
        assert!(matches!(b.insert(entry(2)), Err(Error::Protocol(_))));
        let oversized = Entry {
            sequence: 1,
            record: "12345".into(),
            terminal: false,
        };
        assert!(matches!(b.insert(oversized), Err(Error::Resource)));
        assert_eq!(b.buffered(), 1, "an oversized entry is never buffered");
        assert!(b.sink.calls.is_empty());
    }

    #[test]
    fn calls_are_bounded_by_the_queue_byte_limit() {
        // A 10-byte queue can never accept more than two 4-byte entries in
        // one call, so a call never carries more than the limit admits.
        let mut fake = Fake::new(10);
        fake.entry_cap = 4;
        let mut b = buffer(fake, 3);
        for s in 0..3 {
            b.insert(entry(s)).unwrap();
        }
        assert_eq!(b.sink.calls, vec![(0, 2, false, 2), (2, 1, false, 0)]);
        b.sink.consume_all();
        b.flush().unwrap();
        assert_eq!(b.sink.stream, expected(3));
        sleeps();
    }

    #[test]
    fn batch_of_one_publishes_each_entry_in_its_own_call() {
        let mut b = buffer(Fake::new(1 << 20), 1);
        for s in 0..5 {
            b.insert(entry(s)).unwrap();
            assert_eq!(b.buffered(), 0, "published before the next insert");
        }
        let calls: Vec<_> = (0..5).map(|s| (s, 1, false, 1)).collect();
        assert_eq!(b.sink.calls, calls);
        assert_eq!(b.stats().batches, 5);
        // On a full queue the entry waits; the next insert blocks until both
        // are published, in order.
        b.sink.caps = vec![0, 0, 1, 1];
        b.insert(entry(5)).unwrap();
        assert_eq!(b.buffered(), 1);
        b.insert(entry(6)).unwrap();
        assert_eq!(b.buffered(), 0);
        assert_eq!(b.sink.stream, expected(7));
        sleeps();
    }

    #[test]
    fn sink_errors_propagate_and_write_nothing_more() {
        let mut b = buffer(Fake::new(1 << 20), 2);
        b.insert(entry(0)).unwrap();
        b.insert(entry(1)).unwrap();
        b.sink.poison();
        b.insert(entry(2)).unwrap();
        assert!(matches!(b.insert(entry(3)), Err(Error::Protocol(_))));
        assert_eq!(b.sink.stream, expected(2));
    }
}
