# Sealed Capture Pipeline V1

Status: design decision, intentionally staged. This document does not authorize a
single large implementation.

The approved storage-format addendum is
[`ZSTD_MATERIALIZATION_PIPELINE_V1.md`](../encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md).
It refines §§5–6 with the two staged changes for compressed raw archives and
compressed canonical outputs; this document remains authoritative for capture,
ordering, lane classification, and deletion eligibility.

This design supersedes the recommendation in `Architecture_refinments.md` to use
`monotonic_ns` as the normal cross-lane ordering key. Envelope v2 still records
`monotonic_ns`, but V1 orders captured evidence by `visible_ns`.

## 1. Decision

For a deployment in which every splice runs in a container on one Linux host, the
canonical merge key is:

```text
(visible_ns, lane_rank, delivery_index)
```

The initial lane ranks are:

| Rank | Lane |
|---:|---|
| 0 | `polymarket` |
| 1 | `polymarket_snapshots` |
| 2 | `polymarket_sports` |
| 3 | `polymarket_rtds` |
| 10 | `kalshi` |
| 20 | `limitless` |

This makes Polymarket the deterministic winner only when two records have exactly
the same `visible_ns`. It does not move a Polymarket record ahead of a Kalshi
record with an earlier timestamp. Future twin lanes must receive distinct stable
lane ranks.

Lane rank is a serialization rule, not evidence that one venue moved first. The
canonical file needs a total order so that its bytes and `EvidenceSeq` are
reproducible, but analysis must treat records from different lanes with equal
`visible_ns` as a capture-time tie. It must not derive positive or negative
lead-lag from their rank-imposed order.

`delivery_index` is dense and unique within a lane. A repeated or decreasing
value within a lane is invalid input, not another tie to resolve.

### Why this is sufficient for V1

- All capture containers on one machine observe the same host
  `CLOCK_REALTIME`.
- `visible_ns` survives process and machine restarts in the sense relevant to
  ordering; unlike `CLOCK_MONOTONIC`, it does not reset to a new boot-relative
  origin.
- A nanosecond unit does not guarantee nanosecond clock resolution, but equal
  readings are harmless because the lane rank and `delivery_index` produce a
  total deterministic order.
- The current evidence does not show a wall-clock problem. Across 4,368 captured
  records (3,827 Polymarket and 541 Limitless), there are zero `visible_ns`
  regressions and zero repeated values. The two Limitless files also remain
  increasing when joined by `delivery_index`.
- A later authenticated Kalshi probe captured 46,446 records over 68.6 seconds
  with zero `visible_ns` regressions or repeated values. Its timestamps were
  microsecond-quantized (`visible_ns % 1000` was constant), despite being stored
  in nanosecond units. This is an observation from the macOS development host,
  not a guarantee about the Linux deployment clock.

The original Polymarket and Limitless captures are envelope v1 and contain no
`monotonic_ns`. None of the captures provide an empirical reason to make
monotonic time or a capture epoch part of the authoritative order. Repeat the
clock-resolution and collision measurements on the Linux deployment host before
freezing analytical skew thresholds.

### What `monotonic_ns` is for

Envelope v2 continues to record `monotonic_ns` and the Linux boot scope. V1 uses
it for diagnostics:

- measuring local intervals without wall-clock steps;
- detecting a discontinuity in `visible_ns - monotonic_ns` within one boot;
- measuring scheduling and fsync stalls;
- forensic comparison between lanes sharing a boot scope.

It is not a merge key. No `capture_epoch` is introduced in V1.

## 2. The clock invariant

A k-way merge is correct only when every input lane is already sorted by its
merge key. Before sealing a lane segment, validate:

```text
visible_ns[n] >= visible_ns[n - 1]
delivery_index[n] == delivery_index[n - 1] + 1
```

The writer also performs the `visible_ns` comparison as each record arrives and
raises an external high-priority alert immediately on a regression. This online
check reduces detection latency; it does not replace seal-time revalidation and
does not silently repair, reorder, or discard the record. Without an explicit
and tested early-roll design, online detection does not reduce the segment that
must be treated as uncertified.

At a restart, also validate the first new `visible_ns` against the last sealed
value for that lane. A durable filesystem does not itself make time walk
backwards. If the host clock does move backwards across a restart, this boundary
check makes it explicit.

If a regression occurs:

1. Preserve and archive the raw segment.
2. Mark the segment `ordering_status = visible_clock_regression`.
3. Exclude that lane from the certified merge for the affected window and
   record it as `lane_invalid`.
4. Alert with the previous/current clock values, lane, boot scopes and delivery
   indexes.

After the finalization deadline, the other lanes may advance through an
explicitly incomplete window under §5. The regressed lane is not silently
reordered into it.

V1 must not silently switch clocks or sort away this evidence. A fallback clock
scheme should be designed from the observed failure, if one occurs.

Reconsider an epoch, hybrid logical clock, or external sort only if at least one
of these becomes real:

- a `visible_ns` regression is captured;
- capture is distributed across multiple hosts;
- clock collisions are frequent enough to make lane-rank ordering analytically
  material;
- measured inter-container clock behaviour is outside the analysis tolerance.

## 3. Thirty-minute lane segments

Each splice owns one lane and one writer. It writes UTC-aligned 30-minute
segments:

```text
spool/lane=<lane>/date=<YYYY-MM-DD>/
  <window-start>-<segment-id>.ndjson.open
```

Connection epochs remain envelope fields; reconnecting does not require changing
the lane or its `delivery_index`. A segment may therefore contain several
connection epochs.

At the boundary, the writer:

1. stops assigning records to the old segment;
2. flushes and fsyncs the old data file;
3. atomically renames `.ndjson.open` to `.ndjson`;
4. writes and fsyncs the seal sidecar through a temporary file plus atomic
   rename;
5. fsyncs the containing directory.

The seal is the commit marker. The existence of an `.ndjson` file without its
valid seal never makes it eligible for merge or archive.

Quiet lanes emit an empty sealed segment. This lets the finalizer distinguish
"the lane had no records" from "the lane has not finished this window." The
deployment manifest defines which lanes are expected; a disabled Kalshi profile
is not waited on.

### Seal contents

At minimum:

```json
{
  "seal_version": 1,
  "lane_id": "polymarket",
  "window_start_ns": 0,
  "window_end_ns": 0,
  "data_file": "...ndjson",
  "byte_length": 0,
  "line_count": 0,
  "sha256": "...",
  "first_delivery_index": null,
  "last_delivery_index": null,
  "first_visible_ns": null,
  "last_visible_ns": null,
  "visible_non_decreasing": true,
  "delivery_index_dense": true
}
```

The hash is maintained incrementally while writing, so sealing does not reread a
large file.

## 4. Keeping fsync away from the socket

The network task timestamps and serializes each delivery, then places the
immutable record into a bounded per-lane queue. A dedicated writer drains that
queue and owns all file operations. During an fsync or segment roll, the socket
task can continue receiving into the queue instead of waiting on disk latency.

The queue is a short latency absorber, not durable storage. Its size should be
chosen from measured peak records/second and fsync p99, with high-water and
full-queue metrics. It must never discard records.

The queue remains bounded. Producers use a blocking/awaiting put, so a full queue
applies backpressure while preserving every record already accepted into the
process. Queue fullness by itself must not trigger a venue reconnect: reconnecting
creates an unobservable loss window on an unsequenced venue. A sustained full
queue is instead a storage-path outage. It raises an external alert and remains
backpressured until storage recovers or the process fails explicitly; reconnecting
the venue socket cannot repair a disk failure.

An unbounded in-memory queue is not a durability mechanism. At the measured
Kalshi rate it can turn sustained disk latency into an OOM kill and the same
capture gap, only less predictably. V1 has one host and no separately healthy
durable medium, so there is no spillover path and none should be implied.

### The stall budget

Backpressure is bounded, not a safe steady state. End-to-end tolerance for a
storage stall is the kernel receive buffer plus the writer queue: the socket task
stops reading, the kernel buffer absorbs what arrives, and once it fills TCP
advertises a zero window and the venue sees a slow consumer.

At the steady Kalshi rate (~569 KB/s, §4 sizing below) a 6 MB `tcp_rmem` ceiling
absorbs roughly **10–11 seconds** of stall before that point. That is the budget:
the alert raised on a sustained full queue has about ten seconds to matter, and
past it a venue-initiated disconnect is the expected outcome rather than a
surprise. It must be recorded as an ordinary fault record on the tape, and the
normal reconnect path handles it.

Sizing the queue larger extends this budget linearly and costs only memory; it
does not change the shape of the failure.

A rotation barrier travels through the same FIFO queue. Records before the
barrier belong to the old segment and records after it belong to the new one, so
fsync cannot reorder delivery.

### Measured Kalshi sizing input

The authenticated probe measured approximately 677 records/second and 475 KB/s
at 788 targets. Straight-line decimal projections are:

```text
30-minute segment    0.855 GB    1.219M records
one hour             1.710 GB    2.437M records
one day              41.040 GB   58.498M records
```

These are capacity-planning projections from a 68.6-second development probe,
not retention guarantees. They must be re-measured on Linux over a longer
representative window before choosing queue capacity, disk size, upload
concurrency, or alarms.

## 5. What the ingester produces

The ingester has two separate jobs:

### Provisional validation

Every 5–10 minutes it may tail active files to report malformed envelopes,
counter breaks and capture health. Results from an unsealed file are provisional
and cannot advance the final global sequence.

### Finalization of sealed windows

For each 30-minute window, it:

1. waits for one valid seal from every expected lane until a configured,
   finite finalization deadline;
2. verifies file length, line count and SHA-256;
3. validates closed envelope schemas and per-lane ordering invariants;
4. performs a k-way merge using
   `(visible_ns, lane_rank, delivery_index)`;
5. assigns `EvidenceSeq` in that merged order;
6. writes a deterministic canonical evidence file, provenance index and
   completion receipt.

If the deadline expires, the finalizer still commits the available lanes. Its
receipt is marked `incomplete` and distinguishes:

```text
lane_missing     no seal arrived
lane_invalid     a seal or segment failed validation
```

The receipt records the expected, present, missing and invalid lanes plus the
deadline used. An incomplete canonical window is usable evidence with explicit
coverage limits; it must never be presented as a complete cross-venue window.
This prevents one wedged splice from halting finalization for every healthy
venue.

A seal that arrives after an incomplete window is committed is archived as raw
evidence and labelled `late_after_finalization`. It must not be inserted into the
existing canonical file or renumber already committed `EvidenceSeq`. Incorporating
it later requires a new versioned correction dataset with an explicit parent
manifest; the original canonical object remains immutable.

Here "canonical evidence" does not mean a normalized order-book event. The
canonical data file is the original envelope lines, copied byte-for-byte into
global order. Its line ordinal is `EvidenceSeq`. A separate index binds each
ordinal to:

```text
lane_id
source_segment_sha256
source_line_number
record_id
content_hash
continuity_verdict
```

This makes the canonical sink lossless and replayable while keeping venue-payload
interpretation in replay. Normalized book deltas, trades and snapshots are
derived events and should not replace this evidence file.

The provenance index also records a `visible_tie_group` (or equivalent nullable
group identifier) for records from different lanes sharing the same
`visible_ns`. The physical line order inside that group follows `lane_rank`, but
analysis treats the group as simultaneous at capture-clock resolution.

The current ingester's filename-by-filename `EvidenceSeq` is not this canonical
order. It can remain available during migration, but must be labelled
`file_order` and must not be used as event time.

### What `EvidenceSeq` is not

The canonical order needs the same guard, and needs it more, because a globally
sequenced content-addressed file looks authoritative in a way a filename order
does not.

> `EvidenceSeq` is capture observation order at one host. It is not venue event
> order and V1 does not claim to recover one. Two venues may act on the same
> world event milliseconds apart and be recorded in the opposite order by routing
> and stamping alone. Cross-venue precedence claims are bounded by this and by
> unmeasured capture jitter; the tolerance is deliberately undefined until
> production infrastructure supplies it.

Recovering true event order would require timestamping at or below the socket —
kernel or NIC timestamping, or a lower-level capture runtime. That is not
justified before there is load data to size it against, and no interim estimate
of the jitter should be invented to stand in for the measurement.

## 6. Archiving and local deletion

The archiver consumes only sealed segments. It uploads the data file and seal,
verifies object length and SHA-256, then atomically writes a local archive
receipt. S3 object keys are immutable; retrying the same content is idempotent.

Local deletion is a separate reaper action and requires both:

- a verified archive receipt for the raw segment; and
- an ingest receipt proving that the segment is present in a completed canonical
  window/day.

Raw objects remain in the archive for the chosen 5–7 day investigation period.
They can be expired only after a successful replay/canonicalization receipt.
Canonical evidence and its provenance index have the longer retention policy.

The archiver runs an hourly sweep, but the upload unit remains one sealed
30-minute segment. In a healthy hour it therefore uploads two independent data
objects plus their seals per lane; it does not concatenate them before upload.
Objects live under a daily prefix, and a daily manifest gives replay one logical
dataset without trying to append to an S3 object. If one physical daily object is
important, compact it only after the UTC day closes and verify it before
expiring its segment inputs.

## 7. Recovery and watermarks

The finalizer processes UTC windows in order. It does not finalize a later window
while an earlier one is still inside its seal-wait deadline. Once that deadline
has produced either a complete or explicitly incomplete receipt, the durable
watermark may advance. The watermark contains the last completed window, its
completeness verdict, final `EvidenceSeq`, canonical hashes and all present source
segment hashes.

After a crash:

- `.open` files are repaired only to the last complete newline;
- an unsealed prior window is sealed with a recovery reason after validation;
- already receipted segments are idempotently skipped;
- a boot change is recorded but does not alter the merge key;
- the visible-time boundary invariant is checked before the watermark advances;
- a late segment cannot mutate a completed window and follows the correction
  policy in §5.

Thus a reboot cannot make the merge "walk back" unnoticed. Either visible time
continues and the merge proceeds, or a real regression is detected and the
certified watermark stops.

## 8. Implementation sequence and proof gates

Implement this in small changes:

1. **Ordering tests first.** Construct overlapping lane files whose filenames
   disagree with `visible_ns`. The current ingester must fail the expected
   interleaving before the k-way implementation is accepted.
2. **Segment writer and seals.** Add the bounded writer queue, aligned rotation
   and sidecar validation without changing ingest output.
3. **Sealed-window finalizer.** Add the visible-time k-way merge and canonical
   receipts behind a separate command/mode.
4. **Archiver and reaper.** Implement
   [`PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](../archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md). Prove
   failed upload/checksum verification retains local files; prove deletion
   requires both receipts and an independently durable archive backend.
5. **Daily replay input.** Consume canonical evidence, leaving raw-segment replay
   as the short-retention recovery path.

Required false/failure tests include:

- filenames ordered A then B while timestamps require A1, B1, A2;
- identical timestamps resolve Polymarket, then Kalshi, then Limitless;
- analysis treats that deterministic cross-lane ordering as a tie, not lead-lag;
- equal timestamps on `polymarket` and `polymarket_snapshots` follow their
  declared ranks while remaining one analytical tie group;
- a decreasing `visible_ns` prevents certification;
- a decreasing `visible_ns` raises an online alert before seal-time validation;
- a reset `monotonic_ns` after reboot does not affect a valid visible merge;
- a missing lane seal blocks finalization before the deadline, then produces an
  immutable incomplete receipt with `lane_missing`;
- a segment arriving after that receipt does not change its canonical hash or
  assigned `EvidenceSeq`;
- a changed byte after sealing fails the hash;
- a full writer queue applies backpressure without dropping a record or
  initiating a venue reconnect;
- an upload failure or checksum mismatch prevents local deletion;
- retry after a crash produces the same canonical hashes and `EvidenceSeq`.

No phase should be merged on architectural confidence alone; its test must first
demonstrate the incorrect or unsafe behaviour it replaces.


---

# Review disposition (2026-07-30)

Reviewed against the live system rather than on paper. The normative sections
above incorporate every accepted finding; this section records what was raised,
what became of it, and what remains open. Closed items are kept as one line so
the reasoning behind a section is recoverable without re-deriving it.

The core decision — `visible_ns` as the merge key with `monotonic_ns` demoted to
diagnostics — was reviewed and stands. It is now supported by roughly ten times
the evidence originally cited.

## Closed

| # | Finding | Disposition |
|---|---|---|
| R1 | §1's evidence was 4,368 records from a macOS host with no Kalshi lane | **Resolved.** §1 carries the 46,446-record authenticated measurement, the microsecond-quantization observation, and an explicit instruction to re-measure on Linux. Sizing moved into §4. |
| R2 | Lane rank silently readable as venue precedence | **Resolved, and improved on.** §1 states rank is a serialization rule; §5 adds `visible_tie_group` to the provenance index, so the constraint is machine-readable rather than prose. |
| R3 | Bounded queue reconnecting on fullness | **Resolved against the review.** The recommendation to make the queue unbounded was wrong: at the measured rate a sustained stall becomes an OOM kill, losing the buffered records too and less predictably. §4's blocking-put backpressure with no venue reconnect is the better design. |
| R4 | One unresolved lane halting all finalization | **Resolved, and extended.** §5 adds a finite deadline, `lane_missing`/`lane_invalid` verdicts, and — beyond what was asked — a `late_after_finalization` policy that keeps committed canonical output immutable. |
| R5 | §3 and §6 disagreed on segment granularity | **Resolved.** §6 fixes the upload unit at one sealed 30-minute segment with an hourly sweep and no concatenation. |
| R6 | Clock invariant validated only at seal time | **Resolved, with a correction to the review.** §2 adds the online check for alert latency and correctly notes that, absent a tested early-roll design, it does not shrink the uncertified segment. |
| R7 | §8 missing tie-bias and same-venue-lane tests | **Resolved.** Both added, along with tests for the online alert, incomplete-receipt immutability and queue backpressure. |
| R8 | Sections accepted without reservation | Unchanged: §5's provisional/final split and byte-for-byte canonical evidence, §6's two-receipt deletion rule, §7's restart boundary check, §8's staging discipline. |

### Corrections to the review's own numbers

The first pass reported 66.9M records/day and 48.1 GB/day. Both were wrong — a
68.6-second probe was divided by 60. §4's figures are the correct reading of the
same capture.

Two refinements to §4's derivation, from re-measuring the same probe:

```text
full-span mean (68.6 s)   677 rec/s    58.5M records/day
middle 80% (steady state) 811 rec/s    70.0M records/day
```

The whole-probe mean is diluted by connection setup and the initial 788-market
snapshot burst. Since §4's numbers size queue capacity, disk and alarms — all of
which follow sustained peak rather than mean — **the steady-state rate is the
planning figure**, about 20% above the value currently recorded.

Separately, `41.040 GB` treats `du -k` KiB as if they were kB. The correct daily
figure is approximately **42.0 GB**.

## Withdrawn

Raised in a second pass and withdrawn on the author's reasoning, recorded so they
are not raised again:

- **TCP backpressure instability.** The kernel receive buffer absorbs the stall
  §4 is designed for. The objection only applies past that buffer's depth, which
  §4 already classifies as a storage-path outage rather than a venue problem.
- **Cross-window detection of a chronically excluded lane.** This is a
  monitoring system, not a capture pipeline. The §5 receipt already carries
  expected, present, missing and invalid lanes plus the deadline, which is
  everything an external watcher would need.
- **A defined cross-lane jitter tolerance.** Unmeasurable before production
  infrastructure exists, and any number chosen now would be fabricated. The
  eventual answer is timestamping at or below the socket in a lower-level
  runtime, which is not justified until load data exists. Deriving it instead
  from a twin-lane experiment would add a second subscription slot and
  arbitration machinery to a one-lane reference build.

## Open

None. O1–O3 were applied to the normative sections on 2026-07-30:

| # | Finding | Applied as |
|---|---|---|
| O1 | Stall budget unstated | §4 "The stall budget" — kernel `rcvbuf` plus queue, ~10–11 s at the steady Kalshi rate, with a venue-initiated disconnect named as the expected outcome past it |
| O2 | `EvidenceSeq` readable as event time | §5 "What `EvidenceSeq` is not" |
| O3 | Unsatisfiable spillover clause | Deleted from §4; replaced with the statement that V1 is single-host and has no spillover path |

## Implementation status

§8 phases 1 and 2 landed 2026-07-30; phase 3 on 2026-07-31.

### Phase 2 — segment writer, bounded queue, seals

- `splices/common/segment.py` owns one segment and its seal: incremental sha256
  and counters maintained as bytes are written, and the five-step commit with a
  **second directory fsync** between the rename and the seal write. The spec's
  single trailing fsync leaves a window where a crash makes the seal durable
  while the rename is not, naming a file that does not exist. Directory fsync
  errors propagate: on the Linux deployment target an unsuccessful directory
  sync is an unsuccessful seal, never a warning hidden behind a commit marker.
- `splices/common/writer.py` is the bounded queue, one drain coroutine and one
  writer thread. A thread rather than a coroutine because `os.fsync` inside a
  coroutine blocks the loop just as the old inline write did, which is the whole
  thing §4 asks to avoid. The rotation barrier travels in-band through the same
  FIFO so a boundary cannot overtake writes belonging before it.
- `Spool` is now a facade; `close()` stays **synchronous** because it runs inside
  a `finally` during cancellation, where awaiting is unreliable. `run()` seals
  there, which is why every existing cancel-then-read test passes unchanged.
- `resume_state` reads the newest seal instead of rescanning every file, and
  recovery-seals any orphaned `.open`. The spool docstring's argument against a
  sidecar is answered rather than deleted: a seal is written only after its data
  is durable, is immutable and per-segment, and carries a digest — so the tape
  stays the authority and the seal is a falsifiable index over it.
- Lane cutover `venue=` → `lane=` across the writer, the Rust discovery, and
  replay. The two byte-identical `_lane_of` copies are gone, replaced by
  `replay/lanes.py`, which **raises** instead of falling back to the parent
  directory. That fallback never failed and therefore never surfaced a layout
  change: under `lane=` it returned a *per-date* lane, so one lane spanning
  midnight became two and a `delivery_index` gap across the boundary was
  invisible.
- Gate 1 gains `every_segment_is_sealed`, verifying each seal's length and digest
  against `build_input_manifest`'s independently computed values.
- The Rust ingester now treats the sidecar as the same commit marker as Python:
  missing seals remain invisible while malformed, mismatched or hash-invalid
  seals fail closed. Watch mode hashes a segment once before its first ingest in
  that process, not on every five-second discovery poll.
- Batch writes use an unbuffered file handle and remember the accumulator's last
  committed byte offset. If an OS write places a prefix and then fails, the
  writer truncates and fsyncs back to that offset before retrying; if rollback
  itself fails, it poisons the segment and refuses to seal.
- `BaseSplice` raises a structured critical log as soon as `visible_ns` moves
  backwards, with an injectable callback for an external alert transport. The
  record is still preserved, and the segment writer independently records the
  seal-time `visible_clock_regression`.

**Six defects closed by executable proofs.**
`tests/test_sealed_capture_failure_proofs.py` asserted the safe behaviour rather
than the implementation, and each proof is now a permanent regression test:

| Defect | Why it mattered |
|---|---|
| A write error sealed past an accepted record | The queue had already counted it into `delivery_index`; the seal published a digest and line count omitting it — silent loss wearing a commit marker. The batch is now retained and `close()` refuses to seal while anything is unwritten. |
| A delayed timer misfiled a post-boundary record | Its seal then asserted a window range not containing its own records. Rotation now follows the record's own `visible_ns`; the timer is only a liveness mechanism for quiet lanes. |
| A restart whose clock stepped backwards read as a clean start | The first record of a segment had nothing to compare against. The previous window's last receive time now seeds the check, which is what §2's restart boundary asks for. |
| A renamed-but-unsealed segment counted as evidence | The sidecar, not the suffix, is the commit marker. `sealed_segments` now means sealed. |
| That segment then stayed orphaned forever | Recovery only looked at `.ndjson.open`. It now also seals a renamed file missing its sidecar — the window between §3's steps 3 and 4. |
| Recovery renamed a complete orphan before fsyncing it | A dead process's bytes can sit entirely in page cache, so the seal published a digest for content a second crash could still lose. |

**Final Phase-2 review gates.** Four additional executable proofs close the
handoff to Phase 3:

| Gate | Proof |
|---|---|
| The ingester requires the commit marker | An unsealed `.ndjson` commits zero rows; a valid seal makes it eligible; a mismatched seal or changed byte fails ingestion. |
| A partial OS write is retry-safe | The test writes a real prefix, raises, retries, and asserts the physical bytes, length, line count and SHA-256 all agree. |
| Directory durability failure is fatal | Injecting a directory `fsync` error prevents `seal()` from returning success. |
| Clock regression alerts online | The injected alert receives previous/current visible time, delivery indexes, connection epochs and available boot scope before the segment is sealed. |

The second of these also corrected a test fixture that had been lying: the spool
tests ran a writer clock at 1970 against records timestamped 2023. With rotation
driven by receive time that mismatch became visible immediately, because each
record correctly opened the window its own timestamp belonged to.

**One bug found by running it, not by reading it.** A restart inside a window
opens a second segment for that window, and with only a random id distinguishing
them the two sorted arbitrarily. A live SIGKILL-and-restart produced
`delivery_index` reading 1..3, 300..549, 4..299, 550..600 in file order — which
breaks the one ordering guarantee this layout does make, that within a lane file
order *is* receive order. Segment filenames now carry a zero-padded index read
off disk, so a new process sees a dead one's work. Pinned by
`tests/test_spool.py::test_two_segments_in_one_window_still_sort_into_capture_order`.

### Phase 3 — the sealed-window finalizer

`indexer-finalize` is a separate binary sharing the ingester image. Over the
identical `interleaved_fixture` bytes the two commands now assign two orders,
each honestly labelled:

```text
indexer-ingest    [100, 300, 200]   file_order
indexer-finalize  [100, 200, 300]   EvidenceSeq  (visible_ns, lane_rank, delivery_index)
```

- **`indexer-segment`** holds seal decoding, validation and discovery, so the two
  binaries cannot come to hold different opinions about whether a file is
  committed.
- **Two registries, not one.** `LANE_RANKS` is every lane this build can *rank*;
  the lanes a deployment *expects* come from `--expect-lane`, per §3's "the
  deployment manifest defines which lanes are expected". Conflating them marked
  every default-profile window incomplete with three phantom `lane_missing`
  entries.
- **`--window-seconds` is the authority for window bounds.** Seals declare their
  own, but a declaration is not an authority: a torn one leaves no end, a stray
  longer one re-tiles the day and hides a real window, and a seal naming a window
  its records fall outside of would otherwise validate. Bounds are computed from
  the aligned start; the filename places a segment and the seal is checked
  against it.
- **Canonical output is files**, committed with the same discipline as a seal.
  The receipt is the commit marker; `store.db` is untouched and `file_order`
  stays available (§5).
- **Continuity verdicts** ride in the provenance index. The classifier gate moved
  from `indexer-store`'s concrete receipts to a `Positioned`/`Committed` contract
  in `indexer-types`, because the canonical write *is* a receipt — the bytes are
  down and the position assigned. That removed `indexer-continuity`'s dependency
  on the store rather than adding one, so the finalizer classifies without
  linking SQLite. Ordering state crosses windows via the watermark; identity is
  window-scoped, since carrying it would mean holding every record id in the
  retention period.
- **The watermark** (§7) is a derived index over receipts: deletable, rebuilt
  byte-identically, and checked against the newest receipt on load.

**A seal is a claim, and the reader is where it is falsified.** The digest
proves the bytes have not changed; it proves nothing about whether the seal's
*summary* of them is true. `first_visible_ns`, `last_delivery_index`,
`delivery_index_dense` and the window bounds are assertions the writer made, and
a segment whose records sit outside the window it declares hashes exactly as well
as one whose records do not. Every record is therefore reconciled against the
seal and the window while it streams, which is the only place the actual values
are in hand.

**§7's boundary invariant is enforced upstream, and by that check.** With records
themselves validated against their window, window N holds only instants below
`N.end` and N+1 only instants at or above `N+1.start`, so consecutive windows
cannot overlap and a lane whose clock stepped back is `lane_invalid` before the
merge sees it. `watermark::clock_faults` keeps the comparison as defence in depth
and is unit-tested directly.

**A window behind the watermark is refused before anything is written.** Every
position after it is already assigned, so committing it would run the canonical
sequence backwards in visible time; §5's correction policy owns that case.
Receipt sequence ranges are validated as an unbroken chain on watermark rebuild.

**One writer per canonical root**, held as a lease for the length of a run. Two
finalizers share every intermediate filename and race over the same receipts.

**Thirteen defects closed across two review rounds**, each reproduced by probe
before being fixed and each now a permanent test. Two themes ran through nearly
all of them: a fault attributable to one lane must not cost the whole window, and
anything that cannot be established must fail closed rather than be guessed past.
The full disposition is in the phase 3 plan.

Two further defects were found by building rather than by review: a window that
had not yet *ended* could finalize if every lane happened to have sealed (a
crash mid-window leaves a recovery seal, and the restart opens a second segment
— committing early would push every later record onto the `late_after_finalization`
path §5 forbids); and retiring an epoch when its lane fell *silent* rather than
when it reconnected made the window after a quiet one read `bootstrap` where it
should read `continuous`.

### Phase 1 — the ordering characterisation test

- `ingester/crates/cli/tests/ordering.rs` pins the present cross-lane order as a
  characterisation test: a record received between two records of another lane is
  sequenced after both (`FILE_ORDER = [100, 300, 200]` where the clock demands
  `MERGED_ORDER = [100, 200, 300]`). Before this, nothing in either language
  constrained ordering, so the merge could have landed with every suite green.

  These assertions are **permanent, not temporary**. §8.3 puts the finalizer
  behind a separate command/mode and §5 keeps this path available as
  `file_order`, so phase 3 does not invert them — it adds its own test asserting
  `MERGED_ORDER` over the identical fixture, which `interleaved_fixture` exists
  to supply verbatim.
- The stale ordering rationale is relabelled `file_order` in
  `crates/cli/src/main.rs` (module header and `discover_spool_files`),
  `crates/types/src/sequence.rs`, and `crates/store/src/lib.rs::capture_raw`. The
  module header previously recommended `monotonic_ns` for lead-lag, which §1
  supersedes.
- `crates/continuity/src/state.rs::LaneAuthority` notes that its "lane" means
  `(venue, stream)` and is not the capture lane this document ranks — the names
  collide and the types must not.
