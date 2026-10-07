# Ingester and finalizer

Rust workspace that consumes sealed capture segments, assigns global order,
classifies continuity, and materializes canonical evidence. It has no network
access and no venue-specific behaviour beyond labelled envelope fields: it
validates envelopes, sequences evidence, classifies continuity, and writes the
original envelope lines. It does not normalize payloads or books; that is replay.

The splices and this workspace communicate only through files (sealed segments
under the spool root). A splice crash cannot corrupt the sequence and an ingester
crash cannot cost a frame. Persisted formats (seal, canonical window, receipt,
provenance, watermark, ingest partitions) are in [`FORMATS.md`](FORMATS.md).

## Binaries

| Binary | Purpose |
|---|---|
| `indexer-ingest` | Reads sealed segments in filename order into a SQLite fact store; assigns `file_order`; classifies continuity |
| `indexer-finalize` | Merges sealed UTC windows into canonical evidence (`EvidenceSeq`), provenance and a receipt |
| `indexer-canonical-audit` | Re-verifies every committed canonical window against its receipt |
| `indexer-store-reap` | Audits or removes expired closed ingest-store databases |

```bash
indexer-ingest <spool-root> <store-dir> [--check-integrity] [--watch-interval-seconds N]

indexer-finalize <spool-root> <canonical-root> --expect-lane <lane> [--expect-lane ...] \
    [--window-seconds 1800] [--finalization-deadline-seconds 300] \
    [--interval-seconds N] [--report PATH]
indexer-finalize --print-lane-ranks

indexer-canonical-audit <canonical-root>

indexer-store-reap <store-root> [--retention-hours 24] [--mode audit|delete] [--report PATH]
```

Compose runs these as `ingester`, `ingester-integrity`, `ingest-store-reaper`,
`finalizer`, `finalizer-once` and `canonical-integrity`. Build and test:

```bash
cargo test --manifest-path ingester/Cargo.toml --workspace
cargo clippy --manifest-path ingester/Cargo.toml --workspace --all-targets --all-features -- -D warnings
```

## Crates

| Crate | Owns |
|---|---|
| `types` | Envelope parser (closed, per version), venue/stream/kind vocabularies, identity/content hashes, `EvidenceSeq` / `CanonicalSeq` positions |
| `segment` | Seal decoding and validation, segment discovery; the one definition of "committed segment" shared by both binaries |
| `store` | SQLite fact store: raw capture, facts, exact record-identity index, spool cursors, consumed-segment ledger, integrity and recovery |
| `continuity` | Identity verdict, epoch health, cursor continuity |
| `finalize` | Window assembly, k-way merge, lane ranks, canonical writer, watermark, root lease, canonical audit and audited reader |
| `cli` | The four binaries, ingest partitioning and the ingest-store reaper |

The shared Zstandard codec is `encoder/rust` (see `encoder/README.md`).

## Sealed segments are the input

Only an immutable `.ndjson` with a matching valid `.seal.json` is eligible; open or
unsealed data is invisible. A malformed, mismatched or hash-invalid seal fails
closed. The validator checks seal version, lane and `data_file` against the path,
window ordering, required fields, `ordering_status` against
`visible_non_decreasing`, byte length, SHA-256, line count and trailing newline.
Hashing is cached per process (watch mode hashes a segment once before first
ingest, not on every poll). A seal is a claim: the finalizer additionally reconciles
every record against the seal and the window while streaming, because a segment
whose records sit outside its declared window hashes as well as one whose do not.

## Orderings

Two global orders exist over the same bytes and each is labelled:

```text
indexer-ingest    file_order   filename order, each file consumed whole
indexer-finalize  EvidenceSeq  (visible_ns, lane_rank, delivery_index)
```

`file_order` is not time: a record received between two records of another lane is
sequenced after both (pinned by `crates/cli/tests/ordering.rs` and
`crates/finalize/tests/ordering.rs` over the same fixture). Never use it for
cross-venue analysis.

### The canonical merge key

`indexer-finalize` merges each sealed window across lanes on
`(visible_ns, lane_rank, delivery_index)`, with the lane name as the final tie term
so discovery order never decides bytes. `monotonic_ns` is diagnostic and
boot-scoped and is not a merge key; no capture epoch exists.

| Rank | Lane |
|---:|---|
| 0 | `polymarket` |
| 1 | `polymarket_snapshots` |
| 2 | `polymarket_sports` |
| 3 | `polymarket_rtds` |
| 10 | `kalshi` |
| 20 | `limitless` |

A lane absent from the table ranks 1000 (above all known lanes). `replay/lanes.py`
mirrors this table and a replay test fails if they drift. `--print-lane-ranks`
prints it.

Rank is a serialization rule, not evidence that a venue moved first. It decides
only equal-`visible_ns` ties and never moves a Polymarket record ahead of an
earlier Kalshi record. Provenance carries a `visible_tie_group` for records of
distinct lanes sharing a `visible_ns`; analysis must treat the group as
simultaneous at capture resolution and derive no lead-lag from rank order.

`EvidenceSeq` is capture observation order at one host. It is not venue event
order: two venues acting on the same world event may be recorded in the opposite
order by routing and stamping alone.

The merge requires each lane's input already sorted. A lane is `lane_invalid` for
the window (excluded, never reordered or repaired) when a segment's seal declares
`ordering_status = visible_clock_regression`, or when a record contradicts the
seal's claims while streaming: `visible_ns` outside the window, a decrease or
non-dense `delivery_index` where the seal declared otherwise, or more records than
`line_count`.

## Finalization

`--expect-lane` is the deployment's declaration of which lanes are expected and is
required; the build only knows which lanes it can rank. A typo is rejected at
startup (it would wait out every deadline and commit incomplete forever). The list
is recorded verbatim in each receipt.

For each window the finalizer:

1. Does not finalize a window that has not ended, even if every lane has sealed (a
   crash mid-window leaves a recovery seal and the restart opens a second segment).
2. Waits for a valid seal from every expected lane until `window_end +
   --finalization-deadline-seconds` (default 300). A window inside its deadline
   blocks every later window.
3. After the deadline, commits the available lanes. The receipt is `incomplete` and
   lists `missing_lanes` (`lane_missing`: no valid seal) and `invalid_lanes`
   (`lane_invalid`: seal or records failed validation, including record-level
   faults found during the merge; a retry without that lane starts from fresh
   scratch state). If a record-level fault appears while the deadline has not
   expired, the window is deferred rather than committed incomplete.
4. Merges, assigns `CanonicalSeq` (the line ordinal of the evidence file), writes
   evidence, provenance and the receipt, then advances the watermark.

A window where every lane was down has no seals; absent windows are tiled so a
total outage leaves an incomplete receipt rather than no trace. `--window-seconds`
is the authority for window bounds; seals are checked against bounds computed from
the aligned start in the filename. One writer per canonical root is enforced by a
`.finalize.lease` file held for the run. A killed finalizer may leave it; review
before removing.

A seal arriving after its window was committed is reported under
`late_after_finalization` in the sweep report and is never inserted; canonical
output stays immutable and positions are never renumbered. A window whose start
precedes the watermark is refused before anything is written (the sweep exits
non-zero and lists it under `behind_watermark`). Because records are checked against their window as they stream, windows cannot
overlap and a lane whose clock stepped back is `lane_invalid` before the merge. A
cross-window boundary check (a lane's first `visible_ns` below its previous
window's last) remains as defence in depth: it records `clock_faults`, commits the
window with `certified: false` (also the case for any incomplete window) and lists it under `quarantined` in the watermark,
and the watermark still advances so one fault does not stop healthy lanes.

`--interval-seconds` runs the same sweep repeatedly until SIGTERM/SIGINT;
`--report` writes the sweep JSON (also on failure, with `status: "error"`).

## Continuity classification

Tracked per `(venue, stream, connection_epoch)`:

- **Identity** (`record_id`, `content_hash`): `Unseen`, `Duplicate` (retransmission)
  or `Conflict` (same id, different bytes). Decided before continuity so a
  retransmission cannot move a counter. In `indexer-ingest` the lookup is an
  indexed SQLite table, exact within one UTC ingest partition; a record id first
  repeated after rollover is `Unseen`. The finalizer's identity scope is one window,
  in a disposable on-disk scratch index (`.record-identity.sqlite.open`) removed on
  success, deferral or lane fault. Neither holds record identities in RAM.
- **Epoch health**: `AwaitingBootstrap` then `Healthy`; `GapProven`,
  `CursorWentBackwards` or `LocalCounterBroken` make it `Stale`.
- **Cursor continuity**, per `source_cursor` variant:

| Variant | Established | Gap detectable |
|---|---|---|
| `update_range` | full continuity (a range carries its predecessor) | yes |
| `snapshot` (id or time) | classified `sparse_monotonic` | no |
| `unsequenced` | nothing about the venue | no |

Verdict labels (provenance `continuity_verdict`, fact causes): `lifecycle`,
`bootstrap`, `unsequenced_venue`, `sparse_monotonic`, `continuous`, `gap_proven`,
`cursor_went_backwards`, `local_counter_broken`, `duplicate`, `conflict`.

`local_counter` is tracked per connection, not per stream: one epoch interleaves
`process` records with frames from the same counter. Instrument-level continuity of
a multiplexed stream would require parsing the payload, so it is left to analysis;
the ingester does not claim it.

## Ingest store and retention

`indexer-ingest` is resumable and idempotent. Records and their cursor advance
commit in one SQLite transaction (a cursor ahead of its evidence would skip
records). Memory is bounded independent of corpus size.

The active store is chosen by the UTC day on which a sealed segment is consumed:
see `FORMATS.md` for the `ingest-store/date=<YYYY-MM-DD>/` layout. Rollover happens
only between complete sealed segments. The closed partition receipt records every
consumed segment's spool-relative path and sealed SHA-256 and remains after the
database is reaped, so an archived raw segment retained by the raw reaper is not
ingested a second time; the same path with a different SHA-256 is an integrity
conflict that stops ingestion. Opening a schema-v1 store migrates it
transactionally (current schema 3).

`indexer-store-reap` defaults to audit. Delete mode removes only a closed
`store.db` whose receipt is valid, whose bytes still match the receipt, and whose
`closed_at_ns` is at least the retention age. Retention below 24 hours is rejected.
It never removes `store.db.open`, the partition directory or `receipt.json`. The
default report path is `ops/last_ingest_store_reaper_sweep.json` beside the store
root. It is separate from the raw-data reaper, which guards irreversible evidence
with the stronger archive-plus-canonical gate (`archive/README.md`).

## Canonical reader

`indexer_finalize::select_canonical_windows` chooses the minimal adjacent run of
committed windows covering a half-open interval, and `CanonicalSelection::open`
streams each exact envelope joined to its versioned provenance record in receipt
order, without re-merging or timestamp sorting. The audit validates the commit
marker and receipt schema, stored and decoded object identities, strict Zstandard
EOF, per-line source binding, record/content identity, window bounds and sequence
continuity. Success is an opaque `AuditedCanonicalSelection`, minted only by
reading to the end and calling `finish()`; records yielded earlier are not verified
output.

- `CertifiedPolicy::RequireCertified` (default) rejects an uncertified window
  (incomplete or quarantined); `AllowUncertified` admits it and keeps
  `certified: false`.
- `LowerBoundPolicy::RequireWindowBoundary` (default) fails when the requested
  lower bound falls inside the first window; `Clip` audits the whole window and
  emits records at or after the bound; `ExpandToWindowStart` emits the whole first
  window and records the expanded interval. The upper bound is always clipped.

`indexer-canonical-audit` runs the same checks over a root and reports
`windows_verified`, `windows_partially_reaped`, `windows_archived_and_reaped` and
`evidence_records_verified`. Windows whose frames were reaped are reported
separately; the audit does not claim their records were decoded.

## Invariants

- The seal and the canonical `receipt.json` are the commit markers. A data filename
  or successful decompression is not.
- Committed segments, windows and receipts are never modified. Commit order is:
  finish content, fsync file, rename, fsync directory, then publish the receipt and
  fsync again. Any fsync error is a failed commit.
- Canonical evidence is the original envelope lines copied byte for byte; no
  parse-and-reserialize.
- Identity is hashed over the same bytes that are persisted.
- Anything that cannot be established fails closed; a fault attributable to one lane
  costs that lane in that window, not the window.
