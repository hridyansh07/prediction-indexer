# Persisted formats

Reference for the artifacts the splices, ingester and finalizer write. All
schemas are closed: strict readers reject unknown fields. Times are UTC Unix
nanoseconds; digests are lowercase hex SHA-256; JSON is written through a temp
file, fsynced, renamed, and the directory fsynced. Behavior is in
[`README.md`](README.md).

## Spool segment and seal (written by splices)

```text
<spool-root>/lane=<lane>/date=<YYYY-MM-DD>/
  <YYYYMMDDTHHMMSS000000>-<index:03d>-<segment-id>.ndjson.open   writer-owned, ignored
  <same stem>.ndjson                                              exact envelope lines
  <same stem>.seal.json                                           commit marker
```

Seal (`seal_version` 1), mirrored by `indexer-segment` and
`splices/common/segment.py`:

```json
{"seal_version":1,"lane_id":"polymarket","window_start_ns":0,"window_end_ns":0,
 "data_file":"<stem>.ndjson","byte_length":0,"line_count":0,"sha256":"...",
 "first_delivery_index":null,"last_delivery_index":null,
 "first_visible_ns":null,"last_visible_ns":null,
 "visible_non_decreasing":true,"delivery_index_dense":true,
 "segment_id":"...","segment_index":0,"seal_reason":"boundary",
 "ordering_status":"ok","epochs":[],"repaired_bytes":0,
 "created_ns":0,"writer_version":1}
```

The writer also appends metrics fields (`segment_seconds`, `queue_capacity`,
`queue_high_water`, `queue_full_events`, `segments_sealed`). The record bounds are
null for an empty segment and non-null (with at least one epoch) otherwise;
`ordering_status` is `ok` iff `visible_non_decreasing`, else
`visible_clock_regression`. `sha256` is a plain digest of the file bytes. The
archiver uploads the seal unchanged.

## Canonical window (written by `indexer-finalize`)

```text
<canonical-root>/
  watermark.json                          derived index; rebuildable from receipts
  .finalize.lease                         one finalizer per root
  date=<YYYY-MM-DD>/window=<window_start_ns>/
    evidence.ndjson.zst                   one frame; decoded = original envelope lines in canonical order
    provenance.ndjson.zst                 one frame; one JSON line per evidence line
    receipt.json                          the sole commit marker
    canonical_archive_receipt.json        written later by the archiver (archive/README.md)
```

Temporary names are `evidence.ndjson.zst.open`, `provenance.ndjson.zst.open` and
`receipt.json.open`. Commit order: finish both frames, fsync and rename both,
fsync the directory, write and fsync `receipt.json.open`, rename it to
`receipt.json`, fsync the directory. A crash before the receipt leaves nothing
committed; unreceipted outputs may be deleted and rebuilt. After a receipt exists
the window is immutable. A receipt naming a missing, truncated, corrupt or
multi-frame object is an integrity failure, never an open window, and is never
regenerated over. Both frames follow the codec contract in `encoder/README.md`.

If a lane is excluded and the merge restarts, both partial compressed files and
their identity accumulators are discarded; no data from the failed attempt enters
the committed frame.

### `receipt.json` (`receipt_version` 1, `finalizer_version` 1)

```json
{"receipt_version":1,"window_start_ns":0,"window_end_ns":0,
 "completeness":"complete","certified":true,
 "expected_lanes":[],"present_lanes":[],"unexpected_lanes":[],
 "missing_lanes":[{"lane":"...","reason":"lane_missing","detail":null}],
 "invalid_lanes":[{"lane":"...","reason":"lane_invalid","detail":"..."}],
 "finalization_deadline_seconds":300,"deadline_expired":false,"finalized_at_ns":0,
 "inputs":[{"lane":"...","data_file":"<stem>.ndjson","segment_index":0,"line_count":0,
            "sha256":"...","first_delivery_index":null,"last_delivery_index":null}],
 "evidence":{"file":"evidence.ndjson.zst","content_encoding":"zstd",
   "decoded":{"byte_length":0,"line_count":0,"sha256":"..."},
   "stored":{"byte_length":0,"sha256":"..."},
   "compression":{"algorithm":"zstd","level":3,"frame_checksum":true,
                  "dictionary":null,"frame_count":1,"encoder":"..."}},
 "provenance":{"file":"provenance.ndjson.zst", "...":"same shape as evidence"},
 "first_canonical_seq":1,"last_canonical_seq":1,
 "carried":{"ordering":{},"lane_visible_ns":{}},
 "clock_faults":[]}
```

- `completeness` is `complete` or `incomplete`. `certified` is true only when the
  window is complete and `clock_faults` is empty, so every incomplete window is
  uncertified. `unexpected_lanes` are present but not expected: merged like any
  other evidence, never counted toward completeness. `expected_lanes` is the
  `--expect-lane` list verbatim.
- `inputs` are the source segments the window consumed; the reaper matches them by
  lane, SHA-256, `data_file` and `segment_index`.
- An empty window has `first_canonical_seq` and `last_canonical_seq` null and zero
  decoded line counts; its stored identities are still valid non-empty frames.
- `carried` holds the ordering history and each lane's last `visible_ns` that the
  next window starts from, recorded in the receipt so a deleted watermark rebuilds
  exactly. `clock_faults` entries are
  `{window_start_ns, lane, previous_visible_ns, observed_visible_ns}`.
- Readers fail closed unless every compression field has the V1 value, both stored
  files exist at their stored lengths (or the production-archive tombstone proves
  why they are absent), evidence and provenance decoded line counts are equal, and
  the sequence range agrees with the evidence line count.

### Provenance line

One line per evidence line, same order:

```json
{"canonical_seq":1,"lane_id":"polymarket","source_segment_sha256":"...",
 "source_line_number":1,"record_id":"pm-...-3","content_hash":"...",
 "continuity_verdict":"continuous","visible_tie_group":null}
```

`visible_tie_group` is non-null exactly when two or more distinct lanes share the
record's `visible_ns`. `continuity_verdict` uses the labels in `README.md`;
`duplicate` and `conflict` are window-scoped. A decoded evidence line is an
envelope line copied byte for byte; `canonical_seq` equals its 1-based ordinal
within the global sequence.

### `watermark.json` (`watermark_version` 1)

Derived index, never an authority: checked against the newest receipt on load and
rebuilt from receipts when they disagree. Fields: `last_window_start_ns`,
`last_window_end_ns`, `completeness`, `certified`, `last_canonical_seq` (highest
assigned anywhere so far; an empty window does not reset it), `evidence_sha256`,
`provenance_sha256` (decoded), `source_segment_sha256`, `carried`, `quarantined`
(every `clock_faults` entry so far). Receipt sequence ranges are validated as an
unbroken chain on rebuild.

### Reaped windows

The canonical reaper (`archive/README.md`) may remove only the two `.zst` frames,
evidence first. `receipt.json` and the production `canonical_archive_receipt.json`
stay as a tombstone: the finalizer rebuilds its watermark, global sequence,
continuity and carried clocks from canonical receipts, so deleting one would let
committed history be forgotten and re-finalized. Startup accepts missing frames
only when a strict production archive receipt binds the unchanged local receipt and
both frame identities. Frames present with evidence absent is the one recognised
partial state; evidence present with provenance absent is rejected. `canonical
audit` reports such windows separately.

## Ingest store (written by `indexer-ingest`)

```text
<store-root>/date=<YYYY-MM-DD>/        UTC day on which segments were consumed
  active.json        next global position and bounded initial continuity carry
  store.db.open      the only writable database (SQLite WAL)
  store.db           immutable after rollover
  receipt.json       partition close marker; retained after store.db is reaped
```

`store.db` tables: `meta` (schema version, currently 3), `evidence` (raw captured
lines with `file_order` position), `facts` (continuity classification, hash-bound),
`spool_cursor` (byte offsets for crash resume), `consumed_segment` and
`record_identity(record_id, content_hash)` (`WITHOUT ROWID`, 32-byte content hash,
partition-scoped).

`active.json` (`active_version` 1): `partition_date`, `opened_at_ns`,
`first_file_order_seq`, `parent_receipt_sha256`, `initial_carry`, `identity_scope`
(`ingest_partition`).

`receipt.json` (`ingest_partition_receipt_version` 1): `partition_date`,
`opened_at_ns`, `closed_at_ns`, `database_file`, `database_byte_length`,
`database_sha256`, `first_file_order_seq`, `last_file_order_seq`, `evidence_rows`,
`fact_rows`, `parent_receipt_sha256` (chain), `terminal_carry`, `identity_scope`,
and `segments` (each consumed segment's spool-relative path and sealed SHA-256).
`terminal_carry` carries only connections and epochs observed in the closing
partition, so a connection silent for a whole partition restarts at bootstrap; the
continuity and identity claim is exact over the retained ingest horizon, not
deployment lifetime.

Closure checkpoints every WAL frame, closes and fsyncs `store.db.open`, renames it
to `store.db`, fsyncs the directory, removes the active marker, and publishes
`receipt.json` last.
