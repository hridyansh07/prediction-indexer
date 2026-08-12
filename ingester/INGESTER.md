# INGESTER — global sequencing and continuity

Rust. Tails the venue spools, assigns the one global order, and classifies what
each delivery means for continuity. It has no network access and no venue-specific
behaviour beyond a labelled field.

## Why Rust, and why a separate process

The tape is the contract. The splices exist to produce it, the analysis layer
exists to consume it, and neither reaches across. Putting the sequencer in its own
process on the far side of a file boundary means a splice crash cannot corrupt the
sequence and an ingester crash cannot cost a frame.

Rust specifically because the property this component exists to guarantee — that a
replay of the raw bytes reproduces the recorded state exactly — is enforceable by
construction here and only by convention in Python. commit receipts
have private constructors, so the ordering shows up in the signatures a caller has
to satisfy rather than in a comment asking them to be careful.

## What it does

```
spool files (venue=*/date=*/<ts>-<epoch>.ndjson)
  → capture_raw        exact bytes durable before anything parses them
  → assign EvidenceSeq the one global cross-venue order
  → identity lookup    exact global verdict from the durable SQLite index
  → classify           epoch health and cursor continuity
  → commit_fact        the classification, durable, hash-bound
  → fact log (SQLite)
```

Spool filenames are timestamp-prefixed so a plain directory listing is the correct
read order. The reader needs no index and no sort of its own.

`delivery_index` is the splice's claim about its own order; `EvidenceSeq` is the
global order across venues. Both are recorded. Where they disagree in a way that
isn't explained by interleaving, something is wrong and it is worth knowing.

## Crate layout

| Crate | What | Scope |
|---|---|---|
| `types` | `envelope`, `identity`, `sequence`, `hash`, `sink`, `error` | `kalshi`/`limitless` venues, `public_trade` stream, `control` kind. |
| `store` | `capture_raw`, `commit_fact`, exact record-identity index, integrity, recovery | No approval/parent/command gates — those are order-management concerns this component doesn't have. |
| `continuity` | identity verdict, epoch health, lane authority, cursor continuity | No `classify_gateway`, `classify_reconciliation`, or order-ack parsing — there are no orders to reconcile. |

**Out of scope:** `risk`, `oms`, `market`. Capital reservation, iceberg children and
escrow are a trading system's problems.

**Deliberately stopping before `CanonicalEventV1`.** Normalising venue frames into
typed canonical events at ingest is what a system does when it must act on the
frame immediately. We do not need to, so we don't. Normalising at capture makes a
schema misreading permanent; normalising at replay makes it a code change. This is
why there is no fixed-point money stack (`decimal`, `money`, `scale`, `convert`) —
capture stores bytes, and it is the same argument `docs/CAPTURE_SPEC.md` §6.3 already makes about tags.

The Polymarket and Limitless doc-versus-wire discrepancies are the empirical case:
a normaliser written against either venue's published schema would have produced
confident, wrong typed events, and the raw frames are what made the error visible
and cheap.

## Continuity model

Three things tracked per `(venue, stream, connection_epoch)`:

**Identity** — `Unseen` / `Duplicate` / `Conflict` on `(record_id, content_hash)`.
Duplicate is a retransmission. Conflict is the same id with different bytes, which
is a venue misbehaving and worth surfacing loudly. Identity is decided *before*
continuity so a retransmission cannot move a counter or stale a stream. In the
long-lived ingester this is an indexed SQLite lookup, not a process-local map:
the verdict remains global and exact without retaining every historical record ID
in RAM. The finalizer keeps the narrower window-scoped semantics but uses a
disposable SQLite index for each merge attempt; it does not link or mutate the
ingester's global store.

**Epoch health** — `AwaitingBootstrap` → `Healthy` → `Stale` → `Retired`. A new
connection starts unproven. Snapshot proof is tracked **per instrument, not per
connection**: on a multi-instrument stream the first instrument's snapshot must not
mark the whole lane healthy, or a sibling's delta folds onto a book carried over
from the previous connection. That is a known failure mode for multi-instrument
streams; Polymarket subscriptions are multi-asset by construction, so we would hit
it immediately if snapshot proof were tracked per connection instead.

**Cursor continuity** — per `source_cursor` variant, since our venues don't share
a uniform cursor model and continuity has to be judged per variant rather than
assumed globally:

| Variant | What the ingester can establish | Gap detectable here? |
|---|---|---|
| `update_range` | full continuity — a range carries its own predecessor | **Yes** |
| `snapshot.last_update_id` | that a key was recorded | No |
| `snapshot.source_time_ms` | that a key was recorded | No |
| `unsequenced` | nothing about the venue | No |

Treating `last_update_id` as dense is right for Binance and wrong for us.

### What the first ingest over real spools changed

Running against 4,282 captured records found two bugs and one design limit. All
three are now regression-tested in `crates/continuity/tests/classification.rs`.

**`local_counter` is per connection, not per lane.** It was tracked per
`(venue, stream, epoch)`, but a splice mints one counter per connection and spends
it across every stream — a Polymarket epoch interleaves `process` lifecycle
records with `public_book` frames from the same sequence. Reported **3,306 phantom
breaks out of 3,823 frames**.

**Records in a batch must see each other.** `classify` ran inside the ingest
transaction while `apply` ran after it, so every record after the first in a
512-record batch compared against the state as of the batch start. This was the
dominant source of the same 3,306. The fix applies state inside the transaction,
which bends the classifier's own "state moves only after commit" rule; that is
safe here because a rollback aborts the run, so in-memory state that moved past a
failed commit is never observed.

**Cursor continuity cannot be judged at connection level on a multiplexed lane.**
This one is not a bug in the code but a limit on what the component can honestly
claim. One connection carries every subscribed market, so comparing a snapshot id
against the lane-wide previous value compares two different books. Limitless made
it concrete: its `version` behaves like a server-wide counter sampled per book, so
consecutive frames for different markets legitimately move backwards relative to
each other — **7 "faults" in 451 real frames, none of them real**.

Identifying the instrument would mean parsing the venue payload, which is
normalisation, which this component deliberately does not do. So the key is
recorded on the fact and instrument-level continuity is left to the analysis
layer, which does parse and can group correctly. `SnapshotId` and `SnapshotTime`
now classify as `sparse_monotonic` unconditionally.

`UpdateRange` keeps its check because a range carries its own `previous_last`, so
continuity is verifiable without knowing the instrument. Whether Kalshi actually
numbers per connection or per market is unverified; if per market, it needs the
same treatment.

### Memory bounds and disk-backed identity

The original ingester streamed spool lines in 512-record batches but retained one
`BTreeMap` entry for every unique `record_id`. Startup rebuilt the same map by
folding the complete fact log. File cursors were therefore bounded while retained
classifier state was not.

Measured on the largest complete archived window found for 2026-08-08 — four
lanes, five sealed segments, 2,670,449 records and 2,331,516,814 decoded bytes:

| Release path | Peak RSS |
|---|---:|
| k-way cursors, parse and merge only | 2,592 KiB |
| original `indexer-finalize` | 530,120 KiB |
| original `indexer-ingest`, initial ingest | 510,140 KiB |
| original `indexer-ingest`, no-new-data restart | 520,248 KiB |
| schema-v2 migration plus recovery | 7,472 KiB |
| schema-v2 `indexer-ingest`, initial ingest | 7,972 KiB |
| schema-v2 `indexer-ingest`, no-new-data restart | 6,580 KiB |
| disk-backed `indexer-finalize` | 13,756 KiB |

Schema v2 adds `record_identity(record_id, content_hash, first_seen)`. A first
observation, its fact, and the spool-cursor advance commit in one SQLite
transaction. Duplicate and conflict lookups return the original fact position,
so moving identity to disk changes no verdict semantics. No LRU or probabilistic
filter is used.

Opening a schema-v1 store performs one transactional SQLite migration from the
already committed canonical facts in sequence order. It does not reconstruct the
identity history in Rust memory. On this sample the store grew from 4.9 GiB to
5.3 GiB (rounded `du` values), and fixed initial ingest took 145 seconds — about
18,400 records/second, still over twenty times the measured steady capture rate.
The migration rolls back on malformed historical identity data and leaves the
schema version at 1.

The finalizer has no lifetime-global identity contract: duplicate and conflict
verdicts deliberately start fresh at each 30-minute window. It therefore creates
`.record-identity.sqlite.open` as an exact scratch index for each merge attempt,
with a bounded 2 MiB SQLite page cache, no durability work, and no receipt status.
Success, deferral, or a lane fault closes and removes it; a retry after a lane
fault starts empty so the excluded lane cannot influence surviving records. On
the same sample finalization took 67.52 seconds versus 58.51 seconds before the
change, while peak RSS fell by 97.4%. The independent canonical audit verified
all 2,670,449 evidence/provenance pairs.

## Not in v1

Replay and verify. They are retrofittable; **the property they verify is not.**
The rule that makes them possible later — *`write_to` is the single source of the
bytes: a store encodes once, hashes that buffer, and persists that same buffer* —
has to hold from the first commit or no later work recovers it. So v1 encodes
canonically and hash-binds every row, and the `replay`/`verify` commands land in
iteration two.

Tags also stay out of the raw tape. Tagging is interpretation with today's logic;
baking it in means replay can never re-tag with better logic, which defeats having
a tape. Derived layer keyed by `(venue, stream, seq)`, raw stays immutable.

## Volume

Measured, not estimated: 20 Polymarket assets produced **6.2M records/day and
6.8 GB/day** uncompressed. This is the number that decides whether SQLite-per-frame
survives, and it needs re-measuring at the real subscription width before the
storage choice is fixed.
