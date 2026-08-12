# Architecture

How this system is put together, and — more usefully — *why* each boundary sits
where it does. Most of the shape here is a consequence of one idea, so that comes
first.

---

## 1. The governing principle

> **Capture decisions are irreversible; analysis decisions are not.**

A frame you didn't record is gone. A frame you recorded and interpreted wrongly is
a code change away from being right. Every structural decision below follows from
taking that asymmetry seriously:

- A splice records **every** message verbatim and filters nothing.
- Normalisation happens at *replay*, never at capture.
- The boundary between processes is a **file**, not a socket.
- Exclusion is a label, never a delete.
- A partial discovery result never replaces a complete one.

The corollary that is easiest to violate under deadline: **never gate capture on
an economic threshold.** Thresholds are analysis. Capture everything in scope;
threshold only what you *alert* on.

### This has paid for itself, repeatedly

The live wire has now contradicted published venue documentation three times:

| | Documentation says | Wire actually does |
|---|---|---|
| Polymarket | `{topic, type, payload:{priceChanges}}`, camelCase | flat snake_case — `event_type`, `price_changes`, `best_bid` |
| Polymarket | `hash` nullable, unemphasised | present on **7,414 of 7,414** `price_change` entries |
| Limitless | "no sequence number exists" | `version` on **100%** of `orderbookUpdate` messages |

A splice that normalised against any of those documents would have written
confident, wrong output — and by the time anyone noticed, the frames would be
gone. Recording bytes and asking questions later is the only reason those were
cheap discoveries rather than expensive ones.

---

## 2. The shape

```
                   ┌──────────────────────────────────────────┐
                   │  configs/capture_manifest.json           │
                   │  what to watch, per venue, with cadence  │
                   └────────────────────┬─────────────────────┘
                                        │
                              ┌─────────▼─────────┐
                              │     TARGETER      │   long-lived loop
                              │  discovery + lag  │   Python
                              └─────────┬─────────┘
                                        │  data/live/targets_<venue>.json
                                        │  (polled; digest-gated)
        ┌───────────────────────────────┼───────────────────────────────┐
        │                               │                               │
┌───────▼────────┐            ┌─────────▼────────┐            ┌─────────▼────────┐
│ SPLICE         │            │ SPLICE           │            │ SPLICE           │
│ polymarket     │            │ limitless        │            │ kalshi           │
│ WSS            │            │ Socket.IO        │            │ WSS + RSA-PSS    │
└───────┬────────┘            └─────────┬────────┘            └─────────┬────────┘
        │                               │                               │
        └───────────────────────────────┼───────────────────────────────┘
                                        │  append-only NDJSON, fsync'd
                                        │  data/spool/venue=…/date=…/<ts>-<epoch>.ndjson
                              ┌─────────▼─────────┐
                              │     INGESTER      │   Rust
                              │  global sequence  │   types · store · continuity · cli
                              │  + continuity     │
                              └─────────┬─────────┘
                                        │  SQLite: evidence + facts, hash-bound
                              ┌─────────▼─────────┐
                              │     ANALYSIS      │   Python
                              │  masks · Ω · fees │   analysis/
                              └───────────────────┘
```

Four components, three process boundaries, one direction of flow. **The tape is
the contract**: components 1–3 exist to produce it, component 4 exists to consume
it, and neither side reaches across.

---

## 3. Why the boundary is a file

The most consequential decision in the system, and the least obvious.

gRPC or an internal WebSocket between splice and ingester would make the boundary
**lossy exactly when it matters**. Frames pushed over a socket exist only in the
splice's memory until something on the far side commits them — so an ingester that
is down, restarting, or backpressured costs data. And we would be writing
reconnect logic for our own internal link, which is absurd when the whole point of
the component is that reconnects are hard.

A spool inverts it. The splice fsyncs bytes it owns; the ingester may be absent
for an hour at no cost.

> **The file is the protocol.** If a push channel is ever added it carries "new
> bytes at offset N" — never the data.

This also means the spool *is* the irreversible evidence and the ingester's store
is derived. Losing the store costs a rebuild. Losing a spool is unrecoverable, so
nothing ever rewrites or truncates a committed line. The single exception is
`repair_torn_tail`, which drops a final line lacking its newline — a record is
durable only once its terminator is on disk, and truncating is the only repair
that preserves the invariant every reader depends on.

---

## 4. Components

### 4.1 Targeter — `targeter/`

Decides what to watch. Long-lived, manifest-driven, one process for all venues.

```
manifest.py   the declarative capture manifest, validated strictly
sources.py    one DiscoverySource per venue: catalogue in, targets out
coverage.py   first-sighting ledger — discovery lag as a measured number
targets.py    the targets file, its digest, and atomic writes
run.py        the loop
```

**Why a loop, not a script.** Short-dated crypto markets are created continuously.
A five-minute market discovered a minute late has lost a fifth of its life, and the
missing fifth contains its opening price discovery. A one-shot targeter guarantees
that loss on every market created after the run — and nothing in the captured data
reveals it, because the frames that *did* arrive look perfectly healthy.

Each manifest entry carries its own `discover_every_seconds`, so a 5-minute ladder
can be rediscovered every 30s while a daily one is checked hourly, neither paying
the other's request cost.

**Adding a market is a config change, never a code change.** An entry names one
structural idea; a per-venue selector says how that idea is spelled at each venue,
because market identification differs everywhere.

**The digest is the subscription's identity** — over `(venue, sorted asset_ids)`
only. Reordering the file or editing a note must not move it, or an edit that
changed nothing would force a reconnect and a book resync. When it *does* move, the
splice writes a `subscription_changed` record into the tape before resubscribing,
so a market with no data stays distinguishable from a market never subscribed.

**Rejections are data.** Every candidate considered and dropped is logged with a
reason. Without it, a selector bug looks exactly like a venue not listing the
market, and those have very different fixes.

Two rules that exist because their absence produced real, silent truncation:

- **A venue's file is the union of every active entry, due or not.** Cached results
  serve entries whose cadence hasn't elapsed. Without this, a cycle where only the
  60-second entries were due rewrote the Kalshi file without the 120-second entry's
  ladder — silently unsubscribing 300 live markets.
- **Any failed entry leaves the whole venue file untouched.** Failures are tracked
  per `(entry, venue)`, not per venue; a single dict keyed by venue let a later
  entry's success mask an earlier entry's rate-limit, and the file was written
  without BTC. **Stale-but-complete beats fresh-but-truncated.**

### 4.2 Splices — `splices/`

One per venue, one process each, because a socket outage is venue-local and the
network-facing part is where outages happen.

```
common/envelope.py   the ten-field wire contract, validated at construction
common/spool.py      append-only, fsync'd, torn-tail repair, resume
common/base.py       connection lifecycle, counters, tape discipline
polymarket/          WSS                 · verified live
limitless/           Socket.IO           · verified live
kalshi/              WSS + RSA-PSS auth  · built, awaiting credentials
```

A splice owns auth, subscription, reconnection, backoff, heartbeats, protocol
quirks, the counters, and the durable append. It owns nothing that requires
judgement — what a frame means, whether it's interesting, what a book looks like.

> **A splice does not filter.** `_emit_frame` takes no predicate, so a venue
> subclass that wants to drop something has to work against the base class rather
> than merely forget.

**A delivery is one socket message, verbatim.** Polymarket batches several events
into one frame; splitting them would interpret a schema we are deliberately not
interpreting, and the frame boundary is unrecoverable once gone. The sole exception
is Socket.IO, where the event *name* is part of the delivery and not recoverable
from the payload — so Limitless records `{"event": name, "data": payload}`. That
wrapper is the only framing any splice performs.

**One epoch per connection.** A reconnect always mints a fresh UUID and restarts
`local_counter`. Carrying an epoch across a reconnect would let a delta from the
new socket fold onto a book assembled from the old one — a corrupt book rather than
an error, which no later check catches.

Lifecycle records (`connection_opened`, `subscription_changed`,
`connection_failed`, …) go in the **same** tape as the data. A separate log would
mean the gap and the reason for the gap have to be rejoined by wall-clock time
later, and the reason is exactly what you need when deciding whether to trust the
window around it.

### 4.3 Ingester — `ingester/` (Rust)

Tails the spools, assigns the one global order, classifies continuity. No network
access, no venue-specific behaviour beyond a labelled field.

```
crates/types       envelope parsing, identity, sequences, domain-separated hashing
crates/store       raw/fact commits plus the exact durable record-identity index
crates/continuity  identity verdict, epoch health, cursor classification
crates/cli         indexer-ingest: tail, sequence, classify, report
```

**Why Rust and a separate process.** The property this component guarantees —
that a replay of the raw bytes reproduces recorded state exactly — is enforceable
by construction there and only by convention in Python. Commit receipts have
private constructors, so ordering appears in the signatures a caller must satisfy
rather than in a comment asking them to be careful.

```
capture_raw  -> CapturedRecord    evidence durable, parsing may begin
commit_fact  -> CommittedFact<T>  fact durable, projections may move
```

Two rules this component holds to:

1. **The raw line is durable before anything parses it.** If the parse then fails,
   the bytes still exist and the schema can be revised.
2. **One encode, one hash, one write.** `Sinkable::to_canonical_bytes` produces the
   buffer that is both hashed and persisted. Encoding separately for each would let
   them disagree, surfacing as corruption years of data later.

Global duplicate/conflict identity is an indexed SQLite projection, committed in
the same transaction as its first fact and the spool cursor. It is deliberately
not retained in the long-lived classifier: doing so costs O(all historical
records) RAM even though file reads themselves are cursor-based. Ordering state
remains in memory; exact identity remains durable and global.

The sealed-window finalizer uses the same bounded classifier but not that global
projection: its duplicate/conflict contract is window-scoped. Each merge attempt
gets an exact disposable SQLite index, removed before the attempt can become
canonical evidence. A lane-invalid retry starts with a fresh index so excluded
records cannot affect the surviving merge. Thus finalization memory is independent
of window record count without coupling canonical output to the mutable ingest
store.

**Deliberately stops before normalisation.** Converting venue frames into typed
canonical events at ingest is what a trading system does, because it must act on
the frame immediately. We don't need to, so there is no fixed-point money stack
here. Normalising at capture makes a schema misreading permanent; normalising at
replay makes it a code change — the same argument capture.md §6.3 already makes
about tags.

### 4.4 Analysis — `analysis/`

Masks, outcome spaces, void policy, partition sums, fees, equivalence classes.
Unchanged by the capture work and deliberately so — it consumes the tape and never
reaches across it.

---

## 5. The envelope

The only interface between the Python and Rust halves. **Closed**: the parser
rejects unknown fields and requires all ten.

```json
{"delivery_index":3,"record_id":"pm-427f40aa-3","visible_ns":1785267959274886000,
 "venue":"polymarket","stream":"public_book","connection_epoch":"427f40aa",
 "local_counter":3,"source_cursor":{"type":"unsequenced","counter":3},
 "kind":"venue_frame","raw_payload":"[{\"event_type\":\"book\"}]"}
```

A parser that ignored extras would let a splice write something the tape claims to
carry and no reader ever sees — silently, for as long as nobody checks. Failing
loudly on the first record is cheaper by a wide margin.

### The three counters

The single most important thing in the system, and the easiest to conflate.

| Field | Whose | Scope | Answers |
|---|---|---|---|
| `delivery_index` | ours | splice lifetime, dense | *In what order did we see things?* |
| `local_counter` | ours | one connection, resets on reconnect | *Where in this connection?* |
| `source_cursor` | **theirs** | whatever the venue offers, often nothing | *What did the venue claim about its own continuity?* |

**Only the first two are authoritative.** Replay walks `delivery_index`. Ordering
that depends on a venue agreeing to number its messages breaks the moment a venue
doesn't — and two of our three don't.

Our sequence guarantees replay determinism but **cannot see a frame the venue
dropped**, because our numbering is dense either way. Venue-side gap detection is a
separate mechanism with per-venue strength. These are different questions and
conflating them is what makes a replay untrustworthy.

> **A cursor records what the venue asserted, never what the splice inferred.**

That rule has teeth. An early Kalshi implementation set `previous_last` to the last
sequence the splice had *seen*, which makes every message continuous with its
predecessor by construction — a deliberate 7–8 hole reached the ingester labelled
`continuous` and the loss was undetectable. It is now `seq - 1`: the venue's own
claim, true by definition for a counter Kalshi promises is dense.

### What each venue actually gives us

| Venue | Cursor | Density | Can prove a dropped message? |
|---|---|---|---|
| **Kalshi** | `update_range` from `seq` | dense **per subscription** | **Yes** — the only one |
| **Limitless** | `snapshot.last_update_id` (`version`) | monotonic per market, **not dense**, ranges overlap | No — orders and dates a book, so *stale* is detectable, *missing* is not |
| **Polymarket** | `unsequenced` | — | No sequence exists. But a book `hash` on every entry makes checksum reconciliation available downstream |

Kalshi's `seq` being per *subscription* is why its splice subscribes the entire
ladder in one call. Splitting would give N independent sequences and destroy the
only property that makes authenticating for that venue worthwhile.

---

## 6. Continuity classification

Three things tracked, and the boundary between them is where the subtlety lives.

**Identity** — `Unseen` / `Duplicate` / `Conflict` on `(record_id, content_hash)`.
Decided *before* continuity, so a retransmission cannot move a counter or stale a
stream. A conflict is the same id with different bytes: a venue contradicting
itself, surfaced loudly rather than silently becoming the new truth.

**Epoch health** — `AwaitingBootstrap` → `Healthy` → `Stale`. Snapshot proof is
tracked **per instrument, not per connection**: on a multi-instrument stream the
first instrument's snapshot must not mark the whole lane healthy, or a sibling's
delta folds onto a book carried over from the previous connection.

**Cursor continuity** — and here the ingester is deliberately modest about what it
can establish:

| Variant | What a connection-level view can establish | Gap detectable? |
|---|---|---|
| `update_range` | full continuity — a range carries its own predecessor | **Yes** |
| `snapshot.last_update_id` | that a key was recorded | No |
| `snapshot.source_time_ms` | that a key was recorded | No |
| `unsequenced` | nothing about the venue | No |

**Cursor continuity cannot be judged at connection level on a multiplexed lane.**
One connection carries every subscribed market, so comparing a snapshot id against
the lane-wide previous value compares two different books. Limitless made this
concrete: its `version` behaves like a server-wide counter sampled per book, so
consecutive frames for different markets legitimately move backwards — **7
"faults" in 451 real frames, none of them real.**

Identifying the instrument means parsing the payload, which is normalisation, which
this component doesn't do. So the key is recorded on the fact and instrument-level
continuity is left to the analysis layer, which parses and can group correctly.
`UpdateRange` keeps its check because a range carries its own `previous_last` and
is verifiable without knowing the instrument.

---

## 7. Data layout

```
configs/capture_manifest.json          what to watch
data/live/targets_<venue>.json         current subscription set + digest
data/live/rejected_<venue>.json        candidates dropped, with reasons
data/live/coverage.json                first-sighting ledger
data/spool/venue=…/date=…/<ts>-<epoch>.ndjson   raw tape, append-only, never mutated
data/ingest-store/store.db             evidence + facts, hash-bound, derived
data/analysis/…                        content-addressed analysis runs
```

Spool filenames are **timestamp-prefixed** so a plain directory listing is capture
order and the reader needs no index. The epoch alone is a UUID, which sorts
arbitrarily — an ingester tailing the directory would otherwise sequence a later
connection ahead of an earlier one and contradict the `delivery_index` already
committed.

Storage is three layers, not four: NDJSON spools are raw, SQLite is the fact log
and state store, Parquet appears only when a query is actually slow.

---

## 8. Known limits

Stated because a limit you know about is a caveat and a limit you don't is a bug.

**`EvidenceSeq` is file-ordered, not wall-clock ordered.** Records from two venues
live at the same instant land in file order. `visible_ns` carries real timing and
is preserved on every record, so **any lead-lag analysis must sort on `visible_ns`**.
A k-way merge on `visible_ns` at ingest is the fix; it first needs a way to know
when a live file is complete.

**Replay and verify are not built.** They are retrofittable; the *property* they
verify is not, which is why canonical bytes and hash-bound rows are in from the
first commit.

**Kalshi is unverified against live servers.** Written from the published spec.
Because a splice records verbatim, wrong *message shapes* cost nothing at capture —
only three things can genuinely fail on contact (signature construction, subscribe
shape, cursor extraction) and all three are isolated, with the third degrading to
`unsequenced` rather than raising.

**Discovery lag is only meaningful for markets first seen while the loop runs.** On
a first run every market is "newly seen" at once, so the reported lag is market age
at first sighting, not our latency.

**Volume is measured but not fully stress-tested.** 20 Polymarket assets produce
6.2M records/day and 6.8 GB/day uncompressed. A 2.67M-record archived production
window sustained about 18,400 records/second through the schema-v2 SQLite path
with an 8 MiB peak RSS, but a complete production day and its long-term storage
growth have not been replayed as one acceptance run.

**`resolution_source` is not yet part of condition identity.** A mask is only half
a condition's identity; the settlement oracle is the other half. Two venues quoting
"BTC above $100k at 4pm" against different oracles are different conditions and can
legitimately diverge. This is the most likely way P0 manufactures a fake arbitrage,
in the one class nominated as immune to mask error.

---

## 9. Running it

```bash
# discover — long-lived, or --once
.venv/bin/python targeter/run.py
.venv/bin/python targeter/run.py --once --venue kalshi

# record — one process per venue
.venv/bin/python splices/run.py polymarket
.venv/bin/python splices/run.py limitless
.venv/bin/python splices/run.py kalshi        # needs KALSHI_API_KEY_ID + key

# sequence and classify
cargo build --release --manifest-path ingester/Cargo.toml
indexer-ingest data/spool data/ingest-store --watch-interval-seconds 5
indexer-ingest data/spool data/ingest-store --check-integrity

# tests
.venv/bin/python -m unittest discover -s tests -q     # 207
cd ingester && cargo test                             # 25
```

Only Kalshi needs credentials, and only for the splice — its targeter uses the
public catalogue.

---

## 10. Where to read next

| Document | Covers |
|---|---|
| [`spec/capture.md`](spec/capture.md) | The capture system; **§11 records every decision and measurement since drafting** |
| [`spec/envelope.md`](spec/envelope.md) | The wire contract in full |
| [`spec/splice.md`](spec/splice.md) | Venue adapters, the no-filtering rule, per-venue status |
| [`spec/targeter.md`](spec/targeter.md) | Subscription management and ladder completeness |
| [`spec/ingester.md`](spec/ingester.md) | Sequencing, continuity, and what the first real ingest changed |
| [`docs/idea.md`](docs/idea.md) | The original thesis: conditions, masks, relationship derivation |
| [`docs/partition_sum_test.md`](docs/partition_sum_test.md) | The first experiment and its staleness controls |
