# Architecture

How the system is put together and why each boundary sits where it does. Each
component's README holds its full contract; this page is the map.

## 1. The governing principle

> **Capture decisions are irreversible; interpretation decisions are not.**

A frame that was never recorded is gone. A frame that was recorded and read
wrongly is a code change away from being right. Everything below follows from
that asymmetry:

- A splice records every application delivery verbatim and filters nothing.
- Normalization, book reconstruction, trust and economics happen at replay,
  never at capture.
- Processes hand off through durable files and immutable objects, not sockets.
- Exclusion is a visible label, never a silent delete.
- A partial result never replaces a complete one.

Live venues have repeatedly contradicted their own documentation: Polymarket's
wire shape differs from its documented one, Limitless sends a `version` its docs
say does not exist, and in late September 2026 Kalshi added a top-level
`sending_ts_ms` to every message. Each time the raw bytes were already on the
tape, so the fix was a new normalizer version and a re-materialization, not lost
data.

## 2. The shape

```
 Targeter v2 (scheduled one-shot)         public venue catalogues
   select events, publish a target generation, archive the run
        │  live/targeter-v2/current.json -> generations/<run>/targets_<venue>.json
        ▼
 Splices (one process per lane)           venue WebSockets / pollers
   kalshi · polymarket · polymarket_snapshots · limitless
   (+ polymarket_sports, polymarket_rtds reference feeds)
        │  spool/lane=<lane>/date=<day>/<segment>.ndjson + .seal.json
        ▼
 Ingester (Rust, network-free)
   indexer-ingest    tail -> daily SQLite ingest store (evidence + continuity facts)
   indexer-finalize  sealed segments -> 30-minute canonical windows + receipts
        │
        ▼
 Archive (Python)                          object store: local or GCS
   raw archiver, canonical archiver, receipts/manifests, reapers (audit by default)
        │  raw/lane=…/date=…/…ndjson.zst
        │  canonical/date=…/window=<start_ns>/{evidence,provenance}.ndjson.zst + receipt.json
        │  targeter-v2/runs/date=…/run=…/
        ▼
 Universe (Python, SQLite)                 HTTP API behind Caddy
   event store synced from committed Targeter runs; bundles, history, outcomes;
   Replay job control plane and auth
        │
        ▼
 Replay
   runner tick: resolve via Universe -> fetch canonical windows ->
   engine materializes verified normalized derivatives (Rust) ->
   prepare a hash-bound strategy context -> supervisor runs the strategy over
   Redis streams (replay-publish) -> independent reader verifies -> archive
        │
        ▼
 targeter-ui (TypeScript)                  events, bundles and replay views
```

`analysis/` holds the outcome-space, mask and claim model shared by the
Targeter, Universe and Replay; `encoder/` is the one Zstandard contract every
writer and reader uses.

## 3. Boundaries

**The file is the protocol.** A splice fsyncs bytes it owns; the ingester may be
absent for an hour at no cost. A push channel between them would lose data
exactly when the far side is down. Every hand-off is a durable artifact with an
explicit commit marker:

| Artifact | Commit marker |
|---|---|
| Spool segment | its `.seal.json` |
| Ingest-store day | `receipt.json` beside the closed `store.db` |
| Canonical window | the window `receipt.json` |
| Archived raw/canonical object | the verified archive receipt |
| Target generation | the `current.json` pointer |

A filename, rename, upload result or successful decompression is not a commit
marker. Committed artifacts are never mutated in place; a correction is a new
version.

**Deletion is separate from archival.** Reapers delete local copies only with the
verified archive receipt, the canonical-ingestion receipt and an independently
durable backend, and they run in audit mode unless explicitly enabled.

**Layer ownership.**

| Layer | Owns | Must not own |
|---|---|---|
| `splices/` | transport, auth, subscription, reconnect, timestamps, envelope write | normalization, filtering, economics |
| `targeter/` | catalogue adapters, event matching, selection evidence, run archive/publication/retention | socket capture, execution |
| `ingester/` | sealed-segment validation, deterministic order, continuity facts, canonical windows | networking, book interpretation |
| `encoder/` | the streaming Zstd contract (Python and Rust) | schemas, archive policy |
| `archive/` | object storage, verification, receipts, manifests, deletion eligibility | event meaning, any import of `targeter/` |
| `universe/` | the event store, bundle history, outcomes, Replay job control plane | capture, book reconstruction |
| `engine/` | venue normalizers, verified derivatives, risk reconstruction, Redis transport | strategies, scheduling |
| `replay/` | jobs, preparation, supervisor, strategy SDKs and strategies, readers | mutation of raw/canonical evidence |
| `analysis/` | outcome spaces, masks, claims | capture filtering |

## 4. The envelope

The one interface between splices and everything downstream. It is closed: the
parser rejects unknown fields and requires every field. Version 2:

```
envelope_version delivery_index record_id visible_ns monotonic_ns venue stream
connection_epoch local_counter source_cursor kind raw_payload
```

`raw_payload` is the socket delivery verbatim. One delivery is one record; a
vendor batch is never split. Lifecycle records (`connection_opened`,
`subscription_changed`, `connection_failed`, …) share the data tape so a gap and
its reason stay together.

| Counter | Whose | Scope | Answers |
|---|---|---|---|
| `delivery_index` | ours | one splice lifetime, dense | in what order this lane saw things |
| `local_counter` | ours | one connection, resets on reconnect | where in this connection |
| `source_cursor` | the venue's | whatever the venue offers | what the venue claims about its own continuity |

A cursor records what the venue asserted, never what the splice inferred. A
reconnect always mints a new `connection_epoch`.

| Venue | Cursor | Can prove a dropped message? |
|---|---|---|
| Kalshi | `update_range` from `seq`, dense per subscription | yes |
| Limitless | `snapshot.last_update_id` (`version`), monotonic per market, not dense | no; staleness only |
| Polymarket | `unsequenced`; a book `hash` allows downstream reconciliation | no |

## 5. Order and continuity

The finalizer orders every lane's records into a canonical window by
`(visible_ns, lane_rank, delivery_index)`. `monotonic_ns` is boot-scoped and
diagnostic only. Identity (`Unseen` / `Duplicate` / `Conflict` on record id and
content hash) is decided before continuity, so a retransmission cannot move a
counter. Epoch health is per connection epoch (`AwaitingBootstrap`, `Healthy`,
`Stale`). On a multiplexed lane only `update_range` continuity is judged at
capture time; per-instrument bootstrap and continuity need payload parsing and
are left to the replay normalizers.

## 6. Interpretation

Replay reads only canonical windows. `engine/` normalizes each window into one
immutable, verified derivative per venue normalizer version; a venue shape the
normalizer does not know is rejected visibly, never guessed. A normalizer change
therefore means a new version and re-materialization from the archived
canonical windows; the raw spools are not needed for that.

Strategies run on a frozen, hash-bound context through the economic strategy
SDK. Every strategy ships an independent reader that re-derives its output from
the inputs; a run is complete only when that reader and the supervisor agree.
Outputs describe apparent, conditional opportunity on recorded books, not
executed trades.

## 7. Known limits

- Only Kalshi can prove a venue-side drop. Limitless and Polymarket gaps are
  invisible at capture; replay can detect staleness and checksum mismatches.
- Cross-lane order at equal `visible_ns` is decided by lane rank, not by venue
  time.
- A capture outage leaves a permanent hole. There is one between 2026-08-24 and
  2026-08-25 11:30Z, when capture moved hosts.

## 8. Where to read next

| Topic | Document |
|---|---|
| Commands, layout, tests | [`README.md`](README.md) |
| Splices and the envelope | [`splices/README.md`](splices/README.md) |
| Ingester, sealed segments, canonical windows | [`ingester/README.md`](ingester/README.md) |
| Codec | [`encoder/README.md`](encoder/README.md) |
| Archive, receipts, reapers | [`archive/README.md`](archive/README.md) |
| Targeter v2 | [`targeter/README.md`](targeter/README.md) |
| Universe | [`universe/README.md`](universe/README.md) |
| Normalizers, derivatives, risk | [`engine/README.md`](engine/README.md) |
| Replay and strategies | [`replay/README.md`](replay/README.md) |
| Outcome spaces and masks | [`analysis/README.md`](analysis/README.md) |
| Deployment and operations | [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md), [`docs/RUNBOOK.md`](docs/RUNBOOK.md) |
| Pending specifications | [`docs/specs/`](docs/specs/) |
