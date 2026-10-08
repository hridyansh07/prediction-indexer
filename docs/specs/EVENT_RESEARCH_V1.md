# Event research V1: game-state archive, research outputs and site

Status: **proposed.** Base: `master` at `554f7c4`.

This spec covers everything between the strategy runs and the public
research site:

- **A0, prerequisites:** the game-state merge and two SDK limits that break
  long events.
- **Part A, game-state archive on the Universe VM:** a scheduled Python job.
  When a bundle goes terminal, it pulls Kalshi game state, normalizes it and
  archives it.
- **Part B, research outputs:** verify finished strategy runs, build
  per-event research packs and corpus tables, and publish one immutable
  version for the site.
- **Part C, Universe directory layout:** restructure the `universe/` package
  and document the VM's data paths, with no behavior change.
- **Part D, research site:** two routes in `targeter-ui` that read the
  published files. Built after Part B has published a version.

**Not in this spec: the corpus runner.** For now, strategy runs are done by
hand, one event at a time, with the local bench on a VM or the spare
machine. Part B consumes their output directories. Automating the runs is a
later spec.

Read first:

- [`AGENTS.md`](../../AGENTS.md), especially the invariants on capture,
  archives, the shared encoder and persisted formats;
- [`docs/ECONOMIC_STRATEGY_SDK_V1.md`](../ECONOMIC_STRATEGY_SDK_V1.md) §5
  (output), §8 (bounds), §10 (corpus runs) and §13 (fill checks);
- [`docs/LOCAL_REPLAY_BENCH_V1.md`](../LOCAL_REPLAY_BENCH_V1.md);
- [`docs/specs/MARKET_PROFILE_V2.md`](MARKET_PROFILE_V2.md);
- `scripts/KALSHI_GAME_STATE_PULL_V1.md` on branch
  `feat/kalshi-game-state-pull-v1`;
- [`universe/README.md`](../../universe/README.md) and
  [`docs/DEPLOYMENT.md`](../DEPLOYMENT.md);
- the design reference in the "Event Research — Backend Design" doc. Where it
  and this spec differ, this spec wins.

## 0. Decisions and non-goals

1. **Python computes; the site is static.** Nothing in Part D computes
   economics, masks or book state. It renders published files.
2. **No new API server.** The Universe API stays as it is. Published files
   are served straight from object storage (§B5). The corpus tables are
   queried with DuckDB in the browser.
3. **The backend records; the frontend filters.** Parts A and B apply no
   depth, value or latency threshold. A minimum episode duration (default
   100 ms) and a minimum value are display filters.
4. **Latency is out of scope.** Episodes are shown against the recorded books.
   Whether one was actionable is a separate study.
5. **Outputs come from archived evidence.** Part B reads finished bench run
   directories, their prepared contexts and Part A's archive. It calls no
   live API.
6. **Episodes are published in full.** Published files are public.
7. **One codec.** All Zstandard work goes through the shared `encoder`
   package, and all object access through `archive` adapters. There are no
   `gcloud` or `gsutil` subprocesses and no new codec.

Non-goals for V1:
- the corpus runner (see above);
- per-lens deep-dive pages, depth or levels views, payoff research views;
- live data;
- removing the replay jobs UI or the `bundle_coverage` strategy;
- an SDK accessor for game state inside strategies (§A4 publishes the data
  that accessor will read).

## A0. Prerequisites

### A0.1 Merge the game-state pull

Merge `feat/kalshi-game-state-pull-v1` (commits `2527ea5`, `27260ec`). Part A
moves its code into a package (§A1). It doesn't change its formats.

### A0.2 SDK entity and reason tables as NDJSON (layout 3)

**Problem:** `entities.json` is one JSON record with every scope's entity list.
Each scope repeats identical descriptors (about 560 KB per scope for LoL), and
the record is capped by `bounds.MAX_METADATA` (8 MiB). Cross-venue and the
implication covers therefore fail above roughly 14–20 scopes.

**Change:** add output **layout 3**, identical to layout 2 except for the
tables:

| Layout 2 file | Layout 3 file | Rows |
|---|---|---|
| `entities.json` | `descriptors.ndjson` | `{hash, descriptor}`, one per distinct entity hash, sorted by hash |
| | `entities.ndjson` | `{scope, entity, hash}`, one per (scope, entity index), sorted by scope, then entity |
| `reasons.json` | `reasons.ndjson` | `{reason, value}`, one per reason index, in index order |

Requirements:
- Rows reference entities by `(scope, entity)` index exactly as in layout 2,
  so episode, denominator and slice rows are unchanged.
- Row and line bounds are the SDK's NDJSON bounds (`MAX_ROWS`, `MAX_LINE`).
  `MAX_METADATA` no longer applies to these tables.
- Every SDK strategy writes layout 3 by default. Layout 1 (complement
  policy 1, pinned by `complement_v1_golden.json`) is unchanged.
- `reader.py` and `aggregate_reader.py` read layouts 2 and 3. A layout-2 run
  still validates.
- The manifest records the layout. Closed schemas reject unknown layouts.
- `entity_tables.py`'s preflight check becomes a row-count check.
- **Regression first:** a test with 64 scopes of identical entities fails on
  layout 2 with the current `entity metadata preflight` error, and passes on
  layout 3. Its semantic output (episodes, denominators) must equal a
  15-scope run's rows for the shared scopes.

### A0.3 Detached state budget

**Problem:** `runtime.py` charges `static_reservation + json_cost(snapshot) +
json_cost(policy)` before any trading state. With about 55 scopes, the
snapshot alone plus the resolver reservation (complement, multi-market)
exceeds `bounds.MAX_STATE` (128 MiB).

**Change:**
- `MAX_STATE` becomes a per-run limit carried in the runner config
  (`limits.state_bytes`), defaulting to 128 MiB. It is not part of the
  experiment or semantic identity: it decides only whether a run fails, never
  its output.
- Corpus runs set it to 1 GiB.
- The snapshot is charged once at its real retained shape, not per scope. If
  `json_cost` over-counts repeated scope members, it is fixed with a unit test
  against `sys.getsizeof` traversal, as `bounds.py` already requires.
- **Regression:** the 55-scope Valorant context shape (hand-authored: 55
  scopes, same member count) fails today with `detached state budget`, and
  passes with `state_bytes` at 1 GiB.

## Part A. Game-state archive on the Universe VM

### A1. Package

The pull moves from `scripts/pull_kalshi_game_state.py` into a new package,
`gamestate/`:

| Module | Contents |
|---|---|
| `gamestate/kalshi.py` | Mapping, fetch and raw archive (from the script, unchanged formats) |
| `gamestate/timeline.py` | Timeline derivation (`timeline.v1.json`) |
| `gamestate/run_pull.py` | The existing CLI (bundles, interval, regenerate) |
| `gamestate/run_scheduled.py` | The scheduled job (§A2) |

`AGENTS.md` §5 gains a row:

| Layer | Owns | Must not own |
|---|---|---|
| `gamestate/` | Public game-state pulls, raw archive, timeline derivation | Book interpretation, economics, capture |

It depends on `archive`, `encoder` and Universe's HTTP API, never on
`universe` internals. `scripts/pull_kalshi_game_state.py` becomes a thin
wrapper around `run_pull`, so existing commands keep working.

### A2. Scheduled job

`python -m gamestate.run_scheduled` is a one-shot run. Host cron runs it
every 30 minutes, as a new `event-universe-game-state` service in
`compose.universe.yaml` under profile `jobs`, alongside sync.

On each run:
1. Page `GET /v1/bundles` on the internal Universe URL, and keep bundles with
   `lifecycle == "retired"`.
2. For each, read its history to get `retired_at`. A bundle is **eligible**
   when `retired_at` is at least `settle_delay` (default 2 h) in the past.
3. Skip a bundle whose milestone already has a `complete` receipt. The
   existing skip rules apply unchanged.
4. Pull eligible bundles with the existing mapping, fetch, archive and
   timeline code.
5. Record outcomes in a retry ledger, `gamestate.sqlite3`:
   - schema: `fetch_attempts(bundle_id, attempted_at_ns, outcome,
     milestone_id, prefix, error)`, append-only;
   - an `incomplete` receipt or `fetch_failed` is retried with capped
     backoff (1 h, 2 h, 4 h, up to 24 h), at most 5 attempts;
   - a new attempt always gets a new fetch prefix, and raw objects are never
     rewritten;
   - unmapped reasons other than `fetch_failed` (such as `no_kalshi_events`)
     are final and are not retried.
6. Exit non-zero when any eligible bundle failed this run, as sync does.

Config is the closed JSON `configs/gamestate.json`, version 1:
- `universe_base_url`;
- `universe_spacing_seconds` (0 for the internal URL, which the Universe rate
  limiter exempts);
- `kalshi_spacing_seconds` (0.2);
- `settle_delay_seconds`;
- `max_bundles_per_run` (default 50);
- `ledger_path`;
- the archive backend fields used by the script today.

Secrets come only from environment variables.

**Backfill:** the same entry point with `--backfill --activation-start
2026-08-01T00:00:00Z --activation-end 2026-10-06T00:00:00Z` runs once over
every retired bundle in the archive, ignoring `max_bundles_per_run`. This is
how the existing corpus gets its game state.

### A3. Calibration windows

Part A records raw facts only. Window bounds are applied downstream (§B3.4)
from `configs/research.json` (§B1), which holds them per source and time
basis:

```json
"game_time_windows": {
  "kalshi_market_close": {"before_ms": 2000, "after_ms": 5000},
  "close_minus_duration": {"before_ms": 2000, "after_ms": 65000},
  "kalshi_milestone_end": {"before_ms": 2000, "after_ms": 5000}
}
```

These are starting values. §B4 measures the corpus distribution that replaces
them. A future multi-source normalizer merges reports of one game event
within a buffer: the first report sets the window's start and the last sets
its end. V1 has one source.

### A4. Finding game state for a bundle

There is no separate index. A receipt lists its `bundle_ids`. The corpus
runner lists `gamestate/source=kalshi/` receipts once per pass, validates each
with the existing strict reader, and builds a map from bundle to (milestone,
newest `complete` receipt, newest timeline version). A bundle with only
incomplete receipts maps to `incomplete`; one with none maps to `no_source`.

### A5. Tests

All offline, with hand-authored contract shapes:
- eligibility by `retired_at` and settle delay;
- skip on a complete receipt;
- retry and backoff ledger, and final unmapped reasons;
- the new prefix on retry;
- non-zero exit on failure;
- the closed config;
- the existing game-state suite keeps passing after the package move.

## Part B. Research outputs

The package is `replay/research/`, run as
`python -m replay.research <verify|build|publish>`. It never runs a strategy.
It reads the outputs of runs done by hand.

### B1. Inputs and configuration

The run list, `runs.json` (closed, version 1), names finished work:

```json
{"runs_version": 1,
 "events": [{"bundle_id": "bundle_...",
             "context": "<prepared context directory>",
             "runs": {"profile": "<bench run directory>",
                      "cross_venue": "<bench run directory>"}}]}
```

Inputs:
- Each bench run directory must hold a `result.json`. Its groups are mapped
  to lenses by the config.
- A lens missing from an event is recorded as `not_run`. It is a state, not
  a failure.
- A bench result other than `SUCCESS` is recorded as `failed`, with the
  result's error.

Config is the closed JSON `configs/research.json`, version 1. Unknown
fields fail. Fields:
- `lenses`: lens name → `{group, adapter}`;
- `game_time_windows` (§A3);
- `archive`: the archive backend fields, for game state and the private run
  archive;
- `publish`: the public bucket env var name, prefix `corpus/`, public base
  URL, and private prefix `research-runs/`.

Bucket names come only from environment variables named in `.env.example`
and are never committed.

Lens groups recommended for hand runs (bench specs):

| Lens | Group factory | Notes |
|---|---|---|
| `profile` | `replay.strategies.market_profile:build` | Every group except `levels`, `bucket_ns` 60 s. It feeds the chart (§B3), so it's required for a pack. |
| `cross_venue` | `replay.strategies.cross_venue_arbitrage:build` | Fill mode, policy 3, `edge` sizing, with a pinned fee catalog |
| `multi_market` | `replay.strategies.same_venue_multi_market:build` | |
| `implication_cover_same` / `_cross` | the two implication-cover factories | |
| `complement` | `replay.strategies.same_venue_complement:build` | Layout 2 policy |

An event without an `ok` profile run gets no pack. It is listed in
`events.parquet` with that reason.

### B2. Verify

`verify` checks each listed run without importing the collector, runtime or
reader under test. These checks move from scratch tools into
`replay/research/verify/`:

- **profile:**
  - the trade-row count equals the activity group's trade count;
  - each book's availability intervals exactly partition its in-scope time;
  - no Kalshi complement book is written;
  - with `levels` on, the levels replay matches every top row
    (`check_v2_streams.py`).
- **fill lenses:**
  - every fill leg's fee is recomputed exactly from the fee catalog;
  - net = gross − fees;
  - every `edge` stop is proven by its `beyond` levels;
  - every kill price is consistent (from `.bench/cross/verify_fills.py`).

Results go to `verify.json` (lens, run, check, pass, message). `build`
reads it: a failed check marks that lens `failed` with the message in the
pack. Output is never patched.

### B3. Build: research packs

Each event gets a pack under `packs/<event_id>/`. Every file is plain JSON,
with UTF-8 sorted keys and LF termination. Large integers are decimal
strings. The manifest lists every file's SHA-256 and byte length.

#### B3.1 `manifest.json`

```json
{
  "pack_version": 1,
  "event_id": "...", "bundle_id": "...", "game": "lol",
  "title": "...", "participants": ["..."],
  "capture_start_ns": "...", "capture_end_ns": "...",
  "books": [{"book": 0, "instrument": "...", "venue": "kalshi", "market_id": "...",
             "label": "...", "claim_id": "... or null", "price_scale": 4}],
  "lenses": {"cross_venue": {"state": "ok", "semantic_sha256": "...", "overlay": "overlays/cross_venue.json"},
             "complement": {"state": "failed", "error": "..."},
             "multi_market": {"state": "not_run"}},
  "game_state": {"state": "ok|incomplete|no_source", "file": "game.json"},
  "provenance": {"corpus_version": "...", "snapshot_sha256": "...", "pins_sha256": "...",
                 "source_commit": "..."},
  "files": {"overview.json": {"sha256": "...", "byte_length": 0,
                              "stored": {"sha256": "...", "byte_length": 0}}}
}
```

A file's top-level `sha256` and `byte_length` are those of the decoded JSON,
which is what a browser sees after HTTP decompression. `stored` is the gzip
object's identity, which the runner uses to verify its own uploads.

Details:
- `books` lists the profile's written books: Kalshi as the Yes book with a
  projected ask, Polymarket per token.
- `claim_id` comes from the context's outcomes. Two books that share a claim
  are **cross-venue twins**, and the site groups them in the legend.
- `capture_start_ns` is the first scope's start. The chart's zero is capture
  start, not scheduled start.

#### B3.2 Price series

All series come from the profile's `transitions` `top` rows. A book's state
holds from one top row to the next within a scope, and it is unknown outside
its scopes.

- **`overview.json`:** the whole event in at most 2,048 equal buckets. Per
  book per bucket:
  - `bid` and `ask`, each `[min, max, last]` in price atoms (or `null`);
  - `usable_ppm`, the parts per million of the bucket during which the book
    was usable;
  - `trades` and `trade_qty`.

  A bucket with zero usable time has null quotes, so the chart breaks there.
- **Tiles:** `tiles/1s/<tile>.json`, 10-minute tiles of 1-second buckets in
  the same shape, and `tiles/raw/<tile>.json`, the exact top rows and trade
  rows inside the tile. `<tile>` is the tile's start offset from capture
  start, in whole minutes.

Tile files are written only for 10-minute ranges where at least one book is
in scope.

#### B3.3 `availability.json`

Per book, the usable and unusable intervals with reason kind, from the
profile's `availability.ndjson` and the top rows' `validity`.

#### B3.4 `game.json`

Built from the newest `complete` timeline (§A4), or written as `incomplete`
or `no_source`. Each event is
`{kind, map_index, label, source, time_basis, source_ns, earliest_ns, latest_ns, exact, known_at_ns, detail}`:
- `earliest_ns` and `latest_ns` apply the §A3 window for its `time_basis`.
- `exact` is true only for settlement, a market event.
- `known_at_ns` = `latest_ns`. A lens or the as-of cursor treats a fact as
  known only from then.
- A scheduled start has `kind: "scheduled_start"` and no window.
- `detail` copies the timeline's map scores and winner. It never carries a
  game clock or a field the source lacks.

A hand run may attach the same document to its prepared context as
`context/game_state.json`, for future SDK use. The context's identity
includes it only when present.

#### B3.5 Overlays

`overlays/<lens>.json` has one of three generic shapes, so any future Python
model can produce one without a site change:

```json
{"overlay_version": 1, "lens": "cross_venue", "kind": "intervals",
 "label": "Cross-venue complete set", "items": [...]}
```

- **`intervals` item:**
  - `id`, `start_ns`, `end_ns`, `books` (indexes), `label`;
  - `censored`, `end_reason`;
  - `metrics`: a list of `{name, value, unit}`, such as
    `{name: "net", value: "12.34", unit: "USD"}`,
    `{name: "quantity", value: "1158", unit: "contracts"}` and
    `{name: "duration", value: "106", unit: "ms"}`;
  - `detail`, a free object.
- **`series` item:** `{t_ns, value}`, with a top-level `unit`.
- **`markers` item:** `{t_ns, label, books, detail}`.

The SDK adapter, `replay/corpus/overlays/sdk_episodes.py`, maps each layout-3
episode row to one interval:
- `books` comes from the entity descriptor's legs;
- `metrics` come from the governing fill result: `value` as net, leg
  `atoms` as quantity, and `cost`, with units from `fill.units`;
- `duration` is `end_ns − start_ns`;
- `detail` carries `kind`, `viable_tiers`, `kill_prices` and `end_books`.

Episodes with no fill get duration only. Nothing is filtered.

### B4. Build: corpus tables and viability report

Under `corpus/<version>/`:

| File | One row per | Fields |
|---|---|---|
| `events.parquet` | Event | `event_id`, `bundle_id`, `game`, `title`, `capture_start_ns`, `duration_s`, `venues`, `book_count`, `scope_count`, `game_state`, per-lens state, per-lens episode count |
| `episodes.parquet` | Episode, every lens | `event_id`, `lens`, `episode_id`, `start_ns`, `end_ns`, `duration_ms`, `books`, `markets`, `venues`, `venue_pair`, `censored`, `end_reason`, `net`, `net_unit`, `quantity`, `seconds_to_capture_end`, `route_id`, `shares_leg_with` (count of overlapping episodes sharing a book side) |
| `summary.json` | Lens | Histograms: episodes per event, duration (log buckets), net and quantity; with and without the 100 ms filter |
| `calibration.json` | Source time basis | Measured lag distribution (§A3): for each map, the time between the map-winner market's last trade or quote crossing 2¢ or 98¢ and its `close_time`. p5, p50, p95 and n. |

`viability.md` is a generated report answering the backend design's table:
- episodes per event (mean, median, share of events with at least one), per
  route and deduplicated by shared legs, both shown;
- duration distribution, with the share of episodes at 100 ms or more;
- fill quantity and net distribution;
- the same breakdowns by game, market type and venue pair.

It states its corpus version and the count of failed and excluded events.

### B5. Publish

- Objects go up through the `archive` object-store protocol (`put_immutable`
  with stored identity) under `corpus/<version>/`, where version is
  `<UTC %Y%m%dT%H%M%SZ>-<first 12 hex of the manifest SHA-256>`.
- Order: pack files, tables, `manifest.json` (listing every object with its
  identity), then `corpus/current.json` =
  `{version, manifest_key, manifest_sha256, manifest_byte_length}`.
- `current.json` is the only mutable object. It is overwritten only after
  the new manifest is verified by re-reading it.
- Versions are never deleted by the runner.
- `corpus/` is public-read. Raw captures, canonical windows and game-state raw
  responses are not published; only derived files are.
- Objects are uploaded with `Content-Type: application/json` (or the Parquet
  type) and served with compression (gzip `Content-Encoding` at upload).
  Parquet is not gzipped, so range requests work.

**Where outputs live and how they are served:**

| What | Where | Access |
|---|---|---|
| Published packs and tables | A **separate public bucket**, under `corpus/<version>/`. GCS grants access per bucket under uniform access, so the private archive bucket stays private. | Public read. Browsers fetch over HTTPS from the bucket endpoint, or a CDN in front of it. CORS allows the site origin, `GET` and `HEAD`, and exposes `Content-Range`, `Content-Length` and `ETag` for Parquet range reads. |
| Source run outputs (bench run directories and prepared contexts) | The existing private archive bucket, under `research-runs/<bundle_id>/<lens>/<semantic_sha256>/`, uploaded with `put_immutable` | Private. Kept for provenance and rebuilds. |

No server process is involved. The site needs only the public base URL.


### B6. Tests and acceptance

Unit tests (offline, hand-authored inputs):
- overview and tile bucketing from top rows, including null buckets across a
  gap and a scope exit;
- game windows and `known_at_ns`;
- the overlay adapter from a hand-authored fill episode;
- lens states (`ok`, `failed`, `not_run`);
- table schemas;
- publish order with the pointer last;
- refusal to publish on any identity mismatch;
- a re-publish of identical content returning the same version files.

Acceptance:
1. Build and publish to a test prefix from the existing LoL C9–LYON bench
   acceptance runs, plus one cross-venue run of the same bundle.
2. An independent script (not importing `replay.research`) recomputes the
   overview's bid and ask `last` per bucket from `transitions.ndjson.zst`
   and matches every value.
3. Every overlay interval matches an `episodes.ndjson` row.
4. Fetching each published file over public HTTPS returns bytes matching the
   manifest identity.

## Part C. Universe directory layout

This part is restructuring only: no change to behavior, schema, endpoints,
config or output.

### C1. Package layout

Today `universe/` is 19 flat modules (about 9,100 lines). It mixes HTTP,
storage, ingestion, derivation and the jobs control plane, and `store.py`
alone is 3,045 lines. The target:

```text
universe/
  README.md
  __main__.py            python -m universe {serve,sync,backfill,backup}
  config.py
  schema/                schema.sql, replay_auth.sql, replay_jobs.sql (unchanged)
  store/
    __init__.py          UniverseStore facade; public methods unchanged
    connection.py        open, pragmas, schema validation
    ingest_tx.py         the admitted-run write transaction
    reads/               events.py, markets.py, claims.py, bundles.py, runs.py, health.py
    backup.py
  ingest/
    sync.py, backfill.py
    projection.py, market_projection.py, event_identity.py
  claims/
    claim_projection.py, outcomes.py
  api/
    server.py            ThreadingHTTPServer and dispatch
    routes.py            one route table replacing the if-chain in api.py
    framing.py           JSON encoding, limits, cursors, error mapping
    auth.py, rate_limit.py
    replay_jobs.py       job endpoints (retiring with the jobs UI)
  jobs/
    store.py             today's replay_jobs.py (retiring)
```

Rules:
- **Moves first.** Files move with `git mv` in their own commits, so history
  follows. Edits come in later commits.
- **Shims.** `run_server.py`, `run_sync.py`, `run_backfill.py` and
  `run_backup.py` become three-line shims for `python -m universe …`. After
  compose and the docs switch, the shims are removed in one commit.
- **Stable store API.** `UniverseStore`'s public methods and signatures don't
  change, and tests keep importing `universe.store`.
- **Route table.** Each entry is `(method, pattern, handler, auth)`. Unknown
  parameter rejection, the response budget and error mapping move into
  `framing.py` unchanged.
- **Dependency direction:**
  - `api` may import `store`, `claims` and `jobs`;
  - `ingest` may import `store` and `claims`;
  - `store` and `claims` import nothing from `api` or `ingest`.
  A test asserts this, as the existing archive/targeter test does.
- **Out of scope:** the `targeter.v2.models` timestamp helpers stay where
  they are.

Gates:
- the full Python suite passes;
- `tests/generate_event_universe_contract.py` produces byte-identical output
  before and after;
- `docker compose -f compose.universe.yaml config --quiet` passes.

### C2. Data layout on the VM

Compose binds three roots today: `EVENT_UNIVERSE_DATA_ROOT`,
`EVENT_UNIVERSE_ARCHIVE_ROOT` and `REPLAY_DATA_ROOT`. The auth tables live in
the jobs database. This part adds one root and documents all of them:

| Root (env var) | Path | Holds | Rebuildable |
|---|---|---|---|
| `EVENT_UNIVERSE_DATA_ROOT` | `event-universe.sqlite3`, `backups/` | Event Universe DB and backup staging | Yes, by backfill |
| `REPLAY_DATA_ROOT` | `jobs.sqlite3` | Jobs and auth tables | **No** |
| `GAMESTATE_DATA_ROOT` (new) | `gamestate.sqlite3`, `tmp/` | Game-state retry ledger, pull temp files | Yes. Skip relies on archive receipts, so losing it only resets backoff. |
| `EVENT_UNIVERSE_ARCHIVE_ROOT` | (local backend only) | Local archive objects | — |

Existing paths and env vars don't change, so no data moves on the VM.
`docs/DEPLOYMENT.md` gains this table, with each path's owning service and
its backup rule. Moving auth out of `jobs.sqlite3` waits for the jobs
retirement.

## Part D. Research site

Built after Part B has published a version. It extends `targeter-ui`
(React 19, Vite, react-query) and is hosted as today.

### D1. Routes and data

| Route | Page |
|---|---|
| `/research` | Corpus view (§D2) |
| `/research/events/:eventId` | Event terminal (§D3) |

Existing routes are unchanged. The nav gains "Research".

Data loading:
- Config: `VITE_CORPUS_BASE_URL`, the public base of `corpus/`.
- Load `current.json`, then the version manifest. Every file fetch checks
  byte length, and SHA-256 via `crypto.subtle`, against the manifest. A
  mismatch shows an error state; data is never shown unverified.
- The corpus tables load in **DuckDB-WASM** (`@duckdb/duckdb-wasm`), reading
  Parquet over HTTP range requests. Queries are built from fixed templates
  with bound parameters.
- No auth.

### D2. Corpus view

- **Headline**, from `summary.json` and DuckDB: events, mean and median
  episodes per event, share of events with at least one, total net. Per lens,
  with a per-route / deduplicated toggle.
- **Filters:** game, venue pair, lens, date range, minimum duration (default
  100 ms) and minimum net. Filters change the display only and show "n hidden
  by filter".
- **Distributions:**
  - episodes per event (histogram);
  - duration (log-scale histogram, with the 100 ms line);
  - net and quantity.
- **Events table:**
  - columns: title, game, date, venues, books, game state, a per-lens
    episode count and max net, and a lens state badge (`failed` shows its
    reason);
  - sortable;
  - a row click opens the terminal.

### D3. Event terminal

This follows the prototype's interaction design, adapted:

- **Chart:**
  - one dominant chart built on **uPlot**;
  - x is time since capture start (labelled `+hh:mm:ss`), with absolute UTC
    in hover;
  - y is ¢ per $1;
  - each market draws its bid–ask band with the mid as a line, with a
    mid-only toggle;
  - lines break where the book is unusable or out of scope, never
    interpolated.
- **Zoom:**
  - wheel and drag to zoom, double-click to reset;
  - the source switches with the visible span: above 2 h uses the overview,
    from 2 h down to 10 min uses 1 s tiles, and 10 min or less uses raw
    tiles, drawn as step lines;
  - tiles are fetched lazily, verified and cached.
- **Legend** below the chart:
  - a checkbox per book, with colours stable per book;
  - hiding a book doesn't rescale the chart or change any data;
  - cross-venue twins (shared `claim_id`) sit together under one claim label;
  - venue is shown on every entry.
- **Tracks**, on the same time axis:
  - **Game:** a bracket from `earliest_ns` to `latest_ns`, a diamond only when
    `exact`, and a dashed line for a scheduled start. Hover shows the source,
    time basis, raw timestamp, window and detail.
  - **Lens:** the selected lens's intervals. At wide zoom, sub-pixel episodes
    collapse into a density strip with counts.
  - **Books:** unusable intervals per book, hatched, with the reason on
    hover.
- **Lens selector:** "No lens" or any lens with `state: ok`. Failed lenses
  appear disabled, with their reason.
- **Episode selection:**
  - zooms to the interval with padding and highlights its books;
  - the details panel shows lens, duration, end reason, censored, every
    metric with its unit, the detail fields, and provenance (corpus version,
    semantic SHA-256, snapshot);
  - modeled value is labelled as modeled, never as a return.
- **Cursor:**
  - click or scrub sets it;
  - the readout shows bid, ask and mid per visible book, and game state as
    of the cursor (facts with `known_at_ns` at or before the cursor).
- **Playback** (secondary):
  - play and pause, speed;
  - "Next moment" jumps to the next game fact, episode start or availability
    change;
  - optional auto-pause on those;
  - "Full history" off hides everything after the cursor.
- **Explicit states:** game state `no_source` or `incomplete`, a failed lens,
  a book with no usable time. Each has its own visual; none renders as empty
  or zero.

### D4. Performance and tests

Budgets:
- the first paint of an event fetches at most 3 MB (manifest, overview,
  availability, game and the selected overlay);
- pan and zoom stay smooth with 30 books at 2,048 points.

Tests (`node --test`, offline) cover the view models with a hand-authored
pack:
- bucket-to-path breaks at gaps;
- the zoom-tier choice;
- the twin grouping;
- the game bracket and as-of logic;
- the overlay density collapse;
- filter counts;
- the manifest identity check.

There are no live fixtures.

## Delivery

Order of commits and steps:

1. A0.1, A0.2 and A0.3, each a separate commit with its regression test.
2. Part C, moves first and edits after. Its gates pass before Part A starts,
   because Part A edits `compose.universe.yaml`.
3. Part A.
4. **Operator:** deploy the game-state job on the VM, then run its backfill.
5. Part B, with its acceptance on the LoL C9–LYON runs.
6. **Operator:** hand runs over the chosen events, then `build` and
   `publish`.
7. Part D.

Each step updates the owning README, and `AGENTS.md` (routing table and
component boundaries), for `gamestate/`, `replay/research/` and the new
`universe/` layout.

## Open questions

- Should a book that leaves and re-enters scope start a new path in tiles?
  The profile already writes a fresh opening row. Confirm on a real event.
- Should `summary.json` histograms use fixed bucket edges across versions, so
  versions compare directly? The proposal is yes, with fixed log edges for
  duration.
- What are the public bucket's name and region, and is a CDN worth adding in
  front of it, or is direct GCS serving enough at current traffic?
