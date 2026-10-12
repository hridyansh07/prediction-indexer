# Event research V1: game-state archive, SDK limits and Universe layout

Status: **A0.1, A0.2, A0.3, Part A and Part C implemented.**
Base: `master` at `bd63927`.

This spec covers what the strategy corpus needs beyond the strategies
themselves:

- **A0, prerequisites:** the game-state merge and two SDK limits that break
  long events.
- **Part A, game-state archive on the Universe VM:** a scheduled Python job.
  When a bundle goes terminal, it pulls Kalshi game state, normalizes it and
  archives it.
- **Part C, Universe directory layout:** restructure the `universe/` package
  and document the VM's data paths, with no behavior change.

**Research outputs are the strategy outputs.** The corpus runner runs every
strategy over every retired bundle and commits each bundle's bench outputs
with a receipt. Those committed outputs are the research record. Conclusions
are drawn by a separate, later pipeline that reads them; it is not part of
this spec and writes nothing back into them.

**Dropped from the original draft:** Part B (a `replay/research/` package
that verified runs, built per-event research packs and corpus tables and
published them) and Part D (a public research site in `targeter-ui`). The
section letters are kept so existing references still resolve.

Read first:

- [`AGENTS.md`](../../AGENTS.md), especially the invariants on capture,
  archives, the shared encoder and persisted formats;
- [`docs/ECONOMIC_STRATEGY_SDK_V1.md`](../ECONOMIC_STRATEGY_SDK_V1.md) §5
  (output), §8 (bounds), §10 (corpus runs) and §13 (fill checks);
- [`docs/LOCAL_REPLAY_BENCH_V1.md`](../LOCAL_REPLAY_BENCH_V1.md);
- [`universe/README.md`](../../universe/README.md) and
  [`docs/DEPLOYMENT.md`](../DEPLOYMENT.md).

## 0. Decisions and non-goals

1. **The backend records; consumers filter.** Part A applies no depth, value
   or latency threshold. Thresholds belong to whatever reads the outputs.
2. **Latency is out of scope.** Episodes are measured against the recorded
   books. Whether one was actionable is a separate study.
3. **Outputs come from archived evidence.** Game state and strategy runs read
   archived objects and prepared contexts, not live APIs at analysis time.
4. **One codec.** All Zstandard work goes through the shared `encoder`
   package, and all object access through `archive` adapters. There are no
   `gcloud` or `gsutil` subprocesses and no new codec.

Non-goals for V1:
- the corpus runner itself (its own work; §A4 says how it finds game state);
- the secondary analysis pipeline over corpus outputs;
- research packs, published corpus tables and a research site;
- live data;
- removing the replay jobs UI or the `bundle_coverage` strategy.

## A0. Prerequisites

### A0.1 Merge the game-state pull

Merge `feat/kalshi-game-state-pull-v1` (commits `2527ea5`, `27260ec`). Part A
moves its code into a package (§A1). It doesn't change its formats.

### A0.2 Raise the SDK entity-table limit

**Problem:** `entities.json` is one JSON record with every scope's entity list.
Each scope repeats identical descriptors (about 560 KB per scope for LoL), and
the record was capped by `bounds.MAX_METADATA` (8 MiB). Cross-venue and the
implication covers therefore failed above roughly 14–20 scopes. Retired
bundles measured in corpus trials need 13.8–16 MB.

**Change (decided):** raise the limit rather than change the output layout.

- A new `bounds.MAX_ENTITY_TABLE` of 64 MiB bounds `entities.json` (real and
  control groups) in the preflight, the streaming writer and both readers.
- `MAX_METADATA` stays 8 MiB for `reasons.json`, configurations, summaries and
  manifests.
- The layout, file names, schemas and preflight error text are unchanged, so
  existing layout-2 runs still validate and output identities do not move.
- `docs/ECONOMIC_STRATEGY_SDK_V1.md` §5 and §8 record the new limit.
- No new regression: the existing exact-bound and one-byte-over tests in
  `replay/tests/test_cross_venue_metadata.py` now pin the 64 MiB bound.

A deduplicated layout (distinct descriptors in their own NDJSON table,
referenced by hash from each scope) remains possible if events outgrow 64 MiB.
It is not part of V1.

### A0.3 Detached state budget

**Problem:** `runtime.py` charges `static_reservation + json_cost(snapshot) +
json_cost(policy)` before any trading state. With about 55 scopes, the
snapshot alone plus the resolver reservation (complement, multi-market) is
expected to exceed `bounds.MAX_STATE` (then 128 MiB), which fails the attempt with
`detached state budget`.

**Decision:** no runner-config plumbing. Corpus runs execute in Docker with
a fixed container memory limit, which is the real protection against runaway
memory. `MAX_STATE` is an accounted, in-process check, so the container limit
does not lift it by itself.

**Trigger:** if a real corpus bundle fails with `detached state budget`, raise
the `MAX_STATE` constant (as A0.2 raised the entity table) and rely on the
container limit.

**Triggered (2026-10-11):** the Valorant bundle `3864e…` (55 scopes) failed
every economic group at startup under 128 MiB. `MAX_STATE` is now 512 MiB. The
rerun committed, and its outputs matched an independent recomputation exactly.
The bench runner container does not yet set a memory limit; the corpus runner
must add one before relying on it.

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

Part A records raw facts only. Window bounds around game events are applied
downstream by whatever consumes the corpus outputs, per source and time
basis. Starting values:

```json
"game_time_windows": {
  "kalshi_market_close": {"before_ms": 2000, "after_ms": 5000},
  "close_minus_duration": {"before_ms": 2000, "after_ms": 65000},
  "kalshi_milestone_end": {"before_ms": 2000, "after_ms": 5000}
}
```

A measured corpus distribution should replace them. A future multi-source
normalizer merges reports of one game event within a buffer: the first
report sets the window's start and the last sets its end. V1 has one source.

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

## Delivery

| Step | State |
|---|---|
| A0.1 merge the game-state pull | Done (#61) |
| A0.2 raise the entity-table limit | Done (this branch) |
| A0.3 raise the detached state limit to 512 MiB | Done (this branch) |
| Part C, moves first and edits after | Done (#62) |
| Part A | Done (#62) |
| **Operator:** deploy the game-state job on the VM and run its backfill | Done 2026-10-09 |

Each step updated the owning README and `AGENTS.md` (routing table and
component boundaries) for `gamestate/` and the new `universe/` layout.
