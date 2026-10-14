# Universe

The Universe server is the query and control-plane service in front of committed
Targeter runs. It has three roles:

1. **Event Universe**: a rebuildable SQLite view of committed Targeter v3 runs
   (events, venue events, canonical and venue markets, selection decisions,
   claims, bundle history), served read-only over HTTP.
2. **Authentication**: Sign-In With Ethereum (SIWE) sessions and a member
   allowlist.
3. **Replay jobs control plane**: durable submission, idempotency, quota and
   transition records for Replay jobs (`universe/jobs/store.py`). The runner
   that executes jobs lives under `replay/` (see
   [`docs/REPLAY_JOBS_V1.md`](../docs/REPLAY_JOBS_V1.md)).

The SQLite database is a query accelerator, not an evidence archive. Immutable
Targeter run manifests and their objects in the configured object store remain
authoritative; the database can be deleted and rebuilt by backfill. Universe
never copies raw reports, raw catalogues, capture deliveries or replay state
into it. The API process reads SQLite only and needs no object-store
credentials; sync, backfill and backup own all archive access.

## Commands

Servers and jobs read the JSON config named by `EVENT_UNIVERSE_CONFIG`
(default `configs/event_universe.json`, config version 4;
`${VAR}` references are expanded, an unset reference is an error). Unknown
fields are rejected.

| Command | Purpose |
|---|---|
| `python -m universe serve` | Open/initialize all three SQLite schemas and serve the API (`ThreadingHTTPServer`, host/port from `api`) |
| `python -m universe sync` | One-shot incremental ingestion of newly committed runs |
| `python -m universe backfill` | One-shot oldest-first backfill of `backfill.generated_start` to `backfill.generated_end` (both required) |
| `python -m universe backup` | Online-backup the SQLite file, upload it immutably under `backup.object_prefix`, print path, key, SHA-256, length |
| `python -m universe migrate --to N SOURCE.sqlite3 DEST.sqlite3 [--input NAME=PATH ...]` | Offline SQLite backup into a new file, then consecutive migration transactions; currently m1 (6 → 7) |

Servers and jobs take no options; everything comes from the config. `migrate`
takes explicit paths, never loads that config, and m1 uses no archive or network.

`compose.universe.yaml` runs `event-universe` (the server) and the profile
`jobs` services `event-universe-sync`, `event-universe-backfill` and
`event-universe-backup`, each as a one-shot container; scheduling is external.
Sync and backfill exit non-zero when any run failed or failures remain pending.
Deployment is described in [`docs/DEPLOYMENT.md`](../docs/DEPLOYMENT.md).

## Source contract

The only admitted commit marker is
`targeter-v2/runs/date=YYYY-MM-DD/run=<run_id>/run_manifest.json`. Universe
accepts manifest version 2 carrying one Targeter v3 shadow selection report;
run ID, generated time, completeness and strategy version must agree. It also
reads the manifest-owned normalized event/market NDJSON needed to resolve
candidate references.

Object access is provider-neutral and verified by the shared archive and encoder
packages: metadata via `verify_metadata_objects` in manifest order, bounded JSON
via `archive.read_verified_json`, NDJSON via `ArchivedObjectByteStreamer`.
Universe does not implement store reads, checksums, or Zstandard decoding.

Bounds (`universe/ingest/sync.py`): manifest 16 MiB; selection report 128 MiB; selected
normalized catalogue artifacts 128 MiB decoded per run (warning at 96 MiB); one
NDJSON row 4 MiB; 100,000 catalogue references per report. Only artifacts for
venues referenced by complete-run candidates are retrieved; their stored/logical
identities and line counts are still verified, but only referenced rows are
retained. Invalid NDJSON framing rejects the containing artifact.

Incomplete and complete-empty runs are recorded in `targeter_runs` but create no
event, market, decision or claim rows.

### Selection continuity

A current selection may be absent from the current catalogue. Universe
recursively verifies and ingests its exact complete origin, then copies the
origin's normalized selected-market references into the current run. Missing,
cyclic, mismatched or non-complete origins reject the transaction. Terminal
observation is an upper bound; a clamp is not an exact event end.

## Sync and backfill

Each admitted run and both projections are written in one `BEGIN IMMEDIATE`
transaction (foreign keys on, WAL, `synchronous=FULL`). A projection identity
over the resolved rows detects changed re-ingestion; re-ingesting the same run
is idempotent, except a migrated market-projection v3 run: it fails with
`run … was migrated from projection v3; rebuild to re-ingest it`, including
the sync manifest-known fast path. New runs still ingest at v4.

**Incremental sync** (`UniverseSync.sync`) advances a high-water date
independently of bad manifests. Failures go to the durable
`universe_sync_failures` ledger with capped exponential backoff (at most one
day); each invocation retries at most 32 ledger items, so a systematic
corruption stays visible without unbounded rescans. Malformed listed keys use
the same ledger. A fresh database bootstraps newest-first through at most 144
valid manifests looking for the newest complete run; exhausting that budget is
an explicit degraded result and full history comes from backfill.

**Backfill** (`backfill_targeter_history`) processes the half-open
generated-time range in 100-run batches, prints one JSON progress record per
batch, and checkpoints each committed batch, so restarting the same range
resumes after its cursor. Origin dependencies outside the range are ingested
when needed to prove continuity. Failed manifests are recorded in the ledger,
the scan continues, and `/healthz` stays degraded until they ingest.

Because event ordinals disambiguate same-day evidence with no surviving alias,
the canonical rebuild is oldest-first from an identity-empty database.
`event_identity_lineage` claims the range before the first allocation; a resume
must use identical bounds. Ordinals are encounter-ordered and never renumbered;
repeating the same ingestion order repeats IDs, while a different admitted set or
order can change ordinals for otherwise indistinguishable same-day occurrences.
Universe never prunes history automatically.

## Identity

Umbrella event identity is allocated by Universe, not by Targeter or row order.
Each native reference `(venue, venue_event_id)` resolves through the durable
`venue_events` alias edges:

1. all known aliases name one umbrella event: reuse it and attach new aliases;
2. no alias known: allocate a new identity;
3. aliases name more than one umbrella event: fail the transaction closed.

The public ID is `event:d1:<sha256>` over canonical JSON of
`identity_version = 1`, sport, optional game, optional topology, sorted
participant keys, the UTC activation date observed at allocation, and an
immutable zero-based ordinal among otherwise equal events on that date. Native
refs, exact activation time, bundle ID, titles and venue membership are not in
the preimage. The identity date is frozen at allocation; each run records its
exact observed activation in `event_observations`, and the umbrella row exposes
the first observation as its display time. If every venue replaces every native
ID at once, continuity cannot be proven and a separate event is allocated.
Reusing a known alias with different sport, game, topology or participant keys
fails closed. Venue-native event and market IDs are assumed globally unique and
never reused.

A canonical market ID is a deterministic digest of umbrella event ID, canonical
class and market type, scope, and normalized semantic parameters.
`market_template_version` and `outcome_space_version` are explicit key columns,
not part of the ID. First/last-seen run IDs make continuity explicit.

## Claims and relations

A market's semantic content for relationship purposes is the subset of its
outcome space it resolves YES on (see [`analysis/README.md`](../analysis/README.md)).
Outcome keys are participant-independent, so a claim is global:

- `claim_classes`: one row per distinct outcome subset within a space shape
  (`claim_id` digests subset and shape; coverage, scope, key count, first/last
  seen runs). Equal subsets are one claim, so cross-venue equivalence is two
  `market_claims` rows sharing a claim; IDENTITY is never a stored relation.
- `claim_relations`: how two claims of one shape relate, with no event, run,
  venue or bundle. Types are `IMPLICATION` (antecedent first; reverse
  implication is normalized) and `MUTUAL_EXCLUSION`. OVERLAP is the mask
  comparison's catch-all, not a finding, and is not stored.
- `market_claims`: which claim a venue market's tradable token expresses, as
  eras. A market whose semantics change gets a new row; reads use only the
  current era.

Nothing is keyed by run, so re-observation writes no rows and only moves
last-seen markers. `first_seen_run_id`/`last_seen_run_id` are Targeter
observation bounds resolved against `targeter_runs.generated_at_ns`, not
ingestion order and not lifecycle: absence after the last-seen run is ambiguous
(settled, delisted, or no longer a candidate). A claim whose space shape changes
mints a new claim; coverage `INCOMPLETE_COVERAGE` keeps findings conditional.

Claims are recomputed per candidate bundle, over the bundle minus its excluded
markets, from Universe's own rows (`universe/claims/claim_projection.py`), so no
Targeter change is needed to re-project history. Ingestion checks the
reconstruction against the report's recorded cross-venue relations: a relation
the claims invent is a guessed equivalence and rejects the run
(`EvidenceConflict`); a relation they miss is counted in
`universe_run_projections.claim_relation_shortfall` (and bundles that cannot be
rebuilt in `unreconstructed_bundles`), visible rather than raised.

### Totals direction corrections (projection version 4)

Universe projects markets from the archived reports' stored catalogue
parameters, and an old targeter parser could read a Kalshi or Limitless totals
`direction` from a team name ("ThunderTalk" or "Sunderland" as `under`).
Projection version 4 re-derives `direction` for `kalshi` and `limitless`
`total_maps`/`total_goals` markets with the fixed whole-word rule from the
archived row's own title and event participants (plus, for Kalshi, the first
outcome label as the yes subtitle), changes nothing else, and records every difference in
`universe_run_projection_corrections` (`report_value`, `projected_value`,
`rule_id`; part of the projection identity). A direction the fixed rule cannot
find is removed, so the market yields no claim. A corrected market gets a
different canonical market ID than under version 3. Relations naming a corrected
market are verified against relationships re-derived from the corrected bundle
rather than the report's. Archived reports stay untouched. The previous schema-v6
file is the rollback and the backup. Either migrate it offline to v7 or perform
an oldest-first rebuild into a new file. Spec:
[`docs/specs/UNIVERSE_TOTALS_DIRECTION_V1.md`](../docs/specs/UNIVERSE_TOTALS_DIRECTION_V1.md);
compare an old and a rebuilt file read-only with
`python -m universe.tools.rebuild_diff OLD.sqlite NEW.sqlite`.

## HTTP API

All responses are strict JSON (sorted keys, `Cache-Control: no-store`) and must
stay under 1,750,000 bytes (else 413). Unknown query parameters are rejected
(400). List `limit` is 1 to 100 (default 100; `/v1/targeter/status` defaults to
5); cursors are opaque and query-specific. Detail documents are capped at 1,000
child rows (`DetailTooLarge` yields 413), except bundle contexts: those are
bounded by the response byte budget, since relationships grow with the square of
the market count (production reaches 3,240 rows at about 0.7 MB). A context read
loads at most 20,000 rows per child table. `/v1/bundles` reads only each
context's summary fields, so one large context never fails a list page. Request bodies must be a single JSON
object with `Content-Type: application/json`, at most 64 KiB, with unique keys
and exactly the documented fields.

Read (no authentication):

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Schema, latest run, staleness, normalized counts, sync failures |
| `GET /v1/targeter/status?limit=` | Compact landing status and newest complete selection counts |
| `GET /v1/targeter/runs/<run_id>` | Bounded decisions and normalized event/market references (no raw relation arrays) |
| `GET /v1/events?limit=&cursor=` | Event summaries with identity coordinates and native aliases |
| `GET /v1/events/<event_id>` | Event, venue events, canonical markets, claims, claim relations, observations |
| `GET /v1/markets/<market_id>?market_template_version=&outcome_space_version=` | Canonical market, venue instances, selections, claims |
| `GET /v1/claims/<claim_id>` | Claim, its reach, and its claim relations |
| `GET /v1/claims/<claim_id>/markets?limit=&cursor=` | Markets expressing the claim, paged |
| `GET /v1/bundles/<bundle_id>/outcomes` | Read-only normal-resolution spaces, claim keys, statuses and token alignment (`universe/claims/outcomes.py`; contract in [`docs/OUTCOME_MASKS_V1.md`](../docs/OUTCOME_MASKS_V1.md)); 409 if the bundle maps to more than one event |
| `GET /v1/bundles/<bundle_id>/history` | Selection occurrences of one bundle |
| `GET /v1/bundles?limit=&cursor=` | Bundles by last selection |
| `GET /v1/selections` | Selection occurrences (`activation_start/end`, `selected_start/end`, `venue`, `sort=activation\|selected`) |
| `GET /v1/runs` | Runs (`generated_start/end`, `input_complete`) |
| `GET /v1/runs/<run_id>`, `/audit`, `/selections`, `/selections/<bundle_id>` | Run detail, audit, and its selections |
| `GET /v1/relationship-types` | Closed stored relation-type catalogue (version 2: IMPLICATION, MUTUAL_EXCLUSION) |

Authentication and administration:

| Endpoint | Purpose |
|---|---|
| `GET /v1/auth/nonce` | Issue a SIWE nonce |
| `POST /v1/auth/siwe` `{message, signature}` | Verify and open a bearer session |
| `POST /v1/auth/logout` `{}` | Revoke the bearer session |
| `GET /v1/admin/allowlist` | List members (admin) |
| `POST /v1/admin/allowlist` `{address, note}` | Add a member (admin) |
| `DELETE /v1/admin/allowlist/<address>` | Remove a member (admin); the configured admin cannot be removed |

Replay jobs (served only when auth and the replay job store are configured):

| Endpoint | Purpose |
|---|---|
| `GET /v1/replay/strategies` | Strategy registry from the runner config |
| `GET /v1/replay/jobs?status=&limit=&cursor=` | List jobs |
| `GET /v1/replay/jobs/<job_id>` | Job record (validated against the registry at submission, not re-validated on read) |
| `GET /v1/replay/jobs/<job_id>/events` | Append-only transition events |
| `POST /v1/replay/jobs` | Submit (member; exactly one `Idempotency-Key` header). 201 on create, 200 with `replayed: true` for an idempotent repeat |
| `POST /v1/replay/jobs/<job_id>/cancel` `{}` | Cancel (member) |

Submission requires the bundle to exist in the selection history and respects
the configured quotas (`max_active_jobs_total`, `max_active_jobs_per_submitter`,
`max_queued_jobs_total`). A retired strategy still resolves for an idempotent
repeat of an earlier submission; new jobs naming it are refused.

`GET /v1/targeter/cadence` and `/v1/relations/<id>` no longer exist (404).

## Authentication and rate limiting

`universe/jobs/auth.py` implements SIWE: the server issues a nonce (kept in process
memory with a TTL, at most 500 live, lost on restart), verifies the signed
message against the configured domain, URI, statement and chain ID, and issues
a bearer token whose SHA-256 digest is stored in `sessions`. A member must be on
the allowlist; the configured `admin_address` is the admin. Allowlist changes are
recorded in the append-only `allowlist_events` table. Failed sign-ins return a
uniform 401. Session and nonce TTLs come from `replay.auth`.

`universe/api/rate_limit.py` applies a bounded in-process token bucket only to
requests whose TCP peer is in `replay.rate_limit.trusted_proxy_addresses`
(the reverse proxy); it keys on the final `X-Forwarded-For` address for
unauthenticated requests and on the session otherwise. Direct internal callers
are exempt. Exceeding the limit returns 429 with `Retry-After`.

## Schema and migrations

Two independent SQLite files:

- **Event Universe database** (`database_path`): `universe/schema/schema.sql`,
  `PRAGMA user_version = 7`. **Schema changes ship with an offline migration
  under [Universe offline migrations V1](../docs/specs/UNIVERSE_MIGRATIONS_V1.md)
  or require a rebuild. A migration never runs inside the server and never
  modifies the live file.** Startup validates the stored schema object-for-object
  against `schema.sql`; a stale version or modified schema fails with an
  instruction naming both offline migration and oldest-first backfill. An old
  v7 file without this specification's objects requires a rebuild.
  Tables: `targeter_runs` (exact manifest/report identities),
  `universe_run_projections`, `universe_run_projection_corrections`, `universe_migrations`, `umbrella_events`, `venue_events`,
  `event_observations`, `event_identity_lineage`, `universe_sync_failures`,
  `canonical_markets`, `venue_markets`, `candidate_decisions`,
  `selected_market_occurrences`, `claim_classes`, `claim_relations`,
  `market_claims`, `checkpoints`, and the bundle history tables
  (`bundle_contexts`, `context_*`, `selection_occurrences`,
  `bundle_retirements`) backing `/v1/bundles`, `/v1/selections` and run detail.
  `context_relationships` feeds `context_sha256` and is part of the identity of
  committed reports. Backup uses SQLite's online backup API followed by
  `integrity_check`.
- **Replay database** (`replay.database_path`, `jobs.sqlite3`): additive,
  non-rebuildable. `schema/replay_auth.sql` (`sessions`, `allowlist`,
  `allowlist_events`) and `schema/replay_jobs.sql` (job rows, idempotency
  `job_submissions`, component metadata, append-only `job_events`) coexist in one
  file; neither uses `user_version`. Each component validates only its own
  objects; replay jobs compares a SHA-256 over its ordered,
  whitespace-normalized `sqlite_master` definitions. SIWE nonces are not
  persisted. Never place these records in the rebuildable Event Universe
  database or remove them during a rebuild.

### Offline migration and its limits

```bash
python -m universe migrate --to 7 SOURCE.sqlite3 DEST.sqlite3
python -m universe.tools.rebuild_diff SOURCE.sqlite3 DEST.sqlite3
```

`SOURCE` opens read-only and SQLite's online backup API takes a consistent
snapshot, including committed WAL content. `DEST` must not exist. Each step
uses one transaction, validates the schema and foreign keys, records a STRICT
`universe_migrations` row, then sets `user_version` as its last statement before
commit. Failure rolls back that step, retaining the previous version in `DEST`;
already committed steps remain. The command prints the same JSON summary it
stores. It refuses downward migration, files older than v6, skipped versions,
and undeclared inputs. Delete only your failed disposable destination before a
retry; never overwrite the source.

m1 requires exact schema v6 and all market projections at v3. It reuses the
totals projector and claim reconstruction, preserving market hashes, volumes,
bundle evidence, and source identities. Complete stored contexts supply ordered
participants and candidate membership; unavailable/ambiguous participants or
unreconstructable affected candidate bundles abort with their identity. The v6
index does not preserve venue-specific catalogue participant labels or contexts
for unselected bundles, so serving equivalence must be checked before deployment,
not assumed. Correction membership falls back to selected occurrences only when
exact candidate membership is unavailable, and the summary names those markets.
`claims_rewritten` and `relations_rewritten` count removed plus added row values
in `market_claims` and `claim_relations`, not upsert attempts.

Projection v3 rows retain their original hashes and require an m1 metadata row at
startup; new ingestion writes v4. A rebuilt database has an empty migration table.
`/healthz` lists applied migration IDs, ordered by destination version.

Build the Universe image with `UNIVERSE_CODE_REVISION=$(git rev-parse HEAD)`
(Compose forwards it as a build argument). The image retains that full commit
for migration metadata; a source checkout can resolve it through Git instead.
A missing/invalid image revision aborts before copying.

**m2 (7 → 8) is an explicit aborting stub.** The requested base lacks
`docs/specs/EXECUTION_DELAY_SDK_V1.md` §1.2, including the table name, full column
and timestamp contract. No guessed execution-terms table is installed. `--to 8`
fails nonzero; from v6 it first commits m1 and leaves a valid v7 destination,
while from v7 it leaves the copied v7 file unchanged. Artifact acquisition,
delay/null conversion, missing-artifact summaries, identity/malformed checks,
checkpoint/resume, v8 startup and served execution terms remain unimplemented.
New-run execution-term ingestion is outside this task. Supply the authoritative
§1.2 before implementing m2; m1 can ship alone as the specification permits.

### Short-range equivalence, without remote access

Use a **disposable local** contract-shaped archive and a half-open range that
contains both a corrected Kalshi bundle and a corrected Limitless bundle. Build
two identity-empty files oldest-first with identical `backfill.generated_start`
and `backfill.generated_end` config bounds: one with v6 code, then migrate it to
v7; the other with this code. Use the same local evidence and include any origin
dependencies. Never use the real archive/object store for tests.

The comparison command accepts the bounds explicitly and reads only the two
prepared SQLite files; it does not perform backfill or access any object store:

```bash
python -m universe.tools.rebuild_diff --equivalence \
  --generated-start "$START" --generated-end "$END" \
  MIGRATED.sqlite3 REBUILT.sqlite3 > equivalence.json
```

It compares every table row-for-row, excluding only `universe_migrations`,
`indexed_at_ns`, and the market projection's version/hash. It rejects empty
ranges and reports outside-range origin runs. Exit 0 means equality; exit 1
means differences; exit 2 means invalid inputs. Inspect the listed correction
differences before optionally rerunning with
`--accept-correction-membership-fallback`; this can accept differences only in
`universe_run_projection_corrections`. Every other table must still match.
Rows stream in SQLite sort order; reports retain at most 20 differing samples
per side/table. Keep both files and the comparison JSON as proof artifacts.

The ordinary full-file diff also lists migration rows and discovers execution
tables by their `venue`, `venue_market_id`, `taker_delay_ms`, `observed_at` columns,
reporting observed/unavailable counts per venue. Zero rows means unknown, not
zero delay. This diagnostic support does not implement m2. The real acceptance
set and a v8 equivalence run remain operator gates, not unit-test claims.
Rebuild remains canonical when migration inputs are unavailable or short-range
serving equivalence cannot be shown.

## File layout

| Path | Role |
|---|---|
| `__main__.py`, `commands.py` | `python -m universe {serve,sync,backfill,backup,migrate}` |
| `config.py` | Closed JSON config |
| `schema/` | `schema.sql`, `replay_auth.sql`, `replay_jobs.sql` |
| `store/` | `UniverseStore` facade over `connection.py` (open, pragmas, schema validation), `ingest_tx.py` (the admitted-run write transaction, checkpoints, sync-failure ledger), `reads/` (`health`, `runs`, `events`, `markets`, `claims`, `bundles`), `backup.py`, shared row helpers in `records.py` and response limits in `limits.py` |
| `ingest/` | `sync.py`, `backfill.py`: verified archive retrieval, ingestion, retry ledger; `migrations.py`: offline copy-based migrations |
| `derive/` | `projection.py`, `market_projection.py`, `event_identity.py`: bundle history and market projections, identity allocation |
| `claims/` | `claim_projection.py`, `outcomes.py`: claim reconstruction and the ingestion equivalence check; bundle outcomes read |
| `api/` | `server.py` (application and HTTP handler), `routes.py` (the ordered route table), `framing.py` (validation, cursors, body framing, error mapping, response budget), `rate_limit.py`, `replay_jobs.py` (auth and job endpoints) |
| `tools/` | `rebuild_diff.py`: read-only comparison of two Event Universe SQLite files (standalone, imports nothing from Universe) |
| `jobs/` | `store.py` (durable Replay jobs store) and `auth.py` (SIWE sessions, allowlist); retiring with the jobs UI |

Dependencies point one way: `derive` imports nothing else from Universe;
`claims` imports `derive` (and the store's `DetailTooLarge`, lazily); `store`
imports `derive` and `claims`; `ingest` imports `store`, `derive` and `claims`;
`jobs` imports only `config`; `api` imports `store`, `claims`, `derive`, `jobs`
and `config`. Nothing below `api`/`ingest` imports them.
`tests/test_universe_layout.py` asserts this.

## Tests

```bash
.venv/bin/python -m unittest tests.test_event_universe_store tests.test_universe_outcomes tests.test_universe_layout
```

`tests/generate_event_universe_contract.py` emits real application responses used
to check the UI contract.
