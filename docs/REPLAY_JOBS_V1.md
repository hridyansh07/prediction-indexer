# Replay jobs V1

Status: **proposed**. W0 (§3) is implemented in `replay/jobs/contracts.py`,
`configs/replay_runner.json`, and `replay/tests/test_jobs_contracts.py`. W1–W6
are not implemented.

Builds on [`REPLAY_SUPERVISOR_V1.md`](REPLAY_SUPERVISOR_V1.md),
[`STRATEGY_PREPARATION_V1.md`](STRATEGY_PREPARATION_V1.md),
[`BUNDLE_COVERAGE_V1.md`](BUNDLE_COVERAGE_V1.md),
[`REPLAY_STREAMS_V1.md`](REPLAY_STREAMS_V1.md),
[`engine/README.md`](../engine/README.md) (composite normalizer,
`materialize_range`),
[`ZSTD_MATERIALIZATION_PIPELINE_V1.md`](../encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md)
(canonical archive), and the `ObjectStore` contract in
`archive/storage/base.py`.

## 1. Overview

An allowlisted wallet registers a JSON replay request with the Event Universe
server. A cron-driven runner on the same EC2 host — not the splice host, so
replay I/O never contends with capture — takes the oldest pending job and runs
it end to end: resolve the bundle, reuse or build its derivatives, prepare the
snapshot, run the supervisor, read the result, archive the job. Status is
recorded in a jobs database.

```
Universe EC2
├─ caddy            TLS, limits, static UI, → event-universe
├─ event-universe   existing reads + SIWE/allowlist + job endpoints → jobs.sqlite3
├─ replay-redis     redis:8.2, noeviction, no persistence, compose-network only
└─ replay-runner    host cron each minute; flock-skip if busy
```

Non-goals: parallel or multi-host jobs, automatic retry of `not_ready`,
a local job-directory reaper, request-supplied strategy code, bundle-filtered
derivatives, and any change to the rebuildable Universe query index.

## 2. Workstreams and merge order

```
W0 Contracts ──┬─▶ W1 Auth ────────┐
  (landed)     ├─▶ W2 Job API ─────┤
               ├─▶ W3 Bundle cache ┼─▶ W4 Runner ─▶ W5 Deployment ─▶ live acceptance
               └─▶ W6 UI ◀── W1/W2 API shapes only
```

W1, W2, W3, and W6 proceed in parallel from W0. W2 uses a stub auth hook until
W1 merges. W4 may start in parallel against W3's interface and fakes. W5 comes
last. A change to a §3 contract is a W0 amendment PR; no workstream edits a
contract locally.

| Workstream | Owns (writes) | Must not touch |
|---|---|---|
| W0 | `replay/jobs/contracts.py`, `configs/replay_runner.json`, this document | everything else |
| W1 | `universe/auth.py`, auth tables, `do_POST`/`do_DELETE` plumbing and body cap in `universe/api.py`, the `replay.auth` config section | job tables, runner, `replay/` |
| W2 | `universe/replay_jobs.py`, job routes in `universe/api.py` | auth internals, runner stages |
| W3 | `replay/jobs/bundle.py`, `archive/canonical_restore.py`, `materialize_range --describe` | job DB, Universe, supervisor |
| W4 | `replay/jobs/runner.py`, `replay/jobs/stages.py`, `python -m replay.jobs` | Universe, bundle internals, `archive/` |
| W5 | `compose.universe.yaml` `replay` profile, `docker/replay-runner.Dockerfile`, `docker/Caddyfile`, `docs/DEPLOYMENT.md`, AGENTS routing | application code |
| W6 | `targeter-ui/`, removal of `api/event-universe-proxy.ts` and the `vercel.json` rewrites | Universe, runner |

## 3. W0 — Shared contracts

`replay/jobs/contracts.py` is pure: no filesystem, network, clock, or
randomness. It depends only on the standard library and the existing strict
helpers in `replay.streams.protocol`, `replay.preparation` (`encoded`,
`digest`), and `replay.supervisor` (`_normalizer_descriptor`). The Universe
image already ships all of `replay/`. Every error is `ContractError`
(a `ValueError`) with a message safe to return in a `400`.

### 3.1 Request (`replay_request_version: 1`)

Closed object, at most 64 KiB, strict JSON (duplicate keys, `NaN`, and unknown
fields rejected).

```json
{
  "replay_request_version": 1,
  "bundle_id": "<targeter bundle id>",
  "probe_markets": null,
  "interval": null,
  "strategy": {"name": "bundle_coverage", "config": {}},
  "limits": "small"
}
```

- `bundle_id`: `[A-Za-z0-9_.-]{1,128}`, not `.` or `..` (it is an object-key
  component).
- `probe_markets`: `null` (every listed market) or a sorted unique list of
  1–4096 `venue:native-id` strings. Preparation applies its stricter checks later.
- `interval`: `null` (the whole bundle interval) or
  `{"start_ns", "end_ns"}` as canonical unsigned decimal strings, start < end.
- `strategy.name`: a key of the runner registry; never a module or factory.
  `strategy.config` is checked against the entry's `config_schema` in
  `STRATEGY_CONFIG_SCHEMAS`: request keys are allowed, runner-owned keys and
  unknown keys are rejected. `bundle_coverage_v1` allows no request keys; the
  runner supplies `version`, `snapshot_directory`, and `snapshot_sha256`.
- `limits`: a preset name in the runner config.

API: `parse_request(raw: bytes, config: RunnerConfig) -> Request` (deeply
immutable); `request_sha256(request)` is SHA-256 of preparation's canonical
encoding (sorted keys, compact, UTF-8), so whitespace and key order do not
change it.

### 3.2 Status and stage

```
queued ─▶ running ─▶ archiving ─▶ succeeded
  │          ├──────▶ archiving ─▶ failed | exhausted
  │          ├─▶ not_ready            (terminal)
  │          └─▶ stale_bundle_cache   (terminal)
  └─▶ cancelled
```

`STATUSES`, `TERMINAL`, `RESUMABLE = {running, archiving}`,
`ARCHIVED_OUTCOMES = {succeeded, failed, exhausted}`, `ALLOWED_TRANSITIONS`,
`check_transition(current, new)`, `STAGES = (resolve, bundle, prepare, run,
read, archive)`, and `check_stage`. Any other transition is a bug and raises.
`not_ready` and `stale_bundle_cache` are terminal so that a blocked job never
holds the head of the queue.

### 3.3 Jobs table

`JOBS_SCHEMA_SQL` creates the `STRICT` `jobs` table and its `jobs_queue` index
idempotently, with `CHECK` constraints on status and stage:

```
job_id PK, created_at_ns, submitted_by (lowercase 0x address),
request_json (exact submitted bytes), request_sha256,
status, stage, reason, started_at_ns, updated_at_ns, finished_at_ns,
archive_receipt_key
```

The file is `REPLAY_DATA_ROOT/jobs.sqlite3`, WAL, `busy_timeout = 30000`. It is
**separate from** `event-universe.sqlite3`, which is a rebuildable index whose
rebuild procedure deletes it. W1's auth tables live in the same file.

`job_id(now_ns, suffix_hex)` returns `<yyyymmddTHHMMSSZ>-<16 hex>` from
caller-supplied time and 64 random bits; `check_job_id` validates it. The form
is a valid supervisor `run_id`.

### 3.4 Object keys

| Key | Written by | Commit marker |
|---|---|---|
| `canonical/date=<YYYY-MM-DD>/window=<start_ns>/{evidence,provenance}.ndjson.zst`, `receipt.json` | existing archiver (read-only here) | `receipt.json` |
| `replay/derivatives/<address>/{events,rejects,sources}.ndjson.zst`, `manifest.json`, `receipt.json` | W3 | `receipt.json` |
| `replay/bundles/<bundle_id>/bundle_receipt.json` | W3 | the object |
| `replay/jobs/<job_id>/…`, `job_receipt.json` | W4 | `job_receipt.json` |

Helpers: `date_partition` (the UTC date of a window start, identical to the
finalizer's), `canonical_window_keys`, `derivative_key`, `bundle_receipt_key`,
`job_object_key` (rejects traversal and the reserved `job_receipt.json`),
`job_receipt_key`, and `window_bounds(start, end, window_seconds)`, which
returns the aligned `[first, last)` and every window start.

All writes use `put_immutable`: identical bytes are a no-op, and different bytes
raise `IntegrityConflict`, which is fatal and never repaired by writing. Nothing
in this system deletes objects. An operator deletes
`replay/bundles/<bundle_id>/` by hand to force a rebuild.

### 3.5 Bundle receipt (`replay_bundle_receipt_version: 1`)

```json
{
  "replay_bundle_receipt_version": 1,
  "bundle_id": "…",
  "interval": {"start_ns": "…", "end_ns": "…"},
  "canonical_window_seconds": 1800,
  "normalizer": {"identity_version": 1, "venues": ["…composite identity…"]},
  "windows": [{"window_start_ns": "…", "window_end_ns": "…",
               "canonical_receipt_sha256": "…",
               "derivative_address": "…", "receipt_sha256": "…"}],
  "built_by_job": "<job_id>"
}
```

`parse_bundle_receipt(raw)` is closed and strict. The normalizer must pass the
supervisor's identity validation. The interval must be aligned to the window
period, and windows must be ordered, adjacent, exactly cover the interval, and
use distinct derivative addresses. The input bytes must equal
`bundle_receipt_bytes(receipt)`, the canonical serialization (preparation's
encoding, no trailing LF) that is uploaded and hashed.

The normalizer identity lives only here. The runner compares it with the
current materializer's identity; a mismatch is `stale_bundle_cache`.

### 3.6 Runner configuration

`configs/replay_runner.json`, parsed by `parse_runner_config(raw)`:

```json
{
  "replay_runner_config_version": 1,
  "universe_base_url": "http://event-universe:8080",
  "scope": "jobs",
  "canonical_window_seconds": 1800,
  "authorities": {"kalshi": "kalshi", "limitless": "limitless", "polymarket": "polymarket"},
  "strategies": {
    "bundle_coverage": {
      "factory": "replay.bundle_coverage:build",
      "reader": "replay.coverage_output:read_completed",
      "config_schema": "bundle_coverage_v1"
    }
  },
  "limits": {"small": {"max_entry_bytes": 1048576, "max_queue_bytes": 67108864,
    "command_timeout_ms": 5000, "attempts": 3, "no_progress": 2, "progress_margin": 100,
    "stall_seconds": 30, "attempt_seconds": 300, "run_seconds": 900,
    "poll_seconds": 0.1, "stop_seconds": 2}}
}
```

- `universe_base_url`: http(s), with no credentials, query, or fragment.
- `scope`: the supervisor transport scope used for Redis key names.
- `canonical_window_seconds`: must be a positive divisor of 86400 and equal the
  finalizer's `--window-seconds`.
- `authorities`: the primary lane for each venue's books. The values are the
  splice lane IDs from `replay/lanes.py`; Polymarket's authority is its market
  channel `polymarket`, not the snapshot, sports, or RTDS lanes.
- `limits` presets are checked early against the supervisor's rules. The
  supervisor's own `validate` remains authoritative at run time.

Adding a strategy means changing the image and this registry, never a request.

### 3.7 Tests

`replay/tests/test_jobs_contracts.py` covers request strictness (unknown,
missing, and duplicate fields; bad versions, strings, and intervals;
runner-owned and unknown config keys; size), whitespace-independent request
hashing, every allowed and disallowed transition, table constraints,
`job_id` formatting, date partitions against the finalizer's vectors, canonical
keys against the archiver's `canonical_object_keys`, window alignment, a
byte-exact bundle receipt round trip, receipt tampering, and runner config
strictness.

## 4. W1 — Auth (Universe)

**Delivers** SIWE login, sessions, the allowlist with an admin, and the HTTP
write plumbing.

**Interface for W2:** `authenticate(headers) -> Principal(address, role) | None`;
`require_member(headers)` and `require_admin(headers)`, which raise 401/403.
The write plumbing: `do_POST`/`do_DELETE` dispatch with a 64 KiB body cap and
`Content-Type: application/json` required, returning 413/415 otherwise.

**Config** (`replay.auth` section of `event_universe.json`; all fields required,
closed): `siwe_domain`, `siwe_uri`, `chain_id`, `admin_address` (EIP-55),
`nonce_ttl_seconds` (300), `session_ttl_seconds` (43200).

**Tables** (in `jobs.sqlite3`): `nonces(nonce, expires_at_ns, used_at_ns)`,
`sessions(token_sha256, address, role, expires_at_ns, revoked_at_ns)`,
`allowlist(address, note, added_by, added_at_ns)`, and the append-only
`allowlist_events(seq, actor, action, address, note, at_ns)`.

| Method and path | Auth | Behaviour |
|---|---|---|
| `GET /v1/auth/nonce` | public | `{nonce, expires_at}`; 128 random bits, single use |
| `POST /v1/auth/siwe` | public | `{message, signature}` → `{token, address, role, expires_at}` |
| `POST /v1/auth/logout` | session | revokes the current session |
| `GET /v1/admin/allowlist` | admin | list |
| `POST /v1/admin/allowlist` | admin | `{address, note}`; idempotent |
| `DELETE /v1/admin/allowlist/<address>` | admin | removes the address and revokes its live sessions |

SIWE verification order (the first failure wins):

1. strict EIP-4361 parse;
2. domain, URI, and chain ID equal config;
3. version `1`;
4. nonce known, unexpired, unused, and **consumed atomically**;
5. `issued-at` within the nonce lifetime and `expiration-time`, if present, in
   the future;
6. the recovered EIP-191 signer equals the message address;
7. the address is `admin_address` or on the allowlist.

Tokens are 256 random bits; only their SHA-256 is stored. Addresses are stored
lowercase and returned checksummed. The admin comes from config and cannot be
removed through the API. Dependency: `siwe` or `eth-account`, pinned in the
Universe image.

**Tests** (offline, keys generated in the test): a valid login; wrong domain,
URI, or chain; a replayed or expired nonce; a bad signature; a signer that
differs from the message address; a non-allowlisted address; a member calling
admin routes; revocation on removal; the admin cannot be removed; body cap and
content type; existing GET routes unchanged.

## 5. W2 — Job API and store (Universe)

**Delivers** job submission, listing, and cancellation, plus a store module the
runner reuses.

**Store module** `universe/replay_jobs.py` (standard library plus
`replay.jobs.contracts`): `insert_job(conn, request_bytes, principal, now_ns,
suffix) -> job_id`, `get_job`, `list_jobs(conn, status, limit, after)`,
`cancel_job(conn, job_id, principal)`, `claim_next(conn, now_ns)` (the oldest
resumable job first, then the oldest `queued` marked `running`, in one
`BEGIN IMMEDIATE` transaction), `set_stage`, and
`finish(conn, job_id, status, reason, archive_key)`. Every change passes
`check_transition`.

| Method and path | Auth | Behaviour |
|---|---|---|
| `POST /v1/replay/jobs` | member or admin | `parse_request`; the bundle must exist in the Universe store (in-process, not HTTP); inserts `queued`; returns `201 {job_id, status}` or `400 {"error"}` |
| `GET /v1/replay/jobs?status=&limit=&after=` | public | newest first, `limit` ≤ 100, within the existing response budget |
| `GET /v1/replay/jobs/<job_id>` | public | the row, with the parsed request in place of `request_json` bytes |
| `POST /v1/replay/jobs/<job_id>/cancel` | submitter or admin | only while `queued`; otherwise 409 |

Universe loads `configs/replay_runner.json` to validate strategy and preset
names; W5 mounts it into both containers.

**Tests:** a valid submit and every 400 class; an unknown bundle; 401 when
unauthenticated; ordering and pagination; cancel rules (403 for a non-submitter,
409 while running); `claim_next` precedence and age ordering; illegal
transitions raise; concurrent `claim_next` from two connections claims once.

## 6. W3 — Bundle cache (library)

**Delivers** one call that returns local, verified, pinned derivatives for a
bundle, building and publishing them if absent.

```python
ensure_bundle(bundle_id, interval, *, store, work_root, derivatives_root,
              materializer, window_seconds, job_id)
  -> BundleReady(receipt, pins: [(directory, address, receipt_sha256)])
   | NotReady(reason)
   | StaleCache(cached_identity, current_identity)
```

`interval` is the whole bundle interval; W4 slices the pins to the job
interval. The call raises only on integrity, verification, or tooling failures.

**Cache hit.**

1. Bounded read (1 MiB) of `bundle_receipt_key(bundle_id)`; `parse_bundle_receipt`.
2. Compare its normalizer with `materialize_range --describe`; if they differ,
   return `StaleCache`.
3. For each window, download `replay/derivatives/<address>/*` through
   `open_verified` into `derivatives_root/<address>/`, receipt last, skipping
   directories that are already present and verified.
4. Strict `inspect_pinned` check of each (the publisher's `--validate-only`
   path), then return `BundleReady`.

**Cache miss (build).**

1. **Restore.** For each window from `window_bounds`, `head` its
   `canonical_window_keys` receipt. If any is absent, return
   `NotReady("window <start> not yet archived")` before downloading anything.
   Otherwise use the new generic `archive/canonical_restore.py`: bounded-read
   and strictly parse the archive `receipt.json`, stream evidence and provenance
   through `open_verified` against the receipt's stored identities, decode-verify
   their logical identities, and write them into
   `work_root/canonical/date=…/window=…/` with fsync, receipt last. The trust
   anchor is the archive's receipt-last `receipt.json`; local tombstones are not
   needed. The module must not import `replay` or `targeter`.
2. **Materialize.** Run `materialize_range` with `{canonical_root:
   work_root/canonical, output_root: derivatives_root, start_ns, end_ns}`. Its
   existing receipt scan sees only the restored windows. It returns the identity
   and ordered pins.
3. **Upload** each derivative's files with `put_immutable`, `receipt.json` last.
4. **Publish** `bundle_receipt_bytes(...)` with `put_immutable`.
5. Delete `work_root/canonical/`. It is runner-owned scratch, not evidence.

Every period has a committed canonical receipt: the finalizer's
`tile_absent_windows` commits an empty, incomplete receipt naming every
expected lane for any hole. So a missing key means only "not yet finalized or
archived". Empty and incomplete windows restore and replay normally; finding
where books are usable is the coverage strategy's job.

**Engine change:** `materialize_range --describe` prints the composite
normalizer identity and exits, with no I/O beyond stdout.

**Tests** (disposable in-memory `ObjectStore` with real compressed bytes; the
existing synthetic canonical fixture): a cold build publishes derivatives
before the receipt, and a warm call downloads without materializing; `NotReady`
when any key is missing, with nothing uploaded; `StaleCache` on an identity
mismatch; an empty tiled window builds; a crash after each upload is resumed
idempotently; different bytes at an existing key raise `IntegrityConflict`; a
tampered stored object fails verification; an import test proving
`canonical_restore` never imports `replay` or `targeter`.

## 7. W4 — Runner

**Delivers** `python -m replay.jobs tick`, the container entry point.

```
flock(REPLAY_DATA_ROOT/runner.lock, EX|NB)   busy → exit 0
job ← claim_next()                            none → exit 0
run stages in REPLAY_DATA_ROOT/jobs/<job_id>/, skipping committed ones; set_stage before each
finish(status, reason, archive_key); exit 0
```

The lock file is on the shared bind mount, so separate `docker compose run`
containers exclude each other. A process killed mid-stage leaves the row
resumable; the next tick holds the lock, claims it again, and resumes from the
first uncommitted stage. The supervisor itself refuses to reset its persisted
budgets in the same run directory.

Every stage writes, fsyncs, renames, fsyncs its directory, and writes its
marker last.

| Stage | Marker | Work |
|---|---|---|
| resolve | `resolved.json` | Via Universe HTTP (paged `/v1/bundles/<id>/history`, `/v1/runs/<run_id>`). A bundle that is not `retired` → `not_ready`. Bundle interval = `[first occurrence generated_at_ns, retiring run generated_at_ns)`; occurrence *i* = `[gen_i, gen_{i+1})`, the last ending at retirement; source pins from the run rows. Job interval = request interval or bundle interval, contained in it. |
| bundle | `bundle.json` | `ensure_bundle(...)`: `NotReady` → `not_ready`; `StaleCache` → `stale_bundle_cache` with both identities in `reason`. |
| prepare | `context/receipt.json` | Preparation config: pins sliced to the job interval, `lower_bound: "clip"`, `market_namespace: "targeter_target_id"`, occurrences clipped to the job interval, authorities with lanes from config and scales from the normalizer identity. `prepare(..., universe=UniverseHTTP(base_url, timeout=10), fallback=None)`. |
| run | `run/SUCCESS.json` | Supervisor config: in-image `publisher` and `python`; transport with `run_id = job_id`, config `scope`, the bundle receipt's `normalizer`, pinned inputs, snapshot plans, `groups: [name]`, and the preset's byte caps and timeout; the registry factory, `revision = $REPLAY_IMAGE_REVISION`, config merged with the runner-owned snapshot keys; the preset's limits. `validate(...)`, then `python -m replay.supervisor`, with `REDIS_URL` in the environment only. Exit 20 → `failed` and 21 → `exhausted`, both archived. |
| read | `result.json` | Registry reader (e.g. `read_completed`): summary, semantic hash, snapshot pin, bundle receipt hash, attempts, and qualifiers (`NOT_PROVEN`, `history_complete: false`). A reader failure → `failed`. |
| archive | `job_receipt.json` in the store | Status `archiving`; `put_immutable` of the request, `resolved.json`, `bundle.json`, `context/`, `run/`, and `result.json` (whichever exist), then `job_receipt.json` last, listing keys, identities, final status, and reason. For `ARCHIVED_OUTCOMES` only. The local job directory is kept. |

| Case | Outcome |
|---|---|
| Universe unreachable | `failed` |
| Redis down or OOM | `exhausted` after the supervisor budgets |
| Killed at any point | resumed next tick from the first uncommitted stage |
| Overlapping ticks | the second exits 0 |
| Empty probe union | preparation fails closed → `failed` |

**Tests:** each stage against fakes for `ensure_bundle`, Universe (a small HTTP
stub), and the supervisor (the existing fake-artifact pattern), with real
preparation and readers; a kill after each marker gives identical semantic
outputs on resume; lock contention; exit-code mapping; slicing and clipping at
window and occurrence boundaries. Opt-in acceptance with W3: the synthetic
fixture uploaded to a disposable store → a full tick with disposable Redis →
`read_completed` → `job_receipt.json`, run cold then warm, with byte-identical
semantic files.

## 8. W5 — Deployment

**Delivers** the `replay` profile in `compose.universe.yaml`:

- `caddy`: automatic TLS for `REPLAY_PUBLIC_HOST`, request-body and time limits,
  static `targeter-ui/dist`, and `/v1/*` → `event-universe:8080`.
- `replay-redis`: `redis:8.2` with `--maxmemory <finite>
  --maxmemory-policy noeviction --save "" --appendonly no`; no published port.
- `replay-runner`: `docker/replay-runner.Dockerfile` with the venv including
  `.[replay-redis]`, release builds of `replay-publish` and `materialize_range`,
  `configs/replay_runner.json`, and `REPLAY_IMAGE_REVISION` set to the git SHA
  at build time. No restart policy.
- Volumes: `REPLAY_DATA_ROOT` (read-write for the runner; `event-universe`
  mounts `jobs.sqlite3` and the runner config), plus the existing archive
  backend variables, referenced by name only.
- Host cron: `* * * * * docker compose -f compose.universe.yaml --profile replay
  run --rm replay-runner`.
- The Universe backup job includes `jobs.sqlite3` under its own object prefix.
- `docs/DEPLOYMENT.md` documents the profile, cron, and the manual "delete the
  bundle prefix to rebuild" step. `AGENTS.md` gains a routing row for this
  document.

**Checks:** `docker compose -f compose.universe.yaml --profile replay config
--quiet`; a runner image build; inside the image `replay-publish`,
`materialize_range --describe`, and `python -c "import replay.jobs"` succeed; a
deployment test asserts Redis publishes no port and the runner has no restart
policy. Do not start services against production data in this workstream.

## 9. W6 — UI

**Delivers** the UI served same-origin from Caddy, with sign-in and job views.

- Remove `api/event-universe-proxy.ts`, the `vercel.json` rewrites, and the
  Express proxy. The client calls `/v1/...` on the same origin; the proxy's
  response-schema checks move into the client's existing validators.
- Wallet sign-in: EIP-1193 plus a SIWE message built from `GET /v1/auth/nonce`.
  The token is held in memory only.
- Replay pages: job list; job detail (status, stage, reason, archive receipt
  key; the summary for `bundle_coverage`); a "Replay this bundle" action in the
  History bundle drawer that submits `{bundle_id, interval: null, strategy:
  bundle_coverage}`; admin-only allowlist management.

**Tests:** view models against the W1/W2 response shapes; sign-in state with a
mocked provider; lint, typecheck, and build.

## 10. Resolved decisions

- The canonical trust anchor is the archive's receipt-last `receipt.json`.
- Every period has a committed canonical receipt, empty ones included. A
  missing key means "not yet archived", and the job is `not_ready`.
- Empty and incomplete windows replay normally; usability is the coverage
  strategy's finding.
- The bundle interval starts at the first selection's `generated_at_ns`. Time
  before capture shows up honestly as not initialized.
- Failed and exhausted jobs are archived.
- The normalizer identity lives only on the bundle receipt. A mismatch is
  `stale_bundle_cache`; rebuilding is a manual delete.
- The Vercel proxy is removed. Caddy on the Universe host provides TLS, limits,
  and same-origin UI serving. The 1.75 MB response budget is Universe's own
  constant and can be revisited separately.
