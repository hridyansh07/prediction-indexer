# Replay jobs V1

Status: **in progress**. The shared contracts and durable control plane (§§3–5)
are implemented. Canonical restore and the immutable bundle cache (§6) are the
current stage. Runner, deployment, and UI work (§§7–9) are not implemented.

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
replay I/O never contends with capture — takes the oldest eligible job and runs
it end to end: resolve the bundle, reuse or build its derivatives, prepare the
snapshot, run the supervisor, read the result, archive the job. Every accepted
job ends with an archived job receipt; status is recorded in a jobs database.

```
Universe EC2
├─ caddy            TLS, limits, static UI, → event-universe
├─ event-universe   existing reads + SIWE/allowlist + job endpoints → jobs.sqlite3
├─ replay-redis     redis:8.2, noeviction, no persistence, compose-network only
└─ replay-runner    host cron each minute; flock-skip if busy
```

Non-goals: parallel or multi-host jobs, automatic resubmission of `not_ready`
jobs, cancelling a job after it starts, a local job-directory reaper,
request-supplied strategy code, bundle-filtered derivatives, and any change to
the rebuildable Universe query index.

### 1.1 Deliberate V1 semantics

- **Submission is durably idempotent.** `POST /v1/replay/jobs` requires exactly
  one `Idempotency-Key` matching `[A-Za-z0-9._~-]{1,128}`. The key is scoped to
  the authenticated lowercase submitter, not to `bundle_id`. The first request
  returns `201 {job_id,status,replayed:false}`. The same submitter and key with
  the same canonical request hash returns the original job as
  `200 {job_id,status,replayed:true}` without a new row, event, or quota check;
  a different hash returns `409`. Different keys may create distinct jobs for
  identical requests. Mappings are durable with no TTL and keys cannot be
  recycled. The key is HTTP metadata: it is never part of request v1, exposed,
  or archived.
- **`bundle_id` is one cache coordinate, not the cache identity.** Two jobs may
  reuse a generation only when the complete aligned interval, canonical window
  period, ordered source receipt identities, full Producer, and ordered
  derivative pins match. Jobs still run and archive independently.
- **The archived job receipt and the files it lists are the deliverable.** The
  API reports status, reason, and the receipt key; it does not serve strategy
  results.
- **Local state lives on one persistent volume.** `jobs.sqlite3` and every job
  directory share one persistent EC2 volume. A process or instance restart
  resumes from that volume. Restoring the database without its job
  directories does not support resumption: such jobs fail as
  `local_state_lost` (§3.8). Active job directories are not backed up.
- **Fees are outside `bundle_coverage`.** A future economic strategy must bind
  its fee schedule identities through its own new strategy config schema.

## 2. Workstreams and merge order

```
W0 Contracts ──┬─▶ W1 Auth ────────┐
  (landed)     ├─▶ W2 Job API ─────┤
               ├─▶ Bundle cache ───┼─▶ W4 Runner ─▶ W5 Deployment ─▶ live acceptance
               └─▶ W6 UI ◀── W1/W2 API shapes only
```

The durable W1/W2 control plane has landed. Canonical restore and bundle-cache
work now proceeds against its contracts. W4 may start in parallel against the
bundle interface and fakes. W5 comes last. A change to a §3 contract is a
contract amendment, not an unreviewed local override.

| Workstream | Owns (writes) | Must not touch |
|---|---|---|
| W0 | `replay/jobs/contracts.py`, `configs/replay_runner.json`, this document | everything else |
| W1 | `universe/auth.py`, auth tables, `do_POST`/`do_DELETE` plumbing and body cap in `universe/api.py`, the `replay.auth` config section | job tables, runner, `replay/` |
| W2 | `universe/replay_jobs.py`, job routes in `universe/api.py` | auth internals, runner stages |
| Bundle cache | `replay/jobs/bundle.py`, `archive/canonical_restore.py`, `materialize_range --describe`, strict derivative pin inspection | job DB, Universe, supervisor |
| W4 | `replay/jobs/runner.py`, `replay/jobs/stages.py`, `python -m replay.jobs` | Universe, bundle internals, `archive/` |
| W5 | `compose.universe.yaml` `replay` profile, `docker/replay-runner.Dockerfile`, `docker/Caddyfile`, `docs/DEPLOYMENT.md`, AGENTS routing | application code |
| W6 | `targeter-ui/`, removal of `api/event-universe-proxy.ts` and the `vercel.json` rewrites | Universe, runner |

## 3. W0 — Shared contracts

`replay/jobs/contracts.py` is pure: no filesystem, network, clock, or
randomness; callers pass time and random suffixes in. It reuses the existing
strict JSON decoder (`replay.streams.protocol`), preparation's canonical
encoding (`replay.preparation.encoded`), the supervisor's public
`normalizer_descriptor`, and `archive.storage.base.normalize_key`. The Universe
image already ships `replay/` and `archive/`. Every error is `ContractError`
(a `ValueError`) whose message is safe to return in a `400`; where a failure
maps to a job outcome, `ContractError.code` carries its reason code.

Every persisted document below is closed, versioned JSON with preparation's
canonical serialization (sorted keys, compact separators, UTF-8, no trailing
LF). Each has a strict parser that rejects unknown fields, duplicate keys,
non-canonical bytes, and out-of-bounds sizes, and a writer that re-validates
before serializing. Frozen dataclasses validate on construction, so an invalid
value cannot be built directly either.

### 3.1 Request (`replay_request_version: 1`)

At most 64 KiB.

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
- `interval`: `null` (the whole bundle interval) or `{"start_ns", "end_ns"}` as
  canonical unsigned decimal strings, start < end.
- `strategy.name`: a key of the runner registry; never a module or factory.
  `strategy.config` is checked against the entry's `config_schema` in
  `STRATEGY_CONFIG_SCHEMAS`: request keys are allowed, runner-owned and unknown
  keys are rejected. `bundle_coverage_v1` allows no request keys; the runner
  supplies `version`, `snapshot_directory`, and `snapshot_sha256`.
- `limits`: a preset name in the runner config.

`parse_request(raw, config) -> Request` (deeply immutable);
`request_sha256(request)` hashes the canonical encoding, so whitespace and key
order do not change it.

### 3.2 Statuses and stages

```
queued ──▶ running ──▶ archiving ──▶ succeeded | failed | exhausted
   │                      │  ▲         | not_ready | stale_bundle_cache | cancelled
   │                      ▼  │ (manual resume)
   └──(cancel)──▶ archiving  archive_blocked
```

Every accepted job reaches its terminal status through `archiving`, and every
terminal row has an archived job receipt. While `archiving` or
`archive_blocked`, `pending_outcome` records the terminal status the job will
take once its receipt is durable.

| Status | Meaning |
|---|---|
| `queued` | accepted, not started |
| `running` | in a work stage (`resolve`…`read`) |
| `archiving` | work ended; uploading the job's objects and receipt |
| `archive_blocked` | archival spent its attempts; excluded from automatic claims, resumed only manually |
| terminal | `succeeded`, `failed`, `exhausted`, `not_ready`, `stale_bundle_cache`, `cancelled` |

`STAGES = (resolve, bundle, prepare, run, read, archive)`. Running rows are in a
work stage; `archiving`, `archive_blocked`, and terminal rows are in `archive`.
Only a `queued` job can be cancelled; it goes to `archiving` with pending
`cancelled` and the runner archives it like any other job.

### 3.3 Job row and table

`JobRow` is one row minus `request_json`; its construction enforces the same
rules as the table:

```
job_id PK                  <yyyymmddTHHMMSSZ>-<16 hex>; a valid supervisor run_id
created_at_ns
submitted_by               lowercase 0x address
request_json               exact submitted bytes
request_sha256
status, stage
stage_attempts             starts of the current stage (§3.8)
next_attempt_at_ns         earliest reclaim time; only for running/archiving
started_at_ns              first claim
updated_at_ns
finished_at_ns             terminal only
pending_outcome            archiving/archive_blocked only
reason_code, reason_detail §3.7
blocked_reason_code, blocked_at_ns   archive_blocked only
archive_receipt_key        terminal only; always replay/jobs/<job_id>/job_receipt.json
```

`JOBS_SCHEMA_SQL` creates the `STRICT` table and its queue index idempotently
and enforces the relational rules with `CHECK` constraints: identifier, hash,
and address shapes; `pending_outcome` exactly while archiving or blocked;
blocked fields exactly while blocked; `finished_at_ns` and
`archive_receipt_key` exactly for terminal rows; no schedule on terminal rows;
queued rows without stage or start; `succeeded` without a reason code and every
other terminal status with one. `JobRow` additionally checks that a code
matches its outcome.

The file is `REPLAY_DATA_ROOT/jobs.sqlite3` (WAL, `busy_timeout = 30000`),
**separate from** `event-universe.sqlite3`, which is a rebuildable index whose
rebuild procedure deletes it. W1's auth tables live in the same file.

### 3.4 Object keys

| Key | Written by | Commit marker |
|---|---|---|
| `canonical/date=<YYYY-MM-DD>/window=<start_ns>/{evidence,provenance}.ndjson.zst`, `receipt.json` | existing archiver (read-only here) | `receipt.json` |
| `replay/derivatives/<address>/{events,rejects,sources}.ndjson.zst`, `manifest.json`, `receipt.json` | W3 | `receipt.json` |
| `replay/bundles/<bundle_id>/generations/<generation_sha256>/bundle_receipt.json` | bundle cache | the object |
| `replay/jobs/<job_id>/…`, `job_receipt.json` | W4 | `job_receipt.json` |

Helpers: `date_partition` (identical to the finalizer's), `canonical_window_keys`
(identical to the archiver's `canonical_object_keys`), `derivative_key`,
`bundle_receipt_key`, `job_object_key` (normalized with the object store's own
`normalize_key`, so traversal, empty components, backslash, and NUL are
rejected; `job_receipt.json` is reserved), `job_receipt_key`, and
`window_bounds(start, end, window_seconds)`.

All writes use `put_immutable`: identical bytes are a no-op, and different
bytes raise `IntegrityConflict`, which is never repaired by writing. The
application never deletes objects.

### 3.5 Producer descriptor and bundle receipt

The **producer** is everything besides canonical input that determines
derivative bytes:

```json
{
  "producer_identity_version": 1,
  "normalizer": {"identity_version": 1, "venues": ["…composite identity…"]},
  "normalized_schema_version": 3,
  "materializer_version": 2,
  "materialization_policy_sha256": "<sha256>"
}
```

`materialize_range --describe` prints exactly this canonical object;
`parse_producer` reads it. The normalizer must pass the supervisor's identity
validation. The cache compares the **whole** descriptor, so a change to the
normalizer, schema, materializer, or policy each independently invalidates it.

The bundle receipt (`replay_bundle_receipt_version: 1`):

```json
{
  "replay_bundle_receipt_version": 1,
  "bundle_id": "…",
  "interval": {"start_ns": "…", "end_ns": "…"},
  "canonical_window_seconds": 1800,
  "producer": {"…": "§3.5 descriptor"},
  "windows": [{"window_start_ns": "…", "window_end_ns": "…",
               "canonical_receipt_sha256": "…",
               "derivative_address": "…", "receipt_sha256": "…"}]
}
```

The interval is aligned to the window period; windows are ordered, adjacent,
exactly cover it, and have distinct addresses. The receipt contains nothing
job-specific, so two jobs that build the same bundle with the same producer
publish identical bytes and the second `put_immutable` is a no-op. The job's
`bundle.json` is the byte-exact receipt, parsed with `parse_bundle_receipt`;
there is no separate bundle-stage schema.

`generation_sha256` is the SHA-256 of a domain-separated canonical document
binding `bundle_id`, the complete aligned interval, canonical window seconds,
the ordered canonical receipt SHA-256 values, the full Producer, and ordered
`(derivative_address, receipt_sha256)` pairs. It is not derived from
`bundle_id` alone and there is no mutable current/latest/singleton object. A
changed interval, source receipt, producer field, derivative address, or
derivative receipt creates a distinct immutable generation. Old generations
and derivatives remain readable and are never overwritten or deleted.

### 3.6 Stage and job documents

**`resolved.json`** (`replay_resolved_job_version: 1`): `job_id`,
`request_sha256`, `bundle_id`, `canonical_window_seconds`, `bundle_interval`,
`window_interval` (derived: the aligned bundle interval), `job_interval`, and
`occurrences` partitioning the job interval in order, each `{run_id, start_ns,
end_ns, source: {manifest_key, manifest_sha256, report_key, report_sha256}}` —
the same occurrence shape preparation consumes. At most 128 occurrences, no
repeated run.

**`bundle.json`**: the bundle receipt bytes (§3.5).

**`result.json`** (`replay_job_result_version: 1`): `job_id`, `strategy`,
`strategy_semantic_sha256`, `snapshot_sha256`, `bundle_receipt_sha256`,
`supervisor_identity`, `attempt_id`, `attempts_started`. Strategy outputs and
their qualifiers stay in the archived run directory; this document only binds
their identities.

**`job_receipt.json`** (`replay_job_receipt_version: 1`), uploaded last:

```
job_id, request_sha256, submitted_by, image_revision
final_outcome, reason_code, reason_detail
resolved_sha256            nullable
bundle_receipt_sha256      nullable
snapshot_sha256            nullable
supervisor_identity        nullable
strategy_semantic_sha256   nullable
objects[] {key, sha256, byte_length}   sorted, unique, this job's keys only, ≤ 4096
created_at_ns, finished_at_ns          decimal strings
```

The identities are recorded in stage order: a later one requires every earlier
one, so an early outcome records `null`s and never invents identities.
`succeeded` requires all five and no reason code; every other outcome requires
the reason code that maps to it. The receipt is at most 4 MiB.

### 3.7 Reason codes

Behavior depends only on codes; `reason_detail` is human-readable, printable,
at most 1024 characters, has URL userinfo redacted (`reason_detail()`), and is
never parsed.

| Code | Outcome | Retryable in stage |
|---|---|---|
| `cancelled` | `cancelled` | — |
| `bundle_not_retired`, `canonical_not_archived` | `not_ready` | — |
| `stale_bundle_cache` | `stale_bundle_cache` | — |
| `supervisor_exhausted` | `exhausted` | — |
| `universe_unavailable`, `resource_exhausted` | `failed` once attempts are spent | yes |
| `bundle_history_invalid`, `interval_out_of_range`, `local_state_lost`, `integrity_failure`, `tool_failure`, `supervisor_failed`, `result_invalid`, `stage_attempts_exhausted`, `job_deadline_exceeded`, `internal_failure` | `failed` | — |
| `archive_unavailable` | — (blocks archival; `archive_blocked` once spent) | yes |
| `archive_conflict` | — (blocks archival immediately) | — |

A running row may record its last retryable code while it waits. An
`archive_blocked` row records its cause in `blocked_reason_code`
(`archive_unavailable`, `archive_conflict`, or `stage_attempts_exhausted`) and
keeps the pending outcome's code in `reason_code`.

### 3.8 Scheduling and transitions

Transitions are pure functions over `JobRow`; W2 persists their results and W4
calls them. `select_next(rows, now_ns)` is the reference claim order that W2's
SQL must match:

1. the oldest (`created_at_ns`, `job_id`) row in `running` or `archiving` whose
   `next_attempt_at_ns` is null or has passed;
2. otherwise the oldest `queued` row.

So a job waiting on backoff, and every `archive_blocked` job, never prevents
queued work from starting.

- `claim(row, orchestration, now)` counts the attempt **before** any work runs,
  so a process that dies mid-stage still spends budget, and sets
  `next_attempt_at_ns = now + retry_backoff_seconds`. A queued job becomes
  `running` in `resolve`. For a running job, reaching `max_job_seconds` since
  `started_at_ns` ends it as `job_deadline_exceeded`, and a stage already
  started `max_stage_attempts` times ends it as `stage_attempts_exhausted`,
  both through `archiving`. For an archiving job, spent attempts make it
  `archive_blocked`.
- `advance(row, stage, now)` commits the current stage and starts the next;
  `stage_attempts` resets to 1 (the running attempt) and a retry reason clears.
- `retry_later(row, code, detail, orchestration, now)` accepts only retryable
  codes and schedules backoff.
- `succeed` (after `read`), `fail(code)`, and `cancel` (queued only) enter
  `archiving` with the mapped pending outcome.
- `lose_local_state(row, detail, now)` applies when a running or archiving job's
  directory is missing or its committed markers are invalid: the pending
  outcome becomes `failed` with `local_state_lost`. The runner then archives
  whatever objects exist. It **never recreates** a missing supervisor directory
  under the same job ID, because that would reset the supervisor's persisted
  retry budgets.
- `block_archive`, `resume_blocked` (manual operator action), and `finish`
  (after the job receipt is durable).

### 3.9 Bundle history resolution

`resolve_occurrences(history, retired_at_ns, job_interval)` orders Universe's
occurrences by `(generated_at_ns, run_id)`. Occurrence *i* covers
`[generated_at_i, generated_at_{i+1})` and the last ends at the retiring run's
`generated_at_ns`; the bundle interval spans them. Equal timestamps would
create a zero-length occurrence, so they fail closed with
`bundle_history_invalid`, as do an empty history and a retirement that does not
follow the last occurrence. A job interval outside the bundle interval fails
with `interval_out_of_range`. Occurrences are clipped to the job interval.

### 3.10 Runner configuration

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
    "poll_seconds": 0.1, "stop_seconds": 2}},
  "orchestration": {"max_stage_attempts": 20, "max_job_seconds": 86400,
                    "retry_backoff_seconds": 60}
}
```

- `universe_base_url`: http(s), with no credentials, query, or fragment.
- `scope`: the supervisor transport scope used for Redis key names.
- `canonical_window_seconds`: a positive divisor of 86400 that must equal the
  finalizer's `--window-seconds`.
- `authorities`: each venue's primary book lane, from `replay/lanes.py`.
  Polymarket's authority is its market channel `polymarket`.
- `limits` presets are checked early against the supervisor's rules; the
  supervisor's own `validate` stays authoritative. Every preset's `run_seconds`
  must be below `max_job_seconds`.
- `orchestration` is server-owned; no request can change it. The numbers may be
  tuned, but the semantics in §3.8 are fixed.

Adding a strategy means changing the image and this registry, never a request.

### 3.11 Tests

`replay/tests/test_jobs_contracts.py` covers request strictness and hashing;
every path through `archiving` and every illegal bypass; archive receipts on
every terminal row; `pending_outcome` and the SQL constraints (valid rows insert,
each invalid update is rejected); claim counting, backoff, attempt reset,
exhaustion, deadlines, manual resume, and queued jobs not starved by waiting or
blocked jobs; `local_state_lost` for running and archiving jobs without
restarting work; producer fields changing identity independently; invalid
direct construction and bypassed fields failing serialization; unnormalized,
backslash, and NUL keys; canonical round trips and single-field tampering for
the bundle receipt, resolved job, result, and job receipt; partial receipts for
cancelled, not-ready, stale, failed, and exhausted jobs; deterministic
equal-timestamp failures; and the shipped runner config with its orchestration
section.

## 4. W1 — Auth (Universe)

**Delivers** SIWE login, sessions, the allowlist with an admin, and the HTTP
write plumbing.

**Interface for W2:** `authenticate(headers) -> Principal(address, role) | None`;
`require_member(headers)` and `require_admin(headers)`, which raise 401/403.
The write plumbing: `do_POST`/`do_DELETE` dispatch with a 64 KiB body cap and
`Content-Type: application/json` required, returning 413/415 otherwise.

**Config** (`replay` section of `event_universe.json` version 2; all fields
required, closed): `replay.database_path` (the durable `jobs.sqlite3`) and
`replay.auth` with `siwe_domain`, `siwe_uri`, `siwe_statement`, `chain_id`,
`admin_address` (EIP-55), `nonce_ttl_seconds` (300), and `session_ttl_seconds`
(43200). `siwe_statement` is the exact statement line every sign-in message
carries, e.g. `Sign in to Prediction Indexer.`; the UI uses the same text.
While the shipped zero-address admin placeholder remains, the sign-in routes
return 503 and every other route is unaffected.

**Nonces are held in memory, not in SQLite.** A process-local store maps each
nonce to the server time it expires (`nonce_ttl_seconds` after the server
issued it). It is locked, so consuming a nonce is atomic across the server's
threads, and capped at 500 outstanding nonces: expired ones are pruned on
each issue, and a full store returns 503. Caddy rate limiting on the nonce
route (W5) is the first defence. A restart drops outstanding nonces and users
simply sign in again. This requires exactly one `event-universe` process; a
second worker or replica would need shared nonce storage.

**Tables** (in `jobs.sqlite3`): `sessions(token_hash, address, role,
created_at, expires_at, revoked_at)`, `allowlist(address, note, created_at,
created_by)`, and the append-only `allowlist_events(event_id, action, address,
note, actor_address, created_at)`, whose immutability triggers enforce.

| Method and path | Auth | Behaviour |
|---|---|---|
| `GET /v1/auth/nonce` | public | `{nonce, expires_at}`; 128 random bits, single use |
| `POST /v1/auth/siwe` | public | `{message, signature}` → `{token, address, role, expires_at}` |
| `POST /v1/auth/logout` | session | revokes the current session |
| `GET /v1/admin/allowlist` | admin | list |
| `POST /v1/admin/allowlist` | admin | `{address, note}`; idempotent |
| `DELETE /v1/admin/allowlist/<address>` | admin | removes the address and revokes its live sessions |

SIWE verification (every failure is the same 401):

1. strict EIP-4361 parse of the with-statement layout: the configured domain
   header, a checksummed address, the configured statement, then the fields in
   order;
2. URI, version `1`, and chain ID equal config; `Issued At` is a well-formed
   UTC timestamp;
3. `Not Before`, if present, has passed and `Expiration Time`, if present, has
   not;
4. the recovered EIP-191 signer equals the message address;
5. the nonce is live and is **consumed atomically**. Only a correctly signed
   message spends its nonce;
6. the address is `admin_address` or on the allowlist.

**Freshness is the server's.** The nonce's server-side TTL and single use are
the replay protection. The client-written `Issued At` is never compared with
the server clock, so browser clock drift cannot break sign-in. `Expiration
Time` can only shorten the session, never extend it past
`session_ttl_seconds`.

Tokens are 256 random bits; only their SHA-256 is stored. Addresses are stored
lowercase and returned checksummed. The admin comes from config and cannot be
removed through the API; changing it invalidates the old admin's sessions
because the role is re-checked on every request. Verification is offline
`ecrecover`, so the admin must be an externally owned account, not an EIP-1271
contract wallet. Dependency: `eth-account`, pinned in `pyproject.toml` and the
Universe image.

**Tests** (offline, keys generated in the test): a valid login; wrong domain,
URI, chain, statement, or a missing statement; malformed `Issued At`; client
clocks hours ahead or behind still signing in; a nonce expired by the server
TTL, unknown, replayed, or raced by two logins; a bad signature not spending
the nonce; a restart dropping outstanding nonces; the capped store returning
503 and pruning expired nonces; the placeholder admin disabling sign-in; a
non-allowlisted address; a member calling admin routes; revocation on removal;
the admin cannot be removed; body cap and content type; existing GET routes
unchanged.

## 5. W2 — Job API and store (Universe)

**Delivers** job submission, listing, and cancellation, plus a store module the
runner reuses.

**Store module** `universe/replay_jobs.py` (standard library plus
`replay.jobs.contracts` and W1's `Principal`; it does not import auth internals).
It persists `JobRow`s and never writes a row that `JobRow` would reject; every
state change is one of §3.8's transition functions. Its immutable results are
`SubmissionResult(row, created: bool)` and
`ClaimResult(row, request_bytes: bytes, mode: Literal['initialize','resume'])`.
Methods are `initialize`, `lookup_submission`, `submit`, `get_job`, `list_jobs`,
`list_events`, `cancel_job`, `claim_next`, and `save(before, after)`. `save` is a
compare-and-set on `(job_id,status,stage,stage_attempts,updated_at_ns)` with an
exact-before row check, so stale or corrupt writers cannot overwrite a newer
row. Failed writes append no event.

The component first initializes W0's `JOBS_SCHEMA_SQL`, then its owned schema.
Its metadata binds a SHA-256 over the deterministically ordered, whitespace-
normalized `sqlite_master` definitions for only those owned objects; unrelated
auth objects may coexist and formatting-only changes to the SQL source do not
change the schema identity.
`job_submissions` stores `(submitted_by,idempotency_key,request_sha256,job_id,
created_at_ns)`, with submitter/key as its primary key and a unique job foreign
key. `job_events` has one global monotonic sequence, closed transition fields,
and permits only `submitted`, `cancelled`, `claimed_initialize`,
`claimed_resume`, `stage_advanced`, `retry_scheduled`, `outcome_pending`,
`outcome_replaced`, `archive_blocked`, `archive_resumed`, and `finished`.
Triggers reject event updates and deletes. Every job mutation and its event are
one `BEGIN IMMEDIATE` transaction. A nonempty W0 jobs table without matching
component metadata and event history is an unsupported migration and fails
initialization rather than fabricating history.

Admission is atomic for a new key. Active means every nonterminal status,
including `archive_blocked`. Limits are `max_active_jobs_total`,
`max_active_jobs_per_submitter`, and `max_queued_jobs_total`; administrators are
not exempt. Per-submitter exhaustion is `429`, and either global exhaustion is
`503`. An idempotent replay bypasses current quota. Existing mapping lookup
precedes current bundle visibility and quota checks; a new mapping requires at
least one historical bundle occurrence from the current Universe projection,
without requiring active or retired state.

`claim_next` uses one `BEGIN IMMEDIATE` and exactly §3.8's priority and
`next_attempt_at_ns <= now_ns` boundary. `ClaimResult.mode` is `initialize` only
for a pre-claim queued row or pristine queued-cancellation archival state
(`archiving`, pending `cancelled`, no started time, zero attempts); every other
running or archiving claim is `resume`. A post-claim crash therefore resumes;
missing local state must later fail closed. Claim transitions that exhaust a
deadline or attempts are persisted and returned in their resulting state.

| Method and path | Auth | Behaviour |
|---|---|---|
| `POST /v1/replay/jobs` | member or admin | exactly one valid `Idempotency-Key`; parse the exact received bytes; the bundle must exist in the Universe store for a new key; returns `201/200 {job_id,status,replayed}`; conflicting key/hash is 409, per-user quota is 429, and global quota is 503 |
| `GET /v1/replay/jobs?status=&limit=&after=` | public | newest first, `limit` ≤ 100, within the existing response budget |
| `GET /v1/replay/jobs/<job_id>` | public | the row (status, stage, pending outcome, reason code and detail, attempts, times, archive receipt key) with the stored request, decoded, in place of `request_json` bytes; it is not re-validated against the current runner registry, so renaming a preset or strategy never hides earlier jobs |
| `POST /v1/replay/jobs/<job_id>/cancel` | submitter or admin | only while `queued`, applying `cancel`; otherwise 409 |
| `GET /v1/replay/jobs/<job_id>/events?limit=&cursor=` | public | global event IDs in ascending order, with a strict opaque job-bound `replay_job_events` cursor |

The job list cursor is strict opaque base64url tagged `replay_jobs` and binds
the newest-first `(created_at_ns,job_id)` position. Both list limits default to
and are capped at 100. API u64 nanosecond values are decimal strings;
`submitted_by` is checksummed for display while storage remains lowercase. Job
detail never returns raw request bytes or an idempotency key. Safe request
validation failures return their actionable contract message in `error`
(including malformed JSON location, duplicate keys, missing/unexpected fields,
and request-v1 field errors) rather than the undiagnostic `invalid request`.

Universe loads `configs/replay_runner.json` to validate strategy and preset
names; W5 mounts it into both containers. `resume_blocked` is an operator
command, not an HTTP route in V1.

**Tests:** a valid submit and every 400 class; an unknown bundle; 401 when
unauthenticated; the same durable `Idempotency-Key` and request replay the
original job while a different key may create another job; ordering and
pagination; cancel rules (403 for a non-submitter, 409 once started);
`claim_next` agreeing with `select_next` on randomized row sets; stale
compare-and-set rejected; concurrent `claim_next` from two connections claims
once.

## 6. Canonical restore and immutable bundle cache (library)

**Delivers** one call that returns local, verified, pinned derivatives for a
bundle, building and publishing them if absent.

```python
ensure_bundle(bundle_id, window_interval, *, store, work_root, derivatives_root,
              materializer, window_seconds)
  -> BundleReady(receipt, receipt_bytes, pins: [(directory, address, receipt_sha256)])
   | NotReady(code, detail)                 # canonical_not_archived
   | StaleCache(cached_producer: Producer, current_producer: Producer)
```

`window_interval` is `ResolvedJob.window_interval`; W4 slices the pins to the
job interval. Retryable object-store failures raise with `archive_unavailable`;
integrity and verification failures raise with `integrity_failure`; tool
failures with `tool_failure`.

### 6.1 Trust anchors and strict readers

The remote canonical `receipt.json` is the commit marker and trust anchor. Its
presence is necessary but not sufficient: the reader bootstraps an
`ObjectExpectation` from a bounded `head`, requires provider checksum metadata,
streams the receipt through `open_verified`, and only then strictly parses it.
The parser rejects duplicate/unknown fields, non-canonical serialization,
invalid compression declarations, wrong window/path bindings, impossible
sequence/line counts, and every numeric/hash/schema violation. The evidence and
provenance expectations are then formed from that trusted receipt plus fresh
provider metadata; key existence alone never establishes trust.

The restored local `receipt.json` is **not** an archive authority or a second
trust anchor. It is an exact verified copy written after both verified frames,
with file and directory fsyncs, solely so existing canonical readers see the
same receipt-last committed layout. Restore streams through `open_verified`,
strictly decodes exactly one checksummed Zstandard frame to EOF, verifies stored
and logical identities and line bounds, and persists the original compressed
bytes. Empty and incomplete committed windows are valid; an absent remote
receipt is not an empty window.

Derivative validation uses a dedicated strict Rust pin-inspection interface
backed by `replay-materialize`'s existing independent verifier. It accepts only
an addressed directory and exact `(address, receipt_sha256)` pin and verifies
the receipt, manifest, source binding, domain-separated address, every compressed
data identity, strict line schemas, single-frame EOF, and clean end of input.
`replay-publish --validate-only` validates publisher configuration and is not
the bundle cache's pin-inspection API.

### 6.2 Cache hit

1. Obtain the current Producer from exact bounded
   `materialize_range --describe` output. This mode performs no canonical,
   derivative, or configuration-file I/O.
2. Preflight every required remote canonical receipt before any frame download
   or upload. If any receipt is absent, return
   `NotReady("canonical_not_archived", detail)` with no publication.
3. Compute the source-and-interval generation coordinates. Inspect matching
   immutable generations under the bundle prefix. A generation with the same
   interval/source coordinates and another Producer returns `StaleCache`; a
   byte/schema/hash conflict is `integrity_failure`.
4. Bounded-read the exact generation receipt and parse it strictly.
5. For each window, download `replay/derivatives/<address>/*` through
   `open_verified` into `derivatives_root/<address>/`, receipt last, skipping
   directories already present and verified.
6. Run the strict Rust pin inspector on every ordered pin, then return
   `BundleReady`. A warm hit never invokes materialization.

### 6.3 Cache miss (build)

1. **Restore.** Use provider-neutral `archive/canonical_restore.py` after the
   all-receipts preflight. It implements §6.1 and writes only beneath an owned
   per-generation scratch directory. It must not import `replay` or `targeter`.
2. **Materialize.** Run `materialize_range` with `{canonical_root:
   work_root/canonical, output_root: derivatives_root, start_ns, end_ns}`. Its
   existing receipt scan sees only the restored windows. It returns the
   normalizer identity and ordered pins; the producer comes from `--describe`.
3. **Inspect and upload** each derivative only after strict Rust pin inspection;
   upload the fixed data/manifest allowlist with `put_immutable`, then
   `receipt.json` last.
4. **Publish** `bundle_receipt_bytes(...)` at its content-addressed generation
   key with `put_immutable`. An existing
   identical receipt (another job built it) is a no-op; a different one is an
   `IntegrityConflict` → `integrity_failure`.
5. Remove only the owned generation scratch. Never delete a derivative or any
   other generation.

Every period has a committed canonical receipt: the finalizer's
`tile_absent_windows` commits an empty, incomplete receipt naming every
expected lane for any hole. So a missing key means only "not yet finalized or
archived". Empty and incomplete windows restore and replay normally; finding
where books are usable is the coverage strategy's job.

**Engine change:** `materialize_range --describe` prints the exact canonical
§3.5 Producer and exits, with no materialization or data/configuration I/O. A
separate strict `--inspect-pin` mode is the cache's derivative verification
interface.

### 6.4 Frozen limits, concurrency, and crash recovery

Limits are fixed for V1: at most 4096 windows; 1 MiB per receipt/manifest and
4 MiB total subprocess stdout; 1 MiB captured stderr; 16 MiB per NDJSON line;
8 GiB stored and 32 GiB decoded per canonical frame; 1 TiB aggregate canonical
scratch, 1 TiB aggregate derivative publication input, and 16 GiB verifier
temporary space per call; exactly the five derivative files in §3.4 and no
other object; and a 3600-second materializer/inspector timeout. Reads and copies
are streaming. Subprocesses use a fixed argv, minimal allowlisted environment,
no shell, bounded output, and process-group timeout termination.

All roots and children are checked for containment. Symlinks, traversal,
non-regular files, unexpected entries, over-budget output, and address/directory
mismatch fail closed. Publication order is data, manifest, derivative receipt,
then bundle receipt. Pins are ordered by window and independently verified.

Concurrent callers may build the same generation independently. Immutable
addressing and receipt-last publication make the result deterministic; a local
per-generation `flock` is only an optimization and never the correctness
mechanism. Before the bundle receipt exists, partial local scratch and remote
objects are untrusted: owned scratch is discarded and rebuilt, while immutable
uploads resume only by exact verification/no-op. Once the bundle receipt is
published, the generation is committed. A conflict or tamper at any boundary
fails closed and is never repaired in place.

Error mapping is closed: retryable store availability faults are
`archive_unavailable`; receipt/object/hash/schema/immutability violations are
`integrity_failure`; materializer or inspector launch, timeout, output, exit, or
protocol failures are `tool_failure`.

**Tests** use disposable stores, generated compressed canonical fixtures, and
temporary directories only. They cover strict receipt bootstrap and restore;
missing/oversized/malformed/non-canonical/wrong-store/tampered/identity-mismatched
inputs; empty and incomplete windows; exact `--describe`; every Producer field;
strict pin rejection for receipt/manifest/data/address/source/Zstandard/hash
tampering; cold ordering; warm no-materializer equality; interval/source-aware
generation identity; deterministic concurrent builders; crash injection at each
local/upload/receipt boundary and deterministic retry; immutable conflicts and
partial state; every window/file/object/disk/subprocess/output/path limit; and an
import test proving `canonical_restore` never imports `replay` or `targeter`.

## 7. W4 — Runner

**Delivers** `python -m replay.jobs tick`, the container entry point.

```
flock(REPLAY_DATA_ROOT/runner.lock, EX|NB)      busy → exit 0
row ← claim_next(orchestration, now)            none → exit 0
if row is running/archiving and its directory is missing or its markers are
   invalid: row ← lose_local_state(row)         (never recreate the directory)
run the row's stage and every following one in jobs/<job_id>/,
   skipping stages whose markers are committed, persisting each transition
exit 0 after the job finishes, fails into archiving, or schedules a retry
```

The lock file is on the shared bind mount, so separate `docker compose run`
containers exclude each other. The runner handles at most one claimed job per
tick. Every stage writes, fsyncs, renames, fsyncs its directory, and writes its
marker last.

| Stage | Marker | Work |
|---|---|---|
| resolve | `resolved.json` | Universe HTTP: paged `/v1/bundles/<id>/history` and `/v1/runs/<run_id>`. Not `retired` → `fail("bundle_not_retired")`. `resolve_occurrences(...)` with the request interval; its `ContractError.code` becomes the failure code. Write `resolved_job_bytes`. |
| bundle | `bundle.json` | `ensure_bundle(...)`: `NotReady` → `fail(code)`; `StaleCache` → `fail("stale_bundle_cache")` with both producers in the detail. Write the receipt bytes. |
| prepare | `context/receipt.json` | Preparation config: pins sliced to the job interval, `lower_bound: "clip"`, `market_namespace: "targeter_target_id"`, resolved occurrences, authorities with lanes from config and scales from the producer's normalizer. `prepare(..., universe=UniverseHTTP(base_url, timeout=10), fallback=None)`. |
| run | `run/SUCCESS.json` | Supervisor config: in-image `publisher` and `python`; transport with `run_id = job_id`, config `scope`, the producer's `normalizer`, pinned inputs, snapshot plans, `groups: [name]`, and the preset's byte caps and timeout; the registry factory, `revision = $REPLAY_IMAGE_REVISION`, config merged with the runner-owned snapshot keys; the preset's limits. `validate(...)`, then `python -m replay.supervisor`, with `REDIS_URL` in the environment only. Exit 20 → `supervisor_failed`, 21 → `supervisor_exhausted`. |
| read | `result.json` | The registry reader (e.g. `read_completed`), then `job_result_bytes`. A reader failure → `result_invalid`. Then `succeed`. |
| archive | `job_receipt.json` in the store | `put_immutable` of `request.json`, `resolved.json`, `bundle.json`, `context/`, `run/`, and `result.json` — whichever exist — then `job_receipt_bytes` last, listing exactly the uploaded objects. Then `finish`. Runs for every outcome, including `cancelled` (request only) and early failures. |

Failure handling uses only codes: retryable codes go to `retry_later`, others to
`fail`; unexpected exceptions are `internal_failure`. During archiving,
`archive_unavailable` goes to `retry_later` until `claim` blocks the job, and
`archive_conflict` blocks it at once. The local job directory is kept.

**Tests:** each stage against fakes for `ensure_bundle`, Universe (a small HTTP
stub), and the supervisor (the existing fake-artifact pattern), with real
preparation and readers; a kill after each marker gives identical semantic
outputs on resume; a missing or corrupted job directory → `local_state_lost`
without a new supervisor directory; lock contention; exit-code mapping; the
receipt listing exactly the uploaded objects for each outcome; slicing and
clipping at window and occurrence boundaries. Opt-in acceptance with W3: the
synthetic fixture uploaded to a disposable store → a full tick with disposable
Redis → `read_completed` → `job_receipt.json`, run cold then warm, with
byte-identical semantic files.

## 8. W5 — Deployment

**Delivers** the `replay` profile in `compose.universe.yaml`:

- `caddy`: automatic TLS for `REPLAY_PUBLIC_HOST`, request-body and time limits,
  a rate limit on `GET /v1/auth/nonce`, static `targeter-ui/dist`, and `/v1/*` →
  `event-universe:8080`. `event-universe` stays a single process (§4).
- `replay-redis`: `redis:8.2` with `--maxmemory <finite>
  --maxmemory-policy noeviction --save "" --appendonly no`; no published port.
- `replay-runner`: `docker/replay-runner.Dockerfile` with the venv including
  `.[replay-redis]`, release builds of `replay-publish` and `materialize_range`,
  `configs/replay_runner.json`, and `REPLAY_IMAGE_REVISION` set to the git SHA
  at build time. No restart policy.
- Volumes: `REPLAY_DATA_ROOT` on one persistent volume holding `jobs.sqlite3`
  and every job directory (§1.1) — read-write for the runner; `event-universe`
  mounts `jobs.sqlite3` and the runner config. The existing archive backend
  variables, referenced by name only.
- Host cron: `* * * * * docker compose -f compose.universe.yaml --profile replay
  run --rm replay-runner`.
- The Universe backup job includes `jobs.sqlite3` under its own object prefix;
  §1.1 describes what a restore of it alone can and cannot do.
- `docs/DEPLOYMENT.md` documents the profile, cron, `resume_blocked`, and the
  manual bundle-receipt rebuild procedure (§3.5). `AGENTS.md` gains a routing
  row for this document.

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
- Replay pages: a job list and a job detail showing status, stage, pending
  outcome, reason code and detail, attempts, and the archive receipt key. There
  is no result summary in V1: the archived receipt is the deliverable. A
  "Replay this bundle" action in the History bundle drawer submits
  `{bundle_id, interval: null, strategy: bundle_coverage}`. Admin-only allowlist
  management.

**Tests:** view models against the W1/W2 response shapes; sign-in state with a
mocked provider; lint, typecheck, and build.

## 10. Resolved decisions

- The canonical trust anchor is the archive's receipt-last `receipt.json`.
- Every period has a committed canonical receipt, empty ones included. A missing
  key means "not yet archived" (`canonical_not_archived`).
- Empty and incomplete windows replay normally; usability is the coverage
  strategy's finding.
- The bundle interval starts at the first selection's `generated_at_ns`. Time
  before capture shows up honestly as not initialized.
- Every terminal job, including cancelled, not-ready, and stale ones, is
  archived with a receipt.
- The Producer is compared whole. A matching source/interval generation with a
  different Producer is `stale_bundle_cache`; immutable generations coexist and
  there is no delete/rebuild procedure or mutable bundle singleton.
- Only queued jobs can be cancelled in V1.
- SIWE nonces live in the single Universe process's memory with a server-side
  TTL; the client's `Issued At` is not a freshness check. Every sign-in message
  carries one configured statement.
- The Vercel proxy is removed. Caddy on the Universe host provides TLS, limits,
  and same-origin UI serving. The 1.75 MB response budget is Universe's own
  constant and can be revisited separately.
