# AGENTS.md

Repository guidance for coding agents working on Prediction Indexer.

## 1. Start here

Read these before making a non-trivial change:

1. [`README.md`](README.md) for repository state, commands, and layout.
2. [`ARCHITECTURE.md`](ARCHITECTURE.md) for the governing capture boundary.
3. The task-specific documents in §4 below, in full.

Then inspect the relevant implementation and tests. A document marked
`proposed` describes intended behavior, not proof that the code already has it.
When reviewing or diagnosing, establish current behavior from source and tests
before reporting a finding.

### Authority and supersession

Use this order when instructions overlap:

1. the current user request;
2. this file;
3. a task's newest normative or approved specification;
4. the subsystem README;
5. the root architecture documents;
6. implementation comments.

Settled decisions that older material may contradict:

- Canonical windows order records by `(visible_ns, lane_rank, delivery_index)`
  ([`ingester/README.md`](ingester/README.md)). `monotonic_ns` is diagnostic
  and boot-scoped, not a cross-lane merge key.
- Compressed artifacts are exact NDJSON inside one Zstandard frame, with no SBE
  or custom binary schema ([`encoder/README.md`](encoder/README.md)).
- Targeter v2 is the only targeter
  ([`targeter/README.md`](targeter/README.md),
  [`targeter/v2/SELECTION.md`](targeter/v2/SELECTION.md),
  [`targeter/v2/DELIVERY.md`](targeter/v2/DELIVERY.md)). Targeter v1 was
  removed.
- Pending, not yet implemented work is specified under
  [`docs/specs/`](docs/specs/). A component README describes what the code does
  today.

If two still-current specifications genuinely conflict, stop and surface the
conflict instead of inventing compatibility behavior.

## 2. System invariants

These are repository-wide constraints, not stylistic preferences.

### Capture is irreversible; interpretation is reversible

- A splice records every application delivery verbatim. It does not filter,
  normalize book messages, apply economic thresholds, or decide whether a
  frame is interesting.
- One socket delivery becomes one envelope record. Do not split a vendor batch
  into interpreted child events at capture time.
- The splice owns live-network concerns: authentication, subscription,
  reconnect, heartbeat, counters, timestamps, and durable append.
- The Rust ingester/finalizer stays network-free and venue-payload agnostic. It
  validates envelopes, sequences evidence, classifies continuity, and
  materializes exact envelope lines; it does not normalize order books.
- Venue-payload normalization, book reconstruction, trust, and economic logic
  belong in replay or analysis.

### The file is the protocol

- Splices and ingesters communicate through durable filesystem artifacts, not
  an internal data socket.
- A seal or receipt is a commit marker. A data filename, rename, upload result,
  or successful decompression is not a commit marker by itself.
- Never mutate a committed sealed segment, canonical window, archive object,
  target generation, or receipt in place.
- Preserve exact logical bytes. Hash the same bytes that are persisted.
- Deterministic ordering, serialization, and identities are required. Sort
  explicitly where input iteration order could vary.

### No silent data loss

- Writer queues may apply backpressure but may never drop an accepted record.
- A full queue is not permission to reconnect a venue.
- Missing, invalid, late, or excluded inputs remain visible through structured
  status and diagnostics; do not make them look like ordinary absence.
- Do not delete raw data during archival. Reaping is separate and requires the
  verified archive receipt, the canonical-ingestion receipt, and an
  independently durable backend.
- Reaper mode is audit by default. This holds for both the raw-capture reaper
  and the Targeter v2 run reaper. Do not enable destructive deletion in code,
  Compose, tests against real data, or operations without explicit user scope.
- The Targeter v2 run reaper deletes local run artifacts only, and only against
  a production receipt, an independently durable backend, a pointer proving the
  run is not the published generation, and the retention floor. It never
  removes a receipt, a run directory, an archive object, or a generation.
  Archival and deletion stay separate commands; the run archiver must never
  learn how to delete.

### Compression is shared and streaming

- Use the reusable `encoder` package/crate. Do not reproduce Zstd handling in
  callers.
- Required profile: Zstandard level 3, frame checksum enabled, no dictionary,
  exactly one frame, exact NDJSON logical payload.
- Track both logical identity (decoded SHA-256, byte length, LF count) and
  stored identity (compressed SHA-256 and byte length).
- Production paths may not buffer a whole segment/window in `bytes` or
  `Vec<u8>`. Decoding is bounded and rejects truncation, concatenation,
  trailing data, checksum mismatch, and identity mismatch.

### Targeting is event-first and conservative

- Targeter v2 is a scheduled one-shot transaction. Cron/systemd/Compose owns
  cadence; do not add an internal long-lived interval loop.
- Match an event through reusable structured vendor evidence. Do not add
  event IDs, team names, tournament names, dates, or one-off fixtures to
  production configuration.
- A series moneyline is the event anchor. Sibling markets expand capture
  surface but never establish an event and never veto healthy siblings merely
  because one sibling is unsupported.
- Require at least two venues; prefer three. Preserve the configured one-hour
  pre-activation window and the USD/USDC 25,000 known combined moneyline-volume
  gate unless a new approved spec changes them.
- Kalshi contract counts are not dollar volume. Only an explicit dollar field
  may contribute to the hard volume gate.
- Relationship findings are happy-path/conditional discovery evidence, not an
  unconditional arbitrage or execution claim.
- Fail closed on unknown semantic shapes. Prefer a visible false negative over
  a guessed cross-venue equivalence.

## 3. Working method

### Before editing

- Run `git status --short` and preserve all user changes. Do not revert or
  reformat unrelated files.
- Read every directly applicable spec section and the current tests before
  choosing an implementation.
- Search with `rg`/`rg --files`. Reuse existing helpers and abstractions before
  introducing another one.
- Treat `.env`, private keys, cloud credentials, and files named like keys as
  secrets. Do not print, inspect, copy, edit, or commit their contents.
- Treat `data/` and generated run directories as user evidence. Do not delete,
  rewrite, compact, or use them as test scratch space.

### Bugs and fixes

- A core logical bug needs a falsifying regression before production code is
  changed. The test must fail for the claimed reason, then pass after the fix.
- Do not fix speculative review findings that cannot be demonstrated with
  current data or a minimal contract-shaped test.
- Keep the regression at the lowest boundary that proves the problem, and add
  an end-to-end case when the risk crosses components.
- Preserve failure order and externally consumed error text during a refactor
  unless the task explicitly changes behavior.
- Review requests are read-only unless the user also asks for fixes. Spec-only
  requests do not authorize production implementation.

### API tests and live probes

- Unit/CI tests are offline. Use small hand-authored vendor contract shapes
  containing only fields the adapter reads.
- Do not freeze complete live API responses, volatile market totals, or today's
  selected events into golden fixtures.
- A live targeter acceptance run uses fresh requests and
  `--no-response-cache`; do not use `--reuse-cache` to claim live discovery.
- Preserve each normalized live run directory. Do not publish, archive, or
  alter live splice targets unless the user explicitly asks for that operation.
- A current API change is evidence to update a vendor-scoped adapter and its
  small contract test, not permission to weaken downstream invariants.

### Persisted formats

- Changing an envelope, seal, receipt, manifest, canonical file, archive key,
  or target-generation schema requires reading its owning spec and updating
  all writers, strict readers, audit paths, fixtures, and crash-boundary tests.
- Closed schemas reject unknown fields. Add an explicit version when evolution
  is required; do not make parsing permissive to avoid a migration decision.
- Keep commit ordering explicit: finish content, fsync file, rename, fsync
  directory, then publish the receipt/manifest/pointer and fsync again as its
  specification requires.

## 4. Documentation routing

Read only the rows relevant to the task, but read those documents completely.

| Work area | Read first | Then read when applicable |
|---|---|---|
| Repository architecture or component boundaries | [`README.md`](README.md), [`ARCHITECTURE.md`](ARCHITECTURE.md) | the component README for each layer touched |
| Envelope fields, clocks, counters, source cursors | [`splices/README.md`](splices/README.md) | [`ingester/FORMATS.md`](ingester/FORMATS.md), clock tests in `tests/test_capture_clock.py` and `tests/test_envelope.py` |
| Splice connection/auth/subscription/reconnect/writer behavior | [`splices/README.md`](splices/README.md) | venue tests under `tests/test_*_splice.py`, `tests/test_spool.py`, `tests/test_writer_queue.py` |
| Sealed segments, k-way merge, canonical sequencing, continuity, finalizer | [`ingester/README.md`](ingester/README.md), [`ingester/FORMATS.md`](ingester/FORMATS.md) | [`encoder/README.md`](encoder/README.md), Rust crate-local tests, `tests/test_sealed_capture_failure_proofs.py` |
| Python/Rust Zstd codec or canonical compressed output | [`encoder/README.md`](encoder/README.md) | [`archive/FORMATS.md`](archive/FORMATS.md), `tests/test_encoder.py`, `tests/test_no_sbe.py` |
| Raw/canonical archiver, object stores, receipts, manifests, reapers | [`archive/README.md`](archive/README.md), [`archive/FORMATS.md`](archive/FORMATS.md) | [`ingester/FORMATS.md`](ingester/FORMATS.md), `tests/test_archive*.py`, `tests/test_canonical_*.py`, `tests/test_gcsstore.py`, `tests/test_reaper.py` |
| Docker Compose, Linux deployment, profiles, storage, operations | [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md), [`docs/RUNBOOK.md`](docs/RUNBOOK.md), `.env.example`, `compose.yaml` | `compose.targeter-v2.yaml`, `compose.universe.yaml`, `tests/test_deployment.py` |
| Targeter motivation, discovery, matching, event selection, esports games | [`targeter/README.md`](targeter/README.md), [`targeter/v2/SELECTION.md`](targeter/v2/SELECTION.md) | `configs/targeter_v2.json`, `tests/test_targeter_v2.py`, `tests/test_targeter_v2_lol.py`, `tests/test_targeter_v2_esports_games.py` |
| Targeter v2 archive, atomic publication, splice handoff, continuity, target records, audit | [`targeter/v2/DELIVERY.md`](targeter/v2/DELIVERY.md) | [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md), `tests/test_targeter_v2_delivery.py` |
| Targeter v2 run retention, the run archiver sweep, or the run reaper | [`targeter/v2/DELIVERY.md`](targeter/v2/DELIVERY.md) | [`archive/README.md`](archive/README.md) for the shared separation rule, `tests/test_targeter_v2_retention.py` |
| Legacy replay, book reconstruction, trust, economic gates | [`replay/legacy/README.md`](replay/legacy/README.md) | [`splices/README.md`](splices/README.md), [`ingester/README.md`](ingester/README.md), [`encoder/README.md`](encoder/README.md); relevant `replay/legacy/gate*.py` and replay tests |
| Replay derivative materialization, normalizers, walker, risk reconstruction | [`engine/README.md`](engine/README.md), [`engine/DERIVATIVES.md`](engine/DERIVATIVES.md) | Rust crate-local tests under `engine/crates/` |
| Replay Redis delivery, supervisor, bundle runner | [`docs/REPLAY_STREAMS_V1.md`](docs/REPLAY_STREAMS_V1.md), [`docs/REPLAY_SUPERVISOR_V1.md`](docs/REPLAY_SUPERVISOR_V1.md) | `replay/tests/test_streams*.py`, `test_supervisor.py`, `test_bundle_runner.py`; Redis tests need a disposable Redis ≥8.2 |
| Local replay bench, fixture runs, run comparison | [`docs/LOCAL_REPLAY_BENCH_V1.md`](docs/LOCAL_REPLAY_BENCH_V1.md) | [`docs/STRATEGY_PREPARATION_V1.md`](docs/STRATEGY_PREPARATION_V1.md), [`docs/REPLAY_SUPERVISOR_V1.md`](docs/REPLAY_SUPERVISOR_V1.md), [`replay/fees/README.md`](replay/fees/README.md), `replay/tests/test_bench.py` |
| Replay jobs production runtime, preflight, backup, restore, audit | [`docs/REPLAY_JOBS_V1.md`](docs/REPLAY_JOBS_V1.md) §8, [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) | `compose.universe.yaml`, `docker/replay-runner.Dockerfile`, `docker/Caddyfile`, `tests/test_replay_operations.py`, `tests/test_deployment.py`; never use retained/live production data |
| Replay strategy layout and entry points | [`replay/strategies/README.md`](replay/strategies/README.md) | Each strategy's README, SPEC and configuration example; `replay/tests/test_strategy_packages.py` |
| Cross-venue complete sets and implication covers | [`replay/strategies/cross_venue_arbitrage/SPEC.md`](replay/strategies/cross_venue_arbitrage/SPEC.md), [`replay/strategies/_shared/implication_cover/SPEC.md`](replay/strategies/_shared/implication_cover/SPEC.md) | Economic SDK and outcome-mask specs; `replay/tests/test_cross_venue_arbitrage.py`, `test_implication_cover.py` |
| Economic strategy SDK, same-venue complement, market profile | [`docs/ECONOMIC_STRATEGY_SDK_V1.md`](docs/ECONOMIC_STRATEGY_SDK_V1.md), [`replay/strategies/same_venue_complement/SPEC.md`](replay/strategies/same_venue_complement/SPEC.md) | `replay/tests/test_economic_sdk*.py`, `test_market_profile.py`, `test_same_venue_complement.py`; V1 byte identity is pinned by `replay/tests/fixtures/complement_v1_golden.json` |
| Prepared game state, SDK game views/releases/timers, bench game inputs | [`docs/specs/GAME_STATE_SDK_V1.md`](docs/specs/GAME_STATE_SDK_V1.md), [`gamestate/README.md`](gamestate/README.md), [`docs/ECONOMIC_STRATEGY_SDK_V1.md`](docs/ECONOMIC_STRATEGY_SDK_V1.md) | [`docs/LOCAL_REPLAY_BENCH_V1.md`](docs/LOCAL_REPLAY_BENCH_V1.md), `replay/tests/test_game_state_sdk.py`, `replay/tests/test_bench.py`, `tests/test_kalshi_game_state.py` |
| Same-venue multi-market complete sets | [`replay/strategies/same_venue_multi_market/SPEC.md`](replay/strategies/same_venue_multi_market/SPEC.md) | [`docs/ECONOMIC_STRATEGY_SDK_V1.md`](docs/ECONOMIC_STRATEGY_SDK_V1.md) §13, [`docs/OUTCOME_MASKS_V1.md`](docs/OUTCOME_MASKS_V1.md), [`replay/strategies/cross_venue_arbitrage/SPEC.md`](replay/strategies/cross_venue_arbitrage/SPEC.md); `replay/tests/test_same_venue_multi_market.py` |
| Replay strategy preparation, strategy SDK, bundle coverage output | [`docs/STRATEGY_PREPARATION_V1.md`](docs/STRATEGY_PREPARATION_V1.md), [`replay/strategies/bundle_coverage/SPEC.md`](replay/strategies/bundle_coverage/SPEC.md) | `replay/tests/test_preparation.py`, `test_bundle_coverage.py` |
| Replay fee SDK | [`replay/fees/README.md`](replay/fees/README.md) | `tests/test_fee_sdk.py` |
| Outcome masks for replay strategies | [`docs/OUTCOME_MASKS_V1.md`](docs/OUTCOME_MASKS_V1.md) | [`analysis/README.md`](analysis/README.md), [`docs/STRATEGY_PREPARATION_V1.md`](docs/STRATEGY_PREPARATION_V1.md) |
| Outcome spaces, masks, claims, event relationships | [`analysis/README.md`](analysis/README.md) | [`universe/README.md`](universe/README.md) for claim storage, `tests/test_outcome_space.py`, `tests/test_masks.py`, `tests/test_claims.py` |
| Universe event store, API, bundle history, outcomes, Replay job control plane | [`universe/README.md`](universe/README.md) | `tests/test_event_universe_store.py`, `tests/test_universe_outcomes.py`, `tests/test_universe_layout.py`, `tests/test_replay_jobs*.py` |
| Game-state pulls, scheduling, event-keyed raw receipts and timelines | [`gamestate/README.md`](gamestate/README.md), [`scripts/KALSHI_GAME_STATE_PULL_V1.md`](scripts/KALSHI_GAME_STATE_PULL_V1.md) | `tests/test_kalshi_game_state.py`, `tests/test_gamestate_scheduled.py` |
| Pending specifications | [`docs/specs/`](docs/specs/) | the component README the spec extends |

## 5. Component boundaries

Keep new code in the layer that owns the decision:

| Layer | Owns | Must not own |
|---|---|---|
| `splices/` | live transport, auth, subscriptions, reconnects, timestamps, envelope write | payload normalization, filtering, economic selection |
| `targeter/` | public catalogue adapters, event matching, selection evidence, target-run archive/publication/retention | socket capture, trade execution, runtime prose inference |
| `ingester/` | raw durability, sealed-segment validation, deterministic evidence order, continuity facts, canonical receipts | venue networking, book interpretation, economics |
| `encoder/` | one strict streaming Zstd contract in Python and Rust | event schemas, archive policy, full-buffer production helpers |
| `archive/` | immutable object storage, archive verification/receipts, manifests, deletion eligibility, durable filesystem primitives | canonical event meaning, direct splice control, any knowledge of `targeter/` |
| `universe/` | the event store, bundle history, outcomes, auth, Replay job control plane | capture, book reconstruction, economics |
| `gamestate/` | public game-state pulls, immutable raw archive, offline timeline derivation | book interpretation, economics, capture |
| `engine/` | venue normalizers, verified derivatives, risk reconstruction, Redis transport | strategies, scheduling, deployment |
| `replay/` | decoding, book reconstruction, trust/recovery, ordered gates, strategies | mutation of raw/canonical evidence |
| `analysis/` | outcome spaces, masks, claims | irreversible capture filtering |

Vendor-specific raw fields stop at the adapter boundary. Downstream targeter
matching/selection consumes canonical records. GCS-specific calls stop inside
`GCSObjectStore`; archiver and reaper depend on the generic object-store
protocol.

The dependency between `targeter/` and `archive/` runs one way. Targeter v2's
run archive, run archiver sweep, and run reaper all live under `targeter/v2/`
and reuse `archive/`'s object-store protocol, store factory, and durable
filesystem primitives. Nothing under `archive/` imports `targeter`, and a test
asserts that; retention *policy* for target runs belongs to `targeter/`, while
the mechanics of storing and removing bytes durably belong to `archive/`.

## Secrets

- Never read, print, summarize, attach, or commit `.env`, `.env.*`, `*.key`,
  credential files, tokens, or private keys.
- Use `.env.example` to determine required variable names.
- Refer to environment variables by name only; never display their values.
- Before committing, verify that no secrets or environment files are staged.

## 6. Verification gates

Use the project virtual environment for Python.

### Python

Focused tests while iterating:

```bash
.venv/bin/python -m unittest tests.test_<relevant_module>
```

Full gate:

```bash
.venv/bin/python -m unittest discover -s tests
```

### Rust ingester/finalizer

Run from the repository root:

```bash
cargo test --manifest-path ingester/Cargo.toml --workspace
cargo clippy --manifest-path ingester/Cargo.toml \
  --workspace --all-targets --all-features -- -D warnings
```

For standalone codec work, also run:

```bash
cargo test --manifest-path encoder/rust/Cargo.toml
```

For normalizer, derivative or risk work:

```bash
cargo test --manifest-path engine/Cargo.toml --workspace
```

### TypeScript

`yarn install && yarn test` runs the UI and Node decoder tests. The UI tests
spawn `.venv/bin/python` and assume a UTC clock (`TZ=UTC`).

### Deployment

At minimum:

```bash
docker compose config --quiet
docker compose -f compose.yaml -f compose.targeter-v2.yaml config --quiet
docker compose -f compose.universe.yaml config --quiet
```

Build only the affected images unless the task or rollout gate requires the
full build. Do not start services, publish targets, contact the object store, or enable reaper
deletion merely to validate configuration.

### Live targeter acceptance

When explicitly requested:

```bash
.venv/bin/python targeter/run_v2.py \
  --mode shadow \
  --no-response-cache \
  --strategy configs/targeter_v2.json \
  --cache-root data/targeter-v2-monitor-state \
  --output-root data/targeter-v2-shadow
```

Report incomplete venue discovery as incomplete. Do not retry it within the
same acceptance cycle in a way that hides the failed input snapshot.

## 7. Completion and handoff

Before calling work complete:

- run the focused regression and proportional broader gates;
- verify generated schemas/receipts with their independent reader or audit
  command, not just their writer;
- state which tests ran and which did not;
- state any live/deployment step that remains unverified;
- link the changed files and avoid claiming a proposed phase is implemented;
- leave user data, credentials, current target pointers, archive objects, and
  service state unchanged unless those mutations were explicitly requested.
