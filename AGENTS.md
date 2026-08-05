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

Important known supersessions:

- [`docs/SEALED_CAPTURE_PIPELINE_V1.md`](docs/SEALED_CAPTURE_PIPELINE_V1.md)
  supersedes the normal merge-key recommendation in
  [`Architecture_refinments.md`](Architecture_refinments.md): V1 orders by
  `(visible_ns, lane_rank, delivery_index)`. `monotonic_ns` is diagnostic and
  boot-scoped, not the V1 cross-lane merge key.
- [`encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md`](encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md)
  is the approved compression addendum to the sealed-capture design. V1 uses
  exact NDJSON inside one Zstandard frame and no SBE or custom binary schema.
- [`docs/TARGETER_V2_PHASES_1_5.md`](docs/TARGETER_V2_PHASES_1_5.md) and
  [`docs/TARGETER_V2_PHASES_6_10.md`](docs/TARGETER_V2_PHASES_6_10.md) control
  Targeter v2. [`targeter/TARGETER.md`](targeter/TARGETER.md) describes legacy
  Targeter v1 only.
- [`docs/TARGETER_V2_LEAGUE_OF_LEGENDS_V1.md`](docs/TARGETER_V2_LEAGUE_OF_LEGENDS_V1.md)
  is the structured LoL contract. The newer
  [`docs/TARGETER_V2_MULTI_GAME_ESPORTS_V1.md`](docs/TARGETER_V2_MULTI_GAME_ESPORTS_V1.md)
  extends that design to CS2, Dota 2, Honor of Kings, and Valorant; it is still
  a proposed implementation contract until its code and tests land.

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
| Repository architecture or component boundaries | [`README.md`](README.md), [`ARCHITECTURE.md`](ARCHITECTURE.md) | [`docs/CAPTURE_SPEC.md`](docs/CAPTURE_SPEC.md), [`Architecture_refinments.md`](Architecture_refinments.md) for design history; newer normative specs win |
| Envelope fields, clocks, counters, source cursors | [`splices/common/ENVELOPE.md`](splices/common/ENVELOPE.md) | [`docs/SEALED_CAPTURE_PIPELINE_V1.md`](docs/SEALED_CAPTURE_PIPELINE_V1.md), clock tests in `tests/test_capture_clock.py` and `tests/test_envelope.py` |
| Splice connection/auth/subscription/reconnect/writer behavior | [`splices/SPLICE.md`](splices/SPLICE.md), [`splices/common/ENVELOPE.md`](splices/common/ENVELOPE.md) | [`docs/SEALED_CAPTURE_PIPELINE_V1.md`](docs/SEALED_CAPTURE_PIPELINE_V1.md), venue tests under `tests/test_*_splice.py`, `tests/test_spool.py`, and `tests/test_writer_queue.py` |
| Sealed segments, k-way merge, canonical sequencing, continuity, finalizer | [`docs/SEALED_CAPTURE_PIPELINE_V1.md`](docs/SEALED_CAPTURE_PIPELINE_V1.md), [`ingester/INGESTER.md`](ingester/INGESTER.md) | [`encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md`](encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md), Rust crate-local tests, `tests/test_sealed_capture_failure_proofs.py` |
| Python/Rust Zstd codec or canonical compressed output | [`encoder/README.md`](encoder/README.md), [`encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md`](encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md) | [`archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md), `tests/test_encoder.py`, `tests/test_no_sbe.py` |
| Raw archiver, receipts, manifests, or reaper | [`archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md) | [`encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md`](encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md), [`docs/SEALED_CAPTURE_PIPELINE_V1.md`](docs/SEALED_CAPTURE_PIPELINE_V1.md), archive/reaper tests |
| AWS S3 object-store adapter | [`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md`](archive/S3_RAW_ARCHIVE_ADAPTER_V1.md) | [`archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md), [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md), `tests/test_s3store.py` and `tests/test_s3_pipeline.py` |
| Docker Compose, Linux deployment, profiles, storage, operations | [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md), `.env.example`, `compose.yaml` | `compose.targeter-v2.yaml` and [`docs/TARGETER_V2_PHASES_6_10.md`](docs/TARGETER_V2_PHASES_6_10.md) for Targeter v2 rollout; `tests/test_deployment.py` |
| Targeter motivation and current event-selection semantics | [`targeter/README.md`](targeter/README.md), [`docs/TARGETER_V2_PHASES_1_5.md`](docs/TARGETER_V2_PHASES_1_5.md) | `configs/targeter_v2.json`, `tests/test_targeter_v2.py` |
| Targeter v2 archive, atomic publication, splice handoff, scheduler, audit | [`docs/TARGETER_V2_PHASES_6_10.md`](docs/TARGETER_V2_PHASES_6_10.md) | [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md), `tests/test_targeter_v2_delivery.py` |
| Targeter v2 run retention, the run archiver sweep, or the run reaper | [`docs/TARGETER_V2_PHASES_6_10.md`](docs/TARGETER_V2_PHASES_6_10.md) §7 | [`archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md) for the shared separation rule, [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md), `tests/test_targeter_v2_retention.py` |
| Structured League of Legends targeting | [`docs/TARGETER_V2_LEAGUE_OF_LEGENDS_V1.md`](docs/TARGETER_V2_LEAGUE_OF_LEGENDS_V1.md) | Targeter v2 phase docs, `tests/test_targeter_v2_lol.py` |
| CS2, Dota 2, Honor of Kings, or Valorant targeting | [`docs/TARGETER_V2_MULTI_GAME_ESPORTS_V1.md`](docs/TARGETER_V2_MULTI_GAME_ESPORTS_V1.md) | LoL spec for inherited mechanics, Targeter v2 phase docs; remember the multi-game document is proposed until implemented |
| Legacy crypto-ladder targeter | [`targeter/TARGETER.md`](targeter/TARGETER.md), `configs/capture_manifest.json` | [`docs/CAPTURE_SPEC.md`](docs/CAPTURE_SPEC.md); do not apply legacy daemon behavior to Targeter v2 |
| Replay, book reconstruction, trust, economic gates | [`replay/README.md`](replay/README.md) | envelope, sealed-capture, and codec specs; relevant `replay/gate*.py` and replay tests |
| Outcome spaces, masks, void policy, event relationships | [`analysis/MARKET_RELATIONSHIP_GRAPH.md`](analysis/MARKET_RELATIONSHIP_GRAPH.md) | [`analysis/PIPELINE_SPEC.md`](analysis/PIPELINE_SPEC.md), [`analysis/PARTITION_SUM_TEST_SPEC.md`](analysis/PARTITION_SUM_TEST_SPEC.md), outcome/mask/void tests |
| Historical discovery and pull scripts | Root [`README.md`](README.md) historical sections, [`scripts/GAME_DISCOVERY.md`](scripts/GAME_DISCOVERY.md) | Analysis pipeline specs and the matching script's tests |
| Correlation research | [`analysis/CORRELATION_PIPELINE_REVIEW_V1.md`](analysis/CORRELATION_PIPELINE_REVIEW_V1.md) | [`analysis/MARKET_RELATIONSHIP_GRAPH.md`](analysis/MARKET_RELATIONSHIP_GRAPH.md); do not let exploratory statistics alter capture evidence |

## 5. Component boundaries

Keep new code in the layer that owns the decision:

| Layer | Owns | Must not own |
|---|---|---|
| `splices/` | live transport, auth, subscriptions, reconnects, timestamps, envelope write | payload normalization, filtering, economic selection |
| `targeter/` | public catalogue adapters, event matching, selection evidence, target-run archive/publication/retention | socket capture, trade execution, runtime prose inference |
| `ingester/` | raw durability, sealed-segment validation, deterministic evidence order, continuity facts, canonical receipts | venue networking, book interpretation, economics |
| `encoder/` | one strict streaming Zstd contract in Python and Rust | event schemas, archive policy, full-buffer production helpers |
| `archive/` | immutable object storage, archive verification/receipts, manifests, deletion eligibility, durable filesystem primitives | canonical event meaning, direct splice control, any knowledge of `targeter/` |
| `replay/` | decoding, book reconstruction, trust/recovery, ordered gates | mutation of raw/canonical evidence |
| `analysis/` | outcome spaces, masks, equivalence, void/economic policy | irreversible capture filtering |

Vendor-specific raw fields stop at the adapter boundary. Downstream targeter
matching/selection consumes canonical records. S3-specific calls stop inside
`S3ObjectStore`; archiver and reaper depend on the generic object-store
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

### Deployment

At minimum:

```bash
docker compose config --quiet
docker compose -f compose.yaml -f compose.targeter-v2.yaml config --quiet
```

Build only the affected images unless the task or rollout gate requires the
full build. Do not start services, publish targets, contact S3, or enable reaper
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
