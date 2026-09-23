# Offline bundle coverage V1

Implemented: `replay.bundle_coverage:build` is a coverage-only supervisor factory.
It consumes the existing immutable `replay.streams.Cut` / `Book` interface and
does not reconstruct Risk policy. No economics, fees, opportunities, episodes,
fills, trading, timer/cadence, query language, discovery, cache, or UI is included.
The opt-in Rust-materializer → Risk → Redis → supervisor → coverage acceptance
below uses synthetic contracts. It does not establish retained-data acceptance
or validate live venue normalizers.

## Factory and input binding

Prepare a snapshot using [STRATEGY_PREPARATION_V1.md](STRATEGY_PREPARATION_V1.md).
Supply this closed strategy entry to the existing supervisor:

```json
{
  "factory": "replay.bundle_coverage:build",
  "revision": "<immutable installed code revision>",
  "config": {
    "version": 1,
    "snapshot_directory": "/absolute/path/to/prepared-context",
    "snapshot_sha256": "<64 lowercase hex characters>"
  }
}
```

Transport configuration remains independently supplied. Its ordered pins, requested
start/end, explicit lower-bound policy, and ordered plans must equal the snapshot.
`PreparedInput` loads and independently validates the pinned snapshot before any
callback; `bind(initial)` additionally enforces this equality when the Consumer's
already-validated initial control arrives. No network client is instantiated.
The factory returns a callable with `finish()`, exactly as the existing adapter
expects. It writes only inside `context.output_directory`, which must be empty.

The reusable `replay.strategy_sdk` contains only `PreparedInput`, `plain` (immutable
metadata to JSON containers), and `LineWriter` (bounded, exclusive, durable NDJSON).
It introduces no strategy base class or scheduling interface. Analysis focus is
the resolved `scopes[].members` selected by preparation's bundle/probe configuration;
there is no sport-specific branching or runtime relationship selection.

Both Risk and the wire Decoder currently reject empty native plans. A probe whose
**entire union** is uncaptured therefore fails startup with
`no native risk plans: entirely uncaptured probe unsupported by transport`.
No dummy BookKey or fabricated producer tick is added. A particular scope may be
entirely uncaptured when other scopes provide native plans: its member and bundle
intervals report `NOT_CAPTURED`, even if an earlier scope left usable books.

## Time, usability, and denominator

All intervals are half-open `[start_ns,end_ns)`, within the requested interval.
Exact integer nanoseconds are used throughout. A scope applies as a whole at its
start. The final terminal closes every interval at the requested end. Missing
terminal, a prefix-only stream, a window gap, or any failed callback cannot finish.
Empty Redis polls never call this strategy and never advance its clock.

Group cuts take effect at `origin.visible_ns`; window cuts at their raw window
start, clipped for reporting. Expansion/prologue groups before requested start
may initialize Risk books, but produce no pre-request intervals or trade counts.
Clipping never rewrites the source window interval. Before processing a later
cut, intervening scope boundaries use the **prior immutable books**, because the
Decoder has already installed the incoming cut's entire atomic transition set.
Scope changes do not change Risk books. Same-time cuts produce no positive-length
intermediate state and no overlap; zero-length intervals are discarded.

Native books retain Risk's `not_initialized`, `usable`, or `unusable` state and
the original closed reason object. Book identity is instrument **and orientation**;
source authority is the snapshot plan's explicit lane. Delayed initialization,
quiet periods, empty valid ladders, and trades alone do not imply missing data.
Only authoritative Risk transitions initialize, invalidate, or recover a book.

Each requested member remains in the denominator, including listed-but-unselected
members with unknown native mapping. Their member interval has zero required books,
one uncaptured member, and `NOT_CAPTURED`; no book row is invented. The snapshot
retains listed, capture-selected, and requested sets separately.

Member and bundle rows report native `usable_books`, `required_books`, and
`uncaptured_members`. The state is:

| State | Exact condition |
|---|---|
| `AVAILABLE_UNDER_POLICY` | At least one required book, every required book usable, no uncaptured member |
| `PARTIAL` | At least one usable native book, but some required book unusable/uninitialized or a member uncaptured |
| `UNAVAILABLE` | Required captured books exist, none usable |
| `NOT_CAPTURED` | Every requested member is outside expected capture; no required native books |

Empty requested scopes fail; an empty conjunction never means availability.
Durations reconcile separately for every book/member/bundle within each scope.
Summing all books yields book-nanoseconds, not elapsed bundle duration. Revision
changes and trades do not split intervals. An unchanged book status retains its
first status provenance; fault windows remain separate when their evidence source
interval changes. Scope boundaries always split. There is no staleness timer.

## Evidence is conservative and retrospective

Wire V1 exposes the window's pin/start/end and Risk's scoped invalidations, **not**
the whole verified coverage inventory. Consequently book `evidence` is only:

- `unknown`: no current-window negative fact exposed for this planned book;
  **not** a claim that the lane is present or complete;
- `lane_missing`, `lane_invalid`, `lane_not_expected`, or
  `visible_clock_regression`: an authoritative window invalidation, using the
  existing Risk reason spelling and retaining the exact source window.

No inference of positive coverage from silence is made, and no second Python Risk
verifier or expanded wire format is introduced. Entering a later clean window
resets current evidence to `unknown` but does not clear a latched unusable book
reason. Only a later valid Full recovers that book. Window failures are retrospective
verified interval knowledge, already applied by Risk; these results do not claim
that an online observer could know the failure at the interval's beginning.

Every interval and summary says `vendor_completeness: "NOT_PROVEN"`. Usable under
Risk policy does not certify receipt of all vendor frames. There are **no missing
trade counts**. Trade observations matching a currently required native key within
the requested interval are counted by original disposition (`observed`, `applied`,
`duplicate`, `not_authority`, `invalidated`). `nonduplicate_trade_observations` sums
all except `duplicate`; it is an observation count, not deduplicated economic
volume or a count of executions. Other dispositions, including non-authority,
remain visible. No additional trade identity heuristic is used and trades never
initialize books or fragment availability.

Preparation's limits remain: `membership_basis: "caller_pinned_expectations"`,
`history_complete: false`, no proof of actual socket subscription/retirement,
Universe as trusted historical projection, explicit pinned archived fallback,
and no cross-run cache. Coverage does not strengthen these claims.

## Closed files and identities

Successful local `finish()` produces four provisional regular files:

| File | Closed fields |
|---|---|
| `intervals.ndjson` | Common: `version`, `scope`, `entity`, `start_ns`, `end_ns`, `vendor_completeness`, `kind`, `state`; variants below |
| `summary.json` | `version`, `snapshot_sha256`, `membership_basis`, `history_complete`, `vendor_completeness`, `trades`, `nonduplicate_trade_observations`, `durations` |
| `manifest.json` | `version`, `strategy`, `snapshot_sha256`, `intervals`, `trades`, `nonduplicate_trade_observations`, `summary_sha256` |
| `content_receipt.json` | `version`, `semantic_sha256`, `run_id`, `attempt_id`, `group`, `identity`, `terminal` |

Versions are JSON integer 1; `strategy` is `bundle_coverage_v1`. Times and duration
values are canonical unsigned decimal strings. Scope indexes, counts, terminal,
and versions are JSON integers (booleans/floats rejected). SHA-256 values are
lowercase hex. All fields are required; unknown fields, duplicate JSON keys,
unknown states/reasons/versions, invalid references, and a non-LF-terminated tail
are errors.

`scope` indexes the pinned snapshot's ordered scopes. Entities are `bundle`,
`member:<market_id>`, or `book:<sha256>`, where the book digest is preparation's
canonical JSON hash of `{instrument,orientation}`. `book_id(book)` resolves it.
The pinned snapshot supplies native mapping, explicit authority lane, full listed
and requested membership, run/context identities, and scale metadata without
duplicating it on every interval. It must remain available for independent reading.

Book rows additionally have `evidence`, `reason`, `source`, `evidence_source`.
`source` is the original wire origin of the first status observation retained in
this interval (null for `not_initialized`); it is a bounded source span, not a
list of every revision/dependency. `evidence_source` is a raw window origin for a
current interval fault, otherwise null. Member/bundle rows instead have
`usable_books`, `required_books`, `uncaptured_members`. They inherit provenance
through their scoped native rows and pinned membership.

Manifest `intervals` is `{sha256,byte_length,records}` over exact UTF-8 NDJSON bytes
including LF. Summary `durations` is an ordered list of `{scope,entity,state_ns}`;
`state_ns` maps precisely the states that occurred to their summed duration strings.
Summary hash and receipt `semantic_sha256` are hashes of canonical JSON (the
preparation encoding) of summary and manifest respectively. Whitespace in metadata
is not semantic identity. Output data, manifest, summary and semantic identity are
stable across retries. The receipt separately binds the actual supervisor run
identity, attempt, group and terminal Redis entry sequence; random attempts and
wall-clock times never enter semantic records.

Write ordering is: stream `.open` → flush/fsync → rename → directory fsync → strict
independent content validation → durable summary → durable manifest → durable
content receipt last. An existing output is never resumed or overwritten by a
new factory. Failed files remain provisional evidence. The surrounding supervisor
then owns terminal ACK, participant completion, all-group validation, and `SUCCESS`.
There is no circular receipt hash: the strategy receipt does not hash `SUCCESS`,
and `SUCCESS` attests completion rather than content.

## Independent readers and bounds

```python
from replay.coverage_output import read_completed, read_provisional, validate_content

# The only completed-result API: BOTH supervisor success and content are required.
result = read_completed(run_directory, "coverage")
summary = result["summary"]  # result also contains manifest and content receipt

# For diagnostics only: validates files, never claims committed completion.
result = read_provisional(output_directory, snapshot_directory,
                          expected_sha256=pinned_snapshot_sha256)
```

`validate_content(directory, loaded_snapshot, manifest)` is the lower-level
provisional validator used before manifest publication. The snapshot must come
from the existing strict loader. It independently checks record schemas, pin and
time references, per-entity gap/overlap/order/end closure, exact file identity,
and member/bundle counts and states at every temporal boundary. It derives the
summary from the intervals; it does not import the coverage state algorithm.
`read_completed` also calls the real `replay.supervisor.read_success` and matches
run, identity, attempt, group, output location, and terminal against its receipt.
It independently rechecks the snapshot/transport configuration binding too.
Neither reader authenticates a maliciously rewritten, fully self-consistent tape
and receipt set, nor reexecutes Risk from the derivative; pins and immutable
ownership remain the trust boundary.

No full output/tape is loaded for validation. Limits: 256 MiB interval bytes,
1,000,000 records, 64 KiB per line, 32,768 total scope/entity pairs, 16 MiB metadata;
preparation's snapshot and stream's entry/book bounds apply independently. The
runtime retains current/prior immutable book snapshots and one open row per active
entity, not every event reference. The reader retains bounded entity cursors and
duration totals, one line, and active statuses. A private disposable SQLite temporal
index uses a 2 MiB page-cache target and a 1 GiB main-database cap, with disk-backed
sort/index scratch additionally bounded by input limits. Resource failures abort
without a content receipt. Scratch is removed normally, but hard process death may
leave system temporary files; no broad cleanup service is introduced.

## Offline contract verification

```bash
.venv/bin/python -m unittest replay.tests.test_bundle_coverage \
  replay.tests.test_preparation replay.tests.test_streams replay.tests.test_supervisor
```

Tests use real preparation/strict loaders and real immutable Decoder cuts, small
hand-authored Risk decisions, plus fake supervisor artifacts checked by its real
success reader. They cover delayed initialization, unrelated cuts, quiet intervals,
trades/duplicates, faults/recovery, old latched reasons versus new evidence, scope
changes during silence, mixed/all-uncaptured cases, prologue clipping, equal-time
transitions, missing terminal, byte determinism, bounds/retained memory, rehashed
schema/reference/arithmetic tampering, incomplete files and receipt-last failures.

## Synthetic cross-language acceptance

`redis_bundle_coverage_acceptance` in `engine/crates/transport/tests/contract.rs`
uses the existing hand-authored canonical fixture and scripted normalization seam,
then real materialization, verified traversal, Risk, publisher CLI, Redis Streams,
supervisor subprocesses, `replay.bundle_coverage:build`, and `read_completed`.
`replay/tests/test_coverage_acceptance.py` is its Python assertion driver, **not a
retained-data runner**: it deliberately corrupts a private copy of one synthetic
input for the failure case. Invoke it only through the Rust test.

Four adjacent raw windows cover `[0,80)`; reporting requests `[10,80)` with explicit
`clip`. A pre-request Full at 5 must not initialize reporting. Token 123 initializes
at 14; token 987 at 29, including a valid empty ladder. A trade at 18 and duplicate
at 19 yield one nonduplicate observation. The first scope includes an uncaptured
listed member. At 23 that member leaves the requested denominator; the incoming
29-ns cut must not backdate its newly usable token to that quiet scope boundary.
The primary lane is missing during `[40,50)`. Clean `[50,60)` has no Full and
cannot recover either book. Fulls at 61 and 67 recover them separately.

Independently expected bundle durations:

| Scope | Interval | Unavailable | Partial | Available under policy |
|---|---|---|---|---|
| 0 | `[10,23)` | 4 ns | 9 ns | 0 ns |
| 1 | `[23,80)` | 21 ns | 12 ns | 24 ns |

The test checks every book/member/bundle duration, exact bundle intervals, the
uncaptured denominator, unknown positive evidence, and incomplete-history/vendor
qualifiers. It injects a crash after content completion but before `SUCCESS`:
the completed reader rejects that attempt, restart uses a new attempt, and both
retry and independent fresh run have identical three semantic files/hash. Finally,
a truncated last derivative makes the real publisher fail without terminal,
content receipt, or supervisor success. No Risk decisions are hand-authored in
Python. No live API response or historical capture is used.

Run only against **disposable dedicated Redis ≥8.2**, positive finite maxmemory,
noeviction, no persistence, loopback-only. The wider test suite temporarily changes
maxmemory and pauses Redis, so do not use a shared instance and run serially.
In an Amp orb, use a supervised service (replace the executable path if needed):

```bash
# Install Redis 8.2 separately; do not use a distribution's older Redis.
amp orb service start coverage-redis --command '/absolute/path/redis-server --bind 127.0.0.1 --port 6382 --save "" --appendonly no --maxmemory 128mb --maxmemory-policy noeviction'
uv pip install --python .venv/bin/python 'redis>=6.4,<7'
export REPLAY_REDIS_URL=redis://127.0.0.1:6382/0
cargo test --manifest-path engine/Cargo.toml -p replay-transport \
  --test contract redis_bundle_coverage_acceptance -- --ignored --exact --nocapture
# Optional wider transport and supervisor acceptance, on the SAME disposable server:
cargo test --manifest-path engine/Cargo.toml -p replay-transport \
  --test contract -- --ignored --test-threads=1 --nocapture
.venv/bin/python -m unittest replay.tests.test_streams_redis replay.tests.test_supervisor
amp orb service stop coverage-redis
```

## Bounded retained-data walkthrough (requires actual pinned inputs)

Use the **existing `replay.supervisor` runner**, not the synthetic test driver.
No retained data was available for this implementation's acceptance. The recipe
below is an operational entry point, not a claim that historical data passed.

1. Choose one bundle or an explicit nonempty market subset and a finite requested
   interval. Supply the minimal adjacent profile-2 derivative pins covering it,
   exact absolute directories, explicit primary lanes/scales, and lower-bound
   policy. Do not scan for newest derivatives. Missing derivatives or selection
   occurrences are unavailable input, not permission to fabricate a fixture.
2. Create the closed preparation JSON from `STRATEGY_PREPARATION_V1.md`. Pin every
   caller-declared occurrence partitioning the requested interval and its source
   hashes. Obtain actual IDs/hashes from your reviewed evidence; do not substitute
   the synthetic IDs above. A union with no native plans is unsupported.
3. Prepare into a new writable research directory outside retained data. Universe
   is first, at a directly configurable URL. If archived fallback is desired,
   explicitly supply matching production run receipts and existing archive backend
   configuration; the existing S3/GCS ObjectStore and
   `ArchivedTargeterRunByteStreamer` remain the only archive transport. For example:

```bash
.venv/bin/python - /research/prepare.json /research/context "$UNIVERSE_BASE_URL" \
  /reviewed/receipt-one.json /reviewed/receipt-two.json <<'PY'
import sys
from pathlib import Path
from replay.preparation import MAX_BYTES, UniverseHTTP, prepare
from replay.preparation_sources import ArchivedSelections
from replay.streams.protocol import decode
from archive.storage.factory import build_store
from targeter.v2.run_archive import read_run_archive_receipt

config_path, output, url, *receipt_paths = sys.argv[1:]
with open(config_path, 'rb') as stream:
    config = decode(stream.read(MAX_BYTES + 1), MAX_BYTES)
# Omit receipt arguments to fail closed on unavailable Universe without fallback.
fallback = None
if receipt_paths:
    import os
    if os.environ.get('ARCHIVE_BACKEND') not in ('s3', 'gcs'):
        raise SystemExit('Select the existing S3 or GCS archive backend explicitly')
    fallback = ArchivedSelections(
        build_store(primary_roots=[Path('/retained/capture')]),
        [read_run_archive_receipt(Path(p)) for p in receipt_paths],
    )
prepare(config, Path(output), universe=UniverseHTTP(url, timeout=10), fallback=fallback)
PY
```

Replace paths explicitly; `/retained/capture` is the actual protected primary
root passed to the existing store factory, not a scratch output. This preparation
step only reads remote evidence. It does not publish targets, archive, delete,
discover venues, or introduce a cache. Preserve `context.json` and `receipt.json`
unchanged. Record the receipt's `snapshot_sha256` independently for run binding.

4. Create `coverage-run.json` using the closed supervisor configuration in
   `REPLAY_SUPERVISOR_V1.md`: absolute publisher/Python paths, `groups:["coverage"]`,
   the exact inputs/bounds/policy and independently reviewed ordered native `plans`,
   and the factory entry at the top of this document with the pinned snapshot.
   Supply finite budgets; a small first run can use a 1 MiB entry cap, 64 MiB queue,
   5000-ms command timeout, 3 attempts, 2 no-progress failures, progress margin 100,
   30-s stall, 300-s attempt, 900-s overall, 0.1-s poll, and 2-s stop. These are
   operational examples, not automatic defaults or permission to expand scope.
   Risk's fixed bounds and coverage's output limits apply independently; exhaustions
   fail closed. Provision temporary disk for the verified window and reader SQLite
   scratch; this CLI uses system temporary storage and does not reserve space.
5. Validate binding locally before starting processes, then run on a supplied
   dedicated Redis. `REDIS_URL` is environment-only and must never be printed or
   persisted in the JSON. Keep binaries/imports/configuration and pins unchanged
   across restart. Do not use the destructive integration tests on this server.

```bash
cargo build --manifest-path engine/Cargo.toml -p replay-transport
.venv/bin/python - /research/coverage-run.json <<'PY'
import sys
from replay.supervisor import read, validate, initial
from replay.strategy_sdk import PreparedInput
from replay.streams.protocol import freeze
c = validate(read(sys.argv[1]))
assert c['transport']['groups'] == ['coverage']
assert c['strategies']['coverage']['factory'] == 'replay.bundle_coverage:build'
PreparedInput(c['strategies']['coverage']['config']).bind(freeze(initial(c)))
print('snapshot/transport binding verified; retained bytes not yet replayed')
PY
.venv/bin/python -m replay.supervisor /research/coverage-run.json /research/coverage-run
.venv/bin/python - /research/coverage-run <<'PY'
import json, sys
from replay.coverage_output import read_completed
result = read_completed(sys.argv[1], 'coverage')
print(json.dumps(result, sort_keys=True, indent=2))
PY
```

Only the final successful reader establishes a completed coverage result. Record
its manifest semantic hash, snapshot pin, requested bounds, attempts and qualifiers
alongside any retained-data acceptance report. Preserve failed attempts; restart
the same run directory to preserve budgets, never patch receipts or advance a
cursor. A deliberate fresh comparison uses a new directory with unchanged config
and pins; compare `intervals.ndjson`, `summary.json`, and `manifest.json` bytewise,
not the attempt-bound receipt. Missing `SUCCESS`, content receipt, or terminal is
failure even if some interval rows look plausible. A successful result still says
`NOT_PROVEN` and `history_complete:false`; it is not a trading/economic conclusion.
