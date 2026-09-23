# Offline bundle coverage V1

Implemented: `replay.bundle_coverage:build` is a coverage-only supervisor factory.
It consumes the existing immutable `replay.streams.Cut` / `Book` interface and
does not reconstruct Risk policy. No economics, fees, opportunities, episodes,
fills, trading, timer/cadence, query language, discovery, cache, or UI is included.
The Rust-materializer → Risk → Redis → coverage acceptance walkthrough is a
separate stage; the tests here are offline contract tests, not that walkthrough.

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

## Verification and stage-3 fixture guidance

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

For stage 3, materialize at least two adjacent windows with an explicit primary
lane, two native required keys and an uncaptured listed member. Delay one Full;
include a trade and a duplicate, a scoped interval fault, a later clean window
without a Full, then a recovery Full. Put a scope boundary strictly before a later
book update and request start inside the first raw window. Use these real pins in
preparation and independent transport config. Run `replay.bundle_coverage:build`
through the unchanged supervisor and inspect `read_completed`, checking exact
state durations and NOT_PROVEN/history-incomplete qualifiers. A separate fresh
attempt should have identical three semantic files/hash and a different attempt
binding. Do not claim that these offline unit tests already prove that E2E path.
