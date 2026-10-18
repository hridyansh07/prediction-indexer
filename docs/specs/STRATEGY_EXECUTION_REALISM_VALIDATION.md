# Strategy execution-realism rollout: implementation and validation

Status: **producer and offline research implementation delivered; site UI deferred
by the user; fresh retained-data acceptance and the full corpus rerun pending.**

The approved [supplied specification](STRATEGY_EXECUTION_REALISM_V1.md) was
originally saved byte for byte, including its `proposed` label, with SHA-256
`6ae97739d47dd71414e575a895fc3f9159c3166cd0b0ebf4faeadf36789a1ff6`.
That is the historical supplied-text hash, not the current file's hash: the
approved [R12 amendment](PROBE_EXIT_SOURCE_V1.md) now updates §2's exits and
Polymarket default. The R12 specification and its §8 addendum are each recorded
verbatim in separate commits.
The user explicitly authorized this implementation. The
[producer contract](STRATEGY_EXECUTION_REALISM_CONTRACT.md) records implemented
schemas, units and limits. Current `EXECUTION_DELAY_V1.md` and the volatility
implementation contract govern the mechanisms. Universe execution terms remain
schema **v8**, as required by `UNIVERSE_MIGRATIONS_V1.md`; no database operation
is part of this rollout.

## Scope and output contract

The producer implements closed strategy policies 4/2/4/4, recording t10 beside
governing edge, all 60 reference probes, pure pinned market-delay lookup, exit
ladders, and closed pairing/review/stale flags with independent recomputation.
It reuses the existing fee memo and the reviewed scale-36 loss conversions.
Same-venue complement is unchanged. Earlier policies retain their existing
closed schemas and output bytes, including their opt-in probe format.

The new policies carry each probe decision's own `value` and native decision
`legs` so a consumer can calculate the later-confirmation decision edge without
substituting an opening or arrival value. The reader independently revalues this
evidence. Research divides value by `steps * step_contracts`, with exact rational
comparisons at 1/2/3 cents. Nonterminating values retain a reduced rational string;
native leg atoms and costs never masquerade as sets or common-unit money.

The separate research branch implements episode fields, nested `probes.parquet`,
volatility tables, causal grouped delayed-fill reports, and exact native-side
pair/lens loss comparisons. Missing legacy evidence and recorded evidence with
no defined samples remain explicit. Duration tables are labelled non-causal.
The episode-panel UI and all other site UI changes are deferred. Existing lens
layers and charts are untouched.

The [reference corpus template](../../configs/corpus/README.md) keeps coverage,
complement, transitions and levels and adds the new fill policies and volatility.
The existing corpus key binds every setting. The operator-owned configuration
outside version control was preserved; migration instructions use reviewed local
paths and fee inputs, with no invented archive address or credentials.

## Offline verification

- The final focused producer gate passes **237 tests**, covering all four
  integrated strategies, observed/default delays, all 60 scenarios, flags,
  rehashed tampering, exact 0.01 steps, short recording t10, sparse/full probe
  parity with unchanged Risk numbering, resource admission, corpus validation
  and strategy packaging.
- Five fixture families, independently run on dependency base
  `902871166496aa130fc493685112ba947a08f5e1`, pin every legacy output file hash,
  including manifests and receipts. The old explicit-probe fixture and the
  existing complement goldens pass.
- Independent review ran separate 69- and 81-test producer gates and the full
  research gate. Review findings about empty probe availability, zero-sample
  volatility, signed 80-digit values and declared-missing volatility were
  reproduced, corrected and covered by regressions. The research validation
  report records its final counts.
- An additional cross-branch check generated real new-policy producer output,
  passed its independent reader, and fed the unchanged bytes to the separate
  research consumer: **120 probe rows** became Parquet and **960 causal grouped
  summary rows**, including t10 at the three-cent threshold. The fixture used
  exact 0.01-contract steps and observed execution terms. Its output is preserved
  under `.bench/execution-realism-validation/producer-consumer/`.
- Existing research-site TypeScript typechecking and **75 UI tests** pass with
  `TZ=UTC`. No UI source or dependency manifest changed.
- The full replay gate runs **719 tests**, with **32 skipped, two failures and
  five errors**. The failures are the existing materializer signal-containment
  subtests; the errors are the existing Linux `preexec_fn` process-containment
  assumptions in bundle-runner and supervisor diagnostics. Those source/test
  files are unchanged from the dependency base. The final run uses a canonical
  temporary path and isolated optional replay dependencies; missing Redis Python
  packages and the macOS `/var` alias are not counted as implementation passes.
- The final root gate runs **1,002 tests**, with the existing single failure in
  stale-retrieval cleanup's Linux `/proc` liveness assumption. All other tests
  pass. An initial canonical-temp run also exposed a temporary-directory group
  ownership mismatch; the final ordinary-host-temp run resolves that artifact.
- Whitespace checks pass. No Rust or deployment file changed, so Rust/Compose
  gates and codec integration builds were not run. Redis-dependent tests skipped
  by the suite remain unverified; no live service was started for acceptance.

## Retained inputs and pending acceptance

The five retained results were found at the primary checkout's
`.bench/corpus-sample/6dd9f00ff1cce627`. Each corpus receipt independently verifies
all 82 listed files. A fresh, read-only research build from all five completes
with five packs and no failures. All 30 probe/volatility evidence entries are
explicitly unavailable because these historical runs predate those outputs;
they are never represented as zero-valued measurements. The new build is
preserved at
`.bench/execution-realism-validation/retained-legacy-research/fd594e6020f2c6d1/`.

This is legacy compatibility validation, not a fresh strategy acceptance run.
The retained results omit the mounted `in/` inputs, all referenced Linux runtime
paths are absent locally, and the configured canonical store is unavailable.
The independent `delayed_fill.py` screen was not found in the bounded known
repository-local corpus/analysis locations. Thus the new five-bundle replay,
matched-step screen comparison, empirical reasonableness checks, new-data
viability report and full corpus rerun remain pending complete pinned inputs and
an authorized disposable runtime. Synthetic nonempty report tests do not replace
those acceptance steps.

No cloud/archive request, full live corpus run, Universe migration/deployment,
service-state change, target publication, live pilot or PR merge was performed.

## Rebase after prerequisite merges

PR #78 merged into `feat/corpus-runner` at
`36bfb95f50c2a3df0278edcd20ced68056b984d9`; that branch also contains the
throughput and wire-V4 changes from #76. The producer rollout is now rebased onto
that exact feature-branch tip, excluding its already-merged fixes. The two import
conflicts preserve both the new execution-realism hooks and upstream cached
valuation metadata. Its sparse/full parity fixture now uses the current V4
section encoder; its timing, output-byte and Risk-sequence assertions are intact.

The post-rebase focused gate passes **279 tests**, including the complete rollout,
all four strategies, legacy output goldens, quote/probe readers, profile volatility,
corpus/packaging, completed-reader paths and wire-V4 stream checks. The research
branch is rebased onto `feat/research-site` at PR #77's merged commit
`8d4cfc702cd35d35684a16cf7c26952237094ad0`; its code tree is unchanged by the
rebase and all **64 research tests pass** again. Whitespace checks pass on both.
No Rust or deployment files were changed by conflict resolution.

## Branch separation and merge order

`codex/strategy-execution-realism` now targets `feat/corpus-runner` directly in
PR #79. `codex/research-execution-realism` now targets `feat/research-site`
directly in PR #80. The prerequisite fixes #78 and #77 are already merged and
are excluded from both rollout diffs. The initial producer/research bases were
`902871166496aa130fc493685112ba947a08f5e1` and
`998cfb71aa4232f7ca24562be0ed597730f2ed5e`, respectively; the independent legacy
baseline validation above remains pinned to its original source.

The rollout PRs can each merge into their own feature branch. Research code stays
on its research lineage; the producer does not import that package. Integrate the
updated corpus-runner lineage into `master` before research-site consumes the new
outputs, after the separately pending acceptance. No PR is auto-merged, and code
merges do not authorize operational acceptance or a live pilot.

## R10: Kalshi ask-only uncross recovery (17 October 2026)

The reported cache/dependency mismatch is **confirmed by a runtime regression**,
starting from producer revision `bb3e401a72a869cfed7537a6e92e2cb07d858662`.
Before production code changed, two new tests failed in all eight subcases:
cross-venue arbitrage policy 4, same-venue multi-market policy 2 and both
implication-cover policy-4 factories, each with either ask-only operations or a
same-bid snapshot. Every case uses distinct committed times, usable books,
known synthetic fees and unchanged bid/projected-acquisition fill objects.

At 16s the Kalshi source has bid 60/ask 50. At 18s only its ask recovers to 70;
the projected acquisition ask and the other leg remain 0.40. A fresh production
evaluation has positive gross value of 0.20 per set, but the persisted exclusion
remained until 40s: **24 seconds instead of 2**. The semantic reader accepted
that internally consistent output, demonstrating why this requires a runtime
regression rather than only a rehashed-output check.

The three owning basket constructors now declare the evaluator's `crossed`
input for Kalshi legs. This makes ask-side selection and fingerprint invalidation
observe the transition without changing the SDK or disabling the exclusion.
After the fix, all eight subcases preserve the initial exclusion, record exactly
2s of it, open the expected 18s–40s net episode and write all 60 filled probe
scenarios. The real writer and independent semantic reader complete in each case.

The expanded focused gate passes **295 tests**, including these regressions,
all four strategy packages, SDK cache/port/outcome/metadata checks, quote/probe
readers, wire-V4 streams, corpus validation and the existing all-file legacy
hash goldens. Whitespace checks pass. Full root/replay suites and Rust, UI,
Compose and live acceptance were not rerun for this three-declaration Python
correction; their earlier outcomes above remain historical. Existing recorded
inputs/results are unchanged.

## R12: acquired-book probe exits, bounded walks and sold shortfalls (17 October 2026)

Work started in a new `fix/probe-exit-source` worktree, verified against exactly
`origin/feat/corpus-runner` at `40c280416b402999fb0ce260008d09d64da03c49`
before editing. The first commit records the approved R12 spec verbatim.
The owner's §8 addendum is a separate verbatim spec commit. Regressions for
both stages were written and run before their corresponding production fixes.

### Before/after observations through real writers and semantic readers

All new R12 Kalshi fixtures carry both orientations with native bids only,
never injected native asks. They exercise prepared snapshots, the decoder,
the real strategy factories and the SDK writer, then independent aggregate and
strategy readers. They do not exercise a live venue, retained data or corpus.

| Case | Before | After |
|---|---|---|
| R12, BUY 10 at 0.40, other leg fills 6, acquired bid 0.30 | On unchanged base production code, `exit_consumed: []`, proceeds 0, full excess-cost loss 1.60 | Acquired YES/NO bids cover 4; proceeds 1.20; loss 0.40 (scale-36 `400000000000000000000000000000000000`) |
| R12, acquired bids cover only 2 of 4 at 0.30/0.20 | Empty exit; loss 1.60 | Two real exit levels, proceeds 0.50; uncovered 2 keep full cost; loss 1.10 |
| R12, acquired book empty or unusable | Empty exit, full-cost loss | Unchanged conservative result: no borrowed ladder, proceeds 0, loss 1.60 |
| F2, Kalshi excess 4, bids 30×1/20×1/10×10, cap 2 | With §2 implemented but before §8, attempt raises `probe consumed level bound` | All 60 scenarios read; selected row exits exactly first 2 levels, proceeds 50 native, loss 1.10 at scale 36 |
| F2, Polymarket excess 1,000,000 atoms, 300×250,000/200×250,000/100×1,000,000, cap 2 | Same level-bound attempt failure | Two-level exit, proceeds 125,000,000; loss 275,000,000 (native scale 9); remainder keeps its cost |
| F1, sold excess 1,000,000 atoms at 650, empty repurchase book, max price 1000 | Writer reports −650,000,000 loss; reader accepts it | Writer and reader charge uncovered atoms: +350,000,000 loss; actual exit remains empty with proceeds 0 |
| F1, same sold excess, only 250,000 repurchased at 750 | Writer reports −462,500,000 loss; reader accepts it | Actual repurchase cost 187,500,000 plus 750,000×1000 uncovered charge minus 650,000,000 sales = +287,500,000 loss |

The base R12 run executes six methods with 15 failing subcases, including
acquired YES/NO and 0.01-contract steps. The §8 before run has two failing
sold-loss subcases and four errors (two thin-exit failures and two independent
reader arithmetic cases). Its unchanged entry-bound and reader level-cap
controls pass. After the fixes, the same cases pass. Fully covered favourable
sold exits remain negative losses; there is no clamping.

Additional regressions pin the final same-time arrival view rather than a
decision/intermediate/later book; every factory's 60 scenarios; exact missing
plan/builder, missing ladder and unsupported Kalshi SELL messages; Kalshi bid-only
requirements; and the 1000 ms default with observed zero taking precedence.
The reader rejects rehashed rows with `max_levels + 1` exit levels or sold loss
that omits the uncovered charge. The original entry-bound failure test is
unmodified. Probe row schemas and retained cut/shift boundaries are unchanged.

### Compatibility and final offline gates

- Every old-policy output hash, the complement V1 goldens and A1's non-Kalshi
  probe bytes pass unchanged. A1's probe SHA-256 remains
  `f26f5060f2ad6bc37cf2df4aab40fe2816519baeb4c403c40d7b1eaf739a6900`.
- The PM/Limitless-only realism fixture holds its configured delay fixed;
  its probe SHA-256 remains
  `89f36166ac2f8b3df7a4e05e461d79d555179976cc9d2add79d7933e754036d0`.
- Nine whole probe files from §2-only and §8 output compare byte-identically
  for bought exits within the cap: all four factories, fractional NO,
  insufficient/empty/unusable exits, and PM/Limitless. Four factory hashes are
  pinned in the integrated regression.
- Focused §7 modules plus `test_corpus_execution_realism`: **158 tests pass**.
  Modules: `test_strategy_execution_realism`, `test_execution_probe_integration`,
  `test_execution_delay_probes`, `test_execution_probe_reader`,
  `test_cross_venue_arbitrage`, `test_same_venue_multi_market`,
  `test_implication_cover`, `test_strategy_packages`, `test_economic_sdk_port`,
  `test_streams`, and the reference-corpus module.
- Full Python gates: `.venv/bin/python -m unittest discover -s tests`:
  **1,002 tests pass**; `.venv/bin/python -m unittest discover -s replay/tests`:
  **758 tests, OK with 62 skips**. Optional integration coverage requiring
  prebuilt Rust binaries or explicitly disposable Redis remains unverified.
  Whitespace checks pass. No Rust/UI/deployment files changed.

The initial focused run exposed a synthetic reader double that did not inherit
the SDK `Strategy` default; it now inherits that interface. The first full
replay run also found the reference-corpus test's old 3000 ms expectation;
it now pins the approved 1000 ms default. Neither required production fallback
behavior. R10's old synthetic ask-only tests explicitly opt into ask reads;
production Kalshi requirements do not.

Local evidence is under `.amp/in/artifacts/`: `r12-base-regressions.log`,
`r12-addendum-before.log`, `r12-after-evidence.json`,
`r12-section2-hashes.json`, and the focused/full gate logs. No corpus, bench,
Docker, live service, production data, object store or database operation was
performed, and `data/` and credentials were untouched. Offline unittest-owned
temporary fixtures are the only integration runtime used.

## Follow-up: legacy opt-in retention and separate capacity blocker (17 October 2026)

Local remediation starts from integration
`e3f933004dae0ed1131f844523b47a214001be96` (#82), in the new
`fix/legacy-probe-retention` worktree. The first commit records
[the support and capacity specification](LEGACY_PROBE_RETENTION_V1.md).
The audit checkout and published history are unchanged; this follow-up has not
been pushed. Production code was verified at local revision
`66a4d54f746b60f6a4686809bdcf1ba2416779a3` before this documentation-only commit.

The unchanged five-layout package method fails on the base's multi-market
policy 1 with explicit probes, before its first frame and before compression:
`probe exit ladder not retained`. The mismatch originates in #75 (`f4cfa5c`):
the SDK added optional probes and mandatory exit-retention binding while the
strategies retained only entry ladders. #78 (`36bfb95`) documented opt-in legacy
probes but tested them with a retention override. #79 (`40c2804`) added retention
gated on new-policy flags. #82 first exercises the documented legacy opt-in
without that override. This is neither a new R12 exit-mapping defect nor a codec
failure or invalid policy fixture.

Requirements now follow actual `fills.probes`; policy still selects exit
mapping. Legacy probes retain the entry-source reverse transform, including
its native-ask input, while new policies retain acquired-book native bids.
The change neither fabricates Kalshi asks nor upgrades legacy economics.
Implication covers inherit the owning cross-venue requirements. Bind checks,
their exact errors, row schemas and R12/§8 algorithms remain unchanged.

### Falsification and compatibility evidence

| Check | Before production fix | After |
|---|---|---|
| Unchanged five-layout package method | One multi-market bind error on plain output | Passes all five layouts and both storage representations, including exact decoded bytes, identities and recomputed summaries |
| Legacy probe regressions and unpatched existing integration tests | Seven methods produce 13 bind errors: ten new bids-only subcases and three existing cases | Same seven methods plus the package method: eight pass |
| Budget diagnostics | Three methods produce ten failing subcases against unchanged budget code | Same diagnostics pass; 43-test SDK/profile focused selection passes |

The committed legacy regression uses all four factories, Kalshi YES and NO in
cross-venue, plain and economic-zstd, with 60 scenarios per selected episode.
It uses production requirements without a retention override and adds no native
Kalshi asks. An additional 20-run fixture comparison covers both policy families
and both storage representations: 1,200 selected-episode rows pass real writers
and independent semantic readers; logical probe identities and experiment
identities match across storage. For BUY 10 at 0.40 with peer fill 6 and acquired
bid 0.30, legacy intentionally records empty exits and full-cost loss 1.60;
realism still exits 4 for 1.20 and loses 0.40 at scale 36. Existing explicit-probe,
no-probe and complement byte goldens pass unchanged.

All verification uses offline disposable fixtures under `nice -n 19`:

- Expanded focused selection, including R12 §7 modules, profile/codec controls
  and economic SDK bounds: **324 tests pass**.
- `.venv/bin/python -m unittest discover -s tests -q`: **1,002 tests pass**.
- `.venv/bin/python -m unittest discover -s replay/tests -q`: **777 tests, OK
  with 62 skips**. Skipped optional integrations remain unverified; no Redis
  service, full Rust gate, UI or deployment gate was run.
- `git diff --check` passes. Existing fixture-constructor resource warnings are
  visible, not suppressed. The original package test is unmodified.

Evidence is under this worktree's `.amp/in/artifacts/`:
`base-package-gate.log`, `base-legacy-regressions.log`,
`fixed-legacy-and-package.log`, `base-budget-diagnostics-corrected.log`,
`fixed-budget-focused.log`, `legacy-realism-evidence.json`, and
`final-focused.log`, `final-root.log`, `final-replay.log`.

### The reported 512 MiB failure is not yet classified or resolved

Writer and ordinary plain/zstd reader diagnostics now identify the stream and
logical bytes, rows or line bytes with the applicable cap. Detached-state
diagnostics separately identify retained-byte usage and cap, before growth.
No numeric limit changed: ordinary files remain 512 MiB/2,000,000 rows,
ordinary lines 64 KiB, retained state independently 512 MiB; availability and
16 GiB/50-million-row transitions/levels/volatility limits stay unchanged.
Compression follows bounded plaintext writing and cannot bypass those caps.

The owner's reported ten-bundle failure lacks an exception and failing-stream
metadata, so it cannot yet justify an output-cap increase or a memory change.
The remaining owner input is non-secret exception/traceback, revision and
configuration identity, participant/stream, attempted bytes/rows/line size or
state usage/request/cap, and expected complete counts if available. A measured
stream-specific byte ceiling and adjacent row ceiling remain an explicit
decision after that evidence, with consistent writer/plain/zstd-reader changes.
There is no truncation, sampling, scenario reduction or episode splitting.

No retained/corpus/bench workload, Docker, external service, production data,
object store, secrets, `data/` or `~/pi-data` was accessed for this remediation.
The actual operator-configuration/Universe-v8 five-bundle acceptance remains a
separate operational gate; a smaller run cannot cure an oversized single bundle.

## Follow-up: failure-only probe census (18 October 2026)

Starting from local `0772c1f`, the diagnostic amendment in
[the retention specification §5](LEGACY_PROBE_RETENTION_V1.md#5-failure-only-probe-and-shared-writer-memory-diagnostics)
adds pending scenarios/cap, all runtime open episodes, the priced-fill subset,
probe groups (including a partially admitted grid), admitting scenarios and raw
heap events. The same lazy context covers shared writer retained-state failures
outside probe admission, including view and episode growth. Existing prefixes,
used/replaced/requested/attempted bytes and caps remain intact. Counts run only
on failure, use existing episode tables and constant extra workspace, and do
not traverse heap objects. Rejection still precedes charge/retention.

Before production changes, four regression methods fail in five subcases on
missing counts. Afterward, all four pass. The partial-grid case reports **59
pending scenarios, two runtime episodes, one fill episode, one probe group and
59 heap events**, even though the group is not yet registered. Shared view
growth tests cover one pending scenario/two arrival events with either one
open fill episode or none after closure; failed growth leaves budget usage and
the retained view unchanged.

Under `nice -n 19`, the expanded focused selection passes **201 tests**:
execution delay/probe integration/probe reader, strategy execution realism,
economic SDK/fills/port, strategy packaging and market profile V2. Existing
goldens, all-four-factory 60-scenario controls and semantic readers pass.
A separate real R12 fixture writes all 60 selected rows and passes its reader
with loss 0.40; its diagnostic callback runs once for an induced writer-state
failure and zero times during subsequent successful completion. Independent
reader-state and output-budget controls retain distinct prefixes and contain
no writer probe counts. Whitespace checks pass; existing fixture resource
warnings remain visible. Full root/replay gates were not repeated for this
failure-only amendment; their previous results above remain historical.

Logs: `.amp/in/artifacts/probe-counts-before-corrected.log`,
`probe-counts-after.log`, `probe-counts-focused.log` and
`probe-counts-controls.log`. No limits, scenarios, reservations, release timing,
retention, economics or output schemas changed. No reservation optimization is
included: later decision/arrival books may deepen beyond opening depth. No
corpus/bench workload, Docker, service, network, retained data, secrets, `data/`
or `~/pi-data` access occurred. The owner's actual memory exception and the
separate operational acceptance remain unverified here.
