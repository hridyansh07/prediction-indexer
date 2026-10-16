# Execution-realism research data and reports: validation

Status: **implemented and offline validated; site UI explicitly deferred by the
user; fresh retained-data strategy acceptance and the full corpus rerun pending.**

This branch starts from `codex/research-outcomes-v2` at
`998cfb71aa4232f7ca24562be0ed597730f2ed5e` (PR #77, targeting
`feat/research-site`). It keeps the research package on its existing branch.
Producer policies and runtime changes are delivered separately on
`codex/strategy-execution-realism`, based on PR #78's
`codex/execution-realism-fixes`. Neither PR imports the other package's whole
branch. No UI source, chart, lens-layer behavior or dependency manifest changed.

## Implemented output contract

- Episodes gain exact edge-per-set and t10 value, pairing class,
  settlement-review and stale-leg fields. Missing legacy flags remain visibly
  unavailable.
- `probes.parquet` keeps the episode/event/bundle/game/market keys, every SDK
  decision/arrival field and exact derived sets, common-unit value/loss and
  decision edge. `volatility.parquet` and each pack's `volatility.json` retain
  book/pair 1s/3s metrics, exact counts/sums and jump rates.
- Each new producer decision carries its own scale-36 `value` and native
  `{atoms,cost,taken,consumed}` decision legs. Research uses only that decision's
  value and `decision.steps * policy.fills.step_contracts` for causal thresholds.
  It never substitutes arrival or opening value for a later confirmation.
  Nonterminating ratios retain reduced rational strings; threshold comparisons
  use exact arithmetic. Native costs are never summed as common-unit money.
- The viability report lists t10 first and groups delayed fills by lens, sizing,
  sequencing, confirmation and arrival, with status counts, fill ratio, median
  matched sets, summed value/gross loss/net after loss and positive-event share
  at 1/2/3-cent decision edges, by game/pairing/review. The resolved event
  denominator is explicit. Duration tables are labelled non-causal context.
- Pair-loss comparisons require the same bundle, scope and both native
  `(book, orientation)` sides. Lens/scenario settings stay distinct. Multi-leg
  or unproven pairs remain unavailable.
- Identity mismatch, incomplete/duplicate probe grids, unknown probe fields,
  noncanonical arithmetic and declared-missing volatility fail visibly. Valid
  empty probe evidence is available; zero-sample book/pair volatility produces
  unavailable metric rows. Undeclared legacy evidence remains unavailable.
- Probe inputs have line/byte/row limits and conservative per-row and whole-build
  state admission before retention/publication. Volatility series are bounded.
  Inputs remain read-only; existing content-addressed version/pointer ordering
  is retained.

The [research README](../../replay/research/README.md) describes the artifacts and
units. The producer PR's `STRATEGY_EXECUTION_REALISM_CONTRACT.md` specifies the
strict new-policy writer/reader contract and preserves the approved source spec.

## Verification

- **64 research tests pass**, including nonempty committed-corpus-to-Parquet,
  report and overlay fixtures; exact fractional steps and threshold boundaries;
  native-side pair joins; signed 80-digit magnitudes; legacy unavailable states;
  identity tampering; incomplete grids; empty evidence; and resource limits.
- **36 proportional corpus/preparation/profile tests pass.**
- Independent review reran all **64 research tests**. Its demonstrated findings
  about empty probe availability, zero-sample volatility, signed magnitude
  length and declared-missing volatility were reproduced before their fixes and
  covered by regressions. No actionable findings remain.
- A separate cross-branch check generated real new-policy producer output,
  passed the independent production reader, then fed the identical files to
  this consumer: **120 probe rows** became Parquet and **960 causal grouped
  summaries**, including t10 at 3 cents. It used exact 0.01-contract steps and
  observed delay terms. Its artifacts are preserved in the producer worktree's
  `.bench/execution-realism-validation/producer-consumer/`.
- Existing UI **typechecking and 75 tests pass** with `TZ=UTC`, despite UI work
  being deferred. No Rust/Compose file changed; those gates were not run.
- The producer-side broader gates report their actual platform limitations:
  719 replay tests with 32 skipped, two failures and five errors in unchanged
  Linux process-containment assumptions; 1,002 root tests with the existing
  Linux `/proc` stale-retrieval liveness failure. These are not reported as green.
- Whitespace checks pass, only named source/test/doc files are committed, and
  neither environment files nor credentials were staged.

## Retained results and operations still pending

The five retained primary-checkout results at
`.bench/corpus-sample/6dd9f00ff1cce627` were preserved. Each receipt independently
verifies all 82 listed files. A fresh read-only research build produces all five
packs without failures, marking all 30 probe/volatility evidence entries
unavailable because those historical results predate the evidence. It does not
invent zero measurements. Output is preserved in the producer worktree under
`.bench/execution-realism-validation/retained-legacy-research/fd594e6020f2c6d1/`.

This is compatibility validation, not the new retained-data acceptance. Mounted
`in/` inputs were stripped from those results, their pinned Linux runtime paths
and canonical store are unavailable locally, and the independent
`delayed_fill.py` screen was not found in the bounded known local locations.
A new five-bundle replay, screen agreement, empirical reasonableness checks and
full corpus rerun remain pending complete pinned inputs and an authorized
disposable runtime. No cloud access, service change, migration/deployment,
target publication, live pilot or PR merge was performed.

Merge #77 into `feat/research-site` first, then retarget this rollout there.
Independently merge #78 into `feat/corpus-runner`, then retarget the producer
rollout there. Review their shared output contract together. Integrate the
updated corpus-runner lineage into `master` before research-site consumes the
new outputs. Code merges do not authorize an operational rerun or live pilot.
