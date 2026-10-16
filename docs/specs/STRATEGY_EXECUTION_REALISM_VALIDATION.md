# Strategy execution-realism rollout: implementation and validation

Status: **producer and offline research implementation delivered; site UI deferred
by the user; fresh retained-data acceptance and the full corpus rerun pending.**

The approved [supplied specification](STRATEGY_EXECUTION_REALISM_V1.md) is saved
byte for byte, including its original `proposed` label. Its SHA-256 is
`6ae97739d47dd71414e575a895fc3f9159c3166cd0b0ebf4faeadf36789a1ff6`.
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

## Branch separation and merge order

The producer branch `codex/strategy-execution-realism` starts from PR #78's
`codex/execution-realism-fixes` at
`902871166496aa130fc493685112ba947a08f5e1`. Its new implementation commit is
`11f760a69669b82820cf9d14cc7ef06e704a1fc7`; the approved spec and reference corpus
are a separate cohesive commit. It does not duplicate the dependency fixes.

The research branch `codex/research-execution-realism` starts from PR #77's
`codex/research-outcomes-v2` at
`998cfb71aa4232f7ca24562be0ed597730f2ed5e`. Research code stays on that branch;
the producer branch does not import the research package.

While #78 and #77 are open, each rollout PR is stacked on its corresponding
fix branch. Merge #78 into `feat/corpus-runner`, then retarget the producer
rollout there and review its focused diff. Independently merge #77 into
`feat/research-site`, then retarget the research rollout there. Review the shared
output contract across both PRs before integration. Integrate the updated
corpus-runner lineage into `master` before the research-site lineage consumes
the new outputs, resolving shared history normally. Nothing auto-merges, and
merging code does not authorize operational acceptance or a live pilot.
