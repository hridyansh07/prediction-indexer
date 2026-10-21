# Market volatility V1 validation

Implementation branch: `codex/market-volatility-v1`.
Base: `feat/corpus-runner`, `365d2f68dcd66c952ce56e2ecc4c378bf3736dc2`.
The branch has no dependency on execution-delay changes.

## Offline proof

The supplied specification was copied exactly to `MARKET_VOLATILITY_V1.md`;
`cmp` against the original attachment passed. Its pasted `proposed` status was
preserved; human authorization supplied the implementation scope.

`replay.tests.test_market_volatility` contains 27 tests covering closed policy
types, ordering, limits and dependencies; constant and jumping books; exact
one-sided durations; same-time staging; prologue/scope/terminal samples;
off-grid destinations and cross-bucket horizons; mid versus quantity changes;
Kalshi Yes/No asks; different native scales; scoped complements, unknown masks,
same-market exclusion and exact below-unit duration; rehashed row and summary
tampering; missing/extra/provisional/symlink files; writer and reader resource
bounds; and an independent brute-force causal endpoint oracle.

A read-only independent agent reviewed writer, reader, mask tables, policy and
integration. It demonstrated that the reader retained impossible-horizon
samples unnecessarily. The new constant-book regression failed with
`volatility reader sample budget` before the fix. The fixed reader keeps only
samples with unfinished in-scope destinations and evicts after the last such
destination. Independent probes checked nonmonotone expiries, exact row/totals
agreement, and empty history at scope boundaries and finish. No further
actionable issue was found.

A clean `git archive` of the base revision ran the same offline V1 and V2 profile
tapes in the same Python/codec environment. Every file, including manifests,
compressed streams and receipts, was byte-identical with volatility disabled.
Those complete-output hash comparisons are pinned in the new test suite.
Existing complement V1 goldens remain unchanged.

The existing Rust-generated full and sparse fixture tapes were replayed with
their clock fields expanded to milliseconds and their Risk IDs, books and
terminal digest preserved. Both produced nonempty, byte-identical
`volatility.ndjson.zst`. The current base accepts wire V3 only: both fixture
envelopes are V3, with the full fixture representing the all-cuts delivery
semantics of V2. This checks full versus sparse delivery without adding a legacy
wire compatibility path. The original fixture retains 26 of 40 Risk cuts.

## Gates and environment limits

Host: macOS, isolated worktree `.venv`, Python 3.13.7. Test inputs and outputs were
temporary; no retained evidence was used or changed.

The focused profile, economic SDK, stream and shared-codec gate passed 214 tests:

```sh
.venv/bin/python -m unittest \
  replay.tests.test_market_volatility \
  replay.tests.test_market_profile \
  replay.tests.test_market_profile_v2 \
  replay.tests.test_market_profile_transitions \
  replay.tests.test_economic_sdk \
  replay.tests.test_economic_sdk_fills \
  replay.tests.test_economic_sdk_outcomes \
  replay.tests.test_economic_sdk_port \
  replay.tests.test_streams tests.test_encoder tests.test_no_sbe
```

Full replay discovery also ran in a disposable Linux container with read-only
source, an isolated executable temporary venv, and no Redis service. The final
run passed 643 tests with 62 skips. Network-dependent and unavailable-runtime
tests were skipped by the existing suite. No ignored Rust/Redis integration
tests were run.

Full root discovery ran 947 tests in that Linux image: 946 passed; its one error
was `test_non_replay_compose_renders_with_example_environment`, because the
image has no `docker` CLI. That exact test passed separately on the host.

The initial host full root/replay runs did not pass. Sandbox loopback denial
blocked 15 HTTP tests; rerunning with loopback permitted cleared those errors.
Remaining host failures involved Linux process/prctl and `/proc` assumptions,
temporary-directory group ownership, and `/var` versus `/private/var` resolution.
Representative failures reproduced from a clean archive of the base revision.
Disposable Linux validation cleared these platform failures. Existing tests
also emit ResourceWarnings for deliberately abandoned failure-path writers.

`git diff --check`, compilation of the new Python modules, and exact specification
copy checks passed. Rust, TypeScript, image builds and deployment rollout were
not needed for this Python-only strategy addition and were not run.

## Pending retained acceptance

The five-bundle sample (`.bench/corpus-sample/6dd9f00ff1cce627`) and delayed-fill
screen/results were not present in this worktree; `.bench` contains only the
tracked fixture builder. The coordination chat supplied no completed delayed-fill
results. The retained profile run, per-game/per-market 1 s and 3 s RMS/jump report,
and pair-gap RMS comparison against delayed-fill leg loss remain pending those
inputs. No archive fetch, live service, credentials, target publication, deployment
or execution-realism implementation was used to substitute for acceptance.
