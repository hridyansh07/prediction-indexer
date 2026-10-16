# Execution-realism branch comparison and verified fixes

## Branch identity

`spec/execution-realism` resolves to
`373b0703cd6369d985829c606555d48c881fa9da`. Its parent is
`bd63927a1eced21a4dc19e1e20d9f4e2341c305a`; its only commit adds four documents:

- `docs/specs/EXECUTION_DELAY_SDK_V1.md`
- `docs/specs/MARKET_VOLATILITY_V1.md`
- `docs/specs/REPLAY_THROUGHPUT_V1.md`
- `docs/specs/STRATEGY_EXECUTION_REALISM_V1.md`

There are no executable or test changes in that commit. Its strategy-realism
document is byte-for-byte identical to the user-supplied document reviewed on
16 October 2026. The branch's worktree also contains an uncommitted edit to the
SDK spec; that edit was preserved and is not part of `373b070`.

The code reviewed was the newer `feat/corpus-runner` at
`f4cfa5cdc4481ba9d8e6d63cf1571767d114f0f4`, not the spec branch. The runtime and
reader files for the four economic strategies match between these trees.
Their only strategy-package difference is the five-line cross-venue fee
assessment optimization supporting deferred identities. Corpus-runner also has
the throughput, volatility, execution-delay SDK and Universe migration work that
the older spec branch lacks.

Neither branch implements the third specification's complete rollout: policy
4/2/4/4, reference `t10` and probe policies, episode flags, research probe tables
and reports, site display and new corpus configuration remain pending.

## Delivery scope

`codex/execution-realism-fixes` starts at the exact corpus-runner revision above.
It fixes the demonstrated native-loss unit mismatch and missing snapshots in
reader-only strategy instances. It does not merge the older spec branch or
claim to implement its complete strategy rollout.

The research outcomes-v2 compatibility fix is delivered separately on
`codex/research-outcomes-v2`, based on research-site
`1c47f689888d1a50703d0bb69a3daeda01eb5a29`. The research consumer is not present
on corpus-runner, so importing the entire research branch would obscure this
focused diff.

## Verification

Before production edits, the new regressions reproduced both reader failures
(`snapshot=None`) and the raw-native loss mismatch (`100000000` rather than
`10**35` in the explicit scale-36 synthetic strategy).

After the fixes:

- 142 focused producer tests pass, including six new production integration
  regressions, probe writer/reader tests, fills and all four affected strategies.
- An independent 63-test gate passes. Separate checks cover signed mixed-scale
  cancellation and rejection of rehashed loss corruption.
- Every output byte, including manifests and receipts, matches the unchanged
  corpus-runner base for four built-in strategy fixtures without probes. The
  existing complement goldens also pass.
- The full replay suite runs 708 tests with 32 skips. Its three failure reports
  and five error reports match the pre-fix 702-test baseline: Linux process
  containment/`preexec_fn`, signal subtests and macOS `/var` versus
  `/private/var` spelling. Those failing source/test files are unchanged by
  this branch.
- The final root suite runs 1,002 tests with one failure, the same Linux
  `/proc` liveness assumption in stale retrieval cleanup seen on the untouched
  base. Disposable loopback HTTP tests run successfully with test permissions.
- The research fix passes seven focused tests, all 44 research tests, 48
  preparation/outcomes/game-state compatibility tests, and an independent
  14-test review. Local-archive and committed-game-state builds both accept
  current outcomes v2; closed-schema rejection and v1 controls remain covered.
- Whitespace checks pass on both branches. Only explicitly named source,
  documentation and offline test files are staged; no environment or secret
  files are included.

The research compatibility fix is published separately as
[PR #77](https://github.com/hridyansh07/prediction-indexer/pull/77), targeting
`feat/research-site`.

No retained five-bundle bench acceptance, independent corpus screen, full corpus
rerun, Rust/Redis execution, TypeScript/UI build, live API/cloud request or
deployment is claimed. These Python fixes do not change Rust, TypeScript or
deployment files. The complete strategy-realism rollout remains pending.
