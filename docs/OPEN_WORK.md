# Open verification and deferred work

Implementation contracts are indexed in [`specs/README.md`](specs/README.md).
This list preserves unresolved checks from completed implementation reports;
it does not authorize live runs, archive access, deployment or a pilot.

## Retained replay acceptance

- Run the new execution-realism policies and profile volatility on the pinned
  five-bundle sample with complete canonical inputs, fee evidence, context and
  the original runtime available. Require every independent reader to pass.
  Old results without probe/volatility files prove legacy compatibility only.
- Compare delayed-fill matched steps with the independent `delayed_fill.py`
  screen; report per-status counts and review the reasonableness checks in
  [execution-realism acceptance](specs/STRATEGY_EXECUTION_REALISM_V1.md#6-acceptance).
  The last implementation reports lacked the complete inputs and screen.
- Report per-game/per-market 1 s and 3 s RMS moves and jump rates, and compare
  each established gap pair's RMS with its delayed-fill leg loss. Preserve
  unavailable evidence explicitly. The full corpus rerun and new-data viability
  report remain separate from synthetic tests and legacy-result compatibility.
- Complete the retained C9–LYON game-state/book-close comparison in
  [GAME_STATE_SDK_V1 §8](specs/GAME_STATE_SDK_V1.md#8-tests-and-acceptance).
  Offline preparation, SDK and corpus plumbing do not prove this acceptance or
  live game-state capture.
- Keep the profile V2 retained performance/size comparison in
  [§9](specs/MARKET_PROFILE_V2.md#9-acceptance-retained-data) distinct from its
  offline format and parity tests.

Venue-time materialization/Risk and Linux strategy smoke were verified on the
September 27–28 and October 3 retained fixtures. The full October 3 seven-group
attempt was blocked by the then-8 MiB entity cap; a bounded occurrence passed
all seven groups, and the full interval passed coverage, complement,
multi-market and profile. October 3 outcomes were unavailable, so those runs
did not prove admitted mask-dependent economics. The current 256 MiB entity
limit does not retroactively complete that acceptance. Deployment and live
execution were not verified by those runs.

## Capacity and process memory

- Diagnose the reported ten-bundle capacity failure from its non-secret
  exception/traceback, revision/configuration identity, participant and stream,
  attempted bytes/rows/line size or retained-state usage/request/cap. The prior
  report lacked that evidence; do not infer which limit failed.
- Keep ordinary logical file limits (512 MiB, 2,000,000 rows, 64 KiB lines)
  separate from the shared 512 MiB retained-state budget and from the special
  entity, availability and profile-stream limits. Compression cannot bypass a
  logical cap, and phase-aware probe reservations do not certify corpus capacity.
  Any measured cap change needs consistent writer/plain/zstd-reader checks.
- The bench runner currently has no Docker process-memory limit. Set and verify
  one before relying on container memory protection for corpus workloads. SDK
  retained-state accounting is not a process-RSS bound. Preserve complete
  evidence rather than truncating, sampling or reducing the probe grid.

## Universe and deferred scope

- Keep production-copy migration/rebuild diffs and short-range serving
  equivalence as operator gates under
  [UNIVERSE_MIGRATIONS_V1 §4](specs/UNIVERSE_MIGRATIONS_V1.md#4-proof).
  Unit fixtures do not certify the production correction set or v8 rollout.
- The research consumer lives on its own lineage; the execution-realism site UI
  was deferred. Producer implementation does not implement that UI or authorize
  a live pilot.
- Asserted historical target-record backfill and write-on-change compaction
  remain pending/deferred in
  [TARGET_RECORD_BACKFILL.md](specs/TARGET_RECORD_BACKFILL.md). Replay jobs and
  `bundle_coverage` remain supported; their retirement is not part of this work.
