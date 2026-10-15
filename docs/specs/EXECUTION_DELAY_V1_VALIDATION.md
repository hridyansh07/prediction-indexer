# Execution delay V1 validation

Validated offline on 2026-10-15 with the isolated project virtual environment
(Python 3.13.7, macOS). The implementation branch is `codex/execution-delay-v1`,
rebased onto fetched `origin/feat/corpus-runner` at
`30eacb398fbece4b6b78851b68bdd685b125cb61` before applying the supplied
clarifications. The prior data-layer commit `d0517ea` was already incorporated
upstream, so it was dropped as a duplicate during rebase. The upstream schema-v8
migration supersedes the original execution-delay spec's v7 rebuild paragraph.

## Focused gate

**225 tests passed**, no failures or skips:

```bash
.venv/bin/python -m unittest \
  replay.tests.test_execution_delay_probes \
  replay.tests.test_execution_probe_reader \
  replay.tests.test_execution_terms \
  replay.tests.test_economic_sdk_fills \
  replay.tests.test_economic_sdk_port \
  replay.tests.test_economic_sdk \
  replay.tests.test_same_venue_complement \
  replay.tests.test_preparation \
  replay.tests.test_preparation_outcomes \
  replay.tests.test_economic_sdk_outcomes \
  tests.test_universe_execution_terms \
  tests.test_universe_execution_migration \
  tests.test_universe_migrations
```

Coverage includes opening-fill equivalence, recording sizings with short/zero
opening walks, partial/limit/better-price fills, pinned/default delays,
slow-first flooring, bought/sold/complement unwind, signed gains, every status,
exact internal scope/run ends, sparse/full probe byte equality and same-time
ordering. The compatibility tests include the complement V1 golden.

Regressions demonstrated the old reader's exact-end and sold-loss errors before
fixing them. A second falsifying regression exposed pending-cap exhaustion when
a new scope opened before old-scope censors were released. Direct dataclass
construction also demonstrated bypass of the policy's 64-scenario cap; the
runtime now revalidates the optional policy before creating output files.

All four retainable ladder ages are covered, including exit ladders, best
quantity/price changes, deep-only updates and same-time restoration. Conservative
state accounting and count/state/level failures are tested. The independent
reader rejects malformed millisecond types/ranges, altered walks/values/times,
missing scenario rows, and semantic tampering after file identities are rehashed.
Data tests cover exact UTC observations, absent versus observed-null terms,
strict outcomes v1/v2 preparation, migration equivalence and audit identities.

## Broader gates and host limitations

- `.venv/bin/python -m unittest discover -s tests`: **1,002 tests**, one failure
  in `test_stale_retrieval_cleanup_keeps_live_process_directories`.
- `.venv/bin/python -m unittest discover -s replay/tests`: **702 tests**,
  three failing assertions, five errors, **32 skips**.

All remaining failures were reproduced using only offline fixtures in an
untouched temporary snapshot of the exact `30eacb3` base:

- Retrieval cleanup assumes Linux `/proc` process liveness.
- Bundle-runner parent-death tests and supervisor diagnostics require Linux
  process setup (`prctl`/`preexec_fn`), which fails on macOS.
- The corpus derivative-cache path assertion compares macOS `/var/...` spelling
  with its resolved `/private/var/...` spelling.

These are reported failures, not passing Linux acceptance. They were not changed
in this feature. No disposable Redis was configured; Redis-dependent tests stayed
skipped. Python compile checks and `git diff --check` passed. Rust, TypeScript,
Docker builds and deployment checks were not run; this diff changes Python SDK
code, tests and documentation only.

## Acceptance and delivery limits

The retained `.bench/corpus-sample/6dd9f00ff1cce627` sample and independent
`delayed_fill.py` screen are absent from this worktree. Five-bundle acceptance,
screen agreement and corpus per-status counts remain **pending**. Synthetic
status tests are not substituted for that acceptance.

The current wire contract rejects v2. The synthetic equivalence test therefore
compares full-cut and sparse-cut v3 tapes, preserving Risk sequence identities
and computed probe times, and asserts that probes add no retention boundaries
or shift offsets.

No production strategy is enabled or configured to use probes. No live targets,
services, credentials, archive objects or retained user evidence were changed.
Land corpus-runner first, then rebase this branch onto updated master and rerun
affected checks before merging the focused execution-delay change.
