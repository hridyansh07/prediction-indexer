# Targeter network performance, 2026-10-21

## Current-origin verification

The performance branch is now based on `origin/feat/corpus-runner` at
`2ce0711783dd5a2b0997e7cb9d695c6fe17417db`; the PR targets `feat/corpus-runner`.
That base already declares `polymarket.v2`, including `secondsDelay`, while
`origin/master` at `fd85857` still declares v1. The current catalogue projection,
target-record writer, parsing fixes and consumers are inherited unchanged.

A regression first failed on the older branch because its emitted record named
`polymarket.v1`. On the current base it verifies the emitted v2 identity,
`secondsDelay` participation in the hash, strict downstream consumption of
3 seconds as 3000 ms, and the unchanged `limitless.v1` identity.

| Current-base experiment | Runtime | Completeness | Selected bundles | Polymarket v2 rows |
|---|---:|---|---:|---:|
| Local current-origin source | 36.21 s | All three venues complete | 3 | 11 |
| Current-origin source in `prediction-indexer-capture:targeter-2ce0711` | 38.19 s | All three venues complete | 3 | 11 |

Both fresh runs used `--no-response-cache` and no probe caps. Container timing
includes startup. Each made 75 Kalshi, eight Polymarket and 15 Limitless
requests, with `input_complete: true` and no discovery failures. The runs began
without a committed continuity generation. No production publication or remote
archival was performed.

The independent reader and strict streaming decoder verified both reports and
all 11 artifacts in each run. The current Universe execution-terms reader
accepted all 19 selected target records per run, including all 11 Polymarket
v2 projections and their record/projection hashes; all 11 observations
projected to 1000 ms. Limitless selected no live markets in these snapshots;
its v1 identity is covered offline.

249 focused tests passed, including current Universe execution-term/migration
and replay catalogue/execution-terms tests. The full host gate ran 1007 tests
with the same existing macOS stale-retrieval cleanup failure; its test and
cleanup implementation are unchanged from the base. Rust/UI gates and the full
Linux suite were not rerun for this base update.

Current preserved evidence is under `/tmp/targeter-performance-current-v2/`:
`local-runs/20261021T160611.498891Z/`,
`container-runs/20261021T160803.673976Z/`, `local-independent-audit.json`,
`container-independent-audit.json`, both `*-run.log` files and `full-tests.log`.
Audits and timings live outside the committed runs' artifact inventories.

The original master-based measurements below are historical; the fresh
current-base measurements above are the acceptance evidence for this PR.

## Original objective and measurement history

The user supplied a ten-minute shadow-run baseline from the same Targeter
implementation, tested roughly one hour before this work, and confirmed that
the comparison is shadow mode. The target was under one minute ideally, under
three minutes acceptably, with runs over five minutes considered failures.

Fresh complete shadow runs now take about 45 seconds on this host and in the
existing Linux image. This is approximately a 92.5% reduction against the
user-supplied baseline. The ten-minute baseline was accepted as supplied; it
was not independently rerun during this investigation.

| Experiment | Runtime | Completeness | Selected bundles |
|---|---:|---|---:|
| Persistent sessions and HTTP gzip, serial venues | 57.883 s | All three venues complete | 4 |
| Persistent sessions, gzip, concurrent venues | 45.150 s | All three venues complete | 4 |
| Same optimized source in `prediction-indexer-capture:targeter-2ce0711` | 45.257 s | All three venues complete | 4 |

All three experiments used live requests and `--no-response-cache`, without
probe caps, cache reuse, publication or remote archival. Their live input
snapshots differ naturally; identical-input equivalence is tested separately.
No committed continuity generation was available locally, so these live runs
started without a continuity hold. Concurrent continuity is covered offline.

## Changes

- The targeter HTTP client uses a persistent `requests.Session` per venue,
  requesting gzip/deflate and decoding responses before the existing JSON,
  cache and identity handling. The session does not perform transport retries;
  the existing bounded retry client remains the retry authority.
- Independent venues discover concurrently with at most three workers. Each
  venue has separate request counters and its own session. Futures are consumed
  in adapter order, keeping catalogue and failure ordering deterministic.
- Continuity probes overlap between venues. Lookups and pagination within a
  venue remain sequential. Sessions close after the workers finish, including
  on failures.
- An explicitly injected client retains the existing serial convention for
  offline/debug callers.
- `requests` is now a declared dependency. Version 2.34.2 was already present
  in both the project virtual environment and the existing image.

There are no changes to strategy configuration, venue coverage, request paths,
pagination sizes, selection policy, retry limits, per-host rate limits,
persisted schemas, artifact formats or commit ordering. The shared durable
per-host request slots remain in force. No series filtering or extra same-host
concurrency was needed to achieve the target.

Kalshi remained the limiting stage: 43.806 s in the first run, 44.099 s in the
second, and 43.919 s in the container. Each experiment made 75 Kalshi, seven
Polymarket and 15 Limitless requests, with no request failures. The first run
decoded approximately 346 MB of Kalshi JSON; compression avoids transferring
that representation uncompressed.

## Verification of original measurements

- 209 focused offline tests passed, covering targeter discovery, matching,
  selection, publication, retention, replay streaming and durable HTTP retries.
- A localhost HTTP contract test proves compression negotiation, decoded JSON
  equality, reuse of one TCP connection, bounded retry of HTTP 503 and no retry
  of HTTP 404.
- A barrier-based test forces venue fetches to overlap and finish in reverse
  order, then compares every compressed run artifact, report and report commit
  marker byte-for-byte against serial discovery with the same non-empty input.
- Separate tests preserve terminal/open/unknown continuity evidence across
  concurrent probes, keep other catalogues visible when a venue fails, and
  verify session cleanup and per-venue counters.
- The independent run reader accepted every live report. All 11 compressed
  artifacts in each run passed strict streaming decoding against both logical
  and stored identities. Every report retained version 3 and `artifact_format:
  zstd`, with `input_complete: true` and no discovery failures.
- The full local Python gate ran 938 tests: one existing failure in
  `EventUniverseTests.test_stale_retrieval_cleanup_keeps_live_process_directories`.
  That same failure reproduces in the original checkout without these changes.
- The full Linux-image gate ran 938 tests: the cleanup test passed, but the
  deployment Compose test errored because the capture image has no `docker`
  executable. That deployment test passed in the host gate. These environment
  limitations are recorded rather than hidden or worked around in production.
- `git diff --check` passed. Rust and UI gates were not run; their sources and
  interfaces were unchanged. Production publish/deployment were not performed.

## Original temporary evidence

These paths describe the earlier experiments and may not survive temporary
directory cleanup; current verification paths are listed above. Each original
directory contained its normalized run, request/phase measurements and a
separate independent audit. None of the measurement files were added to a
committed run's inventory.

- `/tmp/targeter-performance/cycle-01-pooled-gzip/`
- `/tmp/targeter-performance/cycle-02-concurrent/`
- `/tmp/targeter-performance/cycle-03-container/`
- `/tmp/targeter-performance-full-tests.log`
- `/tmp/targeter-performance-container-tests.log`

The container experiment mounted the worktree read-only and used only a
separate `/tmp` output volume. Image hashes confirmed that the original durable
HTTP policy, strategy and Kalshi adapter matched the checked-out versions.
Existing user evidence, credentials, target pointers and running services were
not changed.

To measure another independent fresh run from this worktree:

```sh
/usr/bin/time -p .venv/bin/python targeter/run_v2.py \
  --mode shadow --no-response-cache \
  --strategy configs/targeter_v2.json \
  --cache-root /tmp/targeter-performance/rate-state \
  --output-root /tmp/targeter-performance/manual-shadow \
  --live-root /tmp/targeter-performance/empty-live
```

Venue latency, catalogue size and continuity holdings can change future
runtime; the measurements establish the result for these complete fresh runs.
