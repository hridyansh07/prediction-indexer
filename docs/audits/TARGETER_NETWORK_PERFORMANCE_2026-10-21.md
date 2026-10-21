# Targeter network performance, 2026-10-21

## Objective and result

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

## Verification

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

## Preserved evidence

Each directory contains its normalized run, request/phase measurements and a
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
