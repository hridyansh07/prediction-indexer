# Whole-attempt Replay runner V1

`python -m replay.supervisor CONFIG.json RUN_DIRECTORY` runs one Rust publisher
and one Python adapter per required strategy group against a **supplied dedicated
Redis ≥8.2**, with positive maxmemory and noeviction. Set `REDIS_URL` separately;
it is never persisted or printed. Install `.[replay-redis]` and build
`cargo build --manifest-path engine/Cargo.toml -p replay-transport` first.
No Redis startup, configuration, deployment, shared server probe, or ACL management
is performed. Linux and a single-threaded supervisor process are required.

## Required configuration and invocation contract

The closed JSON object contains:

```json
{
  "version": 1,
  "publisher": "/absolute/path/engine/target/debug/replay-publish",
  "python": "/absolute/path/.venv/bin/python",
  "transport": {},
  "strategies": {
    "strategy-a": {"factory": "my_package.strategy:build", "revision": "immutable-code-revision", "config": {}},
    "strategy-b": {"factory": "my_package.other:build", "revision": "immutable-code-revision", "config": {}}
  },
  "limits": {
    "attempts": 3, "no_progress": 2, "progress_margin": 100,
    "stall_seconds": 30, "attempt_seconds": 300, "run_seconds": 900,
    "poll_seconds": 0.1, "stop_seconds": 2
  }
}
```

Replace `transport` with the complete publisher configuration documented in
[REPLAY_STREAMS_V1.md](REPLAY_STREAMS_V1.md), **omitting `attempt_id`**. Use absolute
input directories. All limits above are illustrative operational choices, not
defaults; every field is required. Command timeout is 2–60000 milliseconds.
Groups must match strategy keys exactly; `.`, `..`, `publisher`, `ready`,
`publisher.json`, `result.json`, and `interrupted.json` are reserved.
The config is limited to 1 MiB, validated before launching, copied durably as
`run.json`, and SHA-256 bound to every checkpoint and completion marker.
Validation independently recomputes the typed composite normalizer descriptor,
checks pinned profile-2 receipt/manifest metadata, and derives the only accepted
price/quantity scales for each plan venue. Invalid identity, pin, profile, venue,
or scale combinations fail before any participant is launched.
After the Python binding checks, `validate` invokes the pinned publisher's
read-only `--validate-only` mode with a bounded stdin configuration. This reuses
the authoritative Rust `inspect_pinned` reader for closed nested schemas, source
coverage, exact serialization, directory/address identity, and repeated bindings.
It creates no Redis client, readiness marker, or output. Its output is discarded,
its deadline is `attempt_seconds`, and it is killed if the supervisor dies.
Independent success reading performs the same validation; the pinned executable
must remain available even for read-only reruns.

Each importable factory receives deeply immutable context with `run_id`,
`attempt_id`, `group`, `identity`, strategy `config`, and `output_directory`.
It returns an object implementing `__call__(cut)` and `finish()`.
The adapter delivers initial, cut, and terminal records from `replay.streams`.
The expected initial is built independently from the pinned run config, never
learned from the stream. The terminal callback and `finish()` must both return
successfully before its ACK. `finish()` must flush and close all strategy files.
The adapter then validates its local terminal and writes `complete.json`.

Strategy owns episode schema, economics, filenames and any content verification.
Write only provisional regular files/directories under the supplied output path.
Do not spawn detached processes, close inherited lock descriptors, mutate the
run directory, override signal handlers, or use exit codes to disguise hook
exceptions. Factories/callbacks/finalizers that raise are **nonretryable**, even
if they raise a transport exception. Strategies must not perform irreversible
external side effects. The offline coverage-only factory is documented in
[BUNDLE_COVERAGE_V1.md](BUNDLE_COVERAGE_V1.md); no economic strategy is supplied.
The tiny supervisor test strategy is deliberately not an economic model.

The run identity binds pins, configuration, groups, executable paths, and strategy
revision/config assertions. Executables, installed packages, Python import path,
environment, and pinned derivative directories must remain unchanged throughout
the run and restarts. The runner does not hash executables or certify that a
user-supplied revision string matches installed code.

## Retry budget, interruption, and ownership

`state.json` contains total **started** attempts, historical best slowest-group
completed Redis entry sequence, consecutive failures without meaningful progress,
run start time, and active attempt ID/progress. It contains no books or resume
cursor. Start count and random attempt namespace are durable **before launch**.
Every retry uses the original pins, empty books, sequence zero, fresh stream/state
keys and fresh output directory. Failed outputs and small `result.json` diagnostics
are retained. Child stdout/stderr are discarded rather than growing unbounded or
being parsed as retry evidence.

A failed attempt counts as meaningful progress iff its slowest required group's
completed sequence is **at least previous historical best + progress_margin**.
Best is still updated for sub-margin improvements; regressions/oscillation cannot
grant new retries. Initial best is −1; entry sequence is wire sequence + 1.
At `attempts` starts or `no_progress` consecutive stagnant failures, no further
attempt starts. Fatal input/risk/protocol/schema/invariant/strategy failures stop
immediately. Transport, resource, process signal and deadline failures may retry
within both budgets. Unknown positive participant exits are fatal; 21 is the
closed retryable exit class, 20 fatal, and 0 alone never proves completion.

The supervisor polls readiness/progress with the explicit finite poll interval,
enforces finite no-progress and whole-attempt deadlines, plus a persisted overall
wall-clock deadline across invocations. Command timeouts may extend observed
deadline handling by one command timeout; child teardown adds `stop_seconds`.
Run clocks must not be moved backwards. Setup readiness is a new file created by
the Rust publisher after setup and initial publication; consumers never reconnect
or mistake pre-setup absence for an attempt failure. Queue exhaustion is retryable
but bounded by the same budgets, not hidden by dropping data.

A nonblocking flock protects one local run directory. Its descriptor is inherited
by participants; Linux parent-death SIGKILL kills direct children on supervisor
death. Ordinary failure/signals terminate then kill only owned process groups,
wait for them, and only then delete the two exact disposable Redis keys. No flush,
wildcard deletion, user-data deletion, or failed-output deletion occurs. Only a
post-setup ready file proves key ownership; failed/ambiguous setup leaves keys
alone. If cleanup cannot reach Redis, keys remain. After SIGKILL/reboot, unfinished attempts are
recorded as interrupted, never resumed or inferred successful; their Redis keys
are left as evidence. Restart never signals persisted PIDs. An inherited busy lock
fails closed until its owner exits. Use one stable run directory per logical run.

## Receipt scope and independent reading

Layout: `run.json`, `.lock`, `state.json`, and `<attempt-id>/` containing publisher
config/readiness, each `<group>/output/`, each local completion attestation, and a
bounded attempt result. Nothing under a failed attempt is a committed episode.

Success requires all of: publisher exit 0; validated terminal publication; every
registered group terminal-ACKed; every adapter exit 0 with an independently read
matching local terminal attestation; and no local failure. After owned children
have stopped, files and directories are fsynced, then root `SUCCESS.json` is
written durably **last**. A crash before this marker abandons the attempt even if
all children had finished. A valid marker makes reruns read-only/idempotent.

`replay.supervisor.read_success(Path(run_directory))` strictly checks the marker,
run identity, attempt result and every participant attestation/output location.
The marker binds completion and output locations, **not episode file contents or
economics**. No episode hash, schema guarantee, or exactly-once external side
effect is claimed. Completed attempt directories are immutable by ownership
contract, not chmod or a security boundary; consumers must not modify them.
Strategy-specific readers/receipts remain responsible for episode validation.

CLI returns 0 only for whole-attempt success, 20 for fatal failure, 21 for exhausted
retry budget, interruption, or local resource/lock failure. Restarting with the
same directory cannot reset persisted budgets or change immutable configuration.

## Narrow bundle entry point

`scripts/replay_bundle.py REQUEST.json WORKDIR` is the single automated
materialize-and-supervise entry point. It durably binds the closed canonical
request, invokes a request-pinned prebuilt `materialize_range` example binary,
derives plan scales from the returned normalizer identity, validates this
supervisor configuration, runs it, independently reads `SUCCESS.json`, and writes
a regenerable `WORKDIR/result.json`. `REDIS_URL` remains environment-only and is
never persisted or printed; the script never invokes Cargo. It accepts at most
4096 adjacent pins, which is 85 days 8 hours at half-hour windows.

A separate persistent `WORKDIR/.lock` serializes request binding through result
publication. Contention fails retryably before writing `request.json`; the lock
pathname is never removed. The helper inherits the lock, runs in an owned process
group, and receives a Linux parent-death signal. Cancellation/timeouts stop and
wait for it. Helper stdout is limited to 4 MiB while reading, stderr is discarded,
and `REDIS_URL` is removed from the helper and metadata-check environments.
These are trusted prebuilt binaries, not a sandbox for arbitrary executables;
they must not spawn detached descendants. Use trusted, separately owned canonical,
derivative, and work directories without concurrent symlink/path mutation.
Canonical catalogue discovery still scans retained receipt metadata before the
4096 selected-window check; that limit is not a bound on total catalogue size.

This entry point does not yet use `replay.preparation`, `replay.strategy_sdk`,
`replay.bundle_coverage`, or `replay.coverage_output.read_completed`, although
they exist in this tree. Preparation and bundle coverage currently run through
the manual walkthrough in [BUNDLE_COVERAGE_V1.md](BUNDLE_COVERAGE_V1.md), with
caller-supplied pins. The narrow request therefore carries immutable plan
keys/lanes (never scales), one existing strategy factory/revision/config, capture
roots, and runtime binary paths. Its result attests validated supervisor
completion, not strategy semantics. Preparation can later replace the plan seam,
and a completed-result reader can replace the generic completion payload, without
changing materialization or transport binding.

The closed request is:

```json
{
  "version": 1,
  "run_id": "bundle-run-1",
  "interval": {"start_ns": "0", "end_ns": "100", "lower_bound": "clip"},
  "capture": {"canonical_root": "/absolute/canonical", "derivative_root": "/absolute/derived"},
  "plans": [{"instrument": "kalshi:TICKER", "orientation": "outcome", "lane": "kalshi", "venue": "kalshi"}],
  "strategy": {"group": "test", "factory": "package.module:build", "revision": "immutable-revision", "config": {}},
  "runtime": {
    "materializer": "/absolute/materialize_range",
    "publisher": "/absolute/replay-publish",
    "python": "/absolute/python",
    "scope": "research",
    "max_entry_bytes": "1048576",
    "max_queue_bytes": "67108864",
    "command_timeout_ms": 5000,
    "limits": {"attempts": 3, "no_progress": 2, "progress_margin": 100, "stall_seconds": 30, "attempt_seconds": 300, "run_seconds": 900, "poll_seconds": 0.1, "stop_seconds": 2}
  }
}
```

The caller cannot supply pins, normalizer descriptors, or scales. `result.json`
contains the canonical request digest, helper-returned typed identity and pins,
and the independently validated supervisor completion receipt. It is
regenerable and is not a new commit marker.

## Verification

Offline: `.venv/bin/python -m unittest replay.tests.test_supervisor`. The
subprocess tests import `replay.tests` from a generated executable, so the
repository must be installed in the virtual environment
(`.venv/bin/pip install -e '.[replay-redis]'`). Process-containment tests require
Linux (`prctl` parent-death signal); they fail rather than skip on macOS.
For an explicitly disposable server only, set `REPLAY_REDIS_URL` and run that test
plus `replay.tests.test_streams_redis`. The Rust contract test
`redis_supervisor_retries_entire_attempt` materializes pinned profile-2 input,
runs the real Rust publisher and two real Python adapters, deliberately fails
attempt one, checks fresh sequence-zero retry, preserves provisional evidence,
and independently reads the success marker. Run via:

```bash
cargo test --manifest-path engine/Cargo.toml -p replay-transport \
  --test contract -- --ignored --test-threads=1 --nocapture
```
