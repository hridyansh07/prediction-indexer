# Local replay bench V1

Status: **implemented.** `replay.bench` provides the V1 command surface.
Offline contracts and retained-input Docker runs are verified; fresh Universe
preparation still requires an operator-provided exported endpoint.

The local replay bench is a reusable, agent-agnostic workflow for preparing a
pinned bundle context, running one or more replay strategy groups over it in a
disposable container, and reading and checking the result. It also diffs one
context or run against another.

It turns a loop that has so far been run by hand into one command surface any
agent or operator can use. The bench is for development and verification. It
does not replace production Replay jobs ([REPLAY_JOBS_V1.md](REPLAY_JOBS_V1.md))
or `scripts/replay_bundle.py`.

Nothing in the bench names a fixture, an event, a bundle or a strategy. All of
that comes from input files the caller supplies.

## 1. Scope

In scope:

- preparation through the existing preparation API, with an outcomes warm-up
  and a fail-loud check;
- a structured diff between two prepared contexts;
- a deterministic fee catalog built from a declarative schedule spec;
- a single-attempt supervisor run of declared strategy groups inside the
  replay-runner image, against a disposable Redis;
- completed reading through each group's own reader, followed by declared
  independent checks;
- a structured diff between two runs;
- cleanup of the Docker containers, networks and images the bench created,
  with any cleanup failure reported in retained orchestration evidence.

Out of scope:

- production scheduling, the Universe job queue, archival and publication;
- new strategies, readers or economic logic;
- network access in tests;
- anything that reads `.env`, keys or credentials;
- infrastructure identifiers. This repository is public. How a private Universe
  is reached (a port forward, a VPN) is the operator's concern; the bench only
  reads an exported `UNIVERSE_BASE_URL`.

## 2. Package and entry points

- Package: `replay/bench/`. It ships with the replay package, so the same code
  runs on the host and inside the image.
- Host CLI: `python -m replay.bench <command> …`.
- In-container entry: `python -m replay.bench.inside <resolved-spec>`. It is
  invoked only by `run`. It is not a user command.
- Base branch: the branch that carries outcome-aware preparation, the economic
  SDK and the fee SDK (currently `codex/cross-venue-arbitrage-v1`). `master`
  does not yet carry them.

Exit codes are the same for every command:

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | invalid input or refused operation |
| 2 | ran, but a declared check or comparison expectation failed |
| 3 | environment failure (Docker, Universe, supervisor) |

## 3. Invariants

1. **Outputs are evidence.**
   - Every command that writes takes an output directory and refuses one that
     already exists.
   - Nothing is overwritten, cleaned or deleted inside output directories.
   - A failed attempt keeps its directory and logs.
2. **No secrets.**
   - Preparation reads only an exported `UNIVERSE_BASE_URL`. Call
     `universe_from_environment(env_file=None)`; never parse a dotenv file.
   - The URL value is never logged or written; record only that it was set.
   - No credentials are passed into containers.
3. **Pinned inputs.**
   - A run binds the context by the `snapshot_sha256` from its `receipt.json`,
     the fee catalog by its identity, and the image by its ID.
   - It also records the build source's git commit and whether its tree was
     dirty.
   - All of these appear in `result.json`.
4. **Single attempt.** Supervisor limits force `attempts: 1`. The bench never
   retries a run inside one output directory. A new attempt needs a new
   directory.
5. **Unique Docker names.** Containers, networks and image tags include the run
   label and a random suffix, so concurrent bench runs cannot collide. Fixed
   names such as a shared `cross-redis` are not allowed.
6. **Cleanup.**
   - Containers and networks the bench created are removed in a `finally`
     path, on every exit, including failures and interrupts.
   - An image the bench built is removed after the run unless `--keep-image`
     is given.
   - Ownership labels are verified before collecting logs or removing a resource;
     ambiguous creation or name collisions cannot authorize unrelated cleanup.
     Cleanup failures give exit 3 and remain in `orchestration.json`.
   - Build-cache pruning affects other builds, so it is opt-in only, with
     `--prune-build-cache`.
   - The bench never removes containers, images or volumes it did not create.

## 4. Commands

### 4.1 `prepare CONFIG OUT_DIR [--warm-timeout-s 120] [--allow-outcomes-unavailable]`

1. Validate `CONFIG` with the existing closed preparation configuration
   ([STRATEGY_PREPARATION_V1.md](STRATEGY_PREPARATION_V1.md)).
2. Require an exported `UNIVERSE_BASE_URL`.
3. Warm up the Universe outcomes endpoint for the config's `bundle_id`:
   - `GET /v1/bundles/{bundle_id}/outcomes`, with a generous timeout and up to
     2 retries;
   - record the status codes and durations.

   A cold Universe cache has made the preparation client's short timeout
   produce `outcomes.unavailable` for every book.
4. Run the existing `prepare(config, OUT_DIR, universe=source)`.
5. After preparation, check that `outcomes.provider` is non-null. If outcomes
   are unavailable:
   - exit 3, naming the reason;
   - keep the directory;
   - pass only with `--allow-outcomes-unavailable`.
6. Write `OUT_DIR/bench_prepare.json` with:
   - the config sha256;
   - the snapshot sha256;
   - warm-up timings;
   - the outcomes provider status;
   - per-scope outcome-book status counts.

### 4.2 `compare-context A B [--expect-only outcomes,scopes.outcome_books]`

This command reads only. It loads both contexts through `load_snapshot`, with
each receipt's sha256, and reports:

- for each top-level key, equal or different;
- for `scopes`: whether everything except `outcome_books` is equal, plus a
  per-`(status, market_id)` count diff of `outcome_books`;
- for `outcomes`: whether the documents are equal, and the provider and
  unavailable fields.

`--expect-only` lists the paths allowed to differ. Any other difference exits 2.
The default expects no differences.

### 4.3 `fees SPEC OUT_DIR`

Builds a fee catalog through `replay.fees.artifacts.build_catalog` from a closed
declarative schedule spec, version 1. The spec lists schedules in the fee SDK's
own terms (see [replay/fees/README.md](../replay/fees/README.md)):

- venue and scope;
- the model and its parameters;
- `fee_scale`;
- effective dates;
- sources. Each source is a local file path plus its expected sha256, so the
  catalog's evidence is pinned.

The same spec and sources must give the same catalog identity. The command
prints the identity and writes `OUT_DIR/bench_fees.json`.

There is one validation rule here because it has already caused a failure: a
model that needs a particular `fee_scale` for its rounding must declare it, and a
mismatch is rejected at spec validation, not discovered at run time. PM
`pm_declared_fill_ceil5_scenario` requires scale 5; Kalshi and Limitless CLOB
models require scale 6.

The closed fee spec is `{"version": 1, "schedules": [...]}`. Each schedule uses
the Fee SDK's tagged `Schedule` encoding, with exactly these fields: `type`
(`"Schedule"`), `component`, `scope`, `economics`, `fee_asset`, `fee_scale`,
`model`, `sources`, `effective_from`, `effective_to`, `effective_evidence`,
`extractor_version`, `model_version`, and `supersedes`. Nested tagged objects and
enums retain the SDK's closed encoding and validation; there is no parallel fee
schema or inferred default rate. The one substitution is each source, whose
closed bench fields are `path`, `sha256`, `url`, and `retrieved_at`. `path` is an
absolute existing local file; `sha256` pins its bytes; `url` is the public HTTPS
source attribution; `retrieved_at` is a nonnegative integer Unix-ns timestamp.
The SDK derives byte length. Unknown fields are rejected. Bounds are 10,000
schedules, 32 sources per schedule, and 16 MiB per source/spec artifact.

The identity-named catalog is written under `OUT_DIR/<catalog_identity>/`.
Supply that selected directory as `fee_catalog_directory`, not `OUT_DIR`.

### 4.4 `run SPEC OUT_DIR [--keep-image] [--prune-build-cache] [--label L]`

1. Validate the run spec (§5) and resolve it:
   - substitute path tokens;
   - read `snapshot_sha256` from the context receipt;
   - load the fee catalog identity if one is given.
2. Build the image from `image.build_context` with `image.dockerfile`, or reuse
   `image.reuse_id`. Record the image ID, the build context's `git rev-parse
   HEAD`, and its dirty flag.
3. Create a run network. Start Redis ≥ 8.2 with `--maxmemory`, `noeviction`,
   no persistence, and the configured memory.
4. Start the runner container on that network:
   - every mount from `mounts` is read-only, plus a single writable mount for
     `OUT_DIR`;
   - the entrypoint is `python -m replay.bench.inside /bench/spec.resolved.json`.
5. Inside the container, `inside` does the following:
   1. Build the supervisor config from `base_run_config`. Set `strategies` to
      the declared groups and `transport.groups` to their names in order. Set a
      unique `transport.run_id`. Apply `limits` overrides, then force
      `limits.attempts = 1` (an override cannot enable retries).
   2. Write `OUT_DIR/input.json`.
   3. Call `replay.supervisor.run(config, OUT_DIR/run, redis_url)`.
   4. For each group, call its `reader(OUT_DIR/run, name)`, which must require
      supervisor SUCCESS (every existing `read_completed` does). Write the
      returned receipt, and the summary when present, to
      `OUT_DIR/groups/<name>/`.
   5. For each group, run its `checks` (§6), writing
      `OUT_DIR/groups/<name>/checks/<check>.json`.
   6. Write `OUT_DIR/result.json` (§7).
6. On the host: save the container logs to `OUT_DIR/logs/runner.log` and
   `logs/redis.log`, then clean up (§3.6).

A failing check gives `status: CHECKS_FAILED` and exit 2. A supervisor or
reader failure gives `status: FAILED` with the error and a bounded traceback,
and exit 3.

### 4.5 `compare-runs A B [--expect FILE]`

This command reads only. For each group present in both runs it reports:

- semantic and receipt hashes, equal or different;
- the summary-row diff, using the group's declared `compare` (§5): identical,
  changed (naming the fields), added and removed rows;
- check result differences.

`--expect` is a closed file that declares the expected added, removed and
changed row keys per group. Any unexpected difference exits 2. The default
expects identical rows and checks, with equal semantic hashes. Attempt-bound
receipt hashes may differ. A semantic difference with no declared row changes
is unexpected. Both groups without summaries produce an explicit `NO_SUMMARY`
row report and still compare hashes/checks; a summary missing on just one side
is unexpected. A missing summary cannot satisfy nonempty row expectations.

The expectation file is closed version 1:

```json
{"version": 1, "groups": {"<group>": {
  "added": [["<key-field-1>", "<key-field-2>"]],
  "removed": [], "changed": []
}}}
```

Every group entry requires `added`, `removed`, and `changed` arrays of composite
row keys, in the declared `compare.key` order. Keys are nonempty arrays of JSON
string/integer/boolean/null scalars; arity must match, and duplicate keys fail.
Unknown groups or fields fail. Omitted groups expect no row changes. Group
membership, check differences and one-sided summary absence cannot be waived by
this row-only expectation schema. Comparison reads completed bench results and
validates their pins, group/factory bindings, and copied receipt/check artifacts
against `result.json`; it does not rerun strategy readers or import private checks
on the host.

## 5. Run spec (version 1, closed)

```json
{
  "version": 1,
  "image": {"build_context": "<repo or worktree path>",
            "dockerfile": "docker/replay-runner.Dockerfile",
            "reuse_id": null},
  "redis": {"image": "redis:8.2-alpine", "maxmemory": "150mb"},
  "base_run_config": "<host path to a supervisor run.json>",
  "context_directory": "<host path to a prepared context>",
  "fee_catalog_directory": null,
  "mounts": [{"host": "<host path>", "container": "<absolute path>"}],
  "limits": {"attempt_seconds": 3600, "run_seconds": 3700, "stall_seconds": 120},
  "groups": [
    {"name": "<group>",
     "factory": "<module:build>",
     "revision": "<free text recorded in the run config>",
     "config": {},
     "reader": "<module:read_completed>",
     "checks": ["<module:function>"],
     "compare": {"rows": "rows", "key": ["<field>", "..."]}}
  ]
}
```

**Paths.**

- Host paths must be absolute and must exist.
- The context is mounted at `/bench/context`. Only the selected fee catalog
  is mounted at `/bench/fees/<catalog_identity>`, preserving the identity-named
  directory required by the Fee SDK reader. `{fees}` remains `/bench/fees`; a
  strategy catalog path must use `{fees}/{catalog_identity}`. No SDK validation
  is weakened.
- `mounts` exist so that paths inside `base_run_config` (derivative inputs,
  for example) resolve inside the container unchanged.

**Group config tokens.** These string tokens are substituted inside each group
`config`, recursively, before the run:

| Token | Becomes |
|---|---|
| `{context}` | `/bench/context` |
| `{snapshot_sha256}` | the context receipt's snapshot sha256 |
| `{fees}` | `/bench/fees` |
| `{catalog_identity}` | the fee catalog identity |
| `{output}` | the group's working directory under `OUT_DIR` |

Any other `{…}` token is rejected. The resolved spec is written to
`OUT_DIR/spec.resolved.json`, and that resolved spec is what the run is bound
to.

**Groups.**

- Group names are unique.
- `factory`, `reader` and every check must import inside the image. They are
  validated by import inside `inside` before the supervisor starts, and a
  failure there is an input error, not a run failure.
- Group configs are passed unmodified, apart from token substitution. The bench
  has no knowledge of any strategy's configuration schema.

**Examples.** `configs/bench/*.example.json` and its README contain placeholder
paths and identities only. Replace every `<...>` value with reviewed inputs.
Strategy-owned config schemas remain the strategy author's responsibility.

## 6. Checks

A check is a strategy-owned function, kept next to its reader:

```python
def check(*, run_directory: Path, group: str, output_directory: Path,
          context_directory: Path) -> dict
```

It returns a closed object `{"passed": bool, "details": {...}}`, with
JSON-serializable details. It must be offline and deterministic, and it must
never modify any input or output.

The bench treats an exception as a failed check, recording the exception's type
and message. Checks run after the group's reader succeeds.

Checks are where independent verification lives, for example recomputing every
fee leg from consumed quotes. The bench ships no strategy checks. A strategy
adds them in its own module.

## 7. `result.json` (closed)

- **Run identity:** `version: 1`, `label`, `status` (`SUCCESS`,
  `CHECKS_FAILED` or `FAILED`), `started_at`, `run_seconds`.
- **`image`:** `{id, built: bool, source_commit, source_dirty, dockerfile}`.
  `id` is `null` only when a failed build never established an image identity.
- **`context`:** `{snapshot_sha256, outcomes_provider}`.
- **`fee_catalog_identity`** or `null`.
- **`groups`:** one entry per group, `{name, factory, receipt, checks: {name:
  passed}}`.
- **`error`:** `null` or `{type, message, trace_tail}`.

The values are what the readers and checks returned. The bench computes no
economic figures of its own.

## 8. Documentation and agent routing

- Add a row to the `AGENTS.md` §4 routing table for "Local replay bench,
  fixture runs, run comparison", pointing to this document.
- No agent-specific skill is required. `AGENTS.md` is the single entry point
  shared by Codex and Claude.
- `replay/README.md` gets a short paragraph linking here.

## 9. Tests (offline, contract-shaped)

- Spec validation:
  - closed fields;
  - absolute and existing paths;
  - unique groups;
  - unknown tokens rejected;
  - recursive token substitution;
  - `attempts` forced to 1.
- An existing `OUT_DIR` is refused by every writing command.
- `prepare`:
  - an exported `UNIVERSE_BASE_URL` is required;
  - no dotenv file is read (prove it with a dotenv containing a different
    value);
  - an outcomes-unavailable context exits 3 unless allowed;
  - the warm-up is done through an injected fake client.
- `compare-context` on two small synthetic contexts:
  - an equal pair;
  - an `outcome_books`-only difference with and without `--expect-only`;
  - an evidence difference.
- `fees`:
  - the same spec gives the same identity;
  - a source hash mismatch is rejected;
  - a scale/rounding mismatch is rejected at validation.
- `compare-runs` on synthetic run directories:
  - identical;
  - added, removed and changed rows;
  - an `--expect` match and a mismatch.
- `inside`, with the supervisor and readers injected as fakes:
  - success;
  - reader failure → `FAILED`;
  - a failing check → `CHECKS_FAILED`;
  - a raising check → recorded failure;
  - the `result.json` schema.
- Docker orchestration, with the Docker client injected as a fake:
  - unique names;
  - read-only mounts;
  - cleanup on success, on failure and on `KeyboardInterrupt`;
  - removal limited to bench-created objects;
  - image kept with `--keep-image`;
  - cache pruned only with the flag.
- No test touches Docker, Redis, Universe or the network for real.

## 10. Acceptance (operator-run, outside CI)

1. Prepare a context for a retained bundle twice. `compare-context` reports
   them equal.
2. Run a coverage-only spec, then a spec with an economic strategy and its
   checks. Both succeed. A second run of the same spec reproduces the same
   semantic hashes, with different attempt IDs.
3. `compare-runs` on those two runs reports identical rows.
4. After each run, no bench containers, networks or images remain unless
   `--keep-image` was given.

## 11. Deferred

- Generating `base_run_config` from a bundle receipt and pins through
  `replay.jobs.stages.supervisor_config`. V1 takes an existing supervisor
  config.
- Multi-attempt and resume semantics.
- Running without Docker against locally built Rust binaries.
- Report or page generation from run outputs.
