# Corpus runner

`python -m replay.corpus {run,status,verify} SPEC` replays every retired bundle
through every strategy group in a corpus spec. It keeps one committed result per
bundle under `results_root/<corpus key[:16]>/<bundle>/`, and writes
`corpus_receipt.json` last as the commit marker.

## Per bundle

1. Resolve the bundle's history from Universe.
2. Materialize from the read-only `canonical_store` through a disposable overlay.
   The canonical store is never written.
3. Prepare the context. The outcomes must come from Universe. If preparation
   records them as unavailable, the context is prepared again (3 attempts, with
   5 s then 15 s waits). If they never arrive, the bundle fails at stage `outcomes`.
4. Re-scope the fee catalog and, if enabled, prepare game state.
5. Preflight the economic-SDK groups. Groups that would refuse are recorded as
   `excluded_groups`, and the rest of the bundle still runs.
6. Run one bench replay with every group (`docs/LOCAL_REPLAY_BENCH_V1.md`).
   A failed strategy participant leaves `run/<attempt>/<group>/failure.json`.
7. Commit the results: the bench output (without its mounted `in/` inputs), plus
   the prepared `context/context.json` and `context/receipt.json` and, when game
   state is prepared, `game_state/game_state.json`. These label the event and its
   books downstream. By default the work directory is deleted unconditionally;
   the opt-in private-attempt mode below retains unsuccessful work instead.

A SQLite ledger (`ledger`) makes the run resumable. A failed stage is recorded
for that bundle and never stops the corpus. `--retry-failed` reruns failures.
The retired-bundle list is cached next to the ledger as `retired-bundles.json`.

The corpus key binds the groups, image source commit, materializer, runner
config, fees and limits, so results from different inputs never mix. A dirty
checkout changes the key and is refused unless you pass `--allow-dirty`. Commit
before a real run.

## Verification and deep audits

Plain `python -m replay.corpus verify SPEC` remains the rehash-only reader of
`corpus_receipt.json`. For full semantic validation, use
`python -m replay.corpus verify SPEC --deep --bundle ID --bundle OTHER_ID`, or
`--deep --sample N --seed S`. Sampling uses sorted committed results and a local
random generator; the seed defaults to **0**. The two subset selectors cannot
be combined. Without either selector, deep verification selects all committed
results under this spec's corpus key.

Every selected bundle is first rehashed, then audited in its recorded bench
image (`image.id`, or `result.json.image.id` for reused images). The original
configured package readers run unchanged, each group is reported, and one
bundle/group failure does not stop the rest. The committed run and context,
matching spec fee catalog, committed game state when present, and cached
derivatives by address mount read-only at their original `/bench` paths. The
audit has no network, pulls no image, and never rebuilds, fetches, rematerializes,
updates cache bookkeeping or writes a ledger/result.

Resolve prerequisites before launching: an absent image, evicted/corrupt
derivative or fee catalog identity mismatch reports
`<bundle> deep: missing prerequisite <kind> <id>` and skips that bundle; this
does not invalidate its result. Context/game-state prerequisites are also
checked. Exit status is **0** for a complete passing audit, **1** for any result
failure, or **2** when only missing prerequisites prevent completion. The spec
and source/tool inputs still identify the corpus key; use the original inputs
to select an older corpus.

## Optional persistent derivative cache

Add this field to the corpus spec (version 1); omitting it preserves the
disposable per-bundle path:

```json
"derivative_cache": {
  "root": "/srv/replay/derivative-cache",
  "max_bytes": 21474836480
}
```

Use a dedicated writable local directory, disjoint from `canonical_store`,
`results_root` and `work_root`, on a filesystem supporting flock, atomic rename
and directory fsync. The cache need not share a filesystem with `work_root`.
In Docker the cache path must also be visible to the orchestrator and host
daemon (normally under the mounted `CORPUS_ROOT`). `max_bytes` is a positive
integer, in bytes; this example allows 20 GiB of immutable derivative files
plus immutable index JSON. Mutable lease, lock and touch bookkeeping is excluded
from this cap.

`index/<key hash>.json` maps exact window bounds, canonical receipt SHA-256 and
the complete `materialize_range --describe` Producer to a derivative address
and receipt SHA-256. Entries have closed `index_version: 1` documents, published
by file fsync, atomic rename and directory fsync. Derivatives are installed at
`derivatives/<address>/` using the existing stage-and-rename installer. Every
hit re-hashes the receipt and all four receipted files and checks source/producer
bindings. Invalid entries are dropped and rebuilt; a corrupted directory still
used by another live lease is not replaced, and the rebuild stays private.

Every bundle still preflights every canonical receipt. Only missed windows are
restored through the existing Python archive-identity checks, then materialized
in contiguous runs. A nonempty three-window test proves the middle window's
address, receipt pin and all five files are identical when built alone. The
materializer creates a fresh normalizer per exact window; there is no range
fallback for the current Producer.

A bundle holds `leases/<pid>-<unique>.json` through preparation, bench and result
commit, removing it in `finally`. Each lease inode is flocked: stale dead-process
leases are reclaimed without relying on PIDs shared across container namespaces.
Touch files record entry LRU. Under exclusive `root/lock`, startup reclaims crash
stages and trims old unleased entries; after misses have materialized, admission
counts exact derivative and index bytes, evicts unleased LRU entries and
installs atomically. No eviction is performed by that bundle during its bench;
other lanes may evict only addresses outside live leases.

If an entry cannot fit, the bundle continues with a verified private derivative
under its work directory. The cache remains within `max_bytes` after admission;
live entries are never evicted to make room. All-hit bundles use the cache's
derivative directory read-only at `/bench/in/derivatives`; mixed bundles also
mount private derivatives read-only at `/bench/in/private-derivatives`. There are
no symlinks, hardlinks or per-bundle copies of cache hits.

Ledger `detail.derivative_cache` records `hits`, `misses` and `cache_bypass`
windows/reasons (`max_bytes`, `live_leases`, `corrupt_derivative_in_live_lease`),
including failures after staging. Diagnostics do not enter result receipts or
the corpus semantic key. Cached/uncached bundle receipts, pins and derivative
bytes remain identical. The disposable verifying sink need not re-stream
already hash-verified derivatives; durable object stores still receive them.

## Discard new private materialized inputs after success (default off)

```bash
python -m replay.corpus run SPEC --discard-materialized-inputs
```

This **run-only operational option** bypasses the shared derivative cache, even
when `derivative_cache` is configured. There is no cache construction, lease,
lookup, admission, eviction or writable-cache probe. Configuration/path safety
validation still applies. The configured cache location remains available for
later audit lookup; the option does not change the corpus-spec schema, semantic
key, economic policy, input bytes, declared pins or result receipt format.
Without the flag the existing cache and work-cleanup behavior is unchanged.

**This does not reclaim any pre-existing shared cache.** It never removes
canonical/raw evidence or committed results, and introduces no shared-cache
reaper. Production jobs' never-delete-derivatives rule remains unchanged.
`--retry-failed` retains its existing result-replacement semantics; it does not
authorize cleanup of an older private attempt.

Each attempt exclusively creates a fresh mode-0700 directory at
`work_root/.corpus-private-attempts:/attempt-<random>/`. The reserved namespace
cannot be a valid bundle ID, and bundle IDs are validated before legacy work or
retry deletion. The runner never adopts, reuses, lists or sweeps earlier
attempts. Use a dedicated absolute work root, disjoint from canonical/raw data,
results, the shared cache and configured input/config files. Symlinked root
components, overlapping roots (including existing bind aliases), and nesting
inside an older private namespace are refused. Before removal, the runner
revalidates the roots and checks the namespace and attempt inode identities.
Content removal is anchored to the attempt descriptor held since exclusive
creation, not a later pathname lookup. Replacing the attempt name after its
identity check cannot redirect recursive deletion to the replacement. The root
name is rechecked and removed only with nonrecursive `rmdir`; this last check
is not atomic, but a nonempty replacement cannot lose contents. Linux filesystem
support for fd-relative, symlink-resistant operations and directory fsync is
required.

Private work is eligible for deletion only after **all** of:

1. The bench returns **exit 0** with **`SUCCESS`**, after its consumer cleanup.
   A `SUCCESS` artifact with exit 3 (for example, Docker cleanup failure) is not
   eligible and is recorded as failed in this mode.
2. Result publication copies and fsyncs every output and kept context/game file.
3. Directory entries for the complete output tree and every newly created
   ancestor are fsynced **before** writing the unchanged receipt bytes; the
   receipt file and then its containing directory are fsynced before commit
   returns. A visible receipt alone is not this durability gate.

A stage failure, nonzero/checks-failed result, interrupt, copy/commit/fsync
failure or uncertain consumer shutdown retains the attempt. SIGKILL cannot
report a final status, so the path is printed and flushed before consumers
start. Retries and other workers always use new directories. Retained attempts
need separate operator review, including consumer shutdown; **this flag is not
a general cleanup tool**. A private cleanup error is visible but does not
invalidate an already durable committed result or replace an earlier failure.
A cleanup error/interrupt mid-removal can leave a partially removed directory;
its path and diagnostic remain visible. No result evidence is edited afterward.

Logs report the operational mode and attempt path at creation and the final
`retained`, `discarded` or `cleanup_failed` outcome. Completed/failed ledger
rows also retain `detail.materialized_inputs`, including bench status/exit,
the durable-commit gate when passed, and any cleanup error. If the initial path
log cannot be written/flushed, stage `work_log` fails before consumers start,
retaining work and closing its descriptors. Final logging is best-effort:
`log_error` remains in diagnostics/ledger without replacing the original failure
or invalidating a committed success. Interrupted rows keep the existing
`running` semantics; consult the initial flushed log for their path.

Ordinary receipt verification remains possible. **Future deep audits may be
INCOMPLETE** until exact matching derivative inputs are available elsewhere or
restored to the configured lookup location. Deep audit currently does **not**
rebuild derivatives. Canonical evidence plus the exact producer may allow
reconstruction; that is not immediate audit readiness. Bypassing the cache can
increase rematerialization CPU/I/O and transient space before successful work
is discarded. Results and canonical/raw evidence continue to occupy space.

## Running everything in Docker

`compose.corpus.yaml` runs the orchestrator in a container
(`docker/corpus-runner.Dockerfile`). That container holds Python, the Linux
materializer, git and the Docker CLI. It runs the mounted checkout, and each
bundle's bench containers are started through the host's Docker socket.

The host daemon resolves bind mounts, so the data root and the checkout are
mounted at the same path inside and outside. Every path in the spec is a host
path, and must sit under `CORPUS_ROOT` or `CORPUS_REPO`. The spec's
`materializer` is `/usr/local/bin/materialize_range`, inside the image.

```bash
export CORPUS_ROOT=$HOME/pi-data CORPUS_REPO=$HOME/prediction-indexer \
       CORPUS_UID=$(id -u) CORPUS_GID=$(id -g) \
       DOCKER_GID=$(getent group docker | cut -d: -f3)
cd "$CORPUS_REPO"
docker compose -f compose.corpus.yaml build
docker compose -f compose.corpus.yaml run --rm corpus run    "$CORPUS_ROOT/corpus/smoke-spec.json"
docker compose -f compose.corpus.yaml run --rm corpus status "$CORPUS_ROOT/corpus/smoke-spec.json"
docker compose -f compose.corpus.yaml run --rm corpus verify "$CORPUS_ROOT/corpus/smoke-spec.json"
```

### Stopping a run

Killing only the compose client (Ctrl-C on `docker compose run`) does not stop the
run: the orchestrator keeps going inside the container. Run with a fixed name and
signal the container from another shell:

```bash
docker compose -f compose.corpus.yaml run --rm --name corpus-run corpus run "$CORPUS_ROOT/corpus/smoke-spec.json"
docker kill --signal=INT corpus-run   # clean stop
docker stop corpus-run                # SIGINT, then SIGKILL after the 2 minute grace period
```

The compose file sets `init: true` (tini forwards the signal to the orchestrator),
`stop_signal: SIGINT` and `stop_grace_period: 2m`. On SIGINT the current bundle's
bench removes its containers, network and image, and writes `result.json` and
`orchestration.json`. The corpus then stops with exit 130. That bundle's ledger
row stays `running` and no result or receipt is committed for it, so the next run
redoes it (`attempts` is incremented). By default the bundle's scratch work
directory is deleted, including the interrupted bench output; the redo recreates
it from nothing. With `--discard-materialized-inputs`, interrupted private work
is retained and the redo creates a separate fresh attempt instead.
A SIGKILL (grace period exceeded) skips cleanup: remove leftover `bench-corpus-*`
containers and networks by hand, then rerun.

The container uses host networking, so `universe_base_url` can point at a
Universe tunnel on the host's loopback (for example
`ssh -f -N -L 18080:127.0.0.1:8080 <universe host>`).

Clean up afterwards. The first bundle builds the replay image, and later
bundles reuse it by ID through the ledger. Remove the images and build cache
once the corpus is done.
