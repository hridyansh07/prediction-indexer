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
   records them as unavailable, the context is prepared again (3 attempts, 60 s
   apart). If they never arrive, the bundle fails at stage `outcomes`.
4. Re-scope the fee catalog and, if enabled, prepare game state.
5. Preflight the economic-SDK groups. Groups that would refuse are recorded as
   `excluded_groups`, and the rest of the bundle still runs.
6. Run one bench replay with every group (`docs/LOCAL_REPLAY_BENCH_V1.md`).
   A failed strategy participant leaves `run/<attempt>/<group>/failure.json`.
7. Commit the results: the bench output (without its mounted `in/` inputs), plus
   the prepared `context/context.json` and `context/receipt.json` and, when game
   state is prepared, `game_state/game_state.json`. These label the event and its
   books downstream, and the work directory holding them is deleted.

A SQLite ledger (`ledger`) makes the run resumable. A failed stage is recorded
for that bundle and never stops the corpus. `--retry-failed` reruns failures.
The retired-bundle list is cached next to the ledger as `retired-bundles.json`.

The corpus key binds the groups, image source commit, materializer, runner
config, fees and limits, so results from different inputs never mix. A dirty
checkout changes the key and is refused unless you pass `--allow-dirty`. Commit
before a real run.

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

The container uses host networking, so `universe_base_url` can point at a
Universe tunnel on the host's loopback (for example
`ssh -f -N -L 18080:127.0.0.1:8080 <universe host>`).

Clean up afterwards. The first bundle builds the replay image, and later
bundles reuse it by ID through the ledger. Remove the images and build cache
once the corpus is done.
