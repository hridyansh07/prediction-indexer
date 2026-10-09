# Event Research backend V1

Offline verification and immutable event packs from finished bench runs. The
owner scope is recorded in [the backend contract](../../docs/specs/EVENT_RESEARCH_BACKEND_V1.md).
This package never executes strategies, contacts venue APIs, prepares contexts,
publishes a corpus or writes a current pointer. There is no new HTTP server or UI.

```bash
.venv/bin/python -m replay.research verify --runs /scratch/runs.json --verification /scratch/verify.json
.venv/bin/python -m replay.research build --runs /scratch/runs.json --verification /scratch/verify.json --output /scratch/new-build
.venv/bin/python -m replay.research audit /scratch/new-build
.venv/bin/python -m replay.research query /scratch/new-build episodes --limit 100
```

Run-list version 1 is `{runs_version:1, events:[{bundle_id, context, runs}]}`.
`runs` maps configured lens names to bench directories, not group output paths.
The closed configuration is [research.json](../../configs/research.json); group
names must match the actual runs. `archive:null` is explicitly offline and yields
`no_source`. To read an archive, supply the same five environment-variable-name
fields as the game-state config; only the generic archive factory handles them.
Never put credentials in the run list or config.

The verifier independently streams profile partitions, nonduplicate trades and
optional depth replay, and recomputes fill fees, payout, edge stops and kill
boundaries. Hashes prove bytes, not correctness of vendor claims or truthful
capture. Verification does not certify actionability, profit or economic truth.
It does not import the collector, runtime or their result readers. Existing
layout 2 and new layout 3 are accepted; legacy complement layout 1 is not a
research input. Non-fill episodes are preserved, without invented fill metrics.

`verify.json` binds the config/run list, context and receipt, bench result,
supervisor/attempt/group evidence, content/summary/stream files and every fee
catalogue member. Selected event-keyed game-state archive bytes are also bound.
Build reruns verification and compares the entire report, then rehashes inputs
before committing. A forged pass report is not authority. A missing lens is
`not_run`, rejected evidence is `failed`, and no verified profile means no pack.
Verified episodes remain in the backend tables even when a profile is missing
or failed; the profile prerequisite applies only to chart packs. Selected
game-state archive objects are fully reverified before the build receipt commits.

## Local commits and contracts

`receipt.json` is the build's final local commit marker. A partial build without
it must not be served. Pack manifests are written after their files. Files are
immutable: existing destinations fail, including existing `.open` files. Failure
leaves uncommitted output for diagnosis; use a new disposable destination to
retry. No command deletes old artifacts or inputs.

JSON is sorted-key UTF-8 plus LF, uncompressed locally; decoded and `stored`
identities are equal. HTTP/gzip upload and conditional current-pointer updates
belong to excluded publication work, not this receipt. Never use a filename,
successful download or a Parquet footer as a commit marker.

The pack has overview, 1-second and raw tiles, availability, source-time game
windows and full upstream episode overlays. Raw tiles carry `state_at_start`
and scope boundaries; state never extends through exits/gaps. Kalshi's chart
book is the Yes book with a projected ask. `claims` is scope-indexed; twins use
`effective_claim_id` (space plus effective payout set), not base `claim_id`.
Negated claims stay distinct. Titles absent upstream are null, not synthesized.
Game facts never invent a game clock; only settlement is exact, and
`known_at_ns` is the calibrated window end.

Two backend tables are written directly beneath the build root. Closed Arrow
schemas and `event_research_table_version=1` metadata are in `tables.py`:
`events.parquet` has the spec event fields plus pack/error and nested per-lens
state/error/episode counts; `episodes.parquet` has exactly the spec episode
fields. Large integers, native scale-36 fill values and exact decimal durations
are strings. Quantity and cost metrics state the native plan scales. Duplicate
counts use overlapping half-open intervals sharing an actual native book side,
not projected chart book indexes. There are no labels or new classifications,
summary histograms, calibration/viability reports, release manifests or corpus
versioning. Parquet uses no internal compression and is never HTTP gzipped.

`download_table` consumes archive `open_verified` fully into an exclusive
temporary file before committing it. Queries verify the complete file before
Arrow opens it and recheck after scanning. They use fixed event/lens predicates,
not SQL; no range reads. Static serving/deployment remains an operator action.

## Bounds and operations

Fail closed, never truncate: 64 written books, 128 scopes, seven capture days;
32 MiB JSON files and cut groups; 4 GiB disposable SQLite spool; 1 GiB retained
contexts; 128 MiB entity/episode retained state; 200,000 episodes per lens;
2,048 concurrent episodes per native side and one million duplicate pairs.
Parquet caps are 256 MiB stored/decoded, one million rows and 2,048 row groups.
Queries return at most 500 rows/1 MiB, in 128-row/4 MiB batches. Oversized evidence
needs an explicit new contract, not smaller hidden samples. Allocate disposable
disk for the spool and outputs; do not use retained data as scratch.

No Universe/Replay database migration, auth change, prepared-context identity
change or production mutation is needed. Scheduled game state alone creates
its versioned, rebuildable append-only ledger. Rollback means stop scheduling
the new job and stop serving new derived builds; retain evidence and ledgers.
Cloud/backfill, real supervisor execution of same-bundle fill runs and any HTTP
publication acceptance remain operator tasks. Tests are offline hand-authored
contracts under `replay/tests/test_research*.py`.
