# Event Universe — backfill write amplification from `relation_observations`

**Status:** open, root cause measured. Blocks the schema v5 rollout.
**Measured on:** universe-server (`e2-small`, 2 GB RAM, 20 GB data volume), image `v0.14.0`,
against the real GCS archive, 2026-09-04.
**Impact:** a full-history backfill of 1,202 runs projects to **~2–4 days**. Target is 1–2 hours.

## The number to fix

```
relation_observations   3,333,919 rows over 158 runs   =  21,101 rows per run
relations                  39,025 rows                 =     247 new per run
relation_members           78,050 rows                 =     494 per run
context_relationships       9,351 rows                 =      59 per run
event_observations         15,301 rows                 =      98 per run
venue_markets               2,564 rows                 =      16 per run
```

Every run inserts **21,101** rows into `relation_observations`, of which only ~247
correspond to a relation not already known. The other ~20,850 re-record relations
the database already has, once per run, forever.

Projected at the full 1,202-run history:

| | at 158 runs | projected at 1,202 |
|---|---|---|
| `relation_observations` rows | 3.3 M | **~25.4 M** |
| database size | 1.75 GB | ~13.3 GB |

Every other table is three orders of magnitude smaller. This one table is the
entire cost.

## Why it is slow, precisely

`universe/schema/schema.sql:372-384`:

```sql
CREATE TABLE relation_observations (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    relation_id INTEGER NOT NULL REFERENCES relations(relation_id),
    bundle_id TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    scope TEXT NOT NULL,
    coverage TEXT NOT NULL,
    PRIMARY KEY(run_id, relation_id, bundle_id)
) STRICT;
CREATE INDEX relation_observations_relation
    ON relation_observations(relation_id, event_id, run_id);
CREATE INDEX relation_observations_event
    ON relation_observations(event_id, relation_id, run_id);
```

The primary key leads with `run_id`, which advances monotonically, so PK inserts
append to the right edge of the B-tree — cheap. **Both secondary indexes lead
with something else** (`relation_id`, `event_id`), so each insert lands at a
random offset in a B-tree spread across the whole file.

```
21,101 inserts/run × 2 scattered indexes = ~42,000 random B-tree insertions
× 4 KB page dirtied per insertion         ≈ 170 MB of dirty pages per run
```

Measured WAL growth was **~190 MB for a single run** — the arithmetic matches.
`PRAGMA foreign_keys = ON` adds three more index probes per row (`run_id`,
`relation_id`, `event_id`), so the per-run statement count is roughly:

```
21,101 × (1 INSERT-or-ignore + 2 SELECT + 1 INSERT + 3 FK probes) ≈ 126,000 statements
```

Once the indexes exceed the page cache — which happens early on a 2 GB machine —
essentially every insertion is a fresh page read plus a fresh page write. That is
why the rate **decelerates** rather than holding steady:

```
first 37 min    103 runs    ~22 s/run
later           48 runs    ~131 s/run
at run ~156                ~300 s/run    (measured directly, 1 run per 300 s)
```

Aggregate over the 156-run run: **85.7 GB written, 1.82 GB read**, ~549 MB
written per run for ~11 MB of net database growth. CPU sat at **5%** with
**54–74% iowait**. It is disk-write-bound, start to finish.

## What is *not* the problem

Ruled out by measurement, so they do not get investigated again:

- **Network / fetch throughput.** 242 MB over 2.4 h (~28 KB/s). Prefetching or
  pipelining run downloads would hide a rounding error. CPU 5% means zstd decode
  is not a factor either.
- **The other per-entity UPDATE churn.** `_insert_market_projection` does
  SELECT-then-UPDATE per entity to maintain `first_seen_run_id`/`last_seen_run_id`,
  which looks expensive — but it is only ~98 events and ~16 venue markets per run.
  Negligible.
- **WAL checkpoint misbehaviour.** The WAL did grow to 303 MB and was never reset
  (a `wal_checkpoint(TRUNCATE)` returned `(0, 0, 0)` — zero un-checkpointed pages
  in a 303 MB file). That is real, but it is a *symptom*: the WAL is large because
  42,000 scattered writes dirty that many pages.
- **SQLite pragmas.** Tested directly: `synchronous = NORMAL`,
  `wal_autocheckpoint = 20000`, a 64 MB page cache, and an explicit
  `wal_checkpoint(TRUNCATE)` on every batch boundary. Result: **1 run in 7.5
  minutes — no improvement over baseline.** Tuning durability does not help when
  the cost is the number of pages being dirtied.

## Fix direction

The cheap infrastructure levers are worth roughly a constant factor and do not
change the shape of the curve. The row count does. In rough order of leverage:

**1. Stop storing one row per relation per run.** This is the fix. ~20,850 of the
21,101 rows written each run say nothing new. Options, all schema changes:

- *Interval model.* Replace per-run rows with `first_seen_run_id` /
  `last_seen_run_id` per `(relation_id, bundle_id, event_id, scope, coverage)`,
  updated only when the tuple's semantic content changes. 25.4 M rows collapses
  toward the ~39 K distinct relations, and the write becomes an update to a row
  that is almost always already in cache.
- *Change-only observations.* Keep the observation table append-only, but only
  append when a relation's `scope`/`coverage` differs from its last recorded
  observation. Preserves the full history of *changes* without recording
  1,200 identical restatements.

Either preserves the question the table exists to answer — "which runs saw this
relation, and in what state" — while removing the redundancy. Deciding which
depends on whether a consumer needs "was this relation present in run N" for an
arbitrary N, or only "when did this relation appear, change, and disappear".

**2. If the per-run row must stay, defer the secondary indexes during bulk load.**
Drop `relation_observations_relation` and `relation_observations_event` before a
backfill and rebuild them once at the end, converting ~42,000 random insertions
per run into one sequential sort-build. Note `UniverseStore._validate_schema`
compares live schema against `schema.sql` and will reject a database with missing
indexes, so this needs an explicit sanctioned bulk-load mode rather than a hack.

**3. Infrastructure, only after the above.** The 20 GB volume gives a low IOPS
ceiling (GCP scales IOPS with provisioned capacity), and `e2-small` has 2 GB RAM
so the index working set cannot be cached. Both are worth revisiting — but
raising the ceiling under a workload that writes 549 MB per run to grow 11 MB is
paying for the symptom.

## How to measure a fix

Rate must be measured *late*, not averaged. The first ~100 runs are fast because
the indexes still fit in cache; the pathology only appears once they do not.

```bash
# marginal rate at the current database size
sudo python3 -c "
import sqlite3
c=sqlite3.connect('file:/srv/event-universe/build/universe-v5.sqlite3?mode=ro', uri=True)
print(c.execute('SELECT COUNT(*) FROM universe_run_projections').fetchone()[0])"
sleep 300   # repeat, take the delta

# write amplification for the run in flight
docker stats --no-stream --format "CPU={{.CPUPerc}} BLOCK={{.BlockIO}}" <container>
ls -la /srv/event-universe/build/universe-v5.sqlite3-wal
```

Targets for a fix to be considered working:

- `relation_observations` rows per run in the low hundreds, not ~21,000;
- WAL growth per run in single-digit MB, not ~190 MB;
- marginal seconds-per-run **flat** across the range rather than climbing;
- full 1,202-run backfill inside a couple of hours.

## Current state

Nothing is deployed. universe-server serves `v0.11.0` on schema v2 with the live
database intact and the 10-minute sync cron running normally. The partial v5
build (`/srv/event-universe/build/universe-v5.sqlite3`, 158 runs) resumes from its
batch checkpoint, and `v0.14.0`
(`sha256:94a63606ed667341d2840f89e819be17d6b1af95eb7ec9adb3ece4e9068e306b`) is on
Docker Hub and pulled on the host.

The separate ingestion bug this backfill first hit — an API response limit applied
to the write path — is fixed and on master as `a32b127`; see
[`UNIVERSE_ORIGIN_CONTEXT_DETAIL_LIMIT.md`](./UNIVERSE_ORIGIN_CONTEXT_DETAIL_LIMIT.md).
That fix is confirmed working: 75/75 runs of 2026-08-25 ingested with zero
failures. This document is only about throughput.
