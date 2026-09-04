# Event Universe — API response limit blocks ingestion of retained bundles

**Status:** open, root cause identified and verified.
**Affects:** `v0.12.0` (schema v3) and `v0.13.0` (schema v5, current master `d0e93db`).
**Symptom:** backfill commits a prefix of runs and then commits nothing more,
while continuing to download and burn CPU.

> Supersedes `UNIVERSE_V3_SYNC_LIVELOCK.md`, which was **wrong**. There is no
> livelock, no loop, and no redundant re-fetching. That document blamed an
> `origin_context` / `known_manifest` identity mismatch on the strength of
> circumstantial evidence (a frozen row count plus climbing network counters).
> Instrumenting the actual call path disproved it: the failing run makes
> **exactly one** `_ingest_manifest` call and fails in 0.0 s.

## Root cause

`origin_context()` — on the **ingestion** path — returns its result by calling
`_context()`, which is the **API read** path and enforces HTTP response bounds.

`universe/store.py:1015`, inside `origin_context()`:

```python
return self._context(connection, row["context_sha256"])
```

`universe/store.py:2026`, inside `_context()`:

```python
_ensure_detail_rows(
    (participants, events, markets, targets, assets, relationships),
    "bundle context",
)
```

and `universe/store.py:2636`:

```python
def _ensure_detail_rows(groups: tuple[list[Any], ...], label: str) -> None:
    if any(len(rows) > DETAIL_ROW_LIMIT for rows in groups):
        raise DetailTooLarge(f"{label} exceeds the child-row limit")
```

`DETAIL_ROW_LIMIT = 1000` (`universe/store.py:27`) and the byte guard just below
it (`EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES`) exist to keep a single HTTP response
inside the 1.75 MB budget — `universe/api.py:249` catches `DetailTooLarge` and
turns it into an error response. They are presentation limits.

Ingestion inherits them. A bundle context that is too large **to serve in one
response** therefore becomes impossible **to store as a retained origin**, even
though the data is valid and already committed.

## Verified chain

Traced against the real archive with `_ingest_manifest` instrumented:

```
t=   0.0s depth=0 call#1 20260825T180003.646497Z dep=False
distinct keys: 1   total calls: 1
failures: ["targeter-v2/runs/date=2026-08-25/run=20260825T180003.646497Z/run_manifest.json:
            DetailTooLarge: bundle context exceeds the child-row limit"]
```

Child-row counts across all 19 contexts in the reproduction database:

| table | max rows | over 1000 |
|---|---|---|
| context_participants | 2 | 0 |
| context_events | 6 | 0 |
| context_markets | 38 | 0 |
| context_targets | 36 | 0 |
| context_target_assets | 67 | 0 |
| **context_relationships** | **1081** | **1** |

Relationships are the only dimension anywhere near the limit — everything else
peaks at 67. This is the same fat dimension that made the old 25.6 MB cadence
payload 96% relationship data.

The offending context, `154def5728502790`, was created by run
`20260825T175003.607748Z` — **the last run that committed**. Sequence:

1. Run `17:50:03` originates bundle `bundle_a61879cd1511f2569e59b93b` with 1,081
   relationships. It ingests fine: a `complete` occurrence takes
   `_complete_context(row)` and never touches `origin_context`.
2. Run `18:00:03` **retains** that bundle. `_resolve_origin_context`
   (`universe/sync.py:647`) calls `origin_context` → `_context` →
   `_ensure_detail_rows` → `DetailTooLarge`.
3. `_ingest_direct` catches it, records a durable sync failure, and moves on.
4. Every later run that retains the same bundle fails identically.

So the backfill is not hung. It is walking the rest of the range, downloading
each run's catalogues, failing on every one that retains this bundle, and
committing nothing. That fully explains what looked like a stall: a frozen run
count, steadily climbing network counters, high CPU, and no exception escaping
to stdout — because failures are recorded, not raised.

## Why it looked like something else

- Failures are durable and are only summarised at the **end** of the job
  (`run_backfill.py` prints `backfill_summary`, plus a per-batch `progress`
  record). `BACKFILL_BATCH_SIZE = 100` and Aug 25 holds ~74 runs, so the whole
  day is a single batch and nothing is printed until it finishes.
- Both diagnostic runs sent stdout somewhere unread — once through `tail` in a
  backgrounded call, once into a botched `> /dev/null` redirect. The answer was
  in the summary line the whole time.
- `pending_failures` is surfaced on `/healthz` (`sync.pending_failures`) and
  would have shown this immediately on a deployed instance.

## Fix direction

`_resolve_origin_context` only uses three fields from the returned context
(`universe/sync.py:566-580`): `activation_at`, `capture_start_at`, and
`targets`. It never reads `relationships` — the field that trips the limit.

Options, cheapest first:

1. **Compare identity, not the materialised record.** The occurrence row already
   carries `context_sha256`; verifying the retained bundle against that hash
   avoids rebuilding the full context altogether.
2. **Split the accessor.** Give `_context()` a `bounded: bool = True` parameter
   and have `origin_context()` pass `bounded=False`. Presentation limits stay on
   the API path where they belong.
3. Raising `DETAIL_ROW_LIMIT` is **not** a fix — it moves the wall rather than
   removing it, and the API genuinely does need a bound.

Whichever is chosen, the invariant worth stating in code is that *ingestion must
never fail because a record would be awkward to serve*.

## Regression test

The existing coverage at `tests/test_event_universe_store.py:1014`
(`test_retained_selection_resolves_origin_outside_requested_range`) uses small
fixtures and passes. The missing case is a size one:

- originate a bundle whose context has `DETAIL_ROW_LIMIT + 1` relationships;
- ingest a later run that **retains** that bundle;
- assert the later run ingests successfully;
- separately assert the API still refuses to serve that context, so the
  presentation bound is not weakened by the fix.

## Reproduction

`/srv/event-universe/scratch/scratch.sqlite3` on universe-server holds 38
committed runs and stops exactly at the failing one.

```bash
docker run --rm --env-file /opt/event-universe/.env --user 1000:1003 \
  -v /srv/event-universe/scratch:/var/lib/event-universe \
  -v /tmp/scratch_universe.json:/etc/prediction-indexer/event_universe.json:ro \
  -v /tmp/trace.py:/tmp/trace.py:ro \
  hridyansh07/prediction-indexer-universe:v0.13.0 python -u /tmp/trace.py
```

`/tmp/trace.py` wraps `UniverseSync._ingest_manifest` to print every call with
its recursion depth, then ingests the single failing run.

Note `begin_event_identity_backfill` refuses to resume a checkpointed range with
different bounds (`canonical event-identity backfill must resume with its
original generated-time range`), so a narrowed range cannot be used against an
existing scratch database — use the original bounds or a fresh file.

## Deployment state

Unchanged and healthy: universe-server runs `v0.11.0` on schema v2 with the live
database intact and the 10-minute sync cron running. `v0.13.0` is built, pushed
(`sha256:5d397d3076b31ff36170704506f8b591e071139afb624a67fe876c2491379de2`) and
pulled on the host, but **not deployed** — schema v5 requires a fresh database,
so cutting over before this fix would leave a near-empty UI.
