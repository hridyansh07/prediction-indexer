# Event Universe — relation storage remediation plan

**Status:** proposed. Not implemented.
**Owns:** the fix for
[`UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md`](./UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md),
plus two further defects of the same root shape found while reading the code for
this plan.
**Blocks:** the schema v5 rollout. universe-server still serves `v0.11.0` on
schema v2; nothing is deployed on v5.

## 1. What this plan covers

`UNIVERSE_ORIGIN_CONTEXT_DETAIL_LIMIT.md` is **closed**. Its fix is on master as
`a32b127` and is visible in the code today — `_insert_context` and
`origin_context` both call `self._context(connection, context_sha256,
bounded=False)`, so the presentation bound no longer reaches the write path. No
work here.

`UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md` is **open** and is what
this plan fixes. Its measurement stands: `relation_observations` takes 21,101
rows per run to record ~247 genuinely new relations, and the two secondary
indexes turn that into ~42,000 random B-tree insertions and ~190 MB of WAL per
run. A 1,202-run backfill projects to 2–4 days against a 1–2 hour target.

## 2. Three defects, one root cause

Reading the code to design the throughput fix surfaced two more defects. All
three are the same mistake in different places: **a per-run row for a value that
almost never changes per run.**

### 2.1 Write amplification (documented, measured)

Per §1. This is the backfill blocker.

### 2.2 `GET /v1/targeter/runs/<run_id>` cannot serve a complete run

`run_detail` (`universe/store.py:1440`) selects a run's relations with
`LIMIT DETAIL_ROW_LIMIT + 1` and then passes them through `_ensure_detail_rows`,
which raises `DetailTooLarge` above `DETAIL_ROW_LIMIT = 1000`
(`universe/store.py:27`). The measured relation count for a complete run is
**21,101**. Every complete run therefore returns an error response, and
`universe/api.py:249` turns it into an API error rather than a page of data.

This is not a throughput problem — it is a *correctness* problem that ships with
v5 and takes out the run-detail page in the UI. It is invisible in tests because
the fixture at `tests/test_event_universe_store.py:1431` asserts
`len(detail["relations"]) == 1`.

Note this is exactly the failure mode of the already-closed
`UNIVERSE_ORIGIN_CONTEXT_DETAIL_LIMIT.md` bug: a 1,000-row presentation bound
meeting a relationship dimension that is three orders of magnitude larger than
every other dimension. That bug removed the bound from the write path. This one
is on the read path, where the bound genuinely belongs — so the response shape
has to change instead.

### 2.3 `GET /v1/relations/<relation_id>` degrades to an error over time

`relation_detail` (`universe/store.py:1749`) returns one row per observation,
also capped at `DETAIL_ROW_LIMIT`. A relation is observed once per run for as
long as its markets stay candidates. At the archive's ~75 runs/day cadence, any
relation that survives ~13 days crosses 1,000 observations and the endpoint
starts failing permanently for that relation. Long-dated markets reach this.

The current model has no way out of this: the row count is a function of
calendar time, not of anything a client cares about.

## 3. Findings that constrain the design

These came from the code and change what the fix can be. The write-amplification
document leaves the design choice open pending exactly this information.

**(a) Ingestion is routinely out of generated-time order.** `sync()` ingests due
retry failures first (`universe/sync.py:168`), before walking the date range,
and bootstrap walks newest-first (`universe/sync.py:188`). Any model that
records "the previous run" or assumes append-at-the-right-edge must stay correct
when a run lands *between* two already-ingested runs. Backfill itself
(`backfill_range`) is in order, so this is an incremental-sync concern, but it
is a routine one — every durable sync failure that later succeeds is an
out-of-order insert.

**(b) The write-amplification document's "interval model" option, taken
literally, is wrong.** Plain `first_seen_run_id` / `last_seen_run_id` per
relation cannot answer "was this relation present in run N", because it cannot
represent a gap. `run_detail` needs exactly that question answered for arbitrary
N. The document poses this as an open question — "whether a consumer needs 'was
this relation present in run N' for an arbitrary N, or only when it appeared,
changed, and disappeared". **The answer, from `universe/store.py:1440`, is yes.**
The model must therefore be *segments* — one row per contiguous presence-and-state
span — not a single open interval.

**(c) Three of the four readers only want the newest observation.**
`event_detail` (`universe/store.py:1577`) and `market_detail`
(`universe/store.py:1688`) both carry a correlated `NOT EXISTS (SELECT ... FROM
relation_observations newer ...)` subquery whose only purpose is to discard every
observation but the last. That subquery is itself O(observations per relation),
so those two endpoints get slower for the same reason the writes do. A segment
model makes them a small scan instead.

**(d) The schema change is free right now.** `UniverseStore.initialize`
(`universe/store.py:47`) has no migration path: a version mismatch raises
`EvidenceConflict` and demands a fresh database, and `_validate_schema`
(`universe/store.py:86`) rejects any live schema that differs from `schema.sql`
byte-for-byte. Since the v5 database was never deployed and the v5 build is a
throwaway, a v6 schema costs nothing beyond rebuilding — which we have to do
anyway. This window closes the moment v5 ships.

**(e) Changing storage does not invalidate run idempotence.**
`_market_projection_identity` (`universe/store.py:2502`) hashes the *projection
document*, not the stored rows, and `MARKET_PROJECTION_VERSION` describes the
projection shape. The projection is unchanged by this plan, so
`universe_run_projections.projection_sha256` and `projection_row_count` stay
comparable across the rebuild and `MARKET_PROJECTION_VERSION` stays at 3.

**(f) `ON DELETE CASCADE` on `relation_observations.run_id` is decorative.**
There is no run-deletion path anywhere in `universe/`, and
`EVENT_UNIVERSE_STORE_V1.md` §7 states that Universe never prunes runs. A
segment spans many runs, so per-run cascade has no coherent meaning under the new
model. The replacement table should not carry it, and the plan should say so
rather than copying it forward.

## 4. Phase 0 — measure before building (gate)

The whole fix rests on one unverified assumption: that a relation's
`(bundle_id, event_id, scope, coverage)` is stable across the runs that observe
it. If `scope` or `coverage` churns per run, segments degenerate to one row per
run and buy nothing.

**Do not start Phase 1 before running this.** Against
`/srv/event-universe/build/universe-v5.sqlite3` (158 runs, read-only):

```sql
-- 1. Compression ratio the segment model would actually achieve.
--    Lower bound on segment count; the fix is only worth building if this is
--    a small multiple of the 39,025 distinct relations, not of 3,333,919.
SELECT COUNT(*) FROM (
  SELECT DISTINCT relation_id, bundle_id, event_id, scope, coverage
  FROM relation_observations
);

-- 2. State changes per relation. Expect a small number; a heavy tail here
--    means scope/coverage churn and the segment model needs rethinking.
SELECT changes, COUNT(*) FROM (
  SELECT relation_id, COUNT(DISTINCT bundle_id || '|' || event_id || '|' ||
                            scope || '|' || coverage) AS changes
  FROM relation_observations GROUP BY relation_id
) GROUP BY changes ORDER BY changes;

-- 3. Confirm defect 2.2 directly.
SELECT run_id, COUNT(*) AS relations FROM relation_observations
GROUP BY run_id ORDER BY relations DESC LIMIT 5;

-- 4. Confirm defect 2.3's trajectory.
SELECT MAX(observations) FROM (
  SELECT COUNT(*) AS observations FROM relation_observations GROUP BY relation_id
);

-- 5. Are the other run_detail arrays also over 1000? Decides whether Phase 4
--    paginates relations only or the whole endpoint.
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM candidate_decisions GROUP BY run_id);
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM selected_market_occurrences GROUP BY run_id);
```

**Gate:** query 1 must return a number well under ~500,000 (i.e. under ~15% of
current rows) for the segment model to be the right fix. If it does not, the
fallback is the write-amplification document's option 2 — a sanctioned bulk-load
mode that drops and rebuilds the two secondary indexes — which fixes throughput
only and leaves 2.2 and 2.3 to be solved separately by pagination alone.

## 5. Phase 1 — schema v6: `relation_state_segments`

Replace `relation_observations` with one row per contiguous span over which a
relation was observed in an unchanged state.

```sql
CREATE TABLE relation_state_segments (
    relation_id INTEGER NOT NULL REFERENCES relations(relation_id),
    bundle_id TEXT NOT NULL,
    first_generated_at_ns INTEGER NOT NULL,
    first_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_generated_at_ns INTEGER NOT NULL,
    last_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    scope TEXT NOT NULL,
    coverage TEXT NOT NULL,
    observation_count INTEGER NOT NULL CHECK(observation_count > 0),
    PRIMARY KEY(relation_id, bundle_id, first_generated_at_ns),
    CHECK(last_generated_at_ns >= first_generated_at_ns)
) STRICT;
CREATE INDEX relation_state_segments_event
    ON relation_state_segments(event_id, relation_id, first_generated_at_ns);
```

Bump `SCHEMA_VERSION` to 6 (`universe/store.py:23`). Leave
`MARKET_PROJECTION_VERSION` at 3 per finding (e).

**Segment invariant, stated so it can be tested:** a segment asserts that in
*every ingested run* whose `generated_at_ns` lies in
`[first_generated_at_ns, last_generated_at_ns]`, this relation was observed under
this bundle with this `event_id`, `scope`, and `coverage`. `observation_count`
equals the number of such runs, which makes the invariant checkable by a single
audit query rather than by trust.

**Why this is fast, mechanically.** The hot path per observed relation becomes a
prefix seek on the primary key `(relation_id, bundle_id, ...)` followed by an
`UPDATE` of `last_generated_at_ns`, `last_run_id`, and `observation_count` —
**none of which appear in any index**. So the update dirties one already-hot leaf
page and touches no B-tree. Scattered index insertions drop from ~42,000 per run
to a few hundred, incurred only on genuine state changes. At the segment counts
Phase 0 should show, the whole table plus the `relations` unique index fits in
page cache, which is what removes the deceleration curve rather than merely
flattening it.

Deliberately **no index on `last_generated_at_ns`**: indexing it would reintroduce
per-observation index churn on the one column that changes every run, and the
range query that would use it (§7) is a cached full scan anyway.

## 6. Phase 2 — writer

Replace the `INSERT INTO relation_observations` at `universe/store.py:849` with
segment maintenance. Ingesting run `N` at `generated_at_ns = T`:

1. **Repair.** For every existing segment with
   `first_generated_at_ns < T < last_generated_at_ns`, the segment's assertion now
   covers run `N`. If `N` observes that relation under that bundle with the same
   state, leave it and increment `observation_count`. Otherwise split it at `T`
   into `[first, prev(T)]` and `[next(T), last]`, where `prev`/`next` are the
   neighbouring ingested runs from `targeter_runs`.
2. **Extend or open.** For each relation observed in `N` and not already covered
   by step 1, take the segment for `(relation_id, bundle_id)` with the greatest
   `first_generated_at_ns <= T`. Extend it if its `last_generated_at_ns` is the
   immediately preceding ingested run and its state matches; otherwise insert a
   new `[T, T]` segment.

Implement step 1 as an unconditional scan of the segments table rather than
splitting into a fast in-order path and a slow out-of-order path. The table is
small enough to stay cached, the scan is the same order of magnitude as the
21,101 lookups step 2 performs anyway, and a single code path is the difference
between an invariant that holds and one that holds until the first retry. This
directly addresses finding (a) — correctness under the out-of-order ingestion the
current sync already performs.

## 7. Phase 3 — readers

- `run_detail` (`universe/store.py:1440`) — replace the `run_id` equality
  predicate with `first_generated_at_ns <= :t AND last_generated_at_ns >= :t`
  against the run's own `generated_at_ns`. Full scan of a cached table; correct
  for arbitrary `N` because segments have no holes.
- `event_detail` (`universe/store.py:1577`) and `market_detail`
  (`universe/store.py:1688`) — delete both `NOT EXISTS ... newer` subqueries and
  select the segment with the greatest `last_generated_at_ns` per relation. This
  is finding (c): the subqueries exist only to reconstruct current state from an
  append-only log, and the segment model stores it directly.
- `relation_detail` (`universe/store.py:1749`) — return segments instead of
  observations. This fixes defect 2.3 outright: the row count becomes the number
  of state changes, which is bounded by the data rather than by elapsed time.

## 8. Phase 4 — API bounds (defect 2.2)

Phase 3 shrinks stored rows but not the ~21,101 relations a complete run
genuinely has. Run detail still cannot inline them.

Extend the existing contract rather than inventing a new one.
`EVENT_UNIVERSE_STORE_V1.md` §8 already states that run detail "intentionally
omits raw candidate relationship arrays ... clients follow event, market, and
relation IDs for detail" — so:

- Add `GET /v1/targeter/runs/<run_id>/relations?limit=&cursor=` with the standard
  1–100 limit and opaque cursor.
- Keep `counts.relations` in run detail; replace the inline `relations` array with
  a link to the paginated endpoint.
- Update the UI validator: `targeter-ui/src/server/event-universe.ts:1005`
  currently errors when `validatedCounts.relations !== relations.length`, and
  line 1007 requires every relation to carry a known `event_id`. Both assumptions
  break under pagination and must be revised together with the server change.
- Revisit `decisions` and `selected_markets` in run detail based on Phase 0
  queries 5 — if either also exceeds 1,000, paginate it the same way rather than
  discovering it in production.

The presentation bound itself stays. Per the closed origin-context document, the
invariant is that *ingestion* must never fail because a record is awkward to
serve; the read path is where a bound belongs, and the fix there is a page, not a
larger limit.

## 9. Phase 5 — tests

Per `AGENTS.md` §3, each defect needs a falsifying regression that fails for the
stated reason before the production change lands.

1. **Write amplification.** Ingest the same relation set across several runs;
   assert `relation_state_segments` row count stays flat rather than growing per
   run. Fails today because `relation_observations` grows linearly.
2. **Out-of-order correctness (finding a).** Ingest runs 1 and 3 with a relation
   present in both, then ingest run 2 with that relation absent. Assert
   `run_detail(2)` omits it and `run_detail(1)`/`run_detail(3)` include it — i.e.
   the segment was split, not silently extended. This is the test that a naive
   interval model fails.
3. **State change.** Same relation, changed `coverage` at run 2: assert two
   segments with adjacent, non-overlapping ranges and correct
   `observation_count`s.
4. **Defect 2.2.** Build a run with `DETAIL_ROW_LIMIT + 1` relations; assert run
   detail serves them across pages instead of raising `DetailTooLarge`. Fails
   today.
5. **Defect 2.3.** Observe one relation across `DETAIL_ROW_LIMIT + 1` runs;
   assert `relation_detail` serves it. Fails today.
6. **Invariant audit.** Assert `observation_count` equals the number of ingested
   runs inside each segment's range, over a multi-run fixture with a gap.

Existing coverage to update: `tests/test_event_universe_store.py:1175`,
`:1431`, `:1492`, `:1697`, `:1890`.

## 10. Phase 6 — spec, rebuild, verification

- Update `docs/EVENT_UNIVERSE_STORE_V1.md` §4 (relationship normalization now
  names `relation_state_segments` and states the segment invariant) and §8 (the
  new paginated endpoint). Per `AGENTS.md` §3 "Persisted formats", also update
  `universe/schema/README.md` and any fixture that names the old table.
- Mark `UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md` as superseded by
  this plan once implemented, the way the origin-context document supersedes
  `UNIVERSE_V3_SYNC_LIVELOCK.md`.
- Rebuild from scratch — schema v6 requires it, and the partial v5 build is
  discarded either way.
- Verify against the write-amplification document's own targets, **measured
  late** rather than averaged, since the first ~100 runs are fast while the
  indexes still fit in cache:
  - segments written per run in the low hundreds, not ~21,000;
  - WAL growth per run in single-digit MB, not ~190 MB;
  - marginal seconds-per-run flat across the range rather than climbing;
  - full 1,202-run backfill inside a couple of hours.

## 11. What not to do

- **Do not raise `DETAIL_ROW_LIMIT`.** It moves the wall. Defect 2.2 needs
  pagination; 2.3 needs fewer rows.
- **Do not reach for infrastructure first.** A larger volume for IOPS or a bigger
  machine for page cache is paying for a workload that writes 549 MB per run to
  grow the database by 11 MB. Both are worth revisiting *after* the row count is
  fixed, per the write-amplification document.
- **Do not tune SQLite pragmas.** Already measured: `synchronous = NORMAL`, a
  64 MB cache, `wal_autocheckpoint = 20000`, and explicit checkpoints produced
  **no improvement** over baseline. Durability settings do not help when the cost
  is the number of pages dirtied.
- **Do not add a schema migration path.** `initialize()` deliberately has none,
  the database is a derived artifact rebuildable from the immutable ObjectStore
  (`EVENT_UNIVERSE_STORE_V1.md` §7), and writing one now would be work spent to
  avoid a rebuild we have to do anyway.
- **Do not deploy v5.** Finding (d): shipping it closes the free-schema-change
  window and puts defects 2.2 and 2.3 in front of users.
