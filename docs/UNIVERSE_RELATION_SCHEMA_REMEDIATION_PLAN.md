# Event Universe — relation model remediation plan

**Status:** proposed. Not implemented.
**Supersedes:** the first revision of this document, which planned a
`relation_state_segments` schema. That plan optimised the storage of a
relation set that should not be materialised per run at all. It is retained
below as the fallback (Part D) and is no longer the recommendation.
**Owns:** [`UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md`](./UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md).
**Blocks:** the schema v5 rollout. universe-server still serves `v0.11.0` on
schema v2; nothing is deployed on v5.

---

## Part A — what is actually wrong

### A.0 Already closed

`UNIVERSE_ORIGIN_CONTEXT_DETAIL_LIMIT.md` is fixed on master as `a32b127`;
`_insert_context` and `origin_context` both call `_context(..., bounded=False)`.
No work here.

### A.1 Write amplification (measured)

`relation_observations` takes 21,101 rows per run to record ~247 genuinely new
relations. Two secondary indexes lead with `relation_id` and `event_id`, so each
insert lands at a random offset: ~42,000 scattered B-tree insertions and ~190 MB
of WAL per run, for ~11 MB of net growth. A 1,202-run backfill projects to 2–4
days against a 1–2 hour target.

### A.2 `GET /v1/targeter/runs/<run_id>` cannot serve a complete run

`run_detail` (`universe/store.py:1440`) caps a run's relations at
`DETAIL_ROW_LIMIT = 1000` (`universe/store.py:27`) and raises `DetailTooLarge`
above it. A complete run holds ~21,101. **Every complete run therefore returns an
error.** Invisible in tests: the fixture at
`tests/test_event_universe_store.py:1431` asserts exactly one relation.

### A.3 `GET /v1/relations/<relation_id>` degrades to an error over time

`relation_detail` (`universe/store.py:1749`) caps observations at the same limit.
One row per run means a relation crosses 1,000 observations after ~13 days at the
archive's ~75 runs/day cadence, and then fails permanently.

### A.4 Most of what we store, the producer already discarded

This is the finding that reframes the whole problem.

`derive_bundle_relationships` (`targeter/v2/relationships.py:399`) is an all-pairs
loop over masks, and `relationship()` (`analysis/masks.py:352`) has **no
"unrelated" outcome**:

| set comparison | result |
|---|---|
| `a == b` | IDENTITY |
| `a ⊂ b` / `b ⊂ a` | IMPLICATION / REVERSE_IMPLICATION |
| `a ∩ b = ∅` | MUTUAL_EXCLUSION |
| anything else | **OVERLAP** |

`OVERLAP` is the catch-all `else`, so n masks yield essentially all n(n−1)/2
pairs. The 1,081-relationship context from the origin-context investigation is
n ≈ 47 masks — one bundle, three venues. Masks are not markets: `_market_views`
(`relationships.py:203`) emits one mask per outcome claim, keyed
`venue:target_id#claim=N`.

Three layers of dead weight:

1. **Same-venue pairs.** Neither `relationship()` nor the pairing loop compares
   venues. At 3 venues × 16 masks that is ~32% of pairs. The system exists to
   find *cross-venue* equivalence.
2. **OVERLAP.** "These two markets mention some of the same outcomes." The
   default result for two arbitrary subsets.
3. **Outcome-space tautologies.** The three claim-masks of one 3-way moneyline
   are mutually exclusive *by construction of the space*. All-pairs records that
   as three discovered MUTUAL_EXCLUSION findings, and their cross-venue
   counterparts as eighteen more.

**The targeter's own scorer throws all of this away** (`targeter/v2/selection.py:190`):

```python
cross = [r for r in analysis.relationships
         if r.cross_venue and r.relationship != "OVERLAP" and ...]
```

`Relationship.cross_venue` is even serialized into the report
(`targeter/v2/models.py:515`). Universe's `_relation_rows`
(`universe/market_projection.py:522`) applies **neither** filter — it admits all
five types (`_SYMMETRIC = {IDENTITY, MUTUAL_EXCLUSION, OVERLAP}`) and never reads
`cross_venue`. We index twice and pay 190 MB of WAL per run for evidence that was
discarded before scoring.

---

## Part B — the architectural angle: relations on types, not instances

### B.1 The proposal

Define market types under the outcome spaces we already configure; make an
event's alias select which types apply; define the relation algebra **once at the
type level**; and let an instance-level relation be *implied* by two markets
sharing an event alias with a related type pair, resolved by index at query time
rather than materialised per market pair per run.

### B.2 Why the code says this works

**The mask predicate is already a pure function of typed parameters.** Every
resolver in `analysis/masks.py` is `space.select(...)` over a predicate reading
only typed payload fields:

```python
# series_moneyline
space.select(lambda p: p["winner_side"] == side)                     # (side)
# map_winner
space.select(lambda p: p["sequence"][position] == want)              # (map_index, side)
# total_maps
space.select(lambda p: p["maps_played"] < line)                      # (direction, line)
# map_handicap
space.select(lambda p: p["home_wins"] - p["away_wins"] > line)       # (favoured_side, line)
```

Nothing instance-specific enters the outcome set. What *is* instance-specific is
only the **extraction** of those parameters — `_team_side(label, space)`,
`_line_from(label)`, `re.search(r"(?:game|map)\s*(\d+)", label)`, the handicap
title regex. And the code already prefers typed values when present:
`market.get("map_index")`, `market.get("favoured_side")`, and
`market.parameters.get("line")` are checked *before* the regex fallback.

So `outcome_keys = f(market_type, typed_parameters, space)`, and therefore
`relationship(A, B) = g(type_A, type_B, space_shape)`. **The relation is a
property of the types. We recompute and re-store it per instance per run.** That
is the entire root cause — A.1, A.2, A.3 and A.4 are all downstream of it.

**The space-shape vocabulary is tiny and enumerable.** Series spaces are
generated purely from `(best_of, home, away)` with
`SUPPORTED_BEST_OF = {1, 3, 5, 7, 9}` (`targeter/v2/models.py:17`) — five shapes,
and participant names don't affect relation structure. Score spaces have a
data-dependent cap, `min(20, max(8, max(lines, scores) + 5))`
(`relationships.py:80`) — thirteen shapes. **Eighteen shapes total.**

**The type taxonomy is already config.** `configs/targeter_v2.json` carries nine
`market_classes`, each a `ClassDefinition(id, sport, market_type, scope,
venue_patterns)` (`targeter/v2/registry.py:21`):

```
soccer.correct_score / both_teams_to_score / total_goals / spread / moneyline_3way
esports.series_moneyline / map_winner / map_handicap / total_maps
```

`SCOPE_BY_TYPE` in `analysis/masks.py:49` is a second, hardcoded copy of the same
type→scope mapping. **They disagree**: the config gives `map_winner` scope
`"map"`, `SCOPE_BY_TYPE` gives it `"series"`, and
`derive_bundle_relationships` carries an explicit special case to bridge the two
vocabularies. Two sources of truth for one concept — exactly what this
consolidation removes.

**The claim-type label already exists and already ships.** `Mask.resolver` is the
name of the predicate — `f"series_{side}"`, `f"map_{index}_{side}"`,
`"maps_under"`, `f"handicap_{favoured_side}_covers"` — and
`RelationshipAnalysis.as_record` (`relationships.py:35`) **serializes it into
every report**, one row per mask. Universe reads the quadratic edge list and
discards the linear type labels.

Two masks with the same resolver in the same space have identical `outcome_keys`
by construction, so **IDENTITY ⟺ same resolver**, with no pairwise comparison at
all.

**One gap:** `resolver` omits the line for `total_maps`, `map_handicap` and
`spread` — two `maps_over` masks on different lines share a resolver but not a
key set. The complete type key is `(canonical_class, market_type, scope,
parameters)`, which is exactly `canonical_market_id`'s coordinate
(`market_projection.py:576`) **minus `event_id`**. Universe already stores all of
it in `canonical_markets.parameters_json`.

### B.3 What the model looks like

- **Claim type** — `(market_class, normalized parameters)`, event-independent.
  `esports.total_maps@over:2.5`, `esports.map_winner@map:1,side:home`.
- **Space shape** — `(scope, shape parameters)`. `series@bo5`, `score@cap:12`.
  Eighteen of them.
- **Type relation table** — `(space_shape, type_a, type_b) → kind`, computed once
  by running the existing mask predicates over each shape. Static derived
  configuration, versioned, not per-run data. Order tens of thousands of rows.
- **Market instance** — one row carrying `(event_alias, claim_type, venue)`.
- **A relation between two instances is implied**, never stored: same event
  alias, same space shape, type pair present in the table. Indexed by
  `(event_alias, claim_type)`, so the join is a covering-index lookup.

### B.4 What it removes

- `relation_observations` — **gone**, along with the projected 25.4 M rows and
  every scattered index insertion. A.1 disappears rather than being optimised.
- `relation_state_segments` — never built. The first revision of this plan is
  obsolete if this lands.
- A.2 — run detail returns markets; relations come from a table of thousands.
- A.3 — there are no per-relation observations to overflow.
- A.4 — OVERLAP and same-venue pairs are excluded once, in the type table.

Relations become O(1) in runs and O(types²) rather than O(markets²) × runs.

### B.5 The honest cost and the risks

1. **Typed claim extraction is the real work.** Adapters must emit
   `(claim_type, parameters)` instead of leaving `_series_mask` to parse
   `"Game Handicap: PARI (-1.5) vs TY (+1.5)"`. This spans
   `targeter/v2/adapters/{kalshi,polymarket,limitless}.py`. The migration is
   safe by the repo's own rule — `AGENTS.md`: "Fail closed on unknown semantic
   shapes. Prefer a visible false negative over a guessed cross-venue
   equivalence" — so a market whose claim cannot be typed simply yields no
   relations, visibly.
2. **Parameter domains must be finite and normalized.** Type cardinality drives
   the table quadratically. Lines are 0.5-stepped and bounded in practice, but
   this must be **enumerated from the archive, not assumed** (Phase 0).
3. **The score-space cap is data-dependent**, so the same two markets can sit in
   different shapes in different bundles. Relations are mostly cap-invariant but
   not provably so. Either key the table by cap (13 values, fine) or make the cap
   config per sport — the latter also makes `context_sha256` more stable.
4. **The scope vocabulary must be unified** (config `"map"` vs `SCOPE_BY_TYPE`
   `"series"`), and `SCOPE_BY_TYPE` folded into the config it duplicates.
5. **Rules are not in the outcome space.** Two markets with the same claim type
   may still differ in void policy or extra-time treatment; `rules_hash`,
   `rule_template_id` and `assess_rules` exist precisely because of this. The
   type model does **not** make this worse — the mask engine already ignores
   rules — but it makes the assumption load-bearing and it should be stated in
   the spec rather than left implicit.
6. **Provenance changes shape.** "What did run N believe?" stops being a stored
   fact and becomes a reconstruction from run N's market set plus the versioned
   type table. That is stronger provenance (fewer derived facts asserted) but it
   is an API and UI contract change, and the type table must be versioned and
   immutable per version for it to hold.
7. **Scope is much larger than the backfill fix** — adapters, targeter, report
   content, Universe schema, API, UI. Hence Part C.

**The strongest evidence that this is tractable:** `resolver` is already in every
committed report and `parameters_json` is already in Universe, so the full
1,202-run history can be **re-projected under the new model from the existing
archive** without re-running the targeter.

---

## Part C — sequencing, and the recommendation

The backfill is blocked now. The type model is the right end state but touches
five components. The question is what unblocks the backfill without building
something the type model deletes.

| Work | Unblocks backfill | Survives the type model |
|---|---|---|
| Filter OVERLAP + same-venue at projection | partly (~8×) | **yes** — the type table simply omits those pairs |
| Bulk-load index deferral | **yes** | yes — a load mode, not a schema |
| Run-detail pagination | n/a (fixes A.2) | **yes** — needed under both models |
| `relation_state_segments` (v6) | yes | **no** — deleted by the type model |
| Type-relation model | yes, permanently | it is the end state |

**Recommendation: do the filter, the bulk-load mode and pagination now; skip the
segment schema; go to the type model as the real fix.** That unblocks the
rollout in days, throws nothing away, and avoids designing, testing, shipping and
then deleting a v6 schema.

The interim is deliberately *not* the recommendation for the end state. Filtering
alone leaves per-run rows growing linearly forever; it buys a constant factor, not
the shape.

---

## Phase 0 — measure before building (gate for every part)

Against `/srv/event-universe/build/universe-v5.sqlite3` (158 runs, read-only).
**No code before these run.**

```sql
-- 0.1 How much of the stored relation set is dead weight? (Part A.4, sizes the filter)
SELECT relation_type, pairing, COUNT(*) FROM (
  SELECT r.relation_id, r.relation_type,
         CASE WHEN COUNT(DISTINCT m.venue) = 1 THEN 'same' ELSE 'cross' END AS pairing
  FROM relations r JOIN relation_members m USING (relation_id)
  GROUP BY r.relation_id
) GROUP BY relation_type, pairing ORDER BY 3 DESC;

-- 0.2 Claim-type cardinality: the number that decides whether the type table is
--     small. Distinct (class, type, scope, parameters) ignoring the event.
SELECT COUNT(*) FROM (
  SELECT DISTINCT canonical_class, market_type, scope, parameters_json
  FROM canonical_markets
);

-- 0.3 Parameter domains — are lines finite and 0.5-stepped, or open-ended?
SELECT canonical_class, market_type,
       COUNT(DISTINCT parameters_json) AS distinct_parameterisations
FROM canonical_markets GROUP BY 1, 2 ORDER BY 3 DESC;

-- 0.4 Confirm A.2 directly.
SELECT run_id, COUNT(*) AS relations FROM relation_observations
GROUP BY run_id ORDER BY relations DESC LIMIT 5;

-- 0.5 Confirm A.3's trajectory.
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM relation_observations GROUP BY relation_id);

-- 0.6 Do the other run_detail arrays also exceed 1000?
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM candidate_decisions GROUP BY run_id);
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM selected_market_occurrences GROUP BY run_id);
```

**Gates.**
- 0.2 in the low thousands and 0.3 showing bounded parameterisations → the type
  model is viable; proceed to Part C's recommendation.
- 0.2 in the hundreds of thousands, or 0.3 showing unbounded lines → the type
  table is not small, **stop and reassess**; fall back to Part D.
- 0.1 showing cross-venue non-OVERLAP well under 100% → the interim filter is
  worth shipping regardless, since it is a subset of the type model's own
  exclusions.

---

## Phase 1 — interim unblock (days, nothing wasted)

**1a. Filter at projection.** In `_relation_rows`
(`universe/market_projection.py:522`), admit only cross-venue, non-OVERLAP
relations. `cross_venue` is already on the record. Bump
`MARKET_PROJECTION_VERSION` to 4 (the projection shape changes; the identity hash
at `store.py:2502` is over the projection document, so run idempotence stays
sound). Filter in **Universe, not the targeter**: `context_relationships` feeds
`context_sha256`, and changing what is stored there breaks identity against
already-committed reports. Universe is a rebuildable derived index; the report is
immutable evidence.

**1b. Sanctioned bulk-load mode.** Drop `relation_observations_relation` and
`relation_observations_event` for the duration of a backfill and rebuild them
once at the end, turning ~42,000 random insertions per run into one sequential
sort-build. `_validate_schema` (`store.py:86`) compares the live schema
byte-for-byte against `schema.sql` and will reject a database with missing
indexes, so this needs an explicit sanctioned mode with the validation deferred
to the end of the load — not a hack that leaves the invariant weakened.

**1c. Run-relation pagination** (fixes A.2). Add
`GET /v1/targeter/runs/<run_id>/relations?limit=&cursor=` with the standard
1–100 limit and opaque cursor; keep `counts.relations` in run detail and replace
the inline array with a link. `EVENT_UNIVERSE_STORE_V1.md` §8 already says run
detail "intentionally omits raw candidate relationship arrays ... clients follow
event, market, and relation IDs for detail", so this extends the stated contract
rather than changing it. Update the UI validator together with the server:
`targeter-ui/src/server/event-universe.ts:1005` errors when
`validatedCounts.relations !== relations.length`, and line 1007 requires every
relation to carry a known `event_id`. Both break under pagination. Paginate
`decisions` and `selected_markets` too if Phase 0.6 says they also exceed 1,000.

**Exit criterion:** a full 1,202-run backfill completes. If it does not, that is
the signal to accelerate Phase 2 rather than to tune further.

## Phase 2 — spike the type model (before committing to it)

Offline, against archived reports. No schema change, no production code.

1. Enumerate every distinct `(market_class, normalized parameters)` in the
   archive and every distinct space shape. Confirm Phase 0.2/0.3's numbers hold
   over the full history, not just 158 runs.
2. Build the type-relation table by running the existing mask predicates over
   each shape × type pair. Record its size.
3. **Falsification test:** for a sample of runs, reconstruct the relation set
   from `(event_alias, claim_type)` + the type table, and diff it against the
   relations the report actually recorded. Every divergence is either a bug in
   the type model or a case where the mask engine's text parsing produced
   something the typed model cannot — both must be explained before proceeding.

**Gate:** the reconstruction must match on cross-venue non-OVERLAP relations
(the ones the scorer uses). Divergence on OVERLAP is acceptable and expected.

## Phase 3 — typed claims

- Fold `SCOPE_BY_TYPE` (`analysis/masks.py:49`) into the `market_classes` config
  it duplicates, and unify the scope vocabulary (config `"map"` vs `"series"`),
  removing the special case at `relationships.py:409`.
- Extend `ClassDefinition` with the claim parameterisation: which typed
  parameters define a claim for that class, and their normalization.
- Make adapters emit typed claims, failing closed where they cannot. The regex
  fallbacks in `_series_mask`/`_score_mask` become the *diagnostic* path, not the
  primary one.
- Make the score-space cap config per sport rather than derived from whichever
  markets a venue happened to list.

## Phase 4 — the type-relation model in Universe

- Add the versioned type table and the claim-type column on canonical markets;
  drop `relation_observations`. Schema v6.
- Rewrite the four readers (`store.py:1440`, `:1577`, `:1688`, `:1749`) against
  the implied model. Both `NOT EXISTS ... newer` subqueries — which exist only to
  reconstruct current state from an append-only log — disappear.
- Re-project the full history from the existing archive. No targeter re-run.

## Phase 5 — tests

Per `AGENTS.md` §3, each defect needs a falsifying regression that fails for the
stated reason first.

1. **A.4 / Phase 1a.** A bundle with same-venue and OVERLAP relations: assert
   they are not projected. Fails today.
2. **A.2 / Phase 1c.** A run with `DETAIL_ROW_LIMIT + 1` relations: assert it is
   served across pages, not as `DetailTooLarge`. Fails today.
3. **A.3.** A relation observed across `DETAIL_ROW_LIMIT + 1` runs: assert
   `relation_detail` serves it. Fails today.
4. **Phase 2 equivalence.** The reconstruction diff, as a test over fixtures.
5. **Type-table determinism.** Same shape and type pair yields the same kind
   across builds; the table is a pure function of its version.
6. **Fail-closed.** An untypeable claim yields no relations and a visible
   diagnostic — never a guessed equivalence.

Existing coverage to update: `tests/test_event_universe_store.py:1175`, `:1431`,
`:1492`, `:1697`, `:1890`.

## Phase 6 — spec and verification

- Update `docs/EVENT_UNIVERSE_STORE_V1.md` §4 (relations are implied by type, not
  observed per run; state the rules-are-not-in-the-space assumption from B.5.5
  explicitly) and §8 (new endpoints). Update `universe/schema/README.md` and
  `configs/targeter_v2.json`'s schema notes.
- Mark `UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md` superseded.
- Verify against that document's targets, **measured late** rather than averaged,
  since the first ~100 runs are fast while the indexes still fit in cache:
  WAL growth per run in single-digit MB; marginal seconds-per-run flat across the
  range; full 1,202-run backfill inside a couple of hours.

---

## Part D — fallback if Phase 0.2/0.3 or Phase 2 fails

If claim-type cardinality is too large or the reconstruction cannot be made to
match, the type model is out and the storage problem must be solved directly:
replace `relation_observations` with `relation_state_segments`, one row per
contiguous span over which a relation was observed unchanged.

This was the first revision's plan. Its two load-bearing details, kept here so
they are not rediscovered:

- **A plain `first_seen`/`last_seen` interval is wrong.** It cannot represent a
  gap, and `run_detail` needs exact per-run membership. The model must be
  *contiguous segments*.
- **Ingestion is routinely out of generated-time order** — `sync()` ingests due
  retry failures before walking the date range (`universe/sync.py:168`) and
  bootstrap walks newest-first (`:188`). Segments must therefore be **split** when
  a run lands inside an existing span, or they will silently claim presence in
  runs that never observed the relation.

Sketch, with the primary key chosen so the hot-path `UPDATE` touches no indexed
column:

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
```

No `ON DELETE CASCADE`: a segment spans many runs, there is no run-deletion path
in `universe/`, and `EVENT_UNIVERSE_STORE_V1.md` §7 states Universe never prunes.

---

## What not to do

- **Do not raise `DETAIL_ROW_LIMIT`.** It moves the wall. A.2 needs pagination;
  A.3 needs fewer rows.
- **Do not build the segment schema while the type model is undecided.** It is
  the one piece of work the type model deletes. Phases 0 and 2 are cheap and
  decide it.
- **Do not filter in the targeter.** `context_relationships` feeds
  `context_sha256`; changing stored context content breaks identity against
  committed reports.
- **Do not reach for infrastructure first.** A larger volume or a bigger machine
  is paying for a workload that writes 549 MB per run to grow the database by
  11 MB.
- **Do not tune SQLite pragmas.** Already measured: `synchronous = NORMAL`, a
  64 MB cache, `wal_autocheckpoint = 20000` and explicit checkpoints gave **no
  improvement**. Durability settings do not help when the cost is the number of
  pages dirtied.
- **Do not add a schema migration path.** `initialize()` (`store.py:47`)
  deliberately has none, and the database is a derived artifact rebuildable from
  the immutable ObjectStore.
- **Do not deploy v5.** It closes the free-schema-change window and puts A.2 and
  A.3 in front of users.

---

## Open questions for the author

1. **Scoring.** `cross_venue_relationships` sums MUTUAL_EXCLUSION at weight 10.0
   (`selection.py:199`). Cross-venue mutual exclusions grow as
   `c(c−1)·V(V−1)/2` in claims and venues, while the IDENTITY signal grows only
   as `c·V(V−1)/2`. Bundles with more outcome claims may accumulate score from
   outcome-space tautologies faster than from real equivalences. Intentional?
2. **"Events as a subset of the market category."** Read here as: the event alias
   (sport, game, topology, best-of) selects the space shape, which selects the
   valid claim types. Confirm before Phase 3 — a different reading changes the
   key structure.
3. **OVERLAP's purpose.** Is it ever read by anything downstream, or is it purely
   an artefact of `relationship()` having no null result? If the latter, it should
   arguably not be a catalogue type at all (`universe/api.py:429`).
