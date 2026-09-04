# Event Universe — relations as derived claim algebra

**Status:** proposed. Not implemented.
**Supersedes:** the two earlier revisions on this branch — a `relation_state_segments`
schema, then a staged plan with an interim filter. Both optimised the storage of a
relation set that should not be materialised per run at all. The segment design is
retained only as the fallback (Part E).
**Owns:** [`UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md`](./UNIVERSE_RELATION_OBSERVATION_WRITE_AMPLIFICATION.md).
**Blocks:** the schema v5 rollout. universe-server serves `v0.11.0` on schema v2;
nothing is deployed on v5.

---

## Part A — the defects

**A.0 Closed.** `UNIVERSE_ORIGIN_CONTEXT_DETAIL_LIMIT.md` is fixed on master as
`a32b127`. No work here.

**A.1 Write amplification (measured).** `relation_observations` takes 21,101 rows
per run to record ~247 new relations. Two secondary indexes lead with
`relation_id` and `event_id`, so each insert lands at a random offset: ~42,000
scattered B-tree insertions and ~190 MB of WAL per run for ~11 MB of net growth.
A 1,202-run backfill projects to 2–4 days against a 1–2 hour target.

**A.2 `GET /v1/targeter/runs/<run_id>` cannot serve a complete run.** `run_detail`
(`universe/store.py:1440`) caps relations at `DETAIL_ROW_LIMIT = 1000` and raises
`DetailTooLarge` above it. A complete run holds ~21,101. Every complete run
returns an error. Invisible in tests — `tests/test_event_universe_store.py:1431`
asserts exactly one relation.

**A.3 `GET /v1/relations/<relation_id>` degrades to an error.** `relation_detail`
(`universe/store.py:1749`) caps observations the same way; one row per run means a
relation crosses 1,000 after ~13 days at ~75 runs/day, then fails permanently.

**A.4 Most of what we store, the producer already discarded.**
`relationship()` (`analysis/masks.py:352`) has no "unrelated" result — `OVERLAP`
is the catch-all `else` — so the all-pairs loop in `derive_bundle_relationships`
(`targeter/v2/relationships.py:399`) yields essentially every mask pair, including
same-venue pairs and tautologies implied by the shared outcome space. The
targeter's own scorer keeps only cross-venue non-OVERLAP relations
(`targeter/v2/selection.py:190`) and serialises `cross_venue` into the report
(`models.py:515`). Universe's `_relation_rows` (`market_projection.py:522`)
applies neither filter.

All four are downstream of one mistake: **the relation is a property of the
claims, and we materialise it per market pair per run.**

---

## Part B — the model

A market is not *assigned* a type. Its type is the **image of the market under the
mask function the targeter already computes**. Nothing about typing is load-bearing
on the market row; the market carries a derived class id and nothing else.

- **Claim** — the outcome-key subset a market resolves YES on, within a space
  shape. Identity is the key set itself, not a name.
- **Space shape** — `(scope, shape parameters)`: `series@bo5`, `score@cap:12`.
- **Claim class** — one row per distinct `(space_shape, outcome_keys)`. **Global.**
- **Claim relation** — `(space_shape, class_a, class_b) → kind`, computed once by
  set algebra. **Global, static, versioned.**
- **Market** — carries `claim_class_id`. One row per market, ever.
- **A relation between two markets is implied**, never stored: same event alias,
  same shape, class pair present in the table. Indexed by `(event_alias,
  claim_class_id)`, so the join is a covering-index lookup.

Nothing is written per run. A.1 disappears rather than being optimised.

### B.1 Verified, not assumed

Run against the real code in this repo:

```
outcome keys identical across events of the same shape: True | size: 20
same claim, different venue text -> same keys: True   (relation: IDENTITY)
claim is participant-independent -> keys reusable across events: True
resolver collision: maps_over == maps_over but keys equal? False
  their true relation: IMPLICATION
```

**Outcome keys are participant-independent.** `build_series_space` keys outcomes
`seq:{sequence}` and `build_score_space` keys them `score:{h}-{a}` — neither
mentions the teams. A BO5 space has the same 20 keys whoever is playing. **This is
what makes claim classes global rather than per-event**, and it is the fact the
whole design rests on.

**The claim identity is the key set, not the resolver.** `maps_over@2.5` and
`maps_over@3.5` share the resolver `maps_over` but denote different subsets whose
true relation is IMPLICATION. So `Mask.resolver` — which the report already ships —
is a useful label but **not** a valid type key. The key set is.

**The table is small.** Enumerating a plausible claim vocabulary over the five
series shapes:

| shape | outcomes | claims | signal pairs | all pairs |
|---|---|---|---|---|
| `series@bo1` | 2 | 2 | 1 | 1 |
| `series@bo3` | 6 | 14 | 50 | 91 |
| `series@bo5` | 20 | 24 | 125 | 276 |
| `series@bo7` | 70 | 34 | 233 | 561 |
| `series@bo9` | 252 | 44 | 374 | 946 |

**783 signal rows for the entire esports relation algebra, globally, forever** —
against a projected 25.4 M `relation_observations` rows. Score shapes add more, but
the order of magnitude is settled. (This is an enumeration I constructed, not a
measurement of the archive; Phase 1 confirms it against real data.)

### B.2 Deriving claims without touching adapters or the targeter

"Borrow the code and run it in reverse on the same bundle" works, and Universe
already stores every input.

**Path A — recompute masks in Universe.** `_base_view`
(`targeter/v2/relationships.py:191`) needs `target_id`, `venue`, `market_type`,
`title`, `venue_market_id`, `parameters`; `_meaningful_labels` needs
`outcome_labels` and `subscription_ids`. Universe's `venue_markets` carries
`title`, `parameters_json`, `outcome_labels_json`, `subscription_ids_json`,
`market_type`, `scope` — all of it. The space needs participants
(`umbrella_events.participants_json`) and `best_of`, which derives from
`venue_events.format` (`models.py:466`), also stored. So Universe can rebuild the
space and compile masks **from its own rows**, for all history, with no adapter
change, no targeter change, and no new config taxonomy.

**Path B — components over the IDENTITY edges already stored.** `relationship()`
returns IDENTITY iff set equality, so the IDENTITY subgraph is a disjoint union of
complete cliques; its connected components *are* the claim classes.
`relation_members` preserves `claim_key` (`market_projection.py:601`), so
components land at claim granularity, not market granularity. Needs no new
dependency at all.

**Use A as the model and B as the check.** They are independent derivations of the
same partition, so disagreement means one is wrong — and we want that before
building, not after. B also gives a cheap migration for existing rows. Two
integrity assertions fall out for free: a component of size k must carry exactly
k(k−1)/2 IDENTITY edges, and every edge between two components must agree on kind.

### B.3 Why go straight here, skipping the interim

The earlier revision staged a projection-level filter and a bulk-load index mode
first. That was sized against a much larger end state — one that changed venue
adapters to emit typed claims. Path A removes that work entirely, so the end state
is now a Universe-only change of days, not weeks.

Given that: **the fastest route to an unblocked backfill is the end state itself**,
because it writes no relation rows per run at all. Shipping the interim first costs
two projection-version bumps, two rebuilds, and a bulk-load mode built to keep a
table alive that we then delete.

The risk is real and worth naming: if Phase 1 fails, the backfill stays blocked
while we fall back to Part E. That is why Phase 1 is a day of offline work against
data we already have, before any schema is touched.

### B.4 Known limits, carried forward or introduced

1. **Rules are not in the outcome space.** Two markets in the same claim class may
   differ in void policy or extra-time treatment; `rules_hash`, `rule_template_id`
   and `assess_rules` exist because of this. The mask engine already ignores rules,
   so this is **carried forward, not introduced** — but the class model makes the
   assumption load-bearing and it must be stated in the spec.
2. **The score-space cap is data-dependent** — `min(20, max(8, max(lines, scores)
   + 5))` (`relationships.py:80`) — so a soccer event's shape depends on which
   markets a venue happened to list, and a market's class can move if the cap
   moves. This is the one place the model genuinely needs configuration, and it is
   the part of the proposal that reads "outcome spaces are configurations we
   define": make the cap config per sport. It also makes `context_sha256` more
   stable.
3. **Two scope vocabularies.** `configs/targeter_v2.json` gives `map_winner` scope
   `"map"`; `SCOPE_BY_TYPE` (`analysis/masks.py:49`) gives it `"series"`;
   `relationships.py:409` carries a special case to bridge them. Fold
   `SCOPE_BY_TYPE` into the config it duplicates.
4. **Provenance changes shape.** "What did run N believe?" stops being a stored
   fact and becomes a reconstruction from run N's market set plus the versioned
   class table. That is stronger provenance — fewer derived facts asserted — but it
   requires the table to be immutable per version, and it is an API and UI contract
   change.
5. **Universe would import `analysis.masks`.** Its stored state then depends on the
   mask-engine version, so `mask_engine_version` becomes a schema column and a
   rebuild trigger.

---

## Part C — phases

### Phase 1 — falsification spike (gate; offline; no schema change)

Nothing is built until this passes.

1. Recompute claims for archived runs via Path A. Enumerate distinct
   `(space_shape, outcome_keys)` over the full history and the resulting relation
   table. Confirm the counts stay in the hundreds-to-low-thousands of Part B.1.
2. Derive the same partition via Path B and **diff it against Path A**. Check both
   integrity assertions (clique completeness, inter-component agreement).
3. Reconstruct each run's relation set from `(event_alias, claim_class)` + the
   table and diff against what the report recorded. Every divergence is either a
   model bug or a case where text parsing produced something the claim model
   cannot — both must be explained before proceeding.

**Gate:** reconstruction matches on cross-venue non-OVERLAP relations — the ones
the scorer uses. Divergence on OVERLAP is expected and fine. If claim cardinality
is large, or A and B disagree in ways that are not explainable, **stop** and go to
Part E.

Useful measurements while the data is open:

```sql
-- claim-shaped cardinality, ignoring the event
SELECT COUNT(*) FROM (SELECT DISTINCT canonical_class, market_type, scope, parameters_json
                      FROM canonical_markets);
-- are parameter domains bounded?
SELECT canonical_class, market_type, COUNT(DISTINCT parameters_json)
FROM canonical_markets GROUP BY 1,2 ORDER BY 3 DESC;
-- confirm A.2 and A.3 directly
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM relation_observations GROUP BY run_id);
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM relation_observations GROUP BY relation_id);
-- do the other run_detail arrays also exceed 1000?
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM candidate_decisions GROUP BY run_id);
SELECT MAX(n) FROM (SELECT COUNT(*) n FROM selected_market_occurrences GROUP BY run_id);
```

### Phase 2 — outcome-space configuration

The two config items the model actually needs (B.4.2, B.4.3): make the score-space
cap config per sport so shapes are enumerable, and fold `SCOPE_BY_TYPE` into
`market_classes`, removing the bridging special case. Deliberately scoped to what
the model requires — not a general taxonomy rewrite.

### Phase 3 — schema v6

Drop `relation_observations`, `relations`, `relation_members`. Add:

- `claim_classes(claim_class_id, space_shape, outcome_keys_sha256, mask_engine_version)`
- `claim_relations(space_shape, class_a, class_b, kind, algebra_version)`
- `claim_class_members(claim_class_id, ...)` for explaining a class in the UI
- `venue_markets.claim_class_id`

Bump `SCHEMA_VERSION` to 6. `MARKET_PROJECTION_VERSION` goes to 4: the projection
shape changes, and its identity hash is over the projection document
(`store.py:2502`), so run idempotence stays sound. No migration path —
`initialize()` (`store.py:47`) deliberately has none and the database is a derived
artifact rebuildable from the immutable ObjectStore.

### Phase 4 — writer and readers

Writer: compile claims per market, resolve the class, store the class id. No
per-run relation write at all.

Readers — all four (`store.py:1440`, `:1577`, `:1688`, `:1749`) go to the implied
model. Both `NOT EXISTS ... newer` subqueries, which exist only to reconstruct
current state from an append-only log, disappear.

A.2 still needs bounding even with a small table, since a run genuinely references
many markets: add `GET /v1/targeter/runs/<run_id>/relations?limit=&cursor=` with
the standard 1–100 limit and opaque cursor; keep `counts.relations` and replace the
inline array with a link. `EVENT_UNIVERSE_STORE_V1.md` §8 already states run detail
"intentionally omits raw candidate relationship arrays", so this extends the stated
contract. Update the UI validator with the server:
`targeter-ui/src/server/event-universe.ts:1005` errors when
`validatedCounts.relations !== relations.length` and line 1007 requires every
relation to carry a known `event_id`; both break under pagination. Paginate
`decisions` and `selected_markets` too if Phase 1 shows they exceed 1,000.

### Phase 5 — tests

Per `AGENTS.md` §3, each defect needs a falsifying regression that fails for the
stated reason first.

1. **A.1.** Ingest several runs over the same markets; assert no table grows per
   run. Fails today.
2. **A.2.** A run referencing `DETAIL_ROW_LIMIT + 1` relations: assert it is served
   across pages, not as `DetailTooLarge`. Fails today.
3. **A.3.** A relation across `DETAIL_ROW_LIMIT + 1` runs: assert `relation_detail`
   serves it. Fails today.
4. **Globality.** The same claim in two different events of the same shape resolves
   to the same class — the property proven in B.1, locked as a test.
5. **Resolver is not the key.** `maps_over@2.5` and `maps_over@3.5` land in
   different classes with an IMPLICATION between them.
6. **Path A ≡ Path B.** The Phase 1 diff, as a fixture test.
7. **Determinism.** A class table is a pure function of `(mask_engine_version,
   algebra_version)`.
8. **Fail-closed.** An untypeable claim yields no relations and a visible
   diagnostic, never a guessed equivalence (`AGENTS.md`: "Prefer a visible false
   negative over a guessed cross-venue equivalence").

Existing coverage to update: `tests/test_event_universe_store.py:1175`, `:1431`,
`:1492`, `:1697`, `:1890`.

### Phase 6 — spec, rebuild, verification

Update `docs/EVENT_UNIVERSE_STORE_V1.md` §4 (relations are implied by claim class,
not observed per run; state B.4.1 explicitly) and §8 (endpoints). Update
`universe/schema/README.md`. Mark the write-amplification document superseded.
Rebuild from the archive — no targeter re-run needed. Verify **late, not averaged**,
since the first ~100 runs are fast while indexes still fit in cache: WAL growth per
run in single-digit MB, marginal seconds-per-run flat, full 1,202-run backfill
inside a couple of hours.

---

## Part D — what not to do

- **Do not raise `DETAIL_ROW_LIMIT`.** It moves the wall.
- **Do not use `resolver` as the claim key.** Proven collision: `maps_over` at 2.5
  and 3.5. The key set is the key.
- **Do not change venue adapters.** Path B.2 makes typed-claim emission
  unnecessary; the mask function already is the normalization boundary.
- **Do not filter relations in the targeter.** `context_relationships` feeds
  `context_sha256`; changing stored context content breaks identity against
  committed reports.
- **Do not reach for infrastructure.** A bigger machine pays for a workload that
  writes 549 MB per run to grow the database by 11 MB.
- **Do not tune SQLite pragmas.** Already measured: `synchronous = NORMAL`, a 64 MB
  cache, `wal_autocheckpoint = 20000` and explicit checkpoints gave **no
  improvement**.
- **Do not deploy v5.** It closes the free-schema-change window and puts A.2 and
  A.3 in front of users.

---

## Part E — fallback if Phase 1 fails

Replace `relation_observations` with `relation_state_segments`, one row per
contiguous span over which a relation was observed unchanged. Two details worth not
rediscovering:

- **A plain `first_seen`/`last_seen` interval is wrong.** It cannot represent a gap,
  and `run_detail` needs exact per-run membership. The model must be *contiguous
  segments*.
- **Ingestion is routinely out of generated-time order** — `sync()` ingests due
  retry failures before walking the date range (`universe/sync.py:168`) and
  bootstrap walks newest-first (`:188`). Segments must be **split** when a run lands
  inside an existing span, or they silently claim presence in runs that never
  observed the relation.

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

The primary key is chosen so the hot-path `UPDATE` touches no indexed column. No
`ON DELETE CASCADE`: a segment spans many runs, there is no run-deletion path in
`universe/`, and `EVENT_UNIVERSE_STORE_V1.md` §7 states Universe never prunes.

---

## Open question for the author

`cross_venue_relationships` sums MUTUAL_EXCLUSION at weight 10.0
(`selection.py:199`). Cross-venue mutual exclusions grow as `c(c−1)·V(V−1)/2` in
claims and venues while the IDENTITY signal grows only as `c·V(V−1)/2`, so bundles
with more outcome claims may accumulate score from outcome-space tautologies faster
than from real equivalences. Under the claim model this becomes easy to compute
either way. Intentional?
