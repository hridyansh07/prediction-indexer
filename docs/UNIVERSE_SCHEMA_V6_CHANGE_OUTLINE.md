# Event Universe schema v6 — change outline

**Status:** proposed, awaiting sign-off. No code written against it yet.
**Gate:** passed. See
[`UNIVERSE_RELATION_SCHEMA_REMEDIATION_PLAN.md`](./UNIVERSE_RELATION_SCHEMA_REMEDIATION_PLAN.md)
for the model and the archive run that validated it.

## 1. What the gate established

Against the intact v5 build, 158 runs:

| measure | value |
|---|---|
| `relation_observations` | 3,333,919 |
| distinct claim classes | **2,239** |
| cross-venue classes (what the scorer uses) | 1,199 (54%) |
| **rows removed** | **1,489x** |
| dead weight in the pairwise set | 85.92% |
| IDENTITY clique defects | 0 |
| relation-agreement defects | 0 |

Both halves hold: set equality produces complete cliques on real venue data, and
every relation between two claims agrees once direction is normalized. Claim
classes grow with the market universe, not with run count, so 1,489x improves on
a full 1,202-run backfill rather than degrading.

Two defects are confirmed rather than projected: `max_relations_in_one_run` is
25,007 against a 1,000-row cap, and `max_observations_of_one_relation` is 158 —
exactly the run count, so every relation is observed in every run and
`relation_detail` starts failing at run 1,001 of 1,202.

## 2. The design

Three tables replace three. Nothing is written per run.

```sql
-- The outcome subset a market resolves YES on, within a space shape.
-- Content-addressed and participant-independent, so one row serves every event
-- and every run that expresses it.
CREATE TABLE claim_classes (
    claim_id TEXT PRIMARY KEY,
    space_shape_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    outcome_key_count INTEGER NOT NULL CHECK(outcome_key_count > 0),
    claim_identity_version INTEGER NOT NULL CHECK(claim_identity_version = 1),
    first_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id)
) STRICT;
CREATE INDEX claim_classes_shape ON claim_classes(space_shape_id, claim_id);

-- How two claims of one shape relate. No event, run, venue, or bundle.
CREATE TABLE claim_relations (
    space_shape_id TEXT NOT NULL,
    left_claim_id TEXT NOT NULL REFERENCES claim_classes(claim_id),
    right_claim_id TEXT NOT NULL REFERENCES claim_classes(claim_id),
    relation_type TEXT NOT NULL
        CHECK(relation_type IN ('IMPLICATION', 'MUTUAL_EXCLUSION')),
    algebra_version INTEGER NOT NULL CHECK(algebra_version = 1),
    PRIMARY KEY(space_shape_id, left_claim_id, right_claim_id),
    CHECK(left_claim_id <> right_claim_id)
) STRICT;

-- Which claim a venue market's tradable token expresses, and while it did.
CREATE TABLE market_claims (
    venue TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,
    claim_key TEXT NOT NULL,
    claim_id TEXT NOT NULL REFERENCES claim_classes(claim_id),
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    first_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    PRIMARY KEY(venue, venue_market_id, claim_key, claim_id),
    FOREIGN KEY(venue, venue_market_id)
        REFERENCES venue_markets(venue, venue_market_id)
) STRICT;
CREATE INDEX market_claims_claim
    ON market_claims(claim_id, event_id, venue, venue_market_id);
CREATE INDEX market_claims_event ON market_claims(event_id, claim_id);
```

### Invariants worth stating in code

1. **IDENTITY never appears in `claim_relations`.** Equal key sets are the same
   `claim_id`, so cross-venue equivalence is expressed by two `market_claims`
   rows sharing a `claim_id` — not by an edge. This is the 1,225 cross-venue
   IDENTITY relations collapsing into class membership.
2. **REVERSE_IMPLICATION is normalized away.** `normalize_relation` already
   reduces it to `(antecedent, consequent)`, so the stored type set is
   `{IMPLICATION, MUTUAL_EXCLUSION}`. OVERLAP is not stored at all.
3. **`market_claims` carries eras, not per-run rows.** A market whose semantics
   genuinely change gets a second row with its own `first_seen_run_id`; the
   current claim is the row with the greatest `last_seen_run_id`. This mirrors
   the `venue_markets` idiom rather than inventing one.
4. **Nothing is keyed by `run_id`.** That is the whole point: A.1 disappears.

### Where claims come from

Universe recomputes masks from its own rows and identifies each by its outcome
key set. It already holds every input: `venue_markets` carries `title`,
`parameters_json`, `outcome_labels_json`, `subscription_ids_json`, `market_type`
and `scope` — every field `_base_view` and `_meaningful_labels` read;
`venue_events.format` gives `best_of`; `umbrella_events.participants_json` and
`participant_keys_json` give participants and their normalized keys, so no
targeter alias config is needed. So the full history re-projects with no targeter
change, no adapter change, and no new configuration.

**The reconstruction is the one genuinely risky part**, because a bundle rebuilt
from Universe rows must compile the *same* masks the targeter compiled at report
time. Two known gaps: `venue_events` stores no per-venue participants, so
`_event_for`/`_side_label` must fall back to umbrella participants; and any field
the reconstruction misses fails silently as a missing claim rather than an error.

Mitigation, and the thing that makes this safe: **the equivalence check becomes a
permanent ingestion-time assertion.** At ingest Universe computes claims, rebuilds
the market-pair relation set from them, and compares against the report's recorded
`relationship_analysis.relationships`. Divergence on cross-venue non-OVERLAP
relations raises `EvidenceConflict` and the run does not commit. The throwaway
script becomes an invariant that holds forever, which also satisfies `AGENTS.md`'s
"prefer a visible false negative over a guessed cross-venue equivalence".

## 3. File-by-file changes

### `universe/schema/schema.sql`
- Drop `relations`, `relation_members`, `relation_observations` and their four
  indexes.
- Add the three tables above.
- **`context_relationships` is untouched.** It feeds `context_sha256`
  (`store.py:2322-2360` validates it), so changing it would break identity
  against committed reports.

### `universe/store.py`
- `SCHEMA_VERSION` 5 → 6 (`:23`).
- **Writer** (`:794-855`): replace the relation insert loop with claim
  resolution — upsert `claim_classes`, upsert `claim_relations`, upsert
  `market_claims` with first/last-seen maintenance. Add the equivalence
  assertion.
- **`run_detail`** (`:1440-1499`): relations for a run become a join over the
  run's market set (see §4.1). `counts.relations` stays; the inline array moves
  behind pagination.
- **`event_detail`** (`:1576-1617`): drop the `NOT EXISTS ... newer` subquery —
  it exists only to reconstruct current state from an append-only log. Relations
  become a self-join of `market_claims` on `event_id` through `claim_relations`,
  plus same-claim membership for equivalence.
- **`market_detail`** (`:1687-1722`): same, scoped by canonical market.
- **`relation_detail`** (`:1727-1760`): becomes claim detail. `observations`
  (one row per run) becomes the claim's member markets with their first/last-seen
  runs — bounded by the data, not by elapsed time. Fixes A.3 outright.
- **healthz counts** (`:1289`): `relations` → `claim_classes`.
- `_market_projection_identity` (`:2479`, `:2513`): projection field set changes
  if §4.2 lands.

### `universe/market_projection.py`
- `MARKET_PROJECTION_VERSION` 3 → 4.
- `_relation_rows` (`:522`) either stays as the assertion's input or is replaced
  by claim rows — see §4.2.

### `universe/api.py`
- `/v1/relations/<relation_id>` → `/v1/claims/<claim_id>` (ids become hashes, so
  the positive-integer parse at `:85-90` changes to a hex check).
- New `GET /v1/targeter/runs/<run_id>/relations?limit=&cursor=` — 1-100 limit,
  opaque cursor, per the existing list contract.
- `/v1/relationship-types` (`:429`) drops OVERLAP and REVERSE_IMPLICATION from
  the catalogue, since neither is stored.

### `analysis/claims.py`
- Keep the model: `claim_id`, `space_shape_id`, `Claim`, `ClaimRelation`,
  `usable_masks`, `derive_claims`, `derive_claim_algebra`, `normalize_relation`.
- **Remove** the old-schema verification helpers, which cannot run once
  `relation_observations` is gone: `classes_from_identity_edges`,
  `identity_clique_defects`, `compare_partitions`, `relation_members_to_edges`,
  `relation_agreement_defects`.

### `targeter-ui/src/server/event-universe.ts`
- `validateTargeterRunDetail` (`:955-1030`): `counts.relations !== relations.length`
  (`:1005`) and the "every relation carries a known `event_id`" check (`:1007`)
  both break under pagination and must be revised with the server.
- `validateRelationDetail` (`:1369`): `observations` reshapes from per-run rows to
  member markets with first/last-seen.
- `validateRelationSummary` (`:1150`): `relation_id` becomes `claim_id`, a hash
  rather than a positive integer.
- Client `observability.tsx:429-457` renders `relation.relation_id`; retitle to
  claims.
- **`detail.context.relationships`** (`observability.tsx:547-583`) is the *bundle
  context*, not this model. Unchanged.

### Removals
- `scripts/verify_claim_model.py` — reads `relation_observations`, so it dies
  with the old schema. Its job moves into the ingestion assertion.
- Its two integration tests in `tests/test_event_universe_store.py`.

## 4. Decisions needed before implementation

**4.1 — What "relations in run N" means.** Relations derive from *candidate*
bundles, not just selected ones. The candidate market set per run lives in
`candidate_decisions.eligible_market_ids_json`, so preserving today's semantics
means a `json_each` join. The cheaper alternative scopes run relations to
`selected_market_occurrences`, which is indexed and smaller but changes the
contract. **Recommend: preserve semantics via `json_each`**, and revisit only if
it measures badly.

**4.2 — Whether the projection carries claims or relations.** The projection
document is what `projection_sha256` hashes. Either it keeps emitting relations
(the assertion's input, storage derived at write time) or it emits claims
directly (smaller document, but the assertion then needs the report separately).
**Recommend: keep relations in the projection.** It keeps the assertion's two
sides independent, which is the property that makes it worth having.

**4.3 — Whether the targeter emits `claim_id`.** Not required: Universe can
recompute for all history. Emitting it later would let new runs skip
recomputation and would turn the assertion into an equality check. **Recommend:
not in this change** — one system at a time.

## 5. Tests

Each defect needs a falsifying regression that fails first, per `AGENTS.md` §3.

1. **A.1** — ingest several runs over the same markets; assert no table grows per
   run. Fails today.
2. **A.2** — a run referencing `DETAIL_ROW_LIMIT + 1` relations is served across
   pages, not as `DetailTooLarge`. Fails today.
3. **A.3** — a claim observed across `DETAIL_ROW_LIMIT + 1` runs is served. Fails
   today.
4. **Equivalence assertion** — a run whose recomputed claims contradict the
   report's relations is rejected with `EvidenceConflict`, not committed.
5. **Globality** — the same claim in two events of one shape resolves to one
   `claim_id` and one `claim_classes` row.
6. **Eras** — a market whose claim genuinely changes gets a second
   `market_claims` row, and the older one keeps its `last_seen_run_id`.
7. **Idempotence** — re-ingesting a run changes no row.
8. **Invariants** — no IDENTITY and no OVERLAP in `claim_relations`.

Existing coverage to update: `tests/test_event_universe_store.py:1175`, `:1431`,
`:1492`, `:1697`, `:1890`.

## 6. Sequencing

Four commits, each leaving the tree green:

1. Schema v6 plus the writer and the equivalence assertion.
2. The four readers and the healthz counts.
3. API: claim detail, run-relation pagination, catalogue trim; UI validators and
   client together.
4. Removals, spec updates (`EVENT_UNIVERSE_STORE_V1.md` §4 and §8,
   `universe/schema/README.md`), and marking the write-amplification document
   superseded.

Then rebuild from the archive — no targeter re-run — and verify late rather than
averaged: WAL growth per run in single-digit MB, marginal seconds-per-run flat,
full 1,202-run backfill inside a couple of hours.
