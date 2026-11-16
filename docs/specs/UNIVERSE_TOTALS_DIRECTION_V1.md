# Universe totals direction V1

Status: **implemented** as market projection version 4. Schema v8 is current;
offline migration and operator verification are governed by
[UNIVERSE_MIGRATIONS_V1.md](UNIVERSE_MIGRATIONS_V1.md).

## Problem

`market_parameters` once set a total's `direction` by substring over a text that
names both teams, so "ThunderTalk" or "Sunderland" read as `under` and
"Overtake" as `over`. Commit `b6ab54d` fixed the parser (whole words, team names
removed first). Universe still projects markets from the archived Targeter v3
reports' stored catalogue `parameters`, and the claim engine reads
`parameters.direction` for single-token Kalshi and Limitless totals. History
can therefore hold wrong claims: `KXLOLTOTALMAPS-26AUG280500IGTT-4` ("Over 3.5
maps", IG vs ThunderTalk) expresses the under set, and the Limitless "Sunderland
and Arsenal have 3 or more total goals?" market the low-score set.

## Decision

Projection version `MARKET_PROJECTION_VERSION` is 4. The v6 → v7 offline m1
migration adds the correction table below and allows market-projection rows at
versions 3 and 4. Migrated v3 rows retain their hashes and require m1 metadata;
fresh ingestion writes v4. The v7 → v8 m2 migration adds execution terms.
Alternatively, rebuild oldest-first into a new v8 file. The archived reports
and catalogue objects remain unchanged as evidence; neither path changes the
live database in place.

### Scope

Only markets whose venue is in the single constant `TOTALS_DIRECTION_VENUES`,
currently `{kalshi, limitless}`, and whose `market_type` is `total_maps` or
`total_goals`. Every other venue, market type and field projects exactly as v3.
Polymarket is deliberately not corrected (see Known gaps).

### Rule `totals-direction-v1`

Call the fixed `targeter.v2.parsing.products.market_parameters` for the market
and take only its `direction`; override only that one field of `parameters`.
`line`, `published_line` and every other parameter keep the reported value; a
Limitless "3 or more goals" market keeps `line` 2.5 and `published_line` 3.
When the fixed rule finds no direction the field is removed and no claim is
made (fail closed). A removed direction is a visible false negative, never a
guess.

### Inputs: archived row against what the targeter saw

| Targeter input at report time | In the archived normalized row? |
|---|---|
| `title` (raw `title`, else raw `subtitle`, else ticker) | yes, `title` |
| raw `subtitle` as its own field (Kalshi) | NO (not serialized; survives only when it was the title fallback) |
| raw `yes_sub_title` (Kalshi) | partly: `outcome_labels[0]` when both labels were present (`len == 2`); with one label it cannot be told from `no_sub_title`, so it is not used |
| raw `no_sub_title` (Kalshi) | yes, `outcome_labels[1]`, not read by the rule |
| `groupItemTitle` (Polymarket only) | not applicable |
| Limitless `title` | yes, `title` (the adapter passes the market `title`, else the id) |
| Limitless raw subtitle fields | none exist: the adapter passes its full raw market, but the rule only reads `subtitle`, `yes_sub_title` and `groupItemTitle`, none of which the Limitless adapter or its tests use |
| Limitless labels (`outcomeTokens`, else "Yes"/"No") | yes, `outcome_labels`, not read by the rule |
| event participants | yes: the catalogue event row's `participants`, else the candidate's |
| raw `floor_strike` / `cap_strike` | NO (`CanonicalMarket.raw` is not serialized); they affect only `line`, which is not touched |

The re-derivation therefore calls the rule with `title`, `participants` and, for
Kalshi, `raw={"yes_sub_title": outcome_labels[0]}`. For Limitless it passes
`raw={}`: the adapter fed the rule the title alone, so no "Yes" is injected as a
subtitle. The lost raw subtitle can only turn a
direction found at report time into "none" (never invent one); that case is
removed and counted like any other correction.

### Corrections

If the re-derived direction differs from the report's (including re-derived
absent while the report had one, and the reverse), the projected value wins and
a correction is recorded. Canonical market IDs digest `parameters`, so a
corrected market gets a different canonical market ID than under v3.

Each projected run records its corrections in the projection document
(`corrections`, sorted by venue, venue_market_id, field), so the projection
SHA-256 covers them, and persists them in
`universe_run_projection_corrections` (STRICT; primary key `run_id, venue,
venue_market_id, field`; `run_id` references `targeter_runs` ON DELETE CASCADE):
`venue`, `venue_market_id`, `field` (`direction`), `report_value` (nullable),
`projected_value` (nullable), `rule_id` (`totals-direction-v1`).

### Claim verification

The report's relationships were derived from the wrong parameters, so a relation
whose member is a corrected market is not evidence against the corrected claim.
Such relations are replaced, for verification only, by relationships re-derived
from the corrected rebuilt bundle with `derive_bundle_relationships(bundle,
excluded_market_ids=...)`, using the same exclusions `project_claims` uses.
Every other relation keeps the asymmetric `verify_claims` check: invented
rejects the run (`EvidenceConflict`), missed is counted in
`claim_relation_shortfall`. The projection's stored `relations` stay as reported.

## Rollback and backup

Keep the source SQLite file untouched. Migrate offline or rebuild into a new
file, verify it, then deploy with the file-swap procedure in
[UNIVERSE_MIGRATIONS_V1 §5](UNIVERSE_MIGRATIONS_V1.md#5-deployment). Rollback
restores the retired file with its matching code version.

## Known gaps

- Polymarket totals are not corrected by this rule, by decision.
- Kalshi markets whose direction came only from the raw subtitle lose it and
  produce no claim until a later archive-side fix exists.
- Unselected candidates also project markets. Migration correction membership
  follows candidate evidence rather than only selected occurrences;
  [the migration contract](UNIVERSE_MIGRATIONS_V1.md#23-data) records its visible
  fallback and unavailable-relation handling. Review the full-file diff before
  adopting a migrated or rebuilt file.

## Verification

A read-only `python -m universe.tools.rebuild_diff OLD NEW` reports changed
venue markets, claims, affected bundles and recorded corrections. Current
production-copy and short-range serving-equivalence checks remain operator gates
under [UNIVERSE_MIGRATIONS_V1 §4](UNIVERSE_MIGRATIONS_V1.md#4-proof).
