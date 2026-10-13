# Universe totals direction V1

Status: `proposed`. Not implemented until the code and tests say so.

## Problem

`market_parameters` once set a total's `direction` by substring over a text that
names both teams, so "ThunderTalk" or "Sunderland" read as `under` and
"Overtake" as `over`. Commit `b6ab54d` fixed the parser (whole words, team names
removed first). Universe still projects markets from the archived Targeter v3
reports' stored catalogue `parameters`, and the claim engine reads
`parameters.direction` for single-token Kalshi totals. History therefore holds
wrong claims: `KXLOLTOTALMAPS-26AUG280500IGTT-4` ("Over 3.5 maps", IG vs
ThunderTalk) expresses the under set.

## Decision

Projection version `MARKET_PROJECTION_VERSION` 3 becomes 4. Because Universe has
no in-place migration, the schema changes with it: `PRAGMA user_version` 6
becomes 7, `universe_run_projections.projection_version` is `CHECK(= 4)`, and
one table is added (below). A fresh oldest-first backfill from the archive is
required. The archived reports and catalogue objects are never touched; they stay
as evidence.

### Scope

Only `venue == "kalshi"` markets whose `market_type` is `total_maps` or
`total_goals`. Every other venue, market type and field projects exactly as v3.
Polymarket and Limitless are deliberately not corrected (see Known gaps).

### Rule `kalshi-totals-direction-v1`

Call the fixed `targeter.v2.parsing.products.market_parameters` for the market
and take only its `direction`; override only that one field of `parameters`.
When the fixed rule finds no direction the field is removed and no claim is
made (fail closed). A removed direction is a visible false negative, never a
guess.

### Inputs: archived row against what the targeter saw

| Targeter input at report time | In the archived normalized row? |
|---|---|
| `title` (raw `title`, else raw `subtitle`, else ticker) | yes, `title` |
| raw `subtitle` as its own field | NO (not serialized; survives only when it was the title fallback) |
| raw `yes_sub_title` | partly: `outcome_labels[0]` when both labels were present (`len == 2`); with one label it cannot be told from `no_sub_title`, so it is not used |
| raw `no_sub_title` | yes, `outcome_labels[1]`, not read by the rule |
| `groupItemTitle` (Polymarket only) | not applicable |
| event participants | yes: the catalogue event row's `participants`, else the candidate's |
| raw `floor_strike` / `cap_strike` | NO (`CanonicalMarket.raw` is not serialized); they affect only `line`, which is not touched |

The re-derivation therefore calls the rule with `title`, `participants` and
`raw={"yes_sub_title": outcome_labels[0]}`. The lost raw subtitle can only turn a
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
`projected_value` (nullable), `rule_id` (`kalshi-totals-direction-v1`).

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

The existing schema-v6 SQLite file is the rollback and the backup: keep it
untouched. Universe has no in-place migration and v4 needs a fresh rebuild into a
new file. To roll back, stop Universe and run the v3 code against the old file.

## Known gaps

- Polymarket and Limitless totals are not corrected by this rule, by decision.
- Affected Polymarket/Limitless totals found by the totals scan: (to be listed
  here later).
- Kalshi markets whose direction came only from the raw subtitle lose it and
  produce no claim until a later archive-side fix exists.

## Verification

A read-only `python -m universe.tools.rebuild_diff OLD NEW` reports changed
venue markets, claims and affected bundles between a v6/v3 and a v7/v4 file,
and the new file's recorded corrections.
