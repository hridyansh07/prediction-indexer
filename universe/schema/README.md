# Event Universe schema

[`v1.sql`](v1.sql) is the durable selected-history schema. [`v2.sql`](v2.sql)
adds the disposable newest-five cadence cache. Runtime code applies both as
package resources and upgrades existing v1 databases transactionally; SQL does
not live inside `universe/store.py`.

The database is an append-only selected-history index, not another evidence
archive:

- `targeter_runs` binds each indexed Targeter v3 run to exact manifest/report
  identities and a deterministic SQL projection identity;
- content-addressed `bundle_contexts` and normalized child tables deduplicate
  selected event, sibling-market, target, asset, and relationship context;
- `selection_occurrences` records every selected `(run_id, bundle_id)` and
  references a non-null complete origin occurrence;
- `bundle_retirements` records proven all-terminal or safety-clamp observations
  and references their exact complete origin context; and
- `checkpoints` records incremental object-store discovery progress; and
- `cadence_runs` caches compact operational projections for exactly the newest
  five runs, including incomplete and empty runs.

The schema has no active-snapshot, raw catalogue, raw segment, control,
connection-epoch, venue-delivery, report JSON, or replay-plan table. Exact
evidence remains in the immutable configured ObjectStore. Source keys and
SHA-256 identities are the route back to those bytes.

V1 selected-history rows remain unchanged. The v2 migration creates only the
rebuildable cache; sync repopulates missing newest-run projections from verified
Targeter reports.
