# Event Universe schema

[`v1.sql`](v1.sql) is the complete Event Universe SQLite schema. Runtime code
loads it as a package resource; SQL does not live inside `universe/store.py`.

The database is an append-only selected-history index, not another evidence
archive:

- `targeter_runs` binds each indexed Targeter v3 run to exact manifest/report
  identities and a deterministic SQL projection identity;
- content-addressed `bundle_contexts` and normalized child tables deduplicate
  selected event, sibling-market, target, asset, and relationship context;
- `selection_occurrences` records every selected `(run_id, bundle_id)` and
  references a non-null complete origin occurrence; and
- `checkpoints` records incremental S3 discovery progress.

The schema has no active-snapshot, catalogue, raw segment, control, connection
epoch, venue-delivery, report JSON, or replay-plan table. Exact evidence remains
in immutable S3. Source keys and SHA-256 identities are the route back to those
bytes.

There is no migration from an earlier Universe schema because no Event
Universe database was deployed before this strict v3-only contract.
