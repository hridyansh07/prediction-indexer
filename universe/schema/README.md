# Event Universe schema

`v3.sql` is the complete reviewable SQLite schema. Runtime code loads it as a
package resource; SQL does not live inside `universe/store.py`.

The database is a sparse query index, not another evidence archive:

- one atomically replaced snapshot for the latest complete archived Targeter
  v3 run;
- complete rows only for its active selected bundles, including verified
  immutable origin provenance and normalized event, market, asset, and
  relationship context;
- one raw-segment row per immutable segment universe receipt;
- one normalized row per control envelope, never per venue frame;
- no report, catalogue, target-record, envelope, or other source JSON columns.

Exact evidence remains in immutable object storage. Source keys and SHA-256
identities in this schema are the route back to those bytes. The planned capture
end is the Targeter policy bound, not an observed event close or a completeness
claim. There is no migration from an earlier Universe schema: no Event Universe
version was deployed before this strict v3 contract.
