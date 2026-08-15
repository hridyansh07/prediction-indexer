# Event Universe schema

`v1.sql` is the complete reviewable SQLite schema. Runtime code loads it as a
package resource; SQL does not live inside `universe/store.py`.

The database is a sparse query index, not another evidence archive:

- one Targeter source row per processed selected-bundle index;
- rows only for bundles selected by Targeter and their event, market, asset,
  and relationship references;
- one raw-segment row per immutable segment universe receipt;
- one normalized row per control envelope, never per venue frame;
- no report, catalogue, target-record, envelope, or other source JSON columns.

Exact evidence remains in immutable object storage. Source keys and SHA-256
identities in this schema are the route back to those bytes. The planned capture
end is the Targeter policy bound, not an observed event close or a completeness
claim.
