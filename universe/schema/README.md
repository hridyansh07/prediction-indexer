# Event Universe schema

Runtime schema version 4 applies the single canonical [`schema.sql`](schema.sql)
package resource. It contains both the historical run/bundle API tables and the
event/market view: umbrella and venue events, canonical and venue markets,
candidate decisions, selected-market occurrences, and n-ary relationships.

The database is a rebuildable query index, not another evidence archive:

- `targeter_runs` binds every run to exact manifest/report identities;
- `universe_run_projections` binds its deterministic market projection;
- `umbrella_events` group the exact sorted native-event reference set with
  stable sport/game/topology/participant semantics; activation observations are
  versioned separately in `event_observations`;
- `canonical_markets` group venue markets under explicit market-template and
  outcome-space versions;
- `relations` plus `relation_members` normalize symmetric or directed n-ary
  market relations; and
- the v1 occurrence/context tables preserve historical bundle APIs and exact
  continuity/retirement provenance.

There is no cadence cache, active snapshot, raw report/catalogue, raw segment,
control, connection-epoch, venue-delivery, or replay-plan table. Exact evidence
remains in the immutable configured ObjectStore.

Schema v4 deliberately has no in-place migration. Stop Universe, remove an
existing v1/v2/v3 SQLite file and its WAL/SHM siblings, then run backfill from
the immutable archive. Runtime retains `PRAGMA user_version = 4` and rejects any
older or modified schema with that rebuild instruction.
