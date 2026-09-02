# Event Universe schema

Runtime schema version 4 applies [`v1.sql`](v1.sql) and [`v4.sql`](v4.sql) as
package resources. V1 retains the historical run/bundle API tables. V4 adds the
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

Schema v4 deliberately has no in-place migration. Remove an existing v1/v2/v3
SQLite file and rebuild it from archived runs. Runtime rejects any older or
modified schema rather than guessing compatibility.
