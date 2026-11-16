# Event Universe schema

One SQL resource, owned by [`universe/`](../README.md):

- [`schema.sql`](schema.sql): the Event Universe projection database,
  `PRAGMA user_version = 8`. It is a rebuildable query index. **Schema changes
  ship with an offline migration under
  [Universe offline migrations V1](../../docs/specs/UNIVERSE_MIGRATIONS_V1.md)
  or require a rebuild. A migration never runs inside the server and never
  modifies the live file.** Startup compares the stored schema object-for-object
  against this file and names both offline migration into a new file and
  oldest-first backfill for a stale version or modification. m1 migrates exact
  v6 to v7, including STRICT `universe_migrations` and a market projection CHECK
  allowing `(3, 4)`. Version-3 rows require `m1-totals-direction` metadata and
  cannot be re-ingested; ordinary incremental sync skips an unchanged manifest,
  while force/audit and backfill still conflict. Fresh ingestion writes 4.
  A rebuild has no migration rows. An older v7 file without these objects requires
  a rebuild. m1's v7 output has no execution-terms table or execution-specific
  indexes. m2 migrates exact v7 to v8, adding `market_execution_terms`, keyed by selected
  `(run_id, venue, venue_market_id)`, with nullable `taker_delay_ms` and exact
  `observed_at`. No row means unavailable evidence; a row with a null delay is
  an observation with unknown terms. These rows are included in the run's
  bundle projection identity and audit; m2 can recompute this identity from
  retained rows without changing the unreconstructable market-projection hash.
  m2 checkpoint preparation is resumable; final commit drops its checkpoint table.
  Fresh rebuilds and ingestion write v8. Outcomes version 2 exposes collapsed history;
  [`EXECUTION_DELAY_V1.md`](../../docs/specs/EXECUTION_DELAY_V1.md) owns its contract.

The table inventory and the meaning of each group (identity, claims, bundle
history, sync ledger) is in [`../README.md`](../README.md).
