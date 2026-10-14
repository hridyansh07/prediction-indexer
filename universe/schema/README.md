# Event Universe schema

Three SQL resources, owned by [`universe/`](../README.md):

- [`schema.sql`](schema.sql): the Event Universe projection database,
  `PRAGMA user_version = 7`. It is a rebuildable query index. **Schema changes
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
  a rebuild. m2 remains an
  aborting stub pending the missing `EXECUTION_DELAY_SDK_V1` §1.2 table contract;
  this resource does not define or claim to support v8.
- [`replay_auth.sql`](replay_auth.sql) and [`replay_jobs.sql`](replay_jobs.sql):
  additive component schemas initialized in the durable Replay database
  (`replay.database_path`, `jobs.sqlite3`). They hold bearer-session digests, the
  member allowlist, append-only allowlist events, job rows, idempotency
  mappings, component metadata and append-only job transition events. The file
  does not use `user_version`; each component validates only its own objects
  (replay jobs via a SHA-256 over its ordered, whitespace-normalized
  `sqlite_master` definitions). SIWE nonces are not persisted. These records are
  not rebuildable: never place them in the projection database or remove them
  during a projection rebuild.

The table inventory and the meaning of each group (identity, claims, bundle
history, sync ledger) is in [`../README.md`](../README.md).
