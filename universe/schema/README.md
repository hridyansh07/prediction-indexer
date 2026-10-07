# Event Universe schema

Three SQL resources, owned by [`universe/`](../README.md):

- [`schema.sql`](schema.sql): the Event Universe projection database,
  `PRAGMA user_version = 6`. It is a rebuildable query index with no in-place
  migration; startup compares the stored schema object-for-object against this
  file and fails with a rebuild instruction (stop Universe, remove the SQLite
  file and its WAL/SHM siblings, run an oldest-first backfill) for any other
  version or modification.
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
