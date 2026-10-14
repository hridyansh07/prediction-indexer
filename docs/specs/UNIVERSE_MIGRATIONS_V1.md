# Universe offline migrations V1

Status: **proposed**. Lifts the "no in-place migrations" rule for the Event Universe
database ([universe/README.md](../../universe/README.md),
[universe/schema/README.md](../../universe/schema/README.md)) and defines the first two
migrations:

- **m1** totals direction: schema `user_version` 6 → 7
  ([UNIVERSE_TOTALS_DIRECTION_V1.md](UNIVERSE_TOTALS_DIRECTION_V1.md)).
- **m2** execution terms: 7 → 8 ([EXECUTION_DELAY_SDK_V1.md](EXECUTION_DELAY_SDK_V1.md)
  §1).

## 0. Why

- **The rebuild is too slow.** A full oldest-first rebuild on the Universe VM is
  about 30–36 h: 5,393 runs, about 175 GB of catalogue JSON decoded to keep roughly
  2% of its rows. Each projection change would cost that again.
- **The changes are small.** m1 changes the `direction` of a handful of the 2,260
  Kalshi/Limitless totals markets, and every input it needs is already in the
  database. m2 needs only the runs' `target_records_polymarket` artifacts: about
  70 KB per run, against about 33 MB for that run's Polymarket catalogue.
- **What stays the same:**
  - The archive is still the evidence.
  - A full rebuild remains valid and remains the canonical path.
  - A migrated file must serve exactly what a rebuild of the same code would serve
    (§4).
- **What changes:** a migrated file is not byte-identical to a rebuilt one. Per-run
  projection hashes stay as ingested (§1.3).

## 1. Mechanism

### 1.1 Command

```
python -m universe migrate --to N SOURCE.sqlite3 DEST.sqlite3 [--input NAME=PATH ...]
```

- **Offline and copy-based.** It copies `SOURCE` to `DEST` with the SQLite online
  backup API, which gives a consistent snapshot even while the server is reading.
  It then applies each step from `SOURCE`'s `user_version` up to `N`, in order.
  `SOURCE` is never opened for writing.
- **One transaction per step.** A step's last statement sets `PRAGMA user_version`.
  A failed step rolls back, leaves `DEST` at the previous version and exits
  nonzero.
- **No skipping.** Each step is a pure function of the database and its declared
  inputs. A step whose precondition fails (§2.1, §3.1) aborts.
- **The summary.** It prints one JSON summary per step and stores it in
  `universe_migrations` (§1.2).
- **Deployment is a file swap**, like a rebuild (§5). The server never migrates its
  own file at startup. Startup keeps rejecting a stale `user_version` with an
  instruction that now names both `migrate` and rebuild.

### 1.2 `universe_migrations`

A new STRICT table, created by m1:

| Column | Type | Meaning |
|---|---|---|
| `migration_id` | TEXT PK | `m1-totals-direction`, `m2-execution-terms` |
| `from_user_version`, `to_user_version` | INTEGER | for example 6, 7 |
| `code_revision` | TEXT | the git commit of the image that ran it |
| `inputs_json` | TEXT, json_valid | each declared input's name, sha256 and byte length; `{}` if none |
| `summary_json` | TEXT, json_valid | the step's summary |
| `applied_at_ns` | INTEGER | wall clock, diagnostic |

- A rebuilt file has no rows in this table, and a migrated file has one row per
  step applied. `rebuild_diff` reports it.
- `/healthz` adds `migrations: [migration_id, …]`.

### 1.3 Per-run projection rows

`universe_run_projections` stores, for each run, a hash over that run's whole
projected market document: the projection version plus its venue-market rows,
including that run's volumes. The database keeps only the latest volumes, so a
migration cannot recompute these hashes.

- **The CHECK changes** from `projection_version = 4` to `IN (3, 4)`. Rows ingested
  under v3 keep version 3 and their hash. New ingestion still writes 4.
- **Version 3 rows require m1.** A version-3 row is legal only when
  `universe_migrations` holds `m1-totals-direction`. Startup validation checks this.
- **Re-ingestion of a version-3 run fails visibly.** Re-ingesting one, by backfill
  over an old range or by a sync replay, raises
  `EvidenceConflict("run … was migrated from projection v3; rebuild to re-ingest it")`.
  It is never skipped silently, and never compared against a v4 hash.
- **`/v1/runs/<id>` verification** (`universe/store/reads/runs.py`) is unaffected. It
  checks the bundle projection (`targeter_runs.projection_*`, `PROJECTION_VERSION`),
  not the market projection.

## 2. m1: totals direction (6 → 7)

### 2.1 Precondition

The source is `user_version` 6 with every `universe_run_projections` row at version 3.

### 2.2 Schema

The step creates exactly the v7 objects: `universe_run_projection_corrections` and
the changed `universe_run_projections` CHECK. SQLite cannot alter a CHECK, so that
table is recreated and copied. It also creates `universe_migrations`. The resulting
schema must equal `schema.sql` at v7, apart from the relaxed CHECK and the
migrations table. A test compares `sqlite_master`.

### 2.3 Data

- **Re-derive with the ingestion code.** For each `venue_markets` row with `venue`
  in `TOTALS_DIRECTION_VENUES` and `market_type` in `{total_maps, total_goals}`,
  call the projection's own `_corrected_totals_parameters`. Use the same function,
  not a copy.
  - Its inputs are the row's `title` and `outcome_labels_json`, plus the event
    participants that ingestion passes (`_event_participants`). The step takes
    those from the same stored source that ingestion's context carries.
  - If a market's participants can't be reconstructed, the step aborts and names
    the market, rather than guessing.
- **Corrected rows:** `parameters_json` gets the corrected direction. A correction
  row goes into `universe_run_projection_corrections` for each run in which
  ingestion would have recorded one.
  - If per-run membership can't be reconstructed exactly from stored rows, the
    step records the correction for every run with a `selected_market_occurrences`
    row for that market. §4 reports any difference.
- **Claims:**
  - For the bundles containing a corrected market, the step re-derives claims and
    verification relations through `project_claims` / `rebuild_bundle` with the
    corrected markets, as `claim_projection.py` does at ingestion.
  - It rewrites the affected `market_claims` and `claim_relations` rows, and the
    `claim_classes` rows only those reference. `first_seen_run_id` and
    `last_seen_run_id` keep the span of the markets' occurrences.
  - A `claim_classes` row left unreferenced is deleted only if ingestion would
    never have created it.
- **Summary:** the markets examined and changed, by venue; correction rows written;
  claims and relations rewritten; and the changed list
  (`venue:venue_market_id`, `report_value → projected_value`).

### 2.4 Expected result

The changed list equals the acceptance set in UNIVERSE_TOTALS_DIRECTION_V1 "Known
gaps":

- the 7 Kalshi `total_maps` markets;
- the 3 Limitless Sunderland markets;
- and, decided by the rule, the 2 uninspected Sunderland bundles.

Anything else stops the deployment until it has been reviewed.

## 3. m2: execution terms (7 → 8)

### 3.1 Source

- **Archived target records only.** For every run in `targeter_runs` with
  `input_complete = 1`, read its manifest and fetch only
  `target_records_polymarket.ndjson[.zst]`, through the existing verified-identity
  ObjectStore path. Catalogues are never fetched.
- **Value rules** are EXECUTION_DELAY_SDK_V1 §1.1 unchanged:
  - `taker_delay_ms = record.secondsDelay × 1000`;
  - a missing, non-integer or negative value is null;
  - Kalshi and Limitless have no rows, which means unknown.
- **A run whose manifest has no `target_records_polymarket` artifact** contributes no
  observation. It is counted in the summary, never treated as 0.
- **A malformed or identity-mismatched artifact** aborts the step, with its key named.
- **Resume:** a checkpoint table, dropped at commit, records the runs processed, so
  an interrupted step resumes without refetching. The step's outcome must not
  depend on interruption.
- **Expected cost:** about 5,400 small objects, about 0.4 GB, read from the
  server's own region with no egress charge. That's minutes to about an hour on the
  e2-small.

### 3.2 Schema and data

- The step creates the execution-terms table EXECUTION_DELAY_SDK_V1 §1.2 defines:
  one row per (run, venue, venue_market_id), with `taker_delay_ms` and
  `observed_at`.
- It fills the table from §3.1 and sets `user_version` 8.
- Ingestion of new runs fills the same table from the same artifact, so a rebuild at
  v8 and a migration to v8 produce the same rows.

### 3.3 Not the public API

The public Gamma API still serves closed markets with `secondsDelay`. Checked: market
3580378, closed on 15 Aug, returns 1, and the archived record from that run says 1.
It is not the source, for three reasons:

- it gives only today's value, not the per-run history that the outcomes document's
  `changes` are built from;
- 25,194 Polymarket markets is many paced requests, about the same time as reading
  the archive;
- an API value is not archived evidence.

`universe.tools.check_execution_terms` (optional) compares a sample of migrated
markets with the API and only reports. It writes nothing.

## 4. Proof

1. **Equivalence on a short range.** Choose a half-open range of a few dozen runs
   containing at least one corrected Kalshi bundle (`7eadbbc9…`, 28 Aug) and one
   corrected Limitless bundle.
   - Build it twice from an empty database with the same range: once with the v6
     code, then `migrate --to 8`; once with the v8 code.
   - Every table must be equal row for row, excluding:
     - `universe_run_projections.projection_version` and `projection_sha256`;
     - `universe_migrations`;
     - `indexed_at_ns`.
   - If §2.3's fallback for correction membership applies, its differences are
     listed and accepted only if they are confined to `universe_run_projection_corrections`.
   - This is an offline test against the archive. Its two files and the comparison
     output are kept as evidence.
2. **Full file.** `rebuild_diff live.sqlite3 migrated.sqlite3`:
   - the totals changes equal §2.4;
   - there is no other `venue_markets`, `claim_*` or bundle change;
   - the execution-terms table has rows for Polymarket only, with observed and
     unavailable counts per venue.
3. **Served documents.**
   - On the migrated file, served locally: the outcomes documents of the corrected
     bundles show `over`.
   - The outcomes document of a Polymarket bundle shows `execution_terms` matching
     its archived records.
   - `/healthz` lists the migrations.
4. **Unit tests**, offline and on small hand-built databases:
   - each step's precondition and abort paths;
   - the transaction rollback;
   - the schema-equality check;
   - the version-3 re-ingestion conflict;
   - m2's null rules and missing-artifact counting;
   - resume after interruption giving identical rows.

## 5. Deployment

1. Build the image. Pause the sync and game-state cron entries and wait for running
   jobs.
2. On the VM, run `migrate --to N` from the live file into
   `/srv/event-universe/build7/`. The live server keeps serving while this runs.
   Check §4.2.
3. Stop `event-universe`. Rename the live file to
   `event-universe.sqlite3.v<old>-retired`. Move the migrated file into place and
   start the new image. Point the cron entries at the new tag and unpause them.
4. **Rollback:** stop, restore the retired file, and start the previous image.

m1 can ship alone (to 7) before A2 lands. m2 then migrates 7 → 8 the same way.

## 6. Rules after this spec

- "No in-place migrations" becomes: **"Schema changes ship with an offline migration
  under this spec or require a rebuild. A migration never runs inside the server and
  never modifies the live file."**
- `universe/README.md`, `universe/schema/README.md` and `docs/DEPLOYMENT.md` "Safe full
  rebuild ordering" are updated to say so.
- A rebuild stays the canonical answer when a migration's inputs aren't in the
  database or the archive's small artifacts. It is also the answer when serving
  equivalence can't be shown on a short range.

## 7. Not in V1

- Recomputing per-run market-projection hashes.
- Migrating downward.
- Migrating a file older than v6.
- Running a migration from the sync or backfill jobs.
