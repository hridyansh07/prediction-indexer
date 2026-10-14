# Universe offline migrations V1

Status: **proposed**. Lifts the "no in-place migrations" rule for the Event Universe
database ([universe/README.md](../../universe/README.md),
[universe/schema/README.md](../../universe/schema/README.md)) and defines the first two
migrations:

- **m1** totals direction: schema `user_version` 6 → 7
  ([UNIVERSE_TOTALS_DIRECTION_V1.md](UNIVERSE_TOTALS_DIRECTION_V1.md)).
- **m2** execution terms: 7 → 8 ([EXECUTION_DELAY_V1.md](EXECUTION_DELAY_V1.md)
  §1.1–1.2). The offline migration schema split supersedes that spec's original
  unreleased-v7 rebuild plan: v7 is exactly m1 output; execution terms start at v8.

## 0. Why

- **The rebuild is too slow.** A full oldest-first rebuild on the Universe VM is
  about 30–36 h: 5,393 runs, about 175 GB of catalogue JSON decoded to keep roughly
  2% of its rows. Each projection change would cost that again.
- **The changes are small.** m1 changes the `direction` of a handful of the 2,260
  Kalshi/Limitless totals markets, and every input it needs is already in the
  database. m2 needs only the runs' target-record artifacts for all three venues,
  not their much larger catalogues.
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
python -m universe migrate --to N SOURCE.sqlite3 DEST.sqlite3 [--resume]
```

- **Offline and copy-based.** It copies `SOURCE` to `DEST` with the SQLite online
  backup API, which gives a consistent snapshot even while the server is reading.
  It then applies each step from `SOURCE`'s `user_version` up to `N`, in order.
  `SOURCE` is never opened for writing.
- **One transaction per step.** A step's last statement sets `PRAGMA user_version`.
  A failed step rolls back, leaves `DEST` at the previous version and exits
  nonzero. m2 first commits verified-run checkpoints (§3.1); its versioned schema,
  rows and migration metadata still commit in one transaction.
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

- **The CHECK changes** from `projection_version = 3` to `IN (3, 4)`. Rows ingested
  under v3 keep version 3 and their hash. New ingestion still writes 4.
- **Version 3 rows require m1.** A version-3 row is legal only when
  `universe_migrations` holds `m1-totals-direction`. Startup validation checks this.
- **Re-ingestion of a version-3 run fails visibly.** Backfill re-ingestion and
  force/audit raise
  `EvidenceConflict("run … was migrated from projection v3; rebuild to re-ingest it")`.
  Ordinary incremental sync skips unchanged manifests, as before migration;
  changed identities conflict before any projection-version check. A v3 hash is
  never compared against a v4 hash.
- **`/v1/runs/<id>` verification** (`universe/store/reads/runs.py`) checks the bundle
  projection (`targeter_runs.projection_*`, `PROJECTION_VERSION`), not the market
  projection. m2 can fully reconstruct this identity from retained bundle rows
  and the new terms, so it verifies the old identity and updates its hash/count
  to match fresh v8 ingestion. Market-projection v3 hashes remain unchanged.

## 2. m1: totals direction (6 → 7)

### 2.1 Precondition

The source is `user_version` 6 with every `universe_run_projections` row at version 3.

### 2.2 Schema

The step creates exactly the v7 objects: `universe_run_projection_corrections` and
the changed `universe_run_projections` CHECK. SQLite cannot alter a CHECK, so that
table is recreated and copied. It also creates `universe_migrations`. The resulting
schema must equal `schema.sql` at v7, apart from the relaxed CHECK and the
migrations table. No execution-terms table or execution-specific indexes exist
at v7. A test compares `sqlite_master`.

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
  `input_complete = 1`, read its manifest and fetch the run's
  `target_records_<venue>` artifacts for all venues
  (`target_records_{polymarket,kalshi,limitless}.ndjson[.zst]`), through the existing
  verified-identity ObjectStore path. Catalogues are never fetched, even for
  metadata verification.
- **Value rules** are [EXECUTION_DELAY_V1 §1.1–1.2](EXECUTION_DELAY_V1.md#11-source)
  unchanged. m2 reuses sync's verified reader and `target_record_terms` derivation:
  - Polymarket: `taker_delay_ms = record.secondsDelay × 1000`; a missing,
    non-integer (including boolean) or negative value is null;
  - Kalshi/Limitless: one observed-null row per market present in the records.
    Null is an observation with unknown delay; absence is no evidence.
- **A run whose manifest lacks a venue's target-record artifact** contributes no
  observation for that venue. Count it per venue in the summary; never treat it
  as 0 or null. A present artifact missing a selected market's row also contributes
  no observation for that market.
- **A malformed or identity-mismatched artifact** aborts the step, with its key named.
- **Resume:** STRICT `m2_execution_terms_checkpoint` stores each fully verified
  run's derived rows, missing venues and artifact sizes. A header binds the source
  path/version, target version and code revision. `--resume` continues the same
  destination snapshot without refetching completed runs, and refuses a different
  binding or schema. Final commit drops the checkpoint table. Interruption must
  not change the rows or summary counts.
- **Summary:** rows written, complete runs processed, runs lacking artifacts per
  venue, observed rows and their null subset per venue, artifact object count and
  decoded bytes. No row is labelled observed merely because a market was selected.
- **Expected cost:** one manifest read per complete run, plus about one object per
  venue artifact present per run (at most three). Kalshi/Limitless records are
  small; estimate object count and stored/decoded bytes from the manifests, not
  catalogue sizes or a Polymarket-only extrapolation. No production manifests
  were measured for this integration. The summary reports actual target-record
  objects and decoded bytes; it excludes manifest reads.

### 3.2 Schema and data

- The step creates the STRICT `market_execution_terms` table and indexes from
  EXECUTION_DELAY_V1 §1.2:
  one row per (run, venue, venue_market_id), with `taker_delay_ms` and
  `observed_at`. Its foreign key limits rows to `selected_market_occurrences`.
- It fills the table from §3.1 and sets `user_version` 8.
- Ingestion of new runs fills the same table with d0517ea sync semantics, including
  Kalshi/Limitless observed-null rows, so fresh v8 ingestion and migration produce
  identical rows and outcomes v2 documents. `migrate --to 8` from v6 runs m1 then
  m2; a failed m2 leaves committed m1 and a v7 destination.

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

### 3.4 Command and environment

With the same archive environment already exported as for sync:

```bash
.venv/bin/python -m universe migrate --to 8 SOURCE.sqlite3 DEST.sqlite3
```

After interruption, with the same source, destination and code revision:

```bash
.venv/bin/python -m universe migrate --to 8 SOURCE.sqlite3 DEST.sqlite3 --resume
```

The command calls the same ObjectStore factory as sync. It reads `ARCHIVE_BACKEND`,
`ARCHIVE_ROOT`, `ARCHIVE_STORE_ID`, `ARCHIVE_DURABILITY` and `ARCHIVE_GCS_BUCKET`
according to the selected backend; it never loads dotenv or server config.
`UNIVERSE_CODE_REVISION` identifies the code (a checkout may resolve it from Git).
m1 alone needs no archive environment. Tests use disposable local stores only.

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
   - This is an offline test against hand-built local archive fixtures. Its two
     files and the comparison output are kept as evidence.
2. **Full file.** `rebuild_diff live.sqlite3 migrated.sqlite3`:
   - the totals changes equal §2.4;
   - there is no other `venue_markets`, `claim_*` or bundle change;
   - the execution-terms table has ingestion-equivalent rows for all venues, with
     observed/null counts per venue and no fabricated missing observations.
     `rebuild_diff.execution_terms` lists observed/unavailable markets;
     `execution_term_observations` reports old/new row counts and null subsets.
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

m1 output remains v7 and is readable by the prior v7 image; this integration's
server requires v8. m2 upgrades an m1-only v7 file to v8 the same way.

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
