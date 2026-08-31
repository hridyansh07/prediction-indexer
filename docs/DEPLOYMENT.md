# Docker Compose deployment

The capture deployment is one process per container and one writer per spool
lane. Containers share files, not sockets:

```text
targeter ──> data/live/targets_*.json
                         │
                         ├──> splice-polymarket ──────────┐
                         ├──> splice-limitless ───────────┤
                         ├──> splice-polymarket-snapshots ┤
                         └──> optional splice lanes ──────┤
                                                         v
                                              data/spool/venue=*/
                                                         │
                                                         v
                                                     ingester
                                                         │
                                                         v
                                      data/ingest-store/date=<UTC-day>/store.db.open
```

The target files, raw spool, and daily derived SQLite partitions are bind-mounted
from one host directory. Images are immutable; the repository is not mounted
into running containers.

## Services and profiles

`docker compose up -d` starts the capture path already verified without private
credentials:

| Service | Default | Purpose |
|---|---:|---|
| `targeter` | yes | Continuously publishes per-venue subscription targets |
| `splice-polymarket` | yes | Polymarket market WebSocket |
| `splice-limitless` | yes | Limitless market feed |
| `splice-polymarket-snapshots` | yes | Polled recovery points for Polymarket |
| `ingester` | yes | Tails all spool lanes and advances the derived fact store |
| `splice-kalshi` | `kalshi` profile | Authenticated Kalshi feed |
| `splice-polymarket-sports` | `reference` profile | Polymarket sports reference feed |
| `splice-polymarket-rtds` | `reference` profile | Polymarket RTDS reference prices |
| `ingester-integrity` | `ops` profile | One-shot store integrity check |
| `ingest-store-reaper` | `ops` profile | One-shot audit/delete of closed ingest databases older than 24 hours |
| `finalizer` | `ops` profile | Checks every minute and merges sealed windows into compressed canonical evidence |
| `finalizer-once` | `ops` profile | The same finalization sweep, once |
| `archiver` | `ops` profile | Hourly sweep publishing sealed segments and committed canonical windows as immutable objects |
| `archiver-once` | `ops` profile | The same sweep, once, for an operator or external scheduler |
| `reaper` | `ops` profile | Hourly dual-receipt audit; deletion is opt-in |
| `reaper-once` | `ops` profile | The same reaper sweep, once |
| `canonical-reaper` | `ops` profile | Hourly 18-hour-floor audit of archived canonical frames; deletion is opt-in |
| `canonical-reaper-once` | `ops` profile | The same canonical reaper sweep, once |
| `canonical-integrity` | `ops` profile | Fully decodes local canonical windows and reports archived/reaped tombstones separately |
| `targeter-v2-run-archiver` | `ops` profile, v2 override | Archives complete target-run directories that hold no receipt yet; never deletes |
| `targeter-v2-run-reaper` | `ops` profile, v2 override | Hourly receipt-proved audit of local target-run directories; deletion is opt-in |

`compose.targeter-v2.yaml` is a deliberate production override. With it,
`targeter` becomes a one-shot discover/archive/publish transaction, the
additional `targeter-v2-integrity` service audits the live generation, and
`targeter-v2-run-archiver` and `targeter-v2-run-reaper` bound the disk the run
directories occupy. The base file remains the v1 deployment when the override is
absent, and the last three services also require `--profile ops`.

Kalshi is deliberately opt-in until its splice has been exercised against real
credentials. Reference feeds are opt-in because they are not needed for the core
two-venue book capture and increase storage use.

## First deployment on Linux

Docker Engine with the Compose v2 plugin is required.

```bash
test -e .env || cp .env.example .env
```

Edit `.env` before starting:

```dotenv
CAPTURE_DATA_ROOT=/srv/prediction-indexer/data
PUID=1000
PGID=1000
```

`PUID` and `PGID` must own the data root. Using the deployment account's numeric
IDs avoids root-owned tape files:

```bash
id -u
id -g
sudo install -d -o 1000 -g 1000 /srv/prediction-indexer/data
docker compose config --quiet
docker compose build
docker compose up -d
```

Use the IDs returned by `id`, not necessarily `1000`.

The default services are independently restartable and use
`restart: unless-stopped`. A target-dependent splice waits for its own venue file;
a failed Kalshi discovery cannot hold up Polymarket or Limitless.

## Optional feeds

Start the public reference feeds:

```bash
docker compose --profile reference up -d
```

For Kalshi, keep the private key outside the repository and make it readable only
by the deployment account:

```bash
install -m 600 /path/from/kalshi/private-key.pem /srv/prediction-indexer/kalshi-private-key.pem
```

Set the host path and key ID in `.env`:

```dotenv
KALSHI_API_KEY_ID=your-key-id
KALSHI_PRIVATE_KEY_PATH=/srv/prediction-indexer/kalshi-private-key.pem
```

Then start the profile:

```bash
docker compose --profile kalshi up -d
docker compose logs -f splice-kalshi
```

The profile proves only that the container and credential mount are correct. The
first real connection still has to validate Kalshi's signing and subscription
shape against the venue.

## Targeter v2 opt-in

Targeter v2 is not a long-lived container. A host cron entry or systemd timer
runs one isolated discovery/archive/publication transaction. Every
subscription-driven splice resolves the same atomically replaced pointer, so a
generation cannot be mixed across venues.

Configure the S3 fields from `.env.example`, export credentials only if the host
does not use an instance/task role, then validate the merged deployment:

```bash
docker compose -f compose.yaml -f compose.targeter-v2.yaml config --quiet
docker compose -f compose.yaml -f compose.targeter-v2.yaml build targeter
docker compose -f compose.yaml -f compose.targeter-v2.yaml run --rm targeter
docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops \
  run --rm targeter-v2-integrity
```

Only after the integrity command succeeds should the splice services be
recreated with the same override. Schedule the exact `run --rm targeter`
command rather than `up` for recurring runs. The application lease under
`targeter-v2-runs` rejects overlapping scheduler invocations.

The full archive namespace, commit protocol, failure behavior, cron example,
rollout gate, and rollback steps are normative in
`docs/TARGETER_V2_PHASES_6_10.md`.

### Targeter v2 run retention

Every scheduled run leaves a 12–20 MB directory under `targeter-v2-runs`. At the
ten-minute cadence that is about 2 GB per day, and nothing in the base
deployment removes any of it, so both retention services have to be scheduled
alongside the publish entry:

```cron
5  * * * * cd /opt/prediction-indexer && docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops run --rm targeter-v2-run-archiver >> /var/log/prediction-targeter-v2-archive.log 2>&1
35 * * * * cd /opt/prediction-indexer && docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops run --rm targeter-v2-run-reaper   >> /var/log/prediction-targeter-v2-reaper.log  2>&1
```

Each sweep prints its record and writes it to
`/var/lib/prediction-indexer/ops/last_targeter_v2_archive_sweep.json` and
`…_reaper_sweep.json`. Run either by hand the same way:

```bash
docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops \
  run --rm targeter-v2-run-reaper
```

The archiver covers what an inline `publish` could not archive — a run whose
upload failed, or any run of a shadow deployment. It has no flag that deletes.
Its `lease_acquired: false` is not a fault: a scheduled publish held the run
lease, which happens several times an hour, and the sweep exits zero and defers.
Watch its `failed` count, which is a crashed run process an operator has to
clear, and its `pending` count, which should rise and fall rather than climb.

The reaper deletes only what an archive receipt proves is elsewhere. Two numbers
in its report matter most:

- `counts.unarchived` — runs nothing has archived. These can never be reclaimed,
  so a number that climbs means the archiver is not running and disk is not
  actually bounded.
- `counts.reapable` — runs that passed every condition and were kept only
  because deletion is not enabled. This is what enabling deletion would remove.

**Audit is the default and installing the service does not make deletion
active.** It additionally needs `TARGETER_RUN_REAPER_MODE=delete` in `.env` and
an archive declared as an independent durability domain; delete mode against a
local conformance store is refused at startup. Enable it the same way the raw
reaper is enabled: run in audit for several cycles first, confirm
`counts.unarchived` is zero and no fault reason appears, and only then switch the
mode. `TARGETER_RUN_RETENTION_HOURS` may be raised above the 18-hour floor but
not below it.

Deletion leaves `archive_receipt.json` and the directory behind as a tombstone —
that receipt is what makes the deletion auditable and what makes the next sweep
idempotent. Archive objects, receipts, and published generations are never
touched. The gate, the reason strings, and the rollout are normative in
`docs/TARGETER_V2_PHASES_6_10.md` §7.

### Discovery coverage

Publication records a first sighting for every asset it subscribes, into
`<live-root>/coverage.json`. This is the coverage-from-inception measure of
`docs/CAPTURE_SPEC.md` §6.1 — how much of a market's life the tape actually
contains — and `replay/gate1.py`'s `discovery_coverage` check reads it. No
service or cron entry is needed; it is written inside `publish_run`.

**A deployment that captured before this existed must backfill once, before
enabling run deletion.** Starting the ledger from empty is not neutral: assets
subscribed days ago would be stamped with today's date, and because
`first_seen_at` also bounds how far back the tape counts as covered, frames that
were genuinely captured would look like they predate coverage. The run reaper
reclaims the catalogues the backfill reads for venue creation times, so the
order matters.

```bash
docker compose -f compose.yaml -f compose.targeter-v2.yaml \
  run --rm --no-deps targeter \
  python -u -m scripts.backfill_coverage \
    --live-root /var/lib/prediction-indexer/live \
    --output-root /var/lib/prediction-indexer/targeter-v2-runs \
    --report /var/lib/prediction-indexer/ops/coverage_backfill.json
```

It reconstructs sightings from `<live-root>/targeter-v2/generations/<run_id>/`,
which is exactly what the splices resolved, at the instant each run id names. It
is idempotent, never moves a sighting later, and repairs one already stamped too
late. A reaped run still yields its sighting; only `created_at` is lost with the
catalogue, and an asset without one is reported unmeasurable rather than given a
lag of zero.

## Operations

Inspect status and recent logs:

```bash
docker compose ps
docker compose logs --tail 200 targeter splice-polymarket splice-limitless ingester
```

Follow one lane:

```bash
docker compose logs -f splice-polymarket
```

Restarting a splice opens a new connection epoch and resumes `delivery_index`
from its spool. A normal stop gives the splice up to 30 seconds to write its
closing control record and fsync.

Run a store integrity check without a concurrent ingest writer:

```bash
docker compose stop ingester
docker compose run --rm ingester-integrity
docker compose start ingester
```

### Ingester schema-v3 daily partition migration

The first ingester start after upgrading a schema-v1 `ingest-store/store.db`
first moves that database into the current UTC ingestion-day partition, then
builds the durable `record_identity` index from its committed facts and changes
`meta.schema_version` to `3`. This is one blocking migration before ingest or
continuity recovery starts. Failure rolls it back rather than leaving a partial
identity index, and the raw spool is unchanged. Stop the old ingester before
deploying the new binary.

The index is a `WITHOUT ROWID` table with a 32-byte binary content hash. On the
measured 2,670,449-fact store, migration took 7.10 seconds and complete startup
including continuity recovery took 20.80 seconds, with 7,220 KiB peak RSS. The
permanent identity table added 243,068,928 bytes (5.1% of the schema-v1 database).
At the transaction peak, the database plus WAL had grown by 488,061,880 bytes
(10.2%), so leave **at least 11% of the current store size free**, plus normal
operating margin. A six-day store growing at the runbook's observed 27 GiB/day is
roughly 162 GiB: this measurement projects about 9 GiB permanent growth and at
least 18 GiB of free migration headroom, but the real unique-identity ratio can
change both figures.

Migration duration scales primarily with fact and unique-identity counts, not
database bytes alone. Before the deployment window, time the new binary against a
copy of the actual production store rather than extrapolating from this sample.
After completion, stderr reports the migration duration and record count, and the
JSON report records the same values under `store_migration`. Subsequent schema-v3
starts report `store_migration: null`.

#### Fresh derived-store cutover instead of migration

Migrating the legacy database is optional. The ingest store is a derived
`file_order` projection; the finalizer, archiver, raw reaper, and analysis paths
do not read it. For a very large legacy store, stop the old ingester and move the
entire `ingest-store/` directory to a backup volume before starting the new image.
Starting with no `ingest-store/` directory creates a fresh schema-v3 daily
partition instead of entering the migration path. A rename on the same filesystem
does not release capacity, so it does not solve disk pressure by itself.

This is a new ingest-store lineage: `file_order` starts at 1, prior continuity and
duplicate/conflict history are not carried, and every sealed segment still present
under `spool/` is ingested again into the new store. Segments already removed by
the raw reaper cannot be reconstructed by `indexer-ingest`, even if their
canonical or archived evidence remains available. That does not affect those
independent evidence tiers, but it means a fresh cutover is not a byte-for-byte
historical store rebuild. Preserve `spool/`, `canonical/`, archive objects, and
their receipts; only the derived `ingest-store/` is being replaced.

After startup, `identity_records_in_memory` in the ingester report must be `0`.
Duplicate/conflict detection remains exact through the SQLite index within each
UTC ingestion-day partition; it deliberately resets at rollover. This is not an
LRU or probabilistic cache.

### Daily ingest-store retention

The ingester rotates only between complete sealed segments. The active partition
contains `active.json` and `store.db.open`. At a UTC-day boundary it checkpoints
the WAL, closes and fsyncs the database, renames it to immutable `store.db`,
fsyncs the directory, removes the active marker, and publishes `receipt.json`
last. The receipt records the closed database's exact length and SHA-256 plus a
small consumed-segment ledger. It remains after database deletion so old raw
spool files cannot be ingested twice.

Run the reaper manually in its default audit mode:

```bash
docker compose --profile ops run --rm ingest-store-reaper
sudo python3 -m json.tool \
  ${CAPTURE_DATA_ROOT}/ops/last_ingest_store_reaper_sweep.json
```

It is one-shot, not a service loop. Schedule the same command once per day,
alongside the existing host cron entries:

```cron
25 3 * * * cd /opt/prediction-indexer && docker compose --profile ops run --rm ingest-store-reaper >> /var/log/prediction-ingest-store-reaper.log 2>&1
```

`INGEST_STORE_REAPER_MODE=audit` is the default and deletes nothing. Set it to
`delete` only after reviewing several reports. The command refuses retention
below `INGEST_STORE_RETENTION_HOURS=24`, never deletes the active
`store.db.open`, and deletes a closed database only when it is at least the
configured age and still byte-identical to its valid receipt. It retains every
receipt and partition directory. This is intentionally separate from the raw
reaper: ingest databases are derived, while raw spool deletion needs independent
archive and canonical receipts.

Apply a new v1 manifest without rebuilding (base deployment only):

```bash
docker compose restart targeter
```

The manifest is mounted read-only, and running splices poll the target files for
changes.

Stop without deleting host data:

```bash
docker compose down
```

## Persistence and capacity

Everything durable is below `CAPTURE_DATA_ROOT`:

```text
live/                target files, rejections, and coverage ledger
spool/               irreversible raw NDJSON tape, sealed segments
ingest-store/        daily derived SQLite evidence/fact partitions (file_order)
canonical/           derived merged evidence per window   (EvidenceSeq)
archive-manifests/   derived replay catalog over verified archive receipts
```

`ARCHIVE_ROOT` is deliberately outside this tree — see "Raw archive and local
deletion" below.

The spool is partitioned by **lane** and split into UTC-aligned segments:

```text
spool/lane=<lane>/date=<YYYY-MM-DD>/
  <window-start>-<index>-<id>.ndjson       a sealed segment
  <window-start>-<index>-<id>.seal.json    its commit marker
  <window-start>-<index>-<id>.ndjson.open  the segment being written
```

A lane is one splice process, not a venue: Polymarket runs four of them and every
record from all four carries `venue: polymarket` in its envelope.

**The seal is what makes a segment evidence.** It carries the byte length, line
count and sha256 of exactly those bytes, so an `.ndjson` without a valid seal is
never eligible for merge or archive — that is how a reader tells "this lane had
nothing to say in this window" from "this lane has not finished the window yet".
At most one `.ndjson.open` exists per lane while capture runs, and none after a
clean stop. A crash leaves one behind; the next start repairs any torn tail and
seals it with `seal_reason: "recovery"`.

Segments span reconnects by design, so one file normally holds several
`connection_epoch` values. `SEGMENT_SECONDS` must divide 86400 evenly.

### Canonical evidence

`docker compose --profile ops run --rm finalizer-once` merges sealed windows into
cross-lane receive order:

```text
canonical/date=<YYYY-MM-DD>/window=<start_ns>/
  evidence.ndjson.zst     one checksummed frame; decoded lines remain byte-for-byte evidence
  provenance.ndjson.zst   one checksummed frame; one decoded line per position
  receipt.json        its commit marker
canonical/watermark.json
```

`watermark.json` is a **derived index over the receipts**, the same relationship
a seal has to the tape. It makes "where do I resume, what is the next position,
which windows are committed" three field reads instead of a scan over the whole
retention period. Delete it and the next run rebuilds it byte-identically from
the receipts and re-finalizes nothing; where the two disagree the receipts win.

**Two orders exist and both are honest about what they are.** The ingest store
numbers records in filename order (`file_order`), which is capture order within a
lane and meaningless across lanes. Canonical evidence numbers them on
`(visible_ns, lane_rank, delivery_index)` — that sequence is the spec's
`EvidenceSeq`. Neither is venue event order; both are capture observation order at
one host.

As with a segment, **the receipt is the commit marker**: an `evidence.ndjson.zst`
without one is a crash between two steps and is not evidence.

The receipt records decoded SHA-256, byte length and line count independently
from the compressed object's SHA-256 and length. Run the bounded-memory full
audit before replay or export:

```bash
docker compose --profile ops run --rm canonical-integrity
```

Duplicate/conflict classification uses an exact, window-scoped SQLite scratch
index named `.record-identity.sqlite.open` inside the open window directory. It
is neither canonical output nor a commit marker and is removed after every merge
attempt; a stale file after a killed process is replaced when that window is
retried. Leave temporary disk headroom proportional to the largest window. The
finalizer report's `max_identity_records_in_memory` must be `0`.

On the measured 2.67-million-record production window this reduced finalizer peak
RSS from 530,120 KiB to 13,756 KiB. Finalization took 67.52 seconds instead of
58.51 seconds, and the independent audit verified every evidence/provenance pair.

`--expect-lane` in the `finalizer` service must list exactly the splices this
deployment runs. It ships with the three ungated ones; enabling the `kalshi`
profile means adding `kalshi`, and `reference` means adding `polymarket_sports`
and `polymarket_rtds`. Get this wrong in either direction and completeness stops
meaning anything — a lane listed but never run makes every window `incomplete`,
and a lane run but not listed makes a real outage invisible.

`--window-seconds` **must match `SEGMENT_SECONDS`**, and it is the authority for
every window's bounds. Seals declare their own bounds, but a declaration is not
an authority: a torn seal leaves no end at all, a stray longer seal could re-tile
the day and hide a real window, and a seal naming a window its own records fall
outside of would otherwise still validate. With the period configured, bounds are
computed from the aligned start and every seal is checked against them — a
mismatch faults that lane rather than redefining the window.

`FINALIZATION_DEADLINE_SECONDS` (default 300) is how long a window waits for a
lane that has not delivered a **valid** seal. When it expires the window commits
anyway with the gap named in its receipt, so one wedged splice cannot halt
finalization for every healthy venue. A window that has not yet ended is never
finalized, however complete it looks.

A committed window is immutable. A segment arriving for one afterwards is
reported as `late_after_finalization` and never merged — it cannot renumber
positions or change a canonical hash (§5). Such a segment is archived like any
other and then *retained* by the reaper, since no canonical receipt names it.

One finalizer runs per canonical root, held as a `.finalize.lease` file for the
service lifetime. SIGTERM and SIGINT finish the active sweep and release it;
SIGKILL or a host crash can leave it behind. Its contents name the process that
took it, so remove a stale lease only after confirming no finalizer is running.
`FINALIZER_INTERVAL_SECONDS` defaults to 60. The latest successful or failed
sweep is written to `ops/last_finalizer_sweep.json`.

### Raw archive and local deletion

```bash
docker compose --profile ops up -d archiver        # sweeps hourly, stays up
docker compose --profile ops run --rm archiver-once   # one sweep, then exits
docker compose --profile ops up -d finalizer reaper canonical-reaper
```

The archiver compresses each sealed segment into one Zstandard frame, publishes
it beside the unchanged seal under an immutable key, verifies both objects by
reading them back, and only then writes the receipt:

```text
<ARCHIVE_ROOT>/raw/lane=<lane>/date=<YYYY-MM-DD>/
  <segment>.ndjson.zst      the compressed segment, Content-Encoding: zstd
  <segment>.seal.json       the local seal, byte for byte

spool/lane=<lane>/date=<YYYY-MM-DD>/
  <segment>.ndjson.zst      a rebuildable local derivative
  <segment>.archive.json    the archive commit marker  (durable backend)
  <segment>.archive.local.json   a conformance receipt (test backend)

archive-manifests/date=<YYYY-MM-DD>/manifest.json
```

**The receipt is the archive commit marker.** A compressed file is not one, a
key existing in the store is not one, and a successful upload is not one. A
crash at any earlier step leaves a derivative that the next sweep deletes and
rebuilds, so there is never a half-archived state to reason about.

**Raw local deletion is not active merely because this code is installed.** The
reaper deletes a raw segment and its seal only when all of these hold at the
moment it decides:

1. a structurally valid archive receipt;
2. archive data and seal objects that still match it when read back;
3. an archive backend declared an *independent durability domain*;
4. a structurally valid committed canonical `receipt.json`;
5. a canonical `inputs` entry matching the lane, source SHA-256, file name and
   segment index;
6. the local raw source and seal still matching the receipt, rehashed in full.

Anything less is retention, and the reason appears in the report
(`archive-manifests/last_reaper_sweep.json`) rather than being folded into a
backlog count. A late, excluded or never-canonicalized segment stays on disk and
stays visible; it is never guessed into a canonical window.

**Enabling the durability gate.** Two independent settings, both off by default:

```dotenv
ARCHIVE_ROOT=/srv/prediction-archive     # separate storage, not a subdirectory
ARCHIVE_DURABILITY=independent
```

and then, after the rollout gate below, deliberately enable the periodic mode:

```dotenv
REAPER_MODE=delete
```

`REAPER_MODE=audit` is the default. `reaper-once` uses the same mode and gates,
and `--delete` remains a compatibility alias for direct/manual invocations.

Both commands refuse `independent` when `ARCHIVE_ROOT` and `CAPTURE_DATA_ROOT`
resolve to the same filesystem — a "second copy" that dies with the first is not
a durability domain, whatever the flag says. With the default conformance
backend the archiver writes `.archive.local.json` receipts, which carry a
different version key precisely so a later durable deployment cannot mistake
them for proof that a remote copy exists.

**Cadence.** `ARCHIVER_INTERVAL_SECONDS` (default 3600) is how often the
long-lived `archiver` sweeps; the archive *unit* stays one sealed 30-minute
segment, so a healthy hour publishes two objects per lane and never concatenates
them. Watch mode calls the same sweep the one-shot form does — an external
scheduler running `archiver-once` on a timer is equivalent, and neither has
different eligibility logic. `spool/` therefore holds up to roughly one sweep
interval of unarchived segments on top of the finalization delay; shorten the
interval before shortening the retention.

**Immutable-key conflicts.** The archiver exits `2` and stops the sweep when a
key already holds different content, because that means the namespace or the
data is wrong rather than one segment being malformed. Nothing is overwritten.
In watch mode that exit ends the process, so a `Restarting` archiver in
`docker compose ps` means an integrity conflict rather than a busy spool — read
the last sweep's JSON before touching anything.
Investigate which producer wrote the existing object before touching it; the
local raw segment and seal are untouched and remain the recovery authority.
Exit `1` means one or more segments failed for their own reasons (a malformed
seal, a changed byte, a transient store failure) and the sweep continued.

The same sweep also discovers committed canonical windows. Those files are
already V1 Zstandard frames, so the archiver strictly decodes them against the
finalizer's receipt rather than recompressing them. It publishes the two exact
frames and unchanged receipt under:

```text
canonical/date=<YYYY-MM-DD>/window=<start>/
  evidence.ndjson.zst
  provenance.ndjson.zst
  receipt.json
```

Fresh object-store metadata verifies all three complete object expectations
before the local window receives `canonical_archive_receipt.json`. The local
backend uses `canonical_archive_receipt.local.json`, which is conformance
evidence only.

Canonical deletion is a third, separate authority:

```bash
docker compose --profile ops run --rm canonical-reaper-once
```

It defaults to `CANONICAL_REAPER_MODE=audit`. A window is reapable only when a
production canonical archive receipt binds its unchanged `receipt.json` and
both frame identities, the backend is independently durable, all three remote
objects pass fresh metadata verification against that receipt, and the window
is at least `CANONICAL_REAPER_RETENTION_HOURS` old. The command refuses a value
below 18.
Age is measured from the latest of window end, finalization, archive
verification, and both receipt mtimes, so a backdated test clock cannot shorten
retention.

Delete mode removes only `evidence.ndjson.zst` and
`provenance.ndjson.zst`. It permanently retains the window directory,
`receipt.json`, and `canonical_archive_receipt.json`; those compact files are
the tombstone the finalizer needs to rebuild the watermark and preserve global
sequence/continuity after restart. Canonical integrity reports these windows as
`windows_archived_and_reaped` and does not count their unavailable records as
locally verified; a crash between the two unlinks is separately visible as
`windows_partially_reaped`. Enable `CANONICAL_REAPER_MODE=delete` only after the
same cloud-backend soak and audit review required for raw deletion.

### Cloud archive backends

Both `archiver` and `reaper` build their object store through one factory,
`archive/storage/factory.py`, which reads `ARCHIVE_BACKEND` (`local`, the
default, `s3`, or `gcs`). The provider contracts are
`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md` and
`archive/GCS_RAW_ARCHIVE_ADAPTER_V1.md`; this is the operator summary.

```dotenv
ARCHIVE_BACKEND=s3
ARCHIVE_S3_BUCKET=my-dedicated-archive-bucket
ARCHIVE_S3_REGION=us-east-1
ARCHIVE_S3_EXPECTED_OWNER=123456789012   # the bucket-owning account, 12 digits
```

For native Google Cloud Storage:

```dotenv
ARCHIVE_BACKEND=gcs
ARCHIVE_GCS_BUCKET=my-dedicated-archive-bucket
```

GCS has no server-side SHA-256. The adapter calculates SHA-256 over the exact
conditional resumable-upload stream while the GCS client and service validate
CRC32C. It stores that SHA-256 and byte length as custom metadata and records
the service-returned CRC32C separately in the receipt. Normal `head`, archive
verification, and reaper checks compare current provider metadata with that
closed receipt without downloading object bodies. Retrieval pins a generation
and verifies SHA-256 plus CRC32C while consuming the complete object.

All three `ARCHIVE_S3_*` values are required together; the factory refuses to
start with only some of them set, and separately refuses to start if any of
them is non-empty while `ARCHIVE_BACKEND` is still `local` — both are
configuration mistakes worth failing loudly on rather than guessing past.
`ARCHIVE_GCS_BUCKET` is required for `gcs`; the factory rejects mixed provider
options rather than guessing. All provider configuration reaches the process
through environment values rather than repeated command arguments. Compose
passes `ARCHIVE_ROOT`, `ARCHIVE_STORE_ID`, and `ARCHIVE_DURABILITY` for all
backends; the factory ignores them once S3 or GCS is selected, and either cloud
backend is always the `independent_durable` class. `ARCHIVE_DURABILITY` cannot
downgrade it, and the archiver writes provider-neutral production
`.archive.json` receipts.

Credentials are never set in `.env`. On AWS, prefer an instance or task role
scoped to exactly `s3:ListBucket` on the bucket and
`s3:PutObject`/`s3:GetObject` on `bucket/raw/*`, `bucket/canonical/*`, and
`bucket/targeter-v2/*`, with no delete permission. All three prefixes are
required: sealed capture archives under `raw/`, finalized windows under
`canonical/`, and Targeter v2 run directories under `targeter-v2/`. The exact
policy documents are in
`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md` §12.3.

If the Compose host instead uses temporary or static environment credentials,
export `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and (for temporary
credentials) `AWS_SESSION_TOKEN` in the shell that invokes Compose. Compose
forwards them only to `archiver`, `archiver-once`, `reaper`, `reaper-once`,
`canonical-reaper`, and `canonical-reaper-once`, plus — when the v2 override is
included — `targeter`, `targeter-v2-integrity`,
`targeter-v2-run-archiver`, and `targeter-v2-run-reaper`. It does not forward
them to venue splices or the ingester. A host `~/.aws` directory is not mounted
into the containers. For an EC2 instance role, ensure IMDS is reachable from
bridge containers (including a sufficient IMDSv2 response hop limit).

The bucket itself needs Block Public Access enabled, versioning on, default
encryption on, and a policy requiring `If-None-Match: *` on writes under
`raw/`, `canonical/`, and `targeter-v2/` — see
`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md` §12 for
the full checklist and the JSON.

On GCE, attach a dedicated service account to the VM and let the container use
Application Default Credentials. Grant a bucket-scoped custom role containing
only `storage.objects.create`, `storage.objects.get`, and
`storage.objects.list`, or combine the bucket-level predefined Storage Object
Creator and Storage Object Viewer roles. Do not grant object deletion or update.
Use a private, dedicated regional GCS bucket with uniform bucket-level access
and public access prevention. Keep lifecycle expiry disabled during rollout. No
GCP credential file or value belongs in `.env`.

Switching `ARCHIVE_BACKEND` from `local` to `s3` or `gcs` does not touch the
reaper's own gate: `REAPER_MODE=delete` is still explicit, and the backend's
fixed `INDEPENDENT` durability satisfies condition 3 of the six above by
construction. Run the selected archiver with reaper deletion disabled for at
least 24 hours, sample every lane against retained local raw, and only then
consider enabling destructive reaper runs.

**Still out of scope.** Object-store lifecycle expiry is not configured, so the
archive grows until a later retention policy is enabled. With the local
conformance backend, total local storage is *not*
bounded — the bytes have changed representation and directory, nothing more;
a cloud backend does not bound the spool until `REAPER_MODE=delete` is enabled.

Back up `spool/` first. The ingest store and canonical evidence are both derived
and rebuildable from it; the spool cannot be reconstructed from either. Measured: about 6.8 GB/day uncompressed
for 20 Polymarket assets, and **about 42 GB/day for Kalshi at full ladder width**
— roughly ten times the record count of everything else combined. Size disk from
the Kalshi figure, not the Polymarket one.

Container logs rotate at 25 MB with five files per service by default. Override
`LOG_MAX_SIZE` and `LOG_MAX_FILES` in `.env` if the host has a central log
collector.

## Event Universe deployment

Event Universe is a separate small-server deployment, not another process on
the capture/ingester host. `compose.universe.yaml` uses the dedicated
`docker/universe.Dockerfile`, mounts only its persistent SQLite volume and
`configs/event_universe.json`, and starts the read server with the image's
default command:

```bash
docker compose -f compose.universe.yaml up -d event-universe
```

The JSON config holds the database path, API listener, temporary directory, and
backup destination. Object-store selection is environment-owned and uses the
same provider-neutral `ARCHIVE_BACKEND` factory as Targeter and the archivers.
For local operation, `EVENT_UNIVERSE_ARCHIVE_ROOT` is mounted at
`/var/lib/archive`. For S3 set all three `ARCHIVE_S3_*` values; for GCS set
`ARCHIVE_GCS_BUCKET`. AWS credentials use boto3's standard provider chain and
GCS uses Application Default Credentials; prefer attached workload identities.

Incremental ingestion and backup remain scheduler-owned one-shot jobs, but they
are direct scripts with no argument parser:

```bash
python universe/run_sync.py
python universe/run_backup.py
```

The store is a rebuildable event/market view of committed Targeter runs. It
normalizes cross-venue umbrella events, venue-native events, canonical market
classes, venue market instances, candidate decisions, selected-market
occurrences, relationships, and exact source/origin provenance. It does not
copy raw catalogues or selection reports. `universe/schema/v1.sql` preserves
the historical bundle APIs and `universe/schema/v3.sql` owns the normalized
event/market view. There is no cadence cache.

Schema v3 intentionally does not migrate an existing database. Before rolling
out this version, stop the API and Universe jobs, remove the rebuildable SQLite
file plus its `-wal`/`-shm` siblings, start the service to create schema v3, and
run sync/backfill from the immutable archive. A v1/v2 database is rejected with
a rebuild instruction.

Event Universe is strict Targeter v3-only. Incremental sync discovers immutable
version-2 run manifests and derives selected occurrences directly from each
manifest-owned v3 `selection_report.json[.zst]`. Existing archived v3 runs need
no Universe sidecar or producer backfill. Retained selections recursively verify
their exact immutable v3 origin manifests, including origins outside the
requested range. There is no Universe publication pointer; `/healthz` derives
the latest indexed archived run and marks it stale from that run's
`generated_at`.

Historical **selected-run** rollout uses `python universe/run_backfill.py` after
`backfill.generated_start` and `backfill.generated_end` are explicitly set in
the JSON config. The bounds are half-open Targeter generated times. Backfill and
incremental sync share one idempotent projector and ingestion transaction. Raw
segment selection and trust remain replay responsibilities; the archiver has no
Universe sidecar or receipt-mirror service.

`EVENT_UNIVERSE_DATA_ROOT` must be an attached persistent volume and should be
backed up independently. `EVENT_UNIVERSE_BIND_ADDRESS` defaults to loopback; use
a private interface or authenticated reverse proxy when exposing the API.
The UI consumes `GET /v1/targeter/status?limit=5`, a compact landing-page view
containing only freshness, latest/current-complete run summaries, and selected
counts. `GET /v1/targeter/runs/<run_id>` returns bounded normalized decisions
and references. Event, market, and relationship detail is available from
`/v1/events`, `/v1/markets/<market_id>`, and `/v1/relations/<relation_id>`.
`GET /v1/targeter/cadence` has been removed and returns 404. Universe and the UI
proxy enforce a 1.75 MB serialized response budget; list limits are capped at
100.

## Clock and liveness semantics

Linux containers share the host kernel's `CLOCK_MONOTONIC` and boot ID. Each
splice records `/proc/sys/kernel/random/boot_id` in `connection_opened`, so
monotonic timestamps from different containers are comparable only when that
recorded scope ID matches.

There is intentionally no synthetic "healthy" check based only on process
existence. A quiet market and a silently stalled socket can look identical from
outside the protocol. Docker restarts crashed processes; operational monitoring
must additionally watch spool recency, reconnect control records, and
records-per-subscribed-market.
