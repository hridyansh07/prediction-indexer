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

The version-2 JSON config holds the rebuildable Universe database path, API
listener, temporary directory, backup destination, and a separate durable
`replay.database_path` for authentication and Replay state. The server initializes
the additive authentication schema in `jobs.sqlite3`; rebuilding
`event-universe.sqlite3` must never remove or replace that file. Roll out the
version-2 config and Universe image together because older configs are rejected
actionably. Before exposing the authentication routes, replace the shipped zero
admin placeholder with the operator wallet's EIP-55 address and set
`siwe_domain`/`siwe_uri` to the exact UI origin users sign on (for a Vercel UI,
`siwe_domain` is `<project>.vercel.app` and `siwe_uri` starts with
`https://<project>.vercel.app/`), not the API host. While the zero
placeholder remains, the sign-in routes return `503` and every other route is
unaffected. `siwe_statement` is the exact statement line every sign-in message
must carry; the UI must use the same text.

SIWE verification is offline EIP-191 recovery and makes no wallet RPC or provider
request, so the admin must be an externally owned account, not a contract wallet.
Nonces are single use and held only in the server process's memory, capped at
500 outstanding, and expire `nonce_ttl_seconds` after the server issued them;
the client's `Issued At` is not compared with the server clock. A restart drops
outstanding nonces and users simply sign in again. This requires exactly one
`event-universe` process. Sessions persist only a SHA-256 token digest. The six W1 routes are
`GET /v1/auth/nonce`, `POST /v1/auth/siwe`, `POST /v1/auth/logout`,
`GET /v1/admin/allowlist`, `POST /v1/admin/allowlist`, and
`DELETE /v1/admin/allowlist/<address>`. All responses are `no-store`; expose them
only through the same private interface or authenticated reverse proxy described
below.

Object-store selection is environment-owned and uses the
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
copy raw catalogues or selection reports. `universe/schema/schema.sql` is the
single canonical schema for both historical bundle APIs and the normalized
event/market view. There is no cadence cache.

Schema v5 intentionally does not migrate an existing database. Before rolling
out this version, stop the API and Universe jobs, remove the rebuildable SQLite
file plus its `-wal`/`-shm` siblings, run the oldest-first backfill to create
schema v5, and then start sync/API from the immutable archive. A v1/v2/v3/v4
database is rejected with a rebuild instruction.

Event Universe is strict Targeter v3-only. Incremental sync discovers immutable
version-2 run manifests and derives selected occurrences directly from each
manifest-owned v3 `selection_report.json[.zst]`. Existing archived v3 runs need
no Universe sidecar or producer backfill. Retained selections recursively verify
their exact immutable v3 origin manifests, including origins outside the
requested range. There is no Universe publication pointer; `/healthz` derives
the latest indexed archived run and marks it stale from that run's
`generated_at`.

### Safe full rebuild ordering

For a full historical rebuild, stop the Universe scheduler and API, remove the
disposable SQLite file plus WAL/SHM siblings, set `backfill.generated_start` to
the earliest retained Targeter run and `backfill.generated_end` past the newest
run to include, then run backfill **before** enabling periodic sync:

```bash
docker compose -f compose.universe.yaml --profile jobs run --rm event-universe-backfill
docker compose -f compose.universe.yaml --profile jobs run --rm event-universe-sync
docker compose -f compose.universe.yaml up -d event-universe
```

Backfill visits the archive oldest-first, so disjoint same-day rematches receive
immutable ordinals in successful ingestion order. The first occurrence seen for
an identity tuple receives ordinal zero. Repeating that order repeats event
links, but a previously failed manifest that ingests later may receive a later
ordinal; ordinal allocation is encounter-ordered rather than independent of
ingestion order.

Backfill emits newline-delimited `backfill_batch` progress records for each 100
runs and one `backfill_summary`. Exit 0 means the range scan completed with no
pending source failures; exit 1 means retry or operator investigation is still
required. A failed manifest is omitted from the projection and written to the
durable `universe_sync_failures` ledger, but does not stop later manifests or
leave the identity lineage running. Every processed batch advances a
range-specific SQLite checkpoint, so rerunning the exact same half-open range
resumes rather than restarts. Do not change either bound while resuming; the
identity-lineage guard rejects a different range even though it would otherwise
have a different checkpoint.

Do not run incremental sync first when the historical scan must allocate the
initial event links. Canonical backfill requires an identity-empty database and
records its exact range. Incremental sync is blocked only while that range scan
is in progress; failed manifests remain visible and retryable after the scan,
without blocking later ingestion. A newest-run bootstrap remains useful for a
non-canonical serving baseline, but converting that database to canonical
history requires deleting and rebuilding SQLite. Retained selection origins may
be fetched and indexed outside the configured range to preserve continuity
proof.

Incremental sync uses a forward high-water date plus a durable per-manifest
failure ledger. A bad manifest does not pin the date or block later runs.
Failures retry with exponential backoff capped at one day, at most 32 per job,
and keep `/healthz` degraded and the job exit nonzero until resolved. One
malformed listed key is recorded without aborting other keys. Initial sync on a
fresh database walks back at most 144 runs for a complete serving baseline; an
exhausted walk fails visibly and instructs the operator to run backfill.

Universe verifies metadata for every manifest-owned object. For complete runs
it downloads only normalized catalogue artifacts for venues referenced by
candidates, verifies each complete object, and retains only referenced rows in
memory. The decoded selected-catalogue budget is 128 MiB/run (warning at 96
MiB), each NDJSON row is at most 4 MiB, candidate references are capped at
100,000, and the selection report remains capped at 128 MiB. Decoded retrieval
stages one artifact at a time under `backfill.temporary_directory`; plan at
least 512 MiB free there and 1 GiB container memory. Compose does not impose a
`mem_limit`, consistent with the other services, but these application bounds
prevent catalogue-sized unbounded growth.

Retrieval directories include the owning PID. At job startup Universe removes
only directories older than 24 hours whose PID is no longer live; active or
unrecognized staging is retained. This safely cleans hard-kill leftovers
without racing an active download.

The immutable S3/GCS archive is the evidence and retention authority. Universe
SQLite is disposable and rebuildable, but a built database retains every run it
has indexed. There is no automatic pruning or rolling horizon in this release;
future historical truncation requires a separate API/product policy.

Raw segment selection and trust remain replay responsibilities; the archiver
has no Universe sidecar or receipt-mirror service.

`EVENT_UNIVERSE_DATA_ROOT` must be an attached persistent volume and should be
backed up independently. `EVENT_UNIVERSE_BIND_ADDRESS` defaults to loopback; use
a private interface or authenticated reverse proxy when exposing the API.
The UI consumes `GET /v1/targeter/status?limit=5` to resolve the newest complete
run for its targets and decisions views. The response contains only freshness,
latest/current-complete run summaries, and selected counts.
`GET /v1/targeter/runs/<run_id>` returns bounded normalized decisions
and references. Event, market, and relationship detail is available from
`/v1/events`, `/v1/markets/<market_id>`, `/v1/claims/<claim_id>`, and
`/v1/claims/<claim_id>/markets`.
`GET /v1/targeter/cadence` has been removed and returns 404. Universe and the UI
proxy enforce a 1.75 MB serialized response budget; list limits are capped at
100.

## Replay jobs production runtime

Replay jobs run on the Universe EC2 host, never the splice/capture host. The
topology is Internet → digest-pinned stock Caddy TLS → private
`event-universe:8080`; one-shot runner containers share only the private network
with disposable Redis. A separate runner-only egress network reaches the
independently durable ObjectStore and workload-identity endpoint.
Universe remains the parser and response-budget authority. Caddy has no archive
credentials, and Universe has no archive-write credentials.

### Install and storage preflight

Provision a private persistent filesystem at `REPLAY_DATA_ROOT`, owned by the
runtime uid/gid with mode 0700. Put `jobs.sqlite3`, `jobs/`, `runner.lock`,
`bundle-work/`, `derivatives/`, and `.runner/` on that same filesystem. Enforce
an XFS/ext4 project quota or equivalent no lower than `REPLAY_QUOTA_BYTES`.
Explicitly set `REPLAY_REQUIRED_CAPACITY_BYTES` to the configured worst-case
durable state, work, and scratch demand plus operating safety; there is no
production byte default. Keep scratch/work paths quota-governed. Never mount a
capture root, splice root, repository, credential file, or Docker socket.

Use an S3/GCS instance workload identity scoped to create/get/list for the exact
canonical, derivative, bundle, Replay job, and Replay database-backup prefixes.
It must not update or delete objects. A local backend is production-eligible
only when explicitly `independent` and on a different filesystem/device;
preflight and the store factory reject same-device independence. Receipt-only
bundle deletion uses a separate, audited, short-lived operator identity.
Set `REPLAY_ARCHIVE_PROBE_KEY` to a known immutable canonical receipt that the
normal runner identity must be able to read; preflight performs a provider HEAD
and requires checksum metadata. On EC2, the instance metadata options must
require IMDSv2 and use a hop limit of at least 2 so the container can obtain the
role credentials.

Build the runner with a stable source identifier, push it, and pin the deployed
image by provider digest. The full source SHA is recommended as shown, but any
stable identifier matching `[A-Za-z0-9._:+-]{1,128}` is permitted:

```bash
export REPLAY_IMAGE_REVISION="$(git rev-parse HEAD)"
export REPLAY_RUNNER_BUILD_TAG="registry.example/prediction-indexer-replay-runner:$REPLAY_IMAGE_REVISION"
docker build -f docker/replay-runner.Dockerfile \
  --build-arg REPLAY_IMAGE_REVISION="$REPLAY_IMAGE_REVISION" \
  -t "$REPLAY_RUNNER_BUILD_TAG" .
# Push through the approved registry workflow, resolve its digest, then set both:
# REPLAY_IMAGE_DIGEST=sha256:...
# REPLAY_RUNNER_IMAGE=registry.example/prediction-indexer-replay-runner@sha256:...
```

Both `event-universe` and `replay-runner` read the host checkout's
`configs/replay_runner.json`. When a release changes
`replay_runner_config_version`, re-pin `REPLAY_RUNNER_IMAGE` to a runner built
from that same revision before the checkout is updated on the host: an older
runner rejects the newer config and every tick fails before claiming. Retire a
strategy by setting its `status` to `retired` and restarting `event-universe`;
never delete its entry (see `docs/REPLAY_JOBS_V1.md` §3.10).

Caddy and Redis are pinned by multi-platform OCI digest in Compose, and
the runner is pulled by the digest set above rather than rebuilt. Validate
configuration before touching services:

```bash
docker compose -f compose.universe.yaml --profile replay config --quiet
docker compose -f compose.universe.yaml --profile replay pull replay-runner
docker compose -f compose.universe.yaml --profile replay run --rm --no-deps \
  --entrypoint sh replay-runner -c \
  'test "$(id -u)" != 0 && test -x "$REPLAY_PUBLISHER" && materialize_range --describe && python -c "import replay.jobs,replay.ops"'
```

Stock Caddy enforces TLS, 64 KiB request bodies, 32 KiB headers, connection and
upstream timeouts, security headers, and secret-safe JSON access logging. It
does **not** enforce request rate limits. The singleton Universe process owns
the in-process rate limiter configured in `event_universe.json`: requests whose
direct peer is the statically assigned Caddy address are limited by validated
session hash when authenticated and by the final Caddy-appended
`X-Forwarded-For` address otherwise. Direct private runner/preparation requests
are exempt. Keep the `replay-edge` subnet and Caddy address aligned with
`trusted_proxy_addresses`, and do not add another ingress peer without adding
its exact address. Caddy's `/data` volume persists ACME state; auth responses
are not compressed, and there is no static UI in this rollout.

The UI is deployed separately (Vercel) and calls the API cross-origin from the
browser. Caddy grants CORS to every origin (`Access-Control-Allow-Origin: *`,
`Access-Control-Expose-Headers: Retry-After`, no credentials mode) and answers
every `OPTIONS` preflight itself with `204`, so preflights never reach Universe
or its rate limiter. The production UI, local and preview builds, and
agent-driven browsers therefore all reach the API. This is deliberate: bearer
tokens travel in `Authorization`, never cookies, so there is no ambient
credential another origin could use, and public reads are public anyway. SIWE,
the allowlist, and the rate limiter are the controls. Preflight sends a live
CORS preflight through Caddy and requires the `*` grant.

SIWE is bound by message content, not request origin: the server accepts a
message only if it names the configured `siwe_domain` and `siwe_uri`. Build the
UI with those values from configuration rather than `window.location`, so a
local, preview, or agent-driven build signs messages production accepts. A
human's wallet additionally warns when the page origin differs from the
message domain; an agent's injected test wallet does not. Give each agent its
own allowlisted member wallet (never the admin) so it can be revoked alone, and
keep its key in that environment's secrets. Agent sessions see and act on
production data; they should not submit jobs unless that is the intent.

`REPLAY_PUBLIC_HOST` needs only a DNS name that resolves to the Universe host so
Caddy can obtain a public certificate. Without an owned domain, a free name such
as `<name>.duckdns.org` or `<a-b-c-d>.sslip.io` works; reserve a static public
IP first, since the name only follows the address. Moving to an owned domain
later changes `REPLAY_PUBLIC_HOST` (and the UI's API base URL) only. Moving the
UI changes `siwe_domain` and `siwe_uri`; users then sign in again.

The old unauthenticated Vercel proxy is not a supported production ingress
after this cutover: its users share a small egress-IP pool and therefore share
the unauthenticated bucket. Retire that proxy before enabling public ingress.
The localhost-only Universe port remains available for SSH tunnels and existing
host operations.

Start private services and Caddy, but leave the scheduler disabled:

```bash
docker compose -f compose.universe.yaml --profile replay up -d \
  event-universe replay-redis caddy
docker compose -f compose.universe.yaml --profile replay run --rm replay-preflight
```

Preflight is the scheduling gate. It validates immutable image identity and
release descriptor, both configs, binaries, root ownership/privacy/durability,
free bytes/inodes/quota, additive jobs/auth schema and SQLite integrity, Redis
≥8.2 with finite maxmemory/noeviction/no RDB/AOF/no evictions, independent
archive identity, private Universe health, public Caddy TLS/proxy health, and an
archive HEAD of `REPLAY_ARCHIVE_PROBE_KEY`. Its output is bounded and
secret-safe. A failure keeps the scheduler disabled. The in-process rate limiter
is validated by release tests rather than a public 429 preflight probe. Daemon
health, public DNS/certificates, cloud IAM, and filesystem quota enforcement are
environment gates, not properties a repository test can certify.

### Scheduler and resource controls

The runner is never started with `up`, a restart policy, or a persistent loop.
Use exactly one host cron or systemd timer invocation per minute:

```cron
* * * * * cd /opt/prediction-indexer && docker compose -f compose.universe.yaml --profile replay run --rm --no-deps replay-runner >> /var/log/prediction-replay-runner.log 2>&1
```

Prefer a systemd oneshot service plus a one-minute timer with
`Persistent=false`; use `flock` only inside the application as shipped. The
durable `runner.lock` makes overlap a clean exit zero, and each successful lock
holder claims at most one job. Alert on timer failures and last-success age;
rotate host logs and Compose JSON logs. Never add parallel runners.

Compose supplies CPU, memory reservation/limit, PID, read-only-rootfs, dropped
capabilities, no-new-privileges, and bounded tmpfs controls. Redis uses the exact
declared finite `REPLAY_REDIS_MAXMEMORY_BYTES`, no eviction, no persistence, no
host port, and no volume. It is only transport; restart loses attempts visibly
under existing typed retry/budget rules. Do not use container restart or
eviction to mask `resource_exhausted`,
`supervisor_exhausted`, or transport failures. Application entry/queue/output,
attempt, deadline, and archive limits remain authoritative.

### Monitoring and audit

Probe and alert on:

- public TLS, `/healthz`, `/v1/*` proxy limits, oversized body/header rejection,
  security headers, absent auth-route compression, and 429 plus `Retry-After`
  from controlled authenticated-session and unauthenticated-client-IP probes;
- Universe health, schema identity/integrity, auth configuration, and one-process
  singleton status;
- Redis version, maxmemory, policy, persistence config drift, used memory,
  `evicted_keys` (must remain zero), OOM/transport failures, and absence of a
  published host port;
- scheduler last start/exit, lock contention rate, last completed tick, and
  stale runner containers;
- counts and age by job `status`, `stage`, `reason_code`, `stage_attempts`,
  `archive_blocked`, and `local_state_lost`;
- archive availability/conflicts, receipt verification failures, disk/inodes,
  actual project quota, and backup age/verification/restore-drill age.

The bounded local audit is:

```bash
docker compose -f compose.universe.yaml --profile replay run --rm replay-audit
docker compose -f compose.universe.yaml --profile replay run --rm \
  --entrypoint python replay-runner -m replay.ops verify-job-receipt JOB_ID
```

For an `archive_blocked` job, first inspect its pending outcome, reason, attempts,
age, local frozen `job_receipt.json`/`archive_state.json`, provider state, and
logs. Remedy availability or IAM without modifying those files. If the remote
job receipt exists, strictly verify it and every listed immutable object. If the
block happened before receipt-last publication and verification reports `job
receipt is absent`, compare the local frozen receipt/archive state to every
already-uploaded immutable object and confirm that the remote receipt is truly
absent; that absence is expected and must not be "repaired" manually. Then
explicitly run:

```bash
docker compose -f compose.universe.yaml --profile replay run --rm \
  --entrypoint python replay-runner -m replay.ops resume-blocked JOB_ID
```

Never edit SQLite rows, stage markers, archive state, or receipts. The command
uses the existing compare-and-set transition and normal archive-first path.

### Backup, restore, and drills

Schedule a recurring jobs database backup separately from Universe's rebuildable
database backup:

```bash
docker compose -f compose.universe.yaml --profile replay run --rm replay-backup
```

It uses SQLite's online backup API, so copying `jobs.sqlite3`, WAL, or SHM is
forbidden. It checks the consistent snapshot, uploads immutable bytes under
`REPLAY_BACKUP_PREFIX`, verifies the provider stream, and publishes a receipt
with source/type/time/hash/length/key/provider identity and `integrity_check`.
It never includes `jobs/`, bundle work, derivatives, `.runner`, or other active
directories. Regularly run `verify-backup RECEIPT_KEY` and restore to an empty
drill path with `restore-backup RECEIPT_KEY DESTINATION`; independently open the
result and require `PRAGMA integrity_check = ok`.

A DB-only restore recovers auth, sessions, jobs, events, and submissions. Before
cutover, disable ingress and the Replay scheduler, stop Universe, preserve the current
volume, restore to a new path, verify, then atomically install while stopped.
Revoke all restored sessions or rotate authentication authority before reopening
ingress. Active rows whose job directories are absent resume only to
`local_state_lost`; never recreate/reset them. The archive-first receipt path
still runs and remains auditable.

A full active-state rollback requires a stopped coordinated full-volume snapshot:

1. Obtain change authorization; disable edge ingress and scheduler.
2. Drain the runner and prove no `runner.lock` holder exists.
3. Stop Universe, checkpoint SQLite, and snapshot the entire Replay volume.
4. Record config hashes, image revision/digest, volume ID, archive identity, and
   snapshot identity together.
5. Restore volume/config/revision metadata as one unit while services remain
   stopped; verify SQLite and local markers, then run preflight and synthetic
   acceptance before reopening ingress/scheduling.

Do not call a DB-only restore a full-state rollback. Never roll back to an image
that cannot read current state, delete artifacts to force compatibility, or
mutate committed evidence.

For every rebuild or maintenance operation that stops `event-universe`, disable the Replay scheduler
first and drain any lock holder. Otherwise cron may start a runner against a
deliberately unavailable control plane.

### Break-glass bundle rebuild

The bundle cache remains one singleton receipt. After proving
`stale_bundle_cache`, use separate delete-capable operator authority, record the
ticket/operator/time/bundle key/current receipt identity, strictly verify the
receipt, and delete **only**
`replay/bundles/<bundle_id>/bundle_receipt.json`. Remove the authority, verify no
derivative changed, and allow an explicitly authorized new job to rebuild. This
is receipt-only deletion: never delete or rewrite derivative objects, job
receipts, or canonical objects.

### Rollout and rollback

Rollout order is: build/pin images; provision private volume, quotas, independent
archive, networks, workload identity, Caddy state, and logging;
deploy additive Universe config/schema; drill migration plus online
backup/verify/DB-only restore and stopped full-volume restore with synthetic
data; start private services; pass preflight; validate TLS/proxy/body/header/time
limits, security headers, and controlled in-process limiter 429s; install the
scheduler disabled; run synthetic cold and warm acceptance through canonical
materialize → prepare → supervisor → strict reader → receipt-last archive →
strict job reader, including crash boundaries; then enable and monitor. No
retained or live-provider acceptance is authorized by this procedure.

Rollback disables scheduler and ingress, drains the runner, and preserves the
whole volume. Roll back only to a config/image proven compatible with current
schema and markers. For state rollback use only the stopped full-volume snapshot
procedure above. Rerun preflight and synthetic acceptance before re-enabling.

Replay stream V1's post-baseline `control_events` extension is a lockstep image
boundary: deploy the publisher and all strict consumers together, with no active
attempt crossing the image change. Mixed-version operation and rollback to a
baseline V1 consumer after publishing such a cut are unsupported; the old consumer
intentionally fails closed on the added field.

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
