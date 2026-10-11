# Docker Compose deployment

Three Compose files, three independent deployments:

| File | Deploys | Image(s) |
|---|---|---|
| `compose.yaml` | Capture: targeter, splices, ingester, finalizer, archiver, reapers | `docker/python.Dockerfile` (capture), `docker/ingester.Dockerfile` (Rust ingester/finalizer) |
| `compose.targeter-v2.yaml` | Overlay with the Targeter v2 run-retention and integrity services (`ops` profile) | capture image |
| `compose.universe.yaml` | Event Universe API, its one-shot jobs, and the Replay jobs runtime | `docker/universe.Dockerfile`, `docker/replay-runner.Dockerfile` |

The capture deployment is one process per container and one writer per spool
lane. Containers share files, not sockets:

```text
targeter (one-shot, cron) ──> live/targeter-v2/current.json  (atomic pointer)
                                   │
        ┌──────────────────────────┼───────────────────────────┐
        v                          v                           v
 splice-polymarket          splice-limitless      splice-polymarket-snapshots
        │                          │                           │      (+ splice-kalshi)
        └──────────────┬───────────┴───────────────────────────┘
                       v
              spool/lane=*/date=*/   sealed NDJSON segments + seals
                 │                 │
                 v                 v
             ingester         finalizer ──> canonical/ ──┐
        ingest-store/ (derived)      │                   │
                                     └──> archiver ──> archive backend (local | GCS)
                                                   reaper / canonical-reaper
```

Everything durable on the capture host is bind-mounted from `CAPTURE_DATA_ROOT`
(mounted at `/var/lib/prediction-indexer`), plus the archive mount from
`ARCHIVE_ROOT` (mounted at `/var/lib/prediction-archive`) for the archive
services. Images are immutable; the repository is not mounted into running
containers. Every capture service runs as `PUID:PGID`, read-only root filesystem,
all capabilities dropped, `no-new-privileges`.

## Services and profiles

A service behind a profile is invisible to `docker compose ps` and is not
started by `up -d` unless the profile is named.

| Service | Profile | Purpose |
|---|---|---|
| `targeter` | none | One discovery/archive/publish transaction (`targeter/run_v2.py --mode publish`); `restart: "no"` |
| `splice-polymarket` | none | Polymarket market WebSocket |
| `splice-limitless` | none | Limitless market feed |
| `splice-polymarket-snapshots` | none | Polled Polymarket book snapshots |
| `ingester` | none | Tails all spool lanes into the derived ingest store |
| `splice-kalshi` | `kalshi` | Authenticated Kalshi feed |
| `splice-polymarket-sports` | `reference` | Polymarket sports reference feed |
| `splice-polymarket-rtds` | `reference` | Polymarket RTDS reference prices |
| `ingester-integrity` | `ops` | One-shot ingest-store integrity check |
| `ingest-store-reaper` | `ops` | One-shot audit/delete of closed ingest databases (24 h floor) |
| `finalizer` | `ops` | Long-lived; merges sealed windows into canonical evidence every `FINALIZER_INTERVAL_SECONDS` |
| `finalizer-once` | `ops` | The same sweep, once |
| `archiver` | `ops` | Long-lived; publishes sealed segments and committed canonical windows every `ARCHIVER_INTERVAL_SECONDS` |
| `archiver-once` | `ops` | The same sweep, once |
| `reaper`, `reaper-once` | `ops` | Raw-segment reaper (audit by default) |
| `canonical-reaper`, `canonical-reaper-once` | `ops` | Canonical-frame reaper (audit by default, 18 h floor) |
| `canonical-integrity` | `ops` | Fully decodes local canonical windows |
| `targeter-v2-run-archiver` | `ops`, `compose.targeter-v2.yaml` | Archives complete run directories that hold no receipt; never deletes |
| `targeter-v2-run-reaper` | `ops`, `compose.targeter-v2.yaml` | Receipt-proved audit/delete of local run directories |
| `targeter-v2-integrity` | `ops`, `compose.targeter-v2.yaml` | Read-only audit of the live generation (`run_v2.py --mode audit`) |

**A bare `docker compose up -d` omits every `ops` service**, including the
finalizer and archiver. The splices and ingester keep running while canonical
evidence and archival silently stop. Name the profile whenever you start the
pipeline:

```bash
docker compose --profile ops up -d finalizer archiver
```

Commands touching the three `targeter-v2-*` services need both files:
`docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops ...`.
Everything else needs only `compose.yaml`; passing both is harmless, so an
operator alias with both files is safe.

Kalshi is opt-in because it needs credentials and is by far the largest lane.
Reference feeds are opt-in because the core book capture does not need them.

## First deployment on Linux

Docker Engine with the Compose v2 plugin is required.

```bash
test -e .env || cp .env.example .env
```

Edit `.env` before starting (variable names are in `.env.example`; never commit
`.env`):

```dotenv
CAPTURE_DATA_ROOT=/srv/prediction-indexer/data
PUID=1000
PGID=1000
```

`PUID` and `PGID` must own the data root. Use the deployment account's numeric IDs
(`id -u`, `id -g`), not necessarily `1000`:

```bash
sudo install -d -o 1000 -g 1000 /srv/prediction-indexer/data
docker compose config --quiet
docker compose -f compose.yaml -f compose.targeter-v2.yaml config --quiet
docker compose build
```

To pull prebuilt images instead, set `IMAGE_REGISTRY` (with trailing slash) and
pin `IMAGE_TAG` to an immutable tag. Images are architecture-specific; a
cross-built image needs `DOCKER_DEFAULT_PLATFORM=linux/amd64` for an x86_64 host.
`local` is a moving tag and makes it impossible to say later which code produced a
given archive.

### Startup order

Splices wait for the pointer file, so the targeter must publish first:

1. Configure the archive backend (see "Archive backends"). Targeter publication
   archives each run, so an unreachable backend fails the run and publishes
   nothing.
2. Run the targeter once, then its integrity audit:

   ```bash
   docker compose run --rm targeter
   docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops \
     run --rm targeter-v2-integrity
   ```

3. Start capture and the pipeline:

   ```bash
   docker compose up -d
   docker compose --profile ops up -d finalizer archiver
   ```

   Each target-dependent splice runs `docker/wait_for_target.py`, which blocks
   until `/var/lib/prediction-indexer/live/targeter-v2/current.json` exists and
   then execs the splice. `up -d` also starts `targeter` once, because the
   splices list it in `depends_on` (`service_started`); the container exits after
   its transaction.
4. Schedule the targeter and the retention sweeps (below).

The default services use `restart: unless-stopped` and are independently
restartable. A failed Kalshi discovery cannot hold up Polymarket or Limitless:
each splice reads the venue's own targets from the shared generation.

## Optional feeds

```bash
docker compose --profile reference up -d
```

For Kalshi, keep the private key outside the repository, readable only by the
deployment account:

```bash
install -m 600 /path/from/kalshi/private-key.pem /srv/prediction-indexer/kalshi-private-key.pem
```

```dotenv
KALSHI_API_KEY_ID=your-key-id
KALSHI_PRIVATE_KEY_PATH=/srv/prediction-indexer/kalshi-private-key.pem
```

```bash
docker compose --profile kalshi up -d
docker compose logs -f splice-kalshi
```

Compose mounts the key read-only at `/run/secrets/kalshi-private-key.pem`; the
bind does not create a missing host path. `KALSHI_SNAPSHOT_MAX_AGE_SECONDS`
(default `0`, disabled) enables the splice's snapshot poller; see
[`splices/README.md`](../splices/README.md) before turning it on, because a
rejected command that drops the capture connection costs tape.

**Lane list for the finalizer.** Both finalizer services pass `--expect-lane` for
`polymarket`, `polymarket_snapshots`, `limitless` and `kalshi`. The list must name
exactly the splices the deployment runs. If the `kalshi` profile is off, remove
that `--expect-lane` pair from `finalizer` and `finalizer-once` in `compose.yaml`;
if the `reference` profile is on, add `polymarket_sports` and `polymarket_rtds`.
A lane listed but never run makes every window sit out its deadline and commit
`incomplete`; a lane run but not listed makes a real outage invisible, because
every window still reads complete.

## Targeter scheduling

The targeter is not a long-lived container. Cron or a systemd timer starts one
isolated discovery/archive/publish transaction; the application lease under
`targeter-v2-runs` rejects overlapping invocations. Use one scheduler, not both.
Every subscription-driven splice resolves the same atomically replaced pointer, so
a generation is never mixed across venues.

```cron
*/10 * * * * cd /opt/prediction-indexer && docker compose run --rm targeter >> /var/log/prediction-targeter.log 2>&1
```

Schedule `run --rm targeter`, never `up`. The service passes `--no-response-cache`
(raw HTTP bodies are not persisted; rate-limit state under
`targeter-v2-cache` still is), writes runs to `targeter-v2-runs`, and publishes
under `live/targeter-v2/`. A new image or command takes effect on the next firing.
Run `targeter-v2-integrity` after the first publication, after deployment changes,
and from monitoring at least once per scheduler interval. The archive namespace,
commit protocol and rollback are in
[`targeter/v2/DELIVERY.md`](../targeter/v2/DELIVERY.md).

### Run retention

Each run leaves a 12-20 MB directory under `targeter-v2-runs` (about 2 GB per day
at a ten-minute cadence) and nothing else removes it. Schedule both retention
services off the publish boundary:

```cron
5  * * * * cd /opt/prediction-indexer && docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops run --rm targeter-v2-run-archiver >> /var/log/prediction-targeter-v2-archive.log 2>&1
35 * * * * cd /opt/prediction-indexer && docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops run --rm targeter-v2-run-reaper   >> /var/log/prediction-targeter-v2-reaper.log  2>&1
```

Each sweep prints its record and writes
`/var/lib/prediction-indexer/ops/last_targeter_v2_archive_sweep.json` or
`last_targeter_v2_reaper_sweep.json`. Read the reports as described in
[`RUNBOOK.md`](RUNBOOK.md); the sweep statuses, the eleven-condition reaper gate
and the exit codes are in [`targeter/v2/DELIVERY.md`](../targeter/v2/DELIVERY.md).

**Audit is the default.** Installing the services does not enable deletion. Delete
mode needs `TARGETER_RUN_REAPER_MODE=delete` in `.env` and a backend declared as an
independent durability domain (a local conformance store is refused at startup).
Run audit for several cycles first, confirm `counts.unarchived` is zero and no
fault reason appears, then switch. `TARGETER_RUN_RETENTION_HOURS` may be raised
above the 18-hour floor, not lowered. The reaper never removes a receipt, a run
directory, an archive object or a published generation.

### Discovery coverage backfill

Publication records a first sighting for every subscribed asset in
`<live-root>/coverage.json` (`targeter/coverage.py`), inside `publish_run`; no
service or cron entry is needed. A deployment that captured before the ledger
existed must backfill once, **before enabling run deletion**: starting from empty
would stamp long-subscribed assets with today's date and make genuinely captured
frames look like they predate coverage. The backfill reads venue creation times
from run catalogues, which the run reaper reclaims.

```bash
docker compose run --rm --no-deps targeter \
  python -u -m scripts.backfill_coverage \
    --live-root /var/lib/prediction-indexer/live \
    --output-root /var/lib/prediction-indexer/targeter-v2-runs \
    --report /var/lib/prediction-indexer/ops/coverage_backfill.json
```

It reconstructs sightings from `live/targeter-v2/generations/<run_id>/`, is
idempotent, never moves a sighting later, and repairs one stamped too late. An
asset whose run was reaped keeps its sighting but loses `created_at` and is
reported unmeasurable rather than given a lag of zero.

## Operations

```bash
docker compose ps
docker compose logs --tail 200 targeter splice-polymarket splice-limitless ingester
docker compose logs -f splice-polymarket
docker compose down            # stops services; host data is untouched
```

Splices print nothing during normal operation by design; judge them by whether the
spool is growing. Restarting a splice opens a new connection epoch and resumes
`delivery_index` from its spool. A normal stop gives the splice 30 seconds to write
its closing control record, fsync and seal.

Run a store integrity check without a concurrent ingest writer:

```bash
docker compose stop ingester
docker compose --profile ops run --rm ingester-integrity
docker compose start ingester
```

### Ingest-store schema migration

On first start the ingester moves a schema-v1 `ingest-store/store.db` into the
current UTC ingestion-day partition, builds the durable `record_identity` index and
sets schema version 3, as one blocking migration; failure rolls back and the raw
spool is unchanged. Stop the old ingester before deploying the new binary. Leave at
least 11% of the current store size free (the measured transaction peak was 10.2%
over the original database, with 5.1% permanent growth) plus normal margin, and
time the new binary against a copy of the real store first: duration scales with
fact and unique-identity counts. The duration and record count appear in the
ingester log and under `store_migration` in its JSON report. After startup,
`identity_records_in_memory` in the report must be `0`.

**Fresh derived-store cutover instead of migration.** The ingest store is a derived
`file_order` projection; the finalizer, archiver and raw reaper do not read it. Stop
the ingester and move the whole `ingest-store/` directory to another filesystem
(a rename on the same filesystem releases no capacity), then start the new image;
with no `ingest-store/` it creates a fresh schema-v3 partition. This starts a new
`file_order` lineage: prior continuity and duplicate history are not carried, and
every sealed segment still in `spool/` is ingested again. Segments already removed
by the raw reaper cannot be reconstructed by `indexer-ingest`. Preserve `spool/`,
`canonical/`, archive objects and receipts.

### Daily ingest-store retention

The ingester rotates only between complete sealed segments, at the UTC-day
boundary (layout and receipt in [`ingester/FORMATS.md`](../ingester/FORMATS.md)).
`ingest-store-reaper` is one-shot, not a loop, and is scheduled once per day:

```cron
25 3 * * * cd /opt/prediction-indexer && docker compose --profile ops run --rm ingest-store-reaper >> /var/log/prediction-ingest-store-reaper.log 2>&1
```

`INGEST_STORE_REAPER_MODE=audit` (default) deletes nothing; set `delete` only after
reviewing several reports. `INGEST_STORE_RETENTION_HOURS` defaults to 24 and the
command refuses less. It never deletes the active `store.db.open` and deletes a
closed database only when it is old enough and byte-identical to its valid
receipt; receipts and partition directories remain so old spool files cannot be
ingested twice. Its report is
`ops/last_ingest_store_reaper_sweep.json`. Ingest databases are derived, so this
is separate from the raw reaper.

## Persistence and capacity

Everything durable is below `CAPTURE_DATA_ROOT`:

```text
live/                pointer, generations, coverage ledger
spool/               irreversible raw NDJSON tape and seals
ingest-store/        daily derived SQLite partitions (file_order)
canonical/           merged evidence per window
archive-manifests/   daily manifests and last archiver/reaper sweep reports
targeter-v2-runs/    Targeter v2 run directories
targeter-v2-cache/   rate-limit state (should stay near zero)
ops/                 last-sweep reports for finalizer, ingest reaper, run services
```

`ARCHIVE_ROOT` is deliberately a separate mount; see "Archive backends".

Back up `spool/` first. The ingest store and canonical evidence are derived and
rebuildable from it; the spool cannot be reconstructed from either. The Kalshi
lane at full ladder width is roughly ten times the record count of everything
else combined, so size disk from Kalshi, not Polymarket. Observed daily rates and
how to read them are in [`RUNBOOK.md`](RUNBOOK.md). With the local conformance
backend total local storage is not bounded (the bytes changed representation and
directory, nothing more); a durable backend does not bound the spool until
`REAPER_MODE=delete` is enabled. Object-store lifecycle expiry is not configured.

Container logs rotate at `LOG_MAX_SIZE` (25m) with `LOG_MAX_FILES` (5) per service.

## Canonical evidence and finalizer

`finalizer` (or `finalizer-once`) merges sealed windows from all lanes into
`canonical/date=<YYYY-MM-DD>/window=<start_ns>/` with `evidence.ndjson.zst`,
`provenance.ndjson.zst` and `receipt.json`; the receipt is the commit marker, and
`canonical/watermark.json` is a derived index that is rebuilt from the receipts if
deleted. Formats, ordering (`visible_ns, lane_rank, delivery_index`), lateness and
continuity are specified in [`ingester/README.md`](../ingester/README.md) and
[`ingester/FORMATS.md`](../ingester/FORMATS.md).

Deployment-relevant behavior:

- `--window-seconds` is passed from `SEGMENT_SECONDS` and is the authority for
  window bounds; the splices and the finalizer must agree. `SEGMENT_SECONDS` must
  divide 86400 evenly.
- `FINALIZATION_DEADLINE_SECONDS` (default 300) is how long a window waits for a
  lane without a valid seal before committing with the gap named in its receipt.
  A window that has not ended is never finalized.
- A segment arriving for a committed window is reported `late_after_finalization`,
  archived, then retained by the raw reaper because no canonical receipt names it.
- One finalizer per canonical root, held as a `.finalize.lease` file. SIGTERM and
  SIGINT release it; after SIGKILL or a host crash remove a stale lease only after
  confirming no finalizer is running.
- The latest sweep report is `ops/last_finalizer_sweep.json`. Window duplicate
  detection uses a scratch SQLite file inside the open window directory; leave
  temporary disk headroom proportional to the largest window. The report's
  `max_identity_records_in_memory` must be `0`.

Audit committed windows (bounded memory) before replay or export:

```bash
docker compose --profile ops run --rm canonical-integrity
```

## Archive backends

`archiver`, `reaper`, `canonical-reaper` (and their `-once` forms) and the
Targeter v2 archive services build their object store through one factory,
`archive/storage/factory.py`, from environment values only. The contracts are in
[`archive/README.md`](../archive/README.md) and
[`archive/FORMATS.md`](../archive/FORMATS.md). Compose passes `ARCHIVE_BACKEND`,
`ARCHIVE_ROOT`, `ARCHIVE_STORE_ID`, `ARCHIVE_DURABILITY` and `ARCHIVE_GCS_BUCKET`
to archive services only; venue splices and the ingester receive no archive
configuration.

| `ARCHIVE_BACKEND` | Required | Durability |
|---|---|---|
| `local` (default) | `ARCHIVE_ROOT` (host mount source) | `ARCHIVE_DURABILITY=conformance` exercises the path and writes `.archive.local.json` receipts that authorize nothing. `independent` is the declaration that losing the capture host does not lose the archive and is refused when the archive and capture roots share a filesystem |
| `gcs` | `ARCHIVE_GCS_BUCKET` | Always independent; `ARCHIVE_DURABILITY` cannot downgrade it |

Options for another backend are rejected rather than guessed past. The default
`ARCHIVE_ROOT=./data/archive` lives under the capture root on purpose: it
exercises the full path and measures compression, and the reaper refuses to delete
against it.

**GCS.** Set `ARCHIVE_BACKEND=gcs` and `ARCHIVE_GCS_BUCKET`. The client uses
Application Default Credentials: attach a dedicated service account to the VM
rather than mounting a key, and never put credentials in `.env`. Grant it
create/get/list on the bucket only (a bucket-scoped custom role with
`storage.objects.create`, `storage.objects.get` and `storage.objects.list`, or the
predefined Object Creator plus Object Viewer roles) and no delete or update.
Use a private, dedicated regional bucket with uniform bucket-level access and
public access prevention, and keep lifecycle expiry disabled. Compose forwards no
credential file. The prefixes written are `raw/`, `canonical/` and `targeter-v2/`.

GCS has no server-side SHA-256: the adapter hashes the upload stream while the
client and service validate CRC32C, stores SHA-256 and length as custom metadata,
and records the returned CRC32C in the receipt. Verification compares current
metadata against the closed receipt without downloading bodies; retrieval pins a
generation and verifies SHA-256 and CRC32C over the complete object.

## Archive, receipts and deletion

```bash
docker compose --profile ops up -d archiver           # sweeps hourly, stays up
docker compose --profile ops run --rm archiver-once   # one sweep, then exits
docker compose --profile ops up -d finalizer reaper canonical-reaper
```

The archiver publishes each sealed segment (one Zstandard frame beside the
unchanged seal) and each committed canonical window under immutable keys, verifies
the objects by reading them back, and only then writes the local receipt
(`<segment>.archive.json`, `canonical_archive_receipt.json`; the conformance forms
carry `.local`). **The receipt is the archive commit marker**; a compressed file, a
key in the store or a successful upload is not. Archival never deletes. Key layout
and receipt schemas are in [`archive/FORMATS.md`](../archive/FORMATS.md).

**Archiver exit codes.** `2` stops the sweep on an immutable-key conflict (a key
already holds different content, so the namespace or data is wrong); nothing is
overwritten and watch mode exits instead of retrying, so an `archiver` shown as
`Restarting` in `docker compose ps` means an integrity conflict, not a busy spool.
Read the last sweep's JSON and find which producer wrote the existing object before
touching anything; the local segment and seal remain the recovery authority. `1`
means some segments failed for their own reasons (malformed seal, changed byte,
transient store failure) and the sweep continued.

`ARCHIVER_INTERVAL_SECONDS` (default 3600) sets the sweep interval; the archive
unit stays one sealed segment. `spool/` holds up to about one interval of
unarchived segments on top of the finalization delay, so shorten the interval
before shortening retention.

### Reapers are audit by default

Three separate reapers hold three separate authorities. None of them is active
because the code is installed.

| Reaper | Mode variable | Deletes | Floor |
|---|---|---|---|
| `reaper` / `reaper-once` | `REAPER_MODE` | Raw segment and seal in `spool/` | none; proof-based |
| `canonical-reaper` / `-once` | `CANONICAL_REAPER_MODE` | `evidence.ndjson.zst` and `provenance.ndjson.zst` only | `CANONICAL_REAPER_RETENTION_HOURS`, default 18, refuses less |
| `ingest-store-reaper` | `INGEST_STORE_REAPER_MODE` | Closed derived ingest database | 24 h |

The raw reaper deletes a segment and seal only when, at decision time: a
structurally valid archive receipt exists; the archive data and seal objects still
match it when read back; the backend is an independent durability domain; a valid
committed canonical `receipt.json` exists; its `inputs` entry matches the lane,
source SHA-256, file name and segment index; and the local source and seal still
match the receipt when rehashed in full. Anything less is retention and the reason
appears in `archive-manifests/last_reaper_sweep.json`. A late, excluded or
never-canonicalized segment stays on disk and visible.

The canonical reaper additionally requires a production canonical archive receipt
binding the unchanged `receipt.json` and both frame identities, fresh metadata
verification of all three remote objects, and a window old enough; age is the
latest of window end, finalization, archive verification and both receipt mtimes.
It leaves the window directory, `receipt.json` and `canonical_archive_receipt.json`
as the tombstone the finalizer needs to rebuild the watermark and keep global
sequence and continuity after restart. `canonical-integrity` reports these as
`windows_archived_and_reaped` and a crash between the two unlinks as
`windows_partially_reaped`.

Raw deletion with a local backend additionally needs `ARCHIVE_DURABILITY=independent`
on a different filesystem from the spool, with `ARCHIVE_ROOT` outside
`CAPTURE_DATA_ROOT`. `--delete` remains a compatibility alias for `--mode delete`
on direct invocations; do not combine them.

**Rollout gate.** Switching to a durable backend does not change the reaper's
mode. Run the archiver with reaping in audit for at least 24 hours, confirm
production receipts report provider `gcs` and `CRC32C`, run
`canonical-integrity`, sample every lane against retained local raw by strict
decode, and only then set `REAPER_MODE=delete` / `CANONICAL_REAPER_MODE=delete` as
a separate explicit decision. Raw deletion is irreversible for local analysis; see
the ordering in [`RUNBOOK.md`](RUNBOOK.md).

## Event Universe deployment

Event Universe is a separate small-server deployment, not another process on the
capture host. `compose.universe.yaml` builds `docker/universe.Dockerfile`, mounts
its persistent volume (`EVENT_UNIVERSE_DATA_ROOT` at `/var/lib/event-universe`),
the Replay volume (`REPLAY_DATA_ROOT` at `/var/lib/replay`) and the read-only
`configs/event_universe.json` and `configs/replay_runner.json`, and never mounts
`CAPTURE_DATA_ROOT`.

```bash
docker compose -f compose.universe.yaml up -d event-universe
```

The API binds `EVENT_UNIVERSE_BIND_ADDRESS` (default `127.0.0.1`) on
`EVENT_UNIVERSE_PORT` (default 8080); use a private interface or the Caddy
ingress below when exposing it. `event-universe` has a `/healthz` healthcheck and
resource limits from `UNIVERSE_MEMORY_LIMIT`, `UNIVERSE_MEMORY_RESERVATION`,
`UNIVERSE_CPU_LIMIT` and `UNIVERSE_PIDS_LIMIT`. The server must run as exactly one
process: SIWE nonces are held in memory.

The JSON config (versioned; older versions are rejected, so roll out the config
and the image together) holds the rebuildable Universe database path, API
listener, temporary directory, backup destination, and a separate durable
`replay.database_path` (`jobs.sqlite3`) for authentication and Replay state.
Rebuilding `event-universe.sqlite3` must never remove that file. Before exposing
authentication routes, replace the zero-address `admin_address` placeholder with
the operator wallet's EIP-55 address and set `siwe_domain`/`siwe_uri` to the exact
UI origin users sign on (not the API host). While the placeholder remains, sign-in
routes return `503` and other routes are unaffected. The admin must be an
externally owned account (verification is offline EIP-191 recovery, no RPC). The
routes, schema and sync semantics are in [`universe/README.md`](../universe/README.md).

**Object store.** Universe uses the same factory and `ARCHIVE_*` variables.
`EVENT_UNIVERSE_ARCHIVE_ROOT` is mounted at `/var/lib/archive` for the local
backend; for GCS set `ARCHIVE_BACKEND=gcs` and `ARCHIVE_GCS_BUCKET` and rely on
Application Default Credentials from an attached service account.

**Jobs.** Incremental sync, backfill and backup are one-shot services in the
`jobs` profile, scheduled by the host:

```bash
docker compose -f compose.universe.yaml --profile jobs run --rm event-universe-sync
docker compose -f compose.universe.yaml --profile jobs run --rm event-universe-backup
```

`EVENT_UNIVERSE_DATA_ROOT` must be a persistent volume and should be backed up
independently. The immutable archive is the evidence and retention authority;
the Universe SQLite file is disposable and rebuildable.

### Universe VM data roots

Every persistent path on the Universe VM is one of these host roots, bind-mounted
by `compose.universe.yaml`. Paths and variable names are unchanged by the
`universe/` package layout; no data moves.

| Root (env var) | Container path | Holds | Services | Rebuildable | Backup/rollback rule |
|---|---|---|---|---|---|
| `EVENT_UNIVERSE_DATA_ROOT` | `/var/lib/event-universe` | `event-universe.sqlite3`, Zstd staging, `backups/` | `event-universe`, `-sync`, `-backfill`, `-backup` | Yes, by backfill from the archive | `event-universe-backup` uploads immutable copies; never touch the Replay DB when rebuilding |
| `REPLAY_DATA_ROOT` | `/var/lib/replay` | `jobs.sqlite3` (jobs and auth tables), active Replay state | `event-universe`, `replay-*` | **No** | Independent verified `replay-backup` copies before any change |
| `GAMESTATE_DATA_ROOT` | `/var/lib/gamestate` | `gamestate.sqlite3` (attempt ledger and bundle → event map) | `event-universe-game-state` | Yes | Losing it costs one remapping pass, never a refetch of archived games |
| `EVENT_UNIVERSE_ARCHIVE_ROOT` | `/var/lib/archive` | Local archive objects (local backend only) | sync, backfill, backup, game-state, `replay-*` | — (evidence) | Immutable; never delete on rollback |

Moving the auth tables out of `jobs.sqlite3` waits for the jobs retirement.

### Event-keyed game-state job

`event-universe-game-state` is a one-shot `jobs` service. Host cron may invoke
it every 30 minutes. Its config is `configs/gamestate.json`; it has archive
access, not capture or Replay/auth database mounts. Deploying and running a
backfill are operator actions, not configuration-validation steps.

The new ledger has its own version and needs no Universe/auth migration.
Rollback disables this scheduled job and restores the prior image; retain
all raw receipts and timelines. Existing V1 game-state readers remain valid.
Allow at least 256 MiB of temporary disk per pull. Adapter limits still apply
to backfill, which runs with explicit half-open activation bounds. A complete
raw receipt does not prove settlement; timeline repair is offline regeneration.

### Safe full rebuild ordering

A schema change that does not migrate (a stale database is rejected with a rebuild
instruction) or a full historical rebuild is: stop the Replay scheduler first and
drain any lock holder, stop the Universe API, remove the disposable SQLite file
plus its `-wal`/`-shm` siblings, set `backfill.generated_start` to the earliest
retained Targeter run and `backfill.generated_end` past the newest run to include,
then run backfill **before** enabling periodic sync:

```bash
docker compose -f compose.universe.yaml --profile jobs run --rm event-universe-backfill
docker compose -f compose.universe.yaml --profile jobs run --rm event-universe-sync
docker compose -f compose.universe.yaml up -d event-universe
```

Backfill visits the archive oldest-first, so same-day rematches receive immutable
ordinals in successful ingestion order. It emits `backfill_batch` progress records
for each 100 runs and one `backfill_summary`. Exit 0 means the range completed with
no pending source failures; exit 1 means retry or investigation is needed. A failed
manifest is omitted from the projection and written to the durable
`universe_sync_failures` ledger without stopping later manifests. Every processed
batch advances a range-specific SQLite checkpoint, so rerunning the exact same
half-open range resumes; do not change either bound while resuming.

Canonical backfill requires an identity-empty database. Do not run incremental sync
first when the scan must allocate the initial event links: sync is blocked only
while the range scan is in progress. A newest-run bootstrap is a valid
non-canonical serving baseline, but converting it to canonical history means
deleting and rebuilding the SQLite file.

Incremental sync uses a forward high-water date plus a durable per-manifest failure
ledger. A bad manifest does not pin the date or block later runs; failures retry
with exponential backoff capped at one day, at most 32 per job, and keep `/healthz`
degraded and the job exit nonzero until resolved. Initial sync on a fresh database
walks back at most 144 runs for a complete serving baseline; an exhausted walk fails
visibly and instructs the operator to run backfill.

Universe verifies metadata for every manifest-owned object. For complete runs it
downloads only normalized catalogue artifacts for venues the candidates reference.
The decoded selected-catalogue budget is 128 MiB/run (warning at 96 MiB), each
NDJSON row is at most 4 MiB, candidate references are capped at 100,000, and the
selection report is capped at 128 MiB. Decoded retrieval stages one artifact at a
time under `backfill.temporary_directory`: plan at least 512 MiB free there. The
container memory limit defaults to 2g. Staging directories carry the owning PID; at
job startup Universe removes only those older than 24 hours whose PID is no longer
live.

A built database retains every run it has indexed. There is no automatic pruning
or rolling horizon; historical truncation would need a separate API/product policy.

## Replay jobs production runtime

Replay jobs run on the Universe host, never the splice/capture host. All services
below are in `compose.universe.yaml` under the `replay` profile (the always-on
`event-universe` is outside it). The topology is Internet → digest-pinned stock
Caddy TLS → private `event-universe:8080`; one-shot runner containers share only
the private network with disposable Redis (`replay-redis`). A separate runner-only
egress network (`replay-egress`) reaches the independently durable object store and
the workload-identity endpoint. Universe remains the parser and response-budget
authority. Caddy has no archive credentials.

### Install and storage preflight

Provision a private persistent filesystem at `REPLAY_DATA_ROOT`, owned by the
runtime uid/gid with mode 0700. Put `jobs.sqlite3`, `jobs/`, `runner.lock`,
`bundle-work/`, `derivatives/`, and `.runner/` on that same filesystem. Enforce
an XFS/ext4 project quota or equivalent no lower than `REPLAY_QUOTA_BYTES`.
Explicitly set `REPLAY_REQUIRED_CAPACITY_BYTES` to the configured worst-case
durable state, work, and scratch demand plus operating safety; there is no
production byte default. Keep scratch/work paths quota-governed. Never mount a
capture root, splice root, repository, credential file, or Docker socket.

Use a GCS workload identity (an attached service account via Application Default
Credentials) scoped to create/get/list for the exact canonical, derivative, bundle,
Replay job, and Replay database-backup prefixes.
It must not update or delete objects. A local backend is production-eligible
only when explicitly `independent` and on a different filesystem/device;
preflight and the store factory reject same-device independence. Receipt-only
bundle deletion uses a separate, audited, short-lived operator identity.
Set `REPLAY_ARCHIVE_PROBE_KEY` to a known immutable canonical receipt that the
normal runner identity must be able to read; preflight performs a provider HEAD
and requires checksum metadata.

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
never delete its entry (see [`REPLAY_JOBS_V1.md`](REPLAY_JOBS_V1.md) §3.10).

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

The UI is deployed separately and calls the API cross-origin from the
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

`REPLAY_PUBLIC_HOST` needs a DNS name that resolves to the Universe host so Caddy
can obtain a public certificate; reserve a static public IP first, since the name
only follows the address. Moving to an owned domain changes `REPLAY_PUBLIC_HOST`
(and the UI's API base URL) only; moving the UI changes `siwe_domain` and
`siwe_uri`, and users then sign in again. The localhost-only Universe port remains
available for SSH tunnels and host operations.

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
