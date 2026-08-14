# Event Universe Store V1

**Status:** implemented contract on `feat/event-universe`.

The Event Universe is a durable, queryable index over immutable Targeter and raw
capture evidence. It makes event, sibling-market, selection, connection, and
segment relationships cheap to inspect without deciding how a future replay
harness must consume them.

This document supersedes the earlier proposal in this file. In particular, V1:

- does not add a ReplayPlan or change anything under `replay/`;
- does not add human-authored links;
- does not add or extend a UI;
- does not claim per-asset delivery from venue payloads;
- does not treat a UTC day or one archive object as an event-completeness boundary;
- uses a per-segment universe receipt, not a published daily "catalogue receipt."

The implementation lives in `universe/` and the raw derivative publisher lives
in `archive/archiver/universe.py`.

---

## 1. Decision

Proceed with the Event Universe, but as an evidence index rather than a replay
format.

This is worthwhile because the expensive facts already exist in two durable
streams that are difficult to join interactively:

1. Targeter run archives contain event/market grouping, sibling markets,
   selected targets, and selected markets' exact vendor records.
2. Raw segment archives contain splice lifecycle controls and the raw data spans
   those controls describe.

The universe verifies those sources, indexes their relationships in SQLite, and
keeps enough identity to detect drift or ambiguity. It does not manufacture an
event file, select replay gates, parse venue frames, or claim that one database
row is sufficient input to replay. Those choices depend on the future pull and
execution model and are intentionally deferred.

---

## 2. Authority and trust boundaries

SQLite is durable operational state, but it is not the evidence authority.

| Evidence | Commit marker | Indexed facts |
|---|---|---|
| Targeter v2 run | `run_manifest.json` | catalog events and markets, bundle/sibling membership, selections, target records |
| Raw segment | production `.archive.json` plus the segment universe receipt | verified raw interval and exact control envelopes |
| Universe database | SQLite transaction plus source identity | query acceleration, folds, checkpoints, explicit diagnostics |

An object listing is discovery only. The ingester may discover
`run_manifest.json` and `.universe.json` keys with `ListObjectsV2`, but it must
then closed-parse and verify the commit marker and every object it consumes.
The existence of an uncommitted sidecar, data object, or S3 prefix is never
evidence by itself.

Every indexed source is keyed by its immutable object key and SHA-256. Repeating
the same source is a no-op. Reusing a key with another identity is an integrity
conflict and fails closed.

---

## 3. Raw archive derivatives

After a production raw archive receipt has reverified both its data and seal,
the archiver independently derives:

```text
<segment>.control.ndjson.zst
<segment>.universe.json
```

The control sidecar contains the exact original envelope lines whose
`kind == "control"`, in source order. It does not:

- parse or normalize venue frames;
- count asset delivery;
- infer acceptance from a successful socket send;
- filter lifecycle events by economic relevance.

The shared `encoder` contract applies: exact NDJSON, Zstandard level 3, checksum
enabled, no dictionary, one frame, and both logical and stored identities.

### 3.1 Segment universe receipt

`<segment>.universe.json` is published last and is the commit marker for the
sidecar. Its closed V1 schema binds:

- archive location;
- lane, segment ID/index, and `[window_start_ns, window_end_ns)`;
- the local production archive receipt filename, byte length, SHA-256, and
  verification instant;
- source logical identity;
- raw data key and stored identity;
- seal key and stored identity;
- control key, content metadata, logical/stored identities, compression
  contract, and first/last control delivery indexes;
- publication instant and publisher version.

Call this a **segment universe receipt** or **segment universe commit record**.
It is not a catalogue receipt.

What it buys us is an S3-native, immutable, independently verifiable inventory
of raw segments the universe is allowed to reference. The local raw archive
receipt is authoritative but is not itself uploaded. The universe receipt binds
that receipt's identity to the raw data, seal, and small control derivative, so
the query service does not have to trust an arbitrary data-key listing or copy
the capture host's local filesystem.

It does **not** claim that the segment contains a complete event, all sibling
markets, one Targeter generation, or one day of state.

### 3.2 Independent failure

Raw archival commits first. A sidecar or universe-receipt failure is reported as
`universe_failed` and makes the archive command non-zero, but the raw outcome
remains `archived` or `skipped`. Retrying is immutable and idempotent. A sidecar
failure must never invalidate or overwrite a successful raw archive.

---

## 4. Time and state semantics

A connection epoch may begin in one segment and finish in another. It may cross
UTC midnight. An event may be selected by several Targeter runs and its data may
span many S3 date prefixes.

Therefore:

- fold controls by `(lane_id, delivery_index)` across every ingested segment;
- group lifecycle controls by `(lane_id, connection_epoch)`;
- preserve the predecessor epoch for each lane;
- query raw segments by interval overlap, not by one date or one object;
- retain open/unknown boundaries as unknown rather than truncating at the end of
  a query or UTC day.

Daily manifests may remain useful derived accelerators elsewhere, but they are
not required for V1 universe correctness and are not a commit boundary.

The database stores both verified segment intervals and folded connection
epochs. A future data-pull planner can select every overlapping segment, even
when the event spans multiple dates, without the universe prescribing the
planner's output format.

---

## 5. Evidence language

V1 exposes narrowly stated facts:

| State | Required evidence |
|---|---|
| `selected` | Targeter selection report names the market/bundle |
| `socket_opened` | `connection_opened` control observed |
| `subscription_send_completed` | `subscription_sent` control observed after the transport send returned |
| `venue_acceptance_observed` | an explicit acceptance control, if a future splice emits one |
| `asset_delivery_observed` | not available in V1 |

Absence of acceptance evidence is `unknown`, not rejected. A
`subscription_sent` record proves only that the client completed its send; it
does not prove the venue accepted or delivered the subscription. V1 sidecars do
not parse venue-frame acknowledgements, so current generic acceptance state is
normally `unknown`.

Likewise, the store does not claim "actual per-asset delivery." One socket
delivery can contain vendor-specific batches, and determining asset membership
requires payload interpretation that belongs outside archive extraction. A
future vendor parser may add separately versioned evidence without changing the
meaning of V1 rows.

### 5.1 Target digest ambiguity

`target_digest` hashes only the venue and sorted asset IDs. It is a subscription
set identity, not a unique Targeter-run identity. The same set can be selected
by multiple runs.

The universe computes subscription-set digests from selected target assets and
joins them to control epochs. One candidate run is reported as `exact`; multiple
runs are reported as `ambiguous` with the candidate count. It never chooses the
nearest timestamp and calls that an exact historical join.

`target_metadata_digest` is retained from controls when present, but V1 does
not reconstruct publication metadata merely to force a match.

---

## 6. SQLite store

The deployment target is one SQLite database on an attached persistent volume.
It is appropriate because workload is bursty, writes are serialized ingestion
jobs, queries do not need realtime latency, and one small server should be easy
to operate.

Runtime settings:

- WAL journal mode;
- `synchronous=FULL`;
- foreign keys enabled;
- 30-second busy timeout;
- one `BEGIN IMMEDIATE` transaction per immutable source;
- schema version in `PRAGMA user_version`;
- read-only per-request API connections;
- SQLite-native online backups and `PRAGMA integrity_check`.

V1 tables cover:

- immutable source identities and ingestion checkpoints;
- Targeter runs;
- catalog events and markets;
- event bundles and bundle-to-event/market membership;
- selected targets and computed subscription sets/assets;
- exact selected target records, including the vendor record and its content
  hash;
- verified segment universe receipts;
- exact control envelopes;
- globally folded connection epochs;
- structured missing/ambiguous evidence issues.

There are no human-editable relationship tables. Within-venue event grouping
and cross-venue Targeter bundle membership come from archived machine evidence.
Humans cannot introduce a new market into Targeter through this database, so a
manual linking workflow would add state without improving capture and is out of
scope.

### 6.1 Durability model

The database should not be treated as disposable in normal operation. A full
reseed is possible, deterministic, and useful for disaster recovery, but it can
require reading every historical Targeter artifact and control sidecar. That is
the highest-cost path, not a routine deployment step.

Operate it as durable state:

1. place the database and WAL on an attached persistent SSD volume;
2. preserve the volume across instance replacement;
3. run periodic SQLite online backups;
4. publish backup files immutably to independently durable object storage;
5. periodically restore a backup elsewhere and run `PRAGMA integrity_check`;
6. retain immutable source archives so a full reseed remains possible.

Do not copy a live `.sqlite3` file with a filesystem copy command. Use the
`backup` command, which calls SQLite's backup API and atomically publishes the
completed local file.

---

## 7. Incremental ingestion

`python -m universe.cli ... sync` is a one-shot job. A host scheduler owns its
cadence; the command has no internal interval loop.

It performs two scans:

1. `targeter-v2/runs/` for `run_manifest.json` commit markers;
2. `raw/` for `.universe.json` commit markers.

For Targeter runs it supports committed manifest versions 1 and 2, verifies the
manifest-owned object identities and content metadata, strictly decodes bounded
Zstandard artifacts, and indexes:

- normalized catalog event/market records;
- candidate bundles and sibling membership;
- selected targets and assets;
- exact selected vendor target records when present;
- the selection report and completeness state.

For raw segments it closed-parses the segment universe receipt, reverifies raw,
seal, and control objects, strictly decodes the bounded control frame, inserts
exact envelopes, then refolds the complete affected lane. This makes receipt
arrival order irrelevant to the final epoch state.

One malformed source is reported without hiding later sources. The process exits
non-zero when any source failed.

---

## 8. Query API

The V1 server is read-only JSON. It deliberately has no replay endpoint and no
frontend.

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | schema version and row counts |
| `GET /v1/bundles?limit=N` | latest observation of each event bundle |
| `GET /v1/bundles/<bundle_id>` | events, sibling markets, selection, exact target record, subscription evidence, ambiguity/issues |
| `GET /v1/segments?start_ns=&end_ns=&lane_id=` | verified raw segments overlapping an interval |
| `GET /v1/epochs?start_ns=&end_ns=&lane_id=` | folded connection epochs overlapping an interval |

The existing Targeter UI can consume these endpoints in a later phase, but V1
does not modify that UI or make its current data model part of this contract.

---

## 9. Historical backfill

Historical data is first-class, but backfill is optional on day one.

### 9.1 Targeter artifacts

The ordinary sync path indexes every committed Targeter run manifest it
discovers, so existing catalog events, sibling markets, selections, and target
records are naturally resumable historical backfill. Source identities in
SQLite make retries no-ops.

### 9.2 Raw control sidecars

Previously archived raw segments have no universe derivative. Use:

```text
python -m universe.cli --database <db> backfill-controls \
  --receipt-root <retained-spool-or-receipt-root> \
  <object-store arguments>
```

The backfill accepts retained local production archive receipts or explicit
newline-delimited inventories of receipt paths. It never treats an arbitrary S3
data listing as proof of commitment.

For each receipt it:

1. closed-parses the production archive receipt;
2. reverifies the archived raw and seal objects;
3. uses the local sealed source when retained;
4. otherwise strictly reconstructs exact raw NDJSON into temporary storage from
   the receipted archive object;
5. publishes the control sidecar and segment universe receipt idempotently;
6. advances a durable SQLite checkpoint and reports every failure.

The scan revisits old receipt paths so a late-added receipt cannot hide behind a
high-water mark. Immutable publication makes this safe. No production backfill
is run merely to test the code.

---

## 10. Operations on a small burstable VM

Recommended initial topology:

```text
burstable VM
├── attached persistent SSD
│   ├── event-universe.sqlite3
│   └── local SQLite backups
├── scheduled universe sync (single writer)
├── scheduled SQLite backup/upload
└── read-only universe API
```

Start small. Query latency is not a correctness requirement, and WAL permits
readers while the one ingestion writer commits. Scale the volume before the VM;
catalog and evidence tables are likely to dominate disk long before API CPU.

Operational rules:

- run only one sync/backfill writer at a time;
- keep the database and WAL on the same persistent filesystem;
- monitor disk space, sync failures, backup age, and integrity-check results;
- bind the API to a private interface or put authentication/TLS in the existing
  reverse proxy;
- use an instance role for object-store reads and backup writes;
- do not place the service on the capture host;
- do not rebuild the database during ordinary deploys.

Example lifecycle:

```bash
python -m universe.cli --database /var/lib/prediction-indexer/universe/event-universe.sqlite3 init

python -m universe.cli --database /var/lib/prediction-indexer/universe/event-universe.sqlite3 \
  sync --archive-backend s3 --s3-bucket <bucket> --s3-region <region> \
  --s3-expected-owner <account-id>

python -m universe.cli --database /var/lib/prediction-indexer/universe/event-universe.sqlite3 \
  backup --output /var/lib/prediction-indexer/universe/backups/<timestamp>.sqlite3 \
  --object-key universe/backups/<timestamp>.sqlite3 \
  --archive-backend s3 --s3-bucket <bucket> --s3-region <region> \
  --s3-expected-owner <account-id>

python -m universe.cli --database /var/lib/prediction-indexer/universe/event-universe.sqlite3 \
  serve --host 127.0.0.1 --port 8080
```

Exact scheduler, VM size, volume size, retention, and backup frequency are
deployment choices based on observed database growth. They are not persisted
format semantics.

---

## 11. Failure and crash behavior

- Raw archive committed, sidecar absent: raw remains valid; retry or backfill.
- Control object uploaded, universe receipt absent: sidecar is uncommitted and
  ignored; retry safely publishes/reuses exact bytes.
- Universe receipt present, object missing/drifted: ingestion fails closed.
- Sync crashes before commit: the SQLite source transaction rolls back.
- Sync retries after commit: source key plus SHA-256 returns `skipped`.
- A source key changes identity: integrity conflict; do not overwrite rows.
- Connection opens before the queried day: the global lane fold preserves its
  predecessor/open state.
- Connection close is missing: interval end remains unknown; it is not silently
  clipped.
- Same target digest occurs in multiple runs: historical link is ambiguous.
- Backup crashes before rename: no final backup name is published.

---

## 12. Acceptance criteria

V1 is complete when tests prove:

1. raw archival publishes exact control lines and the receipt last;
2. derivative failure leaves raw archive success intact;
3. immutable retry skips committed artifacts and conflicts on drift;
4. Targeter manifests, catalogs, selections, and target records ingest
   idempotently;
5. one connection epoch folds across segments and UTC dates;
6. socket send and venue acceptance remain distinct evidence states;
7. repeated target digests surface ambiguity;
8. interval queries return every overlapping verified segment;
9. a backfill can reconstruct locally reaped raw data from the verified archive
   and resume safely;
10. a SQLite-native backup passes an independent integrity check;
11. existing archive, S3, Targeter archive, and object-store regressions remain
    green.

No criterion requires replay integration, a ReplayPlan, a manual relationship,
per-asset payload attribution, or UI work.
