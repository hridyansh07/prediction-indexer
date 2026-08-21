# Selected-Bundle Event Universe Store V1

**Status:** implemented contract on `feat/event-universe`.

## 1. Purpose

Event Universe is a sparse, durable index of the **currently active bundles
selected by Targeter**. It exists to answer:

- which event bundles and sibling markets are active now;
- when and from which immutable Targeter run each bundle was first selected;
- which targets and subscription IDs belong to it;
- which planned capture interval bounds the event; and
- which archived S3 segments overlap that interval.

It is not a catalogue of every event Targeter inspected, a Targeter run-history
database, or a second copy of archived JSON. It stores no venue-frame rows and
does not map individual deliveries to markets.

V1 does not define a replay plan, invoke existing replay gates, choose where a
future replay runs, add human-authored links, or add UI behavior. Those remain
separate decisions. The API provides the bounded event and S3-location harness a
future replay path can consume.

## 2. Input contract

Universe consumes only the current Targeter v3 producer contract:

- a version-2 archived run manifest, which remains the immutable remote commit
  marker;
- exactly one manifest-owned Targeter report with `report_version: 3`;
- exactly one manifest-owned `selected_bundle_index.ndjson[.zst]` with
  `selected_bundle_index_version: 3`; and
- `input_complete: true` in both manifest and report.

The report and index are identity-verified and the deterministic index is
reprojected from the report before admission. Rejected candidates, unselected
catalogue records, and complete report JSON are not stored.

There is deliberately no Universe support for Targeter report/index v1 or v2.
No v3 Universe was deployed before this contract, so nullable origins, legacy
lazy derivation, derivative receipts, migration machinery, and mixed-version
admission add risk without buying compatibility. A non-v3 Universe database is
rejected and must be rebuilt.

## 3. Active snapshot selection

Sync lists immutable Targeter run manifests and chooses the greatest canonical
Targeter run ID. Run IDs are UTC timestamps and lexicographically time ordered.
The latest successfully archived, complete v3 run is therefore the active
snapshot.

There is no durable Universe publication pointer. In particular, Universe does
not consume or reinterpret Targeter's atomic `current.json` generation pointer;
that pointer continues to govern splice publication only.

If the newest manifest or any evidence it references fails validation, sync
does not fall back to an older run and does not modify the existing active
snapshot. An identical active run is a no-op. An older run cannot replace a
newer one. A valid newer run with no selected bundles atomically clears the
active bundle set.

`/healthz` reports snapshot age and `stale` after one hour. Age is measured from
the manifest-owned run `generated_at`, not first observation by Universe. That
timestamp precedes archive commitment, so delayed sync is conservatively stale
rather than incorrectly appearing fresh.

## 4. Selected-bundle projection

For a newly selected or held-current candidate, the v3 index carries complete
context:

- run and strategy version;
- bundle ID, sport, optional game/topology, participants and participant keys;
- activation, one-hour lookahead capture start, and bounded planned end;
- event references and all sibling market references;
- selected targets, canonical classes, source references, and subscription IDs;
- relationship edges; and
- continuity indication and `held_current_candidate` disposition when present.

The planned end is `activation_at + post_start_retention_seconds`. It is a
bounded search limit, not an observed event close, settlement time, or data
completeness claim.

The projection is deterministic. Bundles, event references, markets, targets,
assets, and relationships are explicitly sorted. Zstandard artifacts use the
shared `encoder` profile: exact logical bytes, level 3, checksum enabled, no
dictionary, and exactly one frame.

## 5. Continuity-retained selections

Targeter continuity behavior remains authoritative and unchanged. A retained
v3 row is intentionally sparse and must provide:

- `origin_run_id`;
- exact origin report SHA-256;
- exact origin archive-manifest key and SHA-256;
- current retained targets and capture timing; and
- `continuity_selected: true` with disposition `retained`.

Universe resolves that immutable reference during sync. It verifies the exact
origin manifest, report, and native v3 selected-bundle index, then requires one
matching **complete** origin row. Current targets, activation, and capture start
must exactly match the origin row.

The active row copies normalized origin event, sibling, relationship, target,
and planned-bound context. It also records both current snapshot provenance and
non-null origin manifest/report/index keys, hashes, and lengths. Queries do not
join to a prior Universe occurrence, so rebuilding or replacing the database
cannot make an admitted origin unresolved.

Missing origin objects, wrong hashes, retained-origin rows, missing bundle rows,
or target/timing drift reject the entire incoming snapshot. Universe never
weakens continuity, guesses an origin, or admits a nullable/unresolved record.

## 6. Raw segment locations and controls

Event origin and S3 segment location are separate evidence dimensions. A
Targeter run proves what was selected; a segment-universe receipt proves which
archived capture object covers an interval.

After raw archival succeeds, the archiver may publish:

```text
<segment>.archive-receipt-mirror.json
<segment>.control.ndjson.zst
<segment>.universe.json
```

The segment-universe receipt is published last. It binds the source receipt,
archived data and seal objects, lane and segment identity, half-open interval,
exact control lines, compression profile, and logical/stored identities. It is
not an authoritative raw commit marker and never authorizes deletion.

Universe stores one interval/location row per verified receipt and normalized
control/epoch facts only. It queries overlapping S3 objects using:

```text
segment.window_end_ns > bundle.capture_start_at_ns
and segment.window_start_ns < bundle.planned_capture_end_at_ns
```

Events and connections may cross segment boundaries, UTC dates, and S3
prefixes. Controls are therefore folded by `(lane_id, delivery_index)` across
all admitted receipts and grouped by `(lane_id, connection_epoch)`. Missing
open, close, send, or acceptance evidence stays `unknown`.

Historical raw backfill remains optional. `universe/run_backfill.py` discovers
remote receipt mirrors and streams each committed S3 raw object through the
existing strict `archive.common.verify.decode_archived_segment` decoder. It
stages only the current segment and resumes through a SQLite checkpoint; no
capture-spool mount or accumulated local raw history is required.

## 7. SQLite schema and atomicity

The complete schema is separately reviewable at:

```text
universe/schema/README.md
universe/schema/v3.sql
```

It stores:

- one singleton active snapshot;
- one row per active selected bundle;
- copied normalized event, sibling, target, asset, and relationship context;
- exact current and origin object identities;
- verified segment intervals and S3 keys;
- normalized control records and folded connection epochs; and
- ingestion checkpoints for raw-control history.

It has no catalogue, Targeter report, vendor target-record, venue-frame,
envelope-JSON, or arbitrary source-JSON column.

The complete incoming active set is validated before mutation and replaced in
one `BEGIN IMMEDIATE` transaction. SQLite uses WAL, `synchronous=FULL`, foreign
keys, read-only API connections, and SQLite-native online backups. Any insert or
constraint failure rolls back the new snapshot and leaves the prior one intact.

SQLite is durable operational state and a query accelerator. Immutable S3
objects remain evidence authority. A small burstable VM with an attached
persistent SSD is appropriate because the store scales with active selected
bundles and control records, not 13.6 million daily venue deliveries or every
catalogue market. The volume and backups should still be durable because a full
rebuild is the most expensive recovery path.

## 8. Sync and read API

`universe/run_sync.py` is a direct one-shot script with readable JSON config and
no argument parser or internal scheduling loop. External scheduling owns
cadence. Each run:

1. verifies and atomically projects the latest Targeter v3 snapshot; then
2. incrementally verifies new raw segment-universe receipts.

A Targeter failure preserves the prior active snapshot. A malformed raw receipt
is reported without concealing later raw receipts. The process exits non-zero if
any source fails.

The read-only JSON API is:

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | schema version, active run, age/staleness, and row counts |
| `GET /v1/bundles?limit=N` | active selected bundles only |
| `GET /v1/bundles/<bundle_id>` | current and origin provenance plus copied event, sibling, target, relationship, and subscription context |
| `GET /v1/bundles/<bundle_id>/segments?lane_id=` | verified S3 segments overlapping the bundle bounds |
| `GET /v1/segments?start_ns=&end_ns=&lane_id=` | verified S3 segments overlapping explicit bounds |
| `GET /v1/epochs?start_ns=&end_ns=&lane_id=` | folded connection epochs overlapping explicit bounds |

There is no write, replay, manual-link, historical-occurrence, or frontend
endpoint in V1.

## 9. Deployment

Event Universe is deployed separately from capture and ingester processes:

```text
small burstable VM
├── attached persistent SSD
│   ├── event-universe.sqlite3 and WAL
│   ├── bounded temporary workspace
│   └── local SQLite backups
├── read-only Event Universe server
├── scheduled one-shot sync
└── scheduled one-shot backup/upload
```

`docker/universe.Dockerfile` starts the API by default. The server therefore
requires no hand-built command:

```bash
docker compose -f compose.universe.yaml up -d event-universe
```

Compose mounts persistent Universe state and readable config, not the capture
spool. Scheduled jobs directly invoke `universe/run_sync.py`,
`universe/run_backfill.py`, and `universe/run_backup.py`.

## 10. Acceptance criteria

V1 is complete when tests prove:

1. only the newest complete archived Targeter v3 run becomes active;
2. report/index and stored/logical identities are verified;
3. retained selections resolve one exact complete immutable origin;
4. missing, conflicting, or incomplete origin evidence preserves prior state;
5. newer active sets replace older sets atomically and empty sets clear them;
6. older runs cannot replace newer active state;
7. staleness is visible and deterministic;
8. no legacy report/index, lazy derivative, nullable origin, occurrence history,
   catalogue JSON, or venue-frame row is admitted;
9. planned bounds select every and only overlapping verified S3 segment;
10. connection epochs fold across segment and UTC-day boundaries;
11. historical raw backfill uses the shared strict decoder without a capture
    filesystem mount; and
12. backups pass an independent SQLite integrity check.

No criterion requires a durable Universe pointer, ReplayPlan, current replay
gate integration, manual relationships, UI work, per-delivery indexing, or a
production backfill run.
