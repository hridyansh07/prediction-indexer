# Selected-Bundle Event Universe Store V1

**Status:** implemented contract on `feat/event-universe`.

## 1. Purpose and decision

Proceed with Event Universe as a sparse, durable query index over **bundles
selected by Targeter** and the archived capture intervals relevant to them.

This is not a catalogue of every event or market Targeter inspected. It is not
a second copy of Targeter's JSON archives. Its purpose is to make the following
questions cheap and deterministic:

- Which event bundles did the system select?
- Which event and sibling-market references belonged to that selected bundle?
- Which markets were selected for capture, and with which subscription IDs?
- What relationship edges did Targeter surface?
- What bounded interval should a future data pull search?
- Which immutable archived segments overlap that interval?
- Which normalized splice controls and connection epochs exist in that span?

V1 deliberately does **not** define a ReplayPlan, invoke current replay gates,
choose where replay executes, or add a UI. Those decisions depend on a future
data-pull and replay execution model. The existing Targeter UI can consume this
API later without making its current data model part of this contract.

V1 also does not add human-authored links. Humans cannot introduce markets into
Targeter's selected bundle, so manual relationship state would add an
unproductive second authority.

## 2. Governing boundaries

### 2.1 Immutable evidence remains authoritative

SQLite is durable operational state and a query accelerator. It is not the
evidence archive.

| Evidence | Commit marker | Universe use |
|---|---|---|
| New Targeter run | `run_manifest.json` naming `selected_bundle_index.ndjson[.zst]` | selected bundle facts only |
| Legacy Targeter run | original `run_manifest.json`, plus a source-binding derivative receipt | lazily project selected bundle facts once |
| Raw segment | local production `.archive.json`; remote segment-universe receipt commits only its derivative | verified interval and controls |
| Universe database | one SQLite transaction per immutable source | query acceleration and checkpoints |

An object listing is discovery, never a commit marker. Every consumed object is
closed-parsed and checked against its recorded stored and, where applicable,
decoded identity. Repeating an identical source is a no-op. Reusing an
immutable key with another identity fails closed.

### 2.2 Capture and interpretation boundaries do not move

Event Universe does not parse venue data frames, map deliveries to markets, or
change capture behavior. Splices still record one envelope per application
delivery. Venue payload interpretation remains in replay or analysis.

The only raw rows indexed by V1 are normalized control facts. There is no row
per data delivery and no `target_records` table.

## 3. Selected-bundle projection

Every new Targeter run writes one deterministic compact artifact:

```text
selected_bundle_index.ndjson
selected_bundle_index.ndjson.zst
```

The configured Targeter artifact format chooses one name. The artifact is in
the run's artifact inventory and is therefore committed by the immutable run
manifest.

The projection reads `selection.bundle_ids` and emits exactly one row for each
matching candidate. Rejected candidates and unselected catalogue records do
not appear. Each row contains only:

- projection version, run ID, generation time, input completeness, and strategy
  version;
- bundle ID, sport, optional game/topology, participants, and participant keys;
- activation time, capture start, and planned capture end;
- selected bundle event references;
- sibling market references and a selected/not-selected bit;
- selected targets, canonical class, and normalized subscription IDs;
- normalized relationship edges.

It contains no source report, catalogue record, target vendor record, or other
vendor JSON.

Serialization is deterministic: bundles, markets, targets, event references,
assets, and relationships are explicitly sorted. Compressed output uses the
shared `encoder` profile: exact NDJSON, Zstandard level 3, frame checksum, no
dictionary, exactly one frame, and logical plus stored identities.

### 3.1 Planned capture bounds

For strategy policy values recorded by the run:

```text
planned_capture_start = activation_at - pre_event_seconds
planned_capture_end   = activation_at + post_start_retention_seconds
```

The projection checks that Targeter's `capture_start_at` equals the first
expression. Current policy is one hour before activation and six hours after
activation. Historical reports written before the policy fields existed use an
explicit closed mapping for strategy versions 1–3; current mutable config is
never applied retroactively.

`planned_capture_end` is a bounded search limit. It is not an observed event
close, a settlement time, or a completeness claim.

## 4. Lazy projection for committed historical runs

Historical runs need no eager migration before Event Universe can start. When
sync encounters a committed run whose manifest has no selected-bundle index:

1. verify the original immutable run manifest;
2. verify and decode its manifest-owned selection report;
3. project only candidates named by `selection.bundle_ids`;
4. publish `selected_bundle_index.ndjson.zst` beside that run using immutable
   create-or-verify semantics;
5. publish `selected_bundle_index.receipt.json` last;
6. ingest the compact artifact.

The derivative receipt binds:

- the original manifest key and stored identity;
- the selection-report key, stored identity, and decoded identity;
- the selected-bundle projection version;
- the compact artifact key, logical/stored identities, content metadata, and
  compression contract.

It states `authoritative_run_commit: false` and
`authorizes_run_publication: false`. It does not modify or supersede the
original run manifest. It commits only this derived projection.

After the receipt exists, subsequent syncs verify and use the compact artifact
without downloading or parsing the source report again. An orphan artifact
from a crash is accepted only after strict decoding proves that its logical
bytes equal the deterministic projection. Different bytes or metadata at the
same key are an integrity conflict; the receipt is not published.

This lazy path is also the Targeter historical path. No separate bulk Targeter
backfill format or local Targeter archive mount is required. Running it against
production S3 remains an explicit operational choice, not a test step.

## 5. Raw segment universe derivatives

The separately deployed service cannot assume access to the capture spool.
After a production raw archive receipt has reverified its data and seal, the
archiver can publish:

```text
<segment>.archive-receipt-mirror.json
<segment>.control.ndjson.zst
<segment>.universe.json
```

The receipt mirror preserves exact local production-receipt bytes for remote
discovery but explicitly carries:

```json
{"authoritative_commit_marker":false,"authorizes_deletion":false}
```

It never authorizes raw reaping. The raw reaper continues to trust only its
retained local production receipt.

The control sidecar contains exact original envelope lines whose
`kind == "control"`, in source order. It does not parse venue frames, infer
asset delivery, or treat transport send completion as venue acceptance.

`<segment>.universe.json` is published last and commits the derivative. It
binds the source receipt/mirror identity, archive location, lane and segment
identity, half-open segment interval, raw data and seal objects, control object,
logical/stored identities, compression profile, and control delivery bounds.

This is a **segment universe receipt**, not a catalogue receipt. It buys an
S3-native, immutable inventory proving which segment and control derivative the
remote universe may index. It does not claim one segment or one UTC day contains
a complete event.

Raw archive commitment happens first. Derivative failure is reported but never
invalidates or overwrites a successful raw archive.

## 6. Time and connection semantics

Events and connection epochs can cross segments, S3 date prefixes, and UTC
midnight. Therefore V1:

- queries segments by half-open interval overlap;
- folds controls by `(lane_id, delivery_index)` across every ingested segment;
- groups lifecycle controls by `(lane_id, connection_epoch)`;
- preserves predecessor epochs per lane;
- leaves missing open/close/acceptance evidence as `unknown`;
- never truncates an open epoch at a query or UTC-day boundary.

Evidence language remains narrow:

| State | Required evidence |
|---|---|
| `selected` | compact Targeter projection names the target |
| `socket_opened` | `connection_opened` control observed |
| `subscription_send_completed` | `subscription_sent` control observed |
| `venue_acceptance_observed` | explicit acceptance control observed |
| `asset_delivery_observed` | unavailable in V1 |

`target_digest` identifies a venue and sorted asset set, not a unique Targeter
run. One matching selected generation is reported as `exact`; repeated matches
are `ambiguous` with a candidate count. V1 never chooses a nearby run and calls
it exact.

## 7. SQLite schema and durability

The complete V1 schema lives separately for review:

```text
universe/schema/README.md
universe/schema/v1.sql
```

Runtime code loads that packaged SQL; schema text does not live in
`universe/store.py`.

The normalized schema stores:

- immutable ingestion source identities and checkpoints;
- selected-bundle source/run metadata;
- versioned selected bundles keyed by `(run_id, bundle_id)`;
- participants and selected event references;
- sibling market references and selected targets/assets;
- normalized relationship edges and subscription-set identities;
- verified segment intervals;
- normalized control facts and folded connection epochs.

It deliberately has no catalogue-event table, catalogue-market table, target
vendor-record table, source JSON column, report JSON column, control detail JSON
column, or envelope JSON column. Exact bytes remain in immutable object storage;
keys and hashes route back to them.

SQLite on a small burstable VM with an attached persistent SSD is appropriate
for this sparse model. The dense per-delivery or all-catalogue model is not.
V1 uses WAL, `synchronous=FULL`, foreign keys, a busy timeout, one
`BEGIN IMMEDIATE` transaction per source, read-only API connections, and
SQLite-native online backups.

The database is durable state, not disposable deployment scratch. Preserve the
volume across instance replacement, publish periodic immutable backups, test
restores with `PRAGMA integrity_check`, and retain the source archives for
disaster recovery. A full rebuild is possible but is expected to be the most
expensive operational path.

## 8. Incremental ingestion

`universe/run_sync.py` is a direct one-shot script with no argument parser and
no internal interval loop. It reads `configs/event_universe.json`; an external
scheduler owns cadence.

It scans:

1. `targeter-v2/runs/` for run manifests;
2. `raw/` for segment-universe receipts.

For each Targeter manifest it consumes the manifest-committed compact index or
uses the lazy historical path in §4. It does not download catalogue artifacts
or target-record artifacts. It inserts only selected projection rows.

For each raw receipt it verifies and decodes the bounded control sidecar,
inserts normalized controls, and refolds the complete affected lane. Receipt
arrival order therefore does not change final epoch state.

One malformed source is reported without concealing later failures. The process
returns non-zero if any source failed. Source key plus identity makes retries
idempotent.

Historical raw archives are optional on day one. When desired, the capture-side
receipt-mirror job publishes metadata-only mirrors, and
`universe/run_backfill.py` on the universe host streams committed S3 raw objects
through the existing `archive.common.verify.decode_archived_segment` decoder.
It stages only the current segment's control derivative, never accumulates raw
history locally, publishes immutable sidecars/receipts, and resumes through a
SQLite checkpoint.

## 9. Read API

The server is read-only JSON:

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | schema version and normalized row counts |
| `GET /v1/bundles?limit=N` | latest selected version of each bundle |
| `GET /v1/bundles/<bundle_id>` | latest run, participants, event refs, siblings, selected targets/assets, relationships, source identities, and subscription evidence |
| `GET /v1/bundles/<bundle_id>/segments?lane_id=` | verified segments overlapping that bundle's planned capture bounds |
| `GET /v1/segments?start_ns=&end_ns=&lane_id=` | verified segments overlapping an explicit interval |
| `GET /v1/epochs?start_ns=&end_ns=&lane_id=` | folded connection epochs overlapping an interval |

Returning the latest bundle version does not erase older selected generations;
they remain versioned by run in SQLite for provenance and ambiguity checks.

There is no write endpoint, replay endpoint, manual-link endpoint, or frontend
in V1.

## 10. Deployment

Recommended initial topology:

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

`docker/universe.Dockerfile` is separate from capture/ingester images.
`compose.universe.yaml` mounts only readable config and the universe volume; it
does not mount the capture spool. The default image command starts the server,
so operators do not construct a long command:

```bash
docker compose -f compose.universe.yaml up -d event-universe
```

Scheduled jobs invoke the direct scripts already named in Compose. API latency
is not a correctness requirement. Scale persistent storage before VM compute;
monitor disk, sync failures, backup age, and integrity checks. Keep one
sync/backfill writer at a time, database and WAL on the same filesystem, and the
API private or behind the existing authenticated TLS proxy.

## 11. Failure behavior

- Native compact artifact absent: verify the report and lazily publish the
  derivative plus receipt.
- Derivative artifact uploaded but receipt absent: prove exact logical bytes,
  then publish the source-binding receipt; conflict fails closed.
- Derivative receipt present: use its compact artifact without reparsing the
  report.
- Source key or identity changes: fail closed; never overwrite indexed rows.
- SQLite process dies before commit: the source transaction rolls back.
- Identical retry after commit: return `skipped`.
- Raw control object exists without segment-universe receipt: ignore it.
- Segment-universe receipt names missing or drifting bytes: fail closed.
- Connection close is absent: epoch end remains unknown.
- Same target digest occurs in several selected runs: report ambiguity.
- Backup dies before atomic rename: no final backup is published.

## 12. Acceptance criteria

V1 is complete when automated checks prove:

1. new Targeter runs emit a deterministic manifest-committed selected-bundle
   index;
2. rejected candidates and unselected catalogue records never become universe
   bundle rows;
3. the SQL schema is separate and stores no source/report/vendor/control JSON;
4. native compact ingestion does not read report, catalogue, or target-record
   artifacts;
5. a legacy run lazily publishes one immutable source-bound derivative and
   later syncs use it without reparsing the report;
6. conflicting derivative bytes or metadata fail closed;
7. repeated selected generations remain versioned and ambiguity is visible;
8. planned bounds select every and only overlapping verified segment;
9. one connection epoch folds across segment and UTC-day boundaries;
10. raw archival and historical S3 backfill use the existing strict decoder and
    never require a capture-spool mount on the universe host;
11. SQLite-native backup passes an independent integrity check;
12. Targeter, archive, object-store, deployment, and Universe regressions remain
    green.

No criterion requires a ReplayPlan, current replay-gate integration, manual
relationships, per-data-record indexing, UI work, or a production backfill run.
