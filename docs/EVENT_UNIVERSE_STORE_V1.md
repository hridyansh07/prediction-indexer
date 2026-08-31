# Event/Market Universe Store V2

**Status:** implemented contract. The historical filename is retained so
existing documentation links remain valid.

## 1. Purpose and authority

Event Universe is a rebuildable SQLite view of committed Targeter v3 runs. It
normalizes events, venue events, canonical markets, venue markets, selection
decisions, selected-market occurrences, and market relationships so clients do
not need to download or understand Targeter report payloads.

Immutable Targeter run manifests and their objects remain authoritative. The
database is a query accelerator, not a new commit protocol or evidence archive.
It may be deleted and reconstructed by sync/backfill. Universe does not copy
raw reports, raw catalogues, capture deliveries, replay state, or provider
details into SQLite.

The prior selected-bundle history APIs remain available. The former embedded
cadence projection and `GET /v1/targeter/cadence` are removed because their
payload size grows with runs, candidates, and relationships.

## 2. Verified source contract

The only admitted commit marker is:

```text
targeter-v2/runs/date=YYYY-MM-DD/run=<run_id>/run_manifest.json
```

Universe accepts manifest version 2 with one Targeter v3 shadow selection
report. Run ID, generated time, completeness, and strategy version must agree.
It also consumes the manifest-owned normalized event/market NDJSON artifacts
needed to resolve candidate references.

All object access is provider-neutral:

- metadata is checked through `verify_metadata_objects` in manifest order;
- bounded JSON uses `archive.read_verified_json`;
- normalized NDJSON uses `ArchivedObjectByteStreamer`; and
- Zstandard decoding and stored/logical identity checks remain in the shared
  archive/encoder packages.

Universe does not implement S3/GCS reads, checksums, private staging, Zstandard
decoding, or JSON-size enforcement. Selection reports are capped at 128 MiB
decoded and each normalized NDJSON artifact at 256 MiB decoded.

## 3. Canonical identity

An umbrella event ID is a deterministic digest of sport, optional game and
topology, sorted participant keys, and activation time. It never includes a
venue ID. Venue-native identities are `(venue, venue_event_id)` and
`(venue, venue_market_id)`; this contract assumes each pair is globally unique
and never reused by its venue.

A canonical market ID is a deterministic digest of:

- umbrella event ID;
- canonical class and market type;
- scope; and
- normalized semantic parameters.

`market_template_version` and `outcome_space_version` are explicit key columns.
They are not hidden in the canonical market ID.

First- and last-seen run IDs make continuity explicit. Re-ingesting the same
verified run is idempotent. A venue-native identity that is assigned to a
different umbrella event fails closed.

## 4. Relationships

Relationships are normalized into `relations`, `relation_members`, and
`relation_observations`. A relation supports any number of members and optional
claim keys even though current Targeter reports emit two members.

The canonical hash contains only the relationship type and normalized members.
It excludes event, run, bundle, scope, coverage, generation version, market
template version, and outcome-space version. Symmetric types normalize every
member to role `member` and sort by venue, venue market ID, claim key, and role.
Directed types preserve `left`/`right` roles. Generation version is an explicit
database key column rather than part of the hash input.

`GET /v1/relationship-types` publishes the closed current type catalogue and
its directed/member-role semantics.

## 5. Selection continuity and history

Current candidate selections are linked directly to normalized event and venue
market rows. A retained selection may be absent from the current catalogue;
Universe recursively verifies and ingests its exact complete origin, then
copies the origin's normalized selected-market references into the current run.
Missing, cyclic, mismatched, or non-complete origins reject the transaction.

The existing content-addressed bundle context, occurrence, and retirement
tables remain for backward-compatible `/v1/bundles`, `/v1/selections`, and
selection-detail APIs. Terminal observation remains an upper bound; a clamp is
not represented as an exact event end.

## 6. SQLite schema and rebuild

Runtime schema version is 3 and is composed from:

```text
universe/schema/v1.sql  # historical bundle APIs and source identities
universe/schema/v3.sql  # event/market universe
```

Schema v3 intentionally has no in-place migration. Before deploying this
version, stop Universe jobs and remove the existing rebuildable SQLite database
(including its WAL/SHM siblings), then run sync/backfill against the immutable
archive. Starting against schema v1/v2 fails with a clear rebuild instruction.

Each admitted run and both projections are inserted in one `BEGIN IMMEDIATE`
transaction with foreign keys, WAL, and `synchronous=FULL`. A projection
identity detects changed re-ingestion input. SQLite backup uses the native
online backup API followed by `integrity_check`.

## 7. Sync and backfill

`universe/run_sync.py` and `universe/run_backfill.py` are scheduler-owned
one-shot jobs. Incremental sync continues after an independently bad manifest
and keeps its checkpoint at the earliest failed date for retry. Backfill uses
the configured half-open generated-time range. Origin dependencies may be
ingested outside that range when required to prove continuity.

Incomplete and complete-empty runs remain visible but create no selection
rows. The archive remains sufficient to rebuild all SQLite state.

## 8. Read API

All responses are strict JSON and remain under the 1.75 MB server budget. List
limits are 1–100 and list cursors are opaque and query-specific.

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Schema, latest run, staleness, and normalized counts |
| `GET /v1/targeter/status?limit=5` | Compact landing status and newest complete selection counts |
| `GET /v1/targeter/runs/<run_id>` | Bounded decisions and normalized event/market/relation references |
| `GET /v1/events?limit=&cursor=` | Canonical event summaries |
| `GET /v1/events/<event_id>` | Event, venue events, canonical markets, relations, observations |
| `GET /v1/markets/<market_id>` | Canonical market, venue instances, selections, relations |
| `GET /v1/relations/<relation_id>` | Relation, normalized members, observations |
| `GET /v1/relationship-types` | Closed relationship-type catalogue |
| `GET /v1/runs`, `/v1/selections`, `/v1/bundles` | Historical compatibility APIs |

`GET /v1/targeter/cadence` returns 404. Run detail intentionally omits raw
candidate relationship arrays and raw report payloads; clients follow event,
market, and relation IDs for detail.

The API process reads SQLite only and needs no object-store credentials. Sync
owns all verified archive retrieval.
