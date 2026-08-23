# Selected-Bundle Event Universe Store V1

**Status:** implemented contract on `feat/event-universe`.

## 1. Purpose and boundary

Event Universe is a durable, append-only SQL index of the **bundle selections
Targeter made and their proven retirement observations**. Its useful delta over
Targeter observability is history: a consumer can query which selected event
bundles existed at an event time or a Targeter selection time without
downloading and scanning every archived run.

For each selected occurrence it answers:

- which run selected the bundle and whether continuity retained it;
- when the event activates and when its one-hour lookahead starts;
- when Targeter first observed every selected market terminal, or retired the
  bundle at its safety clamp;
- which event, sibling-market, selected-target, subscription, and relationship
  references belong to the bundle; and
- which immutable Targeter manifest and report contain the current occurrence
  and its proven origin.

It is not:

- a catalogue of everything Targeter inspected;
- a latest-only replacement for Targeter observability;
- a second JSON archive;
- a row-level index of captured venue deliveries;
- an authority for choosing usable raw segments, continuity, or replay gates;
- a replay plan, replay executor, or UI; or
- a place for human-authored market links.

Replay remains responsible for locating and validating raw/canonical evidence
for an event interval. Event Universe supplies selected-event context and exact
Targeter S3 provenance, not a claim that any raw segment is replayable.

## 2. Source and version contract

The sole commit marker is an immutable S3 Targeter run manifest at:

```text
targeter-v2/runs/date=YYYY-MM-DD/run=<run_id>/run_manifest.json
```

V1 accepts only:

- `targeter_run_manifest_version: 2`;
- exactly one manifest-owned `selection_report.json` or
  `selection_report.json.zst`;
- `report_version: 3`, `mode: shadow`, and consistent run ID, generated time,
  input-completeness, and strategy version; and
- canonical run keys and run timestamps that identify the same UTC instant.

Every consumed object is checked against the manifest's byte length, SHA-256,
content metadata, and provider SHA-256 checksum. Zstandard reports are decoded
through the shared strict streaming `encoder` with the manifest's stored and
logical identities and decoded-size bound.

Existing archived v3 runs already satisfy this source contract. Universe
derives its projection directly from their report. It does not require, create,
or upload `selected_bundle_index`, `.universe.json`, `.control.ndjson.zst`, a
receipt mirror, or any other Universe-specific archive artifact.

Targeter report v1/v2 and archive-manifest v1 are rejected. There was no
deployed Universe database to migrate, so V1 has no mixed-version admission,
nullable origin, or schema migration path. Create a fresh schema-v1 database.

An incomplete v3 run is retained as visible run history with zero admitted
selections. A complete v3 run may also legitimately contain zero selections,
including continuity retirement that publishes an empty generation.

## 3. Deterministic selected projection

Only IDs in `selection.bundle_ids` become selection occurrences. A prior
selected bundle may additionally produce a retirement observation under §4.
Rejected candidates, unselected catalogues, target-record artifacts, report
bodies, and arbitrary vendor JSON do not enter SQL.

For each selected current candidate, the projector requires and normalizes:

- bundle ID, sport, optional game/topology, participants, and participant keys;
- event activation and capture-start timestamps;
- event references and all sibling market IDs;
- selected targets, canonical classes, source references, and subscription IDs;
- relationship edges; and
- optional `held_current_candidate` continuity provenance.

The report must name every supported venue in `selection.targets`; a selected
candidate must be eligible, carry at least one target, and agree with target
timing. Selected targets must be members of its sibling-market set. Lists and
rows are sorted explicitly before hashing or insertion.

A current candidate is a `complete` occurrence whose immutable origin is its
current run. This includes `held_current_candidate`: Targeter's v3 publication
contract re-origins a current candidate even when continuity protected its
budget position.

## 4. Continuity origin and retirement

Targeter's v3 continuity behavior remains unchanged. In particular, retained
targets can be absent from current candidates and catalogues, and unknown
terminal state remains protected rather than inferred terminal.

A selected bundle with disposition `retained` is admitted only when its
continuity evidence supplies non-null:

- `origin_run_id`;
- `origin_report_sha256`;
- `origin_archive_manifest_key`; and
- `origin_archive_manifest_sha256`.

Universe follows that exact manifest reference even when the origin lies
outside the incremental or requested backfill range. It verifies and ingests
the origin recursively, requires the referenced report to contain one complete
occurrence for the same bundle, and requires current retained activation,
capture start, targets, and subscription IDs to equal that origin context.

The SQL occurrence points to the separately stored origin occurrence. Both
occurrences reference the same content-addressed normalized context. This
avoids copying the context into every continuity run while keeping every query
fully resolvable in SQL. Selection-detail responses join the origin run and
return its immutable manifest/report keys and hashes.

Origin cycles, missing objects, wrong identities, a retained origin, a missing
origin bundle, or timing/target drift reject the current occurrence's entire
run transaction. Universe never guesses or admits unresolved origin.

Targeter and the venues do not supply an exact event-end timestamp. In
particular, Targeter's terminal probes classify a held market for one run but
do not timestamp when its state changed. Universe therefore does not expose a
misleading `ended_at` value.

For `all_markets_terminal`, Universe verifies that every target in the
continuity evidence has a `terminal` probe and records the report's
`generated_at` as `terminal_observed_at`. The actual all-terminal transition
happened no later than this observation, normally within one Targeter cadence.
For `terminal_clamp_elapsed`, it records `retired_at` and the disposition but
leaves `terminal_observed_at` null: the clamp is safe eviction, not proof of
the event's real end.

Both retirement paths require the same non-null immutable origin identities as
a retained selection. Universe recursively verifies that complete origin and
requires exact activation, capture-start, target, and subscription context.
`continuity_budget_trimmed` is not an event ending and is not projected. An
empty run and every retirement observation remain visible through run history.

## 5. Incremental sync and bounded backfill

`universe/run_sync.py` and `universe/run_backfill.py` are direct one-shot scripts
configured by `configs/event_universe.json`. They have no argument parser or
internal scheduling loop.

Both use the same manifest reader, v3 projector, origin resolver, and append
transaction:

- A fresh incremental store discovers all committed manifests but indexes only
  the latest archived run, establishing useful current state cheaply.
- Later incremental runs list date prefixes from their checkpoint through the
  current UTC date. They append every not-yet-indexed committed run they find,
  including earlier runs on the checkpoint date.
- Bounded backfill lists the half-open Targeter generated-time range
  `[generated_start, generated_end)` from JSON config.
- Repeating either operation is idempotent. The same run and identities are a
  verified no-op; changed immutable evidence or a changed projection conflicts.
- One malformed manifest is reported while independent later manifests remain
  eligible. The incremental checkpoint retains the earliest failed date so it
  is retried.

Origin dependencies are not constrained by the backfill range. Their insertion
is required to make the requested retained occurrence provable and queryable.

Discovery by object listing is not commitment. Only a valid `run_manifest.json`
admits a run. Universe does not consume Targeter's atomic `current.json` splice
publication pointer and publishes no pointer of its own. The greatest indexed
run timestamp is the derived latest archived run. `/healthz` reports it stale
after one hour based on its manifest-owned `generated_at`; this is conservative
when archival or sync was delayed.

## 6. SQL model and durability

The separately reviewable schema is:

```text
universe/schema/README.md
universe/schema/v1.sql
```

It contains:

- `targeter_runs`: exact manifest/report identities and deterministic
  projection identity/count;
- `bundle_contexts` and normalized child tables: content-addressed selected
  event, market, target, asset, and relationship context;
- `selection_occurrences`: append-only `(run_id, bundle_id)` history and exact
  complete/retained origin reference;
- `bundle_retirements`: append-only all-terminal or clamp observations linked
  to an exact complete origin and normalized context; and
- `checkpoints`: incremental discovery progress only.

It contains no report/catalogue JSON, active-snapshot table, raw segment,
control-envelope, connection-epoch, venue-delivery, or replay-plan table.

Each run is validated before and inserted inside one `BEGIN IMMEDIATE`
transaction. SQLite uses foreign keys, WAL, `synchronous=FULL`, bounded read
transactions, and SQLite-native online backup followed by `integrity_check`.
Contexts are deduplicated by canonical SHA-256. Re-materializing normalized SQL
rows must reproduce each stored context and run-projection hash.

Immutable S3 remains evidence authority. SQL is durable operational state and
a query accelerator; keep it on an attached persistent volume and back it up
because replaying all historical reports is the expensive recovery path. A
small burstable instance is appropriate because row growth follows selected
bundles and runs, not all catalogues or 13.6 million daily deliveries.

## 7. Read API

The API is read-only JSON with half-open timestamp filters and opaque stable
cursors:

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Schema, latest indexed run, age/staleness, and counts |
| `GET /v1/runs` | Run history; generated-time and completeness filters |
| `GET /v1/runs/<run_id>` | Source identities, projection identity, and local SQL audit |
| `GET /v1/runs/<run_id>/audit` | Re-materialize and verify that run's SQL projection |
| `GET /v1/runs/<run_id>/selections` | Occurrences selected in one run |
| `GET /v1/runs/<run_id>/selections/<bundle_id>` | Full context plus current and origin S3 provenance |
| `GET /v1/selections` | Cross-run selected history |
| `GET /v1/bundles/<bundle_id>/history` | One bundle's selected occurrence history |

Selection queries support:

- `activation_start` and `activation_end` for event-time filtering;
- `selected_start` and `selected_end` for Targeter generated-time filtering;
- `venue`, `sort=activation|selected`, `limit`, and `cursor`.

RFC 3339 bounds are half-open. Activation sorting is the default for general
selection and per-run queries; selected-time sorting is the default for bundle
history. API source/origin objects return exact Targeter manifest/report S3 keys
and SHA-256 identities. They do not return or infer raw capture-object locations.

Selection list and detail responses include `retirement: null` until a terminal
or clamp observation is indexed. Afterwards the object supplies `retired_at`,
the disposition, nullable `terminal_observed_at`, and the exact retirement
report's run ID, manifest/report S3 keys, and hashes. An all-terminal
`terminal_observed_at` is an observation upper bound, not a fabricated exact
match-end timestamp.

The API process requires no S3 credentials. Its audit endpoint verifies the
stored normalized projection. The sync layer additionally supports an
authoritative audit that rereads the exact manifest/report bytes from S3 before
requiring the SQL audit to pass.

## 8. Deployment

Event Universe is separate from capture and ingester services:

```text
small burstable VM
├── attached persistent volume
│   ├── event-universe.sqlite3 and WAL
│   ├── bounded Zstd staging directory
│   └── local SQLite backups
├── read-only API server
├── scheduled one-shot incremental sync
├── optional bounded one-shot backfill
└── scheduled one-shot backup/upload
```

`docker/universe.Dockerfile` starts the API by default. Compose mounts the
Universe volume and readable config, never the capture spool:

```bash
docker compose -f compose.universe.yaml up -d event-universe
```

Scheduler invocations use the `jobs` profile. Backfill refuses to run until
both bounds are set explicitly in JSON. The archive sidecar has no Universe
responsibility and creates no derivative objects.

## 9. Acceptance criteria

V1 is complete when tests prove:

1. strict v3 manifest/report identities, including shared bounded Zstd decode;
2. fresh latest-run bootstrap and append-only incremental history;
3. bounded, idempotent backfill directly from existing reports;
4. deterministic selected-lifecycle projection and context deduplication;
5. exact recursive retained/retirement origin resolution outside requested
   ranges;
6. fail-closed origin identity, context, target/timing, and all-terminal probe
   checks;
7. visible incomplete and complete-empty run history without false selections;
8. independent event-time and Targeter-time filtering with stable pagination;
9. local SQL projection audit and authoritative source re-verification;
10. honest terminal-observation versus clamp semantics without a fabricated
    exact event-end timestamp;
11. absence of Universe archive sidecars and raw/control/replay tables or APIs;
12. independently readable SQLite backup; and
13. no legacy report admission, UI change, durable Universe pointer, or
    Targeter continuity weakening.
