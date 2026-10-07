# Targeter v2 delivery

Run artifacts, target records, immutable archive, atomic publication, splice
handoff, continuity, scheduling and local-run retention. Discovery and
selection are in [`SELECTION.md`](SELECTION.md).

Safety properties:

1. Every attempted run leaves its discovery evidence locally; `archive` and
   `publish` modes also archive incomplete runs.
2. Only a complete multi-venue selection with a verified independent archive
   can replace live targets. The one exception is the terminal-retirement empty
   generation (see Publication).
3. One pointer commits every venue's target file; a crash cannot expose a new
   file for one venue beside an old file for another.
4. The previous pointer stays authoritative on every pre-pointer failure.
5. Every command is a one-shot transaction. Cron or a systemd timer owns
   cadence; no Targeter v2 command has an interval flag or a sleep loop.
6. A filesystem lease serializes discovery.

## Command and exit status

```bash
.venv/bin/python targeter/run_v2.py --mode {shadow|archive|publish|audit} \
  --strategy configs/targeter_v2.json \
  --output-root <runs> --cache-root <http-cache> --live-root <live>
```

| Mode | Effect |
|---|---|
| `shadow` | discover and write the local run directory |
| `archive` | shadow, then archive the run to the object store |
| `publish` | archive, verify, then atomically publish a target generation |
| `audit` | no discovery; verify the current pointer, generation, archive and run |

Other flags: `--artifact-format {zstd,ndjson}` (default `zstd`),
`--reuse-cache` (offline/debug) | `--force-refresh` (no-op; refresh is the
default) | `--no-response-cache` (live requests, keep only per-host rate-limit
state and the normalized artifacts), `--now <ISO-8601>` (probe/test clock), and
the four `--max-*` probe caps. Defaults: `data/targeter-v2-cache`,
`data/targeter-v2-shadow`, `data/live`.

Exit `0`: completed. `1`: discovery evidence preserved but input incomplete (a
probe cap, an adapter failure or an incomplete catalogue), so nothing was
published. `2`: configuration, lease, durability, archive, publication or
integrity failure. A complete run with no qualifying bundles is a successful
empty result.

`<output-root>/.targeter-v2.lock` (`flock`) is acquired before discovery and
held across archival and publication; an overlap exits `2`.

## Run directory

`<output-root>/<run-id>/`, run ID `YYYYMMDDTHHMMSS.ffffffZ`:

```text
catalog_<venue>_events.ndjson[.zst]
catalog_<venue>_markets.ndjson[.zst]
target_records_<venue>.ndjson[.zst]        # one per venue, possibly empty
rule_templates.ndjson[.zst]
rule_drift.ndjson[.zst]
selection_report.json.zst                  # selection_report.json with --artifact-format ndjson
selection_report.meta.json                 # zstd runs only; written last
run_manifest.json, archive_receipt[.local].json   # added by archival
```

`.zst` files follow the shared `encoder` profile: exact NDJSON, Zstandard level
3, frame checksum, no dictionary, one frame. The report records each
normalized artifact's decoded (SHA-256, length, LF count) and stored (SHA-256,
length) identity under `artifacts`; `selection_report.meta.json`
(`targeter_selection_report_metadata_version: 1`) commits the report frame's
own identities and is written after it. Cached raw HTTP bodies are also
checksummed `.json.zst` frames with adjacent metadata.

## Target records

`target_records.py` writes the venue's own raw record, verbatim, for every
selected market, because `CanonicalMarket.as_record()` omits `raw` and replay
needs the venue terms (token mapping, minimum size, resolution, fees). One file
per supported venue is always written, so an empty file is positive evidence
that nothing was subscribed. Rows are in `target_id` order:

```json
{"version": 1, "run_id": "...", "venue": "...", "target_id": "...",
 "subscription_ids": ["..."], "observed_at": "...", "provenance": "captured",
 "projection_id": "polymarket.v1", "projection_sha256": "...",
 "record_sha256": "...", "record": { }}
```

`record_sha256` and `projection_sha256` use the canonical JSON encoding (sorted
keys, compact separators, `ensure_ascii=False`, `allow_nan=False`) from
`replay/catalog.py`, which owns the projection (the fields the reader consumes)
beside `_instrument`. `projection_id` and `projection_sha256` are `null` for a
venue with no declared projection (Kalshi). Rows are written on every run; there
is no write-on-change compaction. A selected market missing from the catalogue,
or without a raw record, is skipped and listed under the report's
`target_record_diagnostics`. `replay_stream.py` serves the archived target
records of a capture window through receipt-driven, hash-verified byte
streamers (`ArchivedTargetRecordByteStreamer`); object-store listings are never
consulted.

## Run archive

`run_archive.archive_run` validates the report and rejects missing, unexpected,
non-regular or changed artifacts, writes `run_manifest.json` locally
(`targeter_run_manifest_version: 2`), then publishes each artifact to:

```text
targeter-v2/runs/date=<UTC-date>/run=<run-id>/<artifact>
```

Writes are immutable: an identical object is an idempotent retry, different
bytes at one key are an integrity conflict. Stored SHA-256 and length are
verified through the object store, compressed artifacts are strictly decoded
against their logical identity before upload, and the provider checksum is
recorded as separate evidence (the application SHA-256 is never replaced by a
provider checksum or ETag). `run_manifest.json` is uploaded last and is the
remote commit marker. Only after it verifies does the local run receive:

- `archive_receipt.json` (production, version 3: store `provider` and
  `location`, per-object provider checksums; versions 1 and 2 still parse), for
  an independent store; or
- `archive_receipt.local.json` (local conformance), which exercises the
  protocol but never authorizes publication or deletion.

An interrupted prefix with no remote manifest is incomplete and safe to retry.
The store comes from `ARCHIVE_BACKEND` and its settings (`archive/README.md`);
`ARCHIVE_DURABILITY=independent` declares the store a separate durability
domain.

## Publication

`publish_run` requires all of:

- `report_version: 3`, `mode: shadow`, matching `strategy_version`,
  `input_complete: true`, empty `discovery_failures`, and complete catalogue
  summaries for exactly Kalshi, Polymarket and Limitless;
- a non-empty selection, or schema-v3 continuity evidence authorizing
  retirement of every prior bundle (terminal-empty);
- every selected bundle on at least `minimum_venues` venues and its target IDs
  equal to its eligible candidate market IDs;
- every subscription ID, canonical class and source reference cross-checked
  against the archived venue catalogue (a forged but well-formed report is not
  enough);
- a production receipt reverified against an independent store, and local run
  bytes matching it exactly.

An incomplete run never replaces the pointer. An ordinarily empty selection
needs explicit human control. The only automatic empty publication is a report
carrying the exact prior continuity bundles and proof that each was retired by
all-terminal evidence, the terminal clamp, or protected-floor budget trimming.

Output, written under `<live-root>/targeter-v2/`:

```text
generations/<run-id>/
  targets_{kalshi,polymarket,limitless}.json   # an empty file for a venue with no targets
  metadata/<venue>/<metadata_sha256>.json
  manifest.json                                # target_publication_manifest_version 1
current.json                                   # target_generation_pointer_version 1
```

Target files and metadata snapshots are fsynced before `manifest.json`, which
records each file's identity, target digest, metadata digest and count, the
selection-report and archive-manifest identities, and `minimum_venues`.
`current.json` is written last by atomic replace plus directory fsync and holds
the run ID plus the manifest path, SHA-256 and length; it is the live commit
marker. Each target carries its run, bundle, canonical class, activation and
capture times and source reference. A complete generation without a pointer is
abandoned-but-safe, and an identical retry republishes it. After the pointer
commits, first sightings of subscribed assets are recorded in
`<live-root>/coverage.json` (`targeter/coverage.py`).

## Splice handoff

Every subscription-driven splice reads the same `current.json`. `targeter.targets.
load_targets(pointer, venue=...)` checks the pointer version, run ID, relative
manifest path and identity; manifest version, run ID and venue entry; the
relative target path (bounded to the generation) and file identity; target
schema, venue, unique subscription IDs, target digest, metadata digest, count
and snapshot; and that the metadata path stays inside the generation. Legacy
direct target documents remain readable. The splice's reload loop replaces its
subscription set only after `load_targets` succeeds, so a missing, partial,
corrupted or traversing pointer leaves the last valid set active and is retried
on the normal poll interval.

## Continuity and terminal eviction

The committed generation is the only continuity authority (`continuity.py`).
Fresh admission remains a filter over the current eligible set; continuity can
retain the exact targets of a previously committed bundle but can never
introduce a target absent from that generation.

**Terminal probes.** Terminal markets disappear from discovery queries, so each
run directly looks up the markets it holds:

| Venue | Lookup | Terminal when |
|---|---|---|
| Kalshi | one `GET /markets?tickers=<csv>` | `status == finalized`, or `close_time < expiration_time` |
| Polymarket | `GET /markets/<id>` per market | `acceptingOrders == false` (`active` is ignored; it stays true after close) |
| Limitless | `GET /markets/<slug>` per market | `expired == true` or `status == RESOLVED` (`tradeType` stays `clob` and is ignored) |

Open requires an affirmative open shape. A failed, malformed, 404 or ambiguous
lookup is `unknown`, which retains exactly like `open`; eviction on a failed
read would drop live capture on an API blip. Probe results are ephemeral and
carried in no later run.

**Bundle retirement.** A bundle is retained while any leg is `open` or
`unknown`, including legs that are already terminal: a known outcome on one
venue against a live book on another is the signal the project exists to
capture. It is released only when every leg is independently terminal in one
run, or when `activation_at + terminal_clamp_seconds` (28800) has passed, which
is the catch-all for postponement, cancellation and unreadable markets.

**Hold.** Each run partitions bundles into held (still eligible, no terminal
leg, unchanged targets), retained (exact prior targets for a bundle no longer
eligible or with a terminal leg, not yet all-terminal or clamped) and additive.
Held and retained bundles claim budget first, ordered by recorded score, then
additive candidates in rank order. If the protected set alone exceeds a reduced
budget or bundle limit, its lowest-score bundles are trimmed atomically
(`continuity_budget_trimmed`). Additive rejections: `displaced_by_continuity_hold`
when removing the protected usage would make the candidate fit (otherwise
`target_budget_exceeded`), and `continuity_identity_collision` when a changed
`bundle_id` collides with a target a retained bundle owns (prior ownership
wins). The report's `continuity` block records every observed bundle, the
retained IDs and a disposition per bundle (`held_current_candidate`,
`retained`, `all_markets_terminal`, `terminal_clamp_elapsed`,
`continuity_budget_trimmed`).

**Publication evidence.** A retained bundle absent from current discovery is
published only on exact equality with the report's continuity evidence, which
was reconstructed from the committed generation. Version-3 continuity targets
additionally carry the prior generation ID and a non-null origin: run ID,
selection-report SHA-256 and archive-manifest key/SHA-256, copied unchanged
through retained generations and consistent across a bundle. Publication
rechecks that the report's continuity base matches the current pointer.

**Degradation.** A missing pointer means nothing has been published. An
unreadable pointer fails closed. If the named generation fails validation or
lacks continuity metadata, discovery fails closed while the pointer is younger
than `continuity_degraded_after_seconds` (14400); after that the run proceeds
without a hold, the report records the generation's run ID
(`continuity_degraded_base_run_id`), and publication rechecks the pointer,
failure and timeout before accepting it.

## Scheduling and Compose

`compose.yaml` defines `targeter` as the one-shot `publish` command and points
every target-dependent splice at
`/var/lib/prediction-indexer/live/targeter-v2/current.json`.
`compose.targeter-v2.yaml` adds only the `ops` services: `targeter-v2-run-archiver`,
`targeter-v2-run-reaper` and `targeter-v2-integrity`. Cadence belongs to the host
scheduler:

```cron
*/10 * * * * cd /opt/prediction-indexer && docker compose run --rm targeter
```

A systemd `Type=oneshot` service on a timer is equivalent; use one scheduler,
not both. Before starting splices, run the targeter once, then the integrity
gate, then start the splices:

```bash
docker compose run --rm targeter
docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops run --rm targeter-v2-integrity
```

`targeter-v2-integrity` (`run_v2.py --mode audit`) is read-only. It verifies the
pointer for every venue, all generation file identities and target semantics,
the production receipt and every remote object, the local run against the
receipt, and exact equality between published targets and the archived
selection report. Run it after the first publication, after deployment
changes, and from monitoring at least once per scheduler interval. To pause
publication, stop all three schedulers (publish, run archiver, run reaper); the
splices keep the last committed generation. Never delete archives or
generations to roll back.

## Run retention

Each run is 12-20 MB uncompressed (roughly 2 GB per day at a ten-minute
cadence); Zstandard reduces this, but nothing deletes files merely because they
compressed well. Two separate commands bound local disk; uploading is never the
last step before deleting:

```bash
python -m targeter.v2.run_archiver_cli --output-root <runs> [--report <json>]
python -m targeter.v2.run_reaper_cli   --output-root <runs> --live-root <live> \
  [--mode audit|delete] [--retention-hours N] [--report <json>]
```

Both read the backend from the `ARCHIVE_*` environment. Compose services
`targeter-v2-run-archiver` and `targeter-v2-run-reaper` (profile `ops`) write
`/var/lib/prediction-indexer/ops/last_targeter_v2_{archive,reaper}_sweep.json`;
the reaper mode and floor come from `TARGETER_RUN_REAPER_MODE` (default
`audit`) and `TARGETER_RUN_RETENTION_HOURS` (default 18). Schedule both off the
publish boundary, for example minute 5 and minute 35 hourly.

### Archiver sweep

`mode publish` archives its own run inside the leased transaction, so healthy
runs already carry a receipt. The sweep covers the tail: runs whose upload
failed and every run of a shadow-only deployment. It reports per run:

| Status | Meaning |
|---|---|
| `archived` | this sweep wrote the receipt |
| `skipped` | the expected receipt already exists (not re-verified) |
| `pending` | structurally incomplete and younger than 18 h; probably running |
| `failed` | incomplete after 18 h, or archival raised |
| `conflict` | an immutable key holds different bytes; the sweep halts |

A pending count that never falls is what a stuck scheduler looks like.
Completeness is structural: `selection_report.meta.json` (written last) must
exist and every file the report names must be present. The sweep imports no
removal primitive (a test asserts it) and takes the same `.targeter-v2.lock`;
if the lease is held it reports `lease_acquired: false` and exits `0`, since a
run in progress is normal. Exit `1` on `failed`, `2` on `conflict`/halt.

### Reaper gate

A run directory has no canonicalization stage; its second proof (beside the
archive receipt) is that it is not the published generation. All eleven
conditions are re-established at decision time:

```text
1  a production archive receipt (a conformance receipt authorizes nothing)
2  a receipt that parses and names this directory
3  a directory holding nothing the receipt did not name
4  (or: no receipted artifact left at all, an earlier reaping)
5  a backend authorized as an independent durability domain
6  a readable publication pointer naming some other run
7  a run older than the retention floor by every clock available
8  archived objects that still match the receipt under head
9  (or: a partial cleanup to finish, from the top of this same path)
10 local artifacts still matching the receipt byte for byte
11 an operator who explicitly enabled deletion (--mode delete)
```

Absence of proof is retention. Each failed condition has a stable reason:

| Reason | Retained because |
|---|---|
| `run_archive_receipt_missing` | nothing archived this run |
| `durability_gate` | only a conformance receipt, or the store is not independent |
| `run_archive_receipt_invalid` | receipt does not parse or names another run |
| `unexpected_run_artifact` | a file the receipt never named |
| `publication_pointer_unreadable` | which generation is live cannot be determined |
| `published_generation` | this run is the live generation |
| `run_clock_unreadable` | the run ID names no instant |
| `retention_floor` | younger than the floor |
| `archive_object_unverified` | an archived object no longer matches |
| `local_run_changed` | a local artifact no longer matches |
| `io_error` | directory or receipt unreadable |
| `audit_mode` | everything passed; deletion not enabled (the reapable set) |

**Audit is the default.** Delete mode requires `--mode delete` and a backend
declared independent; delete against a conformance store exits at startup.
Enable it only after a real independent publication has passed the integrity
gate, the archiver reports zero `failed` and a non-climbing `pending`, several
audit sweeps report no unarchived runs or fault reasons, and the reapable count
matches the cadence and floor.

**Floor and clocks.** A run younger than 18 hours is retained regardless;
`--retention-hours` below 18 is refused, higher is allowed. Age is measured from
`max(instant in the run ID, receipt.archived_at_ns, receipt file mtime)`; the
mtime is the one clock a `--now` argument cannot set, so a backdated `--now`
cannot mint a deletable run. `--live-root` is required, not defaulted: absent is
ambiguous between "nothing published" and "the volume did not mount". The
pointer is read once at the start of a sweep without taking the lease; the floor
makes that safe, because a generation that just went live is seconds old.

**What deletion leaves.** The reaper deletes the artifacts the receipt names:
selection report and its meta file first, the rest in between, `run_manifest.json`
last, each fsynced. The receipt and the directory holding it always remain as
the audit tombstone (about 9 KB) and the marker of an earlier reaping. A
partially deleted directory (receipt plus a proper non-empty subset of
artifacts) is finished only after conditions 1-8 hold again. It never removes
an archive object, a receipt, the published generation or a run directory.

Exit codes: `1` for fault retentions (`run_archive_receipt_invalid`,
`archive_object_unverified`, `local_run_changed`, `unexpected_run_artifact`,
`run_clock_unreadable`, `publication_pointer_unreadable`, `io_error`) and for a
pointer fault; `0` for ordinary gates (`durability_gate`, `retention_floor`,
`published_generation`, `run_archive_receipt_missing`).

## Tests

```bash
.venv/bin/python -m unittest tests.test_targeter_v2_delivery \
  tests.test_targeter_v2_retention tests.test_targeter_replay_stream \
  tests.test_targets tests.test_deployment
docker compose -f compose.yaml -f compose.targeter-v2.yaml config --quiet
```

They cover immutable archive retries and conflicts, remote-manifest commit
order, incomplete and unrelated-empty publication refusal, terminal-empty
publication, atomic pointer failure, path containment, corruption rejection,
archive-to-publication equality, audit without discovery, overlap refusal, each
of the eleven reaper conditions, floor behavior under a backdated `--now`,
tombstone idempotence, that the archiver imports no removal primitive, and that
nothing under `archive/` imports `targeter`.
