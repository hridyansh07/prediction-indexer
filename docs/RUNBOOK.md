# Operator runbook

Commands for the deployed host, and how to read what they produce without
needing to ask anyone what a field means.

`docs/DEPLOYMENT.md` is the reference for *why* the deployment is shaped this
way. This file is for the day-to-day: run a thing, read its report, decide.

---

## 0. Two things that will bite you

**Always pass both compose files.** The v2 override is what points the splices at
`live/targeter-v2/current.json`. Omit it and Compose silently gives you the v1
service definitions instead — no error, just the wrong targets path.

```bash
alias dc='docker compose -f compose.yaml -f compose.targeter-v2.yaml'
```

**Profiles are not optional decoration.** A service behind a profile is invisible
without it — `dc ps` will not show it, and `dc up -d` will not start it.

| Service | Profile | Image |
|---|---|---|
| `splice-polymarket`, `splice-limitless`, `splice-polymarket-snapshots` | *(none)* | capture |
| `targeter` | *(none)* | capture |
| `ingester` | *(none)* | ingester |
| `splice-kalshi` | `kalshi` | capture |
| `finalizer`, `finalizer-once` | `ops` | ingester |
| `ingester-integrity` | `ops` | ingester |
| `ingest-store-reaper` | `ops` | ingester |
| `archiver`, `archiver-once` | `ops` | capture |
| `reaper`, `reaper-once` | `ops` | capture |
| `canonical-integrity` | `ops` | capture |
| `targeter-v2-run-archiver`, `targeter-v2-run-reaper`, `targeter-v2-integrity` | `ops` | capture |

`targeter` is never `up -d`. It is a one-shot transaction started by cron, so it
picks up a new image or command on its next firing with no action from you.

---

## 1. Deploying a new image

```bash
# .env
IMAGE_REGISTRY=hridyansh07/
IMAGE_TAG=v0.2.0
```

```bash
dc --profile ops --profile kalshi pull

# Splices one at a time: the capture gaps then do not land in the same window,
# which keeps cross-venue comparison usable across the restart.
dc up -d splice-polymarket
dc up -d splice-limitless
dc up -d splice-polymarket-snapshots
dc --profile kalshi up -d splice-kalshi

dc up -d ingester
dc --profile ops up -d finalizer archiver
```

A restart does **not** break `delivery_index`. `resume_state`
(`splices/common/spool.py:253`) rebuilds the next index from the seals, because
the index is dense across a splice's lifetime rather than per connection. A new
`connection_epoch` is minted and `local_counter` restarts inside it, which is
what gate 1 expects to see.

`stop_grace_period` is 30s so the splice seals its open segment on the way down.
That matters: a cleanly sealed segment means the next start resumes from seals
(fast). A killed splice leaves an unsealed segment and forces a full byte scan.

---

## 2. Where everything writes

| What | Path |
|---|---|
| Sealed segments | `data/spool/lane=<lane>/date=<date>/*.ndjson` + `.seal.json` |
| Compressed derivative | same directory, `.ndjson.zst` |
| Canonical windows | `data/canonical/date=<date>/window=<ns>/` |
| Live target pointer | `data/live/targeter-v2/current.json` |
| Published generations | `data/live/targeter-v2/generations/<run_id>/` |
| Coverage ledger | `data/live/coverage.json` |
| Targeter runs | `data/targeter-v2-runs/<run_id>/` |
| Finalizer report | `data/ops/last_finalizer_sweep.json` |
| Run archiver report | `data/ops/last_targeter_v2_archive_sweep.json` |
| Run reaper report | `data/ops/last_targeter_v2_reaper_sweep.json` |
| Archiver report | `data/archive-manifests/last_archive_sweep.json` |
| Raw reaper report | `data/archive-manifests/last_reaper_sweep.json` |
| Ingest-store reaper report | `data/ops/last_ingest_store_reaper_sweep.json` |

The raw archive pair write to `archive-manifests/`, not `ops/` — they predate
that directory. Both the long-lived service and its `-once` variant write the
same filename, so a manual `reaper-once` overwrites the scheduled sweep's report.

Everything below assumes `DATA=/srv/prediction-indexer/data`.

---

## 3. Reading the ingest-store reaper report

```bash
sudo python3 -m json.tool $DATA/ops/last_ingest_store_reaper_sweep.json
```

This is a third reaper with a deliberately narrow authority: it deletes only a
derived, closed daily SQLite database. It never touches raw spool files, active
`store.db.open`, partition receipts, or directories.

Read `counts.reapable` first. In audit mode it is the number of closed databases
that are at least 24 hours past `closed_at_ns` and still byte-identical to their
receipts. `counts.reaped` remains zero until
`INGEST_STORE_REAPER_MODE=delete` is explicitly set.

| Reason | Means |
|---|---|
| `audit_mode` | Eligible, retained only because deletion is disabled |
| `retention_floor` | Closed less than `INGEST_STORE_RETENTION_HOURS` ago |
| `active_partition` | Contains the writer's active marker or `.open` database |
| `receipt_missing` | A closed-looking database has no commit receipt; retained |
| `database_identity_mismatch` | Closed bytes no longer match receipt; investigate |
| `database_already_reaped` | Database is gone and its receipt skip ledger remains |

Run one audit by hand with:

```bash
dc --profile ops run --rm ingest-store-reaper
```

The scheduled command is the same one-shot invocation. Do not use `up -d` for
it. The minimum retention is 24 hours, and the command refuses a lower value.

---

## 4. Reading the raw reaper report

```bash
sudo python3 -m json.tool $DATA/archive-manifests/last_reaper_sweep.json | head -40
```

Read `counts` first, then `retained_by_reason`. `decisions` is one entry per
segment and is long; you rarely need it except to chase a specific reason.

```json
"counts": { "considered": 339, "retained": 339, "reaped": 0, "reapable": 335 }
```

- `reapable` — would be deleted if `REAPER_MODE=delete`. In audit this is the
  number that matters: it is what you are authorising when you flip the mode.
- `reaped` — actually deleted. Always 0 in audit.
- `considered` should equal `retained + reaped`.

**Reason strings**, from `archive/reaper/service.py`:

| Reason | Means | Action |
|---|---|---|
| `audit_mode` | Every condition holds; only the mode stops deletion | None. This is the healthy state. |
| `canonical_receipt_missing` | Archived, but the finalizer has not committed its window yet | None if it is a recent window. See below. |
| `durability_gate` | Archive is not an independent durability domain | Check `ARCHIVE_BACKEND=s3`. Nothing can ever be deleted while this shows. |
| `archive_receipt_invalid` | Receipt unreadable or self-inconsistent | Investigate. Never expected. |
| `archive_object_unverified` | The S3 object does not match its receipt | Investigate immediately — the archive is not what it claims. |
| `canonical_segment_mismatch` | Canonical window disagrees with the segment | Investigate. |
| `local_source_changed` | The local file changed after it was archived | Investigate. A sealed segment is immutable. |
| `io_error` | Could not read something | Usually permissions or a full disk. |

A handful of `canonical_receipt_missing` is normal and self-resolving: it is the
window currently in flight. Healthy looks like **one per running lane, all naming
the same window**:

```
polymarket           20260807T120000000000-...  -> canonical_receipt_missing
kalshi               20260807T120000000000-...  -> canonical_receipt_missing
limitless            20260807T120000000000-...  -> canonical_receipt_missing
polymarket_snapshots 20260807T120000000000-...  -> canonical_receipt_missing
```

If the count grows beyond one window's worth, or entries persist across sweeps,
the finalizer is stuck — check it before anything else.

Useful one-liner for the shape of a report:

```bash
sudo python3 -c "
import json, collections
r = json.load(open('$DATA/archive-manifests/last_reaper_sweep.json'))
print('counts:', r['counts'])
print('reasons:', r['retained_by_reason'])
print('per lane:', dict(collections.Counter(d['lane'] for d in r['decisions'])))
for d in r['decisions']:
    if d['reason'] != 'audit_mode':
        print(' ', d['lane'], d['source_file'], '->', d['reason'])
"
```

### Empty segments are normal

A zero-record segment is legitimate evidence: it proves the lane was alive and
the venue was silent. You can spot them by the SHA-256 of the empty string:

```
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

Limitless produces a lot of these — it is a low-volume venue and a quiet
30-minute window is expected. **Gate 1 reports an empty segment as
`"<segment>: sealed but the segment is absent"`**, which is a wording bug, not
data loss: `segments_seen` is only populated per parsed record
(`replay/gate1.py:190`), so a segment with no records is never marked seen. Check
before worrying:

```bash
sudo python3 -c "
import json, os, glob
for seal in glob.glob('$DATA/spool/lane=*/date=*/*.seal.json'):
    d = json.load(open(seal))
    data = os.path.join(os.path.dirname(seal), d['data_file'])
    if not os.path.exists(data): print('MISSING', data)
    elif d['line_count'] == 0: print('EMPTY  ', data)
"
```

`EMPTY` is fine. `MISSING` is not, and means something deleted a segment.

---

## 5. Reading the targeter run reaper report

```bash
sudo python3 -m json.tool $DATA/ops/last_targeter_v2_reaper_sweep.json | head -40
```

Different command, different gate, **different retention semantics** — see §7.

| Reason | Means |
|---|---|
| `audit_mode` | Every condition holds; only the mode stops deletion |
| `retention_floor` | Younger than `TARGETER_RUN_RETENTION_HOURS` (18h floor) |
| `published_generation` | This run is the one `current.json` points at |
| `run_archive_receipt_missing` | Never archived — surfaces as `counts.unarchived` |
| `durability_gate` | Archive is not independent |
| `run_archive_receipt_invalid` | Receipt unreadable or names another run |
| `unexpected_run_artifact` | A file in the run directory the receipt does not name |
| `publication_pointer_unreadable` | `current.json` missing or malformed — **fails closed, retains everything** |
| `run_clock_unreadable` | Could not establish the run's age |
| `archive_object_unverified` | A remote object does not verify |
| `local_run_changed` | Local artifacts no longer match the receipt |
| `run_artifacts_absent` | Already reaped; the receipt tombstone remains |

The two numbers to watch:

- **`counts.unarchived`** — runs with no receipt. This is the disk-bound metric.
  If it climbs, the run archiver sweep is not keeping up and nothing will ever
  reclaim those directories.
- **`counts.reapable`** — what a delete-mode run would remove.

Deletion leaves `archive_receipt.json` and the directory as a tombstone. That is
deliberate: the receipt is what makes the deletion auditable. Archive objects and
published generations are never touched.

---

## 6. Reading the finalizer report

```bash
sudo python3 -m json.tool $DATA/ops/last_finalizer_sweep.json | head -30
```

- `expected_lanes` must list **exactly** the splices you run. Four right now:
  `polymarket`, `polymarket_snapshots`, `limitless`, `kalshi`.
- `unexpected_lanes` should be empty. A lane here is being merged into canonical
  but never counted toward completeness — its outage would be invisible and every
  window would still read complete. Add it to `--expect-lane` in `compose.yaml`.
- A lane listed in `expected_lanes` that is *not* running is the opposite error:
  every window sits out its deadline and commits incomplete forever.

---

## 7. Reading gate 1

```bash
dc run --rm --no-deps targeter \
  python -u -m replay.gate1 /var/lib/prediction-indexer \
    --output /var/lib/prediction-indexer/ops/gate1.json
```

Failures only:

```bash
sudo python3 -c "
import json
r = json.load(open('$DATA/ops/gate1.json'))
for c in r['checks']:
    if c['status'] != 'PASS':
        print(c['status'], c['name'])
        print('   ', c['requirement'])
        print('   ', json.dumps(c['evidence'])[:600]); print()
"
```

Gate 1 is deliberately strict and a failure is a capture-side gap, not an
invitation to filter until it passes. Two checks matter more than the rest:

- **`byte_and_envelope_integrity`** and **`deterministic_capture_order`** — these
  are the ones that cannot be repaired after the fact. If they pass, the tape is
  sound and everything else is metadata or configuration.

Known-failing and why:

| Check | Cause |
|---|---|
| `reference_price_observability`, `game_event_observability` | The `reference` profile is off — no splice produces those records |
| `closed_capture_fixture` | Capture is running; the unclosed connections are the live ones, one per lane |
| `market_rules_and_metadata`, `fee_model_evidence` | v2's `resolution` carries an archive pointer where v1 embedded the catalogue record, so `rules_records` and `fee_records` stay 0 |

Gate 1 reads **only** `spool/**/*.ndjson`, `*.seal.json`, the metadata snapshots
and `coverage.json`. It cannot read `.ndjson.zst` — `iter_ndjson_lines` skips any
key not ending `.ndjson` with a silent `continue` (`replay/stream.py:171`). This
is why the ordering in §7 matters.

**If it aborts with `object changed after stream snapshot: .../coverage.json`:**
the targeter rewrote the ledger mid-run. `DirectoryByteStreamer` snapshots inode
and mtime and re-checks around every read, and `save()` goes through an atomic
replace, so the inode changes every 10 minutes. Pause the targeter cron for the
duration of a gate run.

---

## 8. Enabling deletion — the ordering that matters

The two evidence/run reapers behave very differently and this is the single most
important thing on this page. The ingest-store reaper is independent of both:
its databases are derived, its 24-hour floor is mandatory, and deleting one does
not delete raw or canonical evidence.

| | Raw reaper | Targeter run reaper |
|---|---|---|
| Env | `REAPER_MODE` | `TARGETER_RUN_REAPER_MODE` |
| Deletes | `.ndjson`, `.seal.json`, `.ndjson.zst` in the spool | Run directory artifacts |
| Retention floor | **None** | 18 hours, refuses lower |
| Gate | Archive receipt + canonical receipt + independent durability | Eleven conditions incl. floor and published-generation |

**The raw reaper has no age floor.** Its gate is purely proof-based, so
`REAPER_MODE=delete` removes *everything* archived and ingested on the very next
hourly sweep, however recent. It is not a rolling window.

Because gate 1 reads exactly those files and cannot read the `.ndjson.zst` copies
in S3, run the analysis you want **before** flipping it:

1. Backfill coverage (reads run directories the run reaper reclaims)
2. Run gate 1 to completion (reads the spool the raw reaper deletes)
3. Then `TARGETER_RUN_REAPER_MODE=delete` — safe independently, 18h floor
4. Then `REAPER_MODE=delete` — irreversible for local analysis

Before either flip, confirm from the audit reports that no fault reason appears
and `counts.unarchived` is zero.

---

## 9. Capacity

```bash
sudo du -sh $DATA/* | sort -h
df -h /srv
```

Rough daily rates at the current configuration:

| Tier | Per day |
|---|---|
| ingest-store | ~27 G |
| spool | ~13.5 G |
| targeter-v2-runs | ~4.6 G |
| canonical | ~1.8 G |

The schema-v3 ingester upgrade performs one blocking identity-index migration and
moves the legacy database into the current UTC ingestion-day partition.
At six days the ingest store is roughly 162 GiB at this rate. The measured
2.67-million-fact migration permanently added 5.1% and temporarily needed 10.2%
above the original database while its WAL existed, so have at least 11% free
(about 18 GiB for 162 GiB), plus operating margin. These percentages are capacity
guidance, not a downtime estimate: copy and time the actual production store
before the upgrade because migration time follows fact and unique-identity counts.
The completed time and count appear in the ingester log and in
`store_migration` in its JSON report.

For a large legacy store whose old derived history is not needed, the documented
fresh-store cutover in `docs/DEPLOYMENT.md` avoids this migration. It starts a new
`file_order` lineage and re-ingests every sealed segment still in the local spool;
it does not reconstruct segments the raw reaper already removed. Move any backup
to another filesystem if the purpose is to recover capacity.

Compression on the archive path measures ~12.7x, so S3 growth is far below the
local spool rate. `targeter-v2-cache` should stay near zero now that
`--no-response-cache` is set; if it grows, the flag is not reaching the container
— check that both `-f` files are being passed.

With `INGEST_STORE_REAPER_MODE=delete`, ingest-store usage is bounded to the
active partition plus closed partitions still inside the configured 24-hour
floor. Receipts remain but are small. Nothing reaps
`live/targeter-v2/generations/`; it gains one directory per publish and no command
removes it.

---

## 10. Quick health sweep

```bash
dc --profile ops --profile kalshi ps
sudo tail -3 $DATA/ops/last_finalizer_sweep.json
sudo ls -la $DATA/spool/lane=kalshi/date=$(date -u +%Y-%m-%d)/ | tail -3
sudo python3 -c "
import json
print(json.load(open('$DATA/live/targeter-v2/current.json'))['run_id'])
"
```

Healthy: every service `Up`, the current `.ndjson.open` growing in each lane, and
the pointer's `run_id` within the last 10 minutes.

Splices print nothing to stdout during normal operation by design — a silent
container is a working container, not a stuck one. Judge them by whether the
spool is growing.
