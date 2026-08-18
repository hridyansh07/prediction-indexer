# Targeter v2 Delivery Phases 6–10

Status: implemented pre-deployment contract. This document is normative for
archival, publication, splice handoff, scheduling, and production verification.
Discovery, matching, rule classification, relationship derivation, and ranking
remain defined by `TARGETER_V2_PHASES_1_5.md`.

## 1. Scope and safety boundary

Phases 6–10 turn one phase-5 shadow run into a durable, auditable splice target
generation. They do not price trades, execute orders, claim unconditional
arbitrage, or provide a reviewer UI.

The critical invariants are:

1. Every attempted scheduled run leaves its discovery evidence locally. Archive
   and publish modes also archive that evidence, including incomplete runs.
2. Only a complete multi-venue selection with a verified independent archive may
   replace live targets, except for the terminal-retirement empty generation
   defined in §3.
3. A single pointer commits all venue target files. A crash cannot expose a new
   Kalshi file with an old Polymarket file.
4. The previous pointer remains authoritative on every pre-pointer failure.
5. The command is one-shot. Cron or a systemd timer owns cadence.
6. A filesystem lease prevents two scheduled discoveries from overlapping.

Exit status `0` means the requested operation completed. Status `1` means
discovery evidence was preserved but the input was incomplete, so publication
was deliberately skipped. Status `2` means configuration, durability, archive,
publication, or integrity validation failed.

## 2. Phase 6 — immutable target-run archive

Input is one timestamped phase-5 run directory containing by default:

- `selection_report.json.zst` plus its small `selection_report.meta.json` commit
  marker;
- `rule_templates.ndjson.zst` and `rule_drift.ndjson.zst` by default;
- the event and market catalogue `.ndjson.zst` files named by the report.

Plain `.ndjson` artifacts and `selection_report.json` remain valid only when the
report records the explicit `ndjson` artifact format. Existing version-1
uncompressed receipts remain
readable during rollout; all new manifests and receipts are version 2 and carry
both decoded and stored identities for normalized artifacts.

`archive_run` validates the report and rejects missing, unexpected, non-regular,
or changed artifacts. It writes a local `run_manifest.json`, then publishes each
artifact to:

```text
targeter-v2/runs/date=<UTC-date>/run=<run_id>/<artifact>
```

Writes are immutable. An identical object is an idempotent retry; different
bytes at the same key are an integrity conflict. Stored SHA-256 and byte length
are verified through the object-store adapter, compressed artifacts are also
strictly decoded against their logical identity before archival, and production
objects require the provider's explicit SHA-256 checksum. An S3 ETag is never
accepted as a content checksum.

`run_manifest.json` is uploaded last and is the remote commit marker. Only after
it verifies does the local run receive:

- `archive_receipt.json` for an independent/production archive; or
- `archive_receipt.local.json` for a local conformance archive.

A conformance receipt tests the protocol but never authorizes publication.
Interrupted prefixes without a remote manifest are incomplete and safe to retry.
Incomplete vendor discovery is still archived because the failure evidence is
operationally useful; it remains ineligible for phase 7.

## 3. Phase 7 — atomic target publication

Publication requires all of the following:

- `input_complete: true`, no discovery failures, and every catalogue summary
  complete;
- a non-empty selected bundle set, or exact schema-v3 continuity evidence
  authorizing retirement of every prior bundle;
- every selected bundle represented on at least the strategy's minimum venues;
- target entries for exactly Kalshi, Polymarket, and Limitless, including an
  empty file for a venue with no selected subscriptions;
- a production receipt reverified against an independent object store;
- local run bytes matching that receipt exactly.

The publisher also cross-checks every subscription ID, canonical class, and
source reference against the archived venue catalogue, and requires each
selected bundle's target IDs to equal its eligible candidate market IDs. An
internally well-formed but forged selection report is therefore not sufficient.
For a continuity-retained bundle absent from current discovery, the publisher
instead requires exact equality with the report's archived continuity evidence,
which was reconstructed from the previously committed generation. Retention may
carry no new target or subscription ID.

Output is one immutable local generation:

```text
<live-root>/targeter-v2/
  generations/<run_id>/
    targets_kalshi.json
    targets_polymarket.json
    targets_limitless.json
    metadata/<venue>/<metadata_sha256>.json
    manifest.json
  current.json
```

Each target carries its run ID, bundle ID, canonical class, activation and
capture times, source reference, selection-report SHA-256, and archive-manifest
identity. Version-3 continuity targets additionally carry the immediate prior
generation ID and a non-null origin run ID plus the origin report and archive
manifest identities. The origin evidence is copied unchanged through retained
generations and must be consistent across every target in a bundle. Target files
and metadata snapshots are fsynced before `manifest.json` is durably published.
The generation manifest records every file identity, target digest, metadata
digest, and target count. It is validated against the archived selection before
publication continues.

`current.json` is written last by atomic replace and directory fsync. It contains
the run ID plus the generation-manifest path, SHA-256, and byte length. This
single pointer is the live commit marker. A complete generation without a
pointer is abandoned-but-safe and an identical retry can publish it.

An incomplete run does not replace a prior pointer. An ordinarily empty run also
requires explicit human control because silently unsubscribing every lane is too
destructive. The sole automatic empty-publication path is a schema-v3 report
which carries the exact prior continuity bundles and proves each was retired by
all-terminal evidence, the configured terminal clamp, or explicit protected-floor
budget trimming. This narrow exception is what lets terminal retirement remove
the final live bundle while retaining an explicit continuity-evidence guard.

## 4. Phase 8 — splice handoff and reload behavior

Every subscription-driven splice receives the same path:

```text
/var/lib/prediction-indexer/live/targeter-v2/current.json
```

`load_targets(pointer, venue=<venue>)` performs these checks before returning a
new `TargetSet`:

1. pointer version, run ID, relative manifest path, and manifest identity;
2. manifest version, matching run ID, and a venue entry;
3. relative target path bounded to the generation and exact file identity;
4. target schema, venue, unique subscription IDs, target digest, metadata
   digest, count, and metadata snapshot;
5. metadata path bounded to the same committed generation.

Legacy direct target documents remain readable outside this opt-in deployment.
The splice's existing reload loop replaces its in-memory subscription set only
after `load_targets` succeeds. A missing, partial, corrupted, or traversing
pointer therefore leaves the last valid subscription set active and is retried
on the normal target poll interval.

## 5. Phase 9 — one-shot scheduling and Compose deployment

`targeter/run_v2.py` supports four one-shot modes:

- `shadow`: discover and preserve local phase-5 evidence only;
- `archive`: discover, preserve, and immutably archive;
- `publish`: discover, archive, then atomically publish if eligible;
- `audit`: perform no discovery and verify the current publication.

There is deliberately no `--interval-seconds`, in this command or in any other
Targeter v2 command, including the run archiver and run reaper of §7. Every one
of them is a single transaction that a host scheduler repeats; none owns an
internal sleep loop. `targeter/run_v2.py` acquires `<output-root>/.targeter-v2.lock`
before discovery and refuses an overlap.

The production opt-in is `compose.targeter-v2.yaml`. It changes `targeter` to a
one-shot `publish` command, passes archive credentials only to targeter/audit,
and changes all four target-dependent splice/waiter pairs to the common pointer.
It does not alter `compose.yaml` when the override is absent.

An example ten-minute cron entry is:

```cron
*/10 * * * * cd /opt/prediction-indexer && docker compose -f compose.yaml -f compose.targeter-v2.yaml run --rm targeter >> /var/log/prediction-targeter-v2.log 2>&1
```

The same command can be placed in a systemd `Type=oneshot` service triggered by
a `OnUnitActiveSec=10min` timer. Do not use both schedulers.

Before starting splices, create and audit the first generation:

```bash
docker compose -f compose.yaml -f compose.targeter-v2.yaml run --rm targeter
docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops \
  run --rm targeter-v2-integrity
docker compose -f compose.yaml -f compose.targeter-v2.yaml up --no-deps -d \
  splice-polymarket splice-polymarket-snapshots splice-limitless
```

Add `--profile kalshi splice-kalshi` only on a host with working Kalshi
credentials. Production publication defaults to the S3 backend and refuses
missing bucket, region, or expected-owner configuration. Instance/task roles
are preferred; exported AWS credentials are forwarded only to S3-facing
services.

## 6. Phase 10 — audit and rollout gate

`targeter-v2-integrity` is read-only. It verifies:

- the current pointer for every supported venue;
- all generation file identities and target semantics;
- the production receipt and every remote object through the configured store;
- the local run against that receipt;
- exact equality between published targets and the archived selection report.

Run the gate after the first publication, after deployment changes, and from
monitoring at least once per scheduler interval:

```bash
docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops \
  run --rm targeter-v2-integrity
```

Rollout is complete only when:

1. Compose merge validation and container build pass on the Linux deployment
   host;
2. a real S3-backed `publish` run completes with a production receipt;
3. the integrity service passes against that run;
4. all enabled target-dependent splices report the same target run ID and
   record target-change control evidence;
5. the scheduler completes several non-overlapping runs without discovery,
   archive, publication, or pointer validation failures.

Rollback stops all three schedulers — the publish entry and both retention
entries of §7 — and starts the base Compose file without the v2 override. Do
not delete target-run archives or generations during rollback; they are
immutable audit evidence. That prohibition governs the immutable archive and
the published generations, which the run reaper never touches: the reaper only
reclaims a local run directory's artifacts once the archive holds them, and it
never removes an archive object, a receipt, or a generation. Leaving the reaper
installed during a rollback is therefore safe, but because a rollback is a
state nobody has finished diagnosing, stop its cron entry with the others and
restart it after the cause is understood.

## 7. Run retention and disk bounds

Each scheduled run leaves a complete run directory under `--output-root`: three
venue catalogues, the selection report, the rule templates, the drift record and
the run manifest. Earlier uncompressed runs measured 12–20 MB each, roughly 2 GB
per day at the ten-minute cadence. New normalized artifacts use Zstandard by
default, substantially reducing that input-dependent rate, but no component
removes the files merely because they compressed well. A deployment without
this section remains unbounded and eventually fills its disk.

Two commands, deliberately separate, mirroring `archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`
§7.1: uploading must never be the last step before deleting.

```bash
python -m targeter.v2.run_archiver_cli --output-root … --archive-root …
python -m targeter.v2.run_reaper_cli   --output-root … --live-root … --archive-root …
```

### 7.1 The archiver sweep

`targeter/run_v2.py --mode publish` archives the run it just produced inside the
same transaction, so in a healthy deployment every run already carries a
receipt. The sweep exists for the tail: a run whose upload failed against a
transient S3 error, and every run of a `--mode shadow` deployment. Those hold no
receipt, so the reaper can never reclaim them, and local disk is not actually
bounded until they are archived.

Completeness is decided structurally, not by interpreting an error. `run_shadow`
writes the catalogues, rules, and compressed selection report through atomic
renames, then writes `selection_report.meta.json` last with the report frame's
exact identity. Per run the sweep reports:

| Status | Meaning |
|---|---|
| `archived` | This sweep published a receipt |
| `skipped` | The expected receipt already exists; not re-verified here |
| `pending` | Structurally incomplete and younger than 18 hours — probably still running |
| `failed` | Still incomplete after 18 hours, or archival raised |
| `conflict` | An immutable key holds different bytes; the sweep halts |

`pending` is not noise: a pending count that never falls is what a stuck
scheduler looks like from here. A `conflict` halts the whole sweep because a
namespace already known to disagree with this host must not receive more
objects.

**The sweep cannot delete anything and has no flag that could.** It imports no
removal primitive, and a test asserts that.

It takes the same `<output-root>/.targeter-v2.lock` a scheduled publish takes,
because `targeter/run_v2.py` holds that lease across both discovery and
archival. Without it both processes could archive one directory and write two
receipts recording different archive instants, leaving the run's receipt no
longer the one that authorized its publication. A sweep that cannot take the
lease reports `lease_acquired: false` and **exits zero** — the lease being held
means a scheduled run is in progress, which is the expected state several times
an hour, not a fault.

### 7.2 The reaper gate

The raw-capture reaper needs two receipts because a sealed segment passes
through canonical ingestion. A run directory has no canonicalization stage; its
second proof is that it is not the generation the splices are subscribed to.
All eleven conditions are re-established at decision time, in this order:

```text
1  a production archive receipt          (a conformance receipt authorizes nothing)
2  a receipt that parses and names this directory
3  a directory holding nothing the receipt did not name
4  (or: no receipted artifact left at all, which is an earlier reaping)
5  a backend authorized as an independent durability domain
6  a readable publication pointer that names some other run
7  a run older than the retention floor by every clock available
8  archived objects that still match the receipt under head
9  (or: a partial cleanup to finish, from the top of this same path)
10 local artifacts still matching the receipt byte for byte
11 an operator who explicitly enabled deletion
```

**Absence of proof is retention, never permission.** Each failed condition
produces a stable reason string, which is what an operator alerts on:

| Reason | Retained because |
|---|---|
| `run_archive_receipt_missing` | Nothing has archived this run |
| `durability_gate` | Only a conformance receipt exists, or the store is not independent |
| `run_archive_receipt_invalid` | The receipt does not parse or names another run |
| `unexpected_run_artifact` | The directory holds a file the receipt never named |
| `publication_pointer_unreadable` | Which generation is live cannot be determined |
| `published_generation` | This run is the live generation |
| `run_clock_unreadable` | The run id names no instant |
| `retention_floor` | Younger than the floor |
| `archive_object_unverified` | An archived object no longer matches the receipt |
| `local_run_changed` | A local artifact no longer matches the receipt |
| `io_error` | The directory or receipt could not be read |
| `audit_mode` | Everything else passed; deletion was not enabled |

`audit_mode` is the reapable set — what deletion would remove if enabled.
`run_archive_receipt_missing` is the disk-bounding metric: a count that climbs
means §7.1 is not running.

Verification happens at step 8, immediately before the irreversible act rather
than early, which is both strictly stronger and bounds network cost to the
reapable runs instead of heading every object of every retained run each sweep.

### 7.3 The retention floor and the three clocks

A run younger than **18 hours** is retained whatever else is proven about it.
The command refuses to be configured below that; it may be raised. Age is
measured back from the latest of three clocks:

```text
max(the instant in the run id, receipt.archived_at_ns, the receipt file's mtime)
```

The first two both derive from `--now`, which §5's command documents as a probe
and test flag; `--now 2020-01-01` would otherwise mint an instantly deletable
run. The receipt's mtime is the one clock argv cannot set. `max` is the safe
combinator — a later basis retains longer, so adding a clock can only extend
retention. A run id that will not parse is `run_clock_unreadable`, not zero.

The floor is also what makes reading the publication pointer without a lease
safe. A sweep over a hundred runs can take minutes, and holding
`.targeter-v2.lock` that long would fail the ten-minute publish cron; a cleanup
task must never fail a publish. The pointer is written through an atomic
rename, so the read is never torn, and it is read **once** at the top of the
sweep. If it names generation A while B has just gone live, A is the previous
generation, which nothing reads, and B is seconds old and therefore always on
the floor.

An unreadable or missing pointer retains everything and exits non-zero.
`--live-root` is required rather than defaulted, because absent is ambiguous
between "nothing published yet" and "the live volume did not mount", and
guessing the first while the volume is unmounted would delete the local copy of
the live generation.

### 7.4 What deletion leaves behind

The reaper deletes the artifacts the receipt names — `selection_report.json`
first, the rest in between, `run_manifest.json` last, each fsynced. **The
receipt and the directory holding it always remain.** After the artifacts are
gone the receipt is the only thing that makes the deletion auditable, and its
presence is what lets the next sweep recognize the run as already reaped without
asking the object store anything.

A tombstone costs about 9 KB — roughly 1.3 MB/day, 470 MB/year, against 2 GB/day
today. Tombstones are audit evidence and this command deliberately will not
clean them up.

A partially deleted directory — the receipt plus a proper non-empty subset of
the receipted artifacts — is recognized with no extra marker and finished only
after conditions 1–8 are re-established from the top. Re-archiving a tombstone
fails closed rather than half-re-archiving.

The reaper never removes an archive object, a receipt, the published
generation, or a run directory itself.

### 7.5 Scheduling and rollout

Both entries sit off the `*/10` publish boundary and 30 minutes apart:

```cron
5  * * * * cd /opt/prediction-indexer && docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops run --rm targeter-v2-run-archiver >> /var/log/prediction-targeter-v2-archive.log 2>&1
35 * * * * cd /opt/prediction-indexer && docker compose -f compose.yaml -f compose.targeter-v2.yaml --profile ops run --rm targeter-v2-run-reaper   >> /var/log/prediction-targeter-v2-reaper.log  2>&1
```

Each writes its record to
`/var/lib/prediction-indexer/ops/last_targeter_v2_{archive,reaper}_sweep.json`.

**Audit is the default and installing this does not make deletion active.**
Enabling it requires `TARGETER_RUN_REAPER_MODE=delete` *and* a backend declared
as an independent durability domain; delete mode against a conformance store is
refused at startup, because until the archive is a separate durability domain
the local run directory is the recovery authority. Enable deletion only after:

1. the rollout gate of §6 has passed against a real S3-backed publication;
2. the archiver sweep reports zero `failed` and a non-climbing `pending`;
3. several audit sweeps report `counts.unarchived: 0` and no fault reason;
4. `counts.reapable` is non-zero and matches the run count expected from the
   cadence and the floor.

The reaper exits 1 on the fault reasons above and on a pointer fault. It exits 0
on `durability_gate`, `retention_floor`, `published_generation` and
`run_archive_receipt_missing`: conformance is the default deployment state, and
exiting non-zero hourly on the ordinary shut gate would train an operator to
ignore the command.

## 8. Verification commands

Repository gates:

```bash
python3 -m unittest tests.test_targeter_v2 tests.test_masks \
  tests.test_targeter_v2_delivery tests.test_targeter_v2_retention \
  tests.test_deployment
docker compose -f compose.yaml -f compose.targeter-v2.yaml config --quiet
docker compose -f compose.yaml -f compose.targeter-v2.yaml build targeter
```

Tests must prove immutable archive retries/conflicts, remote-manifest commit
ordering, incomplete and unrelated-empty publication refusal, terminal-empty
publication, atomic pointer failure,
cross-venue pointer consumption, path containment, corruption rejection,
archive-to-publication equality, audit without discovery, and overlap refusal.

For §7 they must additionally prove that every one of the eleven conditions
retains when it fails, that the floor holds against a backdated `--now`, that
audit deletes nothing, that a reaped directory keeps its receipt and is
idempotent under a second sweep, that a partial deletion is not finished once
the archive stops verifying, that the archiver never reads a reaped directory,
and — as absences are facts only when something checks them — that the archiver
imports no removal primitive and that nothing under `archive/` imports
`targeter`.
