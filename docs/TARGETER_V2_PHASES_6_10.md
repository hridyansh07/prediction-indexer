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
2. Only a complete, non-empty, multi-venue selection with a verified independent
   archive may replace live targets.
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

Input is one timestamped phase-5 run directory containing:

- `selection_report.json`;
- `rule_templates.ndjson` and `rule_drift.ndjson`;
- the event and market catalogue NDJSON files named by the report.

`archive_run` validates the report and rejects missing, unexpected, non-regular,
or changed artifacts. It writes a local `run_manifest.json`, then publishes each
artifact to:

```text
targeter-v2/runs/date=<UTC-date>/run=<run_id>/<artifact>
```

Writes are immutable. An identical object is an idempotent retry; different
bytes at the same key are an integrity conflict. SHA-256 and byte length are
verified through the object-store adapter, and production objects require the
provider's explicit SHA-256 checksum. An S3 ETag is never accepted as a content
checksum.

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
- a non-empty selected bundle set;
- every selected bundle represented on at least the strategy's minimum venues;
- target entries for exactly Kalshi, Polymarket, and Limitless, including an
  empty file for a venue with no selected subscriptions;
- a production receipt reverified against an independent object store;
- local run bytes matching that receipt exactly.

The publisher also cross-checks every subscription ID, canonical class, and
source reference against the archived venue catalogue, and requires each
selected bundle's target IDs to equal its eligible candidate market IDs. An
internally well-formed but forged selection report is therefore not sufficient.

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
identity. Target files and metadata snapshots are fsynced before `manifest.json`
is durably published. The generation manifest records every file identity,
target digest, metadata digest, and target count. It is validated against the
archived selection before publication continues.

`current.json` is written last by atomic replace and directory fsync. It contains
the run ID plus the generation-manifest path, SHA-256, and byte length. This
single pointer is the live commit marker. A complete generation without a
pointer is abandoned-but-safe and an identical retry can publish it.

An empty or incomplete run does not replace a prior pointer. Empty publication
requires a future explicit human control because silently unsubscribing every
lane is too destructive for the automatic path.

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

There is deliberately no `--interval-seconds`. The process acquires
`<output-root>/.targeter-v2.lock` before discovery and refuses an overlap.

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

Rollback stops the scheduler and starts the base Compose file without the v2
override. Do not delete target-run archives or generations during rollback;
they are immutable audit evidence.

## 7. Verification commands

Repository gates:

```bash
python3 -m unittest tests.test_targeter_v2 tests.test_masks \
  tests.test_targeter_v2_delivery tests.test_deployment
docker compose -f compose.yaml -f compose.targeter-v2.yaml config --quiet
docker compose -f compose.yaml -f compose.targeter-v2.yaml build targeter
```

Tests must prove immutable archive retries/conflicts, remote-manifest commit
ordering, incomplete and empty publication refusal, atomic pointer failure,
cross-venue pointer consumption, path containment, corruption rejection,
archive-to-publication equality, audit without discovery, and overlap refusal.
