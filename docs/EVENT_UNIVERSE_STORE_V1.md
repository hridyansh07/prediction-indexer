# Event/Market Universe Store V3

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
decoding, or JSON-size enforcement. Selection reports and the combined selected
normalized catalogue artifacts are each capped at 128 MiB decoded per run.

## 3. Canonical identity

Umbrella identity is allocated by Universe, not by Targeter and not by SQLite
row order. Targeter remains the source of matched cross-venue candidate
evidence. Its per-bundle projection ID is only a transaction-local proposal.
During ingestion, Universe resolves every native event reference
`(venue, venue_event_id)` through the durable `venue_events` alias edges:

1. if all known aliases name one umbrella event, reuse it and attach any new
   native aliases;
2. if no alias is known, allocate a new domain identity; and
3. if aliases name more than one umbrella event, fail the transaction closed.

The version-1 identity preimage is canonical JSON containing:

- `identity_version = 1`;
- sport, optional game, and optional topology;
- sorted participant keys;
- the UTC calendar date of activation observed when the identity is allocated;
  and
- an immutable zero-based ordinal for otherwise equal events on that date.

The public ID is `event:d1:<sha256>`, where the full lowercase SHA-256 digest is
over that preimage. Native references, exact activation time, bundle ID, event
title, participant display names, and venue membership are not in the preimage.
The first allocation uses ordinal 0; a disjoint same-day occurrence with equal
domain coordinates receives the next available ordinal in the same write
transaction. Ordinals are never renumbered.

`identity_activation_date` is frozen once allocated. Each run separately
records its exact observed activation in `event_observations`, and the umbrella
row exposes the chronologically first observation as its display/sort time.
Postponement across midnight therefore does not churn an identity when at least
one native alias survives. The native alias is authoritative for continuity;
if every venue replaces every native ID simultaneously, current evidence cannot
prove continuity and Universe allocates a separate event rather than guessing.

Venue-native event and market identities are assumed to be globally unique and
never reused by their venue. Reusing a known alias with different sport, game,
topology, or participant keys fails closed. Participant order and display-name
changes do not matter when the normalized participant-key set is unchanged.

A canonical market ID is a deterministic digest of:

- umbrella event ID;
- canonical class and market type;
- scope; and
- normalized semantic parameters.

`market_template_version` and `outcome_space_version` are explicit key columns.
They are not hidden in the canonical market ID.

First- and last-seen run IDs make continuity explicit. Re-ingesting the same
verified run is idempotent. API `event_refs` are derived in stable order from
the alias edges rather than stored as immutable umbrella content.

## 4. Claims and relationships

A market's semantic content, for relationship purposes, is the subset of the
outcome space it resolves YES on. Two markets naming the same subset are the
same **claim** however their venues word it, and the relationship between two
markets is a function of their two subsets and nothing else — the mask
comparison reads no other field.

Outcome keys are participant-independent, so a claim is identified globally by
its subset: one `claim_classes` row serves every event and every run that
expresses it. `claim_relations` relates two claims of one space shape and names
no event, run, venue, or bundle. `market_claims` records which claim a venue
market's tradable token expresses.

Nothing is keyed by run. A run that observes what earlier runs observed writes
no rows and only moves last-seen markers. This supersedes the former
`relations` / `relation_members` / `relation_observations` tables, which wrote
one row per relation per run — 21,101 rows per run against ~247 genuinely new
relations, and 3.3 M rows for 2,239 distinct claims.

Three consequences are structural rather than incidental:

- **IDENTITY is not a relation.** Equal subsets are one `claim_id`, so
  cross-venue equivalence is two `market_claims` rows sharing a claim.
- **REVERSE_IMPLICATION is normalized** to IMPLICATION with the antecedent
  named first, since it is the same containment written from the other end.
- **OVERLAP is not stored.** It is the catch-all branch of the mask comparison
  rather than a finding, and selection already discards it.

`coverage` on a claim records whether its space enumerates every reachable
outcome. A finding over an `INCOMPLETE_COVERAGE` space stays conditional
discovery evidence, never an unconditional claim.

`first_seen_run_id` and `last_seen_run_id`, on both claims and market claims,
are **targeter observation bounds, not lifecycle**. `last_seen_run_id` is the
last run in which the targeter observed the market expressing the claim.
Absence afterwards is ambiguous by construction — the market may have settled,
been delisted, or stopped being a candidate — and Universe cannot distinguish
those without the venue. This is the same upper-bound rule §5 states for
terminal observation.

Claims are recomputed from each run's own projection, which already carries
every input the mask engine reads, so the whole archive re-projects with no
Targeter change. Because that reconstruction must compile the same masks the
Targeter compiled, ingestion checks it: the claims' implied cross-venue
relations are compared against the ones the report recorded. A relation the
claims **invent** is a guessed equivalence and rejects the run; a relation they
**miss** is a visible false negative, counted rather than raised, per §2's
preference for a false negative over a guess.

`GET /v1/relationship-types` publishes the closed stored type catalogue.

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

Runtime schema version is 5 and is initialized from one canonical resource:

```text
universe/schema/schema.sql
```

Schema v5 intentionally has no in-place migration. Before deploying this
version, stop Universe jobs and remove the existing rebuildable SQLite database
(including its WAL/SHM siblings), then run backfill against the immutable
archive. Starting against schema v1/v2/v3/v4 fails with a clear rebuild
instruction.

Each admitted run and both projections are inserted in one `BEGIN IMMEDIATE`
transaction with foreign keys, WAL, and `synchronous=FULL`. A projection
identity over the resolved public event/market rows detects changed
re-ingestion output. SQLite backup uses the native online backup API followed
by `integrity_check`.

## 7. Sync and backfill

`universe/run_sync.py` and `universe/run_backfill.py` are scheduler-owned
one-shot jobs. Incremental sync advances a high-water date independently of bad
manifests. Failures live in a durable retry ledger with capped exponential
backoff (at most one day) and a 32-item retry budget per invocation, so one
systematic corruption remains visible without forcing unbounded date rescans or
blocking later runs. Malformed listed keys are isolated into the same ledger.

Fresh-database bootstrap walks backward at most 144 valid manifests looking for
the newest complete run. Exhausting that budget is an explicit failed/degraded
result; full history comes from backfill, never from an unbounded bootstrap.

Backfill uses the configured half-open generated-time range, processes 100-run
batches, emits one progress record per batch, and checkpoints each committed
batch in SQLite. Restarting the same range resumes after its durable cursor.
Origin dependencies may be ingested outside that range when required to prove
continuity. Backfill has an independent range checkpoint, so running an initial
incremental sync cannot make older source evidence undiscoverable. However, a
canonical identity rebuild must start from an identity-empty database as
described below.

Because ordinals disambiguate evidence that has no surviving alias edge, the
canonical rebuild visits the retained archive oldest-first. Ordinal allocation
is deliberately encounter-ordered: the first successfully ingested occurrence
for one identity tuple receives ordinal zero, and periodic sync appends but
never renumbers later ordinals. Repeating the same successful ingestion order
repeats IDs and API links; temporarily unavailable evidence that is admitted
later may receive a later ordinal. The newest-run bootstrap remains an
operational recovery path rather than a substitute for a full historical scan.

`event_identity_lineage` claims that generated-time range before the first
allocation. A new claim requires an identity-empty database; a resume must use
the exact same bounds. Backfill records every failed manifest in
`universe_sync_failures`, continues through later manifests, and advances its
checkpoint through the processed batch. Completing the range scan completes
the lineage and unblocks incremental sync even when failed manifests remain in
the retry ledger. Their manifest keys, errors, attempt counts, and retry times
stay durable and `/healthz` stays degraded until they ingest successfully.
This prevents one malformed immutable run from pinning the whole rebuild while
keeping the missing evidence explicit. Changing the evidence admitted or its
encounter order may change ordinals for otherwise indistinguishable same-day
occurrences.

Incomplete and complete-empty runs remain visible. Incomplete runs create no
event, venue-event, market, venue-market, candidate-decision, relationship, or
lifecycle rows; their partial catalogue cannot claim an immutable native
binding. A complete-empty run naturally creates none of those rows either.

Only catalogue artifacts for venues referenced by complete-run candidates are
retrieved. Their complete stored/logical identities and line counts are still
verified, but only referenced rows are retained in memory and validated as
trusted projection input. Unreferenced object rows do not enlarge the SQL
projection or its validation blast radius. Invalid NDJSON framing/JSON still
rejects the containing referenced artifact because no row identity can safely
be established.

The selected normalized artifacts for one run are capped at 128 MiB decoded,
with a warning at 96 MiB; an NDJSON row is capped at 4 MiB and a report at
100,000 catalogue references. The selection report remains capped at 128 MiB.
This supersedes the ineffective former combination of a 256 MiB per-artifact
limit and a 128 MiB per-run limit.

The immutable ObjectStore remains sufficient to rebuild all SQLite state and is
the retention authority. Universe does not automatically prune runs or apply a
rolling horizon. A built database preserves all history it has indexed; future
historical truncation would be a separate product/API contract.

## 8. Read API

All responses are strict JSON and remain under the 1.75 MB server budget. List
limits are 1–100 and list cursors are opaque and query-specific.

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Schema, latest run, staleness, and normalized counts |
| `GET /v1/targeter/status?limit=5` | Compact landing status and newest complete selection counts |
| `GET /v1/targeter/runs/<run_id>` | Bounded decisions and normalized event/market references |
| `GET /v1/events?limit=&cursor=` | Canonical event summaries with identity coordinates and native aliases |
| `GET /v1/events/<event_id>` | Event, venue events, canonical markets, claims, claim relations, observations |
| `GET /v1/markets/<market_id>` | Canonical market, venue instances, selections, claims, claim relations |
| `GET /v1/claims/<claim_id>` | Claim, the markets expressing it, and its claim relations |
| `GET /v1/relationship-types` | Closed relationship-type catalogue |
| `GET /v1/runs`, `/v1/selections`, `/v1/bundles` | Historical compatibility APIs |

`GET /v1/targeter/cadence` returns 404. Run detail intentionally omits raw
candidate relationship arrays and raw report payloads, and carries no relations
of its own: what a run saw is answered by the markets it names plus each claim's
observation bounds. Clients follow event, market, and claim IDs for detail.

The API process reads SQLite only and needs no object-store credentials. Sync
owns all verified archive retrieval.
