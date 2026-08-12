# Target record evidence V1

Status: **implemented**, except §5 write-on-change — see §5.2. The artifact, the
projection, the archive inventory and gate 1's three repointed checks are live.
The backfill tool (§7) is not written.

Replaces an earlier design that carried the raw record for every market in the
catalogue and backfilled it from the HTTP cache. Both are revisited below: the
placement in §2, the backfill in §7.

Scope: persist the raw venue record for every subscribed market, make it queryable
across a slice of targeter runs, and give the gates a way to consume weaker
backfilled evidence without laundering it.

---

## 1. What changes, and why the earlier plan is replaced

`CANONICAL_MARKET.as_record()` (`targeter/v2/domain.py:338`) omits `raw`, so the
venue's own record is fetched and then discarded. `MetadataCatalogue` is
consequently empty, gates 3–5 cannot run, and gate 1's
`market_rules_and_metadata` and `fee_model_evidence` fail. This is the
load-bearing problem.

Two things about the earlier fix were wrong.

**Scope.** §5.1 proposed carrying `raw` for every market in
`catalog_<venue>_markets.ndjson` — all 5,301 markets in a real shadow run
(polymarket 3,678, kalshi 1,489, limitless 134). Only the *selected* markets are
ever subscribed, and only subscribed assets appear in the tape. The observed
selection in that run is **12 markets**. Raw for the other 5,289 is evidence for
a question that cannot be asked, because nothing was captured to interpret.

| | raw per run | per day @ 10 min | to S3 |
|---|---:|---:|---:|
| earlier proposal — whole catalogue | +20.4 MB | +2.9 GB | ~300 MB |
| this document — selected markets, every run | ~54 KB | ~7.8 MB | ~1 MB |
| this document — selected markets, write-on-change | ~54 KB first run | **~50 KB** | negligible |

**Placement.** §5.1 put raw beside the normalized view in the catalogue
artifact. The catalogue is the wrong grain — it is keyed by everything
discovered, not by what was subscribed — and `selection_report.json` is worse:
5.5 MB of its 8 MB is `candidates`, the record of *why* markets were chosen. That
artifact is an argument, and mixing evidence into it makes the two inseparable in
the one place they most need to be apart. It is also a single JSON blob, so
querying a slice means parsing 8 MB per run.

---

## 2. Fixed V1 decisions

| Question | V1 decision |
|---|---|
| Artifact | one new per-venue NDJSON, `target_records_<venue>.ndjson` |
| Grain | one row per selected market per *change*, not per run |
| Contents | the venue record **verbatim**, never trimmed |
| Change detection | a hash over a reader-declared projection, computed at write time from a projection owned by `replay/` |
| Venues with no reader branch | no projection, therefore **no compaction** — write every run |
| Volatile fields | stored, never projected |
| Backfill | a separate tool writing the same row shape, never an edit to an archived run |
| Provenance | a required row field: `captured` or `asserted` |
| Gate network access | none, ever |
| Compression | inherits `artifact_format` from the run, like every other artifact |
| Gate structure | gate 1 stays whole; its three targeter checks are repointed at this artifact |
| De-selection | not modelled; a selected market vanishing is a reported diagnostic |
| `CanonicalEvent.raw` | stays unserialized — no change |

---

## 3. The artifact

Written at the end of `run_targeter_v2` alongside the existing artifacts, via the
same `_write_artifact` helper (`targeter/v2/run.py:60`), so it inherits zstd
framing, `decoded`/`stored` identities, and the compression record with no new
plumbing.

```
<run_directory>/target_records_<venue>.ndjson[.zst]
```

One row per selected market, emitted in `target_id` order:

```json
{
  "version": 1,
  "run_id": "20260803T202615.510658Z",
  "venue": "polymarket",
  "target_id": "polymarket:2758345",
  "subscription_ids": ["11198077718204191404953209021206758204595137249743..."],
  "observed_at": "2026-08-03T20:26:15.510658Z",
  "provenance": "captured",
  "projection_id": "polymarket.v1",
  "projection_sha256": "<hash over the projected fields>",
  "record_sha256": "<hash over the whole record, canonical JSON>",
  "record": { ... the venue's record, verbatim ... }
}
```

`record_sha256` and `projection_sha256` both use the canonical encoding already
used for `catalogue_record_hash` (`targeter/targets.py:112-118`): `sort_keys`,
`separators=(",", ":")`, `ensure_ascii=False`, `allow_nan=False`. `_instrument`
recomputes and compares (`replay/catalog.py:155-165`), so any drift in the
encoding is a loud failure rather than a silent mismatch.

### 3.1 Why verbatim, given we know which fields we read

Storing only the projected fields would save roughly 46 KB per run and cost every
field nobody has thought of yet — `negRisk`, `umaResolutionStatus`, whatever
distinguishes two market variants not yet encountered. That is the same
missing-evidence problem repeated at smaller scale, and at this volume there is
nothing to buy with it. The governing principle in
`Architecture_refinments.md` applies unchanged: capture is irreversible,
analysis is not.

### 3.2 `run_id` is the only key this artifact needs

Artifacts are written before the run is archived, and `metadata_digest` is not
minted until publication (`targeter/v2/publication.py:493`, via `load_targets`),
so the row cannot carry one. This costs nothing.

`metadata_digest` exists so a tape record can name the subscription version it was
captured under. Terms lookup does not use it: `by_asset` takes an optional digest
and is called with one in exactly **one** place across all of `replay/`
(`replay/economics.py:208`, polymarket), falling through to `latest_by_asset`
everywhere else. Under §5, `run_id` + `observed_at` gives a time-indexed lookup —
"what were this market's terms at time T" — which is *strictly better* than the
latest-wins fallback it replaces.

The digest still matters for a different question — does every digest the tape
references resolve to a snapshot that exists — and that question is about the
tape and its snapshots, not about target records. It is handled separately in
§8.3.

---

## 4. The projection

The projection is the set of fields the *reader* consumes. It is not a claim
about which fields the venue keeps stable — that is a venue schema guess and
unknowable. It is a restatement of what `replay/catalog.py:_instrument` already
reads, which is declared in code.

| Venue | Projected fields |
|---|---|
| polymarket | `clobTokenIds`, `outcomes`, `orderMinSize`, `endDate`, `description`, `feesEnabled`, `feeSchedule.rate`, `feeSchedule.exponent`, `feeSchedule.takerOnly`, `feeType` |
| limitless | `tokens.yes`, `collateralToken.decimals`, `priceOracleMetadata.chartSource`, `priceOracleMetadata.chainlinkPair`, `priceOracleMetadata.symbol`, `description`, `title`, `settings.minSize`, `expirationTimestamp`, `feesEnabled`, `feeSchedule.*`, `feeType` |
| kalshi | **none defined** — `replay/catalog.py:257` is a bare `return None` |

**The projection lives in `replay/catalog.py`, beside `_instrument`, and is
imported by the targeter.** Putting it anywhere else guarantees it drifts: a
field added to `_instrument` without being added to the projection produces
intervals that miss real changes, silently. Co-located, that is one file and one
test.

**A venue with no projection is not compacted.** Kalshi has no reader branch, so
there is nothing to project and no basis for deciding a record is unchanged.
Write every run. At 7 selected kalshi markets this is free, and it means that
when the Kalshi branch lands, the history behind it is at full resolution rather
than having been compacted against an empty projection.

Not projected, deliberately: `volume_24h`, `volume_total`, `volume_total_usd`,
`liquidity`, `bestBid`, `bestAsk`, `lastTradePrice`, `spread`, and everything
else that moves with trading. `replay/` reads none of them — verified by grep,
one hit across the package and it is a comment. Volume is derivable from the tape
if it is ever wanted.

### 4.1 Why the projection is computed at write time but owned by the reader

Compaction has to happen at write time or it saves nothing. But the *definition*
belongs to the reader, and the stored bytes stay verbatim, so revising the
projection later means re-deriving intervals from records already held rather
than recollecting. Judgment moves to the reader without moving evidence out of
capture.

---

## 5. Write-on-change and the observation range

A row is emitted when a market's `projection_sha256` differs from the last row
written for that `target_id`, or when it has no previous row. Expected steady
state is one row per market per lifetime.

**De-selection is not modelled.** Selection gates do not walk back; only the
exchange withdrawing a market could remove one mid-window, which is a black swan
and not worth interval-close logic. A selected market that disappears from a
later run is therefore recorded as a run diagnostic and left for a human, in the
same spirit as `lane_of` raising rather than guessing (`replay/lanes.py:57-82`).
Replay closes the interval at the last observation either way.

**A row's absence at run N must not be readable as "not observed at run N".**
This is the same distinction as an empty sealed segment versus a missing one,
resolved in `b9340b6` for the tape and required here for the same reason: without
it, "these terms held from 12:00 to 14:00" and "we stopped looking at 12:10" are
identical bytes.

The run therefore also writes, for every selected market whose projection did
**not** change:

```json
{"version": 1, "run_id": "...", "venue": "...", "target_id": "...",
 "observed_at": "...", "unchanged_since_run_id": "...",
 "projection_sha256": "..."}
```

A tick, not a record — tens of bytes. Replay reconstructs validity intervals by
walking runs in order: a row opens an interval, a tick extends it, and absence of
both closes it at the last observation.

### 5.2 Not implemented: rows are written every run

**Deviation from §2's grain, taken during implementation.** Write-on-change needs
a baseline — the previous run's projection hashes — and a targeter run is a
one-shot process that "cannot silently overlap or retain stale vendor state
between runs" (`targeter/v2/run.py:1-6`). Reading the previous run back means
resolving its artifact identities through the selection report and its zstd
sidecar, and it breaks entirely once the run reaper deletes that run at the
18-hour floor.

Against that: the compaction saves ~7.8 MB/day uncompressed, against the ~4.6
GB/day the run archive already ships. It buys 0.17% for cross-run state in a
process designed not to have any.

So the writer emits one row per selected market per run. The row shape is exactly
§3's, so adding compaction later is purely subtractive — it skips emitting rows,
and needs the tick from §5 at that point. Interval reconstruction on the read
side is unaffected: consecutive equal `projection_sha256` values collapse into
one interval whether or not the redundant rows were ever written, and keeping
them makes "observed and unchanged" explicit rather than inferred.

### 5.1 Instrument this from the first run

The run emits a counter: how many selected markets had a projection change.
Expected ~0 in steady state. If it is not, the projection is wrong, and this
surfaces it in a day rather than in six months.

Inter-run stability was measured once, on two production runs nine minutes
apart: 99.5% of polymarket records and 100% of kalshi and limitless records were
identical once `volume_24h`, `volume_total`, `volume_total_usd` and `liquidity`
were ignored, those being the only fields that moved. That measurement was taken
on the **normalized** record. Raw-record stability under this projection is
**unverified** and cannot be verified until raw is captured. The counter is how
that gets checked.

---

## 6. What replay does with it

Replay reads the slice of archived runs covering the capture window, in `run_id`
order, and builds `MetadataCatalogue` from the reconstructed intervals rather
than from `/metadata/` snapshots alone.

`MetadataCatalogue.from_streamer` (`replay/catalog.py:95`) gains a second source.
`_instrument`'s existing per-venue logic is unchanged — it is fed
`row["record"]` instead of `resolution["catalogue_record"]`.

Two changes to `_instrument` are required regardless of where records come from:

- **`condition_id` must become optional.** It is currently a required `str`
  (`replay/catalog.py:37`) and a hard `return None` gate (`:148`). Kalshi
  publishes no such field — a real record's keys are `ticker`, `event_ticker`,
  and there is no condition identifier at all. Requiring a Polymarket-shaped
  field of every venue is the same category error as `_fee_terms` reading Gamma
  names. Identity needs `venue` + `subscription_asset_id` + `market_id`.
- **A Kalshi branch.** Out of scope here; it needs its own review.

### 6.1 Fees are deferred, not derived

`_fee_terms` should record **`fee_type` and `fee_multiplier` and stop**, leaving
the arithmetic to replay's economics. Computing a fee at catalogue time is
interpretation in the wrong place, and the current shape already leaks one venue's
model into the contract — `rate`/`exponent`/`taker_only` are Polymarket Gamma's
published-curve parameters.

Because records are stored verbatim (§3.1), this is entirely a `replay/catalog.py`
decision and **blocks nothing in this document**. It can land before, with, or
long after the artifact.

One constraint when it does. A real Kalshi market record carries no fee field at
all — only `notional_value_dollars` — so any Kalshi multiplier comes from the
venue's *published schedule*, not from the record. `FeeTerms.source_record_hash`
would then be a claim about its own provenance that is false. Fee terms need a
source discriminator: `record` (hash names where it came from) versus `schedule`
(an identifier for the published schedule and its date). Same field, honest
value; gate 3 can then weight the two differently instead of being unable to tell
them apart.

---

## 7. Backfill

A separate tool, `scripts/backfill_target_records.py`. It walks archived runs,
reads the selected markets from each, fetches the venue's current record, and
writes rows in the shape of §3 with `provenance: "asserted"`.

**It never edits an archived run.** Every archive write is a conditional
`PutObject` with `IfNoneMatch="*"` (`archive/storage/s3.py:216`), and amending an
artifact would change the run digest and break the receipt and manifest chain.
Output goes to a parallel location keyed by `run_id`.

**Why fetching current records works for historical windows.** Every field in the
§4 projection is creation-time immutable — `clobTokenIds`, `outcomes`,
`conditionId`, `endDate` do not change over a market's life. A fetch today
returns August's terms for anything not delisted. This is a stronger position
than a backfill from the HTTP cache, which is bounded by what a
latest-response-per-URL store happens to hold.

**What `asserted` means, precisely:** "we fetched this record later and believe
these terms are immutable", not "this is what the targeter used, proven". It is
weaker evidence and it stays labelled in the data, not in a flag someone forgets
was set.

The tool records a per-market outcome — `fetched`, `not_found`, `error` — so
coverage is a number rather than a boolean.

---

## 8. Gate consumption

### 8.1 Gates never make network calls

A gate's product is that its verdict is a deterministic function of its dataset:
`report_sha256` is a hash over the report body including the input manifest
(`replay/gate1.py:79-85`). An HTTP call inside a gate means the same window
audited tomorrow yields a different answer, the digest stops meaning anything,
and a venue 429 becomes a capture-audit failure. It also breaks the boundary
every other part of replay respects — *"it does not open paths, call S3, or know
whether an object came from NFS"* (`replay/stream.py:1-7`).

The fetch is §7's tool. The gate reads the artifact.

### 8.2 Strict mode is a policy flag, not an I/O switch

`--allow-asserted-records` (default off) controls whether rows with
`provenance: "asserted"` satisfy metadata checks. Off, only `captured` rows
count. On, both count and the report states the split.

Reported either way: subscribed assets with a captured record, with an asserted
record, with neither. Failure is the third bucket only. That names which markets
are unanalysable rather than only that some are.

### 8.3 Gate 1 stays whole; its three targeter checks are repointed

An earlier draft proposed splitting gate 1 into a tape audit and a metadata
audit. Splitting relocates the problem rather than solving it: gate 2 asserts
`gate_1_precondition`, so unless the *dependency* changes too, a metadata failure
still blocks the chain from one gate further down. The split is worth doing when
there are enough targeter-side checks to justify a gate, and not before.

The operational win attributed to the split does not actually require it. What
made gate 1 unable to run from the archive was its **inputs**, not its structure —
`live/coverage.json` and the generation metadata snapshots. All three checks can
be satisfied from this artifact instead:

| Check | Previously read | Reads instead |
|---|---|---|
| `market_rules_and_metadata` | `resolution.catalogue_record` in `/metadata/` snapshots | `record` — `description`, `rules_primary` |
| `fee_model_evidence` | `feeSchedule` / `feeType` in snapshots | `record`, via `has_fee_terms` |
| `discovery_coverage` | `live/coverage.json` sightings | earliest `observed_at` per `target_id`, plus `createdAt` (polymarket) / `created_time` (kalshi) from `record` |

Both sources feed the same counters rather than one replacing the other: a v1
dataset carries the record inside `resolution.catalogue_record`, a v2 dataset
carries it in the artifact, and the question asked is identical.

**`fee_model_evidence` had a bug this surfaced.** Its predicate was
`feeSchedule is not None or feeType is not None`, which disagreed with the reader
in both directions — it missed `feesEnabled: false`, a *complete* fee model (no
fees) and the one Polymarket publishes most often, and it counted a bare
`feeType`, which `_fee_terms` turns into nothing. Both now call
`replay.catalog.has_fee_terms`, so the gate's idea of measurable cannot drift
from the code that measures.

Both creation timestamps are confirmed present in real venue records. So
`discovery_coverage` keeps its exact meaning — first sighting retained for every
subscribed asset, creation time measured where published — while its source
becomes an archived, immutable artifact rather than a file the targeter rewrites
underneath the reader.

Gate 1's input set becomes segments, seals, and target records: all archived, all
immutable. `live/` stops being *required*. No split required.

**It does not stop being read.** `gate1_object` still admits
`live/coverage.json` and the generation metadata snapshots, so a gate pointed at
a root containing `live/` still picks them up and can still abort with
`object changed after stream snapshot: live/coverage.json` if the targeter
republishes mid-audit. The race disappears for an archive-backed dataset, which
has no live tree, and for a spool root the operator scopes away from one — not
because the gate stopped looking. An earlier draft of this document claimed
otherwise.

**One carve-out.** `market_rules_and_metadata` currently also asserts
`metadata_digests_referenced - metadata_digests_seen` is empty — a genuine check,
but about the tape and its snapshots, not about rules. It cannot be satisfied from
target records, and on an archive-only dataset every referenced digest would land
in `missing_references` and fail a tape that is fine.

Give it its own name, `metadata_snapshot_references`, with the resolution scoped
to what the dataset actually carries: `FAIL` when the dataset holds snapshots and
some referenced digest is absent; `ADVISORY` when it holds none, which is what
`ADVISORY` is already for (`replay/gate1.py:33-37`). The guarantee is preserved
wherever it is checkable, and a dataset is not failed for lacking something that
was never in it.

---

## 9. Blast radius

**Must not move:** `target_digest` (`targeter/targets.py:74-88`). It hashes only
venue and sorted asset ids, so it cannot move — but verify explicitly rather than
assume, because if it moved every splice would resubscribe every run and resync
every book.

**Moves, expected and safe:** the run's artifact inventory, run digest, run
manifest and archive receipt. Receipts are per-run and never compared across
runs.

**Does not move:** `metadata_digest`, because raw stays out of `resolution`. The
tape's `connection_opened` records commit to it.

**One allowlist changes, not two.** `_artifact_inventory`
(`targeter/v2/run_archive.py:260-285`) enforces `set(raw) != expected` and gains
the three names. `_legacy_artifact_names` (`:336-347`) must **not**: it is the
fallback for runs archived before the `artifacts` inventory existed, and those
runs genuinely have no target records — adding the names there would fail every
one of them for files that were never written. An earlier draft of this document
said both; the code says otherwise.

A corollary for anyone writing fixtures: stripping `artifacts` from a report to
simulate a legacy run must also delete the target-record files, or it builds a
hybrid that never existed (`tests/test_targeter_v2_delivery.py:143`).

---

## 10. Verification

```bash
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m unittest discover -s replay/tests -t .
```

- A shadow run writes `target_records_<venue>.ndjson` containing venue field
  names (`clobTokenIds`, `rules_primary`, `feeSchedule`), one row per selected
  market, and no row for an unselected one.
- `target_digest` is byte-identical across the upgrade.
- Two consecutive runs with no venue change produce one record row and one tick,
  not two record rows.
- A synthetic projection change produces a second record row, and the interval
  built from the pair has the boundary at the second run.
- A venue with no declared projection writes a record row every run.
- `MetadataCatalogue` over a real archived run yields non-zero instruments for
  polymarket and limitless, with `fee_terms` where the venue publishes one.
- Every published target resolves to an instrument, or is named in the
  uncovered bucket.
- Gate 1 against a dataset with only `asserted` rows fails with
  `--allow-asserted-records` off and passes with it on, and the report states the
  split in both cases.
- An asset with **both** a captured and an asserted row stays covered in strict
  mode. Subtracting every asserted asset made one backfill row nullify a live
  sighting, so the report affirmed captured evidence and called the asset
  uncovered in the same breath.
- The earliest sighting wins whichever source supplied it and however it spells
  the instant. `live/coverage.json` writes `+00:00`; the targeter writes `Z` and
  drops the fraction on a whole second; `'+' < '.' < 'Z'` sorts all three the
  wrong way under a string compare.
- A record for an asset nobody subscribed to satisfies nothing, and a row
  repeated every run counts once.
- `discovery_coverage` derived from target records reports the same
  `subscribed_assets` and `with_created_at` as the same window derived from
  `live/coverage.json`. That equality is what proves the source swap is a swap and
  not a redefinition.
- `metadata_snapshot_references` is `ADVISORY` on a dataset carrying no snapshots
  and `FAIL` on one carrying some but not all referenced digests.
- Gate 1, after §8.3, passes over an archive-only dataset with no `live/` present.

---

## 11. Resolved, and still open

Resolved during review:

| Question | Decision |
|---|---|
| Interval close on de-selection | Not modelled. Selection gates do not walk back; a vanished market is a diagnostic (§5). |
| Does `CanonicalEvent` carry raw? | No. The field exists in memory (`domain.py:203`) and is already dropped by `as_record()` — no change required. |
| `metadata_digest` binding | Not needed. `run_id` + `observed_at` is a better lookup than the one it replaces (§3.2). |
| Split gate 1? | No. Repoint its three targeter checks at this artifact instead (§8.3). Revisit when targeter-side checks are numerous enough to earn a gate. |
| Kalshi fee terms | Record `fee_type` and `fee_multiplier`; defer the arithmetic to economics (§6.1). |

Still open:

0. **Production runs write zstd, and the gate cannot read it.** `.ndjson.zst` is
   deliberately excluded (§3, `target_record_object`) so a compressed artifact
   fails loudly rather than producing a vacuously empty check. But
   `artifact_format` defaults to `zstd`, so a production run's target records are
   not in the dataset at all until the S3 adapter — or any decoding streamer —
   lands. Until then, gate 1 over a production run needs `--artifact-format
   ndjson`. This is the one thing standing between this work and a green gate on
   real data.
1. **Where do asserted rows live?** A parallel per-run tree, or one store keyed by
   `run_id`? §7 requires only that it is not the archived run.
2. **Does this retire the `pm_hashes` memory problem?** Largely, by window size
   rather than by this document: 24.5M hex strings is five days, ~800 MB for one.
   If it returns, gate 1 needs only a distinct count and a membership test against
   the much smaller snapshot set, not the full set.
3. **Does `fee_multiplier` need a unit?** Polymarket's curve parameter and a
   Kalshi schedule multiplier are not the same quantity. The source discriminator
   in §6.1 may be sufficient, or the type may need to carry its own units.

---

## 12. What this does not solve

- The two gate 2 paths that crash instead of reporting. Both sit outside the
  `try` in `Gate2Auditor.audit`, so one failure loses every other check in the
  run; reproduced as `ValueError: analysis evidence requires interval trust`.
  Independent, small.
- The reaper. Deleting the spool destroys replay's input regardless of how often
  replay runs. Fixed by a time-based retention floor on the raw reaper — the run
  reaper already has one at 18h (`targeter/v2/run_reaper.py:119`) — or by the S3
  adapter. The floor needs neither this document nor S3.
- The S3 `ByteStreamer`. **Being implemented on a separate branch, and the two
  should not touch.** This document adds an artifact to a run directory and
  repoints checks at it; the adapter changes where bytes come from. The only
  shared surface is that gate 1's inputs are now all archived, which the adapter
  benefits from and does not depend on. Note for that work: `ObjectStore` has no
  list operation (`archive/storage/base.py:101-111`), and driving `object_keys`
  from archive receipts is likely better than `list_objects_v2`, because receipts
  are already the commit marker and give window scoping for free.
- Kalshi fee terms. No storage decision creates a field the venue does not
  publish; §6.1 defers the interpretation rather than resolving it.
