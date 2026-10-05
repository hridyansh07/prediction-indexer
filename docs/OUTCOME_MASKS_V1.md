# Outcome masks V1

**Status:** implemented; Universe `v0.19.x` serves the endpoint.
**Base:** `feat/same-venue-complement` with `feat/economic-strategy-sdk` merged
(SDK head `07a50c2`). Section references to the SDK spec are to
[`ECONOMIC_STRATEGY_SDK_V1.md`](ECONOMIC_STRATEGY_SDK_V1.md) at that commit.
**Owns:** the "Universe claims and outcome masks (the context.json v2 work)"
open item of the SDK spec.
**Unblocks:** the first-priority structural strategies of the economic
strategy research memo (same-venue complete sets, implication covers,
cross-venue identities). Those
strategies are specified separately, after this lands.

## 1. Purpose and decisions

A structural strategy needs, for every captured book, the subset of the event's
outcome space that the book pays on. Universe already derives exactly this at
ingestion (`universe/claim_projection.py` → `derive_bundle_claims`) but stores
only the claim identity and its key **count**, not the keys. The snapshot that
strategies receive (`context.json` v1) carries pairwise relationships but no
masks.

This spec adds three things and nothing else:

1. a read-only Universe endpoint that returns a bundle's outcome model from one
   ID the caller already has (§3);
2. snapshot `context.json` version 2, which freezes that model and maps every
   captured book to a mask (§4);
3. a small SDK view that strategies call from `baskets()` (§5).

Decisions recorded with the user:

| Decision | Choice |
|---|---|
| Lookup key | The Targeter `bundle_id` already in the preparation config. Universe resolves it to its canonical umbrella `event_id`. No new caller input. |
| Where masks are computed | Universe, on read, from rows it already stores. **No schema change, no migration, no rebuild.** |
| Which market state | The latest stored row per venue market. Venue market IDs are never reused and claim IDs are content hashes, so drift only arises if a market's semantics change after indexing starts, which should never happen. The §3.4 guard marks that case; regressions catch it if it ever appears. |
| Verification | Universe's claim derivation is already differentially tested against Targeter runs. No new relation cross-check in preparation. Preparation verifies only that the document is internally consistent (§4.3). |
| Settlement | Masks describe **normal resolution only**. Void and cancellation branches are deferred. A market whose stored status is a void/cancel terminal state is marked and never priced (§3.5). |
| State transitions | Out of scope, with no hooks. Mid-match mask changes will be delivered through the replay tape in a later spec, so strategies observe them as ordered events. V1 masks are static per bundle. |
| Parallel work | None. This lands and is accepted on the fixture before any strategy work starts. |

## 2. Vocabulary

- **Space**: one enumerated outcome set Ω, from `analysis.outcome_space`. A
  series space for a best-of-3 has the six keys `seq:AA … seq:HH` (sequences of
  map winners, `H`/`A` = the bundle's first/second participant). A score space
  has keys `score:{h}-{a}`.
- **Shape ID**: `analysis.claims.space_shape_id(space)`: a digest of the scope and
  sorted outcome keys. Participant-independent.
- **Claim**: a non-empty, non-total subset of one space's keys.
  `analysis.claims.claim_id(keys, shape_id)` identifies it.
- **Claim key**: `claim=N`, the index Targeter assigns to the N-th tradable claim
  of a venue market (`targeter/v2/relationships.py` `_market_views`). Mask keys
  are `venue:native_id#claim=N`, exactly as in the context's `relationships`.
- **Book key**: the SDK's `(instrument, orientation)` tuple, for example
  `("kalshi:KX…-GLS", "complement")`.
- **Coverage**: `EXHAUSTIVE` when the space enumerates every reachable outcome
  (series spaces), `INCOMPLETE_COVERAGE` otherwise (score spaces assembled from
  listed ladders). Only `EXHAUSTIVE` spaces can support a lock claim.

## 3. Universe: `GET /v1/bundles/{bundle_id}/outcomes`

### 3.1 Resolution

1. Look up `bundle_id` in `selection_occurrences` and join each occurrence's
   origin-run decision in `candidate_decisions` (primary key
   `(run_id, bundle_id)`), which names the umbrella `event_id`. No row →
   `404 {"error": "bundle not found"}`. Only selected bundles resolve, which is
   every bundle preparation can pin.
2. Collect the distinct `event_id` values for that bundle. More than one →
   `409 {"error": "bundle maps to multiple events"}`. Never pick one.

   Do not resolve through `event_observations`: it has a row per candidate per
   run (449k in production) and no `bundle_id` index, and a cold scan took
   about 51 s on the 2 GB Universe VM, past the edge's 30 s upstream timeout.
   On production data both paths agree for all 1,223 selected bundles.
3. Read, for that `event_id` only: the `umbrella_events` row, every
   `venue_events` row, every `venue_markets` row, and the `market_claims` rows of
   those venue markets.

The endpoint reads SQLite only, like every other Universe read API. It needs no
object-store access and writes nothing.

### 3.2 Rebuild

Convert the rows into the projection-row shapes that
`claim_projection._bundle` already accepts (decode the `*_json` columns, map
`accepting_orders` to bool, pass `bundle_id` as `source_bundle_id`). Promote
`_bundle` to a public `rebuild_bundle` so ingestion and this endpoint share it;
do not copy it.

Then compile through the existing single code path. Do not reimplement mask
logic:

- `targeter.v2.relationships._spaces(bundle)` for the spaces and their
  diagnostics;
- for each space and each market, `validate_esports_market` (structured esports)
  or `_market_views` + `compile_mask` (otherwise), exactly as `_scope_masks`
  does, but with **no exclusions** and keeping each market's rejection reason;
- `analysis.claims.claim_id` / `space_shape_id` for identities.

Expose the per-market reason by adding one helper next to `_scope_masks` in
`targeter/v2/relationships.py` that returns `(market, masks, reason)` triples and
have `_scope_masks` call it. The two must stay one code path.

Excluding no markets is intentional. Targeter exclusions are per-run selection
judgements; masks are semantic. A market that cannot be compiled gets a reason
(§3.3), not silence.

### 3.3 Response

Strict JSON, canonical key order, bounded by the existing
`EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES` through `_bounded_detail`. Closed schema:

```json
{
  "version": 1,
  "bundle_id": "bundle_e8a92effa246b9548571c907",
  "event_id": "event:d1:<sha256>",
  "identities": {"claim_identity_version": 2, "claim_algebra_version": 1},
  "status": "complete",
  "diagnostics": [],
  "participants": ["Procyon Gaming", "Galorys"],
  "spaces": [
    {
      "space_shape_id": "<sha256>",
      "scope": "series",
      "coverage": "EXHAUSTIVE",
      "best_of": 3,
      "outcome_keys": ["seq:AA", "seq:AHA", "seq:AHH", "seq:HAA", "seq:HAH", "seq:HH"]
    }
  ],
  "claims": [
    {"claim_id": "<sha256>", "space_shape_id": "<sha256>",
     "outcome_keys": ["seq:AA", "seq:AHA", "seq:HAA"]}
  ],
  "markets": [
    {
      "market_id": "kalshi:KXCS2GAME-26SEP271700PROGLS-GLS",
      "venue": "kalshi",
      "market_type": "series_moneyline",
      "market_status": "open",
      "subscription_ids": ["KXCS2GAME-26SEP271700PROGLS-GLS"],
      "outcome_labels": ["Galorys"],
      "mask_status": "MASKED",
      "reason": null,
      "claims": [{"claim_key": "claim=0", "claim_id": "<sha256>"}],
      "tokens": [{"subscription_id": "KXCS2GAME-26SEP271700PROGLS-GLS",
                  "claim_key": "claim=0", "negated": false}]
    }
  ]
}
```

Field rules:

- `status`: `complete`, or `unreconstructed` when `rebuild_bundle` returns
  `None` (wrong participant count, no venue events or markets). An
  `unreconstructed` document has empty `spaces`, `claims`, and every market
  `mask_status: "NO_SPACE"`. It is a 200, not an error: coverage-only jobs must
  not fail because masks cannot be built.
- `diagnostics`: `_spaces` diagnostics verbatim, sorted
  (`series_scope_missing_unambiguous_best_of_format`, `unsupported_series_format`).
- `spaces`: sorted by `space_shape_id`. `outcome_keys` sorted. `best_of` is an
  integer for series spaces, `null` otherwise.
- `claims`: every distinct claim referenced by any market, sorted by `claim_id`.
  Keys sorted. A claim appears once even when several venues express it.
- `markets`: every venue market of the event, sorted by `market_id`
  (`venue:venue_market_id`, the Targeter target ID).
- `mask_status`: closed set:

  | Value | Meaning |
  |---|---|
  | `MASKED` | Every tradable claim of the market compiled to a usable mask. |
  | `REJECTED` | The esports validator rejected the product; `reason` is its code (`invalid_product_parameters`, `product_outside_series_format`). |
  | `UNSUPPORTED` | No resolver, or `compile_mask` returned `UNSUPPORTED`. |
  | `NOT_A_FUNCTION` | `compile_mask` returned `NOT_A_FUNCTION`. |
  | `DIFFERENT_SCOPE` | No space of the market's scope exists for this event. |
  | `TAUTOLOGY` | A compiled mask was empty or the whole space. |
  | `NO_SPACE` | The market's space could not be built (see `diagnostics`). |
  | `CLAIM_CONFLICT` | §3.4. |
  | `VOID_UNSUPPORTED` | §3.5. |

  A market is `MASKED` only if all its claims are; a partial condition is never
  exposed. Non-`MASKED` markets have empty `claims` and `tokens`.
- `tokens`: the book alignment, computed here because only Universe has the
  labels (§3.6). Empty when alignment is impossible; the market is then
  `UNSUPPORTED` with `reason: "token_alignment"`.

### 3.4 Claim guard

For each compiled `(market, claim_key, claim_id)`, read the `market_claims` rows
for `(venue, venue_market_id, claim_key)`:

- no rows: accept (Targeter excluded the market from every run's derivation, or
  it was never in a derivation);
- rows exist and one has this `claim_id`: accept;
- rows exist and none has this `claim_id`: mark the market `CLAIM_CONFLICT` with
  `reason: "recorded_claim_mismatch"`.

This is a set lookup on content-addressed IDs, not a correctness re-derivation.

### 3.5 Void and cancellation

Masks describe normal resolution. A market whose stored `status`, casefolded,
is in `{"void", "voided", "cancelled", "canceled"}` gets
`VOID_UNSUPPORTED`. The stored status is the catalogue status at the last
Targeter observation, so a market voided afterwards is not detected. Strategies
must label every result as normal-resolution-only (§5.3). Modelling void
branches from `analysis/void_policy.py` is deferred.

### 3.6 Token alignment

| Venue and shape | `tokens` |
|---|---|
| Kalshi, one ticker, one claim | `[{ticker, claim=0, negated: false}]` |
| Limitless, one slug, one claim | `[{slug, claim=0, negated: false}]` |
| Polymarket, *n* ≥ 2 meaningful labels (Targeter's `_meaningful_labels`), *n* tokens, *n* claims | token *i* → `claim=i`, `negated: false` |
| Polymarket, two tokens labelled exactly `Yes`/`No` (casefold), one claim | Yes token → `claim=0`, No token → `claim=0` with `negated: true` |
| Anything else | `[]`, market `UNSUPPORTED`, `reason: "token_alignment"` |

`negated: true` means the token pays on the space's keys minus the claim's keys.
Kalshi's `complement` orientation is handled in preparation (§4.2), not here,
because orientations are a Replay concept.

### 3.7 Bounds

At most 8 spaces, 1,024 keys per space, 4,096 claims, and `DETAIL_ROW_LIMIT`
markets per event; exceeding any of them raises `DetailTooLarge` (`413`, the
existing mapping). Rebuild cost is one bundle's markets per request.

## 4. Preparation: `context.json` version 2

### 4.1 Fetch

`UniverseHTTP` gains `outcomes(bundle_id)`, issuing
`GET /v1/bundles/{bundle_id}/outcomes` with the existing transport rules (no
proxy, no redirects, finite timeout and deadline, 8 MiB body cap).

`prepare(config, directory, *, universe, fallback=None, outcomes=None)` gains an
`outcomes` source: a callable `bundle_id → document | None`. When omitted and
`universe` has an `outcomes` attribute, that is used. For an in-process read,
pass `outcomes=lambda bundle: store.bundle_outcomes(bundle)`.

It is called once per preparation, after the occurrences, and recorded as:

```json
"outcomes": {"provider": "universe", "document": { … §3.3 … }}
```

or, when the source returns `None`, raises `SourceUnavailable` (404, 502, 503,
504, timeout, connection failure), or no `outcomes` source exists (the Targeter
fallback path):

```json
"outcomes": {"provider": null, "unavailable": "universe_outcomes_unavailable"}
```

Unavailability is recorded, not fatal, so coverage-only jobs keep working. Any
other HTTP error (including `409`), a malformed document, or a §4.3 failure
aborts preparation as an integrity failure, like a malformed selection detail.
The config schema is unchanged (still version 1), so job configs and their
hashes do not change.

### 4.2 Book mapping

For each scope, add a key `outcome_books`: one entry per book in that scope's
`members[].books`, sorted by `(instrument, orientation)`:

```json
{
  "instrument": "kalshi:KXCS2GAME-26SEP271700PROGLS-GLS",
  "orientation": "complement",
  "market_id": "kalshi:KXCS2GAME-26SEP271700PROGLS-GLS",
  "status": "MASKED",
  "reason": null,
  "space_shape_id": "<sha256>",
  "claim_id": "<sha256>",
  "negated": true
}
```

Rules:

1. Find the member's market in the document by `market_id`. Absent →
   `status: "NOT_IN_MODEL"`.
2. The market's `subscription_ids` must equal the scope target's
   `subscription_ids` from the pinned context. Different →
   `status: "SUBSCRIPTION_MISMATCH"`.
3. The market's `mask_status` is not `MASKED` → copy it and its `reason`.
4. Otherwise find the token whose `subscription_id` is the book's native ID (the
   instrument after `venue:`). Its `claim_key` gives the `claim_id`; its
   `negated` is the base value. For Kalshi, `orientation: "complement"` flips
   `negated`. Polymarket and Limitless books are always `outcome`.
5. Any non-`MASKED` status sets `space_shape_id`, `claim_id`, `negated` to `null`.

When `outcomes` is unavailable, every entry is
`status: "OUTCOMES_UNAVAILABLE"` with nulls. Members without books
(`uncaptured_mapping_unknown`) have no entries; strategies already see them
through `members`.

Existing scope keys are unchanged. Current readers (`bundle_coverage`,
`coverage_output`, `complement_contract`, the SDK runtime and readers) access
scope fields by name, so the added key does not affect them.

### 4.3 Document validation

Preparation validates the document with the closed-schema helpers in
`replay.streams.protocol` and rejects it unless:

- `version == 1`, `bundle_id` equals `config["bundle_id"]`, identities equal
  the current `CLAIM_IDENTITY_VERSION` and `CLAIM_ALGEBRA_VERSION`;
- every `space_shape_id` recomputes from its `scope` and `outcome_keys`;
- every `claim_id` recomputes from its keys and shape, its shape is a listed
  space, and its keys are a non-empty strict subset of that space's keys;
- every market's `claims` reference listed claims, `tokens` reference its own
  `claims`, and `mask_status`/`reason` are from the closed sets;
- all lists are sorted and unique as §3.3 specifies.

This checks internal consistency and identity only. It does not re-derive masks.

### 4.4 Snapshot

```text
version: 2
config, evidence, plans, membership_basis, history_complete   (unchanged)
scopes[]: existing keys + outcome_books
outcomes: {provider, document} | {provider: null, unavailable}
```

- `build_snapshot(config, evidence, outcomes)` derives `outcome_books`
  deterministically from the config, the evidence, and the recorded `outcomes`.
- `load_snapshot` accepts version 1 (no `outcomes`, no `outcome_books`) and
  version 2, and keeps its rule of rebuilding and comparing byte-for-byte.
- `prepare` always writes version 2.
- The receipt format is unchanged (version 1).
- All existing bounds (§Bounds of
  [`STRATEGY_PREPARATION_V1.md`](STRATEGY_PREPARATION_V1.md)) still apply to
  the whole snapshot.

The job prepare stage (`replay/jobs/stages.py` `prepare_stage`) already passes
`UniverseHTTP`; it needs no change beyond the new method existing.

## 5. SDK: `replay/economic_sdk/outcomes.py`

### 5.1 API

```python
from replay.economic_sdk.outcomes import outcome_scope

scope = outcome_scope(snapshot, scope_index)   # built once per scope, cached
scope.available            # False for v1 snapshots or unavailable outcomes
scope.unavailable          # reason string or None
scope.spaces               # {shape_id: Space(shape_id, scope, coverage, best_of, keys)}
scope.leg(book_key)        # Leg | None
scope.status(book_key)     # (status, reason) from outcome_books
scope.payoff(book_keys)    # {shape_id: tuple[int, ...]} per unit, aligned to Space.keys
scope.is_partition(book_keys)
scope.implications(book_keys)
scope.complete_sets(book_keys, *, max_legs=4, limit=4096)
```

`Leg(book, market_id, shape_id, claim_id, negated, keys)`, where `keys` is the
frozenset the book pays on after negation.

- `payoff`: requires all legs in one space; returns the integer payout count per
  outcome key. A leg listed twice counts twice.
- `is_partition`: true only when all legs are `MASKED`, share one space with
  `coverage == "EXHAUSTIVE"`, are pairwise disjoint, and their union is the
  space. False otherwise; never raises for unmasked legs.
- `implications`: ordered pairs `(a, b)` with `keys(a) ⊂ keys(b)` strictly,
  within one space. Sorted by book key.
- `complete_sets`: every set of at most `max_legs` books from `book_keys` that
  `is_partition` accepts. Exact-cover search in sorted book-key order, so output
  is deterministic. Sets are sorted tuples; the list is sorted. Raises when more
  than `limit` sets exist rather than truncating.

These are called from `baskets()` only. `evaluate` never touches masks, so this
adds no per-cut cost.

### 5.2 Admission

Strategies map a leg whose status is not `MASKED` to the existing SDK admission
`UNSUPPORTED_SHAPE`, with the status as the structured reason. Unavailable
outcomes make every mask-dependent basket `UNSUPPORTED_SHAPE` with reason
`outcomes_unavailable`. Nothing is guessed.

### 5.3 Labelling

Every manifest of a mask-dependent strategy records
`"settlement_model": "normal_resolution_only"` and the snapshot's `outcomes`
provider. Strategies must not describe a payoff as locked without that label.

## 6. Documentation changes

- [`STRATEGY_PREPARATION_V1.md`](STRATEGY_PREPARATION_V1.md): replace "No …
  outcome masks … is performed" with a pointer here; document snapshot version 2
  and the `outcomes` source.
- [`EVENT_UNIVERSE_STORE_V1.md`](EVENT_UNIVERSE_STORE_V1.md) §8: add the
  endpoint row.
- [`ECONOMIC_STRATEGY_SDK_V1.md`](ECONOMIC_STRATEGY_SDK_V1.md) §12: close the
  outcome-mask open item with a pointer here.
- [`AGENTS.md`](../AGENTS.md) §4: add a row: "Outcome masks for replay
  strategies" → this document, then
  `analysis/MARKET_RELATIONSHIP_GRAPH.md` and the preparation spec.
- `replay/README.md`: one line on `outcome_scope`.

## 7. Tests

All offline, with small hand-authored rows, no live Universe or cloud.

**Universe** (`tests/test_event_universe_store.py` and API tests):

- A CS2 Bo3 event with two Kalshi series moneylines, a Polymarket two-token
  series moneyline, and Kalshi map-2 winners: hand-checked spaces, claims,
  masks, and tokens. The two Kalshi moneylines and the two Polymarket tokens are
  each a partition; the Kalshi and Polymarket claims for the same team share one
  `claim_id`.
- A Bo5 event: 20 keys.
- `map_index` greater than `best_of`: `REJECTED`,
  `product_outside_series_format`.
- Conflicting formats: `NO_SPACE` with the diagnostic.
- Polymarket `Yes`/`No` alignment and a misaligned token count:
  `token_alignment`.
- A `market_claims` row with a different `claim_id`: `CLAIM_CONFLICT`.
- A `cancelled` market: `VOID_UNSUPPORTED`.
- Unknown bundle: 404. Bundle on two events: 409. Unreconstructable: 200
  `unreconstructed`.
- Ingestion and the endpoint produce the same `claim_id` for every market claim
  of one ingested run (the shared-path check).
- Two calls return byte-identical documents.

**Preparation** (`replay/tests/test_preparation.py`):

- A version-2 snapshot from a fake `outcomes` source: `outcome_books` for
  Kalshi outcome and complement (complement negated), Polymarket tokens, and
  Limitless.
- `NOT_IN_MODEL`, `SUBSCRIPTION_MISMATCH`, and a copied non-`MASKED` status.
- Unavailable outcomes (404, `SourceUnavailable`, no source): a version-2
  snapshot with `OUTCOMES_UNAVAILABLE` books.
- `409` and a malformed document abort.
- A tampered claim key list, a tampered shape key, and an unsorted list each
  fail §4.3.
- A version-1 snapshot still loads unchanged. A version-2 snapshot reloads and
  rebuilds byte-identically.
- `probe_markets` subsets map only the probed members' books.

**SDK** (`replay/tests/test_economic_sdk_outcomes.py`):

- `payoff`, `is_partition` (true for the two series moneylines; false for
  overlapping legs, a gap, mixed spaces, an `INCOMPLETE_COVERAGE` space, and an
  unmasked leg), `implications` strictness, negated legs.
- `complete_sets` on a Bo3 with series and map claims: exact expected sets,
  deterministic order, and the `limit` error.
- `available` is false for a version-1 snapshot.

Gates: the focused modules above, then
`.venv/bin/python -m unittest discover -s tests` and the replay test suite.

## 8. Acceptance on the fixture

Deploy the Universe image with the endpoint (code only; the schema-v6 database
is reused as is). Then re-prepare the local bench bundle
`bundle_e8a92effa246b9548571c907` (Procyon vs Galorys, CS2) against it:

1. `outcomes.provider == "universe"`, `status == "complete"`, one series space
   with `best_of: 3` and 6 keys.
2. Every captured book has an `outcome_books` entry. The two Kalshi series
   moneyline outcome books form a partition, as do each Kalshi outcome book and
   its own complement. The map-2 winner and total-maps masks, and the
   Polymarket token masks, match a hand check.
3. Every `MASKED` book's claim agrees with the pinned context's recorded
   `relationships` for that pair. This is a one-off acceptance check, not
   production code.
4. Re-preparing into a new directory gives the same `context.json` bytes.
5. The two-group replay output is unchanged except for the snapshot binding:
   `intervals.ndjson` keeps SHA `41be6ad8…`; the coverage and complement
   manifests differ only in `snapshot_sha256` (and the hashes derived from it).
   The reference hashes in the bench memory are then updated to the version-2
   snapshot.

Report any book that is not `MASKED`, with its status, in the acceptance
write-up. Each is either a modelling gap to fix or an expected exclusion to
note.

## 9. Not in this spec

- Void, cancellation, push, and dispute branches.
- Mask changes over a match (state transitions). They will be added to the
  replay tape in a later spec.
- Any Universe schema change or rebuild.
- Strategy implementations, including the P1 structural strategies.
- Cross-venue settlement or source compatibility. Two venues expressing one
  `claim_id` share a normal-resolution mask; whether their rules and void
  policies agree is a later concern.
