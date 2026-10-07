# Analysis

Pure, offline libraries that define what a prediction market *means* relative to
an event: the set of terminal outcomes the event can end in, the subset of those
outcomes a market resolves YES on, and the claim and relation algebra over those
subsets. They are used by Targeter v2 (`targeter/v2/relationships.py`,
`selection.py`), Universe (`universe/claim_projection.py`, `universe/outcomes.py`)
and Replay. Nothing here touches capture evidence or filters at capture time.

## Modules

| Module | Role |
|---|---|
| `outcome_space.py` | Terminal outcome spaces (Omega): `Outcome`, `OutcomeSpace`, `build_score_space`, `build_series_space`, `series_sequences`, `parse_best_of`, `parse_score_ticker`, `reachable_keys`, `build_state_timeline`, `settled_outcome_key` |
| `masks.py` | Compile a normalized market to a `Mask` over a space; classify mask pairs; partition check; state-conditioned relationships |
| `claims.py` | Claim identity (`claim_id`, `space_shape_id`), `Claim`, `ClaimRelation`, `derive_claims`, `derive_claim_algebra`, `implied_market_relations` |
| `storage.py` | Durable file helpers: atomic JSON/NDJSON writes with fsync of file and directory, one-frame Zstandard JSON/NDJSON via the shared `encoder`, `sha256_text` |
| `durable_http.py` | `DurableJsonClient` / `RetryingJsonClient`: content-addressed response cache, cross-process per-host rate limiting, bounded retries |

Tests: `tests/test_outcome_space.py`, `tests/test_masks.py`, `tests/test_claims.py`,
`tests/test_storage.py`, `tests/test_durable_http.py`, `tests/test_retrying_client.py`.

## Outcome spaces

An outcome space is derived from the competition *format*, never from the markets
a venue happened to list. Each `OutcomeSpace` has an `event_key`, a `scope`, a
tuple of `Outcome(key, payload)`, a `coverage`, and metadata.

- Scopes: `regulation_fulltime`, `first_half`, `series`. One event can carry
  several; markets attach to the one they are a function of and spaces are never
  mixed.
- `build_series_space(best_of, home, away)`: every reachable map-winner sequence
  until a side clinches (`seq:HHA` ...). Coverage `EXHAUSTIVE`, because the
  format fixes it. `best_of` must be a positive odd number (`parse_best_of`
  reads `BO<n>` from text). A BO5 space has 20 keys regardless of the teams.
- `build_score_space(score_tickers)`: assembled from a listed correct-score
  ladder (`score:<home>-<away>`). Scorelines outside the listed grid are not
  members of Omega, so coverage is `INCOMPLETE_COVERAGE` and the space cannot
  support a claim that a basket is locked.
- Outcome keys are participant-independent. That is what lets a claim be
  identified globally by its key set.
- `reachable_keys(space, prefix)` restricts a series space to the outcomes still
  reachable after a map-result prefix; transitions only remove outcomes, which is
  why identities appear mid-series that do not hold pre-match.
  `build_state_timeline` rebuilds that prefix history from settled map results.

## Masks

`compile_mask(market, space)` returns a `Mask` (market key, venue, market type,
scope, status, `outcome_keys`, `resolver`, note). Status is one of:

- `DERIVABLE`: resolves YES on a computable subset of this space (moneyline,
  totals, spread, map winner, map handicap, correct score, BTTS, team totals).
- `NOT_A_FUNCTION`: depends on state the space does not record (first team to
  score, method of victory, advance, extra-time score, corners).
- `DIFFERENT_SCOPE`: belongs to another space (for example a first-half market
  against the full-time space).
- `UNSUPPORTED`: the market type is not in `SCOPE_BY_TYPE`, or no resolver
  matched its labels. Unknown shapes are never forced into the nearest space.

Only `DERIVABLE` masks participate in relationships. `Mask.resolver` is a label,
not an identity: `maps_over` at 2.5 and at 3.5 share it but denote different
subsets.

`relationship(left, right, universe=None)` is a pure function of the two key
sets (optionally intersected with a reachable universe), `None` unless both are
derivable, share a scope and are non-empty:

| Condition | Result |
|---|---|
| `a == b` | `IDENTITY` |
| `a` strictly inside `b` | `IMPLICATION` (left implies right) |
| `b` strictly inside `a` | `REVERSE_IMPLICATION` |
| disjoint | `MUTUAL_EXCLUSION` |
| otherwise | `OVERLAP` |

`OVERLAP` is the catch-all branch, not a finding. `derive_relationships` lists
all pairs; `is_partition` reports whether masks tile the space exactly once (with
the space's coverage, so a partition over an incomplete space stays conditional);
`state_conditioned_relationships` recomputes pairs at each series prefix.

## Claims

A claim is the outcome subset a market resolves YES on, within a space shape. Two
markets naming the same subset are the same claim however their venues word it.

- `space_shape_id(space)`: digest of scope and key vocabulary (so any future
  space builder works without a per-sport table).
- `claim_id(keys, shape)`: digest of `CLAIM_IDENTITY_VERSION` (2), shape and
  sorted keys. The shape is part of the identity because a score space is capped
  from the listed lines, so one subset can occur under two shapes with different
  meanings.
- `usable_masks` keeps derivable masks that are neither empty nor the whole space
  (a tautology relates to everything). `derive_claims` groups them by claim id in
  id order, with members sorted by `(venue, market_key)`; `Claim.cross_venue` is
  true when members span venues.
- `derive_claim_algebra` relates every pair of claims of one shape (raising if
  shapes mix). Stored types are `IMPLICATION` (antecedent first; reverse
  implication is swapped) and `MUTUAL_EXCLUSION`; `OVERLAP` is dropped by default
  and `IDENTITY` never appears because equal subsets are one claim. Versioned by
  `CLAIM_ALGEBRA_VERSION` (1).
- `implied_market_relations` rebuilds cross-venue market-pair relations from
  claims alone. Universe compares them with the Targeter report at ingestion: a
  relation the claims invent is a guessed equivalence and rejects the run; one
  they miss is counted as a visible false negative.

A finding over an `INCOMPLETE_COVERAGE` space is conditional discovery evidence,
never an unconditional arbitrage or execution claim. Ordinary resolution is the
only semantics modelled here; resolution-failure branches (void, postponement,
50-50) are not part of the mask algebra.

## HTTP and storage helpers

`DurableJsonClient.get_json(base_url, path, params=, headers=)` caches each
response under `<cache_root>/<host>/<sha256(url)>.json` (or `.json.zst` with
`compress_responses`) with a `.meta.json` carrying fetch time and, for
compressed entries, the decoded and stored identities. A compressed body that
fails identity verification counts as a cache miss, not a failure.
`force_refresh` bypasses reads; `persist_responses=False` disables the cache.
Requests to one host are serialized across processes with a file lock and a
minimum interval (default 0.25 s for the Kalshi and Polymarket hosts).
`RetryingJsonClient` retries transport errors, HTTP 429 and 5xx with exponential
backoff (default 5 retries, 5 s base, 30 s cap); other 4xx errors are not
retried.
