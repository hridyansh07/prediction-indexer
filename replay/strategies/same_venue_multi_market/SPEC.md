# Same-venue multi-market complete sets V1

Status: **implemented.** Synthetic contract tests cover enumeration, admission,
pricing, fills and the reader; one retained fixture run is recorded in §7.

This is the first structural strategy from
[ECONOMIC_STRATEGY_RESEARCH_V2.md](../../../docs/ECONOMIC_STRATEGY_RESEARCH_V2.md) §1. Several
separately traded markets on one venue can jointly pay a fixed amount: on a
best-of-3, "Alpha wins 2–0", "Alpha wins 2–1" and "Beta wins" partition the
series. A matching engine enforces the relationship inside one market, not
across markets, so the combined asks can sum below one unit.

## 1. Baskets

A basket is a set of two to `max_legs` (at most 4) books that:

- are all on one venue;
- come from at least two different markets (sets inside one market are the
  same-instrument complement, [SAME_VENUE_COMPLEMENT_V1.md](../same_venue_complement/SPEC.md));
- have `MASKED` normal-resolution masks that partition one `EXHAUSTIVE` space
  ([OUTCOME_MASKS_V1.md](../../../docs/OUTCOME_MASKS_V1.md) §5, `OutcomeScope.complete_sets`).

Every leg is bought. A Kalshi leg buys its orientation from the opposite
orientation's bids, projected to asks at `P − p`, exactly as the cross-venue
strategy does; the mask stays on the acquired orientation. No name, price or
label establishes a relationship.

Enumeration is per scope and venue, in sorted book order, and capped at 4,096
sets per venue; more fails the run instead of truncating.

**Visible exclusions.** Each scope also emits:

| Row | Admission | Reason `kind` |
|---|---|---|
| An uncaptured member | `NOT_CAPTURED` | `not_captured` |
| A book that is not `MASKED` | `UNSUPPORTED_SHAPE` | its mask status, e.g. `VOID_UNSUPPORTED` |
| A book whose ask source is not planned | `UNSUPPORTED_SHAPE` | `ask_source_not_planned` |
| A venue with captured books but no multi-market set | `UNSUPPORTED_SHAPE` | `no_multi_market_set` |
| A venue when outcomes are unavailable | `UNSUPPORTED_SHAPE` | `outcomes_unavailable` |

Rejected rows carry `size_contracts: null` and their scoped time, like the
cross-venue strategy's.

## 2. Economics

All legs share the venue's quote asset, which is also the payout asset, so no
valuation scenario is needed; a set whose legs bind different quote assets is
`ECONOMICS_UNKNOWN`. Amounts are exact signed integers at scale 36.

- **Gross** for `N` sets is `N` units minus the native cost of every leg.
- **Net** uses one exact Fee SDK single-order assessment per leg
  (`cross_venue_contract.assess_net`, shared with the cross-venue strategy): each
  leg's collateral delta, plus the minimum received outcome quantity across the
  legs. Exactly one leg pays in every outcome, so that minimum is the guaranteed
  payout; token fees reduce it, collateral fees reduce cash once.
- Missing schedules give `FEE_UNKNOWN` on gross-positive time. Missing economics
  give `ECONOMICS_UNKNOWN`. Nothing is rounded or guessed.

No merge, redemption or netting of the purchased claims is assumed: the payout
arrives at normal resolution.

## 3. Trigger and fills

The strategy uses SDK fill checks only
([ECONOMIC_STRATEGY_SDK_V1.md](../../../docs/ECONOMIC_STRATEGY_SDK_V1.md) §13).

- **Trigger:** one complete set at the best asks, with exact fees, has a net
  that is positive and at least the configured minimum (the `net` predicate).
  `gross` episodes stay quote-slice episodes.
- **Fill:** when the trigger turns on, the SDK walks every leg's full ask ladder
  once and prices the declared sizings. The template uses one governing `edge`
  sizing in `0.01`-contract steps; a fixed amount is a governing
  `target_contracts` sizing. `fill_value` is the exact net of the walked sets less
  the minimum, with the trigger's fee assessment at the fill's open time.
- **End:** per-leg kill prices, the trigger turning off, or an unusable book;
  `end_books` records each leg's book at the end.

Controls are not supported with fill checks.

## 4. Configuration

The closed run config is `{version: 1, snapshot_directory, snapshot_sha256,
fees, policy}`. `fees` is the shared complement fee configuration. The closed
policy (version 1) is:

| Field | Meaning |
|---|---|
| `fills` | The SDK's closed fill policy, triggered by `net`. |
| `max_legs` | 2 to 4. |
| `latency_tiers_ns`, `headline_latency_ns` | Q(L) tiers, as in complement policy 2. |
| `minimum_net_gap_per_contract_e18` | Minimum net per set, scale 18. |
| `leg_skew_buckets_ns` | Leg skew buckets. |
| `audit_intervals` | The SDK's opt-in interval audit. |
| `profile` | `null` or a market-profile policy. |
| `game` (optional) | The closed prepared-input policy in [GAME_STATE_SDK_V1.md](../../../docs/specs/GAME_STATE_SDK_V1.md). |

With `game`, layout-2 manifests require its hash/windows/mode binding and episodes
require closed `open.game` and `at_max.game` annotations, checked by the independent
reader. Input bytes and release decisions enter experiment identity. Without it,
the original closed output schemas and identities are unchanged.

`replay/strategies/same_venue_multi_market/config.example.json` is a template with
placeholder inputs. The strategy opts into native scales: each leg stays in its
book's own units, and every sum goes through scale 36 first.

## 5. Output and reading

Output layout 2 with closed manifests (format version 1) and payloads. Each
positive payload carries `gap_gross`, `gap_net`, the gross and net payout
floors, one native leg per book (outcome asset, cost, quote delta, received,
charges, assessment identities), `quote_asset`, fee status and labels,
`settlement_model: "normal_resolution_only"` and the outcomes provider.

The independent reader (`replay.strategies.same_venue_multi_market.output`) reconstructs
entities, re-walks consumed quotes, checks fee, cashflow and payout arithmetic,
recomputes episodes, slices, denominators and Q(L), and re-prices every fill
through the fee engine, so it needs the configured fee bridge:
`validate_content`, `read_provisional` and `read_completed` fail without it.
Summary rows key on `(venue, basket_kind, direction, set_id, size_contracts)`
and add `fill_ns` and `fill_ends`.

## 6. Not in V1

- Short legs, conversion, merge or redemption routes.
- Void, cancellation and push branches; mask changes during a match.
- Settlement or rule compatibility between the venue's markets beyond the
  normal-resolution masks.
- Depth shared between sets: fills measure apparent edge, so two sets that
  share a book each see its whole ladder.
- Cross-venue sets (the cross-venue strategy covers two-leg ones).

## 7. Retained fixture (2026-10-07)

One `python -m replay.bench` run of the re-prepared Procyon context (CS2 Bo3,
14 scopes) with current published fees, Kalshi direct member class, and the
template policy (governing `edge`, `0.01` steps). It succeeded in 54 s; the
completed reader re-priced every fill, and bench cleanup reported no errors.

- **Sets.** Each scope admits four Kalshi sets: YES on both teams' series
  markets, NO on both, and the same two pairs on the map-2 winner markets.
  Polymarket's only captured market is the series, so it has a visible
  `no_multi_market_set` row; one uncaptured member has a `NOT_CAPTURED` row.
- **Opportunity.** The series YES pair was gross-positive for 29.2 s of 3,772 s
  evaluated and net-positive for 0.02 s, in three fills of 2–14 ms: 7, 20 and
  150.59 sets, worth $0.11, $0.29 and $2.01 net. Each ended when the 1-set
  trigger turned off. The other three sets were never net-positive.
- **Independent check** against the published fee formulas, not the fee SDK:
  all 29 opening payloads (1-set gross and net), all 3 fill values, all 6 kill
  prices, and all 3 edge stops' next steps (from `beyond`) agree.
