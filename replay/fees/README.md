# Offline Fee SDK v1

`replay.fees` evaluates **consumer-supplied hypothetical fills**, not books,
orders, execution probability, or strategy profitability. Resolution and
assessment are deterministic and network-free. No Rust bindings, Redis,
normalizers, Targeter, strategy, capture, or legacy economics imports are used.
The existing `replay*` packaging rule includes this package; no dependencies are
added. Old replay gates and research fee calculations are unchanged.

## API and ownership

```python
from replay.fees import Catalog, FeeEngine, Policy, Resolver
from replay.fees.artifacts import load_catalog

catalog = load_catalog(catalog_directory)  # or Catalog.build(tuple_of_schedules)
resolver = Resolver(catalog)
resolved = resolver.resolve(fill.context, fill.event_time, knowledge_cutoff=None)
assessment, next_state = FeeEngine().assess(fill, resolved, Policy(), order_state=None)
assessments = FeeEngine().assess_many(ordered_fills, resolver, Policy())
```

Construct input types from `replay.fees.domain` and schedule types from
`replay.fees.schedules`. All domain dataclasses are frozen, slotted, and validate
exact runtime types; tuples, not mutable dictionaries/lists, hold persisted
collections. `canonical(value)` emits deterministic tagged JSON bytes and every
domain object's `identity` is a domain-separated SHA-256 of those bytes. The
assessment includes the entire immutable fill, resolution, policy, prior state,
and engine version, pinning source/schedule/model/catalog/input identities.

`HypotheticalFill` requires IDs/context, quote price, probability, strictly
positive quantity, gross notional, event time, side, liquidity role, order key,
fill index, explicit new-order declaration, economics, and optional opaque
decision reference. Orientation is a consumer-owned string (YES/NO/indexed
outcome); it is not BUY/SELL or bid/ask. No role is inferred from an order type.
`price × quantity != gross_notional` raises `ValueError`, as do scale and asset
mismatches. These are invalid inputs, not missing schedules.

Times are nonnegative UTC Unix nanoseconds. `knowledge_cutoff=None` is explicitly
**retrospective**. An integer cutoff means **as known**, filters by latest source
retrieval time, and must not exceed event time. Effective claims still require
their own evidence; retrieval time alone never supplies historical applicability.

`Fixed.parse("0.07")`, `Rate.parse(...)`, `Quantity.parse(...)`, etc. accept only
bounded unsigned decimal strings. Values use integer atoms and scale 0–18;
integer magnitude is limited to 10³⁶. Exponent notation and floats are rejected,
including booleans where an integer is required. Arithmetic uses `Fraction`
intermediates, never floats or ambient Decimal precision. PM exponents are
integers 0–16. Rescaling is exact unless an explicitly selected ceiling is used.

Assets include kind, chain/ledger and token identity. USD, USDC, pUSD and each
outcome token remain distinct. `InstrumentEconomics` pins quote asset, outcome,
payout asset/amount and quote/quantity/price scales. PM/Kalshi curves support
only unit payouts in the quote asset with probability equal to quote price.
They do not extrapolate arbitrary contract payouts.

## Labels and native balances

Basis (`gross`, `base_taker`, `maker`, `account_adjusted`) is independent of
evidence (`actual_observed`, `exact_model`, `estimate`,
`conditional_conservative_bound`, `unknown`). **ExactModel is not actual.**

Charges and rebates are separate nonnegative, component-tagged asset amounts.
`net_deltas` contains signed balances by native asset; there is no scalar sum of
unlike assets. BUY contract fees reduce received contracts.
`contract_fee_payout_impact` uses pinned payout per contract, not execution price.
For BUY 100 @ .40 with a three-contract fee, balances are −40 collateral and
+97 contracts; payout impact is three unit payouts, not 1.20 collateral.

Any unknown required component preserves known charges but sets **all net
deltas and payout impact to `None`**. `UnknownSchedule` retains a component,
typed reason, and conflicting candidate identities when available. Exclusions
and assumptions remain on the result. Consumers own admission/headline policy.

Default `Policy()` excludes hypothetical rebates, explicitly assumes taker for
unknown role, and does not assume Kalshi membership. Optional non-direct
membership and Limitless maximum-rate scenarios are labelled assumptions, not
established all-in bounds. `include_rebates=True` opts into complete Kalshi
accumulator accounting and explicit PM account schedules; it never invents an
account tier. Account rebate netting is model accounting, not proof of a daily
cash payment or eligibility. Daily thresholds, account eligibility and payout
timing must be established externally before treating it as cash available.
Maker rewards, bonuses, gas, bridge, deposits/withdrawals, and FCM costs are
excluded.

## Venue models and remaining evidence gaps

* **Polymarket:** `C × rate × [p(1−p)]^exponent`. Applicable taker-only schedules
  establish maker platform zero; `ZeroFee` establishes fee-free, never absence.
  Default `PM_CEIL5_SCENARIO` rounds each declared fill upward and returns
  **Estimate**, not a whole-execution bound. Official five-decimal precision does
  not settle ties/fragmentation. `CEIL`/`EXACT` require a separately evidenced
  rounding claim in the pinned schedule. There is no `.07` fallback. Builder
  rates are additive `notional × bps / 10000`, with integer caps 100 taker/50
  maker; builder asset/rounding must be pinned. Unknown builder is not absence.
  Explicit effective account rebate schedules may use a fraction of the base
  platform charge, but only the same evidenced native asset—USDC is not pUSD.
* **Kalshi:** ordinary taker `.07MCP(1−P)`, enabled maker `.0175MCP(1−P)`.
  `quadratic` is known zero maker platform fee but still applies balance
  rounding. Flat/combo variants return Unknown. Model fee is ceiled to six
  decimals. Signed revenue minus fee is floored to .0001 direct or .01
  non-direct grid. Rounding is its remainder. Revenue must be exactly supported
  at six decimals. With rebates enabled, accumulated rounding is refunded in
  whole grid units, capped by this fill's trade plus rounding fee; carry can
  remain ≥ grid. Conservative omission is a **ConditionalBound** only for the
  pinned supported model, known role/account, and declared fill partition; it
  is not an execution-fragmentation or FCM-cost bound.
* **Limitless CLOB:** Unknown for hypothetical takers by default. No table
  interpolation. Optional BUY `ceil6(quantity × .03)` in the pinned outcome
  token, SELL `ceil6(notional × .015)` in collateral are **Estimates**. One
  schedule serves both sides: `fee_asset` pins collateral and economics pins
  the BUY token. Known applicable maker-free schedules yield zero. AMM supports
  `.004` only with explicit contemporaneous ordinary schedule, quantity or
  quote-notional basis, fee asset, and rounding. Unknown basis/history remains
  Unknown; promotions are not inferred.

Kalshi `OrderState` binds account, subaccount, order, native asset, grid, last
fill index/time and policy. Only an explicit new order at index zero seeds zero
carry. Missing, duplicate, out-of-order, mismatched or uncertain fills cannot
exact-continue. State survives taker→maker transitions. A missing schedule
invalidates carry in `assess_many`; unknown role/membership cannot produce new
exact state. No actual order ledger is persisted. Callers of individual
`assess` calls must pass every intervening fill and replace state with the
returned value, including `None`; retaining an old state is caller misuse.

Observed executions use a different API and type:

```python
from replay.fees import ObservedExecution, ObservedTotal, validate_actual

actual = validate_actual(ObservedExecution(execution_id, source_identity, totals))
```

Each `ObservedTotal` holds gross/fee/net **received-asset** amounts and must
satisfy `gross − fee == net` with identical assets. Validation checks internal
consistency, not source authenticity or settlement finality. Observed values
are never accepted as hypothetical fills or substituted into model results.

## Catalog resolution and local artifact contract

`Schedule` is a common evidence header plus a closed venue/component model.
Every schedule pins raw source URL/SHA-256/length/retrieval time, effective
`[from,to)` or explicit unknown, effective evidence reference, scope, economics,
fee asset/scale, extractor/model versions and supersession identities.
`effective_to=None` means open-ended, not a guarantee against later correction.
Instrument/orientation/account/builder discriminators must also match; set
instrument scope when a catalog contains multiple outcomes in the same market.

Versioned precedence is market > event > series > category > venue/product,
independently per component. Equal-priority conflicts fail closed unless an
explicit supersession resolves them. Unknown applicability at equal/higher
priority blocks a broad default. Kalshi event null fields independently clear
to the **current** series model/multiplier; the resolved selection retains both
source schedules. Builder never replaces platform. `next_boundary` exposes the
next matching effective boundary visible under the chosen knowledge cutoff.
There is no mutable latest pointer, network resolver, or schedule-result cache.
Only bounded static type metadata is cached.

To import retained public evidence, read source bytes yourself, call
`source_from_bytes(url, retrieved_at, data)`, construct reviewed `Schedule`
values, then:

```python
from replay.fees.artifacts import build_catalog, load_catalog

catalog = Catalog.build(schedules)
path = build_catalog(root, catalog, {source.sha256: source_bytes})
verified = load_catalog(path)
```

No production catalog or invented historical effective dates ship with the SDK.
Extraction/effective-time claims are reviewed inputs: a valid hash proves
integrity, not that a source text actually supports the parsed claim.

The directory is named by catalog identity and contains `source-<sha>.blob`,
`schedule-<identity>.json`, canonical `manifest.json`, and **receipt-last**
`receipt.json` (`fee_catalog_receipt_version=1`, catalog identity). Each final
name is published from an fsynced unique temporary file using a no-replace hard
link and directory fsync. Existing different bytes fail closed. The reader
requires the exact receipt, canonical closed JSON, every separately stored
schedule, and freshly hashed source bytes. It re-establishes directory
durability before returning. A pre-receipt crash is uncommitted and retryable.
These files never authorize archival deletion. The root must be a trusted local
directory, not one concurrently writable by an adversary.

Limits: 16 MiB per artifact/source, 10,000 schedules, 32 sources per schedule,
32 decoded nesting levels. No source/catalog I/O occurs in assessment. The
writer follows `archive/common/durable.py`/local-store fsync and no-overwrite
discipline without importing `archive.__init__` and its capture adapters.

## Authoritative documents rechecked for this implementation

* [PM fees](https://docs.polymarket.com/trading/fees),
  [market fee metadata](https://docs.polymarket.com/market-data/market-details),
  [builder fees](https://docs.polymarket.com/programs/builders/fees),
  [taker rebates](https://docs.polymarket.com/programs/taker-rebates).
* [Kalshi fee PDF](https://kalshi.com/docs/kalshi-fee-schedule.pdf),
  [rounding](https://docs.kalshi.com/getting_started/fee_rounding),
  [series](https://docs.kalshi.com/api-reference/market/get-series),
  [event overrides](https://docs.kalshi.com/api-reference/events/get-event-fee-changes).
* [Limitless fees](https://docs.limitless.exchange/user-guide/fees),
  [execution totals](https://docs.limitless.exchange/api-reference/trading/create-order).

These links explain implementation choices; they are not a pinned rate catalog.
Tests use explicitly synthetic contract shapes, not retained live snapshots.

```bash
.venv/bin/python -m unittest tests.test_fee_sdk
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m unittest discover -s replay/tests
```
