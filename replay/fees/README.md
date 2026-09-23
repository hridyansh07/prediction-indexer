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
from replay.fees.domain import AccountClass

catalog = load_catalog(catalog_directory)  # or Catalog.build(tuple_of_schedules)
fees = FeeEngine(
    Policy(
        limitless_buy_bps=300,
        limitless_sell_bps=150,
        kalshi_member_class=AccountClass.NON_DIRECT,
    ),
    resolver=Resolver(catalog, reference_time=reference_unix_ns),
)
assessment, next_state = fees.assess(fill, order_state=None)
assessments = fees.assess_many(ordered_fills)
```

Supply the reference timestamp explicitly once per run; there is no clock read,
network call, refresh, or per-fill catalog/reference override. The frozen resolver
selects by **reference time**, not fill time, and reuses selections across fills.
Original `fill.event_time` and `resolved.event_time` stay unchanged;
`resolved.reference_time`, `catalog_identity`, and `snapshot_identity` record
the separate pricing reference and frozen inputs. The snapshot identity is the
resolver identity (catalog plus reference); the engine identity also pins policy.
Changing reference or catalog changes identities even if the selected fee is equal.
Current-snapshot results explicitly carry `current_snapshot_not_historical_fee_claim`
and are estimates, not statements of fees actually applicable at execution time.

`FeeEngine`, `Resolver`, `Catalog`, and `Policy` are immutable. Neither assessment
method accepts a per-fill policy override. Set `AccountClass.DIRECT` once for the
.0001 Kalshi balance grid; the default `NON_DIRECT` uses .01. This is membership, not a
loyalty/volume tier. `UNKNOWN` is rejected in configuration. Limitless rates
are independently configurable integer basis points in 0–10000; booleans and
floats are rejected. Changing rates, membership, refund/rebate switches, or any
other policy field changes its identity and the assessment identity.

The engine always estimates a **no-builder route**. Optional
`Context.account_class` and `Context.builder` annotations are retained in the
original fill for provenance, but do not override configuration or add builder
charges—even when they conflict or are unknown. Callers need not supply either
field. Builder schedules may remain in a catalog but are not assessed.

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

Times are nonnegative UTC Unix nanoseconds. For a current snapshot, dated
schedules must contain the **reference** in their half-open effective interval,
not the historical fill. Future scheduled changes never activate early. A schedule
with all three of `effective_from`, `effective_to`, and `effective_evidence` set
to `None` is explicitly caller-asserted current evidence in this mode. This does
not infer a historical start from retrieval time or mutate the source schedule.
The caller must review that such evidence is current at the chosen reference;
retrieval timestamps are provenance, not an automatic as-known filter. Scope,
model, economics, fee asset/scale, sources, extractor/model versions remain
required, and same-priority current conflicts still fail visibly. No invented
effective history is needed. PM requires its explicit rate/exponent/rounding;
Kalshi requires its supported kind/multiplier (or event nulls inheriting the series
at reference time). Absence is never a zero PM/Kalshi rate.

The unbound low-level API remains `FeeEngine(policy).assess(fill, resolved,
order_state=None)` or `assess_many(fills, resolver, knowledge_cutoff=None)`.
An unpinned `Resolver(catalog)` still resolves historical effective intervals:
`None` cutoff means retrospective, while an integer cutoff filters retrieval time
and must not exceed fill time. Unknown-start evidence remains unknown there.
Constructor binding requires an explicit reference, and rejects resolved inputs,
resolver overrides, and cutoffs on assessment calls.

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

Default `Policy()` omits Kalshi rounding refunds and hypothetical account rebates,
explicitly assumes taker for unknown role, and configures non-direct Kalshi
membership plus Limitless BUY 300 / SELL 150 bps. Configured estimates are not
observations or established all-in bounds; evidence labels do not gate them.
`include_rounding_refunds=True` opts into Kalshi accumulator refunds only.
Independently, `include_account_rebates=True` enables explicit PM account
schedules; it never invents an account tier. Account rebate netting is model
accounting, not proof of a daily cash payment or eligibility. Daily thresholds,
account eligibility and payout timing must be established externally before
treating it as cash available.
Maker rewards, bonuses, gas, bridge, deposits/withdrawals, and FCM costs are
excluded.

## Venue models and remaining evidence gaps

* **Polymarket:** `C × rate × [p(1−p)]^exponent`. Applicable taker-only schedules
  establish maker platform zero; `ZeroFee` establishes fee-free, never absence.
  Default `PM_CEIL5_SCENARIO` rounds each declared fill upward and returns
  **Estimate**, not a whole-execution bound. Official five-decimal precision does
  not settle ties/fragmentation. `CEIL`/`EXACT` require a separately evidenced
  rounding claim in the pinned schedule. There is no `.07` fallback. Builder
  fees are excluded because this SDK estimates no-builder routes.
  Explicit effective account rebate schedules may use a fraction of the base
  platform charge, but only the same evidenced native asset—USDC is not pUSD.
* **Kalshi:** ordinary taker `.07MCP(1−P)`, enabled maker `.0175MCP(1−P)`.
  `quadratic` is known zero maker platform fee but still applies balance
  rounding. Flat/combo variants return Unknown. Model fee is ceiled to six
  decimals. Signed revenue minus fee is floored to .0001 direct or .01
  non-direct grid. Rounding is its remainder. Revenue must be exactly supported
  at six decimals. With rounding refunds enabled, accumulated rounding is refunded
  in whole grid units, capped by this fill's trade plus rounding fee; carry can
  remain ≥ grid. Conservative omission is a **ConditionalBound** only for the
  pinned supported model, known role, configured membership, and declared fill
  partition; it is not an execution-fragmentation or FCM-cost bound.
* **Limitless CLOB:** Configured BUY `ceil6(quantity × buy_bps / 10000)` in the
  pinned outcome token and SELL `ceil6(notional × sell_bps / 10000)` in collateral
  are **Estimates**. Defaults are 300 and 150 bps respectively. No public tier
  inference or table interpolation. **No public curve or schedule is required
  for takers:** an empty catalog plus supplied CLOB instrument economics works.
  A rounded fee greater than the received contracts (BUY) or collateral (SELL)
  raises `ValueError`; fees are never clamped. Equality is valid and leaves zero
  received balance. A configured zero rate remains zero even for tiny fills.
  If supplied, one schedule serves both sides: `fee_asset` pins collateral and
  economics pins the BUY token. Conflicting, malformed, wrong-asset, unsupported,
  or out-of-interval matching evidence is not replaced by configured defaults.
  Known applicable maker-free schedules yield zero; missing maker evidence stays
  unknown. AMM supports `.004` only with an explicit applicable ordinary schedule,
  quantity or quote-notional basis, fee asset, and rounding. Unknown basis stays
  Unknown; unknown historical applicability is accepted only through the explicit
  current-snapshot assertion described above. Promotions are not inferred.

Minimal standalone Limitless example (synthetic instrument, no public schedule):

```python
from replay.fees import Catalog, FeeEngine, HypotheticalFill, Resolver
from replay.fees.domain import (
    Asset, AssetAmount, AssetKind, Context, Fixed, InstrumentEconomics,
    Notional, Probability, Product, Quantity, QuotePrice, Role, Side, Venue,
)

quote = Asset(AssetKind.USDC, "example-chain", "collateral")
outcome = Asset(AssetKind.OUTCOME, "example-chain", "market-yes")
economics = InstrumentEconomics(
    quote, outcome, AssetAmount(quote, Fixed(1, 0)), 6, 6, 6,
)
context = Context(
    Venue.LIMITLESS, Product.CLOB, "market", None, None, None,
    "market-yes", "yes", "account", "subaccount",
)
fill = HypotheticalFill(
    "fill-1", context, economics, Side.BUY, QuotePrice.parse("0.4"),
    Probability.parse("0.4"), Quantity.parse("100"),
    Notional(quote, Fixed(40, 0)), 1_700_000_000_000_000_000,
    Role.TAKER, "order-1", 0, True,
)
fees = FeeEngine(resolver=Resolver(
    Catalog.build(()), reference_time=1_790_000_000_000_000_000,
))
result, state = fees.assess(fill)
assert result.charges[0].amount == AssetAmount(outcome, Fixed(3_000_000, 6))
assert result.net_deltas is not None  # -40 collateral, +97 contracts
```

Kalshi `OrderState` binds account, subaccount, order, venue/product, market,
instrument, outcome orientation/token, BUY/SELL side, native quote asset, grid,
last fill index/time, policy and `counterfactual_revision`. Supply a distinct,
nonempty revision ID when reusing order keys for independent hypothetical runs;
omitting it puts fills in one shared revision. Each revision/order must explicitly
start at fill index zero. Missing, duplicate, out-of-order, mismatched or uncertain
fills cannot exact-continue. State survives taker→maker transitions and compatible
schedule changes, but never crosses BUY/SELL or revision boundaries. A missing
schedule or invalid continuation invalidates carry in `assess_many`; unknown role
cannot produce new exact state. Stateless assessments require no carry.
No actual order ledger is persisted. Callers of individual
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
explicit supersession resolves them. In unpinned historical resolution, unknown
applicability at equal/higher priority blocks a broad default. In current snapshot
mode, unknown-start inputs participate as current asserted evidence. Kalshi event
null fields independently clear to the **current** series model/multiplier; the resolved selection retains both
source schedules. Builder never replaces platform. `next_boundary` exposes the
next matching effective boundary after the selection time (reference time for
snapshots); crossing it in fill time never refreshes a snapshot. There is no
mutable latest pointer or network resolver. A bounded 1,024-entry selection cache
keys immutable resolver/context/selection-time inputs; eviction only recomputes
the identical selection. Static type metadata also has a bounded cache.

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
