# Same-venue complement strategy V1

Status: **implemented** on the economic strategy SDK
([ECONOMIC_STRATEGY_SDK_V1.md](../../../docs/ECONOMIC_STRATEGY_SDK_V1.md)), as two policy versions.
Deployment is separately authorized.

- **Policy 1** is the frozen V1 experiment described by this document's output
  rules (§9, layout 1).
- **Policy 2** is the SDK default. Where it differs, this document says so:
  - its policy fields (§1);
  - skew (§5);
  - controls (§8);
  - output (§9).

`replay.strategies.same_venue_complement:build` is the first economic Replay strategy. On every
captured binary instrument it measures whether buying, or selling, **both sides of
the same instrument on the same venue** was ever priced through one unit of payout
at a deployable size, and for how long. It is also the integration harness for the
shared economic SDK pieces in §3. Later strategies (several markets on one venue,
cross-venue complete sets, the payout-cover LP) reuse those pieces unchanged.

Expected result: on Kalshi and Polymarket, close to zero. Both venues' matching
mechanics pin the two sides of one instrument together (§4). A frequent positive
gap is therefore first a **data-quality finding** about our normalizers, books, or
clocks, and only second a market finding. The strategy is still a full economic
pipeline: VWAP at size, conservative fees, episode lifetimes, latency-qualified
time, and a placebo. A clean zero here is what lets the later strategies' positives
be trusted.

Out of scope for V1:

- Universe claims and outcome masks; more than one market per basket; cross-venue
  legs.
- Limitless basket economics, fees, and verdicts (§4.3).
- Gas, merge/split, and deposit costs.
- Capital lockup cost, excluded **by decision**. Captured markets run for hours,
  not days, and Kalshi pairs and Polymarket complete sets settle immediately on
  netting or merge anyway.
- Maker or queue-position fills; live orders.
- Any normalizer, stream-format, or Universe endpoint change. None is required.

Governing documents:

- [STRATEGY_PREPARATION_V1.md](../../../docs/STRATEGY_PREPARATION_V1.md): snapshot, members, books.
- [REPLAY_STREAMS_V1.md](../../../docs/REPLAY_STREAMS_V1.md): cuts, in-place books, the zero-copy
  hook contract.
- [REPLAY_SUPERVISOR_V1.md](../../../docs/REPLAY_SUPERVISOR_V1.md): factory, output ownership,
  `SUCCESS`.
- [`replay/fees/README.md`](../../fees/README.md): the fee SDK.
- [`replay/README.md`](../../README.md): ordered gates 2–5.
- [BUNDLE_COVERAGE_V1.md](../bundle_coverage/SPEC.md): time, scope, and output
  conventions, which this document follows unless it states otherwise.

## 1. Factory and closed configuration

Supervisor entry:

```json
{
  "factory": "replay.strategies.same_venue_complement:build",
  "revision": "<immutable installed code revision>",
  "config": {
    "version": 1,
    "snapshot_directory": "/absolute/path/to/prepared-context",
    "snapshot_sha256": "<64 lowercase hex>",
    "fees": {
      "catalog_directory": "/absolute/path/to/fee-catalog",
      "catalog_identity": "<catalog identity>",
      "reference_ns": "1790000000000000000",
      "limitless_buy_bps": 300,
      "limitless_sell_bps": 150,
      "kalshi_member_class": "NON_DIRECT",
      "assets": {
        "kalshi": {"kind": "USD", "ledger": "<ledger>", "token": "<token>"},
        "polymarket": {"kind": "<USDC|PUSD>", "ledger": "<chain>", "token": "<collateral>"}
      },
      "instrument_bindings": [
        {"instrument": "kalshi:<ticker>", "orientation": "complement", "economics": {}},
        {"instrument": "kalshi:<ticker>", "orientation": "outcome", "economics": {}}
      ]
    },
    "policy": {
      "version": 1,
      "sizes_contracts": ["1", "10", "25", "50", "100", "250", "500", "1000"],
      "headline_size_contracts": "100",
      "latency_tiers_ns": ["250000000", "1000000000", "5000000000"],
      "headline_latency_ns": "1000000000",
      "minimum_net_gap_per_contract_e18": "0",
      "leg_skew_buckets_ns": ["100000000", "1000000000", "5000000000", "15000000000", "60000000000"],
      "verdict": {"maximum_positive_time_fraction_ppm": "0", "minimum_evaluated_ns": "3600000000000"}
    }
  }
}
```

**Policy version 2** adds these closed fields to the policy above (SDK spec §11):

- `controls`, default `[]`;
- `controls_episodes` and `controls_slices`, both default `false`;
- `audit_intervals`, default `false`;
- `time_shift_ring_entries`;
- `profile`, which is `null` or a market-profile policy.

**Schema and loading**

- The schema is closed. Unknown or duplicate fields fail.
- Integers are canonical unsigned decimal strings (the Replay convention). The
  exceptions are `version` and the bps fields, which are JSON integers as in the
  existing SDK and fee configurations.
- `snapshot_*` is loaded through `PreparedInput` exactly as in bundle coverage,
  including `bind(initial)` equality.

**Fees**

- `fees` builds one immutable `FeeEngine(Policy(...), resolver=Resolver(catalog,
  reference_time=reference_ns))`.
- `load_catalog(catalog_directory)` must return exactly `catalog_identity`.
- `include_account_rebates` and `include_rounding_refunds` are fixed to `false`.
  §6 relies on every charge being nonnegative.
- Quote assets are explicit per venue and never inferred.
- `instrument_bindings` is a list sorted by `(instrument, orientation)` and unique
  on that key. Each entry supplies a complete SDK `InstrumentEconomics`, in the
  SDK's canonical tagged representation. It pins:
  - outcome asset and quote asset;
  - quote, price, and quantity scales;
  - unit payout.

  Token or ledger identity is never invented from a book name. Before any
  callback, each binding is validated against its plan's scales, the venue's
  configured quote asset, and a unit payout.
  - Malformed or conflicting bindings fail configuration.
  - A missing binding, or a venue with no `assets` entry, is allowed. It yields
    `fee_status: "UNKNOWN"` on positive-gross observations (§6), never zero.
  - An empty list is valid for a gross-only audit.
  - Bindings are reviewed together with the fee catalog.

**Policy validation**

- `policy` is the pre-registration of gate 5.
- `sizes_contracts` holds 1–16 positive whole numbers. `latency_tiers_ns` holds
  1–8 positive values. `leg_skew_buckets_ns` holds 1–16 values. Each list is
  numerically sorted and unique.
- Each `headline_*` value must be a member of its list.
- `maximum_positive_time_fraction_ppm` lies in 0–1,000,000, and
  `minimum_evaluated_ns` is positive.
- `minimum_net_gap_per_contract_e18` is a per-contract threshold in quote units at
  scale 18.

**Identities**, all computed in the factory before the first callback using
canonical JSON (sorted keys, compact, UTF-8, the preparation encoding):

- `policy_sha256` covers `policy` alone. It is written into the manifest and every
  summary row.
- `experiment_sha256` covers the full semantic experiment:
  - `policy`;
  - the strategy and fee-bridge versions;
  - the fee catalog identity, reference, policy fields, assets, and bindings;
  - the snapshot identity.

  It excludes filesystem paths and attempt IDs. `policy_sha256` alone does not
  identify fee assumptions or code. Results with different experiment hashes are
  different experiments.

The factory also fails before any callback if the output directory is not empty
or the configuration exceeds 8 MiB.

## 2. Instruments and baskets

Baskets come from the prepared snapshot's scope `members` and their `books`. They
are **not** read from Universe, and no outcome mask or claim is consulted. The
complement relation used here is a venue-structural fact about one binary
instrument:

| Venue | Member books (preparation mapping) | Basket kind |
|---|---|---|
| Polymarket | exactly two `polymarket:<token>` / `outcome` | `PM_TOKEN_PAIR`: the two outcome tokens of one condition |
| Kalshi | `kalshi:<ticker>` `outcome` + `complement` | `KALSHI_YES_NO`: YES and NO of one ticker |
| Limitless | one `limitless:<slug>` / `outcome` | `LIMITLESS_SELF_CROSS`: crossed-book diagnostic only (§4.3) |

**Admission precedence**, applied per scope and recorded visibly. The denominator
never silently shrinks:

1. A listed-but-uncaptured member is `NOT_CAPTURED`.
2. Any other book shape is `UNSUPPORTED_SHAPE`, recorded with its book count. A
   member is never paired by guesswork. Examples: a Polymarket member with one or
   three tokens, or a Kalshi member missing one orientation.
3. Legs whose plans have different price or quantity scales are
   `UNSUPPORTED_SCALE`. They are never implicitly rounded.
4. Supported baskets then take a per-time book-usability status (§4.4).

The basket identity is `venue`, `market_id`, and the sorted book keys. A pair's two
legs are ordered by book key (instrument bytes, then orientation spelling), giving
leg 1 and leg 2. The strategy never calls one leg "YES".

**Payout assumption.** Exactly one leg pays one unit per contract at resolution.
This is an **assumption**, recorded in the manifest, not evidence. A void, 50/50,
or refund resolution may not reproduce the ordinary one-unit payout. The strategy
neither verifies resolution nor makes any claim about it.

## 3. Shared SDK pieces introduced by this strategy

The strategy module stays thin. Everything below lives in reusable modules under
`replay/` and is imported, never copied, by later strategies. None of it depends
on Redis or the supervisor.

- **Changed books.** `changed_keys(cut)` returns the frozenset of
  `(instrument, orientation)` keys named in `cut.body["book_transitions"]`.
  `bundle_coverage` already implements this pattern privately (`e5c5778`).
  Migrating coverage onto the helper is optional and must leave its output hashes
  unchanged.
- **Cut clock.**
  - Effective time per cut: group cuts use `origin.visible_ns`; window cuts use the
    raw window start; times are clipped to the requested start.
  - Pre-start prologue cuts initialize state but never create durations.
  - Effective time must be monotonic, and the existing window and terminal
    contracts are validated as in coverage.
  - Exposes `time`, `scope`, and `advance(t)`, which yields scope boundaries in
    order.
  - **Same-time staging:** §5.
- **Exact VWAP walker.** `walk(levels, max_atoms) -> Fills`:
  - Walks one side once, best first, up to the largest configured size. It yields
    a `Fill` for every configured size from that one pass.
  - Each `Fill` has:
    - `filled_atoms`;
    - `cost`, an exact integer at `price_scale + quantity_scale`;
    - `depth_limited`;
    - `taken`: `(price_atoms, taken_quantity_atoms)` per touched level, used by
      fees;
    - `consumed`: the touched levels with their **full displayed** quantity, used
      only for slice survival (§7).
  - The fee bridge never charges for `consumed`. The last level's untaken
    quantity was not purchased.
  - It never extrapolates past the last level. Arithmetic is integer only, with no
    floats or ambient `Decimal`.
  - `Book.levels(side, n)` bounds the returned tuple, not the work of selecting
    levels. The walker therefore reads each changed side once per cut, and the
    actual cost is benchmarked (§10).
- **Detached projections.** For every book in the snapshot's union plan, including
  books needed only by later scopes, keep detached copies:
  - `(validity, reason)`, as coverage's `status(book)` does;
  - the last-change time;
  - the per-side `Fills`.

  These may outlive the callback. Live `Book`, cut-body, and level references may
  not.
- **Episode and slice tracker.**
  - Holds per-key half-open intervals over exact integer nanoseconds, with
    successive slices nested inside each episode (§7).
  - Intervals of zero length are never emitted.
  - At terminal, open intervals close with `censored: true`.
  - Rows are emitted in close order (§9). Bounds are checked before state is
    retained, not only on emit.
- **Fee bridge.** It turns one leg's `Fill.taken` into the ordered
  `HypotheticalFill` values of one hypothetical taker order:
  - one declared fill per taken level, `Role.TAKER`;
  - the side fixed by §4;
  - the `Context` from the mapping below;
  - the `InstrumentEconomics` from the binding;
  - then `FeeEngine.assess_many`.

  One level as one fill is a **declared partition**, labelled
  `PER_LEVEL_DECLARED_PARTITION_ESTIMATE`. It is not proof of the exchange's
  actual fragmentation, nor a worst-case bound. SDK assumptions, exclusions, and
  evidence labels are preserved on the result.

**Fee context mapping**, fixed for V1. It needs no new Universe metadata:

| Context field | Source |
|---|---|
| venue / product | member venue; CLOB |
| market | member Targeter ID with its exact venue prefix removed |
| instrument | native BookKey instrument with its exact venue prefix removed |
| orientation | native BookKey orientation, unchanged |
| event / series / category | absent; never inferred from ticker spelling or prose |
| account / subaccount | fixed synthetic `same_venue_complement_v1` / `hypothetical` |
| builder | absent; fixed no-builder route |

Reviewed schedules must resolve under this mapping, at market or instrument scope.
Missing broader context never authorizes guessed inheritance. The binding must
match the selected schedule's economics exactly, as the SDK requires.

**Order identity.**

- Each leg order's `order_key` hashes:
  - `experiment_sha256`, scope, and the real or placebo basket;
  - direction, size, and leg key;
  - the observation's effective time and cut sequence.
- `fill_index` starts at 0 per leg order, and only fill 0 declares a new order.
- Independent observations never share accumulator state.
- The Kalshi `counterfactual_revision` is derived from the same inputs, never from
  the random supervisor attempt.
- Fill time is the observation's effective time. The pricing reference is the
  pinned `reference_ns`.

## 4. Pricing rules, per venue

Notation:

- `P = 10^price_scale` is one unit of payout in price atoms.
- `N` is the ticket size in whole contracts, so `Nq = N × 10^quantity_scale`
  quantity atoms.
- `U = N · P · 10^quantity_scale` is `N` units of payout at gross scale.

Both legs always fill **the same number of contracts** (share matching). A
dollar-matched split is never computed. Each direction below names exactly one fee
scenario. No equality between the net results of different scenarios is claimed.
Inventory, collateral availability, and simultaneous execution are not established.

### 4.1 Polymarket (`PM_TOKEN_PAIR`)

Both token books carry bids and asks.

- **`long`** (BUY both, then merge). The gross gap is `U − (walk(asks_1).cost +
  walk(asks_2).cost)`. The fee scenario is two BUY orders.
- **`short`** (split, then SELL both). The gross gap is `(walk(bids_1).cost +
  walk(bids_2).cost) − U`. The fee scenario is two SELL orders, from an assumed
  complete set split from `N` collateral units.

Expected: both are nonpositive almost always. The tokens can always be merged or
split, and the venue's matching crosses complementary orders. A positive gap can
come from:

1. the two token books updating in separate deliveries;
2. a decoding or normalizer error;
3. two tokens that are not in fact complements.

These are diagnosed in that order (partition-sum spec §6).

### 4.2 Kalshi (`KALSHI_YES_NO`)

**How the books look.**

- The normalizer builds both books from the venue's **bid** ladders only
  (`kalshi/event.rs`: `"yes"` → `outcome`, `"no"` → `complement`, `Side::Bid`).
  Asks are always empty.
- A YES buyer matches resting NO bids. A NO bid at `p` is a YES offer at `P − p`.
- The strategy states this projection explicitly. It is strategy-owned economics,
  not an SDK or transport default, so the preparation rule "no implicit 1−p
  ladder" still holds.

**`both_bids`** is the single reported direction.

- Buying `N` of the outcome orientation walks the `complement` bids, each level
  filling at `P − p`. Buying `N` of the complement walks the `outcome` bids the
  same way.
- The gross gap is `(walk(bids_outcome).cost + walk(bids_complement).cost) − U`:
  the combined bids exceed one unit at size.
- The fee scenario is two **BUY** orders at those projected prices.

BUY is chosen because it needs no inventory. Kalshi holds one signed position per
market, so a holder cannot own YES and NO at the same time, and the second buy
nets the pair. Selling into both bid ladders would require exactly that
impossible inventory. The SELL computation exists only as a gross-pricing identity
in tests, not as a reported scenario.

Expected: never positive beyond one cut. A YES bid and a NO bid summing above one
unit form a crossed book that the matching engine fills. A persistent positive
means our reconstruction is wrong.

### 4.3 Limitless (`LIMITLESS_SELF_CROSS`)

Only the `outcome` book is captured, and it has bids and asks. V1 records
**diagnostic state only**:

- `LOCKED`: `best_bid == best_ask`.
- `CROSSED`: `best_bid > best_ask`.
- The size form: `walk(bids).cost` compared with `walk(asks).cost` at each size.

No fees, net payout, episodes of economic kind, or verdict are inferred. How
Limitless relates the two sides of one market has not been verified. Establishing
that is a prerequisite for any Limitless basket.

### 4.4 Measurement status

At each committed time, every scoped `(basket, direction, N)` key has exactly one
status:

1. **`UNUSABLE`**, if any leg is not `usable`. It carries each leg's detached
   `(validity, reason)`. `not_initialized` counts here. It is not evidence of
   missing data (coverage semantics).
2. **`ONE_SIDED`**, if a required side is empty.
3. **`DEPTH_LIMITED`**, if any leg cannot fill `N`. It carries
   `max_fillable_atoms = min(filled_atoms)`. It is never scored from a partial
   fill.
4. **`DEPTH_SUFFICIENT`**, otherwise. Only these keys enter gaps, fees, episodes,
   and `evaluated_ns`. The word `FILLED` is never used, because nothing was filled.

## 5. Evaluation timing, prior state, and leg skew

The book state is piecewise constant between cuts, so measurements are exact.
Nothing is sampled. Every duration is exact visible time in nanoseconds.
Statistics are **time-weighted**, in basket-direction-nanoseconds. They are never
weighted by cut counts, because cut density varies by orders of magnitude.

**Re-evaluation set.** A basket is re-evaluated when:

- a cut changes one of its legs (from `changed_keys`), or
- **a substituted placebo leg changes** (§8), or
- it enters a scope, or the requested start is reached. These are evaluated from
  the detached projections even without a book transition.

A reverse map from book key to dependent real and placebo baskets is rebuilt at
each scope change.

**Prior state.** The decoder has already applied a cut before the callback.
Boundaries that fall before the cut's time are advanced using the **prior**
detached projections. Only afterwards are the changed projections refreshed from
the current cut.

**Same-time staging.**

- Several cuts can share one effective time. That happens when one delivery's
  events land in consecutive group cuts, or for prologue cuts clipped to the
  requested start.
- The strategy stages one effective time at a time, keeping only bounded detached
  candidate state, never a list of cuts.
- The final state at the staged time is committed to measurement, episode, and
  slice accounting only when a greater time or terminal arrives.
- Same-time false-then-true or change-then-restore sequences therefore neither
  split an existing episode nor end an unchanged slice.
- At a scope boundary, the old scope is closed and the new scope takes the final
  same-time state.
- Superseded positive intermediate observations are counted per key in
  `instantaneous_positive`. That counter is a **writer-attested diagnostic**. It is
  content-hashed, but the reader does not reconstruct it, and it never influences
  a verdict. A cross that exists only between two parts of one delivery is a
  decoding artifact, not an opportunity.

**Leg skew.**

- Each leg's last-change time is the effective time of the latest cut that
  transitioned its book.
- `leg_skew_ns` is the absolute difference between a basket's two legs'
  last-change times. A placebo's skew comes from its actual legs.
- Both legs are the **current** reconstructed state, so skew is not staleness of a
  sample. It measures whether one leg has just moved while the other has not yet.
- Skew changes over a measurement's life. Durations are therefore attributed to
  skew buckets **at each instant**, by intersecting intervals with skew-bucket
  intervals. They are never attributed wholesale to the bucket at an episode's
  opening.
- Bucket edges are half-open. Episode counts may also be grouped by opening skew
  when labelled as such.
- The interpretation is decided in advance. A positive that occurs only in buckets
  ≥ 1 s and disappears below that is labelled `SKEW_ARTIFACT_LIKELY`. It still
  counts toward the verdict, so it remains visible.
- **Policy 2: skew never splits time.** Skew is computed from the legs' *live*
  last-change times, including for controls. It is recorded at each episode and
  slice open. Latency-qualified entry time is attributed to the bucket in force at
  each entry instant, inside positive slices only. `SKEW_ARTIFACT_LIKELY` uses
  positive gross slice time, bucketed by skew at slice open.

## 6. Gaps, fees, and net results

**Gross**

- `gap_gross` is an exact signed integer at `gross_scale = price_scale +
  quantity_scale`.
- Comparisons across different scales use exact integer cross-multiplication.

**Net**

- `gap_net` is a signed integer at `net_scale = 18` quote units. Every SDK amount
  fits exactly, because SDK scales are at most 18.
- Fees are assessed only when `gap_gross > 0`. With rebates and refunds off, every
  charge is nonnegative, so a nonpositive gross gap proves a nonpositive net gap.
  That proof holds only under these fixed assumptions. It is exact, and it keeps
  fee work off the hot path.
- Net is computed from SDK `net_deltas`, which **already include charges**:
  - **BUY scenarios** (PM `long`, Kalshi `both_bids`): sum the signed quote-asset
    deltas of both orders, then add `min(net received outcome quantity across
    legs) × one unit`. Unmatched leftover outcome tokens are valued at zero.
    Outcome-token fees therefore reduce the matched payout.
  - **SELL scenario** (PM `short`): sum the signed quote-asset deltas, then
    subtract `N` quote units for the assumed complete set.
  - Collateral charges are never subtracted a second time.
  - An unexpected asset debit, or an outcome debit beyond the supplied set, fails
    economic admission visibly. Unlike assets are never valued together.
- If any leg returns an `UnknownSchedule`, or `net_deltas is None`, or a binding or
  asset is missing, then `fee_status: "UNKNOWN"` and `gap_net` is `null`. It is
  never zero.
- Assessment identities are kept for **both** leg orders.
- Fees are an **estimate** under the pinned catalog and reference time
  (`current_snapshot_not_historical_fee_claim`).

**Net eligibility** requires `gap_net > 0` **and** `gap_net ≥ N ×
minimum_net_gap_per_contract_e18`. This is integer arithmetic with no division or
rounding. Break-even never qualifies.

**Time partition.** For every `DEPTH_SUFFICIENT` key, `evaluated_ns` is
partitioned exactly into:

- `gross_nonpositive_ns`: gross ≤ 0, fee work skipped;
- `net_assessed_ns`: gross > 0, and both legs' net balances known; this is split
  further into `net_positive_ns` (eligible) and the remainder;
- `fee_unknown_ns`: gross > 0 and at least one fee input or result unknown.

The three parts must sum to `evaluated_ns`. Unknown net time is never silently
removed from this denominator. `UNUSABLE`, `ONE_SIDED`, `DEPTH_LIMITED`,
`UNSUPPORTED_*`, and `NOT_CAPTURED` time stay separately visible.

Signed amounts are written as canonical signed decimal strings, with no `+`, no
leading zeros, and no `-0`.

## 7. Episodes, slices, and latency-qualified time

**Episode kinds**, per `(basket, direction, N)`:

- `gross`: open while `DEPTH_SUFFICIENT` and `gap_gross > 0`;
- `net`: open while `DEPTH_SUFFICIENT` and net-eligible (§6). An unknown net is
  not eligible.

An episode closes at the first committed time its predicate is false. Its
`end_reason` is one of:

- `PREDICATE_FALSE`;
- `UNUSABLE` (with leg reasons), `ONE_SIDED`, or `DEPTH_LIMITED`;
- `SCOPE_END`. Scope boundaries always close both kinds;
- `RUN_END`, with `censored: true`.

**Slices.**

- Each episode is partitioned into successive **slices**. A slice is a maximal
  interval during which every leg's `consumed` displayed slice is unchanged.
- A slice ends on any consumed-level change, including a size increase, or at its
  episode's end. A replacement slice opens immediately while the episode
  continues.
- Changes at unconsumed levels do not split a slice.
- A net slice cannot cross an interval where net eligibility is false or unknown,
  because that ends the episode.
- Each slice's survival is the named `DISPLAYED_DEPTH_SURVIVAL` estimator from
  gate 4, applied to the exact quotes the ticket needs.

**Latency-qualified entry time.**

- For a slice `[a, b)` and latency `L`, the entry times from which the ticket's
  quotes stay unchanged for `L` occupy `[a, b − L)`. Their duration is
  `max(0, b − a − L)`.
- `Q(L)` sums these durations over **net** slices. The same sum over gross slices
  is reported as a diagnostic.
- A slice of exactly `L` reaches the tier, adding to the count, but contributes
  zero entry time.
- Entry time is attributed to the skew bucket at the entry instant (§5).
- A censored slice credits only survival observed before run end. Nothing is
  extrapolated.
- This is a **retrospective** survival estimator. It is never an executable signal,
  because it uses future data.

**Episode viability.**

- An episode reaches a tier if any of its slices does.
- `gap_lifetime_ns` (episode duration) and slice survival are reported with
  separate quantiles.
- `opening_slice_survival_ns` is kept only as an explicitly named diagnostic.

**Latency tiers**, defaulting to 250 ms, 1 s, and 5 s:

- Visible time already includes inbound feed latency.
- A taker must still decide, send both orders to the **same venue**, and have them
  matched, with no co-location.
- **1 s** is the planning estimate for reliable two-order execution from a cloud
  VM. **250 ms** is the optimistic case for a well-placed bot. **5 s** approximates
  a slow or semi-manual operator.
- These are unmeasured planning assumptions. A live order-latency test (gate 4)
  replaces them in a later policy version.

Even a surviving quote may already have been taken by someone faster. Slices
cannot observe queue position, other takers, acknowledgements, or fills. Every
economic output is a detection or an estimate. Labels say `DETECTED`, never
`CAPTURED` or `FILLED`.

## 8. Placebo and controls

Policy 1 always runs the placebo below. Policy 2 runs no control by default. It
can enable this construction as the `cyclic_neighbor` control, and the
`time_shift` staleness control, both under the SDK spec §6. Enabled controls write
only under `controls/<name>/` and never feed a verdict.

The gate 3 placebo is adapted to single-instrument baskets. For each supported pair
basket `A` on venue `V`:

- The placebo pairs `A`'s leg 1 with leg 2 of the next supported pair basket on
  `V` in the same scope, in cyclic `market_id` order.
- Direction, `N`, pricing rule, timing, staging, and fee path are identical. Skew,
  fee context, and order identity derive from the placebo's actual legs.
- A placebo with fewer than two supported pairs on `V` in that scope records an
  explicit `NO_MATCH` measurement. It does not manufacture evaluated time.
- Changes to the substituted leg re-evaluate the placebo, through the reverse map
  in §5.

A placebo is a deliberately **broken pair**: a diagnostic of how often this exact
measurement reports a positive on a basket that is not one instrument's complete
set. It is not proof of statistical independence or economic non-equivalence. Two
distinct markets could still be related. Placebo positives are never locked profit,
and never a test of the real basket's settlement semantics. Placebo construction is
fixed by this section and `policy.version`, and is hashed before any observation.

## 9. Outputs

All files are written under the supplied output directory, which must start empty.
Writers follow the `LineWriter` and `write_json_durable` discipline of bundle
coverage.

**Policy 2** writes the SDK's output layout 2 (SDK spec §5). It contains:

- entity and reason tables;
- one denominator row per (scope, basket, direction, size), with exact time per
  status and per value class;
- real episodes, holding quotes and values at open and at maximum;
- compact slices;
- opt-in controls and audit.

Policy 2 also differs in these ways:

- its reasons are structured (SDK spec §11);
- it has no per-row `fee_status` or `diagnostic`, because the value class
  determines fee knowledge;
- its summary rows have no skew dimension.

The verdict rules below apply to both policies. Rule 3's `X` differs as stated
there.

The rest of this section describes **policy 1** (layout 1).

**Files.** Each ordinary row carries `version`, `experiment_sha256`, and `scope`.

**`measurements.ndjson`.** A complete half-open interval partition of the requested
interval for every scoped `(real/placebo basket, direction, N)` key, plus explicit
`NOT_CAPTURED`, `UNSUPPORTED_*`, and placebo `NO_MATCH` records. Each row holds:

- the status (§4.4) and its reasons;
- the skew bucket, or an explicit unknown;
- the value class: `GROSS_NONPOSITIVE`, `NET_POSITIVE`, `NET_NONPOSITIVE`, or
  `FEE_UNKNOWN`;
- the fee knowledge.

It records **classes, not raw gap values**. Raw values change on almost every
top-of-book update and would make the file grow with the tape. Values are recorded
only inside episodes and slices. Adjacent identical rows coalesce. Limitless rows
record `LOCKED`, `CROSSED`, or `NOT_CROSSED` per size. They carry no fees or net
values.

**`episodes.ndjson` and `placebo_episodes.ndjson`.** One row per non-zero-length
episode, holding:

- a stable `episode_id`, derived from the scoped key, kind, and `start_ns`, never
  from the attempt;
- basket, direction, `size_contracts`, and kind;
- `start_ns`, `end_ns`, `end_reason`, `censored`, `gap_lifetime_ns`, and
  `opening_slice_survival_ns`;
- tiers reached and `Q` per tier;
- at open, and at maximum: `gap_gross`, `gap_net` or `null`, `gross_scale`,
  `net_scale`, and `fee_status`;
- both leg assessment identities at open;
- for placebo rows, the replaced leg is named.

**`slices.ndjson`.** One row per slice, with:

- `episode_id`, `start_ns`, `end_ns`, and `end_reason`;
- `consumed` slices;
- `censored`;
- survival, and tiers reached.

**Close order.** Measurement, episode, and slice files are written in deterministic
**close order**: `(end_ns, scope, venue, market_id, direction, numeric size,
real/placebo, kind, start_ns)`, omitting fields that do not apply to that file.
Completed rows are therefore never accumulated in memory.

**`summary.json`.** Rows per `(venue, basket kind, direction, N, skew bucket)`,
real and placebo, each carrying `policy_sha256`. Each row holds:

- the §6 time partition;
- the non-evaluated durations;
- episode and slice counts, and `instantaneous_positive`;
- `Q` per tier;
- exact nearest-rank quantiles (rank `ceil(p × count)`; `null` with no
  observations) for p50, p90, p99, and max of gap lifetime and slice survival.
  Censored observations are included and counted separately.

The summary also lists per-venue `UNSUPPORTED_*` and `NOT_CAPTURED` members,
Limitless diagnostic durations, and the verdicts.

**Verdict**, per venue except Limitless, which gets none. Inputs, at the headline
size, summed over that venue's directions, in basket-direction-nanoseconds:

- `E`: real-basket evaluated time;
- `Q`: `Q(headline_latency_ns)`;
- `X`: `fee_unknown_ns`.

The rules apply in this order:

1. `E == 0` or `E < minimum_evaluated_ns` gives `INCONCLUSIVE_FIXTURE`.
2. `Q × 1,000,000 > maximum_positive_time_fraction_ppm × E` gives
   `INTRA_INSTRUMENT_GAPS_PRESENT_INVESTIGATE`.
3. `X > 0` gives `INCONCLUSIVE_FIXTURE`, reason `UNRESOLVED_POSITIVE_GROSS`. Under
   policy version 2, `X` counts only `FEE_UNKNOWN` time inside gross slices whose
   survival reaches the headline latency. The total stays visible as
   `fee_unknown_total_ns`.
4. Otherwise the result is `INTRA_INSTRUMENT_GAPS_ABSENT_IN_FIXTURE`.

Every verdict carries `basis: PINNED_FEE_MODEL_AND_DISPLAYED_DEPTH_POLICY`. Absence
means only this:

- the fixture did not exceed the configured fraction under that fee model, size,
  and latency.

It proves none of the following:

- that there were zero gross anomalies;
- execution profitability;
- historical fee applicability;
- that no edge exists elsewhere.

Gross anomalies stay reported even when fees remove them.

**Manifest and receipts**, following bundle coverage's shape and roles:

- **Manifest.** Strategy `same_venue_complement_v1`. It records the snapshot,
  policy, and experiment identities, the fee catalog and engine identities, every
  output file identity, and the summary hash. It excludes attempt metadata.
- **Content receipt.** Binds the manifest's semantic SHA-256 to run, attempt,
  group, and terminal. Retried attempts therefore produce identical semantic
  output and different receipts.

**Independent reader.** Before the manifest is written, an independent streaming
reread checks every output file:

- it rebuilds the time partition, positive and unknown time, and skew totals from
  the measurements;
- it cross-checks that episodes and slices partition consistently with the
  measurements;
- it recomputes lifetimes, `Q`, quantiles, and verdicts;
- it rejects missing or overlapping coverage, orphan slices, inconsistent
  predicates, and modified summaries.

This verifies the internal consistency of the result. It is not a second
reconstruction from the raw tape.

**Completed reader.** It additionally requires supervisor `SUCCESS` and matching
factory, configuration, and snapshot pins. Provisional content validation alone is
not success.

**Bounds.** All bounds fail closed. Nothing is truncated or silently sampled.

| Resource | Limit |
|---|---|
| Episode rows, real + placebo | 2,000,000 |
| Measurement rows / slice rows | 2,000,000 each, real + placebo |
| Each NDJSON file / line | 512 MiB / 64 KiB |
| Detached strategy state, including quantile arrays | 128 MiB, excluding decoder-owned books |
| Configuration / summary / manifest | 8 MiB each |

- Slice line size is checked when a slice is retained, not only when its episode
  closes.
- Quantile inputs are bounded by the row limits and are kept as compact 64-bit
  integer arrays within the state bound.
- A fault that crosses every book on a venue must fail the attempt visibly, not
  silently cap. Limits change only by review.

## 10. Performance

The strategy runs as an additional group beside other strategies on the same
stream.

- Per committed time, only the re-evaluation set from §5 is evaluated.
- Each changed book side is walked once for the whole size sweep. Its `Fills` are
  reused across every dependent real and placebo basket.
- The fee bridge is never called for nonpositive gross gaps.
- Retained state is limited to the detached projections, the staged candidate, the
  open episodes and slices, and the per-row accumulators.

The bench corpus has about 600k planned-book deltas. The economic group must be
benchmarked, and must not become the slowest group in the attempt.

## 11. Verification required before calling V1 implemented

These tests are offline and contract-shaped. Hand-authored books are delivered
through the real `Decoder`, and every expectation is calculated independently.

**VWAP and admission**

- Exact fill; partial and `depth_limited`; a fill ending exactly on a level
  boundary; one-sided; empty; single level; size beyond total depth;
  integer-only results.
- One walk yields every size.
- Fees charge only the taken fraction of the final level, while survival uses its
  full displayed quantity.
- `UNSUPPORTED_SHAPE`, `UNSUPPORTED_SCALE`, and `NOT_CAPTURED` are reported, not
  dropped, in admission precedence order.

**Pricing**

- Share-matched versus dollar-matched cost differ, and share matching is the one
  used.
- A synthetic Polymarket pair with a known 2-cent gross gap recovers exactly 2
  cents.
- Polymarket `long` and `short` are computed independently.
- Kalshi: buying the outcome walks complement bids at `P − p`. The SELL identity
  equals the `both_bids` gross gap. Only the BUY scenario is fee-assessed and
  reported.

**Fees and net**

- SDK collateral fees enter net exactly once.
- Outcome-token fees reduce the matched payout.
- A different declared fragmentation may change fees, and keeps its scenario label.
- Gross and net scales differ without rounding the admission comparison.
- Missing bindings stay unknown. Conflicting bindings fail before callbacks.
- Exact net zero opens no net episode.
- A nonpositive gross gap is accounted without fee assessment.

**Verdict**

- Positive gross with entirely unknown fees cannot yield absence.
- Positive evidence above the ppm threshold stays present despite other unknown
  intervals.
- Insufficient evaluated time takes precedence.
- With no positive episodes, healthy versus unusable measurements give different
  denominators and verdicts.

**Timing, slices, and placebo**

- Episodes and slices open and close at exact nanoseconds.
- Pre-start prologue cuts initialize state only.
- Quiet scope entry is evaluated from prior projections.
- Scope boundaries close episodes with `SCOPE_END`. Terminal censors, crediting no
  future time.
- An unusable leg closes with its reasons copied.
- Same-time false-then-true and change-then-restore preserve episodes and slices,
  and increment `instantaneous_positive`.
- A 10 ms opening slice followed by a 9 s stable replacement qualifies at 1 s.
- An exact-tier slice adds a count but zero entry time.
- A continuous slice with changing skew attributes entry time to the correct
  buckets.
- A change at an unconsumed level does not split a slice.
- A change to only the substituted leg updates the placebo.
- Cyclic placebo determinism, and `NO_MATCH`.

**Outputs and identity**

- An episode that opens earlier but closes later is emitted correctly in close
  order, without retaining completed rows.
- State and output bounds fail closed.
- Missing measurements, orphan slices, and modified summaries fail the
  independent reader.
- The policy and experiment hashes are fixed before the first callback. A changed
  threshold or fee binding changes them.
- Retried attempts produce identical semantic outputs and distinct receipts.
- Missing supervisor `SUCCESS` fails the completed reader.
- The zero-copy contract holds: no reference to `cut.books` or `cut.body` survives
  the callback (the existing stream test pattern).

**Acceptance** on the local bench corpus (`.bench/`, bundle
`bundle_e8a92effa246b9548571c907`), through direct supervisor execution:

- Run this strategy and `bundle_coverage` as two groups in one attempt. The
  coverage output must still hash to its recorded reference.
- Record the runtime and whether this group was ever the slowest.
- The Kalshi `both_bids` and Polymarket token-pair results are the first real
  correctness readout of the normalizer fixes in PR #51.

The following are not covered by these tests and must be reported as unverified:

- retained-data and production runs;
- live order latency;
- fee catalog and binding evidence review.

## 12. Landing and open items

**First landing.** The first landing is the strategy, its offline tests, and its
strict completed reader, run through **direct supervisor execution**. It is **not**
exposed in the Replay jobs catalogue.

**Jobs exposure** is a separate integration step. It requires:

- the shared strategy schema and registry;
- validated ownership of each field between request and runner;
- immutable fee catalog provisioning in the runner. Catalog paths are owned by the
  runner, never supplied by clients;
- result-reader registration.

The API consumes the shared registry, so exposure requires a coordinated server
and runner deployment even though no new HTTP endpoint is needed. Deployment stays
separately authorized.

**Open items**

- **Fee catalog and bindings.** A reviewed fee catalog and instrument bindings for
  current Polymarket and Kalshi schedules are needed for meaningful net results.
  Kalshi publishes fee multipliers per series, but V1 context omits series (§3).
  Kalshi net results therefore need market-scoped schedules, or a later reviewed
  series binding. Without them, net is `UNKNOWN` and verdicts are inconclusive.
  Synthetic tests and explicitly gross-only audits do not need them.
- **Limitless.** Its two-side semantics (§4.3) must be established before a
  Limitless basket can be priced.
- **Latency.** The tiers remain planning assumptions (§7) until a live order test
  exists.
