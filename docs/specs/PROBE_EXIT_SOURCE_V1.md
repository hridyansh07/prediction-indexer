# Probe exit source: unwind into the acquired book's real bids

Status: **implemented**, including §8's bounded exits and uncovered sold excess.
The defect descriptions record pre-fix behavior; final compatibility also
includes [optional legacy probes](LEGACY_PROBE_RETENTION_V1.md). Amends
[EXECUTION_DELAY_V1.md](EXECUTION_DELAY_V1.md) §2.5 (excess and leg loss) and
[STRATEGY_EXECUTION_REALISM_V1.md](STRATEGY_EXECUTION_REALISM_V1.md) §2 (exit
ladders). Fixes audit finding R12. Also sets the Polymarket unknown-delay default
to 1 s (owner decision, 17 Oct 2026).

## 1. The defect

A probe values each leg's unmatched excess by walking an exit ladder saved at
that leg's arrival (`replay/economic_sdk/probes.py`, `EXIT_SOURCE` and
`_arrive`). The exit ladder is read from **the leg's entry source book**:

| Entry source | Exit today | Built from |
|---|---|---|
| `ask` | `bid`, same book | native bids |
| `kalshi_complement_ask` | `kalshi_complement_bid`, same book | the same book's **native asks** |

The Kalshi normalizer builds both orientations from bids only and never emits a
native ask (`engine/crates/replay-normalizers/src/kalshi/event.rs`: every
`FullBook` gets `Vec::new()` asks, every delta is `Side::Bid`). So every
Kalshi exit ladder is empty on real data. Each Kalshi probe with excess records
`exit_consumed: []`, `exit_proceeds: "0"`, and books the full excess
acquisition cost as `leg_loss_gross`, even when the acquired side has real bid
depth. Existing fixtures pass because they inject native Kalshi asks that the
normalizer never produces.

Example (fees ignored): YES bid 0.30, NO bid 0.60, no asks. Buying YES walks
the NO book's projected ask at 0.40. One excess YES should unwind into the YES
bid at 0.30, a loss of 0.10. Today it records proceeds 0 and a loss of 0.40.

Matched steps, `value`, delays, arrival times and statuses are not affected.

## 2. The rule

A BUY leg's excess unwinds by **selling the acquired claim into the acquired
book's own native bids**, at that leg's arrival time.

- **Acquired book:** the descriptor leg's `(instrument, orientation)`
  (`entity.descriptor["legs"][i]`). It is **not** `entity.legs[i]`: for Kalshi,
  `entity.legs` holds the entry *source* books, the opposite orientation, via
  `source_key` in `cross_venue_arbitrage/contract.py`. For Polymarket and
  Limitless the acquired book and the source book are the same, so their exit is
  unchanged: native `bid` on the same book.
- **Planning:** admission today checks only that the source book is planned. An
  admitted entity whose acquired exit book has no plan or builder fails at bind
  time with `"probe exit book not planned"`. Both Kalshi orientations come from
  the same orderbook message, so this should never fire on real data. It must not
  degrade to an empty exit.
- **Kalshi:** acquired YES unwinds into the YES (`outcome`) book's native `bid`.
  Acquired NO unwinds into the NO (`complement`) book's native `bid`. No
  projection, and no reading of native asks.
- **SELL legs:** no current rollout strategy has one. Keep the existing reverse
  mapping for non-Kalshi sold legs (`bid` → `ask` on the same book). A Kalshi
  sold leg with probes enabled fails closed at bind time with
  `"probe exit unsupported for Kalshi sold leg"`, rather than reading a
  ladder that is always empty.
- Exit walks stay best-first with no limit price. The final rule in §8 caps
  saved/walked exit depth at `max_levels` and treats the rest as a shortfall.

## 3. Interfaces

1. **Strategy hook.** Add `Strategy.fill_exits(entity)` to the economic SDK. It
   returns one `(book_key, ladder)` pair per leg. The SDK default returns
   `(entity.legs[i], EXIT_SOURCE[spec.sources[i]])`, which is today's behaviour.
   The four execution-realism strategies (cross-venue arbitrage, same-venue
   multi-market, both implication covers) override it to apply §2, through one
   shared helper in `replay/strategies/_shared/execution_realism.py`. The
   implication covers inherit it from their owning BUY strategy, as they do for
   `fill_delays`.
2. **Bind checks** (`runtime.py`, the probe block of fill-spec binding): replace
   `EXIT_SOURCE[source] in self.builders[key].ladders` with a check that every
   exit `book_key` has a builder and retains the exit `ladder`. The failure text is
   `"probe exit ladder not retained"`, as today.
3. **Arrival** (`probes.py` `_arrive`): read the exit ladder from the exit book's
   view at `at`, using the same committed-view rule as the entry view, which
   applies to the live views. A time-shifted control never reaches probes,
   because fill mode rejects controls. Do not add a new cut time or retention
   boundary.
4. **Requirements.** For Kalshi books under the new policies, retain the native
   `bid` ladder and stop retaining `kalshi_complement_bid`. Stop reading the
   native `ask` side on Kalshi books, since no Kalshi consumer needs it anymore.
   Leave the R10 `("crossed", None)` input in place, because it is harmless.
   Polymarket and Limitless requirements are unchanged.
5. **Probe rows and the reader.** The row schema is unchanged. `probe_reader`
   keeps its arithmetic checks on `exit_consumed` and `exit_proceeds`. Prices
   are checked against the exit book's price scale, which is the same
   instrument's scale for Kalshi. Do not add a field.

## 4. Versions and compatibility

- This amendment keeps the execution-realism versions cross-venue 4,
  multi-market 2 and implication covers 4. Earlier policies preserve their
  schemas and bytes. Legacy fill policies may explicitly enable probes; those
  retain the SDK's entry-source reverse exit mapping, as specified in
  [LEGACY_PROBE_RETENTION_V1.md](LEGACY_PROBE_RETENTION_V1.md). Sweep policies
  do not admit fills or probes.
- The A1 SDK default hook keeps today's behaviour for any other probe user, so
  existing A1 probe goldens that use non-Kalshi books stay byte-identical.

## 5. Polymarket unknown-delay default: 3000 → 1000 ms

Universe v8 observes `taker_delay_ms = 1000` on all 232,495 Polymarket
execution-term rows. Every Polymarket market in all 1,212 retired bundles has
observed terms. The 3 s default rested on a maximum that the data does not
show. Change `unknown_taker_delay_ms.polymarket` to `1000` in:

- `configs/corpus/execution-realism.example.json` (all four groups);
- each fill strategy's `config.example.json` that carries probes;
- `STRATEGY_EXECUTION_REALISM_V1.md` §2 and `STRATEGY_EXECUTION_REALISM_CONTRACT.md`
  (the reference values and the rationale);
- the test reference constant `PROBES` in `replay/tests/test_strategy_execution_realism.py`.

Kalshi and Limitless stay 0. A known numeric observation, including 0, always
wins over the default. `delay_source` semantics are unchanged.

## 6. Tests (falsifying first)

Per AGENTS.md, write the regressions first and show them failing on
`origin/feat/corpus-runner` for the stated reason, then fix.

1. **Normalizer-shaped Kalshi books.** Add a fixture helper that builds Kalshi
   books exactly as the normalizer does: two orientations, bids only, no asks.
   Use it in every new test below. Do not add native Kalshi asks.
2. **Partial excess, acquired YES.** Cross-venue policy 4, Kalshi YES acquired
   via NO bids at 0.40 × 10, other leg depth 6, YES bid 0.30 × 10. Assert that
   `excess_atoms` is 4 contracts' atoms, `exit_consumed` walks the YES bid at
   0.30, `exit_proceeds` is 4 × 0.30, and the scale-36 `leg_loss_gross` equals
   4 × 0.10. Run it through the writer and the semantic reader.
3. **Acquired NO.** The mirror case: NO acquired via YES bids, exit into NO bids.
4. **Insufficient exit depth.** Acquired-side bids cover only part of the excess.
   The exit walks what exists, and the unexited remainder keeps its full cost in
   the loss, as today.
5. **Empty or unusable acquired book.** Zero proceeds, with no crash and no
   borrowed ladder.
6. **All four factories.** Each realism strategy binds, opens, and writes non-empty
   exit walks for a Kalshi excess case. Each of the 60 scenarios reads back.
7. **Bind failures.** An acquired exit book that isn't planned, an exit ladder not
   retained, and a Kalshi sold leg with probes. Each fails with the exact text
   above.
8. **Byte identity.** Old-policy goldens, the complement V1 golden, and A1's
   non-Kalshi probe fixtures are unchanged. Polymarket/Limitless-only probe rows
   are byte-identical before and after, given the same `unknown_taker_delay_ms`.
   The exit change alone must not move a non-Kalshi byte.
9. **Default.** With no observed terms, a Polymarket leg uses 1000 ms and
   `delay_source: "default"`. An observed 0 stays 0.

## 7. Acceptance

- The focused modules pass: `test_strategy_execution_realism`,
  `test_execution_probe_integration`, `test_execution_delay_probes`,
  `test_execution_probe_reader`, `test_cross_venue_arbitrage`,
  `test_same_venue_multi_market`, `test_implication_cover`,
  `test_strategy_packages`, `test_economic_sdk_port`, `test_streams`. Then the full
  Python gate.
- Preserve the falsifying regressions and independent-reader controls. Remaining
  retained-data acceptance is tracked in [open work](../OPEN_WORK.md).
- No corpus, bench, service or production-data run is part of this change.

## 8. Addendum: bounded exit walks and uncovered sold excess

From the execution-realism verification of 40c2804 (findings F2 and F1).

### 8.1 An exit walk never fails the attempt (F2)

Today the exit ladder is sliced to `max_levels + 1`, and `_walk` raises
`"probe consumed level bound"` when the excess needs more than `max_levels`
levels (`probes.py:46,250,275`). Exit walks have no price limit. Once §2 makes
Kalshi exits read real bids, a large excess against a thin, many-level ladder
would fail the whole bundle attempt over a diagnostic field.

The rule:

- An exit walk takes at most `max_levels` levels, best-first, and stops. It
  never raises the level-bound error. The excess it cannot reach within
  `max_levels` levels is an **exit shortfall**, treated exactly like excess
  beyond the ladder's displayed depth.
- Save only `max_levels` exit levels at arrival (not `max_levels + 1`). The
  extra level existed only to detect the failure.
- **The arrival (entry) walk is unchanged.** It still fails the attempt
  when it needs more than `max_levels` levels within its frozen limit, as
  `an_arrival_that_needs_more_than_max_levels_fails_without_truncation` pins.
  Truncating an entry walk would understate matched quantity, which is the
  probe's primary result. Do not change that test.
- **Row schema is unchanged.** The shortfall is already visible as
  `excess_atoms` minus the exit quantity in `exit_consumed`. The reader accepts
  `exit_consumed` with at most `max_levels` levels and an exit quantity of at
  most `excess_atoms`. It rejects more levels than that.

### 8.2 Uncovered sold excess is a loss, never a gain (F1)

Today, for a sold leg, `native_loss = exit_cost − excess_proceeds`. Excess the
repurchase ladder cannot cover is never charged, so an empty repurchase ladder
turns the full excess sale proceeds into a negative loss, a gain
(`probes.py:275-280`, `probe_reader.py:261`). Bought legs are already
conservative: unexited excess keeps its full cost.

The rule mirrors that conservatism:

- For a sold leg, each uncovered excess atom is charged as a repurchase at
  the book's maximum price, the unit payout `10**price_scale` (the same
  `maxima` the runtime already carries).
- `native_loss = exit_cost + uncovered_atoms × max_price − excess_proceeds`.
  It stays signed.
- `exit_proceeds` and `exit_consumed` still record only what the ladder
  actually covered. The charge for uncovered atoms appears only in
  `leg_loss_gross`.
- The writer and `probe_reader` both apply the same rule, independently.
- No current rollout strategy sells, so no corpus byte changes. This closes the
  latent defect for future sold-leg strategies.

### 8.3 Tests (falsifying first)

1. **Thin exit ladder.** A bought Kalshi excess whose acquired-side bids
   are spread over more than `max_levels` levels. The attempt completes, the
   exit walks exactly `max_levels` levels, and the remainder keeps its full
   cost in the loss. This fails today with `"probe consumed level bound"`.
2. **Non-Kalshi thin exit.** The same case on a Polymarket bought leg.
3. **Entry bound is unchanged.** The existing arrival max_levels failure
   test still passes, unmodified.
4. **Sold leg, empty repurchase ladder.** Adapt
   `test_sold_excess_unwind_is_signed_and_matched_keeps_highest_sales`: today
   it yields a negative `leg_loss_gross` equal to the full proceeds, which the
   reader accepts. After the fix, the loss equals
   `uncovered × max_price − proceeds`, which is positive.
5. **Sold leg, partial repurchase.** Covered atoms come from the ladder, and
   uncovered atoms are charged at the maximum price.
6. **Reader rejections.** A row with `max_levels + 1` exit levels is rejected.
   So is a rehashed sold-leg row whose loss omits the uncovered charge.
7. **Byte identity.** Every bought-leg row that needs at most `max_levels`
   exit levels is byte-identical to its §2-only output.
