# PARTITION_SUM_TEST_SPEC.md — v1

**Purpose.** Measure whether size-adjusted mispricing exists in prediction market
partitions at all, before building anything that depends on it. No modelling, no labels,
no correlation, no permutations. One afternoon of work, one number that gates the rest of
the project.

**Relationship to the correlation pipeline.** This runs *first* and is independent. It
reuses the acquire and normalize stages from `PIPELINE_SPEC.md`; everything from features
onward is bypassed. If this test finds nothing, the correlation pipeline is producing
rankings with no economic meaning attached to them.

---

## 1. What a partition is

A set of markets `{M_1 ... M_k}` whose outcome masks are pairwise disjoint and cover the
whole outcome space `Ω`. Exactly one resolves YES. Therefore holding one contract of each
pays exactly $1 regardless of outcome.

```
Σ ask_i  <  1 - fees      ->  locked long basket, profit = 1 - Σask - fees
Σ bid_i  >  1 + fees      ->  locked short basket
```

**Three partition classes available in the current EWC dataset.** Note that all three work
on moneyline-only data — this test does not require sibling markets.

| Class | Construction | Resolution risk |
|---|---|---|
| `INTRA_PM` | the two complementary outcome tokens of one Polymarket binary market | none — same condition, same oracle |
| `INTRA_KALSHI` | YES + NO of one Kalshi ticker | none — same contract |
| `CROSS_VENUE` | Team A on venue X + Team B on venue Y | **real** — different resolution sources and void policies |

Report the three classes separately and never pool them. `INTRA_*` results are clean
measurements of book inefficiency. `CROSS_VENUE` results are only tradeable if the
fungibility test in the design doc passes, and a gap there may simply be the price of
resolution divergence rather than an inefficiency.

---

## 2. The measurement is share-matched, not dollar-matched

This is the most common way to get this wrong. You are buying `N` **contracts** of every
leg so that the basket pays exactly `$N` at resolution. You are not splitting a fixed
dollar amount across legs.

```
cost_per_dollar(N) = Σ_i  vwap_ask_i(N)          # N contracts of each leg i
gap(N)             = 1 - cost_per_dollar(N) - fees(N)
profit(N)          = gap(N) × N
```

A dollar-matched split produces an unbalanced basket that is not a locked position, and
will report gaps that do not exist.

---

## 3. VWAP walker

```python
def vwap_ask(levels, n_contracts):
    """
    levels: list[(price, size_contracts)] sorted ascending by price (ask side)
    returns: (vwap, filled_contracts, depth_limited: bool)

    vwap is cost-per-contract for filling n_contracts by walking the ladder.
    If the ladder cannot fill n_contracts, return the vwap for what IS fillable
    and flag depth_limited. Never extrapolate past the last level.
    """
    remaining = n_contracts
    cost = 0.0
    for price, size in levels:
        take = min(remaining, size)
        cost += take * price
        remaining -= take
        if remaining <= 0:
            return cost / n_contracts, n_contracts, False
    filled = n_contracts - remaining
    if filled == 0:
        return None, 0, True
    return cost / filled, filled, True
```

Rules:
- If **any** leg is `depth_limited` at size `N`, the basket observation at `N` is invalid.
  Record it as `depth_limited` with `max_fillable = min(filled_i)` — do not silently use
  the partial fill.
- One-sided books (no ask side) invalidate the observation at every `N`. Count them.
- Verify the ladder is complete before trusting any of this: pull one raw snapshot, count
  the levels in `yes_bids` / `no_bids`, and confirm the API is not truncating to top-N.
  If it truncates, every VWAP above small `N` is optimistic and the whole test is invalid.

---

## 4. Size sweep — the deliverable is a curve, not a number

Sweep `N ∈ {1, 10, 25, 50, 100, 250, 500, 1000}` contracts and report `gap(N)` at each.

The gap-vs-size curve answers both questions at once: whether an edge exists, and whether
it survives to a tradeable ticket. A gap that is positive at N=1 and negative by N=25 is
the signature of a thin top-of-book quote and is not an opportunity — it is exactly the
11x/5.4x observation, measured properly.

Expected shape if the market is efficient: `gap(N)` negative everywhere and decreasing
monotonically in `N`.

---

## 5. Fees

Fees are not a rounding error here — they are comparable in size to the gaps you are
looking for, and they are venue-asymmetric.

- **Polymarket**: no CLOB trading fee historically; gas on Polygon, small but non-zero.
  Model as a fixed per-transaction cost, configurable.
- **Kalshi**: fee scales with `P × (1 - P)` per contract, so it is *maximal near 50¢ and
  small near the extremes*, and maker/taker schedules differ by market.

**Do not hardcode my numbers.** Pull the current published fee schedules and put every
constant in the config file. The `P(1-P)` shape matters more than the constant: it means
cross-venue partition gaps will look systematically easier to find on lopsided matches and
systematically harder near even odds. If your results show exactly that pattern, check
whether you have modelled the fee curve before concluding anything about market structure.

Report `gap(N)` both gross and net of fees. Gross tells you about book inefficiency; net
tells you whether it is tradeable.

---

## 6. The staleness trap — this is the falsification test inside the test

At 1m/5m granularity, each leg's "current" book is the most recent snapshot at or before
the bar boundary. Two legs can therefore be sampled up to a full bar apart in real time.
**A 3¢ apparent gap between books sampled 55 seconds apart is not an arbitrage. It is
staleness.** This will manufacture fake opportunities and it will do so most aggressively
in exactly the thin markets you care about.

Mandatory instrumentation:

1. Carry the true `snapshot_ts` of every leg through to the result row — not the bar
   boundary, the underlying observation timestamp.
2. Compute `leg_skew = max(snapshot_ts) - min(snapshot_ts)` per basket observation.
3. Stratify every reported statistic by `leg_skew` buckets: `[0-5s, 5-15s, 15-60s, >60s]`.
4. Report the gap distribution within each bucket.

**Interpretation rule, decided in advance:** if positive gaps concentrate in the high-skew
buckets and vanish in the low-skew bucket, you have measured your own sampling artifact
and the answer is "no edge found." Only gaps that survive in the lowest-skew bucket count.

Corollary: `INTRA_PM` baskets should show `Σask ≥ 1` almost always, because the CTF
split/merge mechanism pins it. If your data shows frequent `INTRA_PM` gaps, the most likely
explanations in order are (a) the two legs came from non-simultaneous snapshots, (b) a
one-sided book, (c) the two tokens are not actually complementary. Treat a large
`INTRA_PM` gap rate as a **data bug signal**, not a finding. This is your best built-in
correctness check.

---

## 7. Outputs

One Parquet table, one row per (basket, bar, N):

```
basket_id, partition_class, market_ids[], bar_ts, N
leg_snapshot_ts[], leg_skew_s
vwap_ask[], vwap_bid[]
cost_per_dollar, gap_gross, gap_net
depth_limited, max_fillable, one_sided_legs
time_to_resolution_s, liquidity_decile
```

One JSON summary and one Markdown report containing:

- `gap_net(N)` distribution per `N`, per `partition_class`, per `leg_skew` bucket
- fraction of observation-bars with `gap_net > 0`, same stratification
- when positive: median gap, median `profit(N)`, max
- consecutive-bar run lengths where `gap_net > 0` — with the caveat that at 1m resolution
  this is a lower bound on nothing useful; it cannot see sub-minute persistence, so report
  it as "at least one bar" and defer real persistence to tick data
- breakdown by time-to-resolution and liquidity decile
- `INTRA_PM` gap rate, reported explicitly as a data-quality metric per §6

---

## 8. Gate

Decide the threshold before looking at the output.

> **Pass:** in the `leg_skew < 5s` bucket, `gap_net(N) > 0` for `N` corresponding to at
> least `$X` of payout, in at least `Y%` of observation-bars, on at least one
> `partition_class`.

Set `X` and `Y` now and write them into the config. My suggestion is that `X` should be
whatever ticket size makes the project worth your time, not whatever size the data happens
to support.

**If this fails on the EWC dataset, do not conclude the thesis is dead** — conclude the
dataset is too thin, and re-run on the World Cup and the Kalshi Fed buckets before
deciding. Failing on all three is the real kill.

---

## 9. Build order

1. Reuse `acquire` + `normalize` from the existing pipeline. No new fetching.
2. Verify ladder completeness (§3) on one raw snapshot. **Stop if truncated.**
3. Basket construction for the three partition classes.
4. VWAP walker + unit tests: exact fill, partial fill, one-sided, empty, single level,
   fill exactly at a level boundary.
5. Size sweep + fee model from config.
6. Skew instrumentation and stratification (§6). Not optional, not phase two.
7. Report.

Unit tests worth writing before the analysis, because each of these has silently produced
a wrong answer for someone before:

- share-matched vs dollar-matched basket cost (assert they differ, assert which is used)
- VWAP with `n_contracts` exceeding total ladder depth
- fee curve evaluated at P = 0.02, 0.50, 0.98
- a synthetic basket with a known 2¢ gap recovers exactly 2¢ gross
- a synthetic basket built from two snapshots 45s apart is flagged, not scored