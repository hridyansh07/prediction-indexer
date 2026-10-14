# Execution delay in the economic SDK

Status: **proposed**. Extends [ECONOMIC_STRATEGY_SDK_V1.md](../ECONOMIC_STRATEGY_SDK_V1.md)
§13 (fill checks), [STRATEGY_PREPARATION_V1.md](../STRATEGY_PREPARATION_V1.md) and the
Universe outcomes document ([universe/README.md](../../universe/README.md)).
Companion specs: [MARKET_VOLATILITY_V1.md](MARKET_VOLATILITY_V1.md) and
[STRATEGY_EXECUTION_REALISM_V1.md](STRATEGY_EXECUTION_REALISM_V1.md). This spec owns
the mechanism. The strategy spec owns which strategies use it and with what settings.

## 0. Why

Fill checks today price a fill on the book at the trigger instant `t0`. A bot sees
that book only after it arrives, and its orders reach each venue later still, so
§13's "Fills measure apparent edge, not executable trades" is lookahead for any
latency above zero. A read-only screen over 92 corpus events re-filled every
plausible cross-venue episode at `t0 + L`:

- At L = 0 it reproduced the SDK's opening fills exactly (15,448 of 15,448 episodes).
- At L = 500 ms half the episodes filled nothing, half the legs mismatched, and net
  edge after leg losses fell from $18,532 to $1,190.

Polymarket adds a venue delay on top. Its help centre says "sports markets include a
3-second delay on the placement of marketable orders"
([source](https://help.polymarket.com/en/articles/13364444-limit-orders)). Its order
lifecycle docs describe a `delayed` status: the order "waits for the market's
configured delay window before matching", "cannot be canceled" during it, and is
"placed on the book after the delay expired without a match"
([source](https://docs.polymarket.com/concepts/order-lifecycle)). The Gamma market
record carries it as `secondsDelay`. Cached esports records show 1 s (472 markets)
and 3 s (143 markets).

This spec adds three things:

1. **Market execution terms:** each market's taker delay, carried from the archived
   target records to the strategy.
2. **Delayed-fill probes:** decide at a decision time, arrive per leg after latency
   plus venue delay, fill against the book at arrival.
3. **Opening quote ages.**

All are opt-in. Without them every existing output is byte-identical.

## 1. Market execution terms

### 1.1 Source

`target_records_<venue>.ndjson[.zst]` (targeter/v2/DELIVERY.md, "Target records")
keeps the venue's raw record for every selected market on every run. The terms are:

| Venue | Field | Value |
|---|---|---|
| polymarket | `record.secondsDelay` | non-negative integer seconds → `taker_delay_ms = secondsDelay × 1000` |
| kalshi | none declared | `taker_delay_ms = null` (unknown) |
| limitless | none declared | `taker_delay_ms = null` (unknown) |

- A missing, non-integer or negative `secondsDelay` is `null`, never 0.
- A run with no target-record row for a selected market, or a run archived before
  target records existed, contributes no observation for that market. Absence stays
  visible (§1.3).
- `replay/catalog.py`'s Polymarket projection gains `secondsDelay`
  (`PROJECTION_VERSION` 2, so `projection_id` `polymarket.v2`). That records the field
  as consumed. Projection hashes of old rows are recomputed only by new writers.

### 1.2 Universe

- Universe sync already reads each run's catalogue artifacts. It also reads that
  run's `target_records_*` artifacts through the same verified-identity path.
- It stores one row per (run, venue, venue_market_id): `taker_delay_ms` (nullable)
  and `observed_at` (the record's `observed_at`).
- It is a new STRICT table in the rebuildable Event Universe database. Build on
  `fix/universe-totals-direction` (head `aee4002`) and stay inside the same unreleased
  schema `user_version` 7 bump
  ([UNIVERSE_TOTALS_DIRECTION_V1.md](UNIVERSE_TOTALS_DIRECTION_V1.md)), so both changes
  ship in **one** rebuild. `universe.tools.rebuild_diff` also reports this table: rows
  in the new file per venue, and markets whose terms are `observed` against
  `unavailable`.
- A malformed target-record artifact is a sync failure, as for catalogue artifacts.
  It is not silently skipped.

### 1.3 Outcomes document version 2

`GET /v1/bundles/<id>/outcomes` goes from `version: 1` to `version: 2`. Each entry of
`markets` gains a closed object:

```json
"execution_terms": {
  "status": "observed",
  "changes": [{"from": "2026-08-26T19:40:04.5Z", "taker_delay_ms": 1000}]
}
```

- `changes` are the bundle's selection runs in `generated_at` order, collapsed to
  change points: a row is written only when `taker_delay_ms` differs from the
  previous one, including a change to or from `null`. `from` is that run's
  `observed_at`.
- `status` is `observed` when at least one run observed the market, else
  `unavailable` with `changes: []`.
- The value in force at time `t` is the last change with `from ≤ t`. Before the
  first change it is `null`.
- Version-1 documents stay readable. A strict reader that accepts both treats v1 as
  `unavailable` for every market.

### 1.4 Preparation

`context.json` already pins the outcomes document (`outcomes.document`).
`load_snapshot` validates the v2 closed schema, including change-point order and
types, and rejects unknown fields. No new snapshot version is needed: the document
carries its own version.

## 2. Delayed-fill probes

### 2.1 Policy

`FillPolicy` gains an optional closed `probes` object. Its absence means no probes,
no new files and byte-identical output:

```json
"probes": {
  "version": 1,
  "sizings": ["edge", "t10"],
  "confirm_ms": [0, 500, 1000],
  "arrival_ms": [100, 250, 500, 1000, 2000],
  "sequencing": ["simultaneous", "slow_first"],
  "unknown_taker_delay_ms": {"kalshi": 0, "limitless": 0, "polymarket": 3000}
}
```

- `sizings` name declared sizings, governing or recording.
- `confirm_ms` and `arrival_ms` are strictly increasing non-negative integers with
  at most 8 entries each.
- `sequencing` is a non-empty subset of the two modes in §2.4.
- `unknown_taker_delay_ms` is required for every venue a strategy can trade, and is
  used when the terms give `null` (§1.3).
- The product |sizings| × |confirm_ms| × |arrival_ms| × |sequencing| is at most 64.
  The example is exactly 60.

### 2.2 Strategy hook

`fill_delays(entity, time_ns) -> tuple[int | None, ...]`: per leg, the venue taker
delay in nanoseconds for an order sent at `time_ns`, from the snapshot's execution
terms. `None` means unknown, and the SDK substitutes `unknown_taker_delay_ms`. The
hook is pure: it depends only on its arguments and the pinned snapshot, because the
reader calls it.

### 2.3 Decision

For each opened fill episode (opening time `t0`) and each `c` in `confirm_ms`, the
decision time is `d = t0 + c`:

- **`c = 0`:** the decision is the opening fill itself: the same steps and per-leg
  walked levels.
- **`c > 0`:** the decision exists only if the fill is still live at `d`, meaning
  episode `end_ns > d`. Otherwise every probe for that `c` has status
  `not_confirmed`. If live, each named sizing is walked again at `d` against the
  views committed at `d` (the timing rule of §2.6). A sizing that is not
  tradeable at `d` gives status `decision_untradeable`.
- **Per leg,** the decision fixes the quantity, `steps × units`, and the **limit**,
  the worst price the decision's walk took on that leg.

### 2.4 Arrival and sequencing

For each `L` in `arrival_ms`, each leg has a total delay `D_leg = L + venue delay`,
where the venue delay comes from `fill_delays(entity, d)` or the unknown default.

- **`simultaneous`:** every leg is sent at `d` and arrives at `d + D_leg`.
- **`slow_first`:**
  - The leg with the greatest `D_leg` is sent at `d`; ties go to the lowest leg
    index. It arrives at `a₁ = d + D_slow`.
  - Its filled quantity is known at `a₁`. Every other leg is then sent at `a₁`,
    sized to the matched quantity in steps, floored (§2.5), and arrives at
    `a₁ + D_leg`.
  - If the first leg filled nothing, the others are not sent: status `empty`.

### 2.5 Fill at arrival

At each leg's arrival `a`, the leg walks its fill source ladder (`FillSpec.sources`,
for example `ask` or `kalshi_complement_ask`) in the views committed at `a`:

- It takes best-first, up to the leg's quantity, at prices **no worse than its
  limit**.
- Better prices count: a taker order takes them.
- If the book is not usable at `a`, the leg fills zero and is reported `unusable`.
- An unfilled remainder is assumed cancelled at `a`. A Polymarket order would rest
  on the book after its delay; resting fills are not modelled in V1.

Then:

- **`matched_steps`** is the minimum over legs of `floor(filled_atoms / units)`.
- **Excess** on each leg is `filled_atoms − matched_steps × units`.
- **`value`** is `fill_value(entity, matched_steps, matched_legs, d)`: the strategy's
  own fee-inclusive value of the matched quantity, at decision time `d`.
  `matched_legs` takes each leg's cheapest `matched_steps × units` atoms of what it
  filled. `null` when unknown; 0 when `matched_steps` is 0.
- **Excess unwind:** each excess quantity is sold immediately into that leg's exit
  ladder at its own arrival time. The exit ladder is `bid` for a bought leg, or
  `kalshi_complement_bid` for a bought Kalshi complement; the reverse for a sold
  leg. The walk is best-first and records the proceeds.
  - `leg_loss_gross` is excess cost minus exit proceeds, summed over legs.
  - Fees on the unwind are **not** modelled in V1. The field name says gross, and the
    loss is a lower bound.
  - The strategy's `BookRequirement.ladders` must retain every exit ladder its
    probes need.
- **Status:**
  - `filled`: every leg fully filled;
  - `partial`: `matched_steps` is above 0 but below the decision steps;
  - `empty`: `matched_steps` is 0;
  - `unusable`: a leg's book was not usable at arrival;
  - `censored`: an arrival is after the scope or run end. The probe is unresolved and
    not counted as `empty`.

### 2.6 Timing

Probes are causal and resolve in stream order:

- A pending evaluation at time `a` uses `self.views` at the commit of the greatest
  staged time ≤ `a`. The SDK must evaluate it **before** any later cut refreshes the
  views: in `_flush_stage`/`_advance`, alongside fill opening and timers (§13
  Timing).
- A probe outlives its episode: an episode that ends before `a` still has its probe
  evaluated at `a`.
- Probes with equal times resolve in the order: episode start, sizing, `c`, `L`,
  sequencing, leg.
- Pending probes are charged by `bounds` like open fill state. A run that would
  exceed its bound fails at the attempt that adds the probe, as for fills.

### 2.7 Output

Probes go to their own file, `probes.ndjson`, listed in the manifest. Episode rows
are unchanged, so the 64 KiB episode line limit is not touched. Each row is one
probe:

```json
{"episode_id": "...", "sizing": "t10", "confirm_ms": 500, "arrival_ms": 250,
 "sequencing": "simultaneous", "status": "partial",
 "decision": {"time_ns": "...", "steps": "1000", "limits": ["4100", "5800"]},
 "legs": [{"sent_ns": "...", "arrival_ns": "...", "venue_delay_ms": 1000,
           "delay_source": "terms", "filled_atoms": "...", "cost": "...",
           "consumed": [["4100", "700"]], "excess_atoms": "0",
           "exit_consumed": [], "exit_proceeds": "0"}],
 "matched_steps": "700", "value": "...", "leg_loss_gross": "0"}
```

- `delay_source` is `terms` or `default`.
- `consumed` and `exit_consumed` are best-first and bounded by `max_levels`.
- Integers are canonical decimal strings, as in fill objects.
- Rows are written in resolution order (§2.6). Every configured probe of every
  episode has exactly one row, so the row count per episode is the configured
  product.

### 2.8 Reader

`aggregate_reader` additionally checks:

- the probe policy, and that each episode has exactly its probe rows;
- each decision against the episode's fill: for `c = 0`, steps and limits equal the
  opening result;
- each leg's walk structurally: prices no worse than the limit, best-first,
  quantity, and cost = Σ p·q;
- `matched_steps`, excess, and `value` through `fill_value` at `decision.time_ns`;
- `leg_loss_gross` arithmetic;
- arrival and send times from `fill_delays` plus the policy;
- status from the leg results.

Writer-attested: the ladders at arrival and at decision for `c > 0`. The reader
cannot see the book, which is the same limit as fill checks' no-fill split.

### 2.9 Wire v3 (REPLAY_THROUGHPUT_V1 T1)

Wire v3 publishes only non-empty cuts, window cuts, and cuts at declared retained
boundaries and time-shift deadlines. Probes must not depend on a cut arriving at a
decision time or an arrival time.

- Pending decisions (`c > 0`) and arrivals are **caught up**: before a cut with time
  `> x` refreshes the views, every pending evaluation with time ≤ `x` resolves
  against the current views.
- The run's terminal flush resolves any remaining evaluations that fall inside the
  run. Arrivals after the run end are `censored`.
- This is exact, because an omitted cut changes no planned book: the views at `x`
  equal the views at the last published cut ≤ `x`.
- Probe outputs carry the computed times (`time_ns`, `sent_ns`, `arrival_ns`), never
  the time of the cut that triggered resolution.
- Probes therefore declare **no** `retain_boundaries_ns` or `retain_shift_offsets_ns`.
  Declaring a 100 ms-scale grid there would undo T1.
- Opening quote ages (§3) measure planned books' own updates, which omitted cuts
  never touch.

Acceptance (§5) runs with wire v3. A test publishes the same tape at v2, with every
cut, and at v3, with empty cuts omitted, and requires identical `probes.ndjson`
bytes.

## 3. Opening quote ages

The fill object (and each probe decision for `c > 0`) gains `ages`, one entry per
leg: `{"book_ns": ..., "best_ns": ...}`.

- `book_ns` is the time since that leg's book last committed any update.
- `best_ns` is the time since the best level (price or quantity) of the leg's walked
  ladder last changed.

Both are nanoseconds before the instant priced. The SDK tracks per book the
committed time of the last update and of the last best-level change, per retained
ladder. Writer-attested, with bounds unchanged: two integers per book.

## 4. Tests

- **Offline synthetic tapes** with `replay/tests/test_economic_sdk_fills.py`'s
  two-leg strategy:
  - zero latency reproduces the opening fill;
  - depth eaten before arrival gives `partial`;
  - a price beyond the limit at arrival gives `empty`;
  - a better price at arrival is taken;
  - a venue delay from terms changes `D_leg`, and from `default` when terms are null;
  - `slow_first` sizes the second leg to the first;
  - an excess leg's unwind proceeds;
  - `not_confirmed`, `decision_untradeable`, `unusable` and `censored`.
- **Causality:** a cut after the arrival time never affects the probe.
- **Equal-time ordering:** shuffled input order gives identical bytes.
- **Byte identity** without `probes`, including the complement V1 golden.
- **Outcomes v2** writer and strict reader; v1 is still accepted.
- **Universe sync** of target records:
  - a missing artifact gives `unavailable`;
  - a malformed one fails;
  - change points collapse.

## 5. Acceptance

- Run the bench on the five-bundle sample (`.bench/corpus-sample/6dd9f00ff1cce627`
  inputs, with probes on).
- For `simultaneous`, `c = 0`, sizing `edge` and venue delays forced to 0, the
  independent screen (`delayed_fill.py`, kept with the corpus analysis) must agree
  on every episode's `matched_steps`. Values may differ only by its approximate fee
  model.
- Report the per-status counts.

## 6. Not in V1

- Fees on the excess unwind.
- Resting (passive) fills of delayed orders.
- Queue position.
- Per-leg latency distributions instead of fixed `L`.
- Probes on episodes that never opened a fill.
- Kalshi or Limitless delay sources, none known.
