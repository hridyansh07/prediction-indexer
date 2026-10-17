# Strategy changes for execution realism

Status: **proposed**. Applies the mechanisms of
[EXECUTION_DELAY_SDK_V1.md](EXECUTION_DELAY_SDK_V1.md) and
[MARKET_VOLATILITY_V1.md](MARKET_VOLATILITY_V1.md) to the corpus strategies, the
research build and the corpus run. It is the last set of changes before the full
corpus rerun that sizes the $200 live pilot.

## 0. Scope

- **In scope:** the four fill-mode strategies:
  - `cross_venue_arbitrage` (policy 3 → 4);
  - `same_venue_multi_market` (1 → 2);
  - `same_venue_implication_cover` (3 → 4);
  - `cross_venue_implication_cover` (3 → 4).

  Each policy bump is closed; the old version still parses and writes its old bytes.
- **Unchanged:** `same_venue_complement`, which has no fills and found 0 episodes in
  88 corpus events.
- **The aim:** the corpus should show, for the clip the pilot will actually trade,
  how much edge survives realistic latency, which pairings carry it, and where in
  games and market types it clusters, so the bot can be aimed at them.

## 1. A 10-set clip

- Each strategy's fill policy adds a recording sizing:
  `{"name": "t10", "role": "records", "target_contracts": "10"}`.
- At typical prices one complete set costs about $0.95 to $1.00 with both legs, so 10
  sets is the pilot's roughly $10 clip.
- It is priced at open whatever its value (§13 recording role), so the corpus gets
  the clip's value even when the edge sizing is larger.
- The governing `edge` sizing is unchanged.
- A notional sizing (exactly $10 of spend) is deferred; see §7.

## 2. Delayed fills

Each strategy enables `probes` with the reference settings:

```json
"probes": {
  "version": 1,
  "sizings": ["edge", "t10"],
  "confirm_ms": [0, 500, 1000],
  "arrival_ms": [100, 250, 500, 1000, 2000],
  "sequencing": ["simultaneous", "slow_first"],
  "unknown_taker_delay_ms": {"kalshi": 0, "limitless": 0, "polymarket": 1000}
}
```

- **The `fill_delays` hook** maps each leg to its market and reads that market's
  `execution_terms` from the pinned outcomes document at the decision time. The
  mapping is the entity descriptor's `legs[].market_id`, which matches the outcomes
  document's `markets[].market_id`.
- **The defaults:** 0 for Kalshi and Limitless, which document no taker delay. 1 s for
  a Polymarket market whose terms are unavailable (owner decision, 17 Oct 2026):
  the Universe v8 audit found 1000 ms on all 232,495 Polymarket execution-term
  rows, with observed terms for every market in all 1,212 retired bundles. The
  earlier 3 s maximum was unsupported. A numeric observation, including 0,
  always wins. `delay_source` semantics are unchanged.
- **Exit ladders:** BUY excess sells into the acquired descriptor book's native
  `bid` at arrival, including Kalshi YES and NO. Kalshi entry sources remain
  the opposite orientation's projected bids; they are not the exit books.
  Retain native `bid` and `kalshi_complement_ask`, without reading native asks
  or retaining `kalshi_complement_bid`. Non-Kalshi SELL legs keep the reverse
  mapping; Kalshi sold legs fail closed with probes. The approved
  [R12 amendment](PROBE_EXIT_SOURCE_V1.md) owns binding, timing and compatibility.

## 3. Eligibility flags (recorded, never filtered)

Every fill episode row gains a closed `flags` object:

```json
"flags": {"pairing_class": "map_winner", "settlement_review": true, "stale_legs": [1]}
```

- **`pairing_class`** comes from the legs' `market_type` in the outcomes document:
  - the single type when all legs share one, for example `series_moneyline`,
    `map_winner`, `total_maps`, `total_goals` or `map_handicap`;
  - otherwise `mixed:` plus the sorted distinct types joined by `+`;
  - `unknown` when a leg's market has no outcomes entry.
- **`settlement_review`** is true unless `pairing_class` is in the configured
  allowlist. The allowlist starts as `["series_moneyline"]`. Map markets are where a
  venue was seen staying open while the other priced the map as decided, and totals
  are where the direction bug lived. The allowlist is policy, not code.
- **`stale_legs`** lists the leg indexes whose opening `ages.best_ns`
  (EXECUTION_DELAY_SDK_V1 §3) exceeds `stale_ms`.

The strategy policy carries them:

```json
"flags": {"version": 1, "stale_ms": 5000, "settlement_allowlist": ["series_moneyline"]}
```

The reader recomputes `pairing_class` and `settlement_review` from the snapshot,
and `stale_legs` from the fill's `ages`.

## 4. Research build

These changes land in `replay/research` on the research-site branch.

- **Episodes table:**
  - adds `pairing_class`, `settlement_review`, `stale_legs`, `edge_per_set`
    (value ÷ contracts) and `t10_value`;
  - the viability report gains rows at **≥1¢, ≥2¢ and ≥3¢** per set, beside the
    current ones.
- **New `probes.parquet`:** one row per probe, with episode keys, game, market types,
  pairing class and the probe fields. The viability report gains a "Delayed fills"
  section. For each lens, sizing (`t10` first), sequencing, `confirm_ms` and
  `arrival_ms` it shows:
  - counts by status;
  - the mean fill ratio (matched ÷ decision steps);
  - the median matched sets;
  - Σ value, Σ `leg_loss_gross` and Σ (value − `leg_loss_gross`);
  - the share of events with a positive sum.

  It shows them at each threshold, applied to the decision's own edge per set
  (known at decision time, so causal), and grouped by game, pairing class and
  `settlement_review`.
- **Volatility:** per event and per gap pair, the 1 s and 3 s RMS moves and jump
  rates from the profile's `volatility` rows. The viability report sets each lens's
  delayed-fill leg loss beside the 1 s and 3 s gap RMS of the same pairs.
- **No selection by eventual duration:** "lasted ≥ 500 ms" is known only after the
  fact. Its causal replacement is the `confirm_ms` decision. The episode-duration
  tables stay as research context, labelled non-causal.
- **The site:** the episode panel lists an episode's probes (sizing, `c`, `L`,
  sequencing, status, matched sets, value). Lens layers and charts are unchanged.

## 5. Corpus run

The corpus spec's groups take the new strategy policies with §1–§3. The profile
group adds `volatility`, keeping `transitions` and `levels`. The settings in this
spec are the reference values, and the corpus key binds them.

A new key and one full rerun follow once all of these are merged:

- this spec, EXECUTION_DELAY_SDK_V1 and MARKET_VOLATILITY_V1;
- the Universe totals-direction rebuild, with the execution-terms table in the same
  rebuild;
- the corpus interrupt fix;
- PRs #65, #66 and #67.

Soccer runs under the same key once the rebuilt Universe is live.

## 6. Acceptance

1. **Bench run** on the five-bundle sample: every reader passes, including probe and
   volatility recomputation.
2. **Agreement with the screen:** EXECUTION_DELAY_SDK_V1 §5, with matched steps
   equal.
3. **Reasonableness:**
   - `t10` probes fill more often than `edge` probes at every `L`;
   - `slow_first` has fewer `partial` outcomes with excess than `simultaneous`;
   - Polymarket legs carry `delay_source: "terms"` wherever the bundle's runs had
     target records.
4. **The viability report** shows every new section with non-empty rows for the
   sample.

## 7. Not in V1

- An exact $10-notional sizing.
- Venue minimum order sizes and lot rules. The pilot's engine enforces them. 10
  contracts clears Kalshi's 1-contract minimum and Polymarket's `orderMinSize`,
  which is 5 shares on 6,575 of 6,625 cached markets and 15 on the other 50.
- Account balances and capital lock-up, which are engine state.
- Hard eligibility filters: the engine applies them using these flags.
- Game-phase conditioning, which needs `game_state.prepare`.

## 8. Implementation order

These parts can proceed in parallel:

- **A. EXECUTION_DELAY_SDK_V1:**
  - A1, the SDK: probes and quote ages;
  - A2, Universe and preparation: execution terms and outcomes v2, on top of
    `fix/universe-totals-direction`.
- **B. MARKET_VOLATILITY_V1:** independent of A.
- **C. This spec:** the `t10` sizing and the `pairing_class` and `settlement_review`
  flags can start at once. `stale_legs` and probes wait for A1's interfaces. The
  research build changes can start against hand-written probe rows in A1's
  documented shape.

Each part needs offline tests first, its reader checks, and the five-bundle bench
acceptance before review.

Build on the validated throughput stack (REPLAY_THROUGHPUT_V1): the tip
`perf/t3-derivative-cache`, or its merge. That includes wire v3, so:

- no new time-based behaviour may rely on cuts at its own times;
- probes and volatility catch up (EXECUTION_DELAY_SDK_V1 §2.9, MARKET_VOLATILITY_V1
  §2.1);
- a genuinely new trigger that needs a cut at a time must declare it through
  `retain_boundaries_ns` or `retain_shift_offsets_ns`, or the runtime's coverage
  check fails at bind time;
- fee valuation must go through the shared `FeeBridge`/`assess_net` path to keep
  T2's fee memo.
