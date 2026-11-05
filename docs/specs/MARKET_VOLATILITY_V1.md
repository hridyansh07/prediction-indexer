# Market volatility in the market profile

Status: **implemented**. The [precise contract](MARKET_VOLATILITY_V1_CONTRACT.md)
defines final row shapes, units and bounds; retained acceptance remains in
[open work](../OPEN_WORK.md). Adds a `volatility` group to the market profile, policy
version 2 ([MARKET_PROFILE_V2.md](MARKET_PROFILE_V2.md),
[replay/strategies/market_profile/README.md](../../replay/strategies/market_profile/README.md)).
Companion specs: [EXECUTION_DELAY_V1.md](EXECUTION_DELAY_V1.md) and
[STRATEGY_EXECUTION_REALISM_V1.md](STRATEGY_EXECUTION_REALISM_V1.md).

## 0. Why

An order that lands 0.25 to 3 s after its decision meets a price that has moved in
the meantime. Prediction prices are bounded and jump on game events (a round, a
kill, a map), so a classic diffusion volatility misleads. What execution needs
is the size of price moves **over the delays we actually suffer**, how often they
jump, and how fast a book updates. The same measures over a complete set's
combined cost give the leg risk of a cross-venue pair directly.

The profile already holds the exact top of book for every planned book, the same
state its `transitions` file writes. So this is computed in the same replay pass
and stored with the results. The definitions are written so the execution engine
can compute the identical figures live (§5).

## 1. Policy

`groups` may contain `volatility`. It is allowed only in the standalone profile,
like `transitions`, and requires `transitions` in the same policy (§4). Its closed
configuration sits under `volatility` in the policy:

```json
"volatility": {
  "version": 1,
  "sample_ms": 250,
  "horizons_ms": [250, 500, 1000, 3000, 10000],
  "bucket_ms": 60000,
  "jump_atoms": [100, 200, 500],
  "gap_pairs": "outcome_complements"
}
```

- `sample_ms` and `bucket_ms` are positive, and `bucket_ms` is a multiple of
  `sample_ms`.
- `horizons_ms` is strictly increasing, with at most 8 entries.
- `jump_atoms` are price-atom thresholds (price scale 4, so 100 atoms = 1¢), at
  most 6.
- `gap_pairs` is `null` or `"outcome_complements"` (§3).

## 2. Per-book series

The book set is the written book set of `transitions` (MARKET_PROFILE_V2 §3.1). A
Kalshi market is its Yes book, with asks projected from the complement.

- **Sample grid:** instants `s = scope_start + k × sample_ms` inside the scope.
  Sampling on a clock, not on updates, keeps busy periods from dominating.
- **Mid at `s`:** `bid + ask` in price atoms, kept doubled so it stays an integer.
  It is taken from the state after every cut at or before `s` (the scope gating of
  §3.4). It is defined only when the book is usable and two-sided at `s`.
- **Move:** for each horizon `H` where both `s` and `s + H` lie in the same scope
  and both mids are defined, `Δ = mid₂(s + H) − mid₂(s)` in doubled atoms.
- **Bucket rows:** samples are grouped by the bucket containing `s`
  (`floor((s − scope_start) / bucket_ms)`). One row per (scope, book, bucket) with
  at least one defined sample:

```json
{"type": "book", "scope": 0, "book": 7, "bucket": 12,
 "start_ns": "...", "end_ns": "...", "samples": 240, "two_sided_ns": "...",
 "mid_changes": 31, "top_updates": 58, "spread_sum2": "...",
 "horizons": {"250": {"n": 239, "sum_abs2": "...", "sum_sq4": "...",
                      "max_abs2": "...", "jumps": {"100": 3, "200": 1, "500": 0}}}}
```

- `two_sided_ns` is the exact usable two-sided time inside the bucket.
- `mid_changes` and `top_updates` count changes and top rows in the bucket.
- `spread_sum2` is Σ (ask − bid) × 2 over the defined samples.
- Per horizon: `n` moves; `sum_abs2` = Σ|Δ|; `sum_sq4` = ΣΔ²; `max_abs2` = max|Δ|;
  and `jumps[j]` = number of moves with |Δ| ≥ 2j, using the doubled scale.

All values are exact integers. Means, RMS and percentile-free rolling ("moving")
indices are derived by consumers as sums over consecutive buckets. A 5-minute
moving RMS at 1 s is `sqrt(Σ sum_sq4 / Σ n) / 2` over 5 buckets. Rows are ordered
by (scope, bucket, type, book).

### 2.1 Sparse delivery

The sample grid is a clock, not a cut stream. Under the current
[wire V4 sparse delivery](../REPLAY_STREAMS_V1.md), most cuts are not published,
so samples are **caught up**:

- before a cut with time `> s` refreshes state, every grid instant ≤ `s` that is not
  yet sampled reads the current state;
- the terminal flush samples the rest of the scope.

This is exact, because an omitted cut changes no planned book. The group declares
no retained boundaries; a 250 ms retention grid would defeat sparse delivery.
Bucket rows count `top_updates` and `mid_changes` from top-of-book changes of
planned books, which are identical for full-cut and sparse-cut delivery.

A test publishes the same tape with full and sparse cuts and requires identical
`volatility.ndjson.zst` bytes.

## 3. Complete-set gap series

With `gap_pairs: "outcome_complements"`, the profile builds pairs of two written
books on **different markets** whose bought outcomes are complements. The rule
uses the snapshot's `outcome_books` masks (STRATEGY_PREPARATION_V1 snapshot,
OUTCOME_MASKS_V1):

- the buy-side outcome masks of the two books are disjoint;
- their union is the scope's whole outcome space;
- both masks are `MASKED`, never `UNKNOWN`.

A Kalshi book contributes both its Yes ask and, through the projected complement,
its No ask, as two candidate sides. Each qualifying (book, side) pair is one gap
pair, identified by `(book_a, side_a, book_b, side_b)` in canonical order.

- **Gap at `s`:** `ask_a + ask_b` in price atoms, defined only when both sides
  exist and both books are usable.
- **Gap rows** (`type: "gap"`) carry the same horizon statistics on the gap series,
  plus `below_unit_ns`: the exact time in the bucket with gap < `P`, i.e. a gross
  complete-set edge before fees.
- The pair table (books, sides, markets, mask identities) goes in the summary.

Gap pairs computed this way are a superset of what a cross-venue strategy trades,
and they need no strategy. A pair never established by a mask is never built. It is
a visible false negative, never a guessed equivalence.

## 4. Storage, summary and reader

- `volatility.ndjson.zst` is one Zstandard frame of exact NDJSON (encoder profile).
  Its manifest entry is `{logical, stored}`, like `transitions`.
- The summary gains `volatility`:
  - per book and per gap pair, totals over the run: samples, and per horizon `n`,
    `sum_abs2`, `sum_sq4`, `max_abs2` and jumps;
  - the gap-pair table.
- **The reader recomputes everything from `transitions.ndjson.zst`.** The top rows
  are the complete top-of-book history, which is why `volatility` requires
  `transitions`. It rebuilds the per-sample mids and gaps, every bucket row and the
  summary, and compares exactly. Gap pairs are recomputed from the snapshot masks.
  This makes the group fully reader-proved, with nothing writer-attested.
- **Size check (writer guard):** about 20 books and 12 h at 60 s buckets is about 15k
  rows. A line over 1 MiB fails the run, as for `levels`.

## 5. Live use

A live engine computes the same quantities backward from now: `Δ = mid(now) −
mid(now − H)` on the same `sample_ms` grid. It accumulates the same bucket sums.
Forward (replay) and backward (live) windows have the same distribution over a
long run. Parity tests in the engine repository should feed a recorded
`transitions` file and compare their bucket sums with this group's rows.

## 6. Tests

- **Synthetic tapes:**
  - a constant book gives zero moves;
  - one jump of 300 atoms gives the right horizon counts and `jumps`;
  - a one-sided interval drops samples;
  - a scope boundary never pairs samples across scopes;
  - Kalshi projected asks.
- **Gap pairs:** a complement pair is built; an `UNKNOWN` mask pair is not; same-market
  token pairs are not; `below_unit_ns` is exact.
- **Reader:** recomputation from `transitions` equals the writer. Tampering any row
  fails.
- **Determinism:** shuffled equal-time cuts give identical bytes.
- **Byte identity:** without the group, existing profile outputs are unchanged.

## 7. Acceptance

- Run the market profile with `transitions` and `volatility` over the five-bundle
  sample. The reader must pass.
- Report per game and per market type the 1 s and 3 s RMS move and jump rates.
- Report per gap pair the 1 s and 3 s gap RMS against that pair's cross-venue
  delayed-fill leg loss (EXECUTION_DELAY_V1), as a first check that gap
  volatility explains leg risk.

## 8. Not in V1

- Trade-based (realized trade price) volatility.
- Depth-weighted mids.
- Volatility conditioned on game state (needs `game_state.prepare`).
- An online EWMA form.
