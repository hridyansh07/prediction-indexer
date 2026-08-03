# Partition-Sum Pipeline v1

## Purpose

This pipeline answers one question before correlation work begins:

> Do the existing EWC Dota 2 orderbooks contain a size-adjusted, fee-adjusted
> partition gap that survives strict timestamp-skew controls?

The EWC manifest contains match-winner moneylines only. It is suitable for this
economic test and for plumbing controls, but it must not produce novel
cross-event correlation candidates.

No Oddpool trade history is fetched by this pipeline.

## Frozen configuration

`configs/partition_sum_v1.json` is the machine-readable source of truth. A run
is addressed by the hashes of that configuration, the playoff manifest, the
Oddpool history job, the fee metadata, and the analysis code.

The v1 decision parameters are:

- 60-second UTC bars
- snapshots no older than 60 seconds
- low skew means `leg_skew < 5 seconds`
- sizes `1, 10, 25, 50, 100, 250, 500, 1000` contracts
- the economic gate is evaluated at 100 contracts
- an event needs at least 100 valid low-skew observations
- a class passes at a net-positive rate of at least 1% in at least two events
- long baskets can pass; short baskets are diagnostic only

These values are fixed before looking at the partition results.

## Stages and hard gates

### 1. Resolve inputs and fees

Read the completed Oddpool orderbook job without making Oddpool requests.
Resolve Polymarket fees from the cached market-level `feeSchedule`. Resolve the
Kalshi fee type and multiplier from its public series metadata and historical
fee-change endpoint using the durable HTTP cache.

Every fee record carries its source URL, source file hash, effective timestamp,
formula parameters, and rounding assumption. Missing or conflicting fee
metadata permits gross measurement but makes the final status `DATA_INVALID`.

### 2. Normalize and validate books

Normalized probability instruments have:

```
instrument_id, venue, event_key, market_id, asset_id, position
snapshot_ts, bids[(price,size)], asks[(price,size)], provenance
```

Polymarket token bids and asks are used directly. For a Kalshi ticker:

```
YES bids = yes_bids
YES asks = [(1 - no_bid_price), size]
NO bids  = no_bids
NO asks  = [(1 - yes_bid_price), size]
```

All ladders must be ordered, have unique levels and non-negative sizes, and
reproduce the stored top of book. Manifest membership and provenance are
mandatory. Level-count distributions must not show a fixed top-N ceiling.

For each covered Polymarket condition, paired token snapshots within 100 ms
must mirror one another at a rate of at least 98%. Confirmed-zero conditions do
not fail validation; they simply cannot form observed partitions.

### 3. Build partitions

Four classes are emitted:

| Class | Construction | Gate role |
|---|---|---|
| `SMOKE_PM_CONDITION` | Both tokens of one Polymarket condition | correctness only |
| `SMOKE_KALSHI_CONTRACT` | YES and NO of one ticker | correctness only |
| `PARTITION_KALSHI_EVENT` | Complete team-outcome positions in one mutually-exclusive event | economic |
| `PARTITION_CROSS_VENUE` | Complementary team positions across venues | conditional economic |

Economically equivalent trade representations share an
`economic_partition_id`. Each route retains a distinct `representation_id`.
The cheapest fully fillable representation is selected independently for every
bar, direction, and size. Non-selected routes remain in the output for audit.

Separate Kalshi event markets and cross-venue markets retain their rules hashes,
resolution sources, and partition-risk status. A cross-venue pass is not called
locked arbitrage until a later fungibility review clears its resolution risk.

### 4. Measure

At each epoch-aligned UTC bar boundary, use the latest snapshot at or before
the boundary. Never look ahead and never carry a quote for more than one bar.

For a long basket of `N` contracts:

```
cost(N)       = sum(walk_ask(leg, N).cost)
gross_gap(N)  = 1 - cost(N)
net_gap(N)    = gross_gap(N) - conservative_taker_fees(N)
profit(N)     = N * net_gap(N)
```

Every leg must fill all `N` contracts. Partial fills and one-sided books are
recorded but invalid. Short baskets walk bids and report
`sum(proceeds) - 1`; they are marked diagnostic-only.

The true source timestamps survive into every row. Results are stratified into
`lt_5s`, `5_to_15s`, `15_to_60s`, and `gt_60s`. Only `lt_5s` can pass.

### 5. Gate and report

For each gate-eligible class and event at 100 contracts, collapse alternative
representations to the selected route. An event qualifies only with at least
100 valid, fee-complete, low-skew long observations.

The terminal status is:

- `DATA_INVALID`: a book, complement, provenance, or fee hard gate failed.
- `INSUFFICIENT_LOW_SKEW_DATA`: neither economic class has two qualifying events.
- `EWC_ECONOMIC_PASS`: `PARTITION_KALSHI_EVENT` reaches a 1% positive rate in
  at least two qualifying events.
- `EWC_CONDITIONAL_CROSS_VENUE_PASS`: only the cross-venue class reaches that
  threshold.
- `EWC_NO_SIGNAL`: the data is valid and sufficiently sampled, but neither
  economic class passes.

If only high-skew buckets contain positive gaps, the report labels the result a
sampling artifact.

## Durable outputs

Each content-addressed run directory contains:

```
run_manifest.json
fee_metadata.json
validation.json
normalized_books.parquet
partition_definitions.json
partition_observations.parquet
summary.json
report.md
stages/*.json
```

Parquet rows are deterministic and sorted. JSON and Markdown files omit
wall-clock generation timestamps so an identical rerun has identical content.
Stage manifests contain input/output hashes, row counts, parameters, and status.

## Correlation hold

Correlation remains out of this implementation. It can resume only after the
partition test passes or is replicated on the World Cup and Kalshi Fed datasets,
and only against a dataset with real sibling market types. The later
correlation spec must incorporate the 60-second sampling, usable-bar gate,
adaptive permutation blocks, no metric imputation, hard identity-control
recall, and maker-overlap requirements in `review.md`.

