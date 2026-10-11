# Offline review repair measurements

These are synthetic offline measurements on the development macOS host using
the project Python environment. They measure hypothetical accounting and file
verification, not live execution, latency or profitability. Scratch inputs and
outputs use temporary directories; no captured evidence is modified.

The policy is the recommended **100,000 evaluations across one whole decision**,
quantity increment 1 and cap 100. Actual evaluations can be fewer because exact
bounds terminate a problem, or because the shared pool reserves a deterministic
share for remaining detection/entry searches. Zero is analytical. Partial-bound
nodes and evaluated nonzero portfolios both debit the same pool.

## Economic runs

Every economic run below uses preparation, the real stream Decoder, the
strategy, receipt-last output and the independent provisional economic reader.
Each update changes one native book; the other books remain observable. Times
are elapsed wall time and vary with host load.

| Fixture | Updates | Writer | Supervised finish | Independent economic audit | All output bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Certified negative: 12 Bo3 books, six exhaustive outcomes, asks .900/.901, known zero fees | 100,000 | 138.428 s | 0.00179 s | 69.142 s | 14,829,941 |
| Fee-positive: two complementary Kalshi claims, quadratic collateral fees, asks .47/.48 and .48 | 1,000 | 18.427 s | 0.00317 s | 8.398 s | 4,229,394 |
| Hard overlapping positive: 12 Bo3 books, six outcomes, asks .600/.601, known zero fees | 100 | 11.121 s | 0.00543 s | 0.962 s | 222,749 |

The hard run's initial decision took **1.393 s**, also its worst measured
supervised callback. Its last decision used 50,000 evaluations from the 100,000
global pool. Initial decision time is reported separately from the repeated
writer time in the table. No measured callback or finish approached the example
bench's 120 s stall limit. The reader audit runs after transport completion,
outside the supervised finish callback, and still independently compares the
writer's full summary.

The negative measurement was taken after the compiled bounds, primitive cache,
shared budget and fresh fee-proof repairs, before the final transport pool and
reader fee-availability memoization refinements. Those refinements passed the
final regression suite. The fee-positive and hard measurements use the final
writer/search and independent native arithmetic; the final reader additionally
checks advertised scalar bound/frontier metadata. These measurements do not certify every possible 128-book,
1,024-outcome, multi-valuation domain against the stall limit.

The actual negative event completed writer plus audit in about **208 s**. A
linear projection of the 1,000-update fee-positive pattern to 100,000 updates is
about **2,683 s (44.7 minutes)** for writer plus audit. That positive projection
is not an actual 100,000-decision economic run. Repeating the measured hard
pattern 100,000 times projects about **3.4 hours**. A finite node budget therefore
does not promise universal completion within the example's one-hour attempt
limit; users need to choose a smaller budget or longer attempt for hard domains.
Limited searches retain a feasible certificate or an explicit unknown result.
An uncertified nonpositive search cannot establish a negative interval or rearm.

The earlier local 12-book fixture took 11.456 s for one decision and 11.525 s for
its writer-coupled replay audit. This differs from the independent review's
27.5 s decision / 55 s self-check fixture. The repair removes both per-node rich
proof construction and the writer's end-of-run optimizer replay: scalar mask
arithmetic and exact gross bounds drive search, retained winners receive fresh
native fee certificates, and the reader checks certificates and native flows.
It does not certify unrestricted positive-search optimality by rerunning the
grid. `SEARCH_COMPLETE` positive grid coverage remains a writer attestation;
negative dual bounds and attained objective bounds are independently checked.

## Positive output-volume rehearsal

A separate run wrote **100,000 rows in each** of `decisions.ndjson` and
`episodes.ndjson`, using two real fee-positive economic row shapes with all
held-position, portfolio, source-level and fee fields. It changed times and
substituted synthetic assessment hashes for new decisions. It then independently
checked canonical JSON, dense indices, closed transport pools/patches, expanded
references, line/file limits and byte identities for every row.

This is **transport-only volume evidence**, not an economic reassessment of
100,000 decisions.

| File | Actual bytes | SHA-256 |
| --- | ---: | --- |
| `decisions.ndjson` | 323,935,660 | `54d3eefd874e46f342fa06ef2accce9a6f065b14a2d7dd4e92005bd1b3276941` |
| `episodes.ndjson` | 106,651,171 | `3bd4a44fb7b9d44ae3e6490d308d7f17bf04aa31b29b1823e822ed62207105c7` |

The combined NDJSON size was **430,586,831 bytes (410.64 MiB)**, below 512 MiB
even combined; each individual file satisfies its 512 MiB cap. Writer time was
329.849 s and the full two-file transport audit took 292.768 s. Records were not
truncated. The actual 1,000-decision positive economic run additionally audited
every native fee proof, action, ledger, episode and summary.

Version-2 transport interns exact assets, immutable native models, orders and
portfolios, then carries deterministic checkpoints and deltas. Economic objects
are expanded before validation. The transport only removes repetition; it does
not change payoff vectors, fees or source-capacity accounting. Other strategies'
formats and all raw/prepared evidence remain untouched.

## Reproduction and regression gate

Use the project virtual environment from the repository root:

```bash
.venv/bin/python -m replay.tests.optimizer_review_benchmark \
  --negative-updates 100000 --positive-updates 1000 \
  --volume-rows 100000 --hard-updates 100
.venv/bin/python -m unittest replay.tests.test_bounded_payout_optimizer \
  replay.tests.test_strategy_packages replay.tests.test_bench
```

The benchmark emits separate JSON records for economic fixtures and the clearly
labelled synthetic-hash transport rehearsal. `worst_callback_seconds` includes
the initial decision, per-update callbacks and final flush. Temporary outputs
are removed only by their owning temporary-directory scope.

The final focused/package/bench gate passed **80 tests in 5.290 s**, including
56 optimizer tests. Falsifying regressions cover writer valuation and settlement
corruption, false complete negatives with missing, crossed or unrepresentable declared inputs, mismatched
rule-event pins in provisional reading, hidden positives through false unavailability, whole-decision
budgeting, current-time fee identities and changed fee economics, independently
checked negative fee availability, pending scope cancellation, contradictory
knowledge, skipped-entry reasons, malformed settlement errors, bounded canonical
transport, cache byte accounting, false advertised bound/frontier metadata and
finish outside the long audit. Independent
small Cartesian domains test exact branch bounds with nonlinear fees. The real
bench SUCCESS reader remains part of this gate. The separate existing economic
SDK/game/preparation/profile/complete-set/implication-cover/package/bench
compatibility gate also passed 255 tests.
