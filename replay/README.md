# Standalone replay

Replay accepts only named immutable byte objects through `ByteStreamer`. Storage
selection is therefore an adapter decision:

- NFS/local: `DirectoryByteStreamer`
- tests and exact fixtures: `MemoryByteStreamer`
- receipt-committed raw objects: `archive.ArchivedSegmentByteStreamer`
- receipt-committed canonical windows: `archive.ArchivedCanonicalByteStreamer`
- complete Targeter runs: `targeter.v2.replay_stream.ArchivedTargeterRunByteStreamer`
- capture-window Targeter records: `targeter.v2.replay_stream.ArchivedTargetRecordByteStreamer`

`CompositeByteStreamer` snapshots disjoint adapters into one lexically ordered
dataset and rejects duplicate logical keys. Gate 1 can therefore consume raw
segments and decoded `target_records_<venue>.ndjson` together without learning
about S3, Zstandard, or either receipt protocol.

Replay reconstruction imports no capture, ingester, targeter, or legacy-analysis
module. The optional pre-run `replay.preparation` boundary reuses Universe/Targeter
historical evidence validators and archived streaming adapters; it freezes a
hash-bound context before runtime. See
[`STRATEGY_PREPARATION_V1.md`](../docs/STRATEGY_PREPARATION_V1.md) for its closed
configuration, native book plans, explicit historical expectation scopes, and
offline snapshot loader. The offline `replay.bundle_coverage:build` strategy uses
that context and immutable Risk cuts to report book/member/bundle intervals under
policy, not vendor completeness or opportunities. Its strict completed reader
requires both content identity and supervisor success; see
[`BUNDLE_COVERAGE_V1.md`](../docs/BUNDLE_COVERAGE_V1.md). No shared cache is included.

`python -m replay.bench` provides reusable local preparation with outcomes warm-up,
context comparison, declarative fee catalogs, disposable single-attempt Docker
runs, strategy-owned readers/checks, and run comparison. Inputs are mounted read-only;
failed evidence is retained and writing commands require a new output directory.
Preparation consumes only exported `UNIVERSE_BASE_URL`. See
[LOCAL_REPLAY_BENCH_V1.md](../docs/LOCAL_REPLAY_BENCH_V1.md) and the placeholder
examples in [configs/bench](../configs/bench/README.md).

Economic strategies build on `replay.economic_sdk`
([`ECONOMIC_STRATEGY_SDK_V1.md`](../docs/ECONOMIC_STRATEGY_SDK_V1.md)). A strategy
declares its book requirements, its baskets, and a pure `evaluate`. The SDK owns:

- time and same-time staging;
- shared detached book views;
- per-key denominators, episodes and slices;
- controls;
- opt-in fill checks: strategy-declared governing and recording sizings, one priced
  fill per episode, ended by a per-leg kill price;
- bounded output;
- the independent reader.

`replay.same_venue_complement:build` is the first such strategy
([`SAME_VENUE_COMPLEMENT_V1.md`](../docs/SAME_VENUE_COMPLEMENT_V1.md)).
`replay.market_profile:build` writes per-book trading data points: state time,
spread, depth and cost to fill, activity, quote survival, self-crossing, and pair
consistency. It can run as its own group, or inside any SDK strategy that enables
it.

The package is included in the installed distribution, including the frozen
terminal policy. Storage adapters provide bytes through `ByteStreamer`; none of
the replay, trust, economics, or execution code changes.

Target-record run selection includes every run in the half-open capture window
and the latest run strictly before it. Production run archive receipts remain
the authority: prefix listings are not accepted as commit evidence. Each read
freshly verifies the receipted remote manifest and selected object, fully stages
and verifies the decoded logical identity, and only then yields bytes.

`replay.economic_sdk.outcomes.outcome_scope` exposes frozen normal-resolution masks, payoffs, implications, and exhaustive complete sets during basket construction; see [OUTCOME_MASKS_V1.md](../docs/OUTCOME_MASKS_V1.md).

`replay.same_venue_implication_cover:build` and
`replay.cross_venue_implication_cover:build` share one core for buying a superset
and the complement of its strict subset. They report the normal-resolution
payout floor separately from the extra middle payout, with exact fee-adjusted
outcome vectors and completed bench readers; see
[IMPLICATION_COVERS_V1.md](../docs/IMPLICATION_COVERS_V1.md).

## Ordered exit gates

Work advances only after the preceding gate is demonstrated against real venue
bytes.

1. `python -m replay.gate1 DATASET_ROOT` must pass every irreversible capture
   check. A report is content-addressed to its complete input object manifest.
2. Every analysis output must embed interval trust, coverage percentage,
   Polymarket hash-match rate, and leg-skew strata. Bare numeric output is a type
   error.
3. The economic headline is deployable-ticket VWAP net of conservative fees.
   Episode counts and uncapped gaps are diagnostics only. The subset LP ships
   together with its matched-leg placebo null.
4. Live replay reports quote lifetime and an explicitly named fill estimator;
   a detected opportunity is never labelled captured or filled.
5. Thresholds, controls, placebo construction, and resolution reconciliation
   are hashed before observations are evaluated. The terminal verdict includes
   `NO`.

Gate 1 is intentionally strict. A failed check is a capture-side gap and blocks
all later analysis; it is not an invitation to filter the fixture until it passes.

Run the complete sequence:

```bash
python -m replay.gate1 DATASET_ROOT --output gate1.json
python -m replay.gate2 DATASET_ROOT --output gate2.json
python -m replay.gate3 DATASET_ROOT --output gate3.json
python -m replay.gate4 DATASET_ROOT --output gate4.json
python -m replay.gate5 DATASET_ROOT --output gate5.json
```

Gate 5 validates and hashes `policy.json` before evaluating any tape bytes.
Its negative label is deliberately scoped:
`NO_DEPLOYABLE_EDGE_IN_FIXTURE`, never a claim that no edge can exist elsewhere.

## Trust and recovery semantics

- Polymarket state hashes may repeat across several frames in one logical
  update. Replay applies the complete hash run before checking the state.
- An independent snapshot mismatch opens `UNTRUSTED`; its full book recovers
  the chain, and later hashes are evaluated from that recovery point.
- Limitless full books are exact observations, but its non-dense monotonic
  version cannot prove that no update was dropped. Its completeness verdict is
  therefore `UNKNOWN`.
- Conflicting resolution fields are retained and labelled. In the live fixture,
  Limitless ETH records carried a stale `chainlinkPair=BTC/USD` alongside
  ETH/USD in the title, symbol, URL, and rules. Redundant-field consensus
  displays ETH/USD, while `CONFLICT` prevents exact cross-venue identity.

## Economic and execution semantics

The economic headline is a same-condition, share-matched long basket at 100
contracts. Each leg walks displayed depth and uses the captured fee curve with
conservative per-leg rounding. The symbolic LP solves the payout-cover problem
at the same ticket VWAP. Its matched placebo replaces one leg with the
nearest-time different condition while retaining the same symbolic incidence
matrix; the placebo is a null, never a locked basket.

Execution uses the named `DISPLAYED_DEPTH_SURVIVAL_100MS` estimator. It measures
how long the exact ladder slice required for the ticket remains unchanged. It
does not observe queue position, order acknowledgements, trades, or fills, and
cannot label a quote as captured or filled.

`replay.cross_venue_arbitrage:build` measures two-leg all-BUY complete sets across
venues using the pinned static mask API, native-scale fills, Fee SDK assessments
and an explicit parity valuation scenario. Results are normal-resolution research
detections. Output format 2 keeps size-independent rejection denominators once
per scope with a null size. The SDK checks exact real/control entity-table bytes
before opening outputs; admitted size-specific route identities are preserved.
See [CROSS_VENUE_ARBITRAGE_V1.md](../docs/CROSS_VENUE_ARBITRAGE_V1.md)
for configuration, preparation from `UNIVERSE_BASE_URL`, independent reading and
the bounded pilot recipe.

`replay.same_venue_multi_market:build` measures all-BUY complete sets of two to
four books across one venue's markets, enumerated from the pinned masks, priced
with native-scale SDK fill checks and exact Fee SDK assessments. Sets inside one
market are the complement's domain and are excluded; unmasked books and venues
without a set stay visible as rejected rows. See
[SAME_VENUE_MULTI_MARKET_V1.md](../docs/SAME_VENUE_MULTI_MARKET_V1.md).
