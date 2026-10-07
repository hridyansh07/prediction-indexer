# Replay

Strategy code, configuration examples and strategy specifications live under
[replay/strategies](strategies/README.md), with one package per strategy.

## Code layout

| Path | Role |
|---|---|
| [strategies/](strategies/README.md) | Current strategies, their configs, readers and specifications |
| `economic_sdk/`, [fees/](fees/README.md), `strategy_sdk.py` | Shared strategy runtime, fees and protocol |
| `preparation.py`, `preparation_sources.py`, `prepare_context.py`, `outcome_model.py` | Frozen context preparation and outcome modelling |
| `streams/`, `strategy_adapter.py`, `supervisor.py` | Redis delivery and strategy lifecycle |
| `bench/`, `jobs/`, `ops/` | Local runs, queued jobs and operational checks |
| `economic_fills.py`, `economic_intervals.py` | Shared economic fill and interval contracts |
| `stream.py`, `lanes.py`, `catalog.py` | Byte adapters, lane ranking and target-record projection shared with archive or Targeter |
| [legacy/](legacy/README.md) | Original five-gate raw-envelope pipeline, frozen policy and audit notes |
| `tests/` | Tests for both the current runtime and legacy gates |

Replay accepts only named immutable byte objects through `ByteStreamer`. Storage
selection is therefore an adapter decision:

- NFS/local: `DirectoryByteStreamer`
- tests and exact fixtures: `MemoryByteStreamer`
- receipt-committed raw objects: `archive.ArchivedSegmentByteStreamer`
- receipt-committed canonical windows: `archive.ArchivedCanonicalByteStreamer`
- complete Targeter runs: `targeter.v2.replay_stream.ArchivedTargeterRunByteStreamer`
- capture-window Targeter records: `targeter.v2.replay_stream.ArchivedTargetRecordByteStreamer`

`CompositeByteStreamer` snapshots disjoint adapters into one lexically ordered
dataset and rejects duplicate logical keys. [Legacy Gate 1](legacy/README.md)
can therefore consume raw segments and decoded `target_records_<venue>.ndjson` together without learning
about S3, Zstandard, or either receipt protocol.

Replay reconstruction imports no capture, ingester, targeter, or legacy-analysis
module. The optional pre-run `replay.preparation` boundary reuses Universe/Targeter
historical evidence validators and archived streaming adapters; it freezes a
hash-bound context before runtime. See
[`STRATEGY_PREPARATION_V1.md`](../docs/STRATEGY_PREPARATION_V1.md) for its closed
configuration, native book plans, explicit historical expectation scopes, and
offline snapshot loader. Offline strategies consume this pinned context
and immutable Risk cuts. No shared cache is included.

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

Target-record run selection includes every run in the half-open capture window
and the latest run strictly before it. Production run archive receipts remain
the authority: prefix listings are not accepted as commit evidence. Each read
freshly verifies the receipted remote manifest and selected object, fully stages
and verifies the decoded logical identity, and only then yields bytes.

`replay.economic_sdk.outcomes.outcome_scope` exposes frozen normal-resolution masks, payoffs, implications, and exhaustive complete sets during basket construction; see [OUTCOME_MASKS_V1.md](../docs/OUTCOME_MASKS_V1.md).

## Legacy fixture audits

The original Gate 1–5 pipeline and its trust, economic and execution semantics
are documented in [legacy/README.md](legacy/README.md). Commands use
`python -m replay.legacy.gate1` through `python -m replay.legacy.gate5`.
The legacy package and its frozen policy remain included in the distribution.
