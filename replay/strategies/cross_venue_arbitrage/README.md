# Cross-venue complete sets

Measures two all-BUY legs on different venues whose pinned masks partition
one exhaustive outcome space. Native asset flows remain explicit; scalar
values require the configured parity research scenario.

Read `contract.py` for mask routes and configuration, `strategy.py` for
pricing and fills, and `output.py` for the independent native-cashflow
reader. Multi-market and implication strategies reuse its all-BUY fee
accounting and reader hooks. The unchanged [SOURCE.json](SOURCE.json)
attests the historical implementation revision, before this reorganization.

Factory: `replay.strategies.cross_venue_arbitrage:build`.
Completed bench reader: `replay.strategies.cross_venue_arbitrage:read_completed`.

Configuration: [config.example.json](config.example.json). Full contract: [SPEC.md](SPEC.md).
Tests remain under `replay/tests/`; shared SDK and bench instructions are
linked from the [strategy index](../README.md).
