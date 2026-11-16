# Specification index

These documents retain the contracts for shipped extensions and distinguish
them from pending work. A component README describes current behavior; a
specification's status describes its implementation scope, not deployment or
retained-data acceptance. Outstanding checks are collected in
[`../OPEN_WORK.md`](../OPEN_WORK.md).

## Implemented contracts

| Area | Contract | Owning component |
|---|---|---|
| Venue event-time annotations | [VENUE_EVENT_TIME.md](VENUE_EVENT_TIME.md) | [engine](../../engine/README.md), [derivatives](../../engine/DERIVATIVES.md) |
| Profile availability, transitions, levels and storage | [MARKET_PROFILE_V2.md](MARKET_PROFILE_V2.md) | [market profile](../../replay/strategies/market_profile/README.md) |
| Profile volatility | [MARKET_VOLATILITY_V1.md](MARKET_VOLATILITY_V1.md), [precise contract](MARKET_VOLATILITY_V1_CONTRACT.md) | [market profile](../../replay/strategies/market_profile/README.md) |
| Execution terms and delayed-fill probes | [EXECUTION_DELAY_V1.md](EXECUTION_DELAY_V1.md) | [economic SDK](../ECONOMIC_STRATEGY_SDK_V1.md), [Universe](../../universe/README.md) |
| Structural-strategy execution realism | [STRATEGY_EXECUTION_REALISM_V1.md](STRATEGY_EXECUTION_REALISM_V1.md), [producer contract](STRATEGY_EXECUTION_REALISM_CONTRACT.md) | [strategy packages](../../replay/strategies/README.md) |
| Acquired-book probe exits and conservative shortfalls | [PROBE_EXIT_SOURCE_V1.md](PROBE_EXIT_SOURCE_V1.md) | [economic SDK](../ECONOMIC_STRATEGY_SDK_V1.md) |
| Optional legacy probes, diagnostics and phase-aware retention | [LEGACY_PROBE_RETENTION_V1.md](LEGACY_PROBE_RETENTION_V1.md) | [economic SDK](../ECONOMIC_STRATEGY_SDK_V1.md) |
| Prepared game-state overlay | [GAME_STATE_SDK_V1.md](GAME_STATE_SDK_V1.md) | [game state](../../gamestate/README.md), [economic SDK](../ECONOMIC_STRATEGY_SDK_V1.md) |
| Totals-direction projection | [UNIVERSE_TOTALS_DIRECTION_V1.md](UNIVERSE_TOTALS_DIRECTION_V1.md) | [Universe](../../universe/README.md) |
| Offline copy-based migrations | [UNIVERSE_MIGRATIONS_V1.md](UNIVERSE_MIGRATIONS_V1.md) | [Universe schema](../../universe/schema/README.md) |

The precise volatility contract resolves its overview's implicit row shapes and
units. The producer contract and probe amendments record the final execution
rules, including optional legacy probes; historical implementation-order text
does not override them. Current Redis delivery is specified by
[`REPLAY_STREAMS_V1.md`](../REPLAY_STREAMS_V1.md).

## Pending work

[`TARGET_RECORD_BACKFILL.md`](TARGET_RECORD_BACKFILL.md) retains the unimplemented
asserted-record backfill and deferred write-on-change proposal. The implemented
target-record format remains owned by
[`targeter/v2/DELIVERY.md`](../../targeter/v2/DELIVERY.md).
