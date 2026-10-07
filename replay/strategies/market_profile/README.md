# Market profile

Reports per-book state, spreads, depth, activity, quote stability,
self-crossing and pair consistency. It makes no economic judgment and
requires no fee catalog.

The standalone factory and completed reader live in `strategy.py`. They
reuse the [SDK collector](../../economic_sdk/profile.py) and
[profile reader](../../economic_sdk/profile_reader.py), also available
inside any economic strategy through `Requirements.profile`. The profile
policy is specified in [SDK section 7](../../../docs/ECONOMIC_STRATEGY_SDK_V1.md#7-market-profile).
Review bucket widths, venue tick atoms and size choices in the example
against your pinned plan scales; they are illustrative research settings.

Factory: `replay.strategies.market_profile:build`.
Completed bench reader: `replay.strategies.market_profile:read_completed`.

Configuration: [config.example.json](config.example.json). Full contract: [SDK profile contract](../../../docs/ECONOMIC_STRATEGY_SDK_V1.md#7-market-profile).
Tests remain under `replay/tests/`; shared SDK and bench instructions are
linked from the [strategy index](../README.md).
