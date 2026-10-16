# Same-venue multi-market complete sets

Measures all-BUY complete sets of two to four books spanning at least two
markets on one venue. Pinned masks must partition an exhaustive space;
uncaptured members and unsupported shapes remain visible.

Read `contract.py` for enumeration and policy, `strategy.py` for trigger
and fill pricing, and `output.py` for independent reading. The strategy
reuses complement policy fields, summary aggregation and the cross-venue
all-BUY native fee helpers.

Factory: `replay.strategies.same_venue_multi_market:build`.
Completed bench reader: `replay.strategies.same_venue_multi_market:read_completed`.

Configuration: [config.example.json](config.example.json). Full contract: [SPEC.md](SPEC.md).
Tests remain under `replay/tests/`; shared SDK and bench instructions are
linked from the [strategy index](../README.md).

Execution-realism policy **2** adds recording t10, delayed-fill probes and
reader-checked episode flags. The example uses the reference 60-probe policy;
legacy policy 1 retains its output bytes. The shared
[producer contract](../../../docs/specs/STRATEGY_EXECUTION_REALISM_CONTRACT.md)
defines decision values/native walks, flags, pinned market delays and units.
Retained-data acceptance and the full corpus rerun remain pending.
