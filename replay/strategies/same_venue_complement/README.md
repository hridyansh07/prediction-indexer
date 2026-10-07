# Same-venue complement

Measures buying or selling both sides of one binary instrument on one venue.
Positive gaps are primarily a reconstruction diagnostic. Policy 1 retains
the frozen output bytes; policy 2 uses the SDK aggregate layout.

Read `strategy.py` for evaluation, `contract.py` for policy and structural
baskets, and `output.py` for independent validation. The shared
[fee bridge](../_shared/fee_bridge.py) assesses native orders. Cross-venue
and multi-market strategies reuse this policy and summary machinery.

Factory: `replay.strategies.same_venue_complement:build`.
Completed bench reader: `replay.strategies.same_venue_complement:read_completed`.

Configuration: [config.example.json](config.example.json). Full contract: [SPEC.md](SPEC.md).
Tests remain under `replay/tests/`; shared SDK and bench instructions are
linked from the [strategy index](../README.md).
