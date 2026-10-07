# Shared strategy helpers

[fee_bridge.py](fee_bridge.py) maps native walked orders into the Fee SDK and
preserves the complement's original accounting and order identities. All
economic strategy packages reuse it.

[implication_cover](implication_cover/SPEC.md) holds one mask-proof contract,
evaluator and independent reader. The same-venue and cross-venue packages
supply the mode and expose their own factories, readers and configuration
examples. Changes to common proof or payout arithmetic belong here.

Generic reconstruction, time, book views, depth walking, episodes and fee models
remain in `replay`, `replay.economic_sdk` and `replay.fees`.
