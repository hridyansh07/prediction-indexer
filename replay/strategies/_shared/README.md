# Shared strategy helpers

[fee_bridge.py](fee_bridge.py) maps native walked orders into the Fee SDK and
preserves the complement's original accounting and order identities. All
economic strategy packages reuse it.

Evaluation memoizes native amounts for the exact ordered taken levels, never
scales a per-contract fee, and keeps assessment IDs deferred until a payload is
written. The per-bridge LRU holds at most 1,048,576 orders; its hit/miss counters
are available through `memo_diagnostics` and never enter semantic outputs.
Selection keys respect effective boundaries and the resolver's frozen pricing
reference. `assess_orders` defaults to real `FeeAssessment` objects for callers
that need the full public API; evaluation uses `defer_ids=True`.

`REPLAY_FEE_CROSSCHECK=1` recomputes every memo hit through the full engine and
compares each deferred identity with an eager assessment, including discarded
evaluations. The bench forwards only that exact literal host value. SDK episode,
legacy slice and audit-slice writes materialize ordinary JSON ID lists; line
preflights use equal-length placeholders without hashing. Fee-assessed
observations remain context-dependent because their IDs pin time and sequence.

[implication_cover](implication_cover/SPEC.md) holds one mask-proof contract,
evaluator and independent reader. The same-venue and cross-venue packages
supply the mode and expose their own factories, readers and configuration
examples. Changes to common proof or payout arithmetic belong here.

Generic reconstruction, time, book views, depth walking, episodes and fee models
remain in `replay`, `replay.economic_sdk` and `replay.fees`.
