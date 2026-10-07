# Replay strategies

Each strategy keeps its factory, independent readers, configuration examples
and documentation here. Start with its README, then its specification.

| Package | Measures |
|---|---|
| [bundle_coverage](bundle_coverage/README.md) | Historical book, member and bundle availability |
| [market_profile](market_profile/README.md) | Per-book state, activity, depth and quote stability |
| [same_venue_complement](same_venue_complement/README.md) | Both sides of one instrument on one venue |
| [same_venue_multi_market](same_venue_multi_market/README.md) | Complete sets spanning one venue's markets |
| [cross_venue_arbitrage](cross_venue_arbitrage/README.md) | Two-leg complete sets across venues |
| [same_venue_implication_cover](same_venue_implication_cover/README.md) | Strict implication covers on one venue |
| [cross_venue_implication_cover](cross_venue_implication_cover/README.md) | Strict implication covers across venues |

Every package exposes `build`, `read_provisional` and `read_completed`.
Supervisor and bench references use `replay.strategies.<name>:build` and
`replay.strategies.<name>:read_completed`. Python callers can import those
functions directly from the package. Completed readers still require supervisor
SUCCESS and verify the strategy's content receipt and configuration bindings.

`strategy.py` owns requirements and evaluation; `contract.py`, where present,
owns configuration and baskets; `output.py` owns independent reading.
`config.example.json` is a strategy config object, passed as a group's `config`.
Replace snapshot and fee placeholders with reviewed pinned inputs before use.
The coverage package also has a complete [bench example](bundle_coverage/bench.example.json).
Generic image, fee-catalog and comparison examples remain in
[configs/bench](../../configs/bench/README.md).

Shared preparation, transport, time, depth walking, SDK runtime and fee models
stay outside these packages. [_shared](_shared/README.md) contains the fee
bridge and the common implication-cover implementation. Strategies reuse the
complement policy and cross-venue native-cashflow reader where their contracts
already share those rules; the per-strategy READMEs identify those dependencies.

## Existing run configurations

Bench, the strategy adapter and Replay jobs resolve the previously shipped
`replay.<strategy>:build` and reader module names through `load_entrypoint`.
This leaves original run config bytes, hashes and receipts intact. Completed
readers accept the corresponding historical factory name while retaining all
content and SUCCESS checks. New configurations should use the package paths
above; Python imports should use the new packages or their implementation modules.

`cross_venue_arbitrage/SOURCE.json` is the unchanged historical source attestation.
Its hashes and paths describe its original revision, before this folder move.
