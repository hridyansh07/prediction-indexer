# Bounded minimum-payout optimizer

An offline hypothetical all-BUY portfolio strategy. It searches a pinned finite
quantity grid over exhaustive normal-resolution masks, including unequal sizes,
partitions, implications, known payouts and general overlapping claims. Optional
released game results condition a separate problem and account; the static
reference remains independent. It places no actual orders.

Factory: `replay.strategies.bounded_payout_optimizer:build`.
Completed reader: `replay.strategies.bounded_payout_optimizer:read_completed`.
Bench check: `replay.strategies.bounded_payout_optimizer:check`.

Read [SPEC.md](SPEC.md) for the implemented closed contract, then the approved
[optimizer specification](../../../docs/specs/BOUNDED_PAYOUT_OPTIMIZER_V1.md)
and [economic position scenarios](../../../docs/specs/ECONOMIC_POSITION_SCENARIOS_V1.md).
`core.py` owns exact search, released knowledge and capacity; `scenario.py` owns
native fee pricing and hypothetical accounts; `strategy.py` stages decisions;
`audit.py` independently derives native fee/payoff arithmetic; `output.py`
reconstructs the ledger and required lifecycle without writer economic helpers.
`stream.py` stores version-2 checkpoints, references and deltas for both outputs.

[config.example.json](config.example.json) and [bench.example.json](bench.example.json)
are templates. Replace every angle-bracket/zero pin, supply reviewed fee economics
for the captured books, and explicitly choose capital, rules, settlement and
valuation. The examples' cash and zero settlement delay are modelling inputs,
not observed balances or payout availability. The bench mounts inputs read-only;
its usual context, fee and game tokens resolve without a new bench schema.
For a static run remove `policy.game` and `game_state_path` from the bench example.
For required game evidence set `required` true; unavailable input then fails
before any output writer opens. Use a fresh output directory for each run.

Run the offline contracts with the project virtualenv:

```bash
.venv/bin/python -m unittest replay.tests.test_bounded_payout_optimizer
```

The package has synthetic contract tests and a real Decoder/bench SUCCESS reader
path. No live venue, account, historical profitability or actual execution has
been validated. Separate scenario/group profits and capacities cannot be added.
The reader certifies carried feasible portfolios and negative dual bounds;
positive search coverage and unrestricted global optimality remain writer
attestations. See [PERFORMANCE.md](PERFORMANCE.md) for offline scalability evidence.
