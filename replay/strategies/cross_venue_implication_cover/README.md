# Cross venue implication cover

For a strict implication A ⊂ B, buy YES(B) and NO(A) on different venues.
Under normal resolution one pair pays at least one unit, with two units
in B minus A. The payout floor and extra middle payout are reported separately.

`strategy.py` selects the venue mode and binds its own strategy identity.
Both modes reuse the [common implementation](../_shared/implication_cover/strategy.py),
[mask contract](../_shared/implication_cover/contract.py) and
[independent reader](../_shared/implication_cover/output.py).

Factory: `replay.strategies.cross_venue_implication_cover:build`.
Completed bench reader: `replay.strategies.cross_venue_implication_cover:read_completed`.

Configuration: [config.example.json](config.example.json). Full shared
contract: [SPEC.md](../_shared/implication_cover/SPEC.md).
Offline contracts for both modes remain in `replay/tests/test_implication_cover.py`.

Execution-realism policy **4** adds recording t10, delayed-fill probes and
reader-checked episode flags. The example uses the reference 60-probe policy;
legacy policy 3 retains its output bytes. The shared
[producer contract](../../../docs/specs/STRATEGY_EXECUTION_REALISM_CONTRACT.md)
defines decision values/native walks, flags, pinned market delays and units.
Retained-data acceptance and the full corpus rerun remain pending.
