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
