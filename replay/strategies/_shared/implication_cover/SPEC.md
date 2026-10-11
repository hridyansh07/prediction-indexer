# Implication covers V1

Implemented for offline replay; retained-fixture and live execution acceptance
remain unverified. Cancellation, void, push, dispute and dynamic-mask branches
are deferred by the implementation scope. Every result is labelled
`settlement_model: "normal_resolution_only"`.

## Core and entry points

For a strict implication A ⊂ B, buy YES(B) and NO(A), at equal contract
quantities. One pair pays 1 on A, 2 on B minus A, and 1 outside B. Neither leg
is sold, and no immediate redemption or netting of separate markets is assumed.

Both entry points use `replay.strategies._shared.implication_cover.strategy.ImplicationCover`:

| Routes | Supervisor factory | Completed bench reader |
|---|---|---|
| Same venue, separate markets | `replay.strategies.same_venue_implication_cover:build` | `replay.strategies.same_venue_implication_cover:read_completed` |
| Different venues | `replay.strategies.cross_venue_implication_cover:build` | `replay.strategies.cross_venue_implication_cover:read_completed` |

The wrappers select candidate pairs and bind distinct strategy/experiment
identities; discovery, evaluation, payoff accounting and reading are shared.
The SDK owns book views, depth walking, staging, exact time, scoped denominators,
episodes, slices, controls, optional fills, output bounds and durable commits.
The existing two-leg all-BUY evaluator and its independent native-cashflow
reader checks are reused without changing the existing strategies or SDK.

## Mask proof and admission

Pairs come only from the pinned snapshot's books. For acquired masks C and D,
require one EXHAUSTIVE outcome space, C ∪ D = Ω and C ∩ D nonempty. Taking
B = C and A = Ω minus D then proves A ⊂ B. This also admits the identical
economic basket expressed through the contrapositive; it is enumerated once,
in book-key order, rather than twice under two relationship descriptions.
YES and NO refer to payouts, not token names: negated tokens and meaningful
participant-labelled tokens are handled by their supplied masks.

The descriptor records A, B, the middle keys, the space keys, the binary payoff
of each leg in each state, and the summed unit payout vector. No event names,
prose inference or special fixture identities enter production logic.

All candidate pairs receive explicit admission or rejection evidence. Missing
members get NOT_CAPTURED rows; unavailable/void/unsupported masks, same-market
pairs, mixed or incomplete spaces, gaps, identity covers, unsupported venues
and missing ask sources are visible rejections. Identity covers (A = B) belong
to the existing complete-set strategies. Rejections have null sizes and one
full scoped denominator rather than one rejection per sweep size.

Candidate pairs are sorted and capped at 4,096 per scope before the size sweep.
Exceeding the cap or the SDK's metadata/state/row limits fails visibly.
Kalshi buys consume the opposite orientation's bids projected at 1 minus price;
mask and fee bindings retain the acquired orientation. Native scales are exact.

## Configuration, fees and values

Both wrappers accept the same closed version-1 configuration as
[cross-venue arbitrage](../../cross_venue_arbitrage/SPEC.md): `version`,
`snapshot_directory`, `snapshot_sha256`, `fees`, `policy`, `valuation`.
Use [the example](../../same_venue_implication_cover/config.example.json) with reviewed
context, fee evidence, native asset bindings and valuation assets.

Policy 2 provides the fixed-size sweep and optional time-shift controls. Policy
3 provides a one-contract trigger plus SDK governing/recording fill sizings;
its reader re-prices values and kill prices using the pinned fee bridge.
Cyclic controls are rejected because substitution can destroy the mask proof.
Control results stay isolated from real results.

Both policies optionally accept the prepared-input `game` block defined by
[GAME_STATE_SDK_V1.md](../../../../docs/specs/GAME_STATE_SDK_V1.md). Its hash and
release decisions enter experiment identity. Manifests require the corresponding
game binding and episode `open`/`at_max` rows require closed game annotations;
independent readers enforce presence and consistency. Output without the block
retains its existing schema and bytes.

Both variants require explicit quote-asset valuation before scalar economics.
The existing PARITY_SCENARIO values each listed native quote asset at one
research dollar; USD, USDC and pUSD remain separately identified native flows.
This is not a convertibility or settlement-compatibility proof. A single-venue
run can list its one quote asset in the same scenario schema.

For N acquired contracts on each leg, gross floor is N and gross middle payout
is 2N. Fees are assessed through the existing Fee SDK. Collateral charges enter
cash once; contract fees reduce received tokens. With received quantities qB
and qNotA, recompute the full outcome vector. Its floor is min(qB, qNotA), and
its middle payout is qB + qNotA. The extra middle payout is the middle minus
the floor; it need not equal either original contract quantity after fees.

Payloads retain the existing native costs, cash deltas, received quantities,
charges, fee assessment identities and evidence labels. They additionally
record `payoffs_gross_e36`, `payoffs_net_e36`, `payout_middle_gross_e36`,
`payout_middle_net_e36`, `payout_middle_extra_gross_e36` and
`payout_middle_extra_net_e36`. Amounts are exact signed integers at scale 36;
vectors align to the descriptor's outcome keys. Unknown fees give null net
values. There is no guessed zero fee or probability model.

Only positive floor-minus-cost margins enter gross episodes; net episodes
require known positive net margin and the configured per-contract threshold.
A basket costing at least its floor remains GROSS_NONPOSITIVE even if its
middle payout looks attractive. Expected-value strategies require a separate
probability model and are not implemented here.

## Outputs, readers and bench use

SDK layout 2 is unchanged. New strategy manifests and summaries have version 1,
distinct strategy IDs, and `venue_mode: "same_venue" | "cross_venue"`.
The experiment identity binds mode, snapshot, policy, fee configuration,
valuation, settlement model and entity-contract version. The independent
reader reconstructs routes, checks exact entity tables, re-walks consumed depth,
checks fees/native cashflow arithmetic, and verifies every state's payout,
its minimum and its middle/extra payout. Completed readers additionally require
supervisor SUCCESS and exact factory/configuration/content-receipt bindings.

The existing local bench needs no infrastructure change. Add either or both
factory/reader rows from the table above to a reviewed `groups` list under
[LOCAL_REPLAY_BENCH_V1.md](../../../../docs/LOCAL_REPLAY_BENCH_V1.md), with the pinned existing
context and fee catalog. For run comparison use `rows` with keys
`["venue", "basket_kind", "direction", "route_id", "size_contracts"]`.
Inputs remain read-only and each attempt needs a new output directory.
No retained inputs, current target pointers or service state are modified by
this implementation.

These are detections under displayed-depth and fee scenarios, not executions
or realized profit. Same-venue multi-market orders are not assumed atomic;
cross-venue orders also require prefunding and have asset/settlement basis risk.
Q(L) and time shifts remain retrospective diagnostics. Routes can share depth
and must not be added together as independently executable profit.

## Offline verification

`replay.tests.test_implication_cover` uses a small hand-authored Bo3 sweep and
series-win model through actual preparation, Decoder, SDK output and the
independent readers. It covers both modes, exact 0.58 + 0.39 floor/middle
arithmetic, token-fee shortfalls, unknown fees, depth/trust/valuation gates,
visible rejection denominators, deterministic ordering/retries, same-time
restores, scope boundaries, time shifts, optional fill checks, rehashed payout
tampering and both completed-reader bindings. Completed-reader tests stub only
the external Rust metadata preflight; they are not Redis/Risk acceptance runs.

Run with the project virtual environment:

```bash
.venv/bin/python -m unittest replay.tests.test_implication_cover
```

No retained-fixture replay, cloud, deployment or live venue acceptance is
claimed. The strategy is not registered in the production Replay jobs API.

Verification on 7 October 2026, from `codex/implication-covers` based on
updated master `83c9968`, with the primary project's virtual environment:

- All 10 implication tests passed. A further 99 focused tests passed across
  `test_cross_venue_arbitrage`, `test_cross_venue_metadata`,
  `test_economic_sdk_outcomes`, `test_economic_sdk_fills`,
  `test_economic_sdk_port` and `test_bench`; the complement byte goldens hold.
- Full root suite: 1,026 tests, one failure in
  `test_stale_retrieval_cleanup_keeps_live_process_directories` (Linux `/proc`
  assumption). Disposable loopback-listener tests passed after granting the
  required test permissions.
- Full replay suite: 478 tests, 33 skipped, two failing signal subtests in
  `test_materializer_dies_with_runner` and one error in
  `test_materializer_output_limit_stops_before_timeout` (Linux process
  containment on macOS). The failing root and replay cases were reproduced
  on the untouched master checkout at the same revision.
- Tracked and new-file whitespace checks, in-memory Python compilation and
  example policy/valuation schema validation passed.
- Rust and Compose gates were not run: neither implementation changed.
  Redis/Risk execution and retained-fixture performance remain unverified.
