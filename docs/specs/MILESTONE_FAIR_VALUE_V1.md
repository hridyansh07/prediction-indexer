# Milestone-based fair-value trading V1

Status: **PROPOSED — lower priority than payout covers and leader–follower;
not implemented or empirically validated.**

The [shared economic scenario contract](ECONOMIC_POSITION_SCENARIOS_V1.md) is
normative. This strategy buys a supported claim when a pinned probability model
assigns its retained payout more value than its acquisition cost. Its economic
claim is expected value under a model; it has no minimum-payout guarantee.

## 1. Available inputs and model admission

V1 supports exhaustive, aligned finite series spaces and their admitted claims.
It uses released completed-map results, supported score/prefix, phase, the
pinned best-of format and causal book prices. No kills, gold, tactical events,
opaque statistics or continuous in-map win probabilities are input features.
Unplayed-map/void semantics and normal-resolution compatibility follow the
shared contract. An incomplete space cannot be normalized into an apparently
complete probability distribution by ignoring its missing outcomes.

Game state is required for the primary milestone strategy. Use the current
required/unavailable behavior when the file is unavailable. Partial knowledge
inside an available file is retained explicitly: known winners constrain the
model, a phase transition without a winner does not change its outcome
probabilities, and missing map order cannot be inferred from aggregate score.

Each model pins event-independent game/format support, its probability function,
feature definitions, parameter provenance and any fallback. Unknown parameters,
zero mass for observed facts or an unsupported state yield `PROBABILITY_UNKNOWN`
or `MODEL_CONTRADICTION`; they never yield certainty by default.

## 2. Two implementable probability modes

**A. Assumed independent-map model.** Pin an exact rational map-win probability
p for participant A. A terminal sequence w has prior weight
`p^(A wins in w) * (1-p)^(B wins in w)`, with the sequence stopping at the
format's clinch. Condition these weights on Omega_t and divide by the positive
sum. The resulting probabilities sum to one exactly. p remains fixed through
the event; observations condition outcomes without secretly refitting strength.
This explicitly assumes independent maps with constant strength. It is useful
as a transparent hypothesis, not a claim of calibrated game dynamics.

p is either a pinned symmetric/game prior (e.g. 1/2) or inferred once from a
causal series-moneyline reference. The latter mode declares a reference time
`scheduled_start - reference_lead_ns` (default 60 seconds), fixed venue priority
and fallback eligibility. Read the first usable supported A-series midpoint in
that order at that exact as-of time; if the run has no verified prologue there,
the reference is unknown. Do not seek a better future reference. Scheduled time
does not prove physical pre-match timing; label this a scheduled-reference
assumption, and do not use estimated retrospectively derived starts.

Infer p on the pinned grid `{1/1000, ..., 999/1000}` by minimizing the exact
absolute difference between the model's series-win probability and the reference
midpoint, ties lower p. Record reference book/time/quotes, resulting p and residual.
Reject if residual exceeds `maximum_reference_error` (default .01). This is an
explicit market-implied model, not an external fair-value observation. The target's
current ask cannot refresh p and then be presented as an independent prediction.

**B. Frozen past-event transition model.** Pin conditional probabilities
`P(next winner | game, format, released prefix/complete score, reference bucket)`
for every supported reachable continuation. Multiply them along each terminal
path and condition on released facts. State functions use only milestones and
the same causal reference features. All rows and fallback rules must have been
fit or declared before the evaluated event, on independent earlier events whose
labels were already available. Missing continuation rows make that decision
unknown; never prune the missing path and renormalize the others.

Learned models pin training/validation event identities, cutoff, smoothing,
feature/reference procedure and calibration results. The default model uses
no interpolation unless the interpolation rule is frozen in its identity.
No online fitting from the evaluated event's final result is permitted.

For claim i, `pi_i(t) = sum_{w in its mask intersect Omega_t} P_t(w)`.
This produces coherent sibling probabilities. Known outcomes may yield 0 or 1,
but new entries after `finished` belong to the payout optimizer and are disabled
here to avoid presenting the same opportunity as a new predictive strategy.

## 3. Sizing, triggers and opening

Recompute probabilities on released winner/score changes and the one declared
reference acquisition. Reprice candidates on relevant ask/depth/validity changes,
scope changes and delayed-entry timers. Phase changes trigger re-evaluation for
admission/cohort purposes, but the assumed-map model does not invent new strength
from phase alone. No polling or rich-state feed is required.

For each gross q in the pinned grid, obtain retained h(q) and all-in cash cost C
from current eligible asks and exact BUY fees. For unit payout, under the declared
asset basis:

```
EV(q) = h(q) * pi_i(t) - C - assumed_remaining_holding_charge(q)
```

Use the full native payout expression for differing bound assets. Unknown
settlement duration under nonzero holding charges needs an explicit duration
scenario; it cannot silently be zero. A configured probability uncertainty
deduction epsilon uses `max(0, pi_i-epsilon)` only as a conservative entry score;
it is not a new probability distribution and must not be labelled calibrated
unless supported. Preserve the unadjusted EV too.

Precisely, `entry_EV = h * max(0, pi_i-epsilon) - C - holding_charge` in the
unit-payout expression above; epsilon=0 recovers EV. Apply all entry thresholds
and sizing comparisons to entry_EV, while reporting both quantities.

Require entry_EV>0, entry_EV at least `minimum_expected_margin`, expected return at least
`minimum_expected_return_bps`, exact depth/fees, available collateral and risk
limits. Maximum loss for a long unit claim includes its full acquisition cost
and configured charges; budget it as such. Buy the q with greatest qualifying
entry_EV, ties lower cost then smaller q. There is no automatic short when EV is
negative; evaluate a captured opposite claim independently.

| Configurable field | Initial policy |
|---|---|
| `quantity_grid` | 1, 10, 100 gross contracts, subject to exact increments |
| `minimum_expected_margin` / `minimum_expected_return_bps` | .05 valuation units / 0 |
| `probability_deduction` | 0, explicitly no uncertainty cushion |
| `decision_delay_ns` | 0 |
| `max_entries_per_event` / `max_open_positions` | 1 / 1 |
| `entry_phases` | Released `pre_match`, `in_segment`, `between_segments`; never infer the physical phase |
| `exit_mode` | `HOLD_TO_RESOLUTION` |
| Cash, maximum-loss/event budgets, probability model | Required pinned inputs |

The first qualifying crossing creates an attempt. Economic predicates are
separate from capacity/cash gates, so a skipped attempt is not retried on every
identical quote. At a delayed decision freeze the claim/model and maximum q;
recompute probability from newly released facts and choose only a no-larger
qualifying size. Missing/contradictory state cancels. Opening posts cash, retained
holdings, cost basis and capacity debits as in the shared contract.

## 4. Holding and optional economically defined exit

`HOLD_TO_RESOLUTION` keeps the position through model and quote changes until
the shared supported settlement/scenario credit. Later lower EV does not undo
the original purchase. Report updated expected value, normal-resolution minimum,
liquidation mark and remaining capital separately. Model unavailability after
entry means unknown expected value; holdings remain present.

An optional separately identified `SELL_WHEN_BID_DOMINATES_HOLD` policy compares
full-position net sale proceeds S(h,t) with current model hold value
`H(h,t) = expected native payout - future assumed holding charges`.
Latch an exit if `S >= H + minimum_exit_advantage` (default .01 valuation units).
Past acquisition cost is irrelevant to this replacement decision but remains
in total P&L. A sale can rationally realize a loss when the model now values
holding still less. Require known model/fees and full eligible bid-side depth
to establish this trigger. After it latches, partial/no exits follow the shared
irrevocable liquidation rule; do not assume the forecast payout is already cash.

A configured hard holding deadline is an independent observer-time exit trigger;
default is none. If used, it is frozen at entry and may lead to `EXIT_PENDING`
without depth. At run end unresolved/dust holdings are censored, never discarded.

With repeated entries enabled, require the prior position closed, a newer
released result that changes supported knowledge, and a newly qualifying
predicate after it was known false. No re-entry from repeated prices in the same
milestone state; no resetting cash/debits at scope boundaries. The default cap
of one entry makes each event's risk and evidence contribution clear.

## 5. Examples (synthetic)

With p=1/2, A leading 1-0 in a Bo3 wins the series with probability
`1/2 + (1/2)*(1/2) = 3/4`. Buying 100 at .68 costs 68; cash fees of 2 make
C=70 and EV=75-70=5, with maximum acquisition loss 70. If A eventually wins,
the hypothetical settlement P&L is 30; if A loses, -70. EV=5 was never a booked
profit. A 3-contract token charge instead reduces the expected payout by 2.25;
it must be included before opening.

At ask .74 and cash fees of 2, C=76 and EV=75-76=-1: no entry despite a 75%
win probability. If only a map-end transition is released and its winner is
unknown, no 1-0 update is justified and this .75 probability cannot be used.

An optional exit example: the position retains 100, updated model probability
is .60, future charges zero, and a complete bid sale nets 62. It may exit because
62 exceeds hold value 60 by at least .01. Against original cost 70, closed P&L
is -8; the positive exit advantage 2 is not a profitable complete trade.

## 6. Measurement, priority and future gaps

Report assumed-model EV separately from learned-model EV, calibration by
event/game/state, actual conditional settlement/exit P&L, full loss distribution,
fees, capacity, capital duration and unresolved holdings. Compare frozen
milestone models with the frozen reference-only probability model, using the same
events, accounts, thresholds and exits. Hold out whole later events. A positive
EV detection is model-dependent; held-out positive net hypothetical outcomes
are evidence about the policy, not a minimum-payout result.

No new data beyond coarse milestones is required. Genuine prerequisites are a
declared supported probability model and prior/reference provenance; learned
models additionally need independent earlier events and calibration. Current
SDK machinery does not supply these models, bounded position state or outcome
accounting. The shared implementation gaps apply. Richer game streams may
motivate a future model version, but are excluded from this V1.
