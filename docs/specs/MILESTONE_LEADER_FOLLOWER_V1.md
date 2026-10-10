# Milestone-conditioned venue and sibling leader–follower V1

Status: **PROPOSED — second implementation priority; not implemented or empirically validated.**

The [shared economic scenario contract](ECONOMIC_POSITION_SCENARIOS_V1.md) owns
visibility, exact fees/cash, capacity, hypothetical positions and outputs.
This strategy measures whether causal information in a separate book improves
net target entry/exit outcomes beyond the target's own book and released coarse
milestones. It has no minimum-payout guarantee and does not require live latency
calibration or rich gameplay statistics.

The primary milestone-conditioned run sets `game.required=true`. An unavailable
file is the existing `game_state_unavailable` result. An optional book-only
comparison must be a separately identified policy/model cohort; it cannot be
reported as having used game evidence. Unknown score within an available file
requires an explicitly supported unknown-score cohort.

## 1. Relationships and available features

A candidate is `(leader book L, acquired target claim T, relation, model)` within
one aligned event and supported normal-resolution scope. Both books must be
captured, usable, economically bound and interpretable. Exclude the same native
market's complementary/mirrored books as independent leaders. Signals trade T
only; observing L never purchases it or reserves its depth.

Use masks to admit relationships, not to infer response magnitudes:

| Relation on the currently justified outcome set | Leader coordinate / response rule |
|---|---|
| Same payout mask | Leader unit-payout midpoint, in T's orientation |
| Exact complementary mask | 1 minus the leader midpoint, in T's orientation |
| Strict implication, reverse implication, mutual exclusion or other overlap | Requires a pinned relationship-specific response model; no 1:1 transformation |
| Unknown/mixed scope/incomplete proof | No relationship-normalized candidate |

Containment alone does not establish the sign or size of a short-horizon price
response. A learned cross-claim coefficient may be positive or negative; its
model declares features and direction. The first implementation may support
only identity/complement routes, retaining other routes as `MODEL_UNAVAILABLE`.
Game-created identities are admitted only after the responsible results release.

V1 price features are exact midpoint changes for L and T, their current spreads,
current depth used in pricing, and released milestone state: game, phase,
complete/unknown score, result prefix quality and elapsed time since a released
result. No source `details`, tactical features or intra-map estimates enter.
An optional observed-trade feature requires a pinned model and supported coverage
and aggressor semantics; absent/ambiguous trades are unknown, not zero flow.

For window W, `delta_L(t) = u_L(t) - u_L(t-W)` and likewise for T. The historical
endpoint is the committed as-of view at or before t-W, never a future-nearest
sample. Retain one predecessor plus changes inside a bounded history interval.
No interpolation through gaps/invalidation. Require both endpoints and the
intervening book validity. A quiet but valid book may be carried forward.

Default W is 2 seconds, retained history 30 seconds and 100,000 changes per
book; exceeding the bound makes the attempt visibly fail, not silently evict
necessary evidence. A reconnect/invalidation resets that book's history and
requires a full W warm-up. A changed relationship transformation also warms up
anew; do not apply a new conditional identity to old coordinates. A milestone
that leaves the transformation valid does not erase history. Scope changes
preserve history only for identical supported native identities and authority.

## 2. A precise economical signal

The model returns a forecast target bid-ladder displacement D in probability
price units for a fixed horizon H. The initial transparent model family is:

```
D(t) = beta_L[g] * delta_L(t)
     + beta_T[g] * delta_T(t) + intercept[g]
```

g is a pinned milestone cohort defined by game, released phase and supported
score/prefix category. Coefficients and bucket boundaries are exact pinned
decimals. A declared fallback cohort is permitted only if frozen in the model;
otherwise missing state/cohort is `MODEL_UNAVAILABLE`. No online coefficient
refitting, leader selection using future returns or hidden game-state inference.

Two model provenance modes are allowed:

- `ASSUMED_RESPONSE_MODEL`: e.g. beta_L=1, beta_T=0, intercept=0 for an
  identical claim. This is a declared economic hypothesis, not an estimate of
  expected profit supported by training.
- `PAST_EVENT_FIT`: coefficients fitted on distinct earlier completed events,
  with training/validation event identities, cutoff and feature/target definitions
  pinned. All training outcomes must have become available before the evaluated
  event's first decision. Hyperparameters and route eligibility freeze too.

The target is a long-only acquisition of a supported T orientation. To express a
negative directional view, evaluate an actually captured complementary target
as its own BUY candidate; never invent a short. Require a leader innovation with
`abs(delta_L) >= minimum_leader_move` and positive predicted economic value for
this acquired orientation. Nonidentity models decide which sign is relevant.

For each permitted gross purchase quantity q, walk current target asks and apply
BUY fees to obtain cost C and retained h. Construct a **forecast valuation** of
the current bid ladder: shift every price by D, clip to [0,1], round downward to
the declared target price increment, merge equal prices, and preserve quantities.
Price sale of exactly h into this ladder, with SELL fees at the declared forecast
exit time and holding charges. This assumes the current depth shape persists
for valuation; it is not future observed depth or a fill promise.
Use current capacity-eligible bid quantities for position entry valuation, but
reserve none of that hypothetical exit depth. Future exits compete for the
capacity actually remaining at their own decision time.
Unrepresentable h, insufficient forecast ladder quantity, unknown fees or missing
two-sided prices makes this size unavailable. Do not round holdings away.

Define `forecast_net(q) = forecast_net_sale(h) - C - forecast_holding_charge`.
Require all of:

```
forecast_net(q) > 0
forecast_net(q) >= minimum_forecast_margin
10000 * forecast_net(q) >= minimum_forecast_return_bps * C
abs(delta_L) >= minimum_leader_move
```

Also require current bid depth to support the initial full liquidation mark and
its loss not to exceed the configured stop-loss amount. This prevents opening
a position already beyond its own risk limit. Current bids used for that mark
are not consumed until an actual hypothetical sale. Choose the quantity with
largest forecast net, ties lower C then smaller q. This is a forecast-sized
position, not Kelly sizing or a guaranteed return.

Recommended pinned defaults:

| Field | Initial value/requirement |
|---|---|
| `quantity_grid` | 1, 10, 100 gross contracts, each exactly representable and within declared lot rules |
| `minimum_leader_move` | .02 probability units |
| `minimum_forecast_margin` / `minimum_forecast_return_bps` | .05 valuation units / 0 |
| `window_ns` / `exit_horizon_ns` | 2 seconds / 30 seconds |
| `decision_delay_ns` | 0; separate scenarios may use declared positive delays |
| `take_profit_fraction` / `stop_loss_fraction` | .02 / .05 of opening acquisition cost |
| `exit_on_next_segment_end` / `exit_on_match_end` | true / true |
| `cooldown_after_close_ns` / `rearm_false_ns` | 30 seconds / 1 second |
| `max_entries_per_event` / `max_open_per_target` | 1 / 1 |
| Initial cash, event budget, models and rule bindings | Required pinned inputs |

These numerical defaults define an initial measurement policy, not calibrated
optimal thresholds. Every changed policy/model is a distinct experiment.

## 3. Triggers, opening and re-entry

Re-evaluate on L/T relevant best/depth changes, target validity changes, released
milestones, history-window expiration timers, pending-entry timers and position
deadlines. Identical surviving quotes are not new innovations. A milestone alone
may change the model cohort and value of an existing causal innovation; it cannot
fabricate a nonzero delta. No new directional entry is allowed in `finished`.

The first committed false-to-true signal creates one attempt. Signal state is
based on economic inputs, independently of account/depletion gates; insufficient
capital/capacity is a skipped attempt, not a fresh signal every update. Select
among simultaneous leaders for the same target by forecast net, then lower cost,
then candidate identity. Use only the selected signal; alternate leaders remain
non-additive explanations of the same target exposure.

At zero delay, reprice the selected q against remaining account/capacity and
open if still qualifying. At positive delay, freeze L/T/model, innovation identity
and maximum q. At t+d recompute the model using only then-visible bounded history,
require the same directional innovation still qualifies, and choose only a size
no larger than intended. Missing history, changed transformation or finished
state cancels. A newer unrelated innovation cannot inherit the pending attempt.

Opening debits target cash, posts retained h, consumes ask-source capacity and
records entry cost, baseline predictions, state, model and exit deadlines. There
is one target position; no pyramiding or automatic reversal. A failed attempt
does not reserve cash/depth. Higher entry limits require signal false for the
configured positive duration, cooldown elapsed, no target residual, and a later
leader-book change newer than the previous attempt. A milestone/scope relabel
alone cannot bypass rearm. Unknown signal intervals do not count as false.

An innovation identity is `(leader, last transformed price-change time and
revision, relation-proof identity)`. A delayed attempt cancels if that price
revision or relation proof changes before its due time; pure quantity changes
may reprice it. Rolling-window endpoint changes are recomputed at the due time
and may make the original innovation fail its threshold. This conservative
rule makes "same innovation" decidable without borrowing a later signal.

## 4. Exit behavior

At entry freeze the acquisition cost C, horizon `entry_time+H`, take-profit
amount .02*C and stop-loss amount .05*C (or their configured values), and the
latest consumed milestone identities. The first of these events latches an exit:

1. A supported claim settlement/resolution credits holdings under the shared rule.
2. A later released match end, or segment end when enabled; facts used in the
   entry decision at the same time are not a later milestone.
3. Observer time reaches the fixed horizon.
4. A full current bid-side net liquidation mark minus remaining/allotted cost
   and accrued charges reaches take-profit or falls to negative stop-loss.

Tie priority is settlement, match end, segment end, horizon, stop-loss, take-profit.
Take-profit/stop-loss use current sales of retained quantities and SELL fees,
not midpoint markouts or the original forecast. Unknown/partial full-position
marks cannot establish those price thresholds; time/milestone exits still fire.

After an exit latch, follow shared `SELL_AVAILABLE_THEN_WAIT`. A partial sale
does not reset its deadline or cancel the exit if prices recover. Retry the
residual on eligible bid changes; record elapsed time beyond the desired horizon.
There is no optimistic fill at the horizon's old bid. An unsupported/inaccessible
residual remains censored at end or settles under the declared settlement mode.
Disappearing leader information does not erase a holding or force an invented
price; the predeclared target exit rules continue.

## 5. Baselines and result definitions

Freeze three model policies on the same past-event split: target-only,
target-plus-milestones, and target-plus-milestones-plus-leader. Learned nested
models are fitted separately on those same training events; do not merely select
the best ex post baseline. Assumed models declare their assumptions separately.

Measure forecast error and counterfactual target trade outcomes for all models
at a common decision schedule (union of their candidate triggers), including
no-trade decisions. Then run each model's thresholded policy with identical cash,
size grid, capacity, timing and exit conventions in independent accounts. A
baseline need not satisfy the leader-move gate; its own economic threshold is
its trigger. This separates incremental prediction from signal frequency and
capital-allocation effects. Report both comparisons, not just profitable leader
entries. Matched/shifted controls remain diagnostics and never supply signal input.

Hold out whole later events, including all their venues, siblings and outcomes;
purge overlapping training outcome horizons. Report per-event net totals,
win/loss distributions, tails, capital use, costs, skips and residuals, with
event-clustered uncertainty and the number of models/thresholds tried. A positive
closed hypothetical trade is a measured conditional outcome; incremental held-out
net value versus the state baseline is evidence for leadership. Midpoint forecast
accuracy alone is not an economic positive. No recorded trade is actual execution.

## 6. Hand calculations and refusal cases

**Positive assumed response.** Same-claim leader midpoint rises .51 to .60,
delta=.09. Target bid/ask is .52/.53 with 100 contracts on both sides. The
assumed unit-response model forecasts bid .61. BUY 100 costs 53 plus a .75 cash
fee; a forecast SELL at .61 less .75 yields 60.25, so forecast net=6.50. Initial
net liquidation is 51.25 against cost 53.75, a loss of 2.50, below the default
5% stop limit of 2.6875. The default policy may open. If the next actionable
bid is .61, its take-profit exit closes at hypothetical P&L 6.50; if the next
bid is .49, the stop exit nets 48.25 and P&L is -5.50. The stop threshold does
not promise a sale at the threshold price. These examples assume sufficient
remaining bid capacity and no intervening exit. The forecast is never booked.

**Already beyond the stop.** With target bid .50 instead, initial liquidation
is 49.25, a loss of 4.50 against the same cost. The default policy refuses the
entry despite a positive model forecast.

**Entry spread consumes the move.** With target ask .58, forecast bid .59,
100 contracts and total cash fees 1.50, forecast net=1-1.50=-.50. No entry,
even though the leader move and predicted target direction are correct.

**Wrong sibling transfer.** A sweep claim rises .10. The series-win claim
contains that sweep, but no supported response model maps the move into a
series bid forecast. Result: `MODEL_UNAVAILABLE`, not an automatic +.10 forecast.

**Partial exit.** A position owns 100 retained contracts; only 40 have eligible
bid depth at the horizon. Sell those 40 with exact SELL fees, keep 60 in
`EXIT_PENDING`, and expose partial P&L plus residual cost. Do not value the
remaining 60 at the 40-contract bid and call the trade closed.

## 7. Implementation prerequisites

Reuse SDK book views, exact native walks, fees and game releases. Future bounded
history, signal/account state, entry-anchored timers and independent position
reading are required by the shared contract; current pure `evaluate`/fill episode
machinery does not implement them. State cohorts need complete/unknown score
quality, not blind use of GameView counts. Relationship-specific models and
past-event identities are genuine inputs; continuous gameplay telemetry and
measured real-world order latency are not prerequisites.
