# Economic position scenarios V1

Status: **SHARED CONTRACT — implemented locally by the bounded payout optimizer;
other proposed strategies remain separate work. No empirical/live validation.**

The [optimizer implementation](../../replay/strategies/bounded_payout_optimizer/SPEC.md)
has its own closed output, independent reader and synthetic offline tests. The
shared SDK itself retains its original detection-only API.

This is the shared economic contract for the proposed
[bounded payout optimizer](BOUNDED_PAYOUT_OPTIMIZER_V1.md),
[milestone leader–follower](MILESTONE_LEADER_FOLLOWER_V1.md),
[milestone fair value](MILESTONE_FAIR_VALUE_V1.md), and
[inventory substitution](INVENTORY_SUBSTITUTION_V1.md) strategies. It defines
their accounting and decision conventions, not a new execution platform.
Each strategy may implement the small amount of state it needs locally.

## 1. Authority, scope and current foundations

Authoring baseline: `35d0049f6201bb2418db48e9b4ec25bbc2ecc60a`, inspected in
the originating worktree. The author's saved-project checkout predates the
game overlay; no checkout was synchronized. Relative links below refer to
the baseline repository layout and remain portable when these proposals move.

Reuse the [economic SDK](../ECONOMIC_STRATEGY_SDK_V1.md),
[outcome masks](../OUTCOME_MASKS_V1.md), [claim algebra](../../analysis/README.md),
[game overlay](GAME_STATE_SDK_V1.md), [game input loader](../../replay/game_state.py),
[game runtime](../../replay/economic_sdk/game.py),
[fee SDK](../../replay/fees/README.md),
[fee bridge](../../replay/strategies/_shared/fee_bridge.py), and
[pinned preparation](../STRATEGY_PREPARATION_V1.md).
The [bench](../LOCAL_REPLAY_BENCH_V1.md) supplies pins and read-only inputs;
it supplies no economic decision or evidence of execution.

These proposals add hypothetical holdings and cash accounting beyond existing
SDK detections. They do not change capture, Rust reconstruction, transport,
deployment, existing strategy policies or historical outputs. No actual orders,
queues, matching probabilities, borrowing or transfers are inferred.

## 2. Three separate results

1. **Detection:** a candidate satisfies its economic predicate on committed
   observable books. An episode is a maximal positive-length interval of that
   predicate under unchanged candidate semantics. Superseded same-time states
   have no episode duration. Quote slices, maxima and Q(L) are diagnostics.
2. **Position-taking:** an explicitly declared scenario acquires or disposes of
   quantities, debits capacity/cash and creates or changes hypothetical holdings.
   The default is a simultaneous displayed-book valuation at one observer-time
   decision, labelled `SIMULTANEOUS_DISPLAYED_SCENARIO`; this is no assertion
   that multiple orders could execute atomically.
3. **Position outcome:** later sales or resolution produce hypothetical cash
   flows. `hypothetical_closed_pnl` requires every residual claim to be disposed
   of or paid. Open-position liquidation marks, payout floors and expected values
   are different fields. None is actual realized profit.

An SDK fill episode closing because a trigger fails, a kill price is reached,
a scope ends or a book becomes unusable does **not** sell an owned position.
The existing SDK §13 deliberately does not maintain a portfolio or deplete
shared route depth. Its fills and Q(L) must not be relabelled as these positions.

## 3. Admitted evidence and milestone knowledge

Only captured, mapped, usable native books may supply trade prices. A missing
catalogue member is `NOT_CAPTURED`; an unsupported mask, binding or payout rule
is explicit. Current catalogue rows do not supply historical membership or
historical prices. Preserve preparation's caller-pinned membership limitations.
Known observable closed/suspended trading status blocks a new transaction.
Absent status permits only the explicit `TRADABLE_IF_USABLE_SCENARIO`, the initial
policy here; record `trading_status_unknown` on the finding. Do not import a
retrospectively learned terminal status into earlier decisions. A stricter
known-open policy is a separate scenario and may greatly reduce coverage.
Require supported event, participant, scope, payout and normal-resolution rule
alignment for every relationship; equal claim IDs alone do not prove compatible
void policies or source rules. Pin the reviewed rule evidence and assumptions.

Series best-of comes from a supported exhaustive space/format, never the number
of retrospectively recorded maps. Incomplete spaces cannot establish a payout
floor or a complete probability distribution here. Void, cancellation, push,
unplayed-map and dispute branches remain `normal_resolution_only` exclusions;
do not assign an unplayed map a zero payout without supported semantics.

Use only released milestones: aligned completed-map results, supported score,
map-end times, phase and progression. Ignore opaque `details` in all four V1
strategies. Kills, gold, tactics and continuous in-map probabilities are absent
inputs. Scheduled start is a schedule, not proof of actual play.

Every run pins the current game policy and file. The recommended start mode is
`at_segment_end`, with explicitly configured release delays; zero delay means
an assumed immediate source-time release, not a measured receipt. Estimated
starts are never decision features in this default. An `estimated` sensitivity
run is labelled separately. Delaying starts also means `GameView.phase` may
remain `pre_match` or `between_segments` while physical play is underway; call
this the **released phase**, never a verified continuous live phase.

Maintain a bounded map `released_results[index] = aligned winner or unknown`
from released facts, deduplicated by index/source identity. Preserve the complete
contiguous winner prefix separately from aggregate score. Do not recover skipped
winners from final score, the prepared file's unreleased tail or future books.
GameView's accumulated score is not evidence of a complete score if a released
segment had no winner. Record known counts and a completeness flag. A separately
released supported final score may constrain score-dependent claims, but cannot
invent map order. A phase/map-end transition **without a winner does not shrink
the feasible outcome space** in these strategies.

For an exhaustive space, filter outcomes only by justified released constraints:
known indexed winners, a complete prefix, and supported complete scores. Keep
all sequences consistent with the information actually known. Missing earlier
winners do not invalidate an independently aligned later winner, but the prefix
remains incomplete. Contradiction or an empty feasible set is
`GAME_CONTRADICTION`; never evaluate a minimum over an empty set as a profit.
New entries requiring the affected knowledge stop. Existing holdings remain.

`segment_settled` in the current overlay carries an index, not a fresh winner
or settlement cash flow. Duration/settlement priors are forecasts, not facts.
A known game winner does not by itself prove every venue has paid its contract.

## 4. Decision time and transitions

Use committed observer time and the existing scope/release/timer/same-time book
staging. At a time t: apply the scope and released facts, apply the complete
same-time books, then make one economic decision on that final state. At a quiet
timer use the last usable books observable by t. Venue/send timestamps never
reorder decisions or measure order latency. Age/skew are annotations unless an
explicit policy gate uses them; last-change age is not measured network delay.

Process cash credits whose declared availability is due, then exits/replacements
for existing positions, then new-entry candidates in deterministic order. An
exit and a re-entry in the same claim at the same t are forbidden. Signal
history is observed independently of whether capital is available.

The default decision delay is zero. A separate delay scenario freezes the signal
identity, side and maximum intended quantities at t, and schedules t+d. At t+d
revalidate knowledge, quotes, fees, cash, capacity and the strategy's predicate.
It may shrink only according to the strategy's declared sizing rule; it cannot
increase the intended quantity or silently select a different signal/route.
Unknown or failed conditions cancel that attempt. Do not wait for future
survival before deciding at t. Uncalibrated live latency is no blocker.

Common lifecycle (strategy documents refine the predicates):

| State | Event and action |
|---|---|
| `INADMISSIBLE` | Record missing/unsupported evidence; reassess on relevant evidence/scope change. |
| `READY` | Candidate is supported; evaluate on its declared observable triggers. |
| `SIGNALLED` | Predicate crossed true; record the proposed transaction and its evidence. |
| `PENDING` | Optional delay timer; reserve no depth or cash before position-taking. |
| `OPEN` | Revalidated complete transaction posts cash, retained quantities and capacity debits. |
| `EXIT_PENDING` | An irrevocable exit reason has fired; sell available eligible quantities as specified below. |
| `CLOSED` | All holdings disposed of or paid; compute hypothetical closed P&L. |
| `CENSORED` | Run ended with holdings/uncredited payout; report residuals and marks, not closed profit. |

Cancellation returns to `READY` only after the strategy's rearm condition.
Unavailability is not a false economic signal and cannot by itself rearm one.
Default maximum entries is one per event per strategy scenario; a larger pinned
limit enables the specific re-entry rules and capacity accounting below.

## 5. Exact fills, fees, native cash and capital

Use native integer atoms/scales and exact rational intermediates, preserving
scale-36 price-times-quantity arithmetic where applicable. No binary-float
economic comparisons. Size grids, order increments, time thresholds and valuation
weights are pinned decimal/integer values. Never silently round a notional into
a fee model that cannot represent it. Report `FEE_UNKNOWN` instead.

BUY walks asks best-first. Kalshi BUY of an orientation walks the opposite
orientation's bids at 1-p; retain the acquired orientation's mask and fee binding.
SELL walks the owned orientation's bids. Masks do not create a sellable token,
inventory, synthetic short or a second pool of liquidity.

Use the Fee SDK through native per-order assessments, with one declared order
per leg per action and one ordered fill per consumed level. New exit attempts
are new orders. Preserve assessment identities and the per-level partition
assumption; do not import rounding carry from unrelated alternatives. The current
bridge supports one BUY/SELL direction per call: mixed inventory replacements
need separate calls and combined native accounting. Do not use the complement
bridge's min-quantity basket shortcut for arbitrary payoff vectors.

For a BUY, post the fee result's net collateral delta and net received outcome
quantity h. A token charge reduces h, not cash at the execution price. A SELL
must debit no more outcome quantity than is owned, including token fees if ever
supported, and credits net collateral once. Unknown required fee components
block a net entry; a gross detection can still be reported. Fees for exits are
also required before posting a sale.

Use the frozen reviewed fee configuration/reference; the current bridge is a
current-snapshot scenario, not automatically historical fees. Preserve its
evidence, exclusions and `current_snapshot_not_historical_fee_claim` label.
Any historical fee mode needs independently supported applicability. No rewards,
rebates, refunds or zero-fee fallback are invented.

Keep cash by `(venue, asset identity)` and holdings by native claim/token.
There are no negative balances, collateral transfers or cross-venue borrowing.
Pin initial cash, per-transaction/event budgets, maximum outstanding cost and
maximum concurrent positions. These inputs are required, without invented
account balances. Purchases debit immediately; sales credit at their scenario
decision time. Capital stays unavailable until an explicit credit occurs.

Scalar comparison requires either a single native collateral asset or the
existing explicit `PARITY_SCENARIO` covering all assets. Preserve native flows
even under parity. Optional pinned valuation stress weights form separate
scenarios; they are not actual FX conversions. Unknown valuation prevents a
scalar net verdict. Holding charges default to zero with
`ZERO_FINANCING_COST_SCENARIO`; a configured exact per-time/per-asset charge
accrues on outstanding cost. In these V1 proposals this is an analytical
opportunity-cost adjustment, not a borrowed-cash flow: report trading cash P&L
and holding-cost-adjusted P&L separately, and never credit the adjustment as cash.
An actual funding-fee cash ledger is outside V1. Unknown settlement duration produces conditional
holding-cost sensitivity, not a fabricated finite maximum cost.

## 6. Shared displayed depth and capacity

Detections may inspect the full visible ladders and are non-additive. Positions
within a scenario share one small local capacity ledger. Key resources by actual
native source side, including projection aliases: buying Kalshi YES and selling
Kalshi NO consume the same NO bid source. Different routes never get independent
copies. Explicitly evidenced other aliases are also coalesced; unresolved possible
aliases prevent an additive capacity claim.

Recommended mode is `CUMULATIVE_DISPLAY_CAP`, a conservative scenario, not a
queue reconstruction. For each physical source side and price keep cumulative
quantity consumed D[p], and D_total for that side, across the whole event.
For currently visible quantities Q[p], an action may take x[p] only if:

```
0 <= x[p] <= max(0, Q[p] - D[p])
sum(x[p]) <= max(0, sum(Q[p]) - D_total)
```

Walk the eligible ladder best-first under both constraints, then increment both
debits. Observed trades, deletions, identical snapshots, epoch changes, a scope
boundary and hypothetical sales do not reset these debits. The total-side cap
also prevents simply repricing identical aggregate quantity from restoring all
spent capacity. Increased displayed quantity may create headroom under the
formula; this is the declared capacity scenario, not proof of new independent
orders. Hidden order identity and our absent market impact remain limitations.

This rule is deliberately conservative during later book shrinkage. An isolated
single-decision valuation may also be reported without depletion, explicitly
`ISOLATED_NONADDITIVE`. Never sum those alternative values into portfolio profit.
Alternative sizes, delay scenarios, baselines and separately launched strategy
groups have independent accounts; their profits/capacities cannot be added.
A combined run must share debits and cash, pin strategy/route priority, and
reprice after each accepted action. No cross-process coordinator is required by
these specs; without a combined run, publish no joint capacity total.

## 7. Exits, resolution and portfolio accounting

Once a liquidation reason fires, it remains latched. Default exit behavior is
`SELL_AVAILABLE_THEN_WAIT`: sell the largest supported grid quantity permitted
by owned inventory, usable current bids, fees and remaining capacity. Retry on
relevant bid/validity/fee-availability changes, not on identical states. Depth
shortage gives `PARTIAL_EXIT`; no eligible quantity gives `NO_EXIT_DEPTH`.
Unknown fees/books give an explicit unavailable exit. Holdings and original
cost remain; never assume a fill at a last price or midpoint.

Lots use deterministic FIFO basis allocation, proportional exact cost within a
lot. Retain residual dust that cannot satisfy a declared increment. Record both
closed-lot P&L and residual cost; headline full-position P&L waits for closure.
Do not enter another position in that claim while any residual is open.

Resolution has two separately labelled modes:

- `PINNED_SETTLEMENT`: supported per-contract payout evidence and availability
  times post native cash, including supported exceptions if explicitly modelled.
- `NORMAL_RESOLUTION_SCENARIO`: a released result fixes the supported claim's
  normal payout; a configured nonnegative settlement delay posts that amount.
  This assumes normal settlement and availability at that time. A game's
  `segment_settled` timestamp is not universal payout evidence. Zero delay is
  permitted only with the explicit immediate-availability scenario label.

For the normal-resolution scenario let u be the first release time at which the
claim's supported payoff is determined, and d its pinned availability delay.
Credit an owned lot at `max(lot_open_time, u+d)`. Thus a later known-outcome
purchase does not receive a credit in the past. An immediate credit following
an opening is processed after that transaction, without permitting another
same-time entry. Pinned-settlement mode uses its evidenced availability time
with the same no-pre-acquisition-credit rule. Record these cases separately
from contracts whose trading status was known open at acquisition.

No compatible result or no configured cash-availability rule means unresolved
holdings. A pure minimum-payout opportunity can still be measured. Ex post final
outcomes may score positions separately, but cannot release trading cash early
or change earlier decisions. Rule contradictions suspend assumed resolution.

Book invalidation never destroys acquired claims. A scope change rechecks
admission and cancels affected pending entries; held positions keep their original
contract identity/cost. If their books cease to be observable, retain them as
unpriced until a supported exit or resolution. At run end, report `CENSORED`.
Liquidation marks use actual current bid depth and fees, report unpriced residuals
and are never credits. Positive payoff floor, expected value, liquidation mark,
closed-lot P&L and closed-position P&L remain separate.

## 8. Output obligations and implementation gaps

Record the pins, source revision, rule/fee/model identities, game release policy,
valuation, initial account, capacity mode and all thresholds. Each decision must
carry t, released state/knowledge quality, consumed source levels and aliases,
quantities before/after fees, native cash/holdings before/after, reason, model or
payoff calculation, and detection/signal/position lineage. Final reports include
admission and measurable-time denominators, detected episodes, attempts, skipped
entries by reason, capacity debits, full/partial/no exits, censored residuals,
native P&L and scenario P&L. Do not sum overlapping route-time into event-time.

An independent reader must reconstruct payoff/model arithmetic, fee assessments,
cash/holdings conservation, depth conflicts, lifecycle and output identities.
Unknown output/schema versions fail closed. This document defines economic
content, not a new wire schema; future implementation must version its own
closed output and readers without altering existing strategy artifacts.

The baseline SDK has pure stateless `evaluate`, static baskets, a latest-result
GameView, game-fact timers and detection fills. It lacks the proposed economic
account, released-prefix history, general bounded optimizer, learned models,
entry-anchored deadlines and position outcomes. A bounded strategy-owned decision
state integration is required. In particular, retaining only successive
`last_segment` values can miss several same-time/prologue releases; a future
integration must deliver/retain every **released** fact, without exposing pending
facts. Position timers must be scheduled explicitly in observer time; the existing
game timer formula `max(release, source+offset)` is not `entry+offset` or
`release+offset`. These are implementation gaps, not dependencies on rich telemetry.

The optimizer package now has offline contract-shaped search/account/game/output
tests and a real Decoder/bench reader path. No live calls or empirical
measurements accompany this contract.
