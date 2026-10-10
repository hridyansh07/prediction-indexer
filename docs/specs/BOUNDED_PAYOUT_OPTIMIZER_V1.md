# Game-aware bounded minimum-payout optimizer V1

Status: **PROPOSED — first implementation priority; not implemented or empirically validated.**

The [shared economic scenario contract](ECONOMIC_POSITION_SCENARIOS_V1.md) is
normative for visibility, exact accounting, fees, capacity, positions and outputs.
This document specifies the optimizer's decisions. It subsumes complete sets,
multi-market/cross-venue covers, overlapping claims, unequal quantities,
implications and known-outcome purchases; these are route classifications, not
separate strategies whose profits may be added.

## 1. Question and admissible payoffs

At a committed observer-time decision, can a bounded portfolio of captured
claims have a positive minimum normal-resolution payout after acquisition costs,
fees and declared holding charges? Separately, what happens to a hypothetical
portfolio opened on that finding and held to resolution?

Use one supported event/scope/exhaustive outcome space per optimization problem.
Pin masks, native unit payouts, normal-resolution compatibility and valuation.
Reject incomplete spaces, unknown masks, conflicting format/participants and
unsupported unplayed-map/void semantics. An unrelated unsupported sibling does
not veto otherwise supported candidates. Do not fabricate uncaptured NO tokens.

The reference problem uses the full pinned space Omega. The game-aware problem
uses Omega_t, filtered only by supported released results as defined in the shared
contract. Both are measured with independent scenario accounts. The default game
policy is optional: unavailable game state permits the static problem, labelled
`STATIC_GAME_UNAVAILABLE`; it does not create state-conditioned evidence.

New equivalences and implications are recomputed on Omega_t. A previously
nonconstant admitted mask may become empty or total: retain it for this decision.
A total mask is a known-normal-payout acquisition candidate; an empty mask has
no positive long-only value. A book rejected by preparation as unmasked/tautology
is not retroactively reconstructed from prose or prices.

At 1-0 to A in a supported Bo3, under-2.5 maps and A map-2 winner coincide; A
map-2 winner implies A series winner. At 1-1, A map-3 winner and A series winner
coincide. These relations require released results and aligned identities, not
merely a map-end clock tick.

## 2. Objective and finite search domain

Every action is BUY. For each native claim i choose purchased quantity q_i.
Walk the current eligible ask source, then assess the complete order's fees.
Let h_i(q_i) be retained quantity and c_i(q_i,a) the signed native collateral
delta (negative for acquisition); a names the collateral asset. For outcome w,

```
F_a(w,q) = sum_i c_i(q_i,a)
           + sum_i h_i(q_i) * payout_i(w,a)
M(q) = min_{w in Omega_t} value(F(w,q)) - holding_charge_bound(q)
```

The objective is maximum M(q), not maximum return percentage or maximum payout.
Include q=0 with M=0. Under a single valuation scenario, `value` uses its exact
weights; a configured robust run also takes the minimum over its pinned finite
valuation scenarios. Retain every native outcome vector. A positive scalar floor
under parity is not a native-asset convertibility guarantee.

Constraints:

- q_i is a nonnegative multiple of that leg's declared increment and no greater
  than its per-leg cap, supported depth and native account budget.
- At most `max_legs` distinct acquired native claims have positive quantity.
  Equivalent claims on different venues remain routing alternatives; duplicate
  occurrences of the same native claim are one variable/order.
- Aggregate all uses of an underlying source side/level before checking capacity.
- Per-venue/asset cash, transaction/event budget, outstanding-cost and position
  limits all hold. No sale, short, borrowing, merging or collateral transfer.
- Exact fees are required for a net candidate. Nonzero holding charges require
  an explicit assumed maximum holding duration to define the bound; no duration
  evidence/assumption means `HOLDING_COST_UNKNOWN`, not a finite payout guarantee.

Recommended search policy, all values pinned and configurable:

| Field | Initial value/requirement |
|---|---|
| `max_legs` | 4; admitted range 1–8 |
| `max_books` / `max_outcomes` | 128 / 1,024; excess rejects this problem visibly |
| `quantity_increment` / `quantity_cap` | Required per book; initial survey may use 1 / 100 contracts if representable |
| `max_search_nodes` | 100,000 exact portfolio/partial-bound evaluations per decision |
| `minimum_net_margin` | 0.01 in the pinned valuation unit |
| `minimum_return_bps` | 0; entry still requires strictly positive M |
| `decision_delay_ns` | 0 |
| `max_entries_per_event` / `max_open_positions` | 1 / 1 |
| `rearm_nonpositive_ns` | 1,000,000,000 when repeated entries are enabled |
| `holding_cost` | Explicit zero-financing scenario, or configured bound above |

This is a bounded discrete quantity search, including unequal quantities.
Seed it with compatible one-leg total masks, disjoint partitions and two-leg
implication covers, then enumerate remaining supports/quantities in a pinned
deterministic order. Equivalent descriptions deduplicate by native purchased
quantity vector, not relationship name. An implementation may use exact valid
branch bounds, but cannot assume fee concavity or prune by a one-contract net
test: fee rounding may make a larger purchase positive when one contract is not.

At a search limit, retain independently repriced feasible solutions and record
`SEARCH_LIMITED`, visited counts, search policy and any valid bound. A feasible
positive may open; an absent positive is **not** a complete negative. If the
finite domain is exhausted, report `SEARCH_COMPLETE` and its best value. No
claim of an unrestricted global optimum is made. Detection predicates mean
"this bounded search found a qualifying portfolio" and carry completeness.

Tie-break equal exact margin by lower cash commitment, fewer legs, then sorted
native book keys and quantity atoms. Emit the winning portfolio and a bounded
list of alternatives. Detect on undepleted books; before opening, solve/reprice
against actual scenario cash and remaining capacity. Alternatives are never
independent position profits.

## 3. Evaluation, entry and capacity

Re-evaluate after relevant ask/source depth changes, usability changes, scope
changes, released game facts that alter knowledge, and configured delay/rearm
timers. Bid changes alone do not change this all-BUY acquisition objective,
except where they are Kalshi ask sources. Identical updates do not generate a
new signal. A game transition can trigger evaluation on otherwise quiet books.

Candidate states follow the shared lifecycle. A qualifying signal requires all
admission/fee/valuation conditions and:

```
M(q) > 0
M(q) >= minimum_net_margin
10000 * M(q) >= minimum_return_bps * acquisition_cash_value(q)
```

The first committed qualifying decision may open immediately. It does not wait
for the detection episode to survive. Under delay, freeze the selected support
and quantity caps; at the due time search only subquantities on that support,
requiring the same gates. A changed feasible space is allowed only if all new
constraints were released and the route's contract/rule identity remains valid.

Opening is all-or-none within the simultaneous displayed scenario: all legs,
retained quantities, capital and capacities validate before any balance posts.
If one fails, buy none. This modelling convention avoids inventing partial
cross-venue fills; an optional sequential-leg scenario is outside V1.
Record conditionality explicitly. Once opened, retain a fixed lot per acquired
claim and the entry payoff proof; subsequent detection changes cannot resize it.

Within this event the optimizer selects one joint portfolio, so several route
descriptions cannot independently consume the same cheap leg. If multiple
entries are enabled, re-solve against remaining cash and shared capacity. Keep
the existing positions' quantities/costs in portfolio reports, but the new
transaction must have a positive incremental floor on its own; V1 does not
subsidize losing additions with past gains.

## 4. Holding, resolution and re-entry

Default exit is `HOLD_TO_NORMAL_RESOLUTION`. A market-price reversal, loss of
the current detection or a kill price never liquidates the portfolio. New
released results may increase the conditional floor, but that increase is not
newly earned cash. Report entry floor, current remaining payout vector, capital
still committed and optional bid-side liquidation value separately.

Each leg resolves under the shared explicit settlement mode and availability
rule. Partial settlement credits that leg only; other holdings remain open.
An unavailable book does not prevent a supported settlement. Unsupported
cancellation or contradictory result leaves affected holdings unresolved and
invalidates the conditional guarantee label; do not manufacture a zero or refund.
Run end with any residual is censored. Early discretionary sales are outside
this V1; inventory substitution can be evaluated as a separate declared policy.

With the default entry cap there is no re-entry. With a larger cap, the previous
portfolio must close, the best bounded-search margin must have been known
nonpositive for `rearm_nonpositive_ns`, and a later qualifying crossing must
occur. Unknown/search-limited-with-no-solution intervals do not establish the
nonpositive rearm. Reuse the same event capacity ledger and actual remaining
cash. A new scope or claim description does not reset the event entry count.

## 5. Hand calculations (synthetic, no empirical claim)

**Three-leg partition.** A series win at .46, B 2-0 at .20 and B 2-1 at .25
partition a normal Bo3. BUY 100 of each: cost 91, floor 100. If exact collateral
fees total 3 and token fees are zero, M=6. The legs may cross venues only under
the shared rule and asset basis. At a required size of 100, a B 2-1 ladder with
only 20 available prevents that transaction; a smaller feasible solution needs
its own complete repricing, not the 100-contract headline margin.

**Overlap missed by partitions.** Across categories A 2-0 / A 2-1 / B wins,
A-series YES pays (1,1,0), NO A-2-0 pays (0,1,1), and NO A-2-1 pays (1,0,1).
At .60 each, BUY 50 each costs 90, with exactly two winning legs paying 100.
Collateral fees of 3 leave M=7. Each two-leg pair alone costs 1.20 against
a minimum payout of 1; a pair-only search misses the positive.

**Unequal quantities after token fees.** Complementary claims at .40 and .58,
100 bought on each, cost 98. A 3% token fee on the first leaves (97,100): M=-1
before any other fee, so no entry. On an integer quantity grid, buy 104 of the
first and 100 of the second: retained (100.88,100), cost 99.60, floor 100,
M=.40 under the synthetic exact 3% fee and zero other fees. This opens only if
104 units, their capital and the threshold are available. Unknown fee rounding
is not licensed by this illustrative formula.

**Result visibility.** At released 1-1, BUY A-series YES at .48 and map-3 A NO
at .49. One unit pays 1, giving .03 gross; .04 collateral fees make net -.01,
so the relation is valid but no position opens. Before the supported 1-1 result,
the two claims need not form a cover at all.

**Known winner.** A compatible claim fixed to win by released results, ask .985,
quantity 100: floor 100 minus 98.50 cost minus .40 cash fee = 1.10. A 3-contract
acquisition fee instead reduces the floor to 97 and prevents entry. A released
phase change with unknown winner cannot produce the known-winner candidate.

## 7. Results and implementation boundary

Report static versus state-conditioned opportunities separately; classify the
winning vectors as partition, implication, general overlap or known payout
without double-counting the same vector. Show exact gross/net floors and margins,
quantity/capital frontier, completeness/bounds, depth/capital blocked attempts,
capacity-aware opened positions, settlement cash and censored residuals.

A measured positive is a repriced feasible portfolio with M>0 under declared
rules, fees, visibility, depth and asset/holding assumptions. A complete negative
is no positive in an exhaustively searched declared domain during measurable
time. Missing inputs and limited searches are neither economic negatives nor
ordinary absence. No probability model or actual execution proof is required.

The current [multi-market](../../replay/strategies/same_venue_multi_market/SPEC.md),
[cross-venue](../../replay/strategies/cross_venue_arbitrage/SPEC.md) and
[implication](../../replay/strategies/_shared/implication_cover/SPEC.md) contracts
provide reusable pricing/proof boundaries, but do not implement this general
search, dynamic proof, capacity ledger or held-position lifecycle. Future work
must address the shared released-fact/state/timer/reader gaps. Genuine input
prerequisites are reviewed masks/rules, fee/economics bindings, capital and
visibility/settlement assumptions; richer game telemetry is not required.
