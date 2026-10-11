# Bounded payout optimizer: masks, triggers and acquisition curves

Status: **PROPOSED — settled design decisions and advisory extensions; no implementation.**

Written 2026-10-11 against `codex/payoff-optimizer` at
`f77a5cf1fe7f339e4a0202d6b3d6568c417416e9`. The implementation inspected for
this note is on that separate branch; this authoring worktree contains the
shared specifications but does not contain the optimizer package yet.

Updated after the design discussion in
[this thread](https://ampcode.com/threads/T-01a12a46-bc76-70dc-812f-dcbdcb38705e).
Sections 1–10 record the settled scope and episode semantics for this proposal;
sections 11–12 are algorithm and performance recommendations, not adopted solver
choices or measured results.

This proposal changes the search and reporting policy in
[the V1 optimizer specification](BOUNDED_PAYOUT_OPTIMIZER_V1.md): its primary
result is now an opportunity episode with one onset allocation, not a held-position
simulation. For this proposed pipeline, the decisions below replace V1's broad
mask-count domain, percentage-return gate and position-taking lifecycle. Other V1
policies and the existing implementation are unchanged. Reuse the exact native
pricing, fee, visibility and within-portfolio source checks in
[the economic position scenario contract](ECONOMIC_POSITION_SCENARIOS_V1.md).
Any future held-position analysis remains a separately declared scenario under
that contract; it is not required to discover or report these episodes.

## 1. Settled scope and objective

Build candidate baskets from **distinct payoff masks**, then retain native books
as acquisition routes for those masks. Each verified allocation must acquire
positive grid-sized quantities in **exactly three or four distinct masks**.
One-/two-mask plays are outside this strategy's scope. Do not infer that their
economic domain is exhausted by other strategies, or exclude a compatible
three-/four-mask family merely because another strategy describes it.

Maintain a cheap price screen for each basket. A relevant book update should
first refresh that screen; detailed quantity sizing begins only when the screen
cannot rule out a qualifying purchase. Build cumulative acquisition curves only
for sizing attempts, and extend them only as far as economically justified.

Include the applicable pinned fees in acquisition costs. Cache repeated fee
calculations and cumulative prefixes. Use a continuous optimization relaxation
to find promising sizes and bound unexamined quantities; refine and verify the
actual lot-sized, rounded-fee positions exactly.

At a successful onset, **size once using only the committed state at that time**.
Freeze the proposed native allocation, acquisition cost and payout proof. On later
updates, **reprice that frozen allocation against current depth** to determine the
episode end (section 9); do not resize the allocation or select a retrospectively
better allocation from the interval.
An unsuccessful attempt does not open an episode; the retry recommendation is in
section 11.4.

The objective remains **maximum minimum net profit**, after fees and declared
holding charges, rather than maximum payout, contract count or return percentage.
Require a minimum all-in acquisition commitment and an absolute net-margin floor.
There is no percentage-return gate in this proposed policy; numerical thresholds
and the versioned configuration fields remain to be pinned. Minimum commitment
means portfolio cash acquisition value, not a lower price bound on each contract.

## 2. Overlay on the current pipeline

All current-behavior implementation names refer to the inspected optimizer branch.
The proposed column describes the episode-first pipeline, not shipped behavior.

| Stage | Current behavior | Proposed change and where it fits |
| --- | --- | --- |
| Prepare and validate inputs | Pin membership, exhaustive outcome spaces, native economics, rules, quantities, fees, game policy and account configuration. | Pin the three-/four-mask policy, trigger identity, minimum commitment and net margin. Preserve admission requirements; do not require a held-position account or settlement configuration for opportunity reporting. |
| Stage observable state | `Runtime.__call__`, `release`, `flush` and `decide` commit same-time books, scope changes, released facts and timers. | Preserve this ordering. Add basket dependency tracking after the committed state is available; never use future updates or eventual episode duration to size the onset. |
| Build native models | `scenario.models` constructs a model for each eligible native book, with its mask, source, scales, grid, cap and rule binding. | Separate structural mask groups from native routing models. Compile basket geometry on structural changes, rather than reconstructing native-book support combinations on every price update. |
| Select baskets | `core.solve` seeds covers and enumerates supports from one through `max_legs` native books, subject to the configured limit. | Generate only compatible three-/four-mask baskets. Check outcome coverage before book-depth work; do not add family exclusions without a separately approved domain proof. Count logical masks separately from native orders. |
| Decide whether to size | The current whole-input fingerprint skips identical acquisition states. Other relevant changes can start quantity search; there is no maintained per-basket price gate. | Recheck only affected baskets. Size a potential onset; after a successful opening, only reprice the frozen allocation. Failed-attempt invalidation is a separate concern, not permission to resize an open episode. |
| Acquire prices and fees | `scenario.price_order` walks a requested quantity and assesses one ordered fill per consumed native level. A bounded cache already stores pinned whole-order economic amounts. | Build bounded cumulative prefixes once, including fees. Cache schedule/price/quantity fee primitives separately from source-dependent curves. Query earlier full levels from the prefix and price only the final partial level. |
| Optimize quantities | `core.solve` enumerates quantity grids with gross branch bounds; `PortfolioEvaluator` scores compiled mask arithmetic. | Solve a segment-based continuous relaxation, expand relevant curve prefixes and use its bounds to guide exact discrete refinement. Avoid the full quantity cross-product where a valid bound eliminates it. |
| Verify and qualify | Retained candidates receive fresh fee proofs. Exact margin and return gates determine qualifying findings. | Verify native quantities, exact fees, retained payouts, valuation, holding charges and joint source use. Enforce minimum commitment and absolute margin within sizing; do not percentage-gate only the unconstrained winner. |
| Open an episode | Detection uses undepleted books. Immediate entry re-solves against account cash and cumulative capacity; delayed entry freezes native support and maximum quantities. | Record a verified onset allocation on that committed snapshot. Opening is analytical, not a cash/depth debit or execution claim. Failed or unknown sizing does not open an episode. |
| Track and close | Current episodes follow the selected quantity vector; fixed acquired lots separately remain until supported resolution. | On each update to a book the frozen allocation uses, walk current depth for the same native routes and quantities with exact fees. End when any leg can no longer fill its frozen quantity, the repriced margin fails the gates, or an unavailable/semantic boundary occurs; censor still-active episodes at run end. No held-position settlement is required. |
| Write and independently read | Version-2 output carries economic proofs and lifecycle evidence. `audit.py` and `output.py` independently reprice and reconstruct them. | Version the episode policy/output and carry onset, repricing, screening and solver evidence. Independently check exact opening portfolios and the repricing at each interval boundary without importing the writer's optimizer. |

```diagram
Compatible masks and native routes
                │
                ▼
Three-/four-mask baskets and book dependencies
                │
                ▼
Committed update → affected basket screens
                │
                ├─ ruled out → wait for relevant update
                │
                ▼
Potential onset → lazy curves and bounded exact sizing
                │
                ├─ failed/unknown → no episode; retry policy in §11.4
                │
                ▼
Verified opening → freeze onset allocation and proof
                │
                ▼
Reprice frozen allocation at current depth
                │
                ├─ depth short / margin fails / unavailable → close interval
                │
                ▼
Still qualifying → continue, or censor at run end
```

## 3. Masks are economic legs; books are native routes

An outcome space is the set of possible normal-resolution outcomes. A payoff
mask is the subset on which a claim pays. Sharing an outcome space does not
make A-wins and B-wins the same mask.

Aligned A-wins claims on Polymarket and Kalshi have the same payoff mask under
the reviewed rule assumptions. They remain separate native routes with their
own fees, lots, cash balances, depth and source identities. Mask equality alone
does not establish matching cancellation rules or USD/USDC convertibility.

Split acquisition is **already possible** in the current optimizer: separate
native variables with the same mask can both receive positive quantities. The
change is to organize and size those routes under one logical leg, rather than
counting venue alternatives as different basket masks. Three masks can therefore
require more than three native orders.

Use separate limits for distinct masks and native acquired orders. Do not silently
reinterpret V1's `max_legs`, which currently counts native acquired claims.
Preserve native quantity grids, declared sizing budgets and aggregate physical
source consumption within each candidate. A pooled curve must retain its route
decomposition; scalar valuation does not establish native collateral transfers.

Compile groups separately for the static full-space and released-state-conditioned
problems. Retain original full-space masks and rule identities for traceability.
Supported released results can change conditional equivalences and basket
coverage. A map-end or phase transition without a winner does not shrink the
space. If conditioning collapses a basket to fewer than three distinct masks, it
is outside this policy. Frozen onset allocations keep their original native
identities and quantities; changed semantics end the old episode rather than
retroactively changing its proof.

## 4. Candidate construction and existing-strategy exclusions

Generate sorted, canonical sets of three/four distinct masks within one compatible
exhaustive outcome space. For long-only claims with nonnegative costs, a basket
that leaves a feasible outcome entirely uncovered cannot have positive minimum
profit. Reject that structure before sizing. Coverage alone does not prove profit.

Duplicate native listings do not multiply the number of structural baskets.
For illustration, 12 native books representing six distinct masks have at most
35 three/four-mask subsets before coverage and policy filters: 20 triples and
15 quadruples. This is a combinatorial upper bound, not a measured viable count.

Record exclusions explicitly. The three/four-mask policy intentionally excludes
V1's one-leg known-payout and two-mask candidates; exclusion is not evidence that
those trades have no economic opportunity.

No additional existing-strategy family exclusions have been adopted. Any later
exclusion needs an approved proof covering its routing, quantity, rule and fee
domain. A same-venue complete-set implementation does not establish coverage of
all cross-venue versions, unequal quantities or profitable larger sizes hidden by
a one-contract rounded-fee trigger. Those supported three-/four-mask baskets
must reach the optimizer rather than being removed before their curves are built.

**Do not discard a larger basket merely because a subset is a known cover.**
For example, on outcomes A-2-0 / A-2-1 / B-wins, these three masks pay:

| Mask | A-2-0 | A-2-1 | B-wins |
| --- | ---: | ---: | ---: |
| A-series win | 1 | 1 | 0 |
| NO A-2-0 | 0 | 1 | 1 |
| NO A-2-1 | 1 | 0 | 1 |

At .60 each, every equal-sized pair costs 1.20 per unit against a minimum
payout of 1. All three cost 1.80 against a minimum payout of 2. Excluding every
superset of an existing pair family would remove this useful overlap basket.

## 5. Cheap price triggers and dependencies

The trigger has two boundaries: a cheap **potential-opportunity screen** and the
exact **opening-allocation predicate**. Passing the screen means sizing may be
worthwhile; it does not prove that an executable grid-sized net portfolio exists.
A verified qualifying allocation starts an episode at that committed time.

For a declared fixed ratio r, let L(r) be its minimum unit payout. Before fees,
an additional basket unit improves profit while:

```text
sum_i r_i * marginal_price_i < L(r)
```

For the equal-sized overlap above, this becomes `p1 + p2 + p3 < 2`.
With fees, use applicable effective acquisition prices and holding charges.
Testing only one ratio or a bid/ask spread cannot rule out all unequal-quantity
opportunities. Section 11 recommends a geometry-aware covering trigger; its
algorithm/backend has not yet been adopted.

For free quantities, use a small optimistic relaxation: permit available routes
to acquire up to their caps at their best current prices, with a supported lower
bound on fees. Ignoring additional constraints is allowed for an upper bound;
inventing cheaper prices or fee applicability is not. A certified nonpositive
upper bound can screen out the declared basket. A bound below an entry threshold
can screen out entry eligibility without labelling all profit nonpositive.

The dependency index maps native acquisition sources to their basket users.
Price, relevant depth, usability, scope and supported knowledge changes invalidate
the affected screens and failed-attempt evidence. Fee availability/model changes
and quantity rules also matter. A cheaper alternative venue must invalidate a
basket whose previously selected route did not change.
Changes beyond a retained prefix may be ignored for sizing only if the existing
tail certificate still covers the updated state.

These subscriptions are strategy work selection. Capture and replay continue
preserving the input evidence. After successful onset sizing, updates to the
books an open episode uses reprice its frozen allocation (section 9) and check
observability/semantic boundaries. They do not resize or replace that episode's
allocation, and the screen no longer decides its end. A later separate held-position
scenario would need its own dependencies and accounting.

## 6. Cutoff prices and lazy curve construction

For a fixed ratio, the gross marginal cutoff for book i is:

```text
p_i,max = (L(r) - sum_{j != i} r_j * p_j) / r_i
```

For `r = (1,1,1)`, `L = 2` and the other two prices .60, the cutoff is .80.
Cheapest prices on the other legs give an optimistic cutoff. Their increasing
prices and applicable fees tighten it. With price-dependent cash fees, compare
the marginal all-in cost with the remaining payout allowance, rather than
subtracting one constant fee from every raw price.

This is a cutoff for additional profit along that ratio. It does not mean the
already accumulated basket becomes negative after the next marginal unit loses
money. It also does not establish a universal cutoff for independently varying
quantities, rounded fees or every route allocation.

For unrestricted quantities within the declared baskets:

1. Build short best-first prefixes, only at occupied book levels.
2. Model an unseen tail optimistically at its next available price, with remaining
   configured quantity cap as an upper bound on depth. Use a fee lower bound and
   relax any unresolved native constraints when necessary to preserve optimism.
3. Solve that relaxation to bound what deeper real acquisition could achieve.
4. Expand the routes used by a potentially improving optimistic tail.
5. Stop when a verified bound rules out improvement, or a declared work limit is
   reached. A work-limit stop preserves the remaining uncertainty.

Later raw asks cannot be cheaper in a best-first ladder. The corresponding
fee/retained-payout bound must also cover the unseen prices; do not assume an
arbitrary fee-adjusted curve is monotone. If that property is not established,
use a looser valid envelope or expand more depth.

Keep prefix quantity, gross cost, fee-adjusted cash cost, retained quantity,
native route and physical source use. A curve query uses the complete preceding
levels plus an exact partial final level. For example, 100 contracts at .60 plus
20 at .62 cost 72.40 before fees, not `120 * .62`.

## 7. Fees inside the curves

For the Fee SDK's supported ordinary cash-fee models, the unrounded formulas are:

```text
Kalshi:     F(q,p) = q * .07 * multiplier * p * (1-p)
Polymarket: F(q,p) = q * rate * [p*(1-p)]^exponent
```

At a fixed occupied price, these are linear in quantity. The unrounded segment
cost is `q * (p + fee_per_contract(p))`. Resolve and pin the applicable schedule;
there is no universal venue-wide rate fallback.

Precompute or lazily memoize exact rounded costs for the declared quantity grid.
Keys must cover the resolved model, policy, side, price, quantity, native fee
asset/scales and any relevant rounding state. Equal numeric models may share
economic primitives across books; native bindings and fresh assessment identities
are retained separately. Source-dependent prefix costs additionally depend on
the eligible ladder and declared source/cap constraints.

Preserve one declared order per acquired native claim per action, with one
ordered fill per consumed level. Aggregating different price levels into one average-price fill
changes both the nonlinear price formula and rounding. Multiplying a rounded
one-contract fee by 100 is not the same as assessing a declared 100-contract fill.
The current optimizer excludes account rebates and rounding refunds; preserve
that policy unless a separately specified change introduces stateful accounting.

Synthetic example, .62 asks, ordinary Kalshi taker multiplier 1, non-direct
membership and a single declared fill:

| Quantity | Gross acquisition | Unrounded fee | SDK cash cost including rounding |
| --- | ---: | ---: | ---: |
| 100 | 62.00 | 1.6492 | 63.65 |
| 1,000 | 620.00 | 16.492 | 636.50 |

These amounts are reusable under the same inputs. They are not historical
execution-fee observations. The current SDK's evidence and partition assumptions
remain on all final proofs.

A pointwise maximum of the applicable venue fee curves, in the declared common
valuation basis, is an optional conservative cost model. Positive profit under
that higher cost survives lower supported modeled fees. Its negative result can
hide actual-fee positives: **do not use it as an upper-profit bound to certify a
negative or prune the actual-fee optimum**. Recommended default: optimize using
each native route's actual pinned model, with valid optimistic bounds for pruning.

Any supported outcome-token fee reduces retained payout quantity. It cannot be
represented merely by adding its execution-price value to cash acquisition cost.
Keep a retained-quantity function alongside cash cost for those routes.

## 8. Joint sizing, discrete refinement and certificates

For the three overlap masks, let C_i(q_i) be cumulative cash acquisition cost.
With unit retained payouts, the continuous objective is:

```text
maximize min(q1+q2, q1+q3, q2+q3) - C1(q1) - C2(q2) - C3(q3)
```

Increasing ordered all-in marginal prices make each C_i convex; the minimum of
linear payouts is concave. Representing occupied levels as quantity variables
with constant unrounded unit costs gives a linear-program formulation. For one
valuation scenario, introduce minimum-net-margin variable m:

```text
maximize m
subject to, for every feasible outcome w:
    m <= sum_native_levels payoff(w, level)
         - sum_native_levels acquisition_cost(level)
         - holding_charge_bound
```

Add depth/cap and supported native constraints. Multiple pinned valuation
scenarios add a margin constraint for every outcome/scenario pair. Native routes
remain separate variables even when they share a payoff mask. Physical aliases
share one resource; scalar valuation never enables native cash transfers.

The relaxation guides quantities and provides valid upper bounds. Verified onset
allocations must satisfy lot grids, positive quantity in three/four distinct masks,
the native-order limit, exact rounded fees, declared budgets and joint physical
source capacity on that snapshot. Minimum commitment belongs in the exact sizing
domain, not a post-filter on its unconstrained winner. Maximize net margin among
the allocations meeting that commitment; do not assume an under-committed winner
means no qualifying allocation exists.
Those restrictions may require integer search or a mixed-integer formulation.
Precomputing rounded costs makes evaluation cheaper but does not make the exact
discrete problem continuous or automatically convex.

Use bounded exact refinement around promising candidates **and over any remaining
domain not eliminated by a valid bound**. Only checking nearby rounded versions
can find a feasible positive; it cannot certify the discrete maximum or a negative.
When using a numerical solver, proposals and floating-point statuses are not
economic proofs. Recheck feasible candidates and any claimed upper-bound
certificate with exact arithmetic at the independent verification boundary.

Preserve objective tie-breaking by lower cash commitment, fewer native orders
and deterministic native quantity keys. An attained objective bound can establish
maximum margin while leaving the tie domain unresolved; report that distinction.

Keep one shared whole-decision work budget across affected static/conditioned
screens, onset attempts, prefix expansion and refinement. Successful episodes
consume repricing work (one depth walk per frozen leg), not repeated full-sizing
work. Solver iterations,
candidate construction, curve levels, cache memory and proof size also need
explicit limits: counting one solver call as one old search node would not bound
its internal work. A verified positive at the limit may be reported as best found
with its gap; only a checked bound or exhausted domain establishes the maximum.

## 9. Onset allocations, episode intervals and audit evidence

For a verified onset allocation q, let A(q) be all-in acquisition cash value in
the pinned valuation basis and M(q) its minimum net margin after fees and declared
holding charges. Opening requires admission, exact native/source feasibility and:

```text
A(q) >= minimum_acquisition_cash_value
M(q) > 0
M(q) >= minimum_net_margin
```

The commitment threshold measures the whole purchase including cash fees, not
per-contract price or raw unused book depth. Pin its unit; under multiple valuation
scenarios use the declared robust commitment convention and retain each scenario's
cost/payout arithmetic. Do not infer robust margin by subtracting independently
minimized payout and maximized cost when they come from different scenarios.
`minimum_return_bps` is not an opening condition for this proposed policy. The
field names above describe a future versioned contract, not existing config fields.

Episode identity is the canonical mask basket plus scope, supported outcome/rule
semantics, static/conditioned scenario and trigger policy, not the selected native
quantity vector. Track supported baskets independently rather than reporting only
the globally selected winner. Their allocations are isolated, non-additive
displayed-book opportunities; opening an episode consumes no cash or depth.

The first successful sizing attempt starts the interval at its committed observer
time t1. Freeze its native routes, purchased/retained quantities, consumed levels,
fees, cost and payout proof. Never use later prices/depth, t2, eventual duration,
or a later best allocation to justify that opening. Solver wall time does not
permit consuming a future replay state while finishing the t1 calculation.
Failed or search-limited attempts without a qualifying witness do not open an
episode and cannot establish a complete negative. A later successful retry starts
at that later time, never at the earlier failed screen crossing.

After opening, **reprice the frozen allocation**, not a new sizing objective. On
each committed update to a book that any frozen leg uses, walk that leg's current
eligible depth for its frozen native route and purchased quantity, assess exact
fees for the same declared orders and recompute retained payouts and M(q). This is
a fixed-quantity depth walk with no search. The episode continues while every leg
fills its full frozen quantity from displayed depth and the repriced allocation
still satisfies `M(q) > 0` and `M(q) >= minimum_net_margin`. The minimum
acquisition commitment is an onset condition only: quantities are frozen, so a
cheaper repricing must not end a still-qualifying episode.

Close at the first committed update where the frozen allocation fails. Record the
end reason: `DEPTH_SHORTFALL` (a leg can no longer fill its frozen quantity),
`MARGIN_BELOW_THRESHOLD` (it fills but the repriced margin fails), or an
unavailable-input or changed scope/outcome/rule boundary, kept distinct from the
economic ends. Unknown inputs or unavailable fees are not proof that the
allocation stopped qualifying. An active interval at run end is censored.
Same-time transient states contribute no positive duration. Updates to books the
allocation does not use cannot end the episode, even when a different allocation
would now be better.

The interval means **time during which the exact onset allocation remained
fillable from displayed depth and net-qualified under exact fees**. It does not
assert that a larger or better allocation existed, and it does not deplete depth
between episodes: each episode is an isolated displayed-book opportunity. No
actual execution, queue position, held-lot P&L, cash availability or settlement
is inferred from episode duration.

Report t1/t2, duration, end reason/censoring, mask and native-route identities,
opening allocation and fee proofs, acquisition commitment x, minimum payout y,
net margin, pinned holding/valuation assumptions, and maximum-certified versus
best-found-with-gap status. In a single valuation basis, margin is y minus x minus
the holding bound; preserve full vectors for robust/native reporting. Report only
the onset allocation, not a later optimum or an allocation valid throughout the
interval. Separate structural exclusion, missing input, certified screening,
below-threshold attempts and work-limit uncertainty from opened episodes.

Carry screen inputs/certificates, prefix/tail assumptions, work counters,
failed-attempt reasons and the repriced allocation at each interval end
sufficiently to audit interval boundaries and onset proofs.
Reuse bounded checkpoint/delta transport rather than repeating entire curves.
Version policy/output changes explicitly and update the independent reader and
fixtures; do not reinterpret existing V1/V2 fields silently. The reader must
rederive mask geometry, native fees/payoffs, within-allocation source feasibility,
the frozen-allocation repricing at each interval end and carried bounds without
importing the writer's optimizer.
Unchecked search coverage remains a writer attestation, not proof of an omitted
opportunity or an unrestricted optimum.

## 10. Implementation sequence and decisions still to pin

Suggested sequence:

1. Version the mask/native-order policy, trigger/episode semantics and cash/margin gates.
2. Compile mask baskets, native alternatives and dependency indices.
3. Prototype a certified screen and failed-attempt invalidation before a full rewrite.
4. Add bounded curve prefixes, fee primitive caching and exact partial-level queries.
5. Add onset-only relaxation, tail expansion and bounded discrete refinement.
6. Integrate frozen-allocation episodes and independent onset/interval verification.

Still to pin: numerical minimum commitment/margin and valuation conventions;
native-order cap when venue splitting increases order count; the trigger backend,
failed-attempt retry/certificate policy, solver and exact certificate format;
candidate/work/curve/memory/output limits; whether a separately labelled
conservative-fee scenario is wanted; and a dominance rule for four-mask baskets
that contain a qualifying three-mask basket (a positive triple plus a small
purchase of any fourth mask can still qualify, so one opportunity can otherwise
open several overlapping four-mask episodes). No extra family exclusions, return-percentage
gate or arbitrary percentage defining "near the trigger" have been adopted.

Later verification should cover duplicate-mask routing, the overlap despite pair
covers, unequal sizes, rounded fees, minimum-commitment-constrained sizing, partial
levels, cheaper alternate venues, tail-bound invalidation, game/scope changes,
failed attempts followed by depth-only success, no backdating/lookahead, no sizing
while an episode is open, depth-shortfall and margin ends of a frozen allocation
while the price screen stays positive, updates to unused books not ending an
episode, unavailable/censored ends and certificate corruption.
Measure screening false positives and retry-heavy cases as well as negatives and
successful onsets. No speedup or corpus completion estimate is established here.

Baseline source references:

- [Observer-time runtime](https://github.com/hridyansh07/prediction-indexer/blob/f77a5cf1fe7f339e4a0202d6b3d6568c417416e9/replay/strategies/bounded_payout_optimizer/strategy.py)
- [Native models, pricing and search coordinator](https://github.com/hridyansh07/prediction-indexer/blob/f77a5cf1fe7f339e4a0202d6b3d6568c417416e9/replay/strategies/bounded_payout_optimizer/scenario.py)
- [Current discrete search](https://github.com/hridyansh07/prediction-indexer/blob/f77a5cf1fe7f339e4a0202d6b3d6568c417416e9/replay/strategies/bounded_payout_optimizer/core.py)
- [Independent reader](https://github.com/hridyansh07/prediction-indexer/blob/f77a5cf1fe7f339e4a0202d6b3d6568c417416e9/replay/strategies/bounded_payout_optimizer/output.py)
- [Fee SDK](../../replay/fees/README.md)

This revision changes documentation only. Its examples can be checked offline;
no implementation, corpus benchmark, live probe or deployment accompanies it.

## 11. Advisory: geometry-aware unequal-size triggers

**Recommendation, not an adopted backend.** Prefer a complete fractional
payoff-cover screen over spread heuristics or a handpicked ratio list. The oracle
consultation and arithmetic checks are recorded in the discussion thread above.

### 11.1 Compile payout patterns and cover ratios

For k binary masks, each outcome has a k-bit payoff-incidence pattern. Deduplicate
equal patterns: at most 8 rows for k=3 or 16 for k=4, even if the exhaustive space
contains many more outcomes. This compresses payout constraints, not native routes
or source capacities. Each valuation scenario still needs its own arithmetic.

For unit retained payouts and nonnegative flat acquisition prices p, let A be the
incidence matrix. Any profitable continuous nonnegative ratio exists exactly when:

```text
mu(p) = min p^T r  subject to r >= 0, A r >= 1
mu(p) < 1
```

Normalize a profitable portfolio by its minimum payout to obtain such a cover;
conversely a cover costing less than one has positive minimum gross profit.
Normalization is only an existence screen, not a change to the sizing objective.
Precompute the vertices V of this price-independent covering polyhedron, then
test `min_{v in V} p^T v < 1` on affected price updates. Complete exact enumeration
is essential; selected equal-size ratios cannot establish a negative. Zero-mask
components in a relaxed vertex must not be discarded merely because actual onset
allocations require three/four positive grid-sized masks.

Synthetic four-mask example, with rows `(1100, 1010, 0110, 0001)` and prices
`(.60, .60, .60, .05)`:

| Quantity vector | Acquisition cost | Minimum payout | Gross margin |
| --- | ---: | ---: | ---: |
| (1, 1, 1, 1) | 1.85 | 1.00 | -0.85 |
| (1, 1, 1, 2) | 1.90 | 2.00 | 0.10 |

The normalized cover `(.5, .5, .5, 1)` costs .95. Equal-size testing misses the
unequal-size positive; full cover-vertex testing does not. Spread handling alone
does not supply this outcome geometry.

### 11.2 Add native-route caps and safe fee bounds

For route j, use supported bounds `C_j(q) >= c_j*q` on cash cost and
`h_j(q) <= a_j*q` on retained quantity over its whole admitted domain. Zero cash
fees and gross retained quantity are safe optimistic choices only under supported
nonnegative-charge/no-rebate assumptions. At a single valuation, fixed retention
permits mask-level optimistic cost per retained unit `c_j/a_j`; taking the cheapest
eligible route while ignoring its capacity is a screen, not a feasible allocation.

A route-level LP can tighten absolute-profit screening:

```text
maximize m
subject to 0 <= q_j <= u_j
           m <= sum_j (P_wj - c_j) * q_j  for each payout pattern w
```

P uses optimistic retained payouts and the declared valuation. Relax unsupported
constraints rather than removing real feasible allocations. Price total capped
depth at its best ask, not only best-level quantity: that is optimistic even when
actual orders must consume deeper levels. Native cash, shared-source and positive
mask-count constraints can be relaxed for a screen but must hold in exact sizing.
Keep all routes; cheapest-route-only capacity can miss profitable split acquisition.

A checked upper bound below the absolute-margin threshold excludes qualification;
a nonpositive bound excludes strictly positive profit. Passing does not establish
minimum commitment. Do not impose `C_lower(q) >= minimum_commitment` as a relaxation
of the actual commitment floor: it can exclude qualifying actual costs. Apply that
floor in exact sizing, or derive a correctly directed necessary screening condition.
Unknown fees remain unknown; conservative fee upper bounds cannot prove an
actual-fee negative. A one-lot rounded-fee loss cannot prune larger sizes.

### 11.3 Reuse cached dual certificates before solving again

For the boxed route relaxation, any outcome weights y with `y >= 0` and
`sum(y) = 1` give the upper bound:

```text
U(y) = sum_j u_j * max(0, sum_w y_w * P_wj - c_j)
```

Minimum payout is no greater than weighted-average payout. These weights are
certificates, not estimated outcome probabilities. Recheck a cached rational y
against current coefficients/caps; it remains a valid bound even if no longer
optimal. Solve only when it is inconclusive. The baseline `gross_dual_bound` uses
uniform outcome weights; a route LP can choose tighter weights. Its native-order
limit cannot be silently reused as the new distinct-mask limit.

### 11.4 Retry failed onsets without resizing successful episodes

Recommend retrying on relevant changed inputs while screen-positive and unopened;
skip identical states and reuse any certificate that still covers the update.
Price false-to-true transitions alone can miss depth-only or rounded-fee success.

For three partition masks at unchanged .33 asks and synthetic per-order fee
`.01 * ceil(q/10)`, depth one costs 1.02 against payout 1; depth ten costs 9.93
against payout 10, earning .07. The gross price screen is true throughout. This is
a synthetic counterexample, not a venue fee schedule.

Retain the failed-attempt fingerprint. Retry after relevant depth, alternative
route, fee, usability, quantity or released-knowledge changes unless a valid bound
excludes qualification on the changed state. Limited failure is unknown, not a
complete negative. A success freezes the allocation and stops sizing for that
episode; later success after earlier failure must not be backdated. Repeated
screen-positive failures are the main potential source of high sizing frequency.

### 11.5 Candidate implementations and fallback

- [cddlib](https://github.com/cddlib/cddlib) / [pycddlib rational interface](https://pycddlib.readthedocs.io/en/latest/cddgmp.html): double-description conversion from covering inequalities to exact vertices/rays.
- [lrslib](https://cgm.cs.mcgill.ca/~avis/C/lrs.html): exact reverse-search vertex/ray enumeration, also useful for an independent geometry cross-check.
- [HiGHS Python interface](https://ergo-code.github.io/HiGHS/dev/interfaces/python/example-py/) and [solvers](https://ergo-code.github.io/HiGHS/dev/solvers/): incremental coefficient/bound changes and simplex basis hot starts; a persistent route-level LP is the fallback when mask-price reduction is too loose.
- [Boyd and Vandenberghe, Convex Optimization](https://web.stanford.edu/~boyd/cvxbook/): LP duality and independently checkable bounds.

These are established algorithms/interfaces, not measured improvements here.
Recommend compiled cover tests, cached dual bounds and a capped LP before full
curve sizing. Use exact native verification and bounded integer refinement for
lots/rounded fees; continuous or floating-point solver status is not an economic
certificate. Backend/version choice, deterministic traversal, certificate format
and work limits require a further implementation decision.

## 12. Advisory: corpus performance and prototype gate

The expected saving comes from replacing repeated full sizing with cheap affected-
basket screens, and sizing only prospective onsets or failed-attempt retries.
Three/four masks do not alone bound basket count, native routes, levels or retry
frequency. Compatibility, coverage and price filters reduce work; a raw subset
count is a ceiling, not a forecast of actual solver calls.

Let U be relevant committed updates, B_t affected baskets at update t, E successful
onset attempts and R unsuccessful/unresolved retries. The intended cost shape is:

```text
old: replay + repeated quantity searches + output/independent audit
new: replay + geometry compilation + sum_t(screen work for B_t)
     + (E + R) onset sizing work + output/independent audit
```

If screens remain positive while depth/fees block qualification, R can approach
the update count. This is the key failure mode, not eliminated by warm starts.
Compile equivalent geometry once, maintain book prefixes, dispatch only affected
baskets and reuse valid exclusion bounds before launching another sizing attempt.

The baseline's [recorded synthetic measurements](https://github.com/hridyansh07/prediction-indexer/blob/f77a5cf1fe7f339e4a0202d6b3d6568c417416e9/replay/strategies/bounded_payout_optimizer/PERFORMANCE.md)
are evidence of existing costs, not a benchmark of this proposal:

| Pattern | Writer plus independent audit | Evidence |
| --- | ---: | --- |
| Certified negative, 100,000 updates | About 208 seconds | Measured synthetic run |
| Two-mask fee-positive pattern at 100,000 updates | About 44.7 minutes | Linear projection from 1,000 updates; two masks are outside the new scope |
| Hard overlap pattern at 100,000 updates | About 3.4 hours | Linear projection from 100 updates |

At 1,200 bundles, ten minutes average means 200 serial hours. One minute means
20 hours; 30 seconds means ten hours. Ideal eight-worker times are 25, 2.5 and
1.25 hours respectively, before storage, memory and verification contention.

**Provisional recommendation, not an approved SLO or speed claim:** aim for
30–60 seconds average end-to-end per bundle, then measure tail/runtime distribution
and corpus throughput on the intended hardware. Ten minutes per bundle is not
the accepted design expectation. Do not promise a ten-minute bundle becomes a
thirty-second bundle without evidence, or silently discard hard bundles to meet
the target.

Prototype the screen/onset path before a full rewrite. Compare representative
negative, positive and retry-heavy retained/synthetic inputs on the same stack;
include three-/four-mask portfolios rather than only pair opportunities. Measure
geometry compilation, updates, affected baskets, screen passes, successful/failed
sizings, solver work, output bytes, peak memory and independent audit separately.
Report end-to-end mean, median, tail and worst-case time, with cold/warm input
processing distinguished. Compare equivalent scope and certified/best-found
results so speed is not purchased by hiding opportunities or weakening proof.

Bound construction, screening, solver, refinement and reader work explicitly.
Preserve work-limit statuses and gaps; a fast feasible witness is not proof of a
maximum, and a timed-out absent witness is not a negative. No new solver runtime,
speedup or 1,200-bundle completion time has been measured for this proposal.
