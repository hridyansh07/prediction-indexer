# Inventory substitution and dominance improvements V1

Status: **PROPOSED — lower priority; requires explicit starting inventory and
capital. Not implemented or empirically validated.**

The [shared economic scenario contract](ECONOMIC_POSITION_SCENARIOS_V1.md) owns
fees, visibility, native accounts, capacity, settlement and output distinctions.
This strategy improves an existing portfolio relative to holding it. It does
not manufacture inventory or count sale proceeds as newly earned profit.

## 1. Initial portfolio and comparison accounts

Require a pinned starting inventory ledger: native book/token identities,
quantity atoms, lots and acquisition costs, acquisition times, native collateral
assets, already paid fees, initial cash and evidence/scenario provenance. Unknown
historical cost is allowed only for an explicitly labelled incremental-only
comparison; total portfolio P&L then remains unknown. No assumed zero basis.
The first implementation accepts only long holdings with supported masks/payouts.
In incremental-only mode, capital limits apply to known new purchase commitments
and prefunding; report the starting portfolio cost/risk budget as unavailable.
A policy requiring a known total outstanding-cost limit cannot enter in this mode.

Maintain three separate accounts from the same initial state:

1. `HOLD`: take no substitution; retain starting claims until supported settlement.
2. `LIQUIDATE`: at the configured `comparison_start_ns` (default run start),
   latch sale of starting holdings even if bids are unavailable, then follow the shared partial-exit
   rule until sold/resolved/censored.
3. `SUBSTITUTE`: make the conditional improvements specified below, then hold
   the resulting claims until resolution or a further permitted improvement.

Baselines have independent scenario capacity and cash. Their results are
alternatives and cannot be summed. At each substitution also show the local
immediate-liquidation alternative for the replaced lot at that decision.

## 2. Candidate construction and payoff dominance

Build candidates from actual unreserved owned lots and captured supported claims
in the same aligned exhaustive event/scope. Candidate transaction sells x from
the old holding and buys one or more replacement claims. The default is one
replacement claim; optional bounded replacement baskets use the
[optimizer's payoff-vector reasoning](BOUNDED_PAYOUT_OPTIMIZER_V1.md), with at
most four BUY legs. No duplicated route enumeration by relationship names.

For the sold part, let P_old(w) be its native payoff vector. After exact SELL
and BUY fees, let P_new(w) be the replacement retained payoff vector and R the
net native cash change from the transaction, including conversion charges if any.
Unchanged portfolio holdings cancel in the incremental comparison.

Require terminal exposure to be weakly improved:

```
value(P_new(w) - P_old(w)) >= 0 for every w in Omega_t
```

Evaluate this inequality for every pinned valuation stress scenario if used.
The default single-asset mode proves it natively; cross-asset parity mode proves
only scenario-valued dominance and preserves outcome-dependent native exposure.
Do not offset a loss of terminal claim exposure with cash and describe it as
preserved exposure; that broader risk-changing trade is outside this V1.

Two entry classes are permitted:

- `CASH_RELEASE`: value(R)>0, value(R) at least `minimum_cash_release`, and
  weak terminal dominance above. This gives a positive incremental floor relative
  to HOLD under the assumptions, before any later differential holding charges.
- `STRICT_DOMINANCE`: value(R)>=0, weak terminal dominance, and at least one
  supported outcome has a payoff increase of at least `minimum_state_improvement`.
  This may have zero minimum incremental profit. Report it as a free exposure
  improvement, not a cash-profit opportunity. Default enabled alongside cash release.

Include configured differential holding/settlement charges in the comparison.
An unknown required differential charge makes the candidate unknown. Positive
cash release with a negative total incremental bound cannot pass the cash-release
gate. Under default zero-financing charges, the expression is
`incremental_floor = value(R) + min_w value(P_new-P_old)`.
All entry inequalities must hold in every configured valuation scenario. For
strict-dominance-only admission, nonnegative cash after the declared differential
charge bound is also required; paying a hidden carry cost is not a free upgrade.

Equal masks support substitution. A sold narrow claim X with X subset Y supports
an upgrade into Y only if retained replacement quantities cover the sold payoff
in every state. Nominal purchased quantities before token fees do not prove it.
Milestones may create new equality/dominance relations, but a phase transition
without a winner does not narrow the proof domain. Unknown game state permits
only the static proof; ambiguous rule/source alignment rejects the candidate.

## 3. Sizing, transaction and triggers

Re-evaluate on relevant old-claim bid changes, replacement ask changes, released
result/score changes, scope/validity changes and delayed-decision timers. Keep
results and complete prefix quality through the shared released-fact contract.
No rich game telemetry or model of expected returns is needed.

Search old-lot sale quantities on the declared grid, capped by owned inventory
and eligible bid depth. Search replacement quantities under exact BUY fees,
ask depth, lot increments and remaining budgets. Initially use
`max_sale_contracts=100`, `quantity_increment=1` when representable, four BUY
legs maximum and 100,000 deterministic search evaluations; all are configurable.
Search-limit results carry completeness as in the optimizer; a feasible repriced
improvement is usable, an incomplete search with none is not a proven negative.

Default thresholds are .01 valuation units for `minimum_cash_release` and .01
per compared portfolio for `minimum_state_improvement`. Cash-release candidates
rank first by incremental minimum wealth improvement, then cash released, lower
buy cost and lexicographic transaction identity. Strict-dominance-only candidates
rank by lower buy cost then identity; no invented probability-weighted upside.
Choose one action at each decision and reprice remaining candidates after it.

Every BUY leg must be prefunded in its own venue/asset **before** relying on the
transaction's SELL proceeds. Default mode is
`PREFUNDED_SIMULTANEOUS_REPLACEMENT_SCENARIO`: all sales, purchases, fees,
dominance, capacities and balances validate before anything posts. If any leg
fails, leave the original portfolio untouched. There is no partial hedge sale
waiting for a future replacement and no cross-venue credit transfer.

On acceptance, consume all shared bid/ask capacities, debit sold inventory,
record its FIFO sale P&L, debit purchase cash and create new lots with their full
fee-inclusive cost and retained quantities. Post sale proceeds to the correct
venue/asset. New lots retain lineage to replaced lots, but do not inherit a
fictitious zero or reduced acquisition cost. Net cash release and realized
hypothetical sale P&L are different calculations.

At positive delay, freeze old lot IDs, sell caps and replacement support. Reprice
at the due time with quantities no larger than intended; reprove dominance and
prefunding. A now-inaccessible original lot, unavailable price or lost proof
cancels the entire replacement. The held portfolio continues unchanged.

## 4. Holding, further replacements and resolution

The default `max_actions_per_event=1` holds replacements to supported resolution.
There is no automatic bid-based exit when the original substitution stops being
attractive. Prices reversing do not undo a completed hypothetical transaction.
All residual original and new claims remain in total portfolio accounting.

When more actions are configured, require a new relevant book/result revision,
`cooldown_ns` (default 1 second) since the last action, and a fresh independently
proved improvement on the **current** owned portfolio. No action repeats on an
identical evidence fingerprint. Cash/depth debits and lot basis persist. A
semantic scope relabel does not create fresh starting inventory or reset limits.
Resolve overlapping candidate sales by deterministic priority and reserve lots
within the decision so the same owned quantity cannot be sold twice.

Unsupported future rules or missing books leave holdings present and unpriced;
use supported settlement/scenario credits when available. Optional liquidation
at a configured observer-time deadline uses the shared latched partial-exit
rules. Default has no such deadline. At run end report residuals and censoring.

## 5. Explicitly evidenced native conversions

Conversions are disabled by default. A route may enable a pinned conversion
only with evidence identifying the native mechanism, exact input/output assets
and quantities, eligibility, costs/fees, collateral requirements and availability
timing. Equality of payout masks or membership in a complete economic cover
does not prove native convertibility. No API/on-chain call occurs during replay.

Supported initial shapes are an evidenced complete-token-set merge, and a
collateral split followed by sales of **all** minted claims. Treat each as one
fully validated scenario transaction. Require known charges in every asset;
missing gas/protocol charges are unknown unless an explicit reviewed cost
scenario is supplied. A pinned delayed conversion needs a pending state that
reserves inputs and releases outputs only at its declared due time; V1 initially
admits only immediate conversion scenarios with that assumption labelled.

For merge, debit the exact owned tokens and credit the evidenced collateral less
charges. Compare with holding and selling those tokens. For split-and-sell,
prefund collateral and all charges, mint the evidenced quantities, and require
eligible bid depth for the entire retained output set before posting anything.
If one output cannot be sold, no transaction opens in this V1. A successful
split-and-sell is immediately closed with net native cash profit/loss; do not
also count it as a separate short strategy or recycle unchanged bid depth.
Partial conversions, unevidenced redemption and naked shorting are unsupported.

## 6. Hand calculations (synthetic)

**Same exposure, released cash.** Own 100 A-win contracts with historical cost
40. Sell them at .62 with .50 cash fee: proceeds 61.50, sale P&L 21.50. Buy 100
equivalent contracts at .58 with .50 cash fee: new cost 58.50, cash released 3.
Terminal exposure is unchanged. Total portfolio P&L if A wins is
`21.50 + (100-58.50) = 63`, versus HOLD's 60; if A loses it is
`21.50 - 58.50 = -37`, versus HOLD's -40. The improvement is 3 in either state;
the sale P&L 21.50 is not the incremental strategy gain. Immediate liquidation
would leave 61.50 cash and no exposure, so its eventual outcome differs.

**Token-fee rejection.** The same purchase retains only 97 replacement contracts
after a token fee while 100 original contracts would be sold. On A-win outcomes,
replacement payout falls by 3: the substitution fails exposure preservation even
if its cash release is positive. Search a larger affordable purchase and reprice
all fees, or keep the original holdings.

**Dominance upgrade.** Own 100 A-sweep contracts; sell at .45, buy 100 A-series
contracts at .42, with total cash fees 2. Cash release is 1. Since every sweep
is a series win, payout is never smaller and increases by 100 when A wins 2-1.
Under compatible rules this qualifies without assigning probability to 2-1.

**Native route only with evidence.** An evidenced split turns 100 collateral
into 100 YES and 100 NO. Their bids are .54 and .49 with sufficient independent
eligible depth: proceeds 103. Known aggregate charges 2 leave profit 1. Missing
conversion evidence or only 30 units of NO bid depth rejects the 100-unit
transaction. It does not leave an invented 70-unit short or unsold-free asset.

## 7. Results, limitations and implementation gaps

Report initial provenance/basis, every replacement's old/new payoff vectors,
native cash released, strict-dominance states, incremental floor versus HOLD,
local liquidation alternative, total scenario account P&L, capital/prefunding,
capacity conflicts, search completeness and residual holdings. A cash-release
positive proves a conditional incremental improvement, not that the original
portfolio was profitable. A strict-dominance-only finding is a separate result.

Current SDK detections/fee bridges supply reusable native arithmetic but no
starting-inventory account, replacement lifecycle or conversion evidence model.
These and the shared state/reader gaps require future implementation. Real or
explicitly hypothetical starting holdings, cost basis and prefunding are genuine
prerequisites. Conversion routes additionally need supported native evidence;
they must not block ordinary substitution. Richer telemetry is unnecessary.
