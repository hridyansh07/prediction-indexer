# Implemented optimizer V1 contract

This package implements the approved [optimizer](../../../docs/specs/BOUNDED_PAYOUT_OPTIMIZER_V1.md)
under the [economic position scenario contract](../../../docs/specs/ECONOMIC_POSITION_SCENARIOS_V1.md).
Existing strategy policies and output formats are unchanged. These are synthetic
offline scenarios, with no empirical execution or profitability claim.

## Closed configuration and pins

`version: 1` requires exactly `source_revision`, `snapshot_directory`,
`snapshot_sha256`, `fees`, `policy`, `valuation`, `account` and `rules`.
`source_revision` is a lowercase Git SHA-1 (40 hex) or content SHA-256 (64 hex),
pinned by the caller and included in experiment/output identity. It records the
reviewed code/model revision; it is not a runtime executable-byte attestation.
Prepared membership/masks and their historical limitations are preserved.
Fee configuration is the existing closed FeeBridge format, assessed as native
per-order BUY/SELL fills from its pinned current-snapshot catalog. Unknown fees
never receive a zero fallback. Known charges, assessment identities, assumptions
and bounded unknown-fee samples are retained.

`policy.version: 1` fields are shown in the example. Each sorted unique native
book needs a positive decimal `increment` and `cap`, representable at its native
quantity scale. Caps need not be exact grid multiples; the last admitted grid
point is their floor. Maxima are 8 legs, 128 configured books, 1,024 outcomes,
10,000,000 complete-vector evaluations and 16 alternatives. Recommended search
budget is 100,000. Quantity and rule bindings define the declared domain; an
unconfigured/unsupported sibling does not veto supported candidates. Missing
required declared book inputs remain visible and cannot prove a complete negative.

`rules` pins the event, compatibility/evidence SHA-256, sorted assumptions,
`normal_resolution_only: true` and each book's reviewed rule identity. This is an
explicit caller assertion of compatible native unit payouts, including excluded
void/unplayed-map branches; prices or equal claim IDs do not prove those rules.
Only preparation-admitted masks are used. No uncaptured token is synthesized.

`account` requires sorted native `(venue, asset)` cash plus positive transaction,
event and outstanding-cost budgets and a maximum number of open positions.
Cash is never transferred, borrowed or made negative. `valuation: null` permits
one native collateral identity. Otherwise pin a named finite list of exact
positive asset weights under `PARITY_SCENARIO` (all weights 1) or
`STRESS_SCENARIO`. Unknown weights prevent a scalar verdict. The objective takes
the minimum over both outcomes and valuation scenarios; budget commitment uses
the maximum summed scenario cost. Native flows remain separate.

`holding_cost` has an exact uniform `rate_per_ns` applied to each outstanding
native cost and valuation weight. Zero is labelled `ZERO_FINANCING_COST_SCENARIO`.
Nonzero entry requires `maximum_duration_ns`; absent duration yields
`HOLDING_COST_UNKNOWN`. Actual charges accrue only until each lot's credit and
are analytical adjustments, never cash flows. At the first observer time beyond
the assumed duration, the conditional guarantee is marked invalid.

`settlement` is null (no cash-availability claim),
`NORMAL_RESOLUTION_SCENARIO` with a nonnegative `delay_ns`, or
`PINNED_SETTLEMENT` with sorted book/unit-payout/evidence/availability rows.
Normal settlement uses the first released determination time, credits at
`max(open_time, determination + delay)`, and labels zero delay as assumed
immediate normal availability. Pinned rows use the same no-pre-acquisition-credit
rule. Each lot credits separately; books need not remain available.

Optional `game` uses the existing closed game policy and byte pin. Recommended
`at_segment_end` and explicit release delays apply. Every released result is
retained, including multiple same-time/prologue releases; opaque details and
unreleased tails are ignored. `known_score` names `participant_0` and
`participant_1`, not source home/away: reversed source-side alignment is explicit.
The contiguous winner prefix and completeness are separate. Unknown winners do
not shrink an exhaustive series space. A complete supported final score can
constrain counts without inventing order. Contradiction stops affected entries
and preserves unresolved holdings. Optional unavailable game runs static only;
required unavailable game opens no output.

## Search, signal and held positions

Each committed timestamp applies scope, all due released facts and the complete
same-time book state before one decision. Quiet release, delay, rearm, credit and
holding-assumption timers use detached observable books. Non-Kalshi bid-only
changes affect terminal/current bid marks but do not trigger acquisition search;
Kalshi opposite bids are the native ask source. Identical inputs do not resignal.

The deterministic search includes zero, full-cap total-mask/partition/two-leg
cover seeds, then every remaining support/grid vector until exhausted or limited.
No one-contract fee or concavity pruning is used. Objective ties prefer lower
robust cash commitment, fewer legs, then sorted native keys/quantity atoms.
`SEARCH_COMPLETE` means that declared, currently depth-bounded grid was exhausted;
`SEARCH_LIMITED` reports visited evaluations, repriced feasible best/alternatives
and a conservative gross-payout upper bound. A limited absent solution or missing
fees/depth/valuation is unknown, not an economic negative. A feasible positive
below entry thresholds has its own status and duration, never a nonpositive label. The bounded
`capital_frontier` contains feasible nondominated cost/margin samples labelled
`BOUNDED_FEASIBLE_SAMPLES`; it does not claim a complete continuous frontier.

Detection uses full undepleted displayed sources independently of account cash.
A qualifying margin is strictly positive and meets margin/return thresholds.
A crossing can signal or skip for capital without changing its detection history.
Before entry the optimizer re-solves against actual cash and cumulative capacity.
All legs post together only after all gates pass, under
`SIMULTANEOUS_DISPLAYED_SCENARIO`, `TRADABLE_IF_USABLE_SCENARIO` and unknown trading
status. This is not evidence of atomic fills or venue availability.

Delayed entry freezes shape, support, rule identities and maximum quantities;
the due search can only shrink on that support. Admission loss or rule identity
change cancels visibly. Pending attempts reserve no capital or depth. Positions
keep fixed acquired lots and original proofs until supported resolution. Signal
loss, unavailable books and scope changes do not sell them.

One ledger per static/conditioned scenario retains cumulative source-level and
source-total consumption across the whole event (`CUMULATIVE_DISPLAY_CAP`).
Kalshi projection debits its physical opposite bid. Joint alias uses are checked
before posting; repricing or epochs never reset capacity. Re-entry requires the
previous position to close, a complete known-nonpositive rearm duration and a
later qualifying crossing, with event caps, remaining native cash and old debits.
Unknown periods and capital/depth skips cannot fabricate rearm.

At run end residual positions are `CENSORED`. Native closed-lot cash P&L, remaining
basis/capital, outcome payout vectors, full closed-position P&L, analytical charge
and adjusted P&L remain distinct. Unposted bid-side liquidation marks require a
full supported owned-quantity grid and exact SELL fees; any unpriced residual is
explicit. No marks are credits and no discretionary sales are implemented.

## Output and independent verification

Version-1 closed output consists of `decisions.ndjson`, `episodes.ndjson`,
`summary.json`, `manifest.json`, then receipt-last `content_receipt.json`.
NDJSON is canonical exact JSON with LF. Each file is capped at 512 MiB/1,000,000
records; each line and JSON artifact at 8 MiB. Released history caps at 4,096
result/final facts. Detached views/accounts/active episodes are bounded to
128 MiB; a search additionally caps its memoized exact vectors/prices at 128 MiB.
Excess fails visibly rather than truncating economic evidence.

Every decision carries released knowledge, observable native books, fees, source
levels, search completeness, account before/actions/after and signal/pending
lineage. Episodes are maximal positive-length intervals of the selected vector's
scope/shape/outcomes/quantities/retained-payout/rule semantics; same-time transient
states create no duration. Terminal positive episodes are censored. Summary
qualifying-positive/below-entry-threshold/complete-nonpositive/unknown durations partition event time per account,
without summing route overlap. Attempts, capacity, native ledgers and residuals
are reported separately from detection.

The bounded streaming reader independently loads released facts, reconstructs
signal/rearm/delay state and native accounts/capacity, reprices every carried
proof through the pinned fee engine, verifies held-lot settlement/P&L/marks,
re-solves the grid and verifies exact episodes and required quiet deadlines. It never imports writer Runtime
or Account. Pure fee/search/payoff arithmetic is shared; small-domain reference
tests verify exhaustive search against a separate Cartesian implementation.
Completed reading additionally requires supervisor SUCCESS and factory/config,
run identity, receipt terminal, snapshot/game/source/fee/output bindings.

Carried detached books are writer-attested and tied to the supervisor's pinned
run; this reader does not independently replay every tape cut to authenticate
those books or prove omitted book changes. The output states that limitation.
The source pin and reviewed rule/fee/valuation/settlement assumptions likewise do
not prove actual execution, FX convertibility or historical venue rules.
