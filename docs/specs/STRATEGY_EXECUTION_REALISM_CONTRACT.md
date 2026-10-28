# Strategy execution realism: producer contract

Status: **implemented**. This is the producer contract for
[strategy execution realism](STRATEGY_EXECUTION_REALISM_V1.md): the four policies,
independent reading and reference examples. Retained-data bench acceptance and
the full corpus rerun remain in [open work](../OPEN_WORK.md); implementation
does not establish a live pilot, deployment or database migration.

## Closed policy versions

| Strategy | Legacy fill policy | Execution realism policy |
|---|---:|---:|
| cross_venue_arbitrage | 3 | 4 |
| same_venue_multi_market | 1 | 2 |
| same_venue_implication_cover | 3 | 4 |
| cross_venue_implication_cover | 3 | 4 |

The new versions require the existing governing `edge`, a recording
`{"name":"t10","role":"records","target_contracts":"10"}`, probes naming
both, and a closed flags policy. Existing `step_contracts`, fee assessment,
minimum margin and trigger semantics are unchanged. The target is ten exact
contracts, not ten native atoms or ten steps. With `step_contracts: "0.01"`, it
is 1,000 steps. A short recording target never prevents the governing edge from
opening; c=0 probes take its actual opening quantity. At later confirmations a
short target is `decision_untradeable` and still carries its decision evidence.

Policy flags contain exactly `version`, `stale_ms`, `settlement_allowlist`.
Version is integer 1; staleness is a nonnegative integer of whole milliseconds
that fits unsigned-64-bit nanoseconds. The allowlist is a sorted unique list of
at most 64 strings, each 1–256 characters; an empty list requests review of all
pairings. Booleans and floats do not stand in for integers. Old versions reject
flags, retain their original closed output schemas and remain runnable/readable.
`same_venue_complement` is unchanged.

The four `config.example.json` files carry the exact reference policy: governing
edge plus recording t10; confirmations 0/500/1,000 ms; arrivals
100/250/500/1,000/2,000 ms; simultaneous and slow-first; defaults of 0 ms for
Kalshi/Limitless and 1,000 ms for Polymarket. This is exactly 60 probes per fill
episode. The flags reference is 5,000 ms and `["series_moneyline"]`. Other finite
SDK probe grids and explicitly configured allowlists/staleness remain valid
experiments; all configuration values enter policy and experiment identities.
All three venue defaults must be present in a new policy.

The 1 s unknown-Polymarket default is the owner's 17 October 2026 decision,
based on the Universe v8 audit: all 232,495 Polymarket execution-term rows carry
1000 ms, and every market in all 1,212 retired bundles has observed terms. The
old 3 s maximum rationale was unsupported. Known observations, including zero,
override the default; `delay_source` remains `terms` or `default`.

## Pinned decisions and exits

The four strategies explicitly map acquired `entity.descriptor.legs[].market_id`
to `outcomes.document.markets[].market_id` and select the last execution-term
change point at or before decision time. Native token IDs, subscription slugs,
and the opposite Kalshi source orientation cannot substitute for market identity.
Unavailable terms, version-1 documents, absent entries and null change values
return unknown; only the SDK applies the configured default. The lookup is pure,
and the independent reader performs it at decision time too, including when
slow-first sends other legs later.

The [R12 amendment](PROBE_EXIT_SOURCE_V1.md) makes `fill_exits` resolve BUY exits
to native `bid` on the acquired descriptor book. Kalshi entry books are the
opposite orientation; acquired YES exits into YES bids, acquired NO into NO bids.
New policies retain Kalshi native `bid` and `kalshi_complement_ask`, reading no
native asks and retaining no `kalshi_complement_bid`. Polymarket/Limitless keep
native `ask`/`bid`. Missing exit plans/builders fail with `probe exit book not
planned`; missing retention fails with `probe exit ladder not retained`. A
Kalshi sold leg fails with `probe exit unsupported for Kalshi sold leg`.
Non-Kalshi sold legs keep their same-book reverse mapping. The SDK default
preserves the legacy entry-source reverse mapping for other probe users.

Exit ladders use the exit book's committed usable view at arrival, never a
later book or a borrowed source ladder. Empty/unusable exits yield no proceeds.
Walks stay best-first and unpriced, retaining and walking at most `max_levels`
exit levels. Excess beyond that cap is a shortfall, never an attempt failure;
entry walks still fail if they need more than `max_levels` levels within their
frozen limit. Unexited BUY excess keeps its acquisition cost. Uncovered sold
excess is charged at unit payout (`10**price_scale`) per atom in both writer
and independent reader, without changing actual `exit_consumed`/`exit_proceeds`.
No cut/retention boundary or row field changes. The reader checks exit prices
against the exit book's price scale.
Existing FeeBridge,
`assess_net` memoization and the scale-36 `fill_loss` conversions from PR #78 are
reused. Native costs/proceeds keep their native scales. Aggregate value and gross
loss share scale-36 research-dollar/common quote units. No unwind fee is added.

Kalshi basket inputs declare `("crossed", None)` as well as the projected
acquisition ask and native bid. R10's declaration stays defensively, but with no
native asks read the Kalshi crossed flag cannot fire on normalizer-shaped
books. The old ask-only recovery fixtures remain explicitly synthetic and opt
into ask reads only in those tests. Non-Kalshi self-cross exclusion is unchanged.

## Episode and probe output contract

Only fill episodes of the new policies add the top-level closed object:

```json
"flags": {"pairing_class":"map_winner","settlement_review":true,"stale_legs":[1]}
```

Pairing comes from every descriptor leg's pinned market type: one shared type,
or `mixed:` followed by sorted distinct types joined with `+`, or `unknown` if
any leg lacks an outcomes entry. Review is exactly nonmembership in the policy
allowlist. Stale indexes are in ascending leg order, where opening `ages.best_ns`
strictly exceeds `stale_ms * 1000000`; equality is fresh. A null age does not
assert known staleness. The independent reader re-derives the pairing and review
from the snapshot and staleness from independently schema-checked fill ages. These
flags record evidence and never filter episodes. Gross quote-slice episodes keep
their existing schema.

Only new policies add `value` and `legs` to each non-null probe `decision`:

```json
"decision": {
  "time_ns":"16500000000", "steps":"1000", "limits":["40","400"],
  "ages":[{"book_ns":"0","best_ns":"0"},{"book_ns":"0","best_ns":"0"}],
  "value":"2000000000000000000000000000000000000",
  "legs":[
    {"atoms":"1000","cost":"40000","taken":[["40","1000"]],"consumed":[["40","2000"]]},
    {"atoms":"10000000","cost":"4000000000","taken":[["400","10000000"]],"consumed":[["400","20000000"]]}
  ]
}
```

`value` is signed canonical decimal or null, in the strategy's `fill_value` units
(scale 36 for these strategies), including its configured minimum-margin
adjustment. `legs` has one closed `{atoms,cost,taken,consumed}` per leg, using
native units exactly like opening fill results. `taken` records actual quantity;
`consumed` retains the displayed final level even if partly taken. The decision
value and its actual step count provide the causal edge-per-set denominator,
including for c>0. Never borrow opening t10/edge value for a later decision, and
never use a fixed native atom-to-contract multiplier across legs.

At c=0, decision evidence equals the opening sizing's value and walks exactly.
At c>0, it comes from that decision's walk. Zero-step decisions carry null value
and empty walks; `not_confirmed` keeps a null decision. `decision_untradeable`
still carries the walked evidence, with no fabricated arrival metrics. The reader
checks closed shapes, best-first prices, native quantity/cost, consumed versus
taken, steps times units, worst taken limits, and reprices `fill_value` at the
computed decision time. Confirmation and arrival ladders and quote ages remain
writer-attested, as in the SDK mechanism. No new manifest/file format number is
needed: the closed strategy policy version selects these exact extensions.

No new retained stream boundaries or offsets are declared. Probe decisions and
arrivals retain the sparse catch-up rules and original Risk numbering. The current
transport is wire V4's declared sections; it supersedes the supplied spec's V3
envelope while preserving those timing rules and all semantic output identities.
Pending-state reservations include the extra decision walks/strings and fail
before exceeding the existing shared budget or pending count. Reader opening
walks are also charged to its state budget. Existing line and file limits remain.

## Offline verification

`replay.tests.test_strategy_execution_realism` exercises all four production
strategies through real preparation, decoder, SDK writer and independent readers
with nonempty episode/probe rows. It covers the reference grid, observed and default
delays (including a change at a confirmation instant), mixed/reviewed and stale
flags, exact 0.01-contract steps, a short recording t10, zero-step decision
evidence, rehashed flags/decision tampering, sparse/full probe bytes with preserved
Risk counts, retained-state size reservations and pending-count failure.

The legacy byte golden
`replay/tests/fixtures/strategy_execution_realism_legacy.json` was obtained by
executing the dependency base `902871166496aa130fc493685112ba947a08f5e1` independently
in a temporary source tree. Every output file, including receipts and manifests,
was byte-identical after this change for four old fill-policy fixtures and the
old policy's explicit-probe fixture. The existing complement V1 goldens remain a
separate gate. The preserved five-bundle input/runtime and screen acceptance are
not replaced by these synthetic tests.
