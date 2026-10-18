# Legacy opt-in probes and acceptance limits

Scope: local offline remediation on integration `e3f9330` (#82), containing
R12 (#81). No retained-data acceptance, workload, deployment or cap increase is
authorized by this document.

## 1. Supported policies

| Strategy | Sweep, no fills/probes | Legacy fills, optional probes | Realism, required probes |
|---|---:|---:|---:|
| cross_venue_arbitrage | 2 | 3 | 4 |
| same_venue_multi_market | none | 1 | 2 |
| same_venue_implication_cover | 2 | 3 | 4 |
| cross_venue_implication_cover | 2 | 3 | 4 |

Legacy `fills.probes` uses the existing closed SDK probe policy, including its
64-scenario bound and explicit venue defaults. Omission means no probes. Sweep
policies reject `fills`; legacy policies reject `flags`. Realism policies still
require flags, edge/t10 sizings and probes. Storage selection changes none of
these combinations. Plain and economic-zstd output must both bind, complete and
pass the independent semantic reader using the committed configuration.

## 2. Retention follows configured probes, mapping follows policy

Do not infer whether probes exist from the presence of flags. The owning
strategy must retain every entry ladder and, when actual probes are configured,
every ladder returned by `fill_exits`. Implication covers inherit this from
cross-venue arbitrage.

| Mode | Kalshi sides / transforms / retained ladders |
|---|---|
| Sweep | bid / complement ask / no full ladders |
| Legacy fill without probes | bid / complement ask / complement ask |
| Legacy fill with probes | bid, ask / complement ask, complement bid / both transforms |
| Realism fill with probes | bid / complement ask / complement ask, native bid |

Here “complement ask/bid” names `kalshi_complement_ask/bid`. For non-Kalshi
books retain native ask in fill mode, and ask plus bid with probes; sweep
retains neither full ladder. Existing side reads remain unchanged otherwise.

Legacy exits keep the SDK reverse mapping on the **entry source book**:
`ask → bid`, `bid → ask`, and the analogous complementary transforms. Reading
the native ask input is required to retain a legacy complementary bid; it does
not synthesize asks. On normalizer-shaped bids-only Kalshi data that exit is
empty, so unexited BUY excess retains its full cost. This is a legacy diagnostic
limitation, not evidence that the acquired book lacks bids. Use a realism policy
for R12's acquired-book native-bid exits; do not silently upgrade legacy economics.

R12's committed-arrival view rule, bounded exit walks, unchanged entry-bound
failure and conservative uncovered-sale arithmetic apply unchanged. Keep
`probe exit book not planned`, `probe exit ladder not retained`, and
`probe exit unsupported for Kalshi sold leg` exact. Missing entry retention and
venue defaults still fail with `fill ladder not retained` and
`probe unknown taker delay missing venue`. Do not relax bind checks.

No row, policy, manifest or experiment schema changes. Preserve no-probe bytes,
legacy explicit-probe bytes and new-policy probe bytes. Existing legacy probe
goldens used a test-only retention override; remove that override to exercise
the production support promised by the documentation. Do not add native Kalshi
asks to new regressions.

## 3. History and falsification

SDK probes and their mandatory exit-retention bind check arrived in `f4cfa5c`
(#75), without changing these strategies' entry-only requirements. `36bfb95`
(#78) documented opt-in probes and tested them with locally patched retention.
`40c2804` (#79) added flags-gated retention only for new policies. R12 preserved
the default legacy mapping; its §4 statement that old policies “have no probes”
describes the ordinary old-policy fixtures, not a parser restriction. The SDK
§16 and producer contract explicitly preserve old-policy probe schemas/bytes.
`e3f9330` (#82) added the first unpatched package test of multi-market policy 1
with probes. Its plain subcase fails at initial bind, before compression, with
`probe exit ladder not retained`. This is an older unsupported execution path
despite an accepted configuration and documented opt-in contract, not a codec
regression or an invalid test policy.

Before changing production code, pin legacy bids-only YES/NO excess for all four
factories, plain and zstd, through writers and semantic readers. Expected legacy
exit is empty/full-cost loss even with acquired-side bids. Run the unchanged
five-layout package test before/after. Preserve the R12 60-scenario and byte
goldens and the §8 entry/exit/sold-shortfall controls.

## 4. Separate launch blocker: capacity is not compression

The reported 10-bundle baseline failure has no supplied exception or failing
file metadata. Do not classify it as output rather than retained state yet.

| Limit | Current ordinary SDK/profile bound | Scope |
|---|---|---|
| Logical file bytes, including LF | 536,870,912 (512 MiB) | Each stream, participant and bundle |
| Rows | 2,000,000 | Each ordinary stream, independently of bytes |
| Line bytes, including LF | 65,536 | Each ordinary NDJSON row |
| Retained detached state | 536,870,912 (512 MiB) | Participant writer/reader state, not file size |

Entity/reason tables have separate document limits. Availability has its own
256 MiB/1,000,000-row limits. Transitions/levels/volatility keep their existing
16 GiB/50,000,000-row limits and their distinct line limits. Change none of these
numbers or the memory budget in this remediation.

Bound failures must identify the stream/path (or retained-state context), the
dimension and observed/attempted usage and cap. Preserve existing error prefixes
and pre-write/pre-retention rejection. Distinguish truncation from line overflow.
Ordinary plain/zstd readers must report the same logical capacity dimensions;
compressed physical size cannot substitute for logical size.

Compression happens after bounded plaintext writing, so it cannot avoid an
ordinary stream cap. These limits are not accumulated across the ten bundles;
a smaller sample does not cure one oversized bundle. Preserve complete evidence:
no truncation, sampling, scenario reduction or artificial episode splitting.

Numeric capacity decisions remain **pending owner metadata**: exact exception
and traceback, participant/stream, code revision/configuration identity, logical
bytes/rows and attempted next row/line size (or state used/requested/cap), and,
if available, expected complete stream size/count. Request metadata only, not
retained payloads or secrets. If output is confirmed, propose the smallest
bounded, measured ceiling for that stream and its adjacent row ceiling, apply
it consistently to writers and all plain/zstd readers, and prove exact-bound
success/one-over rejection without increasing retained state. A fresh actual
operator-config/Universe-v8 five-bundle acceptance remains a separate gate and
cannot certify this unmeasured oversized bundle.

## 5. Failure-only probe and shared-writer memory diagnostics

Keep the complete probe grid and existing reservations. When probes are enabled,
pending-scenario admission failures and every shared writer retained-state
failure include these counts, calculated only on failure:

- `pending_scenarios` and `max_pending_probes`;
- `runtime_open_episodes`, counting all kinds, and
  `runtime_open_fill_episodes`, counting only episodes with a priced `fill`
  (not the trigger-positive/no-fill denominator state);
- `probe_episode_groups`, including a partially admitted grid, and
  `admitting_probe_scenarios`, its already queued but unregistered scenarios;
- `heap_events`, the actual queue length, including any stale finished events.

Counts describe retained state before the rejected operation, not the proposed
new scenario/episode. Probe groups may outlive runtime episodes. A grid is
registered only after all its scenarios have been queued; do not mistake zero
registered groups during partial admission for zero probe groups. Calculate
that one in-progress group's count from pending minus registered scenario
counts, without changing registration order or retaining extra probe objects.

Keep `pending probe bound`, `pending probe state budget` and shared-writer
`detached state budget` prefixes. Include the existing attempted retained bytes,
used/replaced/requested bytes and state cap even on pending-count exhaustion.
Do not charge or retain the rejected growth. Shared view/episode growth can fail
while probes consume the same budget, so its diagnostics use the same lazy
writer context. Reader and output failures remain distinct and do not acquire
these writer counts. Do not count, format, traverse the heap or size object
graphs on successful budget checks; failure counting uses bounded existing
episode tables and constant extra workspace. No reservation/retention
optimization, limit, scenario, economics or output-schema change is part of
this diagnostic amendment.
