# Economic strategy SDK V1

Status: **implemented, offline gates only.** The SDK (`replay/economic_sdk/`), the
complement port (policies 1 and 2), and the market profile
(`replay.market_profile:build`) are implemented on `feat/economic-strategy-sdk`, and
the offline tests pass. The bench-corpus acceptance in §9 and §11 has **not** been
run yet. That covers byte identity of the V1 port on the corpus, the timing and
output-size targets, and the profile readouts. §13 records the decisions and
deviations made during implementation. The document builds on the same-venue
complement strategy ([SAME_VENUE_COMPLEMENT_V1.md](SAME_VENUE_COMPLEMENT_V1.md)) and
on the fixture review of that strategy.

Every economic strategy needs the same machinery:

- turn cuts into time;
- keep detached book state;
- decide what to re-evaluate;
- stage same-timestamp cuts;
- split time at scope boundaries;
- track intervals, episodes, and slices;
- write bounded output in close order;
- verify it with an independent reader.

The complement strategy implements all of that inside its own module. This
document moves it into a shared SDK. A strategy then supplies only:

- what book data it needs;
- which baskets it evaluates;
- a pure function that turns book views into an observation.

The SDK also adds a standard **market profile** output: per-book trading data
points. These describe how each market behaves on each venue, whether or not any
strategy finds an edge. That is the primary product of the current research phase.

Out of scope:

- changes to the Replay stream wire, Risk, normalizers, preparation, the
  supervisor, or the fee SDK;
- Universe claims and outcome masks (the separate context.json v2 work);
- cross-bundle corpus orchestration (§10 lists only the requirements it places on
  this SDK).

## 1. Why: findings from the complement fixture review

The complement strategy ran on the local bench corpus: bundle
`bundle_e8a92effa246b9548571c907`, Procyon vs Galorys CS2. The window was 140
minutes, with 18 planned books and 2,008,580 stream entries. It ran beside
`bundle_coverage` in one supervisor attempt.

The arithmetic and timing were correct. Coverage still reproduced its reference
hashes (semantic `87cb094d…`, intervals `41be6ad8…`). The problems all sit in the
shared machinery:

| Finding | Measurement |
|---|---|
| Throughput | About 3,000 stream entries/s, against about 37,000 for coverage alone. The attempt took 665 s, against 55 s for coverage alone. With the stock 300 s attempt deadline, a run failed at 57% (`budget_exhausted`). |
| Where the time went | 77% of strategy callback time was in recursive retained-size accounting (`_deep_size` through `_entry_cost`), measured by a local cProfile on synthetic deep books. Evaluation, walks, and projections took about 15%. |
| Output volume | 372 MB of output for 2.3 hours of one match. `slices.ndjson` was 256 MB of its 512 MiB fail-closed cap. |
| Placebo share | Placebo entities produced 307,522 of 307,676 slice rows, 141,485 of 267,428 measurement rows, and about 323 MB of the 372 MB. 306,831 placebo slices ended `CONSUMED_CHANGED`: one 735-byte row per quote change during minutes-long placebo episodes. |
| Placebo meaning | In a single-event bundle, the cyclic neighbour is a sibling market of the same match. On Kalshi it paired NO on GLS winning with YES on PRO winning, which is the same outcome. That produced "gaps" up to 98–99.8¢ lasting until each scope boundary. As a null control it carried no information here. |
| Real signal | Nine Polymarket self-crossings (a token's bid above its ask) of 0.09–1.95 ms, during fills, each recorded twice: as a long episode and as a short episode. They are useful market data, but no label identified them. |

A follow-up change made the accounting cheaper: 396.6 s, still about 7× coverage
(§9). That accounting is still strategy-local, so the SDK must solve it once for
every strategy.

## 2. Strategy interface

A strategy is a module exposing one object that the SDK wraps into a supervisor
factory:

```python
from replay.economic_sdk import Strategy, factory

class SameVenueComplement(Strategy):
    name = "same_venue_complement"
    version = 2

    def requirements(self, snapshot, policy) -> Requirements: ...
    def baskets(self, snapshot, policy, scope_index) -> tuple[Basket, ...]: ...
    def evaluate(self, basket, views, context) -> Observation: ...

build = factory(SameVenueComplement())
```

- **`requirements`** runs once, before any callback. It returns, per book key:
  - the sides needed;
  - the ticket sizes in contracts, or a level depth;
  - whether consumed displayed quotes are needed (for slices);
  - an optional SDK-provided view transform. V1 has one: `kalshi_complement_ask`,
    which projects the other orientation's bids to asks at `P − p`, as defined in
    the complement spec §4.2.

  Requirements are static for the run and part of the experiment identity.
- **`baskets`** returns the immutable basket descriptors for one scope. Each
  descriptor has a stable sorted identity, its legs (book keys), and an optional
  admission status (`NOT_CAPTURED`, `UNSUPPORTED_SHAPE`, `UNSUPPORTED_SCALE`, or a
  strategy-registered value). The SDK owns:
  - the reverse map from book to baskets, including control legs;
  - entity identities;
  - the denominator rules from the complement spec §2.
- **`evaluate`** is a pure function of the basket and detached `BookView`s. It
  never sees a live `Book` or cut body. It returns an `Observation`:
  - `status`: one of the SDK's closed statuses (`UNUSABLE`, `ONE_SIDED`,
    `DEPTH_LIMITED`, `DEPTH_SUFFICIENT`), or a strategy-registered diagnostic
    status;
  - `value_class`: from a closed set the strategy declares, with a declared map
    from class to episode predicates;
  - `predicates`: map of episode kind to bool, consistent with the class map. The
    SDK rejects contradictions;
  - `payload`: declared numeric fields with explicit scales (for example `gap_gross`
    with `gross_scale`), written at episode open and at maximum;
  - `quotes`: the consumed displayed quotes per leg that define slice identity. The
    default is the consumed levels of the fills the strategy used;
  - `skew_legs`: the legs whose last-change times define skew. The default is all
    legs.

  Fee assessment stays a strategy concern, through the existing fee bridge, and is
  called only from `evaluate`.

Strategies do not write files, track time, or keep state between callbacks. A
strategy that needs history (for example a time-shifted control, §6) declares it
in `requirements`, and the SDK keeps it bounded.

## 3. Book views and projections

For every changed book key in a cut, the SDK:

1. reads each required side **once** (`Book.levels(side)`);
2. walks it **once** for the union of all requested sizes across all baskets,
   using the existing `replay.economic_fills.walk`;
3. applies declared view transforms once;
4. stores a detached `BookView`:
   - `validity` and `reason`;
   - last-change time;
   - side presence;
   - best quotes;
   - per-size `Fill`s (with `taken` and `consumed`);
   - transformed fills.

Views are shared across every real and control basket that uses the book. They
outlive the callback and never reference live decoder objects. The complement
spec's rules on prior projections, scope entry, and pre-start prologue cuts apply
unchanged (§5 there).

## 4. Runtime semantics (moved, not changed)

These rules move verbatim from the complement spec into the SDK and become the
contract for every strategy:

- the cut clock;
- same-time staging and commit;
- prior-state boundary advancement;
- quiet scope entry;
- the re-evaluation set;
- half-open intervals;
- zero-length suppression;
- `instantaneous_positive` as a writer-attested diagnostic;
- censoring at run end;
- `SCOPE_END` closure;
- leg skew at each instant;
- latency-qualified entry time.

The sections concerned are §§3, 5, and 7 of the complement spec. The
complement-specific parts (pricing, fees, verdict wording) stay in the strategy.

## 5. Output detail levels

Every strategy writes the same files with the same row schema. Only the
basket-descriptor and payload fields differ. Detail is chosen per **entity class**
(real or control) in the policy, so it is part of the experiment identity:

| Level | Files | What the reader can recompute exactly |
|---|---|---|
| `intervals` | `measurements.ndjson`, `summary.json` | status and value-class durations, skew attribution of time, denominators |
| `episodes` (default for real) | adds `episodes.ndjson` and compact `slices.ndjson` | the above, plus episode partitions, lifetimes, slice survival, latency-qualified time, tiers, quantiles, verdicts |
| `slices` | full `slices.ndjson` | the above, plus consumed quotes and payload at every slice, and the gross arithmetic of every slice |

- **Compact slice rows** carry `episode_id`, `start_ns`, `end_ns`, `end_reason`, and
  `censored` only, about 180 bytes against today's 735. Quotes and payload are
  written once per episode, at open, so the reader can still check the opening
  arithmetic. Full slice rows are today's format.
- **Controls default to `intervals`.** Their summary rows report the same
  time-weighted fractions as real baskets.
- **Real and control rows go to separate files** at every level. A reader never
  needs an entity lookup to tell them apart.

The independent reader is generic. It checks:

- the complete interval partition per entity;
- maximal predicate runs against episodes;
- slice partitions of episodes;
- close order;
- bounds;
- recomputation of every summary figure available at the chosen level.

Strategy-specific checks (for example the complement re-walk of consumed quotes)
plug in as reader hooks.

## 6. Controls

The placebo becomes an optional, named **control** with a declared construction.
The default is none.

| Control | Construction | What it measures |
|---|---|---|
| `time_shift` | Leg 2 replaced by the same leg's view `Δ` earlier (policy, for example 1 s and 5 s). The SDK keeps a bounded per-book ring of detached views covering `Δ`. | How often stale quotes alone produce a positive. This is the staleness null from the partition-sum spec §6. |
| `cyclic_neighbor` | Today's construction. | A broken-pair diagnostic only. In a single-event bundle it pairs related markets and must not be read as a null. |

- Controls use the same `evaluate` function and the same views.
- They never produce verdict inputs, only comparison rows.
- The ring for `time_shift` is bounded by a per-book entry count. The run fails
  closed if the bound is exceeded inside `Δ`.

## 7. Market profile output

`replay.market_profile:build` is an SDK-provided strategy group, meant to run on
every bundle beside any economic strategy. It needs no fee configuration. It writes
`profile.ndjson`: one row per (book key, scope, bucket), with a policy bucket width
(default 60 s, clipped to scope boundaries). It also writes `incidents.ndjson` for
discrete events.

Every duration is time-weighted in exact nanoseconds over the bucket. Every price
statistic carries its scale.

| Group | Data points |
|---|---|
| State | `usable_ns`, `unusable_ns` by reason kind, `not_initialized_ns`, `bid_empty_ns`, `ask_empty_ns`, `two_sided_ns` |
| Top of book | Time-weighted spread (mean, plus p50/p90 from an exact tick histogram); best bid and ask at bucket open and close; time-weighted mid; top-of-book quantity on each side (time-weighted mean) |
| Depth and cost to fill | Per configured size and side: time-weighted mean slippage of VWAP against best, in ticks; `depth_limited_ns`; time-weighted displayed depth within 1, 5, and 10 ticks |
| Activity | Book transitions; snapshots; operation counts; invalidations by reason; trades by disposition (`applied`, `observed`, `duplicate`, `not_authority`, `invalidated`, as coverage counts them); traded quantity; trade price against the prevailing mid (ticks, signed); aggressor side when the venue provides it |
| Quote stability | Distribution of top-of-book survival (time a best price stays unchanged), as an exact histogram over fixed log-spaced edges |
| Self-crossing | `crossed_ns` and `locked_ns` (book's own bid ≥ ask); per incident, a row in `incidents.ndjson` with start, end, maximum cross in ticks, and the quotes at maximum |
| Pair consistency | For a Kalshi outcome/complement pair and a Polymarket token pair: time-weighted deviation of the best-bid sum and best-ask sum from one unit, and time with each sum on the wrong side of one unit |

Kalshi books carry bids only. For Kalshi, ask-side statistics are computed from the
`kalshi_complement_ask` projection and labelled `projected`.

Profile rows identify the book only by native key, venue, and the snapshot's member
`market_id`. Linking books across venues to one claim or event is downstream work
over the context.json v2 claims. The profile records no economic judgement.

## 8. Retained-state bounds without recursive traversal

The SDK replaces runtime `_deep_size` accounting with **count-based bounds**:

- Per-entry costs are closed-form functions of declared shapes: the number of
  books, sides, and sizes, the levels per consumed slice, the open episodes, and
  the ring entries.
- They use constants that a unit test proves conservative. The test measures a
  real recursive `sys.getsizeof` traversal on representative maximal states, and
  asserts that the formula is greater than or equal to it.
- Hard caps:
  - levels per consumed slice: 1,024;
  - open episodes: entities × kinds;
  - ring entries per book: a policy value.

  Exceeding a cap fails the attempt. Nothing is truncated.
- The 128 MiB detached-state limit and the file, line, and row limits from the
  complement spec stay in force.

## 9. Performance requirements

On the bench corpus, with this SDK's complement port at default detail:

- **Speed.** The economic group must not be the slowest group by more than 20%. The
  two-group attempt must finish within 1.2× the coverage-only wall time (55.2 s on
  the reference build) plus 15 s.
- **Output.** Total output for the two-group attempt must stay below 64 MB at
  default detail, with controls at `intervals`.
- **Profiling harness.** A profiling factory wrapper (cProfile around callbacks
  only, with periodic dumps) ships with the SDK test utilities.

**Baseline at this branch's head**, which includes the follow-up accounting change:

- the two-group attempt took **396.6 s**, against 55.2 s for coverage alone (about
  7.2×);
- the complement semantic SHA was `2d85b833…`, identical to the pre-change run;
- coverage reproduced `87cb094d…`.

The 665 s figure in §1 is the earlier code.

## 10. Requirements for corpus runs

Strategies and the profile will later run unattended over every bundle with
existing materialized derivatives. Materialization is reused unless a normalizer
or materializer identity changes. This SDK must therefore guarantee:

- Output depends only on pins, snapshot, policy, requirements, and code revision.
- Retries reproduce identical semantic hashes. The complement strategy already
  proves this for itself, and the SDK must preserve it for every strategy.
- A completed reader works per bundle without network access.
- Profile and summary rows carry enough identity (snapshot SHA, experiment SHA,
  bundle ID, scope run ID) to concatenate across bundles without joining back to
  run directories.

## 11. Porting and acceptance

1. **Port first, change nothing.** Port `same_venue_complement` onto the SDK as
   policy version 1, with `slices` detail for real and control entities and the
   `cyclic_neighbor` control. On the bench corpus, the port must reproduce the
   committed V1 output byte for byte: semantic SHA, and the measurement, episode,
   placebo, and slice files. This proves the SDK preserves the semantics.
2. **Then switch defaults.** Introduce complement policy version 2:
   - `episodes` detail for real entities;
   - `time_shift` control at `intervals`;
   - a `SELF_CROSSED_LEG` diagnostic status when a leg's own book is crossed;
   - the verdict refinement below.

   It is a new experiment hash and is not expected to match V1.

   **Verdict refinement.** Count only fee-unknown time inside gross slices long
   enough to qualify at the headline latency, instead of all fee-unknown time.
   With this rule, the reviewed fixture reads as absent on both venues, because no
   before-fee slice reached 250 ms. Update the complement spec §9 when this lands.
3. **Market profile.** Run `market_profile` on the bench corpus beside coverage.
   It must:
   - reproduce coverage's trade disposition totals for the same books;
   - report the nine Polymarket self-crossing incidents;
   - show the one-sided onsets (about 12.1 and 64.5 minutes into the window).
4. **Close the open complement findings.**
   - Emit `SKEW_ARTIFACT_LIKELY` (complement spec §5).
   - Stop formatting `encoded()` bytes into reason strings: decode, or use a
     structured reason field.
5. **Port coverage as well, if it stays hash-identical.** Migrating
   `bundle_coverage` onto the SDK's clock and changed-books helpers is optional.
   If attempted, it must keep its reference hashes.

**Offline tests** extend the complement test list with:

- shared-view reuse across baskets;
- one walk per changed side per cut;
- control legs re-evaluated through the reverse map;
- `time_shift` ring bounds and its effect at exact nanoseconds;
- each detail level's reader recomputation;
- separate real and control files;
- count-based bound conservativeness;
- profile time partitions summing to the bucket duration;
- tick histograms;
- incident rows;
- Kalshi projected labels.

## 12. Open items

- **Bucket width and edges.** Choose the profile bucket width and the histogram
  edges for production. Both are part of the profile policy hash.
- **Trade fields.** Confirm which trade fields each venue's normalized `TradeEvent`
  carries (aggressor in particular) before relying on them in profile rows.
- **Fee catalog.** A reviewed fee catalog and instrument bindings remain the
  prerequisite for net economics (complement spec §12). The profile does not need
  them.

## 13. Implementation notes and decisions

This section records how the implementation resolves points that §§2–12 left
open, and where it deviates from them. Where this section and an earlier section
disagree, the code follows this section.

**Interface (§2)**

- `factory(StrategyClass)` builds a fresh, configured instance per supervisor
  context. It is not a singleton instance as the §2 sketch shows, because
  configuration (fees, snapshot) is per run.
- A reader-only instance is built from the manifest. It never sees fee
  configuration or paths.
- Hooks beyond §2:
  - `control_descriptor`, which is how a strategy spells control identities;
  - `manifest`, `validate`, and `bind`;
  - the reader hooks `check_measurement`, `open_facts`, `check_open`,
    `check_quotes`, `summary_key`, `duration_label`, and `summarize`.
- `Basket` carries:
  - `order`, the strategy part of the close-order key;
  - `control_leg`, `peer_group`, and `peer_order`, which drive the
    cyclic-neighbour peers;
  - optional `inputs`.
- `inputs` declares, per leg, every `(source, size)` that `evaluate` reads: a
  side, a transform, or `("best", None)`. `Observation.context_free` asserts that
  the result used nothing else, in particular not the time, sequence, or scope.
  With both, the runtime reuses the previous observation while those inputs are
  unchanged (identity or equality of the detached fills), and re-stages the entity
  only when its leg skew bucket moves. Without them, every affected entity is
  re-evaluated. A strategy that declares inputs incorrectly gets wrong output, so
  declarations are covered by the strategy's own tests.
- Fee-assessed complement observations are never context-free, because their
  order identities hash the time and sequence. They are therefore always
  re-evaluated, which keeps V1's `instantaneous_positive` counts identical.

**Views and reads (§3)**

- Each touched side is read in full once with `Book.levels(side)`, then walked
  once for the union of sizes.
- A bounded `levels(side, n)` prefix read was measured. It is slower, because its
  selection is a Python-level heap and the full read is a C sort.
- The walk validates only the levels it touches.
- Sides that a cut's operations did not touch are reused from the prior view.
- Re-walked fills equal to their predecessors keep the old object, so downstream
  comparisons are identity checks.

**Files (§5)**

- Output layout 1 is the frozen V1 wire. Measurements and slices mix real and
  placebo rows, and `placebo_episodes.ndjson` exists. It is used only by complement
  policy 1, and only at `slices` detail.
- Layout 2 writes real rows to `measurements.ndjson`, `episodes.ndjson`, and
  `slices.ndjson`, and control rows to the same names with a `control_` prefix.
  Only the files a class's detail level needs are created.
- Row `version` equals the layout version.
- At `episodes` detail, episode rows add `open_quotes`. Compact slice rows are
  exactly `{episode_id, start_ns, end_ns, end_reason, censored}`. The reader maps
  them to their parent episode, recomputes tiers and `Q`, and re-walks the opening
  quotes against the opening gross.

**Controls (§6)**

- `time_shift` keeps one ring of committed views per shifted book across all
  scopes.
- A ring entry is recorded at its exact commit time, even when no entity is staged
  at that time.
- Each shift change is re-evaluated by a timer at `τ + Δ`. Timers run in time
  order with scope boundaries. A timer at a cut's time joins that cut's stage.
- Before any history exists at `t − Δ`, the shifted leg is a `not_initialized`
  view, so the control measures `UNUSABLE`. The SDK never invents pre-start state.
- Ring memory charges each shared fill object once, by reference count, and
  exceeding `time_shift_ring_entries` fails the attempt.
- Control descriptors in complement policy 2 carry
  `control: {kind, leg, shift_ns | replaced_leg}`. Real descriptors carry
  `control: null`.

**Market profile (§7)**

- **Gating.** The profile is computed only when asked for. It runs either as its
  own group, `replay.market_profile:build` with
  `{version, snapshot_directory, snapshot_sha256, policy}`, or embedded in an SDK
  strategy through `Requirements.profile`; complement policy 2 exposes this as
  `"profile": null | <profile policy>`. Within a profile, `policy.groups` selects
  any of `activity`, `depth`, `pair_consistency`, `quote_stability`,
  `self_crossing`, and `top_of_book`. `state` is always on. A disabled group does
  no work: without `depth` the collector reads only best quotes and never whole
  ladders.
- **Profile policy**, which is closed:
  - `version`;
  - `bucket_ns`;
  - `groups`;
  - `sizes_contracts`;
  - `tick_atoms`, a per-venue tick in price atoms that must cover every planned
    venue;
  - `depth_ticks`;
  - `survival_edges_ns`.

  The bucket width and edges remain an open production choice (§12).
- **Buckets** are aligned to multiples of `bucket_ns` in visible time, then
  clipped to scope boundaries, so bundles line up on the wall clock.
- **Exact integers.** Prices are integers in plan price atoms, with the row
  carrying `price_scale` and `tick_atoms`. Means are emitted as exact time
  integrals (`*_ns`) plus their nanosecond denominators, and are never rounded.
  For example, `spread_atoms_ns / two_sided_ns` is the time-weighted mean spread,
  `mid2_atoms_ns` integrates bid + ask (twice the mid), and `slippage_cost_ns`
  integrates the absolute VWAP-versus-best cost at `price_scale + quantity_scale`.
  Integrals may exceed 64 bits.
- **Spread** quantiles are time-weighted nearest ranks over an exact histogram
  keyed by spread atoms. The histogram itself is written.
- **Self-crossing incidents** are maximal intervals with bid ≥ ask. A locked book
  counts. The reader checks that incident time equals `crossed_ns + locked_ns`.
- **Quote survival.** Survival is recorded where it ends. A survival that started
  before scope entry, or is cut by a scope end, is counted as `censored`, not
  binned.
- **Activity and trades** are counted only for cuts at or after the requested
  start, for the current scope's books, with coverage's dispositions. A trade's
  mid is the profile state prevailing before that cut's transitions. Trades whose
  wire scales differ from the plan are counted separately and excluded from price
  and quantity statistics. Aggressor counts include `none`; whether each venue's
  normalizer fills `aggressor` is still open (§12).
- **Deviation: pair consistency** is written to its own file,
  `pair_profile.ndjson`, with one row per (scope, member pair, bucket), because it
  is a property of a pair, not of one book. Pairs are PM token pairs and Kalshi
  outcome/complement pairs.
- **Kalshi asks** are the complement projection. Profile and incident rows carry
  `ask_source: "projected"`.
- **Summaries.** A standalone profile writes its own manifest, summary, and
  receipt. An embedded profile's files are listed in the strategy manifest, and
  its summary is included under `summary.profile`.

**Bounds (§8)**

- Costs are closed-form functions of shapes in `replay.economic_sdk.bounds`.
- The tests compare them with a recursive `sys.getsizeof` traversal of maximal
  states: views, observations, episodes, input fingerprints, and snapshot-shaped
  JSON.
- The reader keeps its own budget, using the same closed-form costs.

**Complement policy 2 (§11.2–4)**

- **Policy fields.** Policy 2 adds `detail {real, control}`, `controls` (a list of
  `{kind: "time_shift", shift_ns: [..]}` and/or `{kind: "cyclic_neighbor"}`),
  `time_shift_ring_entries`, and `profile`. All are required, because the schema
  is closed.
- **`SELF_CROSSED_LEG`** applies to pair baskets when a leg's own best bid is
  strictly above its best ask. It is checked after `UNUSABLE` and before
  `ONE_SIDED`, and its reasons name the legs. Kalshi books carry no native asks,
  so this check never fires there; the profile reports Kalshi projected crosses
  instead.
- **Verdict refinement.** Each summary row adds
  `fee_unknown_in_qualifying_gross_slices_ns`. This is the `FEE_UNKNOWN`
  measurement time inside real gross slices whose survival reaches the headline
  latency, attributed at each instant. In verdict rule 3, `X` is the sum of these
  values at the headline size. The old total is kept as `fee_unknown_total_ns`.
- **`SKEW_ARTIFACT_LIKELY`** is set, in a verdict's `labels`, when positive gross
  time at the headline size exists and every bit of it lies in skew buckets whose
  lower edge is ≥ 1 s. It does not change the verdict.
- **`UNUSABLE` reasons** use the reason's JSON text. Policy 1 keeps V1's
  `b'…'` formatting for byte identity.

**Verification**

- **V1 port.** Seven synthetic multi-venue tapes are driven through the real
  `Decoder`. They cover pairs, placebos, slices, scope boundaries, prologue cuts,
  Limitless, and partial fee bindings. Their output hashes were recorded from the
  pre-SDK implementation (`replay/tests/fixtures/complement_v1_golden.json`), and
  the SDK port reproduces every file byte for byte.
- **Synthetic speed.** On a dense synthetic tape, the old runtime takes about
  846 µs per cut. The new runtime takes about 310 µs at policy 1 and about 470 µs
  at policy 2: 150-level books, 8 sizes from 1 to 1000 contracts, and about 2,500
  changes per second. These are relative figures only. The §9 targets are
  measured on the bench corpus.

**Not done**

- `bundle_coverage` is not ported (§11.5, optional).
