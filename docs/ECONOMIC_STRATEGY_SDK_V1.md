# Economic strategy SDK V1

Status: **implemented; offline gates pass; bench acceptance pending.** The
following are implemented on `feat/economic-strategy-sdk`:

- the SDK (`replay/economic_sdk/`);
- the same-venue complement on it (policies 1 and 2, §11);
- the market profile (`replay.market_profile:build`, §7);
- opt-in fill checks (§13), proven by synthetic offline tests only.

The bench-corpus acceptance in §11 runs outside the repository and has not yet
been run against this revision. The document builds on the same-venue complement
strategy ([SAME_VENUE_COMPLEMENT_V1.md](SAME_VENUE_COMPLEMENT_V1.md)).

Every economic strategy needs the same machinery:

- turn cuts into time;
- keep detached book state;
- decide what to re-evaluate;
- stage same-timestamp cuts;
- split time at scope boundaries;
- account time per measured key;
- track episodes and slices;
- write bounded output;
- verify that output with an independent reader.

The SDK owns all of it. A strategy supplies only:

- the book data it needs;
- the baskets it evaluates;
- a pure function that turns book views into an observation.

The SDK also provides a standard **market profile**: per-book trading data points
describing how each market behaves on each venue, whether or not any strategy finds
an edge.

**Principle.** A strategy's result is its **episodes**. "No episodes", together
with **denominators**, is the complete negative result. Denominators record how
long each basket was measurable, and why it was not measurable otherwise.
Everything else is optional diagnostics or audit, and is off by default.

Out of scope:

- changes to the Replay stream wire, Risk, normalizers, preparation, the supervisor
  or the fee SDK;
- Universe claims and outcome masks (the context.json v2 work);
- the fee catalog;
- cross-bundle corpus orchestration (§10 lists only what it requires of this SDK).

## 1. Why: findings

All findings come from the local bench corpus: bundle
`bundle_e8a92effa246b9548571c907`, Procyon vs Galorys CS2. The window is
140 minutes, with 18 planned books and 2,008,580 stream entries. Each run was
beside `bundle_coverage`, which alone takes 55.2 s.

**The pre-SDK complement** had correct arithmetic and timing. Coverage still
reproduced its reference hashes (semantic `87cb094d…`, intervals `41be6ad8…`).
Every problem sat in the shared machinery:

| Finding | Measurement |
|---|---|
| Throughput | The attempt took 665 s, later 396.6 s, against 55 s for coverage alone. 77% of callback time went to recursive retained-size accounting. |
| Output volume | 372 MB for 2.3 hours of one match. `slices.ndjson` was 256 MB of its 512 MiB cap. |
| Placebo share | Placebo entities produced about 323 MB of the 372 MB: one 735-byte slice row per quote change during minutes-long placebo episodes. |
| Placebo meaning | In a single-event bundle, the cyclic neighbour is a sibling market of the same match. On Kalshi it paired NO on one team with YES on the other, which is the same outcome. As a null it carried no information. |
| Real signal | Nine Polymarket self-crossings (a token's bid above its ask) of 0.09–1.95 ms. Each was recorded as both a long and a short episode, and no label identified them. |

**The first SDK revision (80aa64c)** gave these results:

| Run | Result |
|---|---|
| Policy 1 compat | 179.6 s and 372.7 MB. Byte-identical to V1 (semantic `2d85b833…`). |
| Policy 2 with `time_shift` | **Failed.** `control_measurements` hit the 512 MiB cap at 55% of the stream. 1,369,515 of 1,384,680 control rows were skew-bucket flips: a time-shifted leg keeps its historical last-change time, so its skew bucket changed on almost every update. |
| Policy 2 with controls off | 98.8 s and 48.5 MB. Results were correct: nine Polymarket `SELF_CROSSED_LEG` intervals, and both verdicts absent. |
| Market profile | 84.5 s and 8.5 MB. |

In the V1 measurement file, the bloat came from:

- skew-bucket boundaries: 93% of rows;
- the experiment SHA and entity ID repeated in every row: about 40% of bytes;
- reason strings: 17 MB, including a Python `b'…'` repr bug;
- one entity per size × direction × real/placebo.

Placebo and control rows were the majority of every file. §§5 and 6 are the
response: denominators instead of a time series, skew that never splits time, and
controls that are opt-in and isolated.

## 2. Strategy interface

```python
from replay.economic_sdk import Strategy, factory

class SameVenueComplement(Strategy):
    def requirements(self, snapshot, policy) -> Requirements: ...
    def baskets(self, snapshot, policy, scope_index) -> tuple[Basket, ...]: ...
    def control_descriptor(self, basket, control, replacement, admission) -> dict: ...
    def evaluate(self, entity, views, context) -> Observation: ...

build = factory(SameVenueComplement)
```

`factory(StrategyClass)` builds one configured instance per supervisor context,
because fees and the snapshot are configured per run. A reader-only instance is
built from a manifest; it never sees fee configuration or filesystem paths.
Strategies write no files, track no time, and keep no state between callbacks.

**`requirements`** runs once, before any callback. Per book key it gives:

- the sides needed;
- the ticket sizes, in contracts;
- an optional SDK view transform. V1 has one, `kalshi_complement_ask`: the other
  orientation's bids, projected to asks at `P − p` (complement spec §4.2).

It may also request the market profile (§7). Requirements are static for the run.

**`baskets`** returns one `Basket` per measured key: a basket at one direction and
size. A basket carries:

- `descriptor`: strategy-owned JSON. Its digest is the entity identity, and its
  `legs` list corresponds to `legs` by index.
- `legs`: book keys.
- `order`: the strategy part of the close-order key.
- an optional `admission` status (`NOT_CAPTURED`, `UNSUPPORTED_SHAPE`,
  `UNSUPPORTED_SCALE`), with `admission_reasons`.
- `control_leg`, `peer_group`, and `peer_order`: which leg a control substitutes,
  and how cyclic-neighbour peers are chosen.
- optional `inputs`.

`inputs` declares, per leg, every `(source, size)` that `evaluate` reads. A source
is a side, a transform, `("crossed", None)` for the book's own bid-above-ask flag,
or `("best", None)` for the best quotes.

The SDK owns:

- the reverse map from book to entities;
- control construction (§6);
- entity identities;
- the scale admission of substituted legs;
- the denominators.

**`evaluate`** is a pure function of the entity and its detached `BookView`s; it
never sees a live `Book` or cut body. It returns an `Observation` with these
fields:

- `status`: an SDK status (`UNUSABLE`, `ONE_SIDED`, `DEPTH_LIMITED`,
  `DEPTH_SUFFICIENT`) or a declared diagnostic status.
- `reasons`: structured reasons, as canonical JSON object text. They are
  deduplicated into the reason table (§5).
- `value_class`: from the strategy's closed set. It is present exactly when the
  status is `DEPTH_SUFFICIENT`.
- `predicates`: the active episode kinds. These must equal the declared map from
  class to kinds, and the SDK rejects contradictions.
- `payload`: the strategy's values, written at episode open and at maximum.
- `quotes`: the consumed displayed quotes per leg. These define slice identity.
- `skew_legs`: the legs whose last-change times define skew. The default is all
  legs.
- `context_free`: an assertion that the result used only the declared inputs, not
  the time, sequence, or scope.

With `inputs` and `context_free`, an entity whose inputs are unchanged keeps its
observation and is not staged at all (§3). Fee assessment stays a strategy concern,
through the fee bridge. Fee-assessed observations hash the time and sequence into
their order identities, so they are never context-free.

Reader hooks let a strategy add its own checks:

- `open_facts` and `check_open` validate the payload schema and fee class;
- `check_quotes` re-walks consumed quotes against the gross;
- `summary_key` and the summary builders shape `summary.json`.

## 3. Book views

For every changed book key in a cut, the SDK takes these steps:

1. It learns from the cut which sides the operations touched, and the best price
   touched on each side. A snapshot or invalidation counts as touching every side.
2. It reuses a side unchanged when the side was not touched, or when every touched
   price is strictly worse than the deepest level the largest size consumed. Such
   an operation cannot change any fill, the best quote, or presence, so the side
   is neither read nor walked.
3. Otherwise it reads the side once (`Book.levels(side)`) and walks it once, for
   the union of sizes (`replay.economic_fills.walk`). The walk validates only the
   levels it touches. A bounded `levels(side, n)` read was measured and is slower:
   its selection is a Python-level heap, while the full read is a C sort.
4. It interns results. A re-walked fill or best quote equal to its predecessor
   keeps the predecessor's object, so input fingerprints are flat tuples compared
   by object identity.
5. It applies declared transforms. A transform is reused while its source fills
   are identical.
6. It stores a detached `BookView` holding:
   - validity and reason;
   - last-change time;
   - side presence;
   - best quotes;
   - the `crossed` flag;
   - per-size fills;
   - transformed fills.

Views are shared by every real and control entity that uses the book. They outlive
the callback and never reference decoder objects.

**Re-evaluation.** An entity on a changed book is staged unless all three of these
hold:

- its current observation is context-free;
- every object its declared inputs read is identical to its last evaluation;
- in layout 1 only, its skew bucket is unchanged.

A staged entity whose fingerprint still matches reuses its observation without
calling `evaluate`.

In layout 2, skipping economic evaluation does not skip live skew tracking
inside positive episodes. A skew-only change stages the cached observation with
the new skew, without another walk or evaluation, without splitting a slice, and
without counting a new `instantaneous_positive` observation. Same-time staging
still commits only the final skew. For a time-shift control, changes to its shifted
leg's live book refresh skew immediately; its economic inputs advance only at the
shift timer.

## 4. Runtime semantics

These rules move from the complement spec (§§3, 5, 7 there) into the SDK, and are
the contract for every strategy:

- the cut clock;
- same-time staging, where only the final state at a time is committed;
- prior-state boundary advancement;
- quiet scope entry;
- pre-start prologue cuts, which initialize state only;
- half-open, exact-nanosecond time;
- zero-length suppression;
- class time is charged only for positive-length intervals; a class flip at a
  quote-slice boundary never inserts a zero-duration class into episode or
  latency-qualified maps;
- `SCOPE_END` closure;
- censoring at run end;
- `instantaneous_positive` as a writer-attested diagnostic: a count of superseded
  positive same-time observations;
- latency-qualified entry time.

Complement-specific parts (pricing, fees, verdict wording) stay in the strategy.

**Skew** is the absolute spread of the skew legs' **live** last-change times. It is
always computed from the live views, so a time-shifted control leg, whose view
keeps its historical last-change time, cannot churn skew. In layout 2 skew never
splits time:

- it is recorded (`leg_skew_ns` and its bucket) at each episode open and each slice
  open;
- latency-qualified entry time is attributed to the skew bucket in force at each
  entry instant, inside positive slices only.

Layout 1 (complement policy 1) keeps skew as a measurement dimension, for byte
identity.

## 5. Output

Output **layout 2** is the SDK default. These are the files under the group's
output directory:

| File | Contents |
|---|---|
| `entities.json` | One record, `{scopes: [[{hash, descriptor}, …], …]}`. Per scope, the entities in close order. A row's `entity` is an index into its scope's list. |
| `reasons.json` | One record, `{reasons: [object, …]}`. Each distinct structured reason appears once, and rows reference reasons by index. |
| `denominators.ndjson` | One row per (scope, entity). |
| `episodes.ndjson` | One row per episode of a **real** entity. Always written. |
| `slices.ndjson` | Compact rows, only inside positive episodes. |
| `controls/<name>/…` | Only for enabled controls (§6). |
| `audit/measurements.ndjson`, `audit/slices.ndjson` | Only with `audit_intervals`. |
| `manifest.json`, `summary.json`, `content_receipt.json` | As before. |

**Denominator rows** have these fields:

- `scope` and `entity`;
- `status_ns`: exact nanoseconds per status (SDK statuses, admissions,
  diagnostics);
- `class_ns`: per value class within `DEPTH_SUFFICIENT`. It is present exactly when
  that status is.
- `reason_ns`: a list of `[status, [reason indexes], ns]` entries for observations
  that carry reasons. For example, unusable time is broken down by structured
  reason kind.

Durations are canonical decimal strings. Denominators are **writer-attested**. The
reader proves their internal arithmetic and their exact agreement with the
episodes. Proving each interval needs the audit.

**Episode rows** have these fields:

- `scope`, `entity`, and `episode_id`, where the ID is the SHA-256 of
  `[scope, entity hash, kind, start_ns]`;
- `kind`, `start_ns`, `end_ns`, `end_reason`, and `censored`;
- `opening_slice_survival_ns`, `viable_tiers`, and `qualified_ns` per tier;
- `qualified_by_skew_ns`: per tier, entry time by skew bucket;
- `class_ns`: time per value class within the episode;
- `qualifying_class_ns`: per tier, the class time inside slices that reach the tier;
- `open`: `value_class`, reasons, `leg_skew_ns`, `skew_bucket`, `values`, and
  `quotes`;
- `maxima`: per declared maximum field;
- `at_max`: `values` and `quotes` when the first maximum field was last raised.

Consumed quotes and the payload appear only at open and at maximum.

**Compact slice rows** have these fields:

- `episode_id`, `start_ns`, `end_ns`, `end_reason`, and `censored`;
- `leg_skew_ns` and `skew_bucket` at slice open.

A slice ends on any change to the consumed displayed quotes, or at its episode's
end.

**Row hygiene.** The experiment SHA, policy SHA, and version appear once, in the
manifest. Entities and reasons are table indexes. Reasons are never formatted from
`encoded()` bytes, and no field is always null.

**Audit.** `audit_intervals: true` restores the complete per-entity interval
partition:

- `audit/measurements.ndjson` holds `{scope, entity, start_ns, end_ns, status,
  reasons, value_class?}`, without skew splits;
- `audit/slices.ndjson` holds the compact slice fields plus `quotes` and `values`.

Both cover real entities only. The audit is off by default.

**Layout 1** is the frozen V1 wire, used only by complement policy 1. It is a
complete measurement partition with skew buckets, real and placebo rows mixed,
full slices, and `placebo_episodes.ndjson`. It is pinned byte for byte by
`replay/tests/fixtures/complement_v1_golden.json` (§11).

**Static metadata admission.** Before any stream callback or output `.open`
file/profile initialization, layout-2 construction runs
`replay.economic_sdk.entity_tables.preflight(strategy)`. It returns exact canonical
bytes per real/control `entities.json`, including wrappers, scope/row separators,
empty scopes, UTF-8 encoding and the final LF. Each table must fit the unchanged
8 MiB `MAX_METADATA` limit; an oversized table names itself, its exact required
bytes and the limit in a construction error. Preflight and final streaming commit
share the row/chunk encoder and resolve one scope at a time, without a new global
descriptor cache. The final writer also verifies preflight length before rename.
Layout 1 has no entity table and preserves the frozen complement V1 behavior.

A strategy may represent a statically rejected, size-independent basket with a
null size in its own descriptor and a deterministic numeric ordering sentinel.
Admission bypasses evaluation and declared input compilation; it still receives
one full scoped denominator. The strategy reader/summary must make this contract
explicit. Cross-venue format 2 uses null rejection rows; existing complement rows
and default numeric summary sorting are unchanged.

**Independent reader.** The layout-2 reader is `aggregate_reader`. It re-resolves
every entity from the manifest's policy and the snapshot, then checks the
following:

- The entity and reason tables round-trip exactly.
- Every scoped entity has one denominator row. Status time sums exactly to the
  scope length, class time sums to `DEPTH_SUFFICIENT` time, and reason time stays
  within its status.
- Episodes follow a closed schema, stay in bounds, follow close order and do not
  overlap. End reasons are consistent with scope and run ends.
- For every key and kind, the episode lifetimes per class equal the denominators'
  class time for that kind. So every episode lies inside `DEPTH_SUFFICIENT` time,
  and none is missing.
- Slices partition their episodes. Opening survival, tiers and `Q` recompute.
  Skew-attributed entry time sums to `Q`, and skew buckets match `leg_skew_ns`.
- With the audit:
  - the partition recomputes the denominators exactly;
  - its maximal predicate runs and their end reasons equal the episodes;
  - audit slices equal the compact slices;
  - first-slice values and quotes equal the episode's open.
- Strategy hooks check payloads and re-walk quotes.
- The summary and verdicts are recomputed from denominators and episodes.

The layout-1 reader (`reader`) keeps the V1 checks unchanged.

## 6. Controls

Controls are **opt-in**. The default policy has `controls: []`.

| Control | Construction | Measures |
|---|---|---|
| `time_shift` | The control leg is replaced by the same leg's committed view `Δ` earlier. There is one control per shift, named `time_shift_<Δ>`. | How often stale quotes alone produce a positive: the staleness null from the partition-sum spec §6. |
| `cyclic_neighbor` | The control leg comes from the next supported basket in the same peer group, in cyclic order. | A broken-pair diagnostic only. In a single-event bundle it pairs related markets, and it must not be read as a null. |

Isolation rules:

- An enabled control writes only under `controls/<name>/`. By default that is
  `entities.json`, `denominators.ndjson`, and `summary.json`.
- It adds `episodes.ndjson` with `controls_episodes: true`, and `slices.ndjson` with
  `controls_slices: true`, which requires episodes. Without `controls_episodes`,
  control entities track no episodes at all.
- Control rows never appear in the real files, and controls never feed verdicts.
- Controls use the same `evaluate` and the same views.

`time_shift` mechanics:

- The SDK keeps one ring of committed views per shifted book, across all scopes.
- A ring entry is recorded at its exact commit time, even when nothing is staged.
- Each change is re-evaluated by a timer at `τ + Δ`. Timers run in time order
  with scope boundaries, and a timer at a cut's time joins that cut's stage.
- Before any history exists at `t − Δ`, the shifted leg is a `not_initialized`
  view, so the control measures `UNUSABLE`.
- Ring memory charges each shared fill once, by reference count. Exceeding
  `time_shift_ring_entries` fails the attempt.
- Skew uses the live legs (§4).

## 7. Market profile

`replay.market_profile:build` runs beside any strategy and needs no fee
configuration. Its closed configuration is
`{version, snapshot_directory, snapshot_sha256, policy}`. It can instead run inside
an SDK strategy through `Requirements.profile`; complement policy 2 exposes this as
`"profile": null | <profile policy>`. It records no economic judgement.

**Gating.** `policy.groups` selects any of `activity`, `depth`,
`pair_consistency`, `quote_stability`, `self_crossing`, and `top_of_book`. `state`
is always on. A disabled group does no work; without `depth`, the collector reads
best quotes only.

**Profile policy** (closed):

- `version`;
- `bucket_ns`;
- `groups`;
- `sizes_contracts`;
- `tick_atoms`, a tick in price atoms for every planned venue;
- `depth_ticks`;
- `survival_edges_ns`.

**Files.**

- `profile.ndjson` has one row per (scope, book, bucket). Buckets are aligned to
  multiples of `bucket_ns` in visible time, then clipped to scope boundaries.
- `incidents.ndjson` holds self-crossing incidents.
- `pair_profile.ndjson` has one row per (scope, member pair, bucket).

Rows carry:

- the snapshot SHA, experiment SHA, bundle ID, scope and scope run ID;
- the native key, venue and member `market_id`;
- the scales and tick.

**Groups.** Every duration is in exact nanoseconds. Prices are integers in plan
price atoms. Means are exact time integrals (`*_ns`) plus their denominators, and
these integrals may exceed 64 bits.

| Group | Data points |
|---|---|
| state | `usable_ns`, `not_initialized_ns`, `unusable_ns` (with a breakdown by reason kind), `bid_empty_ns`, `ask_empty_ns`, `both_empty_ns`, `two_sided_ns` |
| top_of_book | Open and close quotes; an exact spread histogram, with time-weighted p50/p90 and the spread integral; the integral of bid + ask; top quantity integrals |
| depth | Per side: displayed quantity within each `depth_ticks` band (integral). Per size: filled and depth-limited time, and the integral of the absolute VWAP-versus-best cost |
| activity | Transitions, snapshots, operations, invalidations by reason; trades by coverage's dispositions; traded quantity; signed `2·price − (bid + ask)` against the prevailing book; trades without a mid; trades at mismatched scales; aggressor counts |
| quote_stability | Best-price survival histograms over fixed edges. A survival that started before scope entry, or was cut by scope end, counts as `censored` |
| self_crossing | `crossed_ns` and `locked_ns`. Incidents are maximal intervals with bid ≥ ask, recording start, end, maximum cross and the quotes at maximum |
| pair_consistency | For PM token pairs and Kalshi outcome/complement pairs: signed and absolute deviation integrals of the bid sum and ask sum from one unit, and time on the wrong side of one unit |

**Kalshi.** Asks are the counterpart's bids projected to `P − p`, with rows labelled
`ask_source: "projected"`. A projected ask has the counterpart bid's depth and
slippage, so it needs no extra walk.

**Speed.**

- Per (book, side), the collector keeps the best quote, depth bands, slippages, and
  a horizon. The horizon is the worst price any band or walked fill depends on.
- Only touched sides are recomputed. A touched price strictly beyond the horizon
  leaves a side unchanged.
- Embedded, the collector reuses the economic group's walked fills when they cover
  its sizes.

**Reader checks.** `profile_reader` checks:

- identities and bucket alignment;
- a complete per-book partition;
- the state partition;
- the histogram time, integral and quantiles;
- that crossed and locked time equals the histogram's negative and zero spreads;
- that incident time equals crossed plus locked time;
- the depth time partitions;
- the trade and aggressor partitions;
- close order.

## 8. Retained-state bounds

Recursive size accounting is replaced by **count-based bounds**.

- **Costs.** Costs are closed-form functions of shapes (`replay.economic_sdk.bounds`)
  for views, observations, episodes, fingerprints, ring entries, denominators,
  reasons and snapshot-shaped JSON. A unit test proves each formula is at least a
  real recursive `sys.getsizeof` traversal of representative maximal states.
- **Hard caps.**
  - 1,024 levels per consumed slice;
  - entities × kinds open episodes;
  - 100,000 skew changes per open slice;
  - ring entries per book, from policy.
  - the 128 MiB detached-state limit;
  - the file, line and row limits of the complement spec.

  Exceeding any cap fails the attempt. Nothing is truncated.

## 9. Performance

**Requirements** on the bench corpus, beside coverage:

- every economic and profile group finishes in at most 81.2 s, which is 1.2 × 55.2 s
  plus 15 s;
- complement policy 2 default output is under 5 MB;
- control output with `time_shift` aggregates is under 5 MB.

**Measured on the bench corpus** (bundle `bundle_e8a92effa246b9548571c907`, 2,008,580
stream entries). Each run is one supervisor attempt beside `bundle_coverage`, on the
commit that reads entity and reason tables as bounded single-record documents. The
table reader previously failed every layout-2 attempt at finish, because
`entities.json` (867,761 bytes) exceeded the 64 KiB row cap. In every run, coverage
reproduced semantic `87cb094d…` and intervals `41be6ad8…`.

| Run (beside coverage) | Attempt s | Group output | Semantic SHA | Requirement |
|---|---|---|---|---|
| Coverage alone (reference) | 55.2 | — | `87cb094d…` | — |
| Complement policy 1 compat | 156.8 | 372.7 MB (audit layout) | `2d85b833…`, identical to V1 | byte-identical ✔ |
| Complement policy 2 default | 65.7 | 1.08 MB (`entities.json` 0.87 MB, denominators 0.18 MB, no episodes) | `e4281dc4…` | ≤ 81.2 s ✔, < 5 MB ✔ |
| Complement policy 2 + `time_shift` (1 s, 5 s) | 120.0 | 3.53 MB total, of which controls 2.42 MB | `51a69b90…` | completes ✔, controls < 5 MB ✔, ≤ 81.2 s ✘ |
| Market profile | 54.7 | 7.49 MB | `6ffead22…` | ≤ 81.2 s ✔ |

Policy 2 results on this fixture:

- both venues `INTRA_INSTRUMENT_GAPS_ABSENT_IN_FIXTURE`;
- Polymarket `SELF_CROSSED_LEG` time 8,033,078 ns at sizes 1 and 100, equal to V1's
  nine real crossings;
- Kalshi time only `GROSS_NONPOSITIVE`.

The profiled runs took 151.5 s (complement policy 2, 98.8 s profiled) and 105.4 s
(market profile, 51.1 s profiled). Top cumulative costs:

- **Complement:**
  - `_select` 29.0 s;
  - `views.build` 20.8 s;
  - `_evaluate` 20.6 s (1.25 M calls);
  - `_fingerprint` 11.8 s (8.92 M calls);
  - `walk` 8.5 s.
- **Market profile:**
  - `profile.cut` 30.8 s;
  - `_side` 15.1 s;
  - `walk` 8.6 s.

**Measures, by cost centre** (corpus profile of 80aa64c):

| Cost centre | Cumulative s | Measure |
|---|---|---|
| `views.build` | 35.8 | Untouched and beyond-depth sides are reused, unread |
| `read_side` | 23.1 | Untouched and beyond-depth sides are reused, unread |
| `walk` | 18.8 | Untouched and beyond-depth sides are reused, unwalked |
| `_evaluate` (4.79 M calls) | 35.0 | Unchanged inputs are never staged, and staged matches skip `evaluate` |
| `_fingerprint` (9.59 M calls) | 9.0 | Flat identity tuples: no rebuilt structures, no equality calls |
| skew staging | — | Layout 2 reuses cached observations for skew-only changes inside positive episodes |
| profile `_derive`, `walk` | 44.1, 29.5 | Per-side facts, horizon skips, shared economic walks |

**Synthetic dense tape** (150-level books, 8 sizes from 1 to 1,000 contracts, about
2,500 changes/s). These are relative figures only:

| Run | µs per cut |
|---|---|
| Policy 1 compat | ≈330, as at 80aa64c |
| Policy 2 default | ≈250, from ≈370 before these changes |
| Policy 2 + `time_shift` | ≈440, from ≈700 |
| Standalone profile | ≈146, from ≈178 |

**Profiling harness.** `replay/economic_sdk/profiling.py` wraps a factory with
cProfile around callbacks only. It dumps periodically and at finish, outside the
output directory.

**Skew correction recheck (2026-10-05).** The same 2,008,580-entry bench corpus was
run in fresh direct-supervisor attempts beside coverage. Two Docker images copied
the pre-fix `07a50c2` replay sources and the corrected sources onto the same
`prediction-complement-v1-local` runtime image (`b4ad5fca3b0f`); the Rust publisher
was unchanged. Redis 8.2 was dedicated, disposable, limited to 150 MB, and used
`noeviction`. Progress was sampled once per second using bounded `HMGET` fields.
The fixture context and derivative directories were mounted read-only.

| Run | Before fix s | After fix s | After-fix group output | Result |
|---|---:|---:|---:|---|
| Coverage alone | — | 53.2 | — | Reference hashes preserved |
| Complement policy 2 default | 65.9 | 71.2 | 1,080,827 bytes | ≤ 81.2 s; < 5 MB |
| Complement policy 2 + `time_shift` aggregates | 130.5 | 131.7 | 3,531,842 bytes; controls 2,424,864 bytes | Completes; controls < 5 MB; > 81.2 s before and after |
| Market profile | — | 64.1 | 7,487,727 bytes | ≤ 81.2 s; reference identity preserved |
| Complement policy 1 compat | — | 174.4 | 372,725,852 bytes | Byte-identical V1 semantic output |

Every run reached terminal entry 2,008,580 and passed the strict completed readers.
Coverage retained semantic `87cb094d…` and intervals `41be6ad8…`; the economic and
profile semantic identities remained `e4281dc4…`, `51a69b90…`, `6ffead22…`, and
`2d85b833…`. Default and control output files were byte-identical before and after,
apart from the run-bound content receipt. The profile retained coverage's 1,617
trade observations and the nine crossed Polymarket market intervals (18 native
token incident rows); locked-only intervals are separate diagnostics.

The paired default attempts correspond to about 30,479 and 28,210 entries/s,
including setup and finalization. This is a single paired measurement, not an
isolated estimate of the skew bookkeeping cost. The economic group trailed
coverage in 62 of 63 default progress samples and 115 of 117 control samples.
The existing control speed-target miss remains visible; no acceptance limit was
changed.

**Callback profile of the correction.** The SDK's cProfile wrapper measured
callbacks and `finish`, excluding the decoder, Redis transport, and supervisor.
Profiled default attempts took 160.8 s before and 165.6 s after; these are
instrumented timings, separate from the ordinary attempts above. Cumulative
function times include children and overlap; they must not be added together.

| Default cost centre | Before cumulative s | After cumulative s | Calls after |
|---|---:|---:|---:|
| SDK `_select` | 30.36 | 31.05 | 598,278 |
| `ViewBuilder.build` | 22.20 | 22.23 | 598,296 |
| SDK `_evaluate` | 21.99 | 21.85 | 1,252,278 |
| SDK `_fingerprint` (inside selection and evaluation) | 12.28 | 12.54 | 8,920,784 |
| `walk` (inside view construction) | 8.93 | 8.98 | 272,261 |
| Complement `evaluate` (inside SDK evaluation) | 8.70 | 8.66 | 1,251,934 |

The counts of view builds, walks, fingerprints, and strategy evaluations were
identical. No `_stage_skew` call was needed by the default corpus; its targeted
positive-cache case is covered by the failing-before-fix regressions. The new
`_retain_staged` helper took 0.74 s of self time and 1.89 s cumulatively over
1,252,278 calls, but most of that work was previously inline in `_stage`.
`_begin_stage` took 0.06 s of self time over 183,444 calls. Shared selection and
view construction remain the largest independent cost centres; the measurements
do not attribute the entire ordinary-run 5.3 s difference to the correction.

The corrected time-shift attempt took 336.4 s with instrumentation. Its main
costs were:

| Control cost centre | Cumulative s | Self s | Calls |
|---|---:|---:|---:|
| SDK `_stage` (includes evaluation and retention) | 133.43 | 4.15 | 925,017 |
| SDK `_evaluate` | 107.98 | 12.90 | 10,703,160 |
| SDK `_advance` (includes timer-driven staging) | 87.96 | 2.46 | 2,008,578 |
| SDK `_select` | 71.49 | 20.02 | 598,278 |
| SDK `_fingerprint` (inside selection and evaluation) | 36.65 | 26.94 | 26,366,124 |
| Complement `evaluate` (inside SDK evaluation) | 35.39 | 12.32 | 4,159,031 |
| SDK `_views` (live and delayed-view lookup) | 26.55 | 12.98 | 26,366,124 |
| `ViewBuilder.build` | 22.26 | 3.40 | 598,296 |
| `walk` (inside view construction) | 8.79 | 4.75 | 272,261 |

Controls kept exactly the default's view-build and walk counts. The larger cost
is repeated dependency/fingerprint checks, delayed-view lookup, and timer-driven
staging in the shared SDK; 10.70 million SDK evaluation checks resulted in 4.16
million actual strategy evaluations. Fingerprinting had the largest production
function self time. `_stage_skew` was not invoked with aggregate-only controls.
All three profiled outputs retained their ordinary-run semantic identities;
their strict provisional content readers and supervisor success readers passed,
with the wrapped factory's inner configuration and receipt bindings checked
separately.

Focused Linux verification against disposable Redis ran 139 tests: 138 passed;
`test_waiting_publisher_with_progressing_consumer_is_not_a_stall` failed its
supervisor deadline check and also failed on the pre-fix image. The two new skew
regressions and V1 golden checks passed. The earlier host run passed 126 tests
with 13 Redis-dependent tests skipped. Full root, Rust, and deployment gates were
not rerun for this Python runtime correction.

## 10. Requirements for corpus runs

- Output depends only on pins, snapshot, policy, requirements and code revision.
- Retries reproduce identical semantic hashes, with distinct receipts.
- A completed reader works per bundle without network access.
- Manifest, entity tables and profile rows carry enough identity to concatenate
  results across bundles:
  - snapshot SHA;
  - experiment SHA;
  - bundle ID;
  - scope run ID.

## 11. Complement policies and acceptance

**Policy 1** (compat) uses layout 1:

- full slices;
- the `cyclic_neighbor` placebo beside real rows;
- skew as a measurement dimension;
- V1 reason strings, including the `b'…'` formatting.

It must reproduce the committed V1 output byte for byte (semantic `2d85b833…`).
Offline, seven synthetic multi-venue tapes pin every file. The tapes cover pairs,
placebos, slices, scopes, prologue cuts, Limitless and partial fees. Their hashes
were recorded from the pre-SDK code.

**Policy 2** is the SDK default and uses layout 2. It adds these fields to policy 1's:

| Field | Meaning (default) |
|---|---|
| `controls` | A list of `{kind: "time_shift", shift_ns: [..]}` and/or `{kind: "cyclic_neighbor"}`. Default `[]`. |
| `controls_episodes` | Write control episodes. Default `false`. |
| `controls_slices` | Write control slices; requires `controls_episodes`. Default `false`. |
| `audit_intervals` | Write the full audit partition. Default `false`. |
| `time_shift_ring_entries` | Ring bound per shifted book. |
| `profile` | `null` or a profile policy (§7). |

The schema is closed, so every field is required.

Policy 2 semantics:

- **`SELF_CROSSED_LEG`.** A pair basket has this status when a leg's own best bid is
  strictly above its best ask (the view's `crossed` flag). The check comes after
  `UNUSABLE` and before `ONE_SIDED`, and its reasons name the legs. Kalshi books
  carry no native asks, so the profile reports Kalshi projected crosses instead.
- **Reasons.** Reasons are structured:
  - unusable legs: `{leg, validity, kind}`;
  - self-crossed legs: `{leg, kind: "self_crossed"}`;
  - fee bridge reasons: `{kind: "fee", detail}`;
  - unsupported shape: `{kind: "book_count", value}`.

  `DEPTH_LIMITED` carries no reason, and measurement fields are empty.
- **Verdict refinement.** In verdict rule 3, `X` is the `FEE_UNKNOWN` time inside
  real gross slices whose survival reaches the headline latency, at the headline
  size. This comes from the episodes' `qualifying_class_ns`. The full unknown total
  is reported as `fee_unknown_total_ns`.
- **`SKEW_ARTIFACT_LIKELY`.** This verdict label is set when positive gross slice
  time at the headline size, bucketed by skew at slice open, exists and lies
  entirely in buckets whose lower edge is ≥ 1 s. It does not change the verdict.
- **Summary.** Rows are per (venue, basket kind, direction, size), with no skew
  dimension:
  - status and class time;
  - episode and slice counts;
  - `Q` and gross `Q`, overall and by skew bucket;
  - quantiles;
  - censored counts;
  - qualifying unknown time;
  - gross slice time by skew.

  Each control has its own summary under `summary.controls[<name>]`, also written
  to `controls/<name>/summary.json`.

**Bench acceptance** (run locally after push). Coverage must keep semantic
`87cb094d…` and intervals `41be6ad8…`.

| Run, beside coverage | Required |
|---|---|
| Complement policy 1 compat | Byte-identical, semantic `2d85b833…` |
| Complement policy 2 default | ≤ 81.2 s; complement output < 5 MB; 9 crossings as `SELF_CROSSED_LEG`; both verdicts absent |
| Complement policy 2 + `time_shift` (aggregates) | Completes; control output < 5 MB |
| Market profile | ≤ 81.2 s; coverage's trade disposition totals; the 9 Polymarket incidents; one-sided onsets at about 12.1 and 64.5 minutes |

**Offline tests.** Beyond the complement list, the offline tests cover:

- the V1 golden files and retries;
- shared views;
- one walk per touched side, and none beyond consumed depth;
- the unchanged-fill evaluate skip;
- predicate and class-map contradictions;
- the consumed-level cap;
- denominators summing to the scope length;
- episodes contained in, and exactly covering, positive time;
- a reader rejecting an episode outside positive time;
- entity and reason table round-trip and tamper rejection;
- compact slices with mid-slice skew attribution;
- the audit partition reproducing V1's partition without skew splits;
- control files appearing only when enabled;
- `time_shift` at exact nanoseconds, its ring bound, and history without staging;
- live-leg control skew that never splits time;
- cyclic controls;
- `SELF_CROSSED_LEG`, the verdict refinement, and the skew artifact label;
- conservativeness of the count-based bounds;
- the profiling harness;
- profile partitions, histograms, incidents, Kalshi projection, coverage trade
  parity, group gating, >64-bit integrals, and reader rejection.

## 12. Open items

Outcome masks are available through `outcome_scope` and context.json version 2;
see [OUTCOME_MASKS_V1.md](OUTCOME_MASKS_V1.md). Mask-dependent strategy manifests
must record `settlement_model: "normal_resolution_only"` and the outcomes provider.
Structural strategies remain a separate implementation.

- **Profile production values.** The bucket width and histogram edges are still to
  be chosen. Both are part of the profile policy hash.
- **Trade fields.** Confirm which venues fill `aggressor` in their normalized
  `TradeEvent` before relying on it.
- **Fee catalog.** A reviewed fee catalog and bindings remain the prerequisite for
  net economics (complement spec §12).
- **Coverage.** Porting `bundle_coverage` onto the SDK is optional, and it has not
  been done. If attempted, it must keep coverage's reference hashes.

## Native-scale strategy admission

`Strategy.native_scales` defaults to `False`. A strategy that handles native
price and quantity scales exactly may explicitly set it to `True`; `entities.resolve`
applies this opt-in to real baskets, substituted controls and independent reader
resolution. It changes no reconstruction scales. Complement policies keep their
existing admission and V1 bytes. The first consumer is
[CROSS_VENUE_ARBITRAGE_V1.md](CROSS_VENUE_ARBITRAGE_V1.md).

The reusable `FeeBridge.assess_orders` exposes per-leg native economics and
`FeeEngine.assess_many` assessments without basket accounting. The existing
`assess` wrapper retains complement accounting and its legacy order identities.
`FeeEconomicsUnavailable` preserves the existing ValueError text for a native
notional that cannot fit its pinned Fee SDK quote scale; the cross-venue strategy
records that as FEE_UNKNOWN without rounding.

## 13. Fill checks

Status: **implemented in the SDK; no production strategy uses it yet.** The
cross-venue strategy is not ported (that is a later step). The SDK is proven
offline by a synthetic two-leg strategy in
`replay/tests/test_economic_sdk_fills.py`. Fill mode is an explicit opt-in;
without it every existing output, including complement policies 1 and 2 and
the V1 goldens, is byte-identical.

**Model.** The strategy's pure `evaluate` stays the cheap **trigger**: from best
prices it decides whether a tradeable opportunity exists, through the
predicates of one declared episode kind. When that kind is active and the
entity has no live fill, the SDK prices **one fill** with
`replay.economic_fills.walk_basket` against the ladders the views hold at that
committed instant. The fill is never re-walked or re-sized. It is not priced
"throughout" the episode: that would assume either no fill or infinite depth.

- **Episode = fill lifetime.** A fill episode has exactly one slice; consumed
  quote changes never split it. Its opening survival is its duration, latency
  tiers are viable when the fill survived at least `L`, and `Q`, censoring and
  `SCOPE_END` keep their meaning.
- **Kill prices.** At open, each leg gets a kill price: the lowest integer price
  at which buying that leg's whole fill quantity at that single price, with the
  other legs unchanged and fees recomputed by the strategy's value function,
  leaves the value not positive. A binary search over `[0, 10^price_scale]`
  finds it (`replay.economic_fills.kill_price`); it is `null` when even the
  maximum price keeps the value positive. An unknown value (`None`) counts as
  not positive. Value must fall as a price rises; whatever it does, the result
  `k` satisfies "not positive at `k`, positive at `k − 1`", which is what the
  reader checks.
- **Ending.** On every committed update of a live fill's leg books, the SDK makes
  one comparison per leg: the fill ends (`end_reason: "KILL_PRICE"`) at the first
  update where a leg's best price on its walked ladder (asks, or Kalshi's
  projected asks) is at or above its kill price. This check runs even when
  `evaluate` is skipped because its input fingerprint is unchanged: an entity
  with a live fill, or with trigger-positive time and no fill, is staged on every
  change to one of its leg books. Every other book change "just eats into the
  fill" and is ignored. The fill also ends when the trigger turns false: the
  predicate goes off (`PREDICATE_FALSE`) or the status leaves `DEPTH_SUFFICIENT`
  (the status is the end reason, for example `UNUSABLE`). The trigger check runs
  first, so a fill whose trigger and kill price fail at one instant ends as
  trigger-false.
- **Reopening.** When a fill is killed and the trigger is still on, the SDK prices
  a new fill at that same committed instant, against the same final views. When
  it is tradeable, the next episode opens at that time; otherwise the time goes
  to a no-fill reason.
- **No fill.** When the trigger is on but the priced fill is not tradeable, no
  episode opens. Trigger-positive time is partitioned exactly into
  `FILL_LIVE`, `FILL_NONPOSITIVE` and `FILL_VALUE_UNKNOWN`. While the trigger
  stays on, a new fill is attempted at each later committed update whose
  retained ladders changed; identical ladders give the identical result, so
  they are not re-walked.

**Sizings.** Every configured sizing is priced by the one `walk_basket` call:
each fixed target (price `N` contracts and return what the book can serve),
then the edge walk (add depth while the marginal value is positive). Targets
and edge can be combined, and every sizing has the same result shape. A sizing
is **positive** when it took steps and its value is a known positive integer;
only positive sizings get kill prices. It is **tradeable** when, in addition, no
leg's best walked price is already at or above its kill price. The fill is live
when any sizing is tradeable. Its effective kill price per leg is the lowest over
its tradeable sizings, so the fill ends as soon as any tradeable sizing stops
being worth taking, and the next pricing decides what is still tradeable. The
no-fill reason is `FILL_VALUE_UNKNOWN` when any sizing that took steps has an
unknown value, and otherwise `FILL_NONPOSITIVE` (no steps, or a value of at most
zero).

**Timing.** A same-time stage is committed only when a greater time, a scope
boundary or the terminal arrives (§4), and every commit runs before the next
cut refreshes any view: `_flush_stage` is called on a later cut before its
books are re-read, in `_advance` before any boundary or timer at an earlier
time, and at the terminal. So `self.views` at commit is exactly the final state
at the staged time, and the fill is priced at commit, once per opened episode.
Same-time restaging therefore prices only the final state at that time,
deterministically, and a superseded intermediate is never priced.

### Opt-in surface

- **Policy** (closed, parsed by `replay.economic_sdk.fills.fill_policy(value,
  kind)` into `FillPolicy`):

  ```json
  {"version": 1, "targets_contracts": ["1", "10"], "edge": true,
   "step_contracts": "0.01", "max_levels": 64}
  ```

  Contract counts are canonical decimal strings. Each target must be a whole
  number of steps; targets are sorted and unique, with at most 8. At least one
  target or `edge` is required. `max_levels` (1 to 1,024) bounds the levels any
  leg may consume. A strategy embeds this object in its own closed policy.
- **Experiment.** `Experiment.fills` is the `FillPolicy`; its `kind` is the one
  declared episode kind whose predicate triggers fills. V1 supports a single
  fill kind per strategy. Other declared kinds keep ordinary quote-slice
  episodes. Fill mode requires layout 2.
- **Views.** `BookRequirement.ladders` names the sides or transforms whose full
  best-first ladder the view retains in `BookView.ladders`, from the one full
  side read it already makes (§3). `kalshi_complement_ask` retains the
  projection of the bids to asks at `P − p`, reused while the bid ladder is the
  identical object. A side with a retained ladder is never reused merely
  because a touched price lies beyond consumed depth, and equal ladders keep
  the prior object. Without `ladders`, views are unchanged.
- **Strategy hooks** (used by both the runtime and the reader):
  - `fill_spec(entity) -> FillSpec(sources, units)`: per leg, the ladder it buys
    from (`"ask"` or a transform projecting to asks) and its units, the atoms
    one basket step takes (the leg ratio times one step in that book's quantity
    atoms; `fills.step_atoms` converts exactly). It is static per entity, and
    each source must be a retained ladder of that leg's book.
  - `fill_value(entity, steps, legs) -> int | None`: the pure value of `steps`
    basket steps on per-leg `Fill` objects, including fees. It depends only on
    the entity and the fills, never on time, sequence or scope, because the
    reader recomputes it from the carried fill.

### Output (layout 2)

A fill episode row has the ordinary episode fields plus a closed `fill`
object, so consumers never need the book again:

- `sources`, `units`;
- `results`, one per sizing (targets in policy order, then edge), each with
  `mode` (`target` or `edge`), `target_steps` (`null` for edge), `steps`,
  `stop`, `value` (signed, or `null`), per-leg `legs`
  (`{atoms, cost, taken, consumed}`), `before`, `after` and `impact_ppm` per leg,
  `tradeable`, and `kill_prices` (per leg, or `null` for a sizing that is not
  positive);
- `kill_prices`: the effective per-leg kill prices;
- `kill_leg` and `kill_best`: the crossing leg and its best level at the
  crossing, present only when `end_reason` is `KILL_PRICE`.

`open.values` and `open.quotes` remain the trigger observation's values and
quotes at open; the single slice row is the compact slice of §5. Episode end
reasons add `KILL_PRICE`; trigger-false ends keep `PREDICATE_FALSE` or the
status; `SCOPE_END` and `RUN_END` (censored) are unchanged.

Denominator rows add `fill_ns`, a map from `FILL_LIVE`, `FILL_NONPOSITIVE` and
`FILL_VALUE_UNKNOWN` to exact nanoseconds. It is present exactly when the
trigger kind's positive-class time is nonzero, and sums to it exactly. Zero
entries are never written.

### Reader

`aggregate_reader` additionally checks:

- the closed `fill` object, its spec against the strategy's `fill_spec`, and the
  sizings and stops against the policy;
- each sizing from its own carried levels: taken prices equal consumed prices,
  strictly ascending, every level but the last fully taken, at most
  `max_levels`; taken quantity is `steps × units`; cost is `Σ p·q` of taken;
  `before` is the first consumed level; `after` is the remainder of a partly
  taken last level, or else `null` or a strictly worse level; `impact_ppm`
  recomputes from `before` and `after`;
- the value, through the strategy's `fill_value`, and each kill price by its
  defining property (two value calls per leg), and the tradeable flags and
  effective kill prices;
- that a `KILL_PRICE` end names a leg with a kill price and a best level at or
  above it;
- that fill episodes have exactly one slice; tiers and `Q` recompute as for any
  episode;
- that per class, fill episode lifetimes lie inside the trigger kind's class
  time, that their sum equals `fill_ns.FILL_LIVE`, and that `fill_ns` sums
  exactly to trigger-positive time;
- with the audit, that every fill episode lies inside one maximal trigger run,
  ending at the run's end with its trigger-false reason, or inside it with
  `KILL_PRICE`.

`fill_ns`, like every denominator, is writer-attested: the reader proves its
arithmetic and its agreement with the episodes, but cannot tell
`FILL_NONPOSITIVE` from `FILL_VALUE_UNKNOWN` time without the ladders.

### Bounds

Retained ladders count toward the view's levels, plus one container per
ladder (`bounds.view_cost`). A live fill is charged by `bounds.live_fill_cost`
(its `fill` object and kill prices) for its episode's lifetime. Each open fill
state is charged by `bounds.fill_state_cost`. A fill object that would not fit
the 64 KiB row line fails the attempt at open. The conservativeness tests cover
all three.

### Deferred and not implemented

- **Controls.** Any control (`time_shift` or `cyclic_neighbor`) with fill checks
  is rejected at construction. A time-shifted leg would need its historical
  ladders in the ring, and control kill checks would run on delayed views.
- **Sell-side fills.** Sources are buy-side only (`"ask"` and ask-projecting
  transforms), because the kill price assumes value falls as price rises.
- **Several fill kinds** per strategy.
- **Not wanted:** minimum order size or venue lot rules, trust or anchor checks
  (those live upstream), and display work.
- **Edge optimality** is writer-attested: the reader cannot re-walk depth beyond
  the levels a fill carries.
