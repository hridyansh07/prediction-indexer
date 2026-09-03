# Replay Engine V1

**Status:** design. Supersedes the `replay/gate1..gate5` ladder, which this
document retires rather than revises.

---

## 1. Purpose and boundary

The replay engine answers one question repeatedly and cheaply:

> Over a scoped interval of committed canonical evidence, where did a declared
> strategy see an opportunity, for how long did it survive, and was every book it
> priced usable at that instant?

It assumes canonical records exist. It does not produce them, validate the
capture that produced them, or reach across the byte boundary to fetch them.

**It is not:**

- a capture, ingester, or archive component;
- an authority on whether a window was captured well — seals, receipts, and
  continuity settle that upstream;
- a live trading system, though the engine half is built so one driver swap makes
  it one;
- a fill simulator (§8.2 — simulation is typed separately and deferred);
- a replacement for `analysis/` statistics, which consume its output.

### What it replaces

`replay/gate1..gate5` is retired. The ladder was linear — gate 5 constructs
gate 4 constructs gate 3 — so every question re-walked the tape from byte zero,
one gate's output could not feed several strategy runs, and the terminal verdict

| Retired | Survives as |
|---|---|
| Gate 1 capture checks | reconciler admissibility (§5.6) |
| Gate 2 mandatory trust evidence | book state + verification age on every episode (§6.4, §7.4) |
| Gate 3 economics | Python netting over episodes (§9) |
| Gate 4 depth survival | episode lifetime, intrinsic to detection (§6.8) |
| Gate 5 frozen policy | per-strategy precommitted thresholds (§6.6) |
| Gate 2 retrospective walk-back (`trust.py`) | Python demotion of episodes later proven wrong (§9.1) |
| Gate 3 matched placebo null | one of several Python nulls (§9) |
| Gate 5 leg-skew strata | `last_update_ns` per leg (§7.7), stratified in Python |
| Gate 5 resolution reconciliation, `catalog.py` condition identity | input to fungibility derivation (§12.5); `catalog.py` kept |

Deleted: `gate1..gate5`, `pipeline.py`, `output.py`, `economics.py`, `trust.py`,
`execution.py`, `books.py`. Retired once nothing else imports them, because the
Rust reconciler (§5) replaces what they did: `stream.py`, `order.py`, `lanes.py`,
`envelope.py`. Kept: `catalog.py`, which feeds the Python spec generator (§9),
and `events.py`, which stays as the **conformance oracle** for the Rust
normaliser (§5.10) and as the typer for game-state and reference-tick events that
other consumers read and the segment schema does not carry.

Three pieces of retired code port rather than die: the anchor walk-back in
`trust.py` becomes §9.1, the canonical-form logic in `books.py` seeds §12.9, and
the venue interpretation in `events.py` is ported line for line into the
reconciler with its tests as fixtures.

---

## 2. Governing principles

**2.1 State reconstruction and interrogation are different jobs.** Building the
book has one correct answer, is expensive, and is cacheable. Asking questions of
it has many answers, is cheap, and changes weekly. Every seam below preserves that
split.

**2.2 The segment re-expresses observation and never records judgement.** A
segment may filter, merge, and normalise — that is re-expression of what we saw,
and `RawEvent` keeps anything unparseable. It may **not** carry usable/unusable,
trust verdicts, anchor outcomes, or repositioned records. Those are derived, they
are ours rather than the venue's, and they belong in engine output. A tape that
carries our conclusions stops being evidence.

**2.3 A book is usable or it is not, and only an event can change that.** No
inference, no backfill, no reconciliation of a book we already doubt. §6.4.

**2.4 Rust owns what scales with tape size; Python owns what scales with episode
count.** A bundle-scoped segment is 10^5–10^6 events; a full capture day is ~10^7
(6.2M records/day for 20 Polymarket assets; 14.4M over 19 hours across three
venues). Episodes are 10^3–10^4. Rust is justified by the daily prebuild across
every scope, by analytics consumers that walk whole days, and by the live driver
(§12.8) that must run the same books and consumers against a socket — **not** by
any single segment being too large for Python. The honest reason is the one that
survives scrutiny.

**2.5 Adding a hypothesis is a config change, never a Rust change.** The capture
side already holds this for markets. Strategy *families* are code; strategy
*instances* are data (§6.6).

**2.6 A weaker claim is a different type, never a flag.** An episode depending on
assumed fills, or found on a spliced tape, is not a measurement with a caveat.
Flags get dropped in aggregation; types do not.

**2.7 One language per half, and the boundary is crossed once.** Everything from
receipt to episode is Rust. Everything after the episode is Python. A pipeline
that normalises in Python, evaluates in Rust, and judges in Python again carries
four representations of one event across two language hops with no compiler
watching either. The engine's typed schema (§7.2) is written and read by the same
crate, and the same normaliser that builds a segment is the one a live driver
(§12.8) will run against a socket.

---

## 3. The shape

```
  umbrella event ids  ──►  RESOLVER (Python, reads Universe)  ──►  scope.json
        │                                                        specs.json (§9)
        │            today: CLI  ·  later: Universe job dispatch (§12.1)
        ▼
  ┌───────────────────────────────────────────────────────────┐
  │  TAPE RECONCILER                                Rust      │
  │  receipts → verify → decode → normalise → filter → concat │
  └───────────────────────────┬───────────────────────────────┘
                              │  typed event stream, in-process
                              │  optionally materialised as
                              │  segment.ndjson + manifest.json  (§5.9)
        ┌─────────────────────▼─────────────────────┐
        │  ENGINE                          Rust     │
        │                                           │
        │   apply ──► route ──► evaluate            │
        │     │         │           │               │
        │  books    dispatch    consumers           │
        │  (state   index       strategies          │
        │   machine)            analytics           │
        └─────────────────────┬─────────────────────┘
                              │  episodes.ndjson
                              │  intervals.ndjson
                              │  analytics.ndjson
        ┌─────────────────────▼─────────────────────┐
        │  ECONOMICS + STATISTICS          Python   │
        │  exact fee netting · nulls · verdicts     │
        └───────────────────────────────────────────┘
```

### 3.1 Scope resolution

The reconciler never discovers anything. It is handed a **scope descriptor**
that names instruments and a window, and it filters the tape to them. The open
question was who writes that descriptor, and from what. There were two
candidates:

- the process walks targeter run archives itself, finds the bundle, and derives
  siblings across venues;
- a resolver queries the Universe server, which already is that derivation,
  persisted and queryable.

The first re-implements Universe's projection inside the reconciler, inherits
every retained-bundle and origin-decay problem
(`TARGETER_CONTINUITY_ORIGIN_V1.md` §1) a second time, and produces a scope that
cannot be reproduced later because it was computed from whatever runs were on
disk that day. It is rejected. **Universe is the only resolver source.** If a run
is not in Universe, Universe is synced first; the replay side does not read run
manifests.

**What the resolver needs from Universe.** Four lookups. Table names below are
the local schema's; the contract is the lookups, and the server under
implementation satisfies them the same way — an umbrella event id linked to every
venue event id, venue market id, and captured asset id beneath it, over every
targeter run combined.

| lookup | today |
|---|---|
| umbrella event → venue markets → **captured** asset ids per venue | `context_markets`, `context_targets`, `context_target_assets` |
| capture start, activation, and retirement instants | `bundle_contexts`, `bundle_retirements` |
| relationships between markets, with venue and coverage | `context_relationships` |
| a snapshot identity for the query | the `run_id`s and `context_sha256`s answered from |

The last is what makes a scope reproducible. Universe is re-synced and
backfilled; a descriptor that recorded only "the umbrella event" would resolve
differently next month. The descriptor records exactly which contexts answered.

**Why the resolver is Python.** It sits *before* the byte boundary, its output is
a few kilobytes of configuration rather than tape, and the spec generator (§9)
must run the same query over the same contexts to emit hypotheses. One Universe
client, in the language that already has one. §2.7 governs data that scales; a
descriptor does not. If a Rust client is ever wanted, `rusqlite` is already in
the workspace and the contract above does not change.

**The descriptor.**

```json
{
  "scope_version": 1,
  "umbrella_event_ids": ["…"],
  "universe": {
    "context_sha256": ["…"],
    "run_ids": ["…"],
    "resolved_at_ns": 0
  },
  "window": {
    "start_ns": 0,
    "end_ns": 0,
    "end_source": "retirement | explicit | latest_closed_window"
  },
  "instruments": [
    {
      "id": "polymarket:0x1f3a…",
      "venue": "polymarket",
      "venue_market_id": "…",
      "canonical_market_id": "…",
      "target_id": "…"
    }
  ],
  "uncaptured_markets": [
    { "venue": "kalshi", "venue_market_id": "…", "reason": "not_selected" }
  ]
}
```

- `instruments` is the filter (§5.3). `id` is the venue-qualified instrument the
  engine keys on (§7.1); the other fields are provenance so an episode can be
  read back to a market without another Universe query.
- **Lanes are not in the descriptor.** Canonical windows carry every lane, and
  the tape's own `connection_opened` records list the instruments each lane
  subscribed. The reconciler derives lane membership from those, which is what
  §5.3 needs for control-record retention. Universe does not know lanes and
  should not.
- **The window end is retirement, not activation.** The retired §12.1 text said
  `capture_start_at_ns → activation_at_ns`. That is the pre-match window only;
  for esports the in-play evidence is the point. `end_ns` comes from the bundle's
  retirement, from an explicit `--to`, or from the latest closed canonical window
  when the bundle is still live — and `end_source` records which. `start_ns` is
  the earliest `capture_start_at_ns` over the contexts; the reconciler adds the
  prologue (§5.5) itself.
- `uncaptured_markets` lists markets Universe knows for the event that have no
  captured asset. They are not in the filter; they are recorded so the
  observability gate can say "this relationship was never measurable" rather
  than "no episodes".
- **Strategy specs are a separate file** and are *not* part of the scope. The
  segment address (§5.7) hashes the scope alone, so one segment serves every
  hypothesis set over it. Specs carry the same `context_sha256` list so the two
  files are provably about the same contexts.

**Retained bundles.** A retained occurrence's context lacks markets and
relationships until origin propagation lands
(`TARGETER_CONTINUITY_ORIGIN_V1.md` §2). The resolver follows `origin_run_id` to
the origin context and takes markets, assets, and relationships from there; it
refuses, with the bundle id, if the chain does not terminate in a complete
occurrence. That spec is therefore a dependency of Phase 1.

---

## 4. Phases

**Phase 1 — Resolver, reconciler crate, and `build` command.** Independently
valuable without the engine: a typed, filtered, verified segment is something
Python can read today. The resolver (§3.1) is the small Python half and depends
on origin propagation for retained bundles. The expensive half, the half that stops changing once correct, and
the input to everything after. Ships when the Rust normaliser reproduces
`replay/events.py` on real canonical windows (§5.10) and Kalshi normalisation,
which neither side has yet, is in. The prebuild cron is **not** Phase 1: it waits
on retention and the scope index (§12.2), which are its entry ticket.

**Phase 2 — Engine core.** Books as a state machine, dispatch, one strategy
family (partition-sum), measurement episodes only. Includes Polymarket hash
reproduction (§12.9): the bootstrap protocol in `Architecture_refinments.md` §2
and the canonical-form code in `replay/books.py` are the starting point.

**Phase 3 — Economics and statistics.** Exact netting, nulls, per-hypothesis
verdicts over emitted episodes.

**Phase 4 — Families.** Implication-bound, then cross-venue behind the
fungibility gate (§12.5).

---

## 5. The tape reconciler

### 5.1 Job

One scope's evidence, verified, normalised, filtered, totally ordered,
self-contained, as a typed event stream the engine consumes in-process. When
materialised (§5.9) it is NDJSON: it streams and appends without an index, it is
inspectable with a pager, and Python can read it for ad hoc work without a
binding. The format is a persistence format, not a language boundary — there is
no language boundary here any more (§2.7).

### 5.2 Stages

The input is **canonical windows**, never raw lane segments. A canonical window
is already receipted, already verified at finalization, already merged across
lanes on `(visible_ns, lane_rank, delivery_index)`, and already
continuity-classified in its provenance index. The reconciler inherits all of
that instead of redoing it.

```
 0  stage       archived windows → local canonical root           Python: ArchivedCanonicalByteStreamer
                (only when the local root has been reaped)         (§3.1 resolve --stage)
 1  select      canonical receipts covering [T0 − prologue, T1)   finalize::committed_windows
 2  verify      stored + decoded identity; provenance binding     finalize::audit::audit_window
                to receipt inputs; canonical_seq continuity
 3  decode      Zstandard through the strict decoder              prediction-encoder::StreamingDecoder
 4  parse       envelope lines, closed and versioned              indexer-types::EnvelopeView
 5  normalise   raw_payload → SegmentEvent (§7.2)                 new: tape::normalise
 6  filter      in-scope instruments + control for their lanes    new
 7  concat      windows in order; within a window, canonical      inherited from the finalizer
                order; within a delivery, venue array order
 8  emit        in-process stream, or segment.ndjson + manifest   new
```

**Stages 1–4 already exist as one function.** `finalize::audit::audit_window`
selects a committed window, opens evidence and provenance through the strict
decoder, verifies stored and decoded identity against the receipt, checks
`canonical_seq` continuity line by line, and binds every provenance line to a
receipt input by lane and source digest. That loop *is* the reconciler's read
loop; stage 5 is inserted into it where the audit currently discards the
envelope. The reconciler is therefore the audit with a normaliser and a filter
added, not a new reader. The receipt and identity types currently live in
`finalize`; they move to `indexer-types` so the engine depends on the types
crate and the encoder, never on the finalizer.

**Stage 0 is Python and optional.** The canonical reaper removes local windows
about eighteen hours after they are archived, so the local root is a rolling
window. For anything older, `ArchivedCanonicalByteStreamer` already fetches a
canonical window from S3 or GCS, verifies it against its canonical archive
receipt, and yields exact bytes. The resolve step stages those bytes into a
local root laid out exactly as the finalizer writes one, and Rust re-verifies on
read. Staging sits before the byte boundary with the resolver (§3.1) and is
configuration-shaped work over a handful of objects, so §2.7 is not offended.

**There is no k-way merge and no dedupe stage.** Both existed because the
retired Python replay read raw lane segments. Canonical windows are time-disjoint
and internally ordered, so stage 7 is concatenation. Overlapping window ranges are
rejected at stage 1 rather than deduplicated later. A `late_after_finalization`
correction dataset (`SEALED_CAPTURE_PIPELINE_V1.md` §5) is a different canonical
lineage with its own receipts and is selected as such, never merged in.

**Stage 5 is a port, not a design.** `replay/events.py` already interprets
Polymarket and Limitless payloads and its tests are the fixtures. It is ported
line for line and must pass conformance (§5.10) before anything reads its
output. Kalshi normalisation — `orderbook_snapshot`, `orderbook_delta`, `trade`,
and the `update_range` control — exists on neither side and is written once,
here. Kalshi carries a material share of moneyline volume on the esports bundles
this engine exists to measure, so it is not optional.

**Why normalisation is a reconciler stage and not part of the engine walk.** It is
paid **once per segment** rather than once per run, and it lets the filter be
exact rather than approximate because the builder knows the instrument. The
engine then consumes a closed typed schema and never sees venue JSON.

**Why it is not in the finalizer.** The finalizer is payload-agnostic by
invariant (`AGENTS.md` §2): a canonical object's identity is the hash of exact
envelope bytes, and normalising at capture makes a schema misreading permanent.
`CAPTURE_SPEC.md` §11.3 records the case that settled this. The normaliser lives
under `engine/crates`, reads only committed canonical objects, and its output is
never written into a canonical file. The engine may depend on ingester crates;
the ingester never depends on the engine.

**Twin lanes.** `record_id` is per-splice, so if that experiment
(`Architecture_refinments.md` §5) ever runs, both records reach the engine.
Cross-lane arbitration is a consumer (§6.6), not a reconciler stage.

### 5.3 What the filter keeps

- every event for an in-scope instrument;
- **every control record for any lane carrying an in-scope instrument**, whether
  or not the control record names an instrument. A `connection_failed` that names
  nothing is exactly the record the engine needs;
- `RawEvent` for in-scope instruments, so unparseable entries stay visible.

Out-of-scope records are excluded and the exclusion is **counted in the
manifest**. Lossless with respect to scope is the honest claim; lossless
absolutely is not.

### 5.4 Ordering

Inherited from the finalizer: within a window, `(visible_ns, lane_rank,
delivery_index)`; across windows, window order; within a delivery, the venue's
array order. The reconciler re-derives none of it.

> Segment order is a **serialization** order. `visible_ns` carries real timing;
> no lead-lag conclusion may rest on segment position, and none may rest on
> `event_index` within a delivery either — intra-frame order is the venue's array
> order, not our clock (§6.2).

### 5.5 The prologue

Bootstrap and recovery are the same operation (§6.4), so segment start needs no
special mechanism. The reconciler simply **reads early enough that a full book
lands before `T0`** for every in-scope instrument:

```
prologue_ns = 2 × max(snapshot cadence over venues in scope)
```

The manifest records `window_start_ns`. The engine compares `order_ns` against it
and suppresses episode emission before it. **No per-record annotation** — that
would be judgement on the tape (§2.2).

### 5.6 Admissibility

Fatal for the segment:

1. any object failed verification against its receipt;
2. the window is not closed — no segment is built over an interval still being
   captured.

Fatal **per instrument**, which refuses that instrument's legs rather than the
segment:

3. no full book anywhere in prologue + window, so the book can never leave
   `NotBootstrapped`.

Recorded in the manifest, not fatal:

4. `bootstrap_offset_ns` per instrument — how far into the segment its first
   usable book appears. Normally near zero thanks to the prologue; when it is
   not, the loss is visible instead of silent.
5. delivery-index discontinuities per lane, read from the provenance index's
   continuity verdicts and passed to the engine as an `UnusableCause` at that
   position.
6. an `incomplete` receipt — a lane missing or invalid for the whole window. The
   window is admitted; every instrument on the faulted lane is passed to the
   engine as `SegmentDiscontinuity` spanning the window. This is what stops one
   wedged splice from hiding the other venues' evidence, and what stops it from
   being mistaken for a quiet market.

### 5.7 Identity

```
segment_address = sha256(
    scope_descriptor_canonical_bytes,
    input_object_manifest,        # every canonical receipt + object sha256 + length
    reconciler_version            # one crate; covers normalisation
)
```

Computed and recorded on every run even when the segment is discarded, so results
join to segments retroactively once caching is on. Every episode carries it.
Strategy specs are not in the address (§3.1): one segment serves every hypothesis
set over its scope.

### 5.8 Build order: one pass per window, fanned out to every scope

The filter runs *after* decode, so building one scope costs the full decode of
every canonical window in its time range, all lanes, whether the scope touches
two instruments or two hundred. At 100–150 bundles a day, most of them
overlapping in time, building scopes one at a time decodes the same windows over
and over. The unit of work is therefore the **window**, not the scope:

```
for each canonical window W in the day, on its own thread:
    decode · parse · normalise W once
    for each event e in W:
        for each scope S with e.instrument ∈ S.instruments,
        or e is control and S has an instrument on e's lane:
            append e to slice(W, S)
for each scope S:
    concatenate slice(W, S) over W in window order  →  S.segment
```

Daily prebuild cost is then one decode of the day's canonical data regardless
of scope count; the per-scope cost is a hash lookup and a write. Sealed windows
are time-disjoint and internally ordered, so slices concatenate without
re-interleaving. Never re-interleave across a window boundary by timestamp.
`run --scope` for a single on-demand question is the degenerate case with one
scope and is allowed to be slow.

### 5.9 Persistence

A segment is immutable once built. Its address is a function of the scope, the
canonical inputs, and the reconciler version; none of those can change under
it. It is also expensive to reproduce for the reason §5.8 gives. So a **final**
segment is kept, not rebuilt, and kept the same way everything else here is
kept: published to object storage under its own prefix with its own receipt,
and evicted locally only once that receipt verifies.

**Which segments are final.** Those whose scope has `end_source` of
`retirement` or `explicit` (§3.1). A scope ending at `latest_closed_window` is a
live bundle's moving target; its segment is local only, superseded by the next
build with a later end, and evicted without ceremony.

**The segment archive receipt** is a third receipt kind beside the raw and
canonical ones and carries: `segment_address`, the scope digest, the list of
canonical receipts consumed, `reconciler_version`, and stored and decoded
identity for `segment.ndjson.zst` and `manifest.json`. `publish_files`,
`put_immutable`, the receipt parsers, and `verify_object` are reused as they
are; the object key is `segments/<address>/`. `run --segment` accepts an address
and resolves local first, archive second, verifying either.

**Local eviction** is the reaper's dual-receipt rule with the segment receipt
standing where the canonical receipt stood: a local segment is deletable when its
archive receipt verifies against the store and a byte budget on the volume says
so. Live-bundle segments have no archive receipt and are evicted by age.

**A reconciler version bump** changes every address. Old segments are not
rewritten and not deleted on the bump: the results ledger references them by
address, and a verdict must stay traceable to the segment it was measured on.
They are reaped from the archive only when no ledger row references them.

**Sizing, to be replaced by measurement on the first real build:**

| quantity | estimate |
|---|---|
| events per bundle-scoped segment | 10^5 – 10^6 |
| bytes per typed event as NDJSON | ~250 |
| segment, uncompressed | 25 – 250 MB |
| segment, Zstandard | 3 – 25 MB |
| archive growth at 150 final scopes/day | 0.5 – 4 GB/day |
| canonical decode to build one six-hour scope alone | 1 – 2 GB uncompressed, all lanes |

The last row is why §5.8 fans out. The fourth is why the local volume cannot hold
history and object storage must.

The in-process path is unchanged: `run --scope` builds the typed stream and
evaluates it without writing, and `build` is explicit until §12.2 lands.

### 5.10 Conformance

The Rust normaliser ships only when, over real canonical windows spanning every
venue and stream on tape, it emits the same typed events as `replay/events.py`,
in the same order, field for field. `events.py`'s existing tests are the fixture
set and are extended with the windows used. Differences are resolved by deciding
which side is right and recording why; the Python side is the oracle, not the
authority. After conformance, Kalshi is added on the Rust side alone, with its own
fixtures, and `events.py` is not extended further.

---

## 6. The engine

### 6.1 Crate layout

```
engine/crates/
  tape       reconciler: receipt selection, verification, decode,
             normalise, filter, manifest; segment writer and reader   (§5)
  book       book state machine, UsableBook receipts
  price      ladder walk, combination arithmetic, fee envelope
  consume    dispatch index, consumer trait, episode lifecycle
  families   strategy families
  cli        replay-engine  (build · run)

shared from the ingester workspace, never the reverse:
  indexer-types        EnvelopeView, identities, receipt and identity types
  prediction-encoder   strict Zstandard
```

`book` and `price` know nothing about strategies. Nothing outside `book` can
construct a `UsableBook`. Nothing outside `tape` sees venue JSON.

### 6.2 The walk

Single pass, **per event**, strictly sequential.

```
   apply     mutate book / state machine       (write)
   route     dispatch index → interested set   (read)
   evaluate  consumers over routed set         (read-only)
```

Evaluation is per *event*, not per delivery. A frame carrying three events
produces three evaluation points, so **every episode boundary is attributable to
exactly one record**. The cost — a transient intermediate state may briefly
qualify — is self-limiting, because such an episode has a near-zero lifetime and
sorts to the bottom of the distribution that already ranks episodes by duration.

**Except where the venue defines a larger atomic unit.** Polymarket repeats one
`state_hash` across several `price_change` entries in one logical update; the
intermediate states are serialisation artifacts the venue never held. Evaluating
on them can attribute a real episode's *open* one entry early, which no duration
ranking corrects. The engine therefore **applies a full hash run before
evaluating**, and attributes the boundary to the last record of the run. This is
also what makes §12.9 well-defined: the hash a delta carries describes the book
after the whole run, not after that entry. Venues without such a marker evaluate
per event.

There is **no heartbeat and no synthetic record**. Staleness and timeout closures
are applied **backdated** at the next evaluation: the tracker knows each leg's
`last_update_ns`, so an episode closes at when staleness bit, not when it was
noticed. In replay, emission time is irrelevant; only the recorded interval is
real. A `finalize` pass at segment end closes and right-censors whatever remains
open.

### 6.3 Events survived

Per-event evaluation yields a second lifetime measure for free. An episode carries
both `duration_ns` and `events_survived`. One that survived 40 events is a
different animal from one that survived 1 at the same wall duration.

### 6.4 The book state machine

```
  NotBootstrapped ──full book──► Usable ──divergence──► Unusable
                                    ▲                       │
                                    └──────full book────────┘
```

- Deltas apply **only** in `Usable`. In `Unusable` they are dropped: they act on a
  book we do not trust and we will hard-reset anyway.
- Recovery requires a **full book**, never a hash match — a frozen book can never
  re-converge on its own.
- Missed deltas are never recovered. We only ever bound when trust was lost and
  regained.
- Bootstrap and recovery are the same transition, so segment start is not a
  special case.

**The rule that is easy to get backwards:** a snapshot **verifies** while `Usable`
and **resets** only while `Unusable`. A Polymarket poll is ~955 ms stale on
arrival; using it to reset a healthy book would drag every PM book backwards on a
60-second timer.

**Recovery is marked at reception, never at the snapshot's source time.** We do
not know exactly when the book became correct; understating trusted time is the
only direction that cannot manufacture a false opportunity.

**Causal usability is weaker than the retired retrospective audit, by design.**
`trust.py` walked distrust back to the last proven point when an anchor
mismatched. The engine cannot, because a live driver cannot. Episodes emitted in
a blind window are therefore *kept by the engine* and **demoted in Python** once
the interval and verification records show the book was later proven wrong
(§9.1). The engine stays causal; the replay verdict keeps the audit.

### 6.5 Recovery latency is per-venue and asymmetric

| Venue | In-band gap detection | Recovery source | Typical blind window |
|---|---|---|---|
| Limitless | n/a — every message is a full book | every message | ~one message |
| Kalshi | `update_range` — immediate | 30s sweep + subscribe | ≤ 30s |
| Polymarket | **none** | 60s poller | ~60s, **modal not tail** |

Polymarket is uniquely exposed: no sequence numbers, so divergence is detected
*only* at a poll and recovered *only* at the next poll. Both intervals are ~60s,
which means a book can be silently wrong for up to a poll interval **before** we
know, and episodes are emitted on it during that time.

The response is annotation in the engine and demotion in Python (§9.1), never
retraction of the tape: every episode carries `verification_age_ns` per leg, so a
consumer can require recent confirmation without the engine buffering or
rewriting anything, and the Python side re-types what hindsight later condemns.

§12.9 proposes the investigation that would collapse this.

### 6.6 Consumers

Strategies and analytics are the same interface. Both register interests, both are
routed by the dispatch index, both emit to sinks. Analytics is not a pipeline
stage — it is a consumer on the same single walk.

The dispatch index is keyed by instrument and is **many-to-many**: the instrument
is the key, not the scope of a consumer. A cross-venue strategy watching five
instruments registers on all five; one delta routes to every consumer watching
that instrument. The alternative is broadcast, which is the O(events × consumers)
loop this exists to kill.

**Legs are candidate sets.** A leg is not one instrument. If YES is unusable on
venue A but usable on venue B, and NO is usable on B, the basket is still
priceable.

- dispatch registers the strategy on **every** candidate;
- the episode records **which instrument was actually used** per leg;
- when several candidates are usable, **best price wins** — and that selection is
  the "which venue held the most competitive odds" analytic, falling out of
  evaluation rather than needing a pass;
- substitution requires `IDENTITY`. A `NEAR_IDENTITY` candidate standing in
  silently converts a locked basket into a directional one.

Families are code, instances are data. `thresholds` carries the precommitment that
used to live in one frozen `policy.json`, is hashed with the spec before the walk,
and lands on every episode. One verdict per hypothesis, not per run.

Three families come from machinery that already exists in `analysis/masks.py` and
`targeter/v2/relationships.py` and that Universe already persists per bundle in
`context_relationships`: PARTITION → Σ tests, MUTUAL_EXCLUSION → Σ ≤ 1,
IMPLICATION → `ask_A < bid_B`. The retired pipeline could express none of them
beyond the trivial 2-partition.

### 6.7 Evaluation states

**No open episode.** Maintain the combination incrementally — reprice only the leg
whose instrument the event touched, update Σ, test the threshold. Never recompute
the basket from scratch.

**Open episode.** Apply the fingerprint guard before repricing: *did this event
touch at-or-inside this leg's priced slice, or improve its best?* A fingerprint is
per episode, per leg — a three-leg episode holds three priced slices.

Top-of-book triggering is wrong for these venues in both directions: tickets walk
depth, so a size change three levels down moves VWAP while L1 sits still, and
these venues hold spreads wide precisely so size cannot be absorbed at the top.

### 6.8 Episodes

An opportunity is an interval. Per-revision emission would report a basket sitting
qualifying for 800 ms across 40 events as 40 opportunities and make every rate
statistic meaningless.

Three rules that decide whether the lifetime number means anything:

- **Close on "can no longer fill the ticket at the qualifying gap,"** not on "a
  book changed." Longer episode means easier execution; this is the headline
  execution metric and it dissolves the retired Gate 4 into detection.
- **Carry aggregates, not the opening value.** The gap frequently improves
  mid-episode.
- **Right-censor.** Still open at segment end is censored, not short.
- **Backdate every close.** Staleness and `Unusable` closures are recorded at the
  moment the condition began, not when it was observed. Closing at detection would
  inflate the lifetime preceding every corruption event — biasing the headline
  metric upward, which is the direction that flatters the hypothesis.

Each size in the ladder is its own episode series. A gap qualifying at 10
contracts and not at 100 is two different facts.

### 6.9 The fee envelope

Two numbers, so the language boundary stays where it belongs.

**Trigger envelope — Rust, data.** Python's catalog side precomputes a
conservative *upper bound* of the fee curve over the relevant price region and
puts it in the spec as one number. The engine triggers on `gross_gap − envelope`
and knows no fee model, no bonding curve, no time-effectivity.

**Exact netting — Python, per episode.** Full curve, time-effective schedules,
per-leg conservative rounding, over emitted episodes only.

The receipt then guarantees exactly what Rust can honestly guarantee: **usable,
gross-priced, envelope-cleared.** Python owns the economic verdict. A too-tight
envelope silently loses real episodes, so it is conservative by construction and
recorded on every episode.

---

## 7. The type boundary

The part to converge on before implementation.

### 7.1 Identity and money

```rust
pub struct InstrumentId(Arc<str>);   // venue-qualified: "polymarket:0x1f3a…"
pub struct LaneId(Arc<str>);
pub struct StrategyId(u32);

/// Price in venue ticks. Scale is per-venue and carried in the manifest.
pub struct Px(i64);
/// Size in venue lot units.
pub struct Qty(i64);
pub struct Contracts(i64);

pub enum Side { Bid, Ask }
```

Representation and per-venue scale are deferred (§12.6). The newtypes exist from
the first commit so the decision is one file, not a refactor.

### 7.2 Segment events

A closed schema, defined once in `tape` and written and read by it. The reader
rejects unknown variants rather than skipping them — same discipline as the
envelope parser — which matters less now that writer and reader share the enum,
and still matters for a segment file built by an older reconciler.

```rust
pub struct EventAddress {
    pub lane: LaneId,
    pub delivery_index: u64,
    pub event_index: u32,
}

pub struct EventHeader {
    pub order_ns: i64,
    pub visible_ns: i64,
    pub addr: EventAddress,
    pub record_id: Arc<str>,
}

pub enum SegmentEvent {
    Control(ControlEvent),
    Book(BookEvent),
    Trade(TradeEvent),
    Raw { name: Arc<str>, payload: Box<RawValue> },
}

pub enum BookEvent {
    Full {
        instrument: InstrumentId,
        bids: Vec<(Px, Qty)>,
        asks: Vec<(Px, Qty)>,
        state_hash: Option<Arc<str>>,
        independent: bool,          // polled, versus in-stream
    },
    Delta {
        instrument: InstrumentId,
        side: Side,
        price: Px,
        size: LevelSize,
        state_hash: Option<Arc<str>>,
    },
}

/// Venues disagree on what a delta carries. Polymarket `price_change` sends the
/// new absolute size at the level; Kalshi `orderbook_delta` sends a signed
/// change. Converting one to the other needs book state, which the reconciler
/// does not hold (§2.2), so the segment carries what the venue said and the
/// engine applies it.
pub enum LevelSize {
    Absolute(Qty),                  // 0 deletes the level
    Relative(Qty),                  // signed; the level deletes at 0
}

pub enum ControlEvent {
    ConnectionOpened { epoch: Arc<str>, instruments: Vec<InstrumentId>,
                       delivers_deltas: bool, target_digest: Option<Arc<str>> },
    ConnectionClosed { epoch: Arc<str> },
    ConnectionFailed { epoch: Arc<str>, reason: Arc<str> },
    SubscriptionChanged { from: Option<Arc<str>>, to: Arc<str> },
    MetadataChanged { from: Option<Arc<str>>, to: Arc<str> },
}
```

`Absolute` application is idempotent; `Relative` is not, which is one more reason
a book fed by relative deltas can never re-converge after a gap without a full
book (§6.4). Kalshi's delta semantics are taken from its published spec and are
unverified against live servers (§12.7); the variant exists so that verifying
them is a reconciler change, not a schema change.

### 7.3 The book and its receipt

```rust
pub enum BookState {
    NotBootstrapped,
    Usable   { since_ns: i64, last_verified_ns: Option<i64> },
    Unusable { since_ns: i64, cause: UnusableCause },
}

pub enum UnusableCause {
    AnchorMismatch,          // poll disagreed with our reconstruction
    AnchorConflict,          // two polls, same hash, different contents
    EpochReset,              // reconnect; prior book cannot carry over
    LaneInterrupted,         // connection_failed covering this instrument
    SegmentDiscontinuity,    // manifest-declared delivery gap
    Stale { threshold_ns: i64 },
}

/// The receipt. No public constructor: only `BookStore::usable` mints one.
pub struct UsableBook<'a> { /* private */ }

impl<'a> UsableBook<'a> {
    pub fn instrument(&self) -> &InstrumentId;
    pub fn levels(&self, side: Side) -> &[(Px, Qty)];
    pub fn usable_since_ns(&self) -> i64;
    /// None when never positively confirmed since bootstrap.
    pub fn verification_age_ns(&self) -> Option<i64>;
}

pub struct BookStore { /* … */ }

impl BookStore {
    pub fn apply(&mut self, hdr: &EventHeader, ev: &SegmentEvent) -> Touched;
    pub fn usable(&self, id: &InstrumentId, at_ns: i64) -> Option<UsableBook<'_>>;
    /// Always available, including when `usable` returns None.
    pub fn state(&self, id: &InstrumentId) -> &BookState;
}
```

### 7.4 Pricing requires the receipt

```rust
pub fn walk_ladder(book: &UsableBook<'_>, side: Side, size: Contracts) -> Fill;
```

This one signature is the enforcement point. "Economics ran on a book we did not
trust" is not a bug to be tested for; it does not compile.

### 7.5 Consumers

```rust
pub struct Touched(SmallVec<[InstrumentId; 4]>);

pub struct Ctx<'a> {
    pub books: &'a BookStore,
    pub now_ns: i64,
    pub window_start_ns: i64,
    pub segment_address: &'a str,
}

pub trait Consumer {
    fn interests(&self) -> &[InstrumentId];
    fn on_event(&mut self, ctx: &Ctx<'_>, hdr: &EventHeader,
                ev: &SegmentEvent, out: &mut dyn Sink);
    fn finalize(&mut self, ctx: &Ctx<'_>, out: &mut dyn Sink);
}

pub struct DispatchIndex {
    by_instrument: HashMap<InstrumentId, SmallVec<[ConsumerId; 4]>>,
}
```

### 7.6 Specs

```rust
pub struct StrategySpec {
    pub id: StrategyId,
    pub family: Family,
    pub legs: Vec<Leg>,
    pub sizes: Vec<Contracts>,
    pub thresholds: Thresholds,
    pub fee_envelope: Px,
    pub spec_sha256: [u8; 32],
}

pub struct Leg {
    pub candidates: Vec<InstrumentId>,   // substitutable, IDENTITY only
    pub side: Side,
    pub weight: i8,
    pub fungibility: FungibilityTier,
}

pub enum Family { PartitionSum, ImplicationBound, MutualExclusion, Optimizer }
pub enum FungibilityTier { Identity, NearIdentity }
```

### 7.7 Episodes

```rust
pub enum EpisodeKind {
    Measurement,
    Simulation { fill_model: &'static str },
}

pub enum CloseReason {
    NoLongerQualifies, DepthInsufficient,
    LegUnusable { cause: UnusableCause },
    LegStale, SegmentEnd,
}

pub struct LegFill {
    pub instrument: InstrumentId,        // which candidate was chosen
    pub side: Side,
    pub fill: Fill,
    pub verification_age_ns: Option<i64>,
    pub book_usable_since_ns: i64,
    pub last_update_ns: i64,             // the retired leg-skew stratum, per leg
}

pub struct Episode {
    pub kind: EpisodeKind,
    pub strategy_id: StrategyId,
    pub spec_sha256: [u8; 32],
    pub segment_address: Arc<str>,

    pub opened_ns: i64,
    pub closed_ns: i64,
    pub opened_at: EventAddress,
    pub closed_at: EventAddress,
    pub events_survived: u32,
    pub right_censored: bool,
    pub close_reason: CloseReason,

    pub size: Contracts,
    pub legs: Vec<LegFill>,
    pub gross_gap: Aggregates,           // min / median / max over the episode
    pub min_fillable: Qty,
    pub fee_envelope: Px,
}
```

### 7.8 Interval records

`Unusable` intervals are engine **output**, never tape (§2.2).

```rust
pub struct UnusableInterval {
    pub instrument: InstrumentId,
    pub start_ns: i64,
    pub end_ns: i64,
    pub right_censored: bool,
    pub cause: UnusableCause,
}
```

Emitted so that "no episodes here" is never ambiguous between *no edge existed*
and *we were blind*. Those have opposite implications for whether to keep going.

Every anchor comparison against a `Usable` book is emitted too. Positive results
matter as much as negative ones: §9.1 needs the last instant a book was proven
right, and `verification_age_ns` on an episode only looks backwards.

```rust
pub struct VerificationRecord {
    pub instrument: InstrumentId,
    pub at_ns: i64,
    pub matched: bool,
}
```

### 7.9 CLI

```
python -m replay.scope resolve                    (§3.1, Python)
    --universe   URL|PATH    Universe server or its SQLite file
    --event      ID …        umbrella event ids
    [--to        INSTANT]    explicit window end; default: retirement
    --scope-out  PATH        scope.json
    --specs-out  PATH        specs.json  (§9 generator, same contexts)
    [--stage     DIR]        fetch reaped windows for the scope into a local
                             canonical root via the archive streamer (§5.2 stage 0)

replay-engine build
    --scope      PATH …      one or more scope descriptors; one decode pass
                             per window, fanned out to all of them (§5.8)
    --canonical  PATH        local canonical root (receipts + objects)
    --out        DIR         <address>/segment.ndjson.zst + manifest.json
    [--publish]              archive final segments with receipts (§5.9)

replay-engine run
    --scope      PATH        build in-process and evaluate, nothing written  (§5.9)
  | --segment    PATH        a materialised segment, or its manifest
    --canonical  PATH        required with --scope
    --specs      PATH        strategy specs, JSON
    --episodes   PATH        episodes.ndjson
    --intervals  PATH        unusable intervals + verification records
    --analytics  PATH        analytics consumer output
    --summary    PATH        run summary, including segment_address
    [--parallel-evaluate]    off by default (§11)
```

---

## 8. Output contract

### 8.1 Episode NDJSON

One episode per line, self-describing: strategy, spec hash, segment address, which
instrument each leg used, verification ages, gross aggregates. It carries **no
netted number and no verdict** — those are §9's to add.

This replaces `replay/output.py`'s `AnalysisEvidence`, whose required field set
(`polymarket_hash_match` with exactly four keys, non-empty `leg_skew_strata`) was
Polymarket-binary-shaped and raised `ValueError` for any Kalshi or cross-venue
analysis. Mandatory evidence was right; a fixed schema for one venue's shape was
not.

### 8.2 Two kinds of episode

**Measurement** episodes are stateless tests over books. Facts about the tape.

**Simulation** episodes are position-dependent: the episode at T depends on fills
assumed at T−k, which the tape cannot confirm. Worth having — an optimiser
answering "which venue offers the best odds given what I hold" needs them — but
conditional on a named fill model that the record carries.

The same rule applies to spliced tapes: injecting book data from another source or
time produces a synthetic segment, and episodes on one are typed differently, not
flagged.

Day 0 emits `Measurement` only. A third kind, `MeasurementDoubted`, exists on the
Python side alone (§9.1): the engine never emits it, because only hindsight can.

---

## 9. Python side

Consumes `episodes.ndjson`, `intervals.ndjson`, and the verification records.
Scales with episode count, so it is free to be slow, exploratory, and rewritten
weekly.

### 9.1 Retrospective demotion

The engine is causal (§6.4). Python is not, and holds the whole run. For every
leg of every episode: find that instrument's next `Unusable` interval after
`closed_ns`. If it opens with `AnchorMismatch` or `AnchorConflict`, and no
`VerificationRecord { matched: true }` for the instrument lies between the
episode and that interval, then the book was already wrong while the episode was
open — or cannot be shown not to have been. Such an episode is re-typed
`MeasurementDoubted { proven_wrong_at_ns }`. A type, not a flag (§2.6), and
excluded from headline verdicts.

This restores the walk-back `trust.py` performed, in the layer that scales with
episode count, without putting hindsight into an engine that must one day run
live. The demoted count and gross magnitude are reported beside the headline: they
are the direct measure of what §12.9 would recover.

### 9.2 Stages

- **Exact netting** — ports the fee and partition logic out of
  `analysis/partition_sum.py`, which is already n-leg, already has
  `PARTITION_CROSS_VENUE`, and already models time-effective schedules. Its
  pricing loop does not port; that lives in the engine now.
- **Nulls** — the retired pipeline had one placebo. Add time-shift nulls,
  venue-shuffle nulls, and a fee-off/fee-on decomposition. The size ladder becomes
  an effect curve rather than independent rows.
- **Verdicts** — per hypothesis, against that spec's hashed thresholds. `NO`
  stays first-class and stays scoped to the fixture.
- **Diagnostics** — count *and magnitude* of episodes dying on usability vs.
  staleness vs. depth vs. fees. This is the signal that says whether the next
  month goes into capture quality or into strategy.

---

## 10. Analytics

A consumer (§6.6), not a stage. Same walk, same tape, own output stream.

- **Time-weighted aggregates** over instrument state: average spread, depth at k
  ticks, quote uptime, and which venue held the most competitive odds for an event
  and for what fraction of it. The last is already computed by best-candidate
  selection during evaluation.
- **Poll-cadence economics.** Blind-time-per-corruption × corruption-rate = the
  fraction of the window that could not be evaluated. That number decides whether
  to tighten the Polymarket poll interval. It is replay feeding capture, and
  nothing today produces it.
- **Synchronised quote withdrawal** across books —
  `analysis/MARKET_RELATIONSHIP_GRAPH.md` §5.4 names it the cleanest structural
  fingerprint available, and it is invisible at minute granularity.

---

## 11. Parallelism

**Across canonical windows — the primary axis.** One thread per window, each
decoding once and fanning events out to every scope (§5.8). This is the big
lever and it is what makes the prebuild cron cost one pass over the day
regardless of how many bundles it covers.

**Across scopes — do not.** Scopes are independent, which makes one process per
scope tempting. It multiplies the decode by the scope count, which is the cost
§5.8 exists to remove. Scopes are the fan-out inside a window's pass, not a
parallel axis of their own.

**Within a segment's evaluate phase — off by default.** Evaluation is read-only
over book state and consumer state is per-consumer, so `rayon` over the routed set
is safe. But once dispatch has filtered, an evaluation is a handful of comparisons
and adds. Parallelising that behind a synchronisation barrier is a losing trade
until profiled otherwise.

---

## 12. Intentional gaps

**12.1 Job dispatch.** Resolution itself is settled (§3.1): a Python resolver
reads Universe and writes the descriptor. What is not built is the path from a
UI action to a run — a job carrying umbrella event ids, an optional window end,
and a policy version, queued by the Universe server, executed by
`resolve → build → run`, and landing in the results ledger the UI reads.
`EVENT_UNIVERSE_STORE_V1.md` disclaims being a replay planner, so the queue is a
thin dispatch and the resolver stays on the replay side.

**12.2 Segment index, archive, and eviction.** The prebuild cron needs, before
it can run unattended: a SQLite index (`rusqlite` is in the workspace) from scope
digest to address, and from address to local path, archive key, byte size, and
last read; the segment archive receipt kind and a `segments/` publisher built on
the existing archiver pieces (§5.9); local eviction by the dual-receipt rule
under a byte budget; and the version-bump retention rule tied to the results
ledger. **Blocks the prebuild cron, which is why the cron is not in Phase 1
(§4). The most urgent gap here.**

**12.3 Fill model.** No `Simulation` episodes until a named, defensible model
exists. Position-dependent families wait on it.

**12.4 Position state containers.** Declared on the spec; only stateless families
implemented day 0.

**12.5 Cross-venue fungibility derivation.** `FungibilityTier` is on the spec from
day 0 and the engine refuses substitution or locked baskets on non-`IDENTITY`
legs. The tier is not yet *derived* automatically, so cross-venue families wait.

**12.6 Money representation.** Per-venue tick and lot scale, integer or scaled
decimal. Newtypes exist from commit one so the choice is one file.

**12.7 Kalshi.** Its splice remains unverified against live servers
(`ARCHITECTURE.md` §8). Kalshi legs stay out of headline results until a live
segment exists.

**12.8 Live driver.** Same books, same state machine, same consumers, and the
same normaliser (§5.2 stage 5) fed from a socket instead of a canonical window.
Not built, but the consumer API is forbidden from depending on anything only a
replay has, and the normaliser is forbidden from depending on anything only a
receipt has, which is what keeps it possible.

**12.9 Reproducing Polymarket's state hash.** `splices/polymarket/snapshots.py`
correctly calls this unnecessary for the anchor approach. Under the state machine
it is a different proposition: apply delta → compute hash → compare against the
hash that delta carried gives **per-delta divergence detection** on the one venue
with no in-band gap detector, collapsing both the detection and recovery windows
in §6.5. Scoped: one function, verifiable against the 7,414 hashes already on
tape. Pulled into Phase 2 (§4); it stays listed here because the canonical form
is unverified until the bootstrap protocol has run, and until then a mismatch
measures the serialiser, not the tape.

**12.10 Windback measurement.** Whether a stale poll would ever move a usable book
backwards is empirical. Count it per run rather than designing against it; if the
count is zero on real tapes the question dies.

**12.11 Rust object retrieval.** Closed for V1 by stage 0 (§5.2): the Python
resolve step stages archived windows into a local canonical root through
`ArchivedCanonicalByteStreamer`, and Rust re-verifies on read. A Rust client via
the `object_store` crate would remove the staging step and is worth doing only
if the prebuild cron's staging time becomes the bottleneck. The receipt check is
the guarantee; the transport is not.

---

## 13. Known limits

- **Segment order and `event_index` are serialization orders.** `visible_ns`
  carries real timing. No lead-lag conclusion rests on either.
- **Usability is causal and therefore strictly weaker than a retrospective
  audit.** That is the point. The difference is measurable by replaying one
  segment both ways.
- **The engine cannot observe queue position, acknowledgements, or fills.** An
  episode is a displayed-depth opportunity, never labelled captured or filled.
- **The fee envelope is conservative, so trigger recall is bounded below.** An
  episode the envelope excluded is invisible to exact netting. The bound is
  recorded so the loss is quantifiable.
- **Polymarket episodes can be emitted on a book that was already wrong**, for up
  to one poll interval before divergence is detectable (§6.5). Annotated by
  `verification_age_ns`, demoted after the fact by §9.1, removed only by §12.9.
