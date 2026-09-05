# Replay Engine V1

**Status:** design. Supersedes the `replay/gate1..gate5` ladder, which this
document retires rather than revises.

**Architecture-review amendment:** the V1 implementation gates in §4 are
normative. In particular, canonical provenance survives normalization, replay
outputs are receipt-committed, and scaling/publishing follows serial conformance
rather than preceding it.

**Second amendment, after checking the review's premises against source.** Two
of them did not hold and are corrected here rather than left in place:

- the review's claim that `price_changes[].hash` is an *order* hash is
  unsupported by the venue's documentation and contradicted by measurement.
  Hash equality is evidence for candidate historical placement; the later
  evidence amendment below supersedes this amendment's former claim that it
  uniquely positions a snapshot or makes suffix replay optional (§12.9);
- the Universe umbrella-event API is implemented, not pending. Phase 0 shrinks
  accordingly (§3.1, §4).

**Polymarket evidence amendment (2026-09-04).** The completed live experiment in
[`POLYMARKET_HASH_REPLAY_EVIDENCE_2026-09-04.md`](POLYMARKET_HASH_REPLAY_EVIDENCE_2026-09-04.md)
supersedes the preliminary 24-snapshot reasoning in the second amendment. It
confirms a shared asset-scoped REST/WS full-state hash space, but directly
falsifies per-delivery certification. Sections 4, 6, 7, 9, 12, and 14 state the
resulting recommended two-pass default, address-preserving one-pass requirements,
and choices that remain subject to explicit approval.

**Phase-0 integration note.** The audited canonical selector/reader described in
§5.2 has been implemented in `indexer-finalize` in the Phase-0 work. Its API and
capability lifecycle are the Replay boundary; the older proposal to move receipt
types into `indexer-types` is withdrawn. That implementation is not assumed to be
present on this branch until its source-thread commit
`343db932ac216cfa0ba1c467254a0f6bac6ba589` is integrated.

**Simplification amendment (2026-09-05).** The A–I product response supersedes
the earlier proposal that REST verification gates Polymarket book usability,
that the producer owns strategy triggers, that every same-hash candidate state
is retained, or that a central finalizer re-types episodes. V1 now has three
separate contracts:

1. strict normalization and a captured-state book projector;
2. a strategy-owned pull loop over atomic book updates and generic dependency
   coordinates; and
3. an optional, immutable REST-audit overlay containing discrepancy and
   invalidation controls.

A WebSocket full book may initialize a usable captured-state book without REST.
REST remains delayed audit evidence: it can identify a past discrepancy but
cannot reset a later current state or prove a complete suffix. Canonical evidence
and prior strategy output are never rewritten. Sections 3–14 have been revised
to keep those boundaries explicit.

---

## 1. Purpose and boundary

The replay engine answers one question repeatedly and cheaply:

> Over a scoped interval of committed canonical evidence, where did a declared
> strategy see an opportunity, for how long did it survive, and was every book it
> priced available from captured evidence at that instant?

It assumes canonical records exist. It does not produce, repair, or reinterpret
them. It does verify every selected committed object and consumes the
finalizer's continuity provenance before venue payloads cross the typed replay
boundary.

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
| Gate 2 mandatory trust evidence | captured-state book availability plus optional audit controls (§6.4, §7.8) |
| Gate 3 economics | Python netting over episodes (§9) |
| Gate 4 depth survival | episode lifetime, intrinsic to detection (§6.8) |
| Gate 5 frozen policy | per-strategy precommitted thresholds (§6.6) |
| Gate 2 retrospective walk-back (`trust.py`) | optional two-pass audit overlay; strategy-owned void records (§9.1) |
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

Two pieces of retired code remain useful as conformance evidence rather than
runtime dependencies: the canonical-form logic in `books.py` seeds the optional
two-pass auditor (§12.9), and the venue interpretation in `events.py` is ported
into the reconciler with its tests as fixtures. The retired gate orchestration
does not survive.

---

## 2. Governing principles

**2.1 State reconstruction and interrogation are different jobs.** Building the
book has one correct answer, is expensive, and is cacheable. Asking questions of
it has many answers, is cheap, and changes weekly. Every seam below preserves that
split.

**2.2 The segment re-expresses observation and never records judgement.** A
segment may filter and normalise — that is re-expression of what we saw. Every
accepted segment event retains its canonical address and continuity provenance.
Payloads that cannot enter the closed typed schema go to the receipt-committed
reject sidecar (§5.3), never disappear and never become invented control events.
The segment may **not** carry usable/unusable, trust verdicts, anchor outcomes,
or repositioned records. Those are derived, they are ours rather than the
venue's, and they belong in engine output. A tape that carries our conclusions
stops being evidence.

**2.3 Captured-state availability and retrospective audit are distinct.** A
healthy in-stream full book initializes the captured-state projector. Known
continuity or mutation faults can make it unavailable. A delayed REST snapshot
does not gate initialization: an auditor may instead append a control that
invalidates an exact historical dependency range. §6.4.

**2.4 Rust owns what scales with tape size; Python owns what scales with episode
count.** A bundle-scoped segment is 10^5–10^6 events; a full capture day is ~10^7
(6.2M records/day for 20 Polymarket assets; 14.4M over 19 hours across three
venues). Episodes are 10^3–10^4. Rust is justified by the daily prebuild across
every scope, by analytics clients that walk whole days, and by the live driver
(§12.8) that must run the same books and pull API against a socket — **not** by
any single segment being too large for Python. The honest reason is the one that
survives scrutiny.

**2.5 The producer has no strategy trigger semantics.** Replay applies canonical
atomic groups and exposes a pull cursor. A strategy decides which updates to
inspect and when to evaluate; strategy *families* are code and instances are
data (§6.6).

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
                              │  segment + rejects + manifest + receipt (§5.9)
        ┌─────────────────────▼─────────────────────┐
        │  ENGINE                          Rust     │
        │                                           │
        │   ReplayCursor::next()                     │
        │     ├── apply one canonical atomic group  │
        │     ├── expose touched books + versions   │
        │     └── expose audit controls             │
        └─────────────────────┬─────────────────────┘
                              │  strategy-owned pull/evaluation
                              │  episodes + revisions + dependency records
        ┌─────────────────────▼─────────────────────┐
        │  ECONOMICS + STATISTICS          Python   │
        │  exact fee netting · voids · nulls · verdicts │
        └───────────────────────────────────────────┘

  optional, separate pass:

  segment + REST anchors ──► TWO-PASS AUDITOR ──► immutable audit overlay
                                                   (verification/invalidation
                                                    controls, manifest, receipt)
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
every retained-bundle and origin-resolution problem a second time, and produces
a scope that cannot be reproduced later because it was computed from whatever
runs were on disk that day. It is rejected. **Universe is the only resolver
source.** If a run is not in Universe, Universe is synced first; the replay side
does not read run manifests.

**What the resolver needs from Universe.** Four lookups. Table names below are
the local schema's; the contract is the lookups — an umbrella event id linked to
every venue event id, venue market id, and captured asset id beneath it, over
every Targeter run combined. **The Universe server implements this.** The
umbrella-event model is live: `umbrella_events` joined to `venue_events`,
`canonical_markets`, and `venue_markets` — whose `subscription_ids_json` holds
the captured asset ids — served through `/v1/events/{event_id}` and
`/v1/markets/{market_id}`, with relationships in `relations` and
`relation_observations`.

Two shape mismatches remain, and they are field additions rather than new
subsystems: event detail returns canonical markets with a venue count, so
reaching subscription ids needs a second call per market; and the selection
record does not carry `context_sha256`, which the descriptor needs for
reproducibility (§3.1). Pin the response contract with a test so it cannot drift
under the reconciler, and add those fields. Do not rebuild what exists.

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
    "end_source": "retirement | explicit | latest_closed_window",
    "retirement_disposition": "all_markets_terminal | terminal_clamp_elapsed | null",
    "terminal_observed_at_ns": null
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
- **Lanes are not in the descriptor.** Canonical windows carry every admitted
  lane. Pending §14.1(2), the proposed minimum is for normalizer adapters to bind
  recognized lane ids to minimal venue/stream roles. Runtime
  `connection_opened` evidence narrows a present lane
  to instruments. No separate deployment-wide lane map is required in V1. If a
  required recognized book lane is wholly missing or invalid, requested books
  for that venue stay `NotInitialized`; a missing REST snapshot lane removes
  audit coverage but does not disable the WebSocket book. An unrecognized lane
  fault remains receipt evidence and does not poison unrelated requested books.
  Twin or custom lane layouts require explicit adapter configuration before they
  can be attributed; V1 does not guess.
- **The window end is retirement, not activation.** The retired §12.1 text said
  `capture_start_at_ns → activation_at_ns`. That is the pre-match window only;
  for esports the in-play evidence is the point. `end_ns` comes from the bundle's
  retirement, from an explicit `--to`, or from the latest closed canonical window
  when the bundle is still live — and `end_source` records which. `start_ns` is
  the earliest `capture_start_at_ns` over the contexts; the reconciler adds the
  prologue (§5.5) itself. Universe retirement is an observation upper bound or
  safety clamp, not proof of the exact event end. The scope preserves retirement
  disposition and observation time, and consumers do not relabel it as an exact
  resolution boundary.
- `uncaptured_markets` lists markets Universe knows for the event that have no
  captured asset. They are not in the filter; they are recorded so the
  observability gate can say "this relationship was never measurable" rather
  than "no episodes".
- **Strategy specs are a separate file** and are *not* part of the scope. The
  segment address (§5.7) hashes the scope alone, so one segment serves every
  hypothesis set over it. Specs carry the same `context_sha256` list so the two
  files are provably about the same contexts.

**Retained bundles.** The implemented Universe contract stores retained
occurrences against their separately verified complete origin
(`docs/EVENT_UNIVERSE_STORE_V1.md` §4). The resolver takes markets, assets, and
relationships from that origin and refuses, with the bundle id, if the chain
does not terminate in a complete occurrence.

---

## 4. Phases

Each phase is usable without enabling the next one. Scaling, unattended
operation, simulation, and deletion do not ride along with correctness work.

**Phase 0 — Freeze what the reconciler will depend on.** Smaller than the first
review assumed, because Universe exists and the canonical boundary is implemented
in the Phase-0 `indexer-finalize` work:

- **Universe is implemented** (§3.1). This is a contract test plus two field
  additions, not an API build.
- **Money representation is adopted, not designed.** The intended reference is
  [`hridyansh07/bitfrost-prime-take-home`](https://github.com/hridyansh07/bitfrost-prime-take-home):
  `crates/types` separates borrowed venue decoders from a closed canonical enum,
  checked fixed-point price/quantity/money primitives, and fallible conversion;
  `crates/market` prepares a complete mutation before atomic publication. Replay
  adopts that architecture while preserving its own provenance and stricter
  reject contract. The reference repository exposes no license file or Cargo
  license metadata, so implementation may not copy code verbatim without
  explicit permission/license clarification.

What genuinely has to be settled first: integrating and pinning that public
boundary, the minimal adapter-owned lane role, fixed-point representation, and
the half-open scope interval decision in §14. Integration must preserve the
implemented API and capability lifecycle rather than re-implementing its audit
in Replay. Fee formulae and strategy economics do not block this boundary.

Acceptance: one fixture resolves to the same canonical scope bytes repeatedly;
`select_canonical_windows` proves unique minimal adjacent coverage of
`[T0 − prologue, T1)`; the stream is consumed through `Ok(None)` and only
`finish()` mints `AuditedCanonicalSelection`; Replay explicitly selects
`AllowUncertified` while preserving receipt coverage faults; retirement is
recorded as an observation/clamp rather than asserted to be the exact event end;
selector policy choices are recorded in the segment manifest; and the completed
§12.9 evidence
is pinned. An object, record prefix, or successful decode without the finished
audit capability cannot commit a segment.

Normaliser conformance (§5.10) runs against fixtures and depends on none of the
above, so it starts in parallel rather than queueing behind this phase.

**Phase 1 — Serial reconciler and `build`.** One process and one decode worker,
with no archive publication: verify, decode, join provenance, clip, normalise,
filter, and write a receipt-committed local segment plus reject sidecar.
Acceptance: Rust reproduces `replay/events.py` on fixtures and real canonical
windows (§5.10); duplicate/conflict/gap fixtures exercise the provenance policy;
equal-time tie groups remain atomic; Kalshi fixtures cover relative deltas; and
an independent reader verifies every output identity. No reject is silent and
no rejected payload can produce an available book.

**Phase 2 — Captured-state projector and pull API.** Serial book mutation plus
`ReplayCursor::next`; no producer-owned dispatch, strategy family, REST promotion,
or economics. Acceptance: each valid WebSocket full book initializes its book;
canonical duplicate/conflict/fault behavior is exact; reconnect, impossible
relative mutation, overflow, missing full book, and output-reader failure produce
the specified availability state; every pull returns the completed vendor/
tie-group update and generic book dependency tokens; malformed wire/control
payloads cannot reach the book crate; repeated runs are byte-identical.

**Phase 3A — Optional two-pass audit overlay.** Stream the fixed base segment and
REST anchors, compare historical reconstructed levels, and emit immutable,
receipt-bound verification/invalidation controls without changing base books or
strategy output. Acceptance: split same-hash deliveries, recurrence, delayed H2
at current H3, known continuity faults, no candidate, and missing suffix produce
deterministic controls and exact dependency ranges; retries verify/no-op or get a
new address; a fresh run and later reconciliation consume the same controls.

**Phase 3B — First strategy and deterministic economics.** A strategy owns its
pull loop, evaluation cadence, dependencies, episodes, revisions, and voids. Add
the pinned offline fee module only after representation is settled. Acceptance:
each emitted revision can be netted exactly from event-time inputs; official SDK
results are checked against hand-calculated vectors; candidate substitution
closes/reopens; a late invalidation voids exactly intersecting decisions; and
repeated runs are byte-identical.

**Phase 4 — Durable publication and operations.** A separate Python publisher
uploads final local segments through the existing `ObjectStore`; add the index,
leases, staging quota, monitoring, and bounded cleanup. Acceptance: crash tests
at every file/receipt boundary are idempotent, concurrent jobs cannot publish
different bytes at one address, archive verification precedes local eviction,
and V1 never deletes archived segments.

**Phase 5 — Measured scaling and families.** Introduce the bounded window pool
only after serial conformance is measured, then implication-bound and
cross-venue families behind the fungibility gate (§12.5). Simulation, optimizer,
live driver, and archive garbage collection remain later, separately approved
work.

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
 0  stage       archived windows → local canonical root           new bounded Python stager
                (only when the local root has been reaped)         (§3.1 resolve --stage)
 1  select      gap-free receipts covering [T0 − prologue, T1)    indexer-finalize public API
 2  verify      stored + decoded identity; provenance binding     AuditedCanonicalReader
                to receipt inputs; canonical_seq continuity
 3  decode      Zstandard through the strict decoder              prediction-encoder::StreamingDecoder
 4  parse       envelope lines, closed and versioned              indexer-types::EnvelopeView
 5  normalise   raw_payload → SegmentEvent (§7.2)                 new: tape::normalise
 6  filter      in-scope instruments + control for their lanes    new
 7  clip/concat discard records at or after T1; windows in order  new + finalizer order
                order; within a delivery, venue array order
 8  emit        stream, or segment + rejects + manifest + receipt new
```

**Stages 1–4 consume the implemented `indexer-finalize` boundary.**
`select_canonical_windows(root, start, end, SelectionPolicy)` returns the unique
minimal adjacent committed-window run covering the half-open interval and rejects
gaps, overlaps, duplicate starts, missing/corrupt receipts, missing objects, and
non-adjacent canonical sequence. Empty windows do not reset sequence continuity.
`CanonicalSelection::open()` returns `AuditedCanonicalReader`, whose
`next_record()` yields exact envelope bytes joined to closed provenance in
finalizer order. It rechecks stored and decoded identities, strict Zstandard EOF,
receipt identity, evidence/provenance lockstep, source binding, record/content
identity, window bounds, delivery index, and line-level sequence. An error poisons
the reader permanently.

The caller must consume through `Ok(None)` and call `finish()`. Only `finish()`
returns the opaque `AuditedCanonicalSelection` that pins receipt SHA-256 and byte
length; records yielded earlier are untrusted. Segment output may be staged while
streaming but its commit receipt cannot be published without this capability.
The final upper bound is always clipped. Replay uses `AllowUncertified`: strict
object/receipt/provenance verification still applies, while known coverage and
clock faults remain visible rather than preventing healthy lanes from replaying.
The lower-bound choice remains open (§14). Both the selected policy and effective
interval enter the segment manifest and address.

These APIs and their receipt/provenance types remain owned by
`indexer-finalize`; Replay must not fork them into `indexer-types` or duplicate
the audit. `indexer-types` continues to own shared envelope and identity
primitives only. Stage 5 consumes `JoinedCanonicalRecord`, including exact
envelope bytes, `canonical_seq`, `order_ns = visible_ns`, tie group,
lane/delivery address, record/source/content provenance, and the closed
continuity verdict.

**Stage 0 is Python and optional.** The canonical reaper removes local windows
about eighteen hours after they are archived, so the local root is a rolling
window. `ArchivedCanonicalByteStreamer` currently verifies archived objects but
exposes decoded logical bytes; it does not materialize the original stored Zstd
frames and local receipt layout expected by the Rust audited reader. Phase 0
therefore adds a bounded stager over the existing `ObjectStore` verification
path that preserves each archived object's stored bytes and identity, writes the
retained canonical receipt, fsyncs, and publishes the local window atomically.
Rust then independently re-verifies it on read. Staging sits before the byte
boundary with the resolver (§3.1); it has explicit byte quota and cleanup.

**There is no k-way merge and no new dedupe stage.** Both existed because the
retired Python replay read raw lane segments. Canonical windows are time-disjoint
and internally ordered, so stage 7 is concatenation. Overlapping window ranges are
rejected at stage 1 rather than deduplicated later. A `late_after_finalization`
correction dataset (`SEALED_CAPTURE_PIPELINE_V1.md` §5) is a different canonical
lineage with its own receipts and is selected as such, never merged in.

This does not mean duplicate records are impossible. Canonicalization preserves
the exact envelope and classifies each line. A `duplicate` provenance verdict is
retained but never mutates a book; `conflict`, `gap_proven`, and other continuity
faults transition affected books to `Unavailable` before their payload could mutate
state. That policy is especially important for relative Kalshi deltas.

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

**Twin lanes.** `record_id` is per-splice, so if a redundant-lane experiment ever
runs, both records reach the engine. Cross-lane arbitration is client-owned
policy (§6.6), not a reconciler stage.

### 5.3 What the filter keeps

- every event for an in-scope instrument;
- **every control record for any lane carrying an in-scope instrument**, whether
  or not the control record names an instrument. A `connection_failed` that names
  nothing is exactly the record the engine needs.

Normalisation has two evidence outcomes: an accepted closed venue
`SegmentEvent`, or a `ParseReject` accompanied by a closed
`NormalizationFault` classification. Rejects are appended to `rejects.ndjson`
with the canonical header, exact canonical envelope line, parser version,
optional safely extracted instrument hint, and stable error code. The sidecar and
its counts/identity are committed by the segment manifest. Canonical
frame/envelope corruption has already failed stage 2 or 4; venue payload and
control-shape validation belongs only here. Rejected wire never enters `book` and
the runtime never attempts to classify an unknown control.

A reject with a proven in-scope instrument marks that book unavailable at its
coordinate. When the instrument cannot be extracted, the adapter's recognized
lane role determines the smallest affected set: a book-lane reject affects
requested books for that venue; a snapshot-lane reject removes only audit
coverage. An unrecognized lane reject is retained but does not poison unrelated
books. This is deliberately less machinery than a deployment lane map and must
be extended explicitly before custom/twin lanes are used.

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

`visible_tie_group` crosses the type boundary. All records in one group are
applied before `ReplayCursor::next()` returns the resulting update. A strategy
may inspect or ignore that update but cannot observe a partial tie group. The
deterministic lane rank remains serialization, not an economically meaningful
ordering between simultaneous observations.

The last selected canonical window is clipped at `T1`: records with
`visible_ns >= T1` do not enter the segment or affect books, episodes, or final
state. The prologue is input-only in the other direction; it may bootstrap books
but cannot emit episodes before `T0`.

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
3. the audited reader did not reach EOF and mint `AuditedCanonicalSelection`.

Fatal **per instrument**, which refuses that instrument's legs rather than the
segment:

4. no full book anywhere in prologue + window, so the book can never leave
   `NotInitialized`.

Recorded in the manifest, not fatal:

5. `bootstrap_offset_ns` per instrument — how far into the segment its first
   available book appears. Normally near zero thanks to the prologue; when it is
   not, the loss is visible instead of silent.
6. delivery-index discontinuities per lane, read from the provenance index's
   continuity verdicts and passed to the engine as an `UnavailableCause` at that
   position.
7. an `incomplete` receipt — a lane missing or invalid for the whole window. The
   window is admitted under `AllowUncertified`. A recognized missing book lane
   leaves requested books for that venue `NotInitialized` over the affected
   interval; a recognized missing snapshot lane records absent audit coverage
   only. Healthy lanes continue. If a lane id has no adapter-owned role, Replay
   preserves the receipt fault and makes no guessed book attribution.

### 5.7 Identity

`segment_address` uses a versioned, domain-separated, length-delimited preimage:

```
sha256(
    "prediction-indexer/replay-segment/v1" ||
    u64be(len(scope_descriptor_canonical_bytes)) || scope_descriptor_canonical_bytes ||
    u64be(len(selection_policy_canonical_bytes)) || selection_policy_canonical_bytes ||
    u64be(len(input_object_manifest)) || input_object_manifest ||
    u64be(len(reconciler_version)) || reconciler_version
)
```

`input_object_manifest` itself has canonical bytes and explicitly sorted receipt
entries. `selection_policy_canonical_bytes` records certified and lower-bound
policy plus the requested/effective interval returned by the audited capability.
The same framing rule applies to any future composite run, cache, or episode
identity. It does not alter existing canonical content hashes, archive identities,
or vendor hashes; those follow their owning contracts exactly.

Computed and recorded on every run even when the segment is discarded, so results
join to segments retroactively once caching is on. Every episode carries it.
Strategy specs are not in the address (§3.1): one segment serves every hypothesis
set over its scope.

### 5.8 Build order: bounded window passes, fanned out to every scope

The filter runs *after* decode, so building one scope costs the full decode of
every canonical window in its time range, all lanes, whether the scope touches
two instruments or two hundred. At 100–150 bundles a day, most of them
overlapping in time, building scopes one at a time decodes the same windows over
and over. The unit of work is therefore the **window**, not the scope:

```
for each canonical window W in the day, through a bounded worker pool:
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

Phase 1 is serial. Later, the coordinator caps decode workers, open files,
per-scope sink buffers, and staged bytes from configuration and commits completed
window slices in receipt order regardless of worker completion order. Backpressure
blocks workers; it never drops an accepted event. Each temporary slice has an
identity and restart journal, and is either verified and reused after a crash or
discarded before retry. A logical outcome-space bundle is a good strategy/sink
boundary, but not automatically an OS-thread boundary: decoding once per bundle
would repeat the dominant work.

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
build with a later end, and removed only by the explicit local supersession/age
policy. It is never presented as archived.

**Process ownership is explicit.** Rust writes a receipt-committed local
`<address>/` directory containing the segment, reject sidecar, manifest, and
local receipt using finish → fsync file → rename → fsync directory ordering. A
separate Python publisher consumes only that local receipt and reuses
`publish_files`, `put_immutable`, the existing `ObjectStore`, and
`verify_object`. The segment archive receipt is a third receipt kind beside the
raw and canonical ones and carries: `segment_address`, the scope digest, the
list of canonical receipts consumed, `reconciler_version`, and stored and
decoded identity for every published file. The object key is
`segments/<address>/`. `run --segment` accepts an address and resolves local
first, archive second, verifying either. Rust receives no archive credentials in
V1.

**Local eviction** is the reaper's dual-receipt rule with the segment receipt
standing where the canonical receipt stood: a local segment is deletable when its
archive receipt verifies against the store and a byte budget on the volume says
so. Live-bundle segments have no archive receipt and are evicted by age. V1 has
no archive-object deletion or segment archive garbage collector; existing
`ObjectStore` adapters intentionally expose no deletion authority.

**A reconciler version bump** changes every address. Old segments are not
rewritten and not deleted on the bump: the results ledger references them by
address, and a verdict must stay traceable to the segment it was measured on.
Archive garbage collection is outside V1 and requires a separately approved
retention contract.

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
  book       captured-state projector, availability, versioned views
  cursor     serial atomic-group pull API over tape + optional overlay
  audit      optional two-pass REST comparison and control overlay
  price      ladder walk and checked fill primitives used by strategies
  strategies strategy-owned loops, families, episodes, and void records
  cli        replay-engine  (build · run)

shared from the ingester workspace, never the reverse:
  indexer-finalize     canonical selection, audited reader, provenance types
  indexer-types        EnvelopeView and shared identity primitives
  prediction-encoder   strict Zstandard
```

`tape`, `book`, and `cursor` know nothing about strategy trigger semantics.
Nothing outside `book` can construct an available book view. Nothing outside
`tape` sees venue JSON. `audit` never mutates a base segment or primary book.

### 6.2 The walk

The projector is single pass, **per atomic observation group**, strictly
sequential. The strategy owns the loop:

```
while let Some(update) = replay.next()? {       // applies one complete group
    if strategy.wants(&update) {                // strategy-owned policy
        strategy.evaluate(replay.books(), &update, output)?;
    }
}
```

`ReplayCursor::next()` returns touched instruments, the group's inclusive first
and last canonical addresses, tie-group identity, applied book-version tokens,
and any audit controls effective at that coordinate. It neither registers
interests nor invokes strategy code. A strategy may evaluate every update, only
updates touching selected books, a time bucket, or terminal controls. Its
versioned policy determines observability and belongs in its output identity.

Mutation always completes these boundaries before returning:

1. every entry in one vendor batch whose semantics describe one update is
   applied together; and
2. every canonical `visible_tie_group` is applied before the cursor returns, because
   lane rank is not evidence that one simultaneous market moved first.

Polymarket repeated hashes are not an atomic group. Every canonical address
remains on disk and every delivery boundary remains visible to the pull cursor;
the auditor may compare historical delivery-end states without keeping a full
in-memory state per occurrence (§12.9).

There is **no heartbeat and no synthetic record**. Staleness and timeout closures
are applied **backdated** at the next evaluation: the tracker knows each leg's
`last_update_ns`, so an episode closes at when staleness bit, not when it was
noticed. In replay, emission time is irrelevant; only the recorded interval is
real. A `finalize` pass at segment end closes and right-censors whatever remains
open.

### 6.3 Events survived

A strategy that evaluates each relevant update may record both `duration_ns` and
`updates_survived`. A coarser strategy records its own observation count instead.
The producer does not manufacture a strategy-specific lifetime measure.

### 6.4 Captured-state availability and independent audit

```
  NotInitialized ──valid in-stream full book──► Available
          ▲                                      │
          │                                      ├── canonical continuity fault
          │                                      ├── classified normalization fault
          │                                      ├── impossible relative mutation
          │                                      └── arithmetic overflow
          │                                                        │
          └──────────── later valid in-stream full book ◄──── Unavailable
```

- Canonical provenance is applied first. `duplicate` records do not mutate;
  conflicts and attributable continuity faults make affected books unavailable
  before venue semantics run.
- A valid Polymarket WebSocket `book` is a captured full state and immediately
  initializes `Available`. REST is not a promotion prerequisite. `Available`
  means reconstructable from admitted captured evidence, never venue-proven
  completeness.
- Reconnect/epoch change prevents deltas from extending the old epoch until a
  new full book arrives. A full book is the only bootstrap/reset operation.
- Malformed price, size, and control payloads are rejected and classified by the
  strict normalizer before `book`; the projector receives only a closed
  `NormalizationFault` with an already determined impact set. It does not parse
  or guess malformed wire semantics.
- A syntactically valid but impossible relative mutation, negative resulting
  level, or checked-arithmetic overflow is a projector error and makes only the
  affected book unavailable.
- No-full-book is ordinary `NotInitialized`, not a crash. Strategies querying
  that book receive no available view.

**The rule that is easy to get backwards:** an independent REST snapshot
describes its source observation frontier, not its later receipt frontier. It is
compared with the reconstructed historical state and never resets the current
book. Comparing it with current H3 creates a false mismatch; resetting H3 to
stale H2 rewinds state. The optional auditor therefore writes a control overlay
rather than mutating the primary projector.

On mismatch, the narrowest conservative invalidation begins after the previous
accepted anchor (or segment start when none exists) and ends at the recovered
current frontier. If a complete captured suffix cannot be reconstructed, the
range is open-ended until a later recovery control. This is audit evidence, not
an automatic current-book transition: each strategy decides whether its recorded
dependencies require an append-only void (§9.1).

### 6.5 Detection and recovery latency are different

| Venue | In-band gap detection | Captured-state reset | Delayed audit evidence |
|---|---|---|---|
| Limitless | n/a — every message is a full book | every message | not required for bootstrap |
| Kalshi | `update_range` — immediate | 30s sweep + subscribe | source-sequence evidence where present |
| Polymarket | **none** | next in-stream full book | historical REST anchor + captured suffix; no venue-completeness proof |

Polymarket is uniquely exposed: no sequence numbers, so divergence is detected
only when a full-book poll is received. A delayed poll can anchor its historical
state, but no hash proves that the suffix from that state to current is complete.
The base captured-state book remains runnable; audit consequences arrive later
as controls with exact dependency coordinates.

In the ten-minute sample, chosen anchor→REST receipt lag reached **155.659 s**
and six anchors became visible on WebSocket only after REST receipt. This is a
lower bound on journal demand, not a production recovery bound. Detection
latency is poll schedule + request/anchor delay. Recovery latency additionally
includes historical placement and availability of a captured suffix or later
full book; it can remain unknown. Both distributions are reported separately.

The response is an immutable audit control (§7.8), never retraction of the tape
or a producer-owned demotion. Strategies may ignore audit controls, block on
them, or append voids for intersecting dependencies; that policy and its output
identity remain strategy-owned.

§12.9 records why hash equality enumerates historical candidates and what
reproducing the venue digest proves. Neither uniquely positions every snapshot,
turns a hash into per-delivery verification, or proves suffix completeness.

### 6.6 Strategy-owned pull and evaluation

Strategies and analytics are clients of the same pull API, not callbacks owned
by Replay. Each client owns filtering, cadence, internal state, and output sinks.
An optional client-side instrument index may optimize its own work, but it is not
part of the producer contract or segment identity.

**Legs are candidate sets.** A leg is not one instrument. If YES is unavailable on
venue A but available on venue B, and NO is available on B, the basket is still
priceable.

- the strategy considers every declared candidate it needs;
- the episode records **which instrument was actually used** per leg;
- when several candidates are available, **best price wins** — and that selection is
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

### 6.7 Price/depth strategy states

These are implementation choices for the first price/depth strategy, not engine
semantics. With no open episode, it may maintain the combination incrementally —
reprice only the leg whose instrument the pulled update touched, update Σ, and
test the threshold.

With an open episode, it may apply the fingerprint guard before repricing: *did this event
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
  mid-episode. Also emit a revision whenever an economically relevant leg fill,
  price, quantity, verification status, or fee input changes; aggregates alone
  cannot reconstruct exact nonlinear fees.
- **Right-censor.** Still open at segment end is censored, not short.
- **Backdate every close.** Staleness and `Unavailable` closures are recorded at the
  moment the condition began, not when it was observed. Closing at detection would
  inflate the lifetime preceding every corruption event — biasing the headline
  metric upward, which is the direction that flatters the hypothesis.

Each size in the ladder is its own episode series. A gap qualifying at 10
contracts and not at 100 is two different facts.

Candidate substitution closes the current episode and opens a new one. This
keeps each episode's chosen instruments and fee jurisdiction stable without
retaining an implicit, unbounded candidate history inside one record. Revision
emission is fingerprint-gated to meaningful repricing changes and streamed; its
memory use is bounded even though durable output remains proportional to actual
revisions.

### 6.9 The fee envelope

Two numbers, so the language boundary stays where it belongs.

**Trigger envelope — Rust, data.** Python's catalog side precomputes a
conservative *upper bound* of the fee curve over the relevant price region and
puts it in the spec as one number. The engine triggers on `gross_gap − envelope`
and knows no fee model, no bonding curve, no time-effectivity.

**Exact netting — Python, per revision.** Full curve, time-effective schedules,
per-leg conservative rounding, over emitted episodes only. Every revision
carries the event-time instrument, price, quantity, and schedule key required by
the fee function. Venue SDK implementations may be reused as a black box only
when pinned by version, callable offline for historical runs, and checked against
hand-calculated fixtures. A replay may not consult a venue's current fee service
to price historical evidence.

The receipt then guarantees exactly what Rust can honestly guarantee: **available
from captured evidence, gross-priced, envelope-cleared.** Python owns the economic verdict. A too-tight
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

Representation, overflow behavior, rounding, and per-venue scales are frozen in
Phase 0 (§12.6). The newtypes make invalid cross-unit arithmetic explicit rather
than deferring semantics until after the engine exists.

### 7.2 Segment events

A closed schema, defined once in `tape` and written and read by it. The reader
rejects unknown variants rather than skipping them — same discipline as the
envelope parser — which matters less now that writer and reader share the enum,
and still matters for a segment file built by an older reconciler.

```rust
pub struct EventAddress {
    pub canonical_seq: i64,
    pub lane: LaneId,
    pub delivery_index: u64,
    pub event_index: u32,
}

pub struct EventHeader {
    pub order_ns: i64,
    pub visible_ns: i64,
    pub visible_tie_group: Option<u64>,
    pub addr: EventAddress,
    pub record_id: Arc<str>,
    pub provenance: CanonicalProvenance,
}

pub struct CanonicalProvenance {
    pub source_segment_sha256: Arc<str>,
    pub source_line_number: u64,
    pub content_hash: Arc<str>,
    pub continuity: indexer_finalize::ContinuityVerdict,
}

pub enum SegmentEvent {
    Control(ControlEvent),
    Book(BookEvent),
    AuditAnchor(AuditAnchor),
    NormalizationFault(NormalizationFault),
    Trade(TradeEvent),
}

pub struct SegmentRecord {
    pub header: EventHeader,
    pub event: SegmentEvent,
}

pub enum BookEvent {
    Full {
        instrument: InstrumentId,
        bids: Vec<(Px, Qty)>,
        asks: Vec<(Px, Qty)>,
        snapshot_hash: Option<Arc<str>>,
        source_observed_ns: Option<i64>,
    },
    Delta {
        instrument: InstrumentId,
        side: Side,
        price: Px,
        size: LevelSize,
        book_hash: Option<Arc<str>>,   // venue digest; see §12.9
    },
}

/// Independently polled REST `/books` evidence. The primary book projector does
/// not apply this as a current full book; only the optional auditor consumes it.
pub struct AuditAnchor {
    pub instrument: InstrumentId,
    pub bids: Vec<(Px, Qty)>,
    pub asks: Vec<(Px, Qty)>,
    pub snapshot_hash: Arc<str>,
    pub source_observed_ns: Option<i64>,
}

/// A venue/control payload rejected by strict normalization. Exact bytes and the
/// stable parser error remain in the committed reject sidecar. Impact is decided
/// by the adapter before the book crate and is never inferred by the runtime.
pub struct NormalizationFault {
    pub reject_id: Arc<str>,
    pub impact: FaultImpact,
}

pub enum FaultImpact {
    Instrument(InstrumentId),
    RequestedVenueBooks(Arc<str>),
    AuditCoverageOnly(Arc<str>),
    UnattributedLane(LaneId),
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

The parser preserves the finalizer's exact closed vocabulary (`lifecycle`,
`bootstrap`, `unsequenced_venue`, `sparse_monotonic`, `continuous`,
`gap_proven`, `cursor_went_backwards`, `local_counter_broken`, `duplicate`, and
`conflict`). An unknown label is fatal, not `Continuous`, and Replay does not
collapse distinct upstream evidence into a generic fault.

`Absolute` application is idempotent; `Relative` is not, which is one more reason
a book fed by relative deltas can never re-converge after a gap without a full
book (§6.4). `book_hash` is carried verbatim for audit comparison; canonical
addresses remain in the segment on disk, not as duplicated in-memory candidate
states (§12.9). The official full-book SHA-1 is reproducible, but it certifies
neither an individual delta/delivery nor suffix completeness. Kalshi's delta
semantics are taken from its published spec and are unverified against live
servers (§12.7); the variant exists so that verifying them is a reconciler
change, not a schema change.

### 7.3 The book and its receipt

```rust
pub enum BookState {
    NotInitialized,
    Available { since_ns: i64 },
    Unavailable { since_ns: i64, cause: UnavailableCause },
}

pub enum UnavailableCause {
    EpochReset,              // reconnect; prior book cannot carry over
    LaneInterrupted,         // connection_failed covering this instrument
    SegmentDiscontinuity,    // manifest-declared delivery gap
    CanonicalConflict,
    NormalizationFault,
    InvalidRelativeMutation,
    ArithmeticOverflow,
}

pub struct BookDependency {
    pub instrument: InstrumentId,
    pub epoch: Arc<str>,
    pub from_full_book: EventAddress,
    pub through: EventAddress,
}

/// No public constructor: only `BookStore::available` mints one.
pub struct AvailableBook<'a> { /* private */ }

impl<'a> AvailableBook<'a> {
    pub fn instrument(&self) -> &InstrumentId;
    pub fn levels(&self, side: Side) -> &[(Px, Qty)];
    pub fn available_since_ns(&self) -> i64;
    pub fn dependency(&self) -> &BookDependency;
}

pub struct BookStore { /* … */ }

impl BookStore {
    pub fn apply(&mut self, hdr: &EventHeader, ev: &SegmentEvent) -> Touched;
    pub fn available(&self, id: &InstrumentId) -> Option<AvailableBook<'_>>;
    /// Always available, including when `available` returns None.
    pub fn state(&self, id: &InstrumentId) -> &BookState;
}
```

### 7.4 Pricing requires the receipt

```rust
pub fn walk_ladder(book: &AvailableBook<'_>, side: Side, size: Contracts) -> Fill;
```

This signature prevents pricing an unavailable captured-state book. Independent
audit policy remains strategy-owned and is enforced by the strategy's treatment
of overlay controls, not by forging a different book type.

### 7.5 Pull cursor

```rust
pub struct Touched(SmallVec<[InstrumentId; 4]>);

pub struct ReplayUpdate {
    pub first: EventAddress,
    pub last: EventAddress,
    pub visible_tie_group: Option<u64>,
    pub touched: Touched,
    pub controls: Vec<AuditControl>,
}

pub struct ReplayCursor { /* tape, optional pinned overlay, books */ }

impl ReplayCursor {
    pub fn next(&mut self) -> Result<Option<ReplayUpdate>, ReplayError>;
    pub fn books(&self) -> &BookStore;
    pub fn finish(self) -> Result<FinishedReplay, ReplayError>;
}
```

`next()` applies one complete vendor/tie atomic group and merges controls from
the one overlay identity pinned by the run. It never calls strategy code. The
strategy's own loop decides whether and when to query `books()`.

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
    LegUnavailable { cause: UnavailableCause },
    LegStale, SegmentEnd,
}

pub struct LegFill {
    pub instrument: InstrumentId,        // which candidate was chosen
    pub side: Side,
    pub fill: Fill,
    pub dependency: BookDependency,
    pub book_available_since_ns: i64,
    pub last_update_ns: i64,             // the retired leg-skew stratum, per leg
}

pub struct Episode {
    pub kind: EpisodeKind,
    pub episode_seq: u64,
    pub strategy_id: StrategyId,
    pub spec_sha256: [u8; 32],
    pub segment_address: Arc<str>,

    pub opened_ns: i64,
    pub closed_ns: i64,
    pub opened_at: EventAddress,
    pub closed_at: EventAddress,
    pub opened_tie_group: Option<u64>,
    pub closed_tie_group: Option<u64>,
    pub observations_survived: u32,      // under this strategy's pull policy
    pub right_censored: bool,
    pub close_reason: CloseReason,

    pub size: Contracts,
    pub legs: Vec<LegFill>,
    pub gross_gap: Aggregates,           // bounded min/max/integral; median from revisions
    pub min_fillable: Qty,
    pub fee_envelope: Px,
}

pub struct EpisodeRevision {
    pub episode_seq: u64,
    pub at_ns: i64,
    pub at: EventAddress,
    pub visible_tie_group: Option<u64>,
    pub legs: Vec<LegFill>,
    pub gross_gap: Px,
    pub fee_schedule_keys: Vec<Arc<str>>,
}
```

`EpisodeRevision` is the exact economic trajectory. Python derives exact
time-weighted aggregates and fees from it; Rust never retains all revisions in
memory merely to compute a median.

### 7.8 Availability and audit-overlay records

The base availability record is settled by the captured-state boundary. The
overlay identity/range shape below is the concrete recommendation pending
§14.1(4), not a recorded approval.

Captured-state availability intervals are engine output, never canonical tape.
They distinguish “no edge” from “no reconstructable book.”

```rust
pub struct UnavailableInterval {
    pub instrument: InstrumentId,
    pub start: EventAddress,
    pub end: Option<EventAddress>,
    pub cause: UnavailableCause,
}

pub enum RangeStart {
    SegmentStart,
    After(EventAddress),       // exclusive
}

pub struct InvalidationRange {
    pub start: RangeStart,
    pub through: Option<EventAddress>, // inclusive; None is open-ended
}

/// One endpoint or an equivalence range of level-identical endpoints. A range
/// does not assert that any one member is the unique venue frontier.
pub struct AnchorPlacement {
    pub first: EventAddress,
    pub last: EventAddress,
}

pub enum AuditControl {
    VerifiedAt {
        instrument: InstrumentId,
        anchor_record: EventAddress,
        placement: AnchorPlacement,
    },
    Invalidated {
        instrument: InstrumentId,
        range: InvalidationRange,
        discovered_at: EventAddress,
        previous_verified_through: Option<EventAddress>,
        anchor_record: EventAddress,
        candidate_placement: Option<AnchorPlacement>,
        recovered_frontier: Option<EventAddress>,
    },
    AuditCoverageLost {
        instrument: InstrumentId,
        discovered_at: EventAddress,
        reason: AuditFailure,
    },
    RecoveryAt {
        instrument: InstrumentId,
        at: EventAddress,
        closes_control_id: Arc<str>,
    },
}

pub enum AuditFailure {
    AnchorPending,
    AnchorTooOld,
    AmbiguousAnchor,
    MissingSuffix,
    Divergence,
    JournalBoundExceeded,
}
```

Controls live in an immutable artifact, not the base segment:

```
audit-overlays/<overlay_address>/
  controls.ndjson.zst
  manifest.json
  receipt.json                 # commit marker, written last
```

The versioned, domain-separated overlay address binds the base segment address,
auditor version/policy, exact anchor input receipt identities, and canonical
control bytes. Controls sort by `(discovered_at, instrument, control_seq)`. A run
pins zero or one complete overlay identity; arbitrary overlays are never merged.
A changed auditor, input, or policy produces a new address. Re-running identical
inputs verifies the existing receipt as a no-op or reports conflict. A later
audit creates a new complete overlay that names the prior overlay it supersedes;
it never appends to a committed file.

`controls.ndjson.zst` is append-only while its private build transaction is open;
the receipt seals it. “Append-only control stream” means new facts do not rewrite
canonical evidence or old controls, not that a committed compressed frame can be
mutated. The manifest records control count, logical/stored identities, base
segment address, anchor receipts, auditor policy (including journal bounds), and
superseded-overlay identity when present.

A fresh strategy run merges controls when `ReplayCursor` reaches
`discovered_at`. Reconciliation of an already committed strategy run reads that
same pinned overlay and the strategy's recorded `BookDependency` values, then
appends strategy-owned void records. The projector/auditor does not know what an
episode is.

**Worked invalidation.** Strategy episode E records Polymarket dependency
`[320,340]` and Kalshi dependency `[700,706]`. A control discovered at coordinate
501 invalidates Polymarket `(300,430]`. E intersects and the strategy appends
`EpisodeVoided { episode_id: E, control_id, overlapping_dependency }`. An episode
depending only on Polymarket 450+ remains. Canonical evidence, the original
episode, the control, and the void are all immutable.

### 7.9 CLI

```
python -m replay.scope resolve                    (§3.1, Python)
    --universe   URL|PATH    Universe server or its SQLite file
    --event      ID …        umbrella event ids
    [--to        INSTANT]    explicit window end; default: retirement
    --scope-out  PATH        scope.json
    --specs-out  PATH        specs.json  (§9 generator, same contexts)
    [--stage     DIR]        fetch reaped windows for the scope into a local
                             canonical root via the bounded stager (§5.2 stage 0)

replay-engine build
    --scope      PATH …      one or more scope descriptors; one decode pass
                             per window, fanned out to all of them (§5.8)
    --canonical  PATH        local canonical root (receipts + objects)
    --out        DIR         receipt-committed <address>/ segment directory

python -m replay.publish_segments
    --segments   DIR         receipt-committed local segment root
    --config     PATH        ObjectStore destination; publish final segments (§5.9)

replay-engine audit
    --segment    PATH        immutable base segment
    --out        DIR         receipt-committed audit overlay

replay-engine run
    --scope      PATH        build in-process without materialising a segment
  | --segment    PATH        a materialised segment, or its manifest
    --canonical  PATH        required with --scope
    [--overlay   PATH]       zero or one verified audit overlay
    --specs      PATH        strategy specs, JSON
    --out        DIR         receipt-committed run directory containing
                             episodes, revisions, intervals, analytics, summary
```

---

## 8. Output contract

### 8.1 Episode NDJSON

One episode per line, self-describing: strategy, spec hash, segment and optional
overlay addresses, which instrument each leg used, generic book dependencies,
and bounded gross aggregates.
`revisions.ndjson` carries every economically relevant fill/input change needed
for exact netting and median calculation. Neither carries a netted number or
verdict — those are §9's to add.

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

Day 0 emits `Measurement` only. Audit discrepancy does not create a producer-
defined episode kind; each strategy's explicit policy determines whether to emit
an append-only void (§9.1).

### 8.3 Run transaction and sink failure

All strategy output sinks write into one private run directory. The strategy
streams and fsyncs each file, writes a manifest containing every stored/logical identity,
atomically renames the directory, fsyncs its parent, then publishes the run
receipt last. Any sink error aborts the run: no partial file is a committed
result. Retry with the same segment/overlay/spec/engine identities either verifies
the committed run as a no-op or conflicts. A later reconciliation writes a new
receipt-committed void artifact; it never appends to the committed run directory.

---

## 9. Python side

Consumes strategy-owned episodes, revisions, dependencies, and void records from
a committed run. Scales with episode/revision count,
so it is free to be slow, exploratory, and rewritten weekly.

### 9.1 Strategy-owned discrepancy reconciliation

The optional two-pass auditor (§12.9) reads only base evidence and writes only
the overlay in §7.8. It does not read episodes and does not decide whether a
strategy result remains valid.

A strategy choosing audit sensitivity records the `BookDependency` values used
for every decision. In a fresh run it receives controls at their deterministic
`discovered_at` coordinate. If an overlay is produced after a run, the strategy's
reconciler intersects that same control range with its prior dependencies and
writes a separate receipt-committed void artifact. Original episodes remain
immutable. No result is silently rerun, re-typed, or omitted by a central layer.
Strategies that intentionally do not depend on price-path audit may choose a
different policy, which must be versioned and included in their run identity.

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

A pull client (§6.6), not a producer-owned stage. Same tape and book projector,
own evaluation cadence and output stream.

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

**Across canonical windows — the eventual primary axis.** Phase 1 is serial.
After profiling, a configured bounded worker pool decodes each window once and
fans events out to every scope (§5.8). Worker completion order never changes
window concatenation order. This is the big lever that can make prebuild cost one
pass over the day regardless of how many bundles it covers without making thread,
file-descriptor, memory, or staging use proportional to window count.

**Across scopes — do not.** Scopes are independent, which makes one process per
scope tempting. It multiplies the decode by the scope count, which is the cost
§5.8 exists to remove. Scopes are the fan-out inside a window's pass, not a
parallel axis of their own.

**Within strategy evaluation — strategy-owned and deferred.** The producer stays
serial. A strategy may later parallelize its read-only work, but the first
implementation proves deterministic serial pull/output before adding barriers or
worker pools.

---

## 12. Intentional gaps

**12.1 Job dispatch.** Resolver ownership, output, and its Universe source are
settled and implemented (§3.1); only the contract test and two response fields
remain. What is not built is the path from a UI action to a run — a job carrying umbrella event ids, an optional window end, and a policy
version, queued by the Universe server, executed by
`resolve → build → run`, and landing in the results ledger the UI reads.
`EVENT_UNIVERSE_STORE_V1.md` disclaims being a replay planner, so the queue is a
thin dispatch and the resolver stays on the replay side.

**12.2 Segment index, publication, and eviction.** The prebuild cron needs, before
it can run unattended: a SQLite index (`rusqlite` is in the workspace) from scope
digest to address, and from address to local path, archive key, byte size, and
last read; the Python `segments/` publisher and segment archive receipt kind
(§5.9); and local eviction by the dual-receipt rule under a byte budget. Archive
garbage collection and deletion authority are explicitly outside V1. These
items block the prebuild cron, which is why it waits until Phase 4 (§4).

**12.3 Fill model.** No `Simulation` episodes until a named, defensible model
exists. Position-dependent families wait on it.

**12.4 Position state containers.** Declared on the spec; only stateless families
implemented day 0.

**12.5 Cross-venue fungibility derivation.** `FungibilityTier` is on the spec from
day 0 and the engine refuses substitution or locked baskets on non-`IDENTITY`
legs. The tier is not yet *derived* automatically, so cross-venue families wait.

**12.6 Representation and parsing.** Phase 0 adapts the decode → normalize →
prepare mutation → atomic publish pattern from the exact Bitfrost repository
named in §4. It freezes integer price/quantity representation, scales, checked
decimal parsing/rescaling, overflow, absolute/relative size, side, and contract
orientation. Fee formulae, effective schedules, payout-currency conversion, and
strategy-specific rounding wait for Phase 3B. Because the reference repository
has no discoverable license declaration, its code is not copied verbatim without
explicit permission.

**12.7 Kalshi.** Its splice remains unverified against live servers
(`ARCHITECTURE.md` §8). Kalshi legs stay out of headline results until a live
segment exists.

**12.8 Live driver.** Same books, same state machine, same pull contract, and the
same normaliser (§5.2 stage 5) fed from a socket instead of a canonical window.
Not built, but client strategies are forbidden from requiring producer callbacks
or anything only a historical replay has. The normaliser remains independent of
receipt mechanics.

**12.9 Polymarket's book hash: what it is, and what we may conclude from it.**

The completed evidence is retained in
[`POLYMARKET_HASH_REPLAY_EVIDENCE_2026-09-04.md`](POLYMARKET_HASH_REPLAY_EVIDENCE_2026-09-04.md).
Over 24 assets, 216/216 post-bootstrap REST snapshots had exact WebSocket
candidates and the official full-book SHA-1 reproduced 240/240 REST hashes. REST
and `price_changes[].hash` therefore share an observed asset-scoped full-state
hash space.

That does **not** make a hash a per-delta or per-delivery certificate. Ten cases
had two consecutive same-hash WebSocket delivery candidates: the first state
differed from REST at one level and the second matched. Replay must preserve all
canonical addresses on disk and compare delivery-end states. V1 does **not**
retain a full state per candidate, impose a maximal contiguous-run rule, or make
a later hash recurrence automatically ambiguous. The auditor assesses hash,
exact full levels/metadata, canonical order and time, and lineage after the prior
accepted anchor. If several endpoints remain level-identical, it records their
range and makes only conclusions valid for every placement; it does not pick the
nearest occurrence.

The SHA-1 preimage includes market, asset, timestamp, both sides, minimum order
size, tick size, negative-risk status, and last trade. Reproduction is an
independent full-snapshot integrity check. It does not prove every metadata
transition arrived, every delivery is complete, or H2→H3 suffix completeness.
Polymarket publishes no dense source sequence. Our delivery index proves recorder
accepted-order continuity only.

**V1 audit is a separate two-pass stream.** Pass one spools compact REST-anchor
metadata and identities; pass two reconstructs WebSocket state once, compares
delivery-end states, and emits the immutable overlay in §7.8. It never gates the
base projector, stores a full state for every hash occurrence, resets the base
book, or reads strategy output. Canonical evidence remains the lossless address
journal on disk. This is exact relative to the captured tape; without a dense
venue sequence it is not proof that the venue emitted a complete suffix.

**Optional one-pass audit uses a previous-anchor checkpoint and compact journal,
not candidate-state retention.** Per asset it keeps current levels, one latest
accepted-anchor full checkpoint, compact normalized deltas with canonical
addresses, and small pending-anchor metadata. Time, event-count, and byte caps
bound each journal, and a separate global byte cap bounds the sum across a large
bundle. A scratch book starts from the checkpoint and replays only the needed
delta range. Overflow never makes the healthy base projector unavailable:
it emits `AuditCoverageLost`, and an operator may explicitly run the separately
identified two-pass audit. There is no silent fallback or hidden second tape
walk inside one receipt.

**Worked previous-anchor bound.** A0 is accepted at coordinate 100. Store its
full levels once; journal normalized deltas/addresses 101–500 while the primary
captured-state book advances normally. REST anchor A1 is received at 501 but
describes a state near 420. Copy A0 into scratch, replay 101–420, and compare A1
using its hash, exact levels/metadata, order/time, and A0 lineage. If it matches,
emit `VerifiedAt`; the primary book at 500 is untouched. If hash candidates exist
but none has exact levels, emit an invalidation discovered at 501 covering
`start = After(A0), through = None`; do not reset the current book or guess a
suffix start. A later valid in-stream full book at 550 can emit `RecoveryAt` and
close that range through 549. If the journal cap already evicted 101–420, emit
`AuditCoverageLost` rather
than asserting divergence, and keep the base captured-state book runnable. An
explicit two-pass retry gets its own overlay address and receipt.

The observed anchor→receipt maximum of **155.659 seconds** falsifies any shorter
time cap but does not establish a production bound. A one-pass optimization may
replace the two-pass auditor only after a predeclared 24-hour minimum, preferably
72-hour, stratified run is byte-identical in overlay controls and ranges. Tests
cover startup, reconnect, split same-hash deliveries, unchanged and
non-contiguous recurrence, multiple level-identical placements, late candidates,
stale H2/current H3, parse/continuity faults, every cap overflow, and explicit
retry identity.

**12.10 No heuristic reset or hidden fallback.** Source/request/receipt times may
constrain candidate lineage and be reported, but do not by themselves certify a
frontier. Arrival-time comparison, nearest-time selection, arbitrary equal-hash
selection, applying an old REST anchor to the current book, and changing audit
mode inside one committed attempt are prohibited. Failure to place or replay an
anchor is an audit-control outcome, not grounds to discard otherwise runnable
base data.

**12.11 Rust object retrieval.** Closed for V1 by stage 0 (§5.2): the Python
resolve step stages stored archived windows into a local canonical root through
the bounded ObjectStore stager, and Rust re-verifies on read. A Rust client via
the `object_store` crate would remove the staging step and is worth doing only if
the prebuild cron's staging time becomes the bottleneck. The receipt check is the
guarantee; the transport is not.

**12.12 Deployment and operations.** Replay runs on a separate worker/server with
its own local canonical staging volume, segment/run volume, byte quotas, and
archive read credentials. It queries the separately deployed Universe API; the
heavy combiner does not live inside the Universe API process. One-shot jobs are
owned by an external scheduler/queue and carry an idempotency key, lease with
expiry, attempt count, input identities, and output address. Phase 4 defines
metrics and alerts for queue age, lease expiry, staging pressure, reject counts,
blind intervals, verification mismatches, publication failure, and cleanup.
Credentials are least-privilege: replay reads canonical archives, the Python
publisher writes immutable segment objects, and neither can delete archive data.

---

## 13. Known limits

- **Segment order and `event_index` are serialization orders.** `visible_ns`
  carries real timing. No lead-lag conclusion rests on either.
- **Captured-state availability is not venue completeness.** A healthy
  WebSocket reconstruction is intentionally runnable before delayed REST audit.
- **The engine cannot observe queue position, acknowledgements, or fills.** An
  episode is a displayed-depth opportunity, never labelled captured or filled.
- **The fee envelope is conservative, so trigger recall is bounded below.** An
  episode the envelope excluded is invisible to exact netting. The bound is
  recorded so the loss is quantifiable.
- **Polymarket first-pass episodes can be emitted on a book that was already
  wrong**, for up to one poll interval before divergence is detectable (§6.5).
  An optional overlay later identifies a conservative invalid dependency range;
  only the strategy can void a dependent decision (§9.1). Hash candidates can
  locate a historical state (§12.9) but do not eliminate uncertainty across a
  known gap or prove the current state.
- **The shared hash space is strongly evidenced, not a protocol guarantee.** It
  held for 216/216 post-bootstrap snapshots and official SHA-1 reproduction held
  for 240/240. The sample had no reconnect or tick-size change and does not prove
  non-recurrence, a production journal bound, or venue-complete delivery.

---

## 14. Current A–I approval ledger and completion gates

This ledger interprets the latest A–I response as a simplification of the
architecture, not as approval of the earlier recommendations. Only explicit
choices are marked approved. The prior numbered ledger is retained in Appendix A
as response history and has no normative force.

| Item | Status | Current disposition |
|---|---|---|
| A — same-hash candidates | **Prior rule rejected; replacement pending** | Mandatory contiguous-run retention, a full in-memory state per candidate, and automatic ambiguity on later recurrence are rejected. Canonical addresses still remain on disk. Assessment by exact levels/metadata, order/time, and prior-anchor lineage is the proposed replacement. The bounded checkpoint/journal optimization in §12.9 is not approved. |
| B — strategy triggers | **Approved redirection** | The producer owns only ordered atomic mutation and a pull cursor. Each strategy owns filtering, pull/evaluation cadence, dependencies, and output. No trigger registration or producer callback remains. |
| C — malformed inputs | **Approved intent; ownership corrected by repository invariant** | Malformed canonical envelope/provenance fails in `indexer-finalize`; malformed venue/book/control payload fails in strict Rust normalization. The payload-agnostic finalizer cannot perform venue validation. `book` receives only closed events or an already classified `NormalizationFault` and never interprets malformed wire. |
| D — REST prerequisite | **Approved redirection** | A healthy WebSocket full book initializes a runnable captured-state book. REST is delayed optional audit evidence, never a universal promotion gate. |
| E — previous-anchor bound | **Pending** | §12.9 gives the requested concrete checkpoint, journal, overflow, retry, and receipt example. Recommended V1 default: ship the separate two-pass auditor first and defer bounded one-pass audit. |
| F — discrepancy consequences | **Direction approved; artifact details and Clip pending** | The producer never reruns or voids strategies. An immutable overlay reports exact invalid dependency ranges; strategies append their own voids. §7.8 gives a worked example. Whether to use the proposed one-overlay-per-run identity and whether the base selector uses `Clip` remain unapproved. |
| G — certification modes | **Approved rejection/simplification** | No certified-only headline mode or separate “uncertified diagnostic” run type. Replay uses `AllowUncertified`, strictly verifies bytes/receipts/provenance, preserves coverage faults, and allows healthy lanes to run. |
| H — unknown attribution | **Conditional; minimum pending** | Unknown requested books remain `NotInitialized` rather than poisoning a whole scope. A minimal adapter-owned lane→venue/stream role is still needed to tell a missing required book lane from a missing optional snapshot lane; no deployment-wide map is proposed. |
| I — representation/parsing | **Approved** | Freeze the representation/parser/book boundary before economics. The exact intended reference is `github.com/hridyansh07/bitfrost-prime-take-home`; adopt its architecture, not verbatim unlicensed code. Fee/economic policy remains Phase 3B. |

### 14.1 Smallest remaining decisions

1. **Input lower bound.** Keep `[T0 − prologue, T1)` as the exact base input and
   use `LowerBoundPolicy::Clip` after auditing the complete first storage window?
   This only excludes pre-bound records; it never substitutes a prior price or
   imposes tick tolerance. **Recommended: yes.** The alternative,
   `ExpandToWindowStart`, changes segment bytes/address and exposes earlier state.
2. **Minimal attribution.** Approve the normalizer adapter registry's stable
   `lane_id → venue/stream role`, used only so a missing book lane leaves its
   requested books `NotInitialized` while a missing REST lane removes audit
   coverage? For example, a missing recognized `polymarket` WebSocket lane affects
   requested Polymarket books; a missing `polymarket_snapshots` lane affects only
   REST audit. The finalizer proves the lane fault but, because it is payload-
   agnostic, cannot infer those affected Replay scopes. **Recommended: yes.**
   Custom/twin lanes then require explicit adapter configuration rather than
   guessed attribution.
3. **One-pass audit.** Defer it and ship only the receipt-bound two-pass overlay,
   or include the bounded previous-anchor optimization later? If included,
   approve `AuditCoverageLost` on overflow plus a separately addressed/receipted
   two-pass retry, never silent fallback. **Recommended: defer until the 24–72 h
   equivalence study.**
4. **Overlay selection.** Approve one complete immutable overlay identity pinned
   per strategy run, with a new complete superseding overlay for changed evidence
   or policy and no arbitrary merge of independently produced overlays?
   **Recommended: yes;** this avoids undefined control ordering and duplicate
   invalidations.

### 14.2 Revised completion gates

1. **Phase 0 boundary integrated.** Universe responses pin context identities;
   the Phase-0 selector rejects gaps/overlaps; the audited stream reaches EOF and
   `finish()` before segment commit; `AllowUncertified` preserves coverage facts;
   normalization follows the approved representation/parsing split.
2. **Serial reconciler conformant.** Every normalized child preserves canonical
   address, tie group, source provenance, and continuity. Malformed venue/control
   data becomes a receipt-bound reject plus classified fault before `book`.
3. **Base projector correct.** A WebSocket full book initializes without REST;
   pull updates never expose partial deliveries/tie groups; duplicates do not
   mutate; conflicts, known continuity faults, impossible relative mutations,
   reconnects, and overflows have deterministic availability outcomes.
4. **Audit overlay correct.** Two-pass output is immutable, ordered, base/input-
   bound, and independently verified. It covers split/repeated hashes, delayed
   H2/current H3, level-identical placements, missing suffixes, open invalidation
   ranges, and explicit retry identity without modifying the base projector.
5. **Strategy contract correct.** Strategy-owned cadence is in run identity;
   every decision records generic book dependencies; a late control voids exactly
   intersecting decisions and leaves unrelated decisions unchanged.
6. **Economics and operations bounded.** Offline pinned fee vectors are exact;
   sink failures cannot commit partial output; workers, staging, temporary slices,
   and storage have measured caps; publication reuses the Python `ObjectStore`;
   V1 has no archive deletion authority.

Phase 0–2 may proceed without resolving the optional one-pass optimization.
Lower-bound and minimal-attribution decisions are required before a production
segment contract is frozen; overlay selection is required before Phase 3A commits
audit artifacts.

---

## Appendix A. Superseded numbered response ledger

> Historical context only. The decision requests below were superseded by the
> A–I response and must not be treated as current approval requirements.

The table below records the earlier numbered response on 2026-09-04. Its statuses
and decision requests are superseded by §14 and are retained only to explain how
the design changed.

| # | Decision | Status | Recorded disposition |
|---:|---|---|---|
| 1 | Offline Polymarket trust algorithm | **Approved** | Two-pass is the V1 production/offline oracle and default. One-pass remains experimental until the §12.9 differential gate passes. The old `BookReplay` also used two passes; the retired gate ladder made several complete tape walks, but that fact alone did not provide this address-preserving anchor/suffix contract. |
| 2 | Candidate completion | **Pending explanation/decision** | No run rule approved yet. Every canonical address must be retained regardless of the eventual rule. See §14.1.1. |
| 3 | Strategy evaluation cadence | **Conditional** | The user generally approved per-delivery evaluation but correctly conditioned observability on strategy semantics. No single global cadence is approved. See §14.1.2. |
| 4 | Unknown controls and unusable books | **Partial** | It is approved that fail-closed handling may make order books unavailable. It is not yet explicit whether every unknown control on a relevant attributed lane must stale its books, including a control later shown to be operational-only. See §14.1.3. |
| 5 | One-pass journal bound | **Approved as a measurement process, not a number** | Run a predeclared 72-hour study (24-hour minimum), then approve time, event, and byte caps together. No production bound is approved; below 155.659 seconds is already falsified. |
| 6 | `AnchorPending` horizon | **Pending clarification** | The user asked whether anchors are snapshots; they are the independently polled REST `/books` full-book responses. No timeout/finalization rule was approved. See §14.1.4. |
| 7 | One-pass failure handling | **Pending explanation/decision** | Whether an automatic but explicitly identified two-pass retry is allowed remains open. Silent fallback remains prohibited by the evidence contract. See §14.1.5. |
| 8 | Canonical lower-bound policy | **Not approved; premise corrected** | `Clip` removes canonical records before the requested bound after auditing the whole first window. It neither carries forward previous prices nor applies a tick tolerance. See §14.1.6. |
| 9 | Canonical certification policy | **Pending explanation/decision** | No headline/diagnostic policy was approved. See §14.1.7. |
| 10 | Initial Polymarket trust | **Partial** | Starting provisional was approved. “Until first snapshot” is ambiguous between an in-stream WebSocket `book` bootstrap and an independent REST `/books` anchor, so the promotion condition is not approved. See §14.1.4. |
| 11 | Deployment lane attribution | **Pending explanation/decision** | `indexer-finalize` detects missing/invalid lanes and preserves provenance, but cannot infer which Replay instruments a wholly absent or pre-prologue lane carried. See §14.1.8. |
| 12 | Money and fee inputs | **Conditional** | Fixed-point/economic semantics are approved only in conjunction with the economics implementation. The minimum boundary primitives versus deferred fee policy still need confirmation. See §14.1.9. |

### A.1 Prior clarifications and decision requests

#### A.1.1 Same-hash candidate completion (#2)

A *maximal contiguous same-asset/same-hash candidate run* is the longest sequence
of adjacent canonical WebSocket deliveries for one asset carrying hash H, ending
before that asset next carries a different hash. Example:

```text
address 40  asset A  hash H  apply size 2  → levels do not match REST H
address 41  asset A  hash H  apply size 3  → levels match REST H
address 42  asset A  hash J                → H run is closed at address 41
```

The ten observed split candidates had this shape. Address 40 cannot be erased:
it proves per-delivery certification is false, can delimit retrospective distrust,
and may have been visible to a causal strategy. Address 41 is the evidenced H
frontier. Pooling all H occurrences would also confuse a later non-contiguous H
at address 90 with this run. What remains uncertain is whether maximal-contiguous
completion is a stable venue rule; the sample saw no reconnect or non-contiguous
WebSocket recurrence.

**Decision request:** approve the maximal contiguous same-asset/same-hash rule as
a versioned V1 *anchor-candidate* rule, with every constituent address retained
and any non-contiguous recurrence reported as `AmbiguousAnchor`?

#### A.1.2 Mutation, evaluation, and observability (#3)

Book mutation order is not strategy-specific: normalized changes apply in
canonical order, after completing only proven atomic boundaries (one vendor
delivery and one `visible_tie_group`). Evaluation cadence is strategy-specific.
A price/depth strategy must observe every completed relevant mutation group or it
can miss a 20 ms crossing. A strategy based only on terminal resolution, a game
score, or a minute bucket need not evaluate on an unrelated quote change.

Least restrictive deterministic contract: each versioned consumer declares its
input event classes and evaluation trigger; dispatch invokes it after every
completed atomic group that changes one of those declared inputs. No consumer may
observe a partial vendor delivery or partial visible tie group. Same-hash runs do
not globally delay mutation or evaluation. The trigger declaration is hashed into
the strategy specification.

**Decision request:** approve this per-consumer trigger contract, with
price/depth consumers triggered after every relevant completed mutation group?

#### A.1.3 Unknown controls (#4)

The control's exact canonical envelope may contain anything and is always
preserved. Replay must not invent semantics for an unknown shape. A known
state-neutral control such as a recognized heartbeat may be retained without
changing a book. A malformed or unknown control on a relevant lane is written to
the reject sidecar; if its effect cannot be proven state-neutral, every instrument
attributable to that lane becomes `Unusable` at that address. Strategies then see
“book unavailable,” never a guessed mutation. If lane attribution itself is
unknown, #11 decides whether the whole scope becomes unobservable.

**Decision request:** approve “recognized state-neutral controls do not stale;
unknown or malformed controls stale every attributable book, while preserving the
exact record and reject”?

#### A.1.4 Anchors, pending, and first snapshot (#6 and #10)

The independent anchors in this design are full-book responses from the separate
Polymarket snapshot lane polling REST `POST /books`. A WebSocket `book` event is
also a full book and can bootstrap/reset state at its own stream frontier, but it
is not an independent audit of that same WebSocket delivery chain. Therefore
“provisional until first snapshot” settles the initial state only if “snapshot”
means a successfully placed independent REST anchor, not merely the subscribe-time
WebSocket book.

`AnchorPending` means the REST anchor exists but its matching WebSocket candidate
or candidate-run end is not yet in the observed canonical stream; six such
post-receipt arrivals occurred. Recommended deterministic horizon: in one-pass,
keep it pending until the candidate run closes or the approved journal bound
evicts its possible frontier (`AnchorTooOld`); at selected-input end, retain a
right-censored `AnchorPending`. Offline two-pass examines the entire selected
interval but may not read beyond `T1` merely to manufacture resolution.

**Decision request:** confirm that promotion requires the first successfully
placed independent REST anchor plus required suffix handling, and approve the
pending horizon above?

#### A.1.5 Explicit retry versus silent fallback (#7)

Silent fallback means one committed run starts with bounded one-pass semantics,
encounters an old/ambiguous anchor, secretly rereads arbitrary history with
two-pass, and still publishes the same mode/attempt identity. Operators cannot
predict resources, and two identical requests can take different paths while
appearing equivalent.

An explicit retry records `attempt 1 = one_pass, AnchorTooOld`, then schedules
`attempt 2 = two_pass` with its own mode, input identities, resource class, logs,
and receipt. The final verdict points to attempt 2 and retains attempt 1's failure.
This is reproducible and operationally schedulable, but costs another tape walk.

**Decision request:** allow that explicit two-pass retry, or require the asset to
remain unavailable until a later full anchor with no automatic retry?

#### A.1.6 Lower-bound clipping is not price imputation (#8)

Canonical windows are 30-minute storage units. If the requested Replay input
starts at 10:07, `Clip` audits the whole 10:00 window but yields only records at
or after 10:07. It does **not** use a price from 10:06:59, estimate a price, or
accept movement within a percentage/tick tolerance. The separately requested
prologue normally starts before analytical T0 so a real full book can bootstrap
state; if no admitted full book exists, the book remains unavailable.

Carrying forward a previous price would be a separate imputation policy and is
not recommended for V1. A percentage/tick bound belongs to a strategy's staleness
or sensitivity policy after a real book exists; tick size bounds legal price
increments, not how far an unseen book may have moved.

**Decision request:** approve `Clip` for exact interval selection, with no
previous-price imputation and `NotBootstrapped` until real full-book evidence in
the admitted prologue/interval?

#### A.1.7 Certified canonical windows (#9)

The finalizer may commit an immutable window even when expected evidence was
missing or unsafe, so healthy lanes are not blocked forever. Its receipt sets
`certified = complete && clock_faults.is_empty()`. An uncertified window may name
a missing lane, an invalid/excluded lane, or a cross-window clock fault. The bytes
and provenance can still verify perfectly; what failed is the claim that this is
a complete, normally ordered deployment window.

`RequireCertified` rejects a headline Replay if any selected window is
uncertified. `AllowUncertified` admits it only with the false certification and
fault details preserved; Replay can then produce diagnostic/unobservable
intervals but must not silently call absence “no opportunity.”

**Decision request:** require certified windows for headline results and permit
uncertified windows only in a separately typed diagnostic run?

#### A.1.8 What the finalizer cannot attribute (#11)

`indexer-finalize` proves which deployment lanes were expected, present, missing,
or invalid; verifies every admitted envelope/provenance pair; and preserves each
record's lane and continuity verdict. It deliberately does not parse venue
payloads or know Replay scopes. Combining canonical receipts can therefore detect
“lane `polymarket` was missing,” but not “that lane carried asset A in this Replay
scope.” A wholly missing lane has no `connection_opened`; a selected prologue may
also begin after the subscription record that named its assets.

A versioned deployment lane-role map supplies the stable lane→venue/stream role.
Runtime subscription evidence can narrow that to instruments when present. It
does not duplicate canonicalization; it maps a known capture fault to affected
Replay scopes.

**Decision request:** approve the versioned lane-role map and fail the affected
scope closed when neither it nor runtime evidence can prove a missing lane
irrelevant?

#### A.1.9 Boundary primitives versus economics policy (#12)

Phase 0 must freeze only what the typed normalizer/book needs to be deterministic:
integer-backed price and quantity types; explicit scale/unit metadata; exact
decimal parsing; checked overflow; explicit absolute-versus-relative size; side
and contract orientation; and no implicit cross-venue arithmetic. These choices
determine whether two segment builds encode the same event and cannot wait for fee
implementation.

Economics can defer the venue fee formulas, effective schedules, conservative
trigger envelope, payout-currency conversion, SDK choice/version, and fee-rounding
vectors until Phase 3—but all must be pinned, offline, and fixture-verified before
headline output. No current venue service may price historical evidence.

**Decision request:** approve that split: freeze representation primitives in
Phase 0, and freeze fee/conversion policy with the Phase 3 economics module before
headline use?

### A.2 Prior phase acceptance

1. **Phase 0 boundary integrated.** Universe responses pin all context identities;
   the lane-role map and approved selector policies are versioned; the Phase-0
   selector rejects gaps/overlaps and always clips `T1`; the audited stream is
   consumed to EOF and `finish()` is required before segment commit. Repeated
   runs resolve identical bytes and receipt identities.
2. **Serial reconciler conformant.** Every normalized child preserves canonical
   sequence, lane/delivery/event address, tie group, source provenance, and exact
   continuity verdict. Duplicate never mutates; conflict/fault/reject stales the
   affected book or fails the scope closed when attribution is unknowable. Every
   reject is committed and counted. Independent readers verify local output.
3. **Two-pass engine correct.** Address-preserving anchor fixtures cover split
   runs, recurrence, late arrival, H2/H3 recovery, startup, reconnect, all five
   anchor outcomes, malformed/overflowing relative deltas, unknown controls,
   no-full-book, sink failure, and retrospective episode demotion. Repeated runs
   are byte-identical and no provisional interval enters headline results.
4. **Economics exact.** Every repricing input is in the revision stream; offline
   fee vectors agree with hand calculations; substitution closes/reopens; net
   outputs are deterministic and independently readable.
5. **Operations bounded.** Publication is a separate Python `ObjectStore` client;
   leases and retries are idempotent; staging, workers, open files, journals, and
   temporary slices have measured limits; receipt verification precedes local
   eviction; V1 has no archive deletion authority.
6. **One-pass replacement, if pursued.** Complete the predeclared 24–72-hour gate
   in §12.9 with zero unsafe outcomes and byte-identical book/trust results versus
   two-pass before making it production or allowing its output into headlines.

Replay Engine V1 is complete only after approved items 1–5 pass on real canonical
windows and adversarial fixtures. Item 6 is not required for V1; until it passes,
two-pass remains the recommended production/offline trust path and one-pass is
explicitly experimental.
