# Verified derivative walker V1

Status: independently reviewed design, implemented in `replay-materialize` and
`replay-tape`. This document specifies the generic traversal boundary only.
No production lower-bound policy is selected or approved by this document.

## Baseline and authority

- Requested `rework/replay-kalshi-final` was merged as PR #30 and deleted before
  this work started. GitHub reports its final head as
  `17c38d7e50b8192c95ff94770519a7949c757a1e`; this was the initial implementation base,
  not the older parent-thread reference `acd651b8e084ea3d678c391e8c0d214b29ab2e99`.
- PR #30 was squash-merged into `master` as
  `c4c09b99b786cb2bfab657e88c8cebaad92bf1ea`. The walker-only commits were
  subsequently rebased onto that commit; `master` is the repository's default
  branch (there is no `main`). Review against `master`, not the old PR ancestry.
- The authoritative Replay design is `docs/REPLAY_ENGINE_V1.md` at
  `origin/rework/replay-pipeline`, commit
  `a3caab228a4a5f9d56694875be8c06a929d691d9`. Do not merge that branch to obtain it.
- Its sections 5.2.1, 5.3–5.6, 6.2, 6.4, and 14 govern this boundary. The newer
  task defers overlays and requires historical-pin verification. Existing
  schema-3 domain types supersede the illustrative older money/event types.
- Prior design thread: `T-01a067ff-043d-71fe-b9fd-7f581d2b960c`; prior implementation
  review: `T-01a09c11-f4e7-7458-90e7-4d7c1230eca4`. Neither records approval of
  Clip versus ExpandToWindowStart. Minimal adapter lane attribution also remains
  unapproved, but generic caller-supplied filtering can avoid deciding it here.

## Baseline gaps addressed by this implementation

At the base revision, `replay-materialize::verify_derivative` verified receipt/manifest bindings,
canonical JSON, compressed identities, contiguous child indexes, source disposition
counts, and reject/fault pairing. It returns metadata, not an open read capability.
Its public metadata fields are mutable, and its compressed paths are reopened in
successive verification passes. Calling it and then opening the original events
path is not a verified-open lifecycle.

The base verifier accepted only current format constants, and address
recomputation uses current event/reject/materializer version constants. Its
coexistence test changes bundle and source receipt identities, not schema or
materializer versions. No walker, atomic-group reader, or projector exists.

`BookKey { instrument, orientation }` already exists in `replay-domain`; the
`instrument` field is the requested `instrument_id` concept. Reuse it rather than
rename it or add a competing key. FullBook, BookDelta, and AuditAnchor have
`book_key()` accessors. Kalshi Outcome and Complement remain separate keys.

## Ownership and API

`replay-materialize` owns strict format dispatch, pinned open, source-disposition
joining, immutable per-window verification, and opaque completion capabilities.
It may parse the existing reject sidecar to verify its source binding; the
walker and book-facing API never receive raw canonical envelopes or venue JSON.

New crate `engine/crates/tape`, package `replay-tape`, depends only on
`replay-domain` and `replay-materialize` at runtime. It owns interval selection,
adjacent-window traversal, scope filtering, atomic groups, and traversal counters.
There is no dependency on venue adapters, book, strategies, or a new canonical
reader. The existing transitive finalizer dependency stays in materialize.

Public signatures (opaque types have private fields; errors follow the existing
`String` style rather than introducing wrapper error types):

```rust
// replay-materialize
pub struct PinnedDerivative { pub directory: PathBuf, pub pin: DerivativePin }
pub struct VerifiedWindowReader { /* owned private snapshot and decoder state */ }
pub struct SourceDelivery { /* header, children, optional typed reject summary */ }
pub struct FinishedWindow { /* exact pin, source receipt, counts, bounds */ }

pub fn inspect_pinned(input: &PinnedDerivative, limits: &ReadLimits)
    -> Result<DerivativeMetadata, String>; // planning metadata, not event capability
pub fn open_pinned(input: &PinnedDerivative, limits: &ReadLimits)
    -> Result<VerifiedWindowReader, String>;
impl VerifiedWindowReader {
    pub fn next_delivery(&mut self) -> Result<Option<SourceDelivery>, String>;
    pub fn finish(self) -> Result<FinishedWindow, String>;
}

// replay-tape
pub struct WalkRequest {
    pub start_ns: u64,
    pub end_ns: u64,
    pub lower_bound: LowerBoundPolicy,
    pub scope: ScopeFilter,
}
pub struct DerivativeWalker { /* no public constructor or mutable views */ }
pub struct AtomicGroup { /* read-only deliveries and source span */ }
pub struct FinishedWalk { /* requested/effective interval, exact pins, counters */ }
pub enum WalkItem { WindowStatus(Box<WindowStatus>), Group(AtomicGroup) }

impl DerivativeWalker {
    pub fn open(inputs: Vec<PinnedDerivative>, request: WalkRequest,
                limits: ReadLimits) -> Result<Self, String>;
    pub fn next_item(&mut self) -> Result<Option<WalkItem>, String>;
    pub fn finish(self) -> Result<FinishedWalk, String>;
}
```

Use the repository's existing explicit-error style; no iterator that disguises
an error as exhaustion. `FinishedWalk` is a completion capability, not a persisted
segment receipt or a strategy result. A future bundle writer must require it to
commit; writing such a bundle is not this task.

## Verified open, races, and resource bounds

1. Validate the expected pin's address syntax before using it in paths. Read a
   bounded receipt, check its exact SHA-256 against the caller's pin, dispatch
   its wire version, and require the caller address, receipt address, and addressed
   directory name to agree. `inspect_pinned` verifies all manifest bindings and
   returns immutable planning metadata; open rechecks the same pin. The verified
   reader exposes immutable metadata getters.
2. Retain exact receipt/manifest bytes in bounded owned memory; copy events and
   rejects to an exclusively owned private temporary snapshot using bounded copy
   buffers. On Unix its directory mode is 0700 before content writes. Check per-file and
   total snapshot limits before copying and enforce them while copying. Never
   trust the original path's metadata as an identity or allocation size.
3. Verify the private snapshot completely against the pinned receipt: strict
   canonical metadata, all repeated bindings, address inputs, both compressed
   identities and frame EOF, domain invariants, source dispositions, paired
   faults/rejects, and complete equal-time tie semantics before any status or
   delivery escapes. Read exact expected lengths with an extra-byte check.
4. Rewind/reopen only files in that private snapshot for traversal. No verified
   handle is minted from a caller-supplied `VerifiedDerivative` value. Source
   replacement/deletion after snapshotting cannot change yielded records. A
   concurrent source rewrite during copy either produces the pinned bytes or
   fails verification; no automatic switch to a newer pin.
5. Streaming traversal also checks decoder EOF/finish and poisons on errors.
   Snapshot lifetime is owned by the reader and cleaned on drop. Hard-crash
   residue is operational work, not a new root-wide cleanup feature. Do not
   remove persistent advisory-lock pathnames or change materializer locking.

Metadata for the selected run is bounded by `max_windows` and
`max_metadata_bytes`. Only one complete stored window snapshot is retained at a
time; disk is bounded by `max_snapshot_bytes`. RAM is bounded by metadata limits,
scope/lane limits, fixed codec/copy buffers, `max_line_bytes`, and
`max_group_bytes`/`max_group_records`, plus one bounded lookahead.
Use bounded reads, not `read_until` followed by a size check. Count unfiltered
group data toward limits, so selecting one child cannot hide an oversized group.
Limit overflow fails the attempt; it never splits a group, drops a record, or
silently retries with another policy. Caller-retained output groups are caller
memory, not walker-retained history.

`ReadLimits::snapshot_root` selects an existing scratch directory; `None` uses
the system temporary directory. Production callers should set `Some(path)` on
the data volume, not rely on root-disk `$TMPDIR` capacity. `tempfile` creates a
private per-window child there; missing/unwritable roots fail without fallback or
automatic root creation. The reader deletes only its owned child on ordinary
drop or error. The 8 GiB default snapshot cap is per reader, not a free-space
reservation or process-wide quota; concurrent readers need an external budget.

Build verification intentionally uses default verification limits too: 16 MiB
per logical NDJSON line including LF, 1 MiB per metadata document, 64 MiB/100,000
records per atomic group, and 1,024 lanes. Pinned inspection additionally limits
combined receipt/manifest bytes to 1 MiB. These are operational limits, not new
wire-schema restrictions. An oversized candidate fails before receipt publication;
no truncation or partial commit is permitted. Changing these defaults therefore
requires considering builders as well as walkers. Build verification does not
create a private snapshot or impose the walker's selected-window/scope limits.

The current implementation retains three complete verification passes (pairing,
dispositions, source/tie semantics), then traversal. This is bounded-memory but
costs repeated decompression, including excluded tails. Consolidating the shared
verifier and delivery join is deferred as a separately tested refactor. Errors
remain diagnostic strings; callers must not classify retryability by matching
text. Every error invalidates the attempt. Any retry creates a fresh walker with
the same pins; no automatic retry policy is provided.

## Minimum version-coexistence mechanism

Freeze the currently undeployed format as the first supported wire profile:
manifest 1, receipt 1, normalized schema 3, event serialization 1, reject
serialization 1, reject record 1, materializer 1. Do not invent migrations for
abandoned experimental schemas 1/2 or pretend arbitrary future schemas are readable.

Separate writer-current constants from frozen reader-profile constants. Parse a
bounded version discriminator first, then decode through that exact closed
profile. Keep schema-3 validation/serialization available independently of a
future writer version. A future format change adds a reader profile and tests;
it does not retarget the old profile at changed types. Unknown profiles fail
before event interpretation. Semantic conversion to a shared runtime domain is
explicit, version-directed, and preserves original addresses/provenance; original
wire identity is always checked before conversion.

Address verification uses the receipt/manifest's validated profile and recorded
versions, never unrelated writer-current constants. Preserve the exact existing
V1 address preimage and reject-ID algorithm. Do not readdress old bytes, introduce
`latest`, rewrite receipts, or require rebuilding an old pin to verify it.
Bundle/config revisions can coexist without loading their old normalizer code:
the walker reads already-normalized records. A test-only second writer profile
can prove that changing current writer selection does not change frozen-profile
verification; unsupported real wire versions still fail closed.

## Source deliveries and semantic verification

Join events and rejects by canonical sequence, not by event address ordering
across lanes. Each source has exactly one disposition:

- accepted: one or more children, event indexes 0 through N−1;
- rejected: one event-index-0 NormalizationFault and its exactly paired reject;
- intentionally ignored: one event-index-0 sidecar entry and no event children.

Each delivery retains the exact header and ordered children. Child headers must
match byte-for-byte except for event_index; this includes lane, delivery index,
record ID, both clocks, tie ID, content/source identity, and continuity verdict.
Validate source-sequence density across the union, not just increasing event
sequences. Ignored sources fill real sequence positions. The first sequence may
be greater than one; it must be positive. Counts and the final union agree with
the manifest; checked arithmetic rejects overflow.

Per-lane source delivery indexes must strictly increase within and across
windows, including empty intervening windows and filtered sources. A bounded
`max_lanes` map remembers last indexes; it does not infer missing deliveries from
index gaps or recompute canonical continuity classifications.

A typed sidecar summary exposes reject ID, parser/error code, hint, impact, or
ignored reason, and its original source header. Exact rejected bytes stay in the
verified derivative, addressable through its pin, and never enter the immutable
book-facing group. A source with a paired fault is represented once, not as two
deliveries. Reject hints never override the typed FaultImpact.

Hash/receipt verification proves exact committed interpretation, not that a
normalizer was semantically correct. A consistently authored but incorrect
derivative requires a corrected normalizer and a new pin, not a walker repair.

## Window selection, interval, and ordering

Inputs name exact pins, never a directory scan selecting newest versions. Sort
metadata by source window start, then reject duplicate starts, conflicting pins,
overlaps, gaps, invalid/empty intervals, and redundant nonintersecting windows.
Require the unique minimal adjacent run covering `[start_ns, end_ns)`. Distinct
normalizer identities may be concatenated only as explicitly supplied pins;
retain their individual identities, never claim one bundle version for the run.

Concatenate in source window order. Never merge/re-sort by timestamps, instrument,
lane name, price, hash, or venue sequence. Within a window, traverse canonical
sequence then original child index. Check timestamps lie in the exact source
window and do not decrease within it. Do not recompute finalizer lane ranks.
Across nonempty windows the next source sequence must equal previous last + 1;
empty windows advance interval coverage but do not reset that expectation.
Check these invariants even for entirely filtered or clipped windows.

Upper clipping is always exclusive: no source at `visible_ns >= end_ns` is
exposed. All children of a delivery and all members of a tie have one timestamp,
so clipping includes or excludes the whole original group. Still verify the
entire first and last storage windows, including excluded tails.

**Unapproved lower-bound choice:** recommend `Clip`, so effective start equals
requested start. The alternative `ExpandToWindowStart` exposes earlier evidence
and sets effective start to the first storage-window boundary. These differ in
bootstrap state and eventual strategy results; they are not a performance choice.
`RequireWindowBoundary` can reject partial starts without choosing either.
The API supports all three existing policies, requires an explicit policy, has no
implicit default, and records it with requested/effective bounds in `FinishedWalk`.
Approval is still needed before a production caller selects its policy; §14.1
reserves that choice, while §14.2 gates the production segment contract, not this
generic policy mechanism.
Prologue calculation and analytical T0 are caller concerns. No warm-start price
imputation, hidden expansion, or episode suppression logic lives here.

## Atomic groups and pull semantics

One group is one complete delivery unless its source has a non-null
`visible_tie_group`, in which case it contains the complete contiguous source
tie run. The identity is window-qualified, not a globally unique integer.
The finalizer sets the tie ID to `visible_ns` exactly when an equal-time run
contains more than one lane. Verify the complete equal-time source run: all
members of a cross-lane run must carry `Some(visible_ns)`; every member of a
single-lane run must carry None. Same-lane equal-time deliveries remain separate
atomic groups. Reject split, reused, inconsistent, or incomplete tie membership.
Empty/ignored source deliveries participate in validation and boundaries. A tie cannot cross a
half-open source window because all members have the same in-window timestamp.
Same-hash Polymarket records are not groups and do not delay a pull.

The snapshot verification pass validates equal-time runs using scalar state:
timestamp, first lane, cross-lane flag, tag consistency, and counters. It does not
buffer a potentially large single-lane equal-time run; traversal buffers only
actual atomic groups. Malformed declared tags cannot prematurely release a group.

Read through a different source/tie or verified window EOF before returning the
last group. A decoder failure while closing a group returns an error, not the
partial group. Filter only after group completion. Preserve original source
first/last coordinates and child event indexes even when siblings are excluded;
never densely renumber filtered children. Expose read-only slices and getters,
not mutable record vectors or constructors that can forge verified groups.

The walker does not mutate books. A future cursor consumes one whole group,
prepares all affected changes against private state, and publishes atomically
before exposing a read-only BookStore view. Strategy skipping means repeatedly
pulling that cursor and declining evaluation after completed mutation; it must
not mean filtering mutations or jumping the tape. No seek/skip-to-time method,
strategy callback, or mutable book API is introduced here. A tiny test sink can
count applied groups to prove the cadence boundary without implementing books.

## Filtering, faults, and empty coverage

`ScopeFilter` is bounded, explicit input: requested `InstrumentId`s plus relevant
`LaneId`s. Instrument selection includes every orientation. No venue parsing,
Universe resolver, deployment lane inference, or implicit `1-p` projection occurs.
Callers are responsible for complete lane membership; the finished request
retains exactly the supplied sets. Lane inference/validation against a resolver
is deferred, not claimed correct by a syntactic filter.
Selected venues are the prefix before `:` in validated requested InstrumentIds,
never an interpretation of lane names.

- Keep instrument-addressed book/trade events for requested instruments.
- Keep controls on explicitly relevant lanes, including unnamed connection
  failures before the first selected instrument appears. Do not narrow or rewrite
  a kept control's payload/instrument list.
- Keep Instrument faults for selected instruments; RequestedVenueBooks and
  AuditCoverageOnly faults for selected venues; preserve UnattributedLane faults
  as explicit unattributed diagnostics rather than treating them as absence.
- Preserve all source continuity verdicts and source headers for kept delivery
  groups, including ignored delivery summaries on relevant lanes. Continuity
  faults on relevant lanes cannot disappear just because their child instrument
  is filtered out: retain a typed source-only diagnostic in that case.
- Duplicates stay visible and retain their payload/provenance. The future cursor
  must suppress duplicate mutation before processing events. Conflict, gap,
  backwards cursor, and broken counter remain distinct facts; the walker does
  not decide book availability or apply their payloads.
- Unsupported state-bearing messages arrive as paired rejects/faults, not no-ops.
  Unknown persisted event/continuity variants are fatal reader errors.
- Existing AuditAnchor events are retained as inert typed evidence for selected
  instruments. They never initialize books, gate evaluation, or generate controls.
  No new anchor producer, overlay, or historical invalidation is introduced.

Expose a separate window-status item at each verified window boundary, including
empty windows: pin, SourceReceipt, certification flag, counts, and interval.
These are traversal metadata, not fabricated tape events or mutation groups.
`next_item()` returns WindowStatus once before the window's first admitted group,
after the whole private snapshot has verified. It returns a status even when the
window has no admitted groups. Do not hide statuses in an unbounded history or
provide a convenience group-only API that silently drops them.

Frozen profile-1 metadata preserves only source `certified`. It reports
`coverage_details = NotRecordedInDerivativeV1`, `coverage() = None`, and
`connection_epoch() = None`; no details are invented or recovered from canonical
windows. New builds use the profile-2 extension below. Uncertified windows with
attributed upstream faults remain walkable; neither profile adds a certified-only
gate.

Count included/excluded sources and children, interval exclusions, rejects,
ignored sources, and groups. Scope exclusion is not normalization failure. Empty
selected results still return statuses and require verified EOF/finish.
A source is included when at least one child or required source diagnostic
survives; excluded children are counted separately. A reject/fault pair is one
source, not two.

## Implemented source-evidence profile 2

The normalized domain remains schema 3, with event and reject serialization 1.
Receipt, manifest, and materializer versions are **2**. Profile 1 retains separate
closed metadata wire readers (`materialize/src/profile1.rs`), exact canonical
byte verification before runtime conversion, and its original address algorithm.
No migration, readdressing, historical normalizer load, or changed venue adapter
is required. Unknown profile tuples and extra fields fail closed.

Profile 2 requires two additions:

1. `SourceReceipt.document` is the **exact UTF-8 canonical receipt document**, not
   a reserialization. Its bytes must match the existing source SHA-256/length and
   its interval/certification must match the projection. `CanonicalSelection::
   receipt_documents(maximum_bytes)` captures bounded bytes matching the selected
   identity; publication still requires the matching finished canonical audit.
   This does not alter canonical persisted formats or finalizer diagnoses.
2. `sources.ndjson.zst` is a third level-3, checksummed single-frame stream. Every
   source has exactly one closed record, in source-union order:
   `{"source_version":1,"header":<schema-3 EventHeader at child 0>,"connection_epoch":"..."}`.
   Both manifest and receipt bind its logical/stored identities. Its count equals
   `input_records`. It follows the same staging, fsync, receipt-last, snapshot,
   resource-limit, strict EOF, and poison rules as events/rejects.

The existing address preimage uses the profile-2 source projection (including
the exact document) and materializer version 2. That version fixes source
serialization 1; changing it needs another supported profile. Reject IDs keep
their existing algorithm but bind the new derivative address. Schema-3 accepted
event bytes and normalizer bundle identities do not change.

The sidecar header must match every child's source header, modulo child index,
and any rejected/ignored envelope must also match its epoch. One source owns one
epoch, so children cannot disagree. This is the splice's connection identity,
not a venue cursor/version or a lane-global/venue-global reset. Reconnects are
visible even without opening controls in the selection. No epoch inference,
duplicate suppression, child removal, or book mutation occurs. As with normalized
payloads, verification proves the pinned committed interpretation; it cannot
detect consistently mis-authored accepted metadata under an entirely new pin
without re-auditing canonical evidence.

`CoverageEvidence` validates the receipt's expected/present/missing/invalid and
unexpected inventories, input counts, sequence range, completeness/certification,
and explicit clock diagnoses. Traversal independently reconciles actual per-lane
counts and first clock-fault observations. A missing `clock_faults` field is not
accepted as an explicit empty diagnosis in profile 2. It exposes:

```rust
SourceDelivery::connection_epoch(&self) -> Option<&str>
FilteredDelivery::connection_epoch(&self) -> Option<&str>
DerivativeMetadata::supports_source_evidence(&self) -> bool
DerivativeMetadata::coverage(&self) -> Option<&CoverageEvidence>
WindowStatus::coverage_details(&self) -> CoverageDetails
WindowStatus::coverage(&self) -> Option<&CoverageEvidence>
CoverageEvidence::lane(&self, lane: &LaneId) -> LaneCoverage
CoverageEvidence::faults(&self) -> &[SourceFault]
FinishedWalk::supports_source_evidence(&self) -> bool
```

`LaneCoverage { expected, state }` distinguishes `NotExpected`,
`Present { records: 0 }`, nonempty `Present`, `Missing`, and `Invalid { detail }`.
An unexpected observed lane retains its state with `expected: false`.
`SourceFault { lane, start_ns, end_ns, reason }` preserves `LaneMissing`,
`LaneInvalid { detail }`, and `VisibleClockRegression { previous_visible_ns,
observed_visible_ns }`. Intervals are the upstream half-open storage window, not
clipped request bounds. Status exposes all faults, including out-of-scope/audit
lanes, before any group—even in empty windows. Consumers receive diagnoses, not
invented tape events. Assigning a lane to books versus audit remains an explicit
planner/risk responsibility; spelling supplies no role.

The EOF capability reports source-evidence support only if **every** selected
window has profile 2. Mixed-profile walks remain valid evidence walks but return
false. Strong risk must require this capability and check planned lane coverage;
`certified` alone is not a substitute. Source metadata survives scope filtering
and both clipping bounds. The frozen profile-1 fixture under
`materialize/tests/fixtures/profile1` was produced with the pre-extension writer
at walker tip `5552d2fa3011c44258ce6fd65d53c0a6830a03a9`, not reconstructed by the
new writer; tests pin its exact address and receipt hash.

## EOF, errors, and completion

States are Opening, Active, Exhausted, Poisoned, and Consumed. Only successful
open yields Active. Any read, validation, resource, ordering, or snapshot error
permanently poisons the instance. Subsequent pulls return a stable poisoned error;
they cannot resume or return clean EOF. Retry requires a fresh walker with the
same explicit pins and policy. No path re-resolution under a replacement pin.

Exhausted requires every selected window, both sidecars, all excluded records,
and the final group to have completed verification. Repeated pulls after clean
EOF return `Ok(None)`. `finish(self)` succeeds only after that explicit EOF; it
does not silently drain a prefix or forgive a prior error. Dropping a prefix or
poisoned reader produces no completion capability. A later-window failure may
follow earlier verified groups but prevents completion of the run; downstream
result writers must stage output until FinishedWalk. The walker itself commits
nothing and cannot certify any strategy result.

## Acceptance tests and verification plan

1. Frozen profile: pin a hand-reviewed schema-3 artifact; change test writer
   selection, verify the original exact address and pin; reject unsupported
   schema/materializer/serialization tuples before decoding their events.
2. Pin corruption: change receipt, manifest, file name, address input, and each
   repeated identity independently; reject wrong pins even when the artifact is
   otherwise self-consistent. No unreceipted-directory success.
3. Codec corruption: truncation, concatenated frame, trailing byte, checksum,
   logical SHA/length/LF count, stored SHA/length, missing final LF. Fail even when
   corruption is wholly outside the requested interval/scope.
4. Source union: accepted-many, rejected-pair, ignored-only; omitted and duplicate
   source sequence; reordered children; mismatched same-delivery headers; zero,
   gap, and overflow child indexes; reject/fault mismatch; rehashed count fraud.
5. Grouping: asymmetric two-venue tie with ignored middle source, mixed-scope
   children, same-hash distinct deliveries, malformed tie reuse and timestamps,
   last group at EOF, error on the lookahead needed to close a group. No prefix
   return. No filtered child renumbering.
6. Multi-window: reverse input list normalizes metadata order; duplicate/conflicting
   pins, gap, overlap, skipped canonical sequence, empty middle/first/last windows,
   all-empty runs, unrelated extra windows, differing explicitly pinned normalizers.
7. Bounds: start exactly at/inside first window; both sides of lower and upper
   boundary; whole ties at bounds; no implied prologue; explicit policy and exact
   effective interval in the finished result. Compare Clip and expansion on a
   full book just before the requested start.
8. Scope: unnamed control before first instrument, relevant-lane ignored fault,
   instrument fault versus venue-wide versus unattributed, out-of-scope data,
   duplicate relative delta, empty scope, uncertified empty window with visibly
   unavailable coverage details. No unknown-lane poisoning claim.
9. Orientation: one Kalshi snapshot produces distinct Outcome/Complement keys in
   the same delivery; asymmetric bid prices/quantities survive unchanged and asks
   remain empty. Polymarket token IDs remain separate Outcome instruments.
   Limitless full-book/version evidence retains original ordering and provenance.
10. Races/retry: replace original receipt/data between metadata and copy; mutate
    during copy; delete/replace originals after verified open; identical fresh
    retry; stable poisoned state and prefix-finish rejection. Source changes
    cannot alter private verified output. Inject failures deterministically.
11. Bounds enforcement: one overlong line, one huge tie, oversized metadata,
    excessive windows, snapshot quota overflow, and large filtered-out groups.
    Increase total record count with fixed group size and measure peak retained
    memory; assert no growth proportional to tape length. Include all-reject and
    all-ignore windows, not just accepted events.
12. Pull cadence: inert sink consumes every atomic group while two evaluation
    policies observe different subsets; identical complete application order,
    no consumer hook between children, no strategy implementation.
13. Run engine/ingester fmt, workspace tests and all-target/all-feature Clippy with
    warnings denied; relevant Python replay/encoder/finalizer suites; yarn lint;
    diff/ancestry/secret-staging checks. Obtain final Oracle implementation review.

## Non-goals and approval gate

No book mutation/projector, strategy, fees, scope resolver, audit overlay, REST
gating, historical invalidation, venue-normalizer redesign, archive stager or
publisher, deployment, UI, merge, PR creation, or crash-cleanup redesign.

Oracle's independent design review found no product blocker for this generic
walker. Its engineering findings are incorporated: bounded pinned metadata
inspection, explicit three-way address equality, complete scalar-state tie
verification before output, and deterministic filtering/disposition rules.
Generic explicit lane filters and default-free interval policies do not authorize
a production scope resolver, availability policy, or production lower-bound
selection. Those later choices remain open. Proceed with the walker only.
