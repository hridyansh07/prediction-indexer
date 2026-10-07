# Verified derivative walker and risk reconstruction

Reference for `replay-materialize` (pinned reader), `replay-tape` (walker) and
`replay-risk` (reconstruction). Derivative layout, versions and the build/audit
path are in [`README.md`](README.md). This layer changes no persisted schema,
makes no economic decision, and applies no strategy, fee, audit overlay or
checkpoint.

## Pinned verified open (`replay-materialize`)

```rust
pub struct PinnedDerivative { pub directory: PathBuf, pub pin: DerivativePin }
pub fn inspect_pinned(input: &PinnedDerivative, limits: &ReadLimits)
    -> Result<DerivativeMetadata, String>;
pub fn open_pinned(input: &PinnedDerivative, limits: &ReadLimits)
    -> Result<VerifiedWindowReader, String>;
impl VerifiedWindowReader {
    pub fn next_delivery(&mut self) -> Result<Option<SourceDelivery>, String>;
    pub fn finish(self) -> Result<FinishedWindow, String>;
}
```

Errors are diagnostic `String`s; callers must not classify retryability by text.
Every error invalidates the attempt, and a retry opens a fresh reader with the
same explicit pins.

Open:

1. validates the pin's address syntax, reads the bounded receipt, checks its
   SHA-256 against the pin, dispatches its wire version, and requires the caller
   address, receipt address and addressed directory name to agree;
2. retains exact receipt and manifest bytes in bounded memory and copies the event,
   reject and source streams to an exclusively owned private temporary snapshot
   (directory mode 0700 on Unix), checking each file's exact stored length while
   copying and total snapshot size before and during;
3. binds the snapshot to the receipt (strict canonical metadata, repeated
   bindings, address inputs). No data file is decoded or re-hashed at open.

Traversal decodes each snapshot file once with the codec's structural decoder
(one checksummed dictionary-free frame, no truncation/trailing/concatenated
bytes, output bound, final LF, recorded stored length, decoded length and LF
count) and parses each line once into its final typed record
(`SegmentRecord::from_json` and the closed reject and source types). One parsed
record feeds every semantic check: closed schemas and versions, child order, exact
reject provenance, one-to-one fault/reject pairing, one disposition per source,
delivery/tie/lane/group limits, and coverage agreement. Per-line checks run as a
line is read, per-delivery checks before the delivery is returned, whole-window
checks (frame identity, EOF, counts, final tie run, coverage) at window EOF.
Records may therefore be exposed before their window is fully verified; any later
violation poisons the attempt and only clean EOF can finish. Source replacement or
deletion after snapshotting cannot change yielded records; a concurrent rewrite
that changes a file length fails open, while a length-preserving one is caught
only by the frame and semantic checks.

### Read-time integrity

Exact bytes are bound to the pin by SHA-256 at install, not re-proved per read.

- **Install**: Replay installs a derivative only after the materializer built it
  or the strict `--inspect-pin` audit verified a download. A local copy is bound
  to its pin by `replay/jobs/bundle.py::_check_pinned_files` (receipt hash equals
  the pin; every other file matches the receipt's stored SHA-256 and length);
  a mismatch is `integrity_failure`.
- **Read** (`open_pinned`, hence walker and risk engine): re-hashes only the
  receipt (against the pin) and manifest (against the receipt); performs no
  SHA-256 over stored or decoded data and no decode/re-encode canonical
  comparison. Same-length damage to a hash-bound file is caught by the frame
  checksum or a semantic check during traversal.
- **Audit** (`verify_derivative`, `materialize_range --inspect-pin`, the
  existing-address no-op check): the same single pass plus both SHA-256 identities
  of every output and canonical equality of every line. A digest-only disagreement
  over unchanged valid bytes is an install/audit finding, not a read failure.
  Verification proves the committed interpretation, not that a normalizer was
  correct; a wrong derivative needs a corrected normalizer and a new pin.

### Limits (`ReadLimits::default()`)

| Limit | Default |
|---|---|
| `max_metadata_bytes` | 1 MiB (receipt plus manifest combined for inspection) |
| `max_line_bytes` | 16 MiB including LF |
| `max_group_bytes` / `max_group_records` | 64 MiB / 100,000 |
| `max_snapshot_bytes` | 8 GiB per reader (not a free-space reservation or process quota) |
| `max_windows` | 4096 |
| `max_scope_entries` | 100,000 |
| `max_lanes` | 1,024 |
| `snapshot_root` | `None` (system temp); set `Some(path)` to an existing directory on the data volume. Missing or unwritable roots fail with no fallback. |

Builds enforce the same limits while writing, so an oversized candidate fails
before receipt publication. Limit overflow fails the attempt; it never splits a
group or drops a record. RAM is bounded by metadata, scope/lane, line and group
limits plus one lookahead; scratch disk holds one bounded compressed window. The
reader removes only its owned snapshot on drop or error.

## Source deliveries

Events, rejects and the source stream are joined by canonical sequence. Each
source has exactly one disposition: accepted (children with event indexes
0..N-1), rejected (one index-0 `NormalizationFault` and its paired reject), or
ignored (one index-0 sidecar entry, no children). Child headers must match
byte-for-byte except `event_index`. Source sequence is dense across the union (the
first is positive, ignored sources fill positions) and counts agree with the
manifest with checked arithmetic. Per-lane delivery indexes strictly increase
within and across windows (including empty and filtered ones); gaps are not
inferred and canonical continuity is not recomputed. A reject/fault pair is one
source. A typed summary exposes reject ID, error code, hint, impact or ignored
reason; exact rejected bytes stay in the derivative and never enter book-facing
groups.

## Walker (`replay-tape`)

```rust
DerivativeWalker::open(inputs: Vec<PinnedDerivative>, request: WalkRequest,
                       limits: ReadLimits) -> Result<Self, String>
fn next_item(&mut self) -> Result<Option<WalkItem>, String>
fn finish(self) -> Result<FinishedWalk, String>
// WalkRequest { start_ns, end_ns, lower_bound: LowerBoundPolicy, scope: ScopeFilter }
// WalkItem::{ WindowStatus(Box<WindowStatus>), Group(AtomicGroup) }
```

`replay-tape` depends only on `replay-domain` and `replay-materialize`.

**Windows.** Inputs are exact pins, never a directory scan. Metadata is sorted by
source window start; duplicate starts, conflicting pins, overlaps, gaps, invalid
or empty intervals and redundant nonintersecting windows are rejected, and the
unique minimal adjacent run covering `[start_ns, end_ns)` is required. Windows
concatenate in source order (no re-sorting by time, instrument, lane or hash).
Within a window, order is canonical sequence then child index. Timestamps must lie
in the window and not decrease. Across nonempty windows the next source sequence
is the previous last plus one; empty windows advance coverage without resetting
that expectation. These checks apply to fully filtered or clipped windows too.
Distinct normalizer identities are concatenated only as explicitly supplied pins
and keep their identities.

**Bounds.** `LowerBoundPolicy` is required with no default: `Clip` (effective
start equals the request), `ExpandToWindowStart` (earlier evidence, effective
start is the first window boundary) or `RequireWindowBoundary` (reject partial
starts). No production caller policy is selected by the walker; it records the
policy and requested/effective bounds in `FinishedWalk`. The upper bound is
exclusive: nothing at `visible_ns >= end_ns` is exposed, and since a delivery or
tie shares one timestamp, clipping keeps or drops a whole group. First and last
windows are verified in full, including excluded tails. No prologue, warm start,
price imputation or seek/skip method exists.

**Atomic groups.** A group is one complete delivery, or, when the source has a
`visible_tie_group`, the complete contiguous cross-lane tie run. The finalizer
sets the tie ID to `visible_ns` exactly when an equal-time run spans more than one
lane; all members of such a run must carry `Some(visible_ns)` and every member of
a single-lane run `None`. Same-lane equal-time deliveries stay separate groups,
and same-hash Polymarket records are not groups. Tie runs are validated with
scalar state (timestamp, first lane, cross-lane flag, tag consistency, counters)
without buffering a single-lane run; a tagged group closes on a lookahead that has
validated its run's end. Reject split, reused, inconsistent or incomplete tie
membership. An untagged run that becomes cross-lane late can release earlier
members before the violation is found; the attempt then fails and cannot finish.
The last group is returned only after a different source/tie or verified window
EOF; a decoder failure while closing returns an error, never a partial group.
Group identity is window-qualified. Accessors are read-only (`pin`, `first`,
`last`, `visible_ns`, `visible_tie_group`, `deliveries`, `book_keys`) and groups
cannot be forged.

**Scope.** `ScopeFilter { instruments, lanes }` is explicit caller input; the
walker infers nothing from lane names and performs no resolver lookup or `1 - p`
projection. Instrument selection includes every orientation. Selected venues are
the prefix before `:` of requested instrument IDs. Filtering happens after group
completion and preserves original child indexes and source spans (never
renumbered):

- instrument-addressed book/trade/`AuditAnchor` events are kept for requested
  instruments (anchors are inert evidence);
- controls are kept on explicitly relevant lanes, including unnamed connection
  failures before the first selected instrument, with payloads untouched;
- `Instrument` faults for selected instruments, `RequestedVenueBooks` and
  `AuditCoverageOnly` faults for selected venues, and `UnattributedLane` faults
  are kept (the latter as explicit diagnostics);
- continuity verdicts and source headers are preserved for kept groups; a
  continuity fault on a relevant lane survives as a typed source-only diagnostic
  even if its child instrument is filtered out, and profile-2 deliveries on
  selected lanes keep source-only headers and epochs when all children are
  excluded. These count as included sources, not events;
- duplicates stay visible with payload and provenance (suppressing their mutation
  is the consumer's job); conflict, gap, backwards cursor and broken counter stay
  distinct facts. A validated non-semantic notification (a Polymarket tick-size
  change) is an explicit ignore, while malformed or unsupported state-bearing
  messages remain paired rejects. Unknown persisted variants are fatal.

**Window status.** `next_item()` returns one `WindowStatus` per verified window
before that window's first group, including empty windows: pin, `SourceReceipt`,
certification flag, counts and interval. `coverage_details()` is
`ReceiptBoundV2` for profile 2 with `coverage() -> Option<&CoverageEvidence>`, or
`NotRecordedInDerivativeV1` with `None` for profile 1 (also `None` epoch).
Uncertified windows with attributed faults remain walkable; there is no
certified-only gate. `CoverageEvidence` validates the receipt's
expected/present/missing/invalid/unexpected lane inventories, counts, sequence
range, completeness and explicit clock diagnoses (a missing `clock_faults` field
is rejected); traversal reconciles actual per-lane counts and first clock-fault
observations. `lane(&LaneId) -> LaneCoverage { expected, state }` where state is
`NotExpected`, `Present { records }` (zero records is not missing), `Missing` or
`Invalid { detail }`; `faults()` gives `SourceFault { lane, start_ns, end_ns,
reason }` with reasons `LaneMissing`, `LaneInvalid { detail }` and
`VisibleClockRegression { previous_visible_ns, observed_visible_ns }`, over the
upstream half-open window rather than clipped bounds, including out-of-scope
lanes. `connection_epoch()` on deliveries survives clipping and filtering.
`FinishedWalk::supports_source_evidence()` is true only if every selected window
is profile 2.

**Counters.** Included/excluded sources and children, interval exclusions,
rejects, ignored sources and groups are counted; scope exclusion is not
normalization failure.

**Lifecycle.** States are Opening, Active, Exhausted, Poisoned and Consumed. Any
read, validation, resource, ordering or snapshot error poisons the instance with a
stable error. Exhausted requires every selected window, all sidecars, all excluded
records and the final group to have verified; further pulls return `Ok(None)`.
`finish(self)` succeeds only after explicit EOF and mints a `FinishedWalk`
(requested and effective interval, exact pins, policy, counters); dropping a
prefix or poisoned reader mints nothing. Since verification streams with
traversal, failure may follow groups already returned; consumers must stage output
until `FinishedWalk` and treat the whole attempt as failed otherwise. The walker
commits nothing and never mutates books.

## Risk engine (`replay-risk`)

This is reconstruction-usability policy, not execution risk, vendor
missing-frame certification or an economic gate.

`RiskEngine::open(inputs, start_ns, end_ns, lower_bound, plans, limits)` owns a
fresh walker over exact pins. A `BookPlan` pins each `BookKey`, its single primary
source lane, venue, price scale and quantity scale; duplicate keys, mismatched
instrument/venue, invalid limits and unsupported profiles abort. No role comes
from lane names, the `certified` flag or a prior event. The walker scope is built
from the plans (all orientations of selected instruments); unplanned orientations
remain observations without book authority. All selected pins must be profile 2.

Each `WindowStatus` is checked before its groups: for every primary lane, missing,
invalid, not-expected and clock-regressed states produce scoped invalidations over
the exact upstream interval. An unrelated missing audit lane does not invalidate
primary books; a present empty lane neither initializes a book nor resurrects
invalid state. Pull `next_cut()` until `Ok(None)`, then `finish()` for
`FinishedRisk` (finished walk, plan, cut count). There is no skip, seek, public
group apply or checkpoint; consumers may decline to evaluate a cut but not to apply
it. Further pulls after an error stay poisoned, and cuts from a failed attempt are
provisional (publishers keep strategy outputs provisional until `FinishedRisk`).
Retry starts from the original pins with empty books.

**Book state.** `NotInitialized`, `Usable` or `Unusable(Reason)`. A usable
`BookView` has a native ladder (ascending atom maps; iterate bids in reverse) and
a dependency; an unusable one exposes neither. Scales must equal the plan,
including delta quantities, with no rescaling or rounding.

- `Full` replaces both sides of its key only (empty and one-sided are valid).
  `Set` replaces a level, `Delete` removes it (absent deletion is idempotent),
  `Increase` adds to the current quantity or zero, `Decrease` checks subtraction
  against current or zero, and exact zero removes the level. Overflow, underflow,
  scale mismatch and delta-before-Full invalidate the key.
- Kalshi Outcome/Complement ladders stay separate; Polymarket token IDs stay
  separate; Limitless Fulls replace. No complement conversion. Locked/crossed are
  computed observations, not sticky faults.
- A whole-delivery `Duplicate` is checked first; it never changes a revision or
  resets an epoch, and its observations keep `Duplicate` disposition.
- Every relevant nonduplicate source supplies the splice epoch; an epoch change
  breaks all books dependent on that lane. Lifecycle, bootstrap, unsequenced,
  sparse-monotonic and continuous provenance do not prove initialization. Gap,
  backwards cursor, broken counter and identity conflict invalidate dependent
  books and latch the group. Opening a connection or changing a subscription (or
  an epoch change without an opening control) clears old state, and a valid Full
  later in that group may initialize it. Closed/failed connections invalidate and
  latch the group. `MetadataChanged` is a control observation carrying both target
  metadata digests and never touches books or revisions.
- A normalization fault affects only the intersection of its typed impact and the
  plan's source authority (instrument: its planned orientations; unattributed:
  dependent keys; requested-venue: that lane's planned books of the venue).
  `AuditCoverageOnly` faults, `AuditAnchor` events and anything from unplanned
  lanes never mutate primary books.

Each whole delivery or cross-lane tie stages all affected keys privately and
commits one `RiskCut`; the engine validates all replacements and revision
increments before publishing any. A mutation/fault failure discards that key's
staged operations and latches it for the rest of the group (even a later Full) while
healthy sibling keys still commit. A valid Full in a later group recovers unless
the lane's current receipt interval is faulty, which no Full inside it can heal;
entering a later clean window alone restores nothing. Unusable keys retain the
latched cause; a new explicit fault may replace it. Only affected books advance
revision. `Arc<BookView>` snapshots stay immutable after later cuts.

**`RiskCut`** (private fields, read-only getters): `sequence()` (monotonic; window
status cuts count, even empty); `origin()` (derivative pin and receipt interval, or
the group's source span and visible time); `market_events()` (original ordered
Book/Trade events with disposition, no coalescing); `control_events()` (ordered
`MetadataChanged` observations with `from`/`to` digests); `book_transitions()`
(affected keys, prior revision, new immutable view, `Decision`). `Decision` is
`Operations` (ordered accepted deltas), `Snapshot` (any successful group
containing a Full, with the complete final ladder including later updates in the
group) or `Invalidation` (closed reason, no ladder). Operations rolled back by a
later fault get an `Invalidated` disposition; `NotAuthority` marks observations
without planned authority. Trades are observations, not mutations. Consumers
install every transition of a cut before evaluating.

`BookView` carries revision, validity, `as_of()` cut and, when usable,
`Dependency { epoch, anchor, through }`: references retaining pin, address and both
clocks for the last initializing Full and the last accepted book operation. Age
derives deterministically from visible time; there is no wall-clock expiry.

**Limits.** `RiskLimits` bounds planned books, total plan text and levels per
book; `ReadLimits` bounds the rest. Exhaustion aborts the attempt. Retained state
is bounded by plan size and levels per book plus one group and staged
replacements, independent of tape length (affected ladders are copied when
staging). Usable means reconstructible from the supplied evidence, not that no
vendor frame was lost; Limitless events expose no version, so no version check or
density certification is claimed.

## Tests

`tape/tests/walker.rs` and `risk/tests/risk.rs` build hand-authored canonical
contracts through the real `build_window` and verified walker, covering the
corruption, source-union, grouping, multi-window, bound, scope, orientation, race,
resource and EOF cases above with temporary directories only. The frozen
profile-1 fixture under `materialize/tests/fixtures/profile1` was produced by the
pre-extension writer and pins its exact address and receipt hash.
