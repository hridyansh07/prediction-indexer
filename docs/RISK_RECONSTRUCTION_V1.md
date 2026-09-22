# Rust Replay reconstruction risk V1

Implemented in `engine/crates/risk` (`replay-risk`). This is reconstruction
usability policy, not execution risk, vendor missing-frame certification, or an
economic gate. It consumes existing schema-3 events and source-evidence profile 2;
it changes no persisted schema. The generic walker contract remains in
[`VERIFIED_DERIVATIVE_WALKER_V1.md`](VERIFIED_DERIVATIVE_WALKER_V1.md).

## Plan and lifecycle

`RiskEngine::open(inputs, start_ns, end_ns, lower_bound, plans, limits)` owns a
fresh `DerivativeWalker`. Inputs are exact `PinnedDerivative`s, not paths to
"latest". `LowerBoundPolicy` remains mandatory with no production default.
`BookPlan` explicitly pins each existing `BookKey`, primary source lane, venue,
price scale, and quantity scale. Duplicate keys, mismatched instrument/venue,
invalid limits and unsupported profiles abort. One key has one primary lane.
No role is inferred from a lane's name, the receipt's `certified` flag, or a
previous event. The engine builds the walker scope from these plans, including
every selected instrument orientation; unplanned orientations remain observations
without book authority. Plan getters expose the immutable scales and authority.

All selected pins, including empty/later windows, must support profile 2 before
opening risk. Each verified `WindowStatus` checks the expected/present inventory
and typed receipt faults for each primary lane before any of its groups. Missing,
invalid, not-expected, and clock-regressed lanes produce scoped invalidations.
The exact upstream interval stays in the cut even when request clipping is narrower.
An unrelated missing audit lane does not invalidate primary books. A present empty
lane is not missing; it neither initializes a book nor resurrects invalid state.

Pull `next_cut()` until explicit `Ok(None)`, then consume `finish()` to obtain
`FinishedRisk`. There is no constructor accepting an already-partially-consumed
walker, no public arbitrary-group apply, no seek, skip, or checkpoint API. Strategy
evaluation cadence can skip inspecting a cut, never its application. Prefix drop,
read failure, integrity failure, or resource failure cannot mint completion.
Further pulls after error stay poisoned. Previously returned cuts/views are
provisional results of the failed attempt, not a completed result; any publisher
must keep strategy outputs provisional until `FinishedRisk`. This does not mean
buffering the cut stream until EOF: the Redis delivery boundary streams each cut
immediately, while success/finalization additionally waits for terminal validation
and every registered strategy's processing completion. Retry starts from the original pins with empty
books. `FinishedRisk` retains the finished walker, exact plan and cut count.

## State and recovery

Books have only `NotInitialized`, `Usable`, or `Unusable(Reason)`. A usable view
has a native ladder and initialization dependency. Invalid views expose no usable
ladder or dependency. Exact atom maps are ascending by price; iterate bids in
reverse. Scales must equal the pinned plan, including delta quantities. No implicit
rescaling or rounding occurs. Quantities remain bounded by the domain's signed-64
logical maximum although stored unsigned.

- `Full` replaces both sides of only its key; empty and one-sided books are valid.
- `Set` replaces a level, `Delete` removes it (absent deletion is idempotent),
  `Increase` adds to its current quantity or zero, and `Decrease` checks subtraction
  against the current quantity or zero. Exact zero removes the level. Overflow,
  underflow, scale mismatch, and delta-before-Full invalidate the affected key.
- Native Kalshi Outcome/Complement ladders remain separate bids. Polymarket token
  IDs remain separate instruments. Limitless Fulls replace, never merge. There is
  no complement-price conversion. Locked/crossed flags are computed observations,
  not sticky validity faults.
- Whole-delivery `Duplicate` is checked before epochs, controls, faults, or book
  operations. It never changes a revision or resets the lane epoch. Original
  book/trade observations retain `Duplicate` disposition.
- Every relevant nonduplicate source supplies the splice epoch, including ignored,
  rejected, and source-only filtered deliveries. Epoch changes break all books
  dependent on that lane. The previous Full's address is not an epoch identity.
- Lifecycle, bootstrap, unsequenced, sparse-monotonic and continuous provenance
  do not themselves prove initialization. Gap, backwards cursor, broken local
  counter and identity conflict invalidate dependent books and latch the group.
- Opening a connection or changing a subscription clears old state, but a valid
  Full later in that same group may initialize it. Epoch change without an opening
  control behaves the same way. Closed/failed connections and metadata changes
  invalidate and latch the group; they cannot extend an old delta chain.
- A normalization fault affects only the intersection of its typed impact and
  its planned source authority. An instrument fault affects that instrument's
  planned orientations; an unattributed lane fault affects dependent keys;
  requested-venue faults affect that lane's planned books of the named venue.
  AuditCoverageOnly faults and AuditAnchor events are inert. Events, faults,
  connections and coverage from unplanned lanes never mutate primary books.

Each whole delivery/cross-lane tie stages all affected keys privately and commits
one cut. Actual mutation/fault failure discards that key's staged operations and
latches it for the rest of the group, including any later Full. Healthy sibling
keys still commit in the same cut. A valid Full in a later group recovers, unless
the current receipt interval is faulty for that lane. No Full inside that interval
can heal it. Merely entering a later clean window does not restore old state.
Deltas while unusable retain the latched cause rather than disguising a known
fault as ordinary missing initialization. A new explicit fault may replace it.

The engine has exclusive mutation ownership. It validates all staged replacements
and revision increments before publishing any; `Revisioned<T>`'s independent
stale-writer check is unnecessary because there is no external prepared mutation
or competing writer. Owned `Arc<BookView>` snapshots remain unchanged after
subsequent cuts. There is no callback inside group staging.

## Transport-independent decisions

`RiskCut` has private fields with read-only getters:

- monotonic `sequence()` (window-status cuts count, even when empty);
- `origin()` with exact derivative pin and receipt interval or original group
  source span and visible time;
- `market_events()`: original ordered Book/Trade events, without coalescing trades
  or repeated updates to a key, compact canonical references and disposition;
- `book_transitions()`: affected keys only, prior revision, new immutable view,
  and authoritative `Decision`.

`Decision::Operations` preserves ordered accepted deltas for a normal transition.
Any successful group containing a Full uses `Snapshot` with the complete final
native ladder, including later accepted updates in that group. `Invalidation`
supplies the closed reason and no usable ladder. Earlier book operations rolled
back by a later group fault receive `Invalidated` analytics disposition. Trades
remain observations, not book mutations. `NotAuthority` denotes retained book
observations without planned source/key authority. Controls, raw rejects,
sidecar diagnostic payloads and audit anchors are not market events. Original
book hashes remain uninterpreted fields of the preserved market observations.
Consumers apply decisions, not risk policy. They must install all transitions of
a cut before evaluating or exposing their local state.

`BookView` carries its revision, validity, `as_of()` atomic cut, and (when usable)
`Dependency { epoch, anchor, through }`. The two references retain exact pin,
address and both clocks; `anchor` is the last initializing/replacing Full and
`through` the last accepted book operation. Together with the book revision and
finished pin sequence these identify the reconstruction dependency range without
hashing state. Age can be computed deterministically from the current cut's
visible time and `through.visible_ns`; there is no wall-clock expiry rule.

## Bounds and deliberate limits

`RiskLimits` bounds planned books, total plan text and levels per retained book.
`ReadLimits` bounds windows, metadata, scopes, lanes, line size, unfiltered atomic
group records/bytes and verified snapshot disk. Resource exhaustion aborts the
attempt, never splits a group or drops an accepted input. Retained engine state
is bounded by plan size and levels per book, plus one bounded group and staged
replacements for affected keys; it does not grow with tape length. Caller-retained
cuts/snapshots are caller memory. This V1 copies affected ladders during staging;
persistent maps, batching optimizations and transport memory budgets are deferred.

No Redis, serialization protocol, Python binding, queue, remote query API,
strategies, fee arithmetic, economic logic, fill simulation, audit overlay,
checkpoint/resync, deployment or live operation is implemented here. Usable means
reconstructible from the supplied evidence under this policy, not that no vendor
frame was lost. In particular normalized Limitless events expose no version, so
this engine does not claim version checks or density certification.

Tests in `risk/tests/risk.rs` construct hand-authored canonical contracts, pass
them through the real `build_window` materializer and verified walker, and test
atomicity, exact numeric boundaries, scopes, epochs, receipt faults, immutable
views, ordering, limits, deterministic retries and EOF capability failures. The
frozen pre-extension profile-1 fixture verifies rejection without fabricating a
legacy writer. Tests use temporary directories only; no live data or network.
