# Replay normalized derivative boundary

This workspace contains the stable, venue-independent Replay domain, the
generic boundary that turns one Phase 0 canonical window into one immutable,
verified normalized derivative, the Kalshi, Polymarket, and Limitless normalizers,
a pull-based verified derivative walker, and the `replay-risk` reconstruction
engine. It contains no strategy, publisher, deployment, or scheduler.

## Representation contract

- `Magnitude` is unit-free unsigned exact arithmetic (`u64` atoms plus scale).
  It is not serialized alone and has no financial meaning.
- `Qty` wraps `Magnitude` with `QuantityUnit::Contracts` and is always
  nonnegative. Its storage is `u64`, but V1 deliberately caps persisted atoms at
  `i64::MAX`; constructors, parsing, arithmetic, rescaling, and deserialization
  all enforce that logical maximum. `PositiveQty` additionally makes zero
  unrepresentable for levels, trades, and level changes.
- `Px` remains the general nonnegative `i64` fixed-point price primitive in
  `10^-scale` quote units per contract. `ConditionalMarketPrice` wraps it for
  current market events and rejects values outside the inclusive `[0, 1]`
  interval; it never clamps or rounds.
- `DecimalScale` is the inclusive range 0–18. Decimal input is plain ASCII
  `[0-9]+(.[0-9]+)?`. Whitespace, either lexical sign, exponent notation,
  missing whole/fractional digits, and binary floats are not accepted by the
  domain quantities. A venue adapter may consume a directional wire sign before
  constructing domain values.
- Extra fractional zeroes are exact and accepted. A non-zero discarded digit is
  inexact. Checked rescaling distinguishes a non-zero value below the coarser
  quantum (`Underflow`), other discarded remainder (`InexactRescale`), and
  width failure (`Overflow`). There is no rounding policy.
- Representation equality includes atoms, scale, and unit. Callers must rescale
  explicitly before comparing values expressed at different scales.
- `Side::{Bid, Ask}` is order-book side.
  `ContractOrientation::{Outcome, Complement}` independently records whether a
  price refers to the named outcome or its logical complement. The Replay domain performs no
  implicit `1 - p` conversion.
- `LevelChange::{Set, Delete, Increase, Decrease}` carries operation semantics
  explicitly. Every quantity-bearing variant contains `PositiveQty`; no signed
  or zero relative change can enter the persisted domain.
- `BookStateHash` records the algorithm with the digest. Its current `Sha1`
  variant is a strict lowercase 40-hex newtype for Polymarket full-state
  evidence and cannot be confused with the typed SHA-256 identities used for
  canonical and derivative content.

Currency identity, venue-specific price bands, tick/lot schedules, payout
denomination, currency conversion, fee arithmetic, and strategy rounding are
economic/product choices deferred to their owning later phases. A future
segment manifest must bind currency and the scales expected for each instrument;
the Replay domain does not guess them from a venue.

## Book identity preserves venue-native ladders

`BookKey { instrument, orientation }` identifies the captured ladder used by
future book stores and book dependencies. `FullBook`, `BookDelta`, and
`AuditAnchor` expose `book_key()` without changing their persisted fields.
A full snapshot resets only that key, never every orientation of an instrument.

- [Kalshi](https://docs.kalshi.com/getting_started/orderbook_responses) exposes
  two bid ladders for one market ticker. YES bids use `(kalshi:TICKER, Outcome)`;
  NO bids use `(kalshi:TICKER, Complement)`. These are complementary views of
  one binary market, not independent liquidity: a NO bid at 0.56 implies a YES
  ask at 0.44. Normalization preserves both bid ladders without that conversion.
  Empty `asks` means no explicit ask ladder was emitted, not no implied asks.
- [Polymarket](https://docs.polymarket.com/market-data/overview) gives each
  outcome its own token ID. YES and NO remain different `polymarket:TOKEN_ID`
  instruments, both with `Outcome` orientation relative to their own token.
  Never collapse token IDs to a shared condition ID or reinterpret the NO
  token as `Complement` of the YES token during normalization.

The key distinguishes stored evidence; it does not assert cross-book economic
independence. Any complementary-price view belongs in an explicit later consumer.
`replay-risk` reconstructs native books without an implied-ask projection.

## Closed event contract

`SegmentRecord` schema version 3 owns `EventHeader`, `EventAddress`, complete
downstream canonical provenance, and a closed `SegmentEvent`. Book events use
validated constructors, canonical bid-descending/ask-ascending level ordering,
one scale per full book, positive level quantities, conditional-market prices,
direction-explicit level changes, and no duplicate prices. This is an undeployed
contract; no legacy signed schema exists. `NormalizationFault` carries a closed
impact classification selected before book state. Exact rejected bytes
and parser error codes live in the committed reject sidecar, not this event.

Canonical JSON is compact UTF-8 emitted by `SegmentRecord::to_canonical_json`.
Struct field order and adjacent enum tags are schema. The strict reader rejects
unknown fields/variants, unsupported versions, invalid domain states, alternate
field order, and insignificant whitespace by decode/validate/re-encode equality.
Callers add an LF only when framing records as NDJSON; the LF is not part of one
record's canonical JSON bytes.

`EventAddress.event_index` is zero-based venue-array order. `child(index)` changes
only that index, preserving canonical sequence, lane, and delivery index. Segment
position and event index remain serialization order, never event time.

## Phase 0 stacking boundary

Phase 0 continues to own `CanonicalSelection`, `AuditedCanonicalReader`,
`JoinedCanonicalRecord`, canonical receipt identities, and its finished-audit
capability in `indexer-finalize`. Replay deliberately duplicates none of them.
`canonical-normalizer` performs one exhaustive conversion from each audited joined
record into:

```text
JoinedCanonicalRecord                    replay-domain
canonical_seq --------------------------> EventAddress.canonical_seq
event_address.lane_id ------------------> EventAddress.lane
event_address.delivery_index -----------> EventAddress.delivery_index
normalizer child array position --------> EventAddress.event_index
order_ns / visible_ns / tie group ------> EventHeader clocks/tie
record_id ------------------------------> EventHeader.record_id
source digest/line/content/continuity ---> CanonicalProvenance
```

The conversion stages records while streaming, but derivative publication still
requires Phase 0's `CanonicalSelection`, whose reader yields its audited result
only at verified EOF.
The Replay continuity enum intentionally has the same ten closed labels; the
conversion uses an exhaustive match, not string fallback.

## Normalization and materialization

`canonical-normalizer::Normalizer<A>` implements the normalization lifecycle once.
It decodes each canonical envelope and raw JSON payload, routes by venue, hashes
the adapter's closed, key-sorted canonical configuration, and delegates venue wire semantics
through `VenueAdapter`. Its `Normalize` implementation returns zero/many closed
`SegmentEvent` children, an explicit ignored reason, or an expected
`ParseReject`, and has a final consistency `finish()`.
`serde_json` arbitrary-precision number retention is enabled at this shared decode
seam because Limitless publishes financial values as JSON numbers. Adapters read
the original decimal number lexeme into fixed point; they never round-trip it
through binary floating point. Kalshi continues to require its documented decimal
strings or exact integers, so this seam does not change its accepted shapes.
The seam rejects serde_json's private `Number`/`RawValue` object keys before
deserialization so captured objects cannot be coerced into accepted scalar values.
Every SHA-256 identity crossing this boundary uses `indexer_types::Sha256`, an
invariant-preserving 32-byte value whose unchanged JSON representation is
canonical lowercase hex. Domain-separated derivative and reject addresses remain
distinct string identifiers rather than being conflated with source digests.
Zero children are also an intentional ignore. The materializer records that
source in the sidecar so ignored evidence remains provenance-addressable. An
expected reject produces both an exact-envelope sidecar record and one paired
closed `NormalizationFault`; a normalizer error, panic, invalid domain value,
audit failure, serialization failure, or sink failure is fatal and publishes no
receipt.

`replay-materialize::build_window` intentionally accepts the exact bounds of one
canonical receipt. This is the smallest composition with Phase 0's selected-run
API: selecting exact receipt bounds yields one window and one post-EOF capability,
so no canonical input is reopened and no selector/audit lifecycle changes. The
derivative manifest owns a closed `SourceReceipt` projection converted explicitly
from Phase 0's evolving `ReceiptIdentity`. A caller
wanting a range builds each canonical receipt independently and pins the
resulting `DerivativePin` values.

The immutable layout is:

```text
window=<window-start-ns>/<derivative-address>/
  events.ndjson.zst
  rejects.ndjson.zst
  sources.ndjson.zst      # profile 2: one transport record per source delivery
  manifest.json
  receipt.json            # sole commit marker, written last
```

The domain-separated address binds the complete source receipt identity and
bounds, normalized schema version, normalizer bundle and config digests, policy
digest/effective interval, event/reject serialization versions, and materializer
version. There is no mutable `latest`. Corrected versions coexist under new
addresses.

The first supported reader profile is frozen independently of writer selection:
normalized schema 3, receipt/manifest/event/reject serialization/materializer 1.
Address verification uses the recorded, validated versions, never current writer
constants. Future formats must add explicit closed readers while retaining this
profile's wire types and exact serialization. Experimental schemas 1/2 and unknown
profiles are unsupported; there is no migration or readdressing. Explicitly pinned
bundle/config revisions can coexist without loading historical normalizers.

New writers use profile 2: receipt/manifest/materializer 2, with schema 3 and
event/reject serialization 1 unchanged. The source projection additionally binds
the exact canonical receipt document; its independently checked coverage gives
expected/present/missing/invalid lanes and clock faults without opening raw
evidence. A third stream binds each delivery's child-zero header to the exact
splice `connection_epoch`. The epoch belongs to the source delivery, shared by
all accepted children and retained for rejects/ignores; it is not a venue version.
Profile 1 has separate frozen metadata readers and never invents these fields.
See the walker's [profile-2 contract](../docs/VERIFIED_DERIVATIVE_WALKER_V1.md#implemented-source-evidence-profile-2)
for address inputs, independent validation, and compatibility limits.

All NDJSON files use the shared level-3, checksummed, one-frame Zstandard codec
and carry logical and stored identities. The strict verifier checks canonical
JSON, closed versions and fields, frame EOF and both identities, event/child
order, exact reject-envelope provenance, and one-to-one reject/fault pairing.

Builds use unique private staging directories. After EOF and both `finish()`
calls, they finish and fsync frames, rename and fsync data, write and fsync the
manifest, and strictly verify the complete candidate against the constructed
receipt bytes. Only then do they atomically publish the uncommitted directory and
write/fsync/rename the receipt last. Each build holds its per-address OS advisory
lock from before stage creation through publication and cleanup; process death
releases it. Builds of the same address serialize, while different addresses can
run concurrently. Before writing a new stage, every run scans directory names
under its normalized output root for `window=*/.{address}.{pid}.{nonce}.open`
directories. It prunes only unreceipted stages whose address lock it owns or can
acquire without waiting. Busy stages, directory symlinks, unrelated paths,
committed derivatives, and persistent lock files are preserved. This reads
directory metadata, not compressed payloads; deletion traverses the abandoned
directory and fsyncs its parent. Cleanup failures abort before new stage writes.

Stop all older materializer binaries before upgrading: older binaries lock only
during publication and cannot safely run alongside automatic stage cleanup.
Use a dedicated normalized output root on a filesystem with working OS advisory
locks. No raw/canonical input or archive object is pruned.

An unreceipted final address directory is crash debris and
is rebuilt. A retry rebuilds the candidate: identical receipt bytes verify/no-op;
different bytes at the same address are an immutable conflict.

## Verified derivative traversal

[`VERIFIED_DERIVATIVE_WALKER_V1.md`](../docs/VERIFIED_DERIVATIVE_WALKER_V1.md)
defines the reviewed contract and acceptance cases.

- `replay-materialize::{inspect_pinned, open_pinned}` binds the caller's exact
  `DerivativePin` to receipt, manifest, addressed directory, and compressed data.
  Open verifies a private bounded per-window snapshot completely before exposing
  records; source replacement cannot change the verified stream.
- `replay-tape::DerivativeWalker::open` takes explicit pins, requested bounds,
  `ScopeFilter { instruments, lanes }`, `ReadLimits`, and a required
  `LowerBoundPolicy` with no default. It orders adjacent windows and validates
  source sequence and per-lane delivery order, including excluded data.
- `next_item()` yields a verified `WindowStatus` and immutable `AtomicGroup`s.
  A group contains a whole source delivery or complete cross-lane visible tie.
  Filtering retains original child indexes, source spans, provenance, relevant
  controls/faults, and every selected instrument orientation. `book_keys()` keeps
  Kalshi Outcome/Complement distinct; it performs no projection or mutation.
- Both reader and walker require explicit clean EOF before consuming `finish()`
  can mint a completion capability. Errors poison the attempt. `replay-risk`
  applies every complete group even when its consumer skips evaluation.

RAM is bounded by metadata, lane/scope, line, group, and codec limits; scratch disk
holds one bounded compressed window. Oversized groups fail rather than split.
Set `ReadLimits { snapshot_root: Some(data_volume_scratch), ..Default::default() }`
to place `tempfile` snapshots on the data volume. The root must already exist;
failure never falls back to system temporary storage. `None` uses the system
temporary directory. The 8 GiB per-reader cap does not reserve free space or
budget concurrent readers. Ordinary drop/error removes the owned snapshot only.

The default 16 MiB NDJSON-line limit (including LF), 1 MiB metadata-document
limit, and group/lane limits also deliberately apply to build-candidate
verification: oversized candidates fail before publication, rather than produce
artifacts rejected by the default verifier. These are operational limits, not
wire-format changes. Verification currently performs three full decode passes
before traversal; a single-pass refactor and typed error categories are deferred.
Do not infer retryability from error strings; an error invalidates the attempt,
and any fresh retry must retain the explicit pins.

The private `replay-normalizers/examples/materialize_range.rs` operator helper
selects the minimal adjacent locally committed canonical windows, builds one
profile-2 composite derivative per exact window, and emits only verified ordered
pins. It is an example target, not a stable CLI, archive restorer, or indexer.
The existing 4096-window read limit is unchanged; with half-hour canonical
windows, one initial materialization request can span at most 85 days 8 hours.

Window status includes uncertified/empty evidence. Profile 2 reports
`ReceiptBoundV2` and `coverage() -> Option<&CoverageEvidence>` with typed lane
states and interval-bearing upstream faults, before any groups. It retains
out-of-scope faults without guessing lane roles. Delivery `connection_epoch()`
survives clipping and filtering. Old profile 1 reports
`NotRecordedInDerivativeV1`, with `None` for coverage and epoch.
`FinishedWalk::supports_source_evidence()` is true only when every selected
window has profile 2; `replay-risk` requires it, plus planned lane
coverage, rather than gate on `certified` alone.
No production interval policy, scope resolver, projector, strategy, or audit
overlay is implemented or approved by this generic traversal boundary.

## Reconstruction risk

[`RISK_RECONSTRUCTION_V1.md`](../docs/RISK_RECONSTRUCTION_V1.md) specifies
`RiskEngine::{open, next_cut, view, finish}`, explicit primary-lane `BookPlan`s,
and the transport-independent immutable `RiskCut` boundary. It separates ordered
original Book/Trade observations (including duplicate disposition) from scoped
book decisions. Only affected books advance revision; failures latch per key,
receipt faults block their entire intervals, and later valid Fulls recover.
Views retain exact initialization/epoch dependencies and remain immutable after
later cuts. Profile 1 cannot open a strong risk attempt. There is no transport,
remote query, audit overlay, checkpoint, strategy, or fee change in this crate.

## Prepared mutation boundary

`Revisioned<T>::prepare` runs all fallible validation against immutable state and
returns an opaque, owned, non-cloneable `PreparedMutation<T>`. `apply` first
checks that the current revision equals `prepared_from`; a stale mutation writes
nothing. After that check it performs only the complete replacement and revision
advance. Future books can use this boundary without giving venue adapters or
strategies mutation authority. The risk engine instead stages affected books
privately under its exclusive writer and publishes the whole group together.

## Concepts adapted from Bitfrost

The implementation is original. The reference repository
`github.com/hridyansh07/bitfrost-prime-take-home` has no discovered license file
or Cargo license metadata, so no code was copied. The Replay domain adapts these concepts:

- temporary decode followed by a closed, owned normalized domain;
- distinct financial newtypes, exact decimal lexemes, checked rescaling, and no
  implicit rounding;
- closed persisted schemas with validating decode/re-encode;
- canonical provenance retained beside normalized values;
- complete prepared mutations and stale-before-write atomic publication.

Unlike the reference, the Replay domain adds the canonical lane/delivery/child
address, visible tie group, source segment identity, exact continuity vocabulary,
direction-explicit level changes, and explicit contract orientation.

## Checks

```bash
cargo fmt --manifest-path engine/Cargo.toml --all --check
cargo test --manifest-path engine/Cargo.toml --workspace
cargo clippy --manifest-path engine/Cargo.toml --workspace --all-targets --all-features -- -D warnings
```
