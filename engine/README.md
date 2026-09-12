# Replay normalized derivative boundary

This workspace contains the stable, venue-independent Replay domain, the
generic boundary that turns one Phase 0 canonical window into one immutable,
verified normalized derivative, and the Kalshi and Polymarket normalizers. It
contains no book, strategy, publisher, deployment, or scheduler.

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
  manifest.json
  receipt.json            # sole commit marker, written last
```

The domain-separated address binds the complete source receipt identity and
bounds, normalized schema version, normalizer bundle and config digests, policy
digest/effective interval, event/reject serialization versions, and materializer
version. There is no mutable `latest`. Corrected versions coexist under new
addresses.

Both NDJSON files use the shared level-3, checksummed, one-frame Zstandard codec
and carry logical and stored identities. The strict verifier checks canonical
JSON, closed versions and fields, frame EOF and both identities, event/child
order, exact reject-envelope provenance, and one-to-one reject/fault pairing.

Builds use unique private staging directories. After EOF and both `finish()`
calls, they finish and fsync frames, rename and fsync data, write and fsync the
manifest, and strictly verify the complete candidate against the constructed
receipt bytes. Only then do they atomically publish the uncommitted directory and
write/fsync/rename the receipt last. Publication is serialized per address with an OS advisory lock
that is released by process death. An unreceipted directory is crash debris and
is rebuilt. A retry rebuilds the candidate: identical receipt bytes verify/no-op;
different bytes at the same address are an immutable conflict.

## Prepared mutation boundary

`Revisioned<T>::prepare` runs all fallible validation against immutable state and
returns an opaque, owned, non-cloneable `PreparedMutation<T>`. `apply` first
checks that the current revision equals `prepared_from`; a stale mutation writes
nothing. After that check it performs only the complete replacement and revision
advance. Future books can use this boundary without giving venue adapters or
strategies mutation authority. No book implementation is included in this workspace.

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
