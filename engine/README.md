# Replay Engine S2 domain boundary

This workspace contains only the stable, venue-independent Replay domain and
numeric boundary. It does not contain a venue decoder/normalizer, book,
strategy, segment writer, publisher, or Phase 0 integration.

## Representation contract

- `Px` is a non-negative `i64` atom count in `10^-scale` quote units per
  contract. `PriceUnit` is closed at `quote_per_contract`.
- `Qty` is a signed `i64` atom count in `10^-scale` contracts. Signed quantities
  exist for relative mutations; full-book levels and trades require positive
  quantities, absolute updates permit zero as deletion, and relative updates
  reject zero as a no-op.
- `DecimalScale` is the inclusive range 0–18. Decimal input is plain ASCII
  `[0-9]+(.[0-9]+)?`, with `-` permitted only for quantity. Whitespace, `+`,
  exponent notation, missing whole/fractional digits, and binary floats are not
  accepted.
- Extra fractional zeroes are exact and accepted. A non-zero discarded digit is
  inexact. Checked rescaling distinguishes a non-zero value below the coarser
  quantum (`Underflow`), other discarded remainder (`InexactRescale`), and
  width failure (`Overflow`). There is no rounding policy.
- Representation equality includes atoms, scale, and unit. Callers must rescale
  explicitly before comparing values expressed at different scales.
- `Side::{Bid, Ask}` is order-book side.
  `ContractOrientation::{Outcome, Complement}` independently records whether a
  price refers to the named outcome or its logical complement. S2 performs no
  implicit `1 - p` conversion.

Currency identity, legal price bounds, tick/lot schedules, payout denomination,
currency conversion, fee arithmetic, and strategy rounding are economic/product
choices deferred to their owning later phases. A future segment manifest must
bind currency and the scales expected for each instrument; S2 does not guess
them from a venue.

## Closed event contract

`SegmentRecord` schema version 1 owns `EventHeader`, `EventAddress`, complete
downstream canonical provenance, and a closed `SegmentEvent`. Book events use
validated constructors, canonical bid-descending/ask-ascending level ordering,
one scale per full book, and no duplicate prices. `NormalizationFault` carries a
closed impact classification selected before book state. Exact rejected bytes
and parser error codes belong in the future reject sidecar, not this event.

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
capability in `indexer-finalize`. S2 deliberately duplicates none of them. After
stacking onto `origin/rework/replay-pipeline`, the tape crate should add one
conversion from each audited joined record into:

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

That conversion may stage records while streaming, but later segment publication
must still require Phase 0's `AuditedCanonicalSelection` produced only at verified
EOF. The Replay continuity enum intentionally has the same ten closed labels; the
adapter must use an exhaustive match, not string fallback.

## Prepared mutation boundary

`Revisioned<T>::prepare` runs all fallible validation against immutable state and
returns an opaque, owned, non-cloneable `PreparedMutation<T>`. `apply` first
checks that the current revision equals `prepared_from`; a stale mutation writes
nothing. After that check it performs only the complete replacement and revision
advance. Future books can use this boundary without giving venue adapters or
strategies mutation authority. No book implementation is included in S2.

## Concepts adapted from Bitfrost

The implementation is original. The reference repository
`github.com/hridyansh07/bitfrost-prime-take-home` has no discovered license file
or Cargo license metadata, so no code was copied. S2 adapts these concepts:

- temporary decode followed by a closed, owned normalized domain;
- distinct financial newtypes, exact decimal lexemes, checked rescaling, and no
  implicit rounding;
- closed persisted schemas with validating decode/re-encode;
- canonical provenance retained beside normalized values;
- complete prepared mutations and stale-before-write atomic publication.

Unlike the reference, S2 adds Replay's canonical lane/delivery/child address,
visible tie group, source segment identity, exact continuity vocabulary,
absolute/relative level semantics, and explicit contract orientation.

## Checks

```bash
cargo fmt --manifest-path engine/Cargo.toml -- --check
cargo test --manifest-path engine/Cargo.toml --workspace
cargo clippy --manifest-path engine/Cargo.toml --workspace --all-targets --all-features -- -D warnings
```
