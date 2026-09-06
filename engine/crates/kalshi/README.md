# Kalshi Replay normalizer v1

`replay-kalshi` is the first venue adapter on the generic normalize-once
boundary. It consumes one audited Phase 0 `JoinedCanonicalRecord` and returns
zero or more closed S2 `SegmentEvent` values, an intentional ignore, or one
stable `ParseReject`. It neither owns nor mutates book state.

## Identity and exactness

- Bundle identity is SHA-256 of
  `prediction-indexer/replay-kalshi/v1`. Semantic changes require a version
  bump.
- Config identity is SHA-256 of the canonical config string containing the
  price scale, quantity scale, and `use_yes_price` mode. Changing any field
  changes the derivative address.
- V1 defaults to price scale 4 and quantity scale 2. Decimal strings pass
  through S2's exact parser. Extra fractional zeroes are accepted; non-zero
  discarded digits, exponent notation, floats, overflow, and implicit rounding
  reject.
- The current splice omits Kalshi's `use_yes_price` subscription option, whose
  documented default is `false`. V1 therefore supports only legacy per-outcome
  book pricing and rejects a `use_yes_price=true` config.
- Every emitted record gets its zero-based child `event_index` from
  `replay-materialize`; the adapter cannot alter canonical sequence, lane,
  delivery index, clocks, record ID, source segment/line, content hash, or
  continuity provenance.

## Supported matrix

| Captured shape | Result | Semantics |
|---|---|---|
| `orderbook_snapshot` with `yes_dollars_fp` / `no_dollars_fp` | two `BookEvent::Full` children | Kalshi exposes YES and NO bid books. They remain `Outcome` and `Complement`, each with `Side::Bid`; no `1-p` conversion is performed. Missing side arrays mean an empty side. |
| Retained `yes` / `no` integer snapshot shape | two `BookEvent::Full` children | Integer cents and contracts are exactly rescaled. Mixing old and new fields rejects. |
| `orderbook_delta` | one relative `BookEvent::Delta` | `side=yes/no` becomes `Outcome/Complement`; the order-book side remains `Bid`. Positive and negative deltas are syntactically valid. Applying before a snapshot, deleting a missing level, and underflow are book-preparation concerns. A zero relative delta rejects as a malformed no-op. |
| `trade` | one `TradeEvent` | The exact YES price and count are emitted. `taker_outcome_side`, `taker_book_side`, and legacy `taker_side` must agree. NO price, trade ID, block flag, and timestamps remain in the exact source envelope. No fee is read or calculated. |
| `ticker` | intentional ignore: `ticker_not_in_s2` | The complete authoritative shape and all exact numerics are validated first. S2 has no quote-summary event, so it is not misrepresented as a book or trade. |
| server `subscribed`, `unsubscribed`, `ok`, `error` | intentional ignore: `venue_control_not_in_s2` | Closed shape, IDs, sequence/cursor, and nested control values are validated. The exact frame remains in the sidecar. |
| splice `connection_opened`, `connection_closed`, `connection_failed`, `subscription_changed`, `target_metadata_changed` | typed `ControlEvent` | Epoch comes from the envelope. Asset IDs become venue-qualified instruments. |
| splice subscription, closing, reconciliation, unreadable-target, and non-UTF8 notices | stable intentional ignore | These are validated capture operations with no closed S2 control variant; their exact envelopes remain in the sidecar. |
| top-level JSON array | flattened in array order | Capture records an array delivery as unsequenced because its cursor parser cannot attribute one update range to multiple children. Each child's positive `seq` is still validated, snapshot expansion order is YES then NO, and the materializer assigns contiguous zero-based child indexes. Empty arrays intentionally produce zero children. |

Malformed JSON, non-object children, unknown fields/types, missing required
fields, invalid IDs/sides/directions/timestamps, inexact numeric values,
sequence-to-`update_range` disagreement, and messages routed to the wrong stream
produce stable `ParseReject` plus the materializer's paired
`NormalizationFault`. A malformed child rejects the complete delivered batch;
the boundary cannot truthfully label one source record both accepted and
rejected.

## Evidence and remaining gaps

The fixed-point fixtures in `tests/fixtures` are copied from Kalshi's
authoritative WebSocket AsyncAPI examples. The retained integer snapshot shape
comes from the repository's splice fixture. No archived production Kalshi raw
fixture is present in this checkout, and the repository's Python
`replay.events` has no Kalshi branch to use as an independent semantic oracle.
The current capture comments report live sequence density by channel, but these
adapter gaps still require a retained live-contract acceptance corpus:

1. confirm current production snapshot keys and whether both sides may be
   omitted simultaneously;
2. confirm every optional delta and trade field shape observed in production;
3. confirm whether Kalshi ever batches top-level WebSocket messages;
4. pin the eventual `use_yes_price` migration before changing the splice;
5. add lifecycle-channel schemas only if capture subscribes to those channels.

Unknown future and non-captured private/lifecycle/reference channels reject
rather than being guessed into S2. Fee mechanics, books/projectors, strategies,
and economics are explicitly outside this adapter.
