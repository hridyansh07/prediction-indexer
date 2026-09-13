# Kalshi normalizer v3

`kalshi-normalizer` is the first venue extension for the shared
`canonical_normalizer::Normalizer`. The shared normalizer consumes an audited
`JoinedCanonicalRecord`, validates and decodes its envelope and raw JSON once,
then passes a `CanonicalEnvelope` to `Kalshi` through the `VenueAdapter` trait.
The adapter returns Replay-domain events, an intentional ignore, or one stable
`ParseReject`. It neither owns nor mutates book state.

The crate is split by responsibility: `adapter` integrates Kalshi with the
shared normalizer, `config` owns runtime variables, and `message` dispatches
deliveries and checks stream/SID/sequence/batch rules. `wire` owns closed serde
payload schemas; `event` owns all snapshot, delta, and trade validation.
`process` owns typed splice-control records. Shared structural JSON and exact
decimal conversions live in `canonical_normalizer::validation`; `value` and
`error` only map those neutral failures into Kalshi's stable reject taxonomy.

`Snapshot`, `RelativeDelta`, and `Trade` have private fields containing completed
`FullBook`, `BookDelta`, and `TradeEvent` values. Their `TryFrom<(wire, Config)>`
constructors finish every payload check, including full-book validation, before
an instance exists. `From` into `Vec<SegmentEvent>` is infallible and preserves
YES-before-NO child order. The materializer never sees Kalshi wire types.

To preserve v3 reject precedence even for multiply-invalid inputs, typed wire
values are converted to an in-memory JSON value for one ordered constructor
validator. On serde shape failure, that same validator diagnoses the original
value; serde error text is never persisted. This trades an extra per-message
allocation for a single validation implementation, without reparsing source
bytes. Optional fields preserve absence through that conversion and reject
present nulls. Canonical byte-hash and reject-precedence regressions retain v3
parser, bundle, and configuration identity unchanged.

## Identity and exactness

- Bundle identity is SHA-256 of
  `prediction-indexer/kalshi-normalizer/v3`. Semantic changes require a version
  bump.
- `Config` exposes typed price and quantity scales. The shared normalizer hashes
  a closed identity structure with
  sorted variable keys and scalar value variants. Resolved defaults and an
  explicitly equivalent config therefore hash identically; changing any
  supported variable changes the derivative address.
- V3 defaults to price scale 4 and quantity scale 2. Decimal strings pass
  through the Replay domain's exact parser. Extra fractional zeroes are accepted;
  non-zero discarded digits, exponent notation, floats, values above the
  `i64::MAX` quantity-atom logical limit, overflow, and implicit rounding reject.
- The captured snapshot fields identify legacy integer-cents versus current
  fixed-point pricing directly. This is evidence in each frame, not a runtime
  normalizer switch.
- Every emitted record gets its zero-based child `event_index` from
  `replay-materialize`; the adapter cannot alter canonical sequence, lane,
  delivery index, clocks, record ID, source segment/line, content hash, or
  continuity provenance.

## Supported matrix

| Captured shape | Result | Semantics |
|---|---|---|
| `orderbook_snapshot` with `yes_dollars_fp` / `no_dollars_fp` | two `BookEvent::Full` children | Kalshi exposes YES and NO bid books. They remain `Outcome` and `Complement`, each with `Side::Bid`; no `1-p` conversion is performed. Missing side arrays mean an empty side. |
| Retained `yes` / `no` integer snapshot shape | two `BookEvent::Full` children | Integer cents and contracts are exactly rescaled. Mixing old and new fields rejects. |
| `orderbook_delta` | one relative `BookEvent::Delta` | `side=yes/no` becomes `Outcome/Complement`; the order-book side remains `Bid`. `msg.delta_fp` is the only signed quantity lexeme: its sign is consumed once into `LevelChange::Increase` or `LevelChange::Decrease`, while the stored `PositiveQty` magnitude remains unsigned. Applying before a snapshot, deleting a missing level, and book-state underflow are later preparation concerns. `0`, `-0`, plus signs, malformed signs, and values above the quantity limit reject. |
| `trade` | one `TradeEvent` | The exact YES price and positive count are emitted. `taker_outcome_side`, `taker_book_side`, and legacy `taker_side` must agree. The block flag is validated when present; historical records predating its introduction may omit it and remain unknown, never inferred false. NO price, trade ID, block status, and positive timestamps remain in the exact source envelope. No fee is read or calculated. |
| `ticker` | intentional ignore: `ticker_not_in_replay_domain` | The complete authoritative shape and all exact numerics are validated first. The Replay domain has no quote-summary event, so it is not misrepresented as a book or trade. |
| server `subscribed`, `unsubscribed`, `ok`, `error` | intentional ignore: `venue_control_not_in_replay_domain` | Structural shape, IDs, sequence/cursor, and nested control values are validated. Channel names and nonnegative error codes are open-world values and are preserved without an enum guess. The exact frame remains in the sidecar. |
| splice `connection_opened`, `connection_closed`, `connection_failed`, `subscription_changed`, `target_metadata_changed` | typed `ControlEvent` | Epoch comes from the envelope. Asset IDs become venue-qualified instruments. |
| splice subscription, closing, reconciliation, unreadable-target, and non-UTF8 notices | stable intentional ignore | These are validated capture operations with no corresponding closed Replay control variant; their exact envelopes remain in the sidecar. |
| top-level JSON array | flattened in array order | Capture records an array delivery as unsequenced because its cursor parser cannot attribute one update range to multiple children. Each child's positive `seq` is still validated, snapshot expansion order is YES then NO, and the materializer assigns contiguous zero-based child indexes. Empty arrays intentionally produce zero children. |

Malformed JSON, non-object children, unknown fields, missing required
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
4. add lifecycle-channel schemas only if capture subscribes to those channels.

Well-formed unknown channel names inside supported controls are retained as
intentional ignores. An unknown top-level message type could be state-bearing;
the current generic taxonomy has no separate unsupported-evidence event, so it
is conservatively retained as `unsupported_message_type` plus a paired
`NormalizationFault` rather than silently ignored. A future walker must treat
that fault as incomplete interpretation. Fee mechanics, books/projectors,
strategies, and economics are explicitly outside this adapter.
