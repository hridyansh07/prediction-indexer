# Limitless normalizer v1

`limitless-normalizer` is a `VenueAdapter` for the shared
`canonical_normalizer::Normalizer`. It consumes the splice's lossless Socket.IO
wrapper, emits only closed `replay-domain` events, and relies on the normalized
derivative materializer for Phase 0 provenance and zero-based `event_index`.
Bundle identity is SHA-256 of
`prediction-indexer/limitless-normalizer/v1`; semantic changes require a new
bundle version. Closed config identity contains the resolved price and quantity
scales (defaults 3 and 6).

## Full-book semantics and exact values

Every `orderbookUpdate` is one complete YES-oriented CLOB book. It produces one
`BookEvent::Full` and replaces/initializes state at that observation's own
frontier. A subscription-seeded book has the same role: it is in-band venue state,
not independent audit evidence. This adapter emits no `AuditAnchor`; there is no
separately captured Limitless REST snapshot lane in this repository.

`version` must exactly match Phase 0's `snapshot.last_update_id`. It is sparse and
publisher-scoped: it orders observations and detects some stale delivery, but a
jump does not prove loss and a failover may restart it. The retained continuity
verdict remains `sparse_monotonic`; the adapter never upgrades it to gap proof.

Book prices are exact JSON-number lexemes on the observed 0.001 tick and become
`ConditionalMarketPrice`. The retained production fixture demonstrates valid
resting levels outside the current order-entry API's narrower 0.01–0.99 range, so
the parser accepts observed 0.001–0.999 book state. Sizes are integer raw share
atoms scaled by 1e6 in the official SDK/CLI and become `PositiveQty` without a
float or rounding. Zero, fractional/negative quantities, overflow, inexact prices,
duplicate levels, crossed side labels, unknown fields, and malformed timestamps
reject at the boundary. Derived midpoint/spread fields are validated but not
emitted because the domain book is reconstructed from levels alone.

## Captured shape matrix

| Shape | Result |
|---|---|
| compact documented `orderbookUpdate` (`bids`/`asks`) | one full outcome book |
| rich live/subscription `orderbookUpdate` (token, midpoint, spread, side labels) | one full outcome book after exact validation |
| `newPriceData` | validated current AMM state, then instrument-scoped `amm_price_state_not_in_replay_domain` fault |
| `marketCreated`, `marketResolved` | validated lifecycle state, then instrument-scoped domain-absence fault |
| `system` string or documented `{message,markets?}` | validated state-neutral intentional ignore |
| `exception` | `venue_exception` fault; upstream publishes no closed payload schema |
| splice connection open/close/failure and subscription/metadata changes | typed `ControlEvent` |
| splice subscription sent/closing/unreadable/non-UTF8 notices | validated intentional ignore |
| unknown event, top-level batch, array child, unknown field | deterministic reject/fault |

There is no public Limitless WebSocket stream of every market trade. Book updates
are coalesced resulting state and are never inferred as trades. Public mined trades
exist only through a separately polled REST endpoint that capture does not record,
so this adapter emits no `TradeEvent`. It also computes no fees: the CLOB fee curve
is not public and economics are outside normalization.

## Evidence and gaps

`tests/fixtures` records fixture provenance. It includes a retained public socket
book/system pair captured on 2026-09-12, current authoritative documentation
examples, and immutable official TypeScript/Go SDK commit references. Current
gaps are explicit:

1. no retained `newPriceData`, lifecycle, or exception delivery from production;
2. no independently captured REST snapshot lane, therefore no audit-anchor role;
3. no captured top-level or child batch evidence (authoritative schemas specify
   objects, so arrays reject);
4. no public real-time trade stream;
5. publisher failover/restart behavior is documented but not retained locally.

Tests cover fixture conformance, exact numeric boundaries, source cursor binding,
unsupported-state visibility, process controls, complete Phase 0 provenance, and
materialization/strict verification/idempotent retry.
