# Venue event time in normalized derivatives

Status: **implemented; offline, retained materialization/Risk acceptance, and
Linux Redis/strategy smoke verified.** Strategy run scope and the October 3
metadata-cap limitation are recorded in [the verification report](VENUE_EVENT_TIME_REVIEW.md).
Scope: Kalshi and Polymarket WebSocket book/trade events. Limitless only takes
the schema bump. REST audit semantics remain unchanged.

The user's 2026-10-08 clarification supersedes the earlier draft: normalization
is explicitly run and diagnostics are reviewed manually; no diagnostics sidecar
or new fault class is introduced. Existing required-timestamp validation stays
strict, with no missing-time inference. REST snapshots keep their current
AuditAnchor/source_observed_ns representation and do not enter market_events.

Carry the venue's own timestamps on normalized book and trade events, labelled
with what each timestamp means, so strategies and the market profile can use
exchange time next to our receipt time. Canonical windows already hold the venue
payloads verbatim, so this is a normalizer and derivative-schema change plus a
re-materialization. Capture, canonical windows and archives do not change.

Read first: [`engine/README.md`](../../engine/README.md),
[`engine/DERIVATIVES.md`](../../engine/DERIVATIVES.md),
[`docs/REPLAY_STREAMS_V1.md`](../REPLAY_STREAMS_V1.md), and the persisted-format
rules in [`AGENTS.md`](../../AGENTS.md).

## 1. Evidence (2026-10-07 measurement)

The busiest 30-minute canonical window on each of eight dates from 2026-08-06 to
2026-10-04, about 15M venue messages. "Lag" is our `visible_ns` minus the venue
time.

| Source | Present | Precision | Lag p50 / p99 | Per-market order |
|---|---|---|---|---|
| Kalshi `orderbook_delta` `msg.ts_ms` | 100%, all dates | ms | 34–44 / 45–62 ms | never decreases |
| Kalshi `orderbook_delta` `msg.ts` (ISO) | 100% | µs until between 09-30 and 10-04, ms after; trailing zeros trimmed | same | never decreases |
| Kalshi `trade` `msg.ts_ms` | 100% | ms (`msg.ts` is whole seconds) | 35–45 / 55–127 ms | never decreases |
| Kalshi `orderbook_snapshot` | no event time | — | — | — |
| Kalshi top-level `sending_ts_ms` | from about 09-28 only | ms | 30 / 42 ms | never decreases |
| Polymarket `price_change` `timestamp` | 100% | ms | 8–10 ms before ~08-25; 77–186 ms after, with p99 of 3–14 s | never decreases |
| Polymarket `last_trade_price` `timestamp` | 100% | ms | as above | never decreases |
| Polymarket `book` / REST snapshot `timestamp` | 100% | ms, 12–25% on whole seconds | tails of minutes | — |

No venue time is ever later than our receipt time. What each timestamp marks:

- **Kalshi deltas and trades share one exchange event time.** Every trade has a
  book delta in the same market with exactly the same `ts_ms` (100% on 08-22 and
  10-04). `sending_ts_ms` minus `ts_ms` is 3–9 ms. ISO `ts` floored to ms equals
  `ts_ms` for 100% of 2.0M deltas.
- **Polymarket book changes and trade reports are separate clocks.** A
  `last_trade_price` timestamp is usually *after* the nearest same-price
  `price_change`: p50 +23 ms, p10 −11 ms, p90 +40 ms, and 78% are after. It is a
  trade-report time, not a match time. Every `price_change` carries two
  children, the YES and NO token, sharing the message timestamp.
- **A Polymarket `book` timestamp is the book's last-change time.** It equals the
  asset's latest `price_change` timestamp in 99.98% of cases. It is not when the
  snapshot was sent.
- Within one market, 15–35% of consecutive deltas share a millisecond, so
  ordering inside a millisecond stays canonical order.

## 2. Domain change

Add `venue_time: Option<VenueTime>` to `TradeEvent`, `BookDelta` and `FullBook`.
Leave `EventHeader` alone: the value then reaches the Risk cut's `market_events`
with no transport plumbing beyond the closed-schema updates in §5.

```text
VenueTime {
  event_ns:         Option<u64>          UTC epoch nanoseconds
  event_resolution: Option<Resolution>   millisecond | microsecond; set iff event_ns
  event_kind:       Option<EventKind>    set iff event_ns
  sent_ns:          Option<u64>          venue send time (Kalshi sending_ts_ms)
}
EventKind = exchange_event | book_update | trade_report | book_as_of
```

- At least one of `event_ns` and `sent_ns` is present. An event with neither has
  `venue_time: None`.
- Canonical JSON follows the existing rules: fixed field order, adjacent enum
  tags, closed decode rejecting unknown fields and variants.
- **Annotation only.** `venue_time` must not affect canonical order, event
  index, Risk application order, revisions, cut boundaries, `visible_ns`, book
  state or validity.

## 3. Normalizer rules

### Kalshi (`kalshi-normalizer/v6`)

| Message | `event_ns` | Resolution | Kind | `sent_ns` |
|---|---|---|---|---|
| `orderbook_delta` | ISO `msg.ts`, exact | µs if the fraction has more than 3 digits, else ms | `exchange_event` | top-level `sending_ts_ms` |
| `trade` | `msg.ts_ms` | ms | `exchange_event` | top-level `sending_ts_ms` |
| `orderbook_snapshot` | — | — | — | top-level `sending_ts_ms` |

- Accept top-level `sending_ts_ms` on every message type. Normalizer v5 rejects
  it as `unknown_field`, which makes every Kalshi book unusable from about
  2026-09-28. A failing regression test comes first.
- ISO parsing: the trimmed fraction is exact, so the value is exact regardless
  of the declared resolution. In the µs era about 1% of values have 3 or fewer
  digits and are labelled ms; their value is still correct.
- Consistency: if `msg.ts` and `msg.ts_ms` disagree (floor of ISO to ms ≠
  `ts_ms`), or a trade's `msg.ts` ≠ floor(`ts_ms` / 1000), drop `event_ns` for
  that event and emit a stable diagnostic to stderr for manual review. This does
  **not** reject the valid book/trade or change its validity, state, or index.
  Existing NormalizationFault/reject pairing remains unchanged. Retain a valid
  sent_ns independently. If an optional delta msg.ts is absent, use an available
  ts_ms at ms resolution; if both are absent, leave the event-time trio absent.
  Trades retain their existing required ts/ts_ms wire validation.
- ISO extraction accepts RFC3339 UTC/offset timestamps with exact
  microsecond-or-coarser precision. More than three fractional digits are
  labelled microsecond; up to nine fractional digits are accepted only if they
  represent an exact microsecond (no truncation). Unsupported fractions, leap
  seconds, unparseable nonempty ISO values, and UTC nanosecond overflow emit
  annotation-only diagnostics. Existing malformed wire-field rejects keep
  their current handling and failure order. No inferred or receipt clock is
  substituted.
- Every event derived from one Kalshi message carries that message's
  `VenueTime`. That includes the outcome and complement `FullBook`s from one
  snapshot, and any per-orientation events from one delta.

### Polymarket (`polymarket-normalizer/v4`)

| Message | `event_ns` | Resolution | Kind |
|---|---|---|---|
| `price_change` | `timestamp` × 10⁶, shared by every child delta | ms | `book_update` |
| `last_trade_price` | `timestamp` × 10⁶ | ms | `trade_report` |
| `book` (WebSocket) | `timestamp` × 10⁶ | ms | `book_as_of` |

`sent_ns` is always absent. A `timestamp` that is not a plain decimal integer
keeps its current handling. A missing required timestamp retains the current
reject; it is never inferred. REST snapshots retain their existing required
timestamp, canonical cursor check, and AuditAnchor.source_observed_ns only.
A new overflow while annotating a previously accepted delta/trade emits a
manual diagnostic and leaves venue_time absent; existing book/snapshot
source_time_overflow rejection stays unchanged.

### Limitless (`limitless-normalizer/v3`)

No behaviour change and `venue_time` is always `None`. Bump the bundle ID only
because the record schema version changes, so no two derivative identities
share an ID with different bytes.

## 4. Versions and coexistence

- `SEGMENT_SCHEMA_VERSION` 3 → 4. Readers accept 3 (no `venue_time`) and 4.
- Bump `NORMALIZER_BUNDLE_ID` and `PARSER_VERSION`: Kalshi v5 → v6,
  Polymarket v3 → v4, Limitless v2 → v3.
- Derivatives are keyed by normalizer identity, so v6/v4/v3 derivatives
  coexist with the old ones and replay jobs re-materialize on demand. Update
  any normalizer pins in `configs/replay_runner.json`, bench configs and
  fixtures.
- Canonical windows, raw archives, Targeter records and Universe are untouched.

## 5. Transport and Python

- `replay-transport` serializes the new field inside `market_events[].event.value`
  as `"venue_time": null | {event_ns, event_resolution, event_kind, sent_ns}`,
  with integers as JSON numbers or strings per the existing cut convention.
- `replay/streams/protocol.py` closed key sets for trade and book values add
  `venue_time`. Validate types, enums and the at-least-one rule; reject unknown
  keys.
- `book_transitions` do not change. A consumer pairs a transition with its
  `market_events` book event in the same cut.
- Update `docs/REPLAY_STREAMS_V1.md`. No strategy behaviour changes in this
  spec; exposing venue time in SDK views or the market profile comes later.

## 6. Tests

All are offline, hand-authored, contract-shaped payloads.

1. **Kalshi:**
   - top-level `sending_ts_ms` is accepted. This regression fails on v5 for
     `unknown_field`.
   - a delta with a 6-digit ISO `ts` gives exact µs `event_ns` at µs
     resolution; a 3-digit one gives ms.
   - an inconsistent `ts`/`ts_ms` drops `event_ns`, leaves book state
     unchanged, and records the diagnostic.
   - a trade gives `exchange_event` at ms.
   - a snapshot gives `sent_ns` only.
2. **Polymarket:**
   - both `price_change` children share `book_update`.
   - a trade gives `trade_report`.
   - a WebSocket book gives `book_as_of`; REST retains its exact AuditAnchor
     representation and source_observed_ns.
3. **Schema:**
   - canonical JSON round-trips.
   - closed decode rejects an unknown `venue_time` field, an unknown kind, a
     resolution without `event_ns`, and an empty `VenueTime`.
   - schema-3 records still decode.
4. **Order invariance:** on one Kalshi and one Polymarket fixture, v6/v4 output
   with `venue_time` removed is byte-identical to v5/v3 output after the
   version fields are normalized. Risk cuts have the same book states,
   revisions and boundaries.
5. **Transport and Python:**
   - the golden cut includes `venue_time`.
   - `protocol.py` accepts valid values and rejects malformed ones.
6. **Retained-data acceptance (completed on September 27–28 and October 3
   fixtures; results in [the verification report](VENUE_EVENT_TIME_REVIEW.md)):**
   - re-materialize one real bundle from before 2026-09-28 and one from after.
   - Kalshi deltas and trades and Polymarket changes and trades have
     `event_ns` on ≥99.9% of events.
   - receipt-minus-venue lag is never negative.
   - Kalshi books after 2026-09-28 are usable again.

## 7. How to use these times

| Use | Kalshi | Polymarket |
|---|---|---|
| Book-change timing: intensities, depletion, first passage, imbalance | `exchange_event` | `book_update` |
| Trade timing | `exchange_event`, shared exactly with the trade's book change | `trade_report`: fine for trade arrival rates at second scales. Never order a trade against book changes with it; link a trade to its same-price book change only as labelled inference in model code |
| Snapshot | `sent_ns` only; use receipt time | `book_as_of` is the last-change time, not an event; use receipt time |
| Cross-venue book timing | Kalshi `exchange_event` vs Polymarket `book_update` | |

- **Cross-venue comparisons hold at tens of milliseconds or more.** Each venue
  stamps with its own clock, and we can only bound the skew between them: venue
  time is never later than our receipt, and the minimum receipt lag is about
  7 ms on Polymarket and about 31 ms on Kalshi. Below that, nothing settles which
  venue moved first.
- Polymarket receipt time has multi-second delivery bursts (§1), so Polymarket
  book timing should use `book_update`, not receipt time.
- Within one millisecond, canonical order is the only order.

## 8. Not in scope

- Ordering, merging or "correcting" events by venue time.
- Lead/lag judgements.
- Kalshi `ticker` and Polymarket `tick_size_change` timestamps.
- Limitless timestamps.
- Persisting venue schedule and terminal times in the Targeter. That's a
  separate spec.
