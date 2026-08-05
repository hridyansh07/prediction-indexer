# ENVELOPE — the wire contract

The one record shape every splice writes and the ingester reads. It is the only
interface between the Python capture half and the Rust sequencing half. Each
version is **closed**: the ingester's parser rejects unknown fields outright and
requires that version's complete field set.

That strictness is deliberate. A parser that ignored unknown fields would let a
splice write something the tape claims to carry and no reader ever sees; failing
the whole line instead makes the drift loud on the first record rather than silent
for a month.

```json
{"envelope_version":2,
 "delivery_index":1,
 "record_id":"pm-427f40aa0c6644f3aba2dd55caff2272-3",
 "visible_ns":1785267959274886000,
 "monotonic_ns":918273645000000,
 "venue":"polymarket",
 "stream":"public_book",
 "connection_epoch":"427f40aa0c6644f3aba2dd55caff2272",
 "local_counter":3,
 "source_cursor":{"type":"unsequenced","counter":3},
 "kind":"venue_frame",
 "raw_payload":"[{\"market\":\"0xb5c0…\",\"asset_id\":\"1029797…\",…}]"}
```

The original ten-field shape has no `envelope_version` and is v1 by definition.
The current encoder emits v2. An explicit version other than 2, a v2 record
without `monotonic_ns`, or a v1-shaped record carrying either v2 field is rejected.
This preserves old immutable tapes without making future field drift invisible.

## The three counters

The single most important thing in this file. They answer three different
questions and conflating them is what makes a replay untrustworthy.

| Field | Whose | Scope | Answers |
|---|---|---|---|
| `delivery_index` | ours | this splice's whole lifetime, dense | *In what order did we see things?* |
| `local_counter` | ours | one connection, dense, resets on reconnect | *Where in this connection was it?* |
| `source_cursor` | theirs | whatever the venue offers, often nothing | *What did the venue claim about its own continuity?* |

**Only the first two are authoritative.** Replay walks `delivery_index`. The venue
cursor is *evidence about the venue*, never an ordering — because ordering that
depends on a venue agreeing to number its messages breaks the moment a venue
doesn't, and two of our three don't.

`delivery_index` survives restarts: a splice resumes it by reading the last
complete line of its own spool. The spool is authoritative for this rather than a
sidecar state file, because after a crash a sidecar can disagree with the tape,
and then the tape is what's wrong.

## Field rules

| Field | Type | Rule |
|---|---|---|
| `envelope_version` | uint | Required and exactly `2` for v2; absent means v1. |
| `delivery_index` | uint | ASCII digits only. `1.0`, `1e2`, `true` are rejected. |
| `record_id` | string | Non-empty ASCII, no quote, backslash, or control char — the parser borrows the bytes between the quotes without unescaping. |
| `visible_ns` | uint | `CLOCK_REALTIME` receive clock, nanoseconds; correlates with venue time but may step. |
| `monotonic_ns` | uint | v2 only. `CLOCK_MONOTONIC`, for ordering and intervals within its recorded scope. |
| `venue` | enum | `polymarket` · `kalshi` · `limitless` · `internal` |
| `stream` | enum | `public_book` · `public_trade` · `process` (+ values not used by this envelope) |
| `connection_epoch` | string | Same identifier rules as `record_id`. New UUID per connection. |
| `local_counter` | uint | Dense from 1 within the epoch. |
| `source_cursor` | object\|null | Present always; `null` is legal, absent is not. |
| `kind` | enum | `venue_frame` · `control` · `fault` (+ values not used by this envelope) |
| `raw_payload` | string | The frame verbatim, as a JSON string. Never parsed structure. |

Validation happens at construction in the splice, not at ingest. By the time a bad
line reaches the ingester the socket has moved on and there is nothing to retry
against; failing where the process still holds the message keeps the loss inside
something that can do better.

Every `connection_opened` record carries `clock_scope`. On Linux its `scope_id`
is `/proc/sys/kernel/random/boot_id`, and monotonic readings from different venue
processes are comparable only when that id matches. Development platforms without
that kernel scope use a process-unique id and explicitly set
`comparable_across_processes=false`.

## `source_cursor` variants

```
{"type":"unsequenced","counter":N}                     no venue continuity at all
{"type":"snapshot","last_update_id":N}                 snapshot carrying an id
{"type":"snapshot","source_time_ms":N}                 snapshot carrying only a time
{"type":"update_range","first":A,"last":B,"previous_last":C}   true delta stream
```

What each venue actually emits, measured against the live socket rather than taken
from documentation:

| Venue | Cursor | Density | Detects a dropped message? |
|---|---|---|---|
| Polymarket | `unsequenced` | — | No sequence exists. But every `price_change` entry carries a book `hash` (7,414/7,414 observed), so checksum reconciliation is available to the analysis layer. |
| Limitless | `snapshot.last_update_id` (`version`) | monotonic per market, **not dense**, ranges overlap between markets | No. It orders and dates a book, so a *stale* book is detectable; a *missing* one leaves no hole. |
| Kalshi | `update_range` from `seq` | **dense per subscription** | **Yes** — the only one. Verified end to end: a deliberate 7–8 hole produced `gap_proven`. |

**A consumer must not assume `last_update_id` is dense.** Limitless's is not, and a
classifier expecting Binance-style density would report a gap on nearly every
message.

### The rule for filling in `source_cursor`

Kalshi sends one `seq` per message rather than a range, so the splice emits
`{first: seq, last: seq, previous_last: seq - 1}`. `seq - 1` is the *venue's* claim
about what precedes this message, true by definition for a counter Kalshi promises
is dense.

It is emphatically not "the last sequence the splice saw". An early version did
that and it silently destroyed the signal: deriving `previous_last` from your own
observations makes every message continuous with its predecessor by construction,
so a jump from 6 to 9 arrives labelled continuous and the two lost messages become
undetectable.

> **A cursor records what the venue asserted, never what the splice inferred.**

## Vocabulary the ingester must gain

`control` is the only genuinely new record kind, added because filing connection
lifecycle under `fault` would be a lie for a clean connect and would corrupt any
count of real faults. `kalshi`, `limitless` and `public_trade` are one-line
`wire_enum!` additions.

## Why a file and not a socket

The envelope travels as newline-delimited JSON in an append-only spool that the
splice fsyncs and the ingester tails. gRPC or an internal WebSocket would make the
boundary lossy exactly when it matters: frames pushed over a socket exist only in
the splice's memory until something on the far side commits them, so an ingester
that is down, restarting, or backpressured costs frames. A spool inverts that —
the splice fsyncs bytes it owns and the ingester may be absent for an hour at no
cost. It also means we are not writing reconnect logic for our own internal link.

**The file is the protocol.** If a push channel is added later it carries "new
bytes at offset N", never the data.
