# Splices

A splice owns everything in capture that needs a live network connection and
nothing that needs judgement: authentication, subscription, reconnection,
backoff, heartbeats, protocol quirks, the three counters, timestamps, and the
durable append of one envelope per delivery.

It does not own what a frame means, whether it is interesting, what a book looks
like, or whether two markets are the same condition. Those live on the reversible
side of the tape (`replay/`, `analysis/`).

> **A splice does not filter.** Every application message that arrives becomes
> exactly one record, verbatim: heartbeats, frames of unrecognised shape, frames
> for assets that were not asked for. `BaseSplice._emit_frame` takes no
> predicate, so a venue subclass that wants to drop a message has to work against
> the base class.

A message dropped before anything durable exists can never be reviewed or
recovered. A venue batch is never split (the frame boundary cannot be recovered
once gone). The one framing a splice performs is Socket.IO: the delivery is an
event name plus a payload and the name is not recoverable from the payload, so the
Limitless splice records `{"event": <name>, "data": <payload verbatim>}`.

## Running

One process per feed, deliberately: one venue's outage is not everyone's.

```bash
python3 splices/run.py <feed> [--targets PATH] [--spool-root data/spool] \
    [--segment-seconds 1800] [--writer-queue-capacity 20000] \
    [--fsync-interval-seconds 0.25] [--backoff-max-seconds 60] \
    [--target-poll-seconds 30] [--stop-after-seconds N] [--max-connections N]
```

`--stop-after-seconds` takes the identical code path as a long run. SIGINT and
SIGTERM cancel the run; the `finally` writes `connection_closing`, fsyncs and
seals the open segment, so a stopped splice leaves a tape that ends with a stated
reason.

| Feed | Spool lane | Envelope venue / stream | Targets file |
|---|---|---|---|
| `polymarket` | `polymarket` | `polymarket` / `public_book` | `data/live/targets_polymarket.json` |
| `polymarket-snapshots` | `polymarket_snapshots` | `polymarket` / `public_snapshot` (REST `POST /books` poll every `--poll-seconds`) | same file as `polymarket` |
| `polymarket-sports` | `polymarket_sports` | `polymarket` / `reference_event` | none (broadcast) |
| `polymarket-rtds` | `polymarket_rtds` | `polymarket` / `reference_event` | none (broadcast) |
| `limitless` | `limitless` | `limitless` / `public_book` | `data/live/targets_limitless.json` |
| `kalshi` | `kalshi` | `kalshi` / `public_book`, `public_trade`, `public_quote` | `data/live/targets_kalshi.json` |

A broadcast feed refuses a `--targets` file rather than ignoring it. Targets files
are written by the Targeter and read by `targeter.targets.load_targets`; a bad
targets file never takes a live connection down. A missing targets file stops the
splice with exit code 2 before connecting.

The lane, not the venue, is the partition key: Polymarket runs several lanes
whose records all say `venue: polymarket`. Each lane is its own process because
`delivery_index` is dense across one splice's lifetime and two processes sharing a
lane would interleave two counters.

**Kalshi** credentials are `KALSHI_API_KEY_ID` (alias `KALSHI_API_KEY`) plus
`KALSHI_PRIVATE_KEY_PATH` or `KALSHI_PRIVATE_KEY_PEM`; see `.env.example`.
`run.py` checks them before connecting, so a missing key prints setup text rather
than a 401 inside a reconnect loop. One subscription covers the whole ladder
because Kalshi's `seq` is per subscription: splitting it would give N independent
sequences and discard the only venue-provable continuity. `--snapshot-max-age-seconds`
(default 0, disabled) enables a poller that asks for a fresh orderbook snapshot
for markets idle that long; enabling it is a per-deployment decision.

Fidelity differs by venue and travels with the data (`delivers_deltas` on every
`connection_opened`):

| Venue | Cursor | Detects a dropped message? |
|---|---|---|
| Polymarket market channel | `unsequenced`; every `price_change` entry carries a book `hash` usable by analysis for checksum reconciliation | No sequence exists |
| Limitless | `snapshot.last_update_id` from `version` (else `snapshot.source_time_ms`, else `unsequenced`) | No: monotonic per market but sparse and overlapping across markets; orders and dates a book, so a stale book is detectable, a missing one leaves no hole |
| Kalshi | `update_range` from `seq`; non-sequenced messages fall back to `unsequenced` | Yes, the only venue where a dropped message is provable |

## Interface

```python
class BaseSplice:
    venue; record_prefix; frame_stream; delivers_deltas
    def open_connection(self) -> AsyncContextManager[Transport]
    async def send_subscription(self, transport, targets)
    async def send_heartbeat(self, transport)            # default: none
    async def request_stale_snapshots(self, transport, targets)  # default: none
    async def after_frame(self, transport, message)      # runs after the write
    def stream_for(self, message) -> str                 # default: frame_stream
    def frame_cursor(self, counter, message) -> dict | None
    def connection_detail(self, targets) -> dict
```

`Transport` is pull-based (`send` / `recv` / async context manager); venues that
push through callbacks adapt onto a queue so there is one implementation of the
ordering and counting rules. `after_frame` is called after the record is written,
so it cannot become a filter.

## Connection lifecycle

One epoch per connection, identified by a fresh UUID; `local_counter` restarts at
1. Carrying an epoch across a reconnect would let a delta on the new socket fold
onto a book from the old one. Reconnect backoff is exponential with jitter.

Lifecycle and faults are written into the same tape as the frames they explain, as
`stream: process` records whose payload is `{"event": ..., ...}`:

| Event | Kind | When |
|---|---|---|
| `connection_opened` | control | after connect; carries target and metadata digests, asset ids, `clock_scope`, `delivers_deltas`, fsync interval, repaired bytes |
| `subscription_sent` | control | after the subscribe message |
| `subscription_changed` | control | targets digest moved; carries added/removed, then reconnects |
| `target_metadata_changed` | control | catalogue metadata moved while the asset subscription did not; no reconnect |
| `connection_closing` | control | time limit or cancellation |
| `connection_failed` | fault | exception; type, truncated message, seconds open |
| `connection_closed` | control | always, in `finally` |
| `targets_unreadable` | fault | targets file broke while connected |
| `frame_not_utf8` | fault | binary frame that would not decode (recorded with replacement characters) |

## Durable append: segments and seals

`Spool` is a facade over `splices/common/writer.py` (queue, drain coroutine, one
writer thread) and `splices/common/segment.py` (one file and its seal).

```text
<spool-root>/lane=<lane>/date=<YYYY-MM-DD>/
  <YYYYMMDDTHHMMSS000000>-<index:03d>-<segment-id>.ndjson.open   writer-owned
  <...>.ndjson                                                    renamed, not yet committed
  <...>.seal.json                                                 the commit marker
```

- **A segment is one UTC-aligned window** (`--segment-seconds`, default 1800, must
  divide 86400). A reconnect does not roll a file, so a segment holds several
  connection epochs; `connection_epoch` on each record is the marker between their
  `local_counter` runs.
- **The seal is the commit marker.** An `.ndjson` without a valid seal is not
  evidence. Seal order: fsync data, rename `.open` to `.ndjson`, fsync directory,
  write the seal through a temp file and atomic rename, fsync directory again.
  Directory-fsync errors propagate: an unsuccessful directory sync is an
  unsuccessful seal.
- **Segment index.** A restart inside a window opens a second segment for it, so the
  zero-padded index (read from disk) keeps file order equal to receive order.
- **Quiet lanes seal an empty segment** (digest of zero bytes), so a reader can tell
  "nothing arrived" from "not finished yet".
- **Seal contents** (`seal_version` 1, closed set read by the Rust `indexer-segment`
  crate): `lane_id`, `window_start_ns`, `window_end_ns`, `data_file`, `byte_length`,
  `line_count`, `sha256` (plain digest, maintained incrementally), first/last
  `delivery_index` and `visible_ns` (null when empty), `visible_non_decreasing`,
  `delivery_index_dense`, `segment_id`, `segment_index`, `seal_reason` (`boundary`,
  `shutdown`, `recovery`), `ordering_status` (`ok` or `visible_clock_regression`;
  must agree with `visible_non_decreasing`), `epochs`, `repaired_bytes`,
  `created_ns`, `writer_version`, plus writer metrics (`segment_seconds`,
  `queue_capacity`, `queue_high_water`, `queue_full_events`, `segments_sealed`).
- **Backpressure, never loss.** The writer queue is bounded
  (`--writer-queue-capacity`, default 20,000 records) and the producer awaits, so a
  storage stall suspends the receive loop and the kernel socket buffer absorbs the
  rest. A full queue is a storage fault and never reconnects the venue (a reconnect
  opens an unobservable loss window on an unsequenced venue). Past the kernel
  buffer the venue may disconnect us; that is recorded as an ordinary fault and
  the normal reconnect path handles it.
- **Writes are retry-safe.** The file is unbuffered; if an OS write places a prefix
  and fails, the writer truncates back to the last committed offset and retries; if
  rollback fails the segment is poisoned and refuses to seal. `close()` refuses to
  seal while any accepted record is unwritten. Rotation follows each record's own
  `visible_ns`, with a timer only to rotate quiet lanes.
- **Restart.** `delivery_index` resumes from the newest seal (or last complete line),
  not a sidecar state file. A torn final line in an `.open` file is truncated to the
  last newline (the only mutation of a spool, and refused on a sealed segment).
  Orphaned `.open` files and renamed-but-unsealed files are fsynced and
  recovery-sealed.
- **Clocks.** `visible_ns` is `CLOCK_REALTIME` at receipt; `monotonic_ns` is
  `CLOCK_MONOTONIC`. Every `connection_opened` carries `clock_scope`; on Linux the
  `scope_id` is the kernel boot id, and monotonic readings from different processes
  are comparable only when it matches. Elsewhere the id is process-unique and
  `comparable_across_processes` is false. A `visible_ns` that moves backwards logs
  a critical `capture_clock_regression` immediately (injectable alert callback),
  the record is still preserved, and the segment is sealed
  `visible_clock_regression`. At restart the first new `visible_ns` is checked
  against the previous run's last.

## Envelope

The one record shape every splice writes and the ingester reads: one line of
newline-delimited JSON per delivery. Each version is **closed**: the Rust parser
rejects unknown fields and requires that version's complete field set.
Construction in `splices/common/envelope.py::build_envelope` validates what the
parser would reject, because by ingest time the socket has moved on.

```json
{"envelope_version":2,"delivery_index":1,"record_id":"pm-427f40aa0c66-3",
 "visible_ns":1785267959274886000,"monotonic_ns":918273645000000,
 "venue":"polymarket","stream":"public_book",
 "connection_epoch":"427f40aa0c6644f3aba2dd55caff2272","local_counter":3,
 "source_cursor":{"type":"unsequenced","counter":3},
 "kind":"venue_frame","raw_payload":"[{\"asset_id\":\"1029797\"}]"}
```

The encoder emits v2 with the keys in the order above (deterministic bytes). A
record without `envelope_version` is v1 (the same fields without `envelope_version`
and `monotonic_ns`) and stays readable. An explicit version other than 2, a v2
record without `monotonic_ns`, or a v1-shaped record carrying a v2 field is
rejected.

### The three counters

| Field | Whose | Scope | Answers |
|---|---|---|---|
| `delivery_index` | ours | one lane's whole lifetime, dense, survives restarts | in what order did this lane see things |
| `local_counter` | ours | one connection, dense, resets on reconnect | where in this connection |
| `source_cursor` | the venue's | whatever the venue offers, often nothing | what the venue claimed about its own continuity |

Replay orders by the first two plus `visible_ns`; the venue cursor is evidence
about the venue, never an ordering.

### Fields

| Field | Type | Rule |
|---|---|---|
| `envelope_version` | uint | exactly `2` for v2; absent means v1 |
| `delivery_index` | uint | ASCII digits only; `1.0`, `1e2`, `true` rejected |
| `record_id` | string | `<record_prefix>-<epoch>-<local_counter>`; non-empty ASCII with no quote, backslash or control character (the parser borrows the bytes without unescaping) |
| `visible_ns` | uint | receive time, `CLOCK_REALTIME` ns |
| `monotonic_ns` | uint | v2 only; `CLOCK_MONOTONIC` ns |
| `venue` | enum | `polymarket` `kalshi` `limitless` `internal` |
| `stream` | enum | `public_book` `public_snapshot` `public_trade` `public_quote` `reference_event` `process` |
| `connection_epoch` | string | same rules as `record_id`; new UUID per connection |
| `local_counter` | uint | dense from 1 within the epoch |
| `source_cursor` | object or null | always present; `null` is legal, absence is not |
| `kind` | enum | `venue_frame` `control` `fault` |
| `raw_payload` | string | the frame verbatim as a JSON string, never parsed structure |

`control` exists so a clean connect is not filed under `fault`. The venue, stream
and kind vocabularies are asserted equal to the Rust `wire_enum!` declarations by
`tests/test_envelope.py`, because the ingester rejects an unknown spelling at
ingest, long after the socket moved on. `reference_event` streams (sports game
state, spot price) carry a real venue, never `internal`, since whoever operates the
feed owns its delivery latency.

### `source_cursor` variants

```text
{"type":"unsequenced","counter":N}                              no venue continuity
{"type":"snapshot","last_update_id":N}                          snapshot carrying an id
{"type":"snapshot","source_time_ms":N}                          snapshot carrying only a time
{"type":"update_range","first":A,"last":B,"previous_last":C}    delta stream
```

Consumers must not assume `last_update_id` is dense.

> **A cursor records what the venue asserted, never what the splice inferred.**
> Kalshi sends one `seq` per message, so the splice emits
> `{first: seq, last: seq, previous_last: seq - 1}`: the venue's own claim about
> its predecessor. Deriving `previous_last` from the last value the splice saw
> would label every jump continuous by construction and destroy the signal.

## Why a file and not a socket

The envelope travels as NDJSON in append-only segments the splice fsyncs and the
ingester reads. A socket would be lossy exactly when it matters: frames pushed to
a down or backpressured ingester exist only in splice memory. With a file the
splice fsyncs bytes it owns and the ingester may be absent for an hour at no cost.
The file is the protocol.

## Tests

`tests/test_envelope.py`, `test_capture_clock.py`, `test_segment.py`,
`test_spool.py`, `test_writer_queue.py`, `test_sealed_capture_failure_proofs.py`,
and `test_polymarket_splice.py` / `test_kalshi_splice.py` (offline, scripted
sockets).
