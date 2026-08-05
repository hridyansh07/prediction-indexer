# SPLICE — venue adapters

A splice owns everything that needs a live network connection and nothing that
needs judgement.

**Owns:** auth, subscription, reconnection, backoff, heartbeats, protocol quirks,
the three counters, durable append.

**Does not own:** what a frame means, whether a frame is interesting, what a book
looks like, whether two markets are the same condition. All of that is analysis and
happens on the reversible side of the tape.

## The rule

> **A splice does not filter.**

Every application message that arrives becomes exactly one record, verbatim,
including heartbeats, including frames whose shape we do not recognise, including
frames for assets we did not think we asked for.

A message dropped at the splice is dropped before anything durable exists, so the
decision can never be reviewed, and a question it would have answered gets
answered by re-collection — which for a live feed means never. `_emit_frame` in
`splices/common/base.py` takes no predicate, so a venue subclass that wants to drop
something has to work against the base class rather than merely forget.

This has already paid for itself three times:

1. **Polymarket's live wire is flat snake_case** (`event_type`, `price_changes`,
   `asset_id`, `best_bid`) while its published schema is wrapped camelCase
   (`{topic, type, payload:{priceChanges}}`). A splice normalising against the docs
   would have written nulls into every field.
2. **Polymarket carries a book `hash` on every `price_change` entry** — 7,414 of
   7,414 — which makes checksum gap detection possible. Nothing in the reference
   made that obvious.
3. **Limitless carries a `version` on every `orderbookUpdate`**, which its
   documentation states does not exist.

In all three cases the code that survived contact was the code that recorded bytes
and asked questions later.

## What a delivery is

One socket message, verbatim. Polymarket batches several events into a single
frame; splitting them would interpret a schema we are deliberately not interpreting
yet, and the frame boundary cannot be recovered once gone.

The one exception is Socket.IO. A Socket.IO delivery is an event *name* plus a
payload, and the name is not recoverable from the payload — `marketResolved` and
`orderbookUpdate` are different facts. So the Limitless splice records
`{"event": <name>, "data": <payload verbatim>}`. That wrapper is the only framing
any splice performs, it is lossless, and it is called out because a reader could
otherwise mistake our structure for the venue's.

## Interface

```python
class BaseSplice:
    venue: str; record_prefix: str; frame_stream: str
    delivers_deltas: bool        # travels with every connection record

    def open_connection(self) -> AsyncContextManager[Transport]: ...
    async def send_subscription(self, transport, targets) -> None: ...
    async def send_heartbeat(self, transport) -> None: ...        # default: none
    def frame_cursor(self, counter, message) -> dict | None: ...
    def connection_detail(self, targets) -> dict: ...             # default: {}
```

`Transport` is pull-based (`send` / `recv` / async context manager). Venues that
push through callbacks adapt onto a queue rather than growing a second
callback-shaped loop in the base class — one implementation of the ordering and
counting rules is the point.

The queue is unbounded on purpose. A bound means dropping under load, and dropping
is the one thing a splice may never do. Memory pressure is visible and fixable; a
silently discarded book update is a hole nobody finds.

## Connection lifecycle

One epoch per connection, identified by a fresh UUID. A reconnect always starts a
new epoch and `local_counter` restarts at 1.

Carrying an epoch across a reconnect would let a delta arriving on the new socket
fold onto a book assembled from the old one — which yields a corrupt book rather
than an error, and no later check catches it.

Records written into the tape alongside the frames they explain:

| Event | Kind | When |
|---|---|---|
| `connection_opened` | control | after connect, carrying target and metadata digests, metadata path, asset ids, clock scope, `delivers_deltas`, fsync interval |
| `subscription_sent` | control | after the subscribe message |
| `subscription_changed` | control | targets digest moved; carries added/removed |
| `target_metadata_changed` | control | raw catalogue metadata moved while asset subscription stayed unchanged; no reconnect |
| `connection_closing` | control | clean stop (time limit, cancellation) |
| `connection_failed` | fault | exception, with type and truncated message |
| `connection_closed` | control | always, in `finally` |
| `targets_unreadable` | fault | targets file broke while connected |
| `frame_not_utf8` | fault | binary frame that would not decode |

They go in the *same* tape as the data. A separate log would mean the gap and the
reason for the gap have to be rejoined by wall-clock time later — and the reason is
exactly what you need when deciding whether to trust the window around it.

A bad targets file never takes a live connection down. Backoff is exponential with
jitter, because every market on a venue reconnects from the same outage and a fixed
schedule turns one blip into a synchronised stampede the venue then rate-limits.

## Venue status

| Venue | State | Fidelity |
|---|---|---|
| **Polymarket** | Built, verified live — 3,823 frames / 45s / 20 assets | Deltas, **no sequence**, book hash available |
| **Limitless** | Built, verified live — 451 frames / 40s / 11 markets | **Snapshots only.** Sparse monotonic `version`. Lowest of the three. |
| **Kalshi** | Built, awaiting credentials | **Highest.** Real snapshot+delta, dense per-subscription `seq` — the only venue where a dropped message is *provable* |

### Kalshi

Complete and plug-and-play: set `KALSHI_API_KEY_ID` and a private key (see
`.env.example`) and `python3 splices/run.py kalshi` runs. Credentials load lazily
at connect time, so an unconfigured Kalshi never blocks a configured venue.

**Written from the published specification and never exercised against Kalshi's
servers**, because the credential to do that does not exist yet. The architecture
makes most of that risk cheap — a splice records verbatim, so if message *shapes*
differ the frames still land correctly and only analysis needs updating. Three
things can genuinely fail on contact, and all three are isolated:

1. signature construction (`splices/kalshi/auth.py`),
2. the subscribe command shape (`send_subscription`),
3. cursor extraction (`frame_cursor`) — which degrades to `unsequenced` rather
   than raising, so a schema surprise costs metadata and never a frame.

**One subscription covers the whole ladder, deliberately.** `seq` is per
subscription, not per market, so a single subscription across every strike yields
one dense sequence the ingester can verify. Splitting it would give N independent
sequences and lose the only property that makes this venue worth authenticating
for.

Verified end to end against the real Rust ingester with a scripted socket: a
deliberate 7–8 hole in the sequence produced `gap_proven: 1` and moved the epoch
to `Stale`.

That test also caught a real design error worth recording. The first version set
`previous_last` to *the last sequence the splice had seen*, which makes every
message continuous with its predecessor by construction — the deliberate hole
arrived labelled `continuous` and the loss was undetectable. `previous_last` is
now `seq - 1`, the venue's own claim.

> **A cursor records what the venue asserted, never what the splice inferred.**
> Splice bookkeeping dressed up as venue evidence is worse than no evidence,
> because it looks trustworthy.

Fallback if account access never arrives: Kalshi's REST orderbook is public and
unauthenticated (verified, HTTP 200 with no key). That is snapshot-by-polling and
therefore lossy — a contingency, not a plan.

Still unconfirmed until a key exists: whether `seq` resets per connection or
continues, the real session lifetime, and whether the initial snapshot arrives per
market or per subscription.
