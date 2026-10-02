# Replay Redis delivery V1

Implemented by `engine/crates/transport` (`replay-transport`, `replay-publish`)
and `replay.streams`. Risk remains the sole reconstruction authority. This is a
rebuildable **derived** delivery boundary, not the capture filesystem protocol.
No risk policy, fee policy, episode schema, service deployment, or production
lower-bound selection is introduced. The optional whole-attempt runner is
documented in [REPLAY_SUPERVISOR_V1.md](REPLAY_SUPERVISOR_V1.md).

## Attempt lifecycle and ownership

One immutable run/attempt ID, explicit pins, requested interval, lower-bound
policy, typed composite normalizer identity, book plans and required strategy groups define an attempt. The publisher
opens its own fresh RiskEngine. `step()` publishes one cut, or a terminal only
after `next_cut() == None` and `finish()` return the EOF capability. Cuts stream
immediately; only downstream strategy outputs are staged/provisional. The initial
control record carries the entire pinned plan/config identity (not paths); a
consumer requires equality with the caller's independently supplied initial body.

One stream holds **one copy** of each payload shared across groups. Setup creates
all required groups at `0` and exactly one `worker` consumer per group before any
publication. Each group has a single local SDK instance, registered once. Processes
may start at different times, but the strategy membership is fixed at setup; there
is no API for adding a group. `join` and every poll's `check` verify membership;
publish and ACK do not repeat it (see the stream-path amendment below). External
clients must not modify these dedicated keys or churn/recreate groups; detecting
adversarial delete-and-recreate between commands is outside this trusted-server
contract. Redis ACLs and supervisor process isolation should enforce ownership.

Every error/ambiguous command poisons the local attempt, with a best-effort shared
poison flag. A Redis outage/OOM may prevent that flag: the supervisor must regard
any participant failure/disappearance as fatal even if Redis progress looks good.
There is no command retry, reconnect resume, pending claim, duplicate suppression,
late strategy, resync snapshot or replay within an attempt. A whole new attempt
starts from the original pins and empty books. Redis is at-least-once infrastructure,
**not a global exactly-once processing guarantee**. Hook side effects remain
provisional; a lost ACK reply can follow successful hook execution.

## Wire protocol

Redis entry IDs are `(wire sequence + 1)-0`; sole field `record` is UTF-8 JSON.
Every integer, including version, scales, counters, clocks and domain atoms, is a
canonical unsigned decimal **string** (`0` or `[1-9][0-9]*`). No JSON numeric value,
float, exponent, sign or leading zero is accepted in an integer field. Bounds are
u64 except canonical sequence/quantity ≤ i64::MAX, child index ≤ u32::MAX and
scale ≤18. Conditional price atoms ≤10^scale. All objects are closed and duplicate
JSON keys are rejected. Unknown versions or variants abort, never get skipped.
These are **publisher** obligations. The stream consumer enforces them fully only
for the `initial` record; cut records are checked by the O(1) guard set below.

Envelope: `{version:"1", run_id, attempt_id, sequence, kind, body}`.

`control_events` is an accepted post-baseline V1 lockstep extension. The publisher
and every strict consumer for an attempt must be deployed together. A baseline V1
consumer intentionally fails closed when a cut contains this field; mixed-version
operation and rollback to that consumer across this boundary are unsupported. This
is retained as V1 because the fixed publisher and consumers ship in one image and
attempt streams are ephemeral rather than durable interchange artifacts.

- `initial`, sequence 0: `pins` (`derivative_address`, `receipt_sha256`),
  `start_ns`, `end_ns`, `lower_bound`, `plans`, `groups`, `max_entry_bytes`,
  `max_queue_bytes`. Each plan has `instrument`, `orientation`, `lane`, `venue`,
  `price_scale`, `quantity_scale`. Explicit ordered configuration is the identity;
  no lookup of current/latest data is permitted.
- `cut`, sequences 1…N: `origin`, ordered `market_events`, optional ordered
  `control_events`, and affected-only `book_transitions`. Origin is `window {pin,start_ns,end_ns}` or
  `group {pin,first,last,visible_ns}`. An address has `canonical_seq`, `lane`,
  `delivery_index`, `event_index`. A reference has `pin`, `address`, `visible_ns`,
  `order_ns`. All pins must belong to initial selection.
- Each market record has `reference`, `event {kind:book|trade,value}`, and
  `disposition` (`observed`, `applied`, `duplicate`, `not_authority`, `invalidated`).
  Original domain BookEvent/TradeEvent serialization is retained, except all
  integers become strings. Fulls, repeated deltas and trades remain ordered and
  uncoalesced, including invalidated/duplicate observations and uninterpreted hashes.
- `control_events`, when present, contains referenced non-market observations.
  V1 currently admits only `metadata_changed {from,to}`. It changes no local book;
  legacy cuts that omit this field remain valid. The canonical encoding omits
  `control_events` when there are no controls; an empty array is invalid. The old `metadata_changed`
  invalidation reason remains decodable for already-produced wire records but is
  not emitted by the corrected Risk policy.
- A transition has `key {instrument,orientation}`, `previous_revision`, `revision`,
  `dependency` (null, or `{epoch,anchor,through}`), and `decision`. Decisions are
  `snapshot {bids,asks}` (ascending `[price_atoms,quantity_atoms]` pairs),
  `operations {operations}` (ordered domain BookDelta values), or
  `invalidation {reason}`. Reason variants are explicitly enumerated in
  `protocol.py::reason`, mirroring Rust risk reasons including interval details.
  **Ordinary delta transitions never contain the full ladder.** Full market
  observations remain full because they are original input, not redundant views.
- `terminal`, sequence N+1: closed `{cuts:N, books_sha256}`. Neither an empty
  poll, producer process exit, nor the last data cut is terminal. Missing tail
  cannot pass `finish()`. `books_sha256` is the final-book digest defined below.

### Stream-path amendment (lockstep, still wire version "1")

The hot path was trimmed for throughput. Like `control_events`, this is a lockstep
V1 amendment: the Rust publisher, `attempt.lua`, and every Python consumer ship in
one image; mixing a pre-amendment publisher or consumer with this one is unsupported
(the old terminal `{cuts}` and the new `{cuts, books_sha256}` reject each other).

**Decoder guard set.** Each record is size-checked against `max_entry_bytes` and
parsed with plain `json.loads` (no duplicate-key or constant hooks). The decoder
then checks only O(1) facts: closed envelope, `version == "1"`, run/attempt
identity, contiguous stream sequence, and per transition: planned book key,
`previous_revision ==` local revision and `revision == previous + 1`, known decision
kind, operations only on a `usable` book, price atom within `[0, 10^price_scale]`
(the dense-array index guard), and no negative resulting quantity. Sequence 0
(`initial`) is still validated completely and bound to the caller's expected body.
Origins, references/pins, market events, control events, dependencies, reasons,
operation key/scale agreement, and snapshot ordering are **not** revalidated; the
publisher is trusted for them. The strict helpers (`obj`, `uint`, `decode`,
`reason`, …) remain in `replay.streams.protocol` for configuration and output
readers. Consumers mechanically apply the authoritative operations with exact
integers and must not recompute whether an observation should have applied.

**Terminal digest.** End-to-end book agreement is checked once, at terminal.
`books_sha256` is lowercase-hex SHA-256 over UTF-8 lines, one per planned book,
sorted by (instrument, orientation) — instrument by UTF-8 bytes, orientation by
its wire spelling (`complement` < `outcome`):

```text
<instrument> TAB <orientation> TAB <revision> TAB <validity> TAB <bids> TAB <asks> LF
```

`revision` is decimal; `validity` is `not_initialized`, `usable`, or `unusable`.
`bids` (descending price) and `asks` (ascending) are comma-joined
`<price_atoms>:<quantity_atoms>` decimal pairs, and both are empty unless the book
is usable. The publisher computes it read-only from `RiskEngine::view` for every
planned book before `finish()` (`wire::books_sha256`); the consumer recomputes it
from its local books (`protocol.books_sha256`) and fails the attempt on mismatch.
Corruption healed by a later snapshot before terminal is not detected; earlier
detection is traded for throughput by design.

**In-place books and the zero-copy hook contract.** Each planned book is mutated
in place. Per side it holds a dense list indexed by price atom of length
`10^price_scale + 1` (allocated at first snapshot; scales above 4 use a sparse dict),
the set of occupied prices, and a cached best price. Set/increase/decrease/delete
are O(1); removing the best level scans up to 64 adjacent slots and otherwise takes
`max`/`min` of the occupied set. A snapshot clears only previously occupied slots,
then writes the publisher's ascending ladder without sorting. `Book.levels(side, n=None)`
returns `(price, quantity)` tuples, bids descending and asks ascending (sorted only
when asked); `best_bid()`/`best_ask()` return one pair or `None`; `bids`/`asks` are
lazy properties. Invalid/not-initialized books have no levels. `revision`,
`validity`, `dependency`, `as_of` and `reason` remain attributes.

There is no per-cut staging copy and no frozen cut body: the hook receives the
parsed body and the decoder's live read-only book mapping. **Books, cut bodies and
everything reachable from them are valid only during that hook invocation.** A
strategy that needs any value afterwards (for example the prior state at a later
scope boundary) must copy it explicitly; tuples returned by `levels()`/`best_*()`
are already copies. Strategies must not mutate bodies. Atomic per-cut visibility is
no longer provided because it is no longer needed: any decode failure poisons the
decoder, the hook is never called for that cut, nothing is ACKed, and the attempt
dies. The `initial` and `terminal` bodies are still frozen.
Views are entirely local; there is no remote per-book request, cache, implicit 1-p,
or wall-clock expiry.

## Redis requirements, limits, ACK and progress

Redis ≥8.2, positive configured `maxmemory`, and `maxmemory-policy noeviction`
are mandatory at publisher setup. Setup also requires
`10 × maxmemory ≥ 13 × max_queue_bytes` (exact integers): stream/node/group/hash
overhead was measured at about 22% of payload, plus headroom, so a full queue
cannot by itself drive Redis into OOM. The 64 MiB `small` preset therefore needs
at least 83.2 MiB (a 150 MiB or the default 512 MiB Redis passes). Every setup
requirement fails before any key is created, as a `Protocol` error (exit 20): it
is a deterministic deployment mismatch that a retry against the same Redis
cannot cure. Clients need INFO/CONFIG GET, EVAL, stream/group
commands and hash commands. A trusted standalone Redis endpoint is supported;
Redis Cluster routing and TLS are not implemented by this V1 Rust dependency.
Use isolated networking/ACLs; never put credentials in config/logs.

Connect/read/write timeouts are finite. Python uses redis-py with zero retry;
Rust uses synchronous redis crate commands with no retry. Every SDK poll batches
`XREADGROUP GROUP ... worker COUNT ... BLOCK ... STREAMS ... >`. Batch count ×
max entry bytes must fit the local byte budget before reading. The default budget
is 128 MiB; by default the count is `min(1024, budget // max_entry_bytes)` (128
entries at a 1 MiB cap). The supervisor adapter uses the same default, raised to
one entry when the entry cap exceeds it. One cut is one
entry and cannot be split. Publisher bounds entries before send; the Redis script
also checks entry size and retained payload byte total before XADD. An entry above
`max_entry_bytes` can never fit and remains a fatal `resource_limit` error. When an
entry fits but the retained total would exceed `max_queue_bytes`, `publish` returns
the non-error reply `FULL` and writes nothing (no XADD, byte, size or `published`
change, and no poison). The Rust publisher then sleeps and retries the identical
command (same sequence and previous value) with exponential backoff from 1 ms,
doubling, capped at 50 ms, and reset after each successful publish. The `poisoned`
check runs before the size checks, so a poisoned attempt still stops a waiting
publisher. The queue byte limit therefore still bounds memory; no unread data is
deleted or dropped and nothing is appended past the limit. The publisher has no
wait deadline of its own: the supervisor's stall, attempt and run deadlines bound
the wait, and its no-progress budget bounds retries (a waiting publisher alone is
not progress; only consumer ACKs are). Redis OOM is unchanged: still fatal to the
attempt as a `Resource` failure.
On exit after terminal or failure the CLI prints one stderr diagnostic line,
`replay-publish: queue_full_waits=N wait_ms=M`; it is not persisted or parsed.
Redis maxmemory bounds stream/node/group/hash overhead beyond the payload byte budget.
Risk/walker limits independently bound the one in-flight cut and retained books;
consumers retain current books and one bounded batch, not history. Returned cuts
retained by callers are caller memory. There is no capacity reservation against
other clients; Redis OOM remains a visible fatal attempt failure.

A poll applies each entry and calls the hook for it in order; after **every hook
in the batch has returned**, one batched `ack` script call verifies the group's
completed sequence equals the expected previous value, verifies with one bounded
`XPENDING` range that exactly the batch IDs are pending for `worker`, issues one
`XACKDEL stream group ACKED IDS n id…`, and advances the completed entry sequence
to the batch's last entry. A hook exception poisons the attempt with nothing in
that batch ACKed. Single-entry ACK is the `n = 1` case. Redis's [official XACKDEL specification](https://redis.io/docs/latest/commands/xackdel/)
states ACKED deletes only after **all groups have read and acknowledged**. Publish
records each entry's payload length as `size:<entry sequence>` in the state hash;
per-ID result 1 (deleted) subtracts and removes that stored size, 2 retains it for
other groups, anything else fails `ack_failed`. Byte accounting therefore needs no
`XRANGE`, and deleting the two attempt keys still removes all attempt state.
Scalar state is read with one `HMGET` per script call. Fixed membership (`XINFO
GROUPS`/`CONSUMERS`) is verified only by `join` and `check`; the `poisoned` flag is
checked by every operation. KEEPREF (default)
is unsafe here. No unconditional XDEL, MAXLEN, approximate trim, or PEL claiming is
used. Lua commands can fail after a prefix (not transactional rollback); every
script error is fatal and cannot authorize finalization.

`Publisher.progress()` / `Consumer.progress()` (`check`) returns only these named
progress fields, never the per-entry `size:*` fields (the supervisor likewise reads
named fields with `HMGET`):
`published`, `terminal` (empty until published), `done:<group>` (after hook+ACK),
`poisoned`, and byte/membership bookkeeping. These use **Redis entry sequence**,
one greater than wire sequence; `-1` means no published/completed entry. Terminal
publication is not all-consumer completion. `Consumer.finish()` requires local
validated terminal and returns false until all `done:<group> == terminal`.
Only after that and supervisor confirmation that every participant remains
successful may strategy output be finalized. Progress is not itself a SUCCESS
receipt, and the SDK never creates one. A supervisor must impose a finite overall
attempt deadline for a stuck/missing participant and preserve fatal local errors.

## Minimal API and CLI

Rust: `Publisher::open(redis_url, Config, RiskLimits)`, then `step()` until false;
`progress()` exposes completion facts. `Error::{Protocol,Risk,Poisoned}` are
nonretryable for that input/attempt; `Transport` and `Resource` are typed transport
or resource failures. Every error still kills the attempt. Risk's diagnostic
strings are **not** parsed into retry classes. No retry policy is prescribed.

`replay-publish CONFIG.json` requires `REDIS_URL` in its environment (not printed).
Strict config is ≤1 MiB, with fields:

```json
{
  "run_id":"run-1", "attempt_id":"attempt-1", "scope":"research",
  "normalizer":{"identity_version":1,"venues":[
    {"venue":"kalshi","bundle_id":"prediction-indexer/kalshi-normalizer/v5","parser_version":5,"config":{"schema_version":2,"variables":{"price_scale":{"type":"unsigned","value":4},"quantity_scale":{"type":"unsigned","value":2}}}},
    {"venue":"limitless","bundle_id":"prediction-indexer/limitless-normalizer/v2","parser_version":2,"config":{"schema_version":1,"variables":{"price_scale":{"type":"unsigned","value":3},"quantity_scale":{"type":"unsigned","value":6}}}},
    {"venue":"polymarket","bundle_id":"prediction-indexer/polymarket-normalizer/v3","parser_version":3,"config":{"schema_version":1,"variables":{"accept_additive_fields":{"type":"boolean","value":true},"price_scale":{"type":"unsigned","value":4},"quantity_scale":{"type":"unsigned","value":6}}}}
  ]},
  "inputs":[{"directory":"/pinned/derivative-address","derivative_address":"<64 lowercase hex>","receipt_sha256":"<64 lowercase hex>"}],
  "start_ns":"0", "end_ns":"100", "lower_bound":"clip",
  "plans":[{"instrument":"kalshi:A","orientation":"outcome","lane":"x","venue":"kalshi","price_scale":"4","quantity_scale":"2"}],
  "groups":["strategy-a","strategy-b"], "command_timeout_ms":5000,
  "max_entry_bytes":1048576, "max_queue_bytes":67108864
}
```

Before constructing a Redis client, the publisher recomputes the composite
bundle/config descriptor from `normalizer`, verifies every pin through the strict
metadata reader, requires profile 2, and matches both manifest normalizer digests.
Every plan venue must be present and its price/quantity scales must equal the
typed identity. These are exit-20 input failures and cannot create Redis keys.
`normalizer` is intentionally not added to the V1 initial stream record: the
existing wire remains byte-compatible, while supervisor identity binds the full
transport configuration.

Operational config limits above are JSON numbers; **wire** integers are strings.
Identifiers are 1–128 ASCII alphanumeric/underscore/hyphen/dot. Groups are distinct,
1–128 entries; queue payload cap ≤1GB; timeout ≤60s. Keys are
`replay:<scope>:<run>:<attempt>:stream` and `...:state`. Existing keys fail setup;
never resume them. Supervisor owns deleting only its disposable keys after all
participants stop. CLI exit 0 means terminal published, **not** strategy success.
Exit 20 is nonretryable input/risk/protocol failure; 21 is transport/resource
failure. An optional second argument names a new setup-ready file, created only
after setup and initial publication; its contents are not a success marker.
CLI uses default RiskLimits; library callers can supply tighter limits.

The supervisor uses `replay-publish --validate-only` with the same bounded closed
configuration on stdin. This read-only mode runs `Config::validate` (including
strict pinned metadata inspection) and exits without requiring `REDIS_URL`,
creating a Redis client, writing readiness, or publishing records. Exit 0 here
attests metadata preflight only, not a completed replay.

Python optional install: `.venv/bin/pip install -e '.[replay-redis]'`.

```python
consumer = Consumer(redis_url, scope=scope, run_id=run_id,
    attempt_id=attempt_id, group="strategy-a", initial=expected_initial_body,
    timeout=5)  # default batch: 128 MiB, min(1024, 128 MiB // entry cap) entries
while not consumer.terminal:
    consumer.poll(write_provisional_cut, block_ms=100)
# Caller/supervisor deadline remains mandatory while other groups finish.
all_processed = consumer.finish()
consumer.close()
```

The hook receives initial, cuts and terminal; it may ignore control records for
strategy logic but must return successfully for their ACK. Whatever it keeps from a
cut or book past its own return must be an explicit copy. Caller exceptions are
preserved and poison the attempt. No hook is retried. `abort()` explicitly poisons;
`close()` only releases the client. `Decoder` is the offline/no-network wire API.

## Verification

Default tests are offline. Frozen `transport/tests/fixtures/contract.ndjson` is
generated from small canonical evidence through the real materializer, walker and
risk engine; Rust asserts byte equality and Python independently checks levels,
trades, revisions, dispositions (via explicit hook-time copies), the spelled-out
terminal digest, and decoder poisoning. The O(1) guards, gaps/duplicates, terminal
digest mismatch, missing tail, in-place book operations against a reference ladder
(dense and sparse), and bounded retained memory have offline tests. `REPLAY_UPDATE_GOLDEN=1` deliberately regenerates the fixture.

Explicit **disposable server only**: Redis integration tests issue CLIENT PAUSE and
temporarily CONFIG SET maxmemory to force errors, restoring it afterward. Never
point them at shared Redis. No default tests contact the network.

```bash
.venv/bin/python -m unittest replay.tests.test_streams
# Set REPLAY_REDIS_URL to the disposable local Redis endpoint only.
.venv/bin/python -m unittest replay.tests.test_streams_redis -v
cargo test --manifest-path engine/Cargo.toml -p replay-transport \
  --test contract -- --ignored --test-threads=1 --nocapture
```

Live tests cover two groups, unread retention/last-ACK deletion, batched ACK
(sequence/pending failures, stored-size accounting back to zero), membership only at
join/check, hook-before-ACK,
caller failure, duplicate delivery, group removal/single join, truncated tail,
timeouts, OOM, oversized entries, queue-full `FULL` replies that write nothing,
a publisher that waits on a full queue and completes in order once consumers resume,
the setup maxmemory guard, publisher poisoning/no terminal, and actual Rust CLI
→ Redis → two Python consumers. They do not certify production deployment,
availability, adversarial Redis mutation, global exactly-once effects, or a retry
supervisor.
