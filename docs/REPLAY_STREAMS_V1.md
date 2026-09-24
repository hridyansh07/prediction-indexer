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
is no API for adding a group. Every publish, poll and ACK checks membership. External
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

Envelope: `{version:"1", run_id, attempt_id, sequence, kind, body}`.

- `initial`, sequence 0: `pins` (`derivative_address`, `receipt_sha256`),
  `start_ns`, `end_ns`, `lower_bound`, `plans`, `groups`, `max_entry_bytes`,
  `max_queue_bytes`. Each plan has `instrument`, `orientation`, `lane`, `venue`,
  `price_scale`, `quantity_scale`. Explicit ordered configuration is the identity;
  no lookup of current/latest data is permitted.
- `cut`, sequences 1…N: `origin`, ordered `market_events`, affected-only
  `book_transitions`. Origin is `window {pin,start_ns,end_ns}` or
  `group {pin,first,last,visible_ns}`. An address has `canonical_seq`, `lane`,
  `delivery_index`, `event_index`. A reference has `pin`, `address`, `visible_ns`,
  `order_ns`. All pins must belong to initial selection.
- Each market record has `reference`, `event {kind:book|trade,value}`, and
  `disposition` (`observed`, `applied`, `duplicate`, `not_authority`, `invalidated`).
  Original domain BookEvent/TradeEvent serialization is retained, except all
  integers become strings. Fulls, repeated deltas and trades remain ordered and
  uncoalesced, including invalidated/duplicate observations and uninterpreted hashes.
- A transition has `key {instrument,orientation}`, `previous_revision`, `revision`,
  `dependency` (null, or `{epoch,anchor,through}`), and `decision`. Decisions are
  `snapshot {bids,asks}` (ascending `[price_atoms,quantity_atoms]` pairs),
  `operations {operations}` (ordered domain BookDelta values), or
  `invalidation {reason}`. Reason variants are explicitly enumerated in
  `protocol.py::reason`, mirroring Rust risk reasons including interval details.
  **Ordinary delta transitions never contain the full ladder.** Full market
  observations remain full because they are original input, not redundant views.
- `terminal`, sequence N+1: `{cuts:N}`. Neither an empty poll, producer process
  exit, nor the last data cut is terminal. Missing tail cannot pass `finish()`.

The Python decoder checks contiguous stream sequence and each affected book's
previous/new revision. It validates/stages the complete cut before replacing local
state. It mechanically applies the authoritative operations with exact integers;
arithmetic/scale errors are protocol corruption, not new risk decisions. Consumers
must not recompute whether an observation should have applied. Immutable owned cuts
retain tuple ladders and deeply read-only metadata; all transitions are installed
before the processing hook. `Book.levels("bid")` is descending; asks ascending.
Invalid/not-initialized books have no levels. Views are entirely local; there is
no remote per-book request, cache, book hash, implicit 1-p, or wall-clock expiry.

## Redis requirements, limits, ACK and progress

Redis ≥8.2, positive configured `maxmemory`, and `maxmemory-policy noeviction`
are mandatory at publisher setup. Clients need INFO/CONFIG GET, EVAL, stream/group
commands and hash commands. A trusted standalone Redis endpoint is supported;
Redis Cluster routing and TLS are not implemented by this V1 Rust dependency.
Use isolated networking/ACLs; never put credentials in config/logs.

Connect/read/write timeouts are finite. Python uses redis-py with zero retry;
Rust uses synchronous redis crate commands with no retry. Every SDK poll batches
`XREADGROUP GROUP ... worker COUNT ... BLOCK ... STREAMS ... >`. Batch count ×
max entry bytes must fit the local byte budget before reading. One cut is one
entry and cannot be split. Publisher bounds entries before send; the Redis script
also checks entry size and retained payload byte total before XADD. Queue exhaustion
fails immediately instead of deleting unread data or appending indefinitely. Redis
maxmemory bounds stream/node/group/hash overhead beyond the payload byte budget.
Risk/walker limits independently bound the one in-flight cut and retained books;
consumers retain current books and one bounded batch, not history. Returned cuts
retained by callers are caller memory. There is no capacity reservation against
other clients; Redis OOM remains a visible fatal attempt failure.

After full application **and successful hook return**, a script verifies pending
ownership, calls `XACKDEL stream group ACKED IDS 1 id`, and advances the group's
completed entry sequence. Redis's [official XACKDEL specification](https://redis.io/docs/latest/commands/xackdel/)
states ACKED deletes only after **all groups have read and acknowledged**. Return
1 deletes/decrements retained bytes; return 2 retains for others. KEEPREF (default)
is unsafe here. No unconditional XDEL, MAXLEN, approximate trim, or PEL claiming is
used. Lua commands can fail after a prefix (not transactional rollback); every
script error is fatal and cannot authorize finalization.

`Publisher.progress()` / `Consumer.progress()` returns small progress facts:
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
    {"venue":"kalshi","bundle_id":"prediction-indexer/kalshi-normalizer/v4","parser_version":4,"config":{"schema_version":2,"variables":{"price_scale":{"type":"unsigned","value":4},"quantity_scale":{"type":"unsigned","value":2}}}},
    {"venue":"limitless","bundle_id":"prediction-indexer/limitless-normalizer/v2","parser_version":2,"config":{"schema_version":1,"variables":{"price_scale":{"type":"unsigned","value":3},"quantity_scale":{"type":"unsigned","value":6}}}},
    {"venue":"polymarket","bundle_id":"prediction-indexer/polymarket-normalizer/v2","parser_version":2,"config":{"schema_version":1,"variables":{"accept_additive_fields":{"type":"boolean","value":true},"price_scale":{"type":"unsigned","value":4},"quantity_scale":{"type":"unsigned","value":6}}}}
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
    timeout=5, batch_entries=16, batch_bytes=16 * 1048576)
while not consumer.terminal:
    consumer.poll(write_provisional_cut, block_ms=100)
# Caller/supervisor deadline remains mandatory while other groups finish.
all_processed = consumer.finish()
consumer.close()
```

The hook receives initial, cuts and terminal; it may ignore control records for
strategy logic but must return successfully for their ACK. Caller exceptions are
preserved and poison the attempt. No hook is retried. `abort()` explicitly poisons;
`close()` only releases the client. `Decoder` is the offline/no-network wire API.

## Verification

Default tests are offline. Frozen `transport/tests/fixtures/contract.ndjson` is
generated from small canonical evidence through the real materializer, walker and
risk engine; Rust asserts byte equality and Python independently checks levels,
trades, revisions, dispositions, immutable old cuts and atomic rejection. Numeric
and schema corruption, gaps/duplicates, missing tail and bounded retained memory
have offline tests. `REPLAY_UPDATE_GOLDEN=1` deliberately regenerates the fixture.

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

Live tests cover two groups, unread retention/last-ACK deletion, hook-before-ACK,
caller failure, duplicate delivery, group removal/single join, truncated tail,
timeouts, OOM, queue limits, publisher poisoning/no terminal, and actual Rust CLI
→ Redis → two Python consumers. They do not certify production deployment,
availability, adversarial Redis mutation, global exactly-once effects, or a retry
supervisor.
