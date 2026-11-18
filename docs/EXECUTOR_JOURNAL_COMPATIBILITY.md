# Executor journal price/time compatibility

## Delivery state

This is a compatibility handoff, **not an implemented Indexer reader or a passed
consumer acceptance gate**. No executor-journal consumer source path exists in
the inspected Indexer trees. Do not feed executor journals into Indexer's splice
envelope decoder: those are distinct protocols with distinct ordering contracts.
No Targeter, canonical-envelope, capture-clock or replay schema changes are made.

The executor producer is `hridyansh07/prediction-executor`, review branch
`review-type-integration`, commit `a3eebd05d257abc4067460a0d6985b3f6b8098be`.
It retains human review base `3ff2bb0c81023eb5fb7c97fdf70214c48d110007` and
integrates the rational and account refactors. No default executor branch merge
is implied. Its writer/schema paths are:

- `src/app/journal/unified.rs`, `src/types/journal/unified.rs` and
  `src/types/journal/strategy.rs`: market/control/strategy journal.
- `src/app/journal/execution.rs`, `src/types/journal/execution.rs` and
  `src/types/journal/account.rs`: separate execution/account journal.
- `src/types/time/mod.rs`: wall/monotonic instant contracts.

## Search coverage

Inspected the supplied local Indexer `master` at
`64a751fc5fc3b5291c2ef94c075c0a28e870afe9` (593 tracked files), then fetched
and inspected `origin/master` at
`a127fdf20ce63cb3ccd8885c3a942761f190a0bc` (657 tracked files). The isolated
`review/executor-time-schema` worktree is based on the latter; the original
checkout's tracked and untracked work was preserved.

The following searches returned no executor reader/schema matches in either tree:

```sh
git grep -n -i -E \
  'executor.journal|unified.market|strategy.terms|received_ns|capital.state|execution.journal|prediction.executor' \
  <revision> -- . ':!.env*' ':!*/.env*' ':!*.key' ':!Cargo.lock' ':!*.svg' ':!*.json' ':!uv.lock'
git ls-tree -r --name-only <revision> | rg -i 'executor|journal'
git log --all --oneline --grep='executor'
```

This covers tracked Python, Rust, TypeScript, tests, ordinary documentation and
build/config source, including `ingester/`, `engine/`, `replay/`, `archive/`,
`splices/`, `universe/` and `targeter/`. No generated data, secrets or root WIP
were consumed. Local/remote branch names were inspected; absence is established
for the two trees above, not every historical or inaccessible branch.
The latest tree's `gamestate/` `close_ns` fields are unrelated game-state output,
not executor journal consumption.

## Version dispatch contract

| Stream/version | Decode rule |
|---|---|
| Market v2 | Strategy terms `close` is Unix **milliseconds**. Checked multiplication by 1,000,000 is required to project into a u64-nanosecond wall instant. Preserve the original field/unit/bytes. |
| Market v3 | Strategy terms use **`close_ns`**, already Unix nanoseconds. Reject `close`; do not alias the old name or multiply the value. Raw records omit `boot`; `received_ns` stays process-local nanosecond ticks scoped by run identity. |
| Execution v1 | The positive-price `Prepared.summary` body is retained. Other historical records still require their own version-specific schema; this note does not implement one. |
| Execution v2 | The account milestone adds `kind: "account"`. Nested evidence `received_at` is a boot/nanos object, for example `{"boot":"3","nanos":7}`. |
| Execution v3 | Account evidence `received_at` is a scalar nanosecond tick, for example `7`. Raw captures omit `boot`. The prepared-summary body remains unchanged; the envelope version is explicitly 3. |

A consumer must select a strict schema using **both stream and version** before
decoding the body. Unknown versions and wrong-version field shapes must be
rejected, never guessed by magnitude or accepted through permissive aliases.
Market versions 1/2 with retained account bodies remain historical formats;
do not pretend the newer execution-account schema can decode them unchanged.

Nanosecond wall instants range from 0 through 18,446,744,073,709,551,615.
Legacy milliseconds 18,446,744,073,709 project to 18,446,744,073,709,000,000 ns;
18,446,744,073,710 must overflow and be rejected. Negative instants are invalid.
Monotonic ticks are not wall instants or durations and cannot be compared across
runs. Preserve actual executor consumed order; do not invent ordering between
the independent market and execution journals or substitute canonical lane order.

## Hash and price boundaries

Market v3 chains use the byte domain `prediction-executor/unified-market/v3\0`
(a trailing NUL), followed by the previous 32-byte digest and current 32-byte batch
hash. Historical market v2 uses the v2 domain. Hash original persisted bytes;
never reserialize or unit-convert evidence before verifying its legacy hash.

Current terminal account-state digests use
`prediction-executor/capital-state/v2\0` because retained facts now carry scalar
monotonic instants. Historical shared-journal v1/v2 and execution-v2 state views
use the old state-v1 domain and their original schema. The account model also
changes state fields; a new view cannot verify an old digest. Separate execution
lines have no market-style commit/hash chain; one must not be invented here.

Current `Price` accepts only 0 < p < 1, including after rounding and decoding.
Prepared adapter plans no longer fabricate a zero limit/notional/client ID:
`RequestSummary` exists only after a build supplies them. Its existing positive
price JSON representation stays unchanged. Historical endpoint prices must not
be silently clamped, rounded inward or promoted into the stricter current type.
Preserving legacy evidence is distinct from authorizing it as an order/book price.

## Frozen witnesses and acceptance still required

[`executor-journal-fixtures/`](executor-journal-fixtures/) contains byte-identical
copies of the producer's four frozen writer witnesses:

- `strategy-terms-v2.ndjson`: legacy `close: 123456` milliseconds.
- `strategy-terms-v3.ndjson`: explicit `close_ns: 123456000789` nanoseconds,
  deliberately retaining a sub-millisecond fraction.
- `execution-prepared-v1.ndjson` and `execution-prepared-v3.ndjson`: the same
  positive-price summary under different explicit envelopes.

These are small contract-shaped test lines, not a committed capture or complete
hash-verifiable journal. Do not reinterpret the v2/v3 terms samples as equal
instants: only the legacy projection is 123456000000 ns. Producer tests check
the current writer output and legacy positive-price summary decoding, reject
the old terms field in the strict current schema, verify the v3 market digest
domain across rolls, and exercise scalar account evidence through Application.

Remaining acceptance needs an actual Indexer consumer source/owner, strict
stream/version dispatch, checked legacy unit projection and independent old/new
fixture consumption, including malformed units, overflow and hash-domain checks.
No consumer migration or end-to-end compatibility is claimed by this document.
