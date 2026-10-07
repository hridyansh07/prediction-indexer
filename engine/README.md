# Replay engine

A Rust workspace (`engine/Cargo.toml`) holding the venue-independent Replay
domain, the normalizers that turn one canonical window into an immutable,
verified normalized derivative, the pull-based derivative walker, the
reconstruction risk engine, and the Redis publisher. It contains no strategy,
fee logic, deployment or scheduler.

| Crate (package) | Path | Role |
|---|---|---|
| `replay-domain` | `crates/domain` | Exact numerics, closed event schema, canonical JSON, prepared mutation |
| `canonical-normalizer` | `crates/normalize` | Canonical envelope decode and the `VenueAdapter` lifecycle |
| `replay-normalizers` | `crates/replay-normalizers` | Kalshi, Polymarket and Limitless adapters; `materialize_range` example binary |
| `replay-materialize` | `crates/materialize` | Immutable derivative build, strict audit, pinned verified reader |
| `replay-tape` | `crates/tape` | `DerivativeWalker`: window selection, scope filter, atomic groups |
| `replay-risk` | `crates/risk` | `RiskEngine`: book reconstruction and immutable `RiskCut`s |
| `replay-transport` | `crates/transport` | `replay-publish` binary and Redis stream delivery ([`docs/REPLAY_STREAMS_V1.md`](../docs/REPLAY_STREAMS_V1.md)) |

The walker and risk contracts are in [`DERIVATIVES.md`](DERIVATIVES.md). Canonical
inputs come from the ingester ([`ingester/README.md`](../ingester/README.md)).

```bash
cargo fmt --manifest-path engine/Cargo.toml --all --check
cargo test --manifest-path engine/Cargo.toml --workspace
cargo clippy --manifest-path engine/Cargo.toml --workspace --all-targets --all-features -- -D warnings
```

## Representation contract

- `Magnitude` is unit-free unsigned exact arithmetic (`u64` atoms plus scale),
  never serialized alone.
- `Qty` wraps `Magnitude` with `QuantityUnit::Contracts` and is nonnegative.
  Persisted atoms are capped at `i64::MAX`; constructors, parsing, arithmetic,
  rescaling and deserialization all enforce it. `PositiveQty` makes zero
  unrepresentable for levels, trades and level changes.
- `Px` is the nonnegative `i64` fixed-point price primitive in `10^-scale` quote
  units per contract. `ConditionalMarketPrice` wraps it and rejects values
  outside `[0, 1]`; it never clamps or rounds.
- `DecimalScale` is 0 to 18 inclusive. Decimal input is plain ASCII
  `[0-9]+(.[0-9]+)?`: no whitespace, sign, exponent, missing whole/fractional
  digits or binary floats. A venue adapter may consume a directional wire sign
  before constructing domain values.
- Extra fractional zeroes are exact and accepted; a non-zero discarded digit is
  inexact. Checked rescaling distinguishes `Underflow` (non-zero below the
  coarser quantum), `InexactRescale` and `Overflow`. There is no rounding policy.
  Representation equality includes atoms, scale and unit, so callers rescale
  explicitly before comparing.
- `Side::{Bid, Ask}` is book side. `ContractOrientation::{Outcome, Complement}`
  independently records whether a price refers to the named outcome or its
  logical complement; the domain performs no implicit `1 - p` conversion.
- `LevelChange::{Set, Delete, Increase, Decrease}` is direction-explicit and every
  quantity-bearing variant holds a `PositiveQty`.
- `BookStateHash` records its algorithm; the `Sha1` variant is a strict
  lowercase 40-hex newtype for Polymarket full-state evidence, distinct from the
  typed SHA-256 identities used for canonical and derivative content (all
  `indexer_types::Sha256`, canonical lowercase hex).

Currency, price bands, tick/lot schedules, payout denomination, conversion, fee
arithmetic and strategy rounding are not decided here.

## Book identity

`BookKey { instrument, orientation }` identifies a captured venue-native ladder.
`FullBook`, `BookDelta` and `AuditAnchor` expose `book_key()`. A full snapshot
resets only its key.

- Kalshi exposes two bid ladders per market ticker. YES bids use
  `(kalshi:TICKER, Outcome)` and NO bids `(kalshi:TICKER, Complement)`; they are
  complementary views, not independent liquidity, and normalization does not
  convert between them. Empty `asks` means no explicit ask ladder was emitted. An
  `orderbook_snapshot` top level is closed to `type`, `sid`, `seq`, `msg` and an
  optional positive `id` echoing a `subscribe`/`get_snapshot` command. A `msg`
  with `market_id: ""` treats it as absent, and a snapshot with no level arrays
  is a valid empty book (both orientations empty Fulls).
- Polymarket gives each outcome its own token: `polymarket:TOKEN_ID` instruments,
  all `Outcome` orientation. Token IDs are never collapsed to a condition ID and
  the NO token is never a `Complement` of the YES token. An exact empty
  `last_trade_price` is treated as absent; nonnumeric, null or wrong-type values
  reject. A `tick_size_change` is ignored only after its asset, market,
  timestamp, old tick and new tick all validate; ignored members of a batched
  delivery do not suppress ordered events or validation of later members, and the
  whole delivery is ignored only when every member is a valid ignore.
- Limitless full books replace, never merge, and retain original ordering and
  provenance.

Normalizer bundles: `prediction-indexer/kalshi-normalizer/v5`,
`polymarket-normalizer/v3`, `limitless-normalizer/v2` (parser versions in each
module's `mod.rs`).

## Closed event contract

`SegmentRecord` schema version 3 owns `EventHeader`, `EventAddress`, complete
canonical provenance and a closed `SegmentEvent`. Book events use validated
constructors, bid-descending/ask-ascending ordering, one scale per full book,
positive quantities, conditional-market prices and no duplicate prices.
`NormalizationFault` carries a closed impact classification chosen before book
state; exact rejected bytes and parser error codes live in the reject sidecar.

Canonical JSON is compact UTF-8 from `SegmentRecord::to_canonical_json`; struct
field order and adjacent enum tags are schema. `from_canonical_json` (writers,
audits, tests) rejects unknown fields/variants, unsupported versions, invalid
domain states, alternate field order and whitespace by decode/validate/re-encode
equality. `SegmentRecord::from_json` is the single-pass pinned-read decode: it
checks the version first and rejects the same malformed, unknown, duplicate,
reordered, unsupported and invalid inputs but does not prove canonical spelling
(the install-time SHA-256 binding does). An LF is added only when framing NDJSON.

`EventAddress.event_index` is zero-based venue-array order; `child(index)` changes
only that index and preserves canonical sequence, lane and delivery index.
Position and event index are serialization order, never event time.

`Revisioned<T>::prepare` validates against immutable state and returns an opaque,
owned `PreparedMutation<T>`; `apply` checks that the current revision equals
`prepared_from` (a stale mutation writes nothing), then performs only the
complete replacement and revision advance.

## Canonical to normalized

The ingester owns `CanonicalSelection`, `AuditedCanonicalReader`,
`JoinedCanonicalRecord` and canonical receipt identities; Replay duplicates none
of them. `canonical-normalizer` converts each audited joined record exhaustively:

```text
JoinedCanonicalRecord                    replay-domain
canonical_seq --------------------------> EventAddress.canonical_seq
event_address.lane_id ------------------> EventAddress.lane
event_address.delivery_index -----------> EventAddress.delivery_index
normalizer child array position --------> EventAddress.event_index
order_ns / visible_ns / tie group ------> EventHeader clocks/tie
record_id ------------------------------> EventHeader.record_id
source digest/line/content/continuity ---> CanonicalProvenance
```

The continuity enum has the same ten closed labels (exhaustive match, no string
fallback). Derivative publication requires a `CanonicalSelection`, whose reader
yields its audited result only at verified EOF.

`Normalizer<A>` decodes each canonical envelope and raw JSON payload, routes by
venue, hashes the adapter's closed key-sorted configuration, and delegates wire
semantics through `VenueAdapter`. It returns zero or more closed `SegmentEvent`
children, an explicit ignored reason, or an expected `ParseReject`, and has a
final consistency `finish()`. `serde_json` arbitrary-precision numbers are
enabled at this seam because Limitless publishes financial values as JSON
numbers; adapters read the original lexeme into fixed point and never go through
binary floating point (Kalshi still requires decimal strings or exact integers).
The seam rejects serde_json's private `Number`/`RawValue` object keys before
deserialization.

Production uses `replay-normalizers::CanonicalNormalizer::default()`: one
derivative holds the decisions and source evidence of every Kalshi, Limitless and
Polymarket delivery in its window. The venue adapters are private modules and
cannot be wrapped as a public single-venue normalizer. Internal deliveries are
explicit ignores, unknown envelope venues fail closed, and all three finish hooks
run in order returning the first error. Zero children is an intentional ignore
recorded in the sidecar. An expected reject produces both an exact-envelope
sidecar record and one paired `NormalizationFault`; a normalizer error, panic,
invalid domain value, audit failure, serialization failure or sink failure is
fatal and publishes no receipt.

## Derivative layout and materialization

`replay-materialize::build_window` takes the exact bounds of one canonical
receipt. A caller wanting a range builds each canonical receipt independently and
pins the resulting `DerivativePin` values.

```text
window=<window-start-ns>/<derivative-address>/
  events.ndjson.zst
  rejects.ndjson.zst
  sources.ndjson.zst      # one transport record per source delivery
  manifest.json
  receipt.json            # sole commit marker, written last
```

The domain-separated address binds the complete source receipt identity and
bounds, normalized schema version, normalizer bundle and config digests, policy
digest and effective interval, event/reject serialization versions and
materializer version. There is no mutable `latest`; corrected versions coexist
under new addresses.

**Current writer profile 2**: normalized schema 3, receipt/manifest/materializer
version 2, event/reject serialization 1, reject record 1. The source projection
embeds the exact UTF-8 canonical receipt document (its bytes must match the
source SHA-256 and length) and its independently checked coverage:
expected/present/missing/invalid lanes and clock faults. `sources.ndjson.zst`
holds one closed record per source in source-union order,
`{"source_version":1,"header":<child-0 EventHeader>,"connection_epoch":"..."}`,
bound by logical and stored identities in manifest and receipt, with a count equal
to `input_records`. The `connection_epoch` is the splice connection identity of
the delivery, not a venue cursor or version. Reject IDs bind the derivative
address.

**Frozen profile 1** (manifest/receipt/materializer 1) keeps separate closed
metadata readers (`materialize/src/profile1.rs`), canonical byte verification
before conversion, and its original address algorithm; it never invents
coverage or epoch fields. Address verification uses the receipt's recorded
versions, never current writer constants. Unknown profile tuples and
experimental schemas 1/2 are unsupported; there is no migration or readdressing.

All NDJSON files use the shared level-3, checksummed, one-frame Zstandard codec
with logical and stored identities. `verify_derivative` (strict audit) checks
canonical JSON, closed versions and fields, frame EOF and both identities,
event/child order, exact reject-envelope provenance and one-to-one reject/fault
pairing.

Builds use unique private staging directories and apply the default reader's
delivery-level checks to every emitted delivery (line, group, tie-run and lane
limits; dense sequence, time order, per-lane delivery order; equal-time tie tags;
at EOF the window and agreement with the receipt's coverage and clock claims). A
fault event is only written paired with its reject. After EOF and both `finish()`
calls a build finishes and fsyncs frames, renames and fsyncs data, writes and
fsyncs the manifest, checks metadata-document limits, re-hashes every staged file
against the receipt's stored identities, atomically publishes the uncommitted
directory, then writes/fsyncs/renames the receipt last. It does not decode its own
output.

The strict audit runs on an existing committed address (verify/no-op) and through
`materialize_range --inspect-pin`, which Replay's archive download path calls.
Replay's bundle cache binds a locally installed derivative to its pin by receipt
hash and per-file stored SHA-256 (`replay/jobs/bundle.py::_check_pinned_files`).

Each build holds a per-address OS advisory lock from before stage creation through
publication and cleanup. Before writing, every run scans its output root for
`window=*/.{address}.{pid}.{nonce}.open` stages and prunes only unreceipted ones
whose lock it can acquire without waiting; busy stages, directory symlinks,
unrelated paths, committed derivatives and lock files are preserved, and cleanup
failures abort before new writes. Use a dedicated output root on a filesystem
with working advisory locks, and stop older materializer binaries before
upgrading (they lock only during publication). An unreceipted final address
directory is crash debris and is rebuilt: identical receipt bytes verify/no-op,
different bytes at the same address are an immutable conflict. No raw, canonical
or archive object is pruned.

## `materialize_range`

`replay-normalizers/examples/materialize_range.rs` is the operator helper (an
example target, not a stable CLI, restorer or indexer). It reads a JSON request of
at most 1 MiB on stdin; `--describe` prints the producer identity (policy digest, materializer version, normalizer identity) and
`--inspect-pin` audits a pin. A request selects the minimal adjacent locally
committed canonical windows, builds one profile-2 composite derivative per exact
window, and emits only verified ordered pins. At most 4096 windows are read; with
half-hour canonical windows one request spans at most 85 days 8 hours. Windows
build concurrently in-process on up to `available_parallelism()` workers (honoring
a Linux cgroup CPU quota), each with a fresh normalizer; pins and response stay in
window order and are byte-identical to a sequential build. After a failure no new
window starts, in-flight windows finish, the earliest failing window's error is
reported, and already committed windows verify/no-op on retry.
Transport and Python supervisor preflight bind every selected manifest to the
helper's typed composite identity and derive plan scales from it before any Redis
command or child process.
