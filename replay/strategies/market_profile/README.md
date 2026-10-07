# Market profile

Reports per-book state, spreads, depth, activity, quote stability,
self-crossing and pair consistency. It makes no economic judgment and
requires no fee catalog.

The standalone factory and completed reader live in `strategy.py`. They
reuse the [SDK collector](../../economic_sdk/profile.py) and
[profile reader](../../economic_sdk/profile_reader.py), also available
inside any economic strategy through `Requirements.profile`. The profile
policy is specified in [SDK section 7](../../../docs/ECONOMIC_STRATEGY_SDK_V1.md#7-market-profile).
Review bucket widths, venue tick atoms and size choices in the example
against your pinned plan scales; they are illustrative research settings.

## Policy version 2

A version-2 policy has the same closed fields. Its `groups` may also contain
`availability` and `transitions`, which are allowed only here (the standalone
group); an embedded profile that requests either fails at construction. A
version-1 policy writes exactly the files and bytes it always did. Example:
[config.v2.example.json](config.v2.example.json). Specification:
[MARKET_PROFILE_V2](../../../docs/specs/MARKET_PROFILE_V2.md).

The file set follows the policy: `incidents.ndjson`, `pair_profile.ndjson` and
`profile.ndjson`, plus `availability.ndjson` and `transitions.ndjson.zst` when
those groups are on. The manifest `version` is 2 for a version-2 policy; the
`transitions.ndjson.zst` entry is `{logical: {sha256, byte_length, records},
stored: {sha256, byte_length}}` and the other entries are unchanged.

- **`availability`** runs the interval engine of
  [`replay/economic_sdk/availability.py`](../../economic_sdk/availability.py),
  also used by `bundle_coverage`. `availability.ndjson` is byte-identical to
  bundle coverage's `intervals.ndjson` for the same snapshot and stream, and the
  summary's `availability_durations` equals coverage's `durations`. It is read by
  the same rules as coverage
  ([`availability_reader.py`](../../economic_sdk/availability_reader.py)), plus one
  cross-check: a book's `usable` time in availability rows equals the sum of its
  profile rows' `state.usable_ns`.
- **`transitions`** writes one row per (cut, scoped planned book) for every book
  transition at or after the requested start: the best quotes before and after, the
  quantity that entered and left each native side (in total and at the pre-cut best
  level), depletion, moves in atoms and ticks, `reason` (`snapshot`,
  `invalidation`, `insert` or `unknown`, never a cause), the non-duplicate trades
  and the venue times that share the cut. The collector keeps its own mirror of
  every planned book's levels and checks it against the decoder on every cut; a
  mismatch fails the run. Rows are compressed with `encoder.encode_stream` into one
  frame. Per-book constants are in the summary's `transition_books` table, which
  rows index with `book`. Row counts, scope times and trade attachment are in
  `transition_rows`, `transition_scopes` and `transition_trades`. The reader
  ([`transitions_reader.py`](../../economic_sdk/transitions_reader.py)) never
  imports the collector.

Notes on what the rows do and do not say:

- `first_event_ns`/`last_event_ns` are the earliest and latest event time among the
  cut's book events for the book; `event_kind` and `event_resolution` are copied,
  or `"mixed"` if the events disagree. Quotes, flows, moves and ticks are decimal
  strings.
- A trade in a cut that has no transition for its book belongs to no row.
  `transition_trades.unattached` counts them.
- Row order inside one instant is by book index, but a reader cannot tell two cuts
  at the same nanosecond apart, so Kalshi projected asks are checked against the
  counterpart's bid states at that instant.
- A first row's `prev_*` is checked against the scope-entry quotes only when
  `top_of_book` is on and the row is after the scope start.

Factory: `replay.strategies.market_profile:build`.
Completed bench reader: `replay.strategies.market_profile:read_completed`.

Configuration: [config.example.json](config.example.json). Full contract: [SDK profile contract](../../../docs/ECONOMIC_STRATEGY_SDK_V1.md#7-market-profile).
Tests remain under `replay/tests/`; shared SDK and bench instructions are
linked from the [strategy index](../README.md).
