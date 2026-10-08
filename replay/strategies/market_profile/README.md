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
`availability`, `levels` and `transitions`, which are allowed only here (the
standalone group); an embedded profile that requests any of them fails at
construction. A version-1 policy writes exactly the files and bytes it always
did. Example: [config.v2.example.json](config.v2.example.json) (every group on).
Specification: [MARKET_PROFILE_V2](../../../docs/specs/MARKET_PROFILE_V2.md).

The file set follows the policy: `incidents.ndjson`, `pair_profile.ndjson` and
`profile.ndjson`, plus `availability.ndjson`, `transitions.ndjson.zst` and
`levels.ndjson.zst` when those groups are on. The manifest `version` is 2 for a
version-2 policy; each `.ndjson.zst` entry is `{logical: {sha256, byte_length,
records}, stored: {sha256, byte_length}}` and the other entries are unchanged.

- **`availability`** runs the interval engine of
  [`replay/economic_sdk/availability.py`](../../economic_sdk/availability.py),
  also used by `bundle_coverage`. `availability.ndjson` is byte-identical to
  bundle coverage's `intervals.ndjson` for the same snapshot and stream, and the
  summary's `availability_durations` equals coverage's `durations`. It is read by
  the same rules as coverage
  ([`availability_reader.py`](../../economic_sdk/availability_reader.py)), plus one
  cross-check: a book's `usable` time in availability rows equals the sum of its
  profile rows' `state.usable_ns`.
- **`transitions`** (the default order-flow stream) writes `top` rows and `trade`
  rows. A `top` row carries the best `bid` and `ask` and the `validity` after a
  cut, with a `cause` (`open`, `operations`, `snapshot` or `invalidation`). It is
  written only when `(bid, ask, validity)` differs from the book's previous row in
  the scope, or on a snapshot or invalidation. A `trade` row is one non-duplicate
  trade (`price`, `qty`, `aggressor`, `disposition`); a trade never changes a top
  row. No level state is kept: quotes come from the decoder's books. Kalshi is
  written only as the `outcome` (Yes) book, whose ask is the `complement` book's
  best bid at `P - p`; the `complement` book never appears. Every row has `cut` (the
  cut's sequence; rows of one delivered record share it), `t_ns`, and the optional
  keys `venue_ns`, `venue_first_ns`, `venue_kind`, `venue_res` and `sent_ns` (omitted,
  never null; kinds are copied, not converted). Previous quotes and moves are not
  stored: rebuild them with `iter_transitions`.
- **`levels`** (opt-in depth) writes `ladder` rows (the full ladder as an anchor, at
  scope entry and on every snapshot or invalidation) and `diff` rows (signed
  quantity change of every level that moved in a cut, net-zero levels omitted).
  For Kalshi a `complement` bid change at `q` is an `ask` change at `P - q` in the
  Yes book. The collector keeps a mirror of every planned book's levels and checks it
  against the decoder on every cut; a mismatch fails the run. A line over 1 MiB fails
  the run. The two files join exactly on `cut`.
- Both are stored as one Zstandard frame by `encoder.encode_stream`. Per-book
  constants are in the summary's `transition_books` table, which rows index with
  `book`. The reader ([`transitions_reader.py`](../../economic_sdk/transitions_reader.py))
  never imports the collector and decodes each file in two streaming passes without
  writing decoded bytes to disk.

Research iterators, in
[`replay.economic_sdk.profile_streams`](../../economic_sdk/profile_streams.py):
`iter_transitions(path)` adds `prev_bid`, `prev_ask`, `bid_move_atoms`,
`ask_move_atoms`, `bid_move_ticks` and `ask_move_ticks` to `top` rows, and
`iter_levels(path)` yields `(row, (bids, asks))` with the book's ladder after each
row. See [SDK section 7](../../../docs/ECONOMIC_STRATEGY_SDK_V1.md#7-market-profile).

Notes on what the rows do and do not say:

- Scope 0's opening rows are written at the first cut at or after the requested
  start, with `t_ns` equal to the start and the state after every earlier cut, so
  cuts before the start write nothing else. Later scopes open from the state before
  the cut that crosses their boundary.
- A trade is skipped, and counted in `transition_trades_skipped`, when its scales
  differ from the book's (`scale_mismatch`) or its key is not a written book, such as
  a Kalshi `complement` trade (`unwritten_book`). The counts come from the profile's
  `activity` and are `null` without it.
- A Kalshi Yes book whose `complement` is invalidated stays usable: its `top` row
  and `ladder` keep the bids and lose the projected asks.

Factory: `replay.strategies.market_profile:build`.
Completed bench reader: `replay.strategies.market_profile:read_completed`.

Configuration: [config.example.json](config.example.json). Full contract: [SDK profile contract](../../../docs/ECONOMIC_STRATEGY_SDK_V1.md#7-market-profile).
Tests remain under `replay/tests/`; shared SDK and bench instructions are
linked from the [strategy index](../README.md).
