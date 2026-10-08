# Market profile V2: transitions, levels and availability

Status: **proposed.** Base branch: `codex/venue-event-time`, whose cuts carry
`venue_time` on `market_events`.

This adds three opt-in groups to the market profile
(`replay/strategies/market_profile`, collector `replay/economic_sdk/profile.py`,
reader `replay/economic_sdk/profile_reader.py`):

- **`transitions`:** the default order-flow stream. One row whenever a book's
  best quotes or validity change, and one row per observed trade.
- **`levels`:** the opt-in depth stream. Full ladders at anchor points and
  per-cut level changes, for research on the shape of the book.
- **`availability`:** bundle coverage's book, member and bundle intervals,
  computed by the same engine so that `bundle_coverage` can later be retired.

The profile stays observational: no fees, economics, predictions or fitting.
Nothing is inferred about *why* a quantity changed. A Kalshi trade and the book
change that follows it stay separate observed rows.

Read first:

- [`docs/ECONOMIC_STRATEGY_SDK_V1.md`](../ECONOMIC_STRATEGY_SDK_V1.md) §7;
- [`replay/strategies/bundle_coverage/SPEC.md`](../../replay/strategies/bundle_coverage/SPEC.md);
- [`docs/specs/VENUE_EVENT_TIME.md`](VENUE_EVENT_TIME.md) §7, which says how
  to read each venue-time kind;
- [`encoder/README.md`](../../encoder/README.md).

## 1. Policy and files

- **Profile policy version 2.** The same closed fields as version 1. `groups`
  may also contain `availability`, `levels` and `transitions`. All three are
  allowed only when the profile runs standalone
  (`replay.strategies.market_profile:build`). An embedded profile
  (`Requirements.profile`) that requests any of them fails at construction.
  `levels` and `transitions` are independent; either may be requested alone.
- **Version 1 does not change.** A version-1 policy must produce byte-identical
  output to today. Existing tests and the complement golden pin this; keep
  them passing unmodified.
- **The file set is derived from the policy:**
  - version 1: `incidents.ndjson`, `pair_profile.ndjson`, `profile.ndjson`;
  - plus `availability.ndjson` when `availability` is on;
  - plus `transitions.ndjson.zst` when `transitions` is on;
  - plus `levels.ndjson.zst` when `levels` is on.

  The manifest's `files` map and the reader's `check_files` use exactly this
  set.
- **Manifest `version: 2`** for a version-2 policy. Each `.ndjson.zst` identity
  is `{"logical": {sha256, byte_length, records}, "stored": {sha256, byte_length}}`.
  The other files keep `{sha256, byte_length, records}`.
- `replay/strategies/market_profile/config.v2.example.json` has every group on.

## 2. Availability (the bundle_coverage fold)

Implemented; unchanged by this revision.

1. **Shared engine.** Bundle coverage's interval engine lives in
   `replay/economic_sdk/availability.py`. `bundle_coverage` calls it, and its
   outputs, tests and acceptance stay byte-identical.
2. **Same rows.** The profile's `availability` group writes
   `availability.ndjson`, byte-identical to bundle coverage's
   `intervals.ndjson` for the same snapshot and stream. The summary adds
   `availability_durations`, identical to coverage's summary `durations`.
3. **Same reader rules,** shared with `replay/strategies/bundle_coverage/output.py`,
   plus one cross-check: per scope and book, the `usable` duration in
   availability book rows equals the sum of `state.usable_ns` over that book's
   profile rows.
4. **Not part of this change:** retiring `bundle_coverage` from the jobs
   registry (`configs/replay_runner.json`, `replay/jobs/contracts.py`) and the
   UI presets.

## 3. Shared conventions for `transitions` and `levels`

### 3.1 The written book set

The summary's `transition_books` table lists the books that rows refer to, by
index (`book` in every row):

- **Polymarket:** every planned book. Each token is its own native book with
  native bids and asks.
- **Kalshi:** one book per market, the **`outcome` (Yes) orientation**. Its bid
  is its own native bid; its ask is projected from the `complement` (No)
  book's best bid at `P − p`, with the same quantity, exactly as the
  profile's `_derive` does. The `complement` book is never written: its quotes
  are an exact mirror of the Yes book's, so writing it would double Kalshi rows
  with no new information. If a Kalshi market has only one orientation
  planned, that orientation is written with an absent (null) projected side.
- Table entries: `instrument`, `orientation`, `venue`, `market_id`,
  `price_scale`, `quantity_scale`, `tick_atoms`, `ask_source`
  (`native` or `projected`), `ask_source_book` (the counterpart key for a
  projected ask, else `null`).

A written book is **touched** by a cut when the cut has a book transition for
that book or, for a Kalshi Yes book, for its `complement` book.

### 3.2 Common row fields

| Field | Meaning |
|---|---|
| `type` | Row type (per file, below) |
| `scope`, `book` | Scope index; `transition_books` index |
| `cut` | The cut's `sequence`. Rows of one delivered record share it; it joins `transitions` and `levels` rows exactly. |
| `t_ns` | The cut's effective time (`CutClock` time, as the profile uses) |

Integers that can exceed 64 bits (times, atoms) are canonical decimal strings,
as in `profile.ndjson`. `cut`, `scope`, `book` and small counts are JSON
integers. A quote is `[price_atoms, qty_atoms]` or `null`.

### 3.3 Venue time (flat, optional keys)

A row's venue time comes from the cut's `market_events`: for a top or level
row, the book events of the written book (and, for a Kalshi Yes book, of its
`complement` book) in that cut; for a trade row, the trade's own `venue_time`.
Events with a null `event_ns` do not contribute.

| Key | Present when | Value |
|---|---|---|
| `venue_ns` | at least one event carries `event_ns` | the latest `event_ns` |
| `venue_first_ns` | the earliest `event_ns` differs from the latest | the earliest `event_ns` |
| `venue_kind` | `venue_ns` present | the `event_kind`, or `mixed` when more than one |
| `venue_res` | `venue_ns` present | the `event_resolution`, or `mixed` when more than one |
| `sent_ns` | any event carries `sent_ns` | the latest `sent_ns` |

Kinds and resolutions are copied, never converted. Absent keys are omitted, not
`null`. The reader accepts exactly these optional keys and rejects any other.

### 3.4 Scope entry and gating

- **Opening rows.** When a scope opens (scope 0 at the initial cut, later
  scopes where `_advance` crosses a boundary), every written book in that scope
  gets an opening row whose `t_ns` is the scope's opening time and whose state
  is the state in force at that time, before any later cut is applied. For
  scope 0 that state includes every cut whose raw time is before the
  snapshot's `start_ns` (those cuts write no rows), so scope 0's opening rows
  are written at the first cut with raw ≥ `start_ns`, or at terminal if there
  is none, before that cut's own rows. An opening row's `cut` is the sequence
  of the cut that caused it to be written (`0` for the initial cut).
- **Gating.** Apart from opening rows, a row is written only for a cut with raw
  ≥ snapshot `start_ns`, and only for books in the current scope, as the
  `activity` group gates today.
- **Order.** Rows are in cut order. Within a cut: opening rows first (by book
  index), then per book index, top/level rows before trade rows, and trade rows
  in `market_events` order.

## 4. `transitions` (default: top of book and trades)

Stored as `transitions.ndjson.zst`. No level mirror is kept: quotes are read
from the decoder's books after the cut (`best_bid()`, `best_ask()`, and the
Kalshi projection from the counterpart's `best_bid()`). This group must not
charge per-level state.

### 4.1 Top rows (`type: "top"`)

| Field | Meaning |
|---|---|
| common fields | §3.2 |
| `cause` | `open` (§3.4), `operations`, `snapshot` or `invalidation`: the touching transition's kind. If a Kalshi Yes book and its `complement` both transition in one cut, the stronger cause wins: `invalidation` > `snapshot` > `operations`. |
| `validity` | After the cut: `usable`, `not_initialized`, or `unusable:<reason kind>` |
| `bid`, `ask` | Best quotes after the cut (Kalshi ask projected) |
| venue time | §3.3, optional keys |

**When written.** For each written book touched by a gated cut, a top row is
written when `(bid, ask, validity)` differs from that book's previous row in
the same scope, **or** when the cause is `snapshot` or `invalidation` (resyncs
stay visible even if the top is unchanged). Otherwise nothing is written.

Not persisted, because the reader and SDK rebuild them while streaming:
previous quotes, price and tick moves, mid moves, and flows.

### 4.2 Trade rows (`type: "trade"`)

One row per trade event in a gated cut whose `(instrument, orientation)` is a
written book in the current scope, and whose disposition is not `duplicate`.

| Field | Meaning |
|---|---|
| common fields | §3.2 |
| `price`, `qty` | Atoms, in the book's scales |
| `aggressor` | The normalized `aggressor` (`bid` or `ask`), or `null` |
| `disposition` | The observation's disposition (`applied`, `observed`, `not_authority` or `invalidated`) |
| venue time | §3.3 from the trade's own `venue_time` |

A trade whose price or quantity scale differs from the book's, or whose key is
not a written book (for example a Kalshi `complement` trade), is not written
and is counted in the summary under `transition_trades_skipped` by reason
(`scale_mismatch`, `unwritten_book`). Never attribute a trade to a book change.

## 5. `levels` (opt-in depth)

Stored as `levels.ndjson.zst`. This group keeps the per-level mirror (§5.3).

### 5.1 Rows

Sides are in the written book's terms: for a Kalshi Yes book, a change of the
`complement` bid at price `q` is an `ask` change at `P − q` with the same
quantity delta.

**Ladder rows (`type: "ladder"`).** The full ladder, an anchor that diffs are
applied to.

| Field | Meaning |
|---|---|
| common fields | §3.2 |
| `cause` | `open`, `snapshot` or `invalidation` |
| `validity` | After the cut |
| `bids` | `[[price, qty], …]`, best first (descending price) |
| `asks` | `[[price, qty], …]`, best first (ascending price) |
| venue time | §3.3 |

Written at scope entry (§3.4), and for a touched book whose touching transition
is a `snapshot` or `invalidation` (an invalidation writes empty ladders). For a
Kalshi Yes book whose `complement` was snapshotted, the ladder is rewritten with
the new projected asks.

**Diff rows (`type: "diff"`).**

| Field | Meaning |
|---|---|
| common fields | §3.2 |
| `levels` | `[[side, price, qty_delta], …]`: every level whose quantity changed in the cut, sorted by side (`bid` first) then price ascending. `qty_delta` is a signed decimal string, never `"0"`. A level whose operations net to zero is omitted. |
| venue time | §3.3 |

Written for a touched book whose touching transitions are `operations`, when
`levels` is non-empty. If a Kalshi Yes book's own transition and its
`complement`'s are both `operations`, both sides' changes go in one row.

### 5.2 Size

Ladder lines can be long. The `levels` file has its own line bound of 1 MiB
(`LEVELS_MAX_LINE`); its row and byte caps match `transitions`. A line over the
bound fails the run; it is never truncated.

### 5.3 Level mirror

The decoder applies a cut's operations before the strategy sees the books, and
Polymarket operations are absolute (`set`/`delete`), so deltas need the pre-cut
quantities. The collector keeps `price → qty` per native side of every planned
book (Kalshi: both orientations' native bids), as today's recorder does:

- initialized from the initial cut's ladders; replaced on `snapshot`; cleared on
  `invalidation`; on `operations`, applied one by one, accumulating
  `new − old` per level;
- **self-check on every cut:** for each transitioned book, the mirror's best bid
  and ask (price and quantity) equal the decoder book's. A mismatch fails the
  run (`require`); it is never repaired;
- every level is charged to the state budget.

## 6. Storage and summary

- Each stream is written to a plain provisional file
  (`transitions.ndjson`, `levels.ndjson`). At finish it is streamed through
  `encoder.encode_stream` (level 3, one frame) into the `.zst` file, then the
  plain file is deleted. The manifest records both identities.
- Caps per file: 50,000,000 rows, 16 GiB logical; `MAX_LINE` (64 KiB) per
  `transitions` line, 1 MiB per `levels` line. Exceeding a cap fails the run.
- The reader decodes with `encoder.decode_stream` in two streaming passes and
  writes no decoded bytes to disk: pass 1 verifies both identities, pass 2
  validates rows (as implemented in 2f94315).
- **Summary additions** (only for the groups that are on):
  - `transition_books` (§3.1), shared by both files;
  - `transition_scopes`: per scope `start_ns`, `end_ns`, `run_id`,
    `scheduled_start_ns` (that run's `activation_at` from the context
    `evidence` entry with the same `run_id`) and `capture_start_ns`, `null`
    where absent;
  - `transition_rows`: counts by `type` and `cause`;
  - `transition_trades_skipped`: counts by reason;
  - `level_rows`: counts by `type` and `cause`.

## 7. Reader and SDK

### 7.1 Independent reader checks

The reader must not import the collector. For each file present:

1. The file set, both identities, closed row schemas per `type` (with exactly
   the optional venue-time keys of §3.3), types and canonical decimals.
2. Every `scope` and `book` is valid, the book is a written book of that scope,
   `t_ns` lies inside the scope, and `(cut, t_ns)` is non-decreasing over the
   file. Within a cut, the §3.4 order holds.
3. **`transitions`:**
   - per (scope, book), the first row is a `top` row with cause `open`, and
     there is exactly one `open` row;
   - every later `top` row differs from the previous one in
     `(bid, ask, validity)` unless its cause is `snapshot` or `invalidation`;
   - crossed quotes are not rejected: they are observed facts, and the
     profile's incidents cover them;
   - `venue_first_ns` < `venue_ns` when present.
4. **`levels`:**
   - per (scope, book), the first row is a `ladder` with cause `open`;
   - ladders are sorted best first with positive quantities and no duplicate
     prices; an invalidation ladder is empty;
   - replaying diffs onto the latest ladder never produces a negative
     quantity, and no diff entry has a zero delta.
5. **Cross-checks:**
   - with `activity` on: per (scope, book, bucket), the trade-row count plus the
     skipped trades for that book equals the non-duplicate `activity` trade
     count; and the number of non-`open` top rows is at most the book's
     `activity.transitions` plus, for a Kalshi Yes book, its `complement`'s;
   - with both `transitions` and `levels` on: replaying `levels` gives, after
     each cut, a best bid and ask (Kalshi ask projected) equal to the latest
     `top` row of that book; a cut at which the replayed top changes has a
     `top` row for that book, and a cut with a `top` row of cause
     `operations` has a `diff` row for that book;
   - every validity sequence agrees with the profile's state durations
     (usable time), as checked today, for the written books.

### 7.2 SDK iterators

Add to `replay/economic_sdk` (importable by research code, not by the reader):

- `iter_transitions(path)`: streams decoded `transitions` rows and adds, per
  (scope, book) for `top` rows, `prev_bid`, `prev_ask`, `bid_move_atoms`,
  `ask_move_atoms`, `bid_move_ticks` and `ask_move_ticks` (ticks only when
  the move divides by `tick_atoms`; moves `null` when either side is absent).
  It needs the summary's `transition_books` for `tick_atoms`.
- `iter_levels(path)`: streams `levels` rows and yields `(row, ladder)` where
  `ladder` is the book's reconstructed `(bids, asks)` after the row.

Both are documented in SDK §7 with a short example.

## 8. Tests

All offline, with synthetic cuts in the existing `test_market_profile.py`
style. Replace the tests of the previous transitions row format.

1. **Top rows:** a depth-only change writes nothing; a best-quantity change, a
   best-price change and a validity change each write one row; an unchanged top
   with a `snapshot` or `invalidation` cause still writes one.
2. **Kalshi Yes-only:** no row ever references a `complement` book; a
   `complement`-only operation that moves the projected ask writes a Yes `top`
   row; a `complement` change below the top writes nothing; cause priority when
   both transition in one cut.
3. **Trades:** one row per non-duplicate trade; a duplicate is skipped; a scale
   mismatch and an unwritten book are skipped and counted; trades and book rows
   in one cut keep §3.4 order; trades never change a top row.
4. **Venue time:** single event (no `venue_first_ns`), several events with
   different times, mixed kinds, `sent_ns` present and absent, no venue time
   (keys omitted).
5. **Scope boundaries:** opening rows for every scope, scope 0 opening state
   including pre-start cuts, gating of pre-start cuts, a book leaving and
   re-entering scope.
6. **Levels:** Polymarket `set` up, `set` down and `delete`; several operations
   on one price that net to zero (omitted); Kalshi `increase`/`decrease` on both
   orientations mapped into one Yes diff row with projected ask prices;
   snapshot and invalidation ladders; mirror self-check failure on a corrupted
   operation stream; a ladder over the line bound fails.
7. **Reader rejections:** a tampered row, an unknown key, a `null` optional
   venue key, a missing `open` row, a no-op `top` row, a complement book
   reference, a diff with a zero delta, a replay that goes negative, a
   transitions/levels top mismatch, wrong logical or stored identity, a missing
   and an extra file, a v2 group under an embedded profile.
8. **SDK iterators:** rebuilt `prev_*` and moves on a synthetic stream; ladder
   reconstruction equals the decoder book.
9. **Version-1 policy byte identity** (existing goldens untouched) and
   **availability parity** (existing tests).

## 9. Acceptance (retained data)

The fixture `.bench/fixtures/lol-c9-lyon` (Oct 3, LoL best-of-5) with the
venue-time derivatives `derivatives-v6`.

1. **Run A:** coverage plus a profile with every group on (`transitions` and
   `levels`). Both readers pass; `availability.ndjson` is byte-identical to the
   coverage intervals.
2. **Run C:** the profile with every group except `levels`.
3. **Run B** (existing): the profile without the v2 stream groups (332 s).
4. Report rows by type and cause, logical and stored sizes per file, the share
   of rows with `venue_ns` per venue and kind, trade rows against the
   profile's non-duplicate trade count, wall time and peak memory.
   Targets: run C at most 1.3× run B; run A reported only.

## 10. Not in scope

- Attaching the other venue's state to a row. Do it offline as an as-of join
  on `t_ns` or venue time across books matched by outcome masks.
- Trade/cancel attribution, terminal times, game state.
- Exposing transitions to other strategies.
- Retiring `bundle_coverage` (§2.4).
