# Market profile V2: transitions and availability

Status: **proposed.** Base branch: `codex/venue-event-time`, whose cuts carry
`venue_time` on `market_events`.

This adds two opt-in groups to the market profile
(`replay/strategies/market_profile`, collector `replay/economic_sdk/profile.py`,
reader `replay/economic_sdk/profile_reader.py`):

- **`transitions`:** one exact, observational row per book change, for
  order-flow research.
- **`availability`:** bundle coverage's book, member and bundle intervals,
  computed by the same engine so that `bundle_coverage` can later be retired.

The profile stays observational: no fees, economics, predictions or fitting.

Read first:

- [`docs/ECONOMIC_STRATEGY_SDK_V1.md`](../ECONOMIC_STRATEGY_SDK_V1.md) §7;
- [`replay/strategies/bundle_coverage/SPEC.md`](../../replay/strategies/bundle_coverage/SPEC.md);
- [`docs/specs/VENUE_EVENT_TIME.md`](VENUE_EVENT_TIME.md) §7, which says how
  to read each venue-time kind;
- [`encoder/README.md`](../../encoder/README.md).

## 1. Policy and files

- **Profile policy version 2.** The same closed fields as version 1. `groups`
  may also contain `availability` and `transitions`; both are allowed only
  when the profile runs standalone (`replay.strategies.market_profile:build`).
  An embedded profile (`Requirements.profile`) that requests either fails at
  construction.
- **Version 1 does not change.** A version-1 policy must produce byte-identical
  output to today. Existing tests and the complement golden pin this; keep
  them passing unmodified.
- **The file set is derived from the policy:**
  - version 1: `incidents.ndjson`, `pair_profile.ndjson`, `profile.ndjson`;
  - plus `availability.ndjson` when `availability` is on;
  - plus `transitions.ndjson.zst` when `transitions` is on.

  The manifest's `files` map and the reader's `check_files` use exactly this
  set.
- **Manifest `version: 2`** for a version-2 policy. The
  `transitions.ndjson.zst` identity is
  `{"logical": {sha256, byte_length, records}, "stored": {sha256, byte_length}}`.
  The other files keep `{sha256, byte_length, records}`.
- Add `replay/strategies/market_profile/config.v2.example.json` with every
  group on.

## 2. Availability (the bundle_coverage fold)

1. **Extract the engine.** Move bundle coverage's interval engine out of
   `replay/strategies/bundle_coverage/strategy.py` (status, availability
   states, scope and boundary handling, the denominator with uncaptured
   members, provenance) into a shared module, `replay/economic_sdk/availability.py`.
   `bundle_coverage` then calls the module, and its outputs, tests and
   acceptance stay byte-identical.
2. **Write the same rows.** The profile's `availability` group drives the same
   engine from the same cuts and writes `availability.ndjson`. Its rows are
   **byte-identical** to bundle coverage's `intervals.ndjson` for the same
   snapshot and stream: the same entities (`bundle`, `member:<market_id>`,
   `book:<sha256>`), states, fields and order. The summary adds
   `availability_durations`, identical to coverage's summary `durations`.
3. **Read it with the same rules.** The profile reader validates
   `availability.ndjson` through the shared reader logic in
   `replay/strategies/bundle_coverage/output.py`, extracted alongside the
   engine, and adds one cross-check: per scope and book, the `usable` duration
   in availability book rows equals the sum of `state.usable_ns` over that
   book's profile rows.
4. **Not part of this change:** retiring `bundle_coverage` from the jobs
   registry (`configs/replay_runner.json`, `replay/jobs/contracts.py`) and the
   UI presets. That's a follow-up once the parity acceptance (§5) passes.

## 3. Transitions

### 3.1 Rows

**When a row is written.** One row per (cut, scoped planned book) for every
book transition in the cut: snapshot, operations or invalidation. This uses the
same gating as the `activity` group: the book is in the current scope's books,
and raw ≥ snapshot start. Rows are in cut order; within a cut, sorted by
`(instrument, orientation)`.

**Size.** Per-book constants are not repeated. Rows reference `book`, an index
into the summary's `transition_books` table (§3.4).

| Field | Meaning |
|---|---|
| `scope`, `book` | Scope index; book table index |
| `t_ns` | The cut's visible time (CutClock time, as the profile uses) |
| `kind` | `snapshot`, `operations` or `invalidation` |
| `validity` | After the cut: `usable`, `not_initialized`, or `unusable:<reason kind>` |
| `prev_bid`, `prev_ask`, `bid`, `ask` | Best `[price_atoms, qty_atoms]` or `null`, before and after the cut. A Kalshi ask is projected from the counterpart's bid at `P − p`, as in `_derive`. |
| `bid_added`, `bid_removed`, `ask_added`, `ask_removed` | Total quantity atoms that entered or left that native side, over every level changed in the cut. `null` on snapshot and invalidation rows, and for the ask side of a Kalshi book. |
| `bid_best_added`, `bid_best_removed`, `ask_best_added`, `ask_best_removed` | The same, only at the **pre-cut** best price level. `null` as above, and when that side was empty before the cut. |
| `bid_depleted`, `ask_depleted` | `true` iff the pre-cut best level's quantity reached 0 in this cut. `null` as above. |
| `bid_move_atoms`, `ask_move_atoms` | New best price minus previous best price, or `null` if either is absent |
| `bid_move_ticks`, `ask_move_ticks` | The move divided by the venue's `tick_atoms` when exactly divisible, else `null` |
| `reason` | `snapshot`, `invalidation`, `insert` (every changed level grew), or `unknown` (any level shrank or was deleted). Never `trade` or `cancel`: no venue says why size left. |
| `trades` | Trade events for this book in the same cut that are not `duplicate`: `{count, qty_atoms, aggressor: {bid, ask, none}}`. Co-occurrence only, never attribution. |
| `venue_time` | From this cut's `market_events` book events for this book: `{first_event_ns, last_event_ns, event_kind, event_resolution, last_sent_ns, events}`, or `null` when none carry one. Kinds and resolutions are copied, never converted. |
| `trade_venue_time` | The same over this book's trades in the cut, or `null` |

Integers that can exceed 64 bits are canonical decimal strings, as in
`profile.ndjson`. Small counts are JSON integers.

### 3.2 Level mirror (exact flows)

The decoder applies a cut's operations before the strategy sees the books, and
Polymarket operations are absolute `set`/`delete`. Exact flows therefore need
the pre-cut quantities, so the collector keeps its own mirror:

- `price → qty` per native side of every planned book.
- Initialized from the initial cut's ladders. On `snapshot`, replaced from the
  transition's `bids`/`asks`. On `invalidation`, cleared. On `operations`,
  applied one by one, with the per-op delta (new − old) counted as added or
  removed.
- **Self-check on every cut:** for each transitioned book, the mirror's best
  bid and ask (price and quantity) must equal the decoder book's. A mismatch
  fails the run (`require`); it is never repaired.
- Every level is charged to the state budget.

### 3.3 Storage

- Rows go to `transitions.ndjson.open`. At finish, stream it through
  `encoder.encode_stream` (level 3, one frame) into
  `transitions.ndjson.zst`, then delete the plain file. The manifest records
  both identities.
- Caps: 50,000,000 rows, 16 GiB logical, `MAX_LINE` per line. Exceeding a cap
  fails the run, never truncates.
- The reader decodes with `encoder.decode_stream`, bounded, into a temporary
  file in a scratch directory it creates and removes. It verifies both
  identities before validating any row.

### 3.4 Summary additions

- `transition_books`, ordered by index:
  - `instrument`, `orientation`, `venue`, `market_id`;
  - `price_scale`, `quantity_scale`, `tick_atoms`;
  - `ask_source` (`native` or `projected`);
  - `ask_source_book` (the counterpart key for Kalshi, else `null`).
- `transition_scopes`:
  - per scope: `start_ns`, `end_ns`, `run_id`;
  - `scheduled_start_ns`: that run's `activation_at`, from the context
    `evidence` entry with the same `run_id`, contemporaneous with the run;
  - `capture_start_ns`;
  - `null` where absent.
- Row counts per kind and reason.

### 3.5 Reader checks

The independent reader must not import the collector. It checks:

1. The file set, both identities, closed row keys, types and canonical
   decimals.
2. Every `scope` and `book` is valid. The book belongs to that scope.
   `t_ns` lies inside the scope. Rows are in non-decreasing `t_ns`, with the
   key order inside a cut.
3. **Chain continuity per (scope, book):** a row's `prev_*` equals the previous
   row's `bid`/`ask` for that book. The first row's `prev_*` equals the book's
   state at scope entry. Exception: a Kalshi `prev_ask`/`ask` may change on a
   row where only the counterpart transitioned. The reader recomputes projected
   asks from the counterpart's `bid` chain and checks them.
4. **Arithmetic:**
   - `*_move_atoms` = new − previous;
   - ticks are consistent with `tick_atoms`;
   - flows are non-negative;
   - best flows ≤ side flows;
   - `depleted` ⇒ the best level was removed in full, i.e. `best_removed` ≥
     `prev` quantity;
   - `reason` agrees with the flows: `insert` ⇒ every `*_removed` is 0 and some
     `*_added` is > 0; `unknown` ⇒ some removal;
   - snapshot and invalidation rows have null flows;
   - a Kalshi ask side has null flows;
   - `venue_time` has first ≤ last;
   - `trades` totals are consistent.
5. **Cross-checks** when the groups are on:
   - per (scope, book, bucket), the transition row count equals
     `activity.transitions`, and the trade counts equal `activity.trades`
     minus duplicates;
   - every `validity` sequence agrees with the profile's state durations
     (usable time).

## 4. Tests

All offline, with synthetic cuts in the existing `test_market_profile.py`
style.

1. **Polymarket `set` up, `set` down and `delete`;** several operations on one
   price in one cut; a new best level; best depletion; move atoms and ticks.
2. **Kalshi `increase`/`decrease`;** the projected ask and its source book;
   null ask flows; a counterpart-only change moves the projected ask.
3. **Snapshot** resets the mirror and gives null flows. **Invalidation**
   clears the mirror, and recovery works after it.
4. **Trades and `venue_time` co-occurrence,** with multiple book events in one
   cut (first, last and count).
5. **Scope boundaries:** a book entering and leaving scope, and rows gated by
   scope.
6. **Mirror self-check:** a corrupted operation stream fails.
7. **Reader rejections:**
   - a tampered row;
   - a broken chain;
   - a wrong logical or stored identity;
   - a missing file and an extra file;
   - an unknown key;
   - a v2 group under an embedded profile.
8. **Version-1 policy byte identity** (existing goldens untouched).
9. **Availability parity:** the same synthetic streams through
   `bundle_coverage` and through the profile's `availability` group give
   byte-identical rows and identical durations.

## 5. Acceptance (retained data)

The fixture `.bench/fixtures/lol-c9-lyon` (Oct 3, LoL best-of-5) is
re-materialized with the venue-time normalizers into `derivatives-v6`.

1. One `python -m replay.bench` run with a coverage group and a market-profile
   group under a version-2 policy with every group on. Both readers pass.
2. `availability.ndjson` is byte-identical to the coverage group's
   `intervals.ndjson`.
3. Kalshi books are usable and their rows are present. The share of rows
   with `venue_time`, per venue and kind, is reported.
4. Wall time and peak memory versus the same run without `transitions` are
   reported. Target: at most 2× the profile's time.

## 6. Not in scope

- Attaching the other venue's state to a row. Do it offline as an as-of join
  on `t_ns` or `venue_time` across books matched by outcome masks.
- Trade/cancel attribution, terminal times, game state.
- Exposing transitions to other strategies.
- Retiring `bundle_coverage` (§2.4).
