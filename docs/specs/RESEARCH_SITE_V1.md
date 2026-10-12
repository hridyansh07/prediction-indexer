# Research site V1: research packs and the `/research` UI from corpus results

Status: **proposed.** Base: `feat/corpus-runner` at `fd78f2e` (PR #66), because
the builder reads committed corpus results and verifies them with
`replay.corpus.results.verify`, which is not on `master` yet.

This spec revives Part B (research outputs) and Part D (research site) of the
Event Research V1 spec cut on Oct 11 (`git show cb53652:docs/specs/EVENT_RESEARCH_V1.md`),
adapted to the corpus runner. Part A (game-state archive) and Part C (Universe
layout) are already on `master` and are not part of this spec.

Read first:

- [`AGENTS.md`](../../AGENTS.md), especially the invariants on the shared
  encoder, persisted formats and deterministic serialization;
- [`replay/corpus/README.md`](../../replay/corpus/README.md) for the corpus result
  layout;
- [`docs/ECONOMIC_STRATEGY_SDK_V1.md`](../ECONOMIC_STRATEGY_SDK_V1.md) §5 (episode
  output) and §13 (fill checks);
- [`docs/specs/MARKET_PROFILE_V2.md`](MARKET_PROFILE_V2.md) and
  [`replay/strategies/market_profile/README.md`](../../replay/strategies/market_profile/README.md)
  for `transitions`, `availability` and `levels`;
- [`docs/STRATEGY_PREPARATION_V1.md`](../STRATEGY_PREPARATION_V1.md), "Snapshot",
  for `context.json`;
- [`docs/specs/GAME_STATE_SDK_V1.md`](GAME_STATE_SDK_V1.md) §1 for the prepared
  game-state document.

## 0. Decisions and non-goals

These carry over from cb53652 §0, adapted:

1. **Python computes; the site is static.** Nothing in the site computes
   economics, masks or book state. It renders built files.
2. **No new API server.** The site reads static files from one base URL. The
   corpus tables are queried with DuckDB in the browser.
3. **The backend records; the frontend filters.** The builder applies no
   depth, value or latency threshold. A minimum episode duration (default
   100 ms) and a minimum net are display filters.
4. **Latency is out of scope.** Episodes are shown against the recorded books.
   Whether one was actionable is a separate study.
5. **Outputs come from committed evidence.** The builder reads committed corpus
   results and, optionally, a local game-state archive. It calls no live API,
   never contacts Universe or GCS, and never fetches a missing context.
6. **Episodes are published in full** when a version is eventually published.
7. **One codec.** All Zstandard work goes through the shared `encoder` package.
8. **Read-only inputs.** Corpus results are evidence. The builder opens them
   read-only and writes only to its output directory.

Non-goals for V1:

- publishing (cb53652 §B5): no object-store upload, no public bucket, no
  `current.json` upload. `build` writes a local directory with the same layout a
  later publish would upload;
- running strategies; the builder only reads their committed outputs;
- the heavy recomputation checks of cb53652 §B2 (fees, kill prices, `edge`
  stops, the levels replay); see §8;
- `calibration.json` (cb53652 §B4), which needs game state across the corpus;
  see §8;
- per-lens deep-dive pages, depth or levels views, payoff research views, live
  data;
- removing the replay jobs UI or `bundle_coverage`.

## 1. Inputs

### 1.1 Corpus results

The builder takes one or more **key directories**, each
`results_root/<corpus key[:16]>/`. Each child directory named like a bundle id
(`bundle_` + 24 lowercase hex) is one corpus result:

```text
<key dir>/<bundle_id>/
  corpus_receipt.json            commit marker, written last
  result.json                    bench result (LOCAL_REPLAY_BENCH_V1 §7)
  context/context.json           prepared context (PR #66; absent in older results)
  context/receipt.json
  run/run.json, run/SUCCESS.json supervisor run (absent SUCCESS on failure)
  run/<attempt id>/<group>/output/...
  run/<attempt id>/<group>/failure.json   on a failed participant only
  groups/<group>/...             reader receipt, summary, check results
```

Rules:

- A result without `corpus_receipt.json` is **uncommitted**. It is listed in the
  events table with that state and gets no pack.
- The receipt is verified with `replay.corpus.results.verify`. A failure lists
  the event as `receipt_invalid` with the message and gets no pack.
- **The same bundle in several key directories:** the key directory given
  **last** on the command line wins. The others are recorded under the corpus
  manifest's `superseded` list with their corpus key, so nothing is dropped
  silently. Within one key directory a bundle appears at most once.
- **Attempt choice.** The bench forces `limits.attempts = 1`, so there is
  normally one attempt directory (32 lowercase hex). The builder uses the
  attempt named by `run/SUCCESS.json` when present; otherwise the single
  attempt directory; otherwise (several attempts and no marker) every lens is
  `failed` with `ambiguous attempts: <ids>`. The chosen attempt is recorded in
  the pack manifest.

### 1.2 Lenses

`configs/research.json` (closed, version 1, unknown fields fail) maps corpus
group names to lenses:

```json
{
  "research_version": 1,
  "lenses": {
    "profile":     {"group": "profile",     "adapter": "profile",      "label": "Market profile"},
    "complement":  {"group": "complement",  "adapter": "sdk_episodes", "label": "Same-venue complement",
                    "headline_kind": "net", "value_scale": 36, "value_unit": "USD"},
    "cross":       {"group": "cross",       "adapter": "sdk_episodes", "label": "Cross-venue complete set",
                    "headline_kind": "net", "value_scale": 36, "value_unit": "USD"},
    "multi":       {"group": "multi",       "adapter": "sdk_episodes", "label": "Same-venue multi-market",
                    "headline_kind": "net", "value_scale": 36, "value_unit": "USD"},
    "cover_same":  {"group": "cover_same",  "adapter": "sdk_episodes", "label": "Same-venue implication cover",
                    "headline_kind": "net", "value_scale": 36, "value_unit": "USD"},
    "cover_cross": {"group": "cover_cross", "adapter": "sdk_episodes", "label": "Cross-venue implication cover",
                    "headline_kind": "net", "value_scale": 36, "value_unit": "USD"}
  },
  "game_time_windows": {
    "kalshi_market_close":  {"before_ms": 2000, "after_ms": 5000},
    "close_minus_duration": {"before_ms": 2000, "after_ms": 65000},
    "kalshi_milestone_end": {"before_ms": 2000, "after_ms": 5000}
  },
  "overview_max_buckets": 2048,
  "tile_minutes": 10
}
```

- `adapter` is `profile` (exactly one lens, which feeds the chart) or
  `sdk_episodes` (§2.5).
- `headline_kind` is the episode `kind` counted by the viability report
  (`net`, the fill kind, for the fill lenses). Every kind is still recorded.
- `value_scale` and `value_unit` read a fill result's `value` (the strategy's
  `fill_value`, scale 36 in the valuation's quote asset).
- Groups the config does not map (`coverage`, which is being dropped from the
  corpus) are listed per event under `unmapped_groups`. They are not lenses.
  The LP screen is not a lens.

### 1.3 Lens states

Each lens of each event has exactly one state. None is dropped silently.

| State | When | Carries |
|---|---|---|
| `ok` | `result.json` status `SUCCESS`, the group has a non-null reader receipt and every check passed | `semantic_sha256` from the group's `content_receipt.json` |
| `failed` | anything else for a group the bench ran | `error`: the group's `failure.json` cause chain (`Type: message`, joined with ` <- `) when present, else `result.json` `error.type: error.message`, else `checks failed: <names>` |
| `excluded` | the group is in `corpus_receipt.json` `excluded_groups` | `reason` from the receipt |
| `not_run` | the group is absent from both `result.json` groups and `excluded_groups` | — |

An event gets a pack only when its `profile` lens is `ok`, because the chart is
built from the profile's `transitions`. Otherwise it is listed in the events
table with `pack: "no_profile"` and the profile lens's state.

### 1.4 Event metadata

Metadata comes from the result's committed `context/context.json`:

- `context/receipt.json`'s `snapshot_sha256` must equal the SHA-256 of
  `context/context.json`'s exact bytes and `result.json`'s
  `context.snapshot_sha256`. A mismatch lists the event as `context_invalid`
  with no pack.
- `title` is the outcomes document's participants joined with ` vs `, else the
  bundle id. `game` is the bundle evidence's `game` (all occurrences agree,
  else `mixed`). `event_id` (Universe) is the outcomes document's `event_id`.
- Per written book: `venue`, `market_id`, `market_type`, `label`
  (`<market type> · <outcome label>`), `claim_id` and `price_scale`, from the
  outcomes document's market row and the scopes' `outcome_books` entry for the
  book's `(instrument, orientation)` with `negated: false`.

**Degrade path (no `context/`).** Results committed before PR #66 have no
context. The pack is still built: `title` is the bundle id, `game` is `null`,
book labels are their instrument ids, `claim_id` is `null` (no twins), and
`price_scale` comes from the profile's `transition_books`. Both the pack
manifest and the events table record `context: "missing"`. The site shows this
state explicitly.

The site's event key is the **bundle id** (`/research/events/<bundle_id>`): it
exists for every result, with or without context.

### 1.5 Game state

`game.json` is built only when `--game-state DIR` is given, where `DIR` is a
local root of the Part A archive layout (`gamestate/source=kalshi/...`), opened
read-only through the `archive` local store. For a result with a valid context,
the builder selects and verifies the event's best fetch with
`gamestate.timeline.latest` and reshapes it with the same projection as
`replay.prepare_game_state`, in memory, validated with
`replay.game_state.validate`. Without `--game-state`, or without a context,
`game.json` has state `no_source`. The corpus runs so far had
`game_state.prepare: false`.

## 2. Build: research packs

```bash
.venv/bin/python -m replay.research verify KEY_DIR [KEY_DIR ...] [--config configs/research.json]
.venv/bin/python -m replay.research build  KEY_DIR [KEY_DIR ...] --out OUT_DIR \
    [--config configs/research.json] [--game-state DIR]
```

`verify` prints the §4 checks as JSON and exits 1 when any check fails. `build`
runs the same checks itself (it never trusts an earlier `verify` file), writes
`verify.json` into the version, and builds everything below.

Every JSON file is UTF-8 with sorted keys, compact separators and a final LF.
Times and other integers that can exceed 2^53 are canonical decimal strings.
Prices, quantities in atoms, counts and indexes are JSON integers (a quantity
that exceeds 2^53 − 1 fails the build rather than lose precision).

### 2.1 Layout

```text
OUT_DIR/
  current.json                         pointer, written last
  <version>/
    manifest.json                      corpus manifest
    verify.json
    events.parquet, episodes.parquet, summary.json, viability.md
    events/<bundle_id>/
      manifest.json                    pack manifest
      overview.json, availability.json, game.json
      overlays/<lens>.json
      tiles/1s/<minute>.json, tiles/raw/<minute>.json
```

- `<version>` is the first 16 hex of the corpus manifest's SHA-256. The build is
  deterministic: the same inputs and config give the same bytes and version.
- The version is written to `OUT_DIR/.<version>.tmp`, every file fsynced, then
  renamed to `OUT_DIR/<version>` and the directory fsynced. An existing version
  directory is never rewritten: if it exists, the builder verifies every file
  against the new manifest and reuses it (identical content), or fails.
- `current.json` = `{"version", "manifest_key": "<version>/manifest.json",
  "manifest_sha256", "manifest_byte_length"}`, replaced atomically last.

### 2.2 Corpus manifest

```json
{
  "corpus_manifest_version": 1,
  "config_sha256": "...",
  "inputs": [{"corpus_key": "<64 hex>", "key_prefix": "<16 hex>", "bundles": 4}],
  "superseded": [{"bundle_id": "...", "corpus_key": "...", "by": "..."}],
  "events": {"<bundle_id>": {"pack": "events/<bundle_id>/manifest.json",
                             "sha256": "...", "byte_length": 0}},
  "files": {"events.parquet": {"sha256": "...", "byte_length": 0}, "...": {}}
}
```

No absolute local path is written anywhere in a version.

### 2.3 Pack manifest

```json
{
  "pack_version": 1,
  "event_id": "event:d1:... or null", "bundle_id": "...", "game": "... or null",
  "title": "...", "participants": ["..."], "context": "ok|missing",
  "capture_start_ns": "...", "capture_end_ns": "...",
  "scopes": [{"start_ns": "...", "end_ns": "...", "run_id": "...", "scheduled_start_ns": "... or null"}],
  "books": [{"book": 0, "instrument": "...", "orientation": "outcome", "venue": "kalshi",
             "market_id": "...", "market_type": "... or null", "label": "...",
             "claim_id": "... or null", "price_scale": 4, "quantity_scale": 2}],
  "lenses": {"cross": {"state": "ok", "label": "...", "semantic_sha256": "...",
                       "overlay": "overlays/cross.json", "episodes": 12},
             "complement": {"state": "failed", "label": "...", "error": "..."},
             "multi": {"state": "excluded", "label": "...", "reason": "..."},
             "cover_same": {"state": "not_run", "label": "..."}},
  "unmapped_groups": ["coverage"],
  "game_state": {"state": "ok|incomplete|no_source", "file": "game.json"},
  "provenance": {"corpus_key": "...", "corpus_receipt_sha256": "...", "attempt": "...",
                 "snapshot_sha256": "...", "source_commit": "...", "source_dirty": false,
                 "bench_status": "SUCCESS"},
  "tiles": {"1s": ["0", "10"], "raw": ["0", "10"]},
  "overview": {"bucket_ns": "...", "buckets": 0},
  "files": {"overview.json": {"sha256": "...", "byte_length": 0}}
}
```

- `books` is the profile summary's `transition_books` in order: Kalshi as the
  Yes book with a projected ask, Polymarket per token.
- Two books that share a non-null `claim_id` are **cross-venue twins**; the site
  groups them in the legend.
- `capture_start_ns` is the first scope's start and `capture_end_ns` the last
  scope's end. The chart's zero is capture start, not scheduled start.
- `files` lists every other file of the pack with the SHA-256 and byte length
  of its exact bytes.

### 2.4 Price series

All series come from the profile's `transitions.ndjson.zst` (decoded with
`encoder.decode_stream`, streaming), its `top` and `trade` rows.

**Book state.** Per book, a `top` row's state (`bid`, `ask`, `validity`) holds
from its `t_ns` until the book's next `top` row in the same scope or the scope's
end. Outside its scopes a book's state is unknown. A state is **usable** when
`validity == "usable"`. A state in force for zero time (two rows at one `t_ns`)
contributes nothing.

**Bucket values.** For a bucket `[b0, b1)` and a book, consider every usable
state in force for a positive time inside the bucket:

- `bid` and `ask` are each `[min, max, last]` in price atoms over those states'
  non-null quotes, where `last` is the quote of the latest such state; `null`
  when no such state has that side;
- `usable_ppm` is the usable time inside the bucket in parts per million of the
  bucket's length (floor);
- `trades` and `trade_qty` count the `trade` rows with `b0 <= t_ns < b1` and
  sum their `qty` atoms.

A bucket with no usable time has null quotes, so the chart breaks there.

- **`overview.json`:** `[capture_start, capture_end)` in at most
  `overview_max_buckets` equal buckets. `bucket_ns` is the smallest multiple of
  one second with `ceil(span / bucket_ns) <= overview_max_buckets`.
  Columnar per book:
  `{"overview_version": 1, "start_ns", "bucket_ns", "buckets",
    "books": [{"book", "bid": {"min": [...], "max": [...], "last": [...]},
               "ask": {...}, "usable_ppm": [...], "trades": [...], "trade_qty": [...]}]}`.
- **`tiles/1s/<minute>.json`:** `tile_minutes`-long tiles of 1-second buckets in
  the same shape, `<minute>` being the tile's start offset from capture start in
  whole minutes. The last tile is cut at capture end.
- **`tiles/raw/<minute>.json`:** the exact `top` and `trade` rows with `t_ns`
  inside the tile (row fields as written, minus `cut`), plus, per book in scope
  at the tile's start, the `initial` state in force then. A raw tile alone is
  enough to draw its step lines.
- Tiles are written only for ranges where at least one book is in scope.

### 2.5 `availability.json`

Per written book, the profile's `availability.ndjson` book rows for that
book's own `(instrument, orientation)` (entity `book:<sha256>`), in order:
`{start_ns, end_ns, scope, state, reason}` where `reason` is the row's
`reason.kind` or `null`. Member and bundle rows are not copied.

### 2.6 `game.json`

`{"game_version": 1, "state": "ok|incomplete|no_source", "reason", "source",
"competitors", "events": [...]}`. Each event is
`{kind, segment, label, time_basis, source_ns, earliest_ns, latest_ns, exact,
known_at_ns, detail}`:

| Prepared fact | `kind` | `time_basis` | Window |
|---|---|---|---|
| `scheduled_start_ns` | `scheduled_start` | `scheduled` | none, `exact: false` |
| segment `start_ns` | `segment_start` | `close_minus_duration` | config window |
| segment `end_ns` | `segment_end` | `kalshi_market_close` | config window |
| segment `settled_ns` | `settlement` | `market_settlement` | none, `exact: true` |
| `match.end_ns` | `match_end` | `kalshi_milestone_end` | config window |

- `earliest_ns = source_ns − before_ms`, `latest_ns = source_ns + after_ms`;
  without a window both equal `source_ns`.
- `known_at_ns = latest_ns`. The site's as-of cursor treats a fact as known
  only from then.
- `detail` copies the segment's winner, score and `details`, or the match's
  winner and score. It never carries a game clock or a field the source lacks.

### 2.7 Overlays

`overlays/<lens>.json` has one of three generic shapes, so a future Python
model can produce one without a site change:

```json
{"overlay_version": 1, "lens": "cross", "kind": "intervals", "label": "...",
 "semantic_sha256": "...", "items": [...]}
```

- **`intervals` item:** `id`, `start_ns`, `end_ns`, `books` (written-book
  indexes), `label`, `censored`, `end_reason`, `metrics` (a list of
  `{name, value, unit}` with decimal-string values) and `detail` (a free
  object).
- **`series` item:** `{t_ns, value}` with a top-level `unit`.
- **`markers` item:** `{t_ns, label, books, detail}`.

The SDK episode adapter (`replay/research/overlays.py`) reads the group's
`manifest.json`, `entities.json`, `episodes.ndjson` and `content_receipt.json`.
It imports no strategy runtime. One interval per episode row, in file order,
nothing filtered:

- `id` is `episode_id`; `label` is the episode `kind`, plus the descriptor's
  `basket_kind` when present;
- `books`: each descriptor leg's `instrument` mapped to its written book (a
  Kalshi leg of either orientation maps to the Kalshi Yes book; a Polymarket
  leg to its token's book); sorted, distinct. A leg without a written book is
  listed in `detail.unmapped_legs`;
- `metrics`: always `duration` (`end_ns − start_ns`, unit `ms`, exact
  decimal). With a `fill`: `net` from the first governing result's `value`
  divided by `10^value_scale` (unit `value_unit`, exact decimal) and
  `quantity` = `steps × step_contracts` from the manifest policy (unit
  `contracts`);
- `detail`: `kind`, `route_id`, `legs` (`market_id`, `instrument`,
  `orientation`), `viable_tiers`, and with a fill `stop`, `kill_prices`,
  `end_books`. Modeled values are labelled as modeled by the site.

## 3. Build: corpus tables and viability report

| File | One row per | Columns |
|---|---|---|
| `events.parquet` | Corpus result | `bundle_id`, `event_id`, `corpus_key`, `state` (`ok`, `uncommitted`, `receipt_invalid`, `context_invalid`), `pack` (`ok`, `no_profile`, or null), `context`, `game`, `title`, `capture_start_ns` (int64), `duration_s`, `venues`, `book_count`, `scope_count`, `game_state`, `bench_status`; per lens `<lens>_state`, `<lens>_error`, `<lens>_episodes`, `<lens>_headline`, `<lens>_max_net` |
| `episodes.parquet` | Episode, every lens | `bundle_id`, `lens`, `episode_id`, `kind`, `start_ns`, `end_ns` (int64), `duration_ms` (float64), `books`, `markets`, `venues`, `venue_pair`, `censored`, `end_reason`, `net` (float64, null without a fill), `net_exact` (string), `net_unit`, `quantity` (float64), `seconds_to_capture_end`, `route_id`, `shares_leg_with`, `game`, `market_types` |
| `summary.json` | Lens | Fixed-edge histograms: episodes per event, duration (log edges, 1 ms to 1 h), net and quantity; each with and without the 100 ms filter; counts of events per lens state |

- `shares_leg_with` counts the other episodes of the same event and lens that
  overlap in time and buy the same `(instrument, orientation)` leg.
- **Per route and deduplicated.** Per route counts every episode. Deduplicated
  counts the connected groups of episodes linked by "overlap and share a leg",
  so overlapping routes that walk one book side count once. Both are reported.
- Parquet uses `pyarrow`, already a base dependency in `pyproject.toml`. Parquet
  files are written with fixed writer options so they are byte-deterministic.

`viability.md` is a generated report answering the backend design's table:

- per lens, the headline kind's episodes per event (mean, median, share of
  events with at least one), per route and deduplicated;
- the duration distribution, with the share of episodes at 100 ms or more;
- fill quantity and net distribution;
- the same breakdowns by game, market type and venue pair.

It states its corpus version, its input corpus keys and the count of events per
state and per lens state (failed, excluded, not run), with each failure reason.

## 4. Verify (reduced for V1)

Per corpus result, recorded in `verify.json` as
`{bundle_id, check, pass, message}`:

1. `corpus_receipt`: `replay.corpus.results.verify` passes.
2. `bench_status`: `result.json` status (`SUCCESS` passes; anything else fails
   and is carried into the lens states, §1.3).
3. `group_readers`: every group in `result.json` has a non-null reader receipt
   (the bench ran each group's independent reader in the container).
4. `group_checks`: every bench check recorded for each group passed.
5. `context_binding`: §1.4's three-way SHA-256 binding, or `missing`.
6. `attempt`: §1.1's attempt choice is unambiguous.

A failed check never patches output: it is reflected in the event or lens state.

## 5. Research site

Two routes in `targeter-ui` (React 19, Vite, react-query):

| Route | Page |
|---|---|
| `/research` | Corpus view (§5.2) |
| `/research/events/:bundleId` | Event terminal (§5.3) |

Existing routes are unchanged. The nav gains "Research".

### 5.1 Data

- `VITE_CORPUS_BASE_URL` is the base that holds `current.json`.
- Load `current.json`, then the corpus manifest, then pack manifests. Every
  file fetch checks byte length and SHA-256 (`crypto.subtle`) against its
  manifest. A mismatch shows an error state; data is never shown unverified.
- The corpus tables load in **DuckDB-WASM** over the Parquet files, after the
  same identity check. Queries are fixed templates with bound parameters.
- **Local development:** `RESEARCH_CORPUS_DIR=<OUT_DIR>` makes the Vite dev
  server serve that directory read-only under `/corpus/`, and
  `VITE_CORPUS_BASE_URL` defaults to `/corpus` in development. `/research`
  works offline against a local build.
- No auth.

### 5.2 Corpus view

- **Headline**, from `summary.json` and DuckDB: events, mean and median
  headline episodes per event, share of events with at least one, total net;
  per lens, with a per-route / deduplicated toggle.
- **Filters:** game, venue pair, lens, minimum duration (default 100 ms) and
  minimum net. Filters change the display only and show "n hidden by filter".
- **Distributions:** episodes per event, duration (log scale, with the 100 ms
  line), net and quantity.
- **Events table:** title, game, date, venues, books, context and game-state
  badges, per-lens episode count and max net, and a lens state badge (`failed`
  and `excluded` show their reason). Sortable. A row with a pack opens the
  terminal; a row without one shows why.

### 5.3 Event terminal

As cb53652 §D3, which follows the prototype's interaction design:

- **Chart:** one dominant uPlot chart; x is time since capture start
  (`+hh:mm:ss`) with absolute UTC in hover; y is ¢ per $1; each book draws its
  bid–ask band with the mid as a line, with a mid-only toggle; lines break where
  the book is unusable or out of scope, never interpolated.
- **Zoom:** wheel and drag, double-click to reset. Above 2 h visible the
  overview is used; from 2 h to 10 min the 1 s tiles; 10 min or less the raw
  tiles as step lines. Tiles are fetched lazily, verified and cached.
- **Legend:** a checkbox per book with a stable colour; hiding a book never
  rescales the chart; twins sit together under one claim label; venue on every
  entry; `context: missing` says labels are instrument ids.
- **Tracks** on the same time axis: **game** (bracket from `earliest_ns` to
  `latest_ns`, a diamond only when `exact`, a dashed line for a scheduled
  start); **lens** (the selected lens's intervals; at wide zoom, sub-pixel
  episodes collapse into a density strip with counts); **books** (unusable
  intervals per book, hatched, reason on hover).
- **Lens selector:** "No lens" or any lens with `state: ok`; failed and excluded
  lenses appear disabled with their reason; `not_run` is listed as such.
- **Episode selection:** zooms to the interval with padding and highlights its
  books; the details panel shows lens, kind, duration, end reason, censored,
  every metric with its unit, the detail fields and provenance (corpus key,
  semantic SHA-256, snapshot). Net is labelled "modeled", never a return.
- **Cursor:** click or scrub; the readout shows bid, ask and mid per visible
  book, and game state as of the cursor (facts with `known_at_ns` at or before
  it).
- **Playback** (secondary): play, pause and speed; "Next moment" jumps to the
  next game fact, episode start or availability change; optional auto-pause;
  "Full history" off hides everything after the cursor.
- **Explicit states:** game state `no_source` or `incomplete`, context missing,
  a failed or excluded lens, a book with no usable time. Each has its own
  visual; none renders as empty or zero.

### 5.4 Performance and tests

Budgets:

- the first paint of an event fetches at most 3 MB decoded (manifest, overview,
  availability, game and the selected overlay); the acceptance reports both the
  decoded and gzip sizes for the largest event;
- pan and zoom stay smooth with 30 books at 2,048 points.

`node --test` view-model tests (offline, a hand-authored pack): bucket-to-path
breaks at gaps; the zoom-tier choice; the twin grouping; the game bracket and
as-of logic; the overlay density collapse; filter counts; the manifest identity
check.

## 6. Tests and acceptance

Python unit tests (offline, hand-authored inputs, `replay/tests/test_research*.py`):

- input discovery: uncommitted, receipt-invalid, superseded across key
  directories, attempt choice (marker, single, ambiguous);
- lens states `ok`, `failed` (from `failure.json`, from `result.json`, from
  checks), `excluded`, `not_run`, and an unmapped group;
- context binding and the no-context degrade path;
- overview and tile bucketing from top rows, including null buckets across a
  gap, an unusable state and a scope exit, and zero-duration states;
- raw tiles' initial state;
- game windows and `known_at_ns`;
- the overlay adapter from a hand-authored fill episode and a no-fill episode;
- `shares_leg_with` and deduplicated counts;
- table schemas, deterministic bytes and version, and refusal to rewrite a
  differing existing version.

Acceptance (local, real data, read in place):

1. Build `.bench/research/out` from the trial results on PI-DATA
   (`corpus-results-trial3/b570737f542aefbd` and
   `corpus-results-trial3-state/a32a0fccbc7760e7`, no context) and later from a
   smoke run with `context/`.
2. An independent script that does not import `replay.research` recomputes every
   overview bucket's bid and ask `last` from `transitions.ndjson.zst` and
   matches every value.
3. Every overlay interval matches an `episodes.ndjson` row.
4. The site renders `/research` and every event terminal against the local
   build; the largest event's first-paint fetch size is reported against the
   3 MB budget.

## 7. Documentation

`replay/research/README.md` documents the commands and layout; the `AGENTS.md`
routing table gains a row for research packs and the site.

## 8. Follow-ups (not in V1)

- cb53652 §B2's heavy checks: fee legs recomputed from the catalog,
  `net = gross − fees`, `edge` stops proven by `beyond`, kill prices, and the
  `levels` replay against every top row.
- Publishing (cb53652 §B5) to a public bucket with the pointer last.
- `calibration.json`, once the corpus has game state.
- Model overlays (`series`, `markers`) from research models.

## Open questions

- Should a book that leaves and re-enters scope start a new path in tiles? This
  spec breaks the line (the state is unknown between scopes).
- Should a corpus result carry its prepared `game_state.json` when the runner
  prepared one, so the builder can read it instead of the archive?
