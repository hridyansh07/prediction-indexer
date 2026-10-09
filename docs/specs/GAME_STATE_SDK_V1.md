# Game-state SDK V1

Status: **proposed.** Depends on the event-keyed game-state archive from PR #62
(`gamestate/`, receipt v2, `timeline.v2.json`). Nothing here is implemented.

Strategies that condition on the match (map ends, series score, time into a map)
need game state inside replay. This spec adds it to the economic strategy SDK as a
second kind of input beside books: a pinned, prepared, per-event file whose facts
the runtime **releases into the strategy at the time a live observer would have
known them**. A strategy reads the current state from memory, like a book view.

## 0. Decisions

- **Runtime overlay, not a stream change.** Game state is merged into the SDK
  runtime's timeline, beside scope boundaries and time-shift timers
  (`Runtime._advance`). The Replay wire, Redis transport, supervisor, derivative
  materializer and Rust engine do not change. Game state is a prepared input, not
  capture evidence.
- **Prepared and pinned, offline at run time.** A new preparation step copies the
  event's archived game state into a local directory with a receipt. Strategy
  configuration pins its SHA-256, exactly as it pins `context.json`. Replay never
  reads the archive or a live API.
- **No lookahead.** Every fact carries a release time. A strategy can observe a
  fact only at or after its release. Calibration windows (§3) make release times
  conservative.
- **Facts, not interpretations.** The SDK exposes what Kalshi reported, plus pure
  derived quantities from declared parameters. It does not estimate win
  probabilities, momentum or "who is ahead".
- **Unavailable is a state.** No game state, incomplete game state, or a
  non-Kalshi event gives `phase: "unavailable"` with a reason, for the whole run.
  Strategies still run and record episodes; they decide what unavailable means.
- **Map-level only.** The archive holds end-of-map totals, not timed in-game
  events. In-map state (live gold lead, kill times) is out of scope until
  game state is captured during matches.

## 1. What the archive gives (one LoL example)

From `timeline.v2.json` and its raw responses, per event:

| Fact | Source | Clock |
|---|---|---|
| Scheduled start | milestone `start_date` | schedule, not actual |
| Match end | milestone `end_date` | Kalshi |
| Map *k* end | map event markets' `close_time` (all markets agree) | Kalshi exchange |
| Map *k* settlement | markets' `settlement_ts` | Kalshi exchange, exact |
| Map *k* start (estimated) | close − `map_duration_seconds` | derived |
| Map *k* winner | the `result: yes` market's `yes_sub_title`, cross-checked with live stats | Kalshi |
| Map *k* stats | per side: kills, gold, towers, drakes, barons, firsts, duration, forfeit | end-of-map totals |
| Series winner, score | series event result, live `home_score`/`away_score` | Kalshi |

Times are Kalshi's clock. The SDK clock is the capture's `visible_ns`
(`CutClock`). The offset between them is milliseconds, inside the windows of §3.

## 2. Preparation: `replay.prepare_game_state`

```bash
.venv/bin/python -m replay.prepare_game_state /research/run-1/context /research/run-1/game-state
```

Inputs: a committed context directory (its `outcomes.document.event_id` names the
event) and the archive (`ARCHIVE_*` environment, via `build_store`). Steps:

1. `gamestate.timeline.latest(store, event_id)` selects the best fetch and
   verifies raw, receipt and timeline bytes. If its state is not `ok`, or no
   fetch exists, the step writes an **unavailable** file (`state` and `reason`)
   and still commits. Unavailable is a result, not an error.
2. Copy `responses.ndjson.zst`, `receipt.json` and `timeline.v2.json` byte for
   byte into the output directory.
3. Derive `competitors` from the raw event responses. Each market's
   `custom_strike.esports_competitor` gives a competitor id and its
   `yes_sub_title` a label, so this maps home and away ids to labels. Ids with
   conflicting labels are unaligned.
4. Align competitors to the context's Universe `participants`, using exact
   equality after Unicode casefold and whitespace collapse. A competitor with no
   match, or more than one, gets `participant: null`. Nothing is guessed from
   fuzzy names.
5. Write `game_state.json` (closed schema, version 1), then `receipt.json` last
   (the commit marker: SHA-256 and length of every file, as for contexts).

`game_state.json`:

```json
{
  "version": 1, "event_id": "event:d1:<64 hex>", "source": "kalshi",
  "state": "ok" | "unavailable", "reason": null | "no_fetch" | "incomplete",
  "archive": {"prefix": "gamestate/source=kalshi/event=…/fetch=…",
              "receipt": {"sha256": "…", "byte_length": 0},
              "responses": {"sha256": "…", "byte_length": 0},
              "timeline": {"sha256": "…", "byte_length": 0}},
  "competitors": [{"side": "home", "id": "…", "label": "Cloud9", "participant": 0},
                  {"side": "away", "id": "…", "label": "LYON", "participant": 1}]
}
```

The loader (`replay.game_state.load(directory, expected_sha256)`) checks the
following, and rejects any mismatch:
- the receipt and closed schemas;
- that it can re-derive the timeline from the copied raw responses, matching the
  copied `timeline.v2.json` byte for byte;
- that it can re-derive `competitors`.

Bounds are those of `gamestate.kalshi`, for single files only.

## 3. Facts and release times

The prepared timeline becomes an ordered list of **facts**. Each fact has a
`release_ns`. Windows come from the strategy policy (§5), so changing them
changes the experiment identity, not the prepared file.

| Fact | `release_ns` | Carries |
|---|---|---|
| `scheduled` | run start | scheduled start (a schedule, flagged) |
| `map_start` *k* | estimated start + `map_start.after_ms` | map index, `estimated: true` |
| `map_end` *k* | close + `kalshi_market_close.after_ms` | winner (label, participant), stats per side, duration, forfeit, score after the map |
| `map_settled` *k* | `settlement_ts` (exact) | none |
| `match_end` | match end + `kalshi_milestone_end.after_ms` | series winner and final score |

Each fact also records `earliest_ns`/`latest_ns`: source time ± window. A
strategy can see the uncertainty, but it cannot act before `release_ns`.

Notes:
- **`map_start` is a stand-in for a live feed.** It is close − duration, which the
  archive only learns after the map ends. Releasing it at the estimated start
  reveals nothing about the map's result. It stands for what a live viewer
  knows, and is flagged `estimated`. `policy.game.map_start` can be `"estimated"`
  (default) or `"at_map_end"`, which releases it with `map_end` for strict
  hindsight-free runs.
- **Stats and winners are never released before `map_end`.**
- **Facts are applied before book cuts at the same instant.** A fact released at
  time *t* is applied before the cut at *t*, so both see the same state.

## 4. The game view

`context.game` is a frozen `GameView` built from the facts released so far:

| Field | Meaning |
|---|---|
| `phase` | `unavailable`, `pre_match`, `in_map`, `between_maps`, `finished` |
| `map` | current map index while `in_map`; the last completed map otherwise; `null` before map 1 |
| `phase_since_ns` | release time of the fact that entered this phase |
| `score` | maps won per side, `{home, away}`, from released `map_end` facts |
| `maps_completed` | count of released `map_end` facts |
| `last_map` | the latest released map's winner, stats and duration, or `null` |
| `history` | released map results, in order |
| `competitors` | as prepared (labels, Universe participant indexes or null) |
| `settled_maps` | indexes whose settlement has been released |
| `revision` | increments on every applied fact |
| `reason` | why it is `unavailable`, else `null` |

**Derived quantities** are pure functions in `replay.economic_sdk.game`. Each takes
`(view, now_ns, priors)` with `priors` from policy:
- `elapsed_in_phase(view, now)`;
- `expected_map_end(view, priors)`, which is `phase_since + map_duration[game]`
  while `in_map`;
- `expected_remaining(view, now, priors)`;
- `expected_next_map_start(view, priors)`;
- `expected_settlement(view, priors)`;
- `participant_score(view, participant)`, which aligns score to a Universe
  participant (null when unaligned).

Priors are policy values with no SDK defaults: `map_duration_ns`,
`between_maps_ns` and `settlement_delay_ns`, per Universe game (`league_of_legends`,
`counter_strike_2`, …). §7 gives a tool to measure them from the archive.

## 5. Strategy interface

Requirements and policy:

```python
Requirements(books=..., game=GameRequirement(timers=(("map_start", 600_000_000_000),)))
```

```json
"game": {
  "input": {"directory": "{game_state}", "sha256": "{game_state_sha256}"},
  "windows": {"map_start": {"after_ms": 5000},
              "kalshi_market_close": {"before_ms": 2000, "after_ms": 5000},
              "kalshi_milestone_end": {"before_ms": 2000, "after_ms": 5000}},
  "map_start": "estimated",
  "priors": {"league_of_legends": {"map_duration_ns": "…", "between_maps_ns": "…",
                                   "settlement_delay_ns": "…"}}
}
```

- `game: null` (or absent) keeps today's behaviour exactly. Strategies without
  game state are byte-identical.
- The pinned game-state SHA-256, windows, `map_start` mode and priors all enter
  `experiment_sha256`. The prepared event id must equal the context's event id.
- A basket declares `("game", None)` in its `inputs` to read game state. Only such
  entities are staged when a fact is released.
- **Staging.** Releases are a third event source in `_advance`, after scope
  boundaries and before time-shift timers at the same instant. Each release
  stages every game-reading entity. Their fingerprints include `view.revision`,
  so a cached observation is reused only for an unchanged view.
- **Context-free.** An observation that reads only discrete fields (`phase`,
  `score`, `map`) may stay context-free, because the revision is in its
  fingerprint. One that calls a time-derived function must set
  `context_free = False`.
- **Timers.** `GameRequirement.timers` lists `(anchor_fact, offset_ns)` pairs.
  When the anchor fact is released, the SDK schedules a re-evaluation at
  `max(release_ns, anchor source time + offset)`. That gives exact threshold
  crossings ("10 minutes into the map") without polling.
- **Episode rows.** For an experiment with game state, episode open and maximum
  rows gain a closed `game` object: `phase`, `map`, `score`, `revision`,
  `maps_completed`. The manifest gains a `game_state` binding (identity, windows,
  mode). Readers require `game` exactly when the manifest has the binding.
  Experiments without game state keep the current rows.
- **Denominators.** Optional per-phase denominators: `game.partition_denominators:
  true` adds `phase` to the denominator key, so each basket's measurable time is
  split into in-map, between-maps and settling time. This is off by default.

## 6. Bench and jobs

- `replay.bench` run specs gain an optional `game_state_directory` beside
  `context_directory`, mounted read-only, with `{game_state}` and
  `{game_state_sha256}` tokens.
- `.bench/fixtures/build_fixture.py` gains a `prepare_game_state` stage.
- The jobs runner is out of scope (it is retiring). Corpus runs prepare game
  state per event with the bench.

## 7. Tooling

`python -m gamestate.priors --archive-env …` streams every `ok` event timeline in
the archive. It writes per-game duration statistics (count, median, p10, p90) for
map duration, gap between maps and settlement delay, so priors come from data
rather than guesses. Output is a local JSON report; the tool never writes to the
archive.

## 8. Tests and acceptance

Offline, contract-shaped:

1. **Preparation.**
   - ok, unavailable (`no_fetch`) and incomplete events;
   - byte copies and receipt;
   - a tampered raw or timeline is rejected;
   - competitor alignment: exact, casefold, unaligned and conflicting cases.
2. **Release.**
   - a stats or winner field is invisible one nanosecond before `map_end`
     release, and visible at release;
   - `at_map_end` mode delays `map_start`;
   - same-instant ordering against book cuts and scope boundaries.
3. **Staging.**
   - only game-reading entities are staged at a release;
   - the fingerprint changes with the revision;
   - context-free reuse holds when the view is unchanged;
   - timers fire at exact instants across a quiet book.
4. **Identity.**
   - changing windows, mode, priors or the pinned SHA-256 changes
     `experiment_sha256`;
   - `game: null` output is byte-identical to today's (pinned complement and
     cross-venue goldens).
5. **Reader.**
   - episode `game` objects are required with a binding and rejected without one;
   - an independent replay of facts reproduces every recorded `game` object.
6. **Acceptance.**
   - the LoL C9–LYON fixture with its archived game state: a probe strategy
     records phase-change episodes;
   - map-end releases fall within the window of the observed Kalshi map-market
     close in the profile stream (the book collapses to 0 or 100).

## 9. Open questions

- **Best-of.** The archived LCS Bo5 (3–1) lists map events 1–4 only, so the map
  count is not the format.
  V1 exposes `maps_listed`, not `best_of`. Should a strategy pass the format in
  policy, or should `Total Maps` markets be read?
- **Non-Kalshi events.** Polymarket- or Limitless-only events are `unavailable`
  until another source exists.
- **Live capture.** Timed in-map state needs polling Kalshi `live_data` during
  matches when capture restarts. Its facts would fit this same release model
  with `release_ns = received_at`.
