# Game-state SDK V1

Status: **implemented for offline replay.** Uses the event-keyed game-state
archive (`gamestate/`, receipt v2, `timeline.v2.json`). Synthetic preparation,
runtime, bench and independent-reader tests cover the contract. The retained
C9–LYON acceptance in §8 remains unverified; live capture and corpus execution
are not implemented by this change.

Strategies that condition on the match (a map ending, the series score, time into
a map) need game state inside replay. This spec adds it to the economic strategy
SDK as a second input beside books: one small prepared file per event, loaded
whole into memory, whose facts the runtime **releases into the strategy at the
book time a live observer would have known them**, then drops.

## 0. Decisions

- **The runner prepares, the strategy consumes.** The corpus runner (the bench
  until the corpus runner exists) prepares the file once per event, before any
  strategy line for that event starts. Every line reads the same file read-only.
  A strategy never contacts the archive or an API. The walker and the materializer
  stay out of it: they are network-free and per book.
- **One file, built in memory.** The prepared file is a few kilobytes of JSON. The
  runtime reads it once at run start, checks its schema and pinned SHA-256, and
  builds the release queue in memory.
- **Required is a strategy decision.** Strategy policy says whether game state is
  required. Required and unavailable: the run stops before reading any book and
  reports `game_state_unavailable` with the reason; the runner moves on to the next
  line. Not required: the run proceeds with `phase: "unavailable"`.
- **Released by book time, then dropped.** Facts are a queue ordered by release
  time. At each book cut the runtime applies every fact released at or before the
  cut and discards it. The view keeps only the current state and the latest
  segment result, not the history; a strategy that wants history keeps it itself.
- **Generic across games.** The SDK knows no game-specific fields. The file has a
  header (sport, game, the event's market types, competitors) and a list of
  **segments** (maps in esports; whatever the source divides a match into
  elsewhere), each with times, a winner side and an opaque `details` object of
  source statistics. A soccer event has no `map_winner` or `total_maps` market and
  may have no segments at all; the strategy sees that from the header.
- **No lookahead.** A fact is observable only at or after its `release_ns`.
  Release windows (§3) make release times conservative.
- **Facts, not interpretations.** The SDK exposes what the source reported, plus
  pure derived times from declared priors. It does not estimate win probabilities
  or who is ahead, and it does not interpret `details`.
- **Runtime overlay, not a stream change.** Releases join the SDK runtime's
  timeline (`Runtime._advance`). The Replay wire, Redis transport, supervisor,
  materializer and Rust engine do not change.
- **Segment-level only.** The archive holds end-of-segment totals, not timed
  in-game events. In-segment state waits for live capture (§8).

## 1. The prepared file: `game_state.json`

Closed schema, version 1:

```json
{
  "version": 1,
  "event_id": "event:d1:<64 hex>",
  "state": "ok",
  "reason": null,
  "source": {"name": "kalshi", "prefix": "gamestate/source=kalshi/event=…/fetch=…",
             "timeline_sha256": "<64 hex>"},
  "sport": "esports",
  "game": "league_of_legends",
  "market_types": ["map_winner", "series_moneyline"],
  "competitors": {"home": {"label": "Cloud9", "participant": 0},
                  "away": {"label": "LYON", "participant": 1}},
  "scheduled_start_ns": 1791057600000000000,
  "segment_kind": "map",
  "segments": [
    {"index": 1, "start_ns": 1791058760000000000, "start_estimated": true,
     "end_ns": 1791060593000000000, "settled_ns": 1791060777117186000,
     "winner": "home", "details": {"duration_s": 1833, "forfeit": 0,
                                   "home": {"kills": 0, "...": 0},
                                   "away": {"kills": 0, "...": 0}}}
  ],
  "match": {"end_ns": 1791073420000000000, "winner": "away",
            "score": {"home": 1, "away": 3}}
}
```

- `state` is `ok` or `unavailable`. An unavailable file has a `reason`
  (`no_fetch`, `incomplete`, `no_source` for an event with no Kalshi market) and
  only the header fields; `segments` is empty and `match` is null.
- `sport`, `game` and `market_types` come from the pinned context, in Universe
  vocabulary, so they do not depend on the game-state source. `market_types` is
  the sorted, distinct `market_type` of the event's markets.
- `competitors` maps the source's home/away sides to labels and Universe
  participant indexes. A participant index is set only on an exact match after
  Unicode casefold and whitespace collapse, otherwise `null`. No fuzzy names.
- `details` is the source's per-segment statistics, copied as reported. Its keys
  differ by game and source; the SDK passes it through without a schema.
- `segment_kind` names what a segment is (`map` for Kalshi esports). Segments are
  ordered by `index`.
- `source` traces the file back to the archived fetch it was derived from.
  Its name and prefix are nonempty strings when present. Times and scores are
  unsigned JSON integers bounded by u64; a missing start or settlement is null.
  Segment ends are required. A match-level-only event may have no segments.
  The complete serialized file is bounded at 1 MiB; opaque details still obey
  strict JSON decoding (no duplicate keys or nonfinite numbers).

## 2. Preparation (runner side)

```bash
.venv/bin/python -m replay.prepare_game_state <context_dir> <out_dir>
```

1. Read the event id, sport, game and markets from the committed context.
2. `gamestate.timeline.latest(store, event_id)` selects the best archived fetch
   and verifies it. No fetch, or a state other than `ok`, writes an unavailable
   file and still succeeds: unavailable is a result, not an error.
3. Reshape the fetch's `timeline.v2.json` into §1, align competitors, write
   `game_state.json` atomically, and print its SHA-256.

The event identity, participants and complete event market list come from the
committed context's Universe outcomes document; sport/game come from its bundle
evidence and must agree across occurrences. Preparation requires that pinned
event identity, including for unavailable results. The CLI uses exported
`ARCHIVE_*` configuration through the shared store factory and reads no dotenv.
It refuses an existing output directory. Publication finishes the file, fsyncs
it, renames it, fsyncs the directory, then hashes the exact persisted bytes and
checks them with the independent loader.

The current timeline has competitor IDs rather than names. The Kalshi adapter
therefore streams the same verified raw fetch to recover labels from market
strikes and `yes_sub_title`; raw vendor fields do not enter the SDK. Ambiguous
labels or alignment leave participant indexes null. Contradictory timelines,
unresolved winner sides or invalid final scores produce `incomplete`. Rejected
receipts with no usable fetch also produce `incomplete`, rather than ordinary
absence. An archive with no receipt for a Kalshi event produces `no_fetch`.

The corpus runner runs this once per event before fanning out strategy lines and
passes the directory and SHA-256 to each line. A runner may skip launching a line
whose strategy requires game state when the prepared file is unavailable, and
record the same `game_state_unavailable` result without paying for the replay.

The loader (`replay.game_state.load(path, expected_sha256, context)`) rejects a
SHA-256 mismatch, an unknown field or version, an event id different from the
context's, and unordered or overlapping segments. It does not re-read the archive.

## 3. Facts and release times

The loader turns the file into an ordered queue of facts:

| Fact | `release_ns` | Carries |
|---|---|---|
| `scheduled` | run start | scheduled start (a schedule, not an actual start) |
| `segment_start` *k* | start + `segment_start.after_ms` | index, `estimated` |
| `segment_end` *k* | end + `segment_end.after_ms` | winner side, `details`, score after the segment |
| `segment_settled` *k* | `settled_ns` (exact) | index |
| `match_end` | match end + `match_end.after_ms` | winner side, final score |

- Windows come from strategy policy (§5), so changing them changes the experiment
  identity, not the prepared file.
- **Kalshi segment starts are estimates** (end − duration), which the archive
  only learns after the segment ends. Releasing one at the estimated start reveals
  nothing about the result and stands in for what a live viewer knows.
  `policy.game.segment_start: "at_segment_end"` releases it with `segment_end`
  instead, for strictly hindsight-free runs.
- Winners and `details` are never released before `segment_end`.
- Facts are applied before the book cut at the same instant, so both see one state.
- An applied fact is removed from the queue. Memory falls as the run advances.
- Equal-time segment facts apply in index order, start before end before
  settlement; a match end applies after all segment facts at that instant.
  Windows that invert the known sequence of phases, or overflow u64 release
  times, fail before consumption. Settlement remains independent of a delayed
  segment-end release.

## 4. The game view

`context.game` is a frozen `GameView`:

| Field | Meaning |
|---|---|
| `phase` | `unavailable`, `pre_match`, `in_segment`, `between_segments`, `finished` |
| `segment` | the current segment index while `in_segment`; the last completed one otherwise; `null` before the first |
| `phase_since_ns` | release time of the fact that entered this phase |
| `score` | segments won per side, `{home, away}` |
| `last_segment` | the latest released `segment_end`: index, winner, end time, `details`; else `null` |
| `settled` | the highest segment index whose settlement has been released, or `null` |
| `revision` | increments on every applied fact |
| `reason` | why the view is `unavailable`, else `null` |
| `sport`, `game`, `segment_kind`, `market_types`, `competitors` | the file header |

Derived quantities are pure functions in `replay.economic_sdk.game`, taking the
view, `now_ns` and priors:
- `elapsed_in_phase(view, now)`;
- `expected_segment_end(view, priors)`, `phase_since + segment_duration` while `in_segment`;
- `expected_settlement(view, priors)`;
- `participant_score(view, participant)`, the score aligned to a Universe participant
  (`null` when unaligned).

Priors are policy values per Universe game, with no SDK defaults:
`segment_duration_ns`, `between_segments_ns`, `settlement_delay_ns`. §7 measures
them from the archive.

## 5. Strategy interface

In code, a strategy declares that it reads game state and any timers:

```python
Requirements(books=..., game=GameRequirement(timers=(("segment_start", 600_000_000_000),)))
```

In policy:

```json
"game": {
  "required": true,
  "input": {"path": "{game_state}", "sha256": "{game_state_sha256}"},
  "windows": {"segment_start": {"after_ms": 5000},
              "segment_end": {"after_ms": 5000},
              "match_end": {"after_ms": 5000}},
  "segment_start": "estimated",
  "priors": {"league_of_legends": {"segment_duration_ns": "…",
                                   "between_segments_ns": "…",
                                   "settlement_delay_ns": "…"}}
}
```

- No `game` block keeps today's behaviour exactly; strategies without game state
  are byte-identical.
- `required: true` with an unavailable file stops the run before any book is read
  with result `game_state_unavailable` and the file's reason. No episodes are
  written. `required: false` runs with the unavailable view.
- The pinned SHA-256, `required`, windows, start mode and priors all enter
  `experiment_sha256`.
- A basket declares `("game", None)` in its `inputs` to read game state. Only those
  entities are re-evaluated when a fact is released.
  Place it within an existing leg's input tuple, or append one global input
  tuple `(("game", None),)` after the book-leg input tuples. A reading basket
  requires `GameRequirement` and a game policy. Up to 128 distinct timers are
  allowed, anchored at any fact kind in §3, with nonnegative integer ns offsets.
- Releases are a third event source in `_advance`, after scope boundaries and
  before time-shift timers at the same instant. Fingerprints of game-reading
  entities include `view.revision`, so a cached observation is reused only for an
  unchanged view. An observation that calls a time-derived function sets
  `context_free = False`.
- **Timers.** `(anchor_fact, offset_ns)` schedules one re-evaluation at
  `max(release_ns, anchor source time + offset)` when the anchor is released, for
  exact thresholds ("ten minutes into the map") without polling.
- **Episode rows.** With a `game` block, episode open and maximum rows gain a
  closed `game` object: `phase`, `segment`, `score`, `revision`. The manifest gains
  a `game_state` binding (SHA-256, windows, mode). Readers require the object
  exactly when the binding exists.
  Phase/segment/score consistency is checked even after rehashing a tampered row.
  This extension is supported by complement policy 2, cross-venue/implication
  policies 2 and 3, and multi-market policy 1 (all SDK layout 2). Frozen complement
  policy 1 continues to reject it. Experiment identity binds the file hash and
  decisions; the local mount path is not a semantic experiment input.

## 6. Bench and corpus runner

- Bench run specs gain an optional `game_state_path` beside `context_directory`,
  mounted read-only, with `{game_state}` and `{game_state_sha256}` tokens.
- `.bench/fixtures/build_fixture.py` gains a `prepare_game_state` stage.
  It uses `replay.bench.game_state.prepare_fixture_stage` after context
  preparation, writes one `game_state/game_state.json`, and verifies an existing
  file before reuse without refreshing it. Only this builder source is tracked;
  downloaded windows, derivatives and run outputs remain ignored.
- The corpus runner prepares game state per event as one stage before strategy
  lines. The jobs runner is out of scope (it is retiring).

`python -m replay.bench prepare-game-state <context_dir> <out_dir>` exposes the
same preparation stage. Bench resolves the SHA-256 before Docker, rejects policy
pins differing from that file, mounts it at `/bench/game_state.json` read-only,
and records its hash in the resolved spec and result. Required unavailable groups
get a closed `game_state_unavailable` result with reason and hash, and no strategy
outputs; other groups can run. The aggregate bench status is
`GAME_STATE_UNAVAILABLE` (exit 2) when any group is skipped, or `FAILED` if an
active group fails. An all-skipped invocation does not start the supervisor.

## 7. Priors tool

`python -m gamestate.priors --archive-env ARCHIVE --output /research/priors.json`
streams every verified `ok` timeline in the
archive and writes per-game statistics (count, median, p10, p90) for segment
duration, gap between segments and settlement delay to a local JSON report. It
never writes to the archive.

`--archive-env` names an exported variable prefix, not a dotenv file. Known
Kalshi game names map to Universe vocabulary. Exact samples use temporary
SQLite storage, retaining one timeline at a time. The median averages the middle
two samples; p10/p90 use nearest rank. Report values are decimal ns strings
(a half-ns median is permitted), with null for empty metrics and counts of ok
and incomplete fetches. Existing output files are refused.

## 8. Tests and acceptance

Offline, with small hand-authored inputs:

1. **Preparation.** ok, `no_fetch`, `incomplete` and `no_source` events;
   `market_types`, `sport` and `game` taken from the context; competitor alignment:
   exact, casefold, unaligned, conflicting.
2. **Loader.** SHA-256 mismatch, unknown field, wrong event id, and unordered
   segments are rejected.
3. **Required.** A required strategy with an unavailable file reads no book and
   reports `game_state_unavailable`; a non-required one runs with the
   unavailable view.
4. **Release.** A winner or `details` is invisible one nanosecond before
   `segment_end` release and visible at it; `at_segment_end` delays starts;
   same-instant order against book cuts and scope boundaries; applied facts leave
   the queue.
5. **Staging.** Only game-reading entities are re-evaluated; the fingerprint
   changes with the revision; timers fire at exact instants across a quiet book.
6. **Identity.** Changing `required`, windows, mode, priors or the SHA-256
   changes `experiment_sha256`; output without a `game` block is byte-identical
   to the pinned complement and cross-venue goldens.
7. **Generic shape.** An event with no segments (match-level only) and an unknown
   `details` key both load and release correctly.
8. **Acceptance.** The LoL C9–LYON fixture with its archived game state: a probe
   strategy records phase changes, and each `segment_end` release falls within the
   window of the Kalshi map market's observed close in the book stream (the price
   collapses to 0 or 100).

## 9. Open questions

- **Best-of.** Kalshi lists only the maps played (the archived Bo5 at 3–1 lists
  maps 1–4), so the segment count is not the format. A strategy that needs it can
  read `total_maps` in `market_types` and the context's market lines; V1 does not
  derive it.
- **Non-Kalshi events.** Polymarket- or Limitless-only events are `no_source` until
  another source exists.
- **Live capture.** Timed in-segment state needs polling Kalshi `live_data` during
  matches. Those would be new fact kinds with opaque `details`, released at
  `received_at`, in the same queue.

## 10. Offline verification (9 October 2026)

The final focused gate passed **283 tests**, spanning the new game input tests,
bench, SDK runtime/ports/outcomes/fills, all affected strategy readers, preparation,
Kalshi archival and scheduled game state. Existing complement and cross-venue
byte goldens remain pinned. A separate read-only verification thread passed
**268 focused tests** and reported no remaining material findings. Its source
identity, episode-shape, nonfinite-number and whole-file-hashing findings were
resolved; strict-reader regressions were demonstrated before their fixes.

The broader root gate ran **933 tests**, with one existing stale-retrieval
cleanup failure. Replay ran **555 tests**, with 33 skipped, two failing signal
subtests and one error in the existing bundle-runner process tests. All failing
cases reproduced in the untouched main checkout: they rely on Linux `/proc` or
`prctl` on this macOS host. The final focused gate also covers the subsequent
finite-number and pre-reader cleanup fixes. Python syntax and whitespace checks
passed. No Rust, wire, supervisor or deployment implementation changed, so Rust
and Compose gates were not run. Docker/Redis execution, live services and the
retained C9–LYON book-close acceptance remain unverified.
