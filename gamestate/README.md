# Game state

The public Kalshi adapter owns pulls, immutable raw responses and offline
timeline derivation. It imports archive/encoder, not Universe implementation
or Replay. `python -m gamestate.run_pull` retains the explicit-bundle CLI.

`python -m gamestate.run_scheduled configs/gamestate.json` is one-shot. External
cron owns the 30-minute cadence. It pages every Universe bundle holding only the
cursor. Bundles without a Kalshi market are skipped with no call (Polymarket- or
Limitless-only events need another source). For a retired bundle it has not
mapped yet it reads the retirement history
(two-hour settle delay, not a settlement assertion) and `/v1/bundles/<bundle>/outcomes`
to obtain the immutable `event:d1:<sha256>`, then pulls. Universe responses are
mapping reads, never recorded; only Kalshi responses become archived evidence.
Backfill requires `--backfill --activation-start <UTC> --activation-end <UTC>`; it
removes the per-run pull cap. Nothing else is capped by count: pages and listings
stream, and only single untrusted inputs (one HTTP body, one receipt) are bounded.

New raw prefixes are `gamestate/source=kalshi/event=<event digest>/date=<date>/
milestone=<id>/fetch=<timestamp>`. Receipt version 2 adds only `event_id` to the
closed V1 receipt. Event timelines use derivation version 2 and add `event_id`.
Legacy V1 formats remain readable and are never rewritten. No association
artifact is created.

`gamestate.sqlite3` (ledger version 2) is append-only operational state with two
tables: `fetch_attempts(event_id, bundle_id, attempted_at_ns, outcome, milestone_id,
prefix, error)` and the immutable `bundle_events(bundle_id, event_id, recorded_at_ns)`.
Outcomes `incomplete`, `fetch_failed` and `unavailable` retry after 1/2/4/8 hours, up
to five attempts; every other outcome is final. A final outcome is the skip authority:
finished bundles cost no Universe, Kalshi or archive call. `unavailable` records any
per-bundle failure (outcomes 404/409, missing retirement, archive error) and the run
continues with the next bundle; that event simply has no game state. Failures before
the event is known are keyed by bundle. `complete_inconsistent` (a complete receipt
whose derived timeline is disqualified) and `timeline_failed` (raw archived, timeline
not written; regenerate offline) are final. Losing the ledger costs one remapping
pass: the pull still skips milestones the archive holds complete, so archived games
are never refetched. Never delete archive objects to reset it. Opening a ledger
validates its version and every schema object, including the append-only triggers;
it never repairs an existing schema. Fresh initialization is transactional.

`gamestate.timeline.latest` streams an event's fetches and keeps the best one
(complete first, then newest); an unreadable receipt is skipped and counted in
`rejected_fetches`. It verifies exact raw and timeline bytes and recomputes the
timeline before returning it.

The Compose `jobs` service has a separate `GAMESTATE_DATA_ROOT`, private archive
access and no capture/Replay database mounts. Configure archive environment
variable names in JSON; credentials are never JSON fields. Deployment/backfill
are operator actions. Offline tests: `tests.test_kalshi_game_state` and
`tests.test_gamestate_scheduled`.
