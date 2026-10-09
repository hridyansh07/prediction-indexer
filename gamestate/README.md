# Game state

The public Kalshi adapter owns pulls, immutable raw responses and offline
timeline derivation. It imports archive/encoder, not Universe implementation
or Replay. `python -m gamestate.run_pull` retains the explicit-bundle CLI.

`python -m gamestate.run_scheduled configs/gamestate.json` is one-shot. External
cron owns the 30-minute cadence. It reads retired bundles, retirement history
and `/v1/bundles/<bundle>/outcomes` to obtain `event:d1:<sha256>`. The two-hour
delay is not a settlement assertion. Unknown identity or missing retirement
fails visibly. Backfill requires `--backfill --activation-start <UTC>
--activation-end <UTC>`; it removes the 50-attempt invocation cap, not adapter
HTTP, raw, history or listing bounds.

New raw prefixes are `gamestate/source=kalshi/event=<event digest>/date=<date>/
milestone=<id>/fetch=<timestamp>`. Receipt version 2 adds only `event_id` to the
closed V1 receipt. Event timelines use derivation version 2 and add `event_id`.
Legacy V1 formats remain readable and are never rewritten. No association
artifact is created. `gamestate.timeline.latest` verifies exact raw and timeline
bytes and recomputes the timeline before making it research evidence.

`gamestate.sqlite3` is an append-only version-1 operational ledger with columns
`event_id, bundle_id, attempted_at_ns, outcome, milestone_id, prefix, error`.
Failed/incomplete pulls retry after 1/2/4/8 hours, up to five attempts; delay
calculation is capped at 24 hours. Final unmapped outcomes do not retry.
A complete ledger entry never grants skip: only verified archive bytes do.
A missing timeline does not invalidate raw completeness or trigger a live
refetch; regenerate it offline with the pull CLI. Losing the ledger resets
backoff, not archive identities. Never delete archive objects to reset it.
Opening a ledger validates its version and all schema objects, including the
append-only triggers; it never repairs an existing schema. Fresh initialization
is transactional. There is no in-place migration of evidence or Universe data.

The Compose `jobs` service has a separate `GAMESTATE_DATA_ROOT`, private archive
access and no capture/Replay database mounts. Configure archive environment
variable names in JSON; credentials are never JSON fields. Deployment/backfill
are operator actions. Offline tests: `tests.test_kalshi_game_state` and
`tests.test_gamestate_scheduled`.
