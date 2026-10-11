# Kalshi game-state pull V1

`pull_kalshi_game_state.py` is a one-shot, public-HTTP pull and immutable raw
archive, followed by an offline timeline derivation. It does not change capture,
Universe, replay, targets, or production services. No authentication is sent.

## CLI

Run from the repository root with the project virtual environment:

```bash
export UNIVERSE_BASE_URL=https://<universe-host>

# Explicit bundles; repeat --bundle as needed.
.venv/bin/python scripts/pull_kalshi_game_state.py \
  --bundle bundle_6fa2d7ee2dce01ca0d73f483 --output-root data/game-state-local

# Or discover a bounded activation interval. Both RFC3339 bounds are required.
.venv/bin/python scripts/pull_kalshi_game_state.py \
  --activation-start 2026-09-27T00:00:00Z \
  --activation-end 2026-09-28T00:00:00Z --output-root data/game-state-local

# Fetch again without replacing any existing object.
.venv/bin/python scripts/pull_kalshi_game_state.py \
  --bundle bundle_6fa2d7ee2dce01ca0d73f483 --no-skip-existing \
  --output-root data/game-state-local

# No network, no Universe URL required. Prefix comes from report.json.
.venv/bin/python scripts/pull_kalshi_game_state.py \
  --regenerate 'gamestate/source=kalshi/date=YYYY-MM-DD/milestone=ID/fetch=YYYYMMDDTHHMMSS.ffffffZ' \
  --output-root data/game-state-local --timeline-output data/rebuilt-timeline.json
```

Only `UNIVERSE_BASE_URL` is supported; `UNIVERSE_URL` is not an alias.
By default the local store is `<output-root>/archive`; an explicit `ARCHIVE_ROOT`
takes precedence. Default output root is `kalshi-game-state-output`.
`ARCHIVE_BACKEND=gcs` and `ARCHIVE_GCS_BUCKET` select the existing GCS adapter
(ADC); there are no custom cloud calls. A local smoke must explicitly select
local and use an isolated archive root, without GCS configuration.
The script never reads `.env` files or credentials.

`--skip-existing` is on by default. Discovery and exact mapping still run before
skip, because a bundle ID is not a milestone ID. Skip scans provider listings for
receipt candidates, then strictly validates receipt, object metadata, both byte
identities, codec framing, response records, and required fetched responses.
Malformed/corrupt receipts are not skip authority; provider failures are reported
rather than interpreted as absence. A timeline is not required for skip.

`report.json` is the atomic local invocation summary, or use `--report PATH`.
Exit 1 means discovery/fetch, archive, or timeline failure; an ordinary unmapped
bundle is not an invocation failure unless its reason is `fetch_failed`.
Regeneration returns only a timeline, not a fetch report. Without
`--timeline-output`, regeneration immutably publishes beside the raw receipt as
`timeline.v<derivation_version>.json`: identical bytes are idempotent; different
bytes under the same version conflict. A derivation fix bumps
`DERIVATION_VERSION`, and regeneration then publishes the new version beside the
old one. Raw objects are never rewritten.

## Mapping and capture

Universe history pages have `selections`, `sort`, and `next_cursor`. Every unique
run occurrence is read; `context.event_refs` is unioned across all contexts.
Vendor envelopes are open objects, but all fields the adapter uses are checked.
Mapping uses only native tickers, exact milestone ID/type, and membership in
`related_event_tickers`. No team-name or time matching exists. A bundle may omit
maps that Kalshi lists: this is not an inconsistency.

Unmapped reasons: `no_kalshi_events`, `no_milestone`, `multiple_milestones`,
`ticker_not_related`, `fetch_failed`. A single non-`esports_match` milestone is
`no_milestone`. Validation proceeds in sorted ticker order; a failed query cannot
be treated as an empty result. A nonempty milestone cursor is ambiguous and gives
`multiple_milestones`. Inconsistent snapshots of the same milestone fail closed.

All bundles sharing a milestone share one fetch and one receipt. Raw records are
Kalshi responses only: the mapping `/milestones` attempts, then live data and every
related nested event, in actual request order. Universe history and selection
attempts are mapping evidence, not game state; they stay in `report.json`. Raw
sequence numbers are contiguous within that fetch; report sequence numbers cover
the whole invocation. Unmapped/discovery attempts remain visible as metadata in the report;
there is no guessed milestone archive location for them.

Requests are spaced per host: 0.2 seconds for Kalshi, 3.4 seconds for Universe,
which admits three unauthenticated requests per ten seconds. 429, 5xx, timeouts
and connection errors (including a body cut short) get at most five total attempts
with 1/2/4/8-second backoff. Valid numeric or HTTP-date Retry-After is honored; a
wait over 60 seconds aborts rather than retrying early. 4xx is not retried. Redirects are refused, and HTTP
errors are read as responses. A 200 with invalid JSON or invalid fields is a
visible failure, not an empty result. An exhausted endpoint does not stop fetching
other related events; the committed receipt is `incomplete`.

## Closed V1 formats

No unspecified extra fields are admitted in raw records or receipts. JSON rejects
duplicate keys and nonfinite numeric constants. Times are integer UTC Unix ns;
SHA-256 values are lowercase hex. JSON serialization is sorted, UTF-8, and LF
terminated. The compressed payload is exact serialized NDJSON in the shared
encoder's single level-3, checksummed, dictionary-free Zstandard frame.

Object prefix:

```text
gamestate/source=kalshi/date=<UTC milestone start date>/milestone=<id>/fetch=<fetch start %Y%m%dT%H%M%S.%fZ>
```

The fetch start is the first archived (Kalshi mapping) request. Each prefix
must be unused. Publication is `responses.ndjson.zst`, fresh metadata verification,
then `receipt.json` as the raw commit marker. Only then is the raw reopened,
verified, decoded to temporary disk, and used to publish `timeline.v1.json`.
Timeline failure does not undo a committed receipt, and the report retains its
prefix and fetched count.

**Record:** exactly `record_version` (integer 1), `seq`, `requested_at_ns`,
`received_at_ns` (nullable), `method` (`GET`), `url`, `status` (nullable HTTP code),
`error` (nullable code), `content_type` (nullable), `body_sha256` (nullable), and
exactly one of `body` or `body_b64`. `body` is a UTF-8 string or null when no body
was accepted; `body_b64` is a canonical base64 string for non-UTF-8 bytes, with
`error=non_utf8_body`. Re-encoding `body` or decoding base64 must match
`body_sha256`. An absent body has a null digest. No normalization is performed on
response bodies. Transport errors include `timeout`, `connection`, `http_status`,
`non_utf8_body`, `body_too_large`, and `metadata_too_large`.

**Receipt:** exactly `receipt_version` (1), `source` (`kalshi`), `milestone_id`,
sorted unique `bundle_ids`, `script_version` (1), `fetch_started_ns`,
`fetch_ended_ns`, `status` (`complete` or `incomplete`), `logical`, `stored`,
`request_count`, `provider_checksum`, `provider_checksum_algorithm`.
`logical` has exactly `sha256`, `byte_length`, `line_count`; `stored` has exactly
`sha256`, `byte_length`. Provider checksum is separate from stored SHA-256 and is
required by the existing GCS verified-reader contract. Prefix milestone and fetch
timestamp must agree with the receipt. `complete` means required requests
succeeded with recognized shapes, not that all markets settled or agree.

**Timeline:** exactly `derivation_version` (1), `milestone_id`, `bundle_ids`,
`game`, `league`, `tournament`, `scheduled_start_ns`, `match_end_ns`, `status`,
`series`, `maps`, `inconsistencies`.
Copied values use `{value, source_field}`; absent values are null. `series` has
`winner_market`, `score`. A winner is null or exactly
`{ticker, yes_sub_title, source_field}`. Each map has exactly `index`,
`winner_market`, `forfeit`, `duration_s`, `scores`, `close_ns`, `settlement_ns`,
`derived_start_ns`, `time_basis`. `scores` has `home`, `away`, `home_stats`,
`away_stats`, each a copied-value wrapper. The stats `value` is an opaque copy of
the vendor stat object, preserving game-specific objective totals.
`time_basis` is exactly `{end: kalshi_market_close, start: close_minus_duration}`.
`inconsistencies` entries have exactly `code`, nullable `map_index`, sorted by
index/code and deduplicated.

The adapter recognizes `product_metadata.competition_scope = "Map N Winner"`;
it does not parse titles or ticker names. Live periods come from
`home_stats`/`away_stats` rows, and scores from `home_periods`/`away_periods`.
Bo1 may omit the latter; no final-series-score substitution is made.
Tournament is `details.tournament_name`. The series event is
`details.main_game_event_ticker`, or, when that field is absent, the single entry
of `primary_event_tickers`; `series.event_ticker` records which. With neither, or
when that event was not fetched, the series winner is null and
`series_event_missing` is reported. Only market `result=yes` establishes a market
winner; cross-check uses `custom_strike.esports_competitor` and the exact home/away
competitor IDs. Live winner flags never fill a missing market winner.
Derived start subtracts the agreed duration in ns from the agreed market close.
Disagreement produces a null derived start, not a selected side's duration/time.

Required inconsistencies are `winner_disagreement`, `unsettled_map_market`, and
`map_count_difference`. Additional diagnostics: `response_shape`,
`archived_milestone_missing` (derivation failure), `milestone_changed`,
`related_event_missing`, `live_data_missing`, `invalid_time`, `series_event_missing`,
`unsettled_series_market`, `multiple_winner_markets`, `period_shape`,
`duplicate_period`, `map_event_shape`, `unsupported_event_scope`,
`winner_crosscheck_unavailable`, `map_event_missing`, `duration_disagreement`,
`duration_invalid`, `forfeit_disagreement`, `market_close_disagreement`,
`market_settlement_disagreement`. No inconsistency is economically resolved.

**Report:** `report_version` (1), `bundles_considered`, `bundles_mapped`,
`bundles_unmapped` (reason → count), `milestones_fetched`, `milestones_skipped`,
`milestones_incomplete`, `requests`, `retries`, `bundles`, `fetches`, `failures`,
`request_attempts`. Bundle rows have `bundle_id`, nullable `milestone_id`, nullable
`reason`. Fetch rows have `milestone_id`, `prefix`, `status`, `errors` (URL/reason),
and `disqualified` (whether the derived timeline has a disqualifying inconsistency;
null when the timeline could not be written).
Attempt rows have `seq`, `url`, `status`, `error`, `requested_at_ns`,
`received_at_ns`; no bodies or headers. Failure rows have `milestone_id`, `reason`,
and only discovery failures add `detail`. Failure reasons distinguish
`selection_discovery_failed`, `object_store_failed`, `io_failed`,
`archive_schema_failed`, `timeline_failed`.

## Bounds and verification

Bounds apply to single untrusted inputs, never to how much is paged or listed:
20-second socket timeout; 4 MiB accepted response bodies; 1 KiB content-type;
8 KiB URLs; 4 KiB cursors; 50 rows/page; 100 related tickers/periods; 1,000
markets/event; 128 MiB per-fetch decoded raw; 1 MiB receipt and timeline. Pages
and archive listings stream; only cursors are retained, and a repeated cursor is
an error. A bound is a visible failure, never ordinary absence. Oversized bodies
are rejected rather than partially archived as complete. The attempt journal lives
on temporary disk; compression and verified decoding are streaming. Temporary
files are closed/removed even on failures.

```bash
.venv/bin/python -m unittest tests.test_kalshi_game_state
```

Tests use small hand-authored shapes and injected transports; a socket guard
rejects network access. GCS tests reuse the existing fake provider, not a real
bucket. Neither offline tests nor smoke write production cloud objects. This V1
does not verify map closure against retained order books: that acceptance check
requires separately authorized book evidence, not a broad retained-data pull.
