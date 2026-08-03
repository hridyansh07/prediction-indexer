# Game-level discovery and event grouping

## Exact Kalshi games

Use a Kalshi event ticker or paste the full market URL:

```bash
python3 scripts/discover_kalshi_event.py \
  "https://kalshi.com/markets/kxdota2game/dota-2-game/kxdota2game-26jul181030parity"
```

This makes one event request, then writes:

```text
data/discovery/kalshi/<job-id>/
  events.ndjson
  markets.ndjson
  event_bundles.ndjson
  run.json
```

The event bundle keeps the game as the parent and nests its mutually exclusive
moneyline markets as outcomes. Each child retains its Kalshi ticker because that
is the `market_id` required by Oddpool historical endpoints.

For broader game discovery, use the existing series scanner with game-level
series such as:

- `KXDOTA2GAME`
- `KXLOLGAME`
- `KXVALORANTGAME`
- `KXCS2GAME`
- `KXROCKETLEAGUEGAME`
- `KXR6GAME`

Kalshi game titles do not always name the tournament. The market rules do, and
`--contains` checks those rules locally after caching the API response. For the
EWC 2026 Dota 2 window:

```bash
python3 scripts/discover_kalshi.py \
  --series-ticker KXDOTA2GAME \
  --status settled \
  --min-close 2026-07-15T00:00:00Z \
  --max-close 2026-07-20T23:59:59Z \
  --contains "Esports World Cup 2026"
```

## Polymarket multi-outcome events

Polymarket represents a multi-outcome question as one parent Event containing
multiple binary conditions. Each child condition has its own condition ID and
YES/NO token IDs, so the historical downloader still has to retrieve each child.

Create a grouped view from any completed discovery job without making API calls:

```bash
python3 scripts/build_event_bundles.py \
  --events data/discovery/polymarket/<job-id>/events.ndjson \
  --markets data/discovery/polymarket/<job-id>/markets.ndjson
```

The resulting `event_bundles.ndjson` preserves the parent Event and nests each
binary condition under `outcomes`. It marks multi-outcome events as partition
candidates, not proven partitions, until their rules and catch-all outcomes have
been verified.
