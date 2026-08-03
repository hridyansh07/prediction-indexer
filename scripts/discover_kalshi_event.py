#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.durable_http import DurableJsonClient
from prediction_indexer.event_bundles import write_event_bundle_files
from prediction_indexer.kalshi import (
    KALSHI_BASE_URL,
    normalize_kalshi_event,
    normalize_kalshi_market,
)
from prediction_indexer.storage import (
    stable_job_id,
    utc_now,
    write_json,
    write_ndjson,
)


def event_ticker_from_value(value: str) -> str:
    if "://" in value:
        path = urlparse(value).path.rstrip("/")
        value = path.rsplit("/", 1)[-1]
    ticker = value.strip().upper()
    if not ticker.startswith("KX"):
        raise ValueError(f"Could not parse a Kalshi event ticker from {value!r}")
    return ticker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover exact Kalshi game events from event tickers or URLs."
    )
    parser.add_argument(
        "events",
        nargs="+",
        help="Kalshi event ticker or full market URL.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=PROJECT_ROOT / "data",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore cached responses and query Kalshi again.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tickers = sorted({event_ticker_from_value(value) for value in args.events})
    specification = {"venue": "kalshi", "event_tickers": tickers}
    job_id = stable_job_id(specification)
    job_directory = args.data_dir / "discovery" / "kalshi" / job_id
    events_path = job_directory / "events.ndjson"
    markets_path = job_directory / "markets.ndjson"
    bundles_path = job_directory / "event_bundles.ndjson"
    write_json(job_directory / "request.json", specification)

    client = DurableJsonClient(
        args.data_dir / "cache" / "http",
        force_refresh=args.refresh,
    )
    normalized_events = []
    normalized_markets = []

    for ticker in tickers:
        response = client.get_json(
            KALSHI_BASE_URL,
            f"/events/{ticker}",
            params={"with_nested_markets": True},
        )
        payload = response.data
        if not isinstance(payload, dict) or not isinstance(payload.get("event"), dict):
            raise ValueError(f"Unexpected Kalshi event response for {ticker}")
        event = payload["event"]
        raw_markets = event.get("markets") or payload.get("markets") or []
        markets = [
            normalize_kalshi_market(event, market)
            for market in raw_markets
            if isinstance(market, dict)
        ]
        market_ids = [
            str(market["market_id"])
            for market in markets
            if market.get("market_id")
        ]
        normalized_events.append(normalize_kalshi_event(event, market_ids))
        normalized_markets.extend(markets)

    normalized_events.sort(key=lambda item: str(item.get("event_id") or ""))
    normalized_markets.sort(key=lambda item: str(item.get("market_id") or ""))
    write_ndjson(events_path, normalized_events)
    write_ndjson(markets_path, normalized_markets)
    bundle_summary = write_event_bundle_files(
        events_path,
        markets_path,
        bundles_path,
    )

    summary = {
        "job_id": job_id,
        "generated_at": utc_now(),
        "specification": specification,
        "event_count": len(normalized_events),
        "market_count": len(normalized_markets),
        "bundle_count": bundle_summary["bundle_count"],
        "http": {
            "cache_hits": client.cache_hits,
            "network_requests": client.network_requests,
        },
        "files": {
            "events": str(events_path),
            "markets": str(markets_path),
            "event_bundles": str(bundles_path),
        },
    }
    write_json(job_directory / "run.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

