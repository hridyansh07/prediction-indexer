#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.durable_http import DurableJsonClient
from analysis.kalshi import discover_kalshi


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover Kalshi events and market tickers into durable files."
    )
    parser.add_argument(
        "--series-ticker",
        action="append",
        required=True,
        help="Kalshi series ticker. Repeat for multiple series.",
    )
    parser.add_argument(
        "--status",
        action="append",
        choices=("unopened", "open", "closed", "settled"),
        help="Event status. Repeat as needed; defaults to settled.",
    )
    parser.add_argument("--min-close", help="Inclusive ISO-8601 close-time lower bound.")
    parser.add_argument("--max-close", help="Inclusive ISO-8601 close-time upper bound.")
    parser.add_argument(
        "--contains",
        help="Optional case-insensitive local filter over event, market, and rules text.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=PROJECT_ROOT / "data",
        help="Root for durable caches and generated outputs.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore cached HTTP responses and query the API again.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = DurableJsonClient(
        args.data_dir / "cache" / "http",
        force_refresh=args.refresh,
    )
    summary = discover_kalshi(
        client,
        data_directory=args.data_dir,
        series_tickers=args.series_ticker,
        statuses=args.status or ["settled"],
        min_close=args.min_close,
        max_close=args.max_close,
        contains=args.contains,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

