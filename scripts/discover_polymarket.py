#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.durable_http import DurableJsonClient
from prediction_indexer.polymarket import discover_polymarket


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover Polymarket events, conditions, and token IDs."
    )
    parser.add_argument("--query", help="Public-search query, such as 'EWC 2026'.")
    parser.add_argument(
        "--event-id",
        action="append",
        help="Known Gamma event ID. Repeat for multiple events.",
    )
    parser.add_argument(
        "--max-search-pages",
        type=int,
        default=20,
        help="Safety cap for public-search pagination.",
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
    args = parser.parse_args()
    if not args.query and not args.event_id:
        parser.error("provide --query or at least one --event-id")
    return args


def main() -> int:
    args = parse_args()
    client = DurableJsonClient(
        args.data_dir / "cache" / "http",
        force_refresh=args.refresh,
    )
    summary = discover_polymarket(
        client,
        data_directory=args.data_dir,
        query=args.query,
        event_ids=args.event_id,
        max_search_pages=args.max_search_pages,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

