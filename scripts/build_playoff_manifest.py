#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.event_bundles import read_ndjson
from prediction_indexer.playoff_manifest import build_playoff_manifest
from prediction_indexer.storage import write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match Polymarket game moneylines to Kalshi game events."
    )
    parser.add_argument("--kalshi-events", type=Path, required=True)
    parser.add_argument("--kalshi-markets", type=Path, required=True)
    parser.add_argument("--polymarket-events", type=Path, required=True)
    parser.add_argument("--polymarket-markets", type=Path, required=True)
    parser.add_argument(
        "--title-contains",
        default="Esports World Cup Playoffs",
        help="Required phrase in the Polymarket parent event title.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "data" / "manifests" / "ewc_dota_playoffs.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_playoff_manifest(
        read_ndjson(args.kalshi_events),
        read_ndjson(args.kalshi_markets),
        read_ndjson(args.polymarket_events),
        read_ndjson(args.polymarket_markets),
        title_contains=args.title_contains,
    )
    write_json(args.output, manifest)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "match_count": manifest["match_count"],
                "history_target_count": manifest["history_target_count"],
                "unmatched_count": len(manifest["unmatched_polymarket_events"]),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
