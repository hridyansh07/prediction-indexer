#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.event_bundles import write_event_bundle_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Group normalized child markets under their venue event."
    )
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--markets", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        help="Defaults to event_bundles.ndjson beside the input events file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = args.output or args.events.parent / "event_bundles.ndjson"
    summary = write_event_bundle_files(args.events, args.markets, output)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

