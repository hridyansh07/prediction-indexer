#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.partition_pipeline import run_partition_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the reproducible EWC partition-sum economic gate without "
            "making Oddpool history requests."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=PROJECT_ROOT / "configs" / "partition_sum_v1.json",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Require all public fee metadata to already exist in the durable cache.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_partition_pipeline(
        args.config,
        project_root=PROJECT_ROOT,
        offline=args.offline,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 2 if result["terminal_status"] == "DATA_INVALID" else 0


if __name__ == "__main__":
    raise SystemExit(main())

