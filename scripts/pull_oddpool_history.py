#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.oddpool_client import RetryingOddpoolClient
from prediction_indexer.oddpool import load_oddpool_api_key, pull_manifest_history


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull resumable Oddpool full-depth historical orderbooks."
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=PROJECT_ROOT / "data",
    )
    parser.add_argument("--granularity", choices=("1m", "5m"), default="1m")
    parser.add_argument("--page-limit", type=int, default=200)
    parser.add_argument(
        "--max-pages-per-target",
        type=int,
        help="Optional smoke-test cap. Omit for a full resumable pull.",
    )
    parser.add_argument(
        "--max-network-requests",
        type=int,
        default=900,
        help="Run-level safety cap below the free tier's 1,000-request quota.",
    )
    args = parser.parse_args()
    if args.page_limit < 1 or args.page_limit > 200:
        parser.error("--page-limit must be between 1 and 200")
    return args


def main() -> int:
    args = parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    api_key = load_oddpool_api_key(args.env_file)
    client = RetryingOddpoolClient(args.data_dir / "cache" / "http")
    summary = pull_manifest_history(
        client,
        api_key=api_key,
        manifest=manifest,
        output_root=args.data_dir / "oddpool" / "orderbooks",
        granularity=args.granularity,
        page_limit=args.page_limit,
        max_pages_per_target=args.max_pages_per_target,
        max_network_requests=args.max_network_requests,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary.get("complete") else 2


if __name__ == "__main__":
    raise SystemExit(main())
