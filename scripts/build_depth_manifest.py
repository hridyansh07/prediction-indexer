#!/usr/bin/env python3
"""Convert a sibling manifest into an Oddpool depth-pull manifest.

Oddpool is the only source of historical book depth, and on the free tier the
request budget is small, so a depth pull must be scoped to the partitions that
actually need size-adjusted measurement. This selects targets by market type
and event, estimates the request cost before anything is fetched, and writes a
manifest in the shape `pull_oddpool_history.py` already consumes.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.storage import parse_iso8601, utc_now, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sibling-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--market-type",
        action="append",
        required=True,
        help="Market type to include; repeatable.",
    )
    parser.add_argument(
        "--event-key",
        action="append",
        default=None,
        help="Restrict to these event keys; repeatable. Default: all events.",
    )
    parser.add_argument(
        "--venue",
        action="append",
        choices=("kalshi", "polymarket"),
        default=None,
        help="Restrict to these venues; repeatable. Default: both.",
    )
    parser.add_argument("--page-limit", type=int, default=200)
    return parser.parse_args()


# Snapshots Oddpool actually returns per minute of market life at
# granularity=1m, measured against completed pulls. Kalshi tracks the nominal
# one-per-minute; Polymarket emits roughly one per book change and so runs far
# denser, which makes a naive one-per-minute estimate ~4x too cheap.
SNAPSHOTS_PER_MINUTE = {"kalshi": 1.0, "polymarket": 3.7}


def estimate_pages(
    start: str | None,
    end: str | None,
    page_limit: int,
    *,
    venue: str,
) -> int:
    first, last = parse_iso8601(start), parse_iso8601(end)
    if not first or not last or last <= first:
        return 1
    minutes = (last - first).total_seconds() / 60.0
    density = SNAPSHOTS_PER_MINUTE.get(venue, 1.0)
    return max(1, math.ceil(minutes * density / page_limit))


def main() -> int:
    arguments = parse_args()
    sibling = json.loads(arguments.sibling_manifest.read_text(encoding="utf-8"))
    wanted_types = set(arguments.market_type)
    wanted_events = set(arguments.event_key) if arguments.event_key else None
    wanted_venues = set(arguments.venue) if arguments.venue else {"kalshi", "polymarket"}

    targets: list[dict[str, Any]] = []
    estimated_pages = 0
    for source in sibling.get("history_targets") or []:
        if source.get("market_type") not in wanted_types:
            continue
        if source.get("venue") not in wanted_venues:
            continue
        if wanted_events is not None and source.get("event_key") not in wanted_events:
            continue

        if source["venue"] == "kalshi":
            start, end = source.get("open_time"), source.get("close_time")
            target = {
                "venue": "kalshi",
                "target_id": source["target_id"],
                "market_id": source["ticker"],
                "event_key": source["event_key"],
                "market_type": source["market_type"],
                "outcome": source.get("yes_label"),
                "start_time": start,
                "end_time": end,
            }
        else:
            start, end = source.get("start_time"), source.get("end_time")
            target = {
                "venue": "polymarket",
                "target_id": source["target_id"],
                "market_id": source["market_id"],
                "event_key": source["event_key"],
                "market_type": source["market_type"],
                "asset_id": None,
                "outcome_tokens": source.get("outcome_tokens") or [],
                "start_time": start,
                "end_time": end,
            }
        targets.append(target)
        estimated_pages += estimate_pages(
            start, end, arguments.page_limit, venue=source["venue"]
        )

    targets.sort(key=lambda item: (item["venue"], item["target_id"]))
    event_keys = sorted({str(target["event_key"]) for target in targets})

    # partition_sum.build_partition_definitions reads the nested `matches`
    # shape, so emit it here rather than converting again downstream.
    rules_by_target = {
        str(source["target_id"]): source.get("rules_hash") or ""
        for source in sibling.get("history_targets") or []
    }
    matches: list[dict[str, Any]] = []
    for event_key in event_keys:
        event_targets = [t for t in targets if t["event_key"] == event_key]
        kalshi_targets = [t for t in event_targets if t["venue"] == "kalshi"]
        polymarket_targets = [t for t in event_targets if t["venue"] == "polymarket"]
        match: dict[str, Any] = {
            "event_key": event_key,
            "history_targets": event_targets,
        }
        if kalshi_targets:
            match["kalshi"] = {
                # Every selected type here is a single mutually exclusive Kalshi
                # event, so one outcome resolves YES and the YES legs partition.
                "mutually_exclusive": True,
                "market_ids": [t["market_id"] for t in kalshi_targets],
                "rules_hashes": [
                    rules_by_target.get(t["target_id"], "") for t in kalshi_targets
                ],
            }
        if polymarket_targets:
            first = polymarket_targets[0]
            match["polymarket"] = {
                "condition_id": first["market_id"],
                "outcome_tokens": first.get("outcome_tokens") or [],
                "rules_hash": rules_by_target.get(first["target_id"], ""),
                "resolution_source": "polymarket_uma",
                "condition_ids": [t["market_id"] for t in polymarket_targets],
            }
        matches.append(match)
    manifest = {
        "version": 1,
        "dataset_name": f"{sibling.get('dataset_name')}_depth",
        "generated_at": utc_now(),
        "source_manifest": str(arguments.sibling_manifest),
        "scope": {
            "market_types": sorted(wanted_types),
            "event_keys": event_keys,
            "venues": sorted(wanted_venues),
            "depth_source": "oddpool",
        },
        "matches": matches,
        "estimated_request_upper_bound": estimated_pages,
        "history_target_count": len(targets),
        "history_targets": targets,
    }
    write_json(arguments.output, manifest)

    by_venue: dict[str, int] = {}
    for target in targets:
        by_venue[target["venue"]] = by_venue.get(target["venue"], 0) + 1
    print(f"targets: {len(targets)}  {by_venue}")
    print(f"events: {len(event_keys)}")
    print(f"estimated requests (upper bound): {estimated_pages}")
    print(f"wrote {arguments.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
