#!/usr/bin/env python3
"""Build the Polymarket depth manifest for the equivalence-class conditions.

Only Polymarket needs depth: Kalshi's candlestick series is already a live
top-of-book quote, while Polymarket's free CLOB series is a traded price that
goes stale in thin markets and cannot be compared against a quote.

Targets are ordered cheapest-complete-event-first. A price comparison needs
every leg of a class, so a half-pulled event is worth nothing — if the request
budget runs out mid-pull, this ordering leaves whole usable events behind
rather than fragments of several.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.storage import parse_iso8601, utc_now, write_json

# Measured against completed pulls: Oddpool emits roughly one Polymarket
# snapshot per book change, far denser than the nominal one per minute.
SNAPSHOTS_PER_MINUTE = 3.7
PAGE_LIMIT = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, default=PROJECT_ROOT / "data")
    parser.add_argument(
        "--output", type=Path,
        default=PROJECT_ROOT / "data" / "manifests" / "ewc_dota_pm_depth.json",
    )
    parser.add_argument(
        "--available-events", type=Path, required=True,
        help="JSON list of event keys confirmed present in Oddpool's archive.",
    )
    return parser.parse_args()


def estimate_pages(start: str | None, end: str | None) -> int:
    first, last = parse_iso8601(start), parse_iso8601(end)
    if not first or not last or last <= first:
        return 1
    minutes = (last - first).total_seconds() / 60.0
    return max(1, math.ceil(minutes * SNAPSHOTS_PER_MINUTE / PAGE_LIMIT))


def main() -> int:
    arguments = parse_args()
    data = arguments.data_directory
    available = set(json.loads(arguments.available_events.read_text(encoding="utf-8")))

    sibling = json.loads(
        (data / "manifests" / "ewc_dota_siblings.json").read_text(encoding="utf-8")
    )
    prior = json.loads(
        (data / "manifests" / "ewc_dota_playoffs.json").read_text(encoding="utf-8")
    )
    already = {
        str(t["market_id"])
        for t in prior["history_targets"]
        if t.get("venue") == "polymarket"
    }

    # Types that participate in the equivalence classes.
    wanted_types = {"map_winner", "total_maps", "series_moneyline"}
    by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in sibling["history_targets"]:
        if source["venue"] != "polymarket":
            continue
        if source["event_key"] not in available:
            continue
        if source["market_type"] not in wanted_types:
            continue
        if str(source["market_id"]) in already:
            continue  # depth already held from the original pull
        by_event[source["event_key"]].append(source)

    costed = []
    for event_key, sources in by_event.items():
        cost = sum(
            estimate_pages(s.get("start_time"), s.get("end_time")) for s in sources
        )
        costed.append((cost, event_key, sources))
    costed.sort(key=lambda item: (item[0], item[1]))

    targets: list[dict[str, Any]] = []
    running = 0
    plan: list[dict[str, Any]] = []
    for cost, event_key, sources in costed:
        running += cost
        plan.append(
            {
                "event_key": event_key,
                "conditions": len(sources),
                "estimated_requests": cost,
                "cumulative_requests": running,
            }
        )
        for source in sorted(sources, key=lambda s: str(s["market_id"])):
            targets.append(
                {
                    "venue": "polymarket",
                    "target_id": source["target_id"],
                    "market_id": str(source["market_id"]),
                    "event_key": event_key,
                    "market_type": source["market_type"],
                    "group_item_title": source.get("group_item_title"),
                    "asset_id": None,
                    "outcome_tokens": source.get("outcome_tokens") or [],
                    "start_time": source.get("start_time"),
                    "end_time": source.get("end_time"),
                }
            )

    manifest = {
        "version": 1,
        "dataset_name": "ewc_dota_pm_depth",
        "generated_at": utc_now(),
        "scope": {
            "venues": ["polymarket"],
            "market_types": sorted(wanted_types),
            "depth_source": "oddpool",
            "ordering": "cheapest_complete_event_first",
            "note": (
                "Kalshi is excluded: its candlestick series is already a live "
                "top-of-book quote and needs no metered depth pull."
            ),
        },
        "matches": [{"event_key": entry["event_key"]} for entry in plan],
        "pull_plan": plan,
        "estimated_request_upper_bound": running,
        "history_target_count": len(targets),
        "history_targets": targets,
    }
    write_json(arguments.output, manifest)

    print(f"{'event':30s} {'conds':>5s} {'est req':>8s} {'cumulative':>11s}")
    for entry in plan:
        print(f"{entry['event_key']:30s} {entry['conditions']:5d} "
              f"{entry['estimated_requests']:8d} {entry['cumulative_requests']:11d}")
    print(f"\ntargets={len(targets)}  estimated requests={running}")
    print(f"wrote {arguments.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
