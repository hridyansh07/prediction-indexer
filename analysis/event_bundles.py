from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from analysis.storage import utc_now, write_json, write_ndjson


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number} is not a JSON object")
            rows.append(value)
    return rows


def _polymarket_outcome(market: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "label": market.get("group_item_title") or market.get("question"),
        "market_id": market.get("market_id"),
        "condition_id": market.get("condition_id"),
        "oddpool_market_id": market.get("oddpool_market_id"),
        "question": market.get("question"),
        "token_ids": market.get("token_ids") or [],
        "outcome_tokens": market.get("outcome_tokens") or [],
        "rules_hash": market.get("rules_hash"),
        "active": market.get("active"),
        "closed": market.get("closed"),
        "accepting_orders": market.get("accepting_orders"),
        "volume": market.get("volume"),
        "liquidity": market.get("liquidity"),
        "warnings": market.get("warnings") or [],
    }


def _kalshi_outcome(market: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "label": market.get("yes_label") or market.get("title"),
        "market_id": market.get("market_id"),
        "oddpool_market_id": market.get("oddpool_market_id"),
        "question": market.get("title"),
        "yes_label": market.get("yes_label"),
        "no_label": market.get("no_label"),
        "status": market.get("status"),
        "result": market.get("result"),
        "rules_hash": market.get("rules_hash"),
        "volume": market.get("volume"),
        "liquidity": market.get("liquidity"),
    }


def build_event_bundles(
    events: Iterable[Mapping[str, Any]],
    markets: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    markets_by_event: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for market in markets:
        event_id = market.get("event_id")
        if event_id is not None:
            markets_by_event[str(event_id)].append(market)

    bundles: list[dict[str, Any]] = []
    included_event_ids: set[str] = set()

    for event in events:
        event_id_value = event.get("event_id")
        if event_id_value is None:
            continue
        event_id = str(event_id_value)
        venue = str(event.get("venue") or "")
        child_markets = markets_by_event.get(event_id, [])
        included_event_ids.add(event_id)

        if venue == "kalshi":
            structure = (
                "kalshi_mutually_exclusive_market_group"
                if event.get("mutually_exclusive")
                else "kalshi_event"
            )
            outcomes = [_kalshi_outcome(market) for market in child_markets]
            partition_status = (
                "venue_declared_mutually_exclusive"
                if event.get("mutually_exclusive") and len(outcomes) > 1
                else "not_established"
            )
            event_title = event.get("title")
            event_slug = None
        elif venue == "polymarket":
            structure = (
                "polymarket_event_with_binary_conditions"
                if len(child_markets) > 1
                else "polymarket_single_condition_event"
            )
            outcomes = [_polymarket_outcome(market) for market in child_markets]
            partition_status = (
                "candidate_requires_rules_verification"
                if len(outcomes) > 1
                else "not_applicable"
            )
            event_title = event.get("title")
            event_slug = event.get("slug")
        else:
            structure = "unknown"
            outcomes = [dict(market) for market in child_markets]
            partition_status = "not_established"
            event_title = event.get("title")
            event_slug = event.get("slug")

        outcomes.sort(
            key=lambda item: (
                str(item.get("label") or ""),
                str(item.get("market_id") or ""),
            )
        )
        warning_counts: dict[str, int] = {}
        for outcome in outcomes:
            for warning in outcome.get("warnings") or []:
                warning_counts[warning] = warning_counts.get(warning, 0) + 1

        bundles.append(
            {
                "venue": venue,
                "event_id": event_id,
                "event_slug": event_slug,
                "title": event_title,
                "structure": structure,
                "partition_status": partition_status,
                "mutually_exclusive": event.get("mutually_exclusive"),
                "event_market_count": event.get("market_count"),
                "bundled_market_count": len(outcomes),
                "outcomes": outcomes,
                "warning_counts": warning_counts,
            }
        )

    bundles.sort(key=lambda item: (item["venue"], item["event_id"]))
    orphan_event_ids = sorted(set(markets_by_event) - included_event_ids)
    return bundles, orphan_event_ids


def write_event_bundle_files(
    events_path: Path,
    markets_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    events = read_ndjson(events_path)
    markets = read_ndjson(markets_path)
    bundles, orphan_event_ids = build_event_bundles(events, markets)
    write_ndjson(output_path, bundles)

    summary = {
        "generated_at": utc_now(),
        "events_path": str(events_path),
        "markets_path": str(markets_path),
        "output_path": str(output_path),
        "bundle_count": len(bundles),
        "bundled_market_count": sum(
            int(bundle["bundled_market_count"]) for bundle in bundles
        ),
        "orphan_event_ids": orphan_event_ids,
    }
    write_json(output_path.with_suffix(".summary.json"), summary)
    return summary

