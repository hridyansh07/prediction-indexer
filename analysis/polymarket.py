from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from analysis.durable_http import DurableJsonClient
from analysis.storage import (
    sha256_text,
    stable_job_id,
    utc_now,
    write_json,
    write_ndjson,
)


POLYMARKET_BASE_URL = "https://gamma-api.polymarket.com"
PLACEHOLDER_QUESTION = re.compile(r"^Will [A-E] win\b", re.IGNORECASE)


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


FIFTY_FIFTY = "FIFTY_FIFTY"


def _settled_outcome(
    outcomes: list[str],
    outcome_prices: list[str],
) -> str | None:
    """Read the settled outcome out of ``outcomePrices``.

    A resolved binary market carries ``["1","0"]`` or ``["0","1"]``. An
    ambiguous resolution pays both sides $0.50 and comes back ``["0.5","0.5"]``
    — that is *not* a normal outcome and must stay distinguishable, because a
    basket's payoff under it differs from both a normal settlement and a void.
    Returns ``None`` while the market is still unresolved.
    """
    if not outcome_prices or len(outcome_prices) != len(outcomes):
        return None
    try:
        prices = [float(value) for value in outcome_prices]
    except ValueError:
        return None
    if not prices or any(price not in (0.0, 0.5, 1.0) for price in prices):
        return None
    if all(price == 0.5 for price in prices):
        return FIFTY_FIFTY
    winners = [
        outcomes[index] for index, price in enumerate(prices) if price == 1.0
    ]
    return winners[0] if len(winners) == 1 else None


def normalize_polymarket_market(
    event: Mapping[str, Any],
    market: Mapping[str, Any],
) -> dict[str, Any]:
    token_ids = [str(item) for item in _parse_json_list(market.get("clobTokenIds"))]
    outcomes = [str(item) for item in _parse_json_list(market.get("outcomes"))]
    outcome_tokens = [
        {
            "outcome": outcomes[index] if index < len(outcomes) else None,
            "asset_id": token_id,
        }
        for index, token_id in enumerate(token_ids)
    ]
    rules = str(market.get("description") or event.get("description") or "").strip()
    question = str(market.get("question") or "")
    outcome_prices = [str(item) for item in _parse_json_list(market.get("outcomePrices"))]
    settled_outcome = _settled_outcome(outcomes, outcome_prices)
    warnings: list[str] = []
    if PLACEHOLDER_QUESTION.match(question):
        warnings.append("placeholder_outcome")
    if not market.get("conditionId"):
        warnings.append("missing_condition_id")
    if not token_ids:
        warnings.append("missing_token_ids")

    return {
        "venue": "polymarket",
        "series_id": event.get("seriesSlug") or event.get("series_id"),
        "event_id": str(event.get("id")) if event.get("id") is not None else None,
        "event_slug": event.get("slug"),
        "event_title": event.get("title"),
        "market_id": str(market.get("id")) if market.get("id") is not None else None,
        "market_slug": market.get("slug"),
        "condition_id": market.get("conditionId"),
        "oddpool_market_id": market.get("conditionId"),
        "question": market.get("question"),
        "group_item_title": market.get("groupItemTitle"),
        "token_ids": token_ids,
        "outcome_tokens": outcome_tokens,
        "active": market.get("active"),
        "closed": market.get("closed"),
        "archived": market.get("archived"),
        "accepting_orders": market.get("acceptingOrders"),
        "start_time": market.get("startDate"),
        "end_time": market.get("endDate"),
        "rules": rules or None,
        "rules_hash": sha256_text(rules) if rules else None,
        "resolution_source": market.get("resolutionSource"),
        "outcome_prices": outcome_prices,
        "settled_outcome": settled_outcome,
        "uma_resolution_status": market.get("umaResolutionStatus"),
        "resolved_by": market.get("resolvedBy"),
        "closed_time": market.get("closedTime"),
        "automatically_resolved": market.get("automaticallyResolved"),
        "volume": market.get("volumeNum", market.get("volume")),
        "liquidity": market.get("liquidityNum", market.get("liquidity")),
        "warnings": warnings,
    }


def normalize_polymarket_event(
    event: Mapping[str, Any],
    market_ids: list[str],
) -> dict[str, Any]:
    return {
        "venue": "polymarket",
        "event_id": str(event.get("id")) if event.get("id") is not None else None,
        "slug": event.get("slug"),
        "title": event.get("title"),
        "description": event.get("description"),
        "active": event.get("active"),
        "closed": event.get("closed"),
        "archived": event.get("archived"),
        "start_time": event.get("startDate"),
        "end_time": event.get("endDate"),
        "market_count": len(market_ids),
        "market_ids": sorted(market_ids),
    }


def _search_event_ids(
    client: DurableJsonClient,
    query: str,
    *,
    max_pages: int,
) -> set[str]:
    event_ids: set[str] = set()
    page_number = 1

    while page_number <= max_pages:
        response = client.get_json(
            POLYMARKET_BASE_URL,
            "/public-search",
            params={
                "q": query,
                "limit_per_type": 50,
                "page": page_number,
                "keep_closed_markets": 1,
            },
        )
        payload = response.data
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected Polymarket response for {response.url}")

        for event in payload.get("events", []):
            if isinstance(event, dict) and event.get("id") is not None:
                event_ids.add(str(event["id"]))

        pagination = payload.get("pagination") or {}
        if not isinstance(pagination, dict) or not pagination.get("hasMore"):
            break
        page_number += 1

    return event_ids


def discover_polymarket(
    client: DurableJsonClient,
    *,
    data_directory: Path,
    query: str | None = None,
    event_ids: list[str] | None = None,
    max_search_pages: int = 20,
) -> dict[str, Any]:
    requested_event_ids = sorted(set(event_ids or []))
    specification = {
        "venue": "polymarket",
        "query": query,
        "event_ids": requested_event_ids,
        "max_search_pages": max_search_pages,
    }
    job_id = stable_job_id(specification)
    job_directory = Path(data_directory) / "discovery" / "polymarket" / job_id
    write_json(job_directory / "request.json", specification)

    selected_event_ids = set(requested_event_ids)
    if query:
        selected_event_ids.update(
            _search_event_ids(
                client,
                query,
                max_pages=max_search_pages,
            )
        )
    if not selected_event_ids:
        raise ValueError("Provide a Polymarket query or at least one event ID")

    normalized_events: list[dict[str, Any]] = []
    normalized_markets: list[dict[str, Any]] = []

    for event_id in sorted(selected_event_ids):
        response = client.get_json(
            POLYMARKET_BASE_URL,
            f"/events/{event_id}",
        )
        event = response.data
        if not isinstance(event, dict):
            raise ValueError(f"Unexpected Polymarket event response for {response.url}")

        markets = [
            normalize_polymarket_market(event, market)
            for market in event.get("markets", [])
            if isinstance(market, dict)
        ]
        market_ids = [
            str(market["market_id"])
            for market in markets
            if market.get("market_id")
        ]
        normalized_events.append(normalize_polymarket_event(event, market_ids))
        normalized_markets.extend(markets)

    normalized_events.sort(key=lambda item: str(item.get("event_id") or ""))
    normalized_markets.sort(key=lambda item: str(item.get("market_id") or ""))
    write_ndjson(job_directory / "events.ndjson", normalized_events)
    write_ndjson(job_directory / "markets.ndjson", normalized_markets)

    warning_counts: dict[str, int] = {}
    for market in normalized_markets:
        for warning in market["warnings"]:
            warning_counts[warning] = warning_counts.get(warning, 0) + 1

    summary = {
        "job_id": job_id,
        "generated_at": utc_now(),
        "specification": specification,
        "event_count": len(normalized_events),
        "market_count": len(normalized_markets),
        "warning_counts": warning_counts,
        "http": {
            "cache_hits": client.cache_hits,
            "network_requests": client.network_requests,
        },
        "files": {
            "events": str(job_directory / "events.ndjson"),
            "markets": str(job_directory / "markets.ndjson"),
            "request": str(job_directory / "request.json"),
        },
    }
    write_json(job_directory / "run.json", summary)
    return summary

