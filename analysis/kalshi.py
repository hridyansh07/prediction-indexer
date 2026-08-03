from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from analysis.durable_http import DurableJsonClient
from analysis.storage import (
    in_time_window,
    iso_to_unix_seconds,
    parse_iso8601,
    sha256_text,
    stable_job_id,
    utc_now,
    write_json,
    write_ndjson,
)


KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"


def _iter_cursor_pages(
    client: DurableJsonClient,
    path: str,
    *,
    params: Mapping[str, Any],
    collection_key: str,
) -> Iterable[dict[str, Any]]:
    cursor: str | None = None
    seen_cursors: set[str] = set()

    while True:
        page_params = dict(params)
        if cursor:
            page_params["cursor"] = cursor
        response = client.get_json(
            KALSHI_BASE_URL,
            path,
            params=page_params,
        )
        payload = response.data
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected Kalshi response for {response.url}")

        items = payload.get(collection_key, [])
        if not isinstance(items, list):
            raise ValueError(
                f"Kalshi field {collection_key!r} is not a list for {response.url}"
            )
        for item in items:
            if isinstance(item, dict):
                yield item

        next_cursor = payload.get("cursor")
        if not next_cursor:
            return
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            raise RuntimeError(f"Kalshi returned a repeated cursor for {path}")
        seen_cursors.add(next_cursor)
        cursor = next_cursor


def _market_rules(market: Mapping[str, Any]) -> str:
    primary = str(market.get("rules_primary") or "").strip()
    secondary = str(market.get("rules_secondary") or "").strip()
    return "\n\n".join(part for part in (primary, secondary) if part)


def normalize_kalshi_market(
    event: Mapping[str, Any],
    market: Mapping[str, Any],
) -> dict[str, Any]:
    rules = _market_rules(market)
    return {
        "venue": "kalshi",
        "series_id": event.get("series_ticker"),
        "event_id": event.get("event_ticker"),
        "event_title": event.get("title"),
        "event_subtitle": event.get("sub_title"),
        "market_id": market.get("ticker"),
        "oddpool_market_id": market.get("ticker"),
        "title": market.get("title"),
        "subtitle": market.get("subtitle"),
        "yes_label": market.get("yes_sub_title"),
        "no_label": market.get("no_sub_title"),
        "status": market.get("status"),
        "result": market.get("result"),
        "open_time": market.get("open_time"),
        "close_time": market.get("close_time"),
        "settlement_time": market.get("settlement_ts"),
        "latest_expiration_time": market.get("latest_expiration_time"),
        "rules_primary": market.get("rules_primary"),
        "rules_secondary": market.get("rules_secondary"),
        "rules_hash": sha256_text(rules) if rules else None,
        "volume": market.get("volume_fp"),
        "liquidity": market.get("liquidity_dollars"),
        "mutually_exclusive_event": event.get("mutually_exclusive"),
        "category": event.get("category"),
        "product_metadata": event.get("product_metadata") or {},
    }


def normalize_kalshi_event(
    event: Mapping[str, Any],
    market_ids: list[str],
) -> dict[str, Any]:
    return {
        "venue": "kalshi",
        "series_id": event.get("series_ticker"),
        "event_id": event.get("event_ticker"),
        "title": event.get("title"),
        "subtitle": event.get("sub_title"),
        "category": event.get("category"),
        "mutually_exclusive": event.get("mutually_exclusive"),
        "strike_date": event.get("strike_date"),
        "product_metadata": event.get("product_metadata") or {},
        "market_count": len(market_ids),
        "market_ids": sorted(market_ids),
    }


def _fetch_event_markets(
    client: DurableJsonClient,
    event: Mapping[str, Any],
) -> list[dict[str, Any]]:
    nested = event.get("markets")
    if isinstance(nested, list) and nested:
        return [item for item in nested if isinstance(item, dict)]

    event_ticker = event.get("event_ticker")
    if not event_ticker:
        return []

    current = list(
        _iter_cursor_pages(
            client,
            "/markets",
            params={"event_ticker": event_ticker, "limit": 1000},
            collection_key="markets",
        )
    )
    if current:
        return current

    return list(
        _iter_cursor_pages(
            client,
            "/historical/markets",
            params={"event_ticker": event_ticker, "limit": 1000},
            collection_key="markets",
        )
    )


def discover_kalshi(
    client: DurableJsonClient,
    *,
    data_directory: Path,
    series_tickers: list[str],
    statuses: list[str],
    min_close: str | None = None,
    max_close: str | None = None,
    contains: str | None = None,
) -> dict[str, Any]:
    specification = {
        "venue": "kalshi",
        "series_tickers": sorted(set(series_tickers)),
        "statuses": sorted(set(statuses)),
        "min_close": min_close,
        "max_close": max_close,
        "contains": contains,
    }
    job_id = stable_job_id(specification)
    job_directory = Path(data_directory) / "discovery" / "kalshi" / job_id
    write_json(job_directory / "request.json", specification)

    minimum = parse_iso8601(min_close)
    maximum = parse_iso8601(max_close)
    contains_lower = contains.casefold() if contains else None
    events_by_id: dict[str, dict[str, Any]] = {}

    for series_ticker in specification["series_tickers"]:
        for status in specification["statuses"]:
            params: dict[str, Any] = {
                "series_ticker": series_ticker,
                "status": status,
                "limit": 200,
                "with_nested_markets": True,
            }
            minimum_seconds = iso_to_unix_seconds(min_close)
            if minimum_seconds is not None:
                params["min_close_ts"] = minimum_seconds

            for event in _iter_cursor_pages(
                client,
                "/events",
                params=params,
                collection_key="events",
            ):
                event_id = event.get("event_ticker")
                if event_id:
                    events_by_id[str(event_id)] = event

    normalized_events: list[dict[str, Any]] = []
    normalized_markets: list[dict[str, Any]] = []
    events_without_markets: list[str] = []

    for event_id in sorted(events_by_id):
        event = events_by_id[event_id]
        raw_markets = _fetch_event_markets(client, event)
        selected_markets: list[dict[str, Any]] = []

        for market in raw_markets:
            if not in_time_window(
                market.get("close_time"),
                minimum,
                maximum,
            ):
                continue
            if contains_lower:
                searchable = " ".join(
                    str(value or "")
                    for value in (
                        event.get("title"),
                        event.get("sub_title"),
                        market.get("title"),
                        market.get("subtitle"),
                        market.get("yes_sub_title"),
                        market.get("no_sub_title"),
                        market.get("rules_primary"),
                        market.get("rules_secondary"),
                    )
                ).casefold()
                if contains_lower not in searchable:
                    continue
            selected_markets.append(normalize_kalshi_market(event, market))

        if not raw_markets:
            events_without_markets.append(event_id)
        if not selected_markets:
            continue

        market_ids = [
            str(market["market_id"])
            for market in selected_markets
            if market.get("market_id")
        ]
        normalized_events.append(normalize_kalshi_event(event, market_ids))
        normalized_markets.extend(selected_markets)

    normalized_events.sort(key=lambda item: str(item.get("event_id") or ""))
    normalized_markets.sort(key=lambda item: str(item.get("market_id") or ""))
    write_ndjson(job_directory / "events.ndjson", normalized_events)
    write_ndjson(job_directory / "markets.ndjson", normalized_markets)

    summary = {
        "job_id": job_id,
        "generated_at": utc_now(),
        "specification": specification,
        "event_count": len(normalized_events),
        "market_count": len(normalized_markets),
        "events_without_markets": events_without_markets,
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

