"""Discovery and manifest assembly for sibling market sets.

A *sibling set* is every market on a venue that resolves against the same
underlying event, so their outcome masks live in one enumerable space. The
partition work so far used match-winner moneylines only, which admits no
non-trivial masks; these helpers collect the surrounding market types
(correct score, totals, spreads, map winners, ...) so the mask engine has
structure to derive relationships from.

Kalshi groups sibling types into separate *series* that share a fixture code
inside the event ticker (``KXWCSCORE-26JUL19ESPARG-...`` alongside
``KXWCGAME-26JUL19ESPARG-...``). Polymarket puts them in one event, separated
by ``groupItemTitle``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from analysis.durable_http import DurableJsonClient
from analysis.kalshi import (
    _iter_cursor_pages,
    normalize_kalshi_event,
    normalize_kalshi_market,
)
from analysis.polymarket import (
    POLYMARKET_BASE_URL,
    normalize_polymarket_event,
    normalize_polymarket_market,
)
from analysis.storage import (
    stable_job_id,
    utc_now,
    write_json,
    write_ndjson,
)


# Kalshi series whose markets resolve against a single fixture's outcome space.
# Player props (goalscorer, starters, assists) are deliberately excluded: they
# resolve on player events, not on the match outcome, so they cannot share a
# mask with the markets below.
WORLD_CUP_STRUCTURAL_SERIES: tuple[str, ...] = (
    "KXWCGAME",
    "KXWCSCORE",
    "KXWCSCOREET",
    "KXWCTOTAL",
    "KXWCSPREAD",
    "KXWCMOV",
    "KXWCMOF",
    "KXWCBTTS",
    "KXWCTEAMTOTAL",
    "KXWCFTTS",
    "KXWCADVANCE",
    "KXWC1H",
    "KXWC1HSCORE",
    "KXWC1HTOTAL",
    "KXWC1HSPREAD",
    "KXWC1HBTTS",
    "KXWC2H",
    "KXWC2HTOTAL",
    "KXWC2HSPREAD",
    "KXWC2HBTTS",
    "KXWCCORNERS",
    "KXWCTCORNERS",
)

DOTA_SIBLING_SERIES: tuple[str, ...] = ("KXDOTA2GAME", "KXDOTA2MAP")

KALSHI_MARKET_TYPES: Mapping[str, str] = {
    "KXWCGAME": "moneyline_3way",
    "KXWCSCORE": "correct_score",
    "KXWCSCOREET": "correct_score_extra_time",
    "KXWCTOTAL": "total_goals",
    "KXWCSPREAD": "spread",
    "KXWCMOV": "method_of_victory",
    "KXWCMOF": "method_of_finish",
    "KXWCBTTS": "both_teams_to_score",
    "KXWCTEAMTOTAL": "team_total_goals",
    "KXWCFTTS": "first_team_to_score",
    "KXWCADVANCE": "advance",
    "KXWC1H": "first_half_moneyline_3way",
    "KXWC1HSCORE": "first_half_correct_score",
    "KXWC1HTOTAL": "first_half_total_goals",
    "KXWC1HSPREAD": "first_half_spread",
    "KXWC1HBTTS": "first_half_both_teams_to_score",
    "KXWC2H": "second_half_moneyline_3way",
    "KXWC2HTOTAL": "second_half_total_goals",
    "KXWC2HSPREAD": "second_half_spread",
    "KXWC2HBTTS": "second_half_both_teams_to_score",
    "KXWCCORNERS": "total_corners",
    "KXWCTCORNERS": "team_corners",
    "KXDOTA2GAME": "series_moneyline",
    "KXDOTA2MAP": "map_winner",
}

_POLYMARKET_MAP_WINNER = re.compile(r"^Game\s+(\d+)\s+Winner", re.IGNORECASE)
_POLYMARKET_TOTAL_MAPS = re.compile(r"^O/U\s+([\d.]+)\s+Games", re.IGNORECASE)
_POLYMARKET_HANDICAP = re.compile(r"^Game\s+Handicap", re.IGNORECASE)
_POLYMARKET_DRAW = re.compile(r"^Draw\b", re.IGNORECASE)


def classify_kalshi_market_type(series_ticker: str | None) -> str:
    return KALSHI_MARKET_TYPES.get(str(series_ticker or "").upper(), "other")


def classify_polymarket_market_type(
    group_item_title: str | None,
    *,
    moneyline_outcomes: Iterable[str] = (),
) -> str:
    """Classify a Polymarket child market from its group label.

    ``moneyline_outcomes`` names the competitor labels for the fixture so a
    three-way soccer event's team legs and draw leg classify together.
    """
    title = str(group_item_title or "").strip()
    if not title:
        return "other"
    if title.lower().startswith("match winner"):
        return "series_moneyline"
    if _POLYMARKET_MAP_WINNER.match(title):
        return "map_winner"
    if _POLYMARKET_TOTAL_MAPS.match(title):
        return "total_maps"
    if _POLYMARKET_HANDICAP.match(title):
        return "map_handicap"
    if _POLYMARKET_DRAW.match(title):
        return "moneyline_3way"
    normalized = {str(name).casefold() for name in moneyline_outcomes}
    if title.casefold() in normalized:
        return "moneyline_3way"
    return "other"


def _event_matches_fixture(event_ticker: str, fixture_codes: Mapping[str, str]) -> str | None:
    upper = event_ticker.upper()
    for event_key, code in fixture_codes.items():
        if code.upper() in upper:
            return event_key
    return None


def discover_kalshi_siblings(
    client: DurableJsonClient,
    *,
    data_directory: Path,
    dataset_name: str,
    series_tickers: Sequence[str],
    fixture_codes: Mapping[str, str],
    statuses: Sequence[str] = ("settled",),
) -> dict[str, Any]:
    """Collect every Kalshi market in ``series_tickers`` for the named fixtures."""
    specification = {
        "venue": "kalshi",
        "mode": "sibling_fixtures",
        "dataset_name": dataset_name,
        "series_tickers": sorted(set(series_tickers)),
        "fixture_codes": dict(sorted(fixture_codes.items())),
        "statuses": sorted(set(statuses)),
    }
    job_id = stable_job_id(specification)
    job_directory = Path(data_directory) / "discovery" / "kalshi" / job_id
    write_json(job_directory / "request.json", specification)

    normalized_events: list[dict[str, Any]] = []
    normalized_markets: list[dict[str, Any]] = []
    series_without_fixture: list[str] = []

    for series_ticker in specification["series_tickers"]:
        matched_any = False
        seen_events: dict[str, dict[str, Any]] = {}
        for status in specification["statuses"]:
            for event in _iter_cursor_pages(
                client,
                "/events",
                params={
                    "series_ticker": series_ticker,
                    "status": status,
                    "limit": 200,
                    "with_nested_markets": True,
                },
                collection_key="events",
            ):
                event_ticker = str(event.get("event_ticker") or "")
                if event_ticker:
                    seen_events[event_ticker] = event

        for event_ticker in sorted(seen_events):
            event = seen_events[event_ticker]
            event_key = _event_matches_fixture(event_ticker, fixture_codes)
            if event_key is None:
                continue
            matched_any = True
            markets = [
                item for item in (event.get("markets") or []) if isinstance(item, dict)
            ]
            selected: list[dict[str, Any]] = []
            for market in markets:
                normalized = normalize_kalshi_market(event, market)
                normalized["event_key"] = event_key
                normalized["market_type"] = classify_kalshi_market_type(series_ticker)
                selected.append(normalized)
            if not selected:
                continue
            normalized_event = normalize_kalshi_event(
                event,
                [str(item["market_id"]) for item in selected if item.get("market_id")],
            )
            normalized_event["event_key"] = event_key
            normalized_event["market_type"] = classify_kalshi_market_type(series_ticker)
            normalized_events.append(normalized_event)
            normalized_markets.extend(selected)
        if not matched_any:
            series_without_fixture.append(series_ticker)

    normalized_events.sort(key=lambda item: str(item.get("event_id") or ""))
    normalized_markets.sort(key=lambda item: str(item.get("market_id") or ""))
    write_ndjson(job_directory / "events.ndjson", normalized_events)
    write_ndjson(job_directory / "markets.ndjson", normalized_markets)

    summary = {
        "job_id": job_id,
        "generated_at": utc_now(),
        "specification": specification,
        "job_directory": str(job_directory),
        "event_count": len(normalized_events),
        "market_count": len(normalized_markets),
        "series_without_fixture": sorted(series_without_fixture),
        "http": {
            "cache_hits": client.cache_hits,
            "network_requests": client.network_requests,
        },
    }
    write_json(job_directory / "run.json", summary)
    return summary


def discover_polymarket_siblings(
    client: DurableJsonClient,
    *,
    data_directory: Path,
    dataset_name: str,
    event_slugs: Mapping[str, str],
    moneyline_outcomes: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Collect every Polymarket child market for the named event slugs."""
    specification = {
        "venue": "polymarket",
        "mode": "sibling_fixtures",
        "dataset_name": dataset_name,
        "event_slugs": dict(sorted(event_slugs.items())),
    }
    job_id = stable_job_id(specification)
    job_directory = Path(data_directory) / "discovery" / "polymarket" / job_id
    write_json(job_directory / "request.json", specification)

    outcomes_by_key = moneyline_outcomes or {}
    normalized_events: list[dict[str, Any]] = []
    normalized_markets: list[dict[str, Any]] = []
    missing_slugs: list[str] = []

    for event_key in sorted(event_slugs):
        slug = event_slugs[event_key]
        response = client.get_json(POLYMARKET_BASE_URL, "/events", params={"slug": slug})
        payload = response.data
        events = payload if isinstance(payload, list) else []
        if not events or not isinstance(events[0], dict):
            missing_slugs.append(slug)
            continue
        event = events[0]
        markets: list[dict[str, Any]] = []
        for market in event.get("markets") or []:
            if not isinstance(market, dict):
                continue
            normalized = normalize_polymarket_market(event, market)
            normalized["event_key"] = event_key
            normalized["market_type"] = classify_polymarket_market_type(
                market.get("groupItemTitle"),
                moneyline_outcomes=outcomes_by_key.get(event_key, ()),
            )
            markets.append(normalized)
        normalized_event = normalize_polymarket_event(
            event,
            [str(item["market_id"]) for item in markets if item.get("market_id")],
        )
        normalized_event["event_key"] = event_key
        normalized_events.append(normalized_event)
        normalized_markets.extend(markets)

    normalized_events.sort(key=lambda item: str(item.get("event_id") or ""))
    normalized_markets.sort(key=lambda item: str(item.get("market_id") or ""))
    write_ndjson(job_directory / "events.ndjson", normalized_events)
    write_ndjson(job_directory / "markets.ndjson", normalized_markets)

    type_counts: dict[str, int] = {}
    for market in normalized_markets:
        key = str(market["market_type"])
        type_counts[key] = type_counts.get(key, 0) + 1

    summary = {
        "job_id": job_id,
        "generated_at": utc_now(),
        "specification": specification,
        "job_directory": str(job_directory),
        "event_count": len(normalized_events),
        "market_count": len(normalized_markets),
        "market_type_counts": dict(sorted(type_counts.items())),
        "missing_slugs": sorted(missing_slugs),
        "http": {
            "cache_hits": client.cache_hits,
            "network_requests": client.network_requests,
        },
    }
    write_json(job_directory / "run.json", summary)
    return summary


def build_sibling_manifest(
    *,
    dataset_name: str,
    kalshi_markets: Iterable[Mapping[str, Any]] = (),
    polymarket_markets: Iterable[Mapping[str, Any]] = (),
    include_market_types: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Assemble history targets for the free Kalshi and Polymarket pullers."""
    allowed = set(include_market_types) if include_market_types else None
    targets: list[dict[str, Any]] = []
    events: dict[str, dict[str, Any]] = {}

    for market in kalshi_markets:
        market_type = str(market.get("market_type") or "other")
        if allowed is not None and market_type not in allowed:
            continue
        ticker = market.get("market_id")
        event_key = market.get("event_key")
        if not ticker or not event_key:
            continue
        targets.append(
            {
                "venue": "kalshi",
                "target_id": f"kalshi:{ticker}",
                "ticker": str(ticker),
                "series_ticker": market.get("series_id"),
                "event_key": str(event_key),
                "event_id": market.get("event_id"),
                "market_type": market_type,
                "title": market.get("title"),
                "yes_label": market.get("yes_label"),
                "no_label": market.get("no_label"),
                "open_time": market.get("open_time"),
                "close_time": market.get("close_time"),
                "result": market.get("result"),
                "rules_hash": market.get("rules_hash"),
                "mutually_exclusive_event": market.get("mutually_exclusive_event"),
                "source": "kalshi_candlesticks",
                "depth_available": False,
            }
        )
        bucket = events.setdefault(
            str(event_key),
            {"event_key": str(event_key), "kalshi": {}, "polymarket": {}},
        )
        bucket["kalshi"].setdefault(market_type, []).append(str(ticker))

    for market in polymarket_markets:
        market_type = str(market.get("market_type") or "other")
        if allowed is not None and market_type not in allowed:
            continue
        condition_id = market.get("condition_id")
        event_key = market.get("event_key")
        if not condition_id or not event_key:
            continue
        targets.append(
            {
                "venue": "polymarket",
                "target_id": f"polymarket:{condition_id}",
                "market_id": str(condition_id),
                "event_key": str(event_key),
                "event_id": market.get("event_id"),
                "market_type": market_type,
                "question": market.get("question"),
                "group_item_title": market.get("group_item_title"),
                "outcome_tokens": market.get("outcome_tokens") or [],
                "start_time": market.get("start_time"),
                "end_time": market.get("end_time"),
                "rules_hash": market.get("rules_hash"),
                "resolution_source": market.get("resolution_source"),
                "source": "polymarket_clob_prices",
                "depth_available": False,
            }
        )
        bucket = events.setdefault(
            str(event_key),
            {"event_key": str(event_key), "kalshi": {}, "polymarket": {}},
        )
        bucket["polymarket"].setdefault(market_type, []).append(str(condition_id))

    targets.sort(key=lambda item: (item["venue"], item["target_id"]))
    for bucket in events.values():
        for venue_types in (bucket["kalshi"], bucket["polymarket"]):
            for key in venue_types:
                venue_types[key] = sorted(venue_types[key])

    type_counts: dict[str, int] = {}
    for target in targets:
        key = f"{target['venue']}:{target['market_type']}"
        type_counts[key] = type_counts.get(key, 0) + 1

    return {
        "version": 1,
        "dataset_name": dataset_name,
        "generated_at": utc_now(),
        "scope": {
            "included_market_types": sorted(allowed) if allowed else None,
            "depth_source": None,
            "note": (
                "Kalshi candlesticks and Polymarket CLOB prices are top-of-book "
                "or price-only. No depth ladders are available from these sources."
            ),
        },
        "event_count": len(events),
        "events": [events[key] for key in sorted(events)],
        "history_target_count": len(targets),
        "market_type_counts": dict(sorted(type_counts.items())),
        "history_targets": targets,
    }
