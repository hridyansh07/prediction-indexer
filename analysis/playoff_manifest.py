from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from analysis.storage import utc_now


MATCH_FORMAT = re.compile(r"\((BO\d+)\)", re.IGNORECASE)
VERSUS = re.compile(r"\s+vs\.?\s+", re.IGNORECASE)


def _team_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def extract_team_pair(title: str) -> tuple[str, str]:
    match_title = title.split(":", 1)[-1] if title.lower().startswith("dota 2:") else title
    match_title = MATCH_FORMAT.split(match_title, maxsplit=1)[0]
    match_title = match_title.split(" - ", 1)[0].strip()
    teams = VERSUS.split(match_title, maxsplit=1)
    if len(teams) != 2:
        raise ValueError(f"Could not parse two teams from {title!r}")
    return tuple(sorted((_team_key(teams[0]), _team_key(teams[1]))))


def _moneyline_market(
    event: Mapping[str, Any],
    markets: Iterable[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    event_id = str(event.get("event_id") or "")
    event_slug = str(event.get("slug") or "")
    candidates = [
        market
        for market in markets
        if str(market.get("event_id") or "") == event_id
        and (
            market.get("market_slug") == event_slug
            or market.get("group_item_title") == "Match Winner"
        )
    ]
    exact = [
        market
        for market in candidates
        if market.get("market_slug") == event_slug
        and market.get("group_item_title") == "Match Winner"
    ]
    selected = exact or candidates
    if len(selected) != 1:
        return None
    return selected[0]


def build_playoff_manifest(
    kalshi_events: Iterable[Mapping[str, Any]],
    kalshi_markets: Iterable[Mapping[str, Any]],
    polymarket_events: Iterable[Mapping[str, Any]],
    polymarket_markets: Iterable[Mapping[str, Any]],
    *,
    title_contains: str = "Esports World Cup Playoffs",
) -> dict[str, Any]:
    kalshi_event_rows = list(kalshi_events)
    kalshi_market_rows = list(kalshi_markets)
    polymarket_event_rows = list(polymarket_events)
    polymarket_market_rows = list(polymarket_markets)

    kalshi_by_pair: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for event in kalshi_event_rows:
        title = str(event.get("title") or "")
        try:
            pair = extract_team_pair(title)
        except ValueError:
            continue
        kalshi_by_pair.setdefault(pair, []).append(event)

    kalshi_markets_by_event: dict[str, list[Mapping[str, Any]]] = {}
    for market in kalshi_market_rows:
        event_id = str(market.get("event_id") or "")
        kalshi_markets_by_event.setdefault(event_id, []).append(market)

    matches: list[dict[str, Any]] = []
    unmatched_polymarket: list[dict[str, Any]] = []

    for poly_event in polymarket_event_rows:
        title = str(poly_event.get("title") or "")
        if title_contains.casefold() not in title.casefold():
            continue
        if not title.casefold().startswith("dota 2:"):
            continue

        moneyline = _moneyline_market(poly_event, polymarket_market_rows)
        try:
            pair = extract_team_pair(title)
        except ValueError:
            pair = ("", "")

        kalshi_candidates = kalshi_by_pair.get(pair, [])
        if moneyline is None or len(kalshi_candidates) != 1:
            unmatched_polymarket.append(
                {
                    "event_id": poly_event.get("event_id"),
                    "slug": poly_event.get("slug"),
                    "title": title,
                    "reason": (
                        "moneyline_not_unique"
                        if moneyline is None
                        else "kalshi_event_not_unique"
                    ),
                    "kalshi_candidate_count": len(kalshi_candidates),
                }
            )
            continue

        kalshi_event = kalshi_candidates[0]
        kalshi_event_id = str(kalshi_event.get("event_id"))
        event_key = str(poly_event.get("slug"))
        event_kalshi_markets = sorted(
            kalshi_markets_by_event.get(kalshi_event_id, []),
            key=lambda row: str(row.get("market_id") or ""),
        )
        format_match = MATCH_FORMAT.search(title)

        targets: list[dict[str, Any]] = []
        for market in event_kalshi_markets:
            targets.append(
                {
                    "target_id": f"kalshi:{market.get('market_id')}",
                    "event_key": event_key,
                    "venue": "kalshi",
                    "market_id": market.get("oddpool_market_id"),
                    "outcome": market.get("yes_label"),
                    "start_time": market.get("open_time"),
                    "end_time": market.get("close_time"),
                }
            )
        targets.append(
            {
                "target_id": f"polymarket:{moneyline.get('condition_id')}",
                "event_key": event_key,
                "venue": "polymarket",
                "market_id": moneyline.get("oddpool_market_id"),
                "asset_id": None,
                "outcome_tokens": moneyline.get("outcome_tokens") or [],
                "start_time": moneyline.get("start_time"),
                "end_time": moneyline.get("end_time"),
            }
        )

        matches.append(
            {
                "event_key": event_key,
                "title": title,
                "tournament": title_contains,
                "match_format": format_match.group(1).upper() if format_match else None,
                "basis_class": "candidate_near_identity",
                "review_status": "resolution_spec_review_required",
                "unverified_axes": [
                    "source_identifier",
                    "measurement_window",
                    "void_policy",
                ],
                "kalshi": {
                    "event_id": kalshi_event_id,
                    "title": kalshi_event.get("title"),
                    "mutually_exclusive": kalshi_event.get("mutually_exclusive"),
                    "market_ids": [
                        market.get("market_id") for market in event_kalshi_markets
                    ],
                    "rules_hashes": [
                        market.get("rules_hash") for market in event_kalshi_markets
                    ],
                },
                "polymarket": {
                    "event_id": poly_event.get("event_id"),
                    "event_slug": poly_event.get("slug"),
                    "market_id": moneyline.get("market_id"),
                    "condition_id": moneyline.get("condition_id"),
                    "outcome_tokens": moneyline.get("outcome_tokens") or [],
                    "rules_hash": moneyline.get("rules_hash"),
                    "resolution_source": moneyline.get("resolution_source"),
                },
                "history_targets": targets,
            }
        )

    matches.sort(key=lambda row: row["event_key"])
    history_targets = [
        target for match in matches for target in match["history_targets"]
    ]
    return {
        "version": 1,
        "generated_at": utc_now(),
        "scope": {
            "sport": "Dota 2",
            "title_contains": title_contains,
            "polymarket_market_type": "Match Winner",
        },
        "match_count": len(matches),
        "history_target_count": len(history_targets),
        "matches": matches,
        "history_targets": history_targets,
        "unmatched_polymarket_events": unmatched_polymarket,
    }
