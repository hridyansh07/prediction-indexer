"""Decode product parameters from a market's own labels.

This is keyed by canonical ``market_type`` rather than by family: a line, a
side, or a score reads the same way whichever grammar named the fixture.  The
esports-only types (``total_maps``, ``map_handicap``, ``map_winner``) and the
traditional-only types (``total_goals``, ``correct_score``) simply select
different blocks below.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from targeter.v2.models import canonical_participant, finite_number


_LINE = re.compile(r"(?<!\d)([-+]?\d+(?:\.\d+)?)(?!\d)")
_PAREN_LINE = re.compile(r"\(\s*([-+]?\d+(?:\.\d+)?)\s*\)")
_OU_LINE = re.compile(r"\bo\s*/\s*u\s*([-+]?\d+(?:\.\d+)?)", re.IGNORECASE)
_SCORE = re.compile(r"(?<!\d)(\d+)\s*[-:]\s*(\d+)(?!\d)")
_MAP_INDEX = re.compile(r"\b(?:game|map)\s*(\d+)\b", re.IGNORECASE)


def market_parameters(
    market_type: str,
    *,
    title: str,
    participants: tuple[str, str],
    outcome_labels: Sequence[str] = (),
    raw: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    raw = raw or {}
    parameters: dict[str, Any] = {}
    searchable = " ".join(
        str(value or "")
        for value in (
            title,
            raw.get("subtitle"),
            raw.get("yes_sub_title"),
            raw.get("groupItemTitle"),
        )
    )

    if market_type in {"spread", "total_goals", "team_total_goals", "total_maps", "map_handicap"}:
        ou_match = _OU_LINE.search(searchable)
        parenthetical_match = _PAREN_LINE.search(searchable)
        match = ou_match or parenthetical_match
        if match is None:
            match = _LINE.search(searchable)
        if match:
            parameters["line"] = float(match.group(1))
            if parenthetical_match is not None and market_type in {"spread", "map_handicap"}:
                parameters["line_style"] = "handicap"

    if market_type in {"moneyline_3way", "spread", "team_total_goals", "series_moneyline", "map_winner"}:
        keys = [canonical_participant(participant) for participant in participants]
        normalized = canonical_participant(searchable)
        hits = [index for index, key in enumerate(keys) if key and key in normalized]
        if not hits:
            label_text = " ".join(map(str, outcome_labels))
            normalized = canonical_participant(label_text)
            hits = [index for index, key in enumerate(keys) if key and key in normalized]
        if len(hits) == 1:
            parameters["side"] = "home" if hits[0] == 0 else "away"
        elif "draw" in normalized or "tie" in normalized:
            parameters["side"] = "draw"

    if market_type == "correct_score":
        match = _SCORE.search(searchable)
        if match:
            parameters["home_goals"] = int(match.group(1))
            parameters["away_goals"] = int(match.group(2))

    if market_type == "map_winner":
        match = _MAP_INDEX.search(searchable)
        if match:
            parameters["map_index"] = int(match.group(1))

    if market_type in {"total_goals", "total_maps"}:
        normalized = searchable.casefold()
        if "under" in normalized:
            parameters["direction"] = "under"
        elif "over" in normalized or "or more" in normalized:
            parameters["direction"] = "over"
        # A discrete claim such as "3 or more goals" is exactly the same mask
        # as Over 2.5, not Over 3.  Store the open threshold used by the mask
        # engine while retaining the published boundary for audit.
        if "or more" in normalized and "line" in parameters:
            published = float(parameters["line"])
            if published.is_integer():
                parameters["published_line"] = int(published)
                parameters["line"] = published - 0.5

    for field_name in ("floor_strike", "cap_strike"):
        if "line" not in parameters and (number := finite_number(raw.get(field_name))) is not None:
            parameters["line"] = number

    return parameters
