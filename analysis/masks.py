"""Compile markets to masks over Omega, then derive relationships by set ops.

Not every market is a function of a given Omega, and pretending otherwise is how
a mask engine produces confident nonsense. Three cases are kept distinct:

``DERIVABLE``
    The market resolves YES exactly on a computable subset of this Omega —
    a moneyline, a totals line, a spread, a map winner.
``NOT_A_FUNCTION``
    The market depends on state finer than Omega records. ``first_team_to_score``
    needs goal *order*, which a final score cannot recover; ``method_of_victory``
    separates extra time from penalties, both of which look like a regulation
    draw from inside the regulation space.
``DIFFERENT_SCOPE``
    The market belongs to another Omega entirely — first-half markets, corners,
    correct score after extra time.

Only ``DERIVABLE`` masks participate in relationship derivation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from analysis.outcome_space import (
    SCOPE_FIRST_HALF,
    SCOPE_REGULATION_FULLTIME,
    SCOPE_SERIES,
    OutcomeSpace,
    reachable_keys,
)


STATUS_DERIVABLE = "DERIVABLE"
STATUS_NOT_A_FUNCTION = "NOT_A_FUNCTION"
STATUS_DIFFERENT_SCOPE = "DIFFERENT_SCOPE"
STATUS_UNSUPPORTED = "UNSUPPORTED"

IDENTITY = "IDENTITY"
IMPLICATION = "IMPLICATION"
REVERSE_IMPLICATION = "REVERSE_IMPLICATION"
MUTUAL_EXCLUSION = "MUTUAL_EXCLUSION"
OVERLAP = "OVERLAP"

# Which Omega each market type is a function of. Types absent here are
# unsupported rather than silently forced into the nearest space.
SCOPE_BY_TYPE: Mapping[str, str] = {
    "correct_score": SCOPE_REGULATION_FULLTIME,
    "moneyline_3way": SCOPE_REGULATION_FULLTIME,
    "total_goals": SCOPE_REGULATION_FULLTIME,
    "spread": SCOPE_REGULATION_FULLTIME,
    "both_teams_to_score": SCOPE_REGULATION_FULLTIME,
    "team_total_goals": SCOPE_REGULATION_FULLTIME,
    "first_half_correct_score": SCOPE_FIRST_HALF,
    "first_half_moneyline_3way": SCOPE_FIRST_HALF,
    "first_half_total_goals": SCOPE_FIRST_HALF,
    "first_half_spread": SCOPE_FIRST_HALF,
    "first_half_both_teams_to_score": SCOPE_FIRST_HALF,
    "series_moneyline": SCOPE_SERIES,
    "map_winner": SCOPE_SERIES,
    "total_maps": SCOPE_SERIES,
    "map_handicap": SCOPE_SERIES,
}

# Types that depend on state no score or sequence records.
NOT_A_FUNCTION_TYPES = frozenset(
    {
        "first_team_to_score",      # needs goal order
        "method_of_victory",        # separates extra time from penalties
        "method_of_finish",         # same
        "advance",                  # decided beyond regulation
        "correct_score_extra_time", # its own space, past regulation
        "total_corners",            # unrelated space
        "team_corners",
    }
)

_LINE = re.compile(r"(\d+(?:\.\d+)?)")
_OVER = re.compile(r"\bover\b", re.I)
_UNDER = re.compile(r"\bunder\b", re.I)


@dataclass(frozen=True)
class Mask:
    market_key: str
    venue: str
    market_type: str
    scope: str
    status: str
    outcome_keys: frozenset[str]
    resolver: str
    note: str | None = None

    @property
    def derivable(self) -> bool:
        return self.status == STATUS_DERIVABLE


def _line_from(text: str) -> float | None:
    match = _LINE.search(str(text or ""))
    return float(match.group(1)) if match else None


def _team_side(label: str, space: OutcomeSpace) -> str | None:
    """Match a label against the event's home/away codes or names.

    Returns ``None`` when the text names *both* sides, which happens with
    handicap titles like "Game Handicap: PARI (-1.5) vs Team Yandex (+1.5)".
    Guessing there silently gives both tokens of one condition the same mask.
    """
    text = str(label or "").casefold()
    hits = [
        side
        for side in ("home", "away")
        if (code := str(space.metadata.get(side) or "").casefold()) and code in text
    ]
    return hits[0] if len(hits) == 1 else None


def _score_mask(
    market: Mapping[str, Any],
    space: OutcomeSpace,
) -> tuple[frozenset[str], str] | None:
    """Resolvers for the score-based scopes."""
    market_type = str(market["market_type"])
    suffix = str(market.get("ticker") or "").rsplit("-", 1)[-1].upper()
    label = str(market.get("yes_label") or "")

    if market_type in ("correct_score", "first_half_correct_score"):
        from analysis.outcome_space import parse_score_ticker

        parsed = parse_score_ticker(market.get("ticker") or "")
        if parsed is None:
            return None
        _, home_goals, _, away_goals = parsed
        key = f"score:{home_goals}-{away_goals}"
        return (frozenset({key}) & space.keys, "exact_score")

    if market_type in ("moneyline_3way", "first_half_moneyline_3way"):
        if suffix in ("TIE", "DRAW") or "tie" in label.casefold() or "draw" in label.casefold():
            return (space.select(lambda p: p["goal_difference"] == 0), "moneyline_draw")
        side = _team_side(suffix, space) or _team_side(label, space)
        if side == "home":
            return (space.select(lambda p: p["goal_difference"] > 0), "moneyline_home")
        if side == "away":
            return (space.select(lambda p: p["goal_difference"] < 0), "moneyline_away")
        return None

    if market_type in ("total_goals", "first_half_total_goals"):
        line = _line_from(label)
        if line is None:
            return None
        if _UNDER.search(label):
            return (space.select(lambda p: p["total_goals"] < line), "total_under")
        if _OVER.search(label):
            return (space.select(lambda p: p["total_goals"] > line), "total_over")
        return None

    if market_type == "both_teams_to_score" or market_type == "first_half_both_teams_to_score":
        return (
            space.select(lambda p: p["home_goals"] >= 1 and p["away_goals"] >= 1),
            "btts",
        )

    if market_type == "team_total_goals":
        line = _line_from(label)
        side = _team_side(label, space)
        if line is None or side is None:
            return None
        field = "home_goals" if side == "home" else "away_goals"
        if _UNDER.search(label):
            return (space.select(lambda p: p[field] < line), f"team_total_under_{side}")
        return (space.select(lambda p: p[field] > line), f"team_total_over_{side}")

    if market_type in ("spread", "first_half_spread"):
        explicit_handicap = market.get("handicap_line")
        explicit_side = market.get("side")
        if isinstance(explicit_handicap, (int, float)) and explicit_side in {"home", "away"}:
            handicap = float(explicit_handicap)
            if explicit_side == "home":
                return (
                    space.select(lambda p: p["goal_difference"] + handicap > 0),
                    "handicap_home",
                )
            return (
                space.select(lambda p: -p["goal_difference"] + handicap > 0),
                "handicap_away",
            )
        line = _line_from(label)
        side = _team_side(suffix, space) or _team_side(label, space)
        if line is None or side is None:
            return None
        # "wins by more than N" is strict.
        if side == "home":
            return (
                space.select(lambda p: p["goal_difference"] > line),
                "spread_home",
            )
        return (space.select(lambda p: -p["goal_difference"] > line), "spread_away")

    return None


def _series_mask(
    market: Mapping[str, Any],
    space: OutcomeSpace,
) -> tuple[frozenset[str], str] | None:
    """Resolvers for the map-sequence scope."""
    market_type = str(market["market_type"])
    title = str(market.get("group_item_title") or market.get("yes_label") or "")
    outcome = str(market.get("outcome_label") or "")
    # A Polymarket condition carries one tradable claim per token, so when an
    # outcome label is supplied it — not the shared title — names the side.
    label = f"{title} {outcome}".strip() if outcome else title

    if market_type == "series_moneyline":
        side = _team_side(outcome or label, space)
        if side is None:
            return None
        return (space.select(lambda p: p["winner_side"] == side), f"series_{side}")

    if market_type == "map_winner":
        index = market.get("map_index")
        if index is None:
            match = re.search(r"(?:game|map)\s*(\d+)", label, re.I)
            if match:
                index = int(match.group(1))
            else:
                match = re.search(r"-(\d+)-", str(market.get("ticker") or ""))
                index = int(match.group(1)) if match else None
        side = _team_side(outcome or label, space)
        if index is None or side is None:
            return None
        want = "H" if side == "home" else "A"
        position = int(index) - 1
        # A map only has a winner in sequences long enough to reach it.
        return (
            space.select(
                lambda p: len(p["sequence"]) > position
                and p["sequence"][position] == want
            ),
            f"map_{index}_{side}",
        )

    if market_type == "total_maps":
        line = _line_from(label)
        if line is None:
            return None
        if _UNDER.search(label):
            return (space.select(lambda p: p["maps_played"] < line), "maps_under")
        return (space.select(lambda p: p["maps_played"] > line), "maps_over")

    if market_type == "map_handicap":
        # The title names both sides — "Game Handicap: PARI (-1.5) vs TY (+1.5)"
        # — so the favoured side comes from the negative handicap, and which of
        # the two tokens we are compiling comes from the outcome label. The
        # underdog token is the complement, not the same mask.
        title = str(market.get("group_item_title") or market.get("yes_label") or "")
        favoured = re.search(r"([^:(]+?)\s*\(\s*-\s*(\d+(?:\.\d+)?)\s*\)", title)
        if not favoured:
            return None
        favoured_side = market.get("favoured_side") or _team_side(favoured.group(1), space)
        line = float(favoured.group(2))
        if favoured_side is None:
            return None
        if favoured_side == "home":
            covered = space.select(lambda p: p["home_wins"] - p["away_wins"] > line)
        else:
            covered = space.select(lambda p: p["away_wins"] - p["home_wins"] > line)

        outcome = str(market.get("outcome_label") or "")
        if not outcome:
            return (covered, f"handicap_{favoured_side}")
        outcome_side = _team_side(outcome, space)
        if outcome_side is None:
            return None
        if outcome_side == favoured_side:
            return (covered, f"handicap_{favoured_side}_covers")
        return (space.keys - covered, f"handicap_{favoured_side}_underdog")

    return None


def compile_mask(market: Mapping[str, Any], space: OutcomeSpace) -> Mask:
    """Compile one market to a mask over ``space``."""
    market_type = str(market.get("market_type") or "")
    market_key = str(
        market.get("target_id") or market.get("ticker") or market.get("market_id")
    )
    venue = str(market.get("venue") or "")
    base = dict(
        market_key=market_key,
        venue=venue,
        market_type=market_type,
        scope=space.scope,
        outcome_keys=frozenset(),
    )

    if market_type in NOT_A_FUNCTION_TYPES:
        return Mask(
            **base,
            status=STATUS_NOT_A_FUNCTION,
            resolver="none",
            note="Depends on state this outcome space does not record.",
        )

    expected_scope = SCOPE_BY_TYPE.get(market_type)
    if expected_scope is None:
        return Mask(**base, status=STATUS_UNSUPPORTED, resolver="none")
    if expected_scope != space.scope:
        return Mask(
            **base,
            status=STATUS_DIFFERENT_SCOPE,
            resolver="none",
            note=f"Belongs to {expected_scope}, not {space.scope}.",
        )

    resolved = (
        _series_mask(market, space)
        if space.scope == SCOPE_SERIES
        else _score_mask(market, space)
    )
    if resolved is None:
        return Mask(
            **base,
            status=STATUS_UNSUPPORTED,
            resolver="none",
            note="No resolver matched this market's labels.",
        )
    keys, resolver = resolved
    return Mask(
        market_key=market_key,
        venue=venue,
        market_type=market_type,
        scope=space.scope,
        status=STATUS_DERIVABLE,
        outcome_keys=keys,
        resolver=resolver,
    )


def compile_masks(
    markets: Sequence[Mapping[str, Any]],
    space: OutcomeSpace,
) -> list[Mask]:
    return [compile_mask(market, space) for market in markets]


def relationship(
    left: Mask,
    right: Mask,
    *,
    universe: frozenset[str] | None = None,
) -> str | None:
    """Classify a pair of masks, optionally restricted to a reachable universe."""
    if not (left.derivable and right.derivable) or left.scope != right.scope:
        return None
    a = left.outcome_keys & universe if universe is not None else left.outcome_keys
    b = right.outcome_keys & universe if universe is not None else right.outcome_keys
    if not a or not b:
        return None
    if a == b:
        return IDENTITY
    if a < b:
        return IMPLICATION
    if b < a:
        return REVERSE_IMPLICATION
    if not (a & b):
        return MUTUAL_EXCLUSION
    return OVERLAP


def derive_relationships(
    masks: Sequence[Mask],
    *,
    universe: frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    """All pairwise relationships among derivable masks."""
    usable = [mask for mask in masks if mask.derivable]
    out: list[dict[str, Any]] = []
    for i, left in enumerate(usable):
        for right in usable[i + 1 :]:
            kind = relationship(left, right, universe=universe)
            if kind is None:
                continue
            out.append(
                {
                    "left": left.market_key,
                    "right": right.market_key,
                    "left_type": left.market_type,
                    "right_type": right.market_type,
                    "scope": left.scope,
                    "relationship": kind,
                }
            )
    return out


def is_partition(
    masks: Sequence[Mask],
    space: OutcomeSpace,
    *,
    universe: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Do these masks tile the (reachable) outcome space exactly once?"""
    scope_universe = universe if universe is not None else space.keys
    usable = [m for m in masks if m.derivable]
    covered: set[str] = set()
    overlaps: set[str] = set()
    for mask in usable:
        keys = mask.outcome_keys & scope_universe
        overlaps |= covered & keys
        covered |= keys
    missing = scope_universe - covered
    return {
        "leg_count": len(usable),
        "is_partition": not overlaps and not missing,
        "overlapping_outcomes": sorted(overlaps),
        "uncovered_outcomes": sorted(missing),
        "coverage": space.coverage,
    }


def state_conditioned_relationships(
    masks: Sequence[Mask],
    space: OutcomeSpace,
    prefixes: Sequence[str],
) -> list[dict[str, Any]]:
    """Relationships recomputed at each series state.

    Transitions never add outcomes, they remove them — which is precisely what
    turns two distinct masks into an identity partway through a series.
    """
    rows: list[dict[str, Any]] = []
    for prefix in prefixes:
        universe = reachable_keys(space, prefix)
        for row in derive_relationships(masks, universe=universe):
            rows.append({**row, "state_prefix": prefix or "(pre-match)"})
    return rows
