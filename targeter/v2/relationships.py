"""Outcome spaces and relationship evidence for canonical sports bundles."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Iterable

from analysis.masks import Mask, compile_mask, relationship
from analysis.outcome_space import (
    COVERAGE_INCOMPLETE,
    SCOPE_REGULATION_FULLTIME,
    SCOPE_SERIES,
    Outcome,
    OutcomeSpace,
    build_series_space,
)
from targeter.v2.models import (
    SUPPORTED_BEST_OF,
    CanonicalEvent,
    CanonicalMarket,
    EventBundle,
    Relationship,
)


@dataclass(frozen=True)
class RelationshipAnalysis:
    relationships: tuple[Relationship, ...]
    masks: tuple[Mask, ...]
    spaces: tuple[OutcomeSpace, ...]
    diagnostics: tuple[str, ...] = ()

    def as_record(self) -> dict[str, object]:
        return {
            "relationships": [item.as_record() for item in self.relationships],
            "masks": [
                {
                    "market_key": mask.market_key,
                    "venue": mask.venue,
                    "market_type": mask.market_type,
                    "scope": mask.scope,
                    "status": mask.status,
                    "resolver": mask.resolver,
                    "outcome_count": len(mask.outcome_keys),
                    "note": mask.note,
                }
                for mask in self.masks
            ],
            "spaces": [
                {
                    "scope": space.scope,
                    "coverage": space.coverage,
                    "outcome_count": len(space.outcomes),
                    "coverage_note": space.coverage_note,
                }
                for space in self.spaces
            ],
            "diagnostics": list(self.diagnostics),
        }


def _score_space(bundle: EventBundle) -> OutcomeSpace:
    numeric_lines = [
        abs(float(line))
        for market in bundle.markets
        for line in (market.parameters.get("line"),)
        if isinstance(line, (int, float)) and math.isfinite(float(line))
    ]
    score_values = [
        int(value)
        for market in bundle.markets
        for value in (
            market.parameters.get("home_goals"),
            market.parameters.get("away_goals"),
        )
        if isinstance(value, int) and not isinstance(value, bool)
    ]
    cap = min(20, max(8, int(max(numeric_lines + score_values, default=2)) + 5))
    outcomes = tuple(
        Outcome(
            key=f"score:{home}-{away}",
            payload={
                "home": bundle.participants[0],
                "away": bundle.participants[1],
                "home_goals": home,
                "away_goals": away,
                "total_goals": home + away,
                "goal_difference": home - away,
            },
        )
        for home in range(cap + 1)
        for away in range(cap + 1)
    )
    return OutcomeSpace(
        event_key=bundle.bundle_id,
        scope=SCOPE_REGULATION_FULLTIME,
        outcomes=outcomes,
        coverage=COVERAGE_INCOMPLETE,
        coverage_note=(
            f"Happy-path score grid 0..{cap} per team. Findings are conditional; "
            "the grid is not an exhaustive locked-basket proof."
        ),
        metadata={
            "home": bundle.participants[0],
            "away": bundle.participants[1],
            "max_home_goals": cap,
            "max_away_goals": cap,
        },
    )


def _event_for(bundle: EventBundle, market: CanonicalMarket) -> CanonicalEvent:
    for event in bundle.events:
        if event.venue == market.venue and event.venue_event_id == market.venue_event_id:
            return event
    raise ValueError(f"market {market.target_id} has no event in bundle {bundle.bundle_id}")


def _side_label(
    bundle: EventBundle,
    event: CanonicalEvent,
    side: object,
) -> str | None:
    if side == "draw":
        return "draw"
    if side == "home":
        participant = event.participants[0]
    elif side == "away":
        participant = event.participants[1]
    else:
        return None
    event_key = bundle.participant_key(participant)
    for candidate in bundle.participants:
        if bundle.participant_key(candidate) == event_key:
            return candidate
    return participant


def _meaningful_labels(market: CanonicalMarket) -> tuple[str, ...]:
    ignored = {"", "yes", "no", "true", "false"}
    labels = tuple(label for label in market.outcome_labels if label.strip().casefold() not in ignored)
    # Multiple outcome labels describe independent tradable claims only when
    # there is a corresponding token for each label. A Kalshi ticker is one
    # binary YES contract even when it publishes yes/no subtitles.
    return labels if len(labels) >= 2 and len(market.subscription_ids) == len(market.outcome_labels) else ()


def _is_single_token_affirmative_claim(market: CanonicalMarket) -> bool:
    """Return whether one tradable token carries this market's whole claim.

    Kalshi and Limitless both publish one subscribable token per side and name
    the side out of band, through a ``side`` or ``direction`` parameter, rather
    than through per-outcome tokens.  Once that side resolves, the single token
    *is* the affirmative claim rather than a fragment of a larger basket.  This
    is a property of the token shape, not of the venue.

    The labels must not themselves name two distinct tradable outcomes.  Kalshi
    repeats one subtitle and Limitless publishes a generic yes/no pair, so
    neither describes a second claim; a market advertising two *different*
    participants behind a single token is malformed, and compiling it would
    expose one leg of a condition as though it were the whole market.
    """
    if len(market.subscription_ids) != 1:
        return False
    ignored = {"", "yes", "no", "true", "false"}
    distinct = {
        label.strip().casefold()
        for label in market.outcome_labels
        if label.strip().casefold() not in ignored
    }
    if len(distinct) > 1:
        return False
    if market.market_type in {"series_moneyline", "map_winner", "map_handicap"}:
        return market.parameters.get("side") in {"home", "away"}
    if market.market_type == "total_maps":
        return market.parameters.get("direction") in {"over", "under"}
    return False


def _bundle_side(bundle: EventBundle, label: str) -> str | None:
    key = bundle.participant_key(label)
    keys = [bundle.participant_key(participant) for participant in bundle.participants]
    hits = [index for index, participant_key in enumerate(keys) if key == participant_key]
    if len(hits) != 1:
        return None
    return "home" if hits[0] == 0 else "away"


def _base_view(market: CanonicalMarket) -> dict[str, Any]:
    return {
        "target_id": market.target_id,
        "venue": market.venue,
        "market_type": market.market_type,
        "group_item_title": market.title,
        "yes_label": market.title,
        "ticker": market.venue_market_id,
        **dict(market.parameters),
    }


def _market_views(bundle: EventBundle, market: CanonicalMarket) -> tuple[dict[str, Any], ...]:
    """Translate venue token shapes into affirmative mask claims."""
    event = _event_for(bundle, market)
    view = _base_view(market)
    line = market.parameters.get("line")
    side = _side_label(bundle, event, market.parameters.get("side"))
    labels = _meaningful_labels(market)
    claims: list[dict[str, Any]] = []

    if market.market_type == "map_winner":
        if "map_index" not in market.parameters:
            return ()

    if market.market_type == "map_handicap":
        if "line" not in market.parameters and not re.findall(r"([^:()]+?)\(\s*([-+]?\d+(?:\.\d+)?)\s*\)", market.title):
            return ()

    if market.market_type == "total_maps":
        if "line" not in market.parameters:
            return ()

    if market.market_type in {"moneyline_3way", "series_moneyline", "map_winner"}:
        candidates = labels or ((side or market.title),)
        for index, label in enumerate(candidates):
            candidate = dict(view)
            candidate["target_id"] = f"{market.target_id}#claim={index}"
            candidate["yes_label"] = str(label)
            candidate["outcome_label"] = str(label)
            if market.market_type == "map_winner" and "map_index" in market.parameters:
                candidate["map_index"] = market.parameters["map_index"]
            claims.append(candidate)
        return tuple(claims)

    if market.market_type in {"total_goals", "total_maps"}:
        if not isinstance(line, (int, float)):
            return ()
        directions = [
            label.casefold().split(maxsplit=1)[0]
            for label in labels
            if label.casefold().split(maxsplit=1)[0] in {"over", "under"}
        ] or [str(market.parameters.get("direction") or "")]
        for index, direction in enumerate(directions):
            if direction not in {"over", "under"}:
                continue
            candidate = dict(view)
            candidate["target_id"] = f"{market.target_id}#claim={index}"
            candidate["yes_label"] = f"{direction} {abs(float(line)):g}"
            candidate["outcome_label"] = direction
            claims.append(candidate)
        return tuple(claims)

    if market.market_type == "spread":
        if not isinstance(line, (int, float)) or side is None:
            return ()
        if labels and market.parameters.get("line_style") == "handicap":
            anchor_side = _bundle_side(bundle, side)
            outcome_sides = tuple(_bundle_side(bundle, label) for label in labels)
            if (
                len(labels) != 2
                or anchor_side not in {"home", "away"}
                or set(outcome_sides) != {"home", "away"}
            ):
                # A multi-token condition is only safe when every aligned token
                # can be oriented. Never expose a partial condition as if it
                # represented the entire vendor market.
                return ()
            for index, (label, outcome_side) in enumerate(zip(labels, outcome_sides)):
                assert outcome_side in {"home", "away"}
                handicap = float(line) if outcome_side == anchor_side else -float(line)
                candidate = dict(view)
                candidate["target_id"] = f"{market.target_id}#claim={index}"
                candidate["yes_label"] = f"{label} ({handicap:+g})"
                candidate["outcome_label"] = label
                candidate["handicap_line"] = handicap
                candidate["side"] = outcome_side
                claims.append(candidate)
            return tuple(claims)
        candidate = dict(view)
        candidate["target_id"] = f"{market.target_id}#claim=0"
        candidate["yes_label"] = f"{side} {abs(float(line)):g}"
        candidate["outcome_label"] = side
        if market.parameters.get("line_style") == "handicap":
            candidate["handicap_line"] = float(line)
            candidate["side"] = "home" if side == bundle.participants[0] else "away"
        return (candidate,)

    if market.market_type == "map_handicap":
        candidates = labels or ((side,) if side is not None else ())
        legs = re.findall(r"([^:()]+?)\(\s*([-+]?\d+(?:\.\d+)?)\s*\)", market.title)
        negative_index = next(
            (index for index, (_name, value) in enumerate(legs) if float(value) < 0),
            None,
        )
        favoured_side = (
            _bundle_side(bundle, labels[negative_index])
            if negative_index is not None and negative_index < len(labels)
            else None
        )
        for index, label in enumerate(candidates):
            candidate = dict(view)
            candidate["target_id"] = f"{market.target_id}#claim={index}"
            candidate["outcome_label"] = str(label)
            if favoured_side is not None:
                candidate["favoured_side"] = favoured_side
            claims.append(candidate)
        return tuple(claims)

    if market.market_type == "correct_score":
        home = market.parameters.get("home_goals")
        away = market.parameters.get("away_goals")
        if not isinstance(home, int) or not isinstance(away, int):
            return ()
        event_order = tuple(_bundle_side(bundle, participant) for participant in event.participants)
        if event_order == ("away", "home"):
            home, away = away, home
        elif event_order != ("home", "away"):
            return ()
        candidate = dict(view)
        candidate["target_id"] = f"{market.target_id}#claim=0"
        candidate["ticker"] = f"H{home}A{away}"
        return (candidate,)

    if market.market_type == "both_teams_to_score":
        candidate = dict(view)
        candidate["target_id"] = f"{market.target_id}#claim=0"
        return (candidate,)

    return ()


def validate_esports_market(
    bundle: EventBundle, market: CanonicalMarket, space: OutcomeSpace
) -> tuple[tuple[Mask, ...], str | None]:
    """Compile an esports product atomically, never exposing a partial condition."""
    if market.market_type not in {"series_moneyline", "map_winner", "total_maps", "map_handicap"}:
        return (), None
    if (
        len(market.outcome_labels) > 1
        and len(market.subscription_ids) != len(market.outcome_labels)
        and not _is_single_token_affirmative_claim(market)
    ):
        return (), "invalid_product_parameters"
    best_of = int(space.metadata["best_of"])
    if market.market_type == "map_winner":
        index = market.parameters.get("map_index")
        if not isinstance(index, int) or isinstance(index, bool) or index <= 0:
            return (), "invalid_product_parameters"
        if index > best_of:
            return (), "product_outside_series_format"
    views = _market_views(bundle, market)
    meaningful = _meaningful_labels(market)
    if meaningful and len(views) != len(meaningful):
        return (), "invalid_product_parameters"
    if not views:
        return (), "invalid_product_parameters"
    try:
        masks = tuple(compile_mask(view, space) for view in views)
    except (KeyError, TypeError, ValueError):
        return (), "invalid_product_parameters"
    if any(not mask.derivable for mask in masks):
        return (), "invalid_product_parameters"
    if market.market_type in {"total_maps", "map_handicap"} and any(
        not mask.outcome_keys or len(mask.outcome_keys) == len(space.outcomes)
        for mask in masks
    ):
        return (), "product_outside_series_format"
    return masks, None


def _spaces(bundle: EventBundle) -> tuple[tuple[OutcomeSpace, ...], tuple[str, ...]]:
    spaces: list[OutcomeSpace] = []
    diagnostics: list[str] = []
    scopes = {market.scope for market in bundle.markets}
    if SCOPE_REGULATION_FULLTIME in scopes:
        spaces.append(_score_space(bundle))
    if SCOPE_SERIES in scopes or any(
        market.market_type in {"map_winner", "total_maps", "map_handicap"}
        for market in bundle.markets
    ):
        best_of = bundle.best_of
        if best_of is None:
            diagnostics.append("series_scope_missing_unambiguous_best_of_format")
        elif best_of in SUPPORTED_BEST_OF:
            spaces.append(
                build_series_space(
                    bundle.bundle_id,
                    best_of=best_of,
                    home=bundle.participants[0],
                    away=bundle.participants[1],
                )
            )
        else:
            diagnostics.append("unsupported_series_format")
    return tuple(spaces), tuple(diagnostics)


def derive_bundle_relationships(
    bundle: EventBundle,
    *,
    excluded_market_ids: Iterable[str] = (),
) -> RelationshipAnalysis:
    excluded = frozenset(excluded_market_ids)
    spaces, diagnostics = _spaces(bundle)
    masks: list[Mask] = []
    relationships: list[Relationship] = []
    for space in spaces:
        scope_masks = [mask
            for market in bundle.markets
            if market.target_id not in excluded
            and (
                market.scope == space.scope
                or (
                    space.scope == SCOPE_SERIES
                    and market.market_type
                    in {"map_winner", "total_maps", "map_handicap"}
                )
            )
            for mask in validate_esports_market(bundle, market, space)[0]
            if bundle.game
        ] + [
            compile_mask(view, space)
            for market in bundle.markets
            if not bundle.game and market.target_id not in excluded
            and market.scope == space.scope
            for view in _market_views(bundle, market)
        ]
        masks.extend(scope_masks)
        usable = [
            mask
            for mask in scope_masks
            if mask.derivable
            and 0 < len(mask.outcome_keys) < len(space.outcomes)
        ]
        for index, left in enumerate(usable):
            for right in usable[index + 1 :]:
                kind = relationship(left, right)
                if kind is None:
                    continue
                relationships.append(
                    Relationship(
                        bundle_id=bundle.bundle_id,
                        left=left.market_key,
                        right=right.market_key,
                        relationship=kind,
                        scope=space.scope,
                        left_venue=left.venue,
                        right_venue=right.venue,
                        coverage=space.coverage,
                    )
                )
    return RelationshipAnalysis(
        relationships=tuple(
            sorted(relationships, key=lambda item: (item.scope, item.left, item.right))
        ),
        masks=tuple(sorted(masks, key=lambda item: item.market_key)),
        spaces=spaces,
        diagnostics=diagnostics,
    )
