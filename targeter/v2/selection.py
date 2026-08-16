"""Rank mature multi-venue bundles and produce a phase-5 shadow selection."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping

from targeter.v2.continuity import ContinuityBundle, TerminalState
from targeter.v2.models import (
    SUPPORTED_BEST_OF,
    SUPPORTED_VENUES,
    CatalogSnapshot,
    CanonicalMarket,
    EventBundle,
    format_observation,
    isoformat,
)
from targeter.v2.matching import MatchRejection, match_events
from targeter.v2.relationships import RelationshipAnalysis, derive_bundle_relationships, validate_esports_market
from targeter.v2.registry import Strategy
from targeter.v2.rules import RuleAssessment, assess_rules


@dataclass(frozen=True)
class Candidate:
    bundle: EventBundle
    eligible_markets: tuple[CanonicalMarket, ...]
    relationships: RelationshipAnalysis
    rules: RuleAssessment
    score: float
    score_components: Mapping[str, float]
    capture_start_at: datetime
    eligible: bool
    rejection_reasons: tuple[str, ...]
    market_exclusions: Mapping[str, tuple[str, ...]]
    combined_moneyline_volume_usd: float
    moneyline_volume_usd_by_venue: Mapping[str, float]
    moneyline_volume_usd_coverage: Mapping[str, Mapping[str, int]]
    minimum_moneyline_volume_usd: float

    def as_record(self) -> dict[str, object]:
        return {
            **self.bundle.as_record(),
            **format_observation(self.bundle.observed_formats),
            "capture_start_at": isoformat(self.capture_start_at),
            "score": self.score,
            "score_components": dict(self.score_components),
            "eligible": self.eligible,
            "event_status": "ELIGIBLE" if self.eligible else "REJECTED",
            "rejection_reasons": list(self.rejection_reasons),
            "admission": {
                "combined_moneyline_volume_usd": self.combined_moneyline_volume_usd,
                "minimum_moneyline_volume_usd": self.minimum_moneyline_volume_usd,
                "moneyline_volume_usd_by_venue": dict(
                    sorted(self.moneyline_volume_usd_by_venue.items())
                ),
                "moneyline_volume_usd_coverage": {
                    venue: dict(counts)
                    for venue, counts in sorted(
                        self.moneyline_volume_usd_coverage.items()
                    )
                },
            },
            "market_exclusions": {
                key: list(value) for key, value in sorted(self.market_exclusions.items())
            },
            "eligible_market_ids": [market.target_id for market in self.eligible_markets],
            "relationship_analysis": self.relationships.as_record(),
            "rule_assessment": self.rules.as_record(),
        }


@dataclass(frozen=True)
class SelectionResult:
    generated_at: datetime
    strategy_version: int
    catalog_summaries: tuple[dict[str, object], ...]
    match_rejections: tuple[MatchRejection, ...]
    candidates: tuple[Candidate, ...]
    selected: tuple[Candidate, ...]
    retained: tuple[ContinuityBundle, ...]
    continuity_observed: tuple[ContinuityBundle, ...]
    budget_used: Mapping[str, int]
    targets: Mapping[str, tuple[dict[str, object], ...]]
    allocation_rejections: Mapping[str, str]
    continuity_dispositions: Mapping[str, str]

    def as_record(self) -> dict[str, object]:
        return {
            "report_version": 2,
            "mode": "shadow",
            "generated_at": isoformat(self.generated_at),
            "strategy_version": self.strategy_version,
            "catalogs": list(self.catalog_summaries),
            "match_rejections": [item.as_record() for item in self.match_rejections],
            "candidates": [item.as_record() for item in self.candidates],
            "continuity": {
                "bundles": [item.as_record() for item in self.continuity_observed],
                "retained_bundle_ids": [item.bundle_id for item in self.retained],
                "dispositions": dict(sorted(self.continuity_dispositions.items())),
            },
            "selection": {
                "bundle_ids": [
                    *[item.bundle.bundle_id for item in self.selected],
                    *[item.bundle_id for item in self.retained],
                ],
                "bundle_count": len(self.selected) + len(self.retained),
                "budget_used": dict(self.budget_used),
                "targets": {venue: list(items) for venue, items in sorted(self.targets.items())},
                "allocation_rejections": dict(sorted(self.allocation_rejections.items())),
                "publication_performed": False,
            },
            "finding_semantics": (
                "Relationships are happy-path/conditional discovery evidence, not "
                "unconditional executable arbitrage claims."
            ),
        }


def _market_is_mature(market: CanonicalMarket, now: datetime, strategy: Strategy) -> bool:
    if market.created_at is None:
        # Some public catalogues omit creation time. Proximity to activation is
        # still a maturity gate; absence must not make the whole venue unusable.
        return True
    return (now - market.created_at).total_seconds() >= strategy.minimum_market_age_seconds


def _market_is_open(market: CanonicalMarket) -> bool:
    return market.accepting_orders and market.status.casefold() in {
        "active", "open", "initialized", "created", "funded",
    }


def _activity(market: CanonicalMarket) -> float:
    values = (
        (market.volume_24h, 2.0),
        (market.volume_total, 1.0),
        (market.liquidity, 1.5),
    )
    return sum(
        weight * math.log1p(max(0.0, float(value)))
        for value, weight in values
        if value is not None and math.isfinite(float(value))
    )


def _base_market_id(claim_id: str) -> str:
    return claim_id.split("#claim=", 1)[0]


_MONEYLINE_TYPES = frozenset({"moneyline_3way", "series_moneyline"})


def _moneyline_volume_usd(
    markets: Iterable[CanonicalMarket],
) -> tuple[float, dict[str, float], dict[str, dict[str, int]]]:
    values_by_venue: dict[str, list[float]] = {
        venue: [] for venue in SUPPORTED_VENUES
    }
    coverage = {
        venue: {"known_markets": 0, "unknown_markets": 0}
        for venue in SUPPORTED_VENUES
    }
    for market in markets:
        if market.market_type not in _MONEYLINE_TYPES:
            continue
        if market.volume_total_usd is None:
            coverage[market.venue]["unknown_markets"] += 1
            continue
        coverage[market.venue]["known_markets"] += 1
        values_by_venue[market.venue].append(float(market.volume_total_usd))
    by_venue = {
        venue: round(math.fsum(values), 6)
        for venue, values in values_by_venue.items()
    }
    return round(math.fsum(by_venue.values()), 6), by_venue, coverage


def _score_candidate(
    bundle: EventBundle,
    markets: tuple[CanonicalMarket, ...],
    analysis: RelationshipAnalysis,
    strategy: Strategy,
) -> tuple[float, dict[str, float]]:
    """Return reportable diagnostics; allocation order uses `_ranking_key`."""
    venues = {market.venue for market in markets}
    market_ids = {market.target_id for market in markets}
    cross = [
        relationship
        for relationship in analysis.relationships
        if relationship.cross_venue
        and relationship.relationship != "OVERLAP"
        and _base_market_id(relationship.left) in market_ids
        and _base_market_id(relationship.right) in market_ids
    ]
    weights = {
        "IDENTITY": 40.0,
        "IMPLICATION": 20.0,
        "REVERSE_IMPLICATION": 20.0,
        "MUTUAL_EXCLUSION": 10.0,
    }
    components = {
        "venue_coverage": float(len(venues) * 1_000),
        "preferred_venue_bonus": 1_000.0 if len(venues) >= strategy.preferred_venues else 0.0,
        "cross_venue_relationships": sum(weights[item.relationship] for item in cross),
        "market_class_breadth": float(len({market.canonical_class for market in markets}) * 10),
        "activity": sum(_activity(market) for market in markets),
    }
    return sum(components.values()), components


def _candidate(bundle: EventBundle, *, strategy: Strategy, now: datetime) -> Candidate:
    rules = assess_rules(bundle, known_template_ids=strategy.known_rule_templates)
    market_exclusions: dict[str, list[str]] = {
        target_id: list(reasons)
        for target_id, reasons in rules.contradictions.items()
    }
    for market in bundle.markets:
        if not _market_is_open(market):
            market_exclusions.setdefault(market.target_id, []).append("not_open_for_orders")
        if not _market_is_mature(market, now, strategy):
            market_exclusions.setdefault(market.target_id, []).append(
                "minimum_market_age_not_met"
            )
    best_of = bundle.best_of
    if best_of in SUPPORTED_BEST_OF:
        minimum_maps = best_of // 2 + 1
        for market in bundle.markets:
            if market.market_type == "map_winner" and (
                not isinstance(market.parameters.get("map_index"), int)
                or not 1 <= market.parameters["map_index"] <= best_of
            ):
                market_exclusions.setdefault(market.target_id, []).append("product_outside_series_format")
            elif market.market_type == "total_maps":
                line = market.parameters.get("line")
                valid = (
                    isinstance(line, (int, float))
                    and not isinstance(line, bool)
                    and math.isfinite(float(line))
                    and float(line) > 0
                    and float(line) * 2 == int(float(line) * 2)
                )
                if not valid:
                    market_exclusions.setdefault(market.target_id, []).append("invalid_product_parameters")
                elif not minimum_maps <= float(line) < best_of:
                    market_exclusions.setdefault(market.target_id, []).append("product_outside_series_format")
            elif market.market_type == "map_handicap":
                line = market.parameters.get("line")
                if (
                    not isinstance(line, (int, float))
                    or isinstance(line, bool)
                    or not math.isfinite(float(line))
                    or float(line) == 0
                ):
                    market_exclusions.setdefault(market.target_id, []).append("invalid_product_parameters")
                elif abs(float(line)) >= minimum_maps:
                    market_exclusions.setdefault(market.target_id, []).append("product_outside_series_format")
        preliminary = derive_bundle_relationships(bundle)
        series_space = next((space for space in preliminary.spaces if space.scope == "series"), None)
        if bundle.game and series_space is not None:
            for market in bundle.markets:
                if market.market_type not in {"series_moneyline", "map_winner", "total_maps", "map_handicap"}:
                    continue
                _masks, reason = validate_esports_market(bundle, market, series_space)
                if reason:
                    market_exclusions.setdefault(market.target_id, []).append(reason)
    excluded = set(market_exclusions)
    trusted_markets = tuple(
        market for market in bundle.markets if market.target_id not in excluded
    )
    combined_volume, volume_by_venue, volume_coverage = _moneyline_volume_usd(
        trusted_markets
    )
    analysis = derive_bundle_relationships(bundle, excluded_market_ids=excluded)
    cross_relationships = [
        item
        for item in analysis.relationships
        if item.cross_venue and item.relationship != "OVERLAP"
    ]
    relationship_market_ids = {
        _base_market_id(value)
        for item in cross_relationships
        for value in (item.left, item.right)
    }
    # Do not spend subscription budget on markets which currently participate
    # in no modeled combinatorial relationship.
    for market in trusted_markets:
        if market.target_id not in relationship_market_ids:
            market_exclusions.setdefault(market.target_id, []).append(
                "no_modeled_cross_venue_relationship"
            )
    markets = tuple(
        market
        for market in trusted_markets
        if market.target_id in relationship_market_ids
    )
    venues = {market.venue for market in markets}
    cross_venue = cross_relationships
    capture_start = bundle.activation_at - timedelta(seconds=strategy.pre_event_seconds)
    reasons: list[str] = []
    if len(venues) < strategy.minimum_venues:
        reasons.append("fewer_than_minimum_eligible_venues")
    if not cross_venue:
        reasons.append("no_cross_venue_structural_relationship")
    if "unsupported_series_format" in analysis.diagnostics:
        reasons.append("unsupported_series_format")
    if "series_scope_missing_unambiguous_best_of_format" in analysis.diagnostics:
        reasons.append("series_scope_missing_unambiguous_best_of_format")
    if combined_volume < strategy.minimum_combined_moneyline_volume_usd:
        reasons.append("combined_moneyline_volume_usd_below_minimum")
    if capture_start > now + timedelta(seconds=strategy.selection_lookahead_seconds):
        reasons.append("before_capture_lookahead")
    if bundle.activation_at < now - timedelta(seconds=strategy.post_start_retention_seconds):
        reasons.append("past_post_start_retention")
    score, components = _score_candidate(bundle, markets, analysis, strategy)
    return Candidate(
        bundle=bundle,
        eligible_markets=markets,
        relationships=analysis,
        rules=rules,
        score=score,
        score_components=components,
        capture_start_at=capture_start,
        eligible=not reasons,
        rejection_reasons=tuple(reasons),
        market_exclusions={
            key: tuple(value) for key, value in sorted(market_exclusions.items())
        },
        combined_moneyline_volume_usd=combined_volume,
        moneyline_volume_usd_by_venue=volume_by_venue,
        moneyline_volume_usd_coverage=volume_coverage,
        minimum_moneyline_volume_usd=strategy.minimum_combined_moneyline_volume_usd,
    )


def _ranking_key(candidate: Candidate) -> tuple[float, float, float, float, datetime, str]:
    """Encode the documented lexical ranking without lossy scalar weights."""
    components = candidate.score_components
    return (
        -components["venue_coverage"],
        -components["cross_venue_relationships"],
        -components["market_class_breadth"],
        -components["activity"],
        candidate.bundle.activation_at,
        candidate.bundle.bundle_id,
    )


def select_targets(
    catalogs: Iterable[CatalogSnapshot],
    *,
    strategy: Strategy,
    now: datetime | None = None,
    continuity_bundles: Iterable[ContinuityBundle] = (),
) -> SelectionResult:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    snapshots = tuple(catalogs)
    bundles, match_rejections = match_events(
        snapshots,
        tolerance_seconds=strategy.event_time_tolerance_seconds,
        minimum_venues=strategy.minimum_venues,
        participant_aliases=dict(strategy.participant_aliases),
    )
    candidates = tuple(_candidate(bundle, strategy=strategy, now=now) for bundle in bundles)
    ranked = sorted(
        (candidate for candidate in candidates if candidate.eligible),
        key=_ranking_key,
    )
    continuity = tuple(continuity_bundles) if strategy.continuity_hold_enabled else ()
    continuity_dispositions: dict[str, str] = {}
    protected_by_id: dict[str, ContinuityBundle] = {}
    for bundle in continuity:
        if now >= bundle.activation_at + timedelta(seconds=strategy.terminal_clamp_seconds):
            continuity_dispositions[bundle.bundle_id] = "terminal_clamp_elapsed"
        elif bundle.all_terminal:
            continuity_dispositions[bundle.bundle_id] = "all_markets_terminal"
        else:
            continuity_dispositions[bundle.bundle_id] = "retained"
            protected_by_id[bundle.bundle_id] = bundle

    ranked_by_id = {candidate.bundle.bundle_id: candidate for candidate in ranked}
    held_candidates: list[Candidate] = []
    retained_bundles: list[ContinuityBundle] = []
    for bundle_id, bundle in protected_by_id.items():
        current = ranked_by_id.get(bundle_id)
        current_targets = (
            {
                market.target_id: {
                    "canonical_class": market.canonical_class,
                    "subscription_ids": tuple(market.subscription_ids),
                    "activation_at": current.bundle.activation_at,
                    "capture_start_at": current.capture_start_at,
                    "source_ref": market.source_ref,
                }
                for market in current.eligible_markets
            }
            if current is not None
            else {}
        )
        prior_targets = {
            target.target_id: {
                "canonical_class": target.canonical_class,
                "subscription_ids": target.subscription_ids,
                "activation_at": target.activation_at,
                "capture_start_at": target.capture_start_at,
                "source_ref": target.source_ref,
            }
            for target in bundle.targets
        }
        if (
            current is not None
            and current_targets == prior_targets
            and not any(
                target.probe.state is TerminalState.TERMINAL
                for target in bundle.targets
            )
        ):
            held_candidates.append(current)
            continuity_dispositions[bundle_id] = "held_current_candidate"
        else:
            retained_bundles.append(bundle)
    protected_ids = set(protected_by_id)
    additive = [
        candidate
        for candidate in ranked
        if candidate.bundle.bundle_id not in protected_ids
    ]

    budget_used = {venue: 0 for venue in strategy.target_budgets}
    selected: list[Candidate] = []
    selected_retained: list[ContinuityBundle] = []
    selected_target_ids: dict[str, set[str]] = {venue: set() for venue in strategy.target_budgets}
    targets: dict[str, list[dict[str, object]]] = {venue: [] for venue in strategy.target_budgets}
    allocation_rejections: dict[str, str] = {}

    def candidate_increments(candidate: Candidate) -> dict[str, int]:
        return {
            venue: sum(
                len(market.subscription_ids)
                for market in candidate.eligible_markets
                if market.venue == venue and market.target_id not in selected_target_ids[venue]
            )
            for venue in strategy.target_budgets
        }

    def retained_increments(bundle: ContinuityBundle) -> dict[str, int]:
        return {
            venue: sum(
                len(target.subscription_ids)
                for target in bundle.targets
                if target.venue == venue and target.target_id not in selected_target_ids[venue]
            )
            for venue in strategy.target_budgets
        }

    def fits(increments: Mapping[str, int]) -> bool:
        return not any(
            budget_used[venue] + count > strategy.target_budgets[venue]
            for venue, count in increments.items()
        )

    protected: list[tuple[float, str, Candidate | ContinuityBundle]] = [
        (
            protected_by_id[candidate.bundle.bundle_id].score,
            candidate.bundle.bundle_id,
            candidate,
        )
        for candidate in held_candidates
    ] + [
        (bundle.score, bundle.bundle_id, bundle) for bundle in retained_bundles
    ]
    protected.sort(key=lambda item: (-item[0], item[1]))
    for _score, bundle_id, item in protected:
        increments = (
            candidate_increments(item)
            if isinstance(item, Candidate)
            else retained_increments(item)
        )
        if not fits(increments) or len(selected) + len(selected_retained) >= strategy.maximum_bundles:
            allocation_rejections[bundle_id] = "continuity_budget_trimmed"
            continuity_dispositions[bundle_id] = "continuity_budget_trimmed"
            continue
        if isinstance(item, Candidate):
            selected.append(item)
            markets = item.eligible_markets
            for market in markets:
                if market.target_id in selected_target_ids[market.venue]:
                    continue
                selected_target_ids[market.venue].add(market.target_id)
                budget_used[market.venue] += len(market.subscription_ids)
                targets[market.venue].append(
                    {
                        "target_id": market.target_id,
                        "bundle_id": bundle_id,
                        "canonical_class": market.canonical_class,
                        "subscription_ids": list(market.subscription_ids),
                        "activation_at": isoformat(item.bundle.activation_at),
                        "capture_start_at": isoformat(item.capture_start_at),
                        "source_ref": market.source_ref,
                        "continuity_score": item.score,
                    }
                )
        else:
            selected_retained.append(item)
            for target in item.targets:
                if target.target_id in selected_target_ids[target.venue]:
                    continue
                selected_target_ids[target.venue].add(target.target_id)
                budget_used[target.venue] += len(target.subscription_ids)
                targets[target.venue].append(target.as_selection_target(bundle_id, item.score))

    protected_bundle_selected = bool(selected or selected_retained)
    protected_budget_used = dict(budget_used)

    def blocked_by_protected_budget(increments: Mapping[str, int]) -> bool:
        return all(
            budget_used[venue] - protected_budget_used[venue] + count
            <= strategy.target_budgets[venue]
            for venue, count in increments.items()
        )

    for position, candidate in enumerate(additive):
        if len(selected) + len(selected_retained) >= strategy.maximum_bundles:
            allocation_rejections[candidate.bundle.bundle_id] = (
                "displaced_by_continuity_hold"
                if protected_bundle_selected
                else "maximum_bundles_reached"
            )
            continue
        if any(
            market.target_id in selected_target_ids[market.venue]
            for market in candidate.eligible_markets
        ):
            allocation_rejections[candidate.bundle.bundle_id] = (
                "continuity_identity_collision"
            )
            continue
        increments = {
            venue: sum(
                len(market.subscription_ids)
                for market in candidate.eligible_markets
                if market.venue == venue and market.target_id not in selected_target_ids[venue]
            )
            for venue in strategy.target_budgets
        }
        if not fits(increments):
            allocation_rejections[candidate.bundle.bundle_id] = (
                "displaced_by_continuity_hold"
                if blocked_by_protected_budget(increments)
                else "target_budget_exceeded"
            )
            continue
        selected.append(candidate)
        for market in candidate.eligible_markets:
            if market.target_id in selected_target_ids[market.venue]:
                continue
            selected_target_ids[market.venue].add(market.target_id)
            budget_used[market.venue] += len(market.subscription_ids)
            targets[market.venue].append(
                {
                    "target_id": market.target_id,
                    "bundle_id": candidate.bundle.bundle_id,
                    "canonical_class": market.canonical_class,
                    "subscription_ids": list(market.subscription_ids),
                    "activation_at": isoformat(candidate.bundle.activation_at),
                    "capture_start_at": isoformat(candidate.capture_start_at),
                    "source_ref": market.source_ref,
                    "continuity_score": candidate.score,
                }
            )
        if len(selected) + len(selected_retained) >= strategy.maximum_bundles:
            for remaining in additive[position + 1 :]:
                allocation_rejections[remaining.bundle.bundle_id] = (
                    "displaced_by_continuity_hold"
                    if protected_bundle_selected
                    else "maximum_bundles_reached"
                )
            break

    return SelectionResult(
        generated_at=now,
        strategy_version=strategy.version,
        catalog_summaries=tuple(snapshot.as_summary() for snapshot in snapshots),
        match_rejections=match_rejections,
        candidates=tuple(sorted(candidates, key=lambda item: (item.bundle.activation_at, item.bundle.bundle_id))),
        selected=tuple(selected),
        retained=tuple(selected_retained),
        continuity_observed=continuity,
        budget_used=budget_used,
        targets={
            venue: tuple(sorted(items, key=lambda item: str(item["target_id"])))
            for venue, items in targets.items()
        },
        allocation_rejections=allocation_rejections,
        continuity_dispositions=continuity_dispositions,
    )
