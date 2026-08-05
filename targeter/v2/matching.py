"""Deterministic cross-venue event matching for Targeter v2."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from itertools import combinations
from statistics import median
from typing import Iterable, Mapping

from targeter.v2.domain import (
    ActivationEvidence,
    CanonicalEvent,
    CanonicalMarket,
    CatalogSnapshot,
    EventBundle,
    canonical_participant,
    format_observation,
    isoformat,
    stable_id,
)


@dataclass(frozen=True)
class MatchRejection:
    sport: str
    participant_keys: tuple[str, str]
    event_refs: tuple[str, ...]
    reason: str
    game: str | None = None
    topology: str | None = None
    details: Mapping[str, object] = field(default_factory=dict)

    def as_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "sport": self.sport,
            "participant_keys": list(self.participant_keys),
            "event_refs": list(self.event_refs),
            "reason": self.reason,
            "game": self.game,
            "topology": self.topology,
            "details": dict(self.details),
        }
        if self.game is not None:
            record.update(
                format_observation(
                    int(value)
                    for value in self.details.get("format_observed", [])
                    if not isinstance(value, bool) and str(value).isdigit()
                )
            )
        return record


@dataclass(frozen=True)
class _TimeProposal:
    instant: datetime
    event_refs: tuple[str, str]


def _event_ref(event: CanonicalEvent) -> str:
    return f"{event.venue}:{event.venue_event_id}"


def _activation_median(events: Iterable[CanonicalEvent]) -> datetime:
    seconds = [event.activation_at.timestamp() for event in events]
    return datetime.fromtimestamp(median(seconds), tz=timezone.utc)


def _instant_median(values: Iterable[datetime]) -> datetime:
    seconds = [value.timestamp() for value in values]
    return datetime.fromtimestamp(median(seconds), tz=timezone.utc)


def _event_evidence(event: CanonicalEvent) -> tuple[ActivationEvidence, ...]:
    if event.activation_evidence:
        return event.activation_evidence
    return (
        ActivationEvidence(
            instant=event.activation_at,
            source_kind="structured",
            source_field="activation_at",
            primary=True,
        ),
    )


def _evidence_precedence(item: ActivationEvidence) -> tuple[int, str, str]:
    if item.source_kind == "structured" and item.primary:
        rank = 0
    elif item.source_kind == "structured":
        rank = 1
    else:
        rank = 2
    return rank, item.source_field, item.parser_id or ""


def _supporting_evidence(
    event: CanonicalEvent,
    instant: datetime,
    tolerance_seconds: int,
) -> ActivationEvidence | None:
    candidates = [
        item
        for item in _event_evidence(event)
        if abs((item.instant - instant).total_seconds()) <= tolerance_seconds
    ]
    return min(candidates, key=_evidence_precedence) if candidates else None


def _proposal_clusters(
    anchors: list[CanonicalEvent],
    tolerance_seconds: int,
) -> list[tuple[datetime, list[_TimeProposal]]]:
    proposals: list[_TimeProposal] = []
    for left, right in combinations(anchors, 2):
        if left.venue == right.venue:
            continue
        for left_evidence in _event_evidence(left):
            for right_evidence in _event_evidence(right):
                if (
                    abs(
                        (left_evidence.instant - right_evidence.instant).total_seconds()
                    )
                    <= tolerance_seconds
                ):
                    proposals.append(
                        _TimeProposal(
                            _instant_median(
                                (left_evidence.instant, right_evidence.instant)
                            ),
                            tuple(sorted((_event_ref(left), _event_ref(right)))),
                        )
                    )
    proposals.sort(key=lambda item: (item.instant, item.event_refs))
    clusters: list[list[_TimeProposal]] = []
    for proposal in proposals:
        if (
            not clusters
            or (proposal.instant - clusters[-1][0].instant).total_seconds()
            > tolerance_seconds
        ):
            clusters.append([proposal])
        else:
            clusters[-1].append(proposal)
    return [
        (_instant_median(item.instant for item in cluster), cluster)
        for cluster in clusters
    ]


def _legacy_clusters(
    anchors: list[CanonicalEvent], tolerance_seconds: int
) -> list[list[CanonicalEvent]]:
    ordered = sorted(
        anchors,
        key=lambda item: (item.activation_at, item.venue, item.venue_event_id),
    )
    clusters: list[list[CanonicalEvent]] = []
    for event in ordered:
        if (
            not clusters
            or (event.activation_at - clusters[-1][0].activation_at).total_seconds()
            > tolerance_seconds
        ):
            clusters.append([event])
        else:
            clusters[-1].append(event)
    return clusters


def _bundle_markets(
    snapshots: tuple[CatalogSnapshot, ...], events: Iterable[CanonicalEvent]
) -> tuple[CanonicalMarket, ...]:
    refs = {(event.venue, event.venue_event_id) for event in events}
    return tuple(
        sorted(
            (
                market
                for snapshot in snapshots
                for market in snapshot.markets
                if (market.venue, market.venue_event_id) in refs
            ),
            key=lambda item: item.target_id,
        )
    )


def _participant_alias_map(
    events: Iterable[CanonicalEvent], aliases: Mapping[str, str]
) -> dict[str, str]:
    return {
        raw_key: aliases[raw_key]
        for event in events
        for participant in event.participants
        for raw_key in (canonical_participant(participant),)
        if raw_key in aliases and aliases[raw_key] != raw_key
    }


def _activation_conflict(
    event: CanonicalEvent,
    chosen: ActivationEvidence,
    bundle_activation: datetime,
    tolerance_seconds: int,
) -> dict[str, object] | None:
    primaries = sorted(
        (item for item in _event_evidence(event) if item.primary),
        key=lambda item: (item.instant, item.source_field),
    )
    if not primaries or any(
        abs((item.instant - bundle_activation).total_seconds()) <= tolerance_seconds
        for item in primaries
    ):
        return None
    primary = primaries[0]
    return {
        "venue": event.venue,
        "event_id": event.venue_event_id,
        "primary_instant": isoformat(primary.instant),
        "supporting_instant": isoformat(chosen.instant),
        "code": "activation_primary_conflict",
    }


def _make_bundle(
    *,
    snapshots: tuple[CatalogSnapshot, ...],
    events: list[CanonicalEvent],
    sport: str,
    game: str,
    topology: str,
    participant_keys: tuple[str, str],
    activation: datetime,
    support: list[dict[str, object]],
    conflicts: list[dict[str, object]],
    aliases: Mapping[str, str],
) -> EventBundle:
    ordered_events = tuple(
        sorted(events, key=lambda item: (item.venue, item.venue_event_id))
    )
    primary = ordered_events[0]
    leagues = {event.league for event in ordered_events if event.league}
    warnings = ("league_labels_differ",) if len(leagues) > 1 else ()
    identity: dict[str, object] = {
        "sport": sport,
        "participants": participant_keys,
        "activation": int(activation.timestamp()),
    }
    if game:
        identity.update({"game": game, "topology": topology})
    return EventBundle(
        bundle_id=stable_id("bundle", identity),
        sport=sport,
        participants=primary.participants,
        participant_keys=participant_keys,
        activation_at=activation,
        events=ordered_events,
        markets=_bundle_markets(snapshots, ordered_events),
        confidence="HIGH",
        warnings=warnings,
        participant_key_map=_participant_alias_map(ordered_events, aliases),
        game=game or None,
        topology=topology or None,
        activation_support=tuple(
            sorted(
                support,
                key=lambda item: (
                    str(item.get("venue")),
                    str(item.get("event_id")),
                    str(item.get("instant")),
                    str(item.get("source_field")),
                ),
            )
        ),
        activation_conflicts=tuple(
            sorted(
                conflicts,
                key=lambda item: (
                    str(item.get("venue")),
                    str(item.get("event_id")),
                    str(item.get("primary_instant")),
                ),
            )
        ),
    )


def match_events(
    catalogs: Iterable[CatalogSnapshot],
    *,
    tolerance_seconds: int,
    minimum_venues: int,
    participant_aliases: dict[str, str] | None = None,
) -> tuple[tuple[EventBundle, ...], tuple[MatchRejection, ...]]:
    if tolerance_seconds <= 0:
        raise ValueError("tolerance_seconds must be positive")
    if minimum_venues < 2:
        raise ValueError("minimum_venues must be at least two")

    snapshots = tuple(catalogs)
    aliases = participant_aliases or {}

    def resolved_keys(event: CanonicalEvent) -> tuple[str, str]:
        return tuple(
            sorted(
                aliases.get(
                    canonical_participant(participant),
                    canonical_participant(participant),
                )
                for participant in event.participants
            )
        )  # type: ignore[return-value]

    grouped: dict[
        tuple[str, str, str, tuple[str, str]], list[CanonicalEvent]
    ] = defaultdict(list)
    market_map: dict[tuple[str, str], list[CanonicalMarket]] = defaultdict(list)
    rejected: list[MatchRejection] = []
    for snapshot in snapshots:
        for market in snapshot.markets:
            market_map[(market.venue, market.venue_event_id)].append(market)
        for event in snapshot.events:
            keys = resolved_keys(event)
            if keys[0] == keys[1]:
                rejected.append(
                    MatchRejection(
                        event.sport,
                        keys,
                        (_event_ref(event),),
                        "participant_alias_collision",
                        game=event.game,
                        topology=event.topology,
                    )
                )
                continue
            grouped[
                (event.sport, event.game or "", event.topology or "", keys)
            ].append(event)

    bundles: list[EventBundle] = []
    for (
        sport,
        game,
        topology,
        participant_keys,
    ), candidates in sorted(grouped.items()):
        anchors: list[CanonicalEvent] = []
        siblings: list[CanonicalEvent] = []
        for event in candidates:
            event_markets = market_map[(event.venue, event.venue_event_id)]
            is_anchor = any(
                market.canonical_class
                in {"esports.series_moneyline", "soccer.moneyline_3way"}
                for market in event_markets
            )
            if is_anchor or not game:
                anchors.append(event)
            else:
                siblings.append(event)

        identity_bundles: list[EventBundle] = []
        already_rejected: set[str] = set()
        if not game:
            for cluster in _legacy_clusters(anchors, tolerance_seconds):
                refs = tuple(sorted(_event_ref(event) for event in cluster))
                if len({event.venue for event in cluster}) < minimum_venues:
                    rejected.append(
                        MatchRejection(
                            sport,
                            participant_keys,
                            refs,
                            "fewer_than_minimum_venues",
                        )
                    )
                    continue
                formats = {str(event.format) for event in cluster if event.format}
                if len(formats) > 1:
                    rejected.append(
                        MatchRejection(
                            sport,
                            participant_keys,
                            refs,
                            "competition_format_mismatch",
                        )
                    )
                    continue
                identity_bundles.append(
                    _make_bundle(
                        snapshots=snapshots,
                        events=cluster,
                        sport=sport,
                        game=game,
                        topology=topology,
                        participant_keys=participant_keys,
                        activation=_activation_median(cluster),
                        support=[],
                        conflicts=[],
                        aliases=aliases,
                    )
                )
        else:
            proposal_clusters = _proposal_clusters(anchors, tolerance_seconds)
            supported_by_cluster: list[dict[str, ActivationEvidence]] = []
            for instant, _proposals in proposal_clusters:
                supported_by_cluster.append(
                    {
                        _event_ref(event): evidence
                        for event in anchors
                        for evidence in (
                            _supporting_evidence(event, instant, tolerance_seconds),
                        )
                        if evidence is not None
                    }
                )

            cluster_membership: dict[str, list[int]] = defaultdict(list)
            for index, supported in enumerate(supported_by_cluster):
                for event_ref in supported:
                    cluster_membership[event_ref].append(index)
            ambiguous = {
                event_ref
                for event_ref, indices in cluster_membership.items()
                if len(indices) > 1
            }
            for event_ref in sorted(ambiguous):
                event = next(item for item in anchors if _event_ref(item) == event_ref)
                rejected.append(
                    MatchRejection(
                        sport,
                        participant_keys,
                        (event_ref,),
                        "activation_time_ambiguous",
                        game=game,
                        topology=topology,
                    )
                )
                already_rejected.add(event_ref)

            seen_event_sets: set[tuple[str, ...]] = set()
            used_anchor_refs: set[str] = set()
            for (proposal_instant, _), supported in zip(
                proposal_clusters, supported_by_cluster, strict=True
            ):
                selected_events = [
                    event
                    for event in anchors
                    if _event_ref(event) in supported
                    and _event_ref(event) not in ambiguous
                ]
                by_venue: dict[str, list[CanonicalEvent]] = defaultdict(list)
                for event in selected_events:
                    by_venue[event.venue].append(event)
                for venue, venue_events in sorted(by_venue.items()):
                    if len(venue_events) <= 1:
                        continue
                    refs = tuple(sorted(_event_ref(event) for event in venue_events))
                    rejected.append(
                        MatchRejection(
                            sport,
                            participant_keys,
                            refs,
                            "same_venue_anchor_ambiguous",
                            game=game,
                            topology=topology,
                            details={"venue": venue},
                        )
                    )
                    already_rejected.update(refs)
                    selected_events = [
                        event for event in selected_events if event.venue != venue
                    ]
                refs = tuple(sorted(_event_ref(event) for event in selected_events))
                if refs in seen_event_sets:
                    continue
                seen_event_sets.add(refs)
                if len({event.venue for event in selected_events}) < minimum_venues:
                    continue
                formats = {
                    str(event.format) for event in selected_events if event.format
                }
                if len(formats) > 1:
                    formats_by_event = {
                        _event_ref(event): int(event.format)
                        for event in selected_events
                        if event.format and str(event.format).isdigit()
                    }
                    rejected.append(
                        MatchRejection(
                            sport,
                            participant_keys,
                            refs,
                            "competition_format_mismatch",
                            game=game,
                            topology=topology,
                            details={
                                "format_observed": sorted(int(item) for item in formats),
                                "formats_by_event": dict(sorted(formats_by_event.items())),
                            },
                        )
                    )
                    already_rejected.update(refs)
                    continue

                chosen = {
                    _event_ref(event): supported[_event_ref(event)]
                    for event in selected_events
                }
                activation = _instant_median(
                    item.instant for item in chosen.values()
                )
                support = [
                    {
                        "venue": event.venue,
                        "event_id": event.venue_event_id,
                        "instant": isoformat(chosen[_event_ref(event)].instant),
                        "source_kind": chosen[_event_ref(event)].source_kind,
                        "source_field": chosen[_event_ref(event)].source_field,
                        "parser_id": chosen[_event_ref(event)].parser_id,
                    }
                    for event in selected_events
                ]
                conflicts = [
                    conflict
                    for event in selected_events
                    for conflict in (
                        _activation_conflict(
                            event,
                            chosen[_event_ref(event)],
                            activation,
                            tolerance_seconds,
                        ),
                    )
                    if conflict is not None
                ]
                identity_bundles.append(
                    _make_bundle(
                        snapshots=snapshots,
                        events=selected_events,
                        sport=sport,
                        game=game,
                        topology=topology,
                        participant_keys=participant_keys,
                        activation=activation,
                        support=support,
                        conflicts=conflicts,
                        aliases=aliases,
                    )
                )
                used_anchor_refs.update(refs)

            for event in anchors:
                ref = _event_ref(event)
                if ref in used_anchor_refs or ref in already_rejected:
                    continue
                reason = (
                    "activation_time_conflict"
                    if identity_bundles
                    else "fewer_than_minimum_venues"
                )
                rejected.append(
                    MatchRejection(
                        sport,
                        participant_keys,
                        (ref,),
                        reason,
                        game=game,
                        topology=topology,
                        details={"activation_at": isoformat(event.activation_at)},
                    )
                )

        # Siblings can enrich exactly one immutable anchor bundle.  They never
        # establish minimum venue coverage or alter bundle activation.
        for sibling in sorted(siblings, key=_event_ref):
            eligible: list[tuple[int, ActivationEvidence]] = []
            for index, bundle in enumerate(identity_bundles):
                formats = {
                    str(event.format) for event in bundle.events if event.format
                }
                if (
                    sibling.format
                    and formats
                    and str(sibling.format) not in formats
                ):
                    continue
                evidence = _supporting_evidence(
                    sibling, bundle.activation_at, tolerance_seconds
                )
                if evidence is not None:
                    eligible.append((index, evidence))
            if len(eligible) != 1:
                rejected.append(
                    MatchRejection(
                        sport,
                        participant_keys,
                        (_event_ref(sibling),),
                        "sibling_ambiguous" if len(eligible) > 1 else "sibling_no_anchor",
                        game=game,
                        topology=topology,
                    )
                )
                continue
            bundle_index, evidence = eligible[0]
            bundle = identity_bundles[bundle_index]
            events = tuple(
                sorted(
                    (*bundle.events, sibling),
                    key=lambda item: (item.venue, item.venue_event_id),
                )
            )
            support = (
                *bundle.activation_support,
                {
                    "venue": sibling.venue,
                    "event_id": sibling.venue_event_id,
                    "instant": isoformat(evidence.instant),
                    "source_kind": evidence.source_kind,
                    "source_field": evidence.source_field,
                    "parser_id": evidence.parser_id,
                },
            )
            conflict = _activation_conflict(
                sibling, evidence, bundle.activation_at, tolerance_seconds
            )
            conflicts = bundle.activation_conflicts + ((conflict,) if conflict else ())
            identity_bundles[bundle_index] = replace(
                bundle,
                events=events,
                markets=_bundle_markets(snapshots, events),
                activation_support=tuple(
                    sorted(
                        support,
                        key=lambda item: (
                            str(item.get("venue")),
                            str(item.get("event_id")),
                            str(item.get("instant")),
                        ),
                    )
                ),
                activation_conflicts=tuple(
                    sorted(
                        conflicts,
                        key=lambda item: (
                            str(item.get("venue")),
                            str(item.get("event_id")),
                        ),
                    )
                ),
            )

        bundles.extend(identity_bundles)

    return (
        tuple(sorted(bundles, key=lambda item: (item.activation_at, item.bundle_id))),
        tuple(sorted(rejected, key=lambda item: (item.event_refs, item.reason))),
    )
