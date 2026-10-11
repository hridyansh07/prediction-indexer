"""Deterministic event/market projection of a verified Targeter v3 report."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Iterable, Mapping

from targeter.v2.models import isoformat, parse_timestamp

MARKET_PROJECTION_VERSION = 3
MARKET_TEMPLATE_VERSION = 1
OUTCOME_SPACE_VERSION = 1
RELATION_GENERATION_VERSION = 1

_SYMMETRIC = {"IDENTITY", "MUTUAL_EXCLUSION", "OVERLAP"}
_DIRECTED = {"IMPLICATION", "REVERSE_IMPLICATION"}


class MarketProjectionError(ValueError):
    """The supplied report or catalogue cannot produce a closed projection."""


def project_market_universe(
    report: Mapping[str, Any],
    *,
    catalog_events: Iterable[Mapping[str, Any]],
    catalog_markets: Iterable[Mapping[str, Any]],
    rule_templates: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Project candidate and continuity-selected markets into JSON-safe rows."""
    root = _mapping(report, "report")
    if root.get("report_version") != 3 or root.get("mode") != "shadow":
        raise MarketProjectionError("market projection requires a Targeter v3 shadow report")
    run_id = _text(root, "run_id", "report")
    generated_at = _time(root.get("generated_at"), "report.generated_at")
    if not isinstance(root.get("input_complete"), bool):
        raise MarketProjectionError("report.input_complete must be boolean")
    version = root.get("strategy_version")
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise MarketProjectionError("report.strategy_version must be a positive integer")
    if root["input_complete"] is False:
        return _empty_projection(run_id, generated_at)

    selection = _mapping(root.get("selection"), "report.selection")
    selected_ids = _texts(selection.get("bundle_ids"), "selection.bundle_ids")
    _unique(selected_ids, "selection.bundle_ids")
    if selection.get("bundle_count") != len(selected_ids):
        raise MarketProjectionError("selection.bundle_count disagrees with bundle_ids")
    selected = set(selected_ids)
    allocation = selection.get("allocation_rejections", {})
    allocation = _mapping(allocation, "selection.allocation_rejections")

    candidates = _objects(root.get("candidates"), "report.candidates")
    by_bundle: dict[str, Mapping[str, Any]] = {}
    for candidate in candidates:
        bundle_id = _text(candidate, "bundle_id", "candidate")
        if bundle_id in by_bundle:
            raise MarketProjectionError(f"duplicate candidate bundle {bundle_id}")
        by_bundle[bundle_id] = candidate

    required_event_refs = {
        ref
        for bundle_id, candidate in by_bundle.items()
        for ref in _texts(
            candidate.get("event_refs"), f"candidate {bundle_id} event_refs"
        )
    }
    required_market_refs = {
        ref
        for bundle_id, candidate in by_bundle.items()
        for ref in _texts(
            candidate.get("market_ids"), f"candidate {bundle_id} market_ids"
        )
    }
    events_by_ref = _index_records(
        catalog_events,
        _event_ref,
        _raw_event_ref,
        required_event_refs,
        "catalogue event",
    )
    markets_by_id = _index_records(
        catalog_markets,
        _market_ref,
        _raw_market_ref,
        required_market_refs,
        "catalogue market",
    )
    templates = _index_records(
        rule_templates,
        _template_market_ref,
        _raw_template_ref,
        required_market_refs,
        "rule template",
    )

    continuity = _mapping(root.get("continuity"), "report.continuity")
    retained_ids = _texts(
        continuity.get("retained_bundle_ids"), "continuity.retained_bundle_ids"
    )
    _unique(retained_ids, "continuity.retained_bundle_ids")
    if not set(retained_ids) <= selected:
        raise MarketProjectionError("continuity retains an unselected bundle")
    dispositions = _mapping(continuity.get("dispositions"), "continuity.dispositions")
    continuity_by_bundle: dict[str, Mapping[str, Any]] = {}
    for item in _objects(continuity.get("bundles"), "continuity.bundles"):
        bundle_id = _text(item, "bundle_id", "continuity bundle")
        if bundle_id in continuity_by_bundle:
            raise MarketProjectionError(f"duplicate continuity bundle {bundle_id}")
        continuity_by_bundle[bundle_id] = item
    if set(retained_ids) - set(continuity_by_bundle):
        raise MarketProjectionError("retained bundle lacks continuity evidence")
    missing_selected = selected - set(by_bundle) - set(retained_ids)
    if missing_selected:
        raise MarketProjectionError(
            "selected bundle lacks candidate or retention evidence: "
            + ", ".join(sorted(missing_selected))
        )

    event_rows: dict[str, dict[str, Any]] = {}
    venue_event_rows: dict[tuple[str, str], dict[str, Any]] = {}
    market_rows: dict[str, dict[str, Any]] = {}
    venue_market_rows: dict[tuple[str, str], dict[str, Any]] = {}
    decisions: list[dict[str, Any]] = []
    event_for_bundle: dict[str, str] = {}

    for bundle_id, candidate in sorted(by_bundle.items()):
        event_id = _project_candidate(
            candidate,
            bundle_id=bundle_id,
            events_by_ref=events_by_ref,
            markets_by_id=markets_by_id,
            templates=templates,
            event_rows=event_rows,
            venue_event_rows=venue_event_rows,
            market_rows=market_rows,
            venue_market_rows=venue_market_rows,
        )
        event_for_bundle[bundle_id] = event_id
        eligible = candidate.get("eligible")
        if not isinstance(eligible, bool):
            raise MarketProjectionError(f"candidate {bundle_id} eligible must be boolean")
        decisions.append(
            _finite(
                {
                    "run_id": run_id,
                    "event_id": event_id,
                    "bundle_id": bundle_id,
                    "eligible": eligible,
                    "selected": bundle_id in selected,
                    "score": _number(candidate.get("score"), f"candidate {bundle_id} score"),
                    "score_components": candidate.get("score_components"),
                    "rejection_reasons": candidate.get("rejection_reasons"),
                    "allocation_rejection": allocation.get(bundle_id),
                    "admission": candidate.get("admission"),
                    "market_exclusions": candidate.get("market_exclusions"),
                    "eligible_market_ids": candidate.get("eligible_market_ids"),
                },
                f"candidate {bundle_id} decision",
            )
        )

    for bundle_id in sorted(set(retained_ids) - set(event_for_bundle)):
        evidence = continuity_by_bundle[bundle_id]
        event = _retained_event(evidence, bundle_id)
        if event is not None:
            _put_unique(event_rows, event["event_id"], event, "event")
            event_for_bundle[bundle_id] = event["event_id"]

    selected_markets = (
        _selected_market_rows(
            selection,
            run_id,
            selected,
            event_for_bundle,
            dispositions,
            continuity_by_bundle,
            venue_market_rows,
        )
        if root["input_complete"]
        else []
    )
    relations = _relation_rows(candidates, event_for_bundle) if root["input_complete"] else []
    return {
        "projection_version": MARKET_PROJECTION_VERSION,
        "run_id": run_id,
        "generated_at": generated_at,
        "events": sorted(event_rows.values(), key=lambda row: row["event_id"]),
        "venue_events": sorted(
            venue_event_rows.values(), key=lambda row: (row["venue"], row["venue_event_id"])
        ),
        "markets": sorted(market_rows.values(), key=lambda row: row["market_id"]),
        "venue_markets": sorted(
            venue_market_rows.values(), key=lambda row: (row["venue"], row["venue_market_id"])
        ),
        "decisions": sorted(decisions, key=lambda row: row["bundle_id"]),
        "selected_markets": selected_markets,
        "relations": relations,
    }


def _project_candidate(
    candidate: Mapping[str, Any],
    *,
    bundle_id: str,
    events_by_ref: Mapping[str, Mapping[str, Any]],
    markets_by_id: Mapping[str, Mapping[str, Any]],
    templates: Mapping[str, Mapping[str, Any]],
    event_rows: dict[str, dict[str, Any]],
    venue_event_rows: dict[tuple[str, str], dict[str, Any]],
    market_rows: dict[str, dict[str, Any]],
    venue_market_rows: dict[tuple[str, str], dict[str, Any]],
) -> str:
    sport = _text(candidate, "sport", f"candidate {bundle_id}")
    game = _optional_text(candidate.get("game"), f"candidate {bundle_id} game")
    topology = _optional_text(candidate.get("topology"), f"candidate {bundle_id} topology")
    participants = _texts(candidate.get("participants"), f"candidate {bundle_id} participants")
    participant_keys = _texts(
        candidate.get("participant_keys"), f"candidate {bundle_id} participant_keys"
    )
    if len(participants) != 2 or len(participant_keys) != 2 or len(set(participant_keys)) != 2:
        raise MarketProjectionError(f"candidate {bundle_id} participants are invalid")
    activation_at = _time(candidate.get("activation_at"), f"candidate {bundle_id} activation_at")
    event_refs = _texts(
        candidate.get("event_refs"), f"candidate {bundle_id} event_refs"
    )
    _unique(event_refs, f"candidate {bundle_id} event_refs")
    event_refs = sorted(event_refs)
    event_id = "event-proposal:" + _digest({"bundle_id": bundle_id})
    _put_unique(
        event_rows,
        event_id,
        {
            "event_id": event_id,
            "sport": sport,
            "game": game,
            "topology": topology,
            "activation_at": activation_at,
            "participants": participants,
            "participant_keys": sorted(participant_keys),
            "event_refs": event_refs,
            "source_bundle_id": bundle_id,
        },
        "event",
    )
    for ref in event_refs:
        source = events_by_ref.get(ref)
        if source is None:
            raise MarketProjectionError(
                f"candidate {bundle_id} event {ref} is absent from catalogue"
            )
        _time(source.get("activation_at"), f"catalogue event {ref} activation_at")
        row = {
            "event_id": event_id,
            **{
                field: source.get(field)
                for field in (
                    "venue",
                    "venue_event_id",
                    "title",
                    "league",
                    "status",
                    "source_ref",
                    "format",
                    "fragment_type",
                )
            },
        }
        _required_text_fields(
            row,
            ("venue", "venue_event_id", "title", "status", "source_ref"),
            f"event {ref}",
        )
        _put_unique(
            venue_event_rows,
            (row["venue"], row["venue_event_id"]),
            _finite(row, f"event {ref}"),
            "venue event",
        )

    market_ids = _texts(candidate.get("market_ids"), f"candidate {bundle_id} market_ids")
    _unique(market_ids, f"candidate {bundle_id} market_ids")
    valid_event_ids = {
        (row["venue"], row["venue_event_id"])
        for row in venue_event_rows.values()
        if row["event_id"] == event_id
    }
    for target_id in market_ids:
        source = markets_by_id.get(target_id)
        if source is None:
            raise MarketProjectionError(
                f"candidate {bundle_id} market {target_id} is absent from catalogue"
            )
        venue = _text(source, "venue", f"market {target_id}")
        native_id = _text(source, "venue_market_id", f"market {target_id}")
        venue_event_id = _text(source, "venue_event_id", f"market {target_id}")
        if (venue, venue_event_id) not in valid_event_ids:
            raise MarketProjectionError(
                f"market {target_id} is not linked by candidate {bundle_id}"
            )
        parameters = _finite(
            source.get("parameters"), f"market {target_id} parameters"
        )
        if not isinstance(parameters, dict):
            raise MarketProjectionError(f"market {target_id} parameters must be an object")
        coordinates = {
            "event_id": event_id,
            "canonical_class": _text(
                source, "canonical_class", f"market {target_id}"
            ),
            "market_type": _text(source, "market_type", f"market {target_id}"),
            "scope": _text(source, "scope", f"market {target_id}"),
            "parameters": parameters,
        }
        market_id = canonical_market_id(
            **coordinates,
            market_template_version=MARKET_TEMPLATE_VERSION,
            outcome_space_version=OUTCOME_SPACE_VERSION,
        )
        market_row = {
            "market_id": market_id,
            **coordinates,
            "market_template_version": MARKET_TEMPLATE_VERSION,
            "outcome_space_version": OUTCOME_SPACE_VERSION,
        }
        previous_market = market_rows.get(market_id)
        if previous_market is not None and previous_market != market_row:
            raise MarketProjectionError(f"conflicting market {market_id}")
        market_rows[market_id] = market_row
        template = templates.get(target_id)
        venue_row = {
            "venue": venue,
            "venue_market_id": native_id,
            "venue_event_id": venue_event_id,
            "event_id": event_id,
            "market_id": market_id,
            "market_template_version": MARKET_TEMPLATE_VERSION,
            "outcome_space_version": OUTCOME_SPACE_VERSION,
            **{
                field: source.get(field)
                for field in (
                    "canonical_class",
                    "market_type",
                    "scope",
                    "title",
                    "subscription_ids",
                    "outcome_labels",
                    "status",
                    "accepting_orders",
                    "rules_hash",
                    "source_ref",
                    "created_at",
                    "volume_24h",
                    "volume_total",
                    "volume_total_usd",
                    "liquidity",
                )
            },
            "parameters": parameters,
            "rule_template_id": template.get("template_id") if template else None,
        }
        _required_text_fields(
            venue_row,
            ("canonical_class", "market_type", "scope", "title", "status", "source_ref"),
            f"market {target_id}",
        )
        if not isinstance(venue_row["accepting_orders"], bool):
            raise MarketProjectionError(
                f"market {target_id} accepting_orders must be boolean"
            )
        venue_row["subscription_ids"] = _texts(
            venue_row["subscription_ids"], f"market {target_id} subscription_ids"
        )
        venue_row["outcome_labels"] = _texts(
            venue_row["outcome_labels"], f"market {target_id} outcome_labels"
        )
        _unique(venue_row["subscription_ids"], f"market {target_id} subscription_ids")
        _put_unique(
            venue_market_rows,
            (venue, native_id),
            _finite(venue_row, f"market {target_id}"),
            "venue market",
        )
    return event_id


def _retained_event(evidence: Mapping[str, Any], bundle_id: str) -> dict[str, Any] | None:
    """Use copied origin semantics when present; otherwise leave retention unresolved."""
    required = (
        "sport",
        "participants",
        "participant_keys",
        "activation_at",
        "event_refs",
    )
    if not all(field in evidence for field in required):
        return None
    sport = _text(evidence, "sport", f"continuity bundle {bundle_id}")
    participants = _texts(
        evidence.get("participants"), f"continuity bundle {bundle_id} participants"
    )
    participant_keys = _texts(
        evidence.get("participant_keys"), f"continuity bundle {bundle_id} participant_keys"
    )
    if (
        len(participants) != 2
        or len(participant_keys) != 2
        or len(set(participant_keys)) != 2
    ):
        raise MarketProjectionError(
            f"continuity bundle {bundle_id} participants are invalid"
        )
    game = _optional_text(evidence.get("game"), f"continuity bundle {bundle_id} game")
    topology = _optional_text(
        evidence.get("topology"), f"continuity bundle {bundle_id} topology"
    )
    activation_at = _time(
        evidence.get("activation_at"), f"continuity bundle {bundle_id} activation_at"
    )
    event_refs = sorted(
        _texts(evidence.get("event_refs"), f"continuity bundle {bundle_id} event_refs")
    )
    _unique(event_refs, f"continuity bundle {bundle_id} event_refs")
    return {
        "event_id": "event-proposal:" + _digest({"bundle_id": bundle_id}),
        "sport": sport,
        "game": game,
        "topology": topology,
        "participant_keys": sorted(participant_keys),
        "event_refs": event_refs,
        "activation_at": activation_at,
        "participants": participants,
        "source_bundle_id": bundle_id,
    }


def _selected_market_rows(
    selection: Mapping[str, Any],
    run_id: str,
    selected: set[str],
    event_for_bundle: Mapping[str, str],
    dispositions: Mapping[str, Any],
    continuity: Mapping[str, Mapping[str, Any]],
    venue_markets: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    targets = _mapping(selection.get("targets"), "selection.targets")
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for venue, values in sorted(targets.items()):
        for target in _objects(values, f"selection.targets.{venue}"):
            bundle_id = _text(target, "bundle_id", "selection target")
            if bundle_id not in selected:
                raise MarketProjectionError(
                    f"selection target names unselected bundle {bundle_id}"
                )
            event_id = event_for_bundle.get(bundle_id)
            if event_id is None:  # Retained origin lacks event semantics; do not invent them.
                continue
            target_id = _text(target, "target_id", "selection target")
            parsed_venue, separator, native_id = target_id.partition(":")
            if not separator or parsed_venue != venue or not native_id:
                raise MarketProjectionError(
                    f"selection target {target_id} has invalid identity"
                )
            projected_market = venue_markets.get((venue, native_id))
            if projected_market is None and dispositions.get(bundle_id) == "retained":
                # Copied continuity can identify an event without carrying origin catalogue rows.
                continue
            if projected_market is None or projected_market["event_id"] != event_id:
                raise MarketProjectionError(
                    f"selection target {target_id} is absent from its projected event"
                )
            canonical_class = _text(target, "canonical_class", "selection target")
            if canonical_class != projected_market["canonical_class"]:
                raise MarketProjectionError(
                    f"selection target {target_id} canonical class conflicts"
                )
            disposition = dispositions.get(bundle_id)
            reason = (
                "retained"
                if disposition == "retained"
                else "held_current_candidate"
                if disposition == "held_current_candidate"
                else "selected"
            )
            origin = (
                continuity.get(bundle_id, {}).get("origin_run_id")
                if reason == "retained"
                else run_id
            )
            row = _finite(
                {
                    "run_id": run_id,
                    "event_id": event_id,
                    "bundle_id": bundle_id,
                    "venue": venue,
                    "venue_market_id": native_id,
                    "market_id": projected_market["market_id"],
                    "market_template_version": projected_market[
                        "market_template_version"
                    ],
                    "outcome_space_version": projected_market[
                        "outcome_space_version"
                    ],
                    "canonical_class": canonical_class,
                    "continuity_score": _number(
                        target.get("continuity_score"),
                        "selection target continuity_score",
                    ),
                    "selection_reason": reason,
                    "origin_run_id": origin,
                },
                f"selection target {target_id}",
            )
            _put_unique(rows, (bundle_id, target_id), row, "selected market")
    return sorted(
        rows.values(),
        key=lambda row: (row["bundle_id"], row["venue"], row["venue_market_id"]),
    )


def _relation_rows(
    candidates: list[Mapping[str, Any]], event_for_bundle: Mapping[str, str]
) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        bundle_id = _text(candidate, "bundle_id", "candidate")
        market_ids = set(
            _texts(candidate.get("market_ids"), f"candidate {bundle_id} market_ids")
        )
        analysis = _mapping(
            candidate.get("relationship_analysis"),
            f"candidate {bundle_id} relationship_analysis",
        )
        for relation in _objects(
            analysis.get("relationships"), f"candidate {bundle_id} relationships"
        ):
            if relation.get("bundle_id") != bundle_id:
                raise MarketProjectionError(f"candidate {bundle_id} relationship has wrong bundle")
            kind = _text(relation, "relationship", "relationship")
            if kind not in _SYMMETRIC | _DIRECTED:
                raise MarketProjectionError(f"unsupported relationship type {kind}")
            members = [
                _relation_member(relation, "left"),
                _relation_member(relation, "right"),
            ]
            for member in members:
                target_id = f"{member['venue']}:{member['venue_market_id']}"
                if target_id not in market_ids:
                    raise MarketProjectionError(
                        f"candidate {bundle_id} relationship member {target_id} "
                        "is not a candidate market"
                    )
            if kind in _SYMMETRIC:
                members = sorted(
                    ({**member, "role": "member"} for member in members),
                    key=_member_key,
                )
            canonical_hash = _digest({"relation_type": kind, "members": members})
            row = {
                "canonical_hash": canonical_hash,
                "relation_type": kind,
                "event_id": event_for_bundle[bundle_id],
                "bundle_id": bundle_id,
                "scope": _text(relation, "scope", "relationship"),
                "coverage": _text(relation, "coverage", "relationship"),
                "generation_version": RELATION_GENERATION_VERSION,
                "members": members,
            }
            existing = rows.get(canonical_hash)
            if existing is not None and existing != row:
                raise MarketProjectionError(f"relationship hash conflict {canonical_hash}")
            rows[canonical_hash] = row
    return sorted(rows.values(), key=lambda row: row["canonical_hash"])


def canonical_market_id(
    *,
    event_id: str,
    canonical_class: str,
    market_type: str,
    scope: str,
    parameters: Mapping[str, Any],
    market_template_version: int,
    outcome_space_version: int,
) -> str:
    """Return the canonical ID for a market under a resolved umbrella event."""

    if market_template_version != MARKET_TEMPLATE_VERSION:
        raise MarketProjectionError("unsupported market template version")
    if outcome_space_version != OUTCOME_SPACE_VERSION:
        raise MarketProjectionError("unsupported outcome-space version")
    coordinates = {
        "event_id": event_id,
        "canonical_class": canonical_class,
        "market_type": market_type,
        "scope": scope,
        "parameters": parameters,
    }
    return "market:" + _digest(coordinates)[:32]


def _relation_member(value: Mapping[str, Any], side: str) -> dict[str, str]:
    identifier = _text(value, side, "relationship")
    base, marker, claim = identifier.partition("#")
    venue, separator, native_id = base.partition(":")
    if not separator or not venue or not native_id:
        raise MarketProjectionError(f"relationship member {identifier} is invalid")
    stated = _text(value, f"{side}_venue", "relationship")
    if stated != venue:
        raise MarketProjectionError(f"relationship member {identifier} has conflicting venue")
    return {
        "venue": venue,
        "venue_market_id": native_id,
        "claim_key": claim if marker else "",
        "role": side,
    }


def _member_key(value: Mapping[str, str]) -> tuple[str, str, str, str]:
    return (
        value["venue"],
        value["venue_market_id"],
        value["claim_key"],
        value["role"],
    )


def _index_records(
    values: Iterable[Mapping[str, Any]],
    key_fn: Any,
    raw_key_fn: Any,
    required: set[str],
    label: str,
) -> dict[str, Mapping[str, Any]]:
    output: dict[str, Mapping[str, Any]] = {}
    for value in values:
        row = _mapping(value, label)
        if raw_key_fn(row) not in required:
            continue
        key = key_fn(row)
        if key in output:
            raise MarketProjectionError(f"duplicate {label} {key}")
        output[key] = row
    return output


def _raw_event_ref(row: Mapping[str, Any]) -> str | None:
    venue = row.get("venue")
    identifier = row.get("venue_event_id")
    if not isinstance(venue, str) or not isinstance(identifier, str):
        return None
    return f"{venue}:{identifier}"


def _raw_market_ref(row: Mapping[str, Any]) -> str | None:
    value = row.get("target_id")
    return value if isinstance(value, str) else None


def _raw_template_ref(row: Mapping[str, Any]) -> str | None:
    value = row.get("market_id")
    return value if isinstance(value, str) else None


def _empty_projection(run_id: str, generated_at: str) -> dict[str, Any]:
    return {
        "projection_version": MARKET_PROJECTION_VERSION,
        "run_id": run_id,
        "generated_at": generated_at,
        "events": [],
        "venue_events": [],
        "markets": [],
        "venue_markets": [],
        "decisions": [],
        "selected_markets": [],
        "relations": [],
    }


def _event_ref(row: Mapping[str, Any]) -> str:
    return (
        f"{_text(row, 'venue', 'catalogue event')}:"
        f"{_text(row, 'venue_event_id', 'catalogue event')}"
    )


def _market_ref(row: Mapping[str, Any]) -> str:
    expected = (
        f"{_text(row, 'venue', 'catalogue market')}:"
        f"{_text(row, 'venue_market_id', 'catalogue market')}"
    )
    if row.get("target_id") != expected:
        raise MarketProjectionError(f"catalogue market {expected} has inconsistent target_id")
    return expected


def _template_market_ref(row: Mapping[str, Any]) -> str:
    _text(row, "template_id", "rule template")
    return _text(row, "market_id", "rule template")


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _finite(value: Any, label: str) -> Any:
    try:
        return json.loads(
            json.dumps(
                value,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        )
    except (TypeError, ValueError) as error:
        raise MarketProjectionError(f"{label} must be finite JSON") from error


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or not all(
        isinstance(key, str) for key in value
    ):
        raise MarketProjectionError(f"{label} must be an object")
    return value


def _objects(value: Any, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(
        isinstance(item, Mapping) for item in value
    ):
        raise MarketProjectionError(f"{label} must be an array of objects")
    return list(value)


def _text(value: Mapping[str, Any], field: str, label: str) -> str:
    item = value.get(field)
    if not isinstance(item, str) or not item:
        raise MarketProjectionError(f"{label}.{field} must be non-empty text")
    return item


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise MarketProjectionError(f"{label} must be null or non-empty text")
    return value


def _texts(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise MarketProjectionError(f"{label} must be an array of non-empty text")
    return list(value)


def _time(value: Any, label: str) -> str:
    parsed = parse_timestamp(value)
    if parsed is None:
        raise MarketProjectionError(f"{label} must be a timestamp")
    result = isoformat(parsed)
    assert result is not None
    return result


def _number(value: Any, label: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise MarketProjectionError(f"{label} must be finite")
    return value


def _unique(values: list[str], label: str) -> None:
    if len(values) != len(set(values)):
        raise MarketProjectionError(f"{label} contains duplicates")


def _required_text_fields(
    value: Mapping[str, Any], fields: tuple[str, ...], label: str
) -> None:
    for field in fields:
        _text(value, field, label)


def _put_unique(
    store: dict[Any, dict[str, Any]],
    key: Any,
    value: dict[str, Any],
    label: str,
) -> None:
    previous = store.get(key)
    if previous is not None:
        raise MarketProjectionError(f"duplicate/conflicting {label} {key}")
    store[key] = value
