"""Disposable dashboard projection of one verified Targeter v3 report."""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

CADENCE_PROJECTION_VERSION = 1


class CadenceProjectionError(ValueError):
    pass


def project_cadence_run(report: Mapping[str, Any]) -> dict[str, Any]:
    """Keep operational decision evidence without retaining the raw report."""
    if report.get("report_version") != 3 or report.get("mode") != "shadow":
        raise CadenceProjectionError("cadence projection requires a Targeter v3 report")
    candidates = _objects(report.get("candidates", []), "candidates")
    selection = _mapping(report.get("selection", {}), "selection")
    continuity = _mapping(report.get("continuity", {}), "continuity")
    selected_ids = set(_texts(selection.get("bundle_ids", []), "selected bundle ids"))
    retained_ids = set(
        _texts(continuity.get("retained_bundle_ids", []), "retained bundle ids")
    )
    dispositions = _text_mapping(
        continuity.get("dispositions", {}), "continuity dispositions"
    )
    allocation = _text_mapping(
        selection.get("allocation_rejections", {}), "allocation rejections"
    )
    candidate_records = [
        _candidate(candidate, selected_ids=selected_ids, allocation=allocation)
        for candidate in candidates
    ]
    targets = _targets(selection.get("targets", {}))
    continuity_bundles = [
        _continuity_bundle(item, dispositions)
        for item in _objects(continuity.get("bundles", []), "continuity bundles")
    ]
    retired = sum(
        disposition in {"all_markets_terminal", "terminal_clamp_elapsed"}
        for disposition in dispositions.values()
    )
    rejected_reasons = Counter(
        reason
        for candidate in candidate_records
        if not candidate["eligible"]
        for reason in candidate["rejection_reasons"]
    )
    return {
        "projection_version": CADENCE_PROJECTION_VERSION,
        "catalogs": [
            _catalog(item)
            for item in _objects(report.get("catalogs", []), "catalogs")
        ],
        "discovery_failures": _text_mapping(
            report.get("discovery_failures", {}), "discovery failures"
        ),
        "counts": {
            "candidates": len(candidates),
            "eligible": sum(bool(item["eligible"]) for item in candidate_records),
            "selected": len(selected_ids),
            "rejected": sum(not bool(item["eligible"]) for item in candidate_records),
            "retained": len(retained_ids),
            "retired": retired,
        },
        "reason_summaries": {
            "candidate_rejections": dict(sorted(rejected_reasons.items())),
            "allocation_rejections": dict(
                sorted(Counter(allocation.values()).items())
            ),
            "continuity_dispositions": dict(
                sorted(Counter(dispositions.values()).items())
            ),
        },
        "match_rejections": [
            _copy_fields(
                item,
                (
                    "sport",
                    "game",
                    "topology",
                    "participant_keys",
                    "event_refs",
                    "reason",
                    "details",
                ),
            )
            for item in _objects(report.get("match_rejections", []), "match rejections")
        ],
        "candidates": candidate_records,
        "selected_targets": targets,
        "budget_used": _number_mapping(selection.get("budget_used", {}), "budget used"),
        "continuity": {
            "bundles": continuity_bundles,
            "retained_bundle_ids": sorted(retained_ids),
            "dispositions": dict(sorted(dispositions.items())),
        },
        "diagnostics": {
            "continuity": _texts(
                report.get("continuity_diagnostics", []), "continuity diagnostics"
            ),
            "continuity_degraded_base_run_id": report.get(
                "continuity_degraded_base_run_id"
            ),
            "target_records": _string_lists(
                report.get("target_record_diagnostics", {}),
                "target record diagnostics",
            ),
        },
    }


def _catalog(item: Mapping[str, Any]) -> dict[str, Any]:
    diagnostics = item.get("classification_diagnostics", [])
    if not isinstance(diagnostics, list):
        raise CadenceProjectionError("catalog classification diagnostics must be an array")
    codes = Counter(
        value.get("code")
        for value in diagnostics
        if isinstance(value, dict) and isinstance(value.get("code"), str)
    )
    return {
        **_copy_fields(
            item,
            ("venue", "complete", "events", "markets", "requests", "diagnostics"),
        ),
        "classification_diagnostic_count": len(diagnostics),
        "classification_diagnostics_by_code": dict(sorted(codes.items())),
    }


def _candidate(
    item: Mapping[str, Any], *, selected_ids: set[str], allocation: Mapping[str, str]
) -> dict[str, Any]:
    bundle_id = item.get("bundle_id")
    if not isinstance(bundle_id, str) or not bundle_id:
        raise CadenceProjectionError("candidate bundle_id must be non-empty text")
    relationship = item.get("relationship_analysis", {})
    if not isinstance(relationship, dict):
        raise CadenceProjectionError(
            f"candidate {bundle_id} relationship analysis is invalid"
        )
    record = {
        **_copy_fields(
            item,
            (
                "bundle_id",
                "sport",
                "game",
                "topology",
                "participants",
                "participant_keys",
                "event_refs",
                "activation_at",
                "capture_start_at",
                "score",
                "score_components",
                "eligible",
                "event_status",
                "rejection_reasons",
                "admission",
                "market_exclusions",
                "eligible_market_ids",
            ),
        ),
        "selected": bundle_id in selected_ids,
        "allocation_rejection": allocation.get(bundle_id),
        "relationship_analysis": _copy_fields(
            relationship, ("relationships", "diagnostics", "outcome_spaces")
        ),
    }
    record.setdefault("eligible", False)
    record.setdefault("rejection_reasons", [])
    return record


def _targets(value: Any) -> dict[str, list[dict[str, Any]]]:
    source = _mapping(value, "selected targets")
    output: dict[str, list[dict[str, Any]]] = {}
    for venue, records in sorted(source.items()):
        if not isinstance(venue, str):
            raise CadenceProjectionError("selected target venue must be text")
        output[venue] = [
            _copy_fields(
                item,
                (
                    "target_id",
                    "bundle_id",
                    "canonical_class",
                    "subscription_ids",
                    "activation_at",
                    "capture_start_at",
                    "source_ref",
                ),
            )
            for item in _objects(records, f"selected targets for {venue}")
        ]
    return output


def _continuity_bundle(
    item: Mapping[str, Any], dispositions: Mapping[str, str]
) -> dict[str, Any]:
    bundle_id = item.get("bundle_id")
    if not isinstance(bundle_id, str) or not bundle_id:
        raise CadenceProjectionError("continuity bundle_id must be non-empty text")
    targets = []
    for target in _objects(item.get("targets", []), f"continuity {bundle_id} targets"):
        targets.append(
            _copy_fields(
                target,
                (
                    "target_id",
                    "venue",
                    "canonical_class",
                    "subscription_ids",
                    "activation_at",
                    "capture_start_at",
                    "source_ref",
                    "terminal_probe",
                ),
            )
        )
    return {
        **_copy_fields(
            item,
            ("base_run_id", "bundle_id", "activation_at", "score", "origin_run_id"),
        ),
        "disposition": dispositions.get(bundle_id),
        "targets": targets,
    }


def _copy_fields(value: Mapping[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: value[field] for field in fields if field in value}


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise CadenceProjectionError(f"{label} must be an object")
    return value


def _objects(value: Any, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise CadenceProjectionError(f"{label} must be an array of objects")
    return value


def _texts(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise CadenceProjectionError(f"{label} must be an array of text")
    return list(value)


def _text_mapping(value: Any, label: str) -> dict[str, str]:
    source = _mapping(value, label)
    if any(
        not isinstance(key, str) or not isinstance(item, str)
        for key, item in source.items()
    ):
        raise CadenceProjectionError(f"{label} must map text to text")
    return dict(source)


def _number_mapping(value: Any, label: str) -> dict[str, int | float]:
    source = _mapping(value, label)
    if any(
        not isinstance(key, str)
        or isinstance(item, bool)
        or not isinstance(item, (int, float))
        for key, item in source.items()
    ):
        raise CadenceProjectionError(f"{label} must map text to numbers")
    return dict(source)


def _string_lists(value: Any, label: str) -> dict[str, list[str]]:
    source = _mapping(value, label)
    return {key: _texts(item, f"{label} {key}") for key, item in sorted(source.items())}
