"""Disposable dashboard projection of one verified Targeter v3 report."""

from __future__ import annotations

from collections import Counter
import math
from typing import Any, Mapping

from targeter.v2.models import isoformat, parse_timestamp

CADENCE_PROJECTION_VERSION = 1
CONTINUITY_DISPOSITIONS = {
    "all_markets_terminal",
    "continuity_budget_trimmed",
    "held_current_candidate",
    "retained",
    "terminal_clamp_elapsed",
}
PROJECTION_FIELDS = (
    "projection_version",
    "catalogs",
    "discovery_failures",
    "counts",
    "reason_summaries",
    "match_rejections",
    "candidates",
    "selected_targets",
    "budget_used",
    "continuity",
    "diagnostics",
)


class CadenceProjectionError(ValueError):
    pass


def project_cadence_run(report: Mapping[str, Any]) -> dict[str, Any]:
    """Keep operational decision evidence without retaining the raw report."""
    if report.get("report_version") != 3 or report.get("mode") != "shadow":
        raise CadenceProjectionError("cadence projection requires a Targeter v3 report")
    if not isinstance(report.get("input_complete"), bool):
        raise CadenceProjectionError("cadence report input_complete must be boolean")
    candidates = _objects(report.get("candidates", []), "candidates")
    selection = _mapping(report.get("selection", {}), "selection")
    continuity = _mapping(report.get("continuity", {}), "continuity")
    complete = report.get("input_complete") is True
    selected_ids = (
        set(_texts(selection.get("bundle_ids", []), "selected bundle ids"))
        if complete
        else set()
    )
    retained_ids = (
        set(_texts(continuity.get("retained_bundle_ids", []), "retained bundle ids"))
        if complete
        else set()
    )
    dispositions = (
        _text_mapping(continuity.get("dispositions", {}), "continuity dispositions")
        if complete
        else {}
    )
    allocation = _text_mapping(
        selection.get("allocation_rejections", {}), "allocation rejections"
    )
    candidate_records = [
        _candidate(candidate, selected_ids=selected_ids, allocation=allocation)
        for candidate in candidates
    ]
    targets = _targets(selection.get("targets", {})) if complete else {}
    continuity_bundles = [
        _continuity_bundle(item, dispositions)
        for item in (
            _objects(continuity.get("bundles", []), "continuity bundles")
            if complete
            else []
        )
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
    result = {
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
    return validate_cadence_projection(result)


def validate_cadence_projection(value: Any) -> dict[str, Any]:
    """Fail closed if a generated or cached cadence payload is not API-safe."""
    root = _mapping(value, "cadence projection")
    _exact_keys(root, PROJECTION_FIELDS, "cadence projection")
    if root.get("projection_version") != CADENCE_PROJECTION_VERSION:
        raise CadenceProjectionError("cadence projection version is invalid")
    for catalog in _objects(root.get("catalogs"), "cadence catalogs"):
        _exact_keys(
            catalog,
            (
                "venue",
                "complete",
                "events",
                "markets",
                "requests",
                "diagnostics",
                "classification_diagnostic_count",
                "classification_diagnostics_by_code",
            ),
            "cadence catalog",
        )
        _nonempty_text(catalog.get("venue"), "catalog venue")
        _boolean(catalog.get("complete"), "catalog complete")
        for field in ("events", "markets", "requests", "classification_diagnostic_count"):
            _nonnegative_integer(catalog.get(field), f"catalog {field}")
        _texts(catalog.get("diagnostics"), "catalog diagnostics")
        _nonnegative_number_mapping(
            catalog.get("classification_diagnostics_by_code"),
            "catalog classification diagnostics by code",
            integers=True,
        )
    _text_mapping(root.get("discovery_failures"), "discovery failures")
    counts = _mapping(root.get("counts"), "cadence counts")
    count_fields = ("candidates", "eligible", "selected", "rejected", "retained", "retired")
    _exact_keys(counts, count_fields, "cadence counts")
    for field in count_fields:
        _nonnegative_integer(counts.get(field), f"cadence count {field}")
    summaries = _mapping(root.get("reason_summaries"), "reason summaries")
    summary_fields = (
        "candidate_rejections",
        "allocation_rejections",
        "continuity_dispositions",
    )
    _exact_keys(summaries, summary_fields, "reason summaries")
    for field in summary_fields:
        _nonnegative_number_mapping(
            summaries.get(field), f"reason summary {field}", integers=True
        )
    for rejection in _objects(root.get("match_rejections"), "match rejections"):
        _allowed_keys(
            rejection,
            ("sport", "game", "topology", "participant_keys", "event_refs", "reason", "details"),
            "match rejection",
        )
    for candidate in _objects(root.get("candidates"), "cadence candidates"):
        _validate_candidate(candidate)
    targets = _mapping(root.get("selected_targets"), "selected targets")
    for venue, records in targets.items():
        _nonempty_text(venue, "selected target venue")
        for target in _objects(records, f"selected targets for {venue}"):
            _validate_target(target, f"selected target for {venue}", continuity=False)
    _nonnegative_number_mapping(root.get("budget_used"), "budget used")
    continuity = _mapping(root.get("continuity"), "cadence continuity")
    _exact_keys(
        continuity,
        ("bundles", "retained_bundle_ids", "dispositions"),
        "cadence continuity",
    )
    retained = _texts(continuity.get("retained_bundle_ids"), "retained bundle ids")
    _unique(retained, "retained bundle ids")
    dispositions = _text_mapping(
        continuity.get("dispositions"), "continuity dispositions"
    )
    if any(item not in CONTINUITY_DISPOSITIONS for item in dispositions.values()):
        raise CadenceProjectionError("continuity disposition is invalid")
    for bundle in _objects(continuity.get("bundles"), "continuity bundles"):
        _validate_continuity_bundle(bundle)
    diagnostics = _mapping(root.get("diagnostics"), "cadence diagnostics")
    _exact_keys(
        diagnostics,
        (
            "continuity",
            "continuity_degraded_base_run_id",
            "target_records",
        ),
        "cadence diagnostics",
    )
    _texts(diagnostics.get("continuity"), "continuity diagnostics")
    degraded = diagnostics.get("continuity_degraded_base_run_id")
    if degraded is not None:
        _nonempty_text(degraded, "continuity degraded base run id")
    _string_lists(diagnostics.get("target_records"), "target record diagnostics")
    _validate_finite_json(root)
    return {field: root[field] for field in PROJECTION_FIELDS}


def _catalog(item: Mapping[str, Any]) -> dict[str, Any]:
    _required(
        item,
        ("venue", "complete", "events", "markets", "requests", "diagnostics"),
        "catalog",
    )
    diagnostics = item.get("classification_diagnostics", [])
    if not isinstance(diagnostics, list):
        raise CadenceProjectionError("catalog classification diagnostics must be an array")
    if any(
        not isinstance(value, dict)
        or not isinstance(value.get("code"), str)
        or not value["code"]
        for value in diagnostics
    ):
        raise CadenceProjectionError("catalog classification diagnostic is invalid")
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
    _required(relationship, ("relationships",), f"candidate {bundle_id} relationship analysis")
    _required(
        item,
        (
            "sport", "game", "topology", "participants", "participant_keys",
            "event_refs", "activation_at", "capture_start_at", "score",
            "score_components", "eligible", "event_status", "rejection_reasons",
            "admission", "market_exclusions", "eligible_market_ids",
        ),
        f"candidate {bundle_id}",
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
            _required_record(
                item,
                (
                    "target_id",
                    "bundle_id",
                    "canonical_class",
                    "subscription_ids",
                    "activation_at",
                    "capture_start_at",
                    "source_ref",
                    "continuity_score",
                ),
                f"selected target {venue}",
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
            _required_record(
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
                f"continuity target {bundle_id}",
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


def _required(value: Mapping[str, Any], fields: tuple[str, ...], label: str) -> None:
    missing = [field for field in fields if field not in value]
    if missing:
        raise CadenceProjectionError(f"{label} is missing {', '.join(missing)}")


def _required_record(
    value: Mapping[str, Any], fields: tuple[str, ...], label: str
) -> dict[str, Any]:
    _required(value, fields, label)
    return _copy_fields(value, fields)


def _validate_candidate(candidate: Mapping[str, Any]) -> None:
    label = f"candidate {_nonempty_text(candidate.get('bundle_id'), 'candidate bundle id')}"
    _exact_keys(
        candidate,
        (
            "bundle_id", "sport", "game", "topology", "participants",
            "participant_keys", "event_refs", "activation_at", "capture_start_at",
            "score", "score_components", "eligible", "event_status",
            "rejection_reasons", "admission", "market_exclusions",
            "eligible_market_ids", "selected", "allocation_rejection",
            "relationship_analysis",
        ),
        label,
    )
    _nonempty_text(candidate.get("sport"), f"{label} sport")
    for field in ("game", "topology"):
        value = candidate.get(field)
        if value is not None:
            _nonempty_text(value, f"{label} {field}")
    for field in (
        "participants",
        "participant_keys",
        "event_refs",
        "eligible_market_ids",
        "rejection_reasons",
    ):
        _nonempty_texts(candidate.get(field), f"{label} {field}")
    _timestamp(candidate.get("activation_at"), f"{label} activation_at")
    _timestamp(candidate.get("capture_start_at"), f"{label} capture_start_at")
    _finite_number(candidate.get("score"), f"{label} score")
    _number_mapping(candidate.get("score_components"), f"{label} score components")
    eligible = _boolean(candidate.get("eligible"), f"{label} eligible")
    expected_status = "ELIGIBLE" if eligible else "REJECTED"
    if candidate.get("event_status") != expected_status:
        raise CadenceProjectionError(f"{label} event status is inconsistent")
    _boolean(candidate.get("selected"), f"{label} selected")
    allocation = candidate.get("allocation_rejection")
    if allocation is not None:
        _nonempty_text(allocation, f"{label} allocation rejection")
    admission = _mapping(candidate.get("admission"), f"{label} admission")
    for field in ("combined_moneyline_volume_usd", "minimum_moneyline_volume_usd"):
        _nonnegative_number(admission.get(field), f"{label} admission {field}")
    _nonnegative_number_mapping(
        admission.get("moneyline_volume_usd_by_venue"),
        f"{label} moneyline volume by venue",
    )
    coverage = _mapping(
        admission.get("moneyline_volume_usd_coverage"),
        f"{label} moneyline volume coverage",
    )
    for venue, counts in coverage.items():
        _nonempty_text(venue, f"{label} coverage venue")
        _nonnegative_number_mapping(
            counts, f"{label} coverage for {venue}", integers=True
        )
    _string_lists(candidate.get("market_exclusions"), f"{label} market exclusions")
    relationship = _mapping(
        candidate.get("relationship_analysis"), f"{label} relationship analysis"
    )
    _allowed_keys(
        relationship,
        ("relationships", "diagnostics", "outcome_spaces"),
        f"{label} relationship analysis",
    )
    if "relationships" not in relationship:
        raise CadenceProjectionError(f"{label} relationship analysis is missing relationships")
    for field in ("relationships", "diagnostics", "outcome_spaces"):
        if field in relationship and not isinstance(relationship[field], list):
            raise CadenceProjectionError(
                f"{label} relationship analysis {field} must be an array"
            )


def _validate_target(
    target: Mapping[str, Any], label: str, *, continuity: bool
) -> None:
    _exact_keys(
        target,
        (
            (
                "target_id", "venue", "canonical_class", "subscription_ids",
                "activation_at", "capture_start_at", "source_ref", "terminal_probe",
            )
            if continuity
            else (
                "target_id", "bundle_id", "canonical_class", "subscription_ids",
                "activation_at", "capture_start_at", "source_ref", "continuity_score",
            )
        ),
        label,
    )
    for field in ("target_id", "canonical_class", "source_ref"):
        _nonempty_text(target.get(field), f"{label} {field}")
    if continuity:
        _nonempty_text(target.get("venue"), f"{label} venue")
    else:
        _nonempty_text(target.get("bundle_id"), f"{label} bundle_id")
        _finite_number(target.get("continuity_score"), f"{label} continuity score")
    subscriptions = _nonempty_texts(
        target.get("subscription_ids"), f"{label} subscription ids"
    )
    if not subscriptions:
        raise CadenceProjectionError(f"{label} subscription ids must not be empty")
    _unique(subscriptions, f"{label} subscription ids")
    _timestamp(target.get("activation_at"), f"{label} activation_at")
    _timestamp(target.get("capture_start_at"), f"{label} capture_start_at")
    if continuity:
        probe = _mapping(target.get("terminal_probe"), f"{label} terminal probe")
        if probe.get("state") not in {"open", "terminal", "unknown"}:
            raise CadenceProjectionError(f"{label} terminal probe state is invalid")
        _nonempty_text(probe.get("reason"), f"{label} terminal probe reason")


def _validate_continuity_bundle(bundle: Mapping[str, Any]) -> None:
    bundle_id = _nonempty_text(bundle.get("bundle_id"), "continuity bundle id")
    label = f"continuity bundle {bundle_id}"
    _allowed_keys(
        bundle,
        (
            "base_run_id", "bundle_id", "activation_at", "score", "origin_run_id",
            "disposition", "targets",
        ),
        label,
    )
    _required(
        bundle,
        ("base_run_id", "bundle_id", "activation_at", "score", "disposition", "targets"),
        label,
    )
    _nonempty_text(bundle.get("base_run_id"), f"{label} base run id")
    _timestamp(bundle.get("activation_at"), f"{label} activation_at")
    _finite_number(bundle.get("score"), f"{label} score")
    origin = bundle.get("origin_run_id")
    if origin is not None:
        _nonempty_text(origin, f"{label} origin run id")
    if bundle.get("disposition") not in CONTINUITY_DISPOSITIONS:
        raise CadenceProjectionError(f"{label} disposition is invalid")
    targets = _objects(bundle.get("targets"), f"{label} targets")
    if not targets:
        raise CadenceProjectionError(f"{label} targets must not be empty")
    for target in targets:
        _validate_target(target, f"{label} target", continuity=True)


def _validate_finite_json(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise CadenceProjectionError("cadence projection contains a non-finite number")
    if isinstance(value, dict):
        for item in value.values():
            _validate_finite_json(item)
    elif isinstance(value, list):
        for item in value:
            _validate_finite_json(item)


def _exact_keys(value: Mapping[str, Any], fields: tuple[str, ...], label: str) -> None:
    expected = set(fields)
    if set(value) != expected:
        raise CadenceProjectionError(f"{label} fields are invalid")


def _allowed_keys(value: Mapping[str, Any], fields: tuple[str, ...], label: str) -> None:
    if not set(value) <= set(fields):
        raise CadenceProjectionError(f"{label} fields are invalid")


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
        or not key
        or isinstance(item, bool)
        or not isinstance(item, (int, float))
        or not math.isfinite(float(item))
        for key, item in source.items()
    ):
        raise CadenceProjectionError(f"{label} must map text to numbers")
    return dict(source)


def _string_lists(value: Any, label: str) -> dict[str, list[str]]:
    source = _mapping(value, label)
    if any(not isinstance(key, str) or not key for key in source):
        raise CadenceProjectionError(f"{label} keys must be non-empty text")
    return {
        key: _nonempty_texts(item, f"{label} {key}")
        for key, item in sorted(source.items())
    }


def _nonempty_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise CadenceProjectionError(f"{label} must be non-empty text")
    return value


def _nonempty_texts(value: Any, label: str) -> list[str]:
    output = _texts(value, label)
    if any(not item for item in output):
        raise CadenceProjectionError(f"{label} must contain non-empty text")
    return output


def _boolean(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise CadenceProjectionError(f"{label} must be boolean")
    return value


def _finite_number(value: Any, label: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise CadenceProjectionError(f"{label} must be a finite number")
    return value


def _nonnegative_number(value: Any, label: str) -> int | float:
    output = _finite_number(value, label)
    if output < 0:
        raise CadenceProjectionError(f"{label} must be non-negative")
    return output


def _nonnegative_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CadenceProjectionError(f"{label} must be a non-negative integer")
    return value


def _nonnegative_number_mapping(
    value: Any, label: str, *, integers: bool = False
) -> dict[str, int | float]:
    output = _number_mapping(value, label)
    for item in output.values():
        if item < 0 or (integers and not isinstance(item, int)):
            kind = "non-negative integers" if integers else "non-negative numbers"
            raise CadenceProjectionError(f"{label} must map text to {kind}")
    return output


def _timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise CadenceProjectionError(f"{label} must be a canonical timestamp")
    parsed = parse_timestamp(value)
    if parsed is None or isoformat(parsed) != value:
        raise CadenceProjectionError(f"{label} must be a canonical timestamp")
    return value


def _unique(value: list[str], label: str) -> None:
    if len(value) != len(set(value)):
        raise CadenceProjectionError(f"{label} must be unique")
