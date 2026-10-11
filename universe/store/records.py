"""Row decoding, validation and canonical-JSON helpers shared by the store modules."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from targeter.v2.models import isoformat, parse_timestamp
from universe.derive.market_projection import MARKET_PROJECTION_VERSION
from universe.store import limits


class DetailTooLarge(ValueError):
    """A detail document would require more child rows than the API permits."""
class BundleEventConflict(ValueError):
    """A bundle cannot be resolved to one umbrella event."""


class EvidenceConflict(ValueError):
    """Immutable source evidence or its SQL projection is inconsistent."""


_SELECTION_QUERY = """SELECT
    o.run_id, o.bundle_id, o.context_sha256, o.occurrence_kind,
    o.origin_run_id, o.continuity_selected, o.continuity_disposition,
    c.sport, c.game, c.topology, c.activation_at, c.activation_at_ns,
    c.capture_start_at, c.capture_start_at_ns,
    r.generated_at, r.generated_at_ns, r.manifest_key, r.manifest_sha256,
    r.report_key, r.report_sha256,
    origin.generated_at AS origin_generated_at,
    origin.manifest_key AS origin_manifest_key,
    origin.manifest_sha256 AS origin_manifest_sha256,
    origin.report_key AS origin_report_key,
    origin.report_sha256 AS origin_report_sha256,
    retired.run_id AS retirement_run_id,
    retired.generated_at AS retired_at,
    retirement.disposition AS retirement_disposition,
    retired.manifest_key AS retirement_manifest_key,
    retired.manifest_sha256 AS retirement_manifest_sha256,
    retired.report_key AS retirement_report_key,
    retired.report_sha256 AS retirement_report_sha256
FROM selection_occurrences o
JOIN bundle_contexts c USING (context_sha256)
JOIN targeter_runs r ON r.run_id = o.run_id
JOIN targeter_runs origin ON origin.run_id = o.origin_run_id
LEFT JOIN bundle_retirements retirement
    ON retirement.bundle_id = o.bundle_id
   AND retirement.run_id = (
       SELECT candidate.run_id
       FROM bundle_retirements candidate
       JOIN targeter_runs candidate_run ON candidate_run.run_id = candidate.run_id
       WHERE candidate.bundle_id = o.bundle_id
       ORDER BY candidate_run.generated_at_ns, candidate.run_id
       LIMIT 1
   )
LEFT JOIN targeter_runs retired ON retired.run_id = retirement.run_id"""


def _occurrence(record: dict[str, Any], *, expected_run_id: str) -> dict[str, Any]:
    expected = {
        "run_id",
        "bundle_id",
        "occurrence_kind",
        "origin_run_id",
        "continuity_selected",
        "continuity_disposition",
        "context",
    }
    if set(record) != expected or record.get("run_id") != expected_run_id:
        raise EvidenceConflict("selected occurrence fields disagree with its run")
    bundle_id = _nonempty(record["bundle_id"], "bundle_id")
    kind = record["occurrence_kind"]
    origin_run_id = _nonempty(record["origin_run_id"], "origin_run_id")
    selected = record["continuity_selected"]
    disposition = record["continuity_disposition"]
    if not isinstance(selected, bool) or kind not in {"complete", "retained"}:
        raise EvidenceConflict(f"selected occurrence {bundle_id} is invalid")
    if kind == "complete":
        if origin_run_id != expected_run_id or disposition not in {
            None,
            "held_current_candidate",
        } or selected != (disposition is not None):
            raise EvidenceConflict(f"complete occurrence {bundle_id} provenance is invalid")
    elif (
        origin_run_id == expected_run_id
        or not selected
        or disposition != "retained"
    ):
        raise EvidenceConflict(f"retained occurrence {bundle_id} provenance is invalid")
    context = _context_record(record["context"], expected_bundle_id=bundle_id)
    return {
        **record,
        "bundle_id": bundle_id,
        "origin_run_id": origin_run_id,
        "context": context,
        "context_sha256": _context_sha256(context),
    }


def _retirement(record: dict[str, Any], *, expected_run_id: str) -> dict[str, Any]:
    expected = {
        "run_id",
        "bundle_id",
        "origin_run_id",
        "disposition",
        "terminal_observed",
        "context",
    }
    if set(record) != expected or record.get("run_id") != expected_run_id:
        raise EvidenceConflict("bundle retirement fields disagree with its run")
    bundle_id = _nonempty(record["bundle_id"], "bundle_id")
    origin_run_id = _nonempty(record["origin_run_id"], "origin_run_id")
    disposition = record["disposition"]
    terminal_observed = record["terminal_observed"]
    if (
        origin_run_id == expected_run_id
        or disposition not in {"all_markets_terminal", "terminal_clamp_elapsed"}
        or not isinstance(terminal_observed, bool)
        or terminal_observed != (disposition == "all_markets_terminal")
    ):
        raise EvidenceConflict(f"retired bundle {bundle_id} provenance is invalid")
    context = _context_record(record["context"], expected_bundle_id=bundle_id)
    return {
        **record,
        "bundle_id": bundle_id,
        "origin_run_id": origin_run_id,
        "context": context,
        "context_sha256": _context_sha256(context),
    }


def _context_record(value: Any, *, expected_bundle_id: str) -> dict[str, Any]:
    expected = {
        "bundle_id",
        "sport",
        "game",
        "topology",
        "participants",
        "participant_keys",
        "activation_at",
        "capture_start_at",
        "event_refs",
        "markets",
        "targets",
        "relationships",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise EvidenceConflict("bundle context fields are invalid")
    record = dict(value)
    if record.get("bundle_id") != expected_bundle_id:
        raise EvidenceConflict("bundle context has the wrong bundle_id")
    sport = _nonempty(record["sport"], "sport")
    optional = {
        field: _optional_text(record[field], field) for field in ("game", "topology")
    }
    participants = _text_list(record["participants"], "participants")
    participant_keys = _text_list(record["participant_keys"], "participant_keys")
    if len(participants) != 2 or len(participant_keys) != 2:
        raise EvidenceConflict("bundle context must have two participants")
    activation_at = _canonical_timestamp(record["activation_at"], "activation_at")
    capture_start_at = _canonical_timestamp(
        record["capture_start_at"], "capture_start_at"
    )
    if _timestamp_ns(capture_start_at) >= _timestamp_ns(activation_at):
        raise EvidenceConflict("bundle capture_start_at must precede activation_at")
    event_refs = sorted(_text_list(record["event_refs"], "event_refs"))
    for reference in event_refs:
        _venue_prefix(reference)

    markets: list[dict[str, Any]] = []
    market_ids: set[str] = set()
    selected_market_ids: set[str] = set()
    if not isinstance(record["markets"], list) or not record["markets"]:
        raise EvidenceConflict("bundle markets must be a non-empty array")
    for raw in record["markets"]:
        if not isinstance(raw, Mapping) or set(raw) != {"target_id", "venue", "selected"}:
            raise EvidenceConflict("bundle market fields are invalid")
        target_id = _nonempty(raw["target_id"], "market target_id")
        venue = _nonempty(raw["venue"], "market venue")
        selected = raw["selected"]
        if (
            not isinstance(selected, bool)
            or _venue_prefix(target_id) != venue
            or target_id in market_ids
        ):
            raise EvidenceConflict(f"bundle market {target_id} is invalid")
        market_ids.add(target_id)
        if selected:
            selected_market_ids.add(target_id)
        markets.append({"target_id": target_id, "venue": venue, "selected": selected})
    markets.sort(key=lambda item: item["target_id"])

    targets: list[dict[str, Any]] = []
    target_ids: set[str] = set()
    if not isinstance(record["targets"], list) or not record["targets"]:
        raise EvidenceConflict("bundle targets must be a non-empty array")
    target_fields = {
        "venue",
        "target_id",
        "canonical_class",
        "subscription_ids",
        "source_ref",
    }
    for raw in record["targets"]:
        if not isinstance(raw, Mapping) or set(raw) != target_fields:
            raise EvidenceConflict("bundle target fields are invalid")
        target_id = _nonempty(raw["target_id"], "target_id")
        venue = _nonempty(raw["venue"], "target venue")
        if (
            _venue_prefix(target_id) != venue
            or target_id in target_ids
            or target_id not in selected_market_ids
        ):
            raise EvidenceConflict(f"bundle target {target_id} is invalid")
        target_ids.add(target_id)
        targets.append(
            {
                "venue": venue,
                "target_id": target_id,
                "canonical_class": _nonempty(
                    raw["canonical_class"], "canonical_class"
                ),
                "subscription_ids": sorted(
                    _text_list(raw["subscription_ids"], "subscription_ids")
                ),
                "source_ref": _nonempty(raw["source_ref"], "source_ref"),
            }
        )
    if target_ids != selected_market_ids:
        raise EvidenceConflict("selected markets and targets disagree")
    targets.sort(key=lambda item: (item["venue"], item["target_id"]))

    relationships: list[dict[str, str]] = []
    relationship_fields = {
        "left",
        "right",
        "relationship",
        "scope",
        "left_venue",
        "right_venue",
        "coverage",
    }
    if not isinstance(record["relationships"], list):
        raise EvidenceConflict("bundle relationships must be an array")
    for raw in record["relationships"]:
        if not isinstance(raw, Mapping) or set(raw) != relationship_fields:
            raise EvidenceConflict("bundle relationship fields are invalid")
        relationships.append(
            {field: _nonempty(raw[field], f"relationship {field}") for field in relationship_fields}
        )
    relationships.sort(
        key=lambda item: (
            item["left"],
            item["right"],
            item["relationship"],
            item["scope"],
        )
    )
    return {
        "bundle_id": expected_bundle_id,
        "sport": sport,
        **optional,
        "participants": participants,
        "participant_keys": participant_keys,
        "activation_at": activation_at,
        "capture_start_at": capture_start_at,
        "event_refs": event_refs,
        "markets": markets,
        "targets": targets,
        "relationships": relationships,
    }


def _projection_entry(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bundle_id": value["bundle_id"],
        "context_sha256": value["context_sha256"],
        "occurrence_kind": value["occurrence_kind"],
        "origin_run_id": value["origin_run_id"],
        "continuity_selected": value["continuity_selected"],
        "continuity_disposition": value["continuity_disposition"],
    }


def _retirement_projection_entry(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bundle_id": value["bundle_id"],
        "context_sha256": value["context_sha256"],
        "origin_run_id": value["origin_run_id"],
        "retirement_disposition": value["disposition"],
    }


def _context_sha256(context: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(context).encode("utf-8")).hexdigest()


def _records_sha256(records: Iterable[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update((_canonical_json(record) + "\n").encode("utf-8"))
    return digest.hexdigest()


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _run_record(row: sqlite3.Row) -> dict[str, Any]:
    record = _row_record(row)
    record["input_complete"] = bool(record["input_complete"])
    return record


def _run_summary(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "run_id": row["run_id"],
        "generated_at": row["generated_at"],
        "input_complete": bool(row["input_complete"]),
        "indexed_at": _isoformat_ns(int(row["indexed_at_ns"])),
    }


def _selection_record(row: sqlite3.Row) -> dict[str, Any]:
    retirement = None
    if row["retirement_run_id"] is not None:
        retirement = {
            "retired_at": row["retired_at"],
            "disposition": row["retirement_disposition"],
            "terminal_observed_at": (
                row["retired_at"]
                if row["retirement_disposition"] == "all_markets_terminal"
                else None
            ),
            "source": {
                "run_id": row["retirement_run_id"],
                "manifest_key": row["retirement_manifest_key"],
                "manifest_sha256": row["retirement_manifest_sha256"],
                "report_key": row["retirement_report_key"],
                "report_sha256": row["retirement_report_sha256"],
            },
        }
    return {
        "run_id": row["run_id"],
        "generated_at": row["generated_at"],
        "bundle_id": row["bundle_id"],
        "occurrence_kind": row["occurrence_kind"],
        "continuity_selected": bool(row["continuity_selected"]),
        "continuity_disposition": row["continuity_disposition"],
        "sport": row["sport"],
        "game": row["game"],
        "topology": row["topology"],
        "activation_at": row["activation_at"],
        "capture_start_at": row["capture_start_at"],
        "retirement": retirement,
        "source": {
            "manifest_key": row["manifest_key"],
            "manifest_sha256": row["manifest_sha256"],
            "report_key": row["report_key"],
            "report_sha256": row["report_sha256"],
        },
        "origin": {
            "run_id": row["origin_run_id"],
            "generated_at": row["origin_generated_at"],
            "manifest_key": row["origin_manifest_key"],
            "manifest_sha256": row["origin_manifest_sha256"],
            "report_key": row["origin_report_key"],
            "report_sha256": row["origin_report_sha256"],
        },
    }


# A market's claim can change if its semantics do, which opens a new era rather
# than rewriting the old row. Only the newest era describes the market now, so
# every read filters to it. Eras are near-always one row, unlike the per-run
# observations this model replaced.
_CURRENT_ERA = """
    NOT EXISTS (
        SELECT 1 FROM market_claims superseding
        JOIN targeter_runs superseding_run
          ON superseding_run.run_id = superseding.last_seen_run_id
        JOIN targeter_runs current_run
          ON current_run.run_id = {alias}.last_seen_run_id
        WHERE superseding.venue = {alias}.venue
          AND superseding.venue_market_id = {alias}.venue_market_id
          AND superseding.claim_key = {alias}.claim_key
          AND (superseding_run.generated_at_ns, superseding.last_seen_run_id)
            > (current_run.generated_at_ns, {alias}.last_seen_run_id)
    )
"""


def _current_era(alias: str) -> str:
    return _CURRENT_ERA.format(alias=alias)


def _claim_identifier(value: str) -> str:
    """A claim is addressed by the digest of its outcome subset and shape."""
    identifier = str(value)
    if len(identifier) != 64 or not _is_hex(identifier):
        raise ValueError("claim id must be a sha256 digest")
    return identifier


def _is_hex(value: str) -> bool:
    return all(character in "0123456789abcdef" for character in value)


def _row_record(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _market_projection(
    value: Mapping[str, Any], *, expected_run_id: str, expected_generated_at: str
) -> dict[str, Any]:
    fields = {
        "projection_version",
        "run_id",
        "generated_at",
        "events",
        "venue_events",
        "markets",
        "venue_markets",
        "decisions",
        "selected_markets",
        "relations",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise EvidenceConflict("market projection fields are invalid")
    if (
        value.get("projection_version") != MARKET_PROJECTION_VERSION
        or value.get("run_id") != expected_run_id
        or value.get("generated_at") != expected_generated_at
    ):
        raise EvidenceConflict("market projection disagrees with its Targeter run")
    record = dict(value)
    for field in fields - {"projection_version", "run_id", "generated_at"}:
        if not isinstance(record[field], list) or any(
            not isinstance(item, Mapping) for item in record[field]
        ):
            raise EvidenceConflict(f"market projection {field} must be object rows")
    try:
        json.dumps(record, allow_nan=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise EvidenceConflict("market projection must be finite JSON") from error
    return record


def _market_projection_identity(value: Mapping[str, Any]) -> tuple[str, int]:
    encoded = _canonical_json_value(value)
    rows = sum(
        len(value[field])
        for field in (
            "events",
            "venue_events",
            "markets",
            "venue_markets",
            "decisions",
            "selected_markets",
            "relations",
        )
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest(), rows


def _canonical_json_value(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _require_columns(
    row: sqlite3.Row, expected: Mapping[str, Any], label: str
) -> None:
    if any(row[field] != value for field, value in expected.items()):
        raise EvidenceConflict(f"{label} conflicts with its prior projection")


def _seen_run_ids(
    connection: sqlite3.Connection, existing: sqlite3.Row, run_id: str
) -> tuple[str, str]:
    run_ids = {
        str(existing["first_seen_run_id"]),
        str(existing["last_seen_run_id"]),
        run_id,
    }
    rows = connection.execute(
        f"""SELECT run_id, generated_at_ns FROM targeter_runs
            WHERE run_id IN ({','.join('?' for _ in run_ids)})""",
        tuple(sorted(run_ids)),
    ).fetchall()
    if len(rows) != len(run_ids):
        raise EvidenceConflict("first/last-seen run provenance is incomplete")
    ordered = sorted(rows, key=lambda row: (row["generated_at_ns"], row["run_id"]))
    return str(ordered[0]["run_id"]), str(ordered[-1]["run_id"])


def _run_is_newer(
    connection: sqlite3.Connection, run_id: str, other_run_id: str
) -> bool:
    rows = connection.execute(
        """SELECT run_id, generated_at_ns FROM targeter_runs
           WHERE run_id IN (?, ?)""",
        (run_id, other_run_id),
    ).fetchall()
    if len(rows) != 2:
        raise EvidenceConflict("venue projection observation is incomplete")
    timestamps = {str(row["run_id"]): int(row["generated_at_ns"]) for row in rows}
    return (timestamps[run_id], run_id) > (timestamps[other_run_id], other_run_id)


def _event_record(row: sqlite3.Row) -> dict[str, Any]:
    keys = set(row.keys())
    record = {
        "event_id": row["event_id"],
        "identity_version": row["identity_version"],
        "identity_activation_date": row["identity_activation_date"],
        "identity_ordinal": row["identity_ordinal"],
        "sport": row["sport"],
        "game": row["game"],
        "topology": row["topology"],
        "activation_at": row["activation_at"],
        "participants": json.loads(row["participants_json"]),
        "participant_keys": json.loads(row["participant_keys_json"]),
        "event_refs": json.loads(row["event_refs_json"]),
        "first_seen_run_id": row["first_seen_run_id"],
        "last_seen_run_id": row["last_seen_run_id"],
    }
    for field in ("venue_count", "market_count", "selected_run_count"):
        if field in keys:
            record[field] = int(row[field])
    return record


def _event_refs_sql(event_alias: str) -> str:
    return f"""COALESCE((
        SELECT json_group_array(alias.event_ref)
        FROM (
            SELECT venue || ':' || venue_event_id AS event_ref
            FROM venue_events
            WHERE event_id = {event_alias}.event_id
            ORDER BY venue, venue_event_id
        ) alias
    ), '[]')"""


def _canonical_market_record(row: sqlite3.Row) -> dict[str, Any]:
    keys = set(row.keys())
    record = {
        "market_id": row["market_id"],
        "market_template_version": row["market_template_version"],
        "outcome_space_version": row["outcome_space_version"],
        "event_id": row["event_id"],
        "canonical_class": row["canonical_class"],
        "market_type": row["market_type"],
        "scope": row["scope"],
        "parameters": json.loads(row["parameters_json"]),
        "first_seen_run_id": row["first_seen_run_id"],
        "last_seen_run_id": row["last_seen_run_id"],
    }
    if "venue_market_count" in keys:
        record["venue_market_count"] = int(row["venue_market_count"])
        record["venues"] = sorted(
            item for item in str(row["venues"] or "").split(",") if item
        )
    return record


def _venue_market_record(row: sqlite3.Row) -> dict[str, Any]:
    record = _row_record(row)
    record["parameters"] = json.loads(record.pop("parameters_json"))
    record["subscription_ids"] = json.loads(record.pop("subscription_ids_json"))
    record["outcome_labels"] = json.loads(record.pop("outcome_labels_json"))
    record["accepting_orders"] = bool(record["accepting_orders"])
    return record


def _range(start: int | None, end: int | None, label: str) -> None:
    for value in (start, end):
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or value < 0
        ):
            raise ValueError(f"{label} bounds must be non-negative integers")
    if start is not None and end is not None and start >= end:
        raise ValueError(f"{label}_start must be before {label}_end")


def _limit(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("limit must be a positive integer")
    return min(value, 1000)


def _ensure_detail_rows(
    groups: tuple[list[Any], ...], label: str, limit: int | None = None
) -> None:
    bound = limits.DETAIL_ROW_LIMIT if limit is None else limit
    if any(len(rows) > bound for rows in groups):
        raise DetailTooLarge(f"{label} exceeds the child-row limit")


def _bounded_detail(record: dict[str, Any], label: str) -> dict[str, Any]:
    if (
        len(_canonical_json_value(record).encode("utf-8"))
        > limits.EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES
    ):
        raise DetailTooLarge(f"{label} exceeds the byte limit")
    return record


def _sync_failure_range(
    key_start: str | None,
    key_end: str | None,
    *,
    conjunction: bool = True,
) -> tuple[str, tuple[str, str] | tuple[()]]:
    if (key_start is None) != (key_end is None):
        raise ValueError("sync failure key bounds must both be set")
    if key_start is None:
        return "", ()
    operator = "AND" if conjunction else "WHERE"
    return f"{operator} manifest_key >= ? AND manifest_key < ?", (key_start, key_end)


def _nonempty(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise EvidenceConflict(f"{label} must be non-empty text")
    return value


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _nonempty(value, label)


def _text_list(value: Any, label: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not all(isinstance(item, str) and item for item in value)
        or len(value) != len(set(value))
    ):
        raise EvidenceConflict(f"{label} must contain unique non-empty text")
    return list(value)


def _canonical_timestamp(value: Any, label: str) -> str:
    parsed = parse_timestamp(value)
    if parsed is None:
        raise EvidenceConflict(f"{label} must be a UTC timestamp")
    canonical = isoformat(parsed)
    if value != canonical:
        raise EvidenceConflict(f"{label} must use canonical UTC form")
    return canonical


def _timestamp_ns(value: str) -> int:
    parsed = parse_timestamp(value)
    if parsed is None:
        raise EvidenceConflict("timestamp must be valid")
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _isoformat_ns(value: int) -> str:
    return isoformat(
        datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc)
    )


def _sha256(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise EvidenceConflict(f"{label} is invalid")
    return value


def _venue_prefix(reference: str) -> str:
    venue, separator, identifier = reference.partition(":")
    if not separator or not venue or not identifier:
        raise EvidenceConflict(f"reference has no venue prefix: {reference!r}")
    return venue
