"""Closed target-record evidence and its canonical execution-terms projection."""

from __future__ import annotations

from typing import Any, Mapping

from replay.catalog import canonical_sha256, project_record
from replay.execution_terms import instant_ns
from replay.streams.protocol import ProtocolError


class ExecutionTermsError(ValueError):
    """A target record cannot be admitted as execution-terms evidence."""


_FIELDS = {
    "version", "run_id", "venue", "target_id", "subscription_ids", "observed_at",
    "provenance", "projection_id", "projection_sha256", "record_sha256", "record",
}
_SQLITE_MAX = (1 << 63) - 1


def observation_ns(value: Any) -> int:
    """The exact UTC source timestamp; never canonicalize or round its bytes."""
    try:
        return instant_ns(value)
    except ProtocolError as error:
        raise ExecutionTermsError("target record observed_at must be an exact UTC timestamp") from error


def target_record_terms(
    row: Mapping[str, Any], *, run_id: str, venue: str
) -> dict[str, Any]:
    """Validate a historical closed row without changing any archived hashes."""
    label = "target record"
    if venue not in {"kalshi", "polymarket", "limitless"}:
        raise ExecutionTermsError(f"{label} venue is unsupported")
    if not isinstance(row, Mapping) or set(row) != _FIELDS:
        raise ExecutionTermsError(f"{label} fields are invalid")
    if type(row["version"]) is not int or row["version"] != 1:
        raise ExecutionTermsError(f"{label} version is unsupported")
    if row["run_id"] != run_id or row["venue"] != venue:
        raise ExecutionTermsError(f"{label} run or venue disagrees with its artifact")
    target_id = row["target_id"]
    if not isinstance(target_id, str) or not target_id.startswith(venue + ":") or not target_id[len(venue) + 1:]:
        raise ExecutionTermsError(f"{label} target_id is invalid")
    subscriptions = row["subscription_ids"]
    if not isinstance(subscriptions, list) or not subscriptions or any(
        not isinstance(item, str) or not item for item in subscriptions
    ) or len(subscriptions) != len(set(subscriptions)):
        raise ExecutionTermsError(f"{label} subscription_ids are invalid")
    observed_at = row["observed_at"]
    observation_ns(observed_at)
    if not isinstance(row["provenance"], str) or row["provenance"] not in {"captured", "asserted"}:
        raise ExecutionTermsError(f"{label} provenance is invalid")
    record = row["record"]
    if not isinstance(record, dict):
        raise ExecutionTermsError(f"{label} record must be an object")
    try:
        record_digest = canonical_sha256(record)
    except (TypeError, ValueError) as error:
        raise ExecutionTermsError(f"{label} record must be finite JSON") from error
    if row["record_sha256"] != record_digest:
        raise ExecutionTermsError(f"{label} record_sha256 disagrees with its record")
    if venue == "kalshi":
        if row["projection_id"] is not None or row["projection_sha256"] is not None:
            raise ExecutionTermsError(f"{label} Kalshi projection must be null")
    else:
        versions = {"polymarket.v1", "polymarket.v2"} if venue == "polymarket" else {"limitless.v1"}
        if not isinstance(row["projection_id"], str) or row["projection_id"] not in versions:
            raise ExecutionTermsError(f"{label} projection_id is unsupported")
        projected = project_record(venue, record)
        if row["projection_id"] == "polymarket.v1":
            # v1 committed the older consumed field set. Reading execution
            # terms never upgrades an old row's projection identity in place.
            projected.pop("secondsDelay", None)
        if row["projection_sha256"] != canonical_sha256(projected):
            raise ExecutionTermsError(f"{label} projection_sha256 disagrees with its record")
    seconds = record.get("secondsDelay") if venue == "polymarket" else None
    delay = seconds * 1000 if type(seconds) is int and seconds >= 0 else None
    if delay is not None and delay > _SQLITE_MAX:
        raise ExecutionTermsError(f"{label} taker delay exceeds the SQLite integer bound")
    return {
        "venue": venue,
        "venue_market_id": target_id.split(":", 1)[1],
        "taker_delay_ms": delay,
        "observed_at": observed_at,
    }
