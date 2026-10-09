"""Request framing, query/path validation, cursors, response encoding and error mapping.

Moved unchanged from the former single-module API; ``read_error``, ``body_error``
and ``encode_response`` are the handler's former except-clauses and size check.
"""

from __future__ import annotations

import base64
import json
import re
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Any
from urllib.parse import unquote

from replay.jobs.contracts import ContractError
from targeter.v2.models import isoformat, parse_timestamp
from universe.jobs.auth import AuthError
from universe.jobs.store import ReplayJobError
from universe.store import EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES, DetailTooLarge

class _FramingError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class _RequestError(ValueError):
    """A validation failure whose message is safe to return to the client."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _RequestError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _json_content_type(value: str) -> bool:
    parts = [part.strip().lower() for part in value.split(";")]
    return parts[0] == "application/json" and (
        len(parts) == 1 or (len(parts) == 2 and parts[1] == "charset=utf-8")
    )


def _single_header(headers: Any, field: str) -> str:
    values = headers.get_all(field, []) if hasattr(headers, "get_all") else []
    if not values and isinstance(headers, dict) and field in headers:
        values = [headers[field]]
    if len(values) != 1 or not isinstance(values[0], str):
        raise ReplayJobError(400, f"exactly one {field} header is required")
    return values[0]


def _exact_body(document: dict[str, Any], fields: set[str]) -> None:
    if set(document) != fields:
        missing = sorted(fields - set(document))
        unexpected = sorted(set(document) - fields)
        details = []
        if missing:
            details.append(f"missing fields: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected fields: {', '.join(unexpected)}")
        raise _RequestError("request body fields are invalid; " + "; ".join(details))


def _only(query: dict[str, list[str]], expected: set[str]) -> None:
    unexpected = set(query) - expected
    if unexpected:
        raise ValueError(f"unexpected query parameter: {sorted(unexpected)[0]}")


def _optional(query: dict[str, list[str]], field: str) -> str | None:
    values = query.get(field)
    if values is None:
        return None
    if len(values) != 1 or not values[0]:
        raise ValueError(f"query parameter {field} must appear once and be non-empty")
    return values[0]


def _integer(query: dict[str, list[str]], field: str, *, default: int) -> int:
    raw = _optional(query, field)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"query parameter {field} must be an integer") from error
    if value <= 0 or value > 100:
        raise ValueError(f"query parameter {field} must be between 1 and 100")
    return value


def _optional_positive_integer(
    query: dict[str, list[str]], field: str
) -> int | None:
    raw = _optional(query, field)
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"query parameter {field} must be an integer") from error
    if value <= 0:
        raise ValueError(f"query parameter {field} must be positive")
    return value


def _boolean(query: dict[str, list[str]], field: str) -> bool | None:
    raw = _optional(query, field)
    if raw is None:
        return None
    if raw not in {"true", "false"}:
        raise ValueError(f"query parameter {field} must be true or false")
    return raw == "true"


def _timestamp(query: dict[str, list[str]], field: str) -> int | None:
    raw = _optional(query, field)
    return None if raw is None else _timestamp_ns(raw)


def _timestamp_ns(value: str) -> int:
    parsed = parse_timestamp(value)
    if parsed is None or value != isoformat(parsed):
        raise ValueError("timestamp query parameters must be UTC RFC 3339 timestamps")
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _path_value(value: str, label: str) -> str:
    decoded = unquote(value)
    if not decoded or "/" in decoded:
        raise ValueError(f"invalid {label}")
    return decoded


def _encode_cursor(value: list[Any]) -> str:
    payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor(value: str) -> list[Any]:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("cursor is invalid")
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != value:
            raise ValueError("noncanonical cursor")
        decoded = json.loads(raw)
    except (ValueError, json.JSONDecodeError) as error:
        raise ValueError("cursor is invalid") from error
    if not isinstance(decoded, list):
        raise ValueError("cursor is invalid")
    return decoded


def _replay_jobs_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "replay_jobs"
        or not isinstance(decoded[1], str)
        or not re.fullmatch(r"0|[1-9][0-9]*", decoded[1])
        or not isinstance(decoded[2], str)
    ):
        raise ValueError("cursor does not belong to replay jobs")
    return int(decoded[1]), decoded[2]


def _replay_job_events_cursor(
    value: str | None, job_id: str
) -> int | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "replay_job_events"
        or decoded[1] != job_id
        or not isinstance(decoded[2], str)
        or not re.fullmatch(r"[1-9][0-9]*", decoded[2])
    ):
        raise ValueError("cursor does not belong to this replay job")
    return int(decoded[2])


def _claim_market_cursor(value: str | None) -> tuple[str, str, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 4
        or decoded[0] != "claim_markets"
        or not all(isinstance(item, str) for item in decoded[1:])
    ):
        raise ValueError("cursor is invalid")
    return (decoded[1], decoded[2], decoded[3])


def _run_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "runs"
        or not isinstance(decoded[1], int)
        or not isinstance(decoded[2], str)
    ):
        raise ValueError("cursor does not belong to the runs query")
    return decoded[1], decoded[2]


def _selection_cursor(value: str | None, sort: str) -> tuple[int, str, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 4
        or decoded[0] != sort
        or not isinstance(decoded[1], int)
        or not isinstance(decoded[2], str)
        or not isinstance(decoded[3], str)
    ):
        raise ValueError("cursor does not belong to this selections query")
    return decoded[1], decoded[2], decoded[3]


def _bundle_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "bundles"
        or not isinstance(decoded[1], int)
        or not isinstance(decoded[2], str)
    ):
        raise ValueError("cursor does not belong to the bundles query")
    return decoded[1], decoded[2]


def _event_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "events"
        or not isinstance(decoded[1], int)
        or not isinstance(decoded[2], str)
    ):
        raise ValueError("cursor does not belong to the events query")
    return decoded[1], decoded[2]


def read_error(error: Exception) -> tuple[int, dict[str, Any]] | None:
    """A GET failure's response, or ``None`` for an internal error that must be logged."""
    if isinstance(error, DetailTooLarge):
        return HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": str(error)}
    if isinstance(error, (AuthError, ReplayJobError)):
        return error.status, {"error": str(error)}
    if isinstance(error, (ValueError, TypeError)):
        return HTTPStatus.BAD_REQUEST, {"error": str(error)}
    return None


def body_error(error: Exception) -> tuple[int, dict[str, Any]] | None:
    """A POST/DELETE failure's response (after framing), or ``None`` for an internal error."""
    if isinstance(error, (AuthError, ReplayJobError)):
        return error.status, {"error": str(error)}
    if isinstance(error, (ContractError, _RequestError)):
        return HTTPStatus.BAD_REQUEST, {"error": str(error)}
    if isinstance(error, (ValueError, TypeError)):
        return HTTPStatus.BAD_REQUEST, {"error": "invalid request"}
    return None


def encode_response(status: int, document: dict[str, Any]) -> tuple[int, bytes]:
    """Canonical JSON body, replaced by a 413 when it exceeds the response budget."""
    payload = (
        json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")
    if len(payload) > EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES:
        status = HTTPStatus.REQUEST_ENTITY_TOO_LARGE
        payload = b'{"error":"response exceeds Event Universe size budget"}\n'
    return status, payload
