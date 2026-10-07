"""Standalone strict reader for immutable capture envelopes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

FIELDS_V1 = frozenset(
    {
        "delivery_index",
        "record_id",
        "visible_ns",
        "venue",
        "stream",
        "connection_epoch",
        "local_counter",
        "source_cursor",
        "kind",
        "raw_payload",
    }
)
FIELDS_V2 = FIELDS_V1 | {"envelope_version", "monotonic_ns"}


class EnvelopeError(ValueError):
    pass


@dataclass(frozen=True)
class Envelope:
    envelope_version: int
    delivery_index: int
    record_id: str
    visible_ns: int
    monotonic_ns: int | None
    venue: str
    stream: str
    connection_epoch: str
    local_counter: int
    source_cursor: dict[str, Any] | None
    kind: str
    raw_payload: str
    raw_line_sha256: str

    def payload_json(self) -> Any:
        return json.loads(self.raw_payload)


def parse_envelope(line: bytes) -> Envelope:
    try:
        document = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EnvelopeError(f"not a JSON envelope: {error}") from error
    if not isinstance(document, dict):
        raise EnvelopeError("envelope must be a JSON object")

    version = document.get("envelope_version", 1)
    if version == 1 and "envelope_version" not in document:
        expected = FIELDS_V1
        monotonic_ns = None
    elif version == 2:
        expected = FIELDS_V2
        monotonic_ns = _uint(document.get("monotonic_ns"), "monotonic_ns")
    else:
        raise EnvelopeError(f"unsupported envelope version: {version!r}")

    fields = frozenset(document)
    missing = sorted(expected - fields)
    unknown = sorted(fields - expected)
    if missing or unknown:
        raise EnvelopeError(f"closed envelope mismatch; missing={missing}, unknown={unknown}")

    source_cursor = document["source_cursor"]
    if source_cursor is not None and not isinstance(source_cursor, dict):
        raise EnvelopeError("source_cursor must be an object or null")

    return Envelope(
        envelope_version=int(version),
        delivery_index=_uint(document["delivery_index"], "delivery_index"),
        record_id=_text(document["record_id"], "record_id"),
        visible_ns=_uint(document["visible_ns"], "visible_ns"),
        monotonic_ns=monotonic_ns,
        venue=_text(document["venue"], "venue"),
        stream=_text(document["stream"], "stream"),
        connection_epoch=_text(document["connection_epoch"], "connection_epoch"),
        local_counter=_uint(document["local_counter"], "local_counter"),
        source_cursor=source_cursor,
        kind=_text(document["kind"], "kind"),
        raw_payload=_text(document["raw_payload"], "raw_payload", allow_empty=True),
        raw_line_sha256=hashlib.sha256(line).hexdigest(),
    )


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EnvelopeError(f"{field} must be a non-negative integer")
    return value


def _text(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise EnvelopeError(f"{field} must be a {'possibly empty ' if allow_empty else ''}string")
    return value
