"""The capture envelope: the one record shape every venue splice emits.

The field set is closed per version. Existing v1 records have no version field and
remain readable as exactly the original ten-field shape. New records are v2 and
add an explicit version plus `monotonic_ns`; unknown versions and per-version field
drift are rejected.

Three counters live in the envelope and they answer three different questions:

    delivery_index   this splice's own delivery order, dense across its lifetime
    local_counter    dense within one connection, so a reconnect restarts it
    source_cursor    what the *venue* said about its own continuity, or null

Only the first two are ours, and only the first two are authoritative for replay.
`source_cursor` is evidence about the venue, not an ordering: Polymarket's market
channel carries no sequence at all, so its cursor is `unsequenced` and gap
detection there is a question answered later, from the payload, in the analysis
layer. Ordering never depends on a venue agreeing to number its own messages.
"""

from __future__ import annotations

import json
from typing import Any

VENUE_POLYMARKET = "polymarket"
VENUE_KALSHI = "kalshi"
VENUE_LIMITLESS = "limitless"
VENUE_INTERNAL = "internal"

STREAM_PUBLIC_BOOK = "public_book"
STREAM_PUBLIC_TRADE = "public_trade"

#: A venue's own summary quote — Kalshi's `ticker`. Neither a book nor a trade,
#: and on Kalshi it carries no sequence at all while the other two channels do,
#: so giving it its own stream keeps a feed that proves nothing out of the lane
#: belonging to the one feed on any venue that can prove a loss.
STREAM_PUBLIC_QUOTE = "public_quote"

STREAM_PROCESS = "process"

#: A full book fetched over REST at a moment we chose, rather than pushed at a
#: moment the venue chose. Kept apart from `public_book` because the two are
#: different evidence: a delta belongs to a chain and is worthless out of order,
#: while a snapshot stands alone and re-anchors the chain. Sharing one stream
#: would let a reader fold recovery points into the delta sequence and derive a
#: book that is neither.
STREAM_PUBLIC_SNAPSHOT = "public_snapshot"

#: A feed about the world rather than about a book: sports game state, the
#: underlying spot price a crypto ladder settles against. Nothing here is
#: tradeable, and that is the point — it is the shared input every venue is
#: reacting to, so it is what makes two venues' reaction times comparable.
#:
#: It carries a real `venue`, never `internal`, because whoever operates the feed
#: also owns its delivery latency. Attributing Polymarket's sports socket to a
#: neutral party would launder exactly the bias a skew measurement has to state.
STREAM_REFERENCE_EVENT = "reference_event"

KIND_VENUE_FRAME = "venue_frame"
KIND_CONTROL = "control"
KIND_FAULT = "fault"

#: Emitted in this order so a spool line reads the way the fixture does. The parser
#: is order-agnostic, but a fixed order makes the encoder deterministic, which is
#: what lets a test assert exact bytes.
ENVELOPE_VERSION = 2

ENVELOPE_FIELDS_V1 = (
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
)

ENVELOPE_FIELDS_V2 = (
    "envelope_version",
    "delivery_index",
    "record_id",
    "visible_ns",
    "monotonic_ns",
    "venue",
    "stream",
    "connection_epoch",
    "local_counter",
    "source_cursor",
    "kind",
    "raw_payload",
)

# Current encoder contract. Kept as the public name used by existing callers.
ENVELOPE_FIELDS = ENVELOPE_FIELDS_V2

#: The closed vocabulary both sides must agree on, asserted against the Rust
#: `wire_enum!` declarations by `tests/test_envelope.py`.
#:
#: Drift here is uniquely expensive. The ingester rejects an unknown `stream` or
#: `venue` outright, so a splice that emits a spelling Rust has never heard of
#: writes a spool nothing can read — and it fails at ingest, long after the socket
#: that produced the frames has moved on. Adding a venue or a stream on one side
#: only is therefore a silent capture loss with a delayed alarm, which is the
#: worst shape a bug can have here. The test makes it a red build instead.
WIRE_VOCABULARY = {
    "venue": (VENUE_POLYMARKET, VENUE_KALSHI, VENUE_LIMITLESS, VENUE_INTERNAL),
    "stream": (STREAM_PUBLIC_BOOK, STREAM_PUBLIC_SNAPSHOT, STREAM_PUBLIC_TRADE,
               STREAM_PUBLIC_QUOTE, STREAM_REFERENCE_EVENT, STREAM_PROCESS),
    "kind": (KIND_VENUE_FRAME, KIND_CONTROL, KIND_FAULT),
}


class EnvelopeError(ValueError):
    """A record that the ingester would reject, caught before it reaches the spool."""


def unsequenced_cursor(counter: int) -> dict[str, Any]:
    """For venues that publish no sequence — our own count is the only cursor."""
    return {"type": "unsequenced", "counter": _uint(counter, "counter")}


def snapshot_id_cursor(last_update_id: int) -> dict[str, Any]:
    return {"type": "snapshot", "last_update_id": _uint(last_update_id, "last_update_id")}


def snapshot_time_cursor(source_time_ms: int) -> dict[str, Any]:
    return {"type": "snapshot", "source_time_ms": _uint(source_time_ms, "source_time_ms")}


def update_range_cursor(first: int, last: int, previous_last: int) -> dict[str, Any]:
    return {
        "type": "update_range",
        "first": _uint(first, "first"),
        "last": _uint(last, "last"),
        "previous_last": _uint(previous_last, "previous_last"),
    }


def build_envelope(
    *,
    delivery_index: int,
    record_id: str,
    visible_ns: int,
    monotonic_ns: int | None = None,
    venue: str,
    stream: str,
    connection_epoch: str,
    local_counter: int,
    kind: str,
    raw_payload: str,
    source_cursor: dict[str, Any] | None = None,
    envelope_version: int = ENVELOPE_VERSION,
) -> dict[str, Any]:
    """Assembles one envelope, validating what the ingester's parser would reject.

    Validation happens here rather than at ingest because a splice that writes an
    unparseable line has already lost the frame: the socket has moved on and there
    is nothing to retry against. Failing at construction keeps the loss inside a
    process that still holds the message.
    """
    common = {
        "delivery_index": _uint(delivery_index, "delivery_index"),
        "record_id": _identifier(record_id, "record_id"),
        "visible_ns": _uint(visible_ns, "visible_ns"),
    }
    tail = {
        "venue": _token(venue, "venue"),
        "stream": _token(stream, "stream"),
        "connection_epoch": _identifier(connection_epoch, "connection_epoch"),
        "local_counter": _uint(local_counter, "local_counter"),
        "source_cursor": source_cursor,
        "kind": _token(kind, "kind"),
        "raw_payload": _payload(raw_payload),
    }
    if envelope_version == 1:
        if monotonic_ns is not None:
            raise EnvelopeError("v1 does not carry monotonic_ns")
        record = {**common, **tail}
        expected = ENVELOPE_FIELDS_V1
    elif envelope_version == 2:
        if monotonic_ns is None:
            raise EnvelopeError("v2 requires monotonic_ns")
        record = {
            "envelope_version": 2,
            **common,
            "monotonic_ns": _uint(monotonic_ns, "monotonic_ns"),
            **tail,
        }
        expected = ENVELOPE_FIELDS_V2
    else:
        raise EnvelopeError(f"unsupported envelope_version: {envelope_version}")
    if tuple(record) != expected:
        raise EnvelopeError(f"envelope field order drifted: {tuple(record)}")
    return record


def encode_envelope(record: dict[str, Any]) -> bytes:
    """One record to one newline-terminated line of UTF-8.

    `ensure_ascii=False` keeps the payload's own bytes intact rather than
    re-encoding them as escapes, and the separators drop the whitespace serde
    would have to skip on every line.
    """
    text = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    if "\n" in text:
        raise EnvelopeError("encoded record contains a newline")
    return text.encode("utf-8") + b"\n"


def _uint(value: Any, field: str) -> int:
    """The ingester parses these straight from their ASCII digits.

    A bool is rejected explicitly because `isinstance(True, int)` holds in Python
    and `json.dumps` would emit `true`, which the digit parser refuses — a failure
    that would otherwise surface a whole layer away from its cause.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise EnvelopeError(f"{field} must be an integer, got {type(value).__name__}")
    if value < 0:
        raise EnvelopeError(f"{field} must be non-negative, got {value}")
    return value


def _token(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise EnvelopeError(f"{field} must be a non-empty string")
    return value


def _identifier(value: Any, field: str) -> str:
    """`record_id` and `connection_epoch` are read as borrows out of the raw line.

    The parser takes the bytes between the quotes without unescaping them, so any
    backslash, quote, control character, or non-ASCII byte makes the identifier
    something other than what it appears to be.
    """
    text = _token(value, field)
    if not text.isascii():
        raise EnvelopeError(f"{field} must be ASCII: {text!r}")
    if any(character in text for character in ('"', "\\")):
        raise EnvelopeError(f"{field} must not contain quotes or backslashes: {text!r}")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in text):
        raise EnvelopeError(f"{field} must not contain control characters: {text!r}")
    return text


def _payload(value: Any) -> str:
    """The verbatim frame, carried as a JSON string and never as parsed structure.

    A splice normalises nothing. Whatever arrived on the socket is what goes in
    here, including frames we do not recognise, so that a later reading of the
    schema is a code change rather than a re-collection.
    """
    if not isinstance(value, str):
        raise EnvelopeError(f"raw_payload must be a string, got {type(value).__name__}")
    return value
