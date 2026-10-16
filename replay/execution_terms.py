"""Closed execution observations shared by preparation and offline strategies."""

from datetime import datetime, timezone
import re

from replay.streams.protocol import array, choice, obj, require

_INSTANT = re.compile(r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,9}))?Z")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def instant_ns(value):
    """Parse UTC observations without floating point or submicrosecond loss."""
    match = _INSTANT.fullmatch(value) if type(value) is str else None
    require(match is not None, "execution terms UTC timestamp")
    try:
        base = datetime.fromisoformat(match[1]).replace(tzinfo=timezone.utc)
    except ValueError:
        require(False, "execution terms UTC timestamp")
    elapsed = base - _EPOCH
    result = (elapsed.days * 86400 + elapsed.seconds) * 10**9
    result += int((match[2] or "").ljust(9, "0"))
    require(0 <= result <= 2**64 - 1, "execution terms timestamp range")
    return result


def validate_terms(value):
    obj(value, "status changes")
    status = choice(value["status"], "observed unavailable")
    changes = array(value["changes"])
    require(bool(changes) == (status == "observed"), "execution terms availability")
    previous_time, previous_delay = -1, object()
    for change in changes:
        obj(change, "from taker_delay_ms")
        time = instant_ns(change["from"])
        delay = change["taker_delay_ms"]
        require(delay is None or (type(delay) is int and delay >= 0),
                "execution terms delay must be a nonnegative integer or null")
        require(time > previous_time, "execution terms change-point order")
        require(delay != previous_delay, "execution terms changes must be collapsed")
        previous_time, previous_delay = time, delay
    return value
