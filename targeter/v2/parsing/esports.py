"""Bounded decoding for configured best-of esports game families.

Every configured family shares this one grammar; the family supplies only its
venue aliases.  Adding a game is a strategy-configuration change, never a new
parser here.  This module contains no HTTP or vendor pagination code: venue
adapters select the structured fields, these helpers decode them.
"""

from __future__ import annotations

import html
import re
import unicodedata
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from targeter.v2.parsing.text import normalize_label


_SEPARATOR = re.compile(r"\s+(?:vs\.?|versus|v\.|@)\s+", re.IGNORECASE)
_PRODUCT_SUFFIX = re.compile(
    r"\s*:\s*(?:(?:map|game)\s+[1-9][0-9]*|total\s+(?:maps|games)|"
    r"(?:map|game)\s+handicap|match\s+winner)\s*$",
    re.IGNORECASE,
)
_FORMAT_SUFFIX = re.compile(
    r"\s*\((?:bo|best\s+of)\s*[1-9][0-9]?\)(?:\s+-\s+.*)?$",
    re.IGNORECASE,
)
_MORE_MARKETS_SUFFIX = re.compile(r"\s+-\s+more\s+markets\s*$", re.IGNORECASE)
_BEST_OF = re.compile(r"\b(?:bo|best\s+of)\s*([1-9][0-9]?)\b", re.IGNORECASE)
_RULE_TIME = re.compile(
    r"\boriginally\s+scheduled\s+for\s+"
    r"(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+"
    r"(?P<day>[1-9]|[12][0-9]|3[01]),\s+"
    r"(?P<year>[0-9]{4})\s+at\s+"
    r"(?P<hour>[1-9]|1[0-2]):(?P<minute>[0-5][0-9])\s+"
    r"(?P<ampm>AM|PM)\s+(?P<zone>EDT|EST|ET)\b",
    re.IGNORECASE,
)
_MONTHS = {
    name: index
    for index, name in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"),
        start=1,
    )
}


def title_alias_prefix(title: object, aliases: tuple[str, ...]) -> str | None:
    """Return the game alias that prefixes ``title`` as ``"<alias>: ..."``."""
    normalized = normalize_label(title)
    for alias in sorted(map(normalize_label, aliases), key=len, reverse=True):
        if re.match(rf"^{re.escape(alias)}\s*:\s*", normalized):
            return alias
    return None


def parse_participants(title: object, aliases: tuple[str, ...]) -> tuple[str, str] | None:
    text = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", html.unescape(str(title or "")))).strip()
    alias = title_alias_prefix(text, aliases)
    if alias is not None:
        text = re.sub(rf"^{re.escape(alias)}\s*:\s*", "", text, count=1, flags=re.IGNORECASE)
    else:
        separator = _SEPARATOR.search(text)
        if separator is not None and ":" in text[: separator.start()]:
            return None
    # Vendor labels combine these reviewed suffixes in either order. Peel only
    # terminal reviewed forms; never truncate a participant at a general dash.
    previous = None
    while previous != text:
        previous = text
        text = _PRODUCT_SUFFIX.sub("", text).strip()
        text = _FORMAT_SUFFIX.sub("", text).strip()
        text = _MORE_MARKETS_SUFFIX.sub("", text).strip()
    pieces = _SEPARATOR.split(text)
    if len(pieces) != 2:
        return None
    left = pieces[0].strip()
    right = pieces[1].strip()
    return (left, right) if left and right else None


def parse_best_of_values(*texts: object) -> tuple[int, ...]:
    """Return every distinct best-of length stated across ``texts``."""
    return tuple(sorted({int(match.group(1)) for text in texts for match in _BEST_OF.finditer(str(text or ""))}))


def parse_best_of(*texts: object) -> int | None:
    """Return the single stated best-of length, or None when it is ambiguous."""
    values = parse_best_of_values(*texts)
    return values[0] if len(values) == 1 else None


def parse_kalshi_originally_scheduled(value: object) -> datetime | None:
    """Decode only the reviewed Kalshi esports scheduling clause."""
    matches = tuple(_RULE_TIME.finditer(html.unescape(str(value or ""))))
    if not matches:
        return None
    instants: set[datetime] = set()
    for match in matches:
        parts = match.groupdict()
        hour = int(parts["hour"])
        if parts["ampm"].casefold() == "pm" and hour != 12:
            hour += 12
        elif parts["ampm"].casefold() == "am" and hour == 12:
            hour = 0
        try:
            local = datetime(
                int(parts["year"]),
                _MONTHS[parts["month"].casefold()[:3]],
                int(parts["day"]),
                hour,
                int(parts["minute"]),
                tzinfo=ZoneInfo("America/New_York"),
            )
        except ValueError:
            return None

        explicit_zone = parts["zone"].upper()
        if explicit_zone in {"EDT", "EST"} and local.tzname() != explicit_zone:
            return None
        instants.add(local.astimezone(timezone.utc))
    return next(iter(instants)) if len(instants) == 1 else None
