"""Conservative decoding for traditional (non-series) sports fixtures.

Traditional sports have no game family: one fixture is one contest, and the
grammar is driven by vendor title conventions rather than configured aliases.
"""

from __future__ import annotations

import html
import re


_SUFFIX = re.compile(
    r"\s+-\s+(?:more markets|exact score|correct score|halftime result|"
    r"first half result|second half result|first team to score|total corners)\s*$",
    re.IGNORECASE,
)
_PRODUCT_SUFFIX = re.compile(
    r"\s*:\s*(?:spread|match winner|game winner|winner|total(?: goals)?|"
    r"both teams to score|correct score|exact score)\s*$",
    re.IGNORECASE,
)
_VERSUS = re.compile(r"\s+(?:vs\.?|versus|v\.?|@)\s+", re.IGNORECASE)


def strip_event_suffix(title: str) -> str:
    without_fragment = _SUFFIX.sub("", str(title or ""))
    return _PRODUCT_SUFFIX.sub("", without_fragment).strip()


def parse_participants(title: str) -> tuple[str, str] | None:
    """Parse a fixture title without venue-specific event identifiers.

    A league prefix such as ``UEL, Benfica vs Hearts`` is metadata rather than
    a participant and is removed.  The function stays deliberately strict:
    an event with three separators is ambiguous and belongs in the report, not
    in an auto-matched bundle.

    It knows nothing about game prefixes or best-of suffixes on purpose.  An
    esports title never reaches here: a classified event uses
    ``esports.parse_participants`` with its family's aliases, and an esports
    event the registry could not classify is dropped by the adapter rather
    than parsed by grammar.  Teaching this function to recognize game names
    would re-create exactly the title-pattern authority that split is meant to
    remove.
    """
    text = strip_event_suffix(html.unescape(str(title or ""))).strip()
    if "," in text:
        prefix, remainder = text.split(",", 1)
        if _VERSUS.search(remainder):
            text = remainder.strip()
    pieces = _VERSUS.split(text)
    if len(pieces) != 2:
        return None
    left, right = (piece.strip(" -") for piece in pieces)
    # Polymarket commonly appends tournament metadata after the right-hand
    # participant: "A vs B (BO3) - Group D". A spaced dash is metadata here;
    # hyphens inside a team name remain untouched.
    if " - " in right:
        right = right.split(" - ", 1)[0].strip()
    if not left or not right:
        return None
    return left, right


def parse_prop_participants(title: str) -> tuple[str, str] | None:
    """Limitless spells soccer props as ``A and B both to score``."""
    text = html.unescape(str(title or ""))
    match = re.match(
        r"^(.+?)\s+and\s+(.+?)\s+(?:both\s+to\s+score|have\s+\d+\s+or\s+more\s+total\s+goals)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).strip(), match.group(2).strip()
