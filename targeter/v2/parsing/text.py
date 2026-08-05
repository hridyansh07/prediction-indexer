"""Family-independent text normalization and label classification.

Nothing here knows about a game family or a sport grammar.  These helpers are
shared by every parser and by the venue adapters that select fields to decode.
"""

from __future__ import annotations

import html
import re
import unicodedata
from collections.abc import Sequence
from typing import Any

from targeter.v2.domain import canonical_text


def normalize_label(value: object) -> str:
    """Collapse a vendor label to a stable comparison form."""
    text = html.unescape(str(value or ""))
    text = unicodedata.normalize("NFKC", text).casefold()
    return re.sub(r"\s+", " ", text).strip()


def sport_from_labels(*values: Any) -> str | None:
    labels = " ".join(
        str(item)
        for value in values
        for item in (value if isinstance(value, (list, tuple)) else (value,))
        if item is not None
    ).casefold()
    if any(token in labels for token in ("esport", "dota", "counter-strike", "league of legends")):
        return "esports"
    if any(token in labels for token in ("soccer", "football matches", "fifa", "uefa")):
        return "soccer"
    return None


_GENERIC_TAGS = {
    "sports", "games", "soccer", "esports", "football matches", "props",
    "rewards", "lumy", "limitless",
}


def league_from_labels(values: Sequence[Any]) -> str | None:
    for value in values:
        text = canonical_text(str(value or ""))
        if text and text not in _GENERIC_TAGS and not text.startswith("rewards"):
            return text
    return None
