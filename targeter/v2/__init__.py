"""Targeter v2: event-family discovery for cross-venue sports capture.

Venue catalogues become a reviewed, scored shadow selection, which durable S3
archival and atomic publication then turn into the live splice target files.
Field decoding lives in :mod:`targeter.v2.parsing`, grouped by grammar:
``esports`` for configured best-of game families, ``traditional`` for
single-contest fixtures.
"""

from targeter.v2.models import (
    CanonicalEvent,
    CanonicalMarket,
    CatalogSnapshot,
    EventBundle,
    Relationship,
)
from targeter.v2.registry import MarketClassRegistry, Strategy, load_strategy
from targeter.v2.selection import SelectionResult, select_targets

__all__ = [
    "CanonicalEvent",
    "CanonicalMarket",
    "CatalogSnapshot",
    "EventBundle",
    "MarketClassRegistry",
    "Relationship",
    "SelectionResult",
    "Strategy",
    "load_strategy",
    "select_targets",
]
