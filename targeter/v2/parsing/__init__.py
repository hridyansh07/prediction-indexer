"""Scoped field decoders for Targeter v2.

Grouped by the grammar being decoded rather than by venue or by game:

``text``
    Family-independent normalization and sport/league label classification.
``esports``
    The one best-of series grammar shared by every configured game family.
    A new game is a strategy-configuration entry, not a module here.
``traditional``
    Single-contest fixture grammar for non-series sports.
``products``
    Market parameters (line, side, map index, score) keyed by canonical
    ``market_type``.

Import the module, not the function, so the grammar a call belongs to stays
visible at the call site::

    from targeter.v2.parsing import esports, traditional
    participants = esports.parse_participants(title, family.venue_game_aliases[venue])
"""

from targeter.v2.parsing import esports, products, text, traditional

__all__ = ["esports", "products", "text", "traditional"]
