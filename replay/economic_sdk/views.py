"""Detached per-book views: one read and one walk per changed side per cut."""

from __future__ import annotations

from replay.economic_fills import Fill, walk
from replay.strategy_sdk import plain
from replay.streams.protocol import require

# The book side each retainable ladder is read from.
_LADDER_SIDES = {"ask": "ask", "bid": "bid", "kalshi_complement_ask": "bid",
                 "kalshi_complement_bid": "ask"}

_NO_LADDERS = {}
_NO_AGES = {}


class BookView:
    """Immutable detached state of one book; never references decoder objects.

    ``ladders`` holds, in fill mode only (spec §13), the full best-first ladder
    of each retained side or transform, from the read the view already made.
    """

    __slots__ = ("validity", "reason", "last_change", "bid_present", "ask_present",
                 "best_bid", "best_ask", "fills", "transformed", "levels", "crossed",
                 "ladders", "best_changes", "projection_unit", "pair_counts")

    def __init__(self, validity, reason, last_change, bid_present, ask_present,
                 best_bid, best_ask, fills, transformed, levels, ladders=_NO_LADDERS,
                 best_changes=_NO_AGES, projection_unit=None, pair_counts=None):
        self.validity, self.reason, self.last_change = validity, reason, last_change
        self.bid_present, self.ask_present = bid_present, ask_present
        self.best_bid, self.best_ask = best_bid, best_ask
        self.fills, self.transformed, self.levels = fills, transformed, levels
        self.ladders = ladders
        self.best_changes = best_changes
        self.projection_unit = projection_unit
        # Compact native bid/ask subtotals. Transforms keep their source's
        # pair count; VIEW's existing reservation covers these scalar slots.
        self.pair_counts = (pair_counts if pair_counts is not None else tuple(
            sum(len(f.taken) + len(f.consumed) for f in fills.get(side, {}).values())
            for side in ("bid", "ask")))
        # The book's own best bid strictly above its best ask.
        self.crossed = (best_bid is not None and best_ask is not None
                        and best_bid[0] > best_ask[0])

    def present(self, side):
        return self.bid_present if side == "bid" else self.ask_present


def unavailable_view(time):
    """A book with no observed state (e.g. a time-shifted leg before history)."""
    return BookView("not_initialized", None, time, False, False, None, None,
                    {}, {}, 0)


def read_side(book, side, sizes_atoms):
    """One best-first read of a side and one walk for every size.

    ``Book.levels(side, n)`` bounds only the returned tuple; its selection is a
    Python-level heap that measured slower than the C sort of a full read on
    deep books, so the side is read in full exactly once.
    """
    levels = book.levels(side)
    return levels, walk(levels, sizes_atoms)


def _same(old, new):
    return (old.cost == new.cost and old.filled_atoms == new.filled_atoms
            and old.depth_limited == new.depth_limited and old.consumed == new.consumed
            and old.taken == new.taken)


def complement_ladder(levels, unit):
    """Project a best-first side to the opposite orientation's other side at ``unit - p``.

    Best-first bids become best-first asks and best-first asks best-first bids.
    """
    return tuple((unit - price, quantity) for price, quantity in levels)


def complement_ask(fill, unit):
    """Project a fill to the opposite orientation's other side at ``unit - p``.

    A bid fill becomes an ask fill and an ask fill a bid fill; ``cost`` maps to
    ``unit * filled - cost`` either way.
    """
    return Fill(fill.filled_atoms, unit * fill.filled_atoms - fill.cost, fill.depth_limited,
                tuple((unit - price, quantity) for price, quantity in fill.taken),
                tuple((unit - price, quantity) for price, quantity in fill.consumed))


class ViewBuilder:
    """Builds views for one book key from its static union requirement."""

    __slots__ = ("sides", "sizes", "sizes_atoms", "transforms", "unit", "ladders",
                 "ladder_sides", "ages")

    def __init__(self, requirement, plan, *, ages=False):
        quantity_unit = 10 ** int(plan["quantity_scale"])
        self.sides = requirement.sides
        self.sizes = requirement.sizes_contracts
        self.sizes_atoms = tuple(size * quantity_unit for size in self.sizes)
        self.transforms = requirement.transforms
        self.unit = 10 ** int(plan["price_scale"])
        self.ladders = requirement.ladders
        self.ages = ages
        # A retained ladder depends on every level of its side, so that side is
        # never reused merely because a touched price lies beyond consumed depth.
        self.ladder_sides = frozenset(_LADDER_SIDES[source] for source in self.ladders)
        require(type(self.ladders) is tuple and len(set(self.ladders)) == len(self.ladders)
                and all(source in _LADDER_SIDES for source in self.ladders)
                and all(source in self.sides or source in self.transforms for source in self.ladders)
                and self.ladder_sides <= set(self.sides), "retained ladder requirement")

    def build(self, book, last_change, prior=None, sides=None):
        """Detached view; with ``prior`` and ``sides``, untouched sides are reused.

        ``sides`` names the sides a cut's operations touched on a book that was
        and stays usable, so every other side's fills are provably unchanged.
        """
        reuse = (prior is not None and sides is not None and book.validity == "usable"
                 and prior.validity == "usable")
        fills, presence, best, levels = {}, {}, {}, 0
        pair_counts = [0, 0]
        retained = {} if self.ladders else _NO_LADDERS
        for side in self.sides:
            if reuse and (side not in sides or (side not in self.ladder_sides
                                                and beyond(prior.fills[side], side, sides[side]))):
                fills[side] = prior.fills[side]
                presence[side] = prior.present(side)
                best[side] = prior.best_bid if side == "bid" else prior.best_ask
                if side in self.ladder_sides:
                    retained[side] = prior.ladders[side]
                pair_counts[side == "ask"] = prior.pair_counts[side == "ask"]
            else:
                side_levels, side_fills = read_side(book, side, self.sizes_atoms)
                if side in self.ladder_sides:
                    old = prior.ladders.get(side) if prior is not None else None
                    # Keep the prior object for an equal ladder (identity checks downstream).
                    retained[side] = old if old == side_levels else side_levels
                previous = prior.fills.get(side) if prior is not None else None
                if previous:
                    # Keep the prior object for an equal fill so downstream
                    # input fingerprints compare by identity.
                    side_fills = [old if _same(old, new) else new
                                  for old, new in zip(previous.values(), side_fills)]
                fills[side] = dict(zip(self.sizes, side_fills))
                presence[side] = bool(side_levels)
                best[side] = side_levels[0] if side_levels else None
                if prior is not None:
                    # Keep the prior object for an equal best quote (identity fingerprints).
                    old = prior.best_bid if side == "bid" else prior.best_ask
                    if old == best[side]:
                        best[side] = old
                pair_counts[side == "ask"] = sum(
                    len(f.taken) + len(f.consumed) for f in fills[side].values())
            levels += pair_counts[side == "ask"]
        transformed = {}
        if "kalshi_complement_ask" in self.transforms:
            transformed["kalshi_complement_ask"] = self._project(
                fills["bid"], prior, "bid", "kalshi_complement_ask")
            # Preserve A1's doubling of the *entire* native subtotal, not just bids.
            levels *= 2
        if "kalshi_complement_bid" in self.transforms:
            transformed["kalshi_complement_bid"] = self._project(
                fills["ask"], prior, "ask", "kalshi_complement_bid")
            levels += pair_counts[1]
        for transform, side in (("kalshi_complement_ask", "bid"), ("kalshi_complement_bid", "ask")):
            if transform in self.ladders:
                # Reused while its source ladder is the identical object.
                previous = prior.ladders.get(transform) if prior is not None else None
                retained[transform] = (
                    previous if previous is not None and prior.projection_unit == self.unit
                    and prior.ladders[side] is retained[side]
                    else complement_ladder(retained[side], self.unit))
        for ladder in retained.values():
            levels += len(ladder)
        best_changes = {
            source: (prior.best_changes[source] if prior is not None
                     and prior.ladders[source][:1] == retained[source][:1]
                     else last_change if book.validity != "not_initialized" else None)
            for source in self.ladders} if self.ages else _NO_AGES
        return BookView(book.validity, plain(book.reason), last_change,
                        presence.get("bid", False), presence.get("ask", False),
                        best.get("bid"), best.get("ask"), fills, transformed, levels, retained,
                        best_changes, self.unit, tuple(pair_counts))

    def _project(self, fills, prior, side, transform):
        previous = (prior.transformed.get(transform) if prior is not None
                    and prior.projection_unit == self.unit else None)
        native = prior.fills.get(side) if previous else None
        if native and all(fill is native.get(size) for size, fill in fills.items()):
            return previous
        projected = {}
        for size, fill in fills.items():
            if native and fill is native.get(size):
                old = previous[size]
                # Reuse the projection, but preserve A1's fresh Fill identity
                # on partial rebuilds: selection and ring charges depend on it.
                projected[size] = Fill(old.filled_atoms, old.cost, old.depth_limited,
                                       old.taken, old.consumed)
            else:
                projected[size] = complement_ask(fill, self.unit)
        return projected


def quote_ages(sources, views, time):
    """Ages of each walked ladder's own committed best-level change."""
    rows = []
    for source, view in zip(sources, views):
        best = view.best_changes[source]
        book = None if view.validity == "not_initialized" else time - view.last_change
        require(book is None or book >= 0, "quote age before book update")
        require(best is None or time >= best, "quote age before best update")
        rows.append({"book_ns": None if book is None else str(book),
                     "best_ns": None if best is None else str(time - best)})
    return rows


def touched_sides(cut):
    """Per changed key: ``None`` (rebuild every side) or ``{side: extreme price}``.

    Operations name their side and price. The extreme is the best touched
    price (highest bid, lowest ask): a side whose touched prices are all
    strictly worse than everything its fills consumed is provably unchanged.
    """
    result = {}
    for transition in cut.body["book_transitions"]:
        key = (transition["key"]["instrument"], transition["key"]["orientation"])
        decision = transition["decision"]
        if decision["kind"] != "operations" or result.get(key, ()) is None:
            result[key] = None
            continue
        sides = result.setdefault(key, {})
        for operation in decision["operations"]:
            side, price = operation["side"], int(operation["price"]["atoms"])
            prior = sides.get(side)
            if prior is None or (price > prior if side == "bid" else price < prior):
                sides[side] = price
    return result


def beyond(fills, side, price):
    """True when ``price`` is strictly worse than every level ``fills`` consumed."""
    if not fills:
        return False
    largest = fills[next(reversed(fills))]
    if largest.depth_limited or not largest.consumed:
        return False
    last = largest.consumed[-1][0]
    return price > last if side == "ask" else price < last
