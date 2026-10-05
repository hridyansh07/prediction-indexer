"""Detached per-book views: one read and one walk per changed side per cut."""

from __future__ import annotations

from replay.economic_fills import Fill, walk
from replay.strategy_sdk import plain

class BookView:
    """Immutable detached state of one book; never references decoder objects."""

    __slots__ = ("validity", "reason", "last_change", "bid_present", "ask_present",
                 "best_bid", "best_ask", "fills", "transformed", "levels", "crossed")

    def __init__(self, validity, reason, last_change, bid_present, ask_present,
                 best_bid, best_ask, fills, transformed, levels):
        self.validity, self.reason, self.last_change = validity, reason, last_change
        self.bid_present, self.ask_present = bid_present, ask_present
        self.best_bid, self.best_ask = best_bid, best_ask
        self.fills, self.transformed, self.levels = fills, transformed, levels
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


def complement_ask(fill, unit):
    """Project a bid fill to the opposite orientation's ask at ``unit - p``."""
    return Fill(fill.filled_atoms, unit * fill.filled_atoms - fill.cost, fill.depth_limited,
                tuple((unit - price, quantity) for price, quantity in fill.taken),
                tuple((unit - price, quantity) for price, quantity in fill.consumed))


class ViewBuilder:
    """Builds views for one book key from its static union requirement."""

    __slots__ = ("sides", "sizes", "sizes_atoms", "transforms", "unit")

    def __init__(self, requirement, plan):
        quantity_unit = 10 ** int(plan["quantity_scale"])
        self.sides = requirement.sides
        self.sizes = requirement.sizes_contracts
        self.sizes_atoms = tuple(size * quantity_unit for size in self.sizes)
        self.transforms = requirement.transforms
        self.unit = 10 ** int(plan["price_scale"])

    def build(self, book, last_change, prior=None, sides=None):
        """Detached view; with ``prior`` and ``sides``, untouched sides are reused.

        ``sides`` names the sides a cut's operations touched on a book that was
        and stays usable, so every other side's fills are provably unchanged.
        """
        reuse = (prior is not None and sides is not None and book.validity == "usable"
                 and prior.validity == "usable")
        fills, presence, best, levels = {}, {}, {}, 0
        for side in self.sides:
            if reuse and (side not in sides or beyond(prior.fills[side], side, sides[side])):
                fills[side] = prior.fills[side]
                presence[side] = prior.present(side)
                best[side] = prior.best_bid if side == "bid" else prior.best_ask
            else:
                side_levels, side_fills = read_side(book, side, self.sizes_atoms)
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
            levels += sum(len(f.taken) + len(f.consumed) for f in fills[side].values())
        transformed = {}
        if "kalshi_complement_ask" in self.transforms:
            previous = prior.transformed.get("kalshi_complement_ask") if prior is not None else None
            if previous and prior.fills.get("bid") is not None and all(
                    fills["bid"][size] is prior.fills["bid"][size] for size in self.sizes):
                transformed["kalshi_complement_ask"] = previous
            else:
                unit = self.unit
                transformed["kalshi_complement_ask"] = {
                    size: complement_ask(fill, unit) for size, fill in fills["bid"].items()}
            levels *= 2
        return BookView(book.validity, plain(book.reason), last_change,
                        presence.get("bid", False), presence.get("ask", False),
                        best.get("bid"), best.get("ask"), fills, transformed, levels)


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
