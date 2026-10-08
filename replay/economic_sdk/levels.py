"""Market profile ``levels`` group: full ladders at anchors and per-cut level changes.

The decoder applies a cut's operations before a strategy sees the books, and
Polymarket operations are absolute (``set``/``delete``), so exact deltas need the
pre-cut quantity of every level. The recorder therefore keeps its own mirror of
every planned book's levels and checks it against the decoder on every cut; a
mismatch fails the run and is never repaired. Kalshi is written in the Yes
(``outcome``) book's terms: a change of the ``complement`` bid at ``q`` is an
``ask`` change at ``P - q`` (``docs/specs/MARKET_PROFILE_V2.md`` section 5).
"""

from __future__ import annotations

from replay.economic_sdk import bounds
from replay.economic_sdk.profile_policy import (
    LEVELS_FILE,
    LEVELS_MAX_BYTES,
    LEVELS_MAX_LINE,
    LEVELS_MAX_ROWS,
    LEVELS_PLAIN,
)
from replay.economic_sdk.transitions import ASK, BID, Stream, validity_text, venue_keys
from replay.streams.protocol import require

LEVEL = bounds.SLOT + 2 * bounds.INT  # one mirrored level: dict slot, price and quantity


class _Mirror:
    __slots__ = ("sides", "best", "valid")

    def __init__(self):
        self.sides = ({}, {})   # price -> quantity atoms, per native side
        self.best = [None, None]
        self.valid = False

    def top(self, side):
        price = self.best[side]
        return None if price is None else (price, self.sides[side][price])

    def settle(self, side):
        levels = self.sides[side]
        self.best[side] = (max(levels) if side == BID else min(levels)) if levels else None


class LevelRecorder(Stream):
    def __init__(self, snapshot, plans, policy, counterpart, root, budget):
        super().__init__(snapshot, plans, policy, counterpart, root, budget,
                         plain=LEVELS_PLAIN, final=LEVELS_FILE, max_rows=LEVELS_MAX_ROWS,
                         max_bytes=LEVELS_MAX_BYTES, max_line=LEVELS_MAX_LINE)
        self.mirror = {key: _Mirror() for key in plans}
        self.validity = {}  # planned key -> validity text as of the last cut
        # The planned keys that feed each written book: itself and, for Kalshi, its complement.
        self.sources = {}
        for key, written in self.written_of.items():
            self.sources.setdefault(written, []).append(key)

    # -- mirror --------------------------------------------------------------------
    def initial(self, cut, scoped):
        for key, mirror in self.mirror.items():
            book = cut.books[key]
            mirror.valid = book.validity == "usable"
            self.validity[key] = validity_text(book)
            for side, name in ((BID, "bid"), (ASK, "ask")):
                levels = dict(book.levels(name))
                self.budget.charge(len(levels) * LEVEL, "profile state budget")
                mirror.sides[side].update(levels)
                mirror.settle(side)
            self._check(key, book)
        self.begin(scoped)

    def _check(self, key, book):
        mirror = self.mirror[key]
        require(mirror.top(BID) == book.best_bid() and mirror.top(ASK) == book.best_ask()
                and (mirror.valid or not (mirror.sides[BID] or mirror.sides[ASK])),
                "level mirror diverged from the decoder")

    def _replace(self, mirror, side, levels):
        old = mirror.sides[side]
        self.budget.charge(len(levels) * LEVEL, "profile state budget")
        self.budget.release(len(old) * LEVEL)
        mirror.sides[side].clear()
        mirror.sides[side].update(levels)
        mirror.settle(side)

    def _apply(self, key, transition):
        """Apply one transition; operations return the net quantity change of each level."""
        mirror = self.mirror[key]
        decision = transition["decision"]
        kind = decision["kind"]
        if kind == "snapshot":
            for side, name in ((BID, "bids"), (ASK, "asks")):
                self._replace(mirror, side, {int(p): int(q) for p, q in decision[name]})
            return kind, None
        if kind == "invalidation":
            for side in (BID, ASK):
                self._replace(mirror, side, {})
            return kind, None
        require(kind == "operations", "unknown decision")
        deltas = {}
        for op in decision["operations"]:
            side = BID if op["side"] == "bid" else ASK
            price = int(op["price"]["atoms"])
            change = op["change"]
            levels = mirror.sides[side]
            old = levels.get(price, 0)
            how = change["kind"]
            if how == "set":
                new = int(change["value"]["atoms"])
            elif how == "increase":
                new = old + int(change["value"]["atoms"])
            elif how == "decrease":
                new = old - int(change["value"]["atoms"])
            else:
                require(how == "delete", "unknown level change")
                new = 0
            require(new >= 0, "level mirror underflow")
            if new > 0:
                if price not in levels:
                    self.budget.charge(LEVEL, "profile state budget")
                    top = mirror.best[side]
                    if top is None or (price > top if side == BID else price < top):
                        mirror.best[side] = price
                levels[price] = new
            elif price in levels:
                del levels[price]
                self.budget.release(LEVEL)
                if price == mirror.best[side]:
                    mirror.settle(side)
            if new != old:
                deltas[side, price] = deltas.get((side, price), 0) + new - old
        return kind, deltas

    # -- rows ----------------------------------------------------------------------
    def _ladder(self, key):
        """``(bids, asks)`` of a written book, best first, as ``[price, quantity]`` text pairs."""
        mirror = self.mirror[key]
        if not mirror.valid:
            return [], []
        bids = sorted(mirror.sides[BID].items(), reverse=True)
        if self.plans[key]["venue"] != "kalshi":
            asks = sorted(mirror.sides[ASK].items())
        else:
            asks = []
            for source in self.sources.get(key, ()):
                if source != key and self.mirror[source].valid:
                    unit = self.unit[key]
                    asks = sorted((unit - p, q) for p, q in self.mirror[source].sides[BID].items())
        return ([[str(p), str(q)] for p, q in bids], [[str(p), str(q)] for p, q in asks])

    def _opening(self, scope, key, at, sequence):
        bids, asks = self._ladder(key)
        self.writer.append(self._row("ladder", scope, key, sequence, at, cause="open",
                                     validity=self.validity[key], bids=bids, asks=asks))

    def _diff(self, key, deltas):
        """Signed level changes of a written book, in its terms; net-zero levels are omitted."""
        merged = {}
        kalshi = self.plans[key]["venue"] == "kalshi"
        for source in self.sources.get(key, ()):
            changes = deltas.get(source)
            if not changes:
                continue
            for (side, price), delta in changes.items():
                if source == key:
                    if kalshi and side == ASK:
                        continue  # a Kalshi book is bids only; its ask is projected
                    entry = ("ask" if side == ASK else "bid", price)
                else:
                    if side == ASK or not self.mirror[key].valid:
                        continue
                    entry = ("ask", self.unit[key] - price)
                merged[entry] = merged.get(entry, 0) + delta
        return [[side, str(price), str(delta)]
                for (side, price), delta in sorted(merged.items(), key=lambda e: (e[0][0] != "bid", e[0][1]))
                if delta]

    def cut(self, cut, raw, time, scoped, scope):
        sequence = cut.sequence
        gated = raw >= self.start
        if gated:
            self.flush(sequence)
        keys, causes = self._touches(cut)
        by_key = {(t["key"]["instrument"], t["key"]["orientation"]): t for t in cut.body["book_transitions"]}
        applied = {key: self._apply(key, by_key[key]) for key in sorted(keys)}
        for key in keys:
            book = cut.books[key]
            self.mirror[key].valid = book.validity == "usable"
            self.validity[key] = validity_text(book)
            self._check(key, book)
        if not gated:
            return
        clocks, _ = self._scan(cut, False)
        deltas = {key: detail for key, (kind, detail) in applied.items() if kind == "operations"}
        for key in sorted((k for k in causes if k in scoped), key=self.index.__getitem__):
            cause = causes[key]
            if cause != "operations":
                bids, asks = self._ladder(key)
                self.writer.append(self._row("ladder", scope, key, sequence, time, cause=cause,
                                             validity=self.validity[key], bids=bids, asks=asks,
                                             **venue_keys(clocks.get(key, ()))))
                continue
            levels = self._diff(key, deltas)
            if levels:
                self.writer.append(self._row("diff", scope, key, sequence, time, levels=levels,
                                             **venue_keys(clocks.get(key, ()))))
