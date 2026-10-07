"""Market profile ``transitions`` group: one exact row per book change.

The decoder applies a cut's operations before a strategy sees the books, and
Polymarket operations are absolute (``set``/``delete``), so exact flows need the
pre-cut quantity of every level. The recorder therefore keeps its own mirror of
every planned book's levels and checks it against the decoder on every cut; a
mismatch fails the run and is never repaired. Rows are observational: they say
what entered or left a side, never why (``docs/specs/MARKET_PROFILE_V2.md``).
"""

from __future__ import annotations

import os
from pathlib import Path

from encoder import encode_stream
from replay.economic_sdk import bounds
from replay.economic_sdk.profile_policy import (
    TRANSITIONS_FILE,
    TRANSITIONS_MAX_BYTES,
    TRANSITIONS_MAX_ROWS,
    TRANSITIONS_PLAIN,
)
from replay.strategy_sdk import LineWriter
from replay.streams.protocol import require
from replay.supervisor import fsync_directory

BID, ASK = 0, 1
LEVEL = bounds.SLOT + 2 * bounds.INT  # one mirrored level: dict slot, price and quantity


def book_table(snapshot, plans, policy):
    """Planned books of any scope, in index order, with their per-book constants.

    Shared in shape (not code) with the reader, which re-derives it.
    """
    members = {}
    keys = set()
    for scope in snapshot["scopes"]:
        for member in scope["members"]:
            for book in member["books"]:
                key = (book["instrument"], book["orientation"])
                if key in plans:
                    keys.add(key)
                    members.setdefault(key, member["market_id"])
    table = []
    for key in sorted(keys):
        plan = plans[key]
        projected = plan["venue"] == "kalshi"
        other = (key[0], "complement" if key[1] == "outcome" else "outcome")
        table.append({
            "instrument": key[0], "orientation": key[1], "venue": plan["venue"],
            "market_id": members[key], "price_scale": plan["price_scale"],
            "quantity_scale": plan["quantity_scale"],
            "tick_atoms": policy["tick_atoms"][plan["venue"]],
            "ask_source": "projected" if projected else "native",
            "ask_source_book": list(other) if projected and other in plans else None})
    return table


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


class _Clock:
    """Venue time over the events of one book in one cut; kinds are copied, never converted."""

    __slots__ = ("events", "first", "last", "kinds", "resolutions", "sent")

    def __init__(self):
        self.events = 0
        self.first = self.last = self.sent = None
        self.kinds, self.resolutions = set(), set()

    def add(self, value):
        self.events += 1
        if value["event_ns"] is not None:
            at = int(value["event_ns"])
            self.first = at if self.first is None else min(self.first, at)
            self.last = at if self.last is None else max(self.last, at)
            self.kinds.add(value["event_kind"])
            self.resolutions.add(value["event_resolution"])
        if value["sent_ns"] is not None:
            at = int(value["sent_ns"])
            self.sent = at if self.sent is None else max(self.sent, at)

    def record(self):
        def one(values):
            return None if not values else next(iter(values)) if len(values) == 1 else "mixed"
        return {"first_event_ns": None if self.first is None else str(self.first),
                "last_event_ns": None if self.last is None else str(self.last),
                "event_kind": one(self.kinds), "event_resolution": one(self.resolutions),
                "last_sent_ns": None if self.sent is None else str(self.sent),
                "events": self.events}


class TransitionRecorder:
    def __init__(self, snapshot, plans, policy, counterpart, root, budget):
        self.plans, self.counterpart, self.budget = plans, counterpart, budget
        self.start = int(snapshot["config"]["start_ns"])
        self.table = book_table(snapshot, plans, policy)
        self.index = {(b["instrument"], b["orientation"]): i for i, b in enumerate(self.table)}
        self.tick = {key: int(self.table[i]["tick_atoms"]) for key, i in self.index.items()}
        self.unit = {key: 10 ** int(plan["price_scale"]) for key, plan in plans.items()}
        self.mirror = {key: _Mirror() for key in plans}
        self.root = Path(root)
        self.writer = LineWriter(self.root / TRANSITIONS_PLAIN, max_bytes=TRANSITIONS_MAX_BYTES,
                                 max_records=TRANSITIONS_MAX_ROWS, max_line_bytes=bounds.MAX_LINE)

    # -- mirror ------------------------------------------------------------------
    def initial(self, cut):
        for key, mirror in self.mirror.items():
            book = cut.books[key]
            mirror.valid = book.validity == "usable"
            for side, name in ((BID, "bid"), (ASK, "ask")):
                levels = dict(book.levels(name))
                self.budget.charge(len(levels) * LEVEL, "profile state budget")
                mirror.sides[side].update(levels)
                mirror.settle(side)
            self._check(key, book)

    def _check(self, key, book):
        mirror = self.mirror[key]
        require(mirror.top(BID) == book.best_bid() and mirror.top(ASK) == book.best_ask()
                and (mirror.valid or not (mirror.sides[BID] or mirror.sides[ASK])),
                "transition level mirror diverged from the decoder")

    def _replace(self, mirror, side, levels):
        old = mirror.sides[side]
        self.budget.charge(len(levels) * LEVEL, "profile state budget")
        self.budget.release(len(old) * LEVEL)
        mirror.sides[side].clear()
        mirror.sides[side].update(levels)
        mirror.settle(side)

    def _apply(self, key, transition):
        """Apply one transition; operations return per-side exact flows."""
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
        before = (mirror.best[BID], mirror.best[ASK])
        # added, removed, best added, best removed, best level reached zero
        flows = ([0, 0, 0, 0, False], [0, 0, 0, 0, False])
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
            require(new >= 0, "transition level mirror underflow")
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
            flow = flows[side]
            if new > old:
                flow[0] += new - old
            else:
                flow[1] += old - new
            if price == before[side]:
                if new > old:
                    flow[2] += new - old
                else:
                    flow[3] += old - new
                if new == 0:
                    flow[4] = True
        return kind, (flows, before)

    def _quote(self, key, side):
        """Best ``(price, quantity)``; a Kalshi ask is the counterpart bid at ``P - p``."""
        mirror = self.mirror[key]
        if self.plans[key]["venue"] == "kalshi" and side == ASK:
            other = self.counterpart.get(key)
            if other is None or not mirror.valid or not self.mirror[other].valid:
                return None
            top = self.mirror[other].top(BID)
            return None if top is None else (self.unit[key] - top[0], top[1])
        return mirror.top(side)

    # -- events ------------------------------------------------------------------
    @staticmethod
    def _events(cut, wanted):
        trades, book_clock, trade_clock = {}, {}, {}
        for observation in cut.body["market_events"]:
            event = observation["event"]
            if event["kind"] == "trade":
                value = event["value"]
                key = (value["instrument"], value["orientation"])
                if key not in wanted or observation["disposition"] == "duplicate":
                    continue
                trade = trades.setdefault(key, {"count": 0, "scales": [],
                                                "aggressor": {"bid": 0, "ask": 0, "none": 0}})
                trade["count"] += 1
                trade["aggressor"][value["aggressor"] or "none"] += 1
                trade["scales"].append((value["price"]["scale"], value["quantity"]["scale"],
                                        int(value["quantity"]["atoms"])))
                clock = value.get("venue_time")
                if clock is not None:
                    trade_clock.setdefault(key, _Clock()).add(clock)
            elif event["kind"] == "book":
                inner = event["value"]["value"]
                key = (inner["instrument"], inner["orientation"])
                clock = inner.get("venue_time")
                if key in wanted and clock is not None:
                    book_clock.setdefault(key, _Clock()).add(clock)
        return trades, book_clock, trade_clock

    # -- rows --------------------------------------------------------------------
    def cut(self, cut, raw, time, scoped, scope):
        transitions = cut.body["book_transitions"]
        if not transitions:
            return
        ordered = {}
        for transition in transitions:
            key = (transition["key"]["instrument"], transition["key"]["orientation"])
            require(key in self.mirror, "unplanned transition")
            require(key not in ordered, "duplicate book transition in one cut")
            ordered[key] = transition
        keys = sorted(ordered)
        emit = [key for key in keys if raw >= self.start and key in scoped]
        # Quotes before the cut, for every row, before any transition of the cut applies.
        before = {key: (self._quote(key, BID), self._quote(key, ASK)) for key in emit}
        applied = {key: self._apply(key, ordered[key]) for key in keys}
        for key in keys:
            book = cut.books[key]
            self.mirror[key].valid = book.validity == "usable"
            self._check(key, book)
        if not emit:
            return
        trades, book_clock, trade_clock = self._events(cut, set(emit))
        for key in emit:
            self.writer.append(self._row(cut, scope, time, key, applied[key], before[key],
                                         trades.get(key), book_clock.get(key), trade_clock.get(key)))

    def _row(self, cut, scope, time, key, applied, before, trade, book_clock, trade_clock):
        plan, book = self.plans[key], cut.books[key]
        kind, detail = applied
        if book.validity == "usable":
            validity = "usable"
        elif book.validity == "not_initialized":
            validity = "not_initialized"
        else:
            validity = "unusable:" + str((book.reason or {}).get("kind", "unknown"))
        quotes = {"bid": self._quote(key, BID), "ask": self._quote(key, ASK)}
        previous = {"bid": before[0], "ask": before[1]}
        kalshi = plan["venue"] == "kalshi"
        row = {"scope": scope, "book": self.index[key], "t_ns": str(time), "kind": kind,
               "validity": validity}
        for name in ("bid", "ask"):
            row["prev_" + name] = _quote_text(previous[name])
            row[name] = _quote_text(quotes[name])
        for name in ("bid", "ask"):
            for field in ("added", "removed", "best_added", "best_removed"):
                row[f"{name}_{field}"] = None
            row[name + "_depleted"] = None
        reason = {"snapshot": "snapshot", "invalidation": "invalidation"}.get(kind)
        if kind == "operations":
            flows, tops = detail
            counted = (BID,) if kalshi else (BID, ASK)
            for side in counted:
                name = ("bid", "ask")[side]
                added, removed, best_added, best_removed, zeroed = flows[side]
                row[name + "_added"], row[name + "_removed"] = str(added), str(removed)
                if tops[side] is not None:
                    row[name + "_best_added"], row[name + "_best_removed"] = str(best_added), str(best_removed)
                    row[name + "_depleted"] = zeroed
            removed = sum(flows[side][1] for side in counted)
            added = sum(flows[side][0] for side in counted)
            reason = "insert" if removed == 0 and added > 0 else "unknown"
        row["reason"] = reason
        for name in ("bid", "ask"):
            old, new = previous[name], quotes[name]
            move = None if old is None or new is None else new[0] - old[0]
            row[name + "_move_atoms"] = None if move is None else str(move)
            tick = self.tick[key]
            row[name + "_move_ticks"] = None if move is None or move % tick else str(move // tick)
        quantity = 0
        if trade is not None:
            quantity = sum(atoms for price_scale, quantity_scale, atoms in trade["scales"]
                           if price_scale == plan["price_scale"] and quantity_scale == plan["quantity_scale"])
        row["trades"] = {"count": 0 if trade is None else trade["count"], "qty_atoms": str(quantity),
                         "aggressor": {"bid": 0, "ask": 0, "none": 0} if trade is None else trade["aggressor"]}
        row["venue_time"] = None if book_clock is None else book_clock.record()
        row["trade_venue_time"] = None if trade_clock is None else trade_clock.record()
        return row

    # -- commit ------------------------------------------------------------------
    def finish(self):
        """Seal the rows, store them as one Zstandard frame, then drop the plain file."""
        root = self.root
        logical = self.writer.finish()
        plain = root / TRANSITIONS_PLAIN
        stored = root / TRANSITIONS_FILE
        temporary = stored.with_name(stored.name + ".open")
        require(not stored.exists(), "existing output")
        with plain.open("rb") as source, temporary.open("xb") as sink:
            result = encode_stream(source, sink)
            sink.flush()
            os.fsync(sink.fileno())
        require({"sha256": result.logical.sha256, "byte_length": result.logical.byte_length,
                 "records": result.logical.line_count} == logical, "transitions logical identity")
        temporary.rename(stored)
        fsync_directory(root)
        plain.unlink()
        fsync_directory(root)
        return {"logical": logical,
                "stored": {"sha256": result.stored.sha256, "byte_length": result.stored.byte_length}}


def _quote_text(quote):
    return None if quote is None else [str(quote[0]), str(quote[1])]
