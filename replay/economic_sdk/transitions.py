"""Market profile ``transitions`` group: top-of-book rows and trade rows.

``top`` rows are written when a book's ``(bid, ask, validity)`` changes, and on
every snapshot or invalidation; ``trade`` rows are one per observed trade. No
level mirror is kept here: quotes are read from the decoder's books after the
cut. Depth belongs to the ``levels`` group (``levels.py``). Kalshi is written
only as the ``outcome`` (Yes) book, whose ask is projected from the
``complement`` book's best bid. Rows are observational: they say what the
book showed, never why (``docs/specs/MARKET_PROFILE_V2.md`` section 4).

This module also holds the machinery both streams share: the written-book table,
flat venue-time keys, scope-entry rows and the commit of one Zstandard frame.
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
# A touching transition's strength: the strongest one of a cut names a row's cause.
RANK = {"operations": 0, "snapshot": 1, "invalidation": 2}
STATE = 512  # one remembered book state, upper bound with the key and tuples


def written_key(key, plans, present):
    """The book a plan key is written as: a Kalshi ``complement`` folds into its Yes book."""
    if key[1] == "complement" and plans[key]["venue"] == "kalshi" and (key[0], "outcome") in present:
        return key[0], "outcome"
    return key


def book_table(snapshot, plans, policy):
    """Written books of any scope, in index order, with their per-book constants.

    Shared in shape (not code) with the reader, which re-derives it.
    """
    members = {}
    present = set()
    for scope in snapshot["scopes"]:
        for member in scope["members"]:
            for book in member["books"]:
                key = (book["instrument"], book["orientation"])
                if key in plans:
                    present.add(key)
                    members.setdefault(key, member["market_id"])
    table = []
    for key in sorted({written_key(k, plans, present) for k in present}):
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


def venue_keys(values):
    """Flat optional venue-time keys over the ``venue_time`` records of one row.

    Kinds and resolutions are copied, never converted; absent keys are omitted.
    """
    first = last = sent = None
    kinds, resolutions = set(), set()
    for value in values:
        if value["event_ns"] is not None:
            at = int(value["event_ns"])
            first = at if first is None else min(first, at)
            last = at if last is None else max(last, at)
            kinds.add(value["event_kind"])
            resolutions.add(value["event_resolution"])
        if value["sent_ns"] is not None:
            at = int(value["sent_ns"])
            sent = at if sent is None else max(sent, at)

    def one(names):
        return next(iter(names)) if len(names) == 1 else "mixed"

    row = {}
    if last is not None:
        row["venue_ns"] = str(last)
        if first != last:
            row["venue_first_ns"] = str(first)
        row["venue_kind"], row["venue_res"] = one(kinds), one(resolutions)
    if sent is not None:
        row["sent_ns"] = str(sent)
    return row


def validity_text(book):
    if book.validity == "usable" or book.validity == "not_initialized":
        return book.validity
    return "unusable:" + str((book.reason or {}).get("kind", "unknown"))


def quote_text(quote):
    return None if quote is None else [str(quote[0]), str(quote[1])]


class Stream:
    """One written stream: scope-entry rows, the cut scan and the committed frame."""

    def __init__(self, snapshot, plans, policy, counterpart, root, budget, *, plain, final,
                 max_rows, max_bytes, max_line):
        self.plans, self.counterpart, self.budget = plans, counterpart, budget
        self.start = int(snapshot["config"]["start_ns"])
        self.table = book_table(snapshot, plans, policy)
        self.index = {(b["instrument"], b["orientation"]): i for i, b in enumerate(self.table)}
        present = {(b["instrument"], b["orientation"]) for scope in snapshot["scopes"]
                   for m in scope["members"] for b in m["books"] if (b["instrument"], b["orientation"]) in plans}
        # Every planned key that rows can refer to, mapped to the written book it feeds.
        self.written_of = {key: written_key(key, plans, present) for key in present}
        self.unit = {key: 10 ** int(plan["price_scale"]) for key, plan in plans.items()}
        self.root, self.final, self.plain = Path(root), final, plain
        self.writer = LineWriter(self.root / plain, max_bytes=max_bytes, max_records=max_rows,
                                 max_line_bytes=max_line)
        self.budget.charge(len(plans) * STATE, "profile state budget")
        self.sequence = 0
        self.lazy = None  # scope 0 opens at the first gated cut, once pre-start cuts are applied

    # -- scope entry ---------------------------------------------------------------
    def begin(self, scoped):
        self.lazy = (0, self.start, frozenset(scoped))

    def open_scope(self, scope, at, scoped, sequence):
        """Rows for a scope the collector just crossed into, from the state before this cut."""
        self.flush(sequence)
        self._open(scope, at, scoped, sequence)

    def flush(self, sequence):
        if self.lazy is not None:
            (scope, at, scoped), self.lazy = self.lazy, None
            self._open(scope, at, scoped, sequence)

    def _open(self, scope, at, scoped, sequence):
        for key in sorted((k for k in scoped if k in self.index), key=self.index.__getitem__):
            self._opening(scope, key, at, sequence)

    def _opening(self, scope, key, at, sequence):
        raise NotImplementedError

    # -- cut scan ------------------------------------------------------------------
    def _touches(self, cut):
        """Planned keys with a transition, and the strongest cause per written book."""
        keys, causes, seen = [], {}, set()
        for transition in cut.body["book_transitions"]:
            key = (transition["key"]["instrument"], transition["key"]["orientation"])
            require(key in self.plans, "unplanned transition")
            require(key not in seen, "duplicate book transition in one cut")
            seen.add(key)
            keys.append(key)
            written = self.written_of.get(key)
            kind = transition["decision"]["kind"]
            if written is not None and RANK[kind] >= RANK.get(causes.get(written), -1):
                causes[written] = kind
        return keys, causes

    def _scan(self, cut, want_trades):
        """Venue-time records of each written book's book events, and the trade observations."""
        clocks, trades = {}, []
        for observation in cut.body["market_events"]:
            event = observation["event"]
            if event["kind"] == "book":
                inner = event["value"]["value"]
                written = self.written_of.get((inner["instrument"], inner["orientation"]))
                clock = inner.get("venue_time")
                if written is not None and clock is not None:
                    clocks.setdefault(written, []).append(clock)
            elif want_trades and event["kind"] == "trade":
                trades.append(observation)
        return clocks, trades

    def _row(self, kind, scope, key, sequence, time, **fields):
        return {"type": kind, "scope": scope, "book": self.index[key], "cut": sequence,
                "t_ns": str(time), **fields}

    # -- commit --------------------------------------------------------------------
    def finish(self):
        """Seal the rows, store them as one Zstandard frame, then drop the plain file."""
        root = self.root
        logical = self.writer.finish()
        plain = root / self.plain
        stored = root / self.final
        temporary = stored.with_name(stored.name + ".open")
        require(not stored.exists(), "existing output")
        with plain.open("rb") as source, temporary.open("xb") as sink:
            result = encode_stream(source, sink)
            sink.flush()
            os.fsync(sink.fileno())
        require({"sha256": result.logical.sha256, "byte_length": result.logical.byte_length,
                 "records": result.logical.line_count} == logical, self.final + " logical identity")
        temporary.rename(stored)
        fsync_directory(root)
        plain.unlink()
        fsync_directory(root)
        return {"logical": logical,
                "stored": {"sha256": result.stored.sha256, "byte_length": result.stored.byte_length}}


class TransitionRecorder(Stream):
    def __init__(self, snapshot, plans, policy, counterpart, root, budget):
        super().__init__(snapshot, plans, policy, counterpart, root, budget,
                         plain=TRANSITIONS_PLAIN, final=TRANSITIONS_FILE, max_rows=TRANSITIONS_MAX_ROWS,
                         max_bytes=TRANSITIONS_MAX_BYTES, max_line=bounds.MAX_LINE)
        self.state = {}   # planned key -> (validity, best bid, best ask), as of the last cut
        self.last = {}    # written key -> (validity, bid, ask) of the previous row in the open scope

    # -- state ---------------------------------------------------------------------
    def initial(self, cut, scoped):
        for key in self.plans:
            self._remember(key, cut.books[key])
        self.begin(scoped)

    def _remember(self, key, book):
        validity = validity_text(book)
        if validity != "usable":
            self.state[key] = (validity, None, None)
        else:
            ask = None if self.plans[key]["venue"] == "kalshi" else book.best_ask()
            self.state[key] = (validity, book.best_bid(), ask)

    def _top(self, key):
        """``(validity, bid, ask)``; a Kalshi ask is the counterpart bid at ``P - p``."""
        validity, bid, ask = self.state[key]
        if validity != "usable":
            return validity, None, None
        if self.plans[key]["venue"] == "kalshi":
            ask = None
            other = self.counterpart.get(key)
            if other is not None and self.state[other][0] == "usable":
                source = self.state[other][1]
                if source is not None:
                    ask = (self.unit[key] - source[0], source[1])
        return validity, bid, ask

    # -- rows ----------------------------------------------------------------------
    def _top_row(self, scope, key, sequence, time, cause, top, extra):
        return self._row("top", scope, key, sequence, time, cause=cause, validity=top[0],
                         bid=quote_text(top[1]), ask=quote_text(top[2]), **extra)

    def _opening(self, scope, key, at, sequence):
        top = self.last[key] = self._top(key)
        self.writer.append(self._top_row(scope, key, sequence, at, "open", top, {}))

    def cut(self, cut, raw, time, scoped, scope):
        sequence = cut.sequence
        gated = raw >= self.start
        if gated:
            self.flush(sequence)
        keys, causes = self._touches(cut)
        for key in keys:
            self._remember(key, cut.books[key])
        if not gated:
            return
        clocks, observed = self._scan(cut, True)
        trades = {}
        for observation in observed:
            value = observation["event"]["value"]
            key = (value["instrument"], value["orientation"])
            if key in scoped and observation["disposition"] != "duplicate" and key in self.index:
                plan = self.plans[key]
                if (value["price"]["scale"] == plan["price_scale"]
                        and value["quantity"]["scale"] == plan["quantity_scale"]):
                    trades.setdefault(key, []).append(observation)
        for key in sorted((k for k in set(causes) | set(trades) if k in scoped), key=self.index.__getitem__):
            cause = causes.get(key)
            if cause is not None:
                top = self._top(key)
                if top != self.last[key] or cause != "operations":
                    self.last[key] = top
                    self.writer.append(self._top_row(scope, key, sequence, time, cause, top,
                                                     venue_keys(clocks.get(key, ()))))
            for observation in trades.get(key, ()):
                value = observation["event"]["value"]
                clock = value.get("venue_time")
                self.writer.append(self._row(
                    "trade", scope, key, sequence, time, price=str(int(value["price"]["atoms"])),
                    qty=str(int(value["quantity"]["atoms"])), aggressor=value["aggressor"],
                    disposition=observation["disposition"],
                    **venue_keys(() if clock is None else (clock,))))
