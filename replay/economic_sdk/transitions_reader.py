"""Independent reader for the market profile ``transitions`` and ``levels`` groups.

It imports neither the collector nor the recorders. From the snapshot and the
policy it re-derives the written-book table, decodes each Zstandard file through
the shared codec in two streaming passes (both identities are verified before any
row is read; no decoded byte is written to disk), and checks every row on its own
and in sequence (``docs/specs/MARKET_PROFILE_V2.md`` section 7.1):

- closed schemas per row type, canonical decimals, scope and book membership,
  time and row order;
- per (scope, book) the opening row, the top-of-book chain, the ladder replay;
- consistency with the profile's own rows (activity counts, state durations,
  bucket opens) and, when both files exist, between the two files.

This verifies internal consistency; it is not a second reconstruction of the
book from the tape.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from replay.economic_sdk import bounds
from replay.economic_sdk.profile_policy import (
    LEVELS_FILE,
    LEVELS_MAX_BYTES,
    LEVELS_MAX_LINE,
    LEVELS_MAX_ROWS,
    TRANSITIONS_FILE,
    TRANSITIONS_MAX_BYTES,
    TRANSITIONS_MAX_ROWS,
)
from replay.economic_sdk.profile_stream_io import stream_lines
from replay.streams.protocol import decode, require

VENUE = ("venue_ns", "venue_first_ns", "venue_kind", "venue_res", "sent_ns")
TOP = "type scope book cut t_ns cause validity bid ask"
TRADE = "type scope book cut t_ns price qty aggressor disposition"
LADDER = "type scope book cut t_ns cause validity bids asks"
DIFF = "type scope book cut t_ns levels"
TOP_CAUSES = ("open", "operations", "snapshot", "invalidation")
LADDER_CAUSES = ("open", "snapshot", "invalidation")
DISPOSITIONS = ("applied", "observed", "not_authority", "invalidated")
EVENT_KINDS = ("exchange_event", "book_update", "trade_report", "book_as_of", "mixed")
RESOLUTIONS = ("millisecond", "microsecond", "mixed")
GROUP_ROWS = 100_000  # rows sharing one cut, buffered for the cross-file join


def _big(value):
    require(type(value) is str and 0 < len(value) <= 64 and value.isascii() and value.isdigit()
            and (value == "0" or value[0] != "0"), "canonical unsigned decimal")
    return int(value)


def _signed(value):
    require(type(value) is str and value and value != "-0", "canonical signed decimal")
    body = value[1:] if value.startswith("-") else value
    require(len(body) <= 64 and body.isascii() and body.isdigit() and (body == "0" or body[0] != "0"),
            "canonical signed decimal")
    return int(value)


def _count(value, name):
    require(type(value) is int and value >= 0, name)
    return value


def identity_record(value):
    """Closed manifest identity of a stored file: logical and stored sides."""
    require(type(value) is dict and set(value) == {"logical", "stored"}, "closed schema")
    logical, stored = value["logical"], value["stored"]
    require(type(logical) is dict and set(logical) == {"sha256", "byte_length", "records"}, "closed schema")
    require(type(stored) is dict and set(stored) == {"sha256", "byte_length"}, "closed schema")
    for side in (logical, stored):
        require(type(side["sha256"]) is str and len(side["sha256"]) == 64
                and all(c in "0123456789abcdef" for c in side["sha256"]), "stream sha256")
        require(type(side["byte_length"]) is int and 0 <= side["byte_length"], "stream byte length")
    require(type(logical["records"]) is int and 0 <= logical["records"] <= TRANSITIONS_MAX_ROWS
            and logical["byte_length"] <= TRANSITIONS_MAX_BYTES, "stream budget")
    return logical, stored


def tables(snapshot, policy):
    """Written books in index order, and the set of book indexes written in each scope.

    A Kalshi ``complement`` folds into its Yes (``outcome``) book whenever that is
    present in some scope; otherwise it is written as itself.
    """
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    members, present, scoped = {}, set(), []
    for scope in snapshot["scopes"]:
        inside = set()
        for member in scope["members"]:
            for book in member["books"]:
                key = (book["instrument"], book["orientation"])
                if key in plans:
                    inside.add(key)
                    members.setdefault(key, member["market_id"])
        present |= inside
        scoped.append(inside)
    ordered = sorted({key for key in present
                      if not (key[1] == "complement" and plans[key]["venue"] == "kalshi"
                              and (key[0], "outcome") in present)})
    index = {key: i for i, key in enumerate(ordered)}
    books = []
    for key in ordered:
        plan = plans[key]
        kalshi = plan["venue"] == "kalshi"
        other = (key[0], "complement" if key[1] == "outcome" else "outcome")
        books.append({"instrument": key[0], "orientation": key[1], "venue": plan["venue"],
                      "market_id": members[key], "price_scale": plan["price_scale"],
                      "quantity_scale": plan["quantity_scale"],
                      "tick_atoms": policy["tick_atoms"][plan["venue"]],
                      "ask_source": "projected" if kalshi else "native",
                      "ask_source_book": list(other) if kalshi and other in plans else None})
    return books, index, [{index[key] for key in inside if key in index} for inside in scoped]


def _instant(value):
    if value is None:
        return None
    require(type(value) is str and value.endswith("Z"), "evidence timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        require(False, "evidence timestamp")
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return str((delta.days * 86400 + delta.seconds) * 10**9 + delta.microseconds * 1000)


def scope_table(snapshot):
    details = {}
    for item in snapshot.get("evidence", ()):
        detail = item["detail"]
        details[detail["run_id"]] = detail
    result = []
    for scope in snapshot["scopes"]:
        detail = details.get(scope["run_id"], {})
        result.append({"start_ns": scope["start_ns"], "end_ns": scope["end_ns"], "run_id": scope["run_id"],
                       "scheduled_start_ns": _instant(detail.get("activation_at")),
                       "capture_start_ns": _instant(detail.get("capture_start_at"))})
    return result


def _quote(value, scale):
    if value is None:
        return None
    require(type(value) is list and len(value) == 2, "quote shape")
    price, quantity = _big(value[0]), _big(value[1])
    require(price <= 10 ** int(scale) and quantity > 0, "quote range")
    return price, quantity


def _ladder(value, scale, descending):
    require(type(value) is list, "ladder shape")
    prices, result = [], {}
    for entry in value:
        price, quantity = _quote(entry, scale)
        prices.append(price)
        result[price] = quantity
    require(prices == sorted(set(prices), reverse=descending), "ladder order")
    return result


def _venue(row, kind):
    """The flat optional venue-time keys of section 3.3; absent keys are omitted, never null."""
    present = {name for name in VENUE if name in row}
    if "venue_ns" in row:
        last = _big(row["venue_ns"])
        require("venue_kind" in row and "venue_res" in row, "venue time kind/resolution")
        require(row["venue_kind"] in EVENT_KINDS and row["venue_res"] in RESOLUTIONS, "venue time kind")
        if "venue_first_ns" in row:
            require(kind != "trade" and _big(row["venue_first_ns"]) < last, "venue time order")
    else:
        require(not present & {"venue_first_ns", "venue_kind", "venue_res"}, "venue time without venue_ns")
    if "sent_ns" in row:
        _big(row["sent_ns"])
    require(present <= set(VENUE), "venue time keys")


def _closed(row, fields):
    required = set(fields.split())
    require(type(row) is dict and set(row) - set(VENUE) == required and required <= set(row), "closed schema")


class _Order:
    """Row order of one file: ``(cut, t)`` never decreases; in a cut, opening rows come first."""

    def __init__(self):
        self.cut = self.t = self.key = self.where = None

    def check(self, cut, t, scope, book, opening, trade):
        require(self.cut is None or (cut >= self.cut and t >= self.t), "row order")
        key = (0, scope, book, 0) if opening else (1, 0, book, 1 if trade else 0)
        if self.cut == cut:
            require(key >= self.key and (key != self.key or trade), "row order within a cut")
        else:
            self.where = None
        if not opening:
            require(self.where in (None, (scope, t)), "rows of one cut span scopes or times")
            self.where = (scope, t)
        self.cut, self.t, self.key = cut, t, key


class _Layout:
    """What both files need about the snapshot: books, scope membership, open times."""

    def __init__(self, snapshot, policy):
        self.books, self.index, self.memberships = tables(snapshot, policy)
        self.keys = [(b["instrument"], b["orientation"]) for b in self.books]
        scopes = snapshot["scopes"]
        self.start = int(snapshot["config"]["start_ns"])
        self.ends = [int(s["end_ns"]) for s in scopes]
        self.opens = [self.start] + self.ends[:-1]

    def row(self, row, kind):
        """Shared fields of a row: scope, book, cut and time, checked against the layout."""
        for name in ("scope", "book", "cut"):
            require(type(row[name]) is int and row[name] >= 0, "row " + name)
        scope, book = row["scope"], row["book"]
        require(scope < len(self.ends) and book < len(self.books) and book in self.memberships[scope],
                "row book outside scope")
        t = _big(row["t_ns"])
        opening = row.get("cause") == "open"
        require(t == self.opens[scope] if opening else self.opens[scope] <= t < self.ends[scope],
                "row time outside scope")
        _venue(row, kind)
        return scope, book, row["cut"], t, opening


class _Tops:
    """Row-by-row checks of ``transitions.ndjson.zst``; yields one group per cut."""

    def __init__(self, root, layout, identity, facts, groups):
        self.layout, self.facts, self.groups = layout, facts, groups
        self.logical, self.stored = identity_record(identity)
        self.path = Path(root) / TRANSITIONS_FILE
        require(self.path.is_file() and not self.path.is_symlink(), "regular transitions required")
        self.books = {}      # (scope, book) -> state
        self.order = _Order()
        self.count = 0
        self.types, self.causes = {}, {}
        self.buckets = {}    # (scope, book) -> [bucket pointer, per-bucket [non-open tops, trades]]

    def _book(self, scope, book):
        return self.books.get((scope, book))

    def row(self, line):
        self.count += 1
        require(self.count <= self.logical["records"], "transition count")
        row = decode(line, bounds.MAX_LINE)
        kind = row.get("type") if type(row) is dict else None
        require(kind in ("top", "trade"), "transition row type")
        _closed(row, TOP if kind == "top" else TRADE)
        layout = self.layout
        scope, book, cut, t, opening = layout.row(row, kind)
        self.order.check(cut, t, scope, book, opening, kind == "trade")
        info = layout.books[book]
        state = self.books.get((scope, book))
        key = layout.keys[book]
        held = self.facts["buckets"].get((scope, key))
        activity = "activity" in self.groups and held is not None
        pointer = None
        if activity and not opening:
            pointer = self.buckets.setdefault((scope, book), [0, [[0, 0] for _ in held]])
            while t >= held[pointer[0]][1]:
                pointer[0] += 1
            require(held[pointer[0]][0] <= t, "row outside its profile bucket")
        if kind == "trade":
            require(state is not None, "trade before the opening row")
            require(row["disposition"] in DISPOSITIONS and row["aggressor"] in (None, "bid", "ask"),
                    "trade fields")
            _big(row["price"]), _big(row["qty"])
            if pointer is not None:
                pointer[1][pointer[0]][1] += 1
            self.types["trade"] = self.types.get("trade", 0) + 1
            return {"type": "trade", "scope": scope, "book": book, "cut": cut}
        cause = row["cause"]
        require(cause in TOP_CAUSES, "top cause")
        validity = row["validity"]
        require(type(validity) is str and (validity in ("usable", "not_initialized") or (
            validity.startswith("unusable:") and len(validity) > 9 and validity[9:].replace("_", "").isalnum())),
            "top validity")
        bid, ask = _quote(row["bid"], info["price_scale"]), _quote(row["ask"], info["price_scale"])
        require(validity == "usable" or (bid is None and ask is None), "unusable book with quotes")
        top = (validity, bid, ask)
        if opening:
            require(state is None, "more than one open row")
            state = self.books[scope, book] = _Held(t)
            state.first = top
            state.pending = True
        else:
            require(state is not None, "first top row is not an open row")
            require(top != state.last or cause in ("snapshot", "invalidation"), "top row without a change")
            if state.pending and t > state.opened:
                self._opened(scope, book, state)
            state.pending = False
            state.implied[state.last[0]] = state.implied.get(state.last[0], 0) + t - state.at
            if pointer is not None:
                pointer[1][pointer[0]][0] += 1
        state.last, state.at = top, t
        self.types["top"] = self.types.get("top", 0) + 1
        self.causes[cause] = self.causes.get(cause, 0) + 1
        return {"type": "top", "scope": scope, "book": book, "cut": cut, "cause": cause,
                "bid": bid, "ask": ask}

    def _opened(self, scope, book, state):
        """The opening quotes equal the profile's quotes at the scope's first bucket open."""
        quotes = self.facts["open"].get((scope, self.layout.keys[book]))
        if quotes is not None:
            scale = self.layout.books[book]["price_scale"]
            first = state.first
            require(first[1] == _quote(quotes["bid"], scale) and first[2] == _quote(quotes["ask"], scale),
                    "opening row differs from the profile bucket open")

    def groups_of(self):
        """Yield ``(cut, rows)`` for each cut, validating as rows form."""
        current, rows = None, []
        for line in stream_lines(self.path, self.logical, self.stored, bounds.MAX_LINE, "transitions"):
            result = self.row(line)
            if current is not None and result["cut"] != current:
                yield current, rows
                rows = []
            current = result["cut"]
            rows.append(result)
            require(len(rows) <= GROUP_ROWS, "transition group budget")
        require(self.count == self.logical["records"], "transition count")
        if rows:
            yield current, rows
        self.finish()

    def finish(self):
        layout = self.layout
        for scope, members in enumerate(layout.memberships):
            for book in members:
                require((scope, book) in self.books, "missing open row")
        for (scope, book), state in self.books.items():
            if state.pending:
                self._opened(scope, book, state)
            held = dict(state.implied)
            held[state.last[0]] = held.get(state.last[0], 0) + layout.ends[scope] - state.at
            have = self.facts["state"].get((scope, layout.keys[book]))
            require(have is not None, "transitions for a book without profile rows")
            require({n: ns for n, ns in held.items() if ns} == {n: ns for n, ns in have.items() if ns},
                    "transition validity disagrees with profile state durations")


class _Held:
    __slots__ = ("opened", "first", "last", "at", "implied", "pending")

    def __init__(self, at):
        self.opened = self.at = at
        self.first = self.last = None
        self.implied = {}
        self.pending = False


class _Replay:
    __slots__ = ("sides", "best")

    def __init__(self, bids, asks):
        self.sides = (bids, asks)
        self.best = [None, None]
        self.settle(0), self.settle(1)

    def settle(self, side):
        levels = self.sides[side]
        self.best[side] = (max(levels) if side == 0 else min(levels)) if levels else None

    def top(self):
        return tuple(None if p is None else (p, self.sides[s][p]) for s, p in enumerate(self.best))


class _Levels:
    """Row-by-row checks of ``levels.ndjson.zst``; replays diffs onto the latest ladder."""

    def __init__(self, root, layout, identity):
        self.layout = layout
        self.logical, self.stored = identity_record(identity)
        self.path = Path(root) / LEVELS_FILE
        require(self.path.is_file() and not self.path.is_symlink(), "regular levels required")
        require(self.logical["records"] <= LEVELS_MAX_ROWS and self.logical["byte_length"] <= LEVELS_MAX_BYTES,
                "levels budget")
        self.books = {}
        self.order = _Order()
        self.count = 0
        self.types, self.causes = {}, {}

    def row(self, line):
        self.count += 1
        require(self.count <= self.logical["records"], "level count")
        row = decode(line, LEVELS_MAX_LINE)
        kind = row.get("type") if type(row) is dict else None
        require(kind in ("ladder", "diff"), "level row type")
        _closed(row, LADDER if kind == "ladder" else DIFF)
        layout = self.layout
        scope, book, cut, t, opening = layout.row(row, kind)
        self.order.check(cut, t, scope, book, opening, False)
        info = layout.books[book]
        scale = info["price_scale"]
        state = self.books.get((scope, book))
        out = {"type": kind, "scope": scope, "book": book, "cut": cut, "cause": None}
        if kind == "ladder":
            cause = row["cause"]
            require(cause in LADDER_CAUSES, "ladder cause")
            validity = row["validity"]
            require(type(validity) is str and (validity in ("usable", "not_initialized") or (
                validity.startswith("unusable:") and len(validity) > 9)), "ladder validity")
            bids = _ladder(row["bids"], scale, True)
            asks = _ladder(row["asks"], scale, False)
            # An unusable book has no ladder. A Kalshi Yes book whose complement is invalidated
            # stays usable: it keeps its bids and loses its projected asks.
            require(validity == "usable" or not (bids or asks), "empty ladder required")
            require(cause != "invalidation" or validity != "usable"
                    or (info["ask_source_book"] is not None and not asks), "invalidation ladder")
            if opening:
                require(state is None, "more than one open ladder")
            else:
                require(state is not None, "first ladder row is not an open row")
            out["before"] = None if state is None else state.top()
            state = self.books[scope, book] = _Replay(bids, asks)
            out["cause"] = cause
            self.causes[cause] = self.causes.get(cause, 0) + 1
        else:
            require(state is not None, "first level row is not an open ladder")
            changes = row["levels"]
            require(type(changes) is list and changes, "diff levels")
            seen = []
            out["before"] = state.top()
            for entry in changes:
                require(type(entry) is list and len(entry) == 3 and entry[0] in ("bid", "ask"), "diff entry")
                side = 0 if entry[0] == "bid" else 1
                price, delta = _big(entry[1]), _signed(entry[2])
                require(price <= 10 ** int(scale) and delta != 0, "diff entry range")
                seen.append((side, price))
                levels = state.sides[side]
                new = levels.get(price, 0) + delta
                require(new >= 0, "diff drives a level negative")
                if new:
                    levels[price] = new
                else:
                    levels.pop(price, None)
            require(seen == sorted(set(seen)), "diff order")
            state.settle(0), state.settle(1)
        self.types[kind] = self.types.get(kind, 0) + 1
        out["after"] = state.top()
        out["opening"] = opening
        return out

    def groups_of(self):
        current, rows = None, []
        for line in stream_lines(self.path, self.logical, self.stored, LEVELS_MAX_LINE, "levels"):
            result = self.row(line)
            if current is not None and result["cut"] != current:
                yield current, rows
                rows = []
            current = result["cut"]
            rows.append(result)
            require(len(rows) <= GROUP_ROWS, "level group budget")
        require(self.count == self.logical["records"], "level count")
        if rows:
            yield current, rows
        for scope, members in enumerate(self.layout.memberships):
            for book in members:
                require((scope, book) in self.books, "missing open ladder")


def _join(tops, levels):
    """Lockstep over the two files: replayed ladders must agree with the top rows, cut by cut."""
    top_now, replay_now = {}, {}
    a, b = tops.groups_of(), levels.groups_of()
    try:
        _lockstep(a, b, top_now, replay_now)
    finally:
        a.close()
        b.close()


def _lockstep(a, b, top_now, replay_now):
    left, right = next(a, None), next(b, None)
    while left is not None or right is not None:
        use_left = right is None or (left is not None and left[0] <= right[0])
        use_right = left is None or (right is not None and right[0] <= left[0])
        top_rows = left[1] if use_left else []
        level_rows = right[1] if use_right else []
        by_top, by_level = {}, {}
        for r in top_rows:
            if r["type"] == "top":
                by_top.setdefault((r["scope"], r["book"]), []).append(r)
        for r in level_rows:
            by_level.setdefault((r["scope"], r["book"]), []).append(r)
        for key in sorted(set(by_top) | set(by_level)):
            for r in by_top.get(key, ()):
                top_now[key] = (r["bid"], r["ask"])
            for r in by_level.get(key, ()):
                replay_now[key] = r["after"]
            opened_top = [r for r in by_top.get(key, ()) if r["cause"] == "open"]
            opened_level = [r for r in by_level.get(key, ()) if r["opening"]]
            later_top = [r for r in by_top.get(key, ()) if r["cause"] != "open"]
            later_level = [r for r in by_level.get(key, ()) if not r["opening"]]
            require(len(opened_top) == len(opened_level), "open rows differ between transitions and levels")
            require(len(later_top) <= 1 and len(later_level) <= 1, "rows of one book in one cut")
            if later_top:
                require(later_level, "top row without a levels row")
                cause = later_top[0]["cause"]
                level = later_level[0]
                require(level["type"] == "diff" if cause == "operations"
                        else level["type"] == "ladder" and level["cause"] == cause,
                        "top row cause differs from the levels row")
            elif later_level:
                level = later_level[0]
                require(level["type"] == "diff" and level["before"] == level["after"],
                        "levels change the top without a top row")
            require(replay_now.get(key) == top_now.get(key), "replayed ladder differs from the top row")
        if use_left:
            left = next(a, None)
        if use_right:
            right = next(b, None)


def _drain(groups):
    for _ in groups:
        pass


def validate_streams(root, snapshot, files, policy, facts, groups):
    """Check the stream files against the snapshot, policy and the profile rows' ``facts``.

    Returns the summary additions. ``facts`` carries, per (scope, book key), the
    profile's state durations, bucket-open quotes and per-bucket activity counts;
    each cross-check applies only when its group produced them.
    """
    layout = _Layout(snapshot, policy)
    tops = levels = None
    if "transitions" in groups:
        tops = _Tops(root, layout, files[TRANSITIONS_FILE], facts, groups)
    if "levels" in groups:
        levels = _Levels(root, layout, files[LEVELS_FILE])
    if tops is not None and levels is not None:
        _join(tops, levels)
    elif tops is not None:
        _drain(tops.groups_of())
    else:
        _drain(levels.groups_of())

    result = {"transition_books": layout.books, "transition_scopes": scope_table(snapshot)}
    if tops is not None:
        skipped = _activity(tops, layout, facts, groups)
        result["transition_rows"] = {"total": tops.count, "by_type": dict(sorted(tops.types.items())),
                                     "by_cause": dict(sorted(tops.causes.items()))}
        result["transition_trades_skipped"] = skipped
    if levels is not None:
        result["level_rows"] = {"total": levels.count, "by_type": dict(sorted(levels.types.items())),
                                "by_cause": dict(sorted(levels.causes.items()))}
    require(len(str(result)) < bounds.MAX_METADATA, "stream summary budget")
    return result


def _activity(tops, layout, facts, groups):
    """Trade and top rows against the profile's per-bucket activity; the skipped-trade counts.

    The counts are derived from the profile rows, so they exist only with ``activity``.
    """
    if "activity" not in groups:
        return None
    skipped = {"scale_mismatch": 0, "unwritten_book": 0}
    written = {key: i for i, key in enumerate(layout.keys)}
    for (scope, key), held in sorted(facts["buckets"].items()):
        if key not in written:
            # A planned book the files never write: a Kalshi complement folded into its Yes book.
            skipped["unwritten_book"] += sum(counts[1] for _, _, counts in held)
            continue
        book = written[key]
        seen = tops.buckets.get((scope, book), (0, [[0, 0] for _ in held]))[1]
        other = layout.books[book]["ask_source_book"]
        companion = facts["buckets"].get((scope, tuple(other))) if other is not None else None
        for i, ((_, _, counts), (rows, trades)) in enumerate(zip(held, seen, strict=True)):
            transitions, nonduplicate, mismatch = counts
            require(trades + mismatch == nonduplicate, "trade rows differ from activity trades")
            extra = 0 if companion is None else companion[i][2][0]
            require(rows <= transitions + extra, "top rows exceed activity transitions")
            skipped["scale_mismatch"] += mismatch
    return skipped
