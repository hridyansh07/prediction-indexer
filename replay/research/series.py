"""Overview and zoom tiles from the profile's ``transitions`` stream (spec §2.4).

One streaming pass. A book's ``top`` state holds from its ``t_ns`` until the book's
next ``top`` row in the same scope, or the scope's end; outside its scopes it is
unknown. Every open state is split at each tile boundary, so a tile is complete,
and written, as soon as the stream passes its end.
"""

from __future__ import annotations

from replay.economic_sdk import bounds
from replay.research.files import Files
from replay.research.jsonio import ResearchError, require, safe_int

SECOND = 1_000_000_000
QUOTE_SIDES = ("bid", "ask")


def overview_grid(start, end, max_buckets):
    """The smallest whole-second bucket that fits the span in ``max_buckets``."""
    span = end - start
    require(span > 0, "capture span is empty")
    seconds = max(1, -(-span // (max_buckets * SECOND)))
    bucket = seconds * SECOND
    return bucket, -(-span // bucket)


class Grid:
    """Per-book bucket accumulators over ``[start, end)``."""

    def __init__(self, start, end, bucket_ns, books):
        self.start, self.end, self.bucket_ns = start, end, bucket_ns
        self.buckets = -(-(end - start) // bucket_ns)
        n = self.buckets
        self.books = [{"bid": {"min": [None] * n, "max": [None] * n, "last": [None] * n},
                       "ask": {"min": [None] * n, "max": [None] * n, "last": [None] * n},
                       "usable_ns": [0] * n, "trades": [0] * n, "trade_qty": [0] * n}
                      for _ in range(books)]

    def add_state(self, book, a, b, bid, ask):
        """A usable state in force over ``[a, b)``; zero-length or outside the grid adds nothing."""
        a, b = max(a, self.start), min(b, self.end)
        if a >= b:
            return
        series = self.books[book]
        first, last = (a - self.start) // self.bucket_ns, (b - 1 - self.start) // self.bucket_ns
        for index in range(first, last + 1):
            low = self.start + index * self.bucket_ns
            high = min(low + self.bucket_ns, self.end)
            series["usable_ns"][index] += min(b, high) - max(a, low)
            for side, quote in (("bid", bid), ("ask", ask)):
                if quote is None:
                    continue
                values = series[side]
                if values["min"][index] is None or quote < values["min"][index]:
                    values["min"][index] = quote
                if values["max"][index] is None or quote > values["max"][index]:
                    values["max"][index] = quote
                values["last"][index] = quote

    def add_trade(self, book, t, qty):
        if self.start <= t < self.end:
            index = (t - self.start) // self.bucket_ns
            self.books[book]["trades"][index] += 1
            self.books[book]["trade_qty"][index] += qty

    def document(self, label):
        books = []
        for index, series in enumerate(self.books):
            usable_ppm = []
            for bucket, usable in enumerate(series["usable_ns"]):
                low = self.start + bucket * self.bucket_ns
                length = min(low + self.bucket_ns, self.end) - low
                usable_ppm.append(usable * 1_000_000 // length)
            books.append({"book": index, "bid": series["bid"], "ask": series["ask"], "usable_ppm": usable_ppm,
                          "trades": series["trades"],
                          "trade_qty": [safe_int(q, "trade_qty") for q in series["trade_qty"]]})
        return {label: 1, "start_ns": str(self.start), "end_ns": str(self.end),
                "bucket_ns": str(self.bucket_ns), "buckets": self.buckets, "books": books}


def _price(quote):
    return None if quote is None else safe_int(quote[0], "price atoms")


def profile_tables(output):
    """The profile summary's written books and scopes, validated for this use."""
    summary = Files(output).document("summary.json", bounds.MAX_METADATA)
    books = summary.get("transition_books")
    scopes = summary.get("transition_scopes")
    require(type(books) is list and books, "profile summary has no transition_books (transitions group off?)")
    require(type(scopes) is list and scopes, "profile summary has no transition_scopes")
    return books, scopes


def transitions_rows(output):
    """Decoded rows of the profile's ``transitions.ndjson.zst``, identities verified first."""
    yield from Files(output).lines("transitions.ndjson")


def build_series(rows, *, books, scopes, max_buckets, tile_minutes, write_tile):
    """Consume ``rows`` once; return the overview document and the tile index.

    ``write_tile(kind, minute, document)`` receives each finished tile in order.
    """
    spans = [(int(s["start_ns"]), int(s["end_ns"])) for s in scopes]
    start, end = spans[0][0], spans[-1][1]
    require(all(a < b for a, b in spans), "a profile scope is empty")
    require(all(spans[i][1] <= spans[i + 1][0] for i in range(len(spans) - 1)), "profile scopes overlap")
    bucket, _ = overview_grid(start, end, max_buckets)
    overview = Grid(start, end, bucket, len(books))
    tile_ns = tile_minutes * 60 * SECOND
    tiles = {"1s": [], "raw": []}
    state = {}         # book -> [since, bid, ask, usable, scope_end, row]
    tile = {"index": -1}

    def close(book, until):
        since, bid, ask, usable, scope_end, _ = state[book]
        stop = min(until, scope_end)
        if usable:
            overview.add_state(book, since, stop, bid, ask)
            if tile["grid"] is not None:
                tile["grid"].add_state(book, since, stop, bid, ask)
        if until >= scope_end:
            del state[book]
        else:
            state[book][0] = until

    def open_tile(index):
        low = start + index * tile_ns
        high = min(low + tile_ns, end)
        live = any(a < high and low < b for a, b in spans)
        tile.update(index=index, low=low, high=high, live=live,
                    grid=Grid(low, high, SECOND, len(books)) if live else None, rows=[],
                    initial=[{"book": b, "scope": s[5]["scope"], "since_ns": s[5]["t_ns"], "validity": s[5]["validity"],
                              "bid": s[5]["bid"], "ask": s[5]["ask"]} for b, s in sorted(state.items())])

    def finish_tile():
        for book in sorted(state):
            close(book, tile["high"])
        if tile["live"]:
            minute = str(tile["index"] * tile_minutes)
            write_tile("1s", minute, tile["grid"].document("tile_version"))
            write_tile("raw", minute, {"tile_version": 1, "start_ns": str(tile["low"]), "end_ns": str(tile["high"]),
                                       "initial": tile["initial"], "rows": tile["rows"]})
            tiles["1s"].append(minute)
            tiles["raw"].append(minute)

    open_tile(0)
    last_t = start
    for row in rows:
        t = int(row["t_ns"])
        require(t >= last_t, "transitions rows are not in time order")
        require(start <= t < end, f"transitions row outside the capture span: {t}")
        last_t = t
        while t >= tile["high"]:
            finish_tile()
            open_tile(tile["index"] + 1)
        book = row["book"]
        require(type(book) is int and 0 <= book < len(books), f"transitions row book {book!r}")
        if row["type"] == "top":
            if book in state:
                close(book, t)
            scope_end = spans[row["scope"]][1]
            if t < scope_end:
                state[book] = [t, _price(row["bid"]), _price(row["ask"]), row["validity"] == "usable",
                               scope_end, row]
        elif row["type"] == "trade":
            qty = int(row["qty"])
            overview.add_trade(book, t, qty)
            if tile["grid"] is not None:
                tile["grid"].add_trade(book, t, qty)
        else:
            raise ResearchError(f"unknown transitions row type {row['type']!r}")
        if tile["live"]:
            tile["rows"].append({k: v for k, v in row.items() if k != "cut"})
    while True:
        finish_tile()
        if tile["high"] >= end:
            break
        open_tile(tile["index"] + 1)
    return overview.document("overview_version"), tiles
