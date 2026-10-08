"""Research iterators over the market profile's ``transitions`` and ``levels`` files.

``iter_transitions`` and ``iter_levels`` stream the decoded rows of a completed
profile output and rebuild what the files deliberately do not store: previous
quotes and moves for ``top`` rows, and the full ladder after each ``levels`` row.
They read the output directory's ``manifest.json`` (the stored identities) and,
for ticks, ``summary.json``'s ``transition_books``. Both identities are verified
before the first row is yielded, through the shared codec; no decoded byte is
written to disk. They are conveniences for analysis code and are not the
independent reader (``profile_reader.validate_profile``), which checks the rows.
"""

from __future__ import annotations

import json
from pathlib import Path

from replay.economic_sdk import bounds
from replay.economic_sdk.profile_policy import LEVELS_MAX_LINE
from replay.economic_sdk.profile_stream_io import stream_lines
from replay.streams.protocol import require


def _identities(path):
    path = Path(path)
    manifest = json.loads((path.parent / "manifest.json").read_bytes())
    entry = manifest["files"][path.name]
    return entry["logical"], entry["stored"]


def _rows(path, line_limit, label):
    logical, stored = _identities(path)
    for line in stream_lines(path, logical, stored, line_limit, label):
        yield json.loads(line)


def iter_transitions(path):
    """Stream ``transitions.ndjson.zst`` rows; ``top`` rows gain ``prev_*`` and move fields.

    Added to each ``top`` row, per (scope, book): ``prev_bid`` and ``prev_ask`` (the
    previous row's quotes in the same scope, ``None`` on the opening row),
    ``bid_move_atoms`` and ``ask_move_atoms`` (integers, ``None`` when either quote is
    absent) and ``bid_move_ticks`` and ``ask_move_ticks`` (integers, ``None`` unless
    the move divides by the book's ``tick_atoms``). Trade rows pass through unchanged.
    """
    path = Path(path)
    books = json.loads((path.parent / "summary.json").read_bytes())["transition_books"]
    previous = {}
    for row in _rows(path, bounds.MAX_LINE, "transitions"):
        if row["type"] == "top":
            key = row["scope"], row["book"]
            before = previous.get(key) if row["cause"] != "open" else None
            tick = int(books[row["book"]]["tick_atoms"])
            for side in ("bid", "ask"):
                old = None if before is None else before[side]
                new = row[side]
                row["prev_" + side] = old
                move = None if old is None or new is None else int(new[0]) - int(old[0])
                row[side + "_move_atoms"] = move
                row[side + "_move_ticks"] = None if move is None or move % tick else move // tick
            previous[key] = {"bid": row["bid"], "ask": row["ask"]}
        yield row


def iter_levels(path):
    """Stream ``levels.ndjson.zst`` rows as ``(row, ladder)``.

    ``ladder`` is the book's ``(bids, asks)`` after the row, each a list of
    ``(price_atoms, quantity_atoms)`` integer pairs, best first. A ``ladder`` row
    replaces the book's ladder; a ``diff`` row is applied to it.
    """
    sides = {}
    for row in _rows(Path(path), LEVELS_MAX_LINE, "levels"):
        key = row["scope"], row["book"]
        if row["type"] == "ladder":
            sides[key] = ({int(p): int(q) for p, q in row["bids"]}, {int(p): int(q) for p, q in row["asks"]})
        else:
            held = sides[key]
            for side, price, delta in row["levels"]:
                levels = held[0 if side == "bid" else 1]
                quantity = levels.get(int(price), 0) + int(delta)
                require(quantity >= 0, "diff drives a level negative")
                if quantity:
                    levels[int(price)] = quantity
                else:
                    levels.pop(int(price), None)
        bids, asks = sides[key]
        yield row, (sorted(bids.items(), reverse=True), sorted(asks.items()))


__all__ = ["iter_levels", "iter_transitions"]
