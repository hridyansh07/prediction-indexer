"""Independent reader for the market profile ``transitions`` group.

It does not import the collector. From the snapshot and the policy it
re-derives the book table, decodes ``transitions.ndjson.zst`` through the shared
codec into a scratch file it creates and removes (both identities are verified
before any row is read), and checks every row on its own and in sequence:

- closed schemas, canonical decimals, scope and book membership, time order;
- per (scope, book) continuity of the best quotes, including the Kalshi
  projected ask, which is recomputed from the counterpart's bid chain;
- exact arithmetic of moves, flows, depletion and the reason;
- consistency with the profile's own rows (activity counts, state durations).

This verifies internal consistency; it is not a second reconstruction of the
book from the tape.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

from encoder import CodecError, LogicalIdentity, StoredIdentity, decode_stream
from replay.economic_sdk import bounds
from replay.economic_sdk.profile_policy import (
    TRANSITIONS_FILE,
    TRANSITIONS_MAX_BYTES,
    TRANSITIONS_MAX_ROWS,
)
from replay.streams.protocol import decode, obj, require

_FIELDS = ("scope book t_ns kind validity prev_bid prev_ask bid ask bid_added bid_removed ask_added "
           "ask_removed bid_best_added bid_best_removed ask_best_added ask_best_removed bid_depleted "
           "ask_depleted bid_move_atoms ask_move_atoms bid_move_ticks ask_move_ticks reason trades "
           "venue_time trade_venue_time")
KINDS = ("snapshot", "operations", "invalidation")
REASONS = ("snapshot", "invalidation", "insert", "unknown")
EVENT_KINDS = ("exchange_event", "book_update", "trade_report", "book_as_of", "mixed")
RESOLUTIONS = ("millisecond", "microsecond", "mixed")
GROUP_ROWS = 100_000  # rows sharing one scope and instant buffered for Kalshi checks
_ZERO = {"bid": 0, "ask": 0, "none": 0}


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


def _optional(value, parse):
    return None if value is None else parse(value)


def identity_record(value):
    """Closed manifest identity of the stored file: logical and stored sides."""
    obj(value, "logical stored")
    logical = obj(value["logical"], "sha256 byte_length records")
    stored = obj(value["stored"], "sha256 byte_length")
    for side in (logical, stored):
        require(type(side["sha256"]) is str and len(side["sha256"]) == 64
                and all(c in "0123456789abcdef" for c in side["sha256"]), "transitions sha256")
        require(type(side["byte_length"]) is int and 0 <= side["byte_length"], "transitions byte length")
    require(type(logical["records"]) is int and 0 <= logical["records"] <= TRANSITIONS_MAX_ROWS
            and logical["byte_length"] <= TRANSITIONS_MAX_BYTES, "transitions budget")
    return logical, stored


def tables(snapshot, policy):
    """Planned books of any scope in index order, scope membership and counterparts."""
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    members, keys, scoped = {}, set(), []
    for scope in snapshot["scopes"]:
        inside = set()
        for member in scope["members"]:
            for book in member["books"]:
                key = (book["instrument"], book["orientation"])
                if key in plans:
                    inside.add(key)
                    members.setdefault(key, member["market_id"])
        keys |= inside
        scoped.append(inside)
    ordered = sorted(keys)
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
    return books, index, [{index[key] for key in inside} for inside in scoped]


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
    require(type(value) is list and len(value) == 2, "transition quote shape")
    price, quantity = _big(value[0]), _big(value[1])
    require(price <= 10 ** int(scale) and quantity > 0, "transition quote range")
    return price, quantity


def _clock(value, limit):
    if value is None:
        return None
    obj(value, "first_event_ns last_event_ns event_kind event_resolution last_sent_ns events")
    first, last = _optional(value["first_event_ns"], _big), _optional(value["last_event_ns"], _big)
    sent = _optional(value["last_sent_ns"], _big)
    require((first is None) == (last is None) == (value["event_kind"] is None)
            == (value["event_resolution"] is None), "venue time event fields")
    require(first is not None or sent is not None, "empty venue time")
    require(first is None or first <= last, "venue time order")
    require(value["event_kind"] is None or value["event_kind"] in EVENT_KINDS, "venue time kind")
    require(value["event_resolution"] is None or value["event_resolution"] in RESOLUTIONS, "venue time resolution")
    require(type(value["events"]) is int and 1 <= value["events"] <= limit, "venue time events")
    return value["events"]


def _row(row, books, scopes, memberships, scope_bounds, start):
    """Everything about one row that needs no other row."""
    obj(row, _FIELDS)
    for name in ("scope", "book"):
        require(type(row[name]) is int and row[name] >= 0, "transition " + name)
    scope, index = row["scope"], row["book"]
    require(scope < len(scopes) and index < len(books) and index in memberships[scope],
            "transition book outside scope")
    book = books[index]
    t = _big(row["t_ns"])
    scope_start, scope_end = scope_bounds[scope]
    require(max(scope_start, start) <= t < scope_end, "transition time outside scope")
    kind = row["kind"]
    require(kind in KINDS, "transition kind")
    validity = row["validity"]
    require(type(validity) is str, "transition validity")
    usable = validity == "usable"
    if kind == "invalidation":
        require(validity.startswith("unusable:") and len(validity) > 9
                and validity[9:].replace("_", "").isalnum(), "invalidation validity")
    else:
        require(usable, "transition validity for kind")
    scale = book["price_scale"]
    quotes = {name: _quote(row[name], scale) for name in ("prev_bid", "prev_ask", "bid", "ask")}
    kalshi = book["venue"] == "kalshi"
    if not usable:
        require(quotes["bid"] is None and quotes["ask"] is None, "unusable book with quotes")
    tick = int(book["tick_atoms"])
    for name in ("bid", "ask"):
        old, new = quotes["prev_" + name], quotes[name]
        move = None if old is None or new is None else new[0] - old[0]
        require(row[name + "_move_atoms"] == (None if move is None else str(move)), "move atoms")
        ticks = None if move is None or move % tick else str(move // tick)
        require(row[name + "_move_ticks"] == ticks, "move ticks")

    flows = {}
    for name in ("bid", "ask"):
        values = {field: row[f"{name}_{field}"] for field in ("added", "removed", "best_added", "best_removed")}
        depleted = row[name + "_depleted"]
        counted = kind == "operations" and not (kalshi and name == "ask")
        if not counted:
            require(all(v is None for v in values.values()) and depleted is None, "flows outside operations")
            continue
        added, removed = _big(values["added"]), _big(values["removed"])
        previous = quotes["prev_" + name]
        if previous is None:
            require(values["best_added"] is None and values["best_removed"] is None and depleted is None,
                    "best flows on an empty side")
            best_added = best_removed = None
        else:
            best_added, best_removed = _big(values["best_added"]), _big(values["best_removed"])
            require(type(depleted) is bool and best_added <= added and best_removed <= removed,
                    "best flows exceed side flows")
            require(not depleted or best_removed >= previous[1], "depletion without removal")
            new = quotes[name]
            if new is not None and new[0] == previous[0]:
                require(new[1] == previous[1] + best_added - best_removed, "best level arithmetic")
            elif new is None or (new[0] < previous[0] if name == "bid" else new[0] > previous[0]):
                require(depleted, "best level worsened without depletion")
            else:
                require(added > 0, "better level without addition")
        if previous is None and quotes[name] is not None:
            require(added > 0, "level appeared without addition")
        flows[name] = (added, removed)
    if kind == "operations":
        added = sum(a for a, _ in flows.values())
        removed = sum(r for _, r in flows.values())
        expected = "insert" if removed == 0 and added > 0 else "unknown"
    else:
        expected = kind
    require(row["reason"] == expected, "transition reason")

    trades = obj(row["trades"], "count qty_atoms aggressor")
    require(type(trades["count"]) is int and trades["count"] >= 0, "trade count")
    quantity = _big(trades["qty_atoms"])
    aggressor = obj(trades["aggressor"], "bid ask none")
    require(all(type(v) is int and v >= 0 for v in aggressor.values())
            and sum(aggressor.values()) == trades["count"] and (trades["count"] > 0 or quantity == 0),
            "trade totals")
    _clock(row["venue_time"], 2**31)
    trade_events = _clock(row["trade_venue_time"], trades["count"])
    require(trade_events is None or trades["count"] > 0, "trade venue time without trades")
    return {"scope": scope, "book": index, "t": t, "kind": kind, "usable": usable, "validity": validity,
            "reason": row["reason"], "trades": trades["count"], **quotes}


class _Chain:
    __slots__ = ("bid", "ask", "usable", "first_t", "last_t", "validity", "implied")

    def __init__(self):
        self.bid = self.ask = self.usable = None
        self.first_t = self.last_t = self.validity = None
        self.implied = {}


def _project(quote, unit):
    return None if quote is None else (unit - quote[0], quote[1])


def validate_transitions(root, snapshot, identity, policy, facts, groups):
    """Check the file against the snapshot, policy and the profile rows' ``facts``.

    Returns the summary additions. ``facts`` carries, per (scope, book key), the
    profile's state durations, first-bucket open quotes and per-bucket activity
    counts; each cross-check applies only when its group produced them.
    """
    root = Path(root)
    logical, stored = identity_record(identity)
    books, index, memberships = tables(snapshot, policy)
    keys = [(b["instrument"], b["orientation"]) for b in books]
    scopes = snapshot["scopes"]
    bounds_by_scope = [(int(s["start_ns"]), int(s["end_ns"])) for s in scopes]
    start = int(snapshot["config"]["start_ns"])
    path = root / TRANSITIONS_FILE
    require(path.is_file() and not path.is_symlink(), "regular transitions required")

    chain, pending, last = {}, {}, None
    buckets = {}          # (scope, book) -> [bucket pointer, per-bucket [rows, trades]]
    kinds, reasons, attached = {}, {}, 0
    count = 0
    group = []

    def opened(scope, book):
        quotes = facts["open"].get((scope, keys[book]))
        if quotes is None:
            return None
        scale = books[book]["price_scale"]
        return {name: _quote(quotes[name], scale) for name in ("bid", "ask")}

    def settle(entries):
        scope, t = entries[0]["scope"], entries[0]["t"]
        scope_start = bounds_by_scope[scope][0]
        trajectory, first = {}, {}
        for r in entries:
            book = books[r["book"]]
            state = chain.get((scope, r["book"]))
            kalshi = book["venue"] == "kalshi"
            prior_usable = None
            if state is None:
                state = chain[scope, r["book"]] = _Chain()
                state.first_t = t
                entry = opened(scope, r["book"]) if t > scope_start else None
                if entry is not None:
                    require(r["prev_bid"] == entry["bid"] and (kalshi or r["prev_ask"] == entry["ask"]),
                            "first transition differs from the scope-entry quotes")
                bid_before = r["prev_bid"]
                first[scope, r["book"]] = r
            else:
                require(r["prev_bid"] == state.bid and (kalshi or r["prev_ask"] == state.ask),
                        "transition chain broken")
                bid_before = state.bid
                prior_usable = state.usable
                state.implied[state.validity] = state.implied.get(state.validity, 0) + t - state.last_t
            trajectory.setdefault(r["book"], [bid_before]).append(r["bid"])
            state.bid, state.usable, state.validity, state.last_t = r["bid"], r["usable"], r["validity"], t
            state.ask = None if kalshi else r["ask"]
            r["prior_usable"] = prior_usable
        for r in entries:
            book = books[r["book"]]
            if book["venue"] != "kalshi":
                continue
            counterpart = book["ask_source_book"]
            if counterpart is None:
                require(r["prev_ask"] is None and r["ask"] is None, "projected ask without a source book")
                continue
            other = index[tuple(counterpart)]
            unit = 10 ** int(book["price_scale"])
            if other in trajectory:
                states = trajectory[other]
            elif (r["scope"], other) in chain:
                states = [chain[r["scope"], other].bid]
            else:
                entry = opened(r["scope"], other)
                states = None if entry is None else [entry["bid"]]
            claim = (r["prev_ask"], r["ask"], r["usable"], r["prior_usable"], unit)
            if states is None:
                pending.setdefault((r["scope"], other), []).append(claim)
            else:
                _projection(claim, {_project(s, unit) for s in states})
        # Claims made before the counterpart's first row hold its entry state.
        for key, r in first.items():
            for claim in pending.pop(key, ()):
                _projection(claim, {_project(r["prev_bid"], claim[4])})

    def _projection(claim, allowed):
        prev_ask, ask, usable, prior_usable, _ = claim
        require(ask in allowed if usable else ask is None, "projected ask differs from the counterpart bid")
        if prior_usable is None:
            require(prev_ask is None or prev_ask in allowed, "projected prior ask differs from the counterpart bid")
        else:
            require(prev_ask in allowed if prior_usable else prev_ask is None,
                    "projected prior ask differs from the counterpart bid")

    with tempfile.TemporaryDirectory(prefix="transitions-read-") as scratch:
        plain = Path(scratch) / "transitions.ndjson"
        try:
            with path.open("rb") as source, plain.open("xb") as sink:
                decode_stream(source, sink,
                              expected_logical=LogicalIdentity(logical["sha256"], logical["byte_length"],
                                                               logical["records"]),
                              expected_stored=StoredIdentity(stored["sha256"], stored["byte_length"]),
                              max_decoded_bytes=logical["byte_length"])
        except CodecError as error:
            require(False, f"transitions codec: {error}")
        with plain.open("rb") as stream:
            while payload := stream.readline(bounds.MAX_LINE + 1):
                require(len(payload) <= bounds.MAX_LINE and payload.endswith(b"\n"), "transition line/truncation")
                count += 1
                require(count <= logical["records"], "transition count")
                r = _row(decode(payload, bounds.MAX_LINE), books, scopes, memberships, bounds_by_scope, start)
                order = (r["scope"], r["t"])
                require(last is None or last <= order, "transition time order")
                if last is not None and order != last:
                    settle(group)
                    group = []
                last = order
                group.append(r)
                require(len(group) <= GROUP_ROWS, "transition group budget")
                kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
                reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
                attached += r["trades"]
                held = facts["buckets"].get((r["scope"], keys[r["book"]]))
                if held is not None and "activity" in groups:
                    pointer = buckets.setdefault((r["scope"], r["book"]), [0, [[0, 0] for _ in held]])
                    while r["t"] >= held[pointer[0]][1]:
                        pointer[0] += 1
                    require(held[pointer[0]][0] <= r["t"], "transition outside its profile bucket")
                    pointer[1][pointer[0]][0] += 1
                    pointer[1][pointer[0]][1] += r["trades"]
    require(count == logical["records"], "transition count")
    if group:
        settle(group)

    # The profile's state durations agree with the row validity sequence. Time before
    # a book's first row is held in its (unrecorded) entry state, one kind.
    for (scope, book), state in chain.items():
        held = dict(state.implied)
        held[state.validity] = held.get(state.validity, 0) + bounds_by_scope[scope][1] - state.last_t
        have = facts["state"].get((scope, keys[book]))
        require(have is not None, "transitions for a book without profile rows")
        lead = state.first_t - bounds_by_scope[scope][0]
        difference = {name: have.get(name, 0) - held.get(name, 0) for name in set(have) | set(held)}
        extra = {name: ns for name, ns in difference.items() if ns}
        require(all(ns > 0 for ns in extra.values()) and sum(extra.values()) == lead and len(extra) <= 1,
                "transition validity disagrees with profile state durations")

    unattached = None
    if "activity" in groups:
        total = 0
        for (scope, key), held in sorted(facts["buckets"].items()):
            if key not in index:
                continue
            seen = buckets.get((scope, index[key]), (0, [[0, 0] for _ in held]))[1]
            for (_, _, counts), (rows, trades) in zip(held, seen, strict=True):
                require(counts is not None and rows == counts[0], "transition rows differ from activity transitions")
                require(trades <= counts[1], "transition trades exceed activity trades")
                total += counts[1]
        unattached = total - attached

    result = {"transition_books": books,
              "transition_scopes": scope_table(snapshot),
              "transition_rows": {"total": count, "by_kind": dict(sorted(kinds.items())),
                                  "by_reason": dict(sorted(reasons.items()))}}
    if unattached is not None:
        result["transition_trades"] = {"attached": attached, "unattached": unattached}
    require(len(str(result)) < bounds.MAX_METADATA, "transition summary budget")
    return result
