"""Stream consistency proved independently from profile, availability and level rows."""
from collections import Counter, defaultdict
from itertools import groupby

from replay.research.io import digest, need, natural, rows
from replay.research.layout import layout

VENUE_FIELDS = {"venue_ns", "venue_first_ns", "venue_kind", "venue_res", "sent_ns"}
FIELDS = {"top": "type scope book cut t_ns cause validity bid ask",
          "trade": "type scope book cut t_ns price qty aggressor disposition",
          "ladder": "type scope book cut t_ns cause validity bids asks",
          "diff": "type scope book cut t_ns levels"}


def quote(value, maximum):
    if value is None:
        return None
    need(type(value) is list and len(value) == 2, "quote shape")
    p, q = map(natural, value)
    need(p <= maximum and q > 0, "quote range")
    return p, q


def groups(root, name, expected, snapshot, books, membership):
    previous = None
    for cut, lines in groupby(rows(root, name, expected), key=lambda r: r["cut"]):
        group = []
        need(type(cut) is int and cut >= 0, "cut range")
        for r in lines:
            kind = r["type"]
            need(kind in FIELDS and set(r) - VENUE_FIELDS == set(FIELDS[kind].split()), "stream closed schema")
            s, b, t = r["scope"], r["book"], natural(r["t_ns"])
            need(type(s) is int and 0 <= s < len(membership) and type(b) is int and b in membership[s],
                 "row outside scope")
            scope = snapshot["scopes"][s]
            need(int(scope["start_ns"]) <= t < int(scope["end_ns"]), "time outside scope")
            opening = r.get("cause") == "open"
            need(not opening or t == int(scope["start_ns"]), "opening time")
            order = (cut, t, 0 if opening else 1, s if opening else 0, b, int(kind == "trade"))
            need(previous is None or order >= previous, "stream order")
            previous = order
            group.append(r)
            need(len(group) <= 100000, "cut row limit")
        yield cut, group


def check_profile(root, snapshot, files):
    need({"profile.ndjson", "availability.ndjson", "transitions.ndjson.zst"} <= set(files),
         "profile requires activity, availability and transitions")
    books, membership, native = layout(snapshot)
    by_key = {(b["instrument"], b["orientation"]): b["book"] for b in books}
    duration, expected_trades = defaultdict(Counter), Counter()
    profile_cursor = {}
    for r in rows(root, "profile.ndjson", files["profile.ndjson"]):
        key = r["instrument"], r["orientation"]
        s = r["scope"]
        need(type(s) is int and 0 <= s < len(native) and key in native[s], "profile book membership")
        where = s, key
        start, end = natural(r["start_ns"]), natural(r["end_ns"])
        need(start == profile_cursor.get(where, int(snapshot["scopes"][s]["start_ns"])) and start < end
             <= int(snapshot["scopes"][s]["end_ns"]), "profile partition")
        profile_cursor[where] = end
        state = r["state"]
        u, n, bad = [natural(state[k]) for k in ("usable_ns", "not_initialized_ns", "unusable_ns")]
        need(u + n + bad == end - start, "profile duration partition")
        duration[where].update({"usable": u, "not_initialized": n})
        reasons = {"unusable:" + k: natural(v) for k, v in state["unusable_ns_by_reason"].items()}
        need(sum(reasons.values()) == bad, "profile unusable duration")
        duration[where].update(reasons)
        need("activity" in r, "profile activity required")
        activity = r["activity"]
        need(all(type(v) is int and v >= 0 for v in activity["trades"].values()), "activity count")
        expected_trades[where] += sum(v for k, v in activity["trades"].items() if k != "duplicate") - activity["trades_scale_mismatch"]
    for s, keys in enumerate(native):
        for key in keys:
            need(profile_cursor.get((s, key)) == int(snapshot["scopes"][s]["end_ns"]), "profile partition missing")

    availability, cursor = defaultdict(Counter), {}
    ids = {"book:" + digest({"instrument": k[0], "orientation": k[1]}): k for keys in native for k in keys}
    for r in rows(root, "availability.ndjson", files["availability.ndjson"]):
        if r["kind"] != "book":
            continue
        s, key = r["scope"], ids.get(r["entity"])
        need(type(s) is int and 0 <= s < len(native) and key in native[s], "availability book")
        where = s, key
        start, end = natural(r["start_ns"]), natural(r["end_ns"])
        need(start == cursor.get(where, int(snapshot["scopes"][s]["start_ns"])) and start < end
             <= int(snapshot["scopes"][s]["end_ns"]), "availability partition")
        cursor[where] = end
        validity = "unusable:" + r["reason"]["kind"] if r["state"] == "unusable" else r["state"]
        availability[where][validity] += end - start
    for where, held in duration.items():
        need(cursor.get(where) == int(snapshot["scopes"][where[0]]["end_ns"]), "availability partition missing")
        need(+availability[where] == +held, "availability/profile duration")

    transitions = groups(root, "transitions.ndjson.zst", files["transitions.ndjson.zst"], snapshot, books, membership)
    levels = (groups(root, "levels.ndjson.zst", files["levels.ndjson.zst"], snapshot, books, membership)
              if "levels.ndjson.zst" in files else iter(()))
    held, measured, trades, ladders = {}, defaultdict(Counter), Counter(), {}
    top_rows = trade_rows = 0
    a, b = next(transitions, None), next(levels, None)
    try:
        while a is not None or b is not None:
            cut = min(x[0] for x in (a, b) if x is not None)
            top_group = a[1] if a is not None and a[0] == cut else []
            level_group = b[1] if b is not None and b[0] == cut else []
            touched = set()
            for r in top_group:
                s, book, t = r["scope"], r["book"], int(r["t_ns"])
                key = s, book
                maximum = 10**books[book]["price_scale"]
                if r["type"] == "trade":
                    need(key in held and r["disposition"] in ("applied", "observed", "invalidated", "not_authority"),
                         "trade before opening/disposition")
                    quote([r["price"], r["qty"]], maximum)
                    trades[key] += 1
                    trade_rows += 1
                    continue
                need(r["type"] == "top", "transition type")
                opening = r["cause"] == "open"
                need((key not in held) == opening, "top opening required once")
                if key in held:
                    prior = held[key]
                    measured[key][prior["validity"]] += t - int(prior["t_ns"])
                validity = r["validity"]
                need(validity in ("usable", "not_initialized") or validity.startswith("unusable:"), "top validity")
                bid, ask = quote(r["bid"], maximum), quote(r["ask"], maximum)
                need(validity == "usable" or (bid is None and ask is None), "unusable quote")
                held[key] = r
                touched.add(key)
                top_rows += 1
            for r in level_group:
                key = r["scope"], r["book"]
                maximum = 10**books[r["book"]]["price_scale"]
                if r["type"] == "ladder":
                    need((key not in ladders) == (r["cause"] == "open"), "ladder opening")
                    sides = []
                    for field in ("bids", "asks"):
                        entries = [quote(v, maximum) for v in r[field]]
                        prices = [p for p, _ in entries]
                        need(prices == sorted(set(prices), reverse=field == "bids"), "ladder order")
                        sides.append(dict(entries))
                    ladders[key] = sides
                else:
                    need(r["type"] == "diff" and key in ladders, "ladder diff opening")
                    seen = []
                    for side, price, delta in r["levels"]:
                        need(side in ("bid", "ask"), "diff side")
                        p, d = natural(price), int(delta)
                        need(str(d) == delta and d != 0 and p <= maximum, "diff range")
                        side_index = int(side == "ask")
                        seen.append((side_index, p))
                        levels_side = ladders[key][side_index]
                        new = levels_side.get(p, 0) + d
                        need(new >= 0, "negative ladder quantity")
                        if new:
                            levels_side[p] = new
                        else:
                            levels_side.pop(p, None)
                    need(seen == sorted(set(seen)), "diff order")
                touched.add(key)
            if "levels.ndjson.zst" in files:
                for key in touched:
                    need(key in held and key in ladders, "ladder/top opening")
                    side_tops = []
                    for i, side in enumerate(ladders[key]):
                        p = (min(side) if i else max(side)) if side else None
                        side_tops.append(None if p is None else [str(p), str(side[p])])
                    need(side_tops == [held[key]["bid"], held[key]["ask"]], "replayed ladder differs from top")
            if a is not None and a[0] == cut:
                a = next(transitions, None)
            if b is not None and b[0] == cut:
                b = next(levels, None)
        for s, members in enumerate(membership):
            for book in members:
                key = s, book
                need(key in held, "missing top opening")
                r = held[key]
                measured[key][r["validity"]] += int(snapshot["scopes"][s]["end_ns"]) - int(r["t_ns"])
                native_key = books[book]["instrument"], books[book]["orientation"]
                need(+measured[key] == +duration[s, native_key], "transition/profile duration")
                need(trades[key] == expected_trades[s, native_key], "transition/activity trade count")
    finally:
        transitions.close()
        if hasattr(levels, "close"):
            levels.close()
    return {"top_rows": top_rows, "trades": trade_rows, "books": len(books)}
