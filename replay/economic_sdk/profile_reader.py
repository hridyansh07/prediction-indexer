"""Independent reader for market-profile output; does not import the collector.

It re-derives the scoped book set from the snapshot and checks every row's
identity, bucket alignment, complete per-book time partition, exact internal
arithmetic (state partition, spread histogram integrals and quantiles,
self-crossing time against incident rows, depth time partitions) and close
order, then returns per-book totals.
"""

from __future__ import annotations

from pathlib import Path

from replay.economic_sdk.availability_reader import book_id, duration_rows, validate_intervals
from replay.economic_sdk.profile_policy import (
    AVAILABILITY_FILE,
    DISPOSITIONS,
    FILES,
    GROUPS,
    pairs_of,
    profile_policy,
)
from replay.economic_sdk.reader import lines, signed
from replay.streams.protocol import obj, require, uint  # noqa: F401

_STATE = ("usable_ns", "not_initialized_ns", "unusable_ns", "bid_empty_ns", "ask_empty_ns",
          "both_empty_ns", "two_sided_ns")
_PAIR = ("both_bids_ns", "both_asks_ns", "bid_sum_dev_ns", "ask_sum_dev_ns", "bid_sum_abs_dev_ns",
         "ask_sum_abs_dev_ns", "bid_sum_above_unit_ns", "ask_sum_below_unit_ns")


def _big(value):
    """Canonical unsigned decimal; time integrals (quantity x ns) exceed 64 bits."""
    require(type(value) is str and 0 < len(value) <= 64 and value.isascii() and value.isdigit()
            and (value == "0" or value[0] != "0"), "canonical unsigned integral")
    return int(value)


def _rank(histogram, percent):
    total = sum(ns for _, ns in histogram)
    if not total:
        return None
    rank, running = (total * percent + 99) // 100, 0
    for value, ns in histogram:
        running += ns
        if running >= rank:
            return str(value)


def _quotes(value):
    obj(value, "bid ask")
    for side in ("bid", "ask"):
        quote = value[side]
        if quote is not None:
            require(type(quote) is list and len(quote) == 2, "quote shape")
            uint(quote[0])
            require(uint(quote[1]) > 0, "quote quantity")


def _bucketed(start, end, width, scope_start, scope_end):
    require(scope_start <= start < end <= scope_end, "profile row bounds")
    require(start // width == (end - 1) // width, "row crosses a bucket edge")
    require(start == scope_start or start % width == 0, "row start alignment")
    require(end == scope_end or end % width == 0, "row end alignment")


def validate_profile(root, snapshot, files, policy, experiment_sha256, snapshot_sha256, *,
                     standalone=False):
    policy = profile_policy(policy, standalone=standalone)
    root = Path(root)
    groups = set(policy["groups"])
    width = int(policy["bucket_ns"])
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    scopes = snapshot["scopes"]
    books, members, pairs = {}, {}, {}
    for index, scope in enumerate(scopes):
        books[index] = sorted({(b["instrument"], b["orientation"]) for m in scope["members"]
                               for b in m["books"] if (b["instrument"], b["orientation"]) in plans})
        for member in scope["members"]:
            for book in member["books"]:
                members.setdefault((book["instrument"], book["orientation"]), member["market_id"])
        if "pair_consistency" in groups:
            pairs[index] = [p for p in pairs_of(scope)
                            if all(k in plans for k in p[1])
                            and len({(plans[k]["price_scale"], plans[k]["quantity_scale"]) for k in p[1]}) == 1]

    def identity(row, scope):
        require(row["version"] == 1 and row["experiment_sha256"] == experiment_sha256
                and row["snapshot_sha256"] == snapshot_sha256, "profile identity")
        require(type(scope) is int and 0 <= scope < len(scopes), "profile scope")
        require(row["bundle_id"] == scopes[scope]["bundle_id"]
                and row["scope_run_id"] == scopes[scope]["run_id"], "profile scope identity")

    fields = ("version experiment_sha256 snapshot_sha256 bundle_id scope scope_run_id instrument "
              "orientation venue market_id price_scale quantity_scale start_ns end_ns tick_atoms "
              "ask_source groups state " + " ".join(g for g in GROUPS if g in groups and g != "pair_consistency"))
    last, cursor, totals = None, {}, {}
    for row in lines(root, "profile.ndjson", files["profile.ndjson"]):
        obj(row, fields)
        scope = row["scope"]
        identity(row, scope)
        key = (row["instrument"], row["orientation"])
        require(key in books[scope], "profile book outside scope")
        plan = plans[key]
        require(row["venue"] == plan["venue"] and row["market_id"] == members.get(key)
                and row["price_scale"] == plan["price_scale"]
                and row["quantity_scale"] == plan["quantity_scale"]
                and row["tick_atoms"] == policy["tick_atoms"][plan["venue"]]
                and row["ask_source"] == ("projected" if plan["venue"] == "kalshi" else "native")
                and row["groups"] == policy["groups"], "profile book identity")
        start, end = _big(row["start_ns"]), _big(row["end_ns"])
        scope_start, scope_end = int(scopes[scope]["start_ns"]), int(scopes[scope]["end_ns"])
        _bucketed(start, end, width, scope_start, scope_end)
        order = (end, scope, key[0], key[1])
        require(last is None or last < order, "profile close order")
        last = order
        require(cursor.get((scope, key), scope_start) == start, "profile partition gap/overlap")
        cursor[scope, key] = end
        duration = end - start

        state = obj(row["state"], " ".join(_STATE) + " unusable_ns_by_reason")
        d = {name: _big(state[name]) for name in _STATE}
        require(d["usable_ns"] + d["not_initialized_ns"] + d["unusable_ns"] == duration,
                "profile state partition")
        reasons = state["unusable_ns_by_reason"]
        require(type(reasons) is dict and sum(_big(v) for v in reasons.values()) == d["unusable_ns"],
                "profile unusable reasons")
        require(d["both_empty_ns"] <= min(d["bid_empty_ns"], d["ask_empty_ns"])
                and d["two_sided_ns"] + d["bid_empty_ns"] + d["ask_empty_ns"] - d["both_empty_ns"]
                == d["usable_ns"], "profile side partition")
        total = totals.setdefault((scope, key), {"rows": 0, "crossed_or_locked_ns": 0,
                                                 **{name: 0 for name in _STATE},
                                                 "trades": {k: 0 for k in DISPOSITIONS}})
        total["rows"] += 1
        for name in _STATE:
            total[name] += d[name]

        if "top_of_book" in groups:
            top = obj(row["top_of_book"], "open close spread_atoms_ns spread_histogram spread_p50_atoms "
                                          "spread_p90_atoms mid2_atoms_ns bid_top_quantity_ns ask_top_quantity_ns")
            _quotes(top["open"])
            _quotes(top["close"])
            histogram = []
            for entry in top["spread_histogram"]:
                require(type(entry) is list and len(entry) == 2, "spread histogram entry")
                histogram.append((signed(entry[0]), _big(entry[1])))
            require([v for v, _ in histogram] == sorted({v for v, _ in histogram})
                    and all(ns > 0 for _, ns in histogram), "spread histogram order")
            require(sum(ns for _, ns in histogram) == d["two_sided_ns"], "spread histogram time")
            require(signed(top["spread_atoms_ns"]) == sum(v * ns for v, ns in histogram),
                    "spread integral")
            require(top["spread_p50_atoms"] == _rank(histogram, 50)
                    and top["spread_p90_atoms"] == _rank(histogram, 90), "spread quantiles")
            for name in ("mid2_atoms_ns", "bid_top_quantity_ns", "ask_top_quantity_ns"):
                _big(top[name])
        if "self_crossing" in groups:
            crossing = obj(row["self_crossing"], "crossed_ns locked_ns")
            crossed, locked = _big(crossing["crossed_ns"]), _big(crossing["locked_ns"])
            require(crossed + locked <= d["two_sided_ns"], "self-crossing time")
            if "top_of_book" in groups:
                require(crossed == sum(ns for v, ns in histogram if v < 0)
                        and locked == sum(ns for v, ns in histogram if v == 0), "crossing/histogram")
            total["crossed_or_locked_ns"] += crossed + locked
        if "depth" in groups:
            depth = obj(row["depth"], "sizes_contracts depth_ticks bid ask")
            require(depth["sizes_contracts"] == policy["sizes_contracts"]
                    and depth["depth_ticks"] == policy["depth_ticks"], "depth policy")
            for side in ("bid", "ask"):
                values = obj(depth[side], "within_ticks_quantity_ns filled_ns depth_limited_ns slippage_cost_ns")
                present = d["usable_ns"] - d[side + "_empty_ns"]
                require(len(values["within_ticks_quantity_ns"]) == len(policy["depth_ticks"]))
                for v in values["within_ticks_quantity_ns"]:
                    _big(v)
                for name in ("filled_ns", "depth_limited_ns", "slippage_cost_ns"):
                    require(type(values[name]) is list and len(values[name]) == len(policy["sizes_contracts"]))
                for filled, limited, slip in zip(values["filled_ns"], values["depth_limited_ns"],
                                                 values["slippage_cost_ns"]):
                    require(_big(filled) + _big(limited) == present, "depth time partition")
                    _big(slip)
        if "activity" in groups:
            activity = obj(row["activity"], "transitions snapshots operations invalidations trades "
                                            "traded_quantity_atoms trade_mid2_deviation_atoms trades_priced "
                                            "trades_without_mid trades_scale_mismatch aggressor")
            for name in ("transitions", "snapshots", "operations", "trades_priced",
                         "trades_without_mid", "trades_scale_mismatch"):
                require(type(activity[name]) is int and activity[name] >= 0, "activity count")
            require(type(activity["invalidations"]) is dict
                    and all(type(v) is int and v > 0 for v in activity["invalidations"].values()))
            require(activity["snapshots"] + sum(activity["invalidations"].values())
                    <= activity["transitions"], "activity transitions")
            obj(activity["trades"], " ".join(DISPOSITIONS))
            for name, count in activity["trades"].items():
                require(type(count) is int and count >= 0)
                total["trades"][name] += count
            nonduplicate = sum(c for n, c in activity["trades"].items() if n != "duplicate")
            require(activity["trades_priced"] + activity["trades_without_mid"]
                    + activity["trades_scale_mismatch"] == nonduplicate, "trade partition")
            require(type(activity["aggressor"]) is dict
                    and set(activity["aggressor"]) <= {"bid", "ask", "none"}
                    and sum(activity["aggressor"].values()) == nonduplicate, "aggressor partition")
            _big(activity["traded_quantity_atoms"])
            signed(activity["trade_mid2_deviation_atoms"])
        if "quote_stability" in groups:
            stability = obj(row["quote_stability"], "edges_ns bid ask censored")
            require(stability["edges_ns"] == policy["survival_edges_ns"])
            for side in ("bid", "ask"):
                counts = stability[side]
                require(type(counts) is list and len(counts) == len(policy["survival_edges_ns"]) + 1
                        and all(type(c) is int and c >= 0 for c in counts), "survival histogram")
            obj(stability["censored"], "bid ask")

    for index in range(len(scopes)):
        for key in books[index]:
            require(cursor.get((index, key)) == int(scopes[index]["end_ns"]), "incomplete profile rows")

    incidents, last = {}, None
    for row in lines(root, "incidents.ndjson", files["incidents.ndjson"]):
        obj(row, "version experiment_sha256 snapshot_sha256 bundle_id scope scope_run_id instrument "
                 "orientation venue market_id price_scale quantity_scale kind start_ns end_ns end_reason "
                 "censored max_cross_atoms quotes_at_max tick_atoms ask_source")
        require("self_crossing" in groups, "incident without self_crossing group")
        scope = row["scope"]
        identity(row, scope)
        key = (row["instrument"], row["orientation"])
        require(key in books[scope] and row["kind"] == "self_cross"
                and row["ask_source"] == ("projected" if plans[key]["venue"] == "kalshi" else "native")
                and row["tick_atoms"] == policy["tick_atoms"][plans[key]["venue"]], "incident book")
        start, end = _big(row["start_ns"]), _big(row["end_ns"])
        require(int(scopes[scope]["start_ns"]) <= start < end <= int(scopes[scope]["end_ns"]),
                "incident bounds")
        require(row["end_reason"] in {"UNCROSSED", "SCOPE_END", "RUN_END"}
                and row["censored"] == (row["end_reason"] == "RUN_END"), "incident end")
        require(_big(row["max_cross_atoms"]) >= 0)
        _quotes(row["quotes_at_max"])
        order = (end, scope, key[0], key[1], start)
        require(last is None or last < order, "incident close order")
        last = order
        previous = incidents.get((scope, key))
        require(previous is None or previous[1] <= start, "overlapping incidents")
        incidents[scope, key] = (start, end, (previous[2] if previous else 0) + end - start,
                                 (previous[3] if previous else 0) + 1)
    if "self_crossing" in groups:
        for (scope, key), total in totals.items():
            incident = incidents.get((scope, key))
            require((incident[2] if incident else 0) == total["crossed_or_locked_ns"],
                    "incidents/self-crossing time")
            total["incidents"] = incident[3] if incident else 0

    pair_totals, cursor, last = {}, {}, None
    pair_fields = ("version experiment_sha256 snapshot_sha256 bundle_id scope scope_run_id market_id "
                   "venue books price_scale start_ns end_ns " + " ".join(_PAIR))
    for row in lines(root, "pair_profile.ndjson", files["pair_profile.ndjson"]):
        obj(row, pair_fields)
        require("pair_consistency" in groups, "pair row without pair_consistency group")
        scope = row["scope"]
        identity(row, scope)
        books_key = tuple(tuple(k) for k in row["books"])
        require((row["market_id"], books_key) in pairs[scope], "unknown pair")
        start, end = _big(row["start_ns"]), _big(row["end_ns"])
        _bucketed(start, end, width, int(scopes[scope]["start_ns"]), int(scopes[scope]["end_ns"]))
        order = (end, scope, row["market_id"])
        require(last is None or last < order, "pair close order")
        last = order
        require(cursor.get((scope, row["market_id"]), int(scopes[scope]["start_ns"])) == start,
                "pair partition gap/overlap")
        cursor[scope, row["market_id"]] = end
        values = {name: signed(row[name]) for name in _PAIR}
        require(0 <= values["both_bids_ns"] <= end - start and 0 <= values["both_asks_ns"] <= end - start
                and 0 <= values["bid_sum_above_unit_ns"] <= values["both_bids_ns"]
                and 0 <= values["ask_sum_below_unit_ns"] <= values["both_asks_ns"]
                and abs(values["bid_sum_dev_ns"]) <= values["bid_sum_abs_dev_ns"]
                and abs(values["ask_sum_dev_ns"]) <= values["ask_sum_abs_dev_ns"], "pair arithmetic")
        total = pair_totals.setdefault((scope, row["market_id"]), {name: 0 for name in _PAIR})
        for name in _PAIR:
            total[name] += values[name]
    for index, scope_pairs in pairs.items():
        for market, _ in scope_pairs:
            require(cursor.get((index, market)) == int(scopes[index]["end_ns"]), "incomplete pair rows")

    availability = None
    if "availability" in groups:
        # The shared coverage reader, then one profile-only cross-check: a book's
        # usable time in availability rows is the sum of its profile rows' usable time.
        durations = validate_intervals(root / AVAILABILITY_FILE, snapshot, files[AVAILABILITY_FILE])
        for index in range(len(scopes)):
            for key in books[index]:
                entity = book_id({"instrument": key[0], "orientation": key[1]})
                require((index, entity) in durations, "availability book missing")
                require(durations[index, entity].get("usable", 0) == totals[index, key]["usable_ns"],
                        "availability/profile usable time")
        availability = duration_rows(durations)

    result = {"version": 1, "experiment_sha256": experiment_sha256, "policy": policy,
              "books": [{"scope": scope, "instrument": key[0], "orientation": key[1],
                         **{k: (v if type(v) is not int or k == "rows" else str(v)) for k, v in total.items()}}
                        for (scope, key), total in sorted(totals.items())],
              "pairs": [{"scope": scope, "market_id": market, **{k: str(v) for k, v in total.items()}}
                        for (scope, market), total in sorted(pair_totals.items())]}
    if availability is not None:
        result["availability_durations"] = availability
    return result


__all__ = ["FILES", "validate_profile"]
