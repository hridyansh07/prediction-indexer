"""Lossless episode overlays, native-side overlaps and source-time game facts."""
from collections import defaultdict

from replay.research.io import digest, need, rows
from replay.research.verify.fills import tables


def decimal(value, scale):
    """Exact decimal seconds/milliseconds, including integers beyond float precision."""
    sign = "-" if value < 0 else ""
    whole, fraction = divmod(abs(value), 10**scale)
    tail = str(fraction).zfill(scale).rstrip("0")
    return sign + str(whole) + ("." + tail if tail else "")


def game_document(game, windows, event_id):
    result = {"game_state_version": 1, "event_id": event_id, "state": game["state"], "events": []}
    if "timeline" not in game:
        return result
    timeline = game["timeline"]
    need(timeline["event_id"] == event_id, "game event identity")
    def emit(kind, time, basis, index=None, detail=None):
        if time is None:
            return
        time = int(time)
        if kind in ("settlement", "scheduled_start"):
            a = b = time
        else:
            window = windows[basis]
            a, b = max(0, time - window["before_ms"] * 10**6), time + window["after_ms"] * 10**6
        result["events"].append({"kind": kind, "map_index": index, "source": "kalshi", "time_basis": basis,
                                  "source_ns": str(time), "earliest_ns": str(a), "latest_ns": str(b),
                                  "exact": kind == "settlement", "known_at_ns": str(b), "detail": detail or {}})
    emit("scheduled_start", timeline.get("scheduled_start_ns"), "scheduled_start")
    emit("match_end", timeline.get("match_end_ns"), "kalshi_milestone_end", detail=timeline.get("series"))
    for m in timeline["maps"]:
        detail = {k: m[k] for k in ("scores", "winner_market", "duration_s", "forfeit") if k in m}
        emit("map_start", m["derived_start_ns"], "close_minus_duration", m["index"], detail)
        emit("map_end", m["close_ns"], "kalshi_market_close", m["index"], detail)
        emit("settlement", m["settlement_ns"], "settlement", m["index"], detail)
    result["events"].sort(key=lambda r: (int(r["source_ns"]), r["kind"], r["map_index"] or 0))
    return result


def shared_sides(records):
    """Count each overlapping episode once, even when several native sides are shared."""
    by_side = defaultdict(dict)
    order = sorted(range(len(records)), key=lambda i: (int(records[i]["start_ns"]), records[i]["lens"], records[i]["episode_id"]))
    pairs = 0
    for i in order:
        record, peers = records[i], set()
        start = int(record["start_ns"])
        for side in record["sides"]:
            active = by_side[side]
            expired = [j for j, end in active.items() if end <= start]
            for j in expired:
                del active[j]
            peers.update(active)
            need(len(active) < 2048, "duplicate concurrency bound")
            active[i] = int(record["end_ns"])
        pairs += len(peers)
        need(pairs <= 1000000, "duplicate pair bound")
        record["shares_leg_with"] += len(peers)
        for j in peers:
            records[j]["shares_leg_with"] += 1


def episodes(run, snapshot, event_id, lens, books):
    manifest, root = run["manifest"], run["output"]
    entities = tables(root, manifest)
    index = {(b["instrument"], b["orientation"]): b["book"] for b in books}
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    for row in rows(root, "episodes.ndjson", manifest["files"]["episodes.ndjson"]):
        d = entities[row["scope"], row["entity"]]
        mapped, sides, venues, markets = set(), set(), set(), set()
        fill = row.get("fill")
        for i, leg in enumerate(d["legs"]):
            key = leg["instrument"], leg["orientation"]
            p = plans[key]
            mapped_key = (key[0], "outcome") if p["venue"] == "kalshi" else key
            need(mapped_key in index, "episode leg absent from profile chart")
            mapped.add(index[mapped_key])
            venues.add(p["venue"])
            markets.add(leg.get("market_id", d.get("market_id")))
            source = (fill["sources"][i] if fill else "bid" if d["direction"] in ("short", "both_bids", "self_cross") else
                      "kalshi_complement_ask" if p["venue"] == "kalshi" else "ask")
            if source == "kalshi_complement_ask":
                key = key[0], "complement" if key[1] == "outcome" else "outcome"
            sides.add((*key, "bid" if source in ("bid", "kalshi_complement_ask") else "ask"))
        duration = int(row["end_ns"]) - int(row["start_ns"])
        metrics = [{"name": "duration", "value": str(duration), "unit": "ns"}]
        governing = next((r for r in fill["results"] if r["role"] == "governs"), None) if fill else None
        unit = (manifest.get("valuation") or {}).get("unit", "native_quote") + "_e36"
        if governing is not None:
            if governing["value"] is not None:
                metrics.append({"name": "net", "value": governing["value"], "unit": unit})
            for i, leg in enumerate(governing["legs"]):
                p = plans[d["legs"][i]["instrument"], d["legs"][i]["orientation"]]
                metrics.extend([{"name": "quantity_leg_" + str(i), "value": leg["atoms"],
                                 "unit": "quantity_atoms_scale_" + p["quantity_scale"]},
                                {"name": "cost_leg_" + str(i), "value": leg["cost"],
                                 "unit": "price_quantity_atoms_scale_" + str(int(p["price_scale"]) + int(p["quantity_scale"]))}])
        item = {"id": row["episode_id"], "start_ns": row["start_ns"], "end_ns": row["end_ns"],
                "books": sorted(mapped), "censored": row["censored"], "end_reason": row["end_reason"], "metrics": metrics,
                "detail": {"kind": row["kind"], "viable_tiers": row["viable_tiers"],
                           "kill_prices": None if fill is None else fill["kill_prices"],
                           "end_books": None if fill is None else fill["end_books"], "episode": row, "descriptor": d}}
        record = {"event_id": event_id, "lens": lens, "episode_id": row["episode_id"], "start_ns": row["start_ns"],
                  "end_ns": row["end_ns"], "duration_ms": decimal(duration, 6), "books": sorted(mapped),
                  "markets": sorted(markets), "venues": sorted(venues), "venue_pair": "+".join(sorted(venues)),
                  "censored": row["censored"], "end_reason": row["end_reason"],
                  "net": None if governing is None else governing["value"], "net_unit": None if governing is None else unit,
                  "quantity": None if governing is None else [l["atoms"] for l in governing["legs"]],
                  "seconds_to_capture_end": decimal(int(snapshot["scopes"][-1]["end_ns"]) - int(row["end_ns"]), 9),
                  "route_id": d.get("route_id", digest(d)), "shares_leg_with": 0, "sides": sides}
        yield item, record
