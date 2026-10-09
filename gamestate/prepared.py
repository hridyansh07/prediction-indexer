"""Kalshi archive adapter to generic segment facts, without Replay dependencies."""

from decimal import Decimal
from urllib.parse import urlsplit

from gamestate import kalshi


def project(store, selected):
    """Labels come only from receipted market strikes; never from inferred names."""
    timeline = selected["timeline"]
    key = next(key for key in selected["inputs"] if key.endswith("/timeline.v2.json"))
    prefix = key.removesuffix("/timeline.v2.json")
    receipt, records = kalshi.read_records(store, prefix)
    ids, markets = {}, []
    # Small response records stream from the strict raw reader, verified to EOF.
    # Keep only the fields needed to map side IDs to market labels.
    for row in records:
        if row["error"] or row["status"] is None or not 200 <= row["status"] < 300:
            continue
        document = kalshi.loads(kalshi.body_bytes(row))
        path = urlsplit(row["url"]).path
        if path.endswith("/milestones"):
            for milestone in document["milestones"]:
                if milestone["id"] == receipt["milestone_id"]:
                    ids = {s: milestone["details"].get(s + "_competitor_id") for s in ("home", "away")}
        elif "/events/" in path:
            for market in document["event"]["markets"]:
                markets.append((market.get("custom_strike", {}).get("esports_competitor"),
                                market.get("yes_sub_title"), market.get("ticker")))
    labels = {}
    for side in ("home", "away"):
        candidates = {label for competitor, label, _ in markets if ids.get(side) is not None
                      and competitor == ids[side] and type(label) is str and label.strip()}
        labels[side] = next(iter(candidates)) if len(candidates) == 1 else None

    def winner(value):
        if value is None:
            return None
        sides = {side for side in ("home", "away") for competitor, _, ticker in markets
                 if ticker == value["ticker"] and ids.get(side) is not None and competitor == ids[side]}
        return next(iter(sides)) if len(sides) == 1 else None

    segments = [{"index": row["index"], "start_ns": row["derived_start_ns"], "start_estimated": True,
                 "end_ns": row["close_ns"], "settled_ns": row["settlement_ns"],
                 "winner": winner(row["winner_market"]),
                 "details": {"duration_s": row["duration_s"]["value"], "forfeit": row["forfeit"]["value"],
                             "home": row["scores"]["home_stats"]["value"],
                             "away": row["scores"]["away_stats"]["value"]}}
                for row in timeline["maps"]]
    values = timeline["series"]["score"]["value"]
    match = None
    if timeline["match_end_ns"] is not None:
        match = {"end_ns": timeline["match_end_ns"], "winner": winner(timeline["series"]["winner_market"]),
                 "score": {s: values[s + "_score"] for s in ("home", "away")}}
    # A raw-complete archive can still contain contradictory or unsettled results.
    # Such evidence cannot silently become a usable game-state experiment.
    problems = set()
    issues = {(row["code"], row["map_index"]) for row in timeline["inconsistencies"]}
    if issues and segments and issues == {("map_count_difference", None),
                                          ("map_event_missing", segments[-1]["index"])}:
        # Kalshi lists no winner market for the deciding map: every Bo1, and the last
        # map of a series that went the distance. That map ends the match, so its
        # winner is the series winner, confirmed by its own live statistics, and its
        # end is the match end. No market exists to settle.
        last = segments[-1]
        if match is None:
            problems.add("deciding_segment_without_match_end")
        else:
            side = {(1, 0): "home", (0, 1): "away"}.get(
                (last["details"]["home"].get("winner"), last["details"]["away"].get("winner")))
            duration = last["details"]["duration_s"]
            last.update(end_ns=match["end_ns"], settled_ns=None,
                        winner=side if side == match["winner"] else None,
                        start_ns=None if duration is None
                        else match["end_ns"] - int(Decimal(str(duration)) * 1_000_000_000))
    elif issues:
        problems.add("inconsistent_timeline")
    if not all(s["end_ns"] is not None and s["winner"] is not None for s in segments):
        problems.add("unresolved_segment")
    if match is not None:
        if match["winner"] is None or not all(type(v) is int and v >= 0 for v in match["score"].values()):
            problems.add("unresolved_match")
        elif segments and {s: sum(row["winner"] == s for row in segments) for s in ("home", "away")} != match["score"]:
            problems.add("score_mismatch")
    # Kalshi can close a map market long after the map, even after the next map or
    # the match. The derived times then contradict each other; that event is
    # incomplete evidence, not a preparation error. Mirrors the pinned loader.
    previous = None
    for segment in segments:
        start, end, settled = segment["start_ns"], segment["end_ns"], segment["settled_ns"]
        if end is None:
            continue
        if (start is not None and not 0 <= start <= end) or (settled is not None and settled < end):
            problems.add("segment_time_order")
        if previous is not None and (end if start is None else start) < previous:
            problems.add("late_market_close")
        previous = end
    if match is not None and previous is not None and match["end_ns"] < previous:
        problems.add("late_market_close")
    return {"source": {"name": "kalshi", "prefix": prefix,
                       "timeline_sha256": selected["inputs"][key]["sha256"]},
            "labels": labels, "scheduled_start_ns": timeline["scheduled_start_ns"],
            "segment_kind": "map", "segments": segments, "match": match,
            "complete": not problems, "problems": sorted(problems)}
