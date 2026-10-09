"""Kalshi archive adapter to generic segment facts, without Replay dependencies."""

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
    complete = not timeline["inconsistencies"] and all(s["end_ns"] is not None and s["winner"] is not None
                                                    for s in segments)
    if match is not None:
        complete = complete and match["winner"] is not None and all(
            type(v) is int and v >= 0 for v in match["score"].values())
    return {"source": {"name": "kalshi", "prefix": prefix,
                       "timeline_sha256": selected["inputs"][key]["sha256"]},
            "labels": labels, "scheduled_start_ns": timeline["scheduled_start_ns"],
            "segment_kind": "map", "segments": segments, "match": match, "complete": complete}
