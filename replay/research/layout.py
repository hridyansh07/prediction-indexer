"""Research's independently derived written-book table and effective outcome masks."""
from replay.research.io import digest, need


def layout(snapshot):
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    scopes, markets = [], {}
    for scope in snapshot["scopes"]:
        keys = set()
        for member in scope["members"]:
            for b in member["books"]:
                key = b["instrument"], b["orientation"]
                if key in plans:
                    keys.add(key)
                    markets[key] = member["market_id"]
        scopes.append(keys)
    present = set().union(*scopes)
    ordered = sorted(k for k in present if not (plans[k]["venue"] == "kalshi" and k[1] == "complement"
                                              and (k[0], "outcome") in present))
    need(not any(plans[k]["venue"] == "kalshi" and k[1] == "complement" for k in ordered),
         "Kalshi complement written")
    books = [{"book": i, "instrument": k[0], "orientation": k[1], "venue": plans[k]["venue"],
              "market_id": markets[k], "price_scale": int(plans[k]["price_scale"]),
              "quantity_scale": int(plans[k]["quantity_scale"])} for i, k in enumerate(ordered)]
    index = {k: i for i, k in enumerate(ordered)}
    return books, [{index[k] for k in keys if k in index} for keys in scopes], scopes


def effective_claim(snapshot, scope, key):
    recorded = snapshot.get("outcomes", {})
    if recorded.get("provider") != "universe":
        return None
    for b in snapshot["scopes"][scope].get("outcome_books", []):
        if (b["instrument"], b["orientation"]) != key or b["status"] != "MASKED":
            continue
        doc = recorded["document"]
        claim = next(c for c in doc["claims"] if c["claim_id"] == b["claim_id"])
        space = next(s for s in doc["spaces"] if s["space_shape_id"] == b["space_shape_id"])
        keys = set(claim["outcome_keys"])
        if b["negated"]:
            keys = set(space["outcome_keys"]) - keys
        # Effective identity includes shape and payout set, never the base ID alone.
        return {"claim_id": b["claim_id"], "negated": b["negated"], "space_shape_id": b["space_shape_id"],
                "outcome_keys": sorted(keys),
                "effective_claim_id": digest({"space_shape_id": b["space_shape_id"], "outcome_keys": sorted(keys)})}
    return None
