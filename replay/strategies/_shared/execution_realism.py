"""Closed strategy rollout policy and pure snapshot evidence.

The four opted-in policies bind episode flags and richer decision evidence.
Old policies retain their original schemas and default SDK delay lookup.
"""
from replay.execution_terms import instant_ns
from replay.strategy_sdk import plain
from replay.streams.protocol import obj, require


def flags_policy(value):
    value = obj(plain(value), "version stale_ms settlement_allowlist")
    require(type(value["version"]) is int and value["version"] == 1, "flags policy version")
    require(type(value["stale_ms"]) is int and 0 <= value["stale_ms"] <= (2**64 - 1) // 10**6,
            "flags stale_ms")
    allow = value["settlement_allowlist"]
    require(type(allow) is list and len(allow) <= 64
            and all(type(item) is str and 0 < len(item) <= 256 for item in allow)
            and allow == sorted(set(allow)), "flags settlement allowlist")
    return value


def rollout_policy(value, fill):
    """Validate the new fields without constraining deliberate experiment grids."""
    flags_policy(value["flags"])
    names = {s.name: s for s in fill.sizings}
    require("edge" in names and names["edge"].role == "governs" and names["edge"].mode == "edge",
            "execution realism governing edge sizing")
    require("t10" in names and names["t10"].role == "records" and names["t10"].mode == "target"
            and names["t10"].target_contracts == "10", "execution realism recording t10 sizing")
    require(fill.probes is not None and {"edge", "t10"} <= set(fill.probes.sizings),
            "execution realism requires edge/t10 probes")
    require(set(fill.probes.unknown_taker_delay_ms) == {"kalshi", "limitless", "polymarket"},
            "execution realism venue defaults")


def market_delays(snapshot, entity, time_ns):
    """Map acquired descriptor legs to market terms, never native source books."""
    require(type(time_ns) is int and time_ns >= 0, "execution terms lookup time")
    doc = snapshot.get("outcomes", {}).get("document")
    if doc is None or doc["version"] == 1:
        return (None,) * len(entity.legs)
    markets = {m["market_id"]: m for m in doc["markets"]}
    result = []
    for leg in entity.descriptor["legs"]:
        market, delay = markets.get(leg["market_id"]), None
        if market is not None:
            for change in market["execution_terms"]["changes"]:
                if instant_ns(change["from"]) > time_ns:
                    break
                delay = change["taker_delay_ms"]
        result.append(None if delay is None else delay * 10**6)
    require(len(result) == len(entity.legs), "execution realism descriptor leg count")
    return tuple(result)


def record_flags(snapshot, entity, policy, ages):
    markets = {m["market_id"]: m for m in snapshot.get("outcomes", {}).get("document", {}).get("markets", [])}
    types = [markets.get(leg["market_id"], {}).get("market_type") for leg in entity.descriptor["legs"]]
    distinct = sorted(set(types)) if all(t is not None for t in types) else []
    pairing = (distinct[0] if len(distinct) == 1 else "mixed:" + "+".join(distinct)) if distinct else "unknown"
    return {"pairing_class": pairing, "settlement_review": pairing not in policy["settlement_allowlist"],
            "stale_legs": [i for i, age in enumerate(ages) if age["best_ns"] is not None
                           and int(age["best_ns"]) > policy["stale_ms"] * 10**6]}


def check_flags(value, snapshot, entity, policy, ages):
    """Independently recompute from pinned markets and carried opening ages."""
    value = obj(value, "pairing_class settlement_review stale_legs")
    require(type(value["pairing_class"]) is str and type(value["settlement_review"]) is bool
            and type(value["stale_legs"]) is list
            and all(type(index) is int for index in value["stale_legs"]), "fill flags types")
    model = snapshot.get("outcomes", {}).get("document", {})
    ids = [leg["market_id"] for leg in entity.descriptor["legs"]]
    markets = {m["market_id"]: m["market_type"] for m in model.get("markets", [])}
    if any(mid not in markets for mid in ids):
        pairing = "unknown"
    else:
        types = sorted({markets[mid] for mid in ids})
        pairing = types[0] if len(types) == 1 else "mixed:" + "+".join(types)
    require(value["pairing_class"] == pairing, "fill flags pairing class")
    require(value["settlement_review"] == (pairing not in policy["settlement_allowlist"]),
            "fill flags settlement review")
    stale = []
    for index, age in enumerate(ages):
        if age["best_ns"] is not None and int(age["best_ns"]) > policy["stale_ms"] * 1_000_000:
            stale.append(index)
    require(value["stale_legs"] == stale, "fill flags stale legs")


class ExecutionRealism:
    """Runtime/reader opt-in owned by the four closed strategy policy versions."""
    @property
    def probe_decision_evidence(self):
        return "flags" in self.policy

    def fill_delays(self, entity, time_ns):
        if self.probe_decision_evidence:
            return market_delays(self.snapshot, entity, time_ns)
        return super().fill_delays(entity, time_ns)

    def fill_flags(self, entity, ages):
        if self.probe_decision_evidence:
            return record_flags(self.snapshot, entity, self.policy["flags"], ages)
        return None

    def check_fill_flags(self, value, entity, ages):
        check_flags(value, self.snapshot, entity, self.policy["flags"], ages)
