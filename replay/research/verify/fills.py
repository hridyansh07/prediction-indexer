"""Independent native fee, depth, edge-stop and kill-boundary witnesses."""
from fractions import Fraction

from replay.economic_fills import Fill
from replay.research.io import closed, digest, document, natural, need, rows
from replay.research.layout import effective_claim
from replay.strategies._shared.fee_bridge import FeeBridge
from replay.strategy_sdk import plain


def tables(root, manifest):
    files = manifest["files"]
    if "layout" in manifest:
        need(type(manifest["layout"]) is int and manifest["layout"] == 3, "unknown layout")
        descriptors, prior = {}, None
        for row in rows(root, "descriptors.ndjson", files["descriptors.ndjson"]):
            closed(row, "hash descriptor")
            h = row["hash"]
            need(h == digest(row["descriptor"]) and (prior is None or h > prior), "descriptor identity/order")
            descriptors[h], prior = row["descriptor"], h
        entities, previous, counts = {}, None, {}
        for row in rows(root, "entities.ndjson", files["entities.ndjson"]):
            closed(row, "scope entity hash")
            key = row["scope"], row["entity"]
            need(all(type(v) is int and v >= 0 for v in key) and (previous is None or key > previous),
                 "entity order")
            need(row["entity"] == counts.get(row["scope"], 0) and row["hash"] in descriptors, "entity index/hash")
            entities[key] = descriptors[row["hash"]]
            counts[row["scope"]] = row["entity"] + 1
            previous = key
        return entities
    need("entities.json" in files, "legacy layout 1 is not a research input")
    table = document(root / "entities.json")
    closed(table, "scopes")
    result = {}
    for s, scoped in enumerate(table["scopes"]):
        for e, row in enumerate(scoped):
            closed(row, "hash descriptor")
            need(row["hash"] == digest(row["descriptor"]), "descriptor identity")
            result[s, e] = row["descriptor"]
    return result


def _levels(values):
    need(type(values) is list, "fill levels")
    parsed = []
    for v in values:
        need(type(v) is list and len(v) == 2, "fill level")
        p, q = map(natural, v)
        need(q > 0, "fill quantity")
        parsed.append((p, q))
    return tuple(parsed)


def evaluate(bridge, snapshot, scope, descriptor, fills, steps, time, manifest, *, minimum=True):
    """Native balances plus the least payout in the pinned effective outcome space."""
    legs, economics = descriptor["legs"], []
    requests = []
    for d, fill in zip(legs, fills, strict=True):
        key = d["instrument"], d["orientation"]
        e = bridge.economics(key)
        if e is None:
            return None, []
        economics.append(e)
        requests.append({"market_id": d.get("market_id", descriptor.get("market_id")), "key": key,
                         "fill": fill, "price_scale": e.price_scale, "quantity_scale": e.quantity_scale, "side": "BUY"})
    prepared, missing = bridge.assess_orders(experiment=manifest["experiment_sha256"], scope=0, basket=descriptor,
                                            direction="BUY", size=steps, time=time, sequence=0, legs=tuple(requests))
    if missing:
        return None, []
    cash, received, charges = [], [], []
    for e, assessments in prepared:
        c = r = 0
        charge_rows = []
        for a in assessments:
            if a.net_deltas is None or a.unknowns:
                return None, []
            for charge in a.charges:
                asset = charge.amount.asset
                charge_rows.append({"asset": {"kind": asset.kind.value, "ledger": asset.chain, "token": asset.token},
                                    "amount_e36": str(charge.amount.amount.atoms * 10**(36 - charge.amount.amount.scale)),
                                    "component": charge.component.value})
            for delta in a.net_deltas:
                amount = delta.atoms * 10**(36 - delta.scale)
                if delta.asset == e.quote:
                    c += amount
                elif delta.asset == e.outcome:
                    r += amount
                else:
                    return None, []
        cash.append(c)
        received.append(r)
        charges.append(charge_rows)
    masks = [effective_claim(snapshot, scope, (d["instrument"], d["orientation"])) for d in legs]
    need(all(masks) and len({m["space_shape_id"] for m in masks}) == 1, "fill outcome mask binding")
    doc = snapshot["outcomes"]["document"]
    space = next(s for s in doc["spaces"] if s["space_shape_id"] == masks[0]["space_shape_id"])
    need(space["coverage"] == "EXHAUSTIVE", "fill outcome coverage")
    payout = min(sum(q for q, mask in zip(received, masks) if key in mask["outcome_keys"])
                 for key in space["outcome_keys"])
    value = payout + sum(cash)
    if minimum:
        floor = steps * Fraction(manifest["policy"]["fills"]["step_contracts"]) * int(
            manifest["policy"]["minimum_net_gap_per_contract_e18"]) * 10**18
        need(floor.denominator == 1, "exact minimum")
        value -= int(floor)
    return value, charges


def check_fill(carried, descriptor, start, end_reason, manifest, snapshot, scope, bridge):
    closed(carried, "sources units results kill_prices kill_leg kill_best end_books")
    units = [natural(v) for v in carried["units"]]
    need(all(u > 0 for u in units) and len(units) == len(descriptor["legs"]), "fill unit shape")
    policy = manifest["policy"]["fills"]
    maximums = []
    expected_sources = []
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    for i, d in enumerate(descriptor["legs"]):
        p = plans[d["instrument"], d["orientation"]]
        maximums.append(10**int(p["price_scale"]))
        unit = Fraction(policy["step_contracts"]) * 10**int(p["quantity_scale"])
        need(unit.denominator == 1 and units[i] == unit, "fill unit binding")
        expected_sources.append("kalshi_complement_ask" if p["venue"] == "kalshi" else "ask")
    need(carried["sources"] == expected_sources, "fill source binding")
    need(len(carried["results"]) == len(policy["sizings"]), "fill sizing count")
    effective = [None] * len(units)
    for result, sizing in zip(carried["results"], policy["sizings"]):
        closed(result, "name role mode steps stop value legs before after impact_ppm beyond tradeable kill_prices")
        mode = "edge" if sizing.get("edge") else "target"
        need((result["name"], result["role"], result["mode"]) == (sizing["name"], sizing["role"], mode), "sizing binding")
        steps = natural(result["steps"])
        stop = result["stop"]
        need(stop in (("edge", "book_exhausted", "level_cap", "value_unknown") if mode == "edge"
                      else ("target", "book_exhausted", "level_cap")), "fill stop")
        target = None if mode == "edge" else Fraction(sizing["target_contracts"]) / Fraction(policy["step_contracts"])
        need(target is None or target.denominator == 1, "target units")
        need(target is None or (steps == target if stop == "target" else steps < target), "target stop")
        fills, nexts, next_status = [], [], []
        for i, leg in enumerate(result["legs"]):
            closed(leg, "atoms cost taken consumed")
            taken, consumed = _levels(leg["taken"]), _levels(leg["consumed"])
            need(len(taken) == len(consumed) <= policy["max_levels"], "fill depth bound")
            need(all(p <= maximums[i] for p, _ in consumed) and all(a[0] < b[0] for a, b in zip(consumed, consumed[1:])),
                 "fill depth order")
            need(all(p == c and 0 < q <= d and (q == d or j == len(taken) - 1)
                     for j, ((p, q), (c, d)) in enumerate(zip(taken, consumed))), "taken/consumed")
            atoms, cost = natural(leg["atoms"]), natural(leg["cost"])
            need(atoms == steps * units[i] == sum(q for _, q in taken), "fill atoms")
            need(cost == sum(p * q for p, q in taken), "fill cost")
            before = None if result["before"][i] is None else _levels([result["before"][i]])[0]
            after = None if result["after"][i] is None else _levels([result["after"][i]])[0]
            if consumed:
                need(before == consumed[0], "fill before")
                p, q = consumed[-1]
                need(after == (p, q - taken[-1][1]) if taken[-1][1] < q
                     else after is None or after[0] > p, "fill after")
            else:
                need(steps == 0 and before == after, "fill after empty")
            impact = None if before is None or after is None or before[0] == 0 else str(abs(after[0] - before[0]) * 10**6 // before[0])
            need(result["impact_ppm"][i] == impact, "fill impact")
            fill = Fill(atoms, cost, False, taken, consumed)
            fills.append(fill)
            beyond = _levels(result["beyond"][i])
            need(len(beyond) <= policy["max_levels"] + 1 and (beyond[0] if beyond else None) == after
                 and all(a[0] < b[0] for a, b in zip(beyond, beyond[1:]))
                 and sum(q for _, q in beyond[:-1]) < units[i], "beyond witness")
            need(all(p <= maximums[i] for p, _ in beyond), "beyond price")
            if sum(q for _, q in beyond) < units[i]:
                nexts.append(None)
                next_status.append("short" if len(beyond) <= policy["max_levels"] else "unknown")
            else:
                remaining, added_cost = units[i], cost
                extra_taken, extra_consumed = list(taken), list(consumed)
                for p, q in beyond:
                    amount = min(q, remaining)
                    remaining -= amount
                    added_cost += p * amount
                    if extra_taken and extra_taken[-1][0] == p:
                        extra_taken[-1] = (p, extra_taken[-1][1] + amount)
                    else:
                        extra_taken.append((p, amount))
                        extra_consumed.append((p, q))
                    if remaining == 0:
                        break
                nexts.append(Fill(atoms + units[i], added_cost, False, tuple(extra_taken), tuple(extra_consumed)))
                next_status.append("fits" if len(extra_consumed) <= policy["max_levels"] else "capped")
        def value(n, values):
            return evaluate(bridge, snapshot, scope, descriptor, tuple(values), n, start, manifest)[0]
        computed = value(steps, fills) if steps else None
        need(result["value"] == (None if computed is None else str(computed)), "fee/fill value")
        if stop in ("edge", "value_unknown"):
            need(all(s == "fits" for s in next_status), "edge next step witness")
            more = value(steps + 1, nexts)
            need(more is None if stop == "value_unknown" else more is not None and more <= (computed or 0), "edge stop value")
        elif stop != "target":
            need(any(s != "fits" for s in next_status), "depth stop witness")
            need(stop == ("book_exhausted" if "short" in next_status else "level_cap"), "depth stop kind")
        kills = result["kill_prices"]
        if steps and computed is not None and computed > 0:
            need(type(kills) is list and len(kills) == len(units), "kill shape")
            for i, k in enumerate(kills):
                def at(price):
                    q = fills[i].filled_atoms
                    single = Fill(q, price * q, False, ((price, q),), ((price, q),))
                    return value(steps, fills[:i] + [single] + fills[i + 1:])
                if k is None:
                    need(at(maximums[i]) is not None and at(maximums[i]) > 0, "null kill boundary")
                else:
                    kill = natural(k)
                    need(kill <= maximums[i] and at(kill) is not None and at(kill) <= 0
                         and (kill == 0 or (at(kill - 1) is not None and at(kill - 1) > 0)), "kill boundary")
        else:
            need(kills is None, "kill without positive value")
        tradeable = (not (target is not None and steps < target) and kills is not None
                     and all(k is None or int(result["before"][i][0]) < int(k) for i, k in enumerate(kills)))
        need(result["tradeable"] is tradeable, "tradeable witness")
        if sizing["role"] == "governs":
            need(tradeable, "governing fill")
            effective = [b if a is None else a if b is None else str(min(int(a), int(b))) for a, b in zip(effective, kills)]
    need(carried["kill_prices"] == effective, "effective kills")
    if end_reason == "KILL_PRICE":
        i = carried["kill_leg"]
        need(type(i) is int and 0 <= i < len(units) and effective[i] is not None
             and natural(carried["kill_best"][0]) >= natural(effective[i]), "kill crossing")
    else:
        need(carried["kill_leg"] is None and carried["kill_best"] is None, "unexpected kill crossing")


def check_episodes(root, snapshot, manifest, config):
    snapshot = plain(snapshot)
    entities = tables(root, manifest)
    bridge = FeeBridge(config["fees"], snapshot["plans"])
    need(bridge.semantic_config == manifest["fee_config"] and bridge.engine_identity == manifest["fee_engine_identity"],
         "fee input binding")
    count = fills = 0
    seen, last_end = set(), {}
    for r in rows(root, "episodes.ndjson", manifest["files"]["episodes.ndjson"]):
        count += 1
        key = r["scope"], r["entity"]
        need(key in entities and r["episode_id"] not in seen, "episode entity/identity")
        seen.add(r["episode_id"])
        d = entities[key]
        s = snapshot["scopes"][key[0]]
        start, end = natural(r["start_ns"]), natural(r["end_ns"])
        need(int(s["start_ns"]) <= start < end <= int(s["end_ns"]), "episode scope bounds")
        order_key = (*key, r["kind"])
        need(start >= last_end.get(order_key, 0), "episode overlap")
        last_end[order_key] = end
        if r.get("fill"):
            check_fill(r["fill"], d, start, r["end_reason"], manifest, snapshot, key[0], bridge)
            fills += 1
    return {"episodes": count, "fills": fills}
