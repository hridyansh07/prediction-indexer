"""Pinned two-leg mask routes and closed research configuration."""

from dataclasses import replace
from itertools import combinations, product
import json

from replay.complement_contract import policy_config as sdk_policy, reason, experiment as sdk_experiment
from replay.economic_sdk.types import Basket
from replay.economic_sdk.outcomes import outcome_scope
from replay.fees.domain import Asset, AssetKind, InstrumentEconomics
from replay.fees.artifacts import parse_canonical
from replay.preparation import digest, encoded
from replay.strategy_sdk import plain, PreparedInput
from replay.streams.protocol import obj, require

STRATEGY = "cross_venue_arbitrage_v1"
SETTLEMENT = "normal_resolution_only"
MAX_PAIRS = 4096
SCALE = 36
UNIT = 10 ** SCALE
DIAGNOSTICS = ("SELF_CROSSED_LEG", "ECONOMICS_UNKNOWN", "VALUATION_UNKNOWN")


def asset_row(asset):
    return {"kind": asset.kind.value, "ledger": asset.chain, "token": asset.token}


def asset_value(row):
    obj(row, "kind ledger token")
    return Asset(AssetKind(row["kind"]), row["ledger"], row["token"])


def valuation_config(value):
    if value is None:
        return None
    value = obj(plain(value), "version kind unit assets")
    require(type(value["version"]) is int and value["version"] == 1, "valuation version")
    require(value["kind"] == "PARITY_SCENARIO" and value["unit"] == "research_dollar",
            "explicit parity valuation scenario required")
    require(type(value["assets"]) is list and len(value["assets"]) <= 16, "valuation asset bound")
    for row in value["assets"]:
        require(asset_value(row).kind is not AssetKind.OUTCOME, "cannot value outcome tokens as cash")
    keys = [encoded(row) for row in value["assets"]]
    require(keys == sorted(set(keys)), "valuation assets sorted and unique")
    return value


def policy_config(value):
    policy = sdk_policy(value)
    require(policy["version"] == 2, "cross-venue requires SDK policy 2")
    require(all(c["kind"] == "time_shift" for c in policy["controls"]),
            "cross-venue supports time_shift controls only")
    return policy


def experiment_identity(snapshot_sha, policy, fees, valuation):
    return digest({"strategy": STRATEGY, "bridge_version": 1, "entity_contract_version": 2, "native_scales": True,
                   "settlement_model": SETTLEMENT, "snapshot_sha256": snapshot_sha,
                   "policy": policy, "fees": fees, "valuation": valuation})


def experiment(policy, identity):
    return replace(sdk_experiment(policy, identity), strategy=STRATEGY,
                   diagnostic_statuses=DIAGNOSTICS)


def economics_by_key(fee_config):
    result = {}
    require(type(fee_config["instrument_bindings"]) is list, "binding list")
    prior = None
    for row in fee_config["instrument_bindings"]:
        obj(row, "instrument orientation economics")
        key = (row["instrument"], row["orientation"])
        require(prior is None or key > prior, "bindings sorted and unique")
        prior = key
        value = parse_canonical((json.dumps(row["economics"], sort_keys=True, separators=(",", ":")) + "\n").encode())
        require(type(value) is InstrumentEconomics, "binding economics")
        result[key] = value
    return result


class Inputs:
    def __init__(self, config):
        from replay.complement_fees import FeeBridge
        config = obj(plain(config), "version snapshot_directory snapshot_sha256 fees policy valuation")
        require(len(encoded(config)) <= 8 * 1024 * 1024, "configuration budget")
        self.prepared = PreparedInput({k: config[k] for k in ("version", "snapshot_directory", "snapshot_sha256")})
        self.snapshot = plain(self.prepared.snapshot)
        self.policy = policy_config(config["policy"])
        self.valuation = valuation_config(config["valuation"])
        self.bridge = FeeBridge(config["fees"], self.snapshot["plans"])
        self.identity = experiment_identity(self.prepared.sha256, self.policy,
                                            self.bridge.semantic_config, self.valuation)


def source_key(key):
    if key[0].startswith("kalshi:"):
        return key[0], "complement" if key[1] == "outcome" else "outcome"
    return key


def baskets(snapshot, policy, scope_index):
    scope = snapshot["scopes"][scope_index]
    masks = outcome_scope(snapshot, scope_index)
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    books, missing = [], []
    for member in scope["members"]:
        if not member["capture_selected"] or not member["books"]:
            missing.append(member)
        for b in member["books"]:
            books.append(({"market_id": member["market_id"], **plain(b)}, member["capture_selected"]))
    books.sort(key=lambda row: (row[0]["instrument"], row[0]["orientation"]))
    result = []

    def emit(legs, admission, why, *, missing_market=None):
        acquired = tuple((b["instrument"], b["orientation"]) for b in legs)
        sources = tuple(source_key(k) for k in acquired)
        venues = "+".join(sorted({k[0].split(":", 1)[0] for k in acquired}))
        if not venues:
            venues = missing_market.split(":", 1)[0] if missing_market else "unavailable"
        route = digest(legs if missing_market is None else missing_market)
        # Admission is independent of requested depth. A null size records its
        # scoped denominator once, including in each enabled pair control.
        for size in ([None] if admission is not None else policy["sizes_contracts"]):
            descriptor = {"venue": venues, "basket_kind": "CROSS_VENUE_COMPLETE_SET",
                          "market_id": missing_market or route, "route_id": route, "legs": legs,
                          "mask_legs": [{"status": masks.status(k)[0], "reason": masks.status(k)[1],
                             "space_shape_id": masks.leg(k).shape_id if masks.leg(k) else None,
                             "claim_id": masks.leg(k).claim_id if masks.leg(k) else None,
                             "negated": masks.leg(k).negated if masks.leg(k) else None} for k in acquired],
                          "ask_sources": [{"instrument": k[0], "orientation": k[1],
                              "kind": "projected_opposite_bid" if a[0].startswith("kalshi:") else "native_ask"}
                              for k, a in zip(sources, acquired)],
                          "admission": admission, "direction": "buy", "size_contracts": size,
                          "settlement_model": SETTLEMENT,
                          "outcomes_provider": snapshot.get("outcomes", {}).get("provider")}
            inputs = None if admission is not None else tuple((("kalshi_complement_ask", int(size)), ("bid", int(size)))
                           if key[0].startswith("kalshi:") else
                           (("ask", int(size)), ("crossed", None)) for key in acquired)
            result.append(Basket(descriptor, sources, (venues, route, "buy", 0 if size is None else int(size)),
                                 admission, tuple(reason(r) for r in why),
                                 1 if len(legs) == 2 else None, (venues, size), route, inputs))

    for member in missing:
        emit([], "NOT_CAPTURED", [{"kind": "not_captured", "market_id": member["market_id"]}],
             missing_market=member["market_id"])
    by_venue = {}
    for row in books:
        by_venue.setdefault(row[0]["instrument"].split(":", 1)[0], []).append(row)
    groups = list(combinations(sorted(by_venue), 2))
    pairs = sum(len(by_venue[a]) * len(by_venue[b]) for a, b in groups)
    require(pairs <= MAX_PAIRS, "cross-venue candidate pair limit exceeded")
    candidates = sorted((pair for a, b in groups for pair in product(by_venue[a], by_venue[b])),
                        key=lambda pair: tuple((r[0]["instrument"], r[0]["orientation"]) for r in pair))
    for (a, captured_a), (b, captured_b) in candidates:
        keys = tuple((leg["instrument"], leg["orientation"]) for leg in (a, b))
        admission, why = None, []
        if not captured_a or not captured_b:
            admission, why = "NOT_CAPTURED", [{"kind": "not_captured"}]
        elif not masks.available:
            admission, why = "UNSUPPORTED_SHAPE", [{"kind": "outcomes_unavailable"}]
        else:
            for index, key in enumerate(keys):
                status, detail = masks.status(key)
                if status != "MASKED":
                    why.append({"leg": index, "kind": status, "detail": detail})
            if why:
                admission = "UNSUPPORTED_SHAPE"
            elif not masks.is_partition(keys):
                legs = [masks.leg(k) for k in keys]
                kind = ("mixed_space" if legs[0].shape_id != legs[1].shape_id else
                        "incomplete_space" if masks.spaces[legs[0].shape_id].coverage != "EXHAUSTIVE" else
                        "overlap" if legs[0].keys & legs[1].keys else "gap")
                admission, why = "UNSUPPORTED_SHAPE", [{"kind": kind}]
            elif any(source_key(k) not in plans for k in keys):
                admission, why = "UNSUPPORTED_SHAPE", [{"kind": "ask_source_not_planned"}]
            elif any(k[0].split(":", 1)[0] not in {"kalshi", "polymarket", "limitless"} for k in keys):
                admission, why = "UNSUPPORTED_SHAPE", [{"kind": "unsupported_venue"}]
        emit([a, b], admission, why)
    # No cross-venue pair still has an explicit scope denominator.
    if not result:
        emit([], "UNSUPPORTED_SHAPE", [{"kind": "no_cross_venue_pair"}])
    return tuple(result)
