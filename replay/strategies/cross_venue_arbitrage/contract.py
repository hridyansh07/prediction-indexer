"""Pinned two-leg mask routes and closed research configuration."""

from dataclasses import replace
from itertools import combinations, product
import json

from replay.strategies.same_venue_complement.contract import policy_config as sdk_policy, reason, experiment as sdk_experiment
from replay.strategies._shared.fee_bridge import FeeEconomicsUnavailable
from replay.economic_sdk.fills import fill_policy
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
ACCOUNT = "cross_venue_arbitrage_v1"
# Policy 3 (fill checks, SDK spec §13): the 1-contract net check triggers one
# priced fill per episode; ``fills`` is the SDK's closed fill policy.
FILL_KIND = "net"
TRIGGER_SIZE = "1"
_V3_FIELDS = ("version fills latency_tiers_ns headline_latency_ns minimum_net_gap_per_contract_e18 "
              "leg_skew_buckets_ns verdict audit_intervals profile")


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


def _sweep(policy):
    """Policy 3 as the SDK policy 2 it measures with: one trigger size, no controls."""
    if policy.get("version") != 3:
        return policy
    sweep = {k: v for k, v in policy.items() if k != "fills"}
    sweep.update(version=2, sizes_contracts=[TRIGGER_SIZE], headline_size_contracts=TRIGGER_SIZE,
                 controls=[], controls_episodes=False, controls_slices=False,
                 time_shift_ring_entries="0")
    return sweep


def policy_config(value):
    """Policy 2 (the fixed-size sweep) or policy 3 (fill checks at a 1-contract trigger)."""
    value = plain(value)
    if type(value) is dict and value.get("version") == 3:
        obj(value, _V3_FIELDS)
        sdk_policy(_sweep(value))
        fill_policy(value["fills"], FILL_KIND)
        return value
    policy = sdk_policy(value)
    require(policy["version"] == 2, "cross-venue requires SDK policy 2 or 3")
    require(all(c["kind"] == "time_shift" for c in policy["controls"]),
            "cross-venue supports time_shift controls only")
    return policy


def sizes(policy):
    """Requested sizes: the policy 2 sweep, or policy 3's single trigger size."""
    return _sweep(policy)["sizes_contracts"]


def experiment_identity(snapshot_sha, policy, fees, valuation):
    return digest({"strategy": STRATEGY, "bridge_version": 1, "entity_contract_version": 2, "native_scales": True,
                   "settlement_model": SETTLEMENT, "snapshot_sha256": snapshot_sha,
                   "policy": policy, "fees": fees, "valuation": valuation})


def experiment(policy, identity):
    fills = fill_policy(policy["fills"], FILL_KIND) if policy["version"] == 3 else None
    return replace(sdk_experiment(_sweep(policy), identity), strategy=STRATEGY, policy=policy,
                   policy_sha256=digest(policy), diagnostic_statuses=DIAGNOSTICS, fills=fills)


def assess_net(bridge, *, size, time, fee_legs, account=ACCOUNT, **identity):
    """Fee-assess one all-BUY complete set of ``size`` contracts; one order per leg.

    Shared by the trigger and the fill value so both price fees identically,
    and by every all-BUY complete-set strategy (``account`` labels its orders).
    Returns per-leg ``{assessment_ids, charges, cash, received}`` (``cash`` and
    ``received`` are ``None`` unless that leg's deltas are known), the sorted
    unknown reasons, the assumptions and the evidence. Order identities never
    change an amount.
    """
    try:
        orders, missing = bridge.assess_orders(direction="BUY", size=size, time=time,
                                               legs=tuple(fee_legs), account=account, **identity)
    except FeeEconomicsUnavailable as error:
        orders, missing = (None,) * len(fee_legs), ("fee_economics_unavailable:" + str(error),)
    unknown, assumptions, evidence = list(missing), {"PER_LEVEL_DECLARED_PARTITION_ESTIMATE"}, set()
    legs = []
    for order in orders:
        leg = {"assessment_ids": [], "charges": [], "cash": None, "received": None}
        legs.append(leg)
        if order is None:
            continue
        e, results = order
        leg["assessment_ids"] = [r.identity for r in results]
        cash, received = 0, 0
        known = True
        for r in results:
            assumptions.update(r.assumptions)
            evidence.add(r.evidence.value)
            unknown.extend(f"fee_sdk:{u.component.value}:{u.reason.value}" for u in r.unknowns)
            for charge in r.charges:
                leg["charges"].append({"asset": asset_row(charge.amount.asset),
                    "amount_e36": str(native_amount(charge.amount.amount.atoms, charge.amount.amount.scale)),
                    "component": charge.component.value})
            if r.net_deltas is None:
                known = False
                unknown.append("fee_sdk:missing_net_deltas")
                continue
            for delta in r.net_deltas:
                amount = native_amount(delta.atoms, delta.scale)
                if delta.asset == e.quote:
                    cash += amount
                elif delta.asset == e.outcome:
                    received += amount
                else:
                    known = False
                    unknown.append("unexpected_asset_delta")
        if known:
            leg["cash"], leg["received"] = cash, received
    return legs, unknown, assumptions, evidence


def net_of(legs):
    """Net at scale 36: collateral deltas plus the minimum received payout."""
    return min(leg["received"] for leg in legs) + sum(leg["cash"] for leg in legs)


def native_amount(atoms, scale):
    return atoms * 10 ** (SCALE - scale)


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
        from replay.strategies._shared.fee_bridge import FeeBridge
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
        for size in ([None] if admission is not None else sizes(policy)):
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
