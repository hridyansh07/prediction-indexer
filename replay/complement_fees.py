"""Strict offline bridge from economic ladder fills to the fee SDK."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from replay.economic_fills import Fill
from replay.fees import FeeEngine, Policy, Resolver
from replay.fees.artifacts import load_catalog, parse_canonical
from replay.fees.domain import (
    AccountClass,
    Asset,
    AssetKind,
    Context,
    HypotheticalFill,
    InstrumentEconomics,
    Notional,
    Probability,
    Product,
    Quantity,
    QuotePrice,
    Role,
    Side,
    Venue,
    fixed,
)

_FEE_FIELDS = {
    "catalog_directory", "catalog_identity", "reference_ns",
    "limitless_buy_bps", "limitless_sell_bps", "kalshi_member_class",
    "assets", "instrument_bindings",
}
_ASSET_FIELDS = {"kind", "ledger", "token"}
_BINDING_FIELDS = {"instrument", "orientation", "economics"}


def _tagged(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _unsigned(value, name):
    if type(value) is not str or not value.isascii() or not value.isdecimal() or (
        len(value) > 1 and value[0] == "0"
    ):
        raise ValueError(f"{name} must be a canonical unsigned decimal string")
    return int(value)


class FeeEconomicsUnavailable(ValueError):
    """Native notional cannot be represented by the Fee SDK at its pinned scale."""


class FeeBridge:
    def __init__(self, fees_config: dict, plans: list[dict]):
        if type(fees_config) is not dict or set(fees_config) != _FEE_FIELDS:
            raise ValueError("closed fee configuration required")
        if type(plans) is not list:
            raise TypeError("plans must be a list")
        reference = _unsigned(fees_config["reference_ns"], "reference_ns")
        member_name = fees_config["kalshi_member_class"]
        try:
            member = AccountClass[member_name]
        except (KeyError, TypeError) as error:
            raise ValueError("invalid Kalshi member class") from error
        if member is AccountClass.UNKNOWN:
            raise ValueError("known Kalshi member class required")

        catalog = load_catalog(Path(fees_config["catalog_directory"]))
        if catalog.identity != fees_config["catalog_identity"]:
            raise ValueError("fee catalog identity mismatch")
        policy = Policy(
            include_account_rebates=False,
            include_rounding_refunds=False,
            kalshi_member_class=member,
            limitless_buy_bps=fees_config["limitless_buy_bps"],
            limitless_sell_bps=fees_config["limitless_sell_bps"],
        )
        self._engine = FeeEngine(policy, Resolver(catalog, reference_time=reference))
        self.engine_identity = self._engine.identity
        self.catalog_identity = catalog.identity

        assets = {}
        if type(fees_config["assets"]) is not dict:
            raise ValueError("assets must be an object")
        for venue, item in fees_config["assets"].items():
            if venue not in {v.value for v in Venue} or type(item) is not dict or set(item) != _ASSET_FIELDS:
                raise ValueError("invalid configured venue asset")
            try:
                kind = AssetKind(item["kind"])
            except (ValueError, TypeError) as error:
                raise ValueError("invalid configured asset kind") from error
            if kind is AssetKind.OUTCOME:
                raise ValueError("quote asset cannot be an outcome token")
            assets[venue] = Asset(kind, item["ledger"], item["token"])
        self._assets = assets

        plan_by_key = {}
        for plan in plans:
            required = {"instrument", "orientation", "lane", "venue", "price_scale", "quantity_scale"}
            if type(plan) is not dict or set(plan) != required:
                raise ValueError("closed plan required")
            key = (plan["instrument"], plan["orientation"])
            if key in plan_by_key:
                raise ValueError("duplicate plan key")
            plan_by_key[key] = plan

        bindings = {}
        prior = None
        if type(fees_config["instrument_bindings"]) is not list:
            raise ValueError("instrument_bindings must be a list")
        for item in fees_config["instrument_bindings"]:
            if type(item) is not dict or set(item) != _BINDING_FIELDS:
                raise ValueError("closed instrument binding required")
            key = (item["instrument"], item["orientation"])
            if prior is not None and key <= prior:
                raise ValueError("instrument bindings must be sorted and unique")
            prior = key
            economics = parse_canonical(_tagged(item["economics"]))
            if type(economics) is not InstrumentEconomics:
                raise ValueError("binding economics type mismatch")
            plan = plan_by_key.get(key)
            if plan is None:
                raise ValueError("binding has no matching plan")
            venue_asset = assets.get(plan["venue"])
            if (
                (venue_asset is not None and economics.quote != venue_asset)
                or economics.price_scale != _unsigned(plan["price_scale"], "price_scale")
                or economics.quantity_scale != _unsigned(plan["quantity_scale"], "quantity_scale")
                or economics.payout.asset != economics.quote
                or economics.payout.amount.value != 1
            ):
                raise ValueError("binding conflicts with plan, asset, or unit payout")
            bindings[key] = economics
        self._bindings = bindings
        self.semantic_config = {
            key: fees_config[key] for key in sorted(_FEE_FIELDS - {"catalog_directory"})
        }

    @property
    def static_config(self):
        """Semantic fee configuration suitable for the parent's experiment hash."""
        return self.semantic_config

    def assess_orders(self, *, experiment: str, scope: int, basket: dict, direction: str,
               size: int, time: int, sequence: int, legs: tuple, account="same_venue_complement_v1") -> tuple:
        """Return native per-order assessments; the caller owns basket payout.

        Missing bindings retain a None slot and a visible reason per leg.
        """
        if direction not in {"BUY", "SELL"} or type(legs) is not tuple or not legs:
            raise ValueError("invalid fee scenario")
        expected_side = direction
        prepared = []
        reasons = []
        for leg in legs:
            required = {"market_id", "key", "fill", "price_scale", "quantity_scale", "side"}
            if type(leg) is not dict or set(leg) != required or leg["side"] != expected_side:
                raise ValueError("closed leg with scenario side required")
            key, fill_value = leg["key"], leg["fill"]
            if type(key) is not tuple or len(key) != 2 or type(fill_value) is not Fill:
                raise TypeError("invalid leg key or fill")
            economics = self._bindings.get(key)
            venue_name = key[0].partition(":")[0]
            if economics is None or venue_name not in self._assets:
                prepared.append(None)
                reasons.append(f"missing_fee_binding_or_asset:{key[0]}:{key[1]}")
                continue
            if (leg["price_scale"], leg["quantity_scale"]) != (
                economics.price_scale, economics.quantity_scale
            ):
                raise ValueError("leg scales conflict with binding")
            try:
                venue = Venue(venue_name)
            except ValueError as error:
                raise ValueError("unsupported leg venue") from error
            native_market = leg["market_id"].removeprefix(venue_name + ":")
            native_instrument = key[0].removeprefix(venue_name + ":")
            order_material = {
                "experiment": experiment, "scope": scope, "basket": basket,
                "direction": direction, "size": size, "key": key,
                "time": time, "sequence": sequence,
            }
            order_key = hashlib.sha256(
                json.dumps(order_material, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            fills = []
            for fill_index, (price_atoms, quantity_atoms) in enumerate(fill_value.taken):
                price = QuotePrice(price_atoms, economics.price_scale)
                quantity = Quantity(quantity_atoms, economics.quantity_scale)
                notional_value = price.value * quantity.value
                try:
                    notional = Notional(economics.quote, fixed(notional_value, economics.quote_scale))
                except ValueError as error:
                    raise FeeEconomicsUnavailable("fill notional is not exactly representable") from error
                fills.append(HypotheticalFill(
                    f"{order_key}:{fill_index}",
                    Context(venue, Product.CLOB, native_market, None, None, None,
                            native_instrument, key[1], account,
                            "hypothetical"),
                    economics, Side[direction], price,
                    Probability(price_atoms, economics.price_scale), quantity, notional,
                    time, Role.TAKER, order_key, fill_index, fill_index == 0,
                    counterfactual_revision=order_key,
                ))
            prepared.append((economics, self._engine.assess_many(tuple(fills))))
        return tuple(prepared), tuple(reasons)

    def economics(self, key):
        """Return the configured native economics, or None when required input is absent."""
        return self._bindings.get(key) if key[0].partition(":")[0] in self._assets else None

    def assess(self, *, experiment: str, scope: int, basket: dict, direction: str,
               size: int, time: int, sequence: int, legs: tuple) -> dict:
        prepared, reasons = self.assess_orders(
            experiment=experiment, scope=scope, basket=basket, direction=direction,
            size=size, time=time, sequence=sequence, legs=legs)
        if reasons:
            return self._unknown(reasons, [[] for _ in legs])

        assessments = [[result.identity for result in results] for _, results in prepared]
        all_results = [result for _, results in prepared for result in results]
        unknowns = [unknown for result in all_results for unknown in result.unknowns]
        if unknowns or any(result.net_deltas is None for result in all_results):
            reasons = sorted({
                f"fee_sdk:{unknown.component.value}:{unknown.reason.value}"
                for unknown in unknowns
            } or {"fee_sdk:missing_net_deltas"})
            return self._unknown(reasons, assessments, all_results)

        quote = prepared[0][0].quote
        quote_delta = 0
        received = []
        for economics, results in prepared:
            outcome_delta = 0
            for result in results:
                for delta in result.net_deltas or ():
                    if delta.asset == quote:
                        quote_delta += self._scale_18(delta.atoms, delta.scale)
                    elif delta.asset == economics.outcome:
                        outcome_delta += self._scale_18(delta.atoms, delta.scale)
                    else:
                        return self._unknown(["unexpected_asset_delta"], assessments, all_results)
            received.append(outcome_delta)
        if direction == "BUY":
            gap = quote_delta + min(received)
        else:
            if any(value < -(size * 10**18) for value in received):
                return self._unknown(["outcome_debit_exceeds_supplied_set"], assessments, all_results)
            gap = quote_delta - size * 10**18
        return self._result(gap, "KNOWN", assessments, [], all_results)

    @staticmethod
    def _scale_18(atoms, scale):
        """Convert an SDK native balance to the strategy's exact scale 18."""
        return atoms * 10 ** (18 - scale)

    @staticmethod
    def _result(gap, status, assessments, reasons, results):
        return {
            "gap_net": gap, "fee_status": status, "assessments": assessments,
            "reasons": reasons,
            "assumptions": sorted({"PER_LEVEL_DECLARED_PARTITION_ESTIMATE", *(
                value for result in results for value in result.assumptions
            )}),
            "evidence": sorted({result.evidence.value for result in results}),
        }

    def _unknown(self, reasons, assessments=None, results=()):
        return self._result(None, "UNKNOWN", assessments or [[] for _ in reasons],
                            sorted(set(reasons)), results)
