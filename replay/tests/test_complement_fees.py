from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from replay.strategies._shared.fee_bridge import FeeBridge
from replay.strategies.cross_venue_arbitrage.contract import assess_net
from replay.economic_fills import Fill, walk
from replay.fees.lazy import LazyAssessmentIds, assessment_json
from replay.fees.artifacts import build_catalog, source_from_bytes
from replay.fees.domain import (
    Asset, AssetAmount, AssetKind, Component, Fixed, InstrumentEconomics,
    Multiplier, Rate, Venue, Product, canonical,
)
from replay.fees.schedules import (
    Catalog, Kalshi, KalshiKind, Polymarket, Schedule, Scope,
)
from replay.preparation import encoded


class EconomicFillTests(unittest.TestCase):
    def test_multi_size_partial_final_level(self):
        fills = walk(((40, 5), (50, 10)), (3, 8, 20))
        self.assertEqual(fills[0], Fill(3, 120, False, ((40, 3),), ((40, 5),)))
        self.assertEqual(fills[1].taken, ((40, 5), (50, 3)))
        self.assertEqual(fills[1].consumed, ((40, 5), (50, 10)))
        self.assertEqual((fills[2].filled_atoms, fills[2].cost, fills[2].depth_limited), (15, 700, True))

        repeated = walk(((40, 2), (40, 3)), (5,))[0]
        self.assertEqual(repeated.taken, ((40, 2), (40, 3)))


class FeeBridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source_bytes = b"synthetic fee evidence\n"
        self.source = source_from_bytes("https://example.invalid/fee", 1, self.source_bytes)

    def tearDown(self):
        self.tmp.cleanup()

    def bridge(self, venue="polymarket", model=None, *, bindings=True, assets=True,
               reference=50, boundary=None, buy_bps=300, sell_bps=150):
        quote = Asset(AssetKind.USDC if venue in {"polymarket", "limitless"} else AssetKind.USD,
                      "chain" if venue in {"polymarket", "limitless"} else "ledger", "quote")
        economics = []
        plans = []
        schedules = []
        for orientation in ("complement", "outcome"):
            instrument = f"{venue}:M"
            outcome = Asset(AssetKind.OUTCOME, "chain", f"{orientation}-token")
            econ = InstrumentEconomics(quote, outcome, AssetAmount(quote, Fixed(1, 0)), 6, 2, 2)
            economics.append(econ)
            plans.append({"instrument": instrument, "orientation": orientation, "lane": venue,
                          "venue": venue, "price_scale": "2", "quantity_scale": "2"})
            chosen = model or (Polymarket(Rate.parse("0.04"), 1, True)
                               if venue == "polymarket" else
                               Kalshi(KalshiKind.QUADRATIC, Multiplier(1, 0)))
            if venue != "limitless":
                schedules.append(Schedule(Component.PLATFORM,
                    Scope(Venue(venue), Product.CLOB, market="M", orientation=orientation),
                    econ, quote, 5 if venue == "polymarket" else 6,
                    chosen, (self.source,), 0, boundary or 100, "synthetic", "test", "test"))
                if boundary is not None:
                    schedules.append(replace(schedules[-1], effective_from=boundary,
                        effective_to=100, model=Polymarket(Rate.parse("0.08"), 1, True)))
        catalog = Catalog.build(schedules)
        sources = {} if venue == "limitless" else {self.source.sha256: self.source_bytes}
        directory = build_catalog(self.root, catalog, sources)
        config = {"catalog_directory": str(directory), "catalog_identity": catalog.identity,
                  "reference_ns": str(reference), "limitless_buy_bps": buy_bps, "limitless_sell_bps": sell_bps,
                  "kalshi_member_class": "NON_DIRECT",
                  "assets": ({venue: {"kind": quote.kind.value, "ledger": quote.chain,
                                      "token": quote.token}} if assets else {}),
                  "instrument_bindings": ([
                      {"instrument": f"{venue}:M", "orientation": orientation,
                       "economics": __import__("json").loads(canonical(econ))}
                      for orientation, econ in zip(("complement", "outcome"), economics)]
                      if bindings else [])}
        return FeeBridge(config, plans), economics

    def legs(self, economics, side="BUY", venue="polymarket"):
        return tuple({"market_id": f"{venue}:M", "key": (f"{venue}:M", orientation),
                      "fill": Fill(100, 4900, False, ((49, 100),), ((49, 200),)),
                      "price_scale": econ.price_scale, "quantity_scale": econ.quantity_scale,
                      "side": side}
                     for orientation, econ in zip(("complement", "outcome"), economics))

    def test_collateral_fees_enter_net_once_and_identity_is_deterministic(self):
        bridge, economics = self.bridge()
        args = dict(experiment="e", scope=1, basket={"market": "M"}, direction="BUY",
                    size=1, time=20, sequence=3, legs=self.legs(economics))
        first = bridge.assess(**args)
        self.assertEqual(first["fee_status"], "KNOWN")
        # Each order spends .49 and pays .01 collateral: 1 - 2 * .50 = 0.
        self.assertEqual(first["gap_net"], 0)
        self.assertEqual(first, bridge.assess(**args))
        self.assertEqual(len(first["assessments"]), 2)
        self.assertNotIn("catalog_directory", bridge.static_config)

    def test_fee_uses_taken_not_full_consumed_level(self):
        bridge, economics = self.bridge()
        legs = tuple({"market_id": "polymarket:M", "key": ("polymarket:M", orientation),
                      "fill": Fill(100, 4900, False, ((49, 100),), ((49, 10_000),)),
                      "price_scale": 2, "quantity_scale": 2, "side": "BUY"}
                     for orientation in ("complement", "outcome"))
        result = bridge.assess(experiment="e", scope=1, basket={}, direction="BUY", size=1,
                               time=20, sequence=1, legs=legs)
        self.assertEqual(result["gap_net"], 0)

    def test_missing_binding_is_unknown(self):
        bridge, economics = self.bridge(bindings=False)
        result = bridge.assess(experiment="e", scope=1, basket={}, direction="BUY", size=1,
                               time=20, sequence=1, legs=self.legs(economics))
        self.assertEqual((result["fee_status"], result["gap_net"]), ("UNKNOWN", None))

    def test_missing_asset_is_valid_config_and_unknown(self):
        bridge, economics = self.bridge(assets=False)
        result = bridge.assess(experiment="e", scope=1, basket={}, direction="BUY", size=1,
                               time=20, sequence=1, legs=self.legs(economics))
        self.assertEqual((result["fee_status"], result["gap_net"]), ("UNKNOWN", None))

    def test_limitless_supported_token_haircut_and_sell_inventory_topup(self):
        bridge, economics = self.bridge("limitless")
        buy = bridge.assess(experiment="e", scope=1, basket={}, direction="BUY", size=1,
                            time=20, sequence=1,
                            legs=self.legs(economics, venue="limitless"))
        # BUY: .98 collateral spent and each 1-token receipt is haircut 3%.
        self.assertEqual(buy["gap_net"], -10_000_000_000_000_000)
        sell_legs = tuple({"market_id": "limitless:M",
                           "key": ("limitless:M", orientation),
                           "fill": Fill(100, 5100, False, ((51, 100),), ((51, 100),)),
                           "price_scale": 2, "quantity_scale": 2, "side": "SELL"}
                          for orientation in ("complement", "outcome"))
        sell = bridge.assess(experiment="e", scope=1, basket={}, direction="SELL", size=1,
                             time=20, sequence=2, legs=sell_legs)
        # SELL: 1.02 proceeds - .0153 fees - 1.00 complete-set inventory.
        self.assertEqual(sell["gap_net"], 4_700_000_000_000_000)

    def test_kalshi_projected_buy_prices_are_caller_owned(self):
        bridge, economics = self.bridge("kalshi")
        # Caller projects a resting 40-cent opposite bid to a 60-cent BUY.
        legs = tuple({"market_id": "kalshi:M", "key": ("kalshi:M", orientation),
                      "fill": walk(((price, 100),), (100,))[0], "price_scale": 2,
                      "quantity_scale": 2, "side": "BUY"}
                     for orientation, price in zip(("complement", "outcome"), (60, 35)))
        result = bridge.assess(experiment="e", scope=1, basket={}, direction="BUY", size=1,
                               time=20, sequence=1, legs=legs)
        self.assertEqual(result["fee_status"], "KNOWN")
        # Complementary projected prices are asymmetric after per-order cent-grid fees:
        # .60 costs .62 and .35 costs .37.
        self.assertEqual(result["gap_net"], 10_000_000_000_000_000)

    def test_memo_exact_quantities_and_ordered_partition_with_pm_ceil5(self):
        bridge, economics = self.bridge()
        base = self.legs(economics)[0]
        args = dict(experiment="e", scope=1, basket={}, size=1, time=20, sequence=1)
        for taken, expected in [(((1, 1),), [1]), (((1, 2),), [1]),
                                (((1, 1), (1, 1)), [1, 1]),
                                (((2, 1), (3, 1)), [1, 2])]:
            fill = Fill(sum(q for _, q in taken), sum(p*q for p, q in taken), False, taken, taken)
            legs, unknown, _, _ = assess_net(bridge, fee_legs=[base | {"fill": fill}], **args)
            self.assertFalse(unknown)
            self.assertEqual([int(c["amount_e36"]) for c in legs[0]["charges"]],
                             [atoms * 10**31 for atoms in expected])
            again = assess_net(bridge, fee_legs=[base | {"fill": fill}], **(args | {"sequence": 2}))[0]
            self.assertEqual(legs[0]["charges"], again[0]["charges"])
            self.assertNotEqual(list(legs[0]["assessment_ids"]), list(again[0]["assessment_ids"]))
        self.assertEqual((bridge.memo_diagnostics["hits"], bridge.memo_diagnostics["misses"]), (4, 4))

    def test_lazy_ids_capture_inputs_while_amounts_reuse_across_evaluations(self):
        bridge, economics = self.bridge()
        args = dict(experiment="é", scope=1, basket={"labels": [{"name": "α"}]}, direction="BUY",
                    size=1, time=20, sequence=3, legs=self.legs(economics))
        eager, _ = bridge.assess_orders(**args)
        expected = [[r.identity for r in results] for _, results in eager]
        first = bridge.assess(**args)
        self.assertTrue(all(isinstance(ids, LazyAssessmentIds) for ids in first["assessments"]))
        self.assertEqual(assessment_json(first)["assessments"], expected)
        for change in ({"experiment": "other"}, {"scope": 2}, {"basket": {"labels": []}},
                       {"size": 2}, {"time": 21}, {"sequence": 4}):
            changed = assessment_json(bridge.assess(**(args | change)))
            self.assertEqual(changed["gap_net"], first["gap_net"])
            self.assertNotEqual(changed["assessments"], expected)
        captured = bridge.assess(**args)
        args["basket"]["labels"][0]["name"] = "changed after evaluation"
        self.assertEqual(assessment_json(captured)["assessments"], expected)
        self.assertEqual(bridge.memo_diagnostics["misses"], 2)
        self.assertEqual(bridge.memo_diagnostics["hits"], 14)

    def test_kalshi_multi_level_memo_retains_rounding_state_in_real_ids(self):
        bridge, economics = self.bridge("kalshi")
        taken = ((37, 13), (63, 7))
        leg = self.legs(economics, venue="kalshi")[0] | {
            "fill": Fill(20, 922, False, taken, taken)}
        args = dict(experiment="e", scope=1, basket={}, direction="BUY", size=1,
                    time=20, sequence=1, legs=(leg,))
        eager, _ = bridge.assess_orders(**args)
        results = eager[0][1]
        self.assertEqual([[c.amount.amount.atoms for c in r.charges] for r in results],
                         [[2122, 9778], [1143, 4757]])
        self.assertIsNone(results[0].prior_order_state)
        self.assertEqual(results[1].prior_order_state.carry.atoms, 9778)
        for sequence in (1, 2, 3):
            actual = bridge.assess(**(args | {"sequence": sequence}))
            # .20 acquired - .06 and .05 cash changes = .09.
            self.assertEqual(actual["gap_net"], 9 * 10**16)
            full, _ = bridge.assess_orders(**(args | {"sequence": sequence}))
            self.assertEqual(assessment_json(actual)["assessments"],
                             [[r.identity for r in full[0][1]]])
        self.assertEqual((bridge.memo_diagnostics["hits"], bridge.memo_diagnostics["misses"]), (2, 1))

    def test_limitless_configured_rates_and_nonlinear_sell_ceil6(self):
        bridge, economics = self.bridge("limitless", buy_bps=123, sell_bps=137)
        base = self.legs(economics, venue="limitless")[0]
        args = dict(experiment="e", scope=1, basket={}, size=1, time=20, sequence=1)
        for side, quantity, atoms in [("BUY", 1, 123), ("SELL", 1, 68), ("SELL", 2, 135)]:
            fill = Fill(quantity, 49 * quantity, False, ((49, quantity),), ((49, quantity),))
            leg = base | {"fill": fill, "side": side}
            for sequence in (1, 2):
                orders, _ = bridge.assess_orders(direction=side, legs=(leg,), defer_ids=True,
                                                **(args | {"sequence": sequence}))
                charge = next(iter(orders[0][1])).charges[0]
                self.assertEqual((charge.amount.amount.atoms, charge.amount.amount.scale), (atoms, 6))
                self.assertEqual(charge.amount.asset, economics[0].outcome if side == "BUY" else economics[0].quote)
        self.assertEqual((bridge.memo_diagnostics["hits"], bridge.memo_diagnostics["misses"]), (3, 3))

    def test_effective_boundary_in_bundle_respects_frozen_reference(self):
        before, economics = self.bridge(reference=49, boundary=50)
        after, _ = self.bridge(reference=50, boundary=50)
        args = dict(experiment="e", scope=1, basket={}, direction="BUY", size=1,
                    time=49, sequence=1, legs=self.legs(economics))
        for time in (49, 50, 51):
            # Fill time crosses the half-open change; the frozen snapshot does not.
            self.assertEqual(before.assess(**(args | {"time": time}))["gap_net"], 0)
            self.assertEqual(after.assess(**(args | {"time": time}))["gap_net"], -2 * 10**16)
        self.assertEqual(before.memo_diagnostics["hits"], 4)
        self.assertNotEqual(before.engine_identity, after.engine_identity)

    def test_unknown_schedule_memo_preserves_unknowns_and_ids(self):
        bridge, economics = self.bridge(reference=100)
        args = dict(experiment="e", scope=1, basket={}, direction="BUY", size=1,
                    time=20, sequence=1, legs=self.legs(economics))
        first = assessment_json(bridge.assess(**args))
        second = assessment_json(bridge.assess(**(args | {"time": 21})))
        self.assertEqual((first["fee_status"], first["gap_net"]), ("UNKNOWN", None))
        self.assertEqual(first["reasons"], ["fee_sdk:platform:missing_schedule"])
        self.assertEqual(first["assumptions"], second["assumptions"])
        self.assertEqual(first["evidence"], second["evidence"])
        self.assertNotEqual(first["assessments"], second["assessments"])
        self.assertEqual(bridge.memo_diagnostics["hits"], 2)
        missing, _ = self.bridge(bindings=False)
        self.assertEqual(assessment_json(missing.assess(**args))["assessments"], [[], []])
        self.assertEqual(missing.memo_diagnostics["entries"], 0)

    def test_lazy_line_preflight_and_bounds_do_not_materialize(self):
        import os
        import sys
        from replay.economic_sdk import bounds
        from replay.fees.engine import FeeAssessment
        from replay.tests.test_economic_sdk import deep_size
        with patch.dict(os.environ, {"REPLAY_FEE_CROSSCHECK": "0"}):
            bridge, economics = self.bridge()
        args = dict(experiment="e", scope=1, basket={"long": "α" * 1000}, direction="BUY",
                    size=1, time=20, sequence=1, legs=self.legs(economics))
        first = bridge.assess(**args)
        with patch.object(FeeAssessment, "identity", property(lambda _: (_ for _ in ()).throw(
                AssertionError("identity must remain deferred")))):
            second = bridge.assess(**args)
            self.assertEqual(first, second)
            preflight = encoded(assessment_json(first, placeholders=True))
            costs = [bounds.json_cost(ids) for ids in first["assessments"]]
        self.assertEqual(len(preflight), len(encoded(assessment_json(first))))
        for ids, cost in zip(first["assessments"], costs):
            actual = (sys.getsizeof(ids) + sys.getsizeof(ids.request)
                      + deep_size(ids.request.retained_inputs) + 2 * deep_size(list(ids)))
            self.assertGreaterEqual(cost, actual)


if __name__ == "__main__":
    unittest.main()
