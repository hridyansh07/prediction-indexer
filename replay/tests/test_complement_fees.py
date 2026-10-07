from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from replay.strategies._shared.fee_bridge import FeeBridge
from replay.economic_fills import Fill, walk
from replay.fees.artifacts import build_catalog, source_from_bytes
from replay.fees.domain import (
    Asset, AssetAmount, AssetKind, Component, Fixed, InstrumentEconomics,
    Multiplier, Rate, Venue, Product, canonical,
)
from replay.fees.schedules import (
    Catalog, Kalshi, KalshiKind, Polymarket, Schedule, Scope,
)


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

    def bridge(self, venue="polymarket", model=None, *, bindings=True, assets=True):
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
                    chosen, (self.source,), 0, 100, "synthetic", "test", "test"))
        catalog = Catalog.build(schedules)
        sources = {} if venue == "limitless" else {self.source.sha256: self.source_bytes}
        directory = build_catalog(self.root, catalog, sources)
        config = {"catalog_directory": str(directory), "catalog_identity": catalog.identity,
                  "reference_ns": "50", "limitless_buy_bps": 300, "limitless_sell_bps": 150,
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


if __name__ == "__main__":
    unittest.main()
