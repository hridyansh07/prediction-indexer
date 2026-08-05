from __future__ import annotations

import unittest
from decimal import Decimal

from replay.execution import (
    ESTIMATOR_NAME,
    MINIMUM_SURVIVAL_NS,
    DepthEpisode,
    estimate_candidates,
)
from replay.trust import Verdict


class ExecutionEstimatorTests(unittest.TestCase):
    def test_estimator_reports_survival_without_claiming_a_fill(self) -> None:
        episodes = [
            DepthEpisode(
                venue="polymarket",
                market_id="market",
                asset_id=asset,
                direction="long",
                size_contracts=100,
                start_ns=0,
                end_ns=MINIMUM_SURVIVAL_NS + 50,
                vwap=Decimal("0.49"),
                depth_limited=False,
                fingerprint=(("0.49", "100"),),
                trust_verdict=Verdict.TRUSTED,
                right_censored=False,
            )
            for asset in ("a", "b")
        ]
        candidate = {
            "basket_id": "basket",
            "market_id": "market",
            "observation_ns": 10,
            "size_contracts": 100,
            "net_gap_conservative_per_contract": "0.01",
            "leg_asset_ids": ["a", "b"],
        }

        (estimate,) = estimate_candidates([candidate], episodes)

        self.assertEqual(estimate["estimator"], ESTIMATOR_NAME)
        self.assertEqual(
            estimate["status"], "DISPLAYED_DEPTH_SURVIVED_MINIMUM"
        )
        self.assertTrue(estimate["not_a_fill_claim"])
        self.assertNotIn("filled", estimate)
        self.assertNotIn("captured", estimate)


if __name__ == "__main__":
    unittest.main()
