from __future__ import annotations

import copy
import unittest

from targeter.v2.selected_bundles import SelectedBundleIndexError, selected_bundle_rows


class SelectedBundleIndexTests(unittest.TestCase):
    def test_projects_only_selected_bundles_with_a_bounded_capture_window(self) -> None:
        report = {
            "run_id": "20260101T000000.000001Z",
            "generated_at": "2026-01-01T00:00:00Z",
            "input_complete": True,
            "strategy_version": 3,
            "selection_policy": {
                "pre_event_seconds": 3600,
                "post_start_retention_seconds": 21600,
            },
            "candidates": [
                {
                    "bundle_id": "selected",
                    "sport": "esports",
                    "game": "counter_strike_2",
                    "topology": "series",
                    "participants": ["Alpha", "Beta"],
                    "participant_keys": ["alpha", "beta"],
                    "activation_at": "2026-01-01T01:00:00Z",
                    "capture_start_at": "2026-01-01T00:00:00Z",
                    "event_refs": ["kalshi:event-a", "polymarket:event-b"],
                    "market_ids": ["kalshi:series", "polymarket:series", "polymarket:map-1"],
                    "eligible_market_ids": ["kalshi:series", "polymarket:series"],
                    "relationship_analysis": {
                        "relationships": [
                            {
                                "bundle_id": "selected",
                                "left": "kalshi:series#claim=0",
                                "right": "polymarket:series#claim=0",
                                "relationship": "IDENTITY",
                                "scope": "series",
                                "left_venue": "kalshi",
                                "right_venue": "polymarket",
                                "cross_venue": True,
                                "coverage": "EXHAUSTIVE",
                            }
                        ]
                    },
                },
                {
                    "bundle_id": "rejected",
                    "activation_at": "2026-01-01T02:00:00Z",
                },
            ],
            "selection": {
                "bundle_ids": ["selected"],
                "targets": {
                    "kalshi": [
                        {
                            "bundle_id": "selected",
                            "target_id": "kalshi:series",
                            "canonical_class": "esports.series_moneyline",
                            "subscription_ids": ["K-SERIES"],
                        }
                    ],
                    "polymarket": [
                        {
                            "bundle_id": "selected",
                            "target_id": "polymarket:series",
                            "canonical_class": "esports.series_moneyline",
                            "subscription_ids": ["pm-yes", "pm-no"],
                        }
                    ],
                },
            },
        }

        rows = selected_bundle_rows(report)

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["bundle_id"], "selected")
        self.assertEqual(row["planned_capture_end_at"], "2026-01-01T07:00:00Z")
        self.assertEqual(
            {market["target_id"]: market["selected"] for market in row["markets"]},
            {
                "kalshi:series": True,
                "polymarket:map-1": False,
                "polymarket:series": True,
            },
        )
        self.assertEqual(len(row["relationships"]), 1)
        self.assertNotIn("candidates", row)
        self.assertNotIn("record", row)

        legacy = copy.deepcopy(report)
        legacy.pop("selection_policy")
        legacy["strategy_version"] = 1
        self.assertEqual(
            selected_bundle_rows(legacy)[0]["planned_capture_end_at"],
            "2026-01-01T07:00:00Z",
        )
        legacy["strategy_version"] = 99
        with self.assertRaises(SelectedBundleIndexError):
            selected_bundle_rows(legacy)


if __name__ == "__main__":
    unittest.main()
