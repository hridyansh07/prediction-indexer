from __future__ import annotations

import copy
import unittest

from targeter.v2.selected_bundles import SelectedBundleIndexError, selected_bundle_rows


RUN_ID = "20260101T000000.000001Z"
ORIGIN_RUN_ID = "20251231T230000.000001Z"


def _candidate() -> dict:
    return {
        "bundle_id": "selected",
        "sport": "esports",
        "game": "counter_strike_2",
        "topology": "series",
        "participants": ["Alpha", "Beta"],
        "participant_keys": ["alpha", "beta"],
        "activation_at": "2026-01-01T01:00:00Z",
        "capture_start_at": "2026-01-01T00:00:00Z",
        "event_refs": ["kalshi:event-a", "polymarket:event-b"],
        "market_ids": [
            "kalshi:series",
            "polymarket:series",
            "polymarket:map-1",
        ],
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
    }


def _target(venue: str, target_id: str, subscriptions: list[str]) -> dict:
    return {
        "bundle_id": "selected",
        "target_id": target_id,
        "canonical_class": "esports.series_moneyline",
        "subscription_ids": subscriptions,
        "activation_at": "2026-01-01T01:00:00Z",
        "capture_start_at": "2026-01-01T00:00:00Z",
        "source_ref": f"source:{target_id}",
        "continuity_score": 10.0,
    }


def _report() -> dict:
    return {
        "report_version": 3,
        "run_id": RUN_ID,
        "generated_at": "2026-01-01T00:00:00Z",
        "input_complete": True,
        "strategy_version": 3,
        "selection_policy": {
            "pre_event_seconds": 3600,
            "post_start_retention_seconds": 21600,
        },
        "candidates": [_candidate(), {"bundle_id": "rejected"}],
        "continuity": {
            "bundles": [],
            "retained_bundle_ids": [],
            "dispositions": {},
        },
        "selection": {
            "bundle_ids": ["selected"],
            "targets": {
                "kalshi": [_target("kalshi", "kalshi:series", ["K-SERIES"])],
                "polymarket": [
                    _target(
                        "polymarket",
                        "polymarket:series",
                        ["pm-yes", "pm-no"],
                    )
                ],
                "limitless": [],
            },
        },
    }


class SelectedBundleIndexTests(unittest.TestCase):
    def test_projects_only_selected_v3_candidates_with_complete_context(self) -> None:
        rows = selected_bundle_rows(_report())

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["selected_bundle_index_version"], 3)
        self.assertEqual(row["occurrence_kind"], "complete")
        self.assertEqual(row["origin_run_id"], RUN_ID)
        self.assertFalse(row["continuity_selected"])
        self.assertIsNone(row["continuity_disposition"])
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

    def test_retained_selection_is_a_strict_immutable_origin_reference(self) -> None:
        report = _report()
        report["candidates"] = []
        continuity_targets = [
            {
                "target_id": target["target_id"],
                "venue": venue,
                "venue_market_id": target["target_id"].split(":", 1)[1],
                "canonical_class": target["canonical_class"],
                "subscription_ids": target["subscription_ids"],
                "activation_at": target["activation_at"],
                "capture_start_at": target["capture_start_at"],
                "source_ref": target["source_ref"],
                "terminal_probe": {"state": "unknown", "reason": "probe_failed"},
            }
            for venue, targets in report["selection"]["targets"].items()
            for target in targets
        ]
        report["continuity"] = {
            "bundles": [
                {
                    "base_run_id": "20251231T235000.000001Z",
                    "bundle_id": "selected",
                    "activation_at": "2026-01-01T01:00:00Z",
                    "score": 10.0,
                    "targets": continuity_targets,
                    "origin_run_id": ORIGIN_RUN_ID,
                    "origin_report_sha256": "a" * 64,
                    "origin_archive_manifest_key": (
                        "targeter-v2/runs/date=2025-12-31/"
                        f"run={ORIGIN_RUN_ID}/run_manifest.json"
                    ),
                    "origin_archive_manifest_sha256": "b" * 64,
                }
            ],
            "retained_bundle_ids": ["selected"],
            "dispositions": {"selected": "retained"},
        }

        row = selected_bundle_rows(report)[0]

        self.assertEqual(row["occurrence_kind"], "retained")
        self.assertTrue(row["continuity_selected"])
        self.assertEqual(row["continuity_disposition"], "retained")
        self.assertEqual(row["origin_run_id"], ORIGIN_RUN_ID)
        self.assertEqual(row["origin_report_sha256"], "a" * 64)
        self.assertEqual(row["origin_archive_manifest_sha256"], "b" * 64)
        self.assertNotIn("sport", row)
        self.assertNotIn("participants", row)
        self.assertNotIn("event_refs", row)
        self.assertNotIn("markets", row)
        self.assertNotIn("relationships", row)

    def test_rejects_pre_v3_or_unpersisted_policy_reports(self) -> None:
        report = _report()
        report["report_version"] = 2
        with self.assertRaisesRegex(SelectedBundleIndexError, "report version 3"):
            selected_bundle_rows(report)

        report = _report()
        report.pop("selection_policy")
        with self.assertRaisesRegex(SelectedBundleIndexError, "selection_policy"):
            selected_bundle_rows(report)

        report = copy.deepcopy(_report())
        report["continuity"]["retained_bundle_ids"] = ["selected"]
        with self.assertRaises(SelectedBundleIndexError):
            selected_bundle_rows(report)


if __name__ == "__main__":
    unittest.main()
