from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from analysis.durable_http import DurableJsonClient
from analysis.partition_pipeline import run_partition_pipeline, sha256_file


HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None


@unittest.skipUnless(HAS_PYARROW, "pyarrow is required for the integration test")
class PartitionPipelineIntegrationTests(unittest.TestCase):
    @staticmethod
    def _write_json(path: Path, value) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(value, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )

    @staticmethod
    def _write_cache(cache_root: Path, url: str, value) -> None:
        host = url.split("/")[2]
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        path = cache_root / host / f"{digest}.json"
        PartitionPipelineIntegrationTests._write_json(path, value)

    @staticmethod
    def _provenance(target_id: str) -> dict:
        return {"source": "oddpool", "target_id": target_id}

    def test_offline_rerun_is_content_identical(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data"
            history = data / "history"
            cache = data / "cache" / "http"
            manifest_path = data / "manifest.json"
            output_root = data / "outputs"

            event_key = "event"
            condition_id = "0xcondition"
            targets = [
                {
                    "target_id": "kalshi:KA",
                    "event_key": event_key,
                    "venue": "kalshi",
                    "market_id": "KA",
                    "outcome": "A",
                    "end_time": "2026-07-18T00:02:00Z",
                },
                {
                    "target_id": "kalshi:KB",
                    "event_key": event_key,
                    "venue": "kalshi",
                    "market_id": "KB",
                    "outcome": "B",
                    "end_time": "2026-07-18T00:02:00Z",
                },
                {
                    "target_id": f"polymarket:{condition_id}",
                    "event_key": event_key,
                    "venue": "polymarket",
                    "market_id": condition_id,
                    "outcome_tokens": [
                        {"asset_id": "PA", "outcome": "A"},
                        {"asset_id": "PB", "outcome": "B"},
                    ],
                    "end_time": "2026-07-18T00:02:00Z",
                },
            ]
            manifest = {
                "history_targets": targets,
                "matches": [
                    {
                        "event_key": event_key,
                        "history_targets": targets,
                        "kalshi": {
                            "mutually_exclusive": True,
                            "market_ids": ["KA", "KB"],
                            "rules_hashes": ["ra", "rb"],
                        },
                        "polymarket": {
                            "condition_id": condition_id,
                            "outcome_tokens": targets[2]["outcome_tokens"],
                            "rules_hash": "rp",
                            "resolution_source": "source",
                        },
                    }
                ],
            }
            self._write_json(manifest_path, manifest)

            history_targets = []
            for target in targets[:2]:
                target_dir = history / "kalshi" / target["market_id"]
                rows = []
                for timestamp in (1_784_352_060_000, 1_784_352_120_000):
                    if target["market_id"] == "KA":
                        yes_price, no_price = 0.40, 0.55
                    else:
                        yes_price, no_price = 0.55, 0.40
                    rows.append(
                        {
                            "_provenance": self._provenance(target["target_id"]),
                            "market_id": target["market_id"],
                            "timestamp": timestamp,
                            "yes_bids": [
                                {"price": str(yes_price), "size": 200}
                            ],
                            "no_bids": [
                                {"price": str(no_price), "size": 200}
                            ],
                            "best_yes_bid": yes_price,
                            "best_yes_ask": 1 - no_price,
                        }
                    )
                snapshots = target_dir / "snapshots.ndjson"
                target_dir.mkdir(parents=True)
                snapshots.write_text(
                    "".join(
                        json.dumps(row, separators=(",", ":"), sort_keys=True)
                        + "\n"
                        for row in rows
                    ),
                    encoding="utf-8",
                )
                history_targets.append(
                    {
                        **target,
                        "complete": True,
                        "snapshots_path": str(snapshots),
                    }
                )

            poly_dir = history / "polymarket" / condition_id
            poly_dir.mkdir(parents=True)
            poly_rows = []
            for timestamp in (1_784_352_060_000, 1_784_352_120_000):
                poly_rows.extend(
                    [
                        {
                            "_provenance": self._provenance(
                                targets[2]["target_id"]
                            ),
                            "market_id": condition_id,
                            "asset_id": "PA",
                            "timestamp": timestamp,
                            "bids": [{"price": "0.40", "size": 200}],
                            "asks": [{"price": "0.45", "size": 200}],
                            "best_bid": 0.40,
                            "best_ask": 0.45,
                        },
                        {
                            "_provenance": self._provenance(
                                targets[2]["target_id"]
                            ),
                            "market_id": condition_id,
                            "asset_id": "PB",
                            "timestamp": timestamp,
                            "bids": [{"price": "0.55", "size": 200}],
                            "asks": [{"price": "0.60", "size": 200}],
                            "best_bid": 0.55,
                            "best_ask": 0.60,
                        },
                    ]
                )
            poly_snapshots = poly_dir / "snapshots.ndjson"
            poly_snapshots.write_text(
                "".join(
                    json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n"
                    for row in poly_rows
                ),
                encoding="utf-8",
            )
            history_targets.append(
                {
                    **targets[2],
                    "complete": True,
                    "snapshots_path": str(poly_snapshots),
                }
            )
            self._write_json(history / "run.json", {"targets": history_targets})

            series_url = DurableJsonClient.build_url(
                "https://external-api.kalshi.com/trade-api/v2",
                "/series/KXDOTA2GAME",
            )
            changes_url = DurableJsonClient.build_url(
                "https://external-api.kalshi.com/trade-api/v2",
                "/series/fee_changes",
                {
                    "series_ticker": "KXDOTA2GAME",
                    "show_historical": True,
                },
            )
            self._write_cache(
                cache,
                series_url,
                {
                    "series": {
                        "fee_type": "quadratic",
                        "fee_multiplier": 1,
                    }
                },
            )
            self._write_cache(
                cache,
                changes_url,
                {"series_fee_change_arr": []},
            )
            self._write_json(
                cache / "gamma-api.polymarket.com" / "market.json",
                {
                    "conditionId": condition_id,
                    "feesEnabled": True,
                    "feeType": "sports",
                    "feeSchedule": {"rate": 0.03, "exponent": 1},
                },
            )

            config = {
                "version": 1,
                "dataset_name": "fixture",
                "manifest_path": str(manifest_path),
                "history_job_directory": str(history),
                "http_cache_root": str(cache),
                "output_root": str(output_root),
                "bar_seconds": 60,
                "max_snapshot_age_seconds": 60,
                "low_skew_seconds": 5,
                "skew_bucket_edges_seconds": [5, 15, 60],
                "sizes_contracts": [1, 100],
                "validation": {
                    "price_tolerance": 1e-9,
                    "size_tolerance": 1e-6,
                    "polymarket_pair_tolerance_ms": 100,
                    "minimum_polymarket_complement_rate": 0.98,
                    "minimum_distinct_nonzero_level_counts": 1,
                    "maximum_share_at_observed_level_cap": 1.0,
                },
                "fees": {
                    "kalshi_series_ticker": "KXDOTA2GAME",
                    "kalshi_base_taker_rate": 0.07,
                    "kalshi_base_rate_effective_at": "2026-02-05T00:00:00Z",
                    "kalshi_base_rate_source": "fixture",
                    "kalshi_normal_rounding_dollars": 0.0001,
                    "kalshi_conservative_rounding_dollars": 0.01,
                    "polymarket_normal_rounding_dollars": 0.00001,
                    "polymarket_conservative_rounding_dollars": 0.00001,
                    "fixed_execution_costs_dollars": {
                        "kalshi": 0,
                        "polymarket": 0,
                    },
                },
                "gate": {
                    "ticket_size_contracts": 100,
                    "minimum_valid_low_skew_observations_per_event": 1,
                    "minimum_positive_rate": 0.01,
                    "minimum_passing_events": 1,
                    "direction": "long",
                    "short_gate_enabled": False,
                    "same_venue_class": "PARTITION_KALSHI_EVENT",
                    "cross_venue_class": "PARTITION_CROSS_VENUE",
                },
            }
            config_path = root / "config.json"
            self._write_json(config_path, config)

            first = run_partition_pipeline(
                config_path,
                project_root=project_root,
                offline=True,
            )
            first_directory = Path(first["output_directory"])
            hashes = {
                path.name: sha256_file(path)
                for path in first_directory.iterdir()
                if path.is_file()
            }
            second = run_partition_pipeline(
                config_path,
                project_root=project_root,
                offline=True,
            )
            second_directory = Path(second["output_directory"])
            second_hashes = {
                path.name: sha256_file(path)
                for path in second_directory.iterdir()
                if path.is_file()
            }

            self.assertEqual(first["run_id"], second["run_id"])
            self.assertEqual(hashes, second_hashes)
            self.assertEqual(first["oddpool_network_requests"], 0)
            self.assertEqual(first["metadata_network_requests"], 0)
            report = (first_directory / "report.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("## Depth and fillability", report)
            self.assertIn("## Positive run lengths", report)
            run_manifest = json.loads(
                (first_directory / "run_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertIn("raw_fee_formula", run_manifest["fee_model"][
                "execution_assumptions"
            ])
            self.assertTrue(
                run_manifest["fee_model"]["raw_metadata_hashes"][
                    "kalshi_series_response"
                ]
            )


if __name__ == "__main__":
    unittest.main()
