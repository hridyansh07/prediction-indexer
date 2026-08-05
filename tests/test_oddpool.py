from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analysis.durable_http import JsonResponse
from analysis.oddpool import pull_orderbook_target


class FakeClient:
    def __init__(self) -> None:
        self.calls = 0
        self.cache_hits = 0
        self.network_requests = 0

    def get_json(self, base_url, path, *, params=None, headers=None):
        self.calls += 1
        self.network_requests += 1
        page_two = bool(params.get("pagination_key"))
        payload = {
            "snapshots": [
                {
                    "market_id": "KX-TEST",
                    "timestamp": 2 if page_two else 1,
                    "yes_bids": [],
                    "no_bids": [],
                }
            ],
            "pagination": {
                "has_more": not page_two,
                "pagination_key": None if page_two else "next",
            },
        }
        return JsonResponse(
            data=payload,
            url=f"https://api.oddpool.com{path}",
            cache_path=Path("/tmp/fake.json"),
            from_cache=False,
            fetched_at="2026-07-27T00:00:00Z",
        )


class OddpoolPullerTests(unittest.TestCase):
    def test_checkpointed_pull_resumes_without_repeating_complete_target(self) -> None:
        target = {
            "target_id": "kalshi:KX-TEST",
            "event_key": "test-event",
            "venue": "kalshi",
            "market_id": "KX-TEST",
            "start_time": "2026-07-16T00:00:00Z",
            "end_time": "2026-07-18T00:00:00Z",
        }
        with tempfile.TemporaryDirectory() as directory:
            client = FakeClient()
            output = Path(directory)
            result = pull_orderbook_target(
                client,
                api_key="secret",
                target=target,
                target_directory=output,
                granularity="1m",
                page_limit=200,
            )
            self.assertTrue(result["complete"])
            self.assertEqual(result["records_written"], 2)
            self.assertEqual(client.calls, 2)

            second = pull_orderbook_target(
                client,
                api_key="secret",
                target=target,
                target_directory=output,
                granularity="1m",
                page_limit=200,
            )
            self.assertTrue(second["complete"])
            self.assertEqual(client.calls, 2)

            rows = [
                json.loads(line)
                for line in (output / "snapshots.ndjson")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(len(rows), 2)
            self.assertNotIn("secret", json.dumps(rows))
            self.assertEqual(rows[0]["_provenance"]["source"], "oddpool")


if __name__ == "__main__":
    unittest.main()
