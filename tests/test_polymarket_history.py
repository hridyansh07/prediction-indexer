from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analysis.durable_http import JsonResponse
from analysis.polymarket_history import (
    parse_price_points,
    pull_price_history_token,
)


def _response(payload) -> JsonResponse:
    return JsonResponse(
        data=payload,
        url="https://clob.polymarket.com/prices-history",
        cache_path=Path("/tmp/fake.json"),
        from_cache=False,
        fetched_at="2026-07-27T00:00:00Z",
    )


class ParsePricePointTests(unittest.TestCase):
    def test_points_are_normalized_and_sorted(self) -> None:
        rows = parse_price_points(
            {"history": [{"t": 200, "p": 0.42}, {"t": 100, "p": 0.5}]},
            condition_id="0xabc",
            asset_id="123",
        )
        self.assertEqual([row["timestamp_seconds"] for row in rows], [100, 200])
        self.assertEqual(rows[0]["price"], 0.5)
        self.assertEqual(rows[0]["condition_id"], "0xabc")
        self.assertEqual(rows[0]["asset_id"], "123")

    def test_incomplete_points_are_dropped(self) -> None:
        rows = parse_price_points(
            {"history": [{"t": 1}, {"p": 0.2}, {"t": 3, "p": 0.3}, "junk"]},
            condition_id="0xabc",
            asset_id="123",
        )
        self.assertEqual(len(rows), 1)

    def test_empty_history_is_allowed(self) -> None:
        self.assertEqual(
            parse_price_points({"history": []}, condition_id="c", asset_id="a"), []
        )

    def test_malformed_payloads_raise(self) -> None:
        for payload in ([], {"nope": 1}, "text"):
            with self.assertRaises(ValueError):
                parse_price_points(payload, condition_id="c", asset_id="a")


class PriceHistoryPullTests(unittest.TestCase):
    def setUp(self) -> None:
        self.target = {
            "market_id": "0xcondition",
            "event_key": "wc-2026-07-19-esp-arg",
            "market_type": "moneyline_3way",
            "start_time": "2026-07-16T02:57:42Z",
            "end_time": "2026-07-19T19:00:00Z",
        }

    def test_pull_writes_rows_with_provenance(self) -> None:
        captured: list[dict] = []

        class Client:
            cache_hits = 0
            network_requests = 0

            def get_json(self, base_url, path, *, params=None, headers=None):
                captured.append(dict(params or {}))
                return _response({"history": [{"t": 100, "p": 0.41}, {"t": 160, "p": 0.42}]})

        with tempfile.TemporaryDirectory() as directory:
            target_directory = Path(directory) / "token"
            checkpoint = pull_price_history_token(
                Client(),
                target=self.target,
                asset_id="token-1",
                target_directory=target_directory,
            )
            rows = [
                json.loads(line)
                for line in (target_directory / "prices.ndjson").read_text().splitlines()
            ]

        self.assertTrue(checkpoint["complete"])
        self.assertEqual(checkpoint["records_written"], 2)
        self.assertEqual(checkpoint["first_timestamp_seconds"], 100)
        self.assertEqual(checkpoint["last_timestamp_seconds"], 160)
        self.assertEqual(captured[0]["fidelity"], 1)
        self.assertEqual(captured[0]["market"], "token-1")
        # startTs/endTs must come from the target window, not the interval form.
        self.assertIn("startTs", captured[0])
        self.assertIn("endTs", captured[0])
        self.assertEqual(rows[0]["_provenance"]["source"], "polymarket_clob")

    def test_completed_token_makes_no_requests_on_rerun(self) -> None:
        class Client:
            cache_hits = 0
            network_requests = 0
            calls = 0

            def get_json(self, base_url, path, *, params=None, headers=None):
                type(self).calls += 1
                return _response({"history": [{"t": 100, "p": 0.41}]})

        with tempfile.TemporaryDirectory() as directory:
            target_directory = Path(directory) / "token"
            pull_price_history_token(
                Client(),
                target=self.target,
                asset_id="token-1",
                target_directory=target_directory,
            )
            pull_price_history_token(
                Client(),
                target=self.target,
                asset_id="token-1",
                target_directory=target_directory,
            )

        self.assertEqual(Client.calls, 1)

    def test_missing_window_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                pull_price_history_token(
                    object(),
                    target={"market_id": "0xc"},
                    asset_id="t",
                    target_directory=Path(directory),
                )


if __name__ == "__main__":
    unittest.main()
