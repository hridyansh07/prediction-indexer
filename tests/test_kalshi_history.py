from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analysis.durable_http import JsonResponse
from analysis.kalshi_history import (
    MAX_CANDLESTICK_PERIODS,
    candlestick_windows,
    pull_candlestick_target,
    pull_trades_target,
)


def _response(payload: dict, url: str = "https://kalshi.test/x") -> JsonResponse:
    return JsonResponse(
        data=payload,
        url=url,
        cache_path=Path("/tmp/fake.json"),
        from_cache=False,
        fetched_at="2026-07-27T00:00:00Z",
    )


class CandlestickWindowTests(unittest.TestCase):
    def test_short_range_is_a_single_window(self) -> None:
        windows = candlestick_windows(0, 600, period_minutes=1)
        self.assertEqual(windows, [(0, 600)])

    def test_range_is_split_at_the_api_period_cap(self) -> None:
        # 5719 minutes is the real Spain-Argentina moneyline lifetime, which the
        # API rejects in one request.
        span = 5719 * 60
        windows = candlestick_windows(0, span, period_minutes=1)
        self.assertEqual(len(windows), 2)
        first_span_minutes = (windows[0][1] - windows[0][0]) / 60
        self.assertEqual(first_span_minutes, MAX_CANDLESTICK_PERIODS)
        self.assertEqual(windows[-1][1], span)
        # Windows must tile the range without gaps.
        self.assertEqual(windows[0][1], windows[1][0])

    def test_hourly_periods_allow_a_much_longer_window(self) -> None:
        windows = candlestick_windows(0, 5719 * 60, period_minutes=60)
        self.assertEqual(len(windows), 1)

    def test_rejects_inverted_range(self) -> None:
        with self.assertRaises(ValueError):
            candlestick_windows(100, 0, period_minutes=1)


class CandlestickPullTests(unittest.TestCase):
    def setUp(self) -> None:
        self.target = {
            "ticker": "KXWCGAME-26JUL19ESPARG-ESP",
            "series_ticker": "KXWCGAME",
            "event_key": "wc-2026-07-19-esp-arg",
            "market_type": "moneyline_3way",
            "open_time": "2026-07-15T22:00:00Z",
            "close_time": "2026-07-19T21:18:52Z",
        }

    def test_multi_window_pull_writes_rows_and_completes(self) -> None:
        calls: list[dict] = []

        class Client:
            cache_hits = 0
            network_requests = 0

            def get_json(self, base_url, path, *, params=None, headers=None):
                calls.append(dict(params or {}))
                return _response(
                    {
                        "ticker": "KXWCGAME-26JUL19ESPARG-ESP",
                        "candlesticks": [
                            {
                                "end_period_ts": params["start_ts"] + 60,
                                "yes_bid": {"close_dollars": "0.4100"},
                                "yes_ask": {"close_dollars": "0.4300"},
                            }
                        ],
                    }
                )

        with tempfile.TemporaryDirectory() as directory:
            target_directory = Path(directory) / "market"
            checkpoint = pull_candlestick_target(
                Client(),
                target=self.target,
                target_directory=target_directory,
            )

        self.assertEqual(len(calls), 2, "5719-minute market needs two windows")
        self.assertTrue(checkpoint["complete"])
        self.assertEqual(checkpoint["records_written"], 2)
        self.assertEqual(checkpoint["window_count"], 2)
        self.assertEqual(calls[0]["period_interval"], 1)

    def test_completed_target_makes_no_requests_on_rerun(self) -> None:
        class Client:
            cache_hits = 0
            network_requests = 0
            calls = 0

            def get_json(self, base_url, path, *, params=None, headers=None):
                type(self).calls += 1
                return _response({"candlesticks": []})

        with tempfile.TemporaryDirectory() as directory:
            target_directory = Path(directory) / "market"
            pull_candlestick_target(
                Client(), target=self.target, target_directory=target_directory
            )
            first = Client.calls
            pull_candlestick_target(
                Client(), target=self.target, target_directory=target_directory
            )
            self.assertEqual(Client.calls, first, "rerun must not refetch")

    def test_duplicate_candlesticks_are_not_written_twice(self) -> None:
        class Client:
            cache_hits = 0
            network_requests = 0

            def get_json(self, base_url, path, *, params=None, headers=None):
                # Same candlestick in both windows; only one row should land.
                return _response(
                    {"candlesticks": [{"end_period_ts": 1784152860, "volume_fp": "1.00"}]}
                )

        with tempfile.TemporaryDirectory() as directory:
            target_directory = Path(directory) / "market"
            checkpoint = pull_candlestick_target(
                Client(), target=self.target, target_directory=target_directory
            )
            rows = (target_directory / "candlesticks.ndjson").read_text().splitlines()

        self.assertEqual(len(rows), 1)
        self.assertEqual(checkpoint["records_written"], 1)

    def test_missing_times_raise(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                pull_candlestick_target(
                    object(),
                    target={"ticker": "T", "series_ticker": "S"},
                    target_directory=Path(directory),
                )


class TradePullTests(unittest.TestCase):
    def setUp(self) -> None:
        self.target = {
            "ticker": "KX-TEST",
            "event_key": "test-event",
            "market_type": "moneyline_3way",
            "open_time": "2026-07-15T22:00:00Z",
            "close_time": "2026-07-19T21:18:52Z",
        }

    def test_cursor_pagination_walks_to_the_end(self) -> None:
        class Client:
            cache_hits = 0
            network_requests = 0

            def get_json(self, base_url, path, *, params=None, headers=None):
                if not params.get("cursor"):
                    return _response(
                        {
                            "trades": [{"trade_id": "a", "count_fp": "1.00"}],
                            "cursor": "page2",
                        }
                    )
                return _response(
                    {"trades": [{"trade_id": "b", "count_fp": "2.00"}], "cursor": ""}
                )

        with tempfile.TemporaryDirectory() as directory:
            target_directory = Path(directory) / "market"
            checkpoint = pull_trades_target(
                Client(), target=self.target, target_directory=target_directory
            )
            rows = [
                json.loads(line)
                for line in (target_directory / "trades.ndjson").read_text().splitlines()
            ]

        self.assertTrue(checkpoint["complete"])
        self.assertEqual(checkpoint["pages_completed"], 2)
        self.assertEqual([row["trade_id"] for row in rows], ["a", "b"])
        self.assertEqual(rows[0]["_provenance"]["endpoint"], "/markets/trades")

    def test_empty_page_terminates_even_when_cursor_repeats(self) -> None:
        class Client:
            cache_hits = 0
            network_requests = 0

            def get_json(self, base_url, path, *, params=None, headers=None):
                return _response({"trades": [], "cursor": "stuck"})

        with tempfile.TemporaryDirectory() as directory:
            checkpoint = pull_trades_target(
                Client(),
                target=self.target,
                target_directory=Path(directory) / "market",
            )

        self.assertTrue(checkpoint["complete"])
        self.assertEqual(checkpoint["records_written"], 0)

    def test_repeated_cursor_on_nonempty_pages_raises(self) -> None:
        class Client:
            cache_hits = 0
            network_requests = 0
            calls = 0

            def get_json(self, base_url, path, *, params=None, headers=None):
                type(self).calls += 1
                return _response(
                    {
                        "trades": [{"trade_id": f"t{type(self).calls}"}],
                        "cursor": "same",
                    }
                )

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(RuntimeError):
                pull_trades_target(
                    Client(),
                    target=self.target,
                    target_directory=Path(directory) / "market",
                )


if __name__ == "__main__":
    unittest.main()
