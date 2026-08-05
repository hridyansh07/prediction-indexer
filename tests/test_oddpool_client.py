from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analysis.durable_http import HttpRequestError
from analysis.oddpool_client import RetryingOddpoolClient


class RetryingOddpoolClientTests(unittest.TestCase):
    def test_retries_rate_limit_without_persisting_headers(self) -> None:
        calls = 0

        def transport(request, timeout_seconds):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise HttpRequestError("HTTP 429 for test")
            return 200, {}, b'{"ok":true}'

        with tempfile.TemporaryDirectory() as directory:
            client = RetryingOddpoolClient(
                Path(directory),
                transport=transport,
                min_interval_seconds={"api.oddpool.com": 0.0},
            )
            with patch("analysis.durable_http.time.sleep"):
                response = client.get_json(
                    "https://api.oddpool.com",
                    "/historical/kalshi/orderbook",
                    headers={"X-API-Key": "secret"},
                )

            self.assertEqual(response.data, {"ok": True})
            self.assertEqual(calls, 2)
            self.assertEqual(client.rate_limit_retry_attempts, 1)


if __name__ == "__main__":
    unittest.main()
