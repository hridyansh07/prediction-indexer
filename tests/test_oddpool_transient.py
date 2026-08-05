from __future__ import annotations

import http.client
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analysis.oddpool_client import RetryingOddpoolClient


class OddpoolTransientRetryTests(unittest.TestCase):
    def test_retries_truncated_response(self) -> None:
        calls = 0

        def transport(request, timeout_seconds):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise http.client.IncompleteRead(b"partial", 100)
            return 200, {}, b'{"snapshots":[],"pagination":{"has_more":false}}'

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
                )

            self.assertEqual(response.data["snapshots"], [])
            self.assertEqual(calls, 2)
            self.assertEqual(client.rate_limit_retry_attempts, 1)


if __name__ == "__main__":
    unittest.main()
