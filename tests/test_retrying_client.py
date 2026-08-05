from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import urllib.error
import urllib.request

from analysis.durable_http import (
    HttpRequestError,
    RetryingJsonClient,
    _default_transport,
)


def _client(directory: str, transport, **kwargs) -> RetryingJsonClient:
    return RetryingJsonClient(
        Path(directory),
        transport=transport,
        min_interval_seconds={"example.test": 0.0},
        **kwargs,
    )


class RetryingJsonClientTests(unittest.TestCase):
    def test_read_timeout_is_retried(self) -> None:
        """The failure that killed the first World Cup pull."""
        calls = 0

        def transport(request, timeout_seconds):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError("The read operation timed out")
            return 200, {}, b'{"candlesticks":[]}'

        with tempfile.TemporaryDirectory() as directory:
            client = _client(directory, transport)
            with patch("analysis.durable_http.time.sleep"):
                response = client.get_json("https://example.test", "/candlesticks")

        self.assertEqual(response.data, {"candlesticks": []})
        self.assertEqual(calls, 2)
        self.assertEqual(client.transient_retry_attempts, 1)

    def test_wrapped_socket_timeout_is_retried(self) -> None:
        """Errno 60 from urlopen is wrapped by the transport; it must still retry.

        This is the failure that killed the second World Cup pull: the wrapper
        turned it into an HttpRequestError, which the status-marker check then
        classified as permanent.
        """
        calls = 0

        def transport(request, timeout_seconds):
            nonlocal calls
            calls += 1
            if calls == 1:
                # Exactly what _default_transport does with a connect timeout.
                raise urllib.error.URLError(TimeoutError(60, "Operation timed out"))
            return 200, {}, b'{"ok":true}'

        def wrapping_transport(request, timeout_seconds):
            try:
                return transport(request, timeout_seconds)
            except urllib.error.URLError as error:
                from analysis.durable_http import TransientHttpError

                raise TransientHttpError(f"Network error: {error.reason}") from error

        with tempfile.TemporaryDirectory() as directory:
            client = _client(directory, wrapping_transport)
            with patch("analysis.durable_http.time.sleep"):
                response = client.get_json("https://example.test", "/candlesticks")

        self.assertTrue(response.data["ok"])
        self.assertEqual(calls, 2)
        self.assertEqual(client.transient_retry_attempts, 1)

    def test_default_transport_marks_url_errors_transient(self) -> None:
        """Guards the wiring: the real transport must raise the retryable type."""
        from analysis.durable_http import TransientHttpError

        request = urllib.request.Request("http://127.0.0.1:1/unused")
        with self.assertRaises(TransientHttpError):
            _default_transport(request, 0.05)

    def test_server_errors_are_retried(self) -> None:
        calls = 0

        def transport(request, timeout_seconds):
            nonlocal calls
            calls += 1
            if calls < 3:
                return 503, {}, b"unavailable"
            return 200, {}, b'{"ok":true}'

        with tempfile.TemporaryDirectory() as directory:
            client = _client(directory, transport)
            with patch("analysis.durable_http.time.sleep"):
                response = client.get_json("https://example.test", "/x")

        self.assertTrue(response.data["ok"])
        self.assertEqual(calls, 3)

    def test_client_errors_are_not_retried(self) -> None:
        """A 400 means the request itself is wrong; retrying only wastes budget."""
        calls = 0

        def transport(request, timeout_seconds):
            nonlocal calls
            calls += 1
            return 400, {}, b"bad request"

        with tempfile.TemporaryDirectory() as directory:
            client = _client(directory, transport)
            with patch("analysis.durable_http.time.sleep"):
                with self.assertRaises(HttpRequestError):
                    client.get_json("https://example.test", "/x")

        self.assertEqual(calls, 1)

    def test_retries_are_bounded(self) -> None:
        calls = 0

        def transport(request, timeout_seconds):
            nonlocal calls
            calls += 1
            raise TimeoutError("always down")

        with tempfile.TemporaryDirectory() as directory:
            client = _client(directory, transport, transient_retries=2)
            with patch("analysis.durable_http.time.sleep"):
                with self.assertRaises(TimeoutError):
                    client.get_json("https://example.test", "/x")

        self.assertEqual(calls, 3, "initial attempt plus two retries")

    def test_backoff_grows_and_is_capped(self) -> None:
        delays: list[float] = []

        def transport(request, timeout_seconds):
            raise TimeoutError("down")

        with tempfile.TemporaryDirectory() as directory:
            client = _client(
                directory,
                transport,
                transient_retries=4,
                backoff_seconds=5.0,
                maximum_backoff_seconds=15.0,
            )
            with patch(
                "analysis.durable_http.time.sleep",
                side_effect=delays.append,
            ):
                with self.assertRaises(TimeoutError):
                    client.get_json("https://example.test", "/x")

        self.assertEqual(delays, [5.0, 10.0, 15.0, 15.0])


if __name__ == "__main__":
    unittest.main()
