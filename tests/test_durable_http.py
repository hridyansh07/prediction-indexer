from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analysis.durable_http import DurableJsonClient


class DurableJsonClientTests(unittest.TestCase):
    def test_compressed_response_cache_round_trips_without_plain_json_body(self) -> None:
        calls: list[str] = []

        def transport(request, timeout_seconds):
            calls.append(request.full_url)
            return 200, {"Content-Type": "application/json"}, b'{"items":[1,2,3]}'

        with tempfile.TemporaryDirectory() as directory:
            client = DurableJsonClient(
                Path(directory),
                compress_responses=True,
                transport=transport,
                min_interval_seconds={"example.test": 0.0},
            )
            first = client.get_json("https://example.test", "/items")
            second = client.get_json("https://example.test", "/items")

            self.assertEqual(second.data, first.data)
            self.assertTrue(second.from_cache)
            self.assertEqual(len(calls), 1)
            self.assertEqual(first.cache_path.suffixes[-2:], [".json", ".zst"])
            self.assertEqual(first.cache_path.read_bytes()[:4], b"\x28\xb5\x2f\xfd")
            self.assertFalse(first.cache_path.with_suffix("").exists())

            first.cache_path.with_suffix(".meta.json").unlink()
            recovered = client.get_json("https://example.test", "/items")
            cached_again = client.get_json("https://example.test", "/items")
            self.assertFalse(recovered.from_cache)
            self.assertTrue(cached_again.from_cache)
            self.assertEqual(len(calls), 2)

    def test_identical_request_is_loaded_from_disk(self) -> None:
        calls: list[str] = []

        def transport(request, timeout_seconds):
            calls.append(request.full_url)
            return 200, {"Content-Type": "application/json"}, b'{"ok":true}'

        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            first_client = DurableJsonClient(
                cache_root,
                transport=transport,
                min_interval_seconds={"example.test": 0.0},
            )
            first = first_client.get_json(
                "https://example.test",
                "/items",
                params={"b": 2, "a": 1},
                headers={"X-API-Key": "never-persist-this"},
            )
            second = first_client.get_json(
                "https://example.test",
                "/items",
                params={"a": 1, "b": 2},
                headers={"X-API-Key": "never-persist-this"},
            )

            self.assertEqual(first.data, {"ok": True})
            self.assertFalse(first.from_cache)
            self.assertTrue(second.from_cache)
            self.assertEqual(len(calls), 1)
            self.assertEqual(first_client.network_requests, 1)
            self.assertEqual(first_client.cache_hits, 1)

            metadata = json.loads(
                first.cache_path.with_suffix(".meta.json").read_text(encoding="utf-8")
            )
            self.assertNotIn("never-persist-this", json.dumps(metadata))

            second_client = DurableJsonClient(
                cache_root,
                transport=transport,
                min_interval_seconds={"example.test": 0.0},
            )
            third = second_client.get_json(
                "https://example.test",
                "/items",
                params={"a": 1, "b": 2},
            )
            self.assertTrue(third.from_cache)
            self.assertEqual(second_client.network_requests, 0)
            self.assertEqual(len(calls), 1)

    def test_force_refresh_replaces_cached_response(self) -> None:
        call_count = 0

        def transport(request, timeout_seconds):
            nonlocal call_count
            call_count += 1
            body = json.dumps({"call": call_count}).encode("utf-8")
            return 200, {}, body

        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            original_client = DurableJsonClient(
                cache_root,
                transport=transport,
                min_interval_seconds={"example.test": 0.0},
            )
            original = original_client.get_json("https://example.test", "/value")
            refresh_client = DurableJsonClient(
                cache_root,
                force_refresh=True,
                transport=transport,
                min_interval_seconds={"example.test": 0.0},
            )
            refreshed = refresh_client.get_json("https://example.test", "/value")

            self.assertEqual(original.data, {"call": 1})
            self.assertEqual(refreshed.data, {"call": 2})
            self.assertFalse(refreshed.from_cache)

    def test_response_persistence_can_be_disabled_without_disabling_rate_limits(self) -> None:
        calls: list[str] = []

        def transport(request, timeout_seconds):
            calls.append(request.full_url)
            return 200, {}, b'{"fresh":true}'

        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            client = DurableJsonClient(
                cache_root,
                persist_responses=False,
                transport=transport,
                min_interval_seconds={"example.test": 0.0},
            )
            first = client.get_json("https://example.test", "/value")
            second = client.get_json("https://example.test", "/value")

            self.assertEqual(first.data, {"fresh": True})
            self.assertEqual(second.data, {"fresh": True})
            self.assertFalse(first.from_cache)
            self.assertFalse(second.from_cache)
            self.assertEqual(len(calls), 2)
            self.assertFalse(first.cache_path.exists())
            self.assertFalse(first.cache_path.with_suffix(".meta.json").exists())
            self.assertTrue(
                (cache_root / "_rate_limits" / "example.test.json").exists()
            )


if __name__ == "__main__":
    unittest.main()
