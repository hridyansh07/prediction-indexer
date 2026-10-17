"""Persistent window cache against the real materializer and disposable canonical stores."""

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from archive.archiver.canonical import CanonicalArchiver
from archive.canonical_restore import preflight_canonical_window
from archive.storage.local import LocalObjectStore
from replay.corpus.overlay import OverlayStore, VerifyingSink
from replay.jobs import bundle
from replay.jobs.derivative_cache import DerivativeCache
from replay.tests.test_bundle_cache import MATERIALIZER, RecordingStore
from replay.tests.test_bundle_runner import write_two_venue_canonical
from tests.archive_fixtures import BASE_NS, WINDOW_SECONDS

WIDTH = WINDOW_SECONDS * 1_000_000_000


class WindowCacheTests(unittest.TestCase):
    def setUp(self):
        if not MATERIALIZER.is_file():
            self.skipTest("build the materialize_range example first")
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.canonical = LocalObjectStore(self.root / "canonical")
        self.cache = DerivativeCache(self.root / "cache", 100_000_000)
        self.producer = bundle._describe(MATERIALIZER, self.root / "describe")
        for index in range(5):
            start = BASE_NS + index * WIDTH
            source = self.root / "source"
            write_two_venue_canonical(source, start_ns=start, end_ns=start + WIDTH,
                                      first_sequence=2 * index + 1, delivery_index=index + 1)
            receipt = next(source.glob(f"date=*/window={start}/receipt.json"))
            self.assertEqual(CanonicalArchiver(source, self.canonical).archive_window(receipt).status, "archived")

    def remote(self, index):
        return preflight_canonical_window(self.canonical, BASE_NS + index * WIDTH, WINDOW_SECONDS)

    def ensure(self, name, start=0, end=1, *, lease=None, store=None):
        return bundle.ensure_bundle(name, (BASE_NS + start * WIDTH, BASE_NS + end * WIDTH),
                                    store=store or OverlayStore(self.canonical, VerifyingSink()),
                                    work_root=self.root / name / "work",
                                    derivatives_root=self.root / name / "derivatives", materializer=MATERIALIZER,
                                    window_seconds=WINDOW_SECONDS, derivative_cache=lease)

    def assert_same(self, actual, expected):
        self.assertEqual(actual.receipt_bytes, expected.receipt_bytes)
        self.assertEqual(bundle.parse_bundle_receipt(actual.receipt_bytes), expected.receipt)
        for left, right in zip(actual.pins, expected.pins, strict=True):
            self.assertEqual(left[1:], right[1:])
            for name in bundle.DERIVATIVE_FILES:
                self.assertEqual((left.directory / name).read_bytes(), (right.directory / name).read_bytes())

    def test_index_round_trip_warm_bundle_bytes_and_canonical_read_only(self):
        before = {p.relative_to(self.canonical.root): p.read_bytes()
                  for p in self.canonical.root.rglob("*") if p.is_file()}
        expected = self.ensure("cold", 0, 3)
        with self.cache.lease() as lease:
            first = self.ensure("cold", 0, 3, lease=lease)
            self.assert_same(first, expected)
            self.assertEqual(lease.diagnostics, {"hits": 0, "misses": 3, "cache_bypass": []})
            self.assertLessEqual(self.cache.total_bytes(), self.cache.max_bytes)
        self.assertFalse(list(self.cache.leases.iterdir()))
        reopened = DerivativeCache(self.cache.root, self.cache.max_bytes)
        recorder = RecordingStore(OverlayStore(self.canonical, VerifyingSink()))
        with reopened.lease() as lease:
            second = self.ensure("cold", 0, 3, lease=lease, store=recorder)
            self.assert_same(second, expected)
            self.assertEqual(lease.diagnostics, {"hits": 3, "misses": 0, "cache_bypass": []})
            self.assertFalse(any(key.endswith(".ndjson.zst") for key in recorder.opens))
            self.assertEqual(recorder.puts, [bundle.bundle_receipt_key("cold")])
        durable = LocalObjectStore(self.root / "published")
        with reopened.lease() as lease:
            published = self.ensure("cold", 0, 3, lease=lease, store=OverlayStore(self.canonical, durable))
            self.assert_same(published, expected)
        for pin in published.pins:
            for name in bundle.DERIVATIVE_FILES:
                self.assertEqual(durable.head(bundle.derivative_key(pin.address, name)).byte_length,
                                 (pin.directory / name).stat().st_size)
        self.assertEqual(before, {p.relative_to(self.canonical.root): p.read_bytes()
                                  for p in self.canonical.root.rglob("*") if p.is_file()})

    def test_same_length_corruption_is_detected_and_rebuilt(self):
        with self.cache.lease() as lease:
            original = self.ensure("original", lease=lease)
        path = original.pins[0].directory / "events.ndjson.zst"
        good = path.read_bytes()
        bad = bytearray(good)
        bad[len(bad) // 2] ^= 1
        path.write_bytes(bad)
        expected = self.ensure("rebuilt")
        with self.cache.lease() as lease:
            repaired = self.ensure("rebuilt", lease=lease)
            self.assert_same(repaired, expected)
            self.assertEqual((lease.diagnostics["hits"], lease.diagnostics["misses"]), (0, 1))
        self.assertEqual(path.read_bytes(), good)
        bundle._inspect(MATERIALIZER, repaired.pins[0], self.root / "inspect")

    def test_live_corruption_rebuilds_privately_without_replacing_leased_files(self):
        expected = self.ensure("private")
        with self.cache.lease() as live:
            original = self.ensure("seed", lease=live).pins[0]
            path = original.directory / "events.ndjson.zst"
            bad = bytearray(path.read_bytes())
            bad[len(bad) // 2] ^= 1
            path.write_bytes(bad)
            with self.cache.lease() as next_lane:
                actual = self.ensure("private", lease=next_lane)
                self.assert_same(actual, expected)
                self.assertTrue(actual.pins[0].directory.is_relative_to(self.root / "private" / "derivatives"))
                self.assertEqual(next_lane.diagnostics["cache_bypass"], [
                    {"window_start_ns": BASE_NS, "reason": "corrupt_derivative_in_live_lease"}])
                self.assertEqual(path.read_bytes(), bad)
        with self.cache.lease() as lease:
            repaired = self.ensure("private", lease=lease)
            self.assert_same(repaired, expected)
            self.assertTrue(repaired.pins[0].directory.is_relative_to(self.cache.derivatives))

    def test_warm_cache_does_not_hide_missing_canonical_receipt(self):
        with self.cache.lease() as lease:
            self.ensure("seed", lease=lease)
        (self.canonical.root / bundle.canonical_window_keys(BASE_NS)[2]).unlink()
        with self.cache.lease() as lease:
            result = self.ensure("warm", lease=lease)
            self.assertIsInstance(result, bundle.NotReady)
            self.assertEqual(result.code, "canonical_not_archived")
            self.assertEqual(lease.diagnostics["hits"], 0)

    def test_wrong_window_index_and_changed_producer_never_hit(self):
        with self.cache.lease() as lease:
            original = self.ensure("seed", 0, 2, lease=lease)
            changed = replace(self.producer, materializer_version=self.producer.materializer_version + 1)
            self.assertIsNone(lease.lookup(self.remote(0), changed))
        entries = list(self.cache.index.glob("*.json"))
        first = next(p for p in entries if json.loads(p.read_bytes())["key"]["window_start_ns"] == BASE_NS)
        document = json.loads(first.read_bytes())
        document.update(derivative_address=original.pins[1].address, receipt_sha256=original.pins[1].receipt_sha256)
        first.write_text(json.dumps(document))
        with self.cache.lease() as lease:
            rebuilt = self.ensure("check", lease=lease)
            self.assertEqual(rebuilt.pins[0][1:], original.pins[0][1:])
            self.assertEqual((lease.diagnostics["hits"], lease.diagnostics["misses"]), (0, 1))

    def test_partial_hits_restore_only_misses_and_keep_full_range_bytes(self):
        with self.cache.lease() as lease:
            self.ensure("seed", 1, 2, lease=lease)
        expected = self.ensure("partial", 0, 5)
        recorder = RecordingStore(OverlayStore(self.canonical, VerifyingSink()))
        with self.cache.lease() as lease:
            actual = self.ensure("partial", 0, 5, lease=lease, store=recorder)
            self.assert_same(actual, expected)
            self.assertEqual((lease.diagnostics["hits"], lease.diagnostics["misses"]), (1, 4))
        opened_frames = {key for key in recorder.opens if key.endswith(".ndjson.zst")}
        wanted = {key for index in (0, 2, 3, 4)
                  for key in bundle.canonical_window_keys(BASE_NS + index * WIDTH)[:2]}
        self.assertEqual(opened_frames, wanted)

    def test_two_concurrent_installers_publish_one_identical_address(self):
        source = self.ensure("source").pins[0]
        remote = self.remote(0)
        barrier = threading.Barrier(2)

        def install(index):
            independent = DerivativeCache(self.cache.root, self.cache.max_bytes)
            with independent.lease() as lease:
                barrier.wait(timeout=10)
                return lease.install(source, remote, self.producer, self.root / f"private-{index}",
                                     MATERIALIZER, self.root / f"scratch-{index}")

        with ThreadPoolExecutor(max_workers=2) as pool:
            pins = list(pool.map(install, (0, 1)))
        self.assertEqual(pins[0], pins[1])
        self.assertEqual(len(list(self.cache.index.glob("*.json"))), 1)
        self.assertEqual(list(self.cache.derivatives.iterdir()), [pins[0].directory])
        self.assertFalse(list(self.cache.leases.iterdir()))
        bundle._inspect(MATERIALIZER, pins[0], self.root / "inspect")

    def test_lru_eviction_preserves_live_lease_then_evicts_after_release(self):
        with self.cache.lease() as lease:
            first = self.ensure("first", 0, 1, lease=lease).pins[0]
            second = self.ensure("second", 1, 2, lease=lease).pins[0]
        self.cache.max_bytes = self.cache.total_bytes() + 1000
        with self.cache.lease() as live:
            self.assertEqual(live.lookup(self.remote(0), self.producer), first)
            # Make the leased entry oldest: eviction must still spare it.
            for path in self.cache.index.glob("*.json"):
                address = json.loads(path.read_bytes())["derivative_address"]
                os.utime(self.cache.touches / path.stem, ns=(1, 1) if address == first.address else (2, 2))
            with self.cache.lease() as next_lane:
                third = self.ensure("third", 2, 3, lease=next_lane).pins[0]
                self.assertTrue(first.directory.exists())
                self.assertFalse(second.directory.exists())
                self.assertTrue(third.directory.is_relative_to(self.cache.derivatives))
                self.assertLessEqual(self.cache.total_bytes(), self.cache.max_bytes)
        with self.cache.lease() as next_lane:
            fourth = self.ensure("fourth", 3, 4, lease=next_lane).pins[0]
            self.assertFalse(first.directory.exists())
            self.assertTrue(fourth.directory.is_relative_to(self.cache.derivatives))
            self.assertLessEqual(self.cache.total_bytes(), self.cache.max_bytes)

    def test_capacity_bypass_mixes_leased_hits_with_private_pins(self):
        with self.cache.lease() as lease:
            first = self.ensure("first", lease=lease).pins[0]
        self.cache.max_bytes = self.cache.total_bytes() + 1000
        expected = self.ensure("mixed", 0, 2)
        with self.cache.lease() as lease:
            mixed = self.ensure("mixed", 0, 2, lease=lease)
            self.assert_same(mixed, expected)
            self.assertEqual(mixed.pins[0], first)
            self.assertTrue(mixed.pins[1].directory.is_relative_to(self.root / "mixed" / "derivatives"))
            self.assertEqual(lease.diagnostics, {"hits": 1, "misses": 1,
                             "cache_bypass": [{"window_start_ns": BASE_NS + WIDTH, "reason": "live_leases"}]})
            self.assertLessEqual(self.cache.total_bytes(), self.cache.max_bytes)
            self.assertTrue(first.directory.exists())

    def test_exact_capacity_excludes_bookkeeping_and_bypasses_excess_admissions(self):
        with self.cache.lease() as seed:
            measured = self.ensure("measure", lease=seed).pins[0]
        capacity = (sum((measured.directory / name).stat().st_size for name in bundle.DERIVATIVE_FILES)
                    + sum(path.stat().st_size for path in self.cache.index.glob("*.json")))
        self.cache = DerivativeCache(self.root / "bounded", capacity)
        with self.cache.lease() as first:
            admitted = self.ensure("exact", lease=first).pins[0]
            self.assertTrue(admitted.directory.is_relative_to(self.cache.derivatives))
            self.assertEqual(admitted[1:], measured[1:])
            self.assertEqual(self.cache.total_bytes(), capacity)
            with self.cache.lease() as second:
                warm = self.ensure("second-lane", lease=second).pins[0]
                self.assertEqual(warm, admitted)
                self.assertEqual(second.diagnostics["hits"], 1)
                self.assertEqual(self.cache.total_bytes(), capacity)
                # Actual lease JSON grew, but no immutable cache content did.
                self.assertGreater(sum(path.stat().st_size for path in self.cache.root.rglob("*")
                                       if path.is_file()), capacity)
                private = self.ensure("excess", 1, 2, lease=second).pins[0]
                self.assertTrue(private.directory.is_relative_to(self.root / "excess" / "derivatives"))
                self.assertEqual(second.diagnostics["cache_bypass"][0]["window_start_ns"], BASE_NS + WIDTH)
                self.assertEqual(self.cache.total_bytes(), capacity)
                self.assertTrue(admitted.directory.exists())
        self.assertEqual(self.cache.total_bytes(), capacity)
        # One byte short must bypass, even with no competing live addresses.
        self.cache = DerivativeCache(self.root / "one-byte-short", capacity - 1)
        with self.cache.lease() as lease:
            private = self.ensure("short", lease=lease).pins[0]
            self.assertTrue(private.directory.is_relative_to(self.root / "short" / "derivatives"))
            self.assertEqual(lease.diagnostics["cache_bypass"], [{"window_start_ns": BASE_NS, "reason": "max_bytes"}])
            self.assertEqual(self.cache.total_bytes(), 0)

    def test_oversized_derivative_bypasses_without_populating_index(self):
        self.cache.max_bytes = 1000
        expected = self.ensure("small")
        with self.cache.lease() as lease:
            actual = self.ensure("small", lease=lease)
            self.assert_same(actual, expected)
            self.assertEqual(lease.diagnostics["cache_bypass"], [{"window_start_ns": BASE_NS, "reason": "max_bytes"}])
            self.assertLessEqual(self.cache.total_bytes(), self.cache.max_bytes)
        self.assertFalse(list(self.cache.index.iterdir()))
        self.assertFalse(list(self.cache.derivatives.iterdir()))

    def test_dead_process_lease_is_reclaimed(self):
        code = ("import os,sys; from replay.jobs.derivative_cache import DerivativeCache\n"
                "with DerivativeCache(sys.argv[1], 100000000).lease(): os._exit(0)\n")
        subprocess.run([sys.executable, "-c", code, str(self.cache.root)], check=True, timeout=10)
        stale = list(self.cache.leases.iterdir())
        self.assertEqual(len(stale), 1)
        with self.cache.lease():
            self.assertFalse(stale[0].exists())
        self.assertFalse(list(self.cache.leases.iterdir()))


if __name__ == "__main__":
    unittest.main()
