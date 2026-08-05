"""The segment writer in isolation — no asyncio, no sockets, no venues."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from splices.common.segment import (
    EMPTY_SHA256,
    OPEN_SUFFIX,
    Record,
    SegmentError,
    SegmentWriter,
    read_seal,
    segment_filename,
    window_start_ns,
)

HOUR = 3_600_000_000_000  # ns


def _record(delivery_index: int, visible_ns: int, epoch: str = "aaaa") -> Record:
    line = (
        json.dumps(
            {
                "delivery_index": delivery_index,
                "visible_ns": visible_ns,
                "connection_epoch": epoch,
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    return Record(line=line, visible_ns=visible_ns, delivery_index=delivery_index, epoch=epoch)


class WindowAlignmentTests(unittest.TestCase):
    def test_windows_align_to_the_unix_epoch(self):
        """Which is midnight UTC, so any divisor of a day aligns to the day too."""
        for seconds in (1800, 60, 300, 3600):
            start = window_start_ns(1_785_352_207_071_000_000, seconds)
            self.assertEqual(start % (seconds * 1_000_000_000), 0)

    def test_a_time_inside_a_window_floors_to_its_start(self):
        base = window_start_ns(0, 1800)
        self.assertEqual(base, 0)
        self.assertEqual(window_start_ns(1799 * 1_000_000_000, 1800), 0)
        self.assertEqual(window_start_ns(1800 * 1_000_000_000, 1800), 1800 * 1_000_000_000)

    def test_the_filename_stamp_keeps_the_legacy_width(self):
        """A shorter spelling sorts wrong against pre-cutover names.

        `20260730T163000Z` sorts after `20260730T163001000000` because `Z`
        exceeds `1` in ASCII, so a tree holding both would read out of order —
        silently, and exactly at a cutover.
        """
        name = segment_filename(1_785_336_600_000_000_000, 0, "abcd1234")
        stamp = name.split("-")[0]
        self.assertEqual(len(stamp), 21, stamp)  # YYYYmmdd T HHMMSS ffffff
        self.assertTrue(stamp.endswith("000000"), "aligned windows have no sub-second part")
        self.assertLess(stamp, "20260730T163001000000")


class SegmentWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.addCleanup(self._directory.cleanup)

    def _writer(self, **kwargs) -> SegmentWriter:
        writer = SegmentWriter(self.root, "polymarket", HOUR, segment_seconds=1800, **kwargs)
        # Production always seals; tests that deliberately leave one open would
        # otherwise leak the handle into the next test.
        self.addCleanup(lambda: writer.seal("test_cleanup"))
        return writer

    def test_an_unsealed_segment_wears_the_open_suffix(self):
        """So the ingester skips it: its extension is `open`, not `ndjson`."""
        writer = self._writer()
        self.assertTrue(writer.open_path.name.endswith(OPEN_SUFFIX))
        self.assertTrue(writer.open_path.exists())
        self.assertFalse(writer.data_path.exists())
        self.assertEqual(writer.open_path.suffix, ".open")

    def test_the_incremental_digest_equals_a_digest_of_the_file(self):
        """The whole point of maintaining it during append is never rereading."""
        writer = self._writer()
        writer.write_batch([_record(1, 100), _record(2, 200)])
        writer.write_batch([_record(3, 300)])
        seal = writer.seal("boundary")
        self.assertEqual(seal.sha256, hashlib.sha256(writer.data_path.read_bytes()).hexdigest())
        self.assertEqual(seal.byte_length, writer.data_path.stat().st_size)
        self.assertEqual(seal.line_count, 3)

    def test_a_partial_batch_failure_is_rolled_back_before_retry(self):
        """A retry must start at the last byte represented by the accumulator."""
        writer = self._writer()
        records = [_record(1, 100), _record(2, 200)]
        real_handle = writer._handle

        class PartialFailure:
            def __init__(self):
                self.failed = False

            def write(self, payload):
                if not self.failed:
                    self.failed = True
                    midpoint = max(1, len(payload) // 2)
                    real_handle.write(payload[:midpoint])
                    real_handle.flush()
                    raise OSError("simulated failure after a partial write")
                return real_handle.write(payload)

            def __getattr__(self, name):
                return getattr(real_handle, name)

        writer._handle = PartialFailure()
        with self.assertRaises(OSError):
            writer.write_batch(records)
        writer.write_batch(records)
        seal = writer.seal("boundary")
        data = writer.data_path.read_bytes()

        self.assertEqual(data, b"".join(record.line for record in records))
        self.assertEqual(seal.line_count, 2)
        self.assertEqual(seal.byte_length, len(data))
        self.assertEqual(seal.sha256, hashlib.sha256(data).hexdigest())

    def test_the_digest_is_not_domain_separated(self):
        """It must equal `replay.stream.ObjectIdentity.sha256` exactly, or
        `build_input_manifest` stops being a seal verifier and becomes a second
        implementation to keep in step."""
        writer = self._writer()
        writer.write_batch([_record(1, 100)])
        seal = writer.seal("boundary")
        self.assertEqual(seal.sha256, hashlib.sha256(writer.data_path.read_bytes()).hexdigest())

    def test_a_quiet_lane_seals_an_empty_segment(self):
        """This is what distinguishes 'nothing happened' from 'not finished'."""
        seal = self._writer().seal("boundary")
        self.assertEqual(seal.line_count, 0)
        self.assertEqual(seal.byte_length, 0)
        self.assertEqual(seal.sha256, EMPTY_SHA256)
        self.assertIsNone(seal.first_delivery_index)
        self.assertIsNone(seal.last_visible_ns)

    def test_a_segment_records_every_epoch_it_spans(self):
        """Spanning epochs is the point of the design, so it is stated on the
        seal rather than left to be recovered by reparsing the file."""
        writer = self._writer()
        writer.write_batch([_record(1, 100, "aaaa"), _record(2, 200, "aaaa")])
        writer.write_batch([_record(3, 300, "bbbb")])
        seal = writer.seal("boundary")
        self.assertEqual(seal.epochs, ("aaaa", "bbbb"))
        self.assertTrue(seal.delivery_index_dense)

    def test_a_delivery_index_hole_is_recorded_not_hidden(self):
        writer = self._writer()
        writer.write_batch([_record(1, 100), _record(3, 200)])
        self.assertFalse(writer.seal("boundary").delivery_index_dense)

    def test_a_backwards_receive_clock_sets_the_ordering_status(self):
        """§2 names this exact value; the writer detects it online rather than
        waiting for seal-time revalidation."""
        writer = self._writer()
        writer.write_batch([_record(1, 500), _record(2, 400)])
        seal = writer.seal("boundary")
        self.assertFalse(seal.visible_non_decreasing)
        self.assertEqual(seal.ordering_status, "visible_clock_regression")

    def test_sealing_is_idempotent(self):
        writer = self._writer()
        writer.write_batch([_record(1, 100)])
        first = writer.seal("boundary")
        self.assertIs(writer.seal("shutdown"), first)

    def test_writing_after_a_seal_is_refused(self):
        writer = self._writer()
        writer.seal("boundary")
        with self.assertRaises(SegmentError):
            writer.write_batch([_record(1, 100)])

    def test_the_seal_lands_beside_the_data_and_round_trips(self):
        writer = self._writer()
        writer.write_batch([_record(7, 100)])
        seal = writer.seal("shutdown", extra={"queue_high_water": 3})
        stored = read_seal(writer.seal_path)
        self.assertEqual(stored["data_file"], writer.data_path.name)
        self.assertEqual(stored["seal_reason"], "shutdown")
        self.assertEqual(stored["first_delivery_index"], 7)
        self.assertEqual(stored["queue_high_water"], 3, "extras are carried through")
        self.assertEqual(stored["sha256"], seal.sha256)

    def test_reopening_the_same_segment_is_refused(self):
        writer = self._writer()
        writer.seal("boundary")
        with self.assertRaises(SegmentError):
            SegmentWriter(
                self.root, "polymarket", HOUR, segment_seconds=1800,
                segment_id=writer.segment_id,
            )

    def test_the_seal_syscall_order_commits_data_before_its_marker(self):
        """The order is the durability argument, so it is asserted directly.

        Data fsynced, *then* renamed, *then* the directory synced, *then* the
        seal written and its own rename synced. A seal reaching disk before the
        rename would name a file that does not exist under that name.
        """
        writer = self._writer()
        writer.write_batch([_record(1, 100)])

        events: list[str] = []
        real_replace, real_fsync = os.replace, os.fsync

        def record_replace(source, target):
            events.append(f"replace:{Path(target).suffix}")
            return real_replace(source, target)

        def record_fsync(descriptor):
            events.append("fsync")
            return real_fsync(descriptor)

        os.replace, os.fsync = record_replace, record_fsync
        try:
            writer.seal("boundary")
        finally:
            os.replace, os.fsync = real_replace, real_fsync

        self.assertEqual(
            events,
            [
                "fsync",              # 2: the data file's contents
                "replace:.ndjson",    # 3: .ndjson.open -> .ndjson
                "fsync",              #    the rename itself
                "fsync",              # 4: the seal's temporary file
                "replace:.json",      #    tmp -> .seal.json
                "fsync",              # 5: the seal's rename
            ],
        )

    def test_a_directory_fsync_failure_prevents_a_successful_seal(self):
        writer = self._writer()
        writer.write_batch([_record(1, 100)])
        real_fsync = os.fsync

        def fail_directory(descriptor):
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                raise OSError("simulated directory fsync failure")
            return real_fsync(descriptor)

        with mock.patch("splices.common.segment.os.fsync", fail_directory):
            with self.assertRaisesRegex(OSError, "directory fsync"):
                writer.seal("boundary")


if __name__ == "__main__":
    unittest.main()
