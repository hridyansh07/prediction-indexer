"""The bounded queue and writer thread, against real segments on disk."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from splices.common.segment import Record, read_seal
from splices.common.writer import LaneWriter, validate_segment_seconds

HOUR = 3_600_000_000_000


def _record(delivery_index: int, visible_ns: int, epoch: str = "aaaa") -> Record:
    line = (
        json.dumps(
            {"delivery_index": delivery_index, "visible_ns": visible_ns,
             "connection_epoch": epoch},
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    return Record(line=line, visible_ns=visible_ns, delivery_index=delivery_index, epoch=epoch)


class SegmentPeriodTests(unittest.TestCase):
    def test_a_period_must_tile_the_utc_day(self):
        """Otherwise windows drift across midnight and the `date=` partition,
        derived from the window start, disagrees with the records inside it."""
        for good in (60, 300, 1800, 3600, 86_400):
            self.assertEqual(validate_segment_seconds(good), good)
        for bad in (0, -1, 7, 1700, 100_000):
            with self.assertRaises(ValueError):
                validate_segment_seconds(bad)


class LaneWriterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.addCleanup(self._directory.cleanup)

    def _writer(self, **kwargs) -> LaneWriter:
        kwargs.setdefault("segment_seconds", 1800)
        kwargs.setdefault("clock", lambda: HOUR)
        writer = LaneWriter(self.root, "polymarket", start_ns=HOUR, **kwargs)
        self.addCleanup(writer.close)
        return writer

    async def test_records_reach_disk_in_order(self):
        writer = self._writer()
        writer.start()
        for index in range(1, 51):
            await writer.append(_record(index, index * 100))
        writer.close()

        lines = writer.segment.data_path.read_text().splitlines()
        self.assertEqual(len(lines), 50)
        indices = [json.loads(line)["delivery_index"] for line in lines]
        self.assertEqual(indices, list(range(1, 51)))

    async def test_a_full_queue_blocks_the_producer_and_drops_nothing(self):
        """§8's required failure test: backpressure without loss and without a
        venue reconnect. A timeout on the put would surface in the receive loop
        and be converted into `connection_failed` by the splice's blanket
        handler — the one response to fullness this design forbids."""
        writer = self._writer(queue_capacity=4)
        writer.start()

        for index in range(1, 101):
            await writer.append(_record(index, index * 100))
        writer.close()

        lines = writer.segment.data_path.read_text().splitlines()
        self.assertEqual(len(lines), 100, "every record landed")
        self.assertGreater(writer.queue_high_water, 0)
        self.assertLessEqual(writer.queue_high_water, 4, "the bound held")

    async def test_the_rotation_barrier_partitions_the_stream(self):
        """A record queued before the boundary belongs to the old segment even
        if the disk was busy when the boundary fired."""
        writer = self._writer()
        writer.start()
        await writer.append(_record(1, 100))
        await writer.append(_record(2, 200))
        # Push the barrier in-band, exactly as the rotation clock does.
        await writer.rotate_at(HOUR + 1800 * 1_000_000_000)
        await writer.append(_record(3, 300))
        writer.close()

        self.assertEqual(len(writer.seals), 2)
        first, second = writer.seals
        self.assertEqual((first.first_delivery_index, first.last_delivery_index), (1, 2))
        self.assertEqual((second.first_delivery_index, second.last_delivery_index), (3, 3))
        self.assertEqual(first.seal_reason, "boundary")
        self.assertEqual(second.seal_reason, "shutdown")

    async def test_close_seals_and_is_idempotent(self):
        writer = self._writer()
        writer.start()
        await writer.append(_record(1, 100))
        first = writer.close()
        self.assertIsNotNone(first)
        self.assertIs(writer.close(), first)
        self.assertTrue(writer.segment.data_path.exists())
        self.assertFalse(writer.segment.open_path.exists())

    async def test_close_drains_what_is_still_queued(self):
        """Shutdown must not discard accepted records. They were counted by the
        producer's `delivery_index` before ever reaching the queue."""
        writer = self._writer(queue_capacity=1000)
        # Deliberately not started: nothing drains, so everything sits queued.
        for index in range(1, 21):
            await writer.append(_record(index, index * 100))
        self.assertEqual(writer.pending, 20)
        writer.close()
        self.assertEqual(len(writer.segment.data_path.read_text().splitlines()), 20)

    async def test_the_seal_carries_the_queue_metrics(self):
        writer = self._writer(queue_capacity=8)
        writer.start()
        for index in range(1, 31):
            await writer.append(_record(index, index * 100))
        writer.close()
        stored = read_seal(writer.segment.seal_path)
        self.assertEqual(stored["queue_capacity"], 8)
        self.assertIn("queue_high_water", stored)
        self.assertIn("queue_full_events", stored)

    async def test_a_quiet_lane_still_gets_its_records_fsynced(self):
        """One record then silence used to sit in page cache indefinitely: the
        interval check only ran on a later append, and there was no later
        append."""
        writer = self._writer()
        writer.start()
        await writer.append(_record(1, 100))
        for _ in range(50):
            await asyncio.sleep(0.01)
            if writer.segment.line_count == 1:
                break
        self.assertEqual(writer.segment.line_count, 1)
        self.assertEqual(writer.segment._unsynced, 0, "drained to empty forces an fsync")

    async def test_appending_after_close_is_refused(self):
        writer = self._writer()
        writer.close()
        with self.assertRaises(RuntimeError):
            await writer.append(_record(1, 100))


if __name__ == "__main__":
    unittest.main()
