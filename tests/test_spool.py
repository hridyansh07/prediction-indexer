"""The spool facade: lane partitioning, epochs, resume, and torn-tail recovery."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from splices.common.envelope import (
    KIND_VENUE_FRAME,
    STREAM_PUBLIC_BOOK,
    VENUE_POLYMARKET,
    build_envelope,
    unsequenced_cursor,
)
from splices.common.segment import (
    OPEN_SUFFIX,
    read_seal,
    seal_path_for,
    segment_filename,
)
from splices.common.spool import Spool, SpoolError, resume_state, spool_files

LANE = "polymarket"
HOUR = 3_600_000_000_000
#: Inside the HOUR window. The writer's clock and the records' `visible_ns` must
#: agree: a record now opens the window its own receive time belongs to, so a
#: fixture whose clock says 1970 while its records say 2023 would rotate on every
#: append and seal an empty segment each time.
VISIBLE = HOUR + 1_000_000


def _envelope(**overrides):
    base = dict(
        delivery_index=1,
        record_id="pm-abc-1",
        visible_ns=VISIBLE,
        monotonic_ns=123_456_789,
        venue=VENUE_POLYMARKET,
        stream=STREAM_PUBLIC_BOOK,
        connection_epoch="abc",
        local_counter=1,
        kind=KIND_VENUE_FRAME,
        raw_payload='{"event_type":"book"}',
        source_cursor=unsequenced_cursor(1),
    )
    base.update(overrides)
    return build_envelope(**base)


class SpoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.addCleanup(self._directory.cleanup)

    def _run(self, work):
        """Drives one async body with a started spool, sealing on the way out."""

        async def drive():
            spool = Spool(self.root, LANE, segment_seconds=1800, clock=lambda: HOUR)
            spool.start()
            try:
                return await work(spool)
            finally:
                spool.close()

        return asyncio.run(drive())

    # -- layout ------------------------------------------------------------

    def test_segments_are_partitioned_by_lane_not_venue(self):
        """Polymarket runs four lanes and every record from all four says
        `venue: polymarket`, so the partition and the envelope field answer
        different questions."""
        async def work(spool):
            return spool.path

        path = self._run(work)
        self.assertEqual(path.parent.parent.name, f"lane={LANE}")
        self.assertTrue(path.parent.name.startswith("date="))

    def test_an_unsealed_segment_is_invisible_to_readers(self):
        """`spool_files` and the Rust ingester both skip it: an `.open` has no
        committed length and no digest, so its bytes are not yet evidence."""
        async def work(spool):
            await spool.append(_envelope())
            self.assertTrue(spool.path.name.endswith(OPEN_SUFFIX))
            self.assertEqual(spool_files(self.root, LANE), [])
            self.assertEqual(len(spool_files(self.root, LANE, include_open=True)), 1)
        self._run(work)
        # After close it is sealed and therefore visible.
        self.assertEqual(len(spool_files(self.root, LANE)), 1)

    def test_filenames_sort_into_window_order(self):
        earlier = segment_filename(HOUR, 0, "ffff")
        later = segment_filename(HOUR + 1800 * 1_000_000_000, 0, "0000")
        self.assertLess(earlier, later, "a random segment id must not decide order")

    # -- epochs ------------------------------------------------------------

    def test_reopening_an_epoch_is_refused(self):
        """A segment now holds several epochs by design, and `connection_epoch`
        is the marker between their independent counter runs. Minting one twice
        would produce two indistinguishable runs inside one segment — exactly the
        ambiguity the old per-connection file boundary existed to prevent."""
        async def work(spool):
            spool.begin_epoch("aaaa")
            with self.assertRaises(SpoolError):
                spool.begin_epoch("aaaa")
        self._run(work)

    def test_one_segment_holds_several_epochs(self):
        """The invariant this whole phase exists to establish. The old layout
        proved the opposite: one file per connection."""
        async def work(spool):
            spool.begin_epoch("aaaa")
            await spool.append(_envelope(delivery_index=1, connection_epoch="aaaa", local_counter=1))
            spool.begin_epoch("bbbb")
            await spool.append(_envelope(delivery_index=2, connection_epoch="bbbb", local_counter=1))
        self._run(work)

        segments = spool_files(self.root, LANE)
        self.assertEqual(len(segments), 1, "a reconnect must not roll a segment")
        epochs = [json.loads(line)["connection_epoch"]
                  for line in segments[0].read_text().splitlines()]
        self.assertEqual(epochs, ["aaaa", "bbbb"])
        self.assertEqual(read_seal(seal_path_for(segments[0]))["epochs"], ["aaaa", "bbbb"])

    # -- resume ------------------------------------------------------------

    def test_an_empty_tree_resumes_at_one(self):
        state = resume_state(self.root, LANE)
        self.assertEqual(state.next_delivery_index, 1)
        self.assertEqual(state.repaired_bytes, 0)
        self.assertIsNone(state.last_visible_ns)
        self.assertEqual(state.source, "empty")

    def test_resume_reads_the_seal_rather_than_every_file(self):
        """The old implementation opened and back-scanned every file the lane had
        ever written, which at 48 segments a day stops being viable in a week."""
        async def work(spool):
            for index in (1, 2, 3):
                await spool.append(_envelope(delivery_index=index, local_counter=index,
                                             visible_ns=VISIBLE + index))
        self._run(work)

        state = resume_state(self.root, LANE)
        self.assertEqual(state.next_delivery_index, 4)
        self.assertEqual(state.source, "seal")
        self.assertEqual(state.last_visible_ns, VISIBLE + 3)

    def test_delivery_index_continues_across_a_restart(self):
        """Ours is the authoritative sequence; it must not restart when a
        process does."""
        async def first(spool):
            await spool.append(_envelope(delivery_index=1, local_counter=1))
            await spool.append(_envelope(delivery_index=2, local_counter=2))
        self._run(first)
        self.assertEqual(resume_state(self.root, LANE).next_delivery_index, 3)

        async def second(spool):
            await spool.append(_envelope(delivery_index=3, local_counter=1))
        self._run(second)
        self.assertEqual(resume_state(self.root, LANE).next_delivery_index, 4)

    def test_a_torn_tail_is_repaired_and_the_orphan_is_recovery_sealed(self):
        """A record is durable only once its newline is on disk. A crash leaves
        an `.open` behind; resume repairs and seals it rather than leaving a file
        no reader will ever accept."""
        # Built directly rather than by tearing a live writer's file: a crash
        # leaves behind exactly what was already fsynced, and racing the writer
        # thread to append after it would test the race, not the recovery.
        directory = self.root / f"lane={LANE}" / "date=1970-01-01"
        directory.mkdir(parents=True)
        orphan = directory / f"{segment_filename(HOUR, 0, 'deadbeef')}{OPEN_SUFFIX}"
        complete = json.dumps(_envelope(delivery_index=1, local_counter=1)) + "\n"
        orphan.write_text(complete + '{"delivery_index":2', encoding="utf-8")

        state = resume_state(self.root, LANE)
        self.assertEqual(state.next_delivery_index, 2)
        self.assertGreater(state.repaired_bytes, 0)

        segments = spool_files(self.root, LANE)
        self.assertEqual(len(segments), 1)
        seal = read_seal(seal_path_for(segments[0]))
        self.assertEqual(seal["seal_reason"], "recovery")
        self.assertEqual(seal["line_count"], 1)
        self.assertGreater(seal["repaired_bytes"], 0)
        self.assertEqual(segments[0].stat().st_size, seal["byte_length"])

    def test_two_segments_in_one_window_still_sort_into_capture_order(self):
        """The bug a live crash-recovery run exposed.

        A restart inside a window opens a second segment for it, because a
        process only ever appends to a segment it opened. With a random id alone
        deciding order within the window, the two sorted arbitrarily — the real
        run produced `delivery_index` reading 1..3, 300..549, 4..299, 550..600 in
        file order, which breaks the one ordering guarantee this layout makes:
        within a lane, file order *is* receive order. The zero-padded segment
        index in the filename is what restores it.
        """
        async def first(spool):
            await spool.append(_envelope(delivery_index=1, local_counter=1))
        self._run(first)

        async def second(spool):
            await spool.append(_envelope(delivery_index=2, local_counter=1))
        self._run(second)

        segments = spool_files(self.root, LANE)
        self.assertEqual(len(segments), 2, "same window, two processes, two segments")
        indices = [json.loads(line)["delivery_index"]
                   for path in segments
                   for line in path.read_text().splitlines()]
        self.assertEqual(indices, [1, 2], "file order must be receive order")
        self.assertEqual(
            [read_seal(seal_path_for(path))["segment_index"] for path in segments],
            [0, 1],
            "the index is read off disk, so a new process sees the dead one's work",
        )

    def test_resume_is_idempotent(self):
        async def work(spool):
            await spool.append(_envelope(delivery_index=1, local_counter=1))
        self._run(work)
        first = resume_state(self.root, LANE)
        self.assertEqual(resume_state(self.root, LANE), first)




if __name__ == "__main__":
    unittest.main()
