"""Executable proofs for the Phase-2 sealed-capture failures.

These tests assert the safe behaviour, not the current implementation. Each was
written as an ``expectedFailure`` proof of an open defect; all are now fixed,
the markers are gone, and they stand as the permanent regression tests for:

* a write error silently sealing past a record the queue had already accepted;
* a delayed rotation timer filing a post-boundary record in the previous window,
  so the seal asserted a range not containing its own records;
* a restart whose clock stepped backwards recording a clean start, because the
  first record of a segment had nothing to be compared against;
* a renamed-but-unsealed segment being treated as evidence, when the sidecar and
  not the suffix is the commit marker;
* that same segment staying orphaned forever instead of being recovery-sealed;
* recovery renaming a complete orphan before fsyncing it, publishing a digest
  for bytes a second crash could still lose.
* a decreasing visible clock being recorded in the seal but never raising the
  online critical alert required by the certification rule.
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from splices.common.base import BaseSplice
from splices.common.clock import CaptureClock
from splices.common.envelope import (
    KIND_VENUE_FRAME,
    STREAM_PUBLIC_BOOK,
    build_envelope,
    encode_envelope,
    unsequenced_cursor,
)
from splices.common.segment import (
    OPEN_SUFFIX,
    Record,
    read_seal,
    seal_orphan,
    seal_path_for,
    segment_filename,
)
from splices.common.spool import Spool, resume_state, spool_files
from splices.common.writer import LaneWriter

LANE = "polymarket"
HOUR_NS = 3_600_000_000_000
SEGMENT_NS = 1_800_000_000_000


def _envelope(delivery_index: int, visible_ns: int, *, epoch: str = "epoch") -> dict:
    return build_envelope(
        delivery_index=delivery_index,
        record_id=f"pm-{epoch}-{delivery_index}",
        visible_ns=visible_ns,
        monotonic_ns=delivery_index,
        venue="polymarket",
        stream=STREAM_PUBLIC_BOOK,
        connection_epoch=epoch,
        local_counter=delivery_index,
        kind=KIND_VENUE_FRAME,
        raw_payload="{}",
        source_cursor=unsequenced_cursor(delivery_index),
    )


def _record(delivery_index: int, visible_ns: int, *, epoch: str = "epoch") -> Record:
    envelope = _envelope(delivery_index, visible_ns, epoch=epoch)
    return Record(
        line=encode_envelope(envelope),
        visible_ns=visible_ns,
        delivery_index=delivery_index,
        epoch=epoch,
    )


def _write_renamed_unsealed_segment(root: Path) -> Path:
    directory = root / f"lane={LANE}" / "date=1970-01-01"
    directory.mkdir(parents=True, exist_ok=True)
    data_path = directory / f"{segment_filename(HOUR_NS, 0, 'crash')}.ndjson"
    data_path.write_bytes(encode_envelope(_envelope(1, HOUR_NS + 1)))
    return data_path


class _ProbeSplice(BaseSplice):
    """Only the shared emission path is needed for the restart-boundary proof."""

    venue = "polymarket"
    record_prefix = "pm"
    frame_stream = STREAM_PUBLIC_BOOK
    requires_targets = False


class WriterFailureProofs(unittest.IsolatedAsyncioTestCase):
    async def test_a_writer_error_cannot_seal_past_an_accepted_record(self) -> None:
        """The queue accepted index 1, so either it lands or closing must fail."""

        with tempfile.TemporaryDirectory() as temporary:
            writer = LaneWriter(
                Path(temporary),
                LANE,
                start_ns=HOUR_NS,
                segment_seconds=1800,
                queue_capacity=2,
                clock=lambda: HOUR_NS,
            )
            original_write = writer.segment.write_batch

            def fail_write(_records):
                raise OSError("simulated transient disk failure")

            writer.segment.write_batch = fail_write
            writer.start()
            await writer.append(_record(1, HOUR_NS + 1))

            for _ in range(100):
                await asyncio.sleep(0.001)
                if writer._drain is not None and writer._drain.done():
                    break
            self.assertIsNotNone(writer._drain)
            self.assertTrue(writer._drain.done(), "the injected write error was not observed")
            self.assertIsInstance(writer._drain.exception(), OSError)

            # Model a transient storage fault clearing. A correct implementation
            # may retry the retained batch, or latch the fatal error and refuse to
            # seal. It must not publish a successful seal which omits index 1.
            writer.segment.write_batch = original_write
            try:
                writer.close()
            except Exception:
                return

            stored = [
                json.loads(line)["delivery_index"]
                for line in writer.segment.data_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(stored, [1], "an accepted record disappeared before the seal")

    async def test_a_record_is_sealed_in_its_visible_time_window(self) -> None:
        """A delayed timer must not put a post-boundary record in the old window."""

        visible_ns = HOUR_NS + SEGMENT_NS + 1
        with tempfile.TemporaryDirectory() as temporary:
            writer = LaneWriter(
                Path(temporary),
                LANE,
                start_ns=HOUR_NS,
                segment_seconds=1800,
                clock=lambda: HOUR_NS,
            )
            writer.start()
            await writer.append(_record(1, visible_ns))
            writer.close()

            nonempty = [seal for seal in writer.seals if seal.line_count]
            self.assertEqual(len(nonempty), 1)
            seal = nonempty[0]
            self.assertLessEqual(seal.window_start_ns, visible_ns)
            self.assertLess(visible_ns, seal.window_end_ns)


class RestartBoundaryProofs(unittest.IsolatedAsyncioTestCase):
    async def _emit_once(self, root: Path, visible_ns: int, epoch: str) -> None:
        spool = Spool(
            root,
            LANE,
            segment_seconds=1800,
            clock=lambda: HOUR_NS,
        )
        clock = CaptureClock(
            LANE,
            wall_ns=lambda: visible_ns,
            monotonic_ns=lambda: visible_ns,
            platform_name="Darwin",
            fallback_scope_id=epoch,
        )
        splice = _ProbeSplice(spool, None, clock=clock)
        spool.start()
        spool.begin_epoch(epoch)
        splice._epoch = epoch
        try:
            await splice._emit(
                stream=STREAM_PUBLIC_BOOK,
                kind=KIND_VENUE_FRAME,
                payload="{}",
                cursor=unsequenced_cursor(1),
            )
        finally:
            spool.close()

    async def test_a_restart_visible_clock_regression_marks_the_new_segment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            await self._emit_once(root, HOUR_NS + 500, "first")
            resumed = resume_state(root, LANE)
            self.assertEqual(resumed.last_visible_ns, HOUR_NS + 500)

            await self._emit_once(root, HOUR_NS + 400, "second")
            segments = spool_files(root, LANE)
            self.assertEqual(len(segments), 2)
            second_seal = read_seal(seal_path_for(segments[-1]))
            self.assertEqual(second_seal["first_visible_ns"], HOUR_NS + 400)
            self.assertEqual(second_seal["ordering_status"], "visible_clock_regression")
            self.assertFalse(second_seal["visible_non_decreasing"])

    async def test_a_visible_clock_regression_raises_an_online_alert(self) -> None:
        readings = iter((HOUR_NS + 500, HOUR_NS + 400))
        alerts = []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spool = Spool(root, LANE, segment_seconds=1800, clock=lambda: HOUR_NS)
            clock = CaptureClock(
                LANE,
                wall_ns=lambda: next(readings),
                monotonic_ns=lambda: HOUR_NS,
                platform_name="Darwin",
                fallback_scope_id="alert-scope",
            )
            splice = _ProbeSplice(
                spool,
                None,
                clock=clock,
                clock_regression_alert=alerts.append,
            )
            spool.start()
            spool.begin_epoch("alert-epoch")
            splice._epoch = "alert-epoch"
            await splice._emit(
                stream=STREAM_PUBLIC_BOOK,
                kind=KIND_VENUE_FRAME,
                payload="{}",
                cursor=unsequenced_cursor(1),
            )
            await splice._emit(
                stream=STREAM_PUBLIC_BOOK,
                kind=KIND_VENUE_FRAME,
                payload="{}",
                cursor=unsequenced_cursor(2),
            )
            self.assertEqual(len(alerts), 1, "the alert must fire before the segment is sealed")
            alert = alerts[0]
            self.assertEqual(alert.lane_id, LANE)
            self.assertEqual(alert.previous_visible_ns, HOUR_NS + 500)
            self.assertEqual(alert.current_visible_ns, HOUR_NS + 400)
            self.assertEqual(alert.previous_delivery_index, 1)
            self.assertEqual(alert.current_delivery_index, 2)
            self.assertEqual(alert.current_scope_id, "alert-scope")
            spool.close()


class RenameBeforeSealRecoveryProofs(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary.name)
        self.addCleanup(self._temporary.cleanup)

    def test_renamed_segment_without_a_seal_is_invisible_to_spool_readers(self) -> None:
        data_path = _write_renamed_unsealed_segment(self.root)
        self.assertNotIn(
            data_path,
            spool_files(self.root, LANE),
            "the sidecar—not the .ndjson suffix—is the commit marker",
        )

    def test_resume_recovery_seals_a_complete_already_renamed_segment(self) -> None:
        data_path = _write_renamed_unsealed_segment(self.root)
        state = resume_state(self.root, LANE)
        sidecar = seal_path_for(data_path)

        self.assertTrue(sidecar.exists(), "the rename-before-seal crash state stayed orphaned")
        seal = read_seal(sidecar)
        self.assertEqual(seal["seal_reason"], "recovery")
        self.assertEqual(seal["data_file"], data_path.name)
        self.assertEqual(state.next_delivery_index, 2)


class OrphanDurabilityProofs(unittest.TestCase):
    def test_recovery_fsyncs_a_complete_orphan_before_renaming_it(self) -> None:
        """A newline-complete orphan still needs a data fsync before its seal."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / f"lane={LANE}" / "date=1970-01-01"
            directory.mkdir(parents=True)
            open_path = directory / f"{segment_filename(HOUR_NS, 0, 'orphan')}{OPEN_SUFFIX}"
            open_path.write_bytes(encode_envelope(_envelope(1, HOUR_NS + 1)))

            events: list[str] = []
            real_fsync = os.fsync
            real_replace = os.replace

            def record_fsync(descriptor: int) -> None:
                mode = os.fstat(descriptor).st_mode
                events.append("fsync:file" if stat.S_ISREG(mode) else "fsync:directory")
                real_fsync(descriptor)

            def record_replace(source: str | os.PathLike, target: str | os.PathLike) -> None:
                target_path = Path(target)
                events.append(
                    "replace:data" if target_path.name.endswith(".ndjson") else "replace:seal"
                )
                real_replace(source, target)

            with mock.patch("splices.common.segment.os.fsync", record_fsync), \
                 mock.patch("splices.common.segment.os.replace", record_replace):
                seal_orphan(open_path, LANE)

            data_rename = events.index("replace:data")
            self.assertIn(
                "fsync:file",
                events[:data_rename],
                f"raw file was renamed before any file fsync: {events}",
            )


if __name__ == "__main__":
    unittest.main()
