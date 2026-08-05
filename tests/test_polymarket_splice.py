from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from splices.common.envelope import VENUE_POLYMARKET
from splices.common.segment import read_seal, seal_path_for
from splices.common.spool import Spool, spool_files
from splices.polymarket.splice import BackoffPolicy, PolymarketSplice
from targeter.targets import Target, write_targets


class _FakeSocket:
    """Replays a scripted message sequence, then raises to end the connection."""

    def __init__(self, messages, *, fail_with=None):
        self._messages = list(messages)
        self._fail_with = fail_with
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if self._messages:
            return self._messages.pop(0)
        if self._fail_with is not None:
            raise self._fail_with
        await asyncio.sleep(3600)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


def _factory(*sockets):
    queue = list(sockets)

    def build(_url):
        return queue.pop(0) if queue else _FakeSocket([], fail_with=RuntimeError("exhausted"))

    return build


def _records(root: Path) -> list[dict]:
    rows = []
    for path in spool_files(root, VENUE_POLYMARKET):
        rows.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    return rows


class SpliceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.targets = self.root / "targets.json"
        write_targets(self.targets, venue="polymarket",
                      targets=[Target("asset-1"), Target("asset-2")])
        self.addCleanup(self._directory.cleanup)

    def _splice(self, *sockets, **kwargs):
        spool = Spool(self.root / "spool", VENUE_POLYMARKET)
        return PolymarketSplice(
            spool, self.targets,
            connect_factory=_factory(*sockets),
            backoff=BackoffPolicy(initial_seconds=0.0, maximum_seconds=0.0, jitter=0.0),
            heartbeat_seconds=3600.0,
            loop_wake_seconds=0.01,
            **kwargs,
        )

    def test_every_frame_becomes_one_record_including_unknown_shapes(self) -> None:
        """A splice does not filter. A message it cannot interpret is still the
        only copy that will ever exist."""
        socket = _FakeSocket(
            ['{"event_type":"book"}', "PONG", '{"event_type":"never_seen_before"}'],
            fail_with=ConnectionError("closed"),
        )
        splice = self._splice(socket)
        asyncio.run(splice.run(max_connections=1))

        frames = [r for r in _records(self.root / "spool") if r["kind"] == "venue_frame"]
        self.assertEqual([r["raw_payload"] for r in frames],
                         ['{"event_type":"book"}', "PONG", '{"event_type":"never_seen_before"}'])

    def test_subscription_is_sent_with_the_target_assets(self) -> None:
        socket = _FakeSocket([], fail_with=ConnectionError("closed"))
        asyncio.run(self._splice(socket).run(max_connections=1))
        self.assertEqual(json.loads(socket.sent[0]),
                         {"assets_ids": ["asset-1", "asset-2"], "type": "market"})

    def test_lifecycle_records_bracket_the_frames(self) -> None:
        socket = _FakeSocket(['{"a":1}'], fail_with=ConnectionError("boom"))
        asyncio.run(self._splice(socket).run(max_connections=1))
        events = [json.loads(r["raw_payload"])["event"]
                  for r in _records(self.root / "spool") if r["kind"] != "venue_frame"]
        self.assertEqual(events[0], "connection_opened")
        self.assertIn("subscription_sent", events)
        self.assertIn("connection_failed", events)
        self.assertEqual(events[-1], "connection_closed")

    def test_failure_is_recorded_as_a_fault_not_swallowed(self) -> None:
        socket = _FakeSocket([], fail_with=ConnectionError("network went away"))
        asyncio.run(self._splice(socket).run(max_connections=1))
        faults = [r for r in _records(self.root / "spool") if r["kind"] == "fault"]
        self.assertEqual(len(faults), 1)
        detail = json.loads(faults[0]["raw_payload"])
        self.assertEqual(detail["error_type"], "ConnectionError")
        self.assertIn("network went away", detail["error"])

    def test_reconnect_opens_a_new_epoch_and_restarts_local_counter(self) -> None:
        """Carrying an epoch across a reconnect would let a delta from the new
        socket fold onto a book assembled from the old one."""
        first = _FakeSocket(['{"a":1}'], fail_with=ConnectionError("drop"))
        second = _FakeSocket(['{"b":2}'], fail_with=ConnectionError("drop"))
        asyncio.run(self._splice(first, second).run(max_connections=2))

        rows = _records(self.root / "spool")
        epochs = list(dict.fromkeys(r["connection_epoch"] for r in rows))
        self.assertEqual(len(epochs), 2)
        for epoch in epochs:
            counters = [r["local_counter"] for r in rows if r["connection_epoch"] == epoch]
            self.assertEqual(counters, list(range(1, len(counters) + 1)))

    def test_a_reconnect_does_not_roll_the_segment(self) -> None:
        """The invariant phase 2 exists to establish, at the splice level.

        The old layout was one file per connection, so a reconnect rolled a
        file. A segment is a wall-clock window now, so both epochs land in one —
        and the marker between their independent `local_counter` runs is the
        `connection_epoch` on every record, which both readers already key on.
        """
        first = _FakeSocket(['{"a":1}'], fail_with=ConnectionError("drop"))
        second = _FakeSocket(['{"b":2}'], fail_with=ConnectionError("drop"))
        asyncio.run(self._splice(first, second).run(max_connections=2))

        segments = spool_files(self.root / "spool", VENUE_POLYMARKET)
        self.assertEqual(len(segments), 1, "two connections, one segment")

        rows = _records(self.root / "spool")
        epochs = list(dict.fromkeys(r["connection_epoch"] for r in rows))
        self.assertEqual(len(epochs), 2)
        seal = read_seal(seal_path_for(segments[0]))
        self.assertEqual(seal["epochs"], epochs, "the seal names every epoch it holds")
        self.assertTrue(seal["delivery_index_dense"], "dense across the epoch change")

    def test_delivery_index_is_dense_and_global_across_epochs(self) -> None:
        """Our sequence is the authoritative one; it must not restart when a
        venue connection does."""
        first = _FakeSocket(['{"a":1}'], fail_with=ConnectionError("drop"))
        second = _FakeSocket(['{"b":2}'], fail_with=ConnectionError("drop"))
        asyncio.run(self._splice(first, second).run(max_connections=2))

        indices = [r["delivery_index"] for r in _records(self.root / "spool")]
        self.assertEqual(indices, list(range(1, len(indices) + 1)))

    def test_restart_continues_the_delivery_index(self) -> None:
        first = _FakeSocket(['{"a":1}'], fail_with=ConnectionError("drop"))
        asyncio.run(self._splice(first).run(max_connections=1))
        written = len(_records(self.root / "spool"))

        second = _FakeSocket(['{"b":2}'], fail_with=ConnectionError("drop"))
        asyncio.run(self._splice(second).run(max_connections=1))

        indices = [r["delivery_index"] for r in _records(self.root / "spool")]
        self.assertEqual(indices, list(range(1, len(indices) + 1)))
        self.assertGreater(len(indices), written)

    def test_frame_records_carry_an_unsequenced_cursor(self) -> None:
        """Polymarket publishes no sequence number, so the only cursor is ours —
        and it is labelled as such rather than silently pooled with venues that
        do number their messages."""
        socket = _FakeSocket(['{"a":1}'], fail_with=ConnectionError("drop"))
        asyncio.run(self._splice(socket).run(max_connections=1))
        frame = next(r for r in _records(self.root / "spool") if r["kind"] == "venue_frame")
        self.assertEqual(frame["source_cursor"]["type"], "unsequenced")
        self.assertEqual(frame["source_cursor"]["counter"], frame["local_counter"])

    def test_control_records_carry_a_null_cursor(self) -> None:
        socket = _FakeSocket([], fail_with=ConnectionError("drop"))
        asyncio.run(self._splice(socket).run(max_connections=1))
        control = next(r for r in _records(self.root / "spool") if r["kind"] == "control")
        self.assertIsNone(control["source_cursor"])

    def test_target_change_is_recorded_before_the_resubscribe(self) -> None:
        """A market with no data must be distinguishable from a market that was
        never subscribed."""
        socket = _FakeSocket([], fail_with=None)

        async def scenario() -> None:
            splice = self._splice(socket, _FakeSocket([], fail_with=ConnectionError("drop")),
                                  target_poll_seconds=0.0)
            task = asyncio.ensure_future(splice.run(max_connections=2))
            await asyncio.sleep(0.05)
            write_targets(self.targets, venue="polymarket",
                          targets=[Target("asset-1"), Target("asset-3")])
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(scenario())
        changes = [json.loads(r["raw_payload"]) for r in _records(self.root / "spool")
                   if r["kind"] == "control"
                   and json.loads(r["raw_payload"])["event"] == "subscription_changed"]
        self.assertTrue(changes)
        self.assertEqual(changes[0]["added"], ["asset-3"])
        self.assertEqual(changes[0]["removed"], ["asset-2"])

    def test_metadata_change_is_recorded_without_reconnecting(self) -> None:
        socket = _FakeSocket([], fail_with=None)

        async def scenario() -> int:
            splice = self._splice(socket, target_poll_seconds=0.0)
            task = asyncio.ensure_future(splice.run(max_connections=2))
            await asyncio.sleep(0.05)
            write_targets(
                self.targets,
                venue="polymarket",
                targets=[
                    Target(
                        "asset-1",
                        resolution={"catalogue_record": {"description": "new rules"}},
                    ),
                    Target("asset-2"),
                ],
            )
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return splice.connections

        connections = asyncio.run(scenario())
        changes = [
            json.loads(record["raw_payload"])
            for record in _records(self.root / "spool")
            if record["kind"] == "control"
            and json.loads(record["raw_payload"])["event"] == "target_metadata_changed"
        ]
        self.assertTrue(changes)
        self.assertEqual(connections, 1)

    def test_unreadable_targets_do_not_take_down_a_live_connection(self) -> None:
        socket = _FakeSocket([], fail_with=None)

        async def scenario() -> None:
            splice = self._splice(socket, target_poll_seconds=0.0)
            task = asyncio.ensure_future(splice.run(max_connections=1))
            await asyncio.sleep(0.05)
            self.targets.write_text("{ not json")
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(scenario())
        rows = _records(self.root / "spool")
        events = [json.loads(r["raw_payload"])["event"] for r in rows if r["kind"] == "fault"]
        self.assertIn("targets_unreadable", events)


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
