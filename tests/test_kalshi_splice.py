from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric import rsa

from splices.common.base import BackoffPolicy
from splices.common.envelope import VENUE_KALSHI
from splices.common.spool import Spool, spool_files
from splices.kalshi.auth import KalshiCredentials, KalshiCredentialsError
from splices.kalshi.splice import KalshiSplice
from targeter.targets import Target, write_targets


class _FakeSocket:
    def __init__(self, messages, *, fail_with=None):
        self._messages = list(messages)
        self._fail_with = fail_with
        self.sent = []
        self.recv_calls = 0

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        self.recv_calls += 1
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
    for path in spool_files(root, VENUE_KALSHI):
        rows.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    return rows


def _orderbook(sid: int, seq: int, ticker: str = "KXBTCD-26JUL2904-T72299.99") -> str:
    return json.dumps(
        {
            "type": "orderbook_delta",
            "sid": sid,
            "seq": seq,
            "msg": {"market_ticker": ticker, "price_dollars": "0.51",
                    "delta_fp": "3.00", "side": "yes"},
        }
    )


class KalshiSpliceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.targets = self.root / "targets.json"
        write_targets(
            self.targets,
            venue="kalshi",
            targets=[Target("KXBTCD-26JUL2904-T72299.99"), Target("KXBTCD-26JUL2904-T71299.99")],
        )
        self.addCleanup(self._directory.cleanup)
        self.credentials = KalshiCredentials(
            key_id="test-key",
            private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048),
        )

    def _splice(self, *sockets, **kwargs):
        spool = Spool(self.root / "spool", VENUE_KALSHI)
        return KalshiSplice(
            spool,
            self.targets,
            connect_factory=_factory(*sockets),
            credentials=self.credentials,
            backoff=BackoffPolicy(initial_seconds=0.0, maximum_seconds=0.0, jitter=0.0),
            loop_wake_seconds=0.01,
            **kwargs,
        )

    def test_one_subscription_covers_every_target(self) -> None:
        """`seq` is per subscription, so splitting the ladder across several
        subscriptions would give N independent sequences and lose the only
        property that lets the ingester prove a dropped message."""
        socket = _FakeSocket([], fail_with=ConnectionError("closed"))
        asyncio.run(self._splice(socket).run(max_connections=1))

        self.assertEqual(len(socket.sent), 1)
        command = json.loads(socket.sent[0])
        self.assertEqual(command["cmd"], "subscribe")
        self.assertEqual(
            command["params"]["market_tickers"],
            ["KXBTCD-26JUL2904-T72299.99", "KXBTCD-26JUL2904-T71299.99"],
        )
        self.assertIn("orderbook_delta", command["params"]["channels"])
        self.assertTrue(command["params"]["send_initial_snapshot"])

    def test_seq_becomes_a_one_position_range_declaring_its_predecessor(self) -> None:
        """A range that carries its own predecessor is checkable without knowing
        the instrument, which matters because one connection carries the ladder."""
        socket = _FakeSocket(
            [_orderbook(7, 1), _orderbook(7, 2), _orderbook(7, 3)],
            fail_with=ConnectionError("closed"),
        )
        asyncio.run(self._splice(socket).run(max_connections=1))

        cursors = [r["source_cursor"] for r in _records(self.root / "spool")
                   if r["kind"] == "venue_frame"]
        self.assertEqual(
            cursors,
            [
                {"type": "update_range", "first": 1, "last": 1, "previous_last": 0},
                {"type": "update_range", "first": 2, "last": 2, "previous_last": 1},
                {"type": "update_range", "first": 3, "last": 3, "previous_last": 2},
            ],
        )

    def test_a_venue_gap_survives_into_the_cursor(self) -> None:
        """Regression: `previous_last` is `seq - 1`, the venue's own claim — never
        the last sequence this splice happened to see.

        An earlier version used the last observed value, which makes every message
        continuous with its predecessor by construction. A deliberate 7-8 hole then
        reached the ingester labelled `continuous` and the loss became undetectable.
        A cursor records what the venue asserted, never what the splice inferred.
        """
        socket = _FakeSocket([_orderbook(7, 6), _orderbook(7, 9)],
                             fail_with=ConnectionError("closed"))
        asyncio.run(self._splice(socket).run(max_connections=1))

        cursors = [r["source_cursor"] for r in _records(self.root / "spool")
                   if r["kind"] == "venue_frame"]
        self.assertEqual(
            cursors[1],
            {"type": "update_range", "first": 9, "last": 9, "previous_last": 8},
            "previous_last must be seq-1 so the ingester can see 7 and 8 are missing",
        )

    def test_the_cursor_does_not_depend_on_what_the_splice_saw_before(self) -> None:
        """Two subscriptions interleaved on one connection. Each message's cursor
        is a function of its own `seq` alone, so no cross-subscription bookkeeping
        exists to get wrong."""
        socket = _FakeSocket([_orderbook(1, 5), _orderbook(2, 100), _orderbook(1, 6)],
                             fail_with=ConnectionError("closed"))
        asyncio.run(self._splice(socket).run(max_connections=1))

        cursors = [r["source_cursor"] for r in _records(self.root / "spool")
                   if r["kind"] == "venue_frame"]
        self.assertEqual([c["previous_last"] for c in cursors], [4, 99, 5])

    def test_a_message_without_a_seq_falls_back_rather_than_raising(self) -> None:
        """A schema surprise should cost continuity metadata, never a frame — and
        every other venue here has contradicted its own documentation."""
        socket = _FakeSocket(
            ['{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":7}}',
             '{"type":"new_shape","msg":{"field":"unknown"}}',
             "not json at all"],
            fail_with=ConnectionError("closed"),
        )
        asyncio.run(self._splice(socket).run(max_connections=1))

        frames = [r for r in _records(self.root / "spool") if r["kind"] == "venue_frame"]
        self.assertEqual(len(frames), 3, "no frame may be dropped for being unrecognised")
        self.assertTrue(all(f["source_cursor"]["type"] == "unsequenced" for f in frames))

    def test_a_protocol_error_is_recorded_then_fails_the_connection(self) -> None:
        """A rejected subscription must not leave a live-looking empty socket.

        `BaseSplice` records before calling the venue hook, so terminating here
        preserves Kalshi's explanation on the tape while making the failed
        subscription visible to the reconnect loop and operations.
        """
        error_frame = json.dumps(
            {
                "type": "error",
                "id": 1,
                "msg": {"code": 6, "msg": "market_tickers are invalid"},
            }
        )
        socket = _FakeSocket(
            [error_frame],
            fail_with=AssertionError("splice read again after Kalshi protocol error"),
        )
        asyncio.run(self._splice(socket).run(max_connections=1))

        rows = _records(self.root / "spool")
        frames = [row for row in rows if row["kind"] == "venue_frame"]
        self.assertEqual([row["raw_payload"] for row in frames], [error_frame])
        self.assertEqual(socket.recv_calls, 1)

        faults = [
            json.loads(row["raw_payload"])
            for row in rows
            if row["kind"] == "fault"
            and json.loads(row["raw_payload"]).get("event") == "connection_failed"
        ]
        self.assertEqual(len(faults), 1)
        self.assertEqual(faults[0]["error_type"], "KalshiProtocolError")
        self.assertIn("code=6", faults[0]["error"])

    def test_reconnect_restarts_the_sequence_without_faulting(self) -> None:
        """A new subscription restarts `seq`. Because the cursor is derived from
        the message alone, there is no carried-over state to reset — and the
        ingester files the new epoch's first frame as a bootstrap rather than a
        backwards jump."""
        first = _FakeSocket([_orderbook(1, 500)], fail_with=ConnectionError("drop"))
        second = _FakeSocket([_orderbook(1, 1)], fail_with=ConnectionError("drop"))
        asyncio.run(self._splice(first, second).run(max_connections=2))

        rows = [r for r in _records(self.root / "spool") if r["kind"] == "venue_frame"]
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0]["connection_epoch"], rows[1]["connection_epoch"])
        self.assertEqual(rows[1]["source_cursor"], {"type": "update_range", "first": 1,
                                                    "last": 1, "previous_last": 0})

    def test_the_connection_record_says_it_is_unverified(self) -> None:
        """Written from the spec and never run against Kalshi. That fact belongs
        on the tape, not in someone's memory."""
        socket = _FakeSocket([], fail_with=ConnectionError("closed"))
        asyncio.run(self._splice(socket).run(max_connections=1))

        opened = next(json.loads(r["raw_payload"]) for r in _records(self.root / "spool")
                      if r["kind"] == "control"
                      and json.loads(r["raw_payload"])["event"] == "connection_opened")
        self.assertFalse(opened["verified_against_live_socket"])
        self.assertTrue(opened["delivers_deltas"])
        self.assertEqual(opened["key_id"], "test-key")

    def test_missing_credentials_do_not_break_construction(self) -> None:
        """`splices/run.py` builds by name, so an unconfigured Kalshi must not
        stop a configured venue from starting."""
        spool = Spool(self.root / "spool2", VENUE_KALSHI)
        splice = KalshiSplice(spool, self.targets, dotenv_path=self.root / "absent.env")
        with self.assertRaises(KalshiCredentialsError):
            splice.credentials()


if __name__ == "__main__":
    unittest.main()
