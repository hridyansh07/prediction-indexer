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


TICKER_A = "KXBTCD-26JUL2904-T72299.99"
TICKER_B = "KXBTCD-26JUL2904-T71299.99"


def _orderbook(sid: int, seq: int, ticker: str = TICKER_A) -> str:
    return json.dumps(
        {
            "type": "orderbook_delta",
            "sid": sid,
            "seq": seq,
            "msg": {"market_ticker": ticker, "price_dollars": "0.51",
                    "delta_fp": "3.00", "side": "yes"},
        }
    )


def _snapshot(sid: int, seq: int, ticker: str) -> str:
    return json.dumps(
        {
            "type": "orderbook_snapshot",
            "sid": sid,
            "seq": seq,
            "msg": {"market_ticker": ticker, "yes": [[51, 300]], "no": [[48, 120]]},
        }
    )


def _subscribed(sid: int, *, channel: str = "orderbook_delta", command_id: int = 1) -> str:
    return json.dumps(
        {"id": command_id, "type": "subscribed", "msg": {"channel": channel, "sid": sid}}
    )


def _error(command_id: int, code: int, reason: str) -> str:
    return json.dumps(
        {"type": "error", "id": command_id, "msg": {"code": code, "msg": reason}}
    )


class _KalshiSpliceCase(unittest.TestCase):
    """A temp spool, a two-market targets file, and a throwaway signing key."""

    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.targets = self.root / "targets.json"
        write_targets(
            self.targets,
            venue="kalshi",
            targets=[Target(TICKER_A), Target(TICKER_B)],
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


class KalshiSpliceTests(_KalshiSpliceCase):
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


class _Clock:
    """A monotonic clock the test moves by hand."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _ScriptedSocket(_FakeSocket):
    """A fake socket whose frames are released by what the splice has already sent.

    Two things the plain fake cannot express, and the poller needs both. A reply
    has to follow the command it answers, because an error naming a command the
    splice has not issued yet is a different case with a deliberately different
    outcome. And time has to pass while frames arrive, so a snapshot can be ten
    minutes old without the test waiting ten minutes.

    A scripted entry that is an exception is raised instead of returned, which is
    how a connection drops at a chosen point in the exchange.
    """

    def __init__(self, script, clock, *, seconds_per_frame: float = 0.0) -> None:
        super().__init__([])
        self._script = list(script)
        self._clock = clock
        self._seconds_per_frame = float(seconds_per_frame)

    async def recv(self):
        self.recv_calls += 1
        if self._script and len(self.sent) >= self._script[0][0]:
            self._clock.advance(self._seconds_per_frame)
            message = self._script.pop(0)[1]
            if isinstance(message, BaseException):
                raise message
            return message
        # The base loop caps every read at `loop_wake_seconds` and comes back
        # round, so a socket with nothing to deliver yet simply blocks.
        await asyncio.sleep(3600)


class KalshiSnapshotPollerTests(_KalshiSpliceCase):
    """The poller's sweep cadence, staleness accounting and failure branches.

    Two clocks, deliberately. The sweep cadence runs on real time because it is
    the base loop's own `time.monotonic`, and every duration the poller *reasons
    about* — staleness, cooldown — runs on the injected clock. That split is what
    lets a ten-minute threshold be tested in a quarter of a second without the
    test asserting merely that some code ran.
    """

    def _drive(self, splice, *, steps=(), seconds: float = 0.4, connections: int = 1):
        """Runs `connections` epochs while `steps` act against real time."""

        async def drive():
            task = asyncio.ensure_future(
                splice.run(stop_after_seconds=seconds, max_connections=connections)
            )
            for delay, action in steps:
                await asyncio.sleep(delay)
                action()
            return await task

        return asyncio.run(drive())

    def _commands(self, socket) -> list[dict]:
        return [json.loads(message) for message in socket.sent]

    def _control(self, event: str) -> list[dict]:
        found = []
        for row in _records(self.root / "spool"):
            if row["kind"] not in ("control", "fault"):
                continue
            payload = json.loads(row["raw_payload"])
            if payload.get("event") == event:
                found.append(payload)
        return found

    def test_only_a_market_whose_book_has_gone_quiet_is_asked_for(self) -> None:
        """The saving that makes this affordable: most markets are snapshotted by
        the natural cadence anyway, and blind polling pays for those again."""
        clock = _Clock()
        socket = _ScriptedSocket(
            [(1, _subscribed(1)), (1, _snapshot(1, 2, TICKER_A))],
            clock,
            seconds_per_frame=350.0,
        )
        self._drive(self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01))

        commands = self._commands(socket)
        self.assertEqual([command["cmd"] for command in commands],
                         ["subscribe", "update_subscription"])
        self.assertEqual(
            commands[1]["params"],
            {"sids": [1], "market_tickers": [TICKER_B], "action": "get_snapshot"},
            "the market that received a snapshot 350s ago is not stale yet",
        )

    def test_every_stale_market_goes_into_one_command(self) -> None:
        """`market_tickers` is a list, and that is the whole reason the command
        rate limit is a non-issue: one sweep is one command however wide it is."""
        clock = _Clock()
        socket = _ScriptedSocket([(1, _subscribed(1))], clock)
        self._drive(
            self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[(0.05, lambda: clock.advance(601.0))],
        )

        commands = self._commands(socket)
        self.assertEqual([command["cmd"] for command in commands],
                         ["subscribe", "update_subscription"])
        self.assertEqual(commands[1]["params"]["market_tickers"], [TICKER_A, TICKER_B])

    def test_the_request_record_names_the_tickers_the_sid_and_the_command_id(self) -> None:
        """Named for what it enables rather than for the command it emitted: the
        reader's question is which snapshots were solicited, so that a pair of
        them and the deltas between can be reconciled later."""
        clock = _Clock()
        socket = _ScriptedSocket([(1, _subscribed(1))], clock)
        self._drive(
            self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[(0.05, lambda: clock.advance(601.0))],
        )

        self.assertEqual(
            self._control("orderbook_reconciliation_request"),
            [
                {
                    "event": "orderbook_reconciliation_request",
                    "sid": 1,
                    "command_id": 2,
                    "market_tickers": [TICKER_A, TICKER_B],
                    "reason": "snapshot_older_than_600s",
                }
            ],
        )

    def test_the_cooldown_bounds_the_retry_without_losing_the_staleness_signal(self) -> None:
        """Resetting the staleness clock on *request* would silence a market
        forever the first time a response went missing, so the two clocks are
        separate: the market stays stale, and only the asking is rate-limited."""
        clock = _Clock()
        socket = _ScriptedSocket([(1, _subscribed(1))], clock)
        observed: list[int] = []
        self._drive(
            self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[
                (0.1, lambda: clock.advance(601.0)),
                (0.1, lambda: observed.append(len(socket.sent))),
                (0.1, lambda: clock.advance(30.0)),
                (0.1, lambda: observed.append(len(socket.sent))),
                (0.1, lambda: clock.advance(31.0)),
            ],
            seconds=0.9,
        )

        self.assertEqual(observed, [2, 2], "no second request inside the 60s cooldown")
        self.assertEqual(
            [command["cmd"] for command in self._commands(socket)],
            ["subscribe", "update_subscription", "update_subscription"],
            "and exactly one retry once the cooldown expires with no snapshot back",
        )

    def test_a_snapshot_that_arrives_stops_the_asking(self) -> None:
        """The trigger is the absence of a snapshot, so its arrival — and not the
        cooldown — is what ends the request."""
        clock = _Clock()
        socket = _ScriptedSocket(
            [
                (1, _subscribed(1)),
                (2, _snapshot(1, 9, TICKER_A)),
                (2, _snapshot(1, 10, TICKER_B)),
            ],
            clock,
        )
        self._drive(
            self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[(0.05, lambda: clock.advance(601.0)), (0.1, lambda: clock.advance(300.0))],
            seconds=0.5,
        )

        self.assertEqual(
            [command["cmd"] for command in self._commands(socket)],
            ["subscribe", "update_subscription"],
            "300s past the cooldown, but both books were re-established",
        )

    def test_an_acknowledgement_that_never_arrives_leaves_the_poller_off(self) -> None:
        """`update_subscription` names the subscription it acts on, and a `sid`
        this splice picked would be a valid command about the wrong one. The
        epoch keeps capturing; it just never asks."""
        clock = _Clock()
        socket = _ScriptedSocket([(1, _orderbook(1, 3))], clock)
        self._drive(
            self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[(0.05, lambda: clock.advance(601.0))],
        )

        self.assertEqual([command["cmd"] for command in self._commands(socket)], ["subscribe"])
        self.assertEqual(
            self._control("orderbook_reconciliation_disabled"),
            [
                {
                    "event": "orderbook_reconciliation_disabled",
                    "reason": "no_subscribed_acknowledgement",
                    "channel": "orderbook_delta",
                }
            ],
            "recorded once for the epoch, not once per sweep",
        )

    def test_an_acknowledgement_for_another_channel_is_not_the_book_sid(self) -> None:
        """One subscribe yields three subscription ids and only the book's is
        wanted. Taking whichever arrived would aim the request at the trades."""
        clock = _Clock()
        socket = _ScriptedSocket([(1, _subscribed(2, channel="trade"))], clock)
        self._drive(
            self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[(0.05, lambda: clock.advance(601.0))],
        )

        self.assertEqual([command["cmd"] for command in self._commands(socket)], ["subscribe"])
        self.assertEqual(
            [record["reason"] for record in self._control("orderbook_reconciliation_disabled")],
            ["no_subscribed_acknowledgement"],
        )

    def test_a_rate_limit_reply_widens_the_sweep_instead_of_ending_the_epoch(self) -> None:
        """Code 27 is a statement about cadence, not about validity. The
        connection is the expensive thing here — a poller that never runs costs a
        wider trust interval, one that kills the socket costs tape."""
        clock = _Clock()
        socket = _ScriptedSocket(
            [(1, _subscribed(1)), (2, _error(2, 27, "command rate limit exceeded"))],
            clock,
        )
        splice = self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01)
        self._drive(splice, steps=[(0.05, lambda: clock.advance(601.0))])

        self.assertEqual(splice.snapshot_sweep_seconds, 0.02)
        backoff = self._control("orderbook_reconciliation_backoff")
        self.assertEqual(len(backoff), 1)
        self.assertEqual(backoff[0]["code"], 27)
        self.assertEqual(backoff[0]["from_sweep_seconds"], 0.01)
        self.assertEqual(backoff[0]["to_sweep_seconds"], 0.02)
        self.assertEqual(self._control("connection_failed"), [])
        self.assertEqual(
            [record["reason"] for record in self._control("connection_closing")],
            ["time_limit"],
            "the epoch ran to its own end rather than being ended by the reply",
        )

    def test_any_other_error_reply_disables_the_poller_for_the_epoch(self) -> None:
        """A poller that cannot be trusted to form a valid command must stop
        forming them — and must still not cost the connection its tape."""
        clock = _Clock()
        socket = _ScriptedSocket(
            [(1, _subscribed(1)), (2, _error(2, 11, "Invalid parameter"))],
            clock,
        )
        self._drive(
            self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[(0.05, lambda: clock.advance(601.0)), (0.1, lambda: clock.advance(601.0))],
            seconds=0.5,
        )

        self.assertEqual(
            [command["cmd"] for command in self._commands(socket)],
            ["subscribe", "update_subscription"],
            "still stale, still past the cooldown, and deliberately never asked again",
        )
        disabled = self._control("orderbook_reconciliation_disabled")
        self.assertEqual(len(disabled), 1)
        self.assertEqual(disabled[0]["reason"], "command_rejected")
        self.assertEqual(disabled[0]["code"], 11)
        self.assertEqual(disabled[0]["command_id"], 2)
        self.assertEqual(self._control("connection_failed"), [])
        self.assertEqual(
            [record["reason"] for record in self._control("connection_closing")], ["time_limit"]
        )

    def test_an_error_for_a_command_the_poller_did_not_send_still_ends_the_epoch(self) -> None:
        """Regression on the pre-existing rule. Attribution is by the venue's own
        echoed command id, because assuming an unattributable rejection was the
        poller's would swallow a rejected *subscription* and leave a live-looking
        socket carrying no market data."""
        clock = _Clock()
        socket = _ScriptedSocket(
            [(1, _subscribed(1)), (2, _error(99, 11, "Invalid parameter"))],
            clock,
        )
        self._drive(
            self._splice(socket, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[(0.05, lambda: clock.advance(601.0))],
        )

        failures = self._control("connection_failed")
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["error_type"], "KalshiProtocolError")
        self.assertEqual(self._control("orderbook_reconciliation_disabled"), [])

    def test_configuring_the_poller_off_sends_nothing_at_all(self) -> None:
        """A supported configuration on the capture path, which means the code
        does not run rather than runs and declines to act."""
        for max_age in (0, None):
            with self.subTest(snapshot_max_age_seconds=max_age):
                clock = _Clock()
                socket = _ScriptedSocket([(1, _subscribed(1))], clock)
                splice = self._splice(
                    socket,
                    monotonic=clock,
                    snapshot_max_age_seconds=max_age,
                    snapshot_sweep_seconds=0.01,
                )
                self.assertEqual(splice.snapshot_sweep_seconds, 0.0)
                self._drive(splice, steps=[(0.05, lambda: clock.advance(6_000.0))])

                self.assertEqual(
                    [command["cmd"] for command in self._commands(socket)], ["subscribe"]
                )
                self.assertEqual(self._control("orderbook_reconciliation_request"), [])
                self.assertEqual(self._control("orderbook_reconciliation_disabled"), [])

    def test_a_new_epoch_asks_nothing_until_the_venue_names_the_sid_again(self) -> None:
        """Poller state is per connection and discarded on reconnect: a `sid`
        carried across would address a subscription that no longer exists."""
        clock = _Clock()
        first = _ScriptedSocket(
            [(1, _subscribed(1)), (2, ConnectionError("dropped"))], clock
        )
        second = _ScriptedSocket([], clock)
        self._drive(
            self._splice(first, second, monotonic=clock, snapshot_sweep_seconds=0.01),
            steps=[(0.05, lambda: clock.advance(601.0))],
            seconds=0.4,
            connections=2,
        )

        self.assertEqual([command["cmd"] for command in self._commands(first)],
                         ["subscribe", "update_subscription"])
        self.assertEqual([command["cmd"] for command in self._commands(second)], ["subscribe"])
        self.assertEqual(
            [record["reason"] for record in self._control("orderbook_reconciliation_disabled")],
            ["no_subscribed_acknowledgement"],
        )


if __name__ == "__main__":
    unittest.main()
