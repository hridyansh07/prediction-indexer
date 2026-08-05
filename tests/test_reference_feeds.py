"""The two Polymarket reference feeds, and the vocabulary they land on.

Frames used here are copied verbatim from live captures on 2026-07-29, including
the shapes no document describes: the undocumented `eventState` block, the cricket
frame that carries `metadataGameId` instead of `gameId`, and the empty text frame
RTDS opens every connection with. Interpreting any of it is a later pass — these
tests cover getting the bytes onto the tape unaltered and in the right lane.
"""

from __future__ import annotations

import asyncio
import json
import re
import tempfile
import time
import unittest
import urllib.error
from pathlib import Path

from splices.common.envelope import (
    STREAM_PUBLIC_SNAPSHOT,
    STREAM_REFERENCE_EVENT,
    VENUE_POLYMARKET,
    WIRE_VOCABULARY,
)
from splices.common.spool import Spool, spool_files
from targeter.targets import Target, write_targets
from splices.polymarket.rtds import PolymarketRtdsSplice
from splices.polymarket.sports import PolymarketSportsSplice

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# -- verbatim live frames ---------------------------------------------------

SOCCER = json.dumps({
    "gameId": 90103874, "leagueAbbreviation": "arg", "homeTeam": "CA Barracas Central",
    "awayTeam": "CA Aldosivi", "status": "InProgress",
    "eventState": {"type": "soccer", "createdAt": "2026-07-29T18:06:45.267269546Z",
                   "updatedAt": "2026-07-29T18:06:45.267269546Z", "score": "0-0",
                   "elapsed": "35", "period": "1H", "live": True, "ended": False},
    "score": "0-0", "elapsed": "35", "period": "1H", "live": True, "ended": False,
})

SOCCER_GOAL = SOCCER.replace('"score": "0-0"', '"score": "1-0"').replace(
    '"elapsed": "35"', '"elapsed": "37"')

# Cricket sends a different identifier field and no `gameId` at all.
CRICKET = json.dumps({
    "metadataGameId": "id2703220173085554", "leagueAbbreviation": "cricket",
    "score": "32-186", "period": "Live", "live": True, "ended": False,
})

ESPORTS = json.dumps({
    "gameId": 1597550, "leagueAbbreviation": "lol", "homeTeam": "Anubis Gaming",
    "awayTeam": "GnG Amazigh", "status": "running", "score": "000-000|1-0|Bo3",
    "period": "2/3", "live": True, "ended": False,
})

RTDS_TICK = json.dumps({
    "connection_id": "gVmq8A65uWeIKEiW-A==",
    "payload": {"full_accuracy_value": "1.08750000", "symbol": "xrpusdt",
                "timestamp": 1785348545000, "value": 1.0875},
    "timestamp": 1785348545138, "topic": "crypto_prices", "type": "update",
})


class _FakeSocket:
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


def _factory(socket):
    return lambda _url: socket


class SpliceHarness(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.addCleanup(self._directory.cleanup)

    def _records(self, lane: str) -> list[dict]:
        rows = []
        for path in spool_files(self.root / "spool", lane):
            rows.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
        return rows

    def _run(self, splice_class, lane, socket, **kwargs):
        splice = splice_class(
            Spool(self.root / "spool", lane), None,
            connect_factory=_factory(socket), loop_wake_seconds=0.01, **kwargs,
        )
        asyncio.run(splice.run(max_connections=1, stop_after_seconds=0.4))
        return splice


class SportsSpliceTests(SpliceHarness):
    def test_runs_with_no_targets_file_at_all(self):
        """The feed broadcasts everything; waiting on a targets file would idle it.

        The old loop required a readable, non-empty targets file before it would
        connect, so a reference splice would have sat in the poll branch forever
        while a perfectly good socket went unused.
        """
        splice = self._run(PolymarketSportsSplice, "polymarket_sports",
                           _FakeSocket([SOCCER, CRICKET]))
        self.assertEqual(splice.frames, 2)
        opened = json.loads(self._records("polymarket_sports")[0]["raw_payload"])
        self.assertEqual(opened["target_digest"], "broadcast")
        self.assertIs(opened["reference_feed"], True)

    def test_broadcast_digest_is_not_a_hash_of_nothing(self):
        """"Selects nothing" and "was handed an empty list" must not look alike.

        One is a healthy reference feed, the other is a blind subscription feed.
        A real digest over an empty asset set would render both as plausible hex.
        """
        splice = self._run(PolymarketSportsSplice, "polymarket_sports", _FakeSocket([SOCCER]))
        self.assertEqual(splice.broadcast_target_set().digest, "broadcast")
        self.assertFalse(re.fullmatch(r"[0-9a-f]{32}", "broadcast"))

    def test_ping_is_recorded_before_it_is_answered(self):
        """The tape must show the ping even if sending the pong fails."""
        socket = _FakeSocket(["ping", SOCCER])
        splice = self._run(PolymarketSportsSplice, "polymarket_sports", socket)
        payloads = [r["raw_payload"] for r in self._records("polymarket_sports")
                    if r["kind"] == "venue_frame"]
        self.assertEqual(payloads[0], "ping")
        self.assertEqual(socket.sent, ["pong"])
        self.assertEqual(splice.pongs_sent, 1)

    def test_a_game_frame_never_triggers_a_pong(self):
        socket = _FakeSocket([SOCCER, CRICKET, ESPORTS])
        self._run(PolymarketSportsSplice, "polymarket_sports", socket)
        self.assertEqual(socket.sent, [])

    def test_envelope_carries_venue_polymarket_not_the_lane(self):
        """Provenance is Polymarket's: they operate the feed and own its latency.

        The lane exists only to keep `delivery_index` dense per process.
        """
        self._run(PolymarketSportsSplice, "polymarket_sports", _FakeSocket([SOCCER]))
        frame = [r for r in self._records("polymarket_sports") if r["kind"] == "venue_frame"][0]
        self.assertEqual(frame["venue"], VENUE_POLYMARKET)
        self.assertEqual(frame["stream"], STREAM_REFERENCE_EVENT)
        self.assertEqual(frame["source_cursor"], {"type": "unsequenced", "counter": 3})


class RtdsSpliceTests(SpliceHarness):
    def test_subscription_carries_no_filters_key(self):
        """The documented `filters` form returns silence, not an error.

        Verified twice against a feed proven live in the same window by an
        unfiltered subscription running beside it. A filtered subscription is
        indistinguishable from a quiet market, so this is the failure mode that
        would have produced weeks of clean, empty tapes.
        """
        socket = _FakeSocket([RTDS_TICK])
        self._run(PolymarketRtdsSplice, "polymarket_rtds", socket)
        subscription = json.loads(socket.sent[0])
        self.assertEqual(subscription["action"], "subscribe")
        for entry in subscription["subscriptions"]:
            self.assertNotIn("filters", entry)
            self.assertEqual(entry["type"], "*")

    def test_the_empty_opening_frame_is_recorded(self):
        """RTDS opens every connection with an empty text frame.

        It is the cheapest proof the socket reached application level, and it is
        undocumented — exactly the kind of thing a filtering splice discards.
        """
        self._run(PolymarketRtdsSplice, "polymarket_rtds", _FakeSocket(["", RTDS_TICK]))
        frames = [r for r in self._records("polymarket_rtds") if r["kind"] == "venue_frame"]
        self.assertEqual(frames[0]["raw_payload"], "")
        self.assertEqual(len(frames), 2)


REST_BOOK = json.dumps({
    "market": "0x747d", "asset_id": "1075058827677314893", "timestamp": "1785352207071",
    "hash": "0853ef8651ad672e18834875874668da6988a31e",
    "bids": [{"price": "0.08", "size": "33343.4"}], "asks": [{"price": "0.99", "size": "218442.27"}],
    "min_order_size": "5", "tick_size": "0.01", "neg_risk": False, "last_trade_price": "0.08",
})


class _FakePollTransport:
    """Stands in for the REST poller, one cycle then idle."""

    def __init__(self, books, *, delay=0.0):
        self._books = list(books)
        self._delay = delay
        self.sent = []
        self.asset_ids = ()

    async def send(self, message):
        self.sent.append(message)
        self.asset_ids = tuple(json.loads(message)["asset_ids"])

    async def recv(self):
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._books:
            return self._books.pop(0)
        await asyncio.sleep(3600)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


class SnapshotPollerTests(SpliceHarness):
    def _poller(self, transport, **kwargs):
        from splices.polymarket.snapshots import PolymarketSnapshotSplice

        write_targets(self.root / "t.json", venue="polymarket",
                      targets=[Target("1075058827677314893"), Target("asset-2")])
        splice = PolymarketSnapshotSplice(
            Spool(self.root / "spool", "polymarket_snapshots"), self.root / "t.json",
            transport_factory=lambda _url: transport, loop_wake_seconds=0.01, **kwargs,
        )
        asyncio.run(splice.run(max_connections=1, stop_after_seconds=0.4))
        return splice

    def test_each_book_becomes_one_record_verbatim(self):
        """A batch boundary is ours, not the venue's, and must not be the unit."""
        self._poller(_FakePollTransport([REST_BOOK, REST_BOOK]))
        frames = [r for r in self._records("polymarket_snapshots") if r["kind"] == "venue_frame"]
        self.assertEqual(len(frames), 2)
        self.assertEqual(json.loads(frames[0]["raw_payload"])["hash"],
                         "0853ef8651ad672e18834875874668da6988a31e")

    def test_lands_on_its_own_stream_not_public_book(self):
        """A poll and a delta are different evidence.

        Sharing `public_book` would let a reader fold recovery points into the
        delta sequence and reconstruct a book that is neither.
        """
        self._poller(_FakePollTransport([REST_BOOK]))
        frame = [r for r in self._records("polymarket_snapshots")
                 if r["kind"] == "venue_frame"][0]
        self.assertEqual(frame["stream"], STREAM_PUBLIC_SNAPSHOT)
        self.assertEqual(frame["venue"], VENUE_POLYMARKET)

    def test_cursor_dates_the_snapshot_and_claims_nothing_more(self):
        """One cycle spans hundreds of assets, so a dense cursor would compare
        unrelated books and invent gaps — the Limitless mistake."""
        self._poller(_FakePollTransport([REST_BOOK]))
        frame = [r for r in self._records("polymarket_snapshots")
                 if r["kind"] == "venue_frame"][0]
        self.assertEqual(frame["source_cursor"],
                         {"type": "snapshot", "source_time_ms": 1785352207071})

    def test_a_book_without_a_timestamp_still_records(self):
        """Losing the cursor must never cost the book itself."""
        self._poller(_FakePollTransport([json.dumps({"asset_id": "x", "hash": "abc"})]))
        frame = [r for r in self._records("polymarket_snapshots")
                 if r["kind"] == "venue_frame"][0]
        self.assertEqual(frame["source_cursor"]["type"], "unsequenced")

    def test_poll_survives_the_loop_wake_cancelling_recv(self):
        """The bug that produced a healthy-looking connection and zero books.

        The base loop caps every `recv` at `loop_wake_seconds`, so a fetch awaited
        inline is cancelled about a second in — and a real cycle over 120 assets
        takes longer than that. Polling must therefore run in its own task and
        survive repeated `recv` cancellation.
        """
        from splices.polymarket.snapshots import _BookPollTransport

        transport = _BookPollTransport("http://example.invalid", batch_size=2, poll_seconds=30.0)
        fetched = []

        def fake_fetch(batch):
            time.sleep(0.15)          # longer than the wake below
            fetched.append(batch)
            return [json.loads(REST_BOOK) for _ in batch]

        transport._fetch = fake_fetch

        async def drive():
            async with transport:
                await transport.send(json.dumps({"asset_ids": ["a", "b"]}))
                received = []
                deadline = time.monotonic() + 3.0
                while len(received) < 2 and time.monotonic() < deadline:
                    try:
                        # Exactly what the base loop does, and what broke it.
                        received.append(await asyncio.wait_for(transport.recv(), timeout=0.01))
                    except asyncio.TimeoutError:
                        continue
                return received

        received = asyncio.run(drive())
        self.assertEqual(len(received), 2, "poll did not survive recv cancellation")
        self.assertEqual(len(fetched), 1, "one batch of two assets")

    def test_a_failing_poll_surfaces_rather_than_stalling_silently(self):
        """A dead poller must reconnect, not sit quietly producing nothing."""
        from splices.polymarket.snapshots import _BookPollTransport

        transport = _BookPollTransport("http://example.invalid", batch_size=2, poll_seconds=30.0)

        def boom(_batch):
            raise urllib.error.HTTPError("u", 403, "Forbidden", {}, None)

        transport._fetch = boom

        async def drive():
            async with transport:
                await transport.send(json.dumps({"asset_ids": ["a"]}))
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline:
                    try:
                        await asyncio.wait_for(transport.recv(), timeout=0.01)
                    except asyncio.TimeoutError:
                        continue
                return None

        with self.assertRaises(urllib.error.HTTPError):
            asyncio.run(drive())

    def test_requests_name_the_client(self):
        """urllib's default agent draws a bare 403 from the edge.

        The first live run reconnected six times and captured zero books.
        """
        from splices.polymarket.snapshots import HEADERS

        self.assertIn("prediction-indexer", HEADERS["User-Agent"])


class WireVocabularyTests(unittest.TestCase):
    """Python and Rust must spell the vocabulary identically.

    The ingester rejects an unknown `venue` or `stream` outright, so a splice that
    emits a spelling Rust has never heard of writes a spool nothing can read — and
    the failure surfaces at ingest, long after the socket that produced the frames
    has moved on. That delay is what makes drift here worse than an ordinary bug.
    """

    def test_rust_declares_every_spelling_python_emits(self):
        source = (PROJECT_ROOT / "ingester" / "crates" / "types" / "src" / "identity.rs").read_text()
        for field_name, expected in WIRE_VOCABULARY.items():
            block = re.search(rf'wire_enum!\(\w+, "{field_name}", \{{(.*?)\}}\);',
                              source, re.DOTALL)
            self.assertIsNotNone(block, f"no wire_enum! for {field_name}")
            declared = set(re.findall(r'=>\s*"([^"]+)"', block.group(1)))
            self.assertEqual(declared, set(expected),
                             f"{field_name} vocabulary differs between Python and Rust")


if __name__ == "__main__":
    unittest.main()
