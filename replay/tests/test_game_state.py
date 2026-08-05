"""Game state as an exogenous clock: tagged, kept whole, interpreted nowhere.

The fixture is a real 4-minute capture of the Polymarket sports socket taken on
2026-07-30 — 164 records, 160 frames, 20 games across 7 leagues. It is pinned
because every shape asserted below was discovered on the wire and contradicts the
published schema somewhere: the AsyncAPI spec declares `slug` the sole required
field and it appears in none of these frames, names the finish time
`finished_timestamp` where the wire sends `finishedTimestamp`, and documents a
`last_update` that does not exist.
"""

from __future__ import annotations

import json
import unittest
from collections import Counter
from pathlib import Path

from replay.envelope import parse_envelope
from replay.events import GameState, RawEvent, normalize
from replay.order import OrderedEnvelope

FIXTURE = Path(__file__).parent / "fixtures" / "polymarket_sports_20260730.ndjson"


def _events():
    for number, line in enumerate(FIXTURE.read_bytes().splitlines(), start=1):
        if not line.strip():
            continue
        envelope = parse_envelope(line)
        item = OrderedEnvelope(
            lane="polymarket_sports",
            object_key="fixture.ndjson",
            line_number=number,
            order_ns=envelope.visible_ns,
            order_clock="visible_ns",
            envelope=envelope,
        )
        yield from normalize(item)


def _one(payload: dict) -> GameState | RawEvent:
    line = json.dumps({
        "envelope_version": 2, "delivery_index": 1, "record_id": "pms-e-1",
        "visible_ns": 1, "monotonic_ns": 1, "venue": "polymarket",
        "stream": "reference_event", "connection_epoch": "e", "local_counter": 1,
        "source_cursor": None, "kind": "venue_frame",
        "raw_payload": json.dumps(payload),
    }).encode()
    item = OrderedEnvelope(lane="polymarket_sports", object_key="k.ndjson", line_number=1,
                           order_ns=1, order_clock="visible_ns",
                           envelope=parse_envelope(line))
    return next(iter(normalize(item)))


class GameStateNormalisationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.events = list(_events())
        self.games = [event for event in self.events if isinstance(event, GameState)]

    def test_every_game_frame_becomes_a_game_state(self):
        """Previously all 160 collapsed into an empty ReferenceTick.

        Both reference feeds land on the `reference_event` stream, and the price
        branch matched first, so `topic`, `symbol`, `value` and `source_time` all
        came out empty while teams, score, period and eventState were dropped.
        """
        self.assertEqual(len(self.games), 160)
        kinds = Counter(type(event).__name__ for event in self.events)
        self.assertEqual(kinds["ReferenceTick"], 0)

    def test_cricket_is_keyed_by_its_own_identifier_field(self):
        """`gameId`-only keying drops the sport with no error anywhere."""
        keyed = Counter(game.game_id_field for game in self.games)
        self.assertEqual(keyed["metadataGameId"], 5)
        self.assertEqual(keyed["gameId"], 155)
        self.assertEqual(len({game.game_id for game in self.games}), 20)

    def test_event_state_is_kept_whole(self):
        """Sport-specific extras no fixed field set anticipates.

        Tennis frames carry `tournamentName` and `tennisRound`; soccer carries
        `elapsed`. Storing the block verbatim keeps a later reading a code change
        rather than a re-collection.
        """
        with_state = [game for game in self.games if game.event_state]
        self.assertEqual(len(with_state), 38)
        tennis = next(g for g in with_state if g.event_state.get("type") == "tennis")
        self.assertEqual(tennis.event_state["tournamentName"], "Challenger Bonn")
        self.assertEqual(tennis.event_state["tennisRound"], "Round of 16")
        # And the whole frame too, so nothing at all is lost in normalisation.
        self.assertIn("leagueAbbreviation", tennis.raw)

    def test_the_venue_update_time_is_carried_where_it_exists(self):
        """`eventState.updatedAt` is the only claim about when the world changed.

        Everything else on the frame dates its delivery, not the event.
        """
        stamped = [game for game in self.games if game.state_updated_at]
        self.assertEqual(len(stamped), 38)
        self.assertTrue(all(value.endswith("Z") for value in
                            (game.state_updated_at for game in stamped)))

    def test_score_is_preserved_not_parsed(self):
        """A compound, league-specific encoding with no uniform parse.

        `000-000|1-1|Bo3` is round score, map score, and series format in one
        string; tennis sends games-in-set, cricket sends runs-wickets.
        """
        scores = {game.score for game in self.games}
        self.assertTrue(any("|Bo" in score for score in scores if score))
        compound = next(g for g in self.games if g.score and "|Bo" in g.score)
        self.assertEqual(compound.score, compound.raw["score"])

    def test_ended_is_not_terminal(self):
        """A finished cricket match reverted to Scheduled under the same id.

        It carried `finishedTimestamp` while ended, then came back six seconds
        later with `ended: false`. Anything treating `ended` as absorbing, or the
        first `ended: true` as a resolution time, is wrong for this match — and
        this is the fixture that proves it.
        """
        by_game: dict[str, list[GameState]] = {}
        for game in self.games:
            by_game.setdefault(game.game_id, []).append(game)
        regressions = [
            game_id for game_id, states in by_game.items()
            if any(before.ended and not after.ended
                   for before, after in zip(states, states[1:]))
        ]
        self.assertTrue(regressions, "fixture should contain the ended->not-ended case")
        states = by_game[regressions[0]]
        finished = [state for state in states if state.finished_timestamp]
        self.assertTrue(finished, "the reverting match reported a finish time first")

    def test_a_game_frame_with_no_identifier_is_loud(self):
        """A third identifier field must not be keyed to the empty string.

        Cricket already proved the provider will add one without notice; merging
        every such match under one key would be silent and irreversible.
        """
        event = _one({"leagueAbbreviation": "nba", "score": "10-8", "live": True})
        self.assertIsInstance(event, RawEvent)
        self.assertEqual(event.name, "game_state_without_identifier")
        self.assertEqual(event.raw["leagueAbbreviation"], "nba")

    def test_a_price_tick_is_still_a_price_tick(self):
        """The discriminator must not capture the other feed on this stream."""
        from replay.events import ReferenceTick

        event = _one({
            "topic": "crypto_prices", "type": "update", "timestamp": 1785348545138,
            "payload": {"symbol": "btcusdt", "timestamp": 1785348545000, "value": 64275.5},
        })
        self.assertIsInstance(event, ReferenceTick)
        self.assertEqual(event.symbol, "btcusdt")


if __name__ == "__main__":
    unittest.main()
