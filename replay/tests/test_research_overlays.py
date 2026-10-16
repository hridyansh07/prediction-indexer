import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock
from fractions import Fraction
from pathlib import Path

from replay.research import game, tables
from replay.research.config import load
from replay.research.overlays import book_mapper, episode_intervals, exact_decimal
from replay.tests.research_fixtures import BOOKS, KALSHI, PM_B, S, T0, cross_output

CONFIG = load(Path(__file__).resolve().parents[2] / "configs" / "research.json")


class DecimalTest(unittest.TestCase):
    def test_exact_decimals(self):
        self.assertEqual(exact_decimal(Fraction(21223, 100)), "212.23")
        self.assertEqual(exact_decimal(Fraction(-1, 100)), "-0.01")
        self.assertEqual(exact_decimal(0), "0")
        self.assertEqual(exact_decimal(Fraction(3 * 10 ** 36 + 1, 10 ** 36)), "3." + "0" * 35 + "1")
        with self.assertRaises(ValueError):
            exact_decimal(Fraction(1, 3))


class OverlayTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        output = Path(self._tmp.name) / "cross"
        cross_output(output)
        self.overlay = episode_intervals(output, "cross", CONFIG["lenses"]["cross"], BOOKS)
        self.items = self.overlay["items"]

    def tearDown(self):
        self._tmp.cleanup()

    def test_every_episode_becomes_one_interval_in_file_order(self):
        self.assertEqual(self.overlay["kind"], "intervals")
        self.assertEqual(self.overlay["semantic_sha256"], "2" * 64)
        self.assertEqual([i["detail"]["kind"] for i in self.items], ["net", "net", "net", "gross"])

    def test_fill_metrics_net_and_quantity(self):
        first = {m["name"]: (m["value"], m["unit"]) for m in self.items[0]["metrics"]}
        self.assertEqual(first, {"duration": ("250", "ms"), "net": ("3.05", "USD"), "quantity": ("1067", "contracts")})
        third = {m["name"]: m["value"] for m in self.items[2]["metrics"]}
        self.assertEqual(third, {"duration": "50", "net": "-0.01", "quantity": "1"})
        self.assertEqual(self.items[0]["detail"]["stop"], "edge")
        self.assertEqual(self.items[0]["detail"]["kill_prices"], ["9000", "6000"])

    def test_no_fill_episode_has_duration_only(self):
        self.assertEqual(self.items[3]["metrics"], [{"name": "duration", "value": "3000", "unit": "ms"}])
        self.assertNotIn("stop", self.items[3]["detail"])

    def test_kalshi_legs_map_to_the_yes_book(self):
        self.assertEqual(self.items[0]["books"], [0, 1])
        self.assertEqual(self.items[1]["books"], [0, 2])
        both = [dict(BOOKS[0], orientation="complement"), *BOOKS]
        mapped = book_mapper(both)
        self.assertEqual(mapped({"instrument": KALSHI, "orientation": "complement"}), 1)
        self.assertIsNone(mapped({"instrument": "polymarket:999", "orientation": "outcome"}))
        self.assertEqual(mapped({"instrument": PM_B, "orientation": "outcome"}), 3)

    def test_overlap_links_and_dedup_groups_per_kind(self):
        rows = tables.episode_rows("bundle_x", "cross", self.items, books=[
            {"market_type": "series_moneyline"}] * 3, capture_end_ns=T0 + 1800 * S, game="lol")
        net = [r for r in rows if r["kind"] == "net"]
        self.assertEqual([r["shares_leg_with"] for r in net], [1, 1, 0])
        self.assertEqual(len({r["dedup_group"] for r in net}), 2)
        # Connected components: A~B and B~C put A and C in one group though they never overlap.
        chain = [dict(self.items[0], id=i, start_ns=str(T0 + s * S), end_ns=str(T0 + e * S))
                 for i, (s, e) in enumerate([(0, 10), (5, 20), (15, 30)])]
        self.assertEqual(tables.overlap_links(chain), ([1, 2, 1], [0, 0, 0]))
        gross = [r for r in rows if r["kind"] == "gross"]
        self.assertEqual(gross[0]["shares_leg_with"], 0)
        self.assertEqual(net[0]["venue_pair"], "kalshi+polymarket")
        self.assertEqual(net[0]["net"], 3.05)
        self.assertEqual(net[0]["net_exact"], "3.05")


class GameTest(unittest.TestCase):
    DOCUMENT = {
        "state": "ok", "reason": None, "source": {"name": "kalshi"}, "segment_kind": "map",
        "competitors": {"home": {"label": "Cloud9", "participant": 0}, "away": {"label": "LYON", "participant": 1}},
        "scheduled_start_ns": T0,
        "segments": [{"index": 1, "start_ns": T0 + 100 * S, "start_estimated": True, "end_ns": T0 + 2000 * S,
                      "settled_ns": T0 + 2100 * S, "winner": "home", "details": {"duration_s": 1900}}],
        "match": {"end_ns": T0 + 5000 * S, "winner": "away", "score": {"home": 1, "away": 3}},
    }

    def test_windows_and_known_at(self):
        events = game.game_events(self.DOCUMENT, CONFIG["game_time_windows"])
        by_kind = {e["kind"]: e for e in events}
        self.assertEqual([e["kind"] for e in events],
                         ["scheduled_start", "segment_start", "segment_end", "settlement", "match_end"])
        start = by_kind["segment_start"]
        self.assertEqual((start["time_basis"], start["earliest_ns"], start["latest_ns"]),
                         ("close_minus_duration", str(T0 + 98 * S), str(T0 + 165 * S)))
        end = by_kind["segment_end"]
        self.assertEqual((end["earliest_ns"], end["latest_ns"], end["known_at_ns"], end["exact"]),
                         (str(T0 + 1998 * S), str(T0 + 2005 * S), str(T0 + 2005 * S), False))
        self.assertEqual(end["label"], "Map 1 end · Cloud9")
        self.assertEqual(end["detail"]["details"], {"duration_s": 1900})
        settled = by_kind["settlement"]
        self.assertEqual((settled["exact"], settled["earliest_ns"], settled["latest_ns"]),
                         (True, str(T0 + 2100 * S), str(T0 + 2100 * S)))
        scheduled = by_kind["scheduled_start"]
        self.assertEqual((scheduled["exact"], scheduled["known_at_ns"]), (False, str(T0)))
        self.assertEqual(by_kind["match_end"]["detail"]["score"], {"home": 1, "away": 3})
        self.assertEqual({e["kind"]: e["window_status"] for e in events},
                         {"scheduled_start": None, "segment_start": "provisional", "segment_end": "provisional",
                          "settlement": None, "match_end": "provisional"})
        document = game.game_json(self.DOCUMENT, "archive", CONFIG["game_time_windows"], "provisional")
        self.assertEqual(document["windows"]["status"], "provisional")

    def test_states(self):
        self.assertEqual(game.game_json(None, "no_context", CONFIG["game_time_windows"])["state"], "no_source")
        incomplete = dict(self.DOCUMENT, state="unavailable", reason="incomplete", segments=[], match=None)
        self.assertEqual(game.game_json(incomplete, "archive", CONFIG["game_time_windows"])["state"], "incomplete")
        no_fetch = dict(incomplete, reason="no_fetch")
        self.assertEqual(game.game_json(no_fetch, "archive", CONFIG["game_time_windows"])["reason"], "no_fetch")


class GameSourceTest(unittest.TestCase):
    """Committed game_state.json first (pinned loader), then a local archive, else no_source."""

    def event(self, root, *, context=True, committed=False):
        if committed:
            (root / "game_state").mkdir(parents=True, exist_ok=True)
            (root / "game_state" / "game_state.json").write_text("{}")
        return SimpleNamespace(root=root, context={} if context else None,
                               result={"context": {"snapshot_sha256": "a" * 64}, "game_state_sha256": "b" * 64},
                               game_state_document={"state": "ok"})

    def test_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(game.prepared_document(self.event(root, context=False), root), (None, "no_context"))
            self.assertEqual(game.prepared_document(self.event(root), None), (None, "no_archive"))
            with mock.patch("replay.preparation.load_snapshot", return_value="ctx") as snapshot, \
                    mock.patch("replay.game_state.load") as load, \
                    mock.patch("replay.prepare_game_state.game_state_document", return_value={"state": "x"}) as archive:
                committed = game.prepared_document(self.event(root, committed=True), root)
                self.assertEqual(committed, ({"state": "ok"}, "committed"))
                load.assert_called_once_with(root / "game_state" / "game_state.json", "b" * 64, "ctx")
                archive.assert_not_called()
                (root / "game_state" / "game_state.json").unlink()
                self.assertEqual(game.prepared_document(self.event(root), root), ({"state": "x"}, "archive"))
                self.assertEqual(snapshot.call_count, 2)


if __name__ == "__main__":
    unittest.main()
