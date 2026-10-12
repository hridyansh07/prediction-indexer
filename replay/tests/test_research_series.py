import tempfile
import unittest
from pathlib import Path

from replay.research import series
from replay.research.jsonio import ResearchError
from replay.tests.research_fixtures import BOOKS, S, SCOPES, T0, default_rows, profile_output, q, top


def run(rows=None, max_buckets=30, tile_minutes=10):
    tiles = {}
    overview, index = series.build_series(
        iter(default_rows() if rows is None else rows), books=BOOKS, scopes=SCOPES, max_buckets=max_buckets,
        tile_minutes=tile_minutes, write_tile=lambda kind, minute, doc: tiles.__setitem__((kind, minute), doc))
    return overview, index, tiles


class GridTest(unittest.TestCase):
    def test_overview_grid_is_whole_seconds_within_the_bucket_cap(self):
        self.assertEqual(series.overview_grid(0, 1800 * S, 30), (60 * S, 30))
        self.assertEqual(series.overview_grid(0, 1800 * S, 16), (113 * S, 16))
        self.assertEqual(series.overview_grid(0, 5 * S, 2048), (S, 5))
        with self.assertRaises(ResearchError):
            series.overview_grid(10, 10, 16)


class OverviewTest(unittest.TestCase):
    def setUp(self):
        self.overview, self.index, self.tiles = run()
        self.book0 = self.overview["books"][0]

    def test_shape(self):
        self.assertEqual(self.overview["buckets"], 30)
        self.assertEqual(self.overview["bucket_ns"], str(60 * S))
        self.assertEqual(self.overview["start_ns"], str(T0))

    def test_bucket_min_max_last_over_usable_states(self):
        self.assertEqual([self.book0["bid"][k][0] for k in ("min", "max", "last")], [4000, 4100, 4100])
        self.assertEqual([self.book0["ask"][k][0] for k in ("min", "max", "last")], [4200, 4200, 4200])
        self.assertEqual(self.book0["usable_ppm"][0], 1_000_000)

    def test_zero_duration_state_contributes_nothing(self):
        self.assertEqual([self.book0["bid"][k][1] for k in ("min", "max", "last")], [3900, 4100, 3900])
        self.assertEqual(self.book0["ask"]["max"][1], 4300)

    def test_unusable_time_breaks_the_line(self):
        self.assertEqual(self.book0["usable_ppm"][11], 666_666)
        self.assertEqual(self.book0["bid"]["last"][11], 3900)
        for bucket in (12, 13, 14):
            self.assertIsNone(self.book0["bid"]["last"][bucket])
            self.assertEqual(self.book0["usable_ppm"][bucket], 0)

    def test_scope_gap_is_unknown_and_next_scope_reopens(self):
        for bucket in range(15, 20):
            self.assertIsNone(self.book0["bid"]["min"][bucket])
            self.assertEqual(self.overview["books"][1]["usable_ppm"][bucket], 0)
        self.assertEqual(self.book0["bid"]["last"][20], 6000)

    def test_not_initialized_book_has_no_quotes(self):
        book2 = self.overview["books"][2]
        self.assertTrue(all(v is None for v in book2["bid"]["last"]))
        self.assertTrue(all(v == 0 for v in book2["usable_ppm"]))

    def test_trades_count_into_their_bucket(self):
        self.assertEqual(self.overview["books"][1]["trades"][0], 1)
        self.assertEqual(self.overview["books"][1]["trade_qty"][0], 2_000_000)
        self.assertEqual(self.book0["trades"][21], 1)
        self.assertEqual(sum(self.book0["trades"]), 1)


class TileTest(unittest.TestCase):
    def setUp(self):
        self.overview, self.index, self.tiles = run()

    def test_tiles_written_only_where_a_scope_overlaps(self):
        self.assertEqual(self.index, {"1s": ["0", "10", "20"], "raw": ["0", "10", "20"]})
        _, index, _ = run(tile_minutes=5)
        self.assertEqual(index["1s"], ["0", "5", "10", "20", "25"])   # [15, 20) min is the gap

    def test_one_second_tile_breaks_at_invalidation(self):
        tile = self.tiles["1s", "10"]
        self.assertEqual(tile["buckets"], 600)
        book0 = tile["books"][0]
        self.assertEqual(book0["bid"]["last"][99], 3900)
        self.assertIsNone(book0["bid"]["last"][100])
        self.assertEqual(book0["usable_ppm"][99], 1_000_000)
        self.assertEqual(book0["usable_ppm"][100], 0)

    def test_raw_tile_initial_state_and_rows(self):
        raw = self.tiles["raw", "10"]
        self.assertEqual([(i["book"], i["since_ns"], i["bid"]) for i in raw["initial"]],
                         [(0, str(T0 + 61 * S), q(3900)), (1, str(T0), q(5000)), (2, str(T0), None)])
        self.assertEqual([r["t_ns"] for r in raw["rows"]], [str(T0 + 700 * S)])
        self.assertNotIn("cut", raw["rows"][0])
        last = self.tiles["raw", "20"]
        self.assertEqual(last["initial"], [])
        self.assertEqual([r["type"] for r in last["rows"]], ["top", "top", "trade"])

    def test_rows_out_of_order_fail(self):
        rows = default_rows()
        rows[3], rows[4] = rows[4], rows[3]
        rows[4] = top(T0 + 10 * S, 0, q(1), q(2))
        with self.assertRaises(ResearchError):
            run(rows)


class StreamTest(unittest.TestCase):
    def test_transitions_rows_decode_through_the_shared_codec(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profile"
            profile_output(output)
            self.assertEqual(list(series.transitions_rows(output)), default_rows())
            books, scopes = series.profile_tables(output)
            self.assertEqual((len(books), len(scopes)), (3, 2))


if __name__ == "__main__":
    unittest.main()
