from __future__ import annotations

import unittest

from analysis.equivalence import (
    Instrument,
    analyse_state_intervals,
    compare_routes,
    equivalence_classes,
    summarize_routes,
)


def _instrument(name, venue, mask, prices, market_key=None, market_type="map_winner"):
    return Instrument(
        instrument_id=name,
        venue=venue,
        market_key=market_key or name,
        market_type=market_type,
        position="yes",
        outcome_label=name,
        mask=frozenset(mask),
        prices=prices,
    )


UNIVERSE = frozenset({"seq:HH", "seq:HAH", "seq:HAA"})


class EquivalenceClassTests(unittest.TestCase):
    def test_masks_equal_within_universe_group_together(self) -> None:
        """The user's case: under-2.5 and 'home wins map 2' after map 1."""
        under = _instrument("under", "polymarket", {"seq:HH"}, {}, market_type="total_maps")
        map2 = _instrument("map2", "kalshi", {"seq:HH", "seq:AH"}, {})
        classes = equivalence_classes([under, map2], UNIVERSE)
        self.assertEqual(len(classes), 1)
        self.assertEqual({m.instrument_id for m in classes[0]}, {"under", "map2"})

    def test_masks_differing_outside_universe_still_group(self) -> None:
        """Equality is judged over reachable outcomes, not over all of Omega."""
        a = _instrument("a", "kalshi", {"seq:HH", "seq:AA"}, {})
        b = _instrument("b", "kalshi", {"seq:HH"}, {})
        self.assertEqual(len(equivalence_classes([a, b], UNIVERSE)), 1)

    def test_trivial_masks_are_dropped(self) -> None:
        """Empty and full masks are settled positions, not routes."""
        empty = _instrument("empty", "kalshi", set(), {})
        full = _instrument("full", "kalshi", UNIVERSE, {})
        other = _instrument("other", "kalshi", UNIVERSE, {})
        self.assertEqual(equivalence_classes([empty, full, other], UNIVERSE), [])

    def test_two_positions_on_one_market_are_not_two_routes(self) -> None:
        """Same contract twice is not a second way to express the bet."""
        a = _instrument("a:yes", "kalshi", {"seq:HH"}, {}, market_key="M")
        b = _instrument("a:dup", "kalshi", {"seq:HH"}, {}, market_key="M")
        self.assertEqual(equivalence_classes([a, b], UNIVERSE), [])

    def test_distinct_masks_do_not_group(self) -> None:
        a = _instrument("a", "kalshi", {"seq:HH"}, {})
        b = _instrument("b", "kalshi", {"seq:HAH"}, {})
        self.assertEqual(equivalence_classes([a, b], UNIVERSE), [])


class CompareRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.a = _instrument("a", "kalshi", {"seq:HH"}, {60: 0.40, 120: 0.42, 180: 0.44})
        self.b = _instrument("b", "polymarket", {"seq:HH"}, {60: 0.45, 120: 0.42})

    def test_only_shared_bars_are_compared(self) -> None:
        rows = compare_routes([self.a, self.b])
        self.assertEqual([r["bar_timestamp_seconds"] for r in rows], [60, 120])

    def test_spread_and_cheapest_route(self) -> None:
        rows = compare_routes([self.a, self.b])
        self.assertAlmostEqual(rows[0]["spread"], 0.05)
        self.assertEqual(rows[0]["cheapest_instrument"], "a")
        self.assertAlmostEqual(rows[1]["spread"], 0.0)

    def test_bar_range_filters(self) -> None:
        rows = compare_routes([self.a, self.b], bar_range=(100, 200))
        self.assertEqual([r["bar_timestamp_seconds"] for r in rows], [120])

    def test_single_route_yields_nothing(self) -> None:
        self.assertEqual(compare_routes([self.a]), [])


class SummaryTests(unittest.TestCase):
    def test_within_venue_control_is_reported_separately(self) -> None:
        """Two Kalshi routes plus one Polymarket route: the Kalshi-only spread
        is the artifact-free comparison."""
        k1 = _instrument("k1", "kalshi", {"seq:HH"}, {60: 0.40, 120: 0.40}, market_key="K1")
        k2 = _instrument("k2", "kalshi", {"seq:HH"}, {60: 0.41, 120: 0.41}, market_key="K2")
        pm = _instrument("pm", "polymarket", {"seq:HH"}, {60: 0.70, 120: 0.70}, market_key="P")
        members = [k1, k2, pm]
        rows = compare_routes(members)
        summary = summarize_routes(
            members, rows, state_prefix="H", universe_size=3
        )
        self.assertEqual(summary["venue_composition"], "mixed_venue")
        self.assertAlmostEqual(summary["spread_median"], 0.30)
        self.assertAlmostEqual(
            summary["subset_spreads"]["kalshi"]["spread_median"], 0.01
        )
        self.assertNotIn("polymarket", summary["subset_spreads"])

    def test_single_venue_class_is_labelled(self) -> None:
        k1 = _instrument("k1", "kalshi", {"seq:HH"}, {60: 0.40}, market_key="K1")
        k2 = _instrument("k2", "kalshi", {"seq:HH"}, {60: 0.42}, market_key="K2")
        summary = summarize_routes(
            [k1, k2], compare_routes([k1, k2]), state_prefix="", universe_size=3
        )
        self.assertEqual(summary["venue_composition"], "single_venue")
        self.assertEqual(summary["state_prefix"], "(pre-match)")


class StateIntervalTests(unittest.TestCase):
    def test_class_appears_only_in_the_interval_where_masks_coincide(self) -> None:
        prices = {t: 0.5 for t in range(0, 600, 60)}
        under = _instrument("under", "polymarket", {"seq:HH"}, prices,
                            market_key="U", market_type="total_maps")
        map2 = _instrument("map2", "kalshi", {"seq:HH", "seq:AH"}, prices, market_key="M")
        full = frozenset({"seq:HH", "seq:HAH", "seq:HAA", "seq:AH", "seq:AA"})

        results = analyse_state_intervals(
            [under, map2],
            [
                ("", full, (0, 300)),          # pre-match: masks differ
                ("H", UNIVERSE, (300, 600)),   # after map 1: identical
            ],
            minimum_bars=2,
        )
        self.assertEqual([r["state_prefix"] for r in results], ["H"])

    def test_thin_intervals_are_dropped(self) -> None:
        prices = {60: 0.5}
        a = _instrument("a", "kalshi", {"seq:HH"}, prices, market_key="A")
        b = _instrument("b", "kalshi", {"seq:HH"}, prices, market_key="B")
        self.assertEqual(
            analyse_state_intervals([a, b], [("H", UNIVERSE, None)], minimum_bars=10),
            [],
        )


if __name__ == "__main__":
    unittest.main()
