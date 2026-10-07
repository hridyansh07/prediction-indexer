"""Basket fill walker: target pricing, edge walking and stop reasons."""

import random
import unittest

from replay.economic_fills import BasketFill, Fill, walk, walk_basket

P = 10_000  # price atoms per dollar (scale 4)


def concave_value(payout):
    """Payout minus cost minus a 7% p(1-p) fee per unit, scaled to integers.

    Each leg's marginal cost per unit, p + 0.07 p (1 - p), rises with p, so the
    value is concave in steps on best-first asks.
    """
    def value(steps, legs):
        cost = sum(leg.cost for leg in legs)
        fee = sum(7 * quantity * price * (P - price) for leg in legs for price, quantity in leg.taken)
        return 100 * P * (steps * payout - cost) - fee
    return value


def brute_force(ladders, units, value):
    """Smallest step count maximizing value over every reachable step count."""
    limit = min(sum(q for _, q in ladder) // unit for ladder, unit in zip(ladders, units))
    best, best_value = 0, 0
    for steps in range(1, limit + 1):
        legs = tuple(walk(ladder, (steps * unit,))[0] for ladder, unit in zip(ladders, units))
        current = value(steps, legs)
        if current > best_value:
            best, best_value = steps, current
    return best, limit


class WalkBasketTests(unittest.TestCase):
    a = ((3_800, 300), (3_900, 500), (4_600, 1_000))
    b = ((5_500, 200), (5_600, 400), (6_000, 900))

    def test_target_prices_each_leg_at_its_own_ladder(self):
        (fill,) = walk_basket((self.a, self.b), (1, 1), targets=(400,))
        self.assertEqual(fill.legs, (walk(self.a, (400,))[0], walk(self.b, (400,))[0]))
        self.assertEqual((fill.steps, fill.stop, fill.value), (400, "target", None))
        self.assertEqual(fill.legs[0].cost, 300 * 3_800 + 100 * 3_900)

    def test_targets_and_edge_share_one_result_shape(self):
        value = concave_value(P * 1)
        results = walk_basket((self.a, self.b), (1, 1), targets=(100, 5_000), value=value, edge=True)
        self.assertEqual([r.stop for r in results], ["target", "book_exhausted", "edge"])
        self.assertEqual(results[1].steps, 1_500)
        self.assertTrue(all(type(r) is BasketFill for r in results))
        self.assertEqual(results[0].value, value(100, results[0].legs))

    def test_fill_carries_its_book_before_and_after_and_its_price_impact(self):
        (fill,) = walk_basket((self.a, self.b), (1, 1), targets=(400,))
        self.assertEqual(fill.before, ((3_800, 300), (5_500, 200)))
        # Leg 0 took 100 of 500 at 0.39; leg 1 took 200 of 400 at 0.56.
        self.assertEqual(fill.after, ((3_900, 400), (5_600, 200)))
        self.assertEqual(fill.impact_ppm, (100 * 10**6 // 3_800, 100 * 10**6 // 5_500))
        (whole,) = walk_basket((self.a, self.b), (1, 1), targets=(800,))
        self.assertEqual(whole.after, ((4_600, 1_000), (6_000, 700)))
        (empty,) = walk_basket((((3_800, 5),), ((5_500, 5),)), (1, 1), targets=(5,))
        self.assertEqual((empty.after, empty.impact_ppm), ((None, None), (None, None)))
        (none,) = walk_basket((((6_000, 10),), ((5_000, 10),)), (1, 1),
                              value=concave_value(P), edge=True)
        self.assertEqual((none.after, none.impact_ppm), (none.before, (0, 0)))

    def test_edge_stops_where_the_marginal_level_turns_negative(self):
        # With ~3.4c of fees per set, every segment to 600 costs under $1;
        # from 600 the 0.39 + 0.60 segment costs about $1.02.
        (fill,) = walk_basket((self.a, self.b), (1, 1), value=concave_value(P), edge=True)
        self.assertEqual((fill.steps, fill.stop), (600, "edge"))
        self.assertEqual(fill.legs[0].consumed, ((3_800, 300), (3_900, 500)))
        self.assertEqual(fill.legs[0].taken, ((3_800, 300), (3_900, 300)))

    def test_edge_with_no_profitable_first_level_returns_zero_steps(self):
        (fill,) = walk_basket((((6_000, 10),), ((5_000, 10),)), (1, 1),
                              value=concave_value(P), edge=True)
        self.assertEqual((fill.steps, fill.stop, fill.value), (0, "edge", None))
        self.assertEqual(fill.legs, (Fill(0, 0, False, (), ()),) * 2)

    def test_edge_still_rising_at_full_depth_reports_exhaustion(self):
        (fill,) = walk_basket((((1_000, 10),), ((1_000, 7),)), (1, 1), value=concave_value(P), edge=True)
        self.assertEqual((fill.steps, fill.stop), (7, "book_exhausted"))

    def test_level_cap_is_reported_only_when_it_binds(self):
        (capped,) = walk_basket((self.a, self.b), (1, 1), targets=(900,), max_levels=2)
        self.assertEqual((capped.steps, capped.stop), (600, "level_cap"))
        (free,) = walk_basket((self.a[:2], self.b[:2]), (1, 1), targets=(900,), max_levels=2)
        self.assertEqual((free.steps, free.stop), (600, "book_exhausted"))

    def test_units_carry_ratio_and_native_quantity_scales(self):
        # Leg 0 at quantity scale 2, leg 1 at scale 6: one 0.01-contract step
        # is 1 atom on leg 0 and 10,000 atoms on leg 1.
        ladder0 = ((4_000, 250),)            # 2.50 contracts
        ladder1 = ((5_000, 1_234_567),)      # 1.234567 contracts
        (fill,) = walk_basket((ladder0, ladder1), (1, 10_000), targets=(500,))
        self.assertEqual((fill.steps, fill.stop), (123, "book_exhausted"))
        self.assertEqual((fill.legs[0].filled_atoms, fill.legs[1].filled_atoms), (123, 1_230_000))

    def test_straddling_step_is_a_candidate(self):
        # Leg 1's boundary falls inside a step; the step across it is still
        # worth taking, so the maximum is the ceiling side of the boundary.
        ladder0 = ((4_000, 1_000),)
        ladder1 = ((5_000, 55_000), (5_990, 1_000_000))
        value = concave_value(P)
        (fill,) = walk_basket((ladder0, ladder1), (1, 10_000), value=value, edge=True)
        self.assertEqual(fill.steps, brute_force((ladder0, ladder1), (1, 10_000), value)[0])

    def test_unknown_value_stops_the_edge_walk_visibly(self):
        (fill,) = walk_basket((self.a, self.b), (1, 1), value=lambda steps, legs: None, edge=True)
        self.assertEqual((fill.steps, fill.stop, fill.value), (200, "value_unknown", None))

    def test_edge_matches_brute_force_on_random_books(self):
        rng = random.Random(7)
        for _ in range(300):
            ladders, units = [], []
            for unit in (rng.choice((1, 2, 3)), rng.choice((1, 5))):
                price, levels = rng.randrange(2_000, 6_000), []
                for _ in range(rng.randrange(1, 6)):
                    price += rng.randrange(0, 700)
                    levels.append((min(price, P), rng.randrange(1, 40)))
                ladders.append(tuple(levels))
                units.append(unit)
            ladders, units = tuple(ladders), tuple(units)
            value = concave_value(P * rng.choice((1, 2, 3)))
            expected, limit = brute_force(ladders, units, value)
            (fill,) = walk_basket(ladders, units, value=value, edge=True)
            self.assertEqual(fill.steps, expected, (ladders, units))
            if fill.steps:
                self.assertEqual(fill.value, value(fill.steps, fill.legs))
            # Still rising at full depth is exhaustion; anything short of it is the edge.
            self.assertEqual(fill.stop, "book_exhausted" if fill.steps == limit else "edge")
            for leg, unit in zip(fill.legs, units):
                self.assertEqual(leg.filled_atoms, fill.steps * unit)
                self.assertFalse(leg.depth_limited)

    def test_rejects_malformed_requests(self):
        with self.assertRaises(ValueError):
            walk_basket((self.a,), (1, 1), targets=(1,))
        with self.assertRaises(ValueError):
            walk_basket((self.a,), (1,))
        with self.assertRaises(ValueError):
            walk_basket((self.a,), (1,), edge=True)
        with self.assertRaises(ValueError):
            walk_basket((self.a,), (1,), targets=(5, 2))
        with self.assertRaises(ValueError):
            walk_basket((((4_000, 0),),), (1,), targets=(1,))
        with self.assertRaises(TypeError):
            walk_basket((self.a,), (1,), value=lambda s, l: 1.5, edge=True)


if __name__ == "__main__":
    unittest.main()
