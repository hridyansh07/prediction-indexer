from __future__ import annotations

import unittest
from decimal import Decimal

from replay.catalog import FeeTerms
from replay.economics import (
    conservative_fee,
    solve_cover_lp,
    walk_ladder,
)


class VwapTests(unittest.TestCase):
    def test_exact_boundary_and_partial_depth_are_not_conflated(self) -> None:
        levels = (
            (Decimal("0.40"), Decimal(5)),
            (Decimal("0.50"), Decimal(5)),
        )

        exact = walk_ladder(levels, Decimal(10))
        partial = walk_ladder(levels, Decimal(11))

        self.assertFalse(exact.depth_limited)
        self.assertEqual(exact.vwap, Decimal("0.45"))
        self.assertTrue(partial.depth_limited)
        self.assertEqual(partial.filled, Decimal(10))


class FeeTests(unittest.TestCase):
    def test_conservative_curve_is_largest_near_even_money(self) -> None:
        terms = FeeTerms(
            fee_type="curve",
            rate=Decimal("0.07"),
            exponent=Decimal(1),
            taker_only=True,
            source_record_hash="hash",
        )
        low = conservative_fee(
            terms,
            walk_ladder(((Decimal("0.02"), Decimal(1)),), Decimal(1)),
        )
        middle = conservative_fee(
            terms,
            walk_ladder(((Decimal("0.50"), Decimal(1)),), Decimal(1)),
        )
        high = conservative_fee(
            terms,
            walk_ladder(((Decimal("0.98"), Decimal(1)),), Decimal(1)),
        )

        self.assertEqual(low, high)
        self.assertGreater(middle, low)


class CoverLpTests(unittest.TestCase):
    def test_binary_partition_recovers_the_exact_two_cent_gap(self) -> None:
        solution = solve_cover_lp(
            ("A", "B"),
            (frozenset({"A"}), frozenset({"B"})),
            (Decimal("0.49"), Decimal("0.49")),
        )

        self.assertTrue(solution.feasible)
        self.assertEqual(solution.minimum_cost, Decimal("0.98"))
        self.assertEqual(solution.weights, (Decimal(1), Decimal(1)))

    def test_lp_selects_the_cheapest_symbolic_cover(self) -> None:
        solution = solve_cover_lp(
            ("A", "B"),
            (
                frozenset({"A", "B"}),
                frozenset({"A"}),
                frozenset({"B"}),
            ),
            (Decimal("0.90"), Decimal("0.40"), Decimal("0.40")),
        )

        self.assertEqual(solution.minimum_cost, Decimal("0.80"))
        self.assertEqual(solution.weights, (Decimal(0), Decimal(1), Decimal(1)))


if __name__ == "__main__":
    unittest.main()
