from __future__ import annotations

import unittest

from analysis.partition_sum import (
    PARTITION_CROSS_VENUE,
    PARTITION_KALSHI_EVENT,
    BookSnapshot,
    FeeSchedule,
    InstrumentSeries,
    Level,
    PartitionDefinition,
    PartitionRepresentation,
    bar_snapshots,
    build_partition_definitions,
    fee_for_fills,
    fee_schedule_at,
    measure_partition,
    normalize_kalshi_snapshot,
    polymarket_complement_diagnostics,
    skew_bucket,
    walk_ladder,
)
from analysis.partition_pipeline import determine_terminal_status


class VwapTests(unittest.TestCase):
    def test_exact_fill_and_level_boundary(self) -> None:
        levels = [Level(0.40, 5), Level(0.50, 5)]

        first = walk_ladder(levels, 5)
        self.assertFalse(first.depth_limited)
        self.assertEqual(first.filled_contracts, 5)
        self.assertAlmostEqual(first.vwap, 0.40)

        both = walk_ladder(levels, 10)
        self.assertFalse(both.depth_limited)
        self.assertEqual(both.filled_contracts, 10)
        self.assertAlmostEqual(both.vwap, 0.45)

    def test_partial_empty_and_single_level(self) -> None:
        partial = walk_ladder([Level(0.42, 3)], 5)
        self.assertTrue(partial.depth_limited)
        self.assertEqual(partial.filled_contracts, 3)
        self.assertAlmostEqual(partial.vwap, 0.42)

        empty = walk_ladder([], 5)
        self.assertTrue(empty.depth_limited)
        self.assertEqual(empty.filled_contracts, 0)
        self.assertIsNone(empty.vwap)

    def test_share_matched_is_not_dollar_matched(self) -> None:
        first = walk_ladder([Level(0.25, 100)], 10)
        second = walk_ladder([Level(0.65, 100)], 10)
        share_matched_cost = first.cost + second.cost

        fixed_dollars = 9
        first_contracts = fixed_dollars / 2 / 0.25
        second_contracts = fixed_dollars / 2 / 0.65
        self.assertAlmostEqual(share_matched_cost, 9)
        self.assertNotAlmostEqual(first_contracts, second_contracts)


class NormalizationAndTimingTests(unittest.TestCase):
    def test_kalshi_yes_and_no_asks_are_complemented(self) -> None:
        yes, no = normalize_kalshi_snapshot(
            {
                "timestamp": 1,
                "yes_bids": [{"price": "0.40", "size": 10}],
                "no_bids": [{"price": "0.55", "size": 7}],
            }
        )

        self.assertEqual(yes.bids, (Level(0.40, 10),))
        self.assertAlmostEqual(yes.asks[0].price, 0.45)
        self.assertEqual(yes.asks[0].size, 7)
        self.assertEqual(no.bids, (Level(0.55, 7),))
        self.assertAlmostEqual(no.asks[0].price, 0.60)

    def test_bar_selection_never_looks_ahead_and_rejects_stale(self) -> None:
        snapshots = [
            BookSnapshot(59_000, (), (Level(0.4, 1),)),
            BookSnapshot(61_000, (), (Level(0.5, 1),)),
        ]
        bars = bar_snapshots(
            snapshots,
            bar_seconds=60,
            max_age_seconds=60,
        )

        self.assertEqual(bars[60_000].timestamp_ms, 59_000)
        self.assertEqual(bars[120_000].timestamp_ms, 61_000)
        stale = bar_snapshots(
            [BookSnapshot(1, (), ())],
            bar_seconds=60,
            max_age_seconds=30,
        )
        self.assertEqual(stale, {})

    def test_skew_buckets_flag_45_seconds(self) -> None:
        self.assertEqual(skew_bucket(4.999, [5, 15, 60]), "lt_5s")
        self.assertEqual(skew_bucket(45, [5, 15, 60]), "15_to_60s")
        self.assertEqual(skew_bucket(61, [5, 15, 60]), "gt_60s")

    def test_polymarket_complement_pair(self) -> None:
        first = [
            BookSnapshot(
                1_000,
                bids=(Level(0.40, 5),),
                asks=(Level(0.45, 7),),
            )
        ]
        second = [
            BookSnapshot(
                1_045,
                bids=(Level(0.55, 7),),
                asks=(Level(0.60, 5),),
            )
        ]

        diagnostic = polymarket_complement_diagnostics(
            first,
            second,
            pair_tolerance_ms=100,
            size_tolerance=1e-6,
        )

        self.assertEqual(diagnostic["matched"], 1)
        self.assertEqual(diagnostic["match_rate"], 1)
        self.assertEqual(diagnostic["maximum_pair_skew_ms"], 45)


class FeeAndEconomicsTests(unittest.TestCase):
    def test_quadratic_fee_curve(self) -> None:
        schedule = FeeSchedule(
            venue="kalshi",
            fee_type="quadratic",
            rate=0.07,
            normal_rounding_dollars=0,
            conservative_rounding_dollars=0,
        )
        low = fee_for_fills(schedule, [(0.02, 1)])[0]
        middle = fee_for_fills(schedule, [(0.50, 1)])[0]
        high = fee_for_fills(schedule, [(0.98, 1)])[0]

        self.assertAlmostEqual(low, high)
        self.assertGreater(middle, low)
        self.assertAlmostEqual(middle, 0.0175)

    def test_known_two_cent_gap_is_recovered(self) -> None:
        timestamp = 60_000
        instruments = {
            "a": InstrumentSeries(
                instrument_id="a",
                venue="polymarket",
                event_key="event",
                market_id="a",
                outcome="A",
                position="token",
                asset_id="a",
                fee_key="a",
                end_time_ms=120_000,
                snapshots=[
                    BookSnapshot(
                        timestamp,
                        bids=(Level(0.48, 100),),
                        asks=(Level(0.49, 100),),
                    )
                ],
            ),
            "b": InstrumentSeries(
                instrument_id="b",
                venue="polymarket",
                event_key="event",
                market_id="b",
                outcome="B",
                position="token",
                asset_id="b",
                fee_key="b",
                end_time_ms=120_000,
                snapshots=[
                    BookSnapshot(
                        timestamp,
                        bids=(Level(0.48, 100),),
                        asks=(Level(0.49, 100),),
                    )
                ],
            ),
        }
        definition = PartitionDefinition(
            economic_partition_id="partition",
            partition_class="test",
            event_key="event",
            partition_status="test",
            rules_hashes=(),
            resolution_sources=(),
            representations=(PartitionRepresentation("route", ("a", "b")),),
        )
        zero_fee = FeeSchedule("polymarket", "none", 0)
        rows = measure_partition(
            definition,
            instruments=instruments,
            bars={
                "a": {timestamp: instruments["a"].snapshots[0]},
                "b": {timestamp: instruments["b"].snapshots[0]},
            },
            fee_schedules={"a": zero_fee, "b": zero_fee},
            sizes=[10],
            skew_edges=[5, 15, 60],
        )
        long_row = next(row for row in rows if row["direction"] == "long")

        self.assertTrue(long_row["valid"])
        self.assertTrue(long_row["selected_best_representation"])
        self.assertAlmostEqual(long_row["gross_gap_per_contract"], 0.02)
        self.assertAlmostEqual(long_row["profit_conservative_dollars"], 0.20)

    def test_fees_can_turn_gap_negative(self) -> None:
        schedule = FeeSchedule(
            venue="kalshi",
            fee_type="quadratic",
            rate=0.07,
            normal_rounding_dollars=0.0001,
            conservative_rounding_dollars=0.01,
        )
        _, _, conservative = fee_for_fills(schedule, [(0.49, 1)])
        self.assertEqual(conservative, 0.02)

    def test_historical_fee_schedule_selection(self) -> None:
        timeline = (
            FeeSchedule(
                venue="kalshi",
                fee_type="quadratic",
                rate=0.07,
                multiplier=1,
                effective_at_ms=1_000,
            ),
            FeeSchedule(
                venue="kalshi",
                fee_type="quadratic",
                rate=0.07,
                multiplier=2,
                effective_at_ms=2_000,
            ),
        )

        self.assertIsNone(fee_schedule_at(timeline, 999))
        self.assertEqual(fee_schedule_at(timeline, 1_999).multiplier, 1)
        self.assertEqual(fee_schedule_at(timeline, 2_000).multiplier, 2)


class PartitionConstructionTests(unittest.TestCase):
    @staticmethod
    def _series(instrument_id: str) -> InstrumentSeries:
        venue, market_id, position = instrument_id.split(":")
        return InstrumentSeries(
            instrument_id=instrument_id,
            venue=venue,
            event_key="event",
            market_id=market_id,
            outcome=market_id,
            position=position,
            asset_id=None,
            fee_key=venue,
            end_time_ms=None,
            snapshots=[],
        )

    def test_alternative_kalshi_and_cross_venue_routes_are_preserved(self) -> None:
        manifest = {
            "matches": [
                {
                    "event_key": "event",
                    "history_targets": [
                        {
                            "venue": "kalshi",
                            "market_id": "A",
                            "outcome": "A",
                        },
                        {
                            "venue": "kalshi",
                            "market_id": "B",
                            "outcome": "B",
                        },
                        {
                            "venue": "polymarket",
                            "market_id": "C",
                            "outcome_tokens": [
                                {"asset_id": "PA", "outcome": "A"},
                                {"asset_id": "PB", "outcome": "B"},
                            ],
                        },
                    ],
                    "kalshi": {
                        "mutually_exclusive": True,
                        "market_ids": ["A", "B"],
                        "rules_hashes": ["ra", "rb"],
                    },
                    "polymarket": {
                        "condition_id": "C",
                        "outcome_tokens": [
                            {"asset_id": "PA", "outcome": "A"},
                            {"asset_id": "PB", "outcome": "B"},
                        ],
                        "rules_hash": "rp",
                        "resolution_source": "source",
                    },
                }
            ]
        }
        instrument_ids = [
            "kalshi:A:yes",
            "kalshi:A:no",
            "kalshi:B:yes",
            "kalshi:B:no",
        ]
        instruments = {
            instrument_id: self._series(instrument_id)
            for instrument_id in instrument_ids
        }
        for asset_id, outcome in (("PA", "A"), ("PB", "B")):
            instrument_id = f"polymarket:C:{asset_id}"
            instruments[instrument_id] = InstrumentSeries(
                instrument_id=instrument_id,
                venue="polymarket",
                event_key="event",
                market_id="C",
                outcome=outcome,
                position="token",
                asset_id=asset_id,
                fee_key="polymarket",
                end_time_ms=None,
                snapshots=[],
            )

        definitions = build_partition_definitions(manifest, instruments)
        same_venue = next(
            item
            for item in definitions
            if item.partition_class == PARTITION_KALSHI_EVENT
        )
        cross_venue = next(
            item
            for item in definitions
            if item.partition_class == PARTITION_CROSS_VENUE
        )

        self.assertEqual(len(same_venue.representations), 2)
        self.assertEqual(len(cross_venue.representations), 4)

    def _three_way_manifest(self) -> dict:
        """A World Cup regulation-time moneyline: home / draw / away."""
        outcomes = ("ESP", "TIE", "ARG")
        return {
            "matches": [
                {
                    "event_key": "wc-2026-07-19-esp-arg",
                    "history_targets": [
                        {"venue": "kalshi", "market_id": name, "outcome": name}
                        for name in outcomes
                    ],
                    "kalshi": {
                        "mutually_exclusive": True,
                        "market_ids": list(outcomes),
                        "rules_hashes": ["r1", "r2", "r3"],
                    },
                }
            ]
        }

    def test_three_way_event_yields_one_all_yes_partition(self) -> None:
        manifest = self._three_way_manifest()
        instruments = {
            f"kalshi:{name}:{position}": self._series(f"kalshi:{name}:{position}")
            for name in ("ESP", "TIE", "ARG")
            for position in ("yes", "no")
        }

        definitions = build_partition_definitions(manifest, instruments)
        event = next(
            item
            for item in definitions
            if item.partition_class == PARTITION_KALSHI_EVENT
        )

        # One contract of each YES leg pays exactly $1. The all-NO basket would
        # pay $2 on a three-way event, so it must not be offered.
        self.assertEqual(len(event.representations), 1)
        representation = event.representations[0]
        self.assertTrue(representation.representation_id.endswith(":all-yes"))
        self.assertEqual(
            representation.leg_instrument_ids,
            ("kalshi:ESP:yes", "kalshi:TIE:yes", "kalshi:ARG:yes"),
        )

    def test_two_way_event_still_offers_both_routes(self) -> None:
        """The n=2 case keeps the all-NO route, where NO_A is just YES_B."""
        manifest = self._three_way_manifest()
        match = manifest["matches"][0]
        match["history_targets"] = match["history_targets"][:2]
        match["kalshi"]["market_ids"] = ["ESP", "TIE"]
        instruments = {
            f"kalshi:{name}:{position}": self._series(f"kalshi:{name}:{position}")
            for name in ("ESP", "TIE")
            for position in ("yes", "no")
        }

        definitions = build_partition_definitions(manifest, instruments)
        event = next(
            item
            for item in definitions
            if item.partition_class == PARTITION_KALSHI_EVENT
        )
        suffixes = sorted(
            r.representation_id.rsplit(":", 1)[-1] for r in event.representations
        )
        self.assertEqual(suffixes, ["all-no", "all-yes"])

    def test_missing_leg_drops_the_partition(self) -> None:
        """A partition is only valid if every outcome leg has data."""
        manifest = self._three_way_manifest()
        instruments = {
            f"kalshi:{name}:yes": self._series(f"kalshi:{name}:yes")
            for name in ("ESP", "TIE")  # ARG absent
        }

        definitions = build_partition_definitions(manifest, instruments)
        self.assertEqual(
            [d for d in definitions if d.partition_class == PARTITION_KALSHI_EVENT],
            [],
        )


class GateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = {
            "minimum_passing_events": 2,
            "same_venue_class": PARTITION_KALSHI_EVENT,
            "cross_venue_class": PARTITION_CROSS_VENUE,
        }
        self.validation = {"valid": True}

    @staticmethod
    def _summary(
        partition_class: str,
        *,
        qualifies: bool,
        passes: bool,
    ) -> dict:
        return {
            "gate_events": [
                {
                    "partition_class": partition_class,
                    "event_key": f"event-{index}",
                    "qualifies_sample_size": qualifies,
                    "passes_rate": passes,
                }
                for index in range(2)
            ]
        }

    def test_same_venue_pass_takes_precedence(self) -> None:
        status, _ = determine_terminal_status(
            self._summary(
                PARTITION_KALSHI_EVENT,
                qualifies=True,
                passes=True,
            ),
            self.validation,
            [],
            self.gate,
        )
        self.assertEqual(status, "EWC_ECONOMIC_PASS")

    def test_cross_venue_pass_is_conditional(self) -> None:
        status, _ = determine_terminal_status(
            self._summary(
                PARTITION_CROSS_VENUE,
                qualifies=True,
                passes=True,
            ),
            self.validation,
            [],
            self.gate,
        )
        self.assertEqual(status, "EWC_CONDITIONAL_CROSS_VENUE_PASS")

    def test_no_signal_and_insufficient_are_distinct(self) -> None:
        no_signal, _ = determine_terminal_status(
            self._summary(
                PARTITION_KALSHI_EVENT,
                qualifies=True,
                passes=False,
            ),
            self.validation,
            [],
            self.gate,
        )
        insufficient, _ = determine_terminal_status(
            self._summary(
                PARTITION_KALSHI_EVENT,
                qualifies=False,
                passes=False,
            ),
            self.validation,
            [],
            self.gate,
        )
        self.assertEqual(no_signal, "EWC_NO_SIGNAL")
        self.assertEqual(insufficient, "INSUFFICIENT_LOW_SKEW_DATA")

    def test_invalid_data_blocks_gate(self) -> None:
        status, _ = determine_terminal_status(
            self._summary(
                PARTITION_KALSHI_EVENT,
                qualifies=True,
                passes=True,
            ),
            {"valid": False},
            [],
            self.gate,
        )
        self.assertEqual(status, "DATA_INVALID")


if __name__ == "__main__":
    unittest.main()
