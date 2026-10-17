"""Probe money units and pinned terms through production strategy readers.

Legacy policies explicitly opt into probes, using production requirements,
evaluators, FeeBridge, default delays and readers without retention overrides.
"""

import copy
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.economic_sdk import Strategy
from replay.preparation import encoded
from replay.streams.protocol import ProtocolError
from replay.tests.economic_scenarios import M, ladder
from replay.tests import test_execution_delay_probes as probe_fixtures
from replay.tests.test_execution_delay_probes import DelayedPair, probes
from replay.tests.test_economic_sdk_fills import LEG1


MS = 1_000_000
UNIT = 10**36
TWO = {"name": "two", "role": "governs", "target_contracts": "2"}


def milliseconds(original):
    def config(*args, **kwargs):
        result = original(*args, **kwargs)
        for row in (result, *result["occurrences"]):
            for field in ("start_ns", "end_ns"):
                row[field] = str(int(row[field]) * MS)
        return result
    return config


class ProbeIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def cross(self, *, observed=False):
        import replay.tests.test_cross_venue_arbitrage as fixtures
        original = fixtures.fill_policy_v3

        def policy(*args, **kwargs):
            result = original(*args, **kwargs)
            result["fills"]["probes"] = probes(sizings=["two"],
                unknown_taker_delay_ms={"kalshi": 0, "polymarket": 0})
            return result

        document = fixtures.document()
        if observed:
            document["version"] = 2
            for market in document["markets"]:
                market["execution_terms"] = {"status": "unavailable", "changes": []}
                if market["market_id"] == "polymarket:series":
                    market["execution_terms"] = {"status": "observed", "changes": [
                        {"from": "1970-01-01T00:00:00.011Z", "taker_delay_ms": 1}]}
        with patch.object(fixtures, "config", milliseconds(fixtures.config)), \
             patch.object(fixtures, "fill_policy_v3", policy):
            h = fixtures.Harness(self.root, fills=(TWO,), doc=document)
        self.addCleanup(h.close)
        h.window(end=40 * MS)
        for orientation in ("outcome", "complement"):
            ladder(h, 12 * MS, "kalshi:series", orientation,
                   bids=((60, 2),), asks=((70, 2),))
        ladder(h, 12 * MS, "polymarket:123", bids=((100, 2*M),), asks=((650, 2*M),))
        ladder(h, 12 * MS, "polymarket:987", bids=((100, 2*M),), asks=((500, 2*M),))
        ladder(h, 15 * MS, "polymarket:987", bids=((100, 2*M),), asks=((500, M),))
        return h

    def rehash_probes(self, h, result, rows):
        raw = b"".join(encoded(row) + b"\n" for row in rows)
        (h.output / "probes.ndjson").write_bytes(raw)
        manifest = copy.deepcopy(result["manifest"])
        manifest["files"]["probes.ndjson"] = {"sha256": hashlib.sha256(raw).hexdigest(),
            "byte_length": len(raw), "records": len(rows)}
        return manifest

    def test_cross_venue_mixed_native_scales_normalize_loss_and_reader_keeps_snapshot(self):
        h = self.cross()
        result = h.finish()  # Both the aggregate and provisional production readers.
        (row,) = h.records("probes.ndjson")
        self.assertEqual((row["status"], row["value"], row["leg_loss_gross"]),
                         ("partial", str(UNIT // 10), str(UNIT // 10)))
        self.assertEqual(row["legs"][0]["cost"], "80")  # Native Kalshi cents stay native.
        self.assertEqual(row["legs"][0]["exit_proceeds"], "30")
        self.assertEqual([leg["delay_source"] for leg in row["legs"]], ["default", "default"])

        # Rehashing the historical raw-cents answer must not bypass arithmetic.
        from replay.strategies.cross_venue_arbitrage.output import validate_content
        row["leg_loss_gross"] = "10"
        manifest = self.rehash_probes(h, result, [row])
        with self.assertRaisesRegex(ProtocolError, "probe gross loss"):
            validate_content(h.output, h.strategy.snapshot, manifest, h.strategy.strategy.bridge)

    def test_production_reader_recomputes_pinned_terms_delay(self):
        from replay.strategies.cross_venue_arbitrage.output import validate_content
        h = self.cross(observed=True)
        result = h.finish()
        (row,) = h.records("probes.ndjson")
        self.assertEqual([(leg["delay_source"], leg["venue_delay_ms"], leg["arrival_ns"])
                          for leg in row["legs"]],
                         [("default", 0, str(17 * MS)), ("terms", 1, str(18 * MS))])
        row["legs"][1]["delay_source"] = "default"
        manifest = self.rehash_probes(h, result, [row])
        with self.assertRaisesRegex(ProtocolError, "probe venue delay"):
            validate_content(h.output, h.strategy.snapshot, manifest, h.strategy.strategy.bridge)

    def test_multi_market_probe_reader_keeps_snapshot_and_values_native_loss(self):
        import replay.tests.test_same_venue_multi_market as fixtures
        policy = fixtures.policy(TWO)
        policy["fills"]["probes"] = probes(sizings=["two"],
            unknown_taker_delay_ms={"kalshi": 0, "polymarket": 0})
        with patch.object(fixtures, "config", milliseconds(fixtures.config)):
            h = fixtures.Harness(self.root, value=policy)
        self.addCleanup(h.close)
        h.window(end=40 * MS)
        for market in ("series", "beta"):
            for orientation in ("outcome", "complement"):
                ladder(h, 12 * MS, "kalshi:" + market, orientation,
                       bids=((60, 2),), asks=((70, 2),))
        ladder(h, 15 * MS, "kalshi:beta", "complement", bids=((60, 1),), asks=((70, 2),))
        h.finish()
        rows = [row for row in h.records("probes.ndjson") if row["status"] == "partial"]
        self.assertTrue(rows)
        self.assertTrue(all(row["leg_loss_gross"] == str(UNIT // 10) for row in rows))

    def test_sdk_uses_explicit_strategy_loss_units(self):
        class E36Pair(DelayedPair):
            def fill_value(self, entity, steps, legs, time):
                result = super().fill_value(entity, steps, legs, time)
                return None if result is None else result * 10**27

            def fill_loss(self, entity, native_losses, time):
                return sum(native_losses) * 10**27

        case = probe_fixtures.ProbeTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        h = case.harness(probes(sizings=["two"]), TWO, strategy_class=E36Pair)
        case.open(h)
        case.book(h, 15, LEG1, (500, M))
        h.finish()
        self.assertEqual(h.rows("probes.ndjson")[0]["leg_loss_gross"], str(UNIT // 10))

    def test_sdk_converts_signed_sold_excess_without_clamping(self):
        class E36Pair(DelayedPair):
            def fill_value(self, entity, steps, legs, time):
                return super().fill_value(entity, steps, legs, time) * 10**27

            def fill_loss(self, entity, native_losses, time):
                return sum(native_losses) * 10**27

        case = probe_fixtures.ProbeTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        for repurchase, expected in ((750, UNIT // 10), (600, -UNIT // 20)):
            with self.subTest(repurchase=repurchase):
                h = case.harness(probes(sizings=["two"]), TWO, strategy_class=E36Pair, side="sell")
                case.book(h, 12, "polymarket:123", (repurchase, 2*M), bids=((700, M), (650, M)))
                case.book(h, 12, LEG1, (800, 2*M), bids=((550, 2*M),))
                case.book(h, 15, LEG1, (800, 2*M), bids=((550, M),))
                h.finish()
                self.assertEqual(h.rows("probes.ndjson")[0]["leg_loss_gross"], str(expected))

    def test_sdk_requires_explicit_integer_loss_valuation(self):
        case = probe_fixtures.ProbeTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        for bad in (None, True, 1.0, "1"):
            with self.subTest(result=bad):
                class BadLoss(DelayedPair):
                    def fill_loss(self, entity, native_losses, time):
                        return bad
                h = case.harness(strategy_class=BadLoss)
                case.open(h)
                with self.assertRaisesRegex(ProtocolError, "probe loss value"):
                    h.finish()

        class MissingLoss(DelayedPair):
            fill_loss = Strategy.fill_loss
        h = case.harness(strategy_class=MissingLoss)
        case.open(h)
        with self.assertRaisesRegex(ProtocolError, "probe loss valuation hook required"):
            h.finish()


if __name__ == "__main__":
    unittest.main()
