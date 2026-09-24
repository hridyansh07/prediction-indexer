import copy
import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.streams import Consumer, Decoder, ProtocolError

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "engine/crates/transport/tests/fixtures/contract.ndjson"
)


def records():
    return [json.loads(line) for line in FIXTURE.read_bytes().splitlines()]


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


def decoder(values):
    return Decoder("contract", "golden", values[0]["body"], 65536)


def assert_contract(test, cuts):
    a, b, t = (
        ("kalshi:A", "outcome"),
        ("kalshi:B", "outcome"),
        ("polymarket:T", "outcome"),
    )
    test.assertEqual(cuts[2].books[a].levels("bid"), ((37, 11), (17, 3)))
    test.assertEqual(cuts[3].books[a].levels("bid"), ((37, 7), (17, 3)))
    test.assertEqual(cuts[3].books[t].levels("ask"), ((81, 9007199254740993),))
    events = cuts[3].body["market_events"]
    test.assertEqual(
        [e["event"]["kind"] for e in events], ["book", "trade", "book", "trade", "book"]
    )
    test.assertEqual(
        [
            e["event"]["value"]["quantity"]["atoms"]
            for e in events
            if e["event"]["kind"] == "trade"
        ],
        ["5", "9"],
    )
    test.assertEqual(
        [e["disposition"] for e in events],
        ["applied", "observed", "applied", "observed", "applied"],
    )
    test.assertEqual(
        [e["reference"]["address"]["event_index"] for e in events],
        ["0", "1", "2", "3", "0"],
    )
    test.assertEqual(cuts[4].body["book_transitions"], ())
    test.assertEqual(
        [e["disposition"] for e in cuts[4].body["market_events"]],
        ["duplicate", "duplicate"],
    )
    test.assertEqual(cuts[4].books[a].levels("bid"), ((37, 7), (17, 3)))
    test.assertEqual(cuts[5].books[a].validity, "unusable")
    test.assertEqual(cuts[5].books[a].reason["kind"], "quantity_underflow")
    test.assertEqual(cuts[5].books[a].levels("bid"), ())
    test.assertEqual(cuts[5].books[b].levels("bid"), ((21, 19),))
    test.assertEqual(cuts[6].books[a].levels("bid"), ((12, 4),))
    test.assertEqual(cuts[-1].kind, "terminal")


class ProtocolTests(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec("redis"), "optional redis SDK")
    def test_consumer_rejects_initial_shape_before_creating_client(self):
        initial = records()[0]["body"]
        for bad in [
            None,
            [],
            {},
            {**initial, "max_entry_bytes": "065536"},
            {**initial, "groups": "fast"},
            {**initial, "extra": "x"},
        ]:
            with self.subTest(initial=bad), patch("redis.Redis.from_url") as connect:
                with self.assertRaises(ProtocolError):
                    Consumer(
                        "redis://localhost",
                        scope="test",
                        run_id="contract",
                        attempt_id="golden",
                        group="fast",
                        initial=bad,
                    )
                connect.assert_not_called()

    @unittest.skipUnless(importlib.util.find_spec("redis"), "optional redis SDK")
    def test_evalsha_falls_back_only_when_script_did_not_execute(self):
        import redis
        from unittest.mock import Mock

        from replay.streams.consumer import SCRIPT, SCRIPT_SHA

        c = Consumer.__new__(Consumer)
        c._keys = ["stream", "state"]
        c._redis = Mock()
        c._redis.evalsha.return_value = "cached"
        self.assertEqual(c._eval("check"), "cached")
        c._redis.evalsha.assert_called_once_with(
            SCRIPT_SHA, 2, "stream", "state", "check"
        )
        c._redis.eval.assert_not_called()
        c._redis.evalsha.side_effect = redis.exceptions.NoScriptError()
        c._redis.eval.return_value = "loaded"
        self.assertEqual(c._eval("check"), "loaded")
        c._redis.eval.assert_called_once_with(SCRIPT, 2, "stream", "state", "check")
        for error in [
            redis.TimeoutError(),
            redis.ResponseError("REPLAY membership"),
            redis.exceptions.OutOfMemoryError(),
        ]:
            c._redis.reset_mock()
            c._redis.evalsha.side_effect = error
            with self.assertRaises(type(error)):
                c._eval("check")
            c._redis.eval.assert_not_called()

    def test_rust_golden_exact_state_events_and_owned_cuts(self):
        values = records()
        d = decoder(values)
        cuts = [d.apply(encoded(r)) for r in values]
        d.finish()
        assert_contract(self, cuts)
        with self.assertRaises(TypeError):
            cuts[2].books[("kalshi:A", "outcome")] = None
        with self.assertRaises(TypeError):
            cuts[2].body["book_transitions"][0]["dependency"]["epoch"] = "wrong"
        self.assertEqual(cuts[2].books[("kalshi:A", "outcome")].revision, 1)

    def test_bad_second_transition_exposes_neither_transition(self):
        values = records()
        d = decoder(values)
        for r in values[:3]:
            d.apply(encoded(r))
        prior = d.books
        bad = copy.deepcopy(values[3])
        bad["body"]["book_transitions"][1]["previous_revision"] = "4"
        with self.assertRaisesRegex(ProtocolError, "revision"):
            d.apply(encoded(bad))
        self.assertEqual(d.books, prior)
        self.assertEqual(
            d.books[("kalshi:A", "outcome")].levels("bid"), ((37, 11), (17, 3))
        )
        with self.assertRaises(ProtocolError):
            d.apply(encoded(values[3]))

    def test_duplicate_gap_unknown_schema_malformed_numeric_and_tail(self):
        values = records()
        bad_values = []
        for field, value in [
            ("sequence", "4"),
            ("sequence", "2"),
            ("version", "2"),
            ("extra", None),
        ]:
            bad = copy.deepcopy(values[3])
            bad[field] = value
            bad_values.append(bad)
        for n in [True, 7, 7.0, "07", "+7", "-1", "1e3", "18446744073709551616", "NaN"]:
            bad = copy.deepcopy(values[3])
            bad["body"]["book_transitions"][0]["decision"]["operations"][0]["price"][
                "atoms"
            ] = n
            bad_values.append(bad)
        bad = copy.deepcopy(values[3])
        bad["body"]["market_events"][0]["event"]["value"]["value"]["extra"] = 1
        bad_values.append(bad)
        for bad in bad_values:
            with self.subTest(bad=bad):
                d = decoder(values)
                for r in values[:3]:
                    d.apply(encoded(r))
                with self.assertRaises(ProtocolError):
                    d.apply(encoded(bad))
        d = decoder(values)
        for r in values[:-1]:
            d.apply(encoded(r))
        with self.assertRaisesRegex(ProtocolError, "missing terminal"):
            d.finish()
        terminal = copy.deepcopy(values[-1])
        terminal["body"]["cuts"] = "5"
        with self.assertRaisesRegex(ProtocolError, "terminal count"):
            d.apply(encoded(terminal))
        for raw in [b'{"version":"1","version":"1"}', b"[", b"NaN"]:
            with self.assertRaises(ProtocolError):
                decoder(values).apply(raw)

    def test_initial_binding_entry_limit_and_no_numeric_json_anywhere(self):
        values = records()

        def walk(v):
            self.assertNotIsInstance(v, (int, float))
            if isinstance(v, dict):
                for child in v.values():
                    walk(child)
            elif isinstance(v, list):
                for child in v:
                    walk(child)

        for value in values:
            walk(value)
        changed = copy.deepcopy(values[0])
        changed["body"]["plans"][0]["price_scale"] = "3"
        with self.assertRaisesRegex(ProtocolError, "initial binding"):
            decoder(values).apply(encoded(changed))
        with self.assertRaisesRegex(ProtocolError, "entry limit"):
            decoder(values).apply(b" " * 65537)

    def test_retained_memory_does_not_grow_with_cut_count(self):
        import gc
        import tracemalloc

        values = records()
        d = decoder(values)
        for r in values[:3]:
            d.apply(encoded(r))
        change = copy.deepcopy(values[3])
        change["body"]["market_events"] = []
        change["body"]["book_transitions"] = change["body"]["book_transitions"][:1]
        transition = change["body"]["book_transitions"][0]
        # A bounded level set, many valid authoritative Set decisions.
        operation = transition["decision"]["operations"][0]
        operation["change"] = {"kind": "set", "value": operation["change"]["value"]}
        transition["decision"]["operations"] = [operation]
        tracemalloc.start()
        try:
            retained = []
            for count in range(1, 1201):
                change["sequence"] = str(count + 2)
                transition["previous_revision"] = str(count)
                transition["revision"] = str(count + 1)
                d.apply(encoded(change))
                if count in (100, 1200):
                    gc.collect()
                    retained.append(tracemalloc.get_traced_memory()[0])
            self.assertLess(retained[1] - retained[0], 100_000)
            self.assertEqual(
                d.books[("kalshi:A", "outcome")].levels("bid"), ((37, 3), (17, 3))
            )
        finally:
            tracemalloc.stop()


if __name__ == "__main__":
    unittest.main()
