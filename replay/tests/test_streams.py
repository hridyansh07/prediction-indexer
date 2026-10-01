import copy
import hashlib
import importlib.util
import json
import random
import unittest
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from unittest.mock import patch

from replay.streams import Consumer, Decoder, ProtocolError
from replay.streams.protocol import books_sha256

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


def thaw(value):
    if isinstance(value, (dict, MappingProxyType)):
        return {k: thaw(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [thaw(v) for v in value]
    return value


class Held:
    """What a hook must copy to keep a book past its own invocation."""

    def __init__(self, book):
        self.revision, self.validity = book.revision, book.validity
        self.reason = thaw(book.reason)
        self._levels = {side: book.levels(side) for side in ("bid", "ask")}

    def levels(self, side):
        return self._levels[side]


def held(cut):
    """Explicit copy of a cut under the zero-copy hook contract."""
    return SimpleNamespace(
        sequence=cut.sequence,
        kind=cut.kind,
        body=thaw(cut.body),
        books={k: Held(book) for k, book in cut.books.items()},
    )


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
    test.assertEqual(cuts[4].body["book_transitions"], [])
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


def apply_all(test, values):
    d = decoder(values)
    cuts = [held(d.apply(encoded(r))) for r in values]
    d.finish()
    return d, cuts


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
    def test_default_batch_fills_the_byte_budget_at_the_entry_cap(self):
        initial = records()[0]["body"]
        with patch("redis.Redis.from_url"), patch.object(Consumer, "_eval"):
            c = Consumer(
                "redis://localhost",
                scope="test",
                run_id="contract",
                attempt_id="golden",
                group="fast",
                initial={**initial, "max_entry_bytes": "1048576"},
            )
            self.assertEqual((c._batch_entries, c._batch_bytes), (128, 128 << 20))
            c = Consumer(
                "redis://localhost",
                scope="test",
                run_id="contract",
                attempt_id="golden",
                group="fast",
                initial=initial,
            )
            self.assertEqual(c._batch_entries, 1024)
            for entries, size in ((1025, 128 << 20), (2, 65536), (0, 128 << 20)):
                with self.assertRaises(ProtocolError):
                    Consumer(
                        "redis://localhost",
                        scope="test",
                        run_id="contract",
                        attempt_id="golden",
                        group="fast",
                        initial=initial,
                        batch_entries=entries,
                        batch_bytes=size,
                    )

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

    def test_rust_golden_exact_state_events_and_held_copies(self):
        values = records()
        _, cuts = apply_all(self, values)
        assert_contract(self, cuts)

    def test_books_are_live_in_place_and_read_only_mappings(self):
        values = records()
        d = decoder(values)
        cuts = [d.apply(encoded(r)) for r in values]
        d.finish()
        a = ("kalshi:A", "outcome")
        # Zero-copy contract: every cut exposes the SAME live books.
        self.assertIs(cuts[2].books[a], cuts[-1].books[a])
        self.assertEqual(cuts[2].books[a].levels("bid"), ((12, 4),))
        self.assertEqual(cuts[2].books[a].revision, 4)
        with self.assertRaises(TypeError):
            cuts[2].books[a] = None
        # Control records are frozen; cut bodies are the parsed wire body.
        with self.assertRaises(TypeError):
            cuts[0].body["groups"] = ()
        self.assertIsInstance(cuts[2].body, dict)

    def test_terminal_digest_definition_matches_rust_golden(self):
        values = records()
        d, _ = apply_all(self, values)
        lines = (
            "kalshi:A\toutcome\t4\tusable\t12:4\t\n"
            "kalshi:B\toutcome\t2\tusable\t21:19\t\n"
            "polymarket:T\toutcome\t1\tusable\t\t81:9007199254740993\n"
        )
        expected = hashlib.sha256(lines.encode()).hexdigest()
        self.assertEqual(books_sha256(d.books), expected)
        self.assertEqual(values[-1]["body"]["books_sha256"], expected)

    def test_terminal_digest_mismatch_fails_the_attempt(self):
        values = records()
        for corrupt in (
            # Wrong final quantity, otherwise fully consistent revisions.
            lambda v: v[5]["body"]["book_transitions"][1]["decision"]["operations"][
                0
            ]["change"]["value"].update(atoms="18"),
            lambda v: v[6]["body"]["book_transitions"][0]["decision"].update(
                bids=[["12", "5"]]
            ),
            lambda v: v[-1]["body"].update(books_sha256="0" * 64),
        ):
            bad = copy.deepcopy(values)
            corrupt(bad)
            d = decoder(bad)
            for r in bad[:-1]:
                d.apply(encoded(r))
            with self.assertRaisesRegex(ProtocolError, "terminal book digest"):
                d.apply(encoded(bad[-1]))
            self.assertTrue(d.poisoned)
            with self.assertRaisesRegex(ProtocolError, "missing terminal"):
                d.finish()
        for extra in ({}, {"cuts": "7"}, {"books_sha256": "0" * 64, "x": "1"}):
            bad = copy.deepcopy(values)
            bad[-1]["body"] = extra
            d = decoder(bad)
            for r in bad[:-1]:
                d.apply(encoded(r))
            with self.assertRaises(ProtocolError):
                d.apply(encoded(bad[-1]))

    def test_rust_metadata_control_observation_and_canonical_omission(self):
        values = records()
        _, cuts = apply_all(self, values)
        cut = cuts[-2]

        self.assertEqual(
            cut.body["control_events"][0]["event"],
            {"kind": "metadata_changed", "from": "before", "to": "after"},
        )
        self.assertEqual(
            cut.body["control_events"][0]["reference"]["address"],
            {
                "canonical_seq": "7",
                "lane": "x",
                "delivery_index": "6",
                "event_index": "0",
            },
        )
        self.assertEqual(cut.books[("kalshi:A", "outcome")].validity, "usable")
        self.assertTrue(
            all("control_events" not in prior.body for prior in cuts[1:-2])
        )

    def test_bad_second_transition_poisons_the_decoder(self):
        values = records()
        d = decoder(values)
        for r in values[:3]:
            d.apply(encoded(r))
        bad = copy.deepcopy(values[3])
        bad["body"]["book_transitions"][1]["previous_revision"] = "4"
        with self.assertRaisesRegex(ProtocolError, "revision"):
            d.apply(encoded(bad))
        # No atomic staging: the decoder (and attempt) is dead instead.
        self.assertTrue(d.poisoned)
        with self.assertRaisesRegex(ProtocolError, "closed decoder"):
            d.apply(encoded(values[3]))

    def test_cheap_guards_reject_immediately(self):
        values = records()

        def operation(record):
            return record["body"]["book_transitions"][0]["decision"]["operations"][0]

        cases = []
        for field, value in [
            ("sequence", "4"),
            ("sequence", "2"),
            ("sequence", "03"),
            ("version", "2"),
            ("run_id", "other"),
            ("attempt_id", "other"),
            ("kind", "initial"),
            ("extra", None),
        ]:
            bad = copy.deepcopy(values[3])
            bad[field] = value
            cases.append(bad)
        for atoms in ["-1", "1e3", "NaN", "101", "18446744073709551616", None]:
            bad = copy.deepcopy(values[3])
            operation(bad)["price"]["atoms"] = atoms
            cases.append(bad)
        bad = copy.deepcopy(values[3])
        operation(bad)["change"] = {"kind": "replace", "value": {"atoms": "1"}}
        cases.append(bad)
        bad = copy.deepcopy(values[3])
        operation(bad)["side"] = "middle"
        cases.append(bad)
        bad = copy.deepcopy(values[3])
        operation(bad)["change"] = {"kind": "decrease", "value": {"atoms": "99"}}
        cases.append(bad)
        bad = copy.deepcopy(values[3])
        bad["body"]["book_transitions"][0]["key"]["instrument"] = "kalshi:Z"
        cases.append(bad)
        bad = copy.deepcopy(values[3])
        bad["body"]["book_transitions"][0]["revision"] = "3"
        cases.append(bad)
        bad = copy.deepcopy(values[3])
        bad["body"]["book_transitions"][0]["decision"]["kind"] = "resync"
        cases.append(bad)
        bad = copy.deepcopy(values[3])
        del bad["body"]["book_transitions"]
        cases.append(bad)
        for bad in cases:
            with self.subTest(bad=bad):
                d = decoder(values)
                for r in values[:3]:
                    d.apply(encoded(r))
                with self.assertRaises(ProtocolError):
                    d.apply(encoded(bad))
                self.assertTrue(d.poisoned)
        d = decoder(values)
        for r in values[:-1]:
            d.apply(encoded(r))
        with self.assertRaisesRegex(ProtocolError, "missing terminal"):
            d.finish()
        terminal = copy.deepcopy(values[-1])
        terminal["body"]["cuts"] = "5"
        with self.assertRaisesRegex(ProtocolError, "terminal count"):
            d.apply(encoded(terminal))
        for raw in [b'{"version":"1","version":"1"}', b"[", b"NaN", b"\xff"]:
            with self.assertRaises(ProtocolError):
                decoder(values).apply(raw)

    def test_operations_on_unusable_book_are_rejected(self):
        values = records()
        d = decoder(values)
        for r in values[:6]:
            d.apply(encoded(r))
        self.assertEqual(d.books[("kalshi:A", "outcome")].validity, "unusable")
        bad = copy.deepcopy(values[3])
        bad["sequence"] = "6"
        bad["body"]["book_transitions"] = bad["body"]["book_transitions"][:1]
        bad["body"]["book_transitions"][0].update(previous_revision="3", revision="4")
        with self.assertRaisesRegex(ProtocolError, "unusable"):
            d.apply(encoded(bad))

    def test_unread_fields_are_publisher_trusted(self):
        # Hot-path decoding no longer validates market/control observations or
        # closed cut schemas; the publisher ships in the same image.
        values = records()
        bad = copy.deepcopy(values)
        bad[3]["body"]["market_events"][0]["event"]["value"]["value"]["extra"] = 1
        bad[2]["body"]["control_events"] = []
        bad[2]["body"]["unknown"] = True
        _, cuts = apply_all(self, bad)
        self.assertEqual(cuts[2].body["control_events"], [])

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


PIN = {"derivative_address": "a" * 64, "receipt_sha256": "b" * 64}


class InPlaceBook:
    """Hand-authored transitions against one planned book (dense or sparse)."""

    def __init__(self, price_scale):
        self.initial = {
            "pins": [PIN],
            "start_ns": "0",
            "end_ns": "100",
            "lower_bound": "clip",
            "plans": [
                {
                    "instrument": "kalshi:X",
                    "orientation": "outcome",
                    "lane": "x",
                    "venue": "kalshi",
                    "price_scale": str(price_scale),
                    "quantity_scale": "0",
                }
            ],
            "groups": ["g"],
            "max_entry_bytes": "1048576",
            "max_queue_bytes": "1048576",
        }
        self.decoder = Decoder("r", "a", self.initial, 1048576)
        self.send("initial", self.initial)

    @property
    def book(self):
        return self.decoder.books[("kalshi:X", "outcome")]

    def send(self, kind, body):
        return self.decoder.apply(
            encoded(
                {
                    "version": "1",
                    "run_id": "r",
                    "attempt_id": "a",
                    "sequence": str(self.decoder.sequence + 1),
                    "kind": kind,
                    "body": body,
                }
            )
        )

    def transition(self, decision):
        revision = self.book.revision
        return self.send(
            "cut",
            {
                "origin": {"kind": "window", "pin": PIN, "start_ns": "0", "end_ns": "1"},
                "market_events": [],
                "book_transitions": [
                    {
                        "key": {"instrument": "kalshi:X", "orientation": "outcome"},
                        "previous_revision": str(revision),
                        "revision": str(revision + 1),
                        "dependency": None
                        if decision["kind"] == "invalidation"
                        else {"epoch": "e"},
                        "decision": decision,
                    }
                ],
            },
        )

    def snapshot(self, bids, asks):
        pairs = lambda levels: [[str(p), str(q)] for p, q in sorted(levels.items())]
        return self.transition(
            {"kind": "snapshot", "bids": pairs(bids), "asks": pairs(asks)}
        )

    def operations(self, *ops):
        operations = []
        for side, price, kind, quantity in ops:
            change = {"kind": kind}
            if kind != "delete":
                change["value"] = {"atoms": str(quantity), "scale": "0", "unit": "contracts"}
            operations.append(
                {
                    "instrument": "kalshi:X",
                    "orientation": "outcome",
                    "side": side,
                    "price": {"atoms": str(price), "scale": "4", "unit": "quote_per_contract"},
                    "change": change,
                    "book_hash": None,
                }
            )
        return self.transition({"kind": "operations", "operations": operations})


class InPlaceBookTests(unittest.TestCase):
    def test_level_operations_best_and_limits(self):
        for scale in (4, 6):  # dense array, then sparse dict
            with self.subTest(scale=scale):
                h = InPlaceBook(scale)
                self.assertEqual(h.book.validity, "not_initialized")
                self.assertEqual((h.book.levels("bid"), h.book.best_bid()), ((), None))
                h.snapshot({100: 5, 300: 7, 200: 1}, {500: 2, 900: 4})
                self.assertEqual(h.book.bids, ((300, 7), (200, 1), (100, 5)))
                self.assertEqual(h.book.asks, ((500, 2), (900, 4)))
                self.assertEqual((h.book.best_bid(), h.book.best_ask()), ((300, 7), (500, 2)))
                h.operations(
                    ("bid", 300, "increase", 3),
                    ("bid", 250, "set", 9),
                    ("bid", 100, "decrease", 2),
                    ("ask", 400, "set", 1),
                    ("ask", 900, "delete", None),
                )
                self.assertEqual(h.book.bids, ((300, 10), (250, 9), (200, 1), (100, 3)))
                self.assertEqual(h.book.asks, ((400, 1), (500, 2)))
                self.assertEqual(h.book.levels("bid", 2), ((300, 10), (250, 9)))
                self.assertEqual(h.book.levels("ask", 1), ((400, 1),))
                self.assertEqual(h.book.levels("bid", 0), ())
                # Removing the best level finds the next best, near and far.
                h.operations(("bid", 300, "decrease", 10), ("ask", 400, "delete", None))
                self.assertEqual((h.book.best_bid(), h.book.best_ask()), ((250, 9), (500, 2)))
                h.operations(("bid", 250, "set", 0), ("bid", 200, "delete", None))
                self.assertEqual(h.book.best_bid(), (100, 3))
                h.operations(("bid", 100, "delete", None), ("ask", 500, "decrease", 2))
                self.assertEqual((h.book.best_bid(), h.book.best_ask()), (None, None))
                self.assertEqual((h.book.bids, h.book.asks), ((), ()))
                # Deleting an absent level is a no-op; boundary prices are valid.
                h.operations(("bid", 7, "delete", None), ("bid", 0, "set", 1))
                h.operations(("ask", 10**scale, "set", 2))
                self.assertEqual((h.book.best_bid(), h.book.best_ask()), ((0, 1), (10**scale, 2)))
                # Snapshot replaces the whole ladder, clearing only occupied slots.
                h.snapshot({50: 1}, {})
                self.assertEqual((h.book.bids, h.book.asks), (((50, 1),), ()))
                self.assertEqual(h.book.best_ask(), None)
                h.transition({"kind": "invalidation", "reason": {"kind": "connection_closed"}})
                self.assertEqual(h.book.validity, "unusable")
                self.assertEqual((h.book.bids, h.book.best_bid()), ((), None))
                with self.assertRaisesRegex(ProtocolError, "unusable"):
                    h.operations(("bid", 50, "set", 1))
                h = InPlaceBook(scale)
                with self.assertRaisesRegex(ProtocolError, "unusable"):
                    h.operations(("bid", 50, "set", 1))
                h = InPlaceBook(scale)
                h.snapshot({}, {})
                with self.assertRaisesRegex(ProtocolError, "price outside"):
                    h.operations(("bid", 10**scale + 1, "set", 1))

    def test_random_operations_match_a_reference_ladder(self):
        rng = random.Random(7)
        for scale, span in ((4, 10**4), (4, 300), (6, 10**6)):
            h = InPlaceBook(scale)
            model = {"bid": {}, "ask": {}}
            h.snapshot({}, {})
            for step in range(400):
                if step % 97 == 0:
                    for side in model:
                        model[side] = {
                            rng.randrange(span + 1): rng.randrange(1, 50)
                            for _ in range(rng.randrange(0, 40))
                        }
                    h.snapshot(model["bid"], model["ask"])
                    continue
                ops = []
                for _ in range(rng.randrange(1, 6)):
                    side = rng.choice(("bid", "ask"))
                    levels = model[side]
                    if levels and rng.random() < 0.5:
                        price = rng.choice(sorted(levels))
                    else:
                        price = rng.randrange(span + 1)
                    prior = levels.get(price, 0)
                    kind = rng.choice(("set", "increase", "decrease", "delete"))
                    quantity = rng.randrange(1, 20)
                    if kind == "decrease":
                        if not prior:
                            kind = "increase"
                        else:
                            quantity = rng.randrange(1, prior + 1)
                    new = {
                        "set": quantity,
                        "increase": prior + quantity,
                        "decrease": prior - quantity,
                        "delete": 0,
                    }[kind]
                    if new:
                        levels[price] = new
                    else:
                        levels.pop(price, None)
                    ops.append((side, price, kind, None if kind == "delete" else quantity))
                h.operations(*ops)
                bids = tuple(sorted(model["bid"].items(), reverse=True))
                asks = tuple(sorted(model["ask"].items()))
                self.assertEqual((h.book.bids, h.book.asks), (bids, asks))
                self.assertEqual(h.book.best_bid(), bids[0] if bids else None)
                self.assertEqual(h.book.best_ask(), asks[0] if asks else None)
                self.assertEqual(h.book.levels("bid", 3), bids[:3])


if __name__ == "__main__":
    unittest.main()
