import ast
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
from replay.streams.protocol import SECTIONS, books_sha256
from replay.streams.protocol import delta, event, venue_time

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "engine/crates/transport/tests/fixtures/contract.ndjson"
)


def records():
    return [{**json.loads(line), "version": "4"} for line in FIXTURE.read_bytes().splitlines()]


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


def fields(value):
    """Independent v4 projection of the frozen v3 fixture/test records."""
    value = copy.deepcopy(value)
    parts = {}
    if value["kind"] == "cut":
        body = value["body"]
        events = {name: body.pop(name) for name in ("market_events", "control_events") if name in body}
        if events:
            parts[b"events"] = encoded(events)
        parts[b"deps"] = encoded([t.pop("dependency") for t in body.get("book_transitions", [])])
    return {b"record": encoded(value), **parts}


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
        risk_sequence=cut.risk_sequence,
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
    cuts = [held(d.apply(fields(r))) for r in values]
    d.finish()
    return d, cuts


class ProtocolTests(unittest.TestCase):
    def test_v4_risk_sequence_and_terminal_bounds(self):
        values = records()
        for record in values:
            if record["kind"] == "cut":
                record["body"]["risk_sequence"] = str(2 * int(record["sequence"]))
        values[-1]["body"]["risk_cuts"] = "100"
        _, cuts = apply_all(self, values)
        self.assertEqual(cuts[0].risk_sequence, 0)
        self.assertEqual(cuts[1].risk_sequence, 2)
        self.assertEqual(cuts[-1].risk_sequence, 101)
        for version in ("1", "2", "3"):
            with self.subTest(version=version), self.assertRaisesRegex(ProtocolError, "version"):
                decoder(values).apply(fields({**values[0], "version": version}))
        for risk in ("0", "1", "2", "02", str(2**64), 4):
            d = decoder(values)
            for r in values[:2]:
                d.apply(fields(r))
            bad = copy.deepcopy(values[2])
            bad["body"]["risk_sequence"] = risk
            with self.subTest(risk=risk), self.assertRaises(ProtocolError):
                d.apply(fields(bad))
        for field, value in (("cuts", "0"), ("risk_cuts", "0"), ("risk_cuts", "7")):
            d = decoder(values)
            for r in values[:-1]:
                d.apply(fields(r))
            bad = copy.deepcopy(values[-1])
            bad["body"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ProtocolError):
                d.apply(fields(bad))

    def test_retention_lists_are_closed_canonical_sorted_and_bounded(self):
        values = records()
        for field in ("retain_boundaries_ns", "retain_shift_offsets_ns"):
            for invalid in (["01"], [1], ["2", "1"], ["1", "1"], [str(2**64)], None):
                with self.subTest(field=field, invalid=invalid), self.assertRaises(ProtocolError):
                    bad = [{**values[0], "body": {**values[0]["body"], field: invalid}}]
                    decoder(bad).apply(encoded(bad[0]))
        with self.assertRaises(ProtocolError):
            bad = [{**values[0], "body": {**values[0]["body"], "retain_shift_offsets_ns": [str(i) for i in range(129)]}}]
            decoder(bad).apply(encoded(bad[0]))

    def test_venue_time_closed_schema_and_exact_wire_integers(self):
        valid = {
            "event_ns": "1669149841123456000",
            "event_resolution": "microsecond",
            "event_kind": "exchange_event",
            "sent_ns": "1669149841130000000",
        }
        for value in [None, valid, {**valid, "event_ns": None,
                "event_resolution": None, "event_kind": None},
                {**valid, "sent_ns": None}, {**valid, "event_ns": "0"}]:
            venue_time(value)
        for kind in ("exchange_event", "book_update", "trade_report", "book_as_of"):
            venue_time({**valid, "event_kind": kind, "event_resolution": "millisecond"})
        malformed = [
            {}, [], False,
            {**valid, "unknown": "1"},
            {key: value for key, value in valid.items() if key != "sent_ns"},
            {**valid, "event_ns": None},
            {**valid, "event_resolution": None},
            {**valid, "event_kind": None},
            {"event_ns": None, "event_resolution": None, "event_kind": None, "sent_ns": None},
            {**valid, "event_resolution": "nanosecond"},
            {**valid, "event_kind": "match"},
        ]
        for key in ("event_ns", "sent_ns"):
            for bad in (1, True, 1.0, "-1", "01", "1e3", str(2**64), ""):
                malformed.append({**valid, key: bad})
        for value in malformed:
            with self.subTest(value=value), self.assertRaises(ProtocolError):
                venue_time(value)

    def test_golden_annotations_are_market_observations_only(self):
        observed = set()
        for record in records():
            if record["kind"] != "cut":
                continue
            for market in record["body"]["market_events"]:
                value = market["event"]
                event(value)
                body = value["value"] if value["kind"] == "trade" else value["value"]["value"]
                if body["venue_time"] is not None:
                    observed.add(value["kind"] if value["kind"] == "trade" else value["value"]["kind"])
                missing = copy.deepcopy(value)
                target = missing["value"] if missing["kind"] == "trade" else missing["value"]["value"]
                del target["venue_time"]
                with self.assertRaises(ProtocolError):
                    event(missing)
            for transition in record["body"]["book_transitions"]:
                if transition["decision"]["kind"] == "operations":
                    for operation in transition["decision"]["operations"]:
                        self.assertNotIn("venue_time", operation)
                        delta(operation)
        self.assertEqual(observed, {"full", "delta", "trade"})

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

    def test_stream_json_rejects_constants_and_invalid_utf8_and_poisons(self):
        values = records()
        wire = encoded(values[1])
        for value in (b"NaN", b"Infinity", b"-Infinity", b'"\xff"'):
            d = decoder(values)
            d.apply(encoded(values[0]))
            # Put the malformed JSON in a body field outside the O(1) guard set,
            # so only the parser (not an unrelated identity guard) rejects it.
            raw = wire[:-2] + b',"diagnostic":' + value + b'}}'
            with self.subTest(value=value), self.assertRaisesRegex(ProtocolError, "malformed JSON"):
                d.apply(raw)
            self.assertTrue(d.poisoned)

    def test_books_are_live_in_place_and_read_only_mappings(self):
        values = records()
        d = decoder(values)
        cuts = [d.apply(fields(r)) for r in values]
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
                d.apply(fields(r))
            with self.assertRaisesRegex(ProtocolError, "terminal book digest"):
                d.apply(fields(bad[-1]))
            self.assertTrue(d.poisoned)
            with self.assertRaisesRegex(ProtocolError, "missing terminal"):
                d.finish()
        for extra in ({}, {"cuts": "7"}, {"books_sha256": "0" * 64, "x": "1"}):
            bad = copy.deepcopy(values)
            bad[-1]["body"] = extra
            d = decoder(bad)
            for r in bad[:-1]:
                d.apply(fields(r))
            with self.assertRaises(ProtocolError):
                d.apply(fields(bad[-1]))

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
            d.apply(fields(r))
        bad = copy.deepcopy(values[3])
        bad["body"]["book_transitions"][1]["previous_revision"] = "4"
        with self.assertRaisesRegex(ProtocolError, "revision"):
            d.apply(fields(bad))
        # No atomic staging: the decoder (and attempt) is dead instead.
        self.assertTrue(d.poisoned)
        with self.assertRaisesRegex(ProtocolError, "closed decoder"):
            d.apply(fields(values[3]))

    def test_cheap_guards_reject_immediately(self):
        values = records()

        def operation(record):
            return record["body"]["book_transitions"][0]["decision"]["operations"][0]

        cases = []
        for field, value in [
            ("sequence", "4"),
            ("sequence", "2"),
            ("sequence", "03"),
            ("version", "1"),
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
                    d.apply(fields(r))
                with self.assertRaises(ProtocolError):
                    d.apply(fields(bad))
                self.assertTrue(d.poisoned)
        d = decoder(values)
        for r in values[:-1]:
            d.apply(fields(r))
        with self.assertRaisesRegex(ProtocolError, "missing terminal"):
            d.finish()
        terminal = copy.deepcopy(values[-1])
        terminal["body"]["cuts"] = "5"
        with self.assertRaisesRegex(ProtocolError, "terminal count"):
            d.apply(fields(terminal))
        for raw in [b'{"version":"1","version":"1"}', b"[", b"NaN", b"Infinity", b"-Infinity", b"\xff"]:
            with self.assertRaises(ProtocolError):
                decoder(values).apply(raw)

    def test_operations_on_unusable_book_are_rejected(self):
        values = records()
        d = decoder(values)
        for r in values[:6]:
            d.apply(fields(r))
        self.assertEqual(d.books[("kalshi:A", "outcome")].validity, "unusable")
        bad = copy.deepcopy(values[3])
        bad["sequence"] = "6"
        bad["body"]["risk_sequence"] = "6"
        bad["body"]["book_transitions"] = bad["body"]["book_transitions"][:1]
        bad["body"]["book_transitions"][0].update(previous_revision="3", revision="4")
        with self.assertRaisesRegex(ProtocolError, "unusable"):
            d.apply(fields(bad))

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
            d.apply(fields(r))
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
                change["body"]["risk_sequence"] = str(count + 2)
                transition["previous_revision"] = str(count)
                transition["revision"] = str(count + 1)
                d.apply(fields(change))
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


class SectionTests(unittest.TestCase):
    def test_every_fixture_reassembles_and_all_declarations_preserve_books(self):
        count = 0
        for path in sorted(FIXTURE.parent.glob("*.ndjson")):
            values = [json.loads(line) for line in path.read_bytes().splitlines()]
            for sections in (SECTIONS, {"record"}, {"record", "events"}, {"record", "deps"}):
                d = Decoder(values[0]["run_id"], values[0]["attempt_id"], values[0]["body"],
                            int(values[0]["body"]["max_entry_bytes"]), sections=sections)
                for old in values:
                    cut = d.apply(fields({**old, "version": "4"}))
                    with self.subTest(fixture=path.name, sequence=cut.sequence, sections=sections):
                        if sections == SECTIONS or cut.kind != "cut":
                            self.assertEqual(thaw(cut.body), old["body"])
                        if cut.kind == "cut":
                            if "events" not in sections:
                                self.assertIsInstance(cut.body, dict)
                                for name in ("market_events", "control_events"):
                                    with self.assertRaisesRegex(ProtocolError, "undeclared section"):
                                        cut.body[name]
                                    with self.assertRaisesRegex(ProtocolError, "undeclared section"):
                                        cut.body.get(name, [])
                                self.assertEqual(cut.body.get("absent", 42), 42)
                                with self.assertRaises(KeyError):
                                    cut.body["absent"]
                            for original, t in zip(old["body"]["book_transitions"], cut.body["book_transitions"]):
                                self.assertIs(type(t), dict)
                                book = cut.books[(t["key"]["instrument"], t["key"]["orientation"])]
                                if "deps" in sections:
                                    self.assertEqual(book.dependency, original["dependency"])
                                else:
                                    with self.assertRaises(KeyError):
                                        t["dependency"]
                                    with self.assertRaisesRegex(ProtocolError, "undeclared section"):
                                        getattr(book, "dependency", None)
                d.finish()  # Includes the unmodified Rust terminal books digest.
            count += len(values)
        self.assertEqual(count, 79)

    def test_only_protocol_reassembly_reads_transition_dependencies(self):
        def reads(tree):
            found = []
            for node in ast.walk(tree):
                key = None
                if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load):
                    key = node.slice
                elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                      and node.func.attr in {"get", "pop", "setdefault", "__getitem__"} and node.args):
                    key = node.args[0]
                if isinstance(key, ast.Constant) and key.value == "dependency":
                    found.append(node.lineno)
            return found

        # Ensure the scanner rejects both direct and default-returning reads,
        # but not the assignment used to reassemble declared dependencies.
        self.assertEqual(reads(ast.parse("t['dependency']\nt.get('dependency', None)\nt['dependency'] = None")), [1, 2])
        root = Path(__file__).resolve().parents[1]
        violations = []
        for path in sorted(root.rglob("*.py")):
            if "tests" in path.relative_to(root).parts:
                continue
            tree = ast.parse(path.read_text(), filename=str(path))
            allowed = set()
            if path == root / "streams/protocol.py":
                decoder = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Decoder")
                allowed = {node.lineno for method in decoder.body
                           if isinstance(method, ast.FunctionDef) and method.name in {"_sections", "_cut"}
                           for node in ast.walk(method) if hasattr(node, "lineno")}
            violations.extend(f"{path.relative_to(root)}:{line}" for line in reads(tree) if line not in allowed)
        self.assertEqual(violations, [], "transition dependency reads require a deps declaration")

    def test_skipped_malformed_sections_are_not_parsed_but_declared_ones_fail(self):
        values = records()
        for name in (b"events", b"deps"):
            for invalid in (b"[", b'"\xff"', b"NaN"):
                for declared in (False, True):
                    sections = {"record", name.decode()} if declared else {"record"}
                    d = Decoder("contract", "golden", values[0]["body"], 65536, sections=sections)
                    d.apply(fields(values[0]))
                    entry = {**fields(values[1]), name: invalid}
                    with self.subTest(section=name, invalid=invalid, declared=declared):
                        if declared:
                            with self.assertRaisesRegex(ProtocolError, "malformed JSON"):
                                d.apply(entry)
                            self.assertTrue(d.poisoned)
                        else:
                            d.apply(entry)

    def test_aggregate_entry_limit_includes_unparsed_sections_at_exact_boundary(self):
        values = records()
        for extra in (0, 1):
            d = Decoder("contract", "golden", values[0]["body"], 65536, sections={"record"})
            d.apply(fields(values[0]))
            entry = fields(values[1])
            entry[b"events"] = b"[" * (65536 - len(entry[b"record"]) - len(entry[b"deps"]) + extra)
            self.assertLess(max(map(len, entry.values())), 65536)
            if extra:
                with self.assertRaisesRegex(ProtocolError, "entry limit"):
                    d.apply(entry)
                self.assertTrue(d.poisoned)
            else:
                self.assertEqual(d.apply(entry).sequence, 1)

    def test_section_shapes_and_dependency_alignment_fail_closed(self):
        values = records()
        for changes in ({b"extra": b"{}"}, {b"deps": b"[null]"},
                        {b"events": b'{"unknown":[]}'}, {b"deps": b"{}"},
                        {b"events": None}, {b"deps": "[]"}, {b"record": bytearray(b"{}")}):
            d = decoder(values)
            d.apply(fields(values[0]))
            entry = {**fields(values[1]), **changes}
            with self.subTest(changes=changes), self.assertRaises(ProtocolError):
                d.apply(entry)
            self.assertTrue(d.poisoned)
        for sections in ({"events"}, {"record", "unknown"}, set()):
            with self.assertRaisesRegex(ProtocolError, "section declaration"):
                Decoder("contract", "golden", values[0]["body"], 65536, sections=sections)

    def test_runner_declarations_match_the_sdk_and_factory_defaults(self):
        from replay.strategy_sdk import stream_sections
        economics = ("same_venue_complement", "cross_venue_arbitrage", "same_venue_multi_market",
                     "same_venue_implication_cover", "cross_venue_implication_cover")
        for name in economics:
            for profile in (None, {}):
                spec = {"factory": f"replay.strategies.{name}:build", "config": {"policy": {"profile": profile}}}
                self.assertEqual(stream_sections(spec), {"record"} if profile is None else {"record", "events"})
        for name in ("market_profile", "bundle_coverage"):
            for reference in (f"replay.strategies.{name}:build", f"replay.{name}:build",
                              f"replay.strategies.{name}.strategy:build"):
                self.assertEqual(stream_sections({"factory": reference, "config": {}}), {"record", "events"})
        self.assertEqual(stream_sections({"factory": "replay.tests.test_supervisor:strategy", "config": {}}), SECTIONS)

    def test_consumer_checks_summed_batch_before_hooks_and_passes_sections(self):
        values = records()
        entries = [(f"{i + 1}-0".encode(), fields(r)) for i, r in enumerate(values)]
        total = sum(len(v) for _, f in entries for v in f.values())
        for limit in (total - 1, total):
            with patch("redis.Redis.from_url") as connect, patch.object(Consumer, "_eval"):
                c = Consumer("redis://localhost", scope="test", run_id="contract", attempt_id="golden",
                             group="fast", initial=values[0]["body"], sections={"record"})
                connect.return_value.xreadgroup.return_value = [(c._keys[0].encode(), entries)]
                c._batch_bytes = limit
                cuts = []
                if limit < total:
                    with self.assertRaisesRegex(ProtocolError, "batch limit"):
                        c.poll(cuts.append)
                    self.assertEqual(cuts, [])
                else:
                    self.assertEqual(c.poll(cuts.append), len(values))
                    self.assertTrue(c.terminal)
                    with self.assertRaisesRegex(ProtocolError, "undeclared section"):
                        cuts[2].body.get("market_events", [])
                c.close()


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
            "retain_boundaries_ns": [],
            "retain_shift_offsets_ns": [],
            "max_entry_bytes": "1048576",
            "max_queue_bytes": "1048576",
        }
        self.decoder = Decoder("r", "a", self.initial, 1048576)
        self.send("initial", self.initial)

    @property
    def book(self):
        return self.decoder.books[("kalshi:X", "outcome")]

    def send(self, kind, body):
        if kind == "cut":
            body = {**body, "risk_sequence": str(self.decoder.sequence + 1)}
        return self.decoder.apply(
            fields(
                {
                    "version": "4",
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
    def test_a_wire_version_1_record_is_rejected(self):
        h = InPlaceBook(4)
        decoder = Decoder("r", "a", h.initial, 1048576)
        record = {
            "version": "1",
            "run_id": "r",
            "attempt_id": "a",
            "sequence": "0",
            "kind": "initial",
            "body": h.initial,
        }
        with self.assertRaisesRegex(ProtocolError, "identity/version"):
            decoder.apply(encoded(record))

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
