"""Market profile ``transitions`` group: exact per-cut flows, the level mirror and the reader."""

import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from encoder import encode_stream
from encoder.compression import LogicalIdentity, StoredIdentity, decode_stream
from replay.economic_sdk.profile import Collector, profile_policy
from replay.preparation import load_snapshot
from replay.streams.protocol import Book, ProtocolError
from replay.strategies.market_profile.strategy import validate_content
from replay.tests.economic_scenarios import M, PROFILE_POLICY, ladder
from replay.tests.test_market_profile import Harness
from replay.tests.test_market_profile_v2 import v2

POLICY = v2("availability", "transitions")
PM = ("polymarket:123", "outcome")
PM2 = ("polymarket:987", "outcome")
KO = ("kalshi:series", "outcome")
KC = ("kalshi:series", "complement")


def number(atoms, scale, quantity=False):
    return {"atoms": str(atoms), "scale": str(scale), "unit": "contracts" if quantity else "quote_per_contract"}


def op(key, side, price, how, quantity=None):
    change = {"kind": how} if how == "delete" else {"kind": how, "value": number(quantity, 6, True)}
    return {"instrument": key[0], "orientation": key[1], "side": side, "price": number(price, 3),
            "change": change, "book_hash": None}


def clock(kind="book_update", at=1000, sent=None, resolution="millisecond"):
    return {"event_ns": None if at is None else str(at), "event_resolution": None if at is None else resolution,
            "event_kind": None if at is None else kind, "sent_ns": None if sent is None else str(sent)}


def book_event(h, time, key, venue_time):
    return {"reference": h.ref(time), "disposition": "applied",
            "event": {"kind": "book", "value": {"kind": "delta", "value": {
                "instrument": key[0], "orientation": key[1], "side": "bid", "price": number(400, 3),
                "change": {"kind": "set", "value": number(M, 6, True)}, "book_hash": None,
                "venue_time": venue_time}}}}


def trade_event(h, time, key, disposition, quantity, aggressor=None, venue_time=None, scale=6):
    return {"reference": h.ref(time), "disposition": disposition,
            "event": {"kind": "trade", "value": {
                "instrument": key[0], "orientation": key[1], "price": number(400, 3),
                "quantity": number(quantity, scale, True), "aggressor": aggressor, "venue_time": venue_time}}}


def transition(h, time, key, decision):
    book = h.decoder.books[key]
    ref = h.ref(time)
    return {"key": {"instrument": key[0], "orientation": key[1]},
            "previous_revision": str(book.revision), "revision": str(book.revision + 1),
            "dependency": None if decision["kind"] == "invalidation"
            else {"epoch": "one", "anchor": ref, "through": ref},
            "decision": decision}


def operations(*ops):
    return {"kind": "operations", "operations": list(ops)}


def snapshot(bids=(), asks=()):
    # The wire carries ascending pairs: best bid last, best ask first.
    return {"kind": "snapshot", "bids": [[str(p), str(q)] for p, q in reversed(bids)],
            "asks": [[str(p), str(q)] for p, q in asks]}


def send(h, time, *changes, events=()):
    """One cut: ``changes`` are ``(key, decision)`` pairs, built before the decoder sees them."""
    transitions = [transition(h, time, key, decision) for key, decision in changes]
    ref = h.ref(time)
    return h.send("cut", {"origin": {"kind": "group", "pin": h.pin, "first": ref["address"],
                                     "last": ref["address"], "visible_ns": str(time)},
                          "market_events": list(events), "book_transitions": transitions})


def decoded(path):
    sink = io.BytesIO()
    manifest = json.loads((path / "manifest.json").read_bytes())["files"]["transitions.ndjson.zst"]
    logical, stored = manifest["logical"], manifest["stored"]
    with (path / "transitions.ndjson.zst").open("rb") as source:
        decode_stream(source, sink, expected_logical=LogicalIdentity(logical["sha256"], logical["byte_length"],
                                                                      logical["records"]),
                      expected_stored=StoredIdentity(stored["sha256"], stored["byte_length"]))
    return sink.getvalue()


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.count = 0

    def harness(self, policy=POLICY, **kwargs):
        self.count += 1
        path = self.root / f"h{self.count}"
        path.mkdir()
        h = Harness(path, policy=policy, **kwargs)
        h.window()
        return h

    def rows(self, h, key=None, result=None):
        result = result or h.finish()
        table = result["summary"]["transition_books"]
        rows = [json.loads(line) for line in decoded(h.profile_output).splitlines()]
        for row in rows:
            row["key"] = (table[row["book"]]["instrument"], table[row["book"]]["orientation"])
        return [r for r in rows if key is None or r["key"] == key]


class FlowTests(Case):
    def test_polymarket_set_up_down_delete_and_best_level_moves(self):
        h = self.harness()
        ladder(h, 12, PM[0], bids=((400, 5 * M), (390, 3 * M)), asks=((450, 2 * M), (460, 4 * M)))
        send(h, 13, (PM, operations(op(PM, "ask", 450, "set", 3 * M))))
        # Two operations on one price in one cut count per operation.
        send(h, 14, (PM, operations(op(PM, "bid", 400, "set", 2 * M), op(PM, "bid", 400, "set", 6 * M))))
        send(h, 15, (PM, operations(op(PM, "bid", 400, "delete"))))
        send(h, 16, (PM, operations(op(PM, "bid", 395, "set", M))))
        send(h, 17, (PM, operations(op(PM, "ask", 450, "delete"))))
        rows = self.rows(h, PM)
        self.assertEqual([r["t_ns"] for r in rows], ["12", "13", "14", "15", "16", "17"])
        snap, up, wobble, gone, better, ask_gone = rows
        self.assertEqual((snap["kind"], snap["reason"], snap["bid_added"], snap["ask_best_removed"],
                          snap["bid_depleted"], snap["prev_bid"], snap["bid"]),
                         ("snapshot", "snapshot", None, None, None, None, ["400", str(5 * M)]))
        self.assertEqual((snap["bid_move_atoms"], snap["bid_move_ticks"]), (None, None))

        self.assertEqual((up["reason"], up["ask_added"], up["ask_removed"], up["ask_best_added"],
                          up["ask_best_removed"], up["ask_depleted"]),
                         ("insert", str(M), "0", str(M), "0", False))
        self.assertEqual((up["prev_ask"], up["ask"], up["bid_added"], up["bid_best_added"], up["bid_depleted"]),
                         (["450", str(2 * M)], ["450", str(3 * M)], "0", "0", False))

        self.assertEqual((wobble["reason"], wobble["bid_added"], wobble["bid_removed"], wobble["bid_best_added"],
                          wobble["bid_best_removed"], wobble["bid_depleted"], wobble["bid"]),
                         ("unknown", str(4 * M), str(3 * M), str(4 * M), str(3 * M), False, ["400", str(6 * M)]))

        self.assertEqual((gone["reason"], gone["bid_removed"], gone["bid_best_removed"], gone["bid_depleted"],
                          gone["bid"], gone["bid_move_atoms"], gone["bid_move_ticks"]),
                         ("unknown", str(6 * M), str(6 * M), True, ["390", str(3 * M)], "-10", "-1"))

        self.assertEqual((better["reason"], better["bid_added"], better["bid_best_added"], better["bid_depleted"],
                          better["bid_move_atoms"], better["bid_move_ticks"]),
                         ("insert", str(M), "0", False, "5", None))  # a new best level; 5 is not a whole tick
        self.assertEqual((ask_gone["ask_depleted"], ask_gone["ask"], ask_gone["ask_move_atoms"],
                          ask_gone["ask_move_ticks"]), (True, ["460", str(4 * M)], "10", "1"))

    def test_kalshi_increase_decrease_and_projected_asks(self):
        h = self.harness(mixed=True)
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M),))
        send(h, 13, (KO, operations(op(KO, "bid", 560, "increase", M))))
        send(h, 14, (KO, operations(op(KO, "bid", 560, "decrease", 3 * M))))
        send(h, 15, (KC, operations(op(KC, "bid", 450, "increase", 2 * M))))  # a better bid on the counterpart
        send(h, 16, (KO, operations(op(KO, "bid", 300, "increase", M))))
        result = h.finish()
        table = result["summary"]["transition_books"]
        outcome = next(t for t in table if (t["instrument"], t["orientation"]) == KO)
        self.assertEqual((outcome["ask_source"], outcome["ask_source_book"]), ("projected", list(KC)))
        rows = self.rows(h, KO, result)
        self.assertEqual([r["t_ns"] for r in rows], ["12", "13", "14", "16"])  # no row at 15
        first, grew, shrank, later = rows
        self.assertEqual((first["ask"], first["prev_ask"]), (None, None))  # the counterpart was not there yet
        self.assertEqual((grew["prev_ask"], grew["ask"]), (["600", str(M)], ["600", str(M)]))  # 1000 - 400
        self.assertEqual((grew["bid_added"], grew["reason"]), (str(M), "insert"))
        for name in ("ask_added", "ask_removed", "ask_best_added", "ask_best_removed", "ask_depleted"):
            self.assertIsNone(grew[name])
        self.assertEqual((shrank["bid_removed"], shrank["bid_depleted"], shrank["bid"], shrank["reason"]),
                         (str(3 * M), True, None, "unknown"))
        # The counterpart's better bid (450) moved this book's projected ask at 15.
        self.assertEqual(later["prev_ask"], ["550", str(2 * M)])  # 1000 - 450, the counterpart's new best
        self.assertEqual(later["ask"], later["prev_ask"])
        complement = self.rows(h, KC, result)
        self.assertEqual([r["t_ns"] for r in complement], ["12", "15"])
        self.assertEqual(complement[0]["ask"], ["440", str(2 * M)])  # 1000 - 560, the outcome's bid
        self.assertEqual((complement[1]["bid"], complement[1]["bid_best_added"], complement[1]["bid_move_atoms"]),
                         (["450", str(2 * M)], "0", "50"))
        # The outcome's bid was removed at 14 without a complement row: the projected ask went with it.
        self.assertEqual((complement[1]["prev_ask"], complement[1]["ask"]), (None, None))

    def test_snapshot_resets_the_mirror_and_invalidation_clears_it(self):
        h = self.harness()
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        send(h, 13, (PM, operations(op(PM, "bid", 400, "set", 6 * M))))
        ladder(h, 14, PM[0], bids=((380, M), (370, M)), asks=((420, M),))  # replaces every level
        send(h, 15, (PM, operations(op(PM, "bid", 380, "set", 4 * M))))
        ladder(h, 16, PM[0], why={"kind": "connection_closed"})
        ladder(h, 18, PM[0], bids=((300, M),), asks=((310, M),))
        send(h, 19, (PM, operations(op(PM, "ask", 310, "set", 2 * M))))
        rows = self.rows(h, PM)
        reset = rows[2]
        self.assertEqual((reset["kind"], reset["bid_added"], reset["ask_depleted"], reset["prev_bid"],
                          reset["bid"], reset["bid_move_atoms"]), ("snapshot", None, None, ["400", str(6 * M)],
                                                                  ["380", str(M)], "-20"))
        after = rows[3]  # computed from the new ladder, not the old one
        self.assertEqual((after["bid_added"], after["bid_removed"], after["bid_best_added"]),
                         (str(3 * M), "0", str(3 * M)))
        cleared = rows[4]
        self.assertEqual((cleared["kind"], cleared["reason"], cleared["validity"], cleared["bid"], cleared["ask"],
                          cleared["prev_bid"], cleared["bid_added"]),
                         ("invalidation", "invalidation", "unusable:connection_closed", None, None,
                          ["380", str(4 * M)], None))
        recovered, resumed = rows[5], rows[6]
        self.assertEqual((recovered["kind"], recovered["prev_bid"], recovered["bid"]),
                         ("snapshot", None, ["300", str(M)]))
        self.assertEqual((resumed["ask_added"], resumed["ask_removed"], resumed["ask_best_added"]),
                         (str(M), "0", str(M)))


class EventTests(Case):
    def test_trades_and_venue_time_co_occur_with_the_book_change(self):
        h = self.harness()
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        ladder(h, 12, PM2[0], bids=((400, M),), asks=((450, M),))
        events = [book_event(h, 14, PM, clock(at=1500, sent=1600)),
                  book_event(h, 14, PM, clock(at=1000, sent=1100)),
                  book_event(h, 14, PM2, clock(at=1)),          # a book that did not transition
                  trade_event(h, 14, PM, "applied", 2 * M, "ask", clock("trade_report", 900)),
                  trade_event(h, 14, PM, "duplicate", 9 * M, "bid", clock("trade_report", 1)),
                  trade_event(h, 14, PM, "observed", M, None, clock("trade_report", 950)),
                  trade_event(h, 14, PM, "applied", 7, "bid", None, scale=5)]  # scale mismatch
        send(h, 14, (PM, operations(op(PM, "bid", 400, "decrease", M))), events=events)
        send(h, 15, (PM, operations(op(PM, "bid", 400, "increase", M))),
             events=[book_event(h, 15, PM, clock("book_update", 20)),
                     book_event(h, 15, PM, clock("book_as_of", 30, resolution="microsecond"))])
        send(h, 16, (PM, operations(op(PM, "bid", 400, "increase", M))),
             events=[book_event(h, 16, PM, clock(at=None, sent=77))])
        send(h, 17, (PM, operations(op(PM, "bid", 400, "increase", M))))
        result = h.finish()
        rows = self.rows(h, PM, result)
        co, mixed, sent_only, bare = rows[1:]
        self.assertEqual(co["venue_time"], {"first_event_ns": "1000", "last_event_ns": "1500",
                                            "event_kind": "book_update", "event_resolution": "millisecond",
                                            "last_sent_ns": "1600", "events": 2})
        self.assertEqual(co["trades"], {"count": 3, "qty_atoms": str(3 * M),
                                        "aggressor": {"bid": 1, "ask": 1, "none": 1}})
        self.assertEqual(co["trade_venue_time"], {"first_event_ns": "900", "last_event_ns": "950",
                                                  "event_kind": "trade_report", "event_resolution": "millisecond",
                                                  "last_sent_ns": None, "events": 2})
        self.assertEqual((mixed["venue_time"]["event_kind"], mixed["venue_time"]["event_resolution"],
                          mixed["venue_time"]["events"]), ("mixed", "mixed", 2))
        self.assertEqual(sent_only["venue_time"], {"first_event_ns": None, "last_event_ns": None,
                                                   "event_kind": None, "event_resolution": None,
                                                   "last_sent_ns": "77", "events": 1})
        self.assertEqual((bare["venue_time"], bare["trade_venue_time"], bare["trades"]),
                         (None, None, {"count": 0, "qty_atoms": "0", "aggressor": {"bid": 0, "ask": 0, "none": 0}}))
        other = self.rows(h, PM2, result)
        self.assertEqual(len(other), 1)  # its book event alone is not a transition

    def test_summary_tables_and_counts(self):
        h = self.harness(mixed=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        send(h, 13, (PM, operations(op(PM, "bid", 400, "increase", M))))
        send(h, 14, (PM, operations(op(PM, "bid", 400, "decrease", M))))
        result = h.finish()
        summary = result["summary"]
        self.assertEqual(summary["transition_rows"], {"total": 3, "by_kind": {"operations": 2, "snapshot": 1},
                                                      "by_reason": {"insert": 1, "snapshot": 1, "unknown": 1}})
        books = summary["transition_books"]
        self.assertEqual([(b["instrument"], b["orientation"]) for b in books], sorted(
            (b["instrument"], b["orientation"]) for b in books))
        kalshi = next(b for b in books if b["venue"] == "kalshi" and b["orientation"] == "outcome")
        polymarket = next(b for b in books if b["venue"] == "polymarket")
        self.assertEqual((polymarket["ask_source"], polymarket["ask_source_book"], polymarket["tick_atoms"],
                          polymarket["price_scale"], polymarket["quantity_scale"], polymarket["market_id"]),
                         ("native", None, "10", "3", "6", "polymarket:series"))
        self.assertEqual(kalshi["ask_source_book"], ["kalshi:series", "complement"])
        scope = summary["transition_scopes"][0]
        self.assertEqual((scope["start_ns"], scope["end_ns"], scope["scheduled_start_ns"], scope["capture_start_ns"]),
                         ("10", "40", "1767236400000000000", "1767232800000000000"))
        self.assertEqual(summary["transition_trades"], {"attached": 0, "unattached": 0})


class ScopeTests(Case):
    def test_rows_are_gated_by_scope_and_requested_start_but_the_mirror_is_not(self):
        h = self.harness(mixed=True, scopes="uncaptured", lower_bound="expand_to_window_start")
        ladder(h, 5, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))   # before the requested start: no row
        send(h, 12, (PM, operations(op(PM, "bid", 400, "increase", M))))
        send(h, 25, (PM, operations(op(PM, "bid", 400, "increase", M))))  # the book left the scope
        ladder(h, 26, KO[0], "outcome", bids=((560, M),))
        result = h.finish()
        pm = self.rows(h, PM, result)
        self.assertEqual([(r["scope"], r["t_ns"]) for r in pm], [(0, "12")])
        # The pre-start snapshot is the row's before-state: the mirror followed it.
        self.assertEqual((pm[0]["prev_bid"], pm[0]["bid_added"], pm[0]["bid"]),
                         (["400", str(5 * M)], str(M), ["400", str(6 * M)]))
        kalshi = self.rows(h, KO, result)
        self.assertEqual([(r["scope"], r["t_ns"]) for r in kalshi], [(1, "26")])
        self.assertEqual(len(result["summary"]["transition_scopes"]), 2)


class MirrorTests(Case):
    def test_a_corrupted_operation_stream_fails_the_run(self):
        h = self.harness()
        ladder(h, 12, PM[0], bids=((400, 5 * M), (390, M)), asks=((450, 2 * M),))
        real = Book._operations

        def lossy(book, operations):  # the decoder drops a delete that the stream carries
            return real(book, [o for o in operations if o["change"]["kind"] != "delete"])

        with patch.object(Book, "_operations", lossy):
            with self.assertRaisesRegex(ProtocolError, "mirror diverged"):
                send(h, 13, (PM, operations(op(PM, "bid", 400, "delete"))))
        self.assertTrue(h.strategy.poisoned)


class ReaderTests(Case):
    def build(self):
        h = self.harness(mixed=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M),))
        send(h, 13, (PM, operations(op(PM, "bid", 400, "increase", M))))
        send(h, 14, (PM, operations(op(PM, "bid", 400, "decrease", 2 * M))))
        send(h, 15, (KC, operations(op(KC, "bid", 450, "increase", M))))
        send(h, 16, (KO, operations(op(KO, "bid", 300, "increase", M))))
        send(h, 17, (PM, operations(op(PM, "bid", 400, "decrease", 4 * M))))
        h.finish()
        self.snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        self.manifest = json.loads((h.profile_output / "manifest.json").read_bytes())
        self.original = decoded(h.profile_output)
        self.frame = (h.profile_output / "transitions.ndjson.zst").read_bytes()
        return h

    def original_rows(self):
        return [json.loads(line) for line in self.original.splitlines()]

    def tampered(self, h, edit):
        rows = self.original_rows()
        edit(rows)
        payload = b"".join(json.dumps(r, sort_keys=True, separators=(",", ":")).encode() + b"\n" for r in rows)
        sink = io.BytesIO()
        result = encode_stream(io.BytesIO(payload), sink)
        (h.profile_output / "transitions.ndjson.zst").write_bytes(sink.getvalue())
        manifest = copy.deepcopy(self.manifest)
        manifest["files"]["transitions.ndjson.zst"] = {
            "logical": {"sha256": result.logical.sha256, "byte_length": result.logical.byte_length,
                        "records": result.logical.line_count},
            "stored": {"sha256": result.stored.sha256, "byte_length": result.stored.byte_length}}
        return manifest

    def reject(self, h, manifest, message):
        with self.assertRaisesRegex(ProtocolError, message):
            validate_content(h.profile_output, self.snapshot, manifest)

    def test_accepts_the_writers_output_and_stores_one_checked_frame(self):
        h = self.build()
        files = self.manifest["files"]
        self.assertEqual(set(files), {"incidents.ndjson", "pair_profile.ndjson", "profile.ndjson",
                                      "availability.ndjson", "transitions.ndjson.zst"})
        stored = (h.profile_output / "transitions.ndjson.zst").read_bytes()
        entry = files["transitions.ndjson.zst"]
        self.assertEqual(entry["stored"], {"sha256": hashlib.sha256(stored).hexdigest(), "byte_length": len(stored)})
        payload = decoded(h.profile_output)
        self.assertEqual(entry["logical"], {"sha256": hashlib.sha256(payload).hexdigest(),
                                            "byte_length": len(payload), "records": payload.count(b"\n")})
        self.assertFalse((h.profile_output / "transitions.ndjson").exists())
        self.assertFalse((h.profile_output / "transitions.ndjson.open").exists())
        validate_content(h.profile_output, self.snapshot, self.manifest)

    def test_reading_never_creates_a_temporary_file(self):
        # The replay container mounts a small tmpfs at /tmp; decoded rows must not land there.
        h = self.harness(policy=v2("transitions"), mixed=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        send(h, 13, (PM, operations(op(PM, "bid", 400, "increase", M))))
        h.finish()
        snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        manifest = json.loads((h.profile_output / "manifest.json").read_bytes())

        def forbidden(*args, **kwargs):
            raise AssertionError("temporary file requested")

        with patch("tempfile.TemporaryDirectory", forbidden), patch("tempfile.mkdtemp", forbidden), \
                patch("tempfile.mkstemp", forbidden), patch("tempfile.TemporaryFile", forbidden), \
                patch("tempfile.NamedTemporaryFile", forbidden), patch("tempfile.SpooledTemporaryFile", forbidden):
            summary = validate_content(h.profile_output, snapshot, manifest)
        self.assertEqual(summary["transition_rows"]["total"], 2)
        # Identities are still verified before any row: a wrong digest fails without reading rows.
        manifest["files"]["transitions.ndjson.zst"]["logical"]["sha256"] = "0" * 64
        with patch("replay.economic_sdk.transitions_reader._row", forbidden):
            with self.assertRaisesRegex(ProtocolError, "codec"):
                validate_content(h.profile_output, snapshot, manifest)

    def test_rejects_a_tampered_row(self):
        h = self.build()
        for name, edit, message in (
                ("reason", lambda rows: rows[3].update(reason="unknown" if rows[3]["reason"] == "insert" else "insert"),
                 "reason"),
                ("flow", lambda rows: rows[3].update(bid_added=str(int(rows[3]["bid_added"]) + 1),
                                                     bid_best_added=str(int(rows[3]["bid_best_added"]) + 1)),
                 "best level arithmetic"),
                ("move", lambda rows: rows[3].update(bid_move_atoms="7"), "move atoms"),
                ("depletion", lambda rows: rows[4].update(bid_depleted=True), "depletion|worsened|arithmetic")):
            with self.subTest(name):
                manifest = self.tampered(h, edit)
                self.reject(h, manifest, message)

    def test_rejects_a_broken_chain(self):
        h = self.build()
        rows = self.original_rows()
        victim = next(i for i, r in enumerate(rows) if r["t_ns"] == "13")
        manifest = self.tampered(h, lambda rows: rows.pop(victim))
        self.reject(h, manifest, "chain")

    def test_rejects_a_changed_projected_ask(self):
        h = self.build()
        rows = self.original_rows()
        victim = next(i for i, r in enumerate(rows) if r["t_ns"] == "16")
        manifest = self.tampered(h, lambda rows: rows[victim].update(prev_ask=["999", "1"], ask=["999", "1"]))
        self.reject(h, manifest, "projected|move|arithmetic")

    def test_rejects_wrong_identities_and_file_sets(self):
        h = self.build()
        for part, field in (("logical", "sha256"), ("logical", "byte_length"), ("logical", "records"),
                            ("stored", "sha256"), ("stored", "byte_length")):
            with self.subTest(part, field=field):
                manifest = copy.deepcopy(self.manifest)
                entry = manifest["files"]["transitions.ndjson.zst"][part]
                entry[field] = ("0" * 64) if field == "sha256" else entry[field] + 1
                self.reject(h, manifest, "codec|transition|identity")
        missing = copy.deepcopy(self.manifest)
        del missing["files"]["transitions.ndjson.zst"]
        self.reject(h, missing, "output file set")
        extra = copy.deepcopy(self.manifest)
        extra["files"]["transitions.ndjson"] = extra["files"]["profile.ndjson"]
        self.reject(h, extra, "output file set")
        (h.profile_output / "transitions.ndjson.zst").rename(h.profile_output / "gone")
        with self.assertRaises((ProtocolError, OSError)):
            validate_content(h.profile_output, self.snapshot, self.manifest)

    def test_rejects_unknown_keys_and_trailing_or_concatenated_frames(self):
        h = self.build()
        manifest = self.tampered(h, lambda rows: rows[0].update(surprise=1))
        self.reject(h, manifest, "closed schema")
        manifest = self.tampered(h, lambda rows: None)
        path = h.profile_output / "transitions.ndjson.zst"
        path.write_bytes(path.read_bytes() + b"\x00")
        with self.assertRaises(ProtocolError):
            validate_content(h.profile_output, self.snapshot, manifest)

    def test_rejects_rows_that_disagree_with_the_profile(self):
        h = self.build()
        rows = self.original_rows()
        last = max(i for i, r in enumerate(rows) if r["t_ns"] == "17")
        manifest = self.tampered(h, lambda rows: rows.pop(last))
        self.reject(h, manifest, "activity transitions|validity")

    def test_a_version_two_group_is_rejected_under_an_embedded_profile(self):
        with self.assertRaisesRegex(ProtocolError, "standalone"):
            profile_policy(v2("transitions"))
        with self.assertRaisesRegex(ProtocolError, "standalone"):
            Collector(v2("transitions"), {}, "0" * 64, self.root, "0" * 64)
        self.assertEqual(profile_policy(v2("transitions"), standalone=True)["groups"][-1], "transitions")
        policy = copy.deepcopy(PROFILE_POLICY)
        policy["groups"] = sorted(policy["groups"] + ["transitions"])
        with self.assertRaisesRegex(ProtocolError, "profile groups"):
            profile_policy(policy, standalone=True)


class RandomStreamTests(Case):
    """A long random stream: the writer's own reader (mirror, chains, cross-checks) must accept it."""

    def test_random_operations_snapshots_and_invalidations_validate(self):
        import random

        for seed in range(5):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                h = self.harness(mixed=True)
                levels = {key: {"bid": {}, "ask": {}} for key in (PM, PM2, KO, KC)}
                usable = dict.fromkeys(levels, False)
                times = sorted(rng.randrange(11, 40) for _ in range(240))
                for time in times:
                    changes = []
                    for key in sorted(rng.sample(sorted(levels), rng.randrange(1, 4))):
                        book, kalshi = levels[key], key[0].startswith("kalshi")
                        if not usable[key] or rng.random() < 0.06:
                            if usable[key] and rng.random() < 0.5:
                                changes.append((key, {"kind": "invalidation", "reason": {"kind": "connection_closed"}}))
                                usable[key] = False
                                book["bid"].clear(), book["ask"].clear()
                                continue
                            bids = {rng.randrange(300, 500, 10): rng.randrange(1, 9) * M for _ in range(rng.randrange(4))}
                            asks = {} if kalshi else {rng.randrange(500, 700, 10): rng.randrange(1, 9) * M
                                                      for _ in range(rng.randrange(4))}
                            changes.append((key, snapshot(sorted(bids.items(), reverse=True), sorted(asks.items()))))
                            book["bid"], book["ask"], usable[key] = bids, asks, True
                            continue
                        ops = []
                        for _ in range(rng.randrange(1, 5)):
                            side = "bid" if kalshi or rng.random() < 0.5 else "ask"
                            price = rng.randrange(300, 500, 10) if side == "bid" else rng.randrange(500, 700, 10)
                            held = book[side].get(price, 0)
                            how = rng.choice(("set", "delete", "increase", "decrease"))
                            if how == "decrease" and held:
                                amount = rng.randrange(1, held // M + 1) * M
                                book[side][price] = held - amount
                                ops.append(op(key, side, price, how, amount))
                            elif how == "increase":
                                amount = rng.randrange(1, 4) * M
                                book[side][price] = held + amount
                                ops.append(op(key, side, price, how, amount))
                            elif how == "set":
                                amount = rng.randrange(0, 9) * M
                                ops.append(op(key, side, price, "set" if amount else "delete", amount or None))
                                book[side][price] = amount
                            else:
                                ops.append(op(key, side, price, "delete"))
                                book[side][price] = 0
                            if not book[side][price]:
                                del book[side][price]
                        changes.append((key, operations(*ops)))
                    send(h, time, *changes)
                result = h.finish()
                self.assertGreater(result["summary"]["transition_rows"]["total"], 100)


if __name__ == "__main__":
    unittest.main()
