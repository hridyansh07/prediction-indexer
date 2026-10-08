"""Market profile ``transitions`` (top and trade rows) and ``levels`` (ladders and diffs) groups."""

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
from replay.economic_sdk.profile_streams import iter_levels, iter_transitions
from replay.economic_sdk.transitions import book_table
from replay.economic_sdk.transitions_reader import tables
from replay.preparation import load_snapshot
from replay.streams.protocol import Book, ProtocolError
from replay.strategies.market_profile.strategy import validate_content
from replay.tests.economic_scenarios import M, PROFILE_POLICY, ladder
from replay.tests.test_market_profile import Harness
from replay.tests.test_market_profile_v2 import v2

BOTH = v2("levels", "transitions")
TOPS = v2("transitions")
LEVELS = v2("levels")
PM = ("polymarket:123", "outcome")
PM2 = ("polymarket:987", "outcome")
KO = ("kalshi:series", "outcome")
KC = ("kalshi:series", "complement")
FILES = {"transitions": "transitions.ndjson.zst", "levels": "levels.ndjson.zst"}


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


def trade_event(h, time, key, disposition, quantity, aggressor=None, venue_time=None, scale=6, price=400):
    return {"reference": h.ref(time), "disposition": disposition,
            "event": {"kind": "trade", "value": {
                "instrument": key[0], "orientation": key[1], "price": number(price, 3),
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


def invalid(reason="connection_closed"):
    return {"kind": "invalidation", "reason": {"kind": reason}}


def send(h, time, *changes, events=()):
    """One cut: ``changes`` are ``(key, decision)`` pairs, built before the decoder sees them."""
    transitions = [transition(h, time, key, decision) for key, decision in changes]
    ref = h.ref(time)
    return h.send("cut", {"origin": {"kind": "group", "pin": h.pin, "first": ref["address"],
                                     "last": ref["address"], "visible_ns": str(time)},
                          "market_events": list(events), "book_transitions": transitions})


def decoded(path, name="transitions"):
    sink = io.BytesIO()
    entry = json.loads((path / "manifest.json").read_bytes())["files"][FILES[name]]
    logical, stored = entry["logical"], entry["stored"]
    with (path / FILES[name]).open("rb") as source:
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

    def harness(self, policy=BOTH, **kwargs):
        self.count += 1
        path = self.root / f"h{self.count}"
        path.mkdir()
        h = Harness(path, policy=policy, **kwargs)
        h.window()
        return h

    def rows(self, h, key=None, name="transitions", result=None, kind=None):
        result = result or getattr(h, "result", None) or h.finish()
        h.result = result
        table = result["summary"]["transition_books"]
        rows = [json.loads(line) for line in decoded(h.profile_output, name).splitlines()]
        for row in rows:
            row["key"] = (table[row["book"]]["instrument"], table[row["book"]]["orientation"])
        return [r for r in rows if (key is None or r["key"] == key) and (kind is None or r["type"] == kind)]


def tops(case, h, key, result=None):
    return case.rows(h, key, "transitions", result, "top")


def quote(price, quantity):
    return [str(price), str(quantity)]


class TopRowTests(Case):
    def test_only_a_changed_top_or_validity_writes_a_row(self):
        h = self.harness(TOPS)
        ladder(h, 12, PM[0], bids=((400, 5 * M), (390, 3 * M)), asks=((450, 2 * M), (460, 4 * M)))
        send(h, 13, (PM, operations(op(PM, "bid", 390, "set", M))))             # depth only
        send(h, 14, (PM, operations(op(PM, "ask", 460, "delete"), op(PM, "bid", 380, "set", M))))  # depth only
        send(h, 15, (PM, operations(op(PM, "bid", 400, "increase", M))))        # best quantity
        send(h, 16, (PM, operations(op(PM, "ask", 440, "set", M))))             # better best price
        send(h, 17, (PM, operations(op(PM, "bid", 400, "delete"))))             # best level removed
        send(h, 18, (PM, invalid()))                                            # validity
        rows = tops(self, h, PM)
        self.assertEqual([(r["t_ns"], r["cause"], r["validity"]) for r in rows],
                         [("10", "open", "not_initialized"), ("12", "snapshot", "usable"),
                          ("15", "operations", "usable"), ("16", "operations", "usable"),
                          ("17", "operations", "usable"), ("18", "invalidation", "unusable:connection_closed")])
        snapshot_row, quantity, price, removed, gone = rows[1:]
        self.assertEqual((snapshot_row["bid"], snapshot_row["ask"]), (quote(400, 5 * M), quote(450, 2 * M)))
        self.assertEqual((quantity["bid"], quantity["ask"]), (quote(400, 6 * M), quote(450, 2 * M)))
        self.assertEqual(price["ask"], quote(440, M))
        self.assertEqual(removed["bid"], quote(390, M))
        self.assertEqual((gone["bid"], gone["ask"]), (None, None))
        self.assertEqual({r["cut"] for r in rows if r["cause"] == "open"}, {2})
        self.assertEqual({"top"}, {r["type"] for r in rows})

    def test_a_snapshot_or_invalidation_with_an_unchanged_top_still_writes_a_row(self):
        h = self.harness(TOPS, mixed=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement")                                          # no bids: no projected ask
        ladder(h, 14, PM[0], bids=((400, 5 * M), (390, M)), asks=((450, 2 * M),))   # a resync, same top
        send(h, 15, (KC, invalid()))                                                 # Yes top is unchanged
        rows = tops(self, h, PM)
        self.assertEqual([(r["t_ns"], r["cause"]) for r in rows], [("10", "open"), ("12", "snapshot"),
                                                                    ("14", "snapshot")])
        self.assertEqual(rows[1]["bid"], rows[2]["bid"])
        self.assertEqual(rows[1]["ask"], rows[2]["ask"])
        yes = tops(self, h, KO)
        self.assertEqual([(r["t_ns"], r["cause"]) for r in yes],
                         [("10", "open"), ("12", "snapshot"), ("12", "snapshot"), ("15", "invalidation")])
        self.assertEqual((yes[3]["validity"], yes[3]["bid"], yes[3]["ask"]), ("usable", yes[2]["bid"], None))
        self.assertEqual(yes[2]["ask"], None)    # the complement snapshot had no bid to project


class KalshiTests(Case):
    def test_kalshi_is_written_only_as_the_yes_book(self):
        h = self.harness(mixed=True)
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M), (390, M)))
        send(h, 13, (KC, operations(op(KC, "bid", 380, "set", M))))                  # below the top: nothing
        send(h, 14, (KC, operations(op(KC, "bid", 400, "increase", M))))             # projected ask quantity
        send(h, 15, (KC, operations(op(KC, "bid", 450, "set", M))))                  # projected ask moves
        send(h, 16, (KO, operations(op(KO, "bid", 560, "increase", M))))
        result = h.finish()
        table = result["summary"]["transition_books"]
        self.assertNotIn(KC, [(t["instrument"], t["orientation"]) for t in table])
        yes = next(t for t in table if (t["instrument"], t["orientation"]) == KO)
        self.assertEqual((yes["ask_source"], yes["ask_source_book"]), ("projected", list(KC)))
        for name in ("transitions", "levels"):
            self.assertEqual({r["key"] for r in self.rows(h, None, name, result)} & {KC}, set())
        rows = tops(self, h, KO, result)
        self.assertEqual([(r["t_ns"], r["cause"]) for r in rows],
                         [("10", "open"), ("12", "snapshot"), ("12", "snapshot"), ("14", "operations"),
                          ("15", "operations"), ("16", "operations")])
        self.assertEqual((rows[2]["bid"], rows[2]["ask"]), (quote(560, 2 * M), quote(600, M)))   # 1000 - 400
        self.assertEqual(rows[3]["ask"], quote(600, 2 * M))
        self.assertEqual((rows[4]["bid"], rows[4]["ask"]), (quote(560, 2 * M), quote(550, M)))   # 1000 - 450
        self.assertEqual(rows[5]["bid"], quote(560, 3 * M))

    def test_the_stronger_cause_wins_when_both_orientations_transition(self):
        h = self.harness(mixed=True)
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M),))
        send(h, 13, (KO, operations(op(KO, "bid", 560, "increase", M))),
             (KC, operations(op(KC, "bid", 400, "increase", M))))
        send(h, 14, (KO, operations(op(KO, "bid", 560, "increase", M))), (KC, snapshot(((410, M),))))
        send(h, 15, (KO, operations(op(KO, "bid", 560, "increase", M))), (KC, invalid()))
        send(h, 16, (KC, snapshot(((420, M),))), (KO, invalid()))
        result = h.finish()
        causes = [(r["t_ns"], r["cause"]) for r in tops(self, h, KO, result)][3:]
        self.assertEqual(causes, [("13", "operations"), ("14", "snapshot"), ("15", "invalidation"),
                                  ("16", "invalidation")])
        two = [r for r in tops(self, h, KO, result) if r["t_ns"] == "13"][0]
        self.assertEqual((two["bid"], two["ask"]), (quote(560, 3 * M), quote(600, 2 * M)))


class TradeTests(Case):
    def test_one_row_per_non_duplicate_trade_and_the_skips_are_counted(self):
        h = self.harness(mixed=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M),))
        events = [trade_event(h, 14, PM, "applied", 2 * M, "ask", clock("trade_report", 900)),
                  trade_event(h, 14, PM, "duplicate", 9 * M, "bid", clock("trade_report", 1)),
                  trade_event(h, 14, PM, "observed", M, None),
                  trade_event(h, 14, PM, "applied", 7, "bid", scale=5),               # scale mismatch
                  trade_event(h, 14, KC, "applied", 3 * M, "bid"),                    # unwritten book
                  trade_event(h, 14, KO, "not_authority", M, "ask"),
                  trade_event(h, 14, PM2, "invalidated", M, "bid")]
        send(h, 14, (PM, operations(op(PM, "bid", 400, "decrease", M))), events=events)
        result = h.finish()
        trades = self.rows(h, None, "transitions", result, "trade")
        self.assertEqual([(r["key"], r["disposition"], r["qty"], r["aggressor"]) for r in trades],
                         [(KO, "not_authority", str(M), "ask"), (PM, "applied", str(2 * M), "ask"),
                          (PM, "observed", str(M), None), (PM2, "invalidated", str(M), "bid")])
        self.assertEqual({r["cut"] for r in trades}, {5})
        self.assertEqual(result["summary"]["transition_trades_skipped"], {"scale_mismatch": 1, "unwritten_book": 1})
        self.assertEqual(trades[1]["price"], "400")
        self.assertEqual("venue_ns" in trades[1], True)
        self.assertEqual("venue_ns" in trades[2], False)

    def test_order_within_a_cut_and_trades_never_change_a_top_row(self):
        h = self.harness()
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        ladder(h, 12, PM2[0], bids=((400, M),), asks=((450, M),))
        events = [trade_event(h, 15, PM2, "applied", M), trade_event(h, 15, PM, "applied", M),
                  trade_event(h, 15, PM, "observed", 2 * M)]
        send(h, 15, (PM, operations(op(PM, "bid", 400, "decrease", M))), events=events)
        send(h, 16, events=[trade_event(h, 16, PM, "applied", M)])                      # trade-only cut
        rows = [r for r in self.rows(h, result=h.finish()) if r["cut"] in (4, 5)]
        self.assertEqual([(r["type"], r["key"], r["cut"]) for r in rows],
                         [("top", PM, 4), ("trade", PM, 4), ("trade", PM, 4), ("trade", PM2, 4),
                          ("trade", PM, 5)])
        table_order = [r["book"] for r in rows[:4]]
        self.assertEqual(table_order, sorted(table_order))
        tops_of_pm = [r for r in self.rows(h, PM, kind="top") if r["t_ns"] != "10"]
        self.assertEqual([r["t_ns"] for r in tops_of_pm], ["12", "15"])   # no top row at 16


class VenueTimeTests(Case):
    def test_flat_optional_keys(self):
        h = self.harness(TOPS)
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        ladder(h, 12, PM2[0], bids=((400, M),), asks=((450, M),))

        def change(time, *events, other=False):
            send(h, time, (PM, operations(op(PM, "bid", 400, "increase", M))), events=events)

        change(14, book_event(h, 14, PM, clock(at=1500, sent=1600)), book_event(h, 14, PM, clock(at=1000, sent=1100)),
               book_event(h, 14, PM2, clock(at=1)))                     # another book's event does not count
        change(15, book_event(h, 15, PM, clock(at=20)))                  # a single event
        change(16, book_event(h, 16, PM, clock("book_update", 20)),
               book_event(h, 16, PM, clock("book_as_of", 30, resolution="microsecond")))
        change(17, book_event(h, 17, PM, clock(at=None, sent=77)))      # sent time alone
        change(18)                                                       # no venue time
        rows = {r["t_ns"]: r for r in tops(self, h, PM)}
        venue = ("venue_ns", "venue_first_ns", "venue_kind", "venue_res", "sent_ns")
        pick = lambda r: {k: r[k] for k in venue if k in r}
        self.assertEqual(pick(rows["14"]), {"venue_ns": "1500", "venue_first_ns": "1000", "venue_kind": "book_update",
                                             "venue_res": "millisecond", "sent_ns": "1600"})
        self.assertEqual(pick(rows["15"]), {"venue_ns": "20", "venue_kind": "book_update", "venue_res": "millisecond"})
        self.assertEqual(pick(rows["16"]), {"venue_ns": "30", "venue_first_ns": "20", "venue_kind": "mixed",
                                             "venue_res": "mixed"})
        self.assertEqual(pick(rows["17"]), {"sent_ns": "77"})
        self.assertEqual(pick(rows["18"]), {})
        self.assertEqual(pick(rows["12"]), {})
        self.assertFalse([v for r in rows.values() for k, v in r.items() if k in venue and v is None])


class ScopeTests(Case):
    def test_opening_rows_reflect_pre_start_cuts_and_pre_start_cuts_write_nothing(self):
        h = self.harness(mixed=True, lower_bound="expand_to_window_start")
        ladder(h, 5, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        send(h, 7, (PM, operations(op(PM, "bid", 400, "increase", M))))            # still before the start
        ladder(h, 8, KO[0], "outcome", bids=((560, M),))
        ladder(h, 8, KC[0], "complement", bids=((400, M),))
        send(h, 12, (PM, operations(op(PM, "bid", 400, "increase", M))))
        result = h.finish()
        for name, kind in (("transitions", "top"), ("levels", "ladder")):
            opens = [r for r in self.rows(h, None, name, result) if r.get("cause") == "open"]
            self.assertEqual({(r["scope"], r["t_ns"], r["cut"]) for r in opens}, {(0, "10", 6)}, name)
        pm_open = tops(self, h, PM, result)[0]
        self.assertEqual((pm_open["validity"], pm_open["bid"], pm_open["ask"]),
                         ("usable", quote(400, 6 * M), quote(450, 2 * M)))
        ko_open = tops(self, h, KO, result)[0]
        self.assertEqual((ko_open["bid"], ko_open["ask"]), (quote(560, M), quote(600, M)))
        self.assertEqual([r["t_ns"] for r in tops(self, h, PM, result)], ["10", "12"])
        open_ladder = self.rows(h, PM, "levels", result)[0]
        self.assertEqual((open_ladder["type"], open_ladder["bids"]), ("ladder", [quote(400, 6 * M)]))
        # The first gated cut is the one that wrote them, before its own row.
        self.assertEqual(tops(self, h, PM, result)[1]["cut"], 6)

    def test_scope_zero_opens_at_terminal_when_no_cut_is_gated(self):
        h = self.harness(mixed=True, lower_bound="expand_to_window_start")
        ladder(h, 5, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        result = h.finish()
        rows = tops(self, h, PM, result)
        self.assertEqual([(r["cause"], r["t_ns"], r["bid"]) for r in rows], [("open", "10", quote(400, 5 * M))])
        self.assertEqual(rows[0]["cut"], h.seq - 1)       # the terminal cut

    def test_later_scopes_open_from_the_state_before_the_crossing_cut(self):
        h = self.harness(mixed=True, scopes=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        send(h, 26, (PM, operations(op(PM, "bid", 400, "increase", M))))            # crosses the 23 boundary
        result = h.finish()
        rows = [r for r in tops(self, h, PM, result)]
        self.assertEqual([(r["scope"], r["t_ns"], r["cause"], r["bid"], r["cut"]) for r in rows],
                         [(0, "10", "open", None, 2), (0, "12", "snapshot", quote(400, 5 * M), 2),
                          (1, "23", "open", quote(400, 5 * M), 3), (1, "26", "operations", quote(400, 6 * M), 3)])
        ladders = self.rows(h, PM, "levels", result)
        self.assertEqual([(r["type"], r["scope"], r["t_ns"]) for r in ladders],
                         [("ladder", 0, "10"), ("ladder", 0, "12"), ("ladder", 1, "23"), ("diff", 1, "26")])
        self.assertEqual(ladders[2]["bids"], [quote(400, 5 * M)])
        opens = result["summary"]["transition_scopes"]
        self.assertEqual([(s["start_ns"], s["end_ns"]) for s in opens], [("10", "23"), ("23", "40")])

    def test_the_first_gated_cut_may_cross_a_scope_boundary(self):
        h = self.harness(mixed=True, scopes=True)
        ladder(h, 26, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        result = h.finish()
        for name in ("transitions", "levels"):
            rows = [r for r in self.rows(h, PM, name, result)]
            self.assertEqual([(r["scope"], r["t_ns"], r["cut"]) for r in rows],
                             [(0, "10", 2), (1, "23", 2), (1, "26", 2)], name)
            self.assertEqual(rows[1]["bids"] if name == "levels" else rows[1]["bid"], [] if name == "levels" else None)

    def test_a_book_leaving_the_scope_writes_nothing_and_has_no_opening_row(self):
        h = self.harness(mixed=True, scopes="uncaptured")
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        send(h, 25, (PM, operations(op(PM, "bid", 400, "increase", M))))            # PM left the scope
        ladder(h, 26, KO[0], "outcome", bids=((560, M),))
        result = h.finish()
        self.assertEqual([(r["scope"], r["t_ns"]) for r in tops(self, h, PM, result)], [(0, "10"), (0, "12")])
        self.assertEqual([(r["scope"], r["t_ns"], r["cause"]) for r in tops(self, h, KO, result)],
                         [(0, "10", "open"), (1, "23", "open"), (1, "26", "snapshot")])
        self.assertEqual(len(result["summary"]["transition_scopes"]), 2)


class LevelTests(Case):
    def test_polymarket_set_up_down_delete_and_net_zero(self):
        h = self.harness(LEVELS)
        ladder(h, 12, PM[0], bids=((400, 5 * M), (390, 3 * M)), asks=((450, 2 * M), (460, 4 * M)))
        send(h, 13, (PM, operations(op(PM, "ask", 450, "set", 3 * M))))
        send(h, 14, (PM, operations(op(PM, "bid", 400, "set", 2 * M), op(PM, "bid", 390, "set", 6 * M))))
        send(h, 15, (PM, operations(op(PM, "bid", 400, "delete"), op(PM, "ask", 470, "set", M))))
        send(h, 16, (PM, operations(op(PM, "bid", 395, "set", M), op(PM, "bid", 395, "delete"))))   # nets to zero
        send(h, 17, (PM, operations(op(PM, "ask", 450, "set", 3 * M))))                              # no change
        send(h, 18, (PM, operations(op(PM, "ask", 450, "delete"), op(PM, "bid", 380, "increase", M),
                                    op(PM, "ask", 460, "decrease", M))))
        rows = self.rows(h, PM, "levels")
        self.assertEqual([(r["type"], r["t_ns"]) for r in rows],
                         [("ladder", "10"), ("ladder", "12"), ("diff", "13"), ("diff", "14"), ("diff", "15"),
                          ("diff", "18")])
        self.assertEqual(rows[1]["asks"], [quote(450, 2 * M), quote(460, 4 * M)])
        self.assertEqual(rows[1]["bids"], [quote(400, 5 * M), quote(390, 3 * M)])
        self.assertEqual(rows[2]["levels"], [["ask", "450", str(M)]])
        self.assertEqual(rows[3]["levels"], [["bid", "390", str(3 * M)], ["bid", "400", str(-3 * M)]])
        self.assertEqual(rows[4]["levels"], [["bid", "400", str(-2 * M)], ["ask", "470", str(M)]])
        self.assertEqual(rows[5]["levels"], [["bid", "380", str(M)], ["ask", "450", str(-3 * M)],
                                             ["ask", "460", str(-M)]])
        self.assertEqual([r["cut"] for r in rows[2:]], [3, 4, 5, 8])

    def test_kalshi_both_orientations_map_into_one_yes_diff_with_projected_ask_prices(self):
        h = self.harness(LEVELS, mixed=True)
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M), (380, 2 * M)))
        send(h, 13, (KC, operations(op(KC, "bid", 400, "increase", 2 * M), op(KC, "bid", 350, "set", M))),
             (KO, operations(op(KO, "bid", 560, "decrease", M), op(KO, "bid", 500, "set", M))))
        send(h, 14, (KC, operations(op(KC, "bid", 380, "increase", M), op(KC, "bid", 380, "decrease", M))))
        send(h, 15, (KC, operations(op(KC, "bid", 400, "delete"))))
        result = h.finish()
        rows = self.rows(h, KO, "levels", result)
        self.assertEqual([r["type"] for r in rows], ["ladder", "ladder", "ladder", "diff", "diff"])
        ladder_row = rows[2]
        self.assertEqual((ladder_row["bids"], ladder_row["asks"]),
                         ([quote(560, 2 * M)], [quote(600, M), quote(620, 2 * M)]))   # 1000-400, 1000-380
        self.assertEqual(rows[3]["levels"], [["bid", "500", str(M)], ["bid", "560", str(-M)],
                                             ["ask", "600", str(2 * M)], ["ask", "650", str(M)]])
        self.assertEqual(rows[4]["levels"], [["ask", "600", str(-3 * M)]])   # cut 14 nets to zero: no row
        self.assertEqual([r["cut"] for r in rows[3:]], [4, 6])

    def test_snapshot_and_invalidation_write_ladders(self):
        h = self.harness(LEVELS, mixed=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M),))
        ladder(h, 14, PM[0], bids=((380, M), (370, M)), asks=((420, M),))
        send(h, 15, (PM, invalid()))
        ladder(h, 16, PM[0], bids=((300, M),), asks=((310, M),))
        send(h, 17, (KC, invalid()))                                                     # the Yes book keeps its bids
        ladder(h, 18, KC[0], "complement", bids=((300, M),))
        result = h.finish()
        pm = self.rows(h, PM, "levels", result)
        self.assertEqual([(r["cause"], r["validity"], r["bids"], r["asks"]) for r in pm[2:4]],
                         [("snapshot", "usable", [quote(380, M), quote(370, M)], [quote(420, M)]),
                          ("invalidation", "unusable:connection_closed", [], [])])
        yes = self.rows(h, KO, "levels", result)
        self.assertEqual([(r["cause"], r["asks"]) for r in yes[3:]],
                         [("invalidation", []), ("snapshot", [quote(700, M)])])
        self.assertEqual(yes[3]["bids"], [quote(560, 2 * M)])
        self.assertEqual(yes[3]["validity"], "usable")

    def test_a_corrupted_operation_stream_fails_the_run(self):
        h = self.harness(LEVELS)
        ladder(h, 12, PM[0], bids=((400, 5 * M), (390, M)), asks=((450, 2 * M),))
        real = Book._operations

        def lossy(book, operations):  # the decoder drops a delete that the stream carries
            return real(book, [o for o in operations if o["change"]["kind"] != "delete"])

        with patch.object(Book, "_operations", lossy):
            with self.assertRaisesRegex(ProtocolError, "mirror diverged"):
                send(h, 13, (PM, operations(op(PM, "bid", 400, "delete"))))
        self.assertTrue(h.strategy.poisoned)

    def test_a_ladder_over_the_line_bound_fails_and_is_never_truncated(self):
        with patch("replay.economic_sdk.levels.LEVELS_MAX_LINE", 400):
            h = self.harness(LEVELS)
            with self.assertRaisesRegex(ProtocolError, "output line budget"):
                ladder(h, 12, PM[0], bids=tuple((339 - i, M) for i in range(40)), asks=((450, 2 * M),))
        self.assertTrue(h.strategy.poisoned)

    def test_levels_alone_and_transitions_alone_are_independent(self):
        for policy, present, absent in ((LEVELS, "levels", "transitions"), (TOPS, "transitions", "levels")):
            with self.subTest(present):
                h = self.harness(policy)
                ladder(h, 12, PM[0], bids=((400, 5 * M),), asks=((450, 2 * M),))
                result = h.finish()
                self.assertIn(FILES[present], result["manifest"]["files"])
                self.assertNotIn(FILES[absent], result["manifest"]["files"])
                self.assertFalse((h.profile_output / FILES[absent]).exists())
                self.assertIn("transition_books", result["summary"])
                self.assertEqual("level_rows" in result["summary"], present == "levels")
                self.assertEqual("transition_rows" in result["summary"], present == "transitions")


class TableTests(Case):
    """The recorder and the independent reader derive the written-book table separately."""

    def derive(self, mutate):
        h = self.harness(mixed=True, scopes=True)
        snapshot = copy.deepcopy(h.profile.snapshot)
        mutate(snapshot)
        plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
        policy = h.profile.policy
        books, index, scoped = tables(snapshot, policy)
        self.assertEqual(books, book_table(snapshot, plans, policy))
        return books, index, scoped

    def test_a_lone_kalshi_orientation_is_written_as_itself_with_no_projected_source(self):
        def only_complement(snapshot):
            snapshot["plans"] = [p for p in snapshot["plans"] if (p["instrument"], p["orientation"]) != KO]
            for scope in snapshot["scopes"]:
                for member in scope["members"]:
                    member["books"] = [b for b in member["books"] if (b["instrument"], b["orientation"]) != KO]

        books, _, _ = self.derive(only_complement)
        kalshi = [b for b in books if b["venue"] == "kalshi"]
        self.assertEqual([(b["orientation"], b["ask_source"], b["ask_source_book"]) for b in kalshi],
                         [("complement", "projected", None)])
        books, _, _ = self.derive(lambda snapshot: None)
        self.assertEqual([(b["orientation"], b["ask_source_book"]) for b in books if b["venue"] == "kalshi"],
                         [("outcome", list(KC))])

    def test_scope_membership_follows_each_scopes_own_books(self):
        def three_scopes(snapshot):
            first, second = snapshot["scopes"]
            for member in second["members"]:
                member["books"] = [b for b in member["books"] if b["instrument"] != PM[0]]
            snapshot["scopes"] = [first, second, copy.deepcopy(first)]

        books, index, scoped = self.derive(three_scopes)
        pm = index[PM]
        self.assertEqual([pm in s for s in scoped], [True, False, True])    # the book leaves and re-enters
        self.assertEqual(len(scoped), 3)


class ReaderTests(Case):
    def build(self):
        h = self.harness(mixed=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M), (390, 2 * M)), asks=((450, 2 * M),))
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M),))
        send(h, 13, (PM, operations(op(PM, "bid", 400, "increase", M))))
        send(h, 14, (PM, operations(op(PM, "bid", 390, "decrease", M))))
        send(h, 15, (KC, operations(op(KC, "bid", 450, "increase", M))))
        send(h, 16, (KO, operations(op(KO, "bid", 300, "increase", M))))
        send(h, 17, (PM, operations(op(PM, "bid", 400, "decrease", 4 * M))),
             events=[trade_event(h, 17, PM, "applied", M), trade_event(h, 17, KC, "applied", M),
                     trade_event(h, 17, PM, "applied", 5, scale=5)])
        send(h, 18, (PM, invalid()))
        ladder(h, 19, PM[0], bids=((400, M),), asks=((450, M),))
        h.finish()
        self.snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        self.manifest = json.loads((h.profile_output / "manifest.json").read_bytes())
        self.original = {n: decoded(h.profile_output, n) for n in FILES}
        self.frames = {n: (h.profile_output / f).read_bytes() for n, f in FILES.items()}
        return h

    def original_rows(self, name):
        return [json.loads(line) for line in self.original[name].splitlines()]

    def tampered(self, h, name, edit):
        rows = self.original_rows(name)
        edit(rows)
        payload = b"".join(json.dumps(r, sort_keys=True, separators=(",", ":")).encode() + b"\n" for r in rows)
        sink = io.BytesIO()
        result = encode_stream(io.BytesIO(payload), sink)
        (h.profile_output / FILES[name]).write_bytes(sink.getvalue())
        manifest = copy.deepcopy(self.manifest)
        manifest["files"][FILES[name]] = {
            "logical": {"sha256": result.logical.sha256, "byte_length": result.logical.byte_length,
                        "records": result.logical.line_count},
            "stored": {"sha256": result.stored.sha256, "byte_length": result.stored.byte_length}}
        return manifest

    def restore(self, h):
        for name, frame in self.frames.items():
            (h.profile_output / FILES[name]).write_bytes(frame)

    def reject(self, h, manifest, message):
        with self.assertRaisesRegex(ProtocolError, message):
            validate_content(h.profile_output, self.snapshot, manifest)
        self.restore(h)

    def find(self, name, **fields):
        return next(i for i, r in enumerate(self.original_rows(name)) if all(r.get(k) == v for k, v in fields.items()))

    def test_accepts_the_writers_output_and_stores_checked_frames(self):
        h = self.build()
        files = self.manifest["files"]
        self.assertEqual(set(files), {"incidents.ndjson", "pair_profile.ndjson", "profile.ndjson",
                                      "transitions.ndjson.zst", "levels.ndjson.zst"})
        for name in FILES:
            stored = (h.profile_output / FILES[name]).read_bytes()
            entry = files[FILES[name]]
            self.assertEqual(entry["stored"], {"sha256": hashlib.sha256(stored).hexdigest(), "byte_length": len(stored)})
            payload = self.original[name]
            self.assertEqual(entry["logical"], {"sha256": hashlib.sha256(payload).hexdigest(),
                                                "byte_length": len(payload), "records": payload.count(b"\n")})
            self.assertFalse((h.profile_output / (name + ".ndjson")).exists())
            self.assertFalse((h.profile_output / (name + ".ndjson.open")).exists())
        summary = validate_content(h.profile_output, self.snapshot, self.manifest)
        self.assertEqual(summary["transition_trades_skipped"], {"scale_mismatch": 1, "unwritten_book": 1})
        self.assertEqual(summary["transition_rows"]["by_type"]["trade"], 1)
        self.assertEqual(summary["level_rows"]["by_cause"], {"invalidation": 1, "open": 3, "snapshot": 4})

    def test_reading_never_creates_a_temporary_file(self):
        # The replay container mounts a small tmpfs at /tmp; decoded rows must not land there.
        h = self.build()

        def forbidden(*args, **kwargs):
            raise AssertionError("temporary file requested")

        with patch("tempfile.TemporaryDirectory", forbidden), patch("tempfile.mkdtemp", forbidden), \
                patch("tempfile.mkstemp", forbidden), patch("tempfile.TemporaryFile", forbidden), \
                patch("tempfile.NamedTemporaryFile", forbidden), patch("tempfile.SpooledTemporaryFile", forbidden):
            validate_content(h.profile_output, self.snapshot, self.manifest)
        # Identities are verified before any row: a wrong digest fails without reading rows.
        manifest = copy.deepcopy(self.manifest)
        manifest["files"]["levels.ndjson.zst"]["logical"]["sha256"] = "0" * 64
        with patch("replay.economic_sdk.transitions_reader._Levels.row", forbidden):
            with self.assertRaisesRegex(ProtocolError, "codec"):
                validate_content(h.profile_output, self.snapshot, manifest)

    def test_rejects_tampered_transition_rows(self):
        h = self.build()
        first = self.find("transitions", type="top", cause="operations")
        trade = self.find("transitions", type="trade")
        snap = self.find("transitions", type="top", book=1, cause="snapshot")
        for why, edit, message in (
                ("quote", lambda rows: rows[first].update(bid=quote(401, 7)), "replayed ladder|top row"),
                ("unknown key", lambda rows: rows[0].update(surprise=1), "closed schema"),
                ("null venue key", lambda rows: rows[first].update(venue_ns=None), "canonical|venue"),
                ("venue order", lambda rows: rows[first].update(venue_ns="5", venue_first_ns="9", venue_kind="book_update",
                                                                venue_res="millisecond"), "venue time order"),
                ("venue first alone", lambda rows: rows[first].update(venue_first_ns="9"), "venue time"),
                ("missing open row", lambda rows: rows.pop(self.find("transitions", type="top", book=1, cause="open")),
                 "first top row is not an open row|missing open row"),
                ("no-op top", lambda rows: rows[first].update(bid=rows[snap]["bid"], ask=rows[snap]["ask"]),
                 "top row without a change"),
                ("complement book reference", lambda rows: rows[0].update(book=99), "outside scope"),
                ("duplicate disposition", lambda rows: rows[trade].update(disposition="duplicate"), "trade fields"),
                ("bad aggressor", lambda rows: rows[trade].update(aggressor="up"), "trade fields"),
                ("trade before open", lambda rows: rows.insert(0, rows[trade]), "row order|trade before"),
                ("row order", lambda rows: rows.reverse(), "order|closed|row")):
            with self.subTest(why):
                self.reject(h, self.tampered(h, "transitions", edit), message)

    def test_rejects_tampered_level_rows(self):
        h = self.build()
        diff = self.find("levels", type="diff", book=1)
        pm_ladder = self.find("levels", type="ladder", cause="snapshot", book=1)
        for why, edit, message in (
                ("zero delta", lambda rows: rows[diff]["levels"][0].__setitem__(2, "0"), "diff entry range"),
                ("negative replay", lambda rows: rows[diff]["levels"][0].__setitem__(2, "-999999999999"), "negative"),
                ("top mismatch", lambda rows: rows[diff]["levels"][0].__setitem__(2, str(7 * M)),
                 "replayed ladder differs|levels change"),
                ("unsorted ladder", lambda rows: rows[pm_ladder]["bids"].reverse(), "ladder order"),
                ("empty diff", lambda rows: rows[diff].update(levels=[]), "diff levels"),
                ("unknown key", lambda rows: rows[diff].update(surprise=1), "closed schema"),
                ("null venue key", lambda rows: rows[diff].update(sent_ns=None), "canonical"),
                ("diff before ladder", lambda rows: rows.insert(0, rows[diff]), "row order|first level row"),
                ("dropped diff", lambda rows: rows.pop(diff), "replayed ladder differs|top row without")):
            with self.subTest(why):
                self.reject(h, self.tampered(h, "levels", edit), message)

    def test_rejects_wrong_identities_and_file_sets(self):
        h = self.build()
        for name in FILES.values():
            for part, field in (("logical", "sha256"), ("logical", "byte_length"), ("logical", "records"),
                                ("stored", "sha256"), ("stored", "byte_length")):
                with self.subTest(f"{name} {part} {field}"):
                    manifest = copy.deepcopy(self.manifest)
                    entry = manifest["files"][name][part]
                    entry[field] = ("0" * 64) if field == "sha256" else entry[field] + 1
                    self.reject(h, manifest, "codec|count|identity|budget")
            missing = copy.deepcopy(self.manifest)
            del missing["files"][name]
            self.reject(h, missing, "output file set")
        extra = copy.deepcopy(self.manifest)
        extra["files"]["transitions.ndjson"] = extra["files"]["profile.ndjson"]
        self.reject(h, extra, "output file set")
        (h.profile_output / "levels.ndjson.zst").rename(h.profile_output / "gone")
        with self.assertRaises((ProtocolError, OSError)):
            validate_content(h.profile_output, self.snapshot, self.manifest)
        (h.profile_output / "gone").rename(h.profile_output / "levels.ndjson.zst")

    def test_rejects_trailing_bytes_and_a_group_the_policy_did_not_ask_for(self):
        h = self.build()
        path = h.profile_output / "levels.ndjson.zst"
        path.write_bytes(path.read_bytes() + b"\x00")
        with self.assertRaises(ProtocolError):
            validate_content(h.profile_output, self.snapshot, self.manifest)
        self.restore(h)
        manifest = copy.deepcopy(self.manifest)
        manifest["policy"] = v2("transitions")
        manifest["files"].pop("levels.ndjson.zst")
        from replay.economic_sdk.profile_policy import profile_identity
        manifest["experiment_sha256"] = profile_identity(manifest["snapshot_sha256"], manifest["policy"])
        with self.assertRaisesRegex(ProtocolError, "unexpected output file"):
            validate_content(h.profile_output, self.snapshot, manifest)

    def test_rejects_rows_that_disagree_with_the_profile(self):
        h = self.build()
        rows = self.original_rows("transitions")
        pm_trade = self.find("transitions", type="trade")
        self.reject(h, self.tampered(h, "transitions", lambda r: r.pop(pm_trade)), "trade rows differ")
        extra = self.find("transitions", type="top", book=1, cause="operations")
        self.reject(h, self.tampered(h, "transitions", lambda r: r.pop(extra)),
                    "validity|replayed|top row without|levels")
        self.assertGreater(len(rows), 10)

    def test_a_version_two_group_is_rejected_under_an_embedded_profile(self):
        for group in ("transitions", "levels"):
            with self.assertRaisesRegex(ProtocolError, "standalone"):
                profile_policy(v2(group))
            with self.assertRaisesRegex(ProtocolError, "standalone"):
                Collector(v2(group), {}, "0" * 64, self.root, "0" * 64)
            self.assertIn(group, profile_policy(v2(group), standalone=True)["groups"])
        policy = copy.deepcopy(PROFILE_POLICY)
        policy["groups"] = sorted(policy["groups"] + ["levels"])
        with self.assertRaisesRegex(ProtocolError, "profile groups"):
            profile_policy(policy, standalone=True)


class SdkIteratorTests(Case):
    def test_iterators_rebuild_previous_quotes_moves_and_ladders(self):
        h = self.harness(mixed=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M), (390, 2 * M)), asks=((450, 2 * M),))
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M),))
        send(h, 13, (PM, operations(op(PM, "bid", 400, "delete"))))                      # best 400 -> 390: -10
        send(h, 14, (PM, operations(op(PM, "bid", 395, "set", M))))                      # +5 atoms: not a tick
        send(h, 15, (KC, operations(op(KC, "bid", 450, "increase", M))))                 # ask 600 -> 550
        send(h, 16, (PM, operations(op(PM, "ask", 450, "delete"))))                      # ask side empties
        send(h, 17, (PM, operations(op(PM, "bid", 395, "increase", M))))
        result = h.finish()
        books = result["summary"]["transition_books"]
        pm = next(i for i, b in enumerate(books) if (b["instrument"], b["orientation"]) == PM)
        yes = next(i for i, b in enumerate(books) if (b["instrument"], b["orientation"]) == KO)
        rows = [r for r in iter_transitions(h.profile_output / FILES["transitions"]) if r["type"] == "top"]
        mine = [r for r in rows if r["book"] == pm]
        self.assertEqual([(r["prev_bid"], r["bid_move_atoms"], r["bid_move_ticks"]) for r in mine],
                         [(None, None, None), (None, None, None), (quote(400, 5 * M), -10, -1),
                          (quote(390, 2 * M), 5, None), (quote(395, M), 0, 0), (quote(395, M), 0, 0)])
        self.assertEqual([(r["ask_move_atoms"], r["ask_move_ticks"]) for r in mine][-2:], [(None, None), (None, None)])
        self.assertEqual(mine[4]["prev_ask"], quote(450, 2 * M))
        projected = [r for r in rows if r["book"] == yes][-1]
        self.assertEqual((projected["prev_ask"], projected["ask"], projected["ask_move_atoms"],
                          projected["ask_move_ticks"]), (quote(600, M), quote(550, M), -50, -5))
        # Ladder reconstruction equals the decoder's books (Kalshi: the Yes book with projected asks).
        last = {}
        for row, ladder_after in iter_levels(h.profile_output / FILES["levels"]):
            last[row["book"]] = ladder_after
        decoder = h.decoder.books
        self.assertEqual(last[pm], (list(decoder[PM].levels("bid")), list(decoder[PM].levels("ask"))))
        self.assertEqual(last[yes], (list(decoder[KO].levels("bid")),
                                     [(1000 - p, q) for p, q in decoder[KC].levels("bid")][::1]))

    def test_end_to_end_with_activity_reconstructs_the_same_tops(self):
        h = self.harness(mixed=True, scopes=True)
        ladder(h, 12, PM[0], bids=((400, 5 * M), (390, 2 * M)), asks=((450, 2 * M), (470, M)))
        ladder(h, 12, KO[0], "outcome", bids=((560, 2 * M),))
        ladder(h, 12, KC[0], "complement", bids=((400, M), (390, M)))
        for time, change in ((13, (PM, operations(op(PM, "bid", 400, "increase", M), op(PM, "ask", 470, "set", 2 * M)))),
                             (14, (KC, operations(op(KC, "bid", 400, "decrease", M)))),
                             (15, (KO, operations(op(KO, "bid", 540, "set", M)))),
                             (18, (PM, operations(op(PM, "ask", 450, "delete")))),
                             (21, (KC, snapshot(((410, 3 * M),)))),
                             (26, (PM, operations(op(PM, "bid", 400, "set", 9 * M)))),
                             (28, (KO, operations(op(KO, "bid", 560, "decrease", M)))),
                             (30, (PM, invalid())),
                             (32, (PM, snapshot(((380, M),), ((420, M),))))):
            send(h, time, change, events=[trade_event(h, time, PM, "applied", M),
                                          book_event(h, time, change[0], clock(at=time * 1000))])
        result = h.finish()
        self.assertEqual(result["summary"]["transition_trades_skipped"], {"scale_mismatch": 0, "unwritten_book": 0})
        tops_by, replayed = {}, {}
        for row in iter_transitions(h.profile_output / FILES["transitions"]):
            if row["type"] == "top":
                tops_by[row["scope"], row["book"]] = (row["bid"], row["ask"])
        rows = 0
        for row, (bids, asks) in iter_levels(h.profile_output / FILES["levels"]):
            rows += 1
            replayed[row["scope"], row["book"]] = (quote(*bids[0]) if bids else None, quote(*asks[0]) if asks else None)
        self.assertEqual(replayed, tops_by)
        self.assertEqual({scope for scope, _ in replayed}, {0, 1})
        self.assertEqual(rows, result["summary"]["level_rows"]["total"])


class RandomStreamTests(Case):
    """A long random stream: the writer's own reader (mirror, chains, cross-checks) must accept it."""

    def test_random_operations_snapshots_and_invalidations_validate(self):
        import random

        for seed in range(5):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                h = self.harness(mixed=True, scopes=True)
                levels = {key: {"bid": {}, "ask": {}} for key in (PM, PM2, KO, KC)}
                usable = dict.fromkeys(levels, False)
                times = sorted(rng.randrange(11, 40) for _ in range(240))
                for time in times:
                    changes = []
                    for key in sorted(rng.sample(sorted(levels), rng.randrange(1, 4))):
                        book, kalshi = levels[key], key[0].startswith("kalshi")
                        if not usable[key] or rng.random() < 0.06:
                            if usable[key] and rng.random() < 0.5:
                                changes.append((key, invalid()))
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
                    events = [trade_event(h, time, rng.choice((PM, PM2, KO, KC)), "applied", M)]
                    send(h, time, *changes, events=events)
                result = h.finish()
                self.assertGreater(result["summary"]["transition_rows"]["total"], 100)
                self.assertGreater(result["summary"]["level_rows"]["total"], 100)


if __name__ == "__main__":
    unittest.main()
