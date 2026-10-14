"""Offline clock tapes, independent recomputation and closed volatility V1 policy."""

import copy
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from encoder import encode_stream
from replay.economic_sdk import bounds
from replay.economic_sdk.profile_policy import VOLATILITY_FILE, profile_files, profile_identity, profile_policy
from replay.economic_sdk.profile_stream_io import stream_lines
from replay.economic_sdk.transitions_reader import tables
from replay.economic_sdk.volatility_format import accumulator_cost, pair_tables
from replay.economic_sdk.volatility_reader import _pair_evidence
from replay.preparation import encoded, load_snapshot, prepare
from replay.strategies.market_profile.strategy import read_provisional, validate_content
from replay.streams.protocol import Decoder, ProtocolError
from replay.tests.economic_scenarios import M, ladder
from replay.tests.test_market_profile import Harness
from replay.tests.test_market_profile_transitions import KC, KO, PM, PM2, invalid, op, operations, send, snapshot
from replay.tests.test_market_profile_v2 import exercise, v2
from replay.tests.economic_scenarios import PROFILE_POLICY
from replay.tests.test_preparation_outcomes import document

MS = 1_000_000


def policy(**overrides):
    return v2("transitions", "volatility", bucket_ns=str(10 * MS), volatility={
        "version": 1, "sample_ms": 1, "horizons_ms": [1, 3], "bucket_ms": 10,
        "jump_atoms": [100, 200, 500], "gap_pairs": "outcome_complements", **overrides})


def time_scale(value):
    """Scale only clock/retention fields in a small real Rust fixture, keeping Risk IDs."""
    if isinstance(value, list):
        return [time_scale(v) for v in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for k, v in value.items():
        if k in ("retain_boundaries_ns", "retain_shift_offsets_ns"):
            result[k] = [str(int(t) * MS) for t in v]
        elif k.endswith("_ns") and v is not None and type(v) is str:
            result[k] = str(int(v) * MS)
        else:
            result[k] = time_scale(v)
    return result


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.counter = 0

    def harness(self, profile=None, masks=True, price_scales=None, **kwargs):
        self.counter += 1
        root = self.root / str(self.counter)
        root.mkdir()

        def prepared(config, directory, **sources):
            config = time_scale(copy.deepcopy(config))
            for authority in config["authorities"]:
                if price_scales and authority["venue"] in price_scales:
                    authority["price_scale"] = str(price_scales[authority["venue"]])
            return prepare(config, directory, **sources, outcomes=(lambda _: document()) if masks else None)

        with patch("replay.tests.test_bundle_coverage.prepare", side_effect=prepared):
            h = Harness(root, policy=profile or policy(), **kwargs)
        self.addCleanup(self.close_writers, h)
        if "wire_records" not in kwargs:
            h.window(0, 40 * MS)
        return h

    @staticmethod
    def close_writers(h):
        collector = h.profile.collector
        for writer in list(collector.writers.values()) + [s.writer for s in collector.streams] \
                + ([collector.volatility.writer] if collector.volatility else []):
            if not writer.stream.closed:
                writer.stream.close()

    def rows(self, h):
        result = h.finish()
        entry = result["manifest"]["files"][VOLATILITY_FILE]
        return result, [json.loads(line) for line in stream_lines(
            h.profile_output / VOLATILITY_FILE, entry["logical"], entry["stored"], 1024 * 1024, "volatility")]

    @staticmethod
    def book(result, key=PM):
        return next(i for i, b in enumerate(result["summary"]["transition_books"])
                    if (b["instrument"], b["orientation"]) == key)

    @staticmethod
    def rewrite(h, result, rows):
        logical = b"".join(encoded(row) + b"\n" for row in rows)
        with (h.profile_output / VOLATILITY_FILE).open("wb") as sink:
            identity = encode_stream(io.BytesIO(logical), sink)
        manifest = copy.deepcopy(result["manifest"])
        manifest["files"][VOLATILITY_FILE] = {
            "logical": {"sha256": identity.logical.sha256, "byte_length": identity.logical.byte_length,
                        "records": identity.logical.line_count},
            "stored": {"sha256": identity.stored.sha256, "byte_length": identity.stored.byte_length}}
        return manifest


class ClockTests(Case):
    def test_constant_book_and_terminal_clock(self):
        h = self.harness()
        ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        result, rows = self.rows(h)
        index = self.book(result)
        rows = [r for r in rows if r["type"] == "book" and r["book"] == index]
        self.assertEqual([r["samples"] for r in rows], [10, 10, 10])
        self.assertEqual([r["two_sided_ns"] for r in rows], [str(10 * MS)] * 3)
        self.assertEqual([r["top_updates"] for r in rows], [1, 0, 0])
        total = result["summary"]["volatility"]["books"][index]
        self.assertEqual(total["samples"], 30)
        self.assertEqual([total["horizons"][str(h)]["n"] for h in (1, 3)], [29, 27])
        for row in rows:
            self.assertEqual(row["spread_sum2"], "10000")
            for stat in row["horizons"].values():
                self.assertEqual((stat["sum_abs2"], stat["sum_sq4"], stat["max_abs2"]), ("0", "0", "0"))

    def test_jump_cross_bucket_horizons_and_later_cut_catchup(self):
        h = self.harness()
        ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        ladder(h, 20 * MS, PM[0], bids=((430, M),), asks=((480, M),))
        result, rows = self.rows(h)
        index = self.book(result)
        rows = [r for r in rows if r["type"] == "book" and r["book"] == index]
        self.assertEqual(rows[0]["horizons"]["1"],
                         {"n": 10, "sum_abs2": "600", "sum_sq4": "360000", "max_abs2": "600",
                          "jumps": {"100": 1, "200": 1, "500": 0}})
        self.assertEqual(rows[0]["horizons"]["3"]["sum_abs2"], "1800")
        self.assertEqual(rows[0]["horizons"]["3"]["jumps"], {"100": 3, "200": 3, "500": 0})
        self.assertEqual([r["mid_changes"] for r in rows], [0, 1, 0])

    def test_off_grid_destination_reads_all_cuts_at_destination(self):
        h = self.harness(policy(sample_ms=2, horizons_ms=[1, 3]))
        ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        ladder(h, 11 * MS, PM[0], bids=((430, M),), asks=((480, M),))
        result, rows = self.rows(h)
        row = next(r for r in rows if r["type"] == "book" and r["book"] == self.book(result))
        self.assertEqual(row["horizons"]["1"]["sum_abs2"], "600")
        self.assertEqual(row["horizons"]["1"]["n"], 5)

    def test_one_sided_interval_duration_and_samples(self):
        h = self.harness()
        ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        ladder(h, 15 * MS + 17, PM[0], bids=(), asks=((450, M),))
        ladder(h, 20 * MS + 23, PM[0], bids=((400, M),), asks=((450, M),))
        result, rows = self.rows(h)
        rows = [r for r in rows if r["type"] == "book" and r["book"] == self.book(result)]
        self.assertEqual([r["samples"] for r in rows], [6, 9, 10])
        self.assertEqual([r["two_sided_ns"] for r in rows], [str(5 * MS + 17), str(10 * MS - 23), str(10 * MS)])
        self.assertEqual(sum(r["mid_changes"] for r in rows), 0)

    def test_pre_start_equal_time_quantity_updates_and_scope_isolation(self):
        h = self.harness(scopes=True, lower_bound="expand_to_window_start")
        ladder(h, 5 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        ladder(h, 10 * MS, PM[0], bids=((430, M),), asks=((480, M),))
        send(h, 10 * MS, (PM, operations(op(PM, "bid", 430, "increase", M))))
        result, rows = self.rows(h)
        rows = [r for r in rows if r["type"] == "book" and r["book"] == self.book(result)]
        self.assertEqual(sum(r["samples"] for r in rows), 30)
        self.assertEqual(sum(r["top_updates"] for r in rows), 2)
        self.assertEqual(sum(r["mid_changes"] for r in rows), 1)
        total = result["summary"]["volatility"]["books"][self.book(result)]
        self.assertEqual(total["horizons"]["1"]["n"], 28)
        self.assertEqual(total["horizons"]["3"]["n"], 24)
        self.assertEqual(total["horizons"]["3"]["sum_abs2"], "0")

    def test_equal_time_independent_book_order_is_deterministic(self):
        payloads = []
        for keys in ((PM, PM2), (PM2, PM)):
            h = self.harness()
            for key in keys:
                ladder(h, 10 * MS, key[0], bids=((400, M),), asks=((450, M),))
            h.finish()
            payloads.append((h.profile_output / VOLATILITY_FILE).read_bytes())
        self.assertEqual(*payloads)

    def test_same_time_intermediate_state_never_samples(self):
        h = self.harness()
        ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        ladder(h, 11 * MS, PM[0], bids=((430, M),), asks=((480, M),))
        ladder(h, 11 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        result, rows = self.rows(h)
        total = result["summary"]["volatility"]["books"][self.book(result)]
        self.assertEqual(total["horizons"]["1"]["sum_sq4"], "0")
        self.assertEqual(sum(r["mid_changes"] for r in rows if r["type"] == "book"), 2)

    def test_no_defined_samples_and_horizon_beyond_scope(self):
        h = self.harness(policy(horizons_ms=[100]))
        result, rows = self.rows(h)
        self.assertEqual(rows, [])
        self.assertTrue(all(b["samples"] == 0 for b in result["summary"]["volatility"]["books"]))

    def test_brute_causal_endpoint_oracle_with_irregular_and_equal_time_cuts(self):
        h = self.harness(policy(sample_ms=2, horizons_ms=[1, 3, 13], bucket_ms=6))
        events = [(10 * MS, 400, 450), (12 * MS + 7, 410, 470), (12 * MS + 7, 420, 460),
                  (17 * MS + 3, None, 470), (21 * MS + 19, 430, 480), (28 * MS, 500, 480)]
        for time, bid, ask in events:
            ladder(h, time, PM[0], bids=() if bid is None else ((bid, M),), asks=((ask, M),))
        result, rows = self.rows(h)
        def at(time):
            event = next((e for e in reversed(events) if e[0] <= time), None)
            return None if event is None or event[1] is None else (event[1] + event[2]) * 10
        for row in rows:
            if row["type"] != "book" or row["book"] != self.book(result):
                continue
            samples = [t for t in range(10 * MS, 40 * MS, 2 * MS)
                       if int(row["start_ns"]) <= t < int(row["end_ns"]) and at(t) is not None]
            self.assertEqual(row["samples"], len(samples))
            for horizon, stat in row["horizons"].items():
                deltas = [abs(at(t + int(horizon) * MS) - at(t)) for t in samples
                          if t + int(horizon) * MS < 40 * MS and at(t + int(horizon) * MS) is not None]
                self.assertEqual(stat["n"], len(deltas))
                self.assertEqual(int(stat["sum_abs2"]), sum(deltas))
                self.assertEqual(int(stat["sum_sq4"]), sum(d * d for d in deltas))
                self.assertEqual(int(stat["max_abs2"]), max(deltas, default=0))
                self.assertEqual(stat["jumps"], {str(j): sum(d >= 2 * j for d in deltas) for j in (100, 200, 500)})


class PairTests(Case):
    def test_different_native_scales_have_exact_gap_atom_statistics(self):
        h = self.harness(mixed=True, price_scales={"kalshi": 2, "polymarket": 4})
        ladder(h, 10 * MS, KO[0], KO[1], bids=((60, M),))
        ladder(h, 10 * MS, KC[0], KC[1], bids=((45, M),))
        ladder(h, 10 * MS, PM2[0], bids=((4000, M),), asks=((4300, M),))
        ladder(h, 20 * MS, PM2[0], bids=((4000, M),), asks=((4800, M),))
        result, rows = self.rows(h)
        ko = self.book(result, KO)
        pair = next(p for p in result["summary"]["volatility"]["gap_pairs"]
                    if p["book_a"] == ko and p["side_a"] == "outcome")
        row = next(r for r in rows if r["type"] == "gap" and r["pair"] == pair["pair"])
        self.assertEqual(row["below_unit_ns"], str(10 * MS))
        self.assertEqual(row["horizons"]["1"]["max_abs2"], "1000")
        self.assertEqual(row["horizons"]["1"]["sum_sq4"], "1000000")

    def test_kalshi_projected_sides_and_exact_below_unit_duration(self):
        h = self.harness(mixed=True)
        ladder(h, 10 * MS, KO[0], KO[1], bids=((600, M),))
        ladder(h, 10 * MS, KC[0], KC[1], bids=((450, M),))
        ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((500, M),))
        ladder(h, 10 * MS, PM2[0], bids=((400, M),), asks=((430, M),))
        ladder(h, 15 * MS + 7, PM2[0], bids=((400, M),), asks=((480, M),))
        result, rows = self.rows(h)
        v = result["summary"]["volatility"]
        self.assertEqual(len(v["gap_pairs"]), 2)
        self.assertTrue(all(p["market_a"] != p["market_b"] for p in v["gap_pairs"]))
        ko = self.book(result, KO)
        no_pair = next(p for p in v["gap_pairs"] if p["book_a"] == ko and p["side_a"] == "complement")
        yes_pair = next(p for p in v["gap_pairs"] if p["book_a"] == ko and p["side_a"] == "outcome")
        yes_rows = [r for r in rows if r["type"] == "gap" and r["pair"] == yes_pair["pair"]]
        self.assertEqual(yes_rows[0]["below_unit_ns"], str(5 * MS + 7))
        self.assertEqual(yes_rows[0]["defined_ns"], str(10 * MS))
        self.assertEqual(yes_rows[0]["horizons"]["1"]["max_abs2"], "1000")
        self.assertTrue(all(r["below_unit_ns"] == r["defined_ns"] for r in rows
                            if r["type"] == "gap" and r["pair"] == no_pair["pair"]))
        self.assertEqual(v["unavailable"], [])

    def test_unknown_masks_visible_and_same_market_tokens_excluded(self):
        for masks in (True, False):
            h = self.harness(masks=masks)
            result, _ = self.rows(h)
            v = result["summary"]["volatility"]
            self.assertEqual(v["gap_pairs"], [])
            self.assertEqual(len(v["unavailable"]), 0 if masks else 2)
            if not masks:
                self.assertEqual({x["status"] for x in v["unavailable"]}, {"OUTCOMES_UNAVAILABLE"})

    def test_scoped_masks_pair_bound_and_independent_bitset_equality(self):
        h = self.harness(mixed=True, scopes=True)
        snap = copy.deepcopy(h.profile.snapshot)
        books, _, _ = tables(snap, h.profile.policy)
        pairs, unknown = pair_tables(snap, books)
        self.assertEqual((pairs, unknown), _pair_evidence(snap, books, True))
        self.assertEqual([p["scope"] for p in pairs], [0, 0, 1, 1])
        with self.assertRaisesRegex(ProtocolError, "pair budget"):
            pair_tables(snap, books, limit=1)
        snap["scopes"][0]["outcome_books"][0].update(status="UNSUPPORTED", reason="test", space_shape_id=None,
                                                   claim_id=None, negated=None)
        self.assertEqual(pair_tables(snap, books), _pair_evidence(snap, books, True))
        self.assertEqual(len(pair_tables(snap, books)[1]), 1)

    def test_null_pairs_and_incomplete_space(self):
        h = self.harness(policy(gap_pairs=None), mixed=True)
        self.assertEqual(h.finish()["summary"]["volatility"]["gap_pairs"], [])
        snap = copy.deepcopy(h.profile.snapshot)
        snap["outcomes"]["document"]["spaces"][0]["coverage"] = "INCOMPLETE_COVERAGE"
        books, _, _ = tables(snap, h.profile.policy)
        pairs, unknown = pair_tables(snap, books)
        self.assertEqual(pairs, [])
        self.assertEqual({u["status"] for u in unknown}, {"INCOMPLETE_COVERAGE"})


class ReaderAndBoundsTests(Case):
    def test_rehashed_row_tampering_rejected(self):
        mutations = [lambda r: r.update(samples=True), lambda r: r.update(extra=0),
                     lambda r: r.update(spread_sum2="01"), lambda r: r.update(two_sided_ns="0"),
                     lambda r: r["horizons"]["1"].update(sum_sq4="1"),
                     lambda r: r["horizons"]["1"]["jumps"].update({"100": 1})]
        for mutate in mutations:
            h = self.harness()
            ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
            result, rows = self.rows(h)
            mutate(rows[0])
            manifest = self.rewrite(h, result, rows)
            with self.assertRaisesRegex(ProtocolError, "volatility row recomputation"):
                validate_content(h.profile_output, h.profile.snapshot, manifest)

    def test_missing_extra_rows_and_manifest_identity_rejected(self):
        for action in ("missing", "extra", "stored", "logical"):
            h = self.harness()
            ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
            result, rows = self.rows(h)
            manifest = self.rewrite(h, result, rows[:-1] if action == "missing" else rows + rows[:1] if action == "extra" else rows)
            if action in ("stored", "logical"):
                manifest["files"][VOLATILITY_FILE][action]["sha256"] = "0" * 64
            with self.assertRaises(ProtocolError):
                validate_content(h.profile_output, h.profile.snapshot, manifest)

    def test_file_set_regular_file_and_provisional_checks(self):
        for action in ("missing", "extra_manifest", "provisional", "symlink"):
            h = self.harness()
            result, _ = self.rows(h)
            manifest = copy.deepcopy(result["manifest"])
            path = h.profile_output / VOLATILITY_FILE
            if action == "missing":
                path.unlink()
            elif action == "extra_manifest":
                manifest["files"]["unrequested.ndjson.zst"] = manifest["files"][VOLATILITY_FILE]
            elif action == "provisional":
                path.with_name(path.name + ".open").write_bytes(b"pending")
            else:
                actual = path.with_name("other-frame")
                path.rename(actual)
                path.symlink_to(actual)
            with self.assertRaises(ProtocolError):
                validate_content(h.profile_output, h.profile.snapshot, manifest)
        h = self.harness(v2("transitions", bucket_ns=str(10 * MS)))
        result = h.finish()
        (h.profile_output / VOLATILITY_FILE).write_bytes(b"unrequested")
        with self.assertRaisesRegex(ProtocolError, "unexpected output"):
            validate_content(h.profile_output, h.profile.snapshot, result["manifest"])

    def test_summary_pair_tampering_rejected_even_with_rehashed_receipt(self):
        h = self.harness(mixed=True)
        result, _ = self.rows(h)
        summary = result["summary"]
        summary["volatility"]["gap_pairs"][0]["side_a"] = "made_up"
        (h.profile_output / "summary.json").write_bytes(encoded(summary) + b"\n")
        manifest = result["manifest"]
        manifest["summary_sha256"] = hashlib.sha256(encoded(summary)).hexdigest()
        (h.profile_output / "manifest.json").write_bytes(encoded(manifest) + b"\n")
        receipt = result["receipt"]
        receipt["semantic_sha256"] = hashlib.sha256(encoded(manifest)).hexdigest()
        (h.profile_output / "content_receipt.json").write_bytes(encoded(receipt) + b"\n")
        with self.assertRaisesRegex(ProtocolError, "summary identity/schema"):
            read_provisional(h.profile_output, h.root / "context", expected_sha256=h.sha)

    def test_line_row_and_pending_state_guards(self):
        for field, limit, pattern in (("max_line_bytes", 5, "line budget"), ("max_records", 0, "output budget")):
            h = self.harness()
            setattr(h.profile.collector.volatility.writer, field, limit)
            ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
            with self.assertRaisesRegex(ProtocolError, pattern):
                h.finish()
        h = self.harness()
        ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        with patch("replay.economic_sdk.volatility.MAX_PENDING", 1):
            with self.assertRaisesRegex(ProtocolError, "pending sample budget"):
                h.finish()
        h = self.harness()
        h.profile.collector.budget.limit = h.profile.collector.budget.used
        with self.assertRaisesRegex(ProtocolError, "volatility state budget"):
            ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))

    def test_retained_state_does_not_grow_with_elapsed_buckets(self):
        h = self.harness(policy(bucket_ms=2))
        ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
        for t in range(11, 39):
            send(h, t * MS, (PM, operations(op(PM, "bid", 400, "increase", 1))))
            v = h.profile.collector.volatility
            self.assertLessEqual(len(v.rows), 4)
            self.assertLessEqual(len(v.pending), 4)
        h.finish()

    def test_reader_retains_only_samples_with_unfinished_in_scope_destinations(self):
        for horizons in ([100], [1, 100]):
            h = self.harness(policy(horizons_ms=horizons))
            ladder(h, 10 * MS, PM[0], bids=((400, M),), asks=((450, M),))
            with patch("replay.economic_sdk.volatility_reader.MAX_PENDING", 1):
                result = h.finish()
            total = result["summary"]["volatility"]["books"][self.book(result)]
            self.assertEqual(total["samples"], 30)
            self.assertEqual(total["horizons"]["100"]["n"], 0)

    def test_native_scale4_conversion_and_visible_precision_failure(self):
        h = self.harness()
        v = h.profile.collector.volatility
        self.assertEqual(v._price(["123", "1"], 0), 1230)
        v.table[0]["price_scale"] = "5"
        self.assertEqual(v._price(["12340", "1"], 0), 1234)
        with self.assertRaisesRegex(ProtocolError, "precision"):
            v._price(["12341", "1"], 0)

    def test_real_full_and_sparse_tapes_equal_nonempty_volatility_bytes(self):
        fixtures = Path(__file__).resolve().parents[2] / "engine/crates/transport/tests/fixtures"
        payloads = []
        for name in ("strategy-full.ndjson", "strategy-sparse.ndjson"):
            tape = [time_scale(json.loads(line)) for line in (fixtures / name).read_bytes().splitlines()]
            h = self.harness(scopes=True, lower_bound="expand_to_window_start", pin=tape[0]["body"]["pins"][0],
                             wire_records=tape)
            entry = json.loads((h.profile_output / "manifest.json").read_bytes())["files"][VOLATILITY_FILE]
            self.assertGreater(entry["logical"]["records"], 0)
            payloads.append((h.profile_output / VOLATILITY_FILE).read_bytes())
        self.assertEqual(*payloads)

    def test_volatility_disabled_matches_every_base_revision_output_byte(self):
        # Recorded from a clean git archive of 365d2f68 with the same codec.
        for profile, expected in ((PROFILE_POLICY, "1aa5dadabbe5047093cdc0db120babff9c28fdca2c407dba2adc845fa6ad2029"),
                                  (v2("availability", "levels", "transitions"),
                                   "aa3111061a3b59a9ef9952d08a97fd1ceeb147773334bc229446eb62f9d1f138")):
            self.counter += 1
            root = self.root / str(self.counter)
            root.mkdir()
            h = Harness(root, policy=profile, mixed=True, scopes=True)
            self.addCleanup(self.close_writers, h)
            exercise(h)
            h.finish()
            hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in h.profile_output.iterdir() if p.is_file()}
            self.assertEqual(hashlib.sha256(encoded(hashes)).hexdigest(), expected)


class PolicyTests(unittest.TestCase):
    def test_policy_closed_types_order_caps_and_dependencies(self):
        valid = policy()
        self.assertEqual(profile_policy(valid, standalone=True), valid)
        bad = [{"version": True}, {"sample_ms": True}, {"sample_ms": "1"}, {"sample_ms": 0},
               {"sample_ms": 3}, {"bucket_ms": -1}, {"bucket_ms": 0.5}, {"sample_ms": 1 << 64},
               {"horizons_ms": []}, {"horizons_ms": [1, 1]}, {"horizons_ms": [2, 1]},
               {"horizons_ms": [True]}, {"horizons_ms": [1.5]}, {"horizons_ms": [0]},
               {"horizons_ms": list(range(1, 10))}, {"horizons_ms": [1 << 64]},
               {"jump_atoms": []}, {"jump_atoms": [0]}, {"jump_atoms": ["100"]}, {"jump_atoms": [True]},
               {"jump_atoms": [2, 1]}, {"jump_atoms": [1, 1]}, {"jump_atoms": list(range(1, 8))},
               {"gap_pairs": "guessed"}, {"gap_pairs": False}, {"unknown": 1}]
        for change in bad:
            with self.subTest(change=change), self.assertRaises(ProtocolError):
                profile_policy(policy(**change), standalone=True)
        for field in valid["volatility"]:
            p = copy.deepcopy(valid)
            del p["volatility"][field]
            with self.assertRaises(ProtocolError):
                profile_policy(p, standalone=True)
        for change in ({"version": 1}, {"groups": ["volatility"]}, {"volatility": None}):
            p = copy.deepcopy(valid)
            p.update(change)
            with self.assertRaises(ProtocolError):
                profile_policy(p, standalone=True)
        with self.assertRaisesRegex(ProtocolError, "standalone"):
            profile_policy(valid)
        p = copy.deepcopy(valid)
        del p["volatility"]
        with self.assertRaises(ProtocolError):
            profile_policy(p, standalone=True)

    def test_absent_group_policy_identity_and_file_set_unchanged(self):
        p = v2("transitions")
        self.assertEqual(profile_policy(p, standalone=True), p)
        self.assertEqual(profile_identity("0" * 64, p), hashlib.sha256(encoded(
            {"strategy": "market_profile_v1", "policy": p, "snapshot_sha256": "0" * 64})).hexdigest())
        self.assertNotIn(VOLATILITY_FILE, profile_files(p))
        p["volatility"] = policy()["volatility"]
        with self.assertRaises(ProtocolError):
            profile_policy(p, standalone=True)

    def test_accumulator_cost_is_conservative(self):
        from replay.economic_sdk.volatility_reader import _Reconstruction
        # A maximal-shape reader accumulator, visited once per object.
        r = object.__new__(_Reconstruction)
        r.policy = policy(horizons_ms=list(range(1, 9)), jump_atoms=list(range(1, 7)))["volatility"]
        shape = r._blank()
        seen = set()
        def size(v):
            if id(v) in seen:
                return 0
            seen.add(id(v))
            return sys.getsizeof(v) + (sum(size(k) + size(x) for k, x in v.items()) if isinstance(v, dict)
                                      else sum(size(x) for x in v) if isinstance(v, (list, tuple)) else 0)
        self.assertGreaterEqual(accumulator_cost(r.policy), size(shape))
