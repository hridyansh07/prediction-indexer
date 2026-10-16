"""Execution realism research arithmetic; synthetic committed evidence only."""

import io
import json
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from unittest import mock

import pyarrow.parquet as pq

from encoder import encode_stream

from replay.research import realism
from replay.research.jsonio import ResearchError
from replay.research.jsonio import file_identity, read_json
from replay.research import build as builder
from replay.research.config import load
from replay.research.overlays import episode_intervals
from replay.streams.protocol import ProtocolError
from replay.tests.research_fixtures import ATTEMPT, BOOKS, T0, S, corpus_result, cross_output, ndjson, write_json


SCALE = 10 ** 36


def probe(*, decision_value=3 * SCALE // 10, value=SCALE // 100, steps="1000", matched="500", confirm=500):
    return {"episode_id": "e", "sizing": "t10", "confirm_ms": confirm, "arrival_ms": 250,
            "sequencing": "simultaneous", "status": "partial",
            "decision": {"time_ns": "1000000000", "steps": steps, "limits": [], "ages": [],
                         "value": str(decision_value), "legs": []},
            "legs": [], "matched_steps": matched, "value": str(value), "leg_loss_gross": str(SCALE // 200)}


class ExactArithmeticTest(unittest.TestCase):
    def test_steps_are_not_contracts_or_native_atoms(self):
        row = realism.probe_detail(probe(), Fraction("0.01"), 36, "USD")
        self.assertEqual(row["matched_sets"], "5")
        self.assertEqual(row["decision_edge_per_set"], "0.03")
        self.assertEqual(row["value_exact"], "0.01")
        self.assertEqual(row["leg_loss_gross_exact"], "0.005")
        self.assertTrue(realism.at_threshold(row["decision_edge_per_set"], 3))
        self.assertFalse(realism.at_threshold(row["decision_edge_per_set"], 4))

    def test_nonterminating_ratio_remains_exact(self):
        self.assertEqual(realism.exact_ratio(Fraction("3.05") / 1067), "61/21340")
        self.assertTrue(realism.at_threshold("29999999999999999999999999999999999/1000000000000000000000000000000000000", 2))
        self.assertFalse(realism.at_threshold("29999999999999999999999999999999999/1000000000000000000000000000000000000", 3))

    def test_threshold_never_uses_arrival_or_opening_value(self):
        row = realism.probe_detail(probe(decision_value=0, value=1000 * SCALE), Fraction("0.01"), 36, "USD")
        self.assertFalse(realism.at_threshold(row["decision_edge_per_set"], 1))
        old = probe()
        del old["decision"]["value"]
        del old["decision"]["legs"]
        self.assertIsNone(realism.probe_detail(old, Fraction("0.01"), 36, "USD")["decision_edge_per_set"])
        self.assertFalse(realism.at_threshold(None, 1))

    def test_unresolved_probe_is_missing_not_zero(self):
        raw = probe()
        raw.update(decision=None, status="not_confirmed", matched_steps=None, value=None, leg_loss_gross=None)
        row = realism.probe_detail(raw, Fraction("0.01"), 36, "USD")
        self.assertIsNone(row["matched_sets"])
        self.assertIsNone(row["value_exact"])
        self.assertIsNone(row["decision_edge_per_set"])

    def test_unknown_probe_fields_are_rejected_not_dropped_by_parquet(self):
        raw = {**probe(), "unexpected": "evidence"}
        with self.assertRaisesRegex(ResearchError, "probe fields"):
            realism.probe_detail(raw, Fraction("0.01"), 36, "USD")

    def test_float_boolean_and_noncanonical_steps_are_rejected(self):
        for steps in (True, 1.5, "01", "-1", "1.0"):
            with self.subTest(steps=steps), self.assertRaises(ResearchError):
                realism.probe_detail(probe(steps=steps), Fraction("0.01"), 36, "USD")

    def test_signed_integer_limit_counts_magnitude_digits(self):
        amount = -(10 ** 80 - 1)
        row = realism.probe_detail(probe(value=amount), Fraction("0.01"), 36, "USD")
        self.assertEqual(Fraction(row["value_exact"]), Fraction(amount, SCALE))
        with self.assertRaisesRegex(ResearchError, "canonical integer"):
            realism.probe_detail(probe(value=-(10 ** 81 - 1)), Fraction("0.01"), 36, "USD")


class DelaySummaryTest(unittest.TestCase):
    def row(self, bundle, **kwargs):
        return {"bundle_id": bundle, "event_id": bundle, "lens": "cross", "game": "lol",
                "pairing_class": "series_moneyline", "settlement_review": False,
                "gap_pair": 7, "value_unit": "USD", **realism.probe_detail(probe(**kwargs), Fraction("0.01"), 36, "USD")}

    def test_causal_thresholds_statuses_exact_sums_and_event_denominator(self):
        rows = [self.row("a"), self.row("b", decision_value=2 * SCALE // 10, value=-SCALE // 100)]
        result = realism.delayed_summaries(rows)
        all_row = next(r for r in result if r["group_by"] == "all" and r["threshold_cents"] == 3)
        self.assertEqual(all_row["probes"], 1)
        self.assertEqual(all_row["status_counts"], {"partial": 1})
        self.assertEqual(all_row["mean_fill_ratio"], "0.5")
        self.assertEqual(all_row["median_matched_sets"], "5")
        self.assertEqual(all_row["value_sum"], "0.01")
        self.assertEqual(all_row["leg_loss_gross_sum"], "0.005")
        self.assertEqual(all_row["net_after_loss_sum"], "0.005")
        self.assertEqual(all_row["positive_event_share"], "1")
        base = next(r for r in result if r["group_by"] == "all" and r["threshold_cents"] == 0)
        self.assertEqual(base["positive_event_share"], "0.5")
        self.assertEqual({r["group_by"] for r in result}, {"all", "game", "pairing_class", "settlement_review"})

    def test_pair_loss_keeps_lens_and_native_pair_separate(self):
        first = self.row("a")
        second = {**self.row("a"), "lens": "cover_cross", "leg_loss_gross_exact": "-0.005"}
        volatility = [{"bundle_id": "a", "pair": 7, "type": "gap", "horizon_ms": 1000, "rms": 0.01},
                      {"bundle_id": "a", "pair": 8, "type": "gap", "horizon_ms": 1000, "rms": 0.5}]
        rows = realism.pair_loss_comparisons([first, second], volatility)
        self.assertEqual({r["lens"]: r["leg_loss_gross_sum"] for r in rows},
                         {"cross": "0.005", "cover_cross": "-0.005"})
        self.assertTrue(all(r["gap_rms_1s"] == 0.01 and r["gap_rms_3s"] is None for r in rows))


CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "research.json"
CONFIG = load(CONFIG_PATH)
BUNDLE = "bundle_" + "9" * 24


def add_execution_evidence(staging):
    """Only fields consumed by the adapter; corpus receipt commits them afterwards."""
    output = staging / "run" / ATTEMPT / "cross" / "output"
    episodes = [json.loads(line) for line in (output / "episodes.ndjson").read_bytes().splitlines()]
    rows = []
    for episode in episodes:
        if "fill" not in episode:
            continue
        episode["flags"] = {"pairing_class": "series_moneyline", "settlement_review": False, "stale_legs": [1]}
        episode["fill"]["results"][0].update(steps="2000", value=str(6 * SCALE // 10))
        episode["fill"]["results"].append({**episode["fill"]["results"][0], "name": "t10", "role": "records",
                                             "mode": "target", "steps": "1000", "value": str(3 * SCALE // 10)})
        for sizing in ("edge", "t10"):
            for confirm in (0, 500, 1000):
                for arrival in (100, 250, 500, 1000, 2000):
                    for mode in ("simultaneous", "slow_first"):
                        row = probe(steps="1000" if sizing == "t10" else "2000", matched="500", confirm=confirm,
                                    decision_value=3 * SCALE // 10 if sizing == "t10" else 6 * SCALE // 10)
                        row.update(episode_id=episode["episode_id"], sizing=sizing, arrival_ms=arrival, sequencing=mode)
                        row["decision"]["time_ns"] = str(int(episode["start_ns"]) + confirm * 1_000_000)
                        rows.append(row)
    ndjson(output / "episodes.ndjson", episodes)
    ndjson(output / "probes.ndjson", rows)
    manifest = read_json(output / "manifest.json")
    manifest["policy"]["fills"]["probes"] = {"sizings": ["edge", "t10"], "confirm_ms": [0, 500, 1000],
        "arrival_ms": [100, 250, 500, 1000, 2000], "sequencing": ["simultaneous", "slow_first"]}
    manifest["files"] = {"probes.ndjson": {**file_identity(output / "probes.ndjson"), "records": len(rows)}}
    write_json(output / "manifest.json", manifest)
    profile = staging / "run" / ATTEMPT / "profile" / "output"
    pairs = [{"scope": 0, "pair": 7, "book_a": 0, "side_a": "complement", "book_b": 1,
              "side_b": "outcome", "market_a": BOOKS[0]["market_id"], "market_b": BOOKS[1]["market_id"],
              "mask_a": {}, "mask_b": {}},
             {"scope": 0, "pair": 8, "book_a": 0, "side_a": "outcome", "book_b": 2,
              "side_b": "outcome", "market_a": BOOKS[0]["market_id"], "market_b": BOOKS[2]["market_id"],
              "mask_a": {}, "mask_b": {}}]
    volatility = []
    for kind, index in (("book", 0), ("gap", 7), ("gap", 8)):
        volatility.append({"type": kind, kind if kind == "book" else "pair": index, "scope": 0, "bucket": 0,
            "start_ns": str(T0), "end_ns": str(T0 + 60 * S), "samples": 4,
            **({"two_sided_ns": str(60 * S), "mid_changes": 4, "top_updates": 4, "spread_sum2": "0"}
               if kind == "book" else {"defined_ns": str(60 * S), "below_unit_ns": str(60 * S)}),
            "horizons": {str(h): {"n": 4, "sum_sq4": str(4 * (h // 5) ** 2), "sum_abs2": str(4 * h // 5),
                                  "max_abs2": str(h // 5), "jumps": {"100": 4, "200": 0}}
                         for h in (1000, 3000)}})
    sink = io.BytesIO()
    encoded = encode_stream(io.BytesIO(b"".join((json.dumps(row) + "\n").encode() for row in volatility)), sink)
    (profile / "volatility.ndjson.zst").write_bytes(sink.getvalue())
    profile_manifest = read_json(profile / "manifest.json")
    profile_manifest["files"]["volatility.ndjson.zst"] = {
        "logical": {"sha256": encoded.logical.sha256, "byte_length": encoded.logical.byte_length,
                    "records": encoded.logical.line_count},
        "stored": {"sha256": encoded.stored.sha256, "byte_length": encoded.stored.byte_length}}
    write_json(profile / "manifest.json", profile_manifest)
    summary = read_json(profile / "summary.json")
    summary["volatility"] = {"gap_pairs": pairs, "books": [], "gaps": [], "unavailable": []}
    write_json(profile / "summary.json", summary)


class ExecutionBuildTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.key = self.root / "aaaaaaaaaaaaaaaa"
        self.key.mkdir()
        self.result = corpus_result(self.key, BUNDLE, before_commit=add_execution_evidence)

    def tearDown(self):
        self.tmp.cleanup()

    def test_committed_corpus_to_nonempty_tables_report_and_overlay(self):
        version, failures = builder.build([self.key], self.root / "out", CONFIG, config_bytes=CONFIG_PATH.read_bytes(), log=lambda _: None)
        self.assertEqual(failures, [])
        root = self.root / "out" / version
        episodes = pq.read_table(root / "episodes.parquet").to_pylist()
        first = next(r for r in episodes if r["kind"] == "net")
        self.assertEqual((first["quantity_exact"], first["edge_per_set"], first["t10_value"]), ("20", "0.03", "0.3"))
        self.assertEqual((first["pairing_class"], first["settlement_review"], first["stale_legs"]),
                         ("series_moneyline", False, [1]))
        probes = pq.read_table(root / "probes.parquet").to_pylist()
        self.assertEqual(len(probes), 180)
        self.assertEqual(probes[0]["decision"]["value"], str(6 * SCALE // 10))
        self.assertEqual(probes[0]["decision_edge_per_set"], "0.03")
        self.assertEqual({p["gap_pair"] for p in probes if p["gap_pair"] is not None}, {7})
        overlay = read_json(root / "events" / BUNDLE / "overlays" / "cross.json")
        self.assertEqual(overlay["items"][0]["detail"]["probes"]["state"], "ok")
        self.assertEqual(len(overlay["items"][0]["detail"]["probes"]["items"]), 60)
        summary = read_json(root / "summary.json")["execution_realism"]
        self.assertEqual(summary["delayed_fills"][0]["sizing"], "t10")
        self.assertTrue(any(r["threshold_cents"] == 3 for r in summary["delayed_fills"]))
        self.assertEqual({r["pair"] for r in summary["pair_loss_comparisons"]}, {7})
        self.assertAlmostEqual(summary["pair_loss_comparisons"][0]["gap_rms_1s"], 0.01)
        self.assertAlmostEqual(summary["pair_loss_comparisons"][0]["gap_rms_3s"], 0.03)
        self.assertEqual(pq.read_table(root / "volatility.parquet").num_rows, 10)
        report = (root / "viability.md").read_text()
        for text in ("## Delayed fills", "## Volatility", "Gap volatility beside", "non-causal research context", "≥3¢"):
            self.assertIn(text, report)
        from replay.corpus.results import verify
        verify(self.result)
        manifest = read_json(root / "manifest.json")
        for name, expected in manifest["files"].items():
            self.assertEqual(file_identity(root / name), expected)
        version2, failures2 = builder.build([self.key], self.root / "out2", CONFIG,
                                            config_bytes=CONFIG_PATH.read_bytes(), log=lambda _: None)
        self.assertEqual((version2, failures2), (version, []))
        self.assertEqual((root / "probes.parquet").read_bytes(),
                         (self.root / "out2" / version / "probes.parquet").read_bytes())

    def output(self):
        return self.result / "run" / ATTEMPT / "cross" / "output"

    def test_tampered_probe_is_rejected_before_rows(self):
        output = self.output()
        with (output / "probes.ndjson").open("ab") as handle:
            handle.write(b"{}\n")
        with self.assertRaisesRegex(ResearchError, "identity mismatch"):
            episode_intervals(output, "cross", CONFIG["lenses"]["cross"], BOOKS)

    def test_missing_or_duplicate_probe_grid_is_rejected(self):
        output = self.output()
        rows = (output / "probes.ndjson").read_bytes().splitlines(keepends=True)
        for modified, reason in ((rows[:-1], "incomplete"), (rows + rows[:1], "duplicate")):
            (output / "probes.ndjson").write_bytes(b"".join(modified))
            manifest = read_json(output / "manifest.json")
            manifest["files"]["probes.ndjson"] = {**file_identity(output / "probes.ndjson"), "records": len(modified)}
            write_json(output / "manifest.json", manifest)
            with self.assertRaisesRegex(ResearchError, reason):
                episode_intervals(output, "cross", CONFIG["lenses"]["cross"], BOOKS)

    def test_probe_memory_bounds_fail_visibly(self):
        output = self.output()
        for constant, value, message in (("MAX_PROBE_ROWS", 1, "row budget"), ("MAX_PROBE_BYTES", 1, "byte budget"),
                                         ("MAX_LINE", 1, "line bound")):
            with mock.patch.object(realism, constant, value):
                with self.assertRaisesRegex(ResearchError, message):
                    episode_intervals(output, "cross", CONFIG["lenses"]["cross"], BOOKS)

    def test_legacy_inputs_are_explicitly_unavailable(self):
        output = self.root / "old"
        cross_output(output)
        items = episode_intervals(output, "cross", CONFIG["lenses"]["cross"], BOOKS)["items"]
        self.assertEqual(items[0]["detail"]["probes"]["state"], "unavailable")
        self.assertEqual(items[0]["detail"]["flags_state"], "unavailable")

    def test_volatility_identity_and_resource_limit(self):
        output = self.result / "run" / ATTEMPT / "profile" / "output"
        with mock.patch.object(realism, "MAX_VOLATILITY_SERIES", 1):
            with self.assertRaisesRegex(ResearchError, "series bound"):
                realism.volatility_document(output, BUNDLE, None, "lol", [{**b, "market_type": "series_moneyline"} for b in BOOKS])
        path = output / "volatility.ndjson.zst"
        data = bytearray(path.read_bytes())
        data[-1] ^= 1
        path.write_bytes(data)
        with self.assertRaises(ProtocolError):
            realism.volatility_document(output, BUNDLE, None, "lol", BOOKS)

    def test_enabled_empty_probe_file_is_available(self):
        key = self.root / "bbbbbbbbbbbbbbbb"
        key.mkdir()
        corpus_result(key, BUNDLE, episodes=[], before_commit=add_execution_evidence)
        version, failures = builder.build([key], self.root / "empty", CONFIG, config_bytes=b"{}", log=lambda _: None)
        self.assertEqual(failures, [])
        evidence = read_json(self.root / "empty" / version / "summary.json")["execution_realism"]["evidence"]
        probes = next(r for r in evidence if r["kind"] == "probes")
        self.assertEqual(probes["state"], "ok")
        self.assertIsNone(probes["reason"])

    def test_zero_sample_books_and_pairs_are_explicit_unavailable(self):
        output = self.result / "run" / ATTEMPT / "profile" / "output"
        document = realism.volatility_document(output, BUNDLE, None, "lol", BOOKS)
        missing = [r for r in document["rows"] if r["type"] == "book" and r["book"] == 2]
        self.assertEqual(len(missing), 2)
        self.assertTrue(all(r["state"] == "unavailable" and r["rms"] is None for r in missing))

    def test_whole_build_probe_budget_fails_before_publication(self):
        with mock.patch.object(realism, "MAX_PROBE_STATE", 1, create=True):
            version, failures = builder.build([self.key], self.root / "bound", CONFIG,
                                               config_bytes=b"{}", log=lambda _: None)
        self.assertEqual(len(failures), 1)
        self.assertIn("retained probe state budget", failures[0][1])
        summary = read_json(self.root / "bound" / version / "summary.json")
        self.assertFalse(summary["execution_realism"]["delayed_fills"])

    def test_budget_is_shared_across_events_not_reset_per_pack(self):
        corpus_result(self.key, "bundle_" + "a" * 24, before_commit=add_execution_evidence)
        captured = []
        original = builder.build_pack

        def capture(*args):
            result = original(*args)
            captured.append(sum(realism.ProbeBudget.cost(row) for row in result[3]))
            return result

        with mock.patch.object(builder, "build_pack", side_effect=capture):
            builder.build([self.key], self.root / "measure", CONFIG, config_bytes=b"{}", log=lambda _: None)
        self.assertEqual(len(captured), 2)
        cap = max(captured) + min(captured) // 2
        with mock.patch.object(realism, "MAX_PROBE_STATE", cap):
            version, failures = builder.build([self.key], self.root / "shared", CONFIG,
                                               config_bytes=b"{}", log=lambda _: None)
        self.assertEqual(len(failures), 1)
        rows = pq.read_table(self.root / "shared" / version / "probes.parquet")
        self.assertEqual(rows.num_rows, 180)
        self.assertIn("retained probe state budget", failures[0][1])


if __name__ == "__main__":
    unittest.main()
