import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay import supervisor
from replay.bundle_coverage import build
from replay.coverage_output import (
    book_id,
    read_completed,
    read_provisional,
    validate_content,
)
from replay.preparation import encoded, prepare
from replay.strategy_sdk import plain
from replay.streams.protocol import Decoder, ProtocolError, freeze
from replay.tests.test_preparation import G2, R1, R2, config, detail
from replay.tests.test_supervisor import config as supervisor_config


class Harness:
    """Hand-authored decisions, real preparation + Decoder; NOT a Risk E2E test."""

    def __init__(
        self,
        root,
        *,
        mixed=False,
        scopes=False,
        all_uncaptured=False,
        attempt="a" * 32,
        lower_bound="clip",
    ):
        self.root = root
        d = detail()
        c = config()
        c["lower_bound"] = lower_bound
        if not mixed:
            c["probe_markets"] = ["polymarket:series"]
        if all_uncaptured:
            c["probe_markets"] = ["polymarket:map-one"]
        if scopes:
            c["occurrences"][0]["end_ns"] = "23"
            later = copy.deepcopy(d)
            later["context"]["markets"] = [
                m for m in later["context"]["markets"] if m["selected"]
            ]
            if scopes == "uncaptured":
                later["context"]["targets"] = [
                    t
                    for t in later["context"]["targets"]
                    if t["target_id"].startswith("kalshi:")
                ]
                for market in later["context"]["markets"]:
                    market["selected"] = market["target_id"].startswith("kalshi:")
            later["run_id"], later["generated_at"] = R2, G2
            later["source"] = {k: v.replace(R1, R2) for k, v in d["source"].items()}
            later["source"]["report_sha256"] = "c" * 64
            later["origin"] = {**later["source"], "run_id": R2, "generated_at": G2}
            c["occurrences"].append(
                {
                    "start_ns": "23",
                    "end_ns": "40",
                    "run_id": R2,
                    "source": later["source"],
                }
            )
            source = lambda occurrence, _: d if occurrence["run_id"] == R1 else later
        else:
            source = lambda *_: d
        self.snapshot = prepare(c, root / "context", universe=source)
        self.sha = json.loads((root / "context/receipt.json").read_bytes())[
            "snapshot_sha256"
        ]
        self.cfg = {
            "version": 1,
            "snapshot_directory": str(root / "context"),
            "snapshot_sha256": self.sha,
        }
        self.output = root / "output"
        self.output.mkdir()
        self.context = {
            "config": self.cfg,
            "run_id": "coverage-test",
            "attempt_id": attempt,
            "group": "coverage",
            "identity": "f" * 64,
            "output_directory": str(self.output),
        }
        self.strategy = build(freeze(self.context))
        self.initial = {
            **{
                f: plain(self.snapshot["config"][f])
                for f in ("pins", "start_ns", "end_ns", "lower_bound")
            },
            "plans": plain(self.snapshot["plans"]),
            "groups": ["coverage"],
            "max_entry_bytes": "65536",
            "max_queue_bytes": "1048576",
        }
        self.decoder = Decoder("coverage-test", attempt, self.initial, 65536)
        self.seq = 0
        self.pin = self.initial["pins"][0]
        self.send("initial", self.initial)

    def send(self, kind, body):
        cut = self.decoder.apply(
            encoded(
                {
                    "version": "1",
                    "run_id": "coverage-test",
                    "attempt_id": self.context["attempt_id"],
                    "sequence": str(self.seq),
                    "kind": kind,
                    "body": body,
                }
            )
        )
        self.seq += 1
        self.strategy(cut)
        return cut

    def window(self, start=0, end=40, why=None):
        transitions = (
            [self.transition(p, 0, why) for p in self.initial["plans"]] if why else []
        )
        return self.send(
            "cut",
            {
                "origin": {
                    "kind": "window",
                    "pin": self.pin,
                    "start_ns": str(start),
                    "end_ns": str(end),
                },
                "market_events": [],
                "book_transitions": transitions,
            },
        )

    def ref(self, time):
        return {
            "pin": self.pin,
            "address": {
                "canonical_seq": str(self.seq),
                "lane": "explicit-polymarket",
                "delivery_index": str(self.seq),
                "event_index": "0",
            },
            "visible_ns": str(time),
            "order_ns": str(time),
        }

    def transition(self, p, time, why=None):
        key = {f: p[f] for f in ("instrument", "orientation")}
        revision = self.decoder.books[p["instrument"], p["orientation"]].revision
        return {
            "key": key,
            "previous_revision": str(revision),
            "revision": str(revision + 1),
            "dependency": None
            if why
            else {"epoch": "one", "anchor": self.ref(time), "through": self.ref(time)},
            "decision": {"kind": "invalidation", "reason": why}
            if why
            else {"kind": "snapshot", "bids": [["320", "7"]], "asks": [["890", "19"]]},
        }

    def group(self, time, plans=(), why=None, trades=()):
        ref = self.ref(time)
        market = []
        for disposition in trades:
            market.append(
                {
                    "reference": ref,
                    "disposition": disposition,
                    "event": {
                        "kind": "trade",
                        "value": {
                            "instrument": "polymarket:123",
                            "orientation": "outcome",
                            "price": {
                                "atoms": "400",
                                "scale": "3",
                                "unit": "quote_per_contract",
                            },
                            "quantity": {
                                "atoms": "3",
                                "scale": "6",
                                "unit": "contracts",
                            },
                            "aggressor": None,
                        },
                    },
                }
            )
        return self.send(
            "cut",
            {
                "origin": {
                    "kind": "group",
                    "pin": self.pin,
                    "first": ref["address"],
                    "last": ref["address"],
                    "visible_ns": str(time),
                },
                "market_events": market,
                "book_transitions": [self.transition(p, time, why) for p in plans],
            },
        )

    def finish(self):
        self.send("terminal", {"cuts": str(self.seq - 1)})
        self.decoder.finish()
        self.strategy.finish()
        return read_provisional(
            self.output, self.root / "context", expected_sha256=self.sha
        )

    def rows(self, entity=None):
        rows = [
            json.loads(line)
            for line in (self.output / "intervals.ndjson").read_bytes().splitlines()
        ]
        return [r for r in rows if entity is None or r["entity"] == entity]


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def harness(self, **kwargs):
        h = Harness(self.root, **kwargs)
        self.addCleanup(h.strategy.writer.stream.close)
        return h

    def test_delayed_initialization_trades_quiet_fault_and_recovery(self):
        h = self.harness()
        h.window()
        h.group(11, trades=("observed", "duplicate"))
        old = h.group(14, h.initial["plans"][:1])
        h.group(17, h.initial["plans"][1:])
        h.group(19, trades=("observed",))
        h.group(20)  # unrelated/empty observations cannot split availability
        h.group(28, h.initial["plans"][:1], {"kind": "quantity_underflow"})
        h.group(31, h.initial["plans"][:1])
        result = h.finish()
        self.assertEqual(
            old.books["polymarket:987", "outcome"].validity, "not_initialized"
        )
        self.assertEqual(
            [(r["start_ns"], r["end_ns"], r["state"]) for r in h.rows("bundle")],
            [
                ("10", "14", "UNAVAILABLE"),
                ("14", "17", "PARTIAL"),
                ("17", "28", "AVAILABLE_UNDER_POLICY"),
                ("28", "31", "PARTIAL"),
                ("31", "40", "AVAILABLE_UNDER_POLICY"),
            ],
        )
        summary = result["summary"]
        self.assertEqual(summary["nonduplicate_trade_observations"], 2)
        self.assertEqual(summary["trades"]["duplicate"], 1)
        self.assertEqual(
            next(
                d["state_ns"] for d in summary["durations"] if d["entity"] == "bundle"
            ),
            {"UNAVAILABLE": "4", "PARTIAL": "6", "AVAILABLE_UNDER_POLICY": "20"},
        )
        for duration in summary["durations"]:
            self.assertEqual(sum(map(int, duration["state_ns"].values())), 30)

    def test_scope_boundary_in_silence_uses_prior_not_future_books(self):
        h = self.harness(mixed=True, scopes=True)
        h.window()
        h.group(29, h.initial["plans"])
        h.finish()
        self.assertEqual(
            [
                (r["scope"], r["start_ns"], r["end_ns"], r["state"])
                for r in h.rows("bundle")
            ],
            [
                (0, "10", "23", "UNAVAILABLE"),
                (1, "23", "29", "UNAVAILABLE"),
                (1, "29", "40", "AVAILABLE_UNDER_POLICY"),
            ],
        )
        unresolved = h.rows("member:polymarket:map-one")
        self.assertEqual(
            [(r["start_ns"], r["end_ns"], r["state"]) for r in unresolved],
            [("10", "23", "NOT_CAPTURED")],
        )

    def test_mixed_uncaptured_never_available_and_all_uncaptured_diagnostic(self):
        h = self.harness(mixed=True)
        h.window()
        h.group(12, h.initial["plans"])
        h.finish()
        self.assertEqual(h.rows("bundle")[-1]["state"], "PARTIAL")
        self.assertEqual(h.rows("bundle")[-1]["uncaptured_members"], 1)
        other = self.root / "other"
        other.mkdir()
        with self.assertRaisesRegex(ProtocolError, "no native risk plans"):
            Harness(other, all_uncaptured=True)

    def test_window_evidence_is_not_latched_reason_and_clip_preserves_origin(self):
        h = self.harness()
        h.window(0, 20, {"kind": "lane_missing"})
        h.window(20, 40)
        h.group(25, h.initial["plans"])
        h.finish()
        rows = h.rows(
            book_id({"instrument": "polymarket:123", "orientation": "outcome"})
        )
        self.assertEqual(
            [(r["start_ns"], r["end_ns"], r["state"], r["evidence"]) for r in rows],
            [
                ("10", "20", "unusable", "lane_missing"),
                ("20", "25", "unusable", "unknown"),
                ("25", "40", "usable", "unknown"),
            ],
        )
        self.assertEqual(rows[0]["evidence_source"]["start_ns"], "0")
        self.assertEqual(rows[1]["reason"], {"kind": "lane_missing"})

    def test_same_time_transitions_have_no_positive_intermediate_interval(self):
        h = self.harness()
        h.window()
        h.group(12, h.initial["plans"])
        h.group(12, h.initial["plans"], {"kind": "connection_closed"})
        h.group(12, h.initial["plans"])
        h.finish()
        self.assertEqual(
            [(r["start_ns"], r["end_ns"], r["state"]) for r in h.rows("bundle")],
            [("10", "12", "UNAVAILABLE"), ("12", "40", "AVAILABLE_UNDER_POLICY")],
        )

    def test_deterministic_semantics_across_attempts(self):
        outputs = []
        for attempt in ("a" * 32, "b" * 32):
            root = self.root / attempt
            root.mkdir()
            h = Harness(root, attempt=attempt)
            h.window()
            h.group(13, h.initial["plans"])
            h.finish()
            outputs.append(
                {
                    name: (h.output / name).read_bytes()
                    for name in ("intervals.ndjson", "manifest.json", "summary.json")
                }
            )
        self.assertEqual(outputs[0], outputs[1])

    def test_missing_terminal_and_incomplete_window_cannot_commit(self):
        h = self.harness()
        h.window(0, 20)
        with self.assertRaisesRegex(ProtocolError, "missing terminal"):
            h.strategy.finish()
        self.assertFalse((h.output / "content_receipt.json").exists())
        other_root = self.root / "incomplete"
        other_root.mkdir()
        other = Harness(other_root)
        self.addCleanup(other.strategy.writer.stream.close)
        other.window(0, 20)
        with self.assertRaisesRegex(ProtocolError, "incomplete window"):
            other.finish()
        self.assertFalse((h.output / "content_receipt.json").exists())

    def test_failed_finish_cannot_be_recovered_by_later_terminal(self):
        h = self.harness()
        h.window()
        with self.assertRaisesRegex(ProtocolError, "missing terminal"):
            h.strategy.finish()
        with self.assertRaisesRegex(ProtocolError, "closed coverage"):
            h.send("terminal", {"cuts": str(h.seq - 1)})
        self.assertFalse((h.output / "content_receipt.json").exists())

    def test_snapshot_and_transport_binding(self):
        h = self.harness()
        for field, value in (
            ("end_ns", "41"),
            ("start_ns", "9"),
            ("lower_bound", "expand_to_window_start"),
            ("pins", [{"derivative_address": "3" * 64, "receipt_sha256": "4" * 64}]),
            ("plans", []),
        ):
            with self.subTest(field=field):
                from replay.strategy_sdk import PreparedInput

                inp = PreparedInput(h.cfg)
                initial = plain(h.initial)
                initial[field] = value
                with self.assertRaisesRegex(ProtocolError, "snapshot/transport"):
                    inp.bind(freeze(initial))
        with self.assertRaisesRegex(ProtocolError, "snapshot pin"):
            build(
                freeze({**h.context, "config": {**h.cfg, "snapshot_sha256": "0" * 64}})
            )

    def test_tamper_hash_schema_interval_reference_and_arithmetic(self):
        h = self.harness()
        h.window()
        h.group(15, h.initial["plans"])
        result = h.finish()
        original = h.rows()
        for mutate in (
            lambda rows: rows[0].update(extra=True),
            lambda rows: rows[0].update(start_ns="11"),
            lambda rows: next(r for r in rows if r["kind"] == "book" and r["source"])[
                "source"
            ]["pin"].update(receipt_sha256="9" * 64),
            lambda rows: next(r for r in rows if r["kind"] == "bundle").update(
                usable_books=2
            ),
            lambda rows: next(r for r in rows if r["kind"] == "book" and r["source"])[
                "source"
            ].update(visible_ns="39"),
        ):
            rows = copy.deepcopy(original)
            mutate(rows)
            payload = b"".join(encoded(row) + b"\n" for row in rows)
            (h.output / "intervals.ndjson").write_bytes(payload)
            manifest = copy.deepcopy(result["manifest"])
            manifest["intervals"] = {
                "sha256": hashlib.sha256(payload).hexdigest(),
                "byte_length": len(payload),
                "records": len(rows),
            }
            with self.assertRaises(ProtocolError):
                validate_content(h.output, h.snapshot, manifest)
        (h.output / "intervals.ndjson").write_bytes(
            b"".join(encoded(row) + b"\n" for row in original)[:-1]
        )
        with self.assertRaisesRegex(ProtocolError, "truncation"):
            read_provisional(h.output, h.root / "context", expected_sha256=h.sha)

    def test_finish_crashes_receipt_last_and_no_overwrite(self):
        h = self.harness()
        h.window()
        h.send("terminal", {"cuts": str(h.seq - 1)})
        real = supervisor.write_json_durable

        def fail(path, value):
            if path.name == "manifest.json":
                raise OSError("crash")
            real(path, value)

        with (
            patch("replay.bundle_coverage.write_json_durable", side_effect=fail),
            self.assertRaises(OSError),
        ):
            h.strategy.finish()
        self.assertFalse((h.output / "content_receipt.json").exists())
        with self.assertRaises(ProtocolError):
            build(freeze(h.context))

    def test_repeated_fault_windows_preserve_each_source_interval(self):
        h = self.harness()
        why = {"kind": "lane_invalid", "detail": "seal mismatch"}
        h.window(0, 20, why)
        h.window(20, 40, why)
        h.finish()
        rows = h.rows(
            book_id({"instrument": "polymarket:123", "orientation": "outcome"})
        )
        self.assertEqual(
            [
                (r["start_ns"], r["end_ns"], r["evidence_source"]["start_ns"])
                for r in rows
            ],
            [("10", "20", "0"), ("20", "40", "20")],
        )

    def test_summary_rejects_boolean_version_even_though_python_equality_matches(self):
        h = self.harness()
        h.window()
        h.finish()
        path = h.output / "summary.json"
        summary = json.loads(path.read_bytes())
        summary["version"] = True
        path.write_bytes(encoded(summary))
        with self.assertRaisesRegex(ProtocolError, "summary identity/schema"):
            read_provisional(h.output, h.root / "context", expected_sha256=h.sha)

    def test_uncaptured_later_scope_retains_denominator_despite_usable_books(self):
        h = self.harness(scopes="uncaptured")
        h.window()
        h.group(12, h.initial["plans"])
        h.finish()
        self.assertEqual(
            [
                (r["scope"], r["start_ns"], r["end_ns"], r["state"])
                for r in h.rows("bundle")
            ],
            [
                (0, "10", "12", "UNAVAILABLE"),
                (0, "12", "23", "AVAILABLE_UNDER_POLICY"),
                (1, "23", "40", "NOT_CAPTURED"),
            ],
        )
        self.assertEqual(h.rows("bundle")[-1]["required_books"], 0)

    def test_expanded_prologue_initializes_but_reports_only_requested_interval(self):
        h = self.harness(lower_bound="expand_to_window_start")
        h.window()
        h.group(5, h.initial["plans"], trades=("observed",))
        result = h.finish()
        self.assertEqual(
            [(r["start_ns"], r["end_ns"], r["state"]) for r in h.rows("bundle")],
            [("10", "40", "AVAILABLE_UNDER_POLICY")],
        )
        self.assertEqual(result["summary"]["nonduplicate_trade_observations"], 0)

    def test_clip_does_not_accept_expanded_groups(self):
        h = self.harness()
        h.window()
        with self.assertRaisesRegex(ProtocolError, "group before requested start"):
            h.group(5, h.initial["plans"])

    def test_invalid_second_transition_never_reaches_strategy(self):
        h = self.harness()
        h.window()
        before = h.strategy.books
        plans = h.initial["plans"]
        transitions = [h.transition(p, 12) for p in plans]
        transitions[1]["previous_revision"] = "9"
        ref = h.ref(12)
        with self.assertRaisesRegex(ProtocolError, "revision"):
            h.send(
                "cut",
                {
                    "origin": {
                        "kind": "group",
                        "pin": h.pin,
                        "first": ref["address"],
                        "last": ref["address"],
                        "visible_ns": "12",
                    },
                    "market_events": [],
                    "book_transitions": transitions,
                },
            )
        self.assertIs(h.strategy.books, before)
        self.assertEqual(h.strategy.sequence, 1)
        with self.assertRaises(ProtocolError):
            h.strategy.finish()
        self.assertFalse((h.output / "content_receipt.json").exists())

    def test_quiet_revision_churn_has_bounded_retention_and_no_tick_fragments(self):
        import gc
        import tracemalloc

        h = self.harness()
        h.window()
        tracemalloc.start()
        try:
            retained = []
            for i in range(1001):
                h.group(12, h.initial["plans"])
                if i in (100, 1000):
                    gc.collect()
                    retained.append(tracemalloc.get_traced_memory()[0])
            self.assertLess(retained[1] - retained[0], 100_000)
        finally:
            tracemalloc.stop()
        h.finish()
        self.assertEqual(len(h.rows("bundle")), 2)
        self.assertEqual(len(h.rows()), 8)

    def test_writer_budgets_fsync_failure_and_independent_validation_failure(self):
        h = self.harness()
        h.window()
        h.strategy.writer.max_records = 0
        with self.assertRaisesRegex(ProtocolError, "output budget"):
            h.group(12, h.initial["plans"])
        with self.assertRaises(ProtocolError):
            h.strategy.finish()
        self.assertFalse((h.output / "content_receipt.json").exists())
        for failure in ("fsync", "validator"):
            root = self.root / failure
            root.mkdir()
            other = Harness(root)
            self.addCleanup(other.strategy.writer.stream.close)
            other.window()
            other.send("terminal", {"cuts": str(other.seq - 1)})
            target = (
                "replay.strategy_sdk.os.fsync"
                if failure == "fsync"
                else "replay.bundle_coverage.validate_content"
            )
            with (
                patch(target, side_effect=OSError("injected failure")),
                self.assertRaises(OSError),
            ):
                other.strategy.finish()
            self.assertFalse((other.output / "content_receipt.json").exists())

    def test_real_success_reader_with_fake_supervisor_artifacts(self):
        h = self.harness()
        c = supervisor_config()
        c["transport"].update(
            run_id="coverage-test",
            groups=["coverage"],
            plans=h.initial["plans"],
            start_ns="10",
            end_ns="40",
            inputs=[{"directory": "/unused", **h.pin}],
        )
        c["strategies"] = {
            "coverage": {
                "factory": "replay.bundle_coverage:build",
                "revision": "test",
                "config": h.cfg,
            }
        }
        identity = supervisor.identity(c)
        h.strategy.binding["identity"] = identity
        h.window()
        h.finish()
        run = self.root / "run"
        participant = run / h.context["attempt_id"] / "coverage"
        participant.mkdir(parents=True)
        h.output.rename(participant / "output")
        supervisor.write_json_durable(run / "run.json", c)
        terminal = h.seq
        attestation = {
            "version": 1,
            "identity": identity,
            "attempt": h.context["attempt_id"],
            "group": "coverage",
            "terminal": terminal,
        }
        supervisor.write_json_durable(participant / "complete.json", attestation)
        supervisor.write_json_durable(
            participant.parent / "result.json",
            {
                "version": 1,
                "identity": identity,
                "attempt": h.context["attempt_id"],
                "outcome": "success",
                "fatal": False,
                "progress": terminal,
                "terminal": terminal,
                "participants": {"publisher": 0, "coverage": 0},
            },
        )
        with self.assertRaises(FileNotFoundError):
            read_completed(run, "coverage")
        supervisor.write_json_durable(
            run / "SUCCESS.json",
            {
                "version": 1,
                "identity": identity,
                "attempt": h.context["attempt_id"],
                "terminal": terminal,
                "outputs": {"coverage": h.context["attempt_id"] + "/coverage/output"},
            },
        )
        self.assertEqual(
            read_completed(run, "coverage")["receipt"]["identity"], identity
        )
        path = participant / "output/content_receipt.json"
        receipt = json.loads(path.read_bytes())
        receipt["attempt_id"] = "b" * 32
        path.write_bytes(encoded(receipt))
        with self.assertRaisesRegex(ProtocolError, "supervisor/content binding"):
            read_completed(run, "coverage")


if __name__ == "__main__":
    unittest.main()
