"""Invoked ONLY by the opt-in Rust contract test, which owns disposable inputs."""

import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from replay import supervisor as s
from replay.coverage_output import book_id, read_completed
from replay.preparation import prepare
from replay.tests.test_preparation import G2, R1, R2, config, detail
from replay.tests.test_supervisor import config as supervisor_config


def acceptance(config_path, publisher):
    test = unittest.TestCase()
    transport = s.read(config_path)
    del transport["attempt_id"]
    url = os.environ["REPLAY_REDIS_URL"]
    with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, REDIS_URL=url):
        root = Path(tmp)
        first = detail()
        # Keep one selected PM market (two native tokens) and its unselected sibling.
        first["context"]["markets"] = [
            m
            for m in first["context"]["markets"]
            if m["target_id"].startswith("polymarket:")
        ]
        first["context"]["targets"] = [
            t
            for t in first["context"]["targets"]
            if t["target_id"].startswith("polymarket:")
        ]
        later = copy.deepcopy(first)
        later["context"]["markets"] = [
            m for m in later["context"]["markets"] if m["selected"]
        ]
        later.update(run_id=R2, generated_at=G2)
        later["source"] = {k: v.replace(R1, R2) for k, v in first["source"].items()}
        later["source"]["report_sha256"] = "c" * 64
        later["origin"] = {**later["source"], "run_id": R2, "generated_at": G2}
        preparation = config(first)
        preparation.update(
            pins=[
                {k: p[k] for k in ("derivative_address", "receipt_sha256")}
                for p in transport["inputs"]
            ],
            end_ns="80",
            authorities=[
                {
                    "venue": "polymarket",
                    "lane": "primary",
                    "price_scale": "2",
                    "quantity_scale": "0",
                }
            ],
            occurrences=[
                {
                    "run_id": R1,
                    "start_ns": "10",
                    "end_ns": "23",
                    "source": first["source"],
                },
                {
                    "run_id": R2,
                    "start_ns": "23",
                    "end_ns": "80",
                    "source": later["source"],
                },
            ],
        )
        prepare(
            preparation,
            root / "context",
            universe=lambda occurrence, _: (
                first if occurrence["run_id"] == R1 else later
            ),
        )
        sha = s.read(root / "context/receipt.json")["snapshot_sha256"]
        c = supervisor_config(str(Path(publisher).resolve()))
        # Independently authored Rust plans, not snapshot-derived.
        c["transport"] = transport
        c["strategies"] = {
            "coverage": {
                "factory": "replay.bundle_coverage:build",
                "revision": "synthetic-acceptance-v1",
                "config": {
                    "version": 1,
                    "snapshot_directory": str(root / "context"),
                    "snapshot_sha256": sha,
                },
            }
        }
        c["limits"].update(stall_seconds=3, attempt_seconds=10, run_seconds=40)
        s.validate(c)
        run = root / "run"
        original = s.write_json_durable

        def crash_before_success(path, value):
            if path.name == "SUCCESS.json":
                raise OSError("injected crash before supervisor receipt")
            original(path, value)

        with patch.object(s, "write_json_durable", side_effect=crash_before_success):
            with test.assertRaisesRegex(OSError, "injected crash"):
                s.run(c, run, url)
        old = s.read_state(run, s.identity(c))["active"]["id"]
        old_output = run / old / "coverage/output"
        test.assertTrue((old_output / "content_receipt.json").exists())
        with test.assertRaises(FileNotFoundError):
            read_completed(run, "coverage")

        receipt = s.run(c, run, url)
        result = read_completed(run, "coverage")
        test.assertNotEqual(receipt["attempt"], old)
        test.assertEqual(s.read_state(run, s.identity(c))["attempts"], 2)
        test.assertTrue((run / old / "interrupted.json").exists())
        summary = result["summary"]
        test.assertEqual(summary["vendor_completeness"], "NOT_PROVEN")
        test.assertEqual(summary["membership_basis"], "caller_pinned_expectations")
        test.assertIs(summary["history_complete"], False)
        test.assertEqual(
            summary["trades"],
            {
                "observed": 1,
                "duplicate": 1,
                "applied": 0,
                "not_authority": 0,
                "invalidated": 0,
            },
        )
        test.assertEqual(summary["nonduplicate_trade_observations"], 1)
        durations = {
            (d["scope"], d["entity"]): d["state_ns"] for d in summary["durations"]
        }
        a = book_id({"instrument": "polymarket:123", "orientation": "outcome"})
        b = book_id({"instrument": "polymarket:987", "orientation": "outcome"})
        # Independent arithmetic from fixture timestamps, not the implementation.
        test.assertEqual(
            durations,
            {
                (0, a): {"not_initialized": "4", "usable": "9"},
                (0, b): {"not_initialized": "13"},
                (0, "member:polymarket:series"): {"UNAVAILABLE": "4", "PARTIAL": "9"},
                (0, "member:polymarket:map-one"): {"NOT_CAPTURED": "13"},
                (0, "bundle"): {"UNAVAILABLE": "4", "PARTIAL": "9"},
                (1, a): {"usable": "36", "unusable": "21"},
                (1, b): {"not_initialized": "6", "usable": "24", "unusable": "27"},
                (1, "member:polymarket:series"): {
                    "PARTIAL": "12",
                    "AVAILABLE_UNDER_POLICY": "24",
                    "UNAVAILABLE": "21",
                },
                (1, "bundle"): {
                    "PARTIAL": "12",
                    "AVAILABLE_UNDER_POLICY": "24",
                    "UNAVAILABLE": "21",
                },
            },
        )
        output = run / receipt["outputs"]["coverage"]
        rows = [
            json.loads(line)
            for line in (output / "intervals.ndjson").read_bytes().splitlines()
        ]
        test.assertEqual(
            [
                (r["scope"], r["start_ns"], r["end_ns"], r["state"])
                for r in rows
                if r["entity"] == "bundle"
            ],
            [
                (0, "10", "14", "UNAVAILABLE"),
                (0, "14", "23", "PARTIAL"),
                (1, "23", "29", "PARTIAL"),
                (1, "29", "40", "AVAILABLE_UNDER_POLICY"),
                (1, "40", "61", "UNAVAILABLE"),
                (1, "61", "67", "PARTIAL"),
                (1, "67", "80", "AVAILABLE_UNDER_POLICY"),
            ],
        )
        for r in rows:
            test.assertEqual(r["vendor_completeness"], "NOT_PROVEN")
            if r["kind"] == "book":
                if r["start_ns"] in ("40", "45"):
                    test.assertEqual(
                        r["end_ns"], "45" if r["start_ns"] == "40" else "50"
                    )
                    test.assertEqual(r["evidence"], "visible_clock_regression")
                    test.assertEqual(
                        r["reason"]["kind"],
                        "visible_clock_regression"
                        if r["start_ns"] == "40"
                        else "connection_closed",
                    )
                    test.assertEqual(
                        r["evidence_source"],
                        {
                            "kind": "window",
                            "pin": preparation["pins"][1],
                            "start_ns": "40",
                            "end_ns": "50",
                        },
                    )
                else:
                    test.assertEqual(r["evidence"], "unknown")
                    test.assertIsNone(r["evidence_source"])
            if r["entity"] == a and r["start_ns"] == "50":
                test.assertEqual(
                    (r["state"], r["evidence"], r["reason"]),
                    ("unusable", "unknown", {"kind": "connection_closed"}),
                )
        test.assertEqual(
            sum(r["kind"] == "book" and r["start_ns"] == "45" for r in rows), 2
        )
        test.assertTrue(any(r["entity"] == a and r["start_ns"] == "50" for r in rows))
        fresh = root / "fresh"
        fresh_receipt = s.run(c, fresh, url)
        fresh_result = read_completed(fresh, "coverage")
        fresh_output = fresh / fresh_receipt["outputs"]["coverage"]
        for name in ("intervals.ndjson", "summary.json", "manifest.json"):
            test.assertEqual(
                (old_output / name).read_bytes(), (output / name).read_bytes()
            )
            test.assertEqual(
                (fresh_output / name).read_bytes(), (output / name).read_bytes()
            )
        test.assertEqual(
            result["receipt"]["semantic_sha256"],
            fresh_result["receipt"]["semantic_sha256"],
        )
        test.assertNotEqual(receipt["attempt"], fresh_receipt["attempt"])
        test.assertEqual(s.run(c, run, url), receipt)  # committed rerun is read-only

        # Corrupt a private copy; even successful synthetic inputs stay immutable.
        bad = copy.deepcopy(c)
        source = Path(transport["inputs"][-1]["directory"])
        damaged = root / "damaged" / source.name
        shutil.copytree(source, damaged)
        with (damaged / "events.ndjson.zst").open("r+b") as stream:
            stream.truncate(1)
        bad["transport"]["inputs"][-1]["directory"] = str(damaged)
        # Inspect actual Redis state before cleanup, not merely absent SUCCESS.
        attempt = uuid.uuid4().hex
        publisher_config = root / "bad-publisher.json"
        s.write_json_durable(
            publisher_config, {**bad["transport"], "attempt_id": attempt}
        )
        admin = s.client(url, 2)
        try:
            process = subprocess.run(
                [c["publisher"], str(publisher_config)], capture_output=True, timeout=10
            )
            test.assertEqual(process.returncode, 20)
            state = admin.hgetall(s.keys(bad, attempt)[1])
            test.assertEqual(state["terminal"], "")
            test.assertGreater(int(state["published"]), 1)  # genuine accepted prefix
        finally:
            admin.delete(*s.keys(bad, attempt))
            admin.close()
        failed = root / "failed"
        with test.assertRaises(s.AttemptFailure):
            s.run(bad, failed, url)
        test.assertFalse((failed / "SUCCESS.json").exists())
        test.assertFalse(list(failed.glob("*/coverage/output/content_receipt.json")))
        results = list(failed.glob("*/result.json"))
        test.assertEqual(len(results), 1)
        test.assertIsNone(s.read(results[0])["terminal"])
        with test.assertRaises(FileNotFoundError):
            read_completed(failed, "coverage")
        print(
            "coverage acceptance: exact durations, scope boundary, fault/recovery, "
            "trade duplicate, dual receipts, crash retry/fresh determinism, "
            "late integrity failure without terminal: PASS"
        )


if __name__ == "__main__":
    acceptance(*sys.argv[1:])
