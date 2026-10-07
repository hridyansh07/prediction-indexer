"""Market profile version 2: availability parity, policy plumbing and reader cross-checks."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from replay.economic_sdk.profile import Collector, profile_files, profile_policy
from replay.economic_sdk.reader import check_files
from replay.preparation import load_snapshot
from replay.streams.protocol import ProtocolError
from replay.strategies.market_profile.strategy import validate_content
from replay.tests.economic_scenarios import M, PROFILE_POLICY, ladder, v2_policy
from replay.tests.test_same_venue_complement import Harness as ComplementHarness
from replay.tests.test_market_profile import Harness, book_rows

V1_GROUPS = list(PROFILE_POLICY["groups"])


def v2(*extra, **overrides):
    policy = copy.deepcopy(PROFILE_POLICY)
    policy.update(version=2, groups=sorted(V1_GROUPS + list(extra)))
    policy.update(overrides)
    return policy


AVAILABILITY = v2("availability")


def exercise(h, *, evidence=False):
    """Delayed initialization, a quiet book, faults, recovery and trades."""
    if evidence:
        # Window-level faults latch the books; a clean window resets the evidence only.
        h.window(0, 20, {"kind": "lane_missing"})
        h.window(20, 40)
        h.group(26, h.initial["plans"])
        return
    h.window()
    h.group(11, trades=("observed", "duplicate"))
    h.group(14, h.initial["plans"][:1])
    h.group(17, h.initial["plans"][1:])
    h.group(19, trades=("applied",))
    h.group(28, h.initial["plans"][:1], {"kind": "quantity_underflow"})
    h.group(31, h.initial["plans"][:1])


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def sub(self, name):
        path = self.root / name
        path.mkdir()
        return path


class AvailabilityParityTests(Case):
    def test_availability_rows_and_durations_equal_bundle_coverage(self):
        for index, kwargs in enumerate((
                {}, {"mixed": True}, {"mixed": True, "scopes": True},
                {"mixed": True, "scopes": "uncaptured"}, {"scopes": True, "evidence": True})):
            with self.subTest(**kwargs):
                evidence = kwargs.pop("evidence", False)
                h = Harness(self.sub(f"p{index}"), policy=AVAILABILITY, with_coverage=True, **kwargs)
                exercise(h, evidence=evidence)
                result = h.finish()
                coverage = (h.output / "intervals.ndjson").read_bytes()
                self.assertTrue(coverage)
                self.assertEqual((h.profile_output / "availability.ndjson").read_bytes(), coverage)
                summary = json.loads((h.output / "summary.json").read_bytes())
                self.assertEqual(result["summary"]["availability_durations"], summary["durations"])
                self.assertEqual(result["manifest"]["version"], 2)
                self.assertEqual(set(result["manifest"]["files"]),
                                 {"incidents.ndjson", "pair_profile.ndjson", "profile.ndjson",
                                  "availability.ndjson"})

    def test_availability_book_usable_time_equals_profile_usable_time(self):
        h = Harness(self.sub("t"), policy=AVAILABILITY, mixed=True)
        exercise(h)
        result = h.finish()
        books = {(b["scope"], b["instrument"], b["orientation"]): int(b["usable_ns"])
                 for b in result["summary"]["books"]}
        self.assertTrue(any(books.values()))
        usable = {}
        for row in h.records("availability.ndjson"):
            if row["kind"] == "book" and row["state"] == "usable":
                usable[row["scope"], row["entity"]] = usable.get((row["scope"], row["entity"]), 0) \
                    + int(row["end_ns"]) - int(row["start_ns"])
        self.assertEqual(sum(usable.values()), sum(books.values()))

    def test_reader_rejects_availability_that_disagrees_with_profile_usable_time(self):
        a = Harness(self.sub("a"), policy=AVAILABILITY, mixed=True)
        exercise(a)
        a.finish()
        b = Harness(self.sub("b"), policy=AVAILABILITY, mixed=True)
        b.window()
        b.group(30, b.initial["plans"])  # a different, valid availability tape
        b.finish()
        self.assertEqual(a.sha, b.sha)
        payload = (b.profile_output / "availability.ndjson").read_bytes()
        self.assertNotEqual(payload, (a.profile_output / "availability.ndjson").read_bytes())
        manifest = json.loads((a.profile_output / "manifest.json").read_bytes())
        (a.profile_output / "availability.ndjson").write_bytes(payload)
        manifest["files"]["availability.ndjson"] = {
            "sha256": hashlib.sha256(payload).hexdigest(), "byte_length": len(payload),
            "records": payload.count(b"\n")}
        snapshot = load_snapshot(a.root / "context", expected_sha256=a.sha)
        with self.assertRaisesRegex(ProtocolError, "availability/profile usable time"):
            validate_content(a.profile_output, snapshot, manifest)

    def test_reader_rejects_a_tampered_availability_row(self):
        h = Harness(self.sub("x"), policy=AVAILABILITY, mixed=True)
        exercise(h)
        h.finish()
        manifest = json.loads((h.profile_output / "manifest.json").read_bytes())
        rows = h.records("availability.ndjson")
        rows[0]["end_ns"] = str(int(rows[0]["end_ns"]) + 1)
        payload = b"".join(json.dumps(r, sort_keys=True, separators=(",", ":")).encode() + b"\n" for r in rows)
        (h.profile_output / "availability.ndjson").write_bytes(payload)
        manifest["files"]["availability.ndjson"] = {
            "sha256": hashlib.sha256(payload).hexdigest(), "byte_length": len(payload), "records": len(rows)}
        snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        with self.assertRaises(ProtocolError):
            validate_content(h.profile_output, snapshot, manifest)

    def test_availability_file_is_part_of_the_derived_set(self):
        h = Harness(self.sub("m"), policy=AVAILABILITY)
        exercise(h)
        h.finish()
        manifest = json.loads((h.profile_output / "manifest.json").read_bytes())
        snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        for mutate, why in ((lambda f: f.pop("availability.ndjson"), "missing"),
                            (lambda f: f.update({"extra.ndjson": f["profile.ndjson"]}), "extra")):
            with self.subTest(why):
                broken = copy.deepcopy(manifest)
                mutate(broken["files"])
                with self.assertRaisesRegex(ProtocolError, "output file set"):
                    validate_content(h.profile_output, snapshot, broken)


class PolicyVersionTests(Case):
    def test_version_one_keeps_its_files_and_manifest_version(self):
        h = Harness(self.sub("v1"))
        h.window()
        ladder(h, 12, "polymarket:123", bids=((400, M),), asks=((450, M),))
        result = h.finish()
        self.assertEqual(result["manifest"]["version"], 1)
        self.assertEqual(set(result["manifest"]["files"]), set(profile_files(PROFILE_POLICY)))
        self.assertFalse((h.profile_output / "availability.ndjson").exists())
        self.assertNotIn("availability_durations", result["summary"])

    def test_version_two_without_new_groups_is_the_version_one_file_set(self):
        h = Harness(self.sub("v2"), policy=v2())
        h.window()
        ladder(h, 12, "polymarket:123", bids=((400, M),), asks=((450, M),))
        result = h.finish()
        self.assertEqual(result["manifest"]["version"], 2)
        self.assertEqual(set(result["manifest"]["files"]), set(profile_files(PROFILE_POLICY)))

    def test_version_one_policy_rejects_version_two_groups(self):
        policy = copy.deepcopy(PROFILE_POLICY)
        policy["groups"] = sorted(V1_GROUPS + ["availability"])
        with self.assertRaisesRegex(ProtocolError, "profile groups"):
            profile_policy(policy, standalone=True)
        with self.assertRaisesRegex(ProtocolError, "profile policy version"):
            profile_policy({**PROFILE_POLICY, "version": 3}, standalone=True)

    def test_embedded_profile_rejects_version_two_groups_at_construction(self):
        with self.assertRaisesRegex(ProtocolError, "standalone"):
            profile_policy(AVAILABILITY)
        self.assertEqual(profile_policy(AVAILABILITY, standalone=True)["version"], 2)
        h = Harness(self.sub("e"))
        with self.assertRaisesRegex(ProtocolError, "standalone"):
            Collector(AVAILABILITY, h.strategy.snapshot, h.sha, self.root, "0" * 64)
        with self.assertRaisesRegex(ProtocolError, "standalone"):
            ComplementHarness(self.sub("c"), policy=v2_policy(profile=copy.deepcopy(AVAILABILITY)))

    def test_manifest_version_must_follow_the_policy_version(self):
        h = Harness(self.sub("mv"), policy=AVAILABILITY)
        exercise(h)
        h.finish()
        manifest = json.loads((h.profile_output / "manifest.json").read_bytes())
        manifest["version"] = 1
        snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        with self.assertRaisesRegex(ProtocolError, "manifest version"):
            validate_content(h.profile_output, snapshot, manifest)


if __name__ == "__main__":
    unittest.main()
