"""Market profile version 2: availability parity, policy plumbing and reader cross-checks."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from replay.economic_sdk.profile import Collector, profile_files, profile_policy
from replay.preparation import load_snapshot
from replay.streams.protocol import ProtocolError
from replay.strategies.market_profile.strategy import validate_content
from replay.tests.economic_scenarios import M, PROFILE_POLICY, ladder, v2_policy
from replay.tests.test_same_venue_complement import Harness as ComplementHarness
from replay.tests.test_market_profile import Harness

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
    def test_the_version_two_example_turns_every_group_on(self):
        path = Path(__file__).parent.parent / "strategies" / "market_profile" / "config.v2.example.json"
        policy = json.loads(path.read_text())["policy"]
        self.assertEqual(policy["version"], 2)
        self.assertEqual(set(policy["groups"]), set(V1_GROUPS) | {"availability", "levels", "transitions"})
        profile_policy(policy, standalone=True)


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


# Recorded from commit 063bd5e, before the shared engine and policy version 2 existed.
V1_DIGESTS = {
    "content_receipt.json": "e16b597873546ac8db3ce12305d9808817cdc733195d8aea407d9a943b1ddc01",
    "coverage/content_receipt.json": "4ff12672b4563f6a208750b2efcd60d3658437b56e2de8e5e4bbdda8473fdd4e",
    "coverage/intervals.ndjson": "22bf42f7bb969e59958dcc86956afad7e8d51a038455d63542284344eb973633",
    "coverage/manifest.json": "66adb26b3babfcab372bbe8a1df7dbd0779c447bff54a5c927170ea40b026fb0",
    "coverage/summary.json": "67218b6b22e82116a4334bedd22872c217aa409ac22c53bb7c23577c6871fdf1",
    "incidents.ndjson": "8f3cca1bcb95b8ba9111dce356ec6158f545bc79d21da71a2bf6482247ce772a",
    "manifest.json": "d73562d9066e9fdc8c62b5e3c26dae011b1ab763b25c62d168cc94f257fc182b",
    "pair_profile.ndjson": "1f1e6aded6ed199c71bd70197b42d49bdf920d69bd517d9a9e8a91b0a6f10749",
    "profile.ndjson": "dec117f92622101badde904dcfa2ce25f986fad6e1681971510b6b931a8fbec2",
    "summary.json": "a859d52cc20255d8dd36a9e67e00ed6f6c74256f867864ee2d92b38a86f2b7b6",
}


class VersionOneByteIdentityTests(Case):
    def test_version_one_profile_and_coverage_outputs_are_unchanged(self):
        h = Harness(self.sub("id"), mixed=True, scopes=True, with_coverage=True)
        h.window()
        ladder(h, 12, "polymarket:123", bids=((400, 2 * M),), asks=((450, M),))
        ladder(h, 12, "kalshi:series", "outcome", bids=((560, 2 * M),))
        ladder(h, 12, "kalshi:series", "complement", bids=((400, M),))
        h.group(14, trades=("applied", "observed", "duplicate"))
        ladder(h, 16, "polymarket:123", bids=(), asks=((450, M),))
        ladder(h, 19, "polymarket:123", why={"kind": "connection_closed"})
        ladder(h, 24, "polymarket:123", bids=((450, M),), asks=((440, M),))
        h.finish()
        digests = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(h.profile_output.iterdir())}
        digests.update({"coverage/" + p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in sorted(h.output.iterdir())})
        self.assertEqual(digests, V1_DIGESTS)


if __name__ == "__main__":
    unittest.main()
