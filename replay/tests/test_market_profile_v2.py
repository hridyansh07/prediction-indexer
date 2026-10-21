"""Market profile version 2: availability parity, policy plumbing and reader cross-checks."""

import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from encoder import encode_stream
from replay.economic_sdk import bounds
from replay.economic_sdk.profile import Collector, profile_files, profile_policy
from replay.economic_sdk.profile_stream_io import stream_lines
from replay.preparation import load_snapshot
from replay.streams.protocol import Decoder, ProtocolError
from replay.strategies.market_profile.strategy import profile_config, read_provisional, validate_content
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
    def test_declared_sections_preserve_every_group_output_byte(self):
        from replay.streams.protocol import SECTIONS
        from replay.tests.test_bundle_coverage import Harness as CoverageHarness

        tape = [json.loads(line) for line in
                (Path(__file__).resolve().parents[2] / "engine/crates/transport/tests/fixtures/strategy-sparse.ndjson").read_bytes().splitlines()]
        for name, factory, kwargs, output_attr, declared in (
                ("economic", ComplementHarness, {"known": True, "policy": v2_policy()}, "output", {"record"}),
                ("embedded", ComplementHarness, {"known": True, "policy": v2_policy(profile=PROFILE_POLICY)}, "output", {"record", "events"}),
                ("profile", Harness, {"policy": v2("availability", "levels", "transitions")}, "profile_output", {"record", "events"}),
                ("coverage", CoverageHarness, {}, "output", {"record", "events"})):
            outputs = []
            for index, sections in enumerate((SECTIONS, declared)):
                h = factory(self.sub(f"{name}-sections-{index}"), scopes=True, lower_bound="expand_to_window_start",
                            pin=tape[0]["body"]["pins"][0], wire_records=tape, sections=sections, **kwargs)
                root = getattr(h, output_attr)
                outputs.append({str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()})
            with self.subTest(group=name):
                self.assertEqual(outputs[0], outputs[1])  # Includes receipts and stored compressed bytes.

    def test_real_rust_full_and_sparse_wire_have_byte_identical_semantic_outputs(self):
        fixtures = Path(__file__).resolve().parents[2] / "engine/crates/transport/tests/fixtures"
        tapes = [[json.loads(line) for line in (fixtures / name).read_bytes().splitlines()]
                 for name in ("strategy-full.ndjson", "strategy-sparse.ndjson")]
        full, sparse = tapes
        total, retained = int(full[-1]["body"]["cuts"]), int(sparse[-1]["body"]["cuts"])
        self.assertLess(retained, total)
        print(f"v3 fixture retained/total cuts: {retained}/{total} ({retained / total:.1%})")
        profile = v2("availability", "levels", "transitions")
        policy = v2_policy(controls=[{"kind": "time_shift", "shift_ns": ["5"]}],
                           controls_episodes=True, controls_slices=True, audit_intervals=True,
                           profile=PROFILE_POLICY)
        for name, factory, kwargs, output_attr in (
                ("economic", ComplementHarness, {"known": True, "policy": policy}, "output"),
                ("profile", Harness, {"policy": profile}, "profile_output")):
            outputs = []
            receipts = []
            for index, tape in enumerate(tapes):
                h = factory(self.sub(f"{name}-{index}"), scopes=True, lower_bound="expand_to_window_start",
                            pin=tape[0]["body"]["pins"][0], wire_records=tape, **kwargs)
                root = getattr(h, output_attr)
                outputs.append({str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*")
                                if p.is_file() and p.name != "content_receipt.json"})
                receipts.append(json.loads((root / "content_receipt.json").read_bytes()))
                self.assertTrue(json.loads((root / "manifest.json").read_bytes())["files"])
            with self.subTest(strategy=name):
                self.assertEqual(outputs[0], outputs[1])
                self.assertEqual(receipts[0]["semantic_sha256"], receipts[1]["semantic_sha256"])
                self.assertNotEqual(receipts[0]["terminal"], receipts[1]["terminal"])

    def test_initial_fails_fast_for_missing_scope_or_shift_coverage(self):
        for index, (factory, field, missing, kwargs) in enumerate((
                (Harness, "retain_boundaries_ns", "23", {}),
                (ComplementHarness, "retain_shift_offsets_ns", "5",
                 {"policy": v2_policy(controls=[{"kind": "time_shift", "shift_ns": ["5"]}])}))):
            h = factory(self.sub(f"missing-{index}"), scopes=True, defer_initial=True, **kwargs)
            writers = h.profile.collector.writers if factory is Harness else h.strategy.writers
            for writer in writers.values():
                self.addCleanup(writer.stream.close)
            h.initial[field].remove(missing)
            h.decoder = Decoder("coverage-test", h.context["attempt_id"], h.initial, 65536)
            with self.subTest(field=field), self.assertRaisesRegex(ProtocolError, "v3 retention does not cover"):
                h.send("initial", h.initial)

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


STORAGE = {"version": 1, "profile": "zstd"}
PROFILE_ZSTD = "profile.ndjson.zst"


class ProfileStorageTests(Case):
    def produced(self, name, **kwargs):
        h = Harness(self.sub(name), policy=v2(), storage=STORAGE, mixed=True, scopes=True, **kwargs)
        exercise(h)
        return h, h.finish()

    def validate(self, h, manifest):
        return validate_content(h.profile_output, load_snapshot(h.root / "context", expected_sha256=h.sha), manifest)

    def decoded(self, h, manifest):
        entry = manifest["files"][PROFILE_ZSTD]
        return b"".join(stream_lines(h.profile_output / PROFILE_ZSTD, entry["logical"], entry["stored"],
                                    bounds.MAX_LINE, "profile"))

    def test_storage_preserves_exact_logical_bytes_summary_identity_and_all_other_files(self):
        from replay.tests.test_market_volatility import policy
        profile = policy()
        profile["groups"] = sorted(profile["groups"] + ["availability", "levels"])
        outputs = []
        for name, storage in (("plain", None), ("zstd", STORAGE)):
            h = Harness(self.sub(name), policy=profile, storage=storage, mixed=True, scopes=True)
            exercise(h)
            outputs.append((h, h.finish()))
        (a, plain), (b, framed) = outputs
        payload = (a.profile_output / "profile.ndjson").read_bytes()
        self.assertTrue(payload.endswith(b"\n"))
        self.assertEqual(self.decoded(b, framed["manifest"]), payload)
        self.assertEqual(framed["manifest"]["files"][PROFILE_ZSTD]["logical"],
                         {"sha256": hashlib.sha256(payload).hexdigest(), "byte_length": len(payload),
                          "records": payload.count(b"\n")})
        self.assertEqual(framed["manifest"]["files"][PROFILE_ZSTD]["logical"],
                         plain["manifest"]["files"]["profile.ndjson"])
        self.assertEqual(framed["manifest"]["experiment_sha256"], plain["manifest"]["experiment_sha256"])
        self.assertEqual(framed["summary"], plain["summary"])
        self.assertEqual(self.validate(b, framed["manifest"]), plain["summary"])
        self.assertNotEqual(framed["receipt"]["semantic_sha256"], plain["receipt"]["semantic_sha256"])
        self.assertEqual((plain["manifest"]["version"], framed["manifest"]["version"]), (2, 3))
        self.assertEqual(framed["manifest"]["storage"], STORAGE)
        for name in set(plain["manifest"]["files"]) - {"profile.ndjson"}:
            self.assertEqual((a.profile_output / name).read_bytes(), (b.profile_output / name).read_bytes(), name)
        self.assertEqual({p.name for p in b.profile_output.iterdir()},
                         set(framed["manifest"]["files"]) | {"manifest.json", "summary.json", "content_receipt.json"})

    def test_storage_is_closed_v2_standalone_only_and_never_an_economic_policy_field(self):
        config = {"version": 1, "snapshot_directory": "/unused", "snapshot_sha256": "0" * 64,
                  "policy": v2(), "storage": STORAGE}
        self.assertEqual(profile_config(config), config)
        for storage in (None, {}, {**STORAGE, "version": True}, {**STORAGE, "version": 2},
                        {**STORAGE, "profile": "plain"}, {**STORAGE, "level": 3}):
            with self.subTest(storage=storage), self.assertRaises(ProtocolError):
                profile_config({**config, "storage": storage})
        with self.assertRaisesRegex(ProtocolError, "standalone policy 2"):
            profile_config({**config, "policy": PROFILE_POLICY})
        with self.assertRaises(ProtocolError):
            profile_policy({**v2(), "storage": STORAGE}, standalone=True)
        with self.assertRaisesRegex(ProtocolError, "standalone policy 2"):
            Collector(v2(), {}, "0" * 64, self.root, "1" * 64, storage=STORAGE)

    def test_strict_read_rejects_extra_missing_and_provisional_representations(self):
        h, result = self.produced("files")
        for name in ("profile.ndjson", "profile.ndjson.open", PROFILE_ZSTD + ".open", "levels.ndjson.zst.open"):
            path = h.profile_output / name
            path.write_bytes(b"")
            try:
                with self.subTest(name=name), self.assertRaisesRegex(ProtocolError, "output file set"):
                    read_provisional(h.profile_output, h.root / "context", expected_sha256=h.sha)
            finally:
                path.unlink()
        path = h.profile_output / PROFILE_ZSTD
        saved = h.root / "saved-frame"
        path.rename(saved)
        try:
            with self.assertRaisesRegex(ProtocolError, "output file set"):
                self.validate(h, result["manifest"])
            path.symlink_to(saved)
            try:
                with self.assertRaisesRegex(ProtocolError, "regular output"):
                    self.validate(h, result["manifest"])
            finally:
                path.unlink()
            # A plaintext file does not substitute for the manifest-selected frame.
            (h.profile_output / "profile.ndjson").write_bytes(b"")
            with self.assertRaisesRegex(ProtocolError, "output file set"):
                self.validate(h, result["manifest"])
            (h.profile_output / "profile.ndjson").unlink()
        finally:
            saved.rename(path)
        for version in (1, 2):
            broken = copy.deepcopy(result["manifest"])
            broken["version"] = version
            with self.subTest(version=version), self.assertRaises(ProtocolError):
                self.validate(h, broken)
        for mutation in ("missing-storage", "plaintext-identity", "both", "neither"):
            broken = copy.deepcopy(result["manifest"])
            if mutation == "missing-storage":
                broken.pop("storage")
            elif mutation == "plaintext-identity":
                broken["files"][PROFILE_ZSTD] = broken["files"][PROFILE_ZSTD]["logical"]
            elif mutation == "both":
                broken["files"]["profile.ndjson"] = broken["files"][PROFILE_ZSTD]["logical"]
            else:
                broken["files"].pop(PROFILE_ZSTD)
            with self.subTest(mutation=mutation), self.assertRaises(ProtocolError):
                self.validate(h, broken)

    def test_strict_read_rejects_bad_frames_even_with_rehashed_stored_identity(self):
        h, result = self.produced("frames")
        path = h.profile_output / PROFILE_ZSTD
        frame = path.read_bytes()
        mutations = (frame[:-1], frame + b"trailing", frame + frame,
                     frame[:-1] + bytes([frame[-1] ^ 1]))
        for index, damaged in enumerate(mutations):
            broken = copy.deepcopy(result["manifest"])
            broken["files"][PROFILE_ZSTD]["stored"] = {
                "sha256": hashlib.sha256(damaged).hexdigest(), "byte_length": len(damaged)}
            path.write_bytes(damaged)
            try:
                with self.subTest(mutation=index), self.assertRaisesRegex(ProtocolError, "profile codec"):
                    self.validate(h, broken)
            finally:
                path.write_bytes(frame)
        for side, field, value in (("logical", "sha256", "0" * 64), ("stored", "sha256", "0" * 64),
                                   ("logical", "records", 1), ("logical", "byte_length", 1),
                                   ("stored", "byte_length", 1)):
            broken = copy.deepcopy(result["manifest"])
            broken["files"][PROFILE_ZSTD][side][field] = value
            with self.subTest(side=side, field=field), self.assertRaisesRegex(ProtocolError, "profile codec"):
                self.validate(h, broken)

    def test_rehashed_rows_still_require_independent_semantic_validation(self):
        h, result = self.produced("rows")
        payload = self.decoded(h, result["manifest"])
        rows = payload.splitlines(keepends=True)
        rows.pop(1)
        path = h.profile_output / PROFILE_ZSTD
        with path.open("wb") as sink:
            encoded = encode_stream(io.BytesIO(b"".join(rows)), sink)
        broken = copy.deepcopy(result["manifest"])
        broken["files"][PROFILE_ZSTD] = {
            "logical": {"sha256": encoded.logical.sha256, "byte_length": encoded.logical.byte_length,
                        "records": encoded.logical.line_count},
            "stored": {"sha256": encoded.stored.sha256, "byte_length": encoded.stored.byte_length}}
        with self.assertRaisesRegex(ProtocolError, "partition|incomplete"):
            self.validate(h, broken)

    def test_profile_caps_do_not_inherit_the_larger_stream_caps(self):
        h, result = self.produced("caps")
        for field, over in (("records", 2_000_001), ("byte_length", 512 * 1024**2 + 1)):
            broken = copy.deepcopy(result["manifest"])
            broken["files"][PROFILE_ZSTD]["logical"][field] = over
            with self.subTest(field=field), self.assertRaises(ProtocolError):
                self.validate(h, broken)
        path = h.profile_output / PROFILE_ZSTD
        for length in (64 * 1024, 64 * 1024 + 1):
            # Both are far below the levels 1 MiB bound; only the latter exceeds the profile bound.
            payload = b" " * (length - 3) + b"{}\n"
            with path.open("wb") as sink:
                encoded = encode_stream(io.BytesIO(payload), sink)
            broken = copy.deepcopy(result["manifest"])
            broken["files"][PROFILE_ZSTD] = {
                "logical": {"sha256": encoded.logical.sha256, "byte_length": length, "records": 1},
                "stored": {"sha256": encoded.stored.sha256, "byte_length": encoded.stored.byte_length}}
            with self.subTest(length=length), self.assertRaises(ProtocolError) as error:
                self.validate(h, broken)
            self.assertEqual("line/truncation" in str(error.exception), length > 64 * 1024)
        with patch("replay.economic_sdk.profile.MAX_PROFILE_ROWS", 1):
            limited = Harness(self.sub("limited"), policy=v2(), storage=STORAGE)
        limited.window()
        try:
            with self.assertRaisesRegex(ProtocolError, "output budget"):
                limited.terminal()
        finally:
            for writer in limited.profile.collector.writers.values():
                writer.stream.close()
        self.assertFalse((limited.profile_output / "manifest.json").exists())

    def test_failed_codec_or_frame_directory_sync_never_commits_or_removes_its_spool(self):
        for boundary in ("encode_stream", "fsync_directory"):
            h = Harness(self.sub(boundary), policy=v2(), storage=STORAGE)
            exercise(h)
            h.terminal()
            h.decoder.finish()
            with patch("replay.economic_sdk.framing." + boundary, side_effect=OSError("synthetic failure")):
                with self.subTest(boundary=boundary), self.assertRaisesRegex(OSError, "synthetic failure"):
                    h.strategy.finish()
            self.assertTrue((h.profile_output / "profile.ndjson").is_file())
            self.assertEqual((h.profile_output / PROFILE_ZSTD).exists(), boundary == "fsync_directory")
            self.assertFalse((h.profile_output / "manifest.json").exists())
            self.assertFalse((h.profile_output / "content_receipt.json").exists())

    def test_generic_corpus_commit_and_verify_bind_the_compressed_physical_file(self):
        from replay.corpus import results
        h, result = self.produced("corpus")
        destination = self.root / "committed"
        results.commit(h.profile_output, destination, corpus_key="offline", bundle_id="synthetic",
                       status="SUCCESS", bench_exit=0, game_state=None)
        receipt = results.verify(destination)
        self.assertEqual(receipt["corpus_receipt_version"], 1)
        stored = next(row for row in receipt["files"] if row["path"] == PROFILE_ZSTD)
        self.assertEqual({k: stored[k] for k in ("sha256", "byte_length")},
                         result["manifest"]["files"][PROFILE_ZSTD]["stored"])
        frame = destination / PROFILE_ZSTD
        frame.write_bytes(frame.read_bytes()[:-1])
        with self.assertRaisesRegex(ValueError, "mismatch"):
            results.verify(destination)


if __name__ == "__main__":
    unittest.main()
