from __future__ import annotations

import hashlib
import json
import unittest

from replay.gate1 import Gate1Auditor, gate1_object, generation_metadata_object
from replay.stream import MemoryByteStreamer


def _line(index: int, local: int, payload: dict, *, event_kind: str = "venue_frame") -> bytes:
    record = {
        "envelope_version": 2,
        "delivery_index": index,
        "record_id": f"pm-a-{local}",
        "visible_ns": 100 + index,
        "monotonic_ns": 50 + index,
        "venue": "polymarket",
        "stream": "public_book" if event_kind == "venue_frame" else "process",
        "connection_epoch": "a",
        "local_counter": local,
        "source_cursor": None,
        "kind": event_kind,
        "raw_payload": json.dumps(payload, separators=(",", ":")),
    }
    return json.dumps(record, separators=(",", ":")).encode() + b"\n"


class Gate1Tests(unittest.TestCase):
    def test_a_thin_tape_returns_no_and_names_every_missing_observable(self) -> None:
        opened = {
            "event": "connection_opened",
            "target_digest": "x",
            "target_metadata_digest": "missing",
            "asset_ids": ["asset"],
            "delivers_deltas": True,
            "fsync_interval_seconds": 0.25,
            "clock_scope": {
                "scope_id": "boot",
                "comparable_across_processes": True,
            },
        }
        closed = {"event": "connection_closed"}
        data = (
            _line(1, 1, opened, event_kind="control")
            + _line(2, 2, {"event_type": "book", "bids": [], "asks": [], "hash": "h"})
            + _line(3, 3, closed, event_kind="control")
        )
        streamer = MemoryByteStreamer(
            {
                "spool/venue=polymarket/date=2026-01-01/a.ndjson": data,
                "live/coverage.json": b'{"sightings":[]}\n',
            },
            chunk_size=3,
        )
        report = Gate1Auditor().audit(streamer)
        failed = {check.name for check in report.checks if check.status == "FAIL"}
        self.assertFalse(report.passed)
        self.assertIn("market_rules_and_metadata", failed)
        self.assertIn("polymarket_recovery_anchors", failed)
        self.assertIn("trade_and_fill_observability", failed)
        self.assertIn("game_event_observability", failed)

        # Market creation and resolution are reported but do not block. They
        # arrive only on a subscription sending `custom_feature_enabled`, which
        # capture does not set, so a blocking check here would gate every later
        # analysis on a capture change nobody has asked for.
        advisory = {check.name for check in report.checks if check.status == "ADVISORY"}
        self.assertEqual(advisory, {"market_lifecycle_observability"})
        self.assertNotIn("market_lifecycle_observability", failed)

    def test_report_is_identical_across_storage_chunking(self) -> None:
        data = _line(
            1,
            1,
            {
                "event": "connection_opened",
                "target_digest": "broadcast",
                "asset_ids": [],
                "delivers_deltas": False,
                "fsync_interval_seconds": 0.25,
                "clock_scope": {
                    "scope_id": "boot",
                    "comparable_across_processes": True,
                },
            },
            event_kind="control",
        ) + _line(2, 2, {"event": "connection_closed"}, event_kind="control")
        objects = {"spool/venue=polymarket_rtds/date=2026-01-01/a.ndjson": data}
        one = Gate1Auditor().audit(MemoryByteStreamer(objects, chunk_size=1)).as_record()
        many = Gate1Auditor().audit(MemoryByteStreamer(objects, chunk_size=4096)).as_record()
        self.assertEqual(one, many)


def _seal(data: bytes, *, data_file: str) -> bytes:
    return json.dumps(
        {
            "data_file": data_file,
            "byte_length": len(data),
            "line_count": data.count(b"\n"),
            "sha256": hashlib.sha256(data).hexdigest(),
        },
        separators=(",", ":"),
    ).encode()


class SealedSegmentTests(unittest.TestCase):
    """A silent lane and a deleted segment must not produce the same verdict.

    `segments_seen` is populated per parsed record, so a sealed segment holding
    no records never enters it. Reporting that as "the segment is absent" turns
    the normal state of a low-volume venue into an integrity failure, and hides
    the real thing that message is for.
    """

    LANE = "spool/lane=limitless/date=2026-08-05"

    def _report(self, objects: dict[str, bytes]):
        return Gate1Auditor().audit(MemoryByteStreamer(objects))

    def _check(self, report, name: str):
        return next(item for item in report.as_record()["checks"] if item["name"] == name)

    def _populated(self) -> dict[str, bytes]:
        # One segment carrying a record, so `segments_seen` is non-empty and the
        # check is not vacuously false.
        data = _line(1, 1, {"bids": [], "asks": []})
        return {
            f"{self.LANE}/full.ndjson": data,
            f"{self.LANE}/full.seal.json": _seal(data, data_file="full.ndjson"),
        }

    def test_an_empty_sealed_segment_is_evidence_not_a_missing_segment(self) -> None:
        objects = self._populated()
        objects[f"{self.LANE}/quiet.ndjson"] = b""
        objects[f"{self.LANE}/quiet.seal.json"] = _seal(b"", data_file="quiet.ndjson")

        check = self._check(self._report(objects), "every_segment_is_sealed")
        self.assertEqual(check["status"], "PASS")
        self.assertEqual(check["evidence"]["failures"], [])
        self.assertEqual(check["evidence"]["empty_segments"], 1)
        self.assertEqual(
            check["evidence"]["empty_examples"], [f"{self.LANE}/quiet.ndjson"]
        )

    def test_a_seal_whose_segment_is_really_gone_still_fails(self) -> None:
        objects = self._populated()
        # The seal names a data file the dataset does not carry at all.
        objects[f"{self.LANE}/gone.seal.json"] = _seal(b"", data_file="gone.ndjson")

        check = self._check(self._report(objects), "every_segment_is_sealed")
        self.assertEqual(check["status"], "FAIL")
        self.assertEqual(
            check["evidence"]["failures"],
            [f"{self.LANE}/gone.ndjson: sealed but the segment is absent"],
        )
        self.assertEqual(check["evidence"]["empty_segments"], 0)

    def test_an_empty_segment_is_verified_against_its_seal_not_skipped(self) -> None:
        # The old loop never reached an empty segment, so a seal that lied about
        # it was accepted. Both the length and the digest must now be checked.
        objects = self._populated()
        objects[f"{self.LANE}/quiet.ndjson"] = b""
        objects[f"{self.LANE}/quiet.seal.json"] = json.dumps(
            {
                "data_file": "quiet.ndjson",
                "byte_length": 41,
                "line_count": 0,
                "sha256": hashlib.sha256(b"not what is on disk").hexdigest(),
            },
            separators=(",", ":"),
        ).encode()

        check = self._check(self._report(objects), "every_segment_is_sealed")
        self.assertEqual(check["status"], "FAIL")
        failures = check["evidence"]["failures"]
        self.assertIn(f"{self.LANE}/quiet.ndjson: byte_length 41 != 0", failures)
        self.assertIn(
            f"{self.LANE}/quiet.ndjson: sha256 disagrees with the bytes", failures
        )

    def test_a_segment_with_records_but_no_seal_still_fails(self) -> None:
        data = _line(1, 1, {"bids": [], "asks": []})
        check = self._check(
            self._report({f"{self.LANE}/unsealed.ndjson": data}),
            "every_segment_is_sealed",
        )
        self.assertEqual(check["status"], "FAIL")
        self.assertEqual(
            check["evidence"]["failures"],
            [f"{self.LANE}/unsealed.ndjson: no seal"],
        )


class GateOneObjectFilterTests(unittest.TestCase):
    """What the gate will read. An input it excludes is an input it cannot fail on.

    `iter_ndjson_lines` skips a non-`.ndjson` key with a bare `continue`, and this
    filter runs earlier still, at `DirectoryByteStreamer` construction. Both are
    silent. A shape that belongs in the dataset and is not named here does not
    make the gate fail loudly — it makes the evidence invisible.
    """

    V2_METADATA = (
        "live/targeter-v2/generations/20260806T101500.123456Z/metadata/kalshi/"
        "3f1a2b.json"
    )

    def test_a_v2_generation_metadata_snapshot_is_admitted(self) -> None:
        self.assertTrue(generation_metadata_object(self.V2_METADATA))
        self.assertTrue(gate1_object(self.V2_METADATA))

    def test_a_dataset_rooted_at_the_live_directory_resolves_the_same_shape(self) -> None:
        without_live = self.V2_METADATA.removeprefix("live/")
        self.assertTrue(generation_metadata_object(without_live))
        self.assertTrue(gate1_object(without_live))

    def test_the_v1_flat_metadata_layout_is_still_admitted(self) -> None:
        self.assertTrue(gate1_object("live/metadata/polymarket/3f1a2b.json"))
        self.assertTrue(gate1_object("metadata/polymarket/3f1a2b.json"))

    def test_every_generation_is_admitted_not_only_the_published_one(self) -> None:
        for run_id in ("20260806T101500.123456Z", "20260806T102500.654321Z"):
            key = f"live/targeter-v2/generations/{run_id}/metadata/limitless/aa.json"
            self.assertTrue(gate1_object(key), key)

    def test_a_generations_target_file_is_not_metadata(self) -> None:
        # The snapshot is the content-addressed evidence; `targets_<venue>.json`
        # is the mutable-by-generation pointer at it, and admitting it would put
        # a second, unhashed spelling of the same targets into the manifest.
        key = "live/targeter-v2/generations/20260806T101500.123456Z/targets_kalshi.json"
        self.assertFalse(generation_metadata_object(key))
        self.assertFalse(gate1_object(key))

    def test_the_publication_pointer_and_run_artifacts_stay_excluded(self) -> None:
        for key in (
            "live/targeter-v2/current.json",
            "live/targeter-v2/generations/20260806T101500.123456Z/manifest.json",
            "targeter-v2-runs/20260806T101500.123456Z/selection_report.json",
            "targeter-v2-runs/20260806T101500.123456Z/catalog_kalshi_markets.ndjson",
        ):
            self.assertFalse(gate1_object(key), key)

    def test_an_unsealed_segment_is_never_admitted(self) -> None:
        self.assertFalse(gate1_object("spool/lane=kalshi/date=2026-08-06/a.ndjson.open"))

    def test_a_compressed_segment_is_not_mistaken_for_a_readable_one(self) -> None:
        # `iter_ndjson_lines` would skip it silently, so admitting it would add
        # bytes to `dataset_sha256` that no check ever reads.
        self.assertFalse(gate1_object("spool/lane=kalshi/date=2026-08-06/a.ndjson.zst"))


if __name__ == "__main__":
    unittest.main()
