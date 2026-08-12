from __future__ import annotations

import hashlib
import json
import unittest

from replay.catalog import canonical_sha256
from replay.gate1 import (
    Gate1Auditor,
    gate1_object,
    generation_metadata_object,
    target_record_object,
)
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


def _target_record(asset: str, *, provenance: str = "captured", **overrides) -> bytes:
    record = {
        "clobTokenIds": f'["{asset}", "no"]',
        "outcomes": '["Yes", "No"]',
        "orderMinSize": "5",
        "endDate": "2026-08-04T00:00:00Z",
        "description": "Resolves against the Binance 1 minute candle.",
        "feesEnabled": False,
        "createdAt": "2026-08-02T12:00:00Z",
        "volume24hr": 1000,
    }
    record.update(overrides.pop("record", {}))
    row = {
        "version": 1,
        "run_id": "20260803T120000.000000Z",
        "venue": "polymarket",
        "target_id": f"polymarket:{asset}",
        "subscription_ids": [asset],
        "observed_at": "2026-08-03T12:00:00Z",
        "provenance": provenance,
        "projection_id": "polymarket.v1",
        "projection_sha256": "unused-by-gate-1",
        "record_sha256": canonical_sha256(record),
        "record": record,
    }
    row.update(overrides)
    return json.dumps(row, separators=(",", ":")).encode() + b"\n"


def _dataset(*, records: bytes, asset: str = "asset") -> dict[str, bytes]:
    """A tape subscribing to `asset`, plus one run's target records. No `live/`."""
    opened = {
        "event": "connection_opened",
        "target_digest": "d",
        "asset_ids": [asset],
        "delivers_deltas": True,
        "fsync_interval_seconds": 0.25,
        "clock_scope": {"scope_id": "boot", "comparable_across_processes": True},
    }
    tape = (
        _line(1, 1, opened, event_kind="control")
        + _line(2, 2, {"event_type": "book", "bids": [], "asks": [], "hash": "h"})
        + _line(3, 3, {"event": "connection_closed"}, event_kind="control")
    )
    segment = "spool/lane=polymarket/date=2026-08-03/seg.ndjson"
    return {
        segment: tape,
        segment.replace(".ndjson", ".seal.json"): json.dumps(
            {
                "data_file": "seg.ndjson",
                "byte_length": len(tape),
                "line_count": 3,
                "sha256": hashlib.sha256(tape).hexdigest(),
            }
        ).encode(),
        "targeter-v2-runs/20260803T120000.000000Z/target_records_polymarket.ndjson": records,
    }


class TargetRecordTests(unittest.TestCase):
    def _checks(self, objects: dict[str, bytes], **kwargs) -> dict[str, dict]:
        report = Gate1Auditor(**kwargs).audit(MemoryByteStreamer(objects)).as_record()
        return {check["name"]: check for check in report["checks"]}

    def test_a_run_bundle_satisfies_the_metadata_checks_with_no_live_tree(self) -> None:
        # The point of the whole exercise: rules, fees and discovery coverage
        # all resolve from an archived run artifact, so gate 1's inputs are
        # segments, seals and target records — every one of them immutable and
        # already in the archive.
        checks = self._checks(_dataset(records=_target_record("asset")))
        self.assertEqual(checks["market_rules_and_metadata"]["status"], "PASS")
        self.assertEqual(checks["fee_model_evidence"]["status"], "PASS")
        self.assertEqual(checks["discovery_coverage"]["status"], "PASS")
        coverage = checks["discovery_coverage"]["evidence"]
        self.assertEqual(coverage["subscribed_assets"], 1)
        self.assertEqual(coverage["covered_assets"], 1)
        # `createdAt` travels in the venue's own record, so the discovery-lag
        # measurement survives the move off `live/coverage.json` intact.
        self.assertEqual(coverage["with_created_at"], 1)

    def test_disabled_fees_are_counted_as_a_fee_model(self) -> None:
        checks = self._checks(_dataset(records=_target_record("asset")))
        evidence = checks["fee_model_evidence"]["evidence"]
        self.assertEqual(evidence["metadata_records_with_fees"], 1)

    def test_an_asserted_record_does_not_count_unless_it_is_allowed(self) -> None:
        objects = _dataset(records=_target_record("asset", provenance="asserted"))

        strict = self._checks(objects)
        self.assertEqual(strict["market_rules_and_metadata"]["status"], "FAIL")
        self.assertEqual(strict["fee_model_evidence"]["status"], "FAIL")
        # An asserted record proves the market's terms, not when we first saw
        # it, so it cannot answer a discovery-coverage question either.
        self.assertEqual(strict["discovery_coverage"]["status"], "FAIL")
        rules = strict["market_rules_and_metadata"]["evidence"]
        self.assertEqual(rules["subscribed_with_rules_captured"], 0)
        self.assertEqual(rules["subscribed_with_rules_asserted"], 1)

        allowed = self._checks(objects, allow_asserted_records=True)
        self.assertEqual(allowed["market_rules_and_metadata"]["status"], "PASS")
        self.assertEqual(allowed["fee_model_evidence"]["status"], "PASS")
        self.assertEqual(allowed["discovery_coverage"]["status"], "PASS")

    def test_the_report_states_which_evidence_it_counted(self) -> None:
        # Weaker evidence stays visible in the report rather than being folded
        # into a number that looks like the real thing.
        objects = _dataset(
            records=_target_record("asset")
            + _target_record("other", provenance="asserted")
        )
        evidence = self._checks(objects)["market_rules_and_metadata"]["evidence"]
        self.assertEqual(evidence["provenance"], {"asserted": 1, "captured": 1})
        self.assertEqual(evidence["subscribed_with_rules_captured"], 1)
        self.assertFalse(evidence["allow_asserted_records"])

    def test_a_record_that_disagrees_with_its_own_hash_is_reported(self) -> None:
        objects = _dataset(
            records=_target_record("asset", record_sha256="0" * 64)
        )
        check = self._checks(objects)["market_rules_and_metadata"]
        self.assertEqual(check["status"], "FAIL")
        self.assertIn(
            "polymarket:asset: record hash disagrees with bytes",
            check["evidence"]["invalid"],
        )

    def test_an_unknown_provenance_is_a_failure_not_a_default(self) -> None:
        objects = _dataset(records=_target_record("asset", provenance="probably-fine"))
        checks = self._checks(objects)
        self.assertEqual(checks["byte_and_envelope_integrity"]["status"], "FAIL")

    def test_a_backfill_row_does_not_nullify_captured_evidence(self) -> None:
        # A market observed live and later re-fetched by a backfill has both a
        # captured and an asserted row. Subtracting every asserted asset made
        # the report say "captured evidence exists" and "this asset is
        # uncovered" at the same time.
        objects = _dataset(
            records=_target_record("asset")
            + _target_record("asset", provenance="asserted")
        )
        checks = self._checks(objects)
        coverage = checks["discovery_coverage"]["evidence"]
        self.assertEqual(checks["discovery_coverage"]["status"], "PASS")
        self.assertEqual(coverage["uncovered"], [])
        self.assertEqual(coverage["covered_by_captured"], 1)
        self.assertEqual(coverage["covered_by_asserted_only"], 0)

    def test_a_live_ledger_sighting_is_not_nullified_by_a_backfill(self) -> None:
        objects = _dataset(records=_target_record("asset", provenance="asserted"))
        objects["live/coverage.json"] = json.dumps(
            {
                "version": 1,
                "sightings": [
                    {
                        "venue": "polymarket",
                        "asset_id": "asset",
                        "first_seen_at": "2026-08-03T11:00:00+00:00",
                        "created_at": "2026-08-02T12:00:00+00:00",
                    }
                ],
            }
        ).encode()
        coverage = self._checks(objects)["discovery_coverage"]["evidence"]
        self.assertEqual(coverage["uncovered"], [])
        self.assertEqual(coverage["covered_by_captured"], 1)

    def test_the_earliest_sighting_wins_regardless_of_source_or_spelling(self) -> None:
        # `live/coverage.json` writes `+00:00`; the targeter writes `Z` and
        # drops the fraction on a whole second. Lexically `'+' < '.' < 'Z'`, so
        # a string compare picks the later row in both directions. The ledger
        # here is strictly later and must not displace the record's sighting.
        objects = _dataset(records=_target_record("asset"))
        objects["live/coverage.json"] = json.dumps(
            {
                "version": 1,
                "sightings": [
                    {
                        "venue": "polymarket",
                        "asset_id": "asset",
                        "first_seen_at": "2099-01-01T00:00:00.500000+00:00",
                        "created_at": None,
                    }
                ],
            }
        ).encode()
        checks = self._checks(objects)
        coverage = checks["discovery_coverage"]["evidence"]
        self.assertEqual(checks["discovery_coverage"]["status"], "PASS")
        # The record's own `createdAt` survives a ledger row that has none, so
        # adding evidence cannot take the discovery-lag measurement away.
        self.assertEqual(coverage["with_created_at"], 1)

    def test_a_record_for_an_asset_nobody_subscribed_to_proves_nothing(self) -> None:
        # "for the markets we watched" is what the requirement says. A run
        # bundle with no relationship to the tape beside it must not satisfy it.
        objects = _dataset(records=_target_record("never-subscribed"))
        checks = self._checks(objects)
        self.assertEqual(checks["market_rules_and_metadata"]["status"], "FAIL")
        self.assertEqual(checks["fee_model_evidence"]["status"], "FAIL")
        self.assertEqual(checks["discovery_coverage"]["status"], "FAIL")

    def test_repeating_a_row_every_run_does_not_inflate_the_counts(self) -> None:
        # Rows repeat per run by design, so counting rows would let one market
        # observed 144 times a day look like 144 markets.
        objects = _dataset(records=_target_record("asset") * 3)
        evidence = self._checks(objects)["market_rules_and_metadata"]["evidence"]
        self.assertEqual(evidence["subscribed_with_rules"], 1)
        self.assertEqual(evidence["target_records"], 3)

    def test_target_records_are_not_parsed_as_tape(self) -> None:
        # They are NDJSON with no envelope and no lane partition. Routed by key,
        # so they neither register a parse failure nor make `lane_of` raise.
        checks = self._checks(_dataset(records=_target_record("asset")))
        self.assertEqual(checks["byte_and_envelope_integrity"]["status"], "PASS")
        self.assertEqual(checks["deterministic_capture_order"]["status"], "PASS")


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
        #
        # `metadata_snapshot_references` is advisory for a different reason:
        # this tape carries no metadata snapshots at all, so there is nothing
        # for its referenced digests to resolve against. A dataset is not failed
        # for lacking something that was never in it — the check goes blocking
        # the moment the dataset carries a single snapshot.
        advisory = {check.name for check in report.checks if check.status == "ADVISORY"}
        self.assertEqual(
            advisory,
            {"market_lifecycle_observability", "metadata_snapshot_references"},
        )
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

    def test_target_records_are_the_only_part_of_a_run_a_gate_reads(self) -> None:
        run = "targeter-v2-runs/20260806T101500.123456Z"
        self.assertTrue(gate1_object(f"{run}/target_records_kalshi.ndjson"))
        self.assertTrue(target_record_object(f"{run}/target_records_polymarket.ndjson"))
        # The archive spells the same object differently. Both resolve.
        self.assertTrue(
            gate1_object(
                "targeter-v2/runs/date=2026-08-06/run=20260806T101500.123456Z"
                "/target_records_limitless.ndjson"
            )
        )
        # The selection report is the record of *why* markets were chosen and
        # the catalogue describes markets that were never subscribed. Neither
        # can interpret a tape, so neither is evidence a gate reads.
        self.assertFalse(gate1_object(f"{run}/selection_report.json"))
        self.assertFalse(gate1_object(f"{run}/catalog_kalshi_markets.ndjson"))

    def test_a_compressed_target_record_is_excluded_rather_than_skipped(self) -> None:
        # `iter_ndjson_lines` skips a key it cannot frame with a bare `continue`,
        # so admitting this would make the metadata checks vacuously empty
        # instead of failing. Excluded, the dataset has no records at all and
        # the gate says so.
        key = "targeter-v2-runs/20260806T101500.123456Z/target_records_kalshi.ndjson.zst"
        self.assertFalse(target_record_object(key))
        self.assertFalse(gate1_object(key))

    def test_an_unsealed_segment_is_never_admitted(self) -> None:
        self.assertFalse(gate1_object("spool/lane=kalshi/date=2026-08-06/a.ndjson.open"))

    def test_a_compressed_segment_is_not_mistaken_for_a_readable_one(self) -> None:
        # `iter_ndjson_lines` would skip it silently, so admitting it would add
        # bytes to `dataset_sha256` that no check ever reads.
        self.assertFalse(gate1_object("spool/lane=kalshi/date=2026-08-06/a.ndjson.zst"))


if __name__ == "__main__":
    unittest.main()
