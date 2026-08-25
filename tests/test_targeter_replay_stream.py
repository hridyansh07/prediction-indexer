from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from archive.storage.base import (
    INDEPENDENT,
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    VerificationFailure,
    provider_checksum_of,
)
from archive.storage.local import LocalObjectStore
from encoder import (
    LogicalIdentity,
    StoredIdentity,
    encode_stream,
    encoder_version,
    logical_identity_of,
    stored_identity_of,
)
from replay.catalog import canonical_sha256
from replay.gate1 import Gate1Auditor
from replay.stream import CompositeByteStreamer, MemoryByteStreamer
from targeter.v2.replay_stream import (
    ArchivedTargetRecordByteStreamer,
    ArchivedTargeterRunByteStreamer,
    RunReceiptSelectionError,
    select_run_receipts,
)
from targeter.v2.run_archive import (
    RunArchiveReceipt,
    parse_run_archive_receipt,
    parse_run_id_ns,
)

OWNER = "test-archive"
RUN_IDS = (
    "20260803T115000.000000Z",
    "20260803T120000.000000Z",
    "20260803T121000.000000Z",
    "20260803T122000.000000Z",
)


def _stored(payload: bytes) -> StoredIdentity:
    return stored_identity_of(io.BytesIO(payload))


def _logical(payload: bytes) -> LogicalIdentity:
    return logical_identity_of(io.BytesIO(payload))


def _compression() -> dict[str, object]:
    return {
        "algorithm": "zstd",
        "level": 3,
        "frame_checksum": True,
        "dictionary": None,
        "frame_count": 1,
        "encoder": encoder_version(),
    }


def _entry(
    *,
    name: str,
    key: str,
    stored: StoredIdentity,
    content_type: str,
    content_encoding: str | None,
    logical: LogicalIdentity | None = None,
) -> dict[str, object]:
    record: dict[str, object] = {
        "file": name,
        "key": key,
        "byte_length": stored.byte_length,
        "sha256": stored.sha256,
        "content_type": content_type,
        "content_encoding": content_encoding,
        "provider_checksum": provider_checksum_of(stored.sha256),
        "provider_checksum_algorithm": "SHA256",
    }
    if logical is not None:
        record["decoded"] = logical.as_record()
        record["compression"] = _compression() if content_encoding == "zstd" else None
    return record


class RunArchiveFixture:
    def __init__(self, root: Path) -> None:
        self.store = LocalObjectStore(root, store_id=OWNER, durability=INDEPENDENT)

    def receipt(
        self,
        run_id: str,
        *,
        rows: bytes | None = None,
        compressed: bool = True,
        include_target_records: bool = True,
        store_selection: bool = False,
    ) -> RunArchiveReceipt:
        date = datetime.strptime(run_id[:8], "%Y%m%d").date().isoformat()
        prefix = f"targeter-v2/runs/date={date}/run={run_id}"
        selection = b'{"report_version":1}\n'
        selection_entry = _entry(
            name="selection_report.json",
            key=f"{prefix}/selection_report.json",
            stored=_stored(selection),
            content_type=JSON_CONTENT_TYPE,
            content_encoding=None,
        )
        objects: list[dict[str, object]] = [selection_entry]
        if store_selection:
            self.store.put_immutable(
                selection_entry["key"],
                io.BytesIO(selection),
                _stored(selection),
                content_type=JSON_CONTENT_TYPE,
                content_encoding=None,
            )

        if include_target_records:
            logical_bytes = (
                rows if rows is not None else _target_record("asset", run_id=run_id)
            )
            if compressed:
                sink = io.BytesIO()
                encoded = encode_stream(io.BytesIO(logical_bytes), sink)
                stored_bytes = sink.getvalue()
                logical = encoded.logical
                stored = encoded.stored
                name = "target_records_polymarket.ndjson.zst"
                encoding = "zstd"
            else:
                stored_bytes = logical_bytes
                logical = _logical(logical_bytes)
                stored = _stored(logical_bytes)
                name = "target_records_polymarket.ndjson"
                encoding = None
            target_entry = _entry(
                name=name,
                key=f"{prefix}/{name}",
                stored=stored,
                logical=logical,
                content_type=NDJSON_CONTENT_TYPE,
                content_encoding=encoding,
            )
            objects.append(target_entry)
            self.store.put_immutable(
                target_entry["key"],
                io.BytesIO(stored_bytes),
                stored,
                content_type=NDJSON_CONTENT_TYPE,
                content_encoding=encoding,
            )

        manifest_bytes = (
            json.dumps(
                {
                    "targeter_run_manifest_version": 2,
                    "run_id": run_id,
                    "files": [item["file"] for item in objects],
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        manifest_stored = _stored(manifest_bytes)
        manifest_entry = _entry(
            name="run_manifest.json",
            key=f"{prefix}/run_manifest.json",
            stored=manifest_stored,
            content_type=JSON_CONTENT_TYPE,
            content_encoding=None,
        )
        objects.append(manifest_entry)
        self.store.put_immutable(
            manifest_entry["key"],
            io.BytesIO(manifest_bytes),
            manifest_stored,
            content_type=JSON_CONTENT_TYPE,
            content_encoding=None,
        )

        document = {
            "targeter_run_archive_receipt_version": 2,
            "run_id": run_id,
            "bucket": OWNER,
            "prefix": prefix,
            "archived_at_ns": parse_run_id_ns(run_id),
            "manifest": manifest_entry,
            "objects": objects,
            "durability": "independent_durable",
            "authorizes_publication": True,
        }
        return parse_run_archive_receipt(
            document,
            path=Path(run_id) / "archive_receipt.json",
        )


def _target_record(asset: str, *, run_id: str = RUN_IDS[0]) -> bytes:
    observed = datetime.strptime(run_id, "%Y%m%dT%H%M%S.%fZ").replace(
        tzinfo=timezone.utc
    )
    record = {
        "clobTokenIds": json.dumps([asset, "no"]),
        "outcomes": json.dumps(["Yes", "No"]),
        "description": "Resolves against the Binance 1 minute candle.",
        "feesEnabled": False,
        "createdAt": "2026-08-03T11:00:00Z",
    }
    row = {
        "version": 1,
        "run_id": run_id,
        "venue": "polymarket",
        "target_id": f"polymarket:{asset}",
        "subscription_ids": [asset],
        "observed_at": observed.isoformat().replace("+00:00", "Z"),
        "provenance": "captured",
        "projection_id": "polymarket.v1",
        "projection_sha256": "0" * 64,
        "record_sha256": canonical_sha256(record),
        "record": record,
    }
    return json.dumps(row, separators=(",", ":")).encode() + b"\n"


def _tape(asset: str) -> dict[str, bytes]:
    def line(index: int, payload: dict, *, control: bool = False) -> bytes:
        envelope = {
            "envelope_version": 2,
            "delivery_index": index,
            "record_id": f"pm-a-{index}",
            "visible_ns": index,
            "monotonic_ns": index,
            "venue": "polymarket",
            "stream": "process" if control else "public_book",
            "connection_epoch": "a",
            "local_counter": index,
            "source_cursor": None,
            "kind": "control" if control else "venue_frame",
            "raw_payload": json.dumps(payload, separators=(",", ":")),
        }
        return json.dumps(envelope, separators=(",", ":")).encode() + b"\n"

    data = (
        line(
            1,
            {
                "event": "connection_opened",
                "target_digest": "digest",
                "asset_ids": [asset],
                "delivers_deltas": True,
                "fsync_interval_seconds": 0.25,
                "clock_scope": {
                    "scope_id": "boot",
                    "comparable_across_processes": True,
                },
            },
            control=True,
        )
        + line(2, {"event_type": "book", "bids": [], "asks": [], "hash": "h"})
        + line(3, {"event": "connection_closed"}, control=True)
    )
    key = "spool/lane=polymarket/date=2026-08-03/segment.ndjson"
    return {
        key: data,
        key.replace(".ndjson", ".seal.json"): json.dumps(
            {
                "data_file": "segment.ndjson",
                "byte_length": len(data),
                "line_count": data.count(b"\n"),
                "sha256": hashlib.sha256(data).hexdigest(),
            },
            separators=(",", ":"),
        ).encode(),
    }


class RunReceiptSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        fixture = RunArchiveFixture(Path(self.directory.name))
        self.receipts = tuple(fixture.receipt(run_id) for run_id in RUN_IDS)

    def test_selects_predecessor_and_half_open_window(self) -> None:
        start = parse_run_id_ns(RUN_IDS[1])
        end = parse_run_id_ns(RUN_IDS[3])
        assert start is not None and end is not None
        selection = select_run_receipts(self.receipts, start_ns=start, end_ns=end)

        self.assertEqual(selection.predecessor.run_id, RUN_IDS[0])
        self.assertEqual(
            [receipt.run_id for receipt in selection.in_window],
            [RUN_IDS[1], RUN_IDS[2]],
        )
        self.assertEqual(
            [receipt.run_id for receipt in selection.receipts],
            [RUN_IDS[0], RUN_IDS[1], RUN_IDS[2]],
        )

    def test_missing_predecessor_and_duplicate_run_ids_fail_loudly(self) -> None:
        start = parse_run_id_ns(RUN_IDS[0])
        end = parse_run_id_ns(RUN_IDS[2])
        assert start is not None and end is not None
        with self.assertRaises(RunReceiptSelectionError):
            select_run_receipts(self.receipts, start_ns=start, end_ns=end)
        with self.assertRaisesRegex(RunReceiptSelectionError, "duplicate run_id"):
            select_run_receipts(
                (*self.receipts, self.receipts[0]),
                start_ns=parse_run_id_ns(RUN_IDS[1]),
                end_ns=end,
            )


class ArchivedTargetRecordByteStreamerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.fixture = RunArchiveFixture(self.root / "store")

    def streamer(self, receipts: tuple[RunArchiveReceipt, ...]):
        start = parse_run_id_ns(RUN_IDS[1])
        end = parse_run_id_ns(RUN_IDS[2])
        assert start is not None and end is not None
        return ArchivedTargetRecordByteStreamer(
            self.fixture.store,
            receipts,
            start_ns=start,
            end_ns=end,
            temp_root=self.root,
            chunk_size=7,
        )

    def test_streams_zstd_and_plain_records_only_after_verification(self) -> None:
        predecessor = self.fixture.receipt(RUN_IDS[0], compressed=True)
        current = self.fixture.receipt(RUN_IDS[1], compressed=False)
        streamer = self.streamer((predecessor, current))

        self.assertEqual(len(streamer.object_keys()), 2)
        self.assertTrue(all(key.endswith(".ndjson") for key in streamer.object_keys()))
        self.assertTrue(all("run=" in key for key in streamer.object_keys()))
        for key, run_id in zip(streamer.object_keys(), RUN_IDS[:2], strict=True):
            self.assertEqual(
                b"".join(streamer.iter_bytes(key)),
                _target_record("asset", run_id=run_id),
            )
        self.assertFalse(list(self.root.glob("target-record-replay-*")))

    def test_complete_run_streamer_exposes_every_receipted_artifact(self) -> None:
        receipt = self.fixture.receipt(RUN_IDS[0], store_selection=True)
        streamer = ArchivedTargeterRunByteStreamer(
            self.fixture.store, (receipt,), temp_root=self.root, chunk_size=7
        )

        self.assertEqual(len(streamer.object_keys()), len(receipt.objects))
        selection_key = next(
            key
            for key in streamer.object_keys()
            if key.endswith("selection_report.json")
        )
        target_key = next(
            key
            for key in streamer.object_keys()
            if key.endswith("target_records_polymarket.ndjson")
        )
        manifest_key = next(
            key for key in streamer.object_keys() if key.endswith("run_manifest.json")
        )
        self.assertEqual(
            b"".join(streamer.iter_bytes(selection_key)), b'{"report_version":1}\n'
        )
        self.assertEqual(
            b"".join(streamer.iter_bytes(target_key)),
            _target_record("asset", run_id=RUN_IDS[0]),
        )
        self.assertTrue(b"".join(streamer.iter_bytes(manifest_key)))

    def test_unrelated_missing_run_objects_do_not_block_target_records(self) -> None:
        predecessor = self.fixture.receipt(RUN_IDS[0])
        current = self.fixture.receipt(RUN_IDS[1])
        # The fixture deliberately never stores selection_report.json. Gate 1
        # needs the receipted manifest and target record, not unrelated reports.
        streamer = self.streamer((predecessor, current))
        self.assertTrue(b"".join(streamer.iter_bytes(streamer.object_keys()[0])))

    def test_manifest_or_target_drift_exposes_no_bytes(self) -> None:
        predecessor = self.fixture.receipt(RUN_IDS[0])
        current = self.fixture.receipt(RUN_IDS[1])
        streamer = self.streamer((predecessor, current))
        key = streamer.object_keys()[0]
        receipt = streamer.selection.predecessor
        manifest_path = self.fixture.store.root / receipt.manifest.key
        manifest_path.write_bytes(b"changed\n")
        with self.assertRaises(VerificationFailure):
            next(streamer.iter_bytes(key))
        self.assertFalse(list(self.root.glob("target-record-replay-*")))

    def test_a_second_read_rechecks_remote_state(self) -> None:
        predecessor = self.fixture.receipt(RUN_IDS[0])
        current = self.fixture.receipt(RUN_IDS[1])
        streamer = self.streamer((predecessor, current))
        key = streamer.object_keys()[0]
        self.assertTrue(b"".join(streamer.iter_bytes(key)))
        target = next(
            item
            for item in streamer.selection.predecessor.objects
            if item.file.startswith("target_records_")
        )
        (self.fixture.store.root / target.key).unlink()
        with self.assertRaises(VerificationFailure):
            next(streamer.iter_bytes(key))

    def test_legacy_run_without_target_records_contributes_no_keys(self) -> None:
        predecessor = self.fixture.receipt(RUN_IDS[0], include_target_records=False)
        current = self.fixture.receipt(RUN_IDS[1], include_target_records=False)
        self.assertEqual(self.streamer((predecessor, current)).object_keys(), ())

    def test_target_record_without_logical_identity_fails_closed(self) -> None:
        predecessor = self.fixture.receipt(RUN_IDS[0])
        current = self.fixture.receipt(RUN_IDS[1])
        target = next(
            item
            for item in predecessor.objects
            if item.file.startswith("target_records_")
        )
        changed_target = replace(target, logical=None)
        objects = tuple(
            changed_target if item == target else item for item in predecessor.objects
        )
        malformed = replace(predecessor, objects=objects)

        with self.assertRaisesRegex(VerificationFailure, "lacks decoded identity"):
            self.streamer((malformed, current))

    def test_gate1_consumes_decoded_target_records_through_composition(self) -> None:
        predecessor = self.fixture.receipt(RUN_IDS[0])
        current = self.fixture.receipt(RUN_IDS[1])
        composite = CompositeByteStreamer(
            MemoryByteStreamer(_tape("asset")),
            self.streamer((predecessor, current)),
        )
        report = Gate1Auditor().audit(composite).as_record()
        checks = {check["name"]: check for check in report["checks"]}

        self.assertEqual(checks["market_rules_and_metadata"]["status"], "PASS")
        self.assertEqual(checks["fee_model_evidence"]["status"], "PASS")
        self.assertEqual(checks["discovery_coverage"]["status"], "PASS")
        target_inputs = [
            item
            for item in report["input"]["objects"]
            if "target_records_" in item["key"]
        ]
        self.assertEqual(len(target_inputs), 2)
        self.assertTrue(all(item["key"].endswith(".ndjson") for item in target_inputs))


if __name__ == "__main__":
    unittest.main()
