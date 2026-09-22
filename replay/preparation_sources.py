"""Preparation-only adapters to existing receipt-verified Targeter streaming."""

import hashlib
from itertools import islice

from replay.preparation import MAX_BYTES, MAX_OCCURRENCES, source_pin
from replay.streams.protocol import decode, require


class ArchivedSelections:
    """Read exactly supplied production receipts through the shared S3/GCS adapter.

    The caller builds ObjectStore using archive.storage.factory.build_store and
    parses receipts with targeter.v2.run_archive.read_run_archive_receipt. No
    listing, discovery, new cloud client, or cross-run cache is involved.
    """

    def __init__(self, store, receipts, *, temp_root=None):
        from targeter.v2.replay_stream import ArchivedTargeterRunByteStreamer

        receipts = tuple(islice(receipts, MAX_OCCURRENCES * 2 + 1))
        require(0 < len(receipts) <= MAX_OCCURRENCES * 2, "receipt budget")
        require(len({r.run_id for r in receipts}) == len(receipts), "duplicate receipt")
        require(sum(len(r.objects) for r in receipts) <= 16384, "receipt object budget")
        self.receipts = {r.run_id: r for r in receipts}
        self.streamer = ArchivedTargeterRunByteStreamer(
            store, receipts, temp_root=temp_root
        )

    def _read(self, key):
        data = bytearray()
        for chunk in self.streamer.iter_bytes(key):
            require(
                len(data) + len(chunk) <= MAX_BYTES,
                "Targeter preparation object too large",
            )
            data.extend(chunk)
        return bytes(data)

    def _row(self, run_id, bundle_id, expected):
        from targeter.v2.manifest import parse_run_manifest
        from universe.projection import project_selected_bundles

        receipt = self.receipts.get(run_id)
        require(receipt is not None, "missing pinned Targeter receipt")
        require(
            receipt.manifest.key == expected["manifest_key"]
            and receipt.manifest.stored.sha256 == expected["manifest_sha256"],
            "manifest pin conflict",
        )
        require(
            receipt.manifest.stored.byte_length <= MAX_BYTES, "manifest byte budget"
        )
        payload = self._read(receipt.manifest.key)
        require(hashlib.sha256(payload).hexdigest() == expected["manifest_sha256"])
        manifest = parse_run_manifest(
            decode(payload, MAX_BYTES), key=receipt.manifest.key
        )
        reports = [
            item
            for item in manifest.objects
            if item.file in {"selection_report.json", "selection_report.json.zst"}
        ]
        require(len(reports) == 1, "ambiguous selection report")
        report_item = reports[0]
        require(
            report_item.key == expected["report_key"]
            and report_item.stored.sha256 == expected["report_sha256"],
            "report pin conflict",
        )
        identity = report_item.logical or report_item.stored
        require(
            identity.byte_length <= MAX_BYTES
            and report_item.stored.byte_length <= MAX_BYTES,
            "report byte budget",
        )
        receipted = [item for item in receipt.objects if item.key == report_item.key]
        require(
            len(receipted) == 1
            and receipted[0].stored == report_item.stored
            and receipted[0].logical == report_item.logical
            and receipted[0].content_encoding == report_item.content_encoding,
            "manifest/receipt report conflict",
        )
        payload = self._read(
            report_item.key.removesuffix(".zst")
            if report_item.content_encoding == "zstd"
            else report_item.key
        )
        require(
            len(payload) == identity.byte_length
            and hashlib.sha256(payload).hexdigest() == identity.sha256,
            "report logical identity",
        )
        report = decode(payload, MAX_BYTES)
        require(
            manifest.run_id == run_id
            and report.get("run_id") == run_id
            and manifest.generated_at == report.get("generated_at")
            and manifest.input_complete is True
            and report.get("input_complete") is True,
            "manifest/report conflict",
        )
        rows = project_selected_bundles(report)
        matches = [row for row in rows if row["bundle_id"] == bundle_id]
        require(len(matches) == 1, "selection absent from committed report")
        return matches[0]

    def __call__(self, occurrence, bundle_id):
        from universe.sync import _complete_context, _projected_targets

        expected = source_pin(occurrence["source"])
        row = self._row(occurrence["run_id"], bundle_id, expected)
        origin_source, origin_row = expected, row
        if row["occurrence_kind"] == "retained":
            origin_receipt = self.receipts.get(row["origin_run_id"])
            require(origin_receipt is not None, "retained origin receipt required")
            reports = [
                item
                for item in origin_receipt.objects
                if item.file in {"selection_report.json", "selection_report.json.zst"}
            ]
            require(len(reports) == 1, "origin report missing/ambiguous")
            origin_source = {
                "manifest_key": row["origin_archive_manifest_key"],
                "manifest_sha256": row["origin_archive_manifest_sha256"],
                "report_key": reports[0].key,
                "report_sha256": row["origin_report_sha256"],
            }
            require(row["origin_run_id"] < row["run_id"], "origin chronology")
            origin_row = self._row(row["origin_run_id"], bundle_id, origin_source)
            require(
                origin_row["occurrence_kind"] == "complete", "origin must be complete"
            )
        context = _complete_context(origin_row)
        require(
            _projected_targets(row["targets"]) == context["targets"]
            and row["activation_at"] == context["activation_at"]
            and row["capture_start_at"] == context["capture_start_at"],
            "retained context conflict",
        )
        return {
            **{
                f: row[f]
                for f in (
                    "run_id",
                    "generated_at",
                    "bundle_id",
                    "occurrence_kind",
                    "continuity_selected",
                    "continuity_disposition",
                )
            },
            **{
                f: context[f]
                for f in (
                    "sport",
                    "game",
                    "topology",
                    "activation_at",
                    "capture_start_at",
                )
            },
            "source": expected,
            "origin": {
                **origin_source,
                "run_id": origin_row["run_id"],
                "generated_at": origin_row["generated_at"],
            },
            # A pinned selected occurrence does not establish subsequent retirement.
            "retirement": None,
            "context": context,
        }
