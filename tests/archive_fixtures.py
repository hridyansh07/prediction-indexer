"""Real sealed segments and real canonical receipts for the archive tests.

Fixtures are produced by the actual capture writer rather than hand-rolled JSON.
A hand-written seal is a second opinion about what a seal looks like, and the
archiver's whole job is to disagree with seals that are wrong — a fixture that
drifts from `splices/common/segment.py` would let it pass while disagreeing with
production.
"""

from __future__ import annotations

import json
import io
from pathlib import Path
from typing import Any, Iterable

from splices.common.segment import Record, SegmentWriter
from encoder import DEFAULT_ZSTD_LEVEL, encode_stream, encoder_version

NANOSECONDS = 1_000_000_000
WINDOW_SECONDS = 1800
#: A window start that is aligned to both the segment period and the UTC day.
BASE_NS = 1_785_369_600_000_000_000


def record(delivery_index: int, visible_ns: int, epoch: str = "e1a2b3c4") -> Record:
    line = (
        json.dumps(
            {
                "delivery_index": delivery_index,
                "visible_ns": visible_ns,
                "connection_epoch": epoch,
                "venue": "polymarket",
                "payload": {"asks": [[0.51, 100]], "bids": [[0.49, 100]]},
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    return Record(line=line, visible_ns=visible_ns, delivery_index=delivery_index, epoch=epoch)


def write_sealed_segment(
    spool_root: Path,
    lane: str = "polymarket",
    *,
    start_ns: int = BASE_NS,
    segment_index: int = 0,
    segment_id: str = "abcd1234",
    records: int = 4,
    first_delivery_index: int = 1,
) -> Path:
    """Writes and seals one segment exactly as a splice would, returning its path."""
    writer = SegmentWriter(
        spool_root,
        lane,
        start_ns,
        segment_seconds=WINDOW_SECONDS,
        segment_index=segment_index,
        segment_id=segment_id,
    )
    batch = [
        record(first_delivery_index + offset, start_ns + (offset + 1) * NANOSECONDS)
        for offset in range(records)
    ]
    if batch:
        writer.write_batch(batch)
    writer.seal("boundary")
    return writer.data_path


def write_canonical_receipt(
    canonical_root: Path,
    *,
    window_start_ns: int = BASE_NS,
    window_end_ns: int | None = None,
    inputs: Iterable[dict[str, Any]] = (),
    completeness: str = "complete",
    certified: bool = True,
    evidence_lines: int | None = None,
) -> Path:
    """A committed canonical window in the shape `indexer-finalize` writes.

    The evidence and provenance files are written at the recorded lengths
    because the receipt reader checks them — a receipt whose outputs are absent
    is not a committed window, which is one of the fail-closed cases the reaper
    has to honour.
    """
    from datetime import datetime, timezone

    inputs = list(inputs)
    window_end_ns = window_end_ns or window_start_ns + WINDOW_SECONDS * NANOSECONDS
    lines = sum(entry["line_count"] for entry in inputs) if evidence_lines is None else evidence_lines
    moment = datetime.fromtimestamp(window_start_ns / NANOSECONDS, tz=timezone.utc)
    directory = (
        Path(canonical_root)
        / f"date={moment.strftime('%Y-%m-%d')}"
        / f"window={window_start_ns}"
    )
    directory.mkdir(parents=True, exist_ok=True)

    evidence = b"".join(b'{"canonical":%d}\n' % index for index in range(lines))
    provenance = b"".join(b'{"canonical_seq":%d}\n' % (index + 1) for index in range(lines))
    evidence_path = directory / "evidence.ndjson.zst"
    provenance_path = directory / "provenance.ndjson.zst"
    with evidence_path.open("wb") as sink:
        evidence_result = encode_stream(io.BytesIO(evidence), sink, level=DEFAULT_ZSTD_LEVEL)
    with provenance_path.open("wb") as sink:
        provenance_result = encode_stream(io.BytesIO(provenance), sink, level=DEFAULT_ZSTD_LEVEL)

    def output(path: Path, result) -> dict[str, Any]:
        return {
            "file": path.name,
            "content_encoding": "zstd",
            "decoded": result.logical.as_record(),
            "stored": result.stored.as_record(),
            "compression": {
                "algorithm": "zstd",
                "level": DEFAULT_ZSTD_LEVEL,
                "frame_checksum": True,
                "dictionary": None,
                "frame_count": 1,
                "encoder": encoder_version(),
            },
        }

    receipt = {
        "receipt_version": 1,
        "window_start_ns": window_start_ns,
        "window_end_ns": window_end_ns,
        "completeness": completeness,
        "certified": certified,
        "expected_lanes": sorted({entry["lane"] for entry in inputs}),
        "present_lanes": sorted({entry["lane"] for entry in inputs}),
        "unexpected_lanes": [],
        "missing_lanes": [],
        "invalid_lanes": [],
        "finalization_deadline_seconds": 300,
        "deadline_expired": False,
        "finalized_at_ns": window_end_ns + 60 * NANOSECONDS,
        "inputs": inputs,
        "evidence": output(evidence_path, evidence_result),
        "provenance": output(provenance_path, provenance_result),
        "first_canonical_seq": 1 if lines else None,
        "last_canonical_seq": lines if lines else None,
        "finalizer_version": 1,
    }
    path = directory / "receipt.json"
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def canonical_input_for(segment_path: Path, lane: str = "polymarket") -> dict[str, Any]:
    """The `inputs` entry `indexer-finalize` would record for a sealed segment."""
    seal = json.loads(
        segment_path.with_name(segment_path.name[: -len(".ndjson")] + ".seal.json").read_text(
            encoding="utf-8"
        )
    )
    return {
        "lane": lane,
        "data_file": seal["data_file"],
        "segment_index": seal["segment_index"],
        "line_count": seal["line_count"],
        "sha256": seal["sha256"],
        "first_delivery_index": seal["first_delivery_index"],
        "last_delivery_index": seal["last_delivery_index"],
    }
