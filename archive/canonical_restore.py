"""Provider-neutral, receipt-first restore of archived canonical windows."""

from __future__ import annotations

import json
import os
import shutil
import stat
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, BinaryIO

from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ZSTD_CONTENT_ENCODING,
    ObjectExpectation,
    ObjectMetadata,
    ObjectStore,
)
from encoder import LogicalIdentity, StoredIdentity, decode_stream

MAX_RECEIPT_BYTES = 1024 * 1024
MAX_STORED_FRAME_BYTES = 8 * 1024 * 1024 * 1024
MAX_DECODED_FRAME_BYTES = 32 * 1024 * 1024 * 1024
MAX_LINE_BYTES = 16 * 1024 * 1024


class CanonicalRestoreError(ValueError):
    """Remote canonical evidence is absent from, or violates, its contract."""


@dataclass(frozen=True)
class CanonicalFrame:
    file: str
    expected: ObjectExpectation
    logical: LogicalIdentity


@dataclass(frozen=True)
class RemoteCanonicalWindow:
    window_start_ns: int
    window_end_ns: int
    receipt_bytes: bytes
    receipt_sha256: str
    receipt: ObjectExpectation
    evidence: CanonicalFrame
    provenance: CanonicalFrame


def _date_partition(window_start_ns: int) -> str:
    return f"{date(1970, 1, 1) + timedelta(days=window_start_ns // 86_400_000_000_000):%Y-%m-%d}"


def _base(window_start_ns: int) -> str:
    return f"canonical/date={_date_partition(window_start_ns)}/window={window_start_ns}"


def _expectation(metadata: ObjectMetadata) -> ObjectExpectation:
    if not metadata.provider_checksum or not metadata.provider_checksum_algorithm:
        raise CanonicalRestoreError(f"{metadata.key}: provider checksum metadata is absent")
    return ObjectExpectation(
        key=metadata.key,
        stored=metadata.stored,
        provider_checksum=metadata.provider_checksum,
        provider_checksum_algorithm=metadata.provider_checksum_algorithm,
        content_type=metadata.content_type,
        content_encoding=metadata.content_encoding,
    )


def _read_all(source: BinaryIO, maximum: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while chunk := source.read(min(1024 * 1024, maximum + 1 - total)):
        chunks.append(chunk)
        total += len(chunk)
        if total > maximum:
            raise CanonicalRestoreError(f"canonical receipt exceeds {maximum} bytes")
    return b"".join(chunks)


def _strict_json(raw: bytes) -> dict[str, Any]:
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise CanonicalRestoreError(f"duplicate canonical receipt field {key!r}")
            value[key] = item
        return value

    def invalid_constant(value):
        raise CanonicalRestoreError(f"canonical receipt contains invalid JSON constant {value}")

    try:
        document = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise CanonicalRestoreError(f"canonical receipt is not strict JSON: {error}") from error
    if not isinstance(document, dict):
        raise CanonicalRestoreError("canonical receipt is not an object")
    # The finalizer's durable serializer is two-space pretty JSON plus LF. Key
    # order is schema-owned by Rust and is therefore retained during this check.
    try:
        canonical = (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode()
    except UnicodeError as error:
        raise CanonicalRestoreError(f"canonical receipt contains invalid Unicode: {error}") from error
    if raw != canonical:
        raise CanonicalRestoreError("canonical receipt is not in finalizer serialization")
    return document


def _closed(value: Any, fields: set[str], where: str, optional: set[str] = frozenset()) -> dict:
    if not isinstance(value, dict) or not (fields - optional) <= set(value) or set(value) - fields:
        raise CanonicalRestoreError(f"{where} has an invalid closed schema")
    return value


def _u64(value: Any, where: str) -> int:
    if type(value) is not int or not 0 <= value < 2**64:
        raise CanonicalRestoreError(f"{where} must be u64")
    return value


def _sha(value: Any, where: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise CanonicalRestoreError(f"{where} must be lowercase SHA-256")
    return value


def _parse_output(value: Any, name: str, key: str, metadata: ObjectMetadata) -> CanonicalFrame:
    output = _closed(value, {"file", "content_encoding", "decoded", "stored", "compression"}, name)
    if output["file"] != name or output["content_encoding"] != "zstd":
        raise CanonicalRestoreError(f"canonical {name} filename or encoding is invalid")
    decoded = _closed(output["decoded"], {"byte_length", "line_count", "sha256"}, f"{name}.decoded")
    stored = _closed(output["stored"], {"byte_length", "sha256"}, f"{name}.stored")
    compression = _closed(
        output["compression"],
        {"algorithm", "level", "frame_checksum", "dictionary", "frame_count", "encoder"},
        f"{name}.compression",
    )
    if (
        compression["algorithm"] != "zstd"
        or type(compression["level"]) is not int
        or compression["level"] != 3
        or compression["frame_checksum"] is not True
        or compression["dictionary"] is not None
        or type(compression["frame_count"]) is not int
        or compression["frame_count"] != 1
        or not isinstance(compression["encoder"], str)
        or not compression["encoder"]
    ):
        raise CanonicalRestoreError(f"canonical {name} compression contract is invalid")
    logical = LogicalIdentity(
        sha256=_sha(decoded["sha256"], f"{name}.decoded.sha256"),
        byte_length=_u64(decoded["byte_length"], f"{name}.decoded.byte_length"),
        line_count=_u64(decoded["line_count"], f"{name}.decoded.line_count"),
    )
    expected_stored = StoredIdentity(
        sha256=_sha(stored["sha256"], f"{name}.stored.sha256"),
        byte_length=_u64(stored["byte_length"], f"{name}.stored.byte_length"),
    )
    if expected_stored.byte_length > MAX_STORED_FRAME_BYTES or logical.byte_length > MAX_DECODED_FRAME_BYTES:
        raise CanonicalRestoreError(f"canonical {name} exceeds its byte budget")
    if metadata.key != key or metadata.stored != expected_stored:
        raise CanonicalRestoreError(f"canonical {name} provider identity disagrees with receipt")
    if metadata.content_type != NDJSON_CONTENT_TYPE or metadata.content_encoding != ZSTD_CONTENT_ENCODING:
        raise CanonicalRestoreError(f"canonical {name} provider metadata is invalid")
    return CanonicalFrame(name, _expectation(metadata), logical)


def _text(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise CanonicalRestoreError(f"{where} must be nonempty text")
    return value


def _validate_receipt_details(document: dict[str, Any]) -> None:
    for name in ("expected_lanes", "present_lanes", "unexpected_lanes"):
        lanes = document[name]
        if not isinstance(lanes, list) or any(not isinstance(lane, str) or not lane for lane in lanes):
            raise CanonicalRestoreError(f"canonical receipt {name} is invalid")
        if lanes != sorted(set(lanes)):
            raise CanonicalRestoreError(f"canonical receipt {name} is not sorted and unique")
    for name, reason in (("missing_lanes", "lane_missing"), ("invalid_lanes", "lane_invalid")):
        if not isinstance(document[name], list):
            raise CanonicalRestoreError(f"canonical receipt {name} is not a list")
        for index, fault in enumerate(document[name]):
            fault = _closed(fault, {"lane", "reason", "detail"}, f"{name}[{index}]")
            _text(fault["lane"], f"{name}[{index}].lane")
            if fault["reason"] != reason or (fault["detail"] is not None and not isinstance(fault["detail"], str)):
                raise CanonicalRestoreError(f"canonical receipt {name} has an invalid fault")
    if not isinstance(document["inputs"], list):
        raise CanonicalRestoreError("canonical receipt inputs is not a list")
    for index, item in enumerate(document["inputs"]):
        item = _closed(
            item,
            {"lane", "data_file", "segment_index", "line_count", "sha256", "first_delivery_index", "last_delivery_index"},
            f"inputs[{index}]",
        )
        _text(item["lane"], f"inputs[{index}].lane")
        _text(item["data_file"], f"inputs[{index}].data_file")
        _u64(item["segment_index"], f"inputs[{index}].segment_index")
        lines = _u64(item["line_count"], f"inputs[{index}].line_count")
        _sha(item["sha256"], f"inputs[{index}].sha256")
        first, last = item["first_delivery_index"], item["last_delivery_index"]
        if first is None and last is None and lines == 0:
            continue
        if first is None or last is None or _u64(first, "first_delivery_index") > _u64(last, "last_delivery_index"):
            raise CanonicalRestoreError("canonical input delivery span is invalid")
    _u64(document["finalization_deadline_seconds"], "finalization_deadline_seconds")
    _u64(document["finalized_at_ns"], "finalized_at_ns")
    if type(document["deadline_expired"]) is not bool:
        raise CanonicalRestoreError("deadline_expired is not boolean")
    if "carried" in document:
        carried = _closed(document["carried"], {"ordering", "lane_visible_ns"}, "carried")
        ordering = _closed(carried["ordering"], {"connections", "epochs"}, "carried.ordering")
        for name, fields in (
            ("connections", {"lane", "venue", "epoch", "local_counter"}),
            ("epochs", {"lane", "venue", "stream", "epoch", "monotonic_key"}),
        ):
            if not isinstance(ordering[name], list):
                raise CanonicalRestoreError(f"carried.ordering.{name} is not a list")
            for index, item in enumerate(ordering[name]):
                item = _closed(item, fields, f"carried.ordering.{name}[{index}]")
                for field in fields - {"local_counter", "monotonic_key"}:
                    _text(item[field], f"carried.ordering.{name}[{index}].{field}")
                number = "local_counter" if name == "connections" else "monotonic_key"
                _u64(item[number], f"carried.ordering.{name}[{index}].{number}")
        if not isinstance(carried["lane_visible_ns"], dict):
            raise CanonicalRestoreError("carried.lane_visible_ns is not an object")
        for lane, visible in carried["lane_visible_ns"].items():
            _text(lane, "carried lane")
            _u64(visible, f"carried.lane_visible_ns.{lane}")
    if "clock_faults" in document:
        if not isinstance(document["clock_faults"], list):
            raise CanonicalRestoreError("clock_faults is not a list")
        for index, fault in enumerate(document["clock_faults"]):
            fault = _closed(
                fault,
                {"window_start_ns", "lane", "previous_visible_ns", "observed_visible_ns"},
                f"clock_faults[{index}]",
            )
            _u64(fault["window_start_ns"], "clock fault window")
            _text(fault["lane"], "clock fault lane")
            _u64(fault["previous_visible_ns"], "clock fault previous_visible_ns")
            _u64(fault["observed_visible_ns"], "clock fault observed_visible_ns")


def preflight_canonical_window(
    store: ObjectStore, window_start_ns: int, window_seconds: int
) -> RemoteCanonicalWindow | None:
    """Verify and parse the remote commit marker without downloading frames."""
    if type(window_start_ns) is not int or window_start_ns < 0:
        raise CanonicalRestoreError("window_start_ns must be nonnegative")
    if type(window_seconds) is not int or window_seconds <= 0:
        raise CanonicalRestoreError("window_seconds must be positive")
    base = _base(window_start_ns)
    receipt_key = f"{base}/receipt.json"
    receipt_metadata = store.head(receipt_key)
    if receipt_metadata is None:
        return None
    if receipt_metadata.byte_length > MAX_RECEIPT_BYTES:
        raise CanonicalRestoreError("canonical receipt exceeds metadata budget")
    if receipt_metadata.content_type != JSON_CONTENT_TYPE or receipt_metadata.content_encoding is not None:
        raise CanonicalRestoreError("canonical receipt provider metadata is invalid")
    receipt_expectation = _expectation(receipt_metadata)
    with store.open_verified(receipt_expectation) as source:
        raw = _read_all(source, MAX_RECEIPT_BYTES)
    document = _strict_json(raw)
    fields = {
        "receipt_version", "window_start_ns", "window_end_ns", "completeness", "certified",
        "expected_lanes", "present_lanes", "unexpected_lanes", "missing_lanes", "invalid_lanes",
        "finalization_deadline_seconds", "deadline_expired", "finalized_at_ns", "inputs",
        "evidence", "provenance", "first_canonical_seq", "last_canonical_seq", "carried",
        "clock_faults", "finalizer_version",
    }
    _closed(document, fields, "canonical receipt", {"carried", "clock_faults"})
    start = _u64(document["window_start_ns"], "window_start_ns")
    end = _u64(document["window_end_ns"], "window_end_ns")
    if (
        type(document["receipt_version"]) is not int
        or document["receipt_version"] != 1
        or type(document["finalizer_version"]) is not int
        or document["finalizer_version"] != 1
    ):
        raise CanonicalRestoreError("canonical receipt version is unsupported")
    if start != window_start_ns or end != start + window_seconds * 1_000_000_000:
        raise CanonicalRestoreError("canonical receipt names another window")
    if document["completeness"] not in ("complete", "incomplete") or type(document["certified"]) is not bool:
        raise CanonicalRestoreError("canonical receipt verdict is invalid")
    _validate_receipt_details(document)
    evidence_key = f"{base}/evidence.ndjson.zst"
    provenance_key = f"{base}/provenance.ndjson.zst"
    evidence_metadata = store.head(evidence_key)
    provenance_metadata = store.head(provenance_key)
    if evidence_metadata is None or provenance_metadata is None:
        raise CanonicalRestoreError("canonical receipt is committed but a named frame is absent")
    evidence = _parse_output(document["evidence"], "evidence.ndjson.zst", evidence_key, evidence_metadata)
    provenance = _parse_output(document["provenance"], "provenance.ndjson.zst", provenance_key, provenance_metadata)
    if evidence.logical.line_count != provenance.logical.line_count:
        raise CanonicalRestoreError("canonical evidence and provenance line counts disagree")
    first, last = document["first_canonical_seq"], document["last_canonical_seq"]
    if first is None and last is None:
        if evidence.logical.line_count != 0:
            raise CanonicalRestoreError("nonempty canonical receipt has no sequence range")
    elif (
        type(first) is not int
        or type(last) is not int
        or not 1 <= first <= last < 2**63
        or last - first + 1 != evidence.logical.line_count
    ):
        raise CanonicalRestoreError("canonical sequence range is invalid")
    return RemoteCanonicalWindow(start, end, raw, receipt_metadata.sha256, receipt_expectation, evidence, provenance)


class _TeeReader:
    def __init__(self, source: BinaryIO, sink: BinaryIO):
        self.source, self.sink = source, sink

    def read(self, size: int = -1) -> bytes:
        chunk = self.source.read(size)
        if chunk:
            self.sink.write(chunk)
        return chunk


class _LineSink:
    def __init__(self):
        self.current = 0

    def write(self, data: bytes | memoryview) -> int:
        for part in bytes(data).splitlines(keepends=True):
            self.current += len(part)
            if self.current > MAX_LINE_BYTES:
                raise CanonicalRestoreError("canonical NDJSON line exceeds line budget")
            if part.endswith(b"\n"):
                self.current = 0
        return len(data)

    def finish(self) -> None:
        if self.current:
            raise CanonicalRestoreError("canonical NDJSON ends without LF")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _restore_frame(store: ObjectStore, frame: CanonicalFrame, directory: Path) -> None:
    temporary = directory / f".{frame.file}.restore"
    final = directory / frame.file
    logical_sink = _LineSink()
    with open(temporary, "xb") as sink, store.open_verified(frame.expected) as source:
        decode_stream(
            _TeeReader(source, sink),
            logical_sink,
            expected_logical=frame.logical,
            expected_stored=frame.expected.stored,
            max_decoded_bytes=frame.logical.byte_length,
        )
        logical_sink.finish()
        sink.flush()
        os.fsync(sink.fileno())
    os.replace(temporary, final)
    _fsync_directory(directory)


def restore_canonical_window(
    store: ObjectStore, remote: RemoteCanonicalWindow, canonical_root: Path | str
) -> Path:
    """Restore one preflighted window under an owned root, receipt last."""
    root = Path(canonical_root)
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or not stat.S_ISDIR(root.stat(follow_symlinks=False).st_mode):
        raise CanonicalRestoreError("canonical restore root is not an owned regular directory")
    partition = root / f"date={_date_partition(remote.window_start_ns)}"
    partition.mkdir(exist_ok=True)
    if partition.is_symlink() or not stat.S_ISDIR(partition.stat(follow_symlinks=False).st_mode):
        raise CanonicalRestoreError("canonical restore date partition is not a regular directory")
    directory = partition / f"window={remote.window_start_ns}"
    if directory.exists():
        if directory.is_symlink() or not directory.is_dir():
            raise CanonicalRestoreError("canonical restore destination is not a directory")
        if (directory / "receipt.json").exists():
            raise CanonicalRestoreError("canonical restore destination is already committed")
        shutil.rmtree(directory)
    directory.mkdir(parents=True)
    try:
        _restore_frame(store, remote.evidence, directory)
        _restore_frame(store, remote.provenance, directory)
        temporary = directory / ".receipt.json.restore"
        with open(temporary, "xb") as sink:
            sink.write(remote.receipt_bytes)
            sink.flush()
            os.fsync(sink.fileno())
        receipt = directory / "receipt.json"
        os.replace(temporary, receipt)
        _fsync_directory(directory)
        return receipt
    except BaseException:
        shutil.rmtree(directory, ignore_errors=True)
        raise
