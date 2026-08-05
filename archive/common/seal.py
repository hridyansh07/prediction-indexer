"""Validate that a sealed segment is what it says it is.

The Python mirror of `ingester/crates/segment/src/lib.rs`. Three readers now
decide whether a segment is committed — `indexer-ingest`, `indexer-finalize`,
and the archiver — and §6.1 requires this one to enforce the same fields and
relationships as the Rust crate: filename, lane, window, segment index, lengths,
ordering flags, delivery bounds, and digest. It deliberately does **not** reuse
replay's Gate 1 checks, which verify a dataset rather than validate a seal and
would arrive here as a partial second opinion.

The split between structure and content is load-bearing:

```text
validate_seal_document   everything decidable without reading the segment
```

The digest, byte length and LF count are *not* checked here, because the
archiver recomputes them while the source streams through the encoder (§2
invariant 1) and hashing the file twice would double the cost of an hourly sweep
over gigabyte segments. `logical_matches_seal` is where that comparison lands.

**A seal is a claim.** The digest proves the bytes have not changed since it was
written; it proves nothing about whether the seal's summary of them is true. So
every internal relationship it asserts is checked against itself and against the
path it sits at, and a segment whose name disagrees with its declared window is
an integrity fault rather than pending work.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from encoder import LogicalIdentity
from splices.common.segment import SegmentError, parse_segment_name

__all__ = [
    "SEAL_SUFFIX",
    "SealError",
    "SealedSegment",
    "logical_matches_seal",
    "pending_segments",
    "read_sealed_segment",
    "sealed_segments",
    "validate_seal_document",
]

SEAL_SUFFIX = ".seal.json"
DATA_SUFFIX = ".ndjson"
OPEN_SUFFIX = ".ndjson.open"
DERIVATIVE_SUFFIX = ".ndjson.zst"

SEAL_VERSION = 1
NANOSECONDS = 1_000_000_000

#: Present in every seal `splices.common.segment.Seal` writes. Decoded even
#: where this module has no use for the value, so a truncated or wrong-version
#: sidecar fails closed instead of reading as a valid seal missing fields.
REQUIRED_FIELDS = (
    "seal_version",
    "lane_id",
    "window_start_ns",
    "window_end_ns",
    "data_file",
    "byte_length",
    "line_count",
    "sha256",
    "first_delivery_index",
    "last_delivery_index",
    "first_visible_ns",
    "last_visible_ns",
    "visible_non_decreasing",
    "delivery_index_dense",
    "segment_id",
    "segment_index",
    "seal_reason",
    "ordering_status",
    "epochs",
    "repaired_bytes",
    "created_ns",
    "writer_version",
)


class SealError(ValueError):
    """A seal is unreadable, malformed, or disagrees with where it sits.

    Never "not yet sealed": a missing sidecar is pending work and is reported as
    absence, while a sidecar that exists and is wrong is an integrity fault.
    """


@dataclass(frozen=True)
class SealedSegment:
    """One validated sealed segment and the paths derived from it."""

    lane: str
    data_path: Path
    seal_path: Path
    seal: dict[str, Any]

    @property
    def segment_stem(self) -> str:
        return self.data_path.name[: -len(DATA_SUFFIX)]

    @property
    def window_start_ns(self) -> int:
        return int(self.seal["window_start_ns"])

    @property
    def window_end_ns(self) -> int:
        return int(self.seal["window_end_ns"])

    @property
    def segment_index(self) -> int:
        return int(self.seal["segment_index"])

    @property
    def segment_id(self) -> str:
        return str(self.seal["segment_id"])

    @property
    def date_partition(self) -> str:
        moment = datetime.fromtimestamp(self.window_start_ns / NANOSECONDS, tz=timezone.utc)
        return moment.strftime("%Y-%m-%d")

    @property
    def logical(self) -> LogicalIdentity:
        """What the seal claims the segment's bytes are."""
        return LogicalIdentity(
            sha256=str(self.seal["sha256"]),
            byte_length=int(self.seal["byte_length"]),
            line_count=int(self.seal["line_count"]),
        )

    @property
    def derivative_path(self) -> Path:
        return self.data_path.with_name(self.segment_stem + DERIVATIVE_SUFFIX)


def read_sealed_segment(lane: str, data_path: Path) -> SealedSegment:
    """Loads and validates a segment's seal without reading the segment."""
    data_path = Path(data_path)
    seal_path = seal_path_for(data_path)
    try:
        encoded = seal_path.read_text(encoding="utf-8")
    except OSError as error:
        raise SealError(f"unreadable seal {seal_path}: {error}") from error
    try:
        document = json.loads(encoded)
    except json.JSONDecodeError as error:
        raise SealError(f"invalid seal {seal_path}: {error}") from error
    validate_seal_document(document, lane=lane, data_path=data_path, seal_path=seal_path)
    return SealedSegment(lane=lane, data_path=data_path, seal_path=seal_path, seal=document)


def seal_path_for(data_path: Path) -> Path:
    name = Path(data_path).name
    if not name.endswith(DATA_SUFFIX):
        raise SealError(f"not a segment path: {data_path}")
    return Path(data_path).with_name(name[: -len(DATA_SUFFIX)] + SEAL_SUFFIX)


def validate_seal_document(
    document: Any,
    *,
    lane: str,
    data_path: Path,
    seal_path: Path | None = None,
) -> None:
    """Every relationship a seal asserts, checked against itself and its path."""
    where = seal_path or data_path

    def invalid(detail: str) -> SealError:
        return SealError(f"invalid seal {where}: {detail}")

    if not isinstance(document, dict):
        raise invalid("seal is not an object")
    missing = [field for field in REQUIRED_FIELDS if field not in document]
    if missing:
        raise invalid(f"missing fields {', '.join(missing)}")
    if document["seal_version"] != SEAL_VERSION:
        raise invalid(f"unsupported seal_version {document['seal_version']!r}")

    lane_id = document["lane_id"]
    if lane_id != lane:
        raise invalid(f"lane_id {lane_id!r} does not match lane {lane!r}")
    if document["data_file"] != data_path.name:
        raise invalid(f"data_file {document['data_file']!r} does not name {data_path.name!r}")

    start = _integer(document, "window_start_ns", invalid)
    end = _integer(document, "window_end_ns", invalid)
    if start >= end:
        raise invalid("window_start_ns must precede window_end_ns")

    byte_length = _integer(document, "byte_length", invalid)
    line_count = _integer(document, "line_count", invalid)
    segment_index = _integer(document, "segment_index", invalid)
    _integer(document, "repaired_bytes", invalid)
    _integer(document, "created_ns", invalid)
    if _integer(document, "writer_version", invalid) == 0:
        raise invalid("writer_version is required")
    _hex_digest(document, "sha256", invalid)

    if not isinstance(document["segment_id"], str) or not document["segment_id"]:
        raise invalid("segment_id is required")
    if not isinstance(document["seal_reason"], str) or not document["seal_reason"]:
        raise invalid("seal_reason is required")
    if not isinstance(document["epochs"], list) or not all(
        isinstance(epoch, str) for epoch in document["epochs"]
    ):
        raise invalid("epochs must be a list of connection epochs")

    non_decreasing = document["visible_non_decreasing"]
    dense = document["delivery_index_dense"]
    if not isinstance(non_decreasing, bool) or not isinstance(dense, bool):
        raise invalid("ordering flags must be booleans")
    expected_status = "ok" if non_decreasing else "visible_clock_regression"
    if document["ordering_status"] != expected_status:
        raise invalid("ordering_status disagrees with visible_non_decreasing")

    first_delivery = _optional_integer(document, "first_delivery_index", invalid)
    last_delivery = _optional_integer(document, "last_delivery_index", invalid)
    first_visible = _optional_integer(document, "first_visible_ns", invalid)
    last_visible = _optional_integer(document, "last_visible_ns", invalid)
    bounds = (first_delivery, last_delivery, first_visible, last_visible)

    if line_count == 0:
        # A quiet lane still seals, and what it seals carries no record bounds.
        if byte_length != 0 or any(bound is not None for bound in bounds) or document["epochs"]:
            raise invalid("empty segment carries record bounds")
    else:
        if any(bound is None for bound in bounds):
            raise invalid("non-empty segment is missing record bounds")
        if not document["epochs"]:
            raise invalid("non-empty segment has no connection epochs")
        assert first_delivery is not None and last_delivery is not None
        if first_delivery > last_delivery:
            raise invalid("delivery bounds are inverted")
        if dense and last_delivery - first_delivery + 1 != line_count:
            raise invalid("dense delivery bounds disagree with line_count")
        assert first_visible is not None and last_visible is not None
        # A segment holds the records of its own window and no others. Without
        # this a seal can name a window its records never belonged to, and every
        # digest still checks out while the window bounds are fiction.
        if first_visible < start or last_visible >= end:
            raise invalid(
                f"records span {first_visible}..={last_visible}, outside the declared "
                f"window {start}..{end}"
            )

    _validate_path_coherence(document, data_path, start, segment_index, invalid)


def _validate_path_coherence(
    document: dict[str, Any],
    data_path: Path,
    start: int,
    segment_index: int,
    invalid,
) -> None:
    """The filename places a segment; the seal is checked against it.

    The same argument `indexer-finalize` makes for `--window-seconds`: a seal's
    declaration is not an authority over where the file sits. A stamp that
    disagrees with `window_start_ns`, or an index that disagrees with
    `segment_index`, means one of the two is wrong and neither can be preferred
    silently — the archiver would otherwise publish an object under a key
    derived from a value the file itself contradicts.
    """
    # The writer's own parser, not a second one. `splices.common.segment` names
    # these files and is the only place that decides what the parts mean; a
    # reader with its own opinion is how the two come to disagree about a
    # boundary neither of them noticed changing.
    try:
        parsed_start, parsed_index, segment_id = parse_segment_name(data_path.name)
    except SegmentError as error:
        raise invalid(str(error)) from error

    if parsed_start != start:
        raise invalid(f"segment name places it at {parsed_start}, not window_start_ns {start}")
    if parsed_index != segment_index:
        raise invalid(f"segment name index {parsed_index} does not match segment_index {segment_index}")
    if segment_id != document["segment_id"]:
        raise invalid(f"segment name id {segment_id!r} does not match segment_id")

    moment = datetime.fromtimestamp(start / NANOSECONDS, tz=timezone.utc)
    date_directory = data_path.parent.name
    expected_date = moment.strftime("date=%Y-%m-%d")
    if date_directory != expected_date:
        raise invalid(f"segment sits under {date_directory!r}, not {expected_date!r}")
    lane_directory = data_path.parent.parent.name
    if lane_directory != f"lane={document['lane_id']}":
        raise invalid(f"segment sits under {lane_directory!r}, not lane={document['lane_id']}")


def logical_matches_seal(logical: LogicalIdentity, segment: SealedSegment) -> None:
    """The recomputed identity against the claim. Raises on any disagreement.

    Invariant 1: the seal is a claim, not a substitute for reading the source.
    All three values are compared because they fail differently — a truncated
    file matches neither length nor digest, an appended one may match the line
    count, and a byte flipped in place matches both lengths.
    """
    claimed = segment.logical
    if logical.byte_length != claimed.byte_length:
        raise SealError(
            f"{segment.data_path.name}: read {logical.byte_length} bytes, "
            f"seal claims {claimed.byte_length}"
        )
    if logical.line_count != claimed.line_count:
        raise SealError(
            f"{segment.data_path.name}: read {logical.line_count} lines, "
            f"seal claims {claimed.line_count}"
        )
    if logical.sha256 != claimed.sha256:
        raise SealError(
            f"{segment.data_path.name}: sha256 {logical.sha256} does not match the "
            f"sealed {claimed.sha256}"
        )


def sealed_segments(spool_root: Path) -> list[tuple[str, Path]]:
    """Every `(lane, path)` under a spool whose sidecar exists.

    `.ndjson.open` is invisible because the writer still owns it, and a renamed
    `.ndjson` without a sidecar is invisible because §3 makes the sidecar the
    commit marker — the rename happens first, so that file is a crash between
    two steps and recovery will seal it. Neither is an error here.
    """
    root = Path(spool_root)
    if not root.is_dir():
        return []
    found: list[tuple[str, Path]] = []
    for lane_directory in sorted(root.iterdir()):
        if not lane_directory.is_dir() or not lane_directory.name.startswith("lane="):
            continue
        lane = lane_directory.name[len("lane=") :]
        for date_directory in sorted(lane_directory.iterdir()):
            if not date_directory.is_dir() or not date_directory.name.startswith("date="):
                continue
            for path in sorted(date_directory.iterdir()):
                if path.name.endswith(DATA_SUFFIX) and seal_path_for(path).is_file():
                    found.append((lane, path))
    return sorted(found, key=lambda entry: (entry[1].name, str(entry[1])))


def pending_segments(spool_root: Path) -> list[Path]:
    """Segments that exist but are not eligible yet, and are not faults.

    An `.ndjson.open` still belongs to its writer; a renamed `.ndjson` with no
    sidecar is the window between §3's steps 3 and 4, which recovery closes.
    Neither is an error and neither is archivable, so they are counted rather
    than reported — a sweep that archived nothing because every segment is still
    open reads very differently from one that found nothing at all.
    """
    root = Path(spool_root)
    if not root.is_dir():
        return []
    found: list[Path] = []
    for path in root.glob("lane=*/date=*/*"):
        if not path.is_file():
            continue
        if path.name.endswith(OPEN_SUFFIX):
            found.append(path)
        elif path.name.endswith(DATA_SUFFIX) and not seal_path_for(path).is_file():
            found.append(path)
    return sorted(found)


def _integer(document: dict[str, Any], field: str, invalid) -> int:
    value = document[field]
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise invalid(f"{field} is not a non-negative integer")
    return value


def _optional_integer(document: dict[str, Any], field: str, invalid) -> int | None:
    value = document[field]
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise invalid(f"{field} is not a non-negative integer or null")
    return value


def _hex_digest(document: dict[str, Any], field: str, invalid) -> str:
    value = document[field]
    if not isinstance(value, str) or len(value) != 64:
        raise invalid(f"{field} is not a 64-character digest")
    if value != value.lower() or any(character not in "0123456789abcdef" for character in value):
        raise invalid(f"{field} is not lowercase hexadecimal")
    return value
