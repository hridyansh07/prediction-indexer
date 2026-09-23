"""Local-only immutable fee catalogs. Receipt last; strict read before use.

This small standalone writer avoids importing archive.__init__, which imports
capture/codec adapters. It follows archive.common.durable's fsync discipline and
LocalObjectStore's no-replace link publication. No receipt authorizes deletion.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import is_dataclass
from enum import Enum
from pathlib import Path

from . import domain, schedules
from .domain import canonical, persisted_fields
from .schedules import Catalog, Schedule, Source

MAX_BYTES = 16 * 1024 * 1024
_TYPES = {
    name: cls
    for module in (domain, schedules)
    for name, cls in vars(module).items()
    if isinstance(cls, type)
    and cls.__module__ == module.__name__
    and (is_dataclass(cls) or issubclass(cls, Enum))
}


def _decode(value, depth=0):
    if depth > 32:
        raise ValueError("artifact nesting exceeds limit")
    if type(value) is list:
        if len(value) > 10000:
            raise ValueError("artifact collection exceeds limit")
        return tuple(_decode(v, depth + 1) for v in value)
    if type(value) is dict:
        if "enum" in value:
            if set(value) != {"enum", "value"} or value["enum"] not in _TYPES:
                raise ValueError("invalid enum encoding")
            cls = _TYPES[value["enum"]]
            if not issubclass(cls, Enum):
                raise ValueError("expected enum")
            return cls(value["value"])
        cls = _TYPES.get(value.get("type"))
        if (
            cls is None
            or not is_dataclass(cls)
            or set(value) != {"type", *(f.name for f in persisted_fields(cls))}
        ):
            raise ValueError("unknown or missing artifact fields")
        return cls(
            **{k: _decode(v, depth + 1) for k, v in value.items() if k != "type"}
        )
    if value is None or type(value) in (str, int, bool):
        return value
    raise ValueError("invalid artifact scalar")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _reject(value):
    raise ValueError("noninteger JSON number forbidden")


def _integer(value):
    if len(value) > 38:
        raise ValueError("JSON integer exceeds bound")
    return int(value)


def parse_canonical(data: bytes):
    if type(data) is not bytes or len(data) > MAX_BYTES:
        raise ValueError("artifact byte limit")
    try:
        document = json.loads(
            data,
            object_pairs_hook=_pairs,
            parse_float=_reject,
            parse_constant=_reject,
            parse_int=_integer,
        )
        value = _decode(document)
    except (RecursionError, UnicodeError, TypeError, KeyError) as error:
        raise ValueError("invalid artifact") from error
    if canonical(value) != data:
        raise ValueError("noncanonical artifact bytes")
    return value


def _fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _read(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError("artifact must be a regular nonsymlink file")
    with path.open("rb") as handle:
        data = handle.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("artifact byte limit")
    return data


def _put(path, data):
    if len(data) > MAX_BYTES:
        raise ValueError("artifact byte limit")
    fd, name = tempfile.mkstemp(prefix=".fee-open-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(name, path)  # no TOCTOU overwrite of immutable final keys
        except FileExistsError:
            if _read(path) != data:
                raise ValueError("immutable artifact conflict")
        _fsync_directory(path.parent)
    finally:
        os.unlink(name)


def source_from_bytes(url: str, retrieved_at: int, data: bytes) -> Source:
    if type(data) is not bytes:
        raise TypeError("exact source bytes required")
    return Source(url, hashlib.sha256(data).hexdigest(), len(data), retrieved_at)


def build_catalog(root: Path, catalog: Catalog, source_blobs: dict[str, bytes]) -> Path:
    """Import reviewed schedules and retained bytes. Never fetch or infer dates.

    Caller owns extraction/effective-time claims. Integrity is verified here;
    semantic truth of a manually extracted schedule is not certified by a hash.
    """
    if type(catalog) is not Catalog:
        raise TypeError("Catalog required")
    sources = {s.sha256: s for schedule in catalog.schedules for s in schedule.sources}
    if set(source_blobs) != set(sources):
        raise ValueError("exact referenced source set required")
    for digest, source in sources.items():
        data = source_blobs[digest]
        if (
            type(data) is not bytes
            or len(data) != source.byte_length
            or hashlib.sha256(data).hexdigest() != digest
        ):
            raise ValueError("source identity mismatch")
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise ValueError("symlink root")
    _fsync_directory(root.parent)
    destination = root / catalog.identity
    destination.mkdir(exist_ok=True)
    if destination.is_symlink():
        raise ValueError("symlink catalog")
    _fsync_directory(root)
    for digest, data in sorted(source_blobs.items()):
        _put(destination / f"source-{digest}.blob", data)
    for schedule in catalog.schedules:
        _put(destination / f"schedule-{schedule.identity}.json", canonical(schedule))
    _put(destination / "manifest.json", canonical(catalog))
    _verify_content(destination, catalog.identity)
    _put(destination / "receipt.json", _receipt(catalog.identity))
    load_catalog(destination)
    return destination


def _receipt(identity):
    return (
        json.dumps(
            {"catalog": identity, "fee_catalog_receipt_version": 1},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()


def _verify_content(directory, identity):
    catalog = parse_canonical(_read(directory / "manifest.json"))
    if type(catalog) is not Catalog or catalog.identity != identity:
        raise ValueError("catalog identity mismatch")
    for schedule in catalog.schedules:
        parsed = parse_canonical(
            _read(directory / f"schedule-{schedule.identity}.json")
        )
        if type(parsed) is not Schedule or parsed != schedule:
            raise ValueError("schedule identity mismatch")
        for source in schedule.sources:
            data = _read(directory / f"source-{source.sha256}.blob")
            if (
                len(data) != source.byte_length
                or hashlib.sha256(data).hexdigest() != source.sha256
            ):
                raise ValueError("source identity mismatch")
    return catalog


def load_catalog(directory: Path) -> Catalog:
    """Independent strict reader: no receipt, no catalog; rehash every input."""
    directory = Path(directory)
    if directory.is_symlink():
        raise ValueError("symlink catalog")
    receipt = _read(directory / "receipt.json")
    if receipt != _receipt(directory.name):
        raise ValueError("invalid receipt/schema/catalog directory")
    catalog = _verify_content(directory, directory.name)
    _fsync_directory(directory)
    return catalog
