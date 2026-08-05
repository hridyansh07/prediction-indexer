"""What a splice is told to watch, and how it notices that changed.

A file rather than a control socket, for the same reason the tape is a file: the
splice keeps working when whatever produces the file is not running, and the
subscription set is a thing you can read, diff, and commit to git after the fact.
The targeter writes it; the splice polls it; neither has to be up for the other to
be useful.

The digest is the load-bearing part. A subscription change is recorded in the tape
as a control record carrying the digest, so a market with no data can always be
told apart from a market that was never subscribed — which is the difference
between "quiet" and "we weren't looking", and there is no way to recover it later
if the transition went unrecorded.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from analysis.storage import write_json


class TargetsError(ValueError):
    pass


TARGET_GENERATION_POINTER_VERSION = 1
TARGET_PUBLICATION_MANIFEST_VERSION = 1


@dataclass(frozen=True)
class Target:
    """One subscribable instrument. `asset_id` is what the venue's socket wants."""

    asset_id: str
    market_id: str | None = None
    condition_id: str | None = None
    note: str | None = None
    # Raw venue catalogue evidence. Interpretation into resolution_source,
    # observation_method, and fixing_time belongs later; keeping the original
    # record now means that interpretation can be revised without recollection.
    resolution: dict[str, Any] | None = None

    def as_record(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "note": self.note,
            "resolution": self.resolution,
        }


@dataclass(frozen=True)
class TargetSet:
    venue: str
    targets: tuple[Target, ...]
    digest: str
    source_path: str
    metadata_digest: str
    metadata_path: str | None

    def asset_ids(self) -> tuple[str, ...]:
        return tuple(target.asset_id for target in self.targets)

    def __len__(self) -> int:
        return len(self.targets)


def target_digest(venue: str, targets: tuple[Target, ...]) -> str:
    """Identity of a subscription set, insensitive to ordering and annotation.

    Only the venue and the asset IDs participate: reordering the file or editing a
    note does not change what the socket is subscribed to, and a digest that moved
    on those would force a reconnect — and therefore a book resync — for an edit
    that changed nothing.
    """
    payload = json.dumps(
        {"venue": venue, "asset_ids": sorted({target.asset_id for target in targets})},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def target_metadata_digest(venue: str, targets: tuple[Target, ...]) -> str:
    """Identity of the selected catalogue evidence, independent of file order."""
    payload = json.dumps(
        {
            "version": 1,
            "venue": venue,
            "targets": [
                target.as_record()
                for target in sorted(targets, key=lambda target: target.asset_id)
            ],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def raw_resolution_evidence(venue: str, catalogue_record: dict[str, Any]) -> dict[str, Any]:
    """Preserve one decoded venue record without pretending to understand it yet."""
    canonical = json.dumps(
        catalogue_record,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )
    return {
        "version": 1,
        "venue": venue,
        "catalogue_record_hash": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        # Round-tripping makes a detached copy: a source may continue sorting or
        # annotating its local object after the Target has been constructed.
        "catalogue_record": json.loads(canonical),
    }


def load_targets(path: Path, *, venue: str) -> TargetSet:
    """Reads a targets manifest, refusing anything the splice could misread.

    An empty set is legal — it means "watch nothing yet" and lets the targeter run
    before it has decided anything — but a duplicate or blank asset ID is not,
    because the socket would silently collapse them and the coverage record would
    then overstate what was subscribed.
    """
    path = Path(path)
    if not path.exists():
        raise TargetsError(f"targets file not found: {path}")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise TargetsError(f"targets file is not valid JSON: {error}") from error
    if not isinstance(document, dict):
        raise TargetsError("targets file must be a JSON object")

    if "target_generation_pointer_version" in document:
        return _load_generation_targets(path, venue=venue, pointer=document)
    return _load_direct_targets(path, venue=venue, document=document)


def _load_direct_targets(
    path: Path,
    *,
    venue: str,
    document: dict[str, Any],
    allowed_root: Path | None = None,
) -> TargetSet:
    """Validate one legacy/direct venue target document."""

    declared = document.get("venue")
    if declared is not None and declared != venue:
        raise TargetsError(f"targets file declares venue {declared!r}, splice is {venue!r}")

    entries = document.get("targets")
    if entries is None:
        raise TargetsError("targets file has no 'targets' array")
    if not isinstance(entries, list):
        raise TargetsError("'targets' must be an array")

    seen: set[str] = set()
    targets: list[Target] = []
    for position, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise TargetsError(f"target {position} is not an object")
        asset_id = entry.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise TargetsError(f"target {position} has no usable asset_id")
        if asset_id in seen:
            raise TargetsError(f"duplicate asset_id at target {position}: {asset_id}")
        seen.add(asset_id)
        resolution = entry.get("resolution")
        if resolution is not None and not isinstance(resolution, dict):
            raise TargetsError(f"target {position} resolution must be an object or null")
        targets.append(
            Target(
                asset_id=asset_id,
                market_id=_optional_text(entry.get("market_id")),
                condition_id=_optional_text(entry.get("condition_id")),
                note=_optional_text(entry.get("note")),
                resolution=resolution,
            )
        )

    frozen = tuple(targets)
    computed_metadata_digest = target_metadata_digest(venue, frozen)
    declared_metadata_digest = document.get("metadata_digest")
    if declared_metadata_digest is not None:
        if not isinstance(declared_metadata_digest, str):
            raise TargetsError("metadata_digest must be a string")
        if declared_metadata_digest != computed_metadata_digest:
            raise TargetsError("metadata_digest does not match target metadata")

    declared_metadata_path = document.get("metadata_path")
    metadata_path: str | None = None
    if declared_metadata_path is not None:
        if not isinstance(declared_metadata_path, str) or not declared_metadata_path:
            raise TargetsError("metadata_path must be a non-empty string")
        resolved_metadata_path = (path.parent / declared_metadata_path).resolve()
        if allowed_root is not None:
            try:
                resolved_metadata_path.relative_to(Path(allowed_root).resolve())
            except ValueError as error:
                raise TargetsError("metadata snapshot path escapes its generation root") from error
        if not resolved_metadata_path.is_file():
            raise TargetsError(f"metadata snapshot not found: {resolved_metadata_path}")
        try:
            snapshot = json.loads(resolved_metadata_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise TargetsError(
                f"metadata snapshot is not valid JSON: {resolved_metadata_path}"
            ) from error
        expected_snapshot = {
            "version": 1,
            "venue": venue,
            "metadata_digest": computed_metadata_digest,
            "targets": [
                target.as_record()
                for target in sorted(frozen, key=lambda target: target.asset_id)
            ],
        }
        if snapshot != expected_snapshot:
            raise TargetsError(
                f"metadata snapshot does not match target metadata: {resolved_metadata_path}"
            )
        metadata_path = str(resolved_metadata_path)

    return TargetSet(
        venue=venue,
        targets=frozen,
        digest=target_digest(venue, frozen),
        source_path=str(path),
        metadata_digest=computed_metadata_digest,
        metadata_path=metadata_path,
    )


def _load_generation_targets(
    pointer_path: Path,
    *,
    venue: str,
    pointer: dict[str, Any],
) -> TargetSet:
    """Resolve one venue through the atomically replaced v2 generation pointer."""
    if pointer.get("target_generation_pointer_version") != TARGET_GENERATION_POINTER_VERSION:
        raise TargetsError("unsupported target generation pointer version")
    run_id = pointer.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise TargetsError("target generation pointer has no run_id")
    manifest_path = _resolve_relative(
        pointer_path.parent,
        pointer.get("manifest_path"),
        description="target generation manifest",
    )
    manifest_identity = _stored_identity_section(pointer.get("manifest"), "pointer manifest")
    _verify_file_identity(manifest_path, manifest_identity, "target generation manifest")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise TargetsError(f"target generation manifest is not valid JSON: {error}") from error
    if not isinstance(manifest, dict):
        raise TargetsError("target generation manifest must be a JSON object")
    if manifest.get("target_publication_manifest_version") != TARGET_PUBLICATION_MANIFEST_VERSION:
        raise TargetsError("unsupported target publication manifest version")
    if manifest.get("run_id") != run_id:
        raise TargetsError("target generation pointer and manifest name different runs")
    venues = manifest.get("venues")
    if not isinstance(venues, dict) or venue not in venues:
        raise TargetsError(f"target generation has no entry for venue {venue!r}")
    entry = venues[venue]
    if not isinstance(entry, dict):
        raise TargetsError(f"target generation venue {venue!r} is not an object")
    target_file = entry.get("target_file")
    target_identity = _stored_identity_section(target_file, f"{venue} target file")
    assert isinstance(target_file, dict)
    target_path = _resolve_relative(
        manifest_path.parent,
        target_file.get("file"),
        description=f"{venue} target file",
    )
    _verify_file_identity(target_path, target_identity, f"{venue} target file")
    try:
        target_document = json.loads(target_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise TargetsError(f"{venue} target file is not valid JSON: {error}") from error
    if not isinstance(target_document, dict):
        raise TargetsError(f"{venue} target file must be a JSON object")
    loaded = _load_direct_targets(
        target_path,
        venue=venue,
        document=target_document,
        allowed_root=manifest_path.parent,
    )
    if entry.get("target_digest") != loaded.digest:
        raise TargetsError(f"{venue} target digest disagrees with generation manifest")
    if entry.get("metadata_digest") != loaded.metadata_digest:
        raise TargetsError(f"{venue} metadata digest disagrees with generation manifest")
    count = entry.get("target_count")
    if isinstance(count, bool) or not isinstance(count, int) or count != len(loaded):
        raise TargetsError(f"{venue} target count disagrees with generation manifest")
    return replace(loaded, source_path=str(pointer_path))


def _resolve_relative(base: Path, value: Any, *, description: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise TargetsError(f"{description} path must be non-empty and relative")
    root = Path(base).resolve()
    resolved = (root / value).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise TargetsError(f"{description} path escapes its generation root") from error
    return resolved


def _stored_identity_section(value: Any, description: str) -> tuple[str, int]:
    if not isinstance(value, dict):
        raise TargetsError(f"{description} identity must be an object")
    digest = value.get("sha256")
    length = value.get("byte_length")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise TargetsError(f"{description} sha256 is invalid")
    if isinstance(length, bool) or not isinstance(length, int) or length < 0:
        raise TargetsError(f"{description} byte_length is invalid")
    return digest, length


def _verify_file_identity(path: Path, expected: tuple[str, int], description: str) -> None:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise TargetsError(f"cannot read {description} {path}: {error}") from error
    actual = (hashlib.sha256(payload).hexdigest(), len(payload))
    if actual != expected:
        raise TargetsError(f"{description} identity does not match its commit record")


def write_targets(path: Path, *, venue: str, targets: list[Target], note: str | None = None) -> str:
    """Writes a manifest atomically and returns its digest.

    Atomic because the splice polls this path and a half-written file would either
    fail to parse — costing a poll — or, worse, parse as a shorter subscription set
    and trigger a real resubscribe.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frozen = tuple(targets)
    metadata_digest = target_metadata_digest(venue, frozen)
    metadata_relative_path = Path("metadata") / venue / f"{metadata_digest}.json"
    metadata_path = path.parent / metadata_relative_path
    metadata_document = {
        "version": 1,
        "venue": venue,
        "metadata_digest": metadata_digest,
        "targets": [
            target.as_record()
            for target in sorted(frozen, key=lambda target: target.asset_id)
        ],
    }
    if metadata_path.exists():
        try:
            existing = json.loads(metadata_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise TargetsError(f"metadata snapshot is corrupt: {metadata_path}") from error
        if existing != metadata_document:
            raise TargetsError(f"metadata digest collision or corrupt snapshot: {metadata_path}")
    else:
        write_json(metadata_path, metadata_document)

    document = {
        "version": 2,
        "venue": venue,
        "note": note,
        "digest": target_digest(venue, frozen),
        "metadata_digest": metadata_digest,
        "metadata_path": str(metadata_relative_path),
        "targets": [target.as_record() for target in frozen],
    }
    write_json(path, document)
    return document["digest"]


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TargetsError(f"expected a string or null, got {type(value).__name__}")
    return value or None
