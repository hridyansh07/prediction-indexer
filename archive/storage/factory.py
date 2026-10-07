"""Build the shared ObjectStore from process environment configuration."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Mapping

from archive.storage.base import CONFORMANCE, INDEPENDENT, ObjectStore, ObjectStoreError
from archive.storage.gcs import GCSObjectStore
from archive.storage.local import LocalObjectStore

__all__ = ["GCS_BACKEND", "LOCAL_BACKEND", "build_store"]

LOCAL_BACKEND = "local"
GCS_BACKEND = "gcs"


def build_store(
    primary_roots: Iterable[Path],
    *,
    environ: Mapping[str, str] | None = None,
) -> ObjectStore:
    """Build the configured backend or refuse an unsafe or ambiguous one."""
    configuration = os.environ if environ is None else environ
    backend = configuration.get("ARCHIVE_BACKEND", LOCAL_BACKEND)
    if backend == GCS_BACKEND:
        return _build_gcs_store(configuration)
    if backend != LOCAL_BACKEND:
        raise SystemExit(f"ARCHIVE_BACKEND must be local or gcs; got {backend!r}")
    return _build_local_store(
        configuration, tuple(Path(path) for path in primary_roots)
    )


def _build_gcs_store(configuration: Mapping[str, str]) -> GCSObjectStore:
    bucket = configuration.get("ARCHIVE_GCS_BUCKET", "")
    if not bucket:
        raise SystemExit("ARCHIVE_BACKEND=gcs requires ARCHIVE_GCS_BUCKET")
    try:
        return GCSObjectStore(bucket)
    except (ValueError, ObjectStoreError) as error:
        raise SystemExit(f"invalid GCS archive configuration: {error}") from error


def _build_local_store(
    configuration: Mapping[str, str], primary_roots: tuple[Path, ...]
) -> LocalObjectStore:
    if configuration.get("ARCHIVE_GCS_BUCKET", ""):
        raise SystemExit(
            "ARCHIVE_BACKEND=local was selected but ARCHIVE_GCS_BUCKET was also set. Set "
            "ARCHIVE_BACKEND=gcs or clear ARCHIVE_GCS_BUCKET."
        )
    archive_root_value = configuration.get("ARCHIVE_ROOT", "")
    if not archive_root_value:
        raise SystemExit("ARCHIVE_BACKEND=local requires ARCHIVE_ROOT")
    archive_root = Path(archive_root_value)

    durability = CONFORMANCE
    durability_name = configuration.get("ARCHIVE_DURABILITY", "conformance")
    if durability_name not in ("conformance", "independent"):
        raise SystemExit(
            "ARCHIVE_DURABILITY must be conformance or independent; got "
            f"{durability_name!r}"
        )
    if durability_name == "independent":
        # Invariant 7 as a `st_dev` comparison rather than a promise: an
        # archive root on the same filesystem as any primary data root is not
        # a second copy whatever the flag claims, because one device failure
        # takes both.
        archive_root.mkdir(parents=True, exist_ok=True)
        archive_device = _device_of(archive_root)
        for primary_root in primary_roots:
            if _device_of(primary_root) == archive_device:
                raise SystemExit(
                    f"refusing ARCHIVE_DURABILITY=independent: {archive_root} and "
                    f"{primary_root} are on the same filesystem, so losing it loses both "
                    "copies. Point the archive at separate storage, or leave the durability "
                    "class at 'conformance'."
                )
        durability = INDEPENDENT
    return LocalObjectStore(
        archive_root,
        store_id=configuration.get("ARCHIVE_STORE_ID") or None,
        durability=durability,
    )


def _device_of(path: Path) -> int:
    """Module-level so a test can fake two paths onto different devices."""
    return Path(path).resolve().stat().st_dev
