"""Build the shared ObjectStore from process environment configuration."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Mapping

from archive.storage.base import CONFORMANCE, INDEPENDENT, ObjectStore, ObjectStoreError
from archive.storage.gcs import GCSObjectStore
from archive.storage.local import LocalObjectStore
from archive.storage.s3 import S3ObjectStore

__all__ = ["GCS_BACKEND", "LOCAL_BACKEND", "S3_BACKEND", "build_store"]

LOCAL_BACKEND = "local"
S3_BACKEND = "s3"
GCS_BACKEND = "gcs"


def build_store(
    primary_roots: Iterable[Path],
    *,
    environ: Mapping[str, str] | None = None,
) -> ObjectStore:
    """Build the configured backend or refuse an unsafe or ambiguous one."""
    configuration = os.environ if environ is None else environ
    backend = configuration.get("ARCHIVE_BACKEND", LOCAL_BACKEND)
    if backend == S3_BACKEND:
        return _build_s3_store(configuration)
    if backend == GCS_BACKEND:
        return _build_gcs_store(configuration)
    if backend != LOCAL_BACKEND:
        raise SystemExit(f"ARCHIVE_BACKEND must be local, s3, or gcs; got {backend!r}")
    return _build_local_store(
        configuration, tuple(Path(path) for path in primary_roots)
    )


def _build_gcs_store(configuration: Mapping[str, str]) -> GCSObjectStore:
    bucket = configuration.get("ARCHIVE_GCS_BUCKET", "")
    if not bucket:
        raise SystemExit("ARCHIVE_BACKEND=gcs requires ARCHIVE_GCS_BUCKET")
    live_s3_options = [
        name
        for name, value in (
            ("ARCHIVE_S3_BUCKET", configuration.get("ARCHIVE_S3_BUCKET", "")),
            ("ARCHIVE_S3_REGION", configuration.get("ARCHIVE_S3_REGION", "")),
            (
                "ARCHIVE_S3_EXPECTED_OWNER",
                configuration.get("ARCHIVE_S3_EXPECTED_OWNER", ""),
            ),
        )
        if value
    ]
    if live_s3_options:
        raise SystemExit(
            "ARCHIVE_BACKEND=gcs cannot be combined with " + ", ".join(live_s3_options)
        )
    try:
        return GCSObjectStore(bucket)
    except (ValueError, ObjectStoreError) as error:
        raise SystemExit(f"invalid GCS archive configuration: {error}") from error


def _build_s3_store(configuration: Mapping[str, str]) -> S3ObjectStore:
    if configuration.get("ARCHIVE_GCS_BUCKET", ""):
        raise SystemExit(
            "ARCHIVE_BACKEND=s3 cannot be combined with ARCHIVE_GCS_BUCKET"
        )
    required = (
        ("ARCHIVE_S3_BUCKET", configuration.get("ARCHIVE_S3_BUCKET", "")),
        ("ARCHIVE_S3_REGION", configuration.get("ARCHIVE_S3_REGION", "")),
        (
            "ARCHIVE_S3_EXPECTED_OWNER",
            configuration.get("ARCHIVE_S3_EXPECTED_OWNER", ""),
        ),
    )
    missing = [name for name, value in required if not value]
    if missing:
        raise SystemExit(
            f"ARCHIVE_BACKEND=s3 requires {', '.join(missing)}; the S3 adapter never infers "
            "bucket, region, or account configuration."
        )
    try:
        return S3ObjectStore(required[0][1], required[1][1], required[2][1])
    except ValueError as error:
        raise SystemExit(f"invalid S3 archive configuration: {error}") from error


def _build_local_store(
    configuration: Mapping[str, str], primary_roots: tuple[Path, ...]
) -> LocalObjectStore:
    live_s3_options = {
        name: value
        for name, value in (
            ("ARCHIVE_S3_BUCKET", configuration.get("ARCHIVE_S3_BUCKET", "")),
            ("ARCHIVE_S3_REGION", configuration.get("ARCHIVE_S3_REGION", "")),
            (
                "ARCHIVE_S3_EXPECTED_OWNER",
                configuration.get("ARCHIVE_S3_EXPECTED_OWNER", ""),
            ),
        )
        if value
    }
    if live_s3_options:
        offered = ", ".join(
            f"{name}={value!r}" for name, value in live_s3_options.items()
        )
        raise SystemExit(
            f"ARCHIVE_BACKEND=local was selected but {offered} was also set. Refusing to "
            "guess which backend is really wanted: set ARCHIVE_BACKEND=s3, or clear the "
            "S3 options."
        )
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
