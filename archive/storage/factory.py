"""Configure the ObjectStore shared by archive and reaper commands.

Local-only options are ignored by cloud backends so Compose can use one static
command line. Options for an unselected cloud provider are rejected rather
than silently connecting to a different archive than the operator intended.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from archive.storage.base import CONFORMANCE, INDEPENDENT, ObjectStore, ObjectStoreError
from archive.storage.gcs import GCSObjectStore
from archive.storage.local import LocalObjectStore
from archive.storage.s3 import S3ObjectStore

__all__ = ["GCS_BACKEND", "LOCAL_BACKEND", "S3_BACKEND", "add_store_arguments", "build_store"]

LOCAL_BACKEND = "local"
S3_BACKEND = "s3"
GCS_BACKEND = "gcs"


def add_store_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds the one shared backend contract (§11) to a command's parser."""
    parser.add_argument(
        "--archive-backend",
        choices=(LOCAL_BACKEND, S3_BACKEND, GCS_BACKEND),
        default=LOCAL_BACKEND,
        help="which ObjectStore backend this command writes to and reads from",
    )
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="local backend only: where the object store writes",
    )
    parser.add_argument(
        "--archive-durability",
        choices=("conformance", "independent"),
        default="conformance",
        help="local backend only; cloud backends are always independent",
    )
    parser.add_argument(
        "--store-id",
        default=None,
        help="local backend only: names this archive in receipts",
    )
    parser.add_argument("--s3-bucket", default="", help="s3 backend only: the dedicated bucket name")
    parser.add_argument("--s3-region", default="", help="s3 backend only: the bucket's AWS region")
    parser.add_argument(
        "--s3-expected-owner",
        default="",
        help="s3 backend only: the bucket owner's 12-digit AWS account id",
    )
    parser.add_argument("--gcs-bucket", default="", help="gcs backend only: archive bucket")


def build_store(arguments: argparse.Namespace) -> ObjectStore:
    """Builds the configured backend, or refuses an unsafe or ambiguous one.

    Reads the primary data roots on the shared namespace for the local
    backend's `st_dev` independence check. Raw commands always provide
    `spool_root`; combined canonical commands also provide `canonical_root`.
    """
    if arguments.archive_backend == S3_BACKEND:
        return _build_s3_store(arguments)
    if arguments.archive_backend == GCS_BACKEND:
        return _build_gcs_store(arguments)
    return _build_local_store(arguments)


def _build_gcs_store(arguments: argparse.Namespace) -> GCSObjectStore:
    if not arguments.gcs_bucket:
        raise SystemExit("--archive-backend gcs requires --gcs-bucket")
    live_s3_options = [
        name
        for name, value in (
            ("--s3-bucket", arguments.s3_bucket),
            ("--s3-region", arguments.s3_region),
            ("--s3-expected-owner", arguments.s3_expected_owner),
        )
        if value
    ]
    if live_s3_options:
        raise SystemExit(
            "--archive-backend gcs cannot be combined with " + ", ".join(live_s3_options)
        )
    try:
        return GCSObjectStore(arguments.gcs_bucket)
    except (ValueError, ObjectStoreError) as error:
        raise SystemExit(f"invalid GCS archive configuration: {error}") from error


def _build_s3_store(arguments: argparse.Namespace) -> S3ObjectStore:
    if arguments.gcs_bucket:
        raise SystemExit("--archive-backend s3 cannot be combined with --gcs-bucket")
    required = (
        ("--s3-bucket", arguments.s3_bucket),
        ("--s3-region", arguments.s3_region),
        ("--s3-expected-owner", arguments.s3_expected_owner),
    )
    missing = [name for name, value in required if not value]
    if missing:
        raise SystemExit(
            f"--archive-backend s3 requires {', '.join(missing)}; the S3 adapter never infers "
            "bucket, region, or account configuration."
        )
    try:
        return S3ObjectStore(
            arguments.s3_bucket, arguments.s3_region, arguments.s3_expected_owner
        )
    except ValueError as error:
        raise SystemExit(f"invalid S3 archive configuration: {error}") from error


def _build_local_store(arguments: argparse.Namespace) -> LocalObjectStore:
    live_s3_options = {
        name: value
        for name, value in (
            ("--s3-bucket", arguments.s3_bucket),
            ("--s3-region", arguments.s3_region),
            ("--s3-expected-owner", arguments.s3_expected_owner),
        )
        if value
    }
    if live_s3_options:
        offered = ", ".join(f"{name}={value!r}" for name, value in live_s3_options.items())
        raise SystemExit(
            f"--archive-backend local was selected but {offered} was also set. Refusing to "
            "guess which backend is really wanted: pass --archive-backend s3, or clear the "
            "S3 options."
        )
    if arguments.gcs_bucket:
        raise SystemExit(
            "--archive-backend local was selected but --gcs-bucket was also set. Pass "
            "--archive-backend gcs or clear the GCS option."
        )
    if arguments.archive_root is None:
        raise SystemExit("--archive-backend local requires --archive-root")

    durability = CONFORMANCE
    if arguments.archive_durability == "independent":
        # Invariant 7 as a `st_dev` comparison rather than a promise: an
        # archive root on the same filesystem as any primary data root is not
        # a second copy whatever the flag claims, because one device failure
        # takes both.
        primary_roots = [Path(arguments.spool_root)]
        canonical_root = getattr(arguments, "canonical_root", None)
        if (
            canonical_root is not None
            and Path(canonical_root).resolve() != primary_roots[0].resolve()
        ):
            primary_roots.append(Path(canonical_root))
        arguments.archive_root.mkdir(parents=True, exist_ok=True)
        archive_device = _device_of(arguments.archive_root)
        for primary_root in primary_roots:
            if _device_of(primary_root) == archive_device:
                raise SystemExit(
                    f"refusing --archive-durability independent: {arguments.archive_root} and "
                    f"{primary_root} are on the same filesystem, so losing it loses both "
                    "copies. Point the archive at separate storage, or leave the durability "
                    "class at 'conformance'."
                )
        durability = INDEPENDENT
    return LocalObjectStore(
        arguments.archive_root, store_id=arguments.store_id, durability=durability
    )


def _device_of(path: Path) -> int:
    """Module-level so a test can fake two paths onto different devices."""
    return Path(path).resolve().stat().st_dev
