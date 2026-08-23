"""Shared backend configuration for both archive commands.

`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md` §11: `archive.archiver.cli` and
`archive.reaper.cli` must interpret `--archive-backend` and its flags
identically. A factory that only one command used, or that each command
reimplemented, is exactly how the archiver could come to believe it is writing
production receipts while the reaper reads a receipt against a differently
configured backend — the two processes silently disagreeing about what "the
archive" is.

`--archive-root`, `--archive-durability` and `--store-id` are local-only.
The Compose `archiver`/`reaper` commands are one static argument list that
keeps passing them regardless of which backend is selected (§11), so this
factory *ignores* them for an S3 backend rather than rejecting them — S3 is
always `INDEPENDENT` and its `store_id` is always the bucket name, so nothing
those flags say can change either. The one asymmetric case is the reverse:
a non-empty `--s3-*` option while `local` is selected fails at startup,
because unlike the local flags an operator has no reason to be carrying S3
configuration around unless they meant to select S3 — silently ignoring it
would let a mistyped `--archive-backend` read local while quietly holding
live bucket credentials nobody is using.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from archive.storage.base import CONFORMANCE, INDEPENDENT, ObjectStore
from archive.storage.local import LocalObjectStore
from archive.storage.s3 import S3ObjectStore

__all__ = ["LOCAL_BACKEND", "S3_BACKEND", "add_store_arguments", "build_store"]

LOCAL_BACKEND = "local"
S3_BACKEND = "s3"


def add_store_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds the one shared backend contract (§11) to a command's parser."""
    parser.add_argument(
        "--archive-backend",
        choices=(LOCAL_BACKEND, S3_BACKEND),
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
        help="local backend only. An S3 backend is always independent; this is ignored "
        "rather than rejected when --archive-backend s3 is selected, so one static command "
        "line works for either backend.",
    )
    parser.add_argument(
        "--store-id",
        default=None,
        help="local backend only: names this archive in receipts. An S3 backend's store_id "
        "is always its bucket name and this is ignored when --archive-backend s3 is selected.",
    )
    parser.add_argument("--s3-bucket", default="", help="s3 backend only: the dedicated bucket name")
    parser.add_argument("--s3-region", default="", help="s3 backend only: the bucket's AWS region")
    parser.add_argument(
        "--s3-expected-owner",
        default="",
        help="s3 backend only: the bucket owner's 12-digit AWS account id",
    )


def build_store(arguments: argparse.Namespace) -> ObjectStore:
    """Builds the configured backend, or refuses an unsafe or ambiguous one.

    Reads the primary data roots on the shared namespace for the local
    backend's `st_dev` independence check. Raw commands always provide
    `spool_root`; combined canonical commands also provide `canonical_root`.
    """
    if arguments.archive_backend == S3_BACKEND:
        return _build_s3_store(arguments)
    return _build_local_store(arguments)


def _build_s3_store(arguments: argparse.Namespace) -> S3ObjectStore:
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
