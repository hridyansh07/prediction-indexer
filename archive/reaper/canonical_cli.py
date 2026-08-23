#!/usr/bin/env python3
"""Audit or delete local canonical frames proven archived for at least 18 hours."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from archive.common.durable import write_json_durable  # noqa: E402
from archive.reaper.canonical import (  # noqa: E402
    ARCHIVE_OBJECT_UNVERIFIED,
    IO_ERROR,
    LOCAL_WINDOW_CHANGED,
    RECEIPT_INVALID,
    RETAINED,
    RETENTION_FLOOR_HOURS,
    UNEXPECTED_WINDOW_ARTIFACT,
    CanonicalReaper,
)
from archive.storage.factory import add_store_arguments, build_store  # noqa: E402

EXIT_OK = 0
EXIT_RETAINED_FAULT = 1
DEFAULT_INTERVAL_SECONDS = 3600
FAULT_REASONS = frozenset(
    (
        RECEIPT_INVALID,
        UNEXPECTED_WINDOW_ARTIFACT,
        ARCHIVE_OBJECT_UNVERIFIED,
        LOCAL_WINDOW_CHANGED,
        IO_ERROR,
    )
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical-root", required=True, type=Path)
    add_store_arguments(parser)
    parser.add_argument("--mode", choices=("audit", "delete"), default="audit")
    parser.add_argument("--retention-hours", type=int, default=RETENTION_FLOOR_HOURS)
    parser.add_argument("--interval-seconds", type=int, default=None)
    parser.add_argument("--report", type=Path, default=None)
    return parser


def sweep_once(arguments: argparse.Namespace, store) -> int:
    destructive = arguments.mode == "delete"
    if destructive and not store.durability.independent:
        raise SystemExit(
            "refusing canonical delete mode without an independently durable archive backend"
        )
    result = CanonicalReaper(
        arguments.canonical_root,
        store,
        destructive=destructive,
        retention_ns=arguments.retention_hours * 3600 * 1_000_000_000,
    ).sweep()
    record = result.as_record()
    record["archive"] = {
        "store": store.store_id,
        "durability": store.durability.name,
    }
    record["retention_hours"] = arguments.retention_hours
    print(json.dumps(record, sort_keys=True))
    if arguments.report is not None:
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        write_json_durable(arguments.report, record)
    return (
        EXIT_RETAINED_FAULT
        if any(
            item.decision == RETAINED and item.reason in FAULT_REASONS
            for item in result.decisions
        )
        else EXIT_OK
    )


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.retention_hours < RETENTION_FLOOR_HOURS:
        raise SystemExit(
            f"--retention-hours must be at least {RETENTION_FLOOR_HOURS}"
        )
    if arguments.interval_seconds is not None and arguments.interval_seconds <= 0:
        raise SystemExit("--interval-seconds must be positive")
    # The shared local-store guard calls this side of the durability boundary
    # spool_root. For canonical reaping, canonical_root is the primary copy.
    arguments.spool_root = arguments.canonical_root
    store = build_store(arguments)
    if arguments.interval_seconds is None:
        return sweep_once(arguments, store)

    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    status = EXIT_OK
    while not stopping:
        status = sweep_once(arguments, store)
        if status != EXIT_OK:
            return status
        deadline = time.monotonic() + arguments.interval_seconds
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
