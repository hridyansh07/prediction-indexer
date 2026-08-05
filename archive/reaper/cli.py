#!/usr/bin/env python3
"""Delete local raw segments that two receipts prove are safe to delete.

```sh
python -m archive.reaper.cli --spool-root     /var/lib/prediction-indexer/spool \
                             --canonical-root /var/lib/prediction-indexer/canonical \
                             --archive-root   /var/lib/prediction-archive
```

Or, against an S3 backend, with `--archive-backend s3` in place of `--archive-root`
and `--archive-durability` (both commands share `archive/storage/factory.py`'s
reading of `--archive-backend local|s3`; see `run_archiver.py`).

**Audit mode is the default and stays the default.** Installing this code does
not make raw deletion active. Deleting requires all of:

```text
--archive-durability independent   the archive is a separate durability domain
--mode delete                      the operator explicitly enabled deletion
```

and the archive root must not share a filesystem with the spool. §8.2 keeps the
`--delete` remains a manual compatibility alias. The deployment CLI's gate
stays even though the reaper *library* can be pointed at a
temporary backend by a test — the gate is what stops a compression probe on the
capture disk from authorizing the deletion of the only copy.
"""

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

from archive.reaper import AUDIT_MODE, RETAINED, Reaper  # noqa: E402
from archive.common.durable import write_json_durable  # noqa: E402
from archive.storage.factory import add_store_arguments, build_store  # noqa: E402

EXIT_OK = 0
EXIT_RETAINED_FAULT = 1
DEFAULT_INTERVAL_SECONDS = 3600


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spool-root", required=True, type=Path)
    parser.add_argument("--canonical-root", required=True, type=Path)
    add_store_arguments(parser)
    parser.add_argument(
        "--delete",
        action="store_true",
        help="actually remove proven-safe raw segments. Without it the reaper reports "
        "what it would do and touches nothing.",
    )
    parser.add_argument(
        "--mode",
        choices=("audit", "delete"),
        default="audit",
        help="audit is non-destructive; delete removes only segments proven safe",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help=f"run continuously, sweeping every N seconds (recommended {DEFAULT_INTERVAL_SECONDS})",
    )
    parser.add_argument("--report", type=Path, default=None, help="write decisions as JSON")
    return parser


def sweep_once(arguments: argparse.Namespace, store) -> int:
    destructive = arguments.mode == "delete" or arguments.delete

    if destructive and not store.durability.independent:
        raise SystemExit(
            "refusing delete mode against a local conformance store. Raw local data is the "
            "recovery authority until an independently durable archive backend is "
            "configured; see archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md §5.3."
        )

    reaper = Reaper(
        arguments.spool_root,
        arguments.canonical_root,
        store,
        destructive=destructive,
    )
    result = reaper.sweep()
    record = result.as_record()
    record["archive"] = {
        "store": store.store_id,
        "durability": store.durability.name,
    }
    print(json.dumps(record, sort_keys=True))
    if arguments.report is not None:
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        write_json_durable(arguments.report, record)

    # Retention for want of a canonical receipt is ordinary backlog. Retention
    # because a receipt or an object failed to verify is not, and §7.3 requires
    # the difference to be visible rather than pooled into one number.
    faults = [
        decision
        for decision in result.decisions
        if decision.decision == RETAINED and decision.reason not in (AUDIT_MODE,)
    ]
    unverifiable = [
        decision
        for decision in faults
        if decision.reason in ("archive_receipt_invalid", "archive_object_unverified", "io_error")
    ]
    if unverifiable or result.canonical_faults:
        return EXIT_RETAINED_FAULT
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.interval_seconds is not None and arguments.interval_seconds <= 0:
        raise SystemExit("--interval-seconds must be positive")
    if arguments.delete and arguments.mode != "audit":
        raise SystemExit("--delete is a compatibility alias; do not combine it with --mode delete")
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
