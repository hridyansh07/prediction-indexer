#!/usr/bin/env python3
"""Delete local Targeter v2 run artifacts that an archive receipt proves are safe.

```sh
python -m targeter.v2.run_reaper_cli \
    --output-root /var/lib/prediction-indexer/targeter-v2-runs \
    --live-root   /var/lib/prediction-indexer/live \
    --archive-root /var/lib/prediction-archive
```

Or against S3 with `--archive-backend s3` in place of `--archive-root` and
`--archive-durability`; both this command and `run_archiver_cli` build their
store through `archive/storage/factory.py`, the same factory the raw archive
commands use.

**Audit mode is the default and stays the default.** Installing this code does
not make run deletion active. Deleting requires all of:

```text
--archive-durability independent   the archive is a separate durability domain
--mode delete                      the operator explicitly enabled deletion
```

and, for a local backend, an archive root on a different filesystem from the
runs. `--live-root` is required rather than defaulted because an unreadable
publication pointer retains every run: a command that could be invoked without
it would turn "the operator forgot a flag" into "nothing is protected".

One sweep per invocation. There is deliberately no `--interval-seconds` — every
Targeter v2 command is a one-shot transaction that a host scheduler repeats, so
none of them owns an internal sleep loop.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from archive.common.durable import write_json_durable  # noqa: E402
from archive.storage.factory import add_store_arguments, build_store  # noqa: E402
from targeter.v2.run_reaper import (  # noqa: E402
    ARCHIVE_OBJECT_UNVERIFIED,
    AUDIT_MODE,
    IO_ERROR,
    LOCAL_RUN_CHANGED,
    POINTER_UNREADABLE,
    RECEIPT_INVALID,
    RETAINED,
    RETENTION_FLOOR_HOURS,
    RUN_CLOCK_UNREADABLE,
    UNEXPECTED_RUN_ARTIFACT,
    TargetRunReaper,
)

EXIT_OK = 0
EXIT_RETAINED_FAULT = 1

#: Retentions that mean something is wrong rather than that the gate is simply
#: shut. A conformance backend, an unreaped floor and the published generation
#: are all ordinary steady state; exiting non-zero on them every hour would
#: train an operator to ignore this command.
FAULT_REASONS = (
    RECEIPT_INVALID,
    ARCHIVE_OBJECT_UNVERIFIED,
    LOCAL_RUN_CHANGED,
    UNEXPECTED_RUN_ARTIFACT,
    RUN_CLOCK_UNREADABLE,
    POINTER_UNREADABLE,
    IO_ERROR,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--live-root", required=True, type=Path)
    add_store_arguments(parser)
    parser.add_argument(
        "--mode",
        choices=("audit", "delete"),
        default="audit",
        help="audit is non-destructive; delete removes only runs proven safe",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=RETENTION_FLOOR_HOURS,
        help=(
            f"retain every run younger than this, whatever else is proven "
            f"({RETENTION_FLOOR_HOURS} is the minimum; it may be raised)"
        ),
    )
    parser.add_argument("--report", type=Path, default=None, help="write decisions as JSON")
    return parser


def sweep_once(arguments: argparse.Namespace, store) -> int:
    destructive = arguments.mode == "delete"

    if destructive and not store.durability.independent:
        raise SystemExit(
            "refusing delete mode against a local conformance store. The local run "
            "directory is the recovery authority until an independently durable "
            "archive backend is configured; see docs/TARGETER_V2_PHASES_6_10.md."
        )

    reaper = TargetRunReaper(
        arguments.output_root,
        arguments.live_root,
        store,
        destructive=destructive,
        retention_ns=arguments.retention_hours * 3600 * 1_000_000_000,
    )
    result = reaper.sweep()
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

    faults = [
        decision
        for decision in result.decisions
        if decision.decision == RETAINED and decision.reason in FAULT_REASONS
    ]
    if faults or result.pointer_faults:
        return EXIT_RETAINED_FAULT
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.retention_hours < RETENTION_FLOOR_HOURS:
        raise SystemExit(
            f"--retention-hours must be at least {RETENTION_FLOOR_HOURS}; a shorter "
            "floor stops protecting a run before the next scheduled audit can read it"
        )
    # The shared archive factory calls this root `spool_root` because raw
    # capture introduced the adapter. For run reaping the analogous primary
    # copy is output_root, so the same-filesystem guard must compare the
    # archive against that directory.
    arguments.spool_root = arguments.output_root
    store = build_store(arguments)
    return sweep_once(arguments, store)


if __name__ == "__main__":
    raise SystemExit(main())
