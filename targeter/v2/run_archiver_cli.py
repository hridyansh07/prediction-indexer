#!/usr/bin/env python3
"""Archive Targeter v2 run directories that hold no receipt yet.

```sh
python -m targeter.v2.run_archiver_cli \
    --output-root /var/lib/prediction-indexer/targeter-v2-runs \
    --archive-root /var/lib/prediction-archive
```

Or against S3 with `--archive-backend s3`, through the same store factory the
raw archive commands use.

This command **never deletes anything**, and there is no flag that makes it.
Archival and deletion are separate authorities (`PHASE_4 §7.1`); reclaiming
disk is `run_reaper_cli`'s job, and it will only reclaim runs this command has
already proven are somewhere else.

It holds the same run lease a scheduled `targeter/run_v2.py` takes, because
that process archives inside its own leased region: without the lease both
could archive one directory and write two receipts recording different archive
instants, leaving the run's receipt no longer the one that authorized its
publication. A sweep that cannot take the lease reports that and exits zero —
the lease being held means a scheduled run is in progress, which is the
expected state several times an hour, not a fault.

One sweep per invocation; a host scheduler repeats it.
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
from targeter.v2.lease import TargeterLeaseError, TargeterRunLease  # noqa: E402
from targeter.v2.run_archiver import (  # noqa: E402
    CONFLICT,
    FAILED,
    RunSweepResult,
    TargetRunArchiveSweep,
)

EXIT_OK = 0
EXIT_FAILURES = 1
EXIT_CONFLICT = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-root", required=True, type=Path)
    add_store_arguments(parser)
    parser.add_argument("--report", type=Path, default=None, help="write outcomes as JSON")
    return parser


def _publish(arguments: argparse.Namespace, store, result: RunSweepResult) -> None:
    record = result.as_record()
    record["archive"] = {
        "store": store.store_id,
        "durability": store.durability.name,
    }
    print(json.dumps(record, sort_keys=True))
    if arguments.report is not None:
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        write_json_durable(arguments.report, record)


def sweep_once(arguments: argparse.Namespace, store) -> int:
    try:
        lease = TargeterRunLease.acquire(arguments.output_root)
    except TargeterLeaseError:
        _publish(arguments, store, RunSweepResult(lease_acquired=False))
        return EXIT_OK

    try:
        result = TargetRunArchiveSweep(arguments.output_root, store).sweep()
    finally:
        lease.close()

    _publish(arguments, store, result)
    if result.halted or result.count(CONFLICT):
        return EXIT_CONFLICT
    if result.count(FAILED):
        return EXIT_FAILURES
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    # The shared archive factory calls this root `spool_root`; for run archival
    # the analogous primary copy is output_root, so the same-filesystem guard
    # must compare the archive against that directory.
    arguments.spool_root = arguments.output_root
    store = build_store(arguments)
    return sweep_once(arguments, store)


if __name__ == "__main__":
    raise SystemExit(main())
