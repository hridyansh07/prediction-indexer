#!/usr/bin/env python3
"""Archive sealed segments to an object store. One sweep, or on an interval.

```sh
ARCHIVE_BACKEND=local ARCHIVE_ROOT=/var/lib/prediction-archive \
python -m archive.archiver.cli --spool-root  /var/lib/prediction-indexer/spool \
                               --manifest-root /var/lib/prediction-indexer/manifests
```

Or configure S3 in the environment (`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md`):

```sh
ARCHIVE_BACKEND=s3 ARCHIVE_S3_BUCKET=my-archive-bucket \
ARCHIVE_S3_REGION=us-east-1 ARCHIVE_S3_EXPECTED_OWNER=123456789012 \
python -m archive.archiver.cli --spool-root /var/lib/prediction-indexer/spool
```

Both commands build their backend through `archive/storage/factory.py`, the one
place archive environment configuration is interpreted.

Watch mode calls the same sweep implementation on a timer. It must not, and does
not, introduce different eligibility logic — a scheduler running the one-shot
form and a long-lived container running the watch form archive exactly the same
segments (§8.1).

The exit code carries the one thing an operator has to react to immediately: a
non-zero exit means an immutable key holds unexpected content, which is a
namespace or integrity failure rather than a segment to retry.
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

from archive.archiver import CONFLICT, FAILED, Archiver  # noqa: E402
from archive.archiver.canonical import (  # noqa: E402
    CONFLICT as CANONICAL_CONFLICT,
    FAILED as CANONICAL_FAILED,
    CanonicalArchiver,
)
from archive.common.durable import write_json_durable  # noqa: E402
from archive.archiver.manifest import (  # noqa: E402
    build_daily_manifests,
    discover_archive_receipts,
    write_daily_manifests,
)
from archive.storage.base import ObjectStore  # noqa: E402
from archive.common.receipts import LOCAL, PRODUCTION  # noqa: E402
from archive.storage.factory import build_store  # noqa: E402

#: §8.1's recommended sweep interval. The archive *unit* stays one sealed
#: segment however many an hour discovers.
DEFAULT_INTERVAL_SECONDS = 3600

EXIT_OK = 0
EXIT_FAILURES = 1
EXIT_CONFLICT = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spool-root", required=True, type=Path)
    parser.add_argument(
        "--canonical-root",
        type=Path,
        default=None,
        help="also archive receipt-committed canonical windows from this root",
    )
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=None,
        help="where daily manifests are regenerated. Derived and rebuildable; omit to skip.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help=f"run continuously, sweeping every N seconds (recommended {DEFAULT_INTERVAL_SECONDS})",
    )
    parser.add_argument(
        "--report", type=Path, default=None, help="write the sweep result as JSON"
    )
    return parser


def sweep_once(arguments: argparse.Namespace, store: ObjectStore) -> int:
    archiver = Archiver(arguments.spool_root, store)
    result = archiver.sweep()
    record = result.as_record()

    canonical = None
    if (
        arguments.canonical_root is not None
        and not result.halted
        and not result.count(CONFLICT)
    ):
        canonical = CanonicalArchiver(arguments.canonical_root, store).sweep()
        record["canonical"] = canonical.as_record()

    if arguments.manifest_root is not None:
        kind = PRODUCTION if store.durability.receipt_kind == PRODUCTION else LOCAL
        manifests = build_daily_manifests(
            store,
            discover_archive_receipts(arguments.spool_root, kind=kind),
            kind=kind,
            now_ns=time.time_ns(),
        )
        write_daily_manifests(arguments.manifest_root, manifests)
        record["manifests"] = manifests.counts
        record["manifest_exclusions"] = manifests.excluded

    record["archive"] = {
        "store": store.store_id,
        "durability": store.durability.name,
        "receipt_kind": store.durability.receipt_kind,
    }
    print(json.dumps(record, sort_keys=True))
    if arguments.report is not None:
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        write_json_durable(arguments.report, record)

    if (
        result.halted
        or result.count(CONFLICT)
        or (
            canonical is not None
            and (canonical.halted or canonical.count(CANONICAL_CONFLICT))
        )
    ):
        return EXIT_CONFLICT
    if result.count(FAILED) or (
        canonical is not None and canonical.count(CANONICAL_FAILED)
    ):
        return EXIT_FAILURES
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.interval_seconds is not None and arguments.interval_seconds <= 0:
        raise SystemExit("--interval-seconds must be positive")
    primary_roots = [arguments.spool_root]
    if arguments.canonical_root is not None:
        primary_roots.append(arguments.canonical_root)
    store = build_store(primary_roots)

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
        if status == EXIT_CONFLICT:
            # A conflict is not a transient condition, and sweeping again would
            # bury the alert under identical output every interval.
            return status
        deadline = time.monotonic() + arguments.interval_seconds
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
