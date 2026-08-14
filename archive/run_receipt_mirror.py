#!/usr/bin/env python3
"""Mirror retained production receipt documents for remote universe backfill."""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from archive.archiver.receipt_mirror import (  # noqa: E402
    load_config,
    mirror_retained_receipts,
)


def main() -> int:
    config = load_config()
    result = mirror_retained_receipts(config.receipt_root, config.object_store())
    print(json.dumps(result.as_record(), ensure_ascii=False, sort_keys=True))
    return 1 if result.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
