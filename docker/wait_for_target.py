#!/usr/bin/env python3
"""Wait for one target file, then replace this process with a splice command."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path


def main() -> int:
    try:
        separator = sys.argv.index("--")
        target = Path(sys.argv[1])
        command = sys.argv[separator + 1 :]
    except (ValueError, IndexError):
        print(
            "usage: wait_for_target.py <targets.json> -- <command> [args...]",
            file=sys.stderr,
        )
        return 2

    if separator != 2 or not command:
        print(
            "usage: wait_for_target.py <targets.json> -- <command> [args...]",
            file=sys.stderr,
        )
        return 2

    announced = False
    while not target.is_file():
        if not announced:
            print(f"waiting for targeter to publish {target}", file=sys.stderr, flush=True)
            announced = True
        time.sleep(1.0)

    print(f"targets ready: {target}; starting {' '.join(command)}", file=sys.stderr, flush=True)
    os.execvp(command[0], command)
    return 127


if __name__ == "__main__":
    raise SystemExit(main())

