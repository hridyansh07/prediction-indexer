#!/usr/bin/env python3
"""Compatibility launcher for ``python3 targeter/run_v2.py``."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from targeter.v2.run import main


if __name__ == "__main__":
    raise SystemExit(main())
