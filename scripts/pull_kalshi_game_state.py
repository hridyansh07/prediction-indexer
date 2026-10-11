#!/usr/bin/env python3
"""Compatibility entry point; the pull implementation lives in gamestate."""
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gamestate import kalshi

if __name__ == "__main__":
    try:
        raise SystemExit(kalshi.main())
    except (ValueError, OSError, kalshi.ObjectStoreError):
        print("game-state pull failed: invalid input, storage, or schema; no response bodies logged", file=sys.stderr)
        raise SystemExit(1)
else:
    # Existing Python callers and monkeypatch seams retain the same module.
    sys.modules[__name__] = kalshi
