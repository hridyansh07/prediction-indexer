from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def safe_name(value: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", value)
    if len(safe) <= 120:
        return safe
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"{safe[:96]}-{digest}"


def row_fingerprint(row: Mapping[str, Any]) -> str:
    """Hash a row ignoring provenance so re-fetched pages deduplicate."""
    native = {key: value for key, value in row.items() if key != "_provenance"}
    encoded = json.dumps(
        native,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def existing_fingerprints(path: Path) -> set[str]:
    fingerprints: set[str] = set()
    if not path.exists():
        return fingerprints
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number} is not a JSON object")
            fingerprints.add(row_fingerprint(value))
    return fingerprints


def append_rows(path: Path, rows: list[Mapping[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            )
        handle.flush()
        os.fsync(handle.fileno())
