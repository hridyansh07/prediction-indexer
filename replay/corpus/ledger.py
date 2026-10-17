"""Resumable per-bundle run ledger (SQLite on the local disk, never the data drive)."""

from __future__ import annotations

import sqlite3
import time
from contextlib import closing
from pathlib import Path

FINAL = ("done", "failed", "game_state_unavailable")
SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    corpus_key TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('running', 'done', 'failed', 'game_state_unavailable')),
    attempts INTEGER NOT NULL,
    started_ns INTEGER NOT NULL,
    finished_ns INTEGER,
    seconds REAL,
    bench_exit INTEGER,
    stage TEXT,
    detail TEXT,
    receipt_sha256 TEXT,
    PRIMARY KEY (corpus_key, bundle_id)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


class Ledger:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection, connection:
            connection.executescript(SCHEMA)

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def state(self, corpus_key, bundle_id):
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM runs WHERE corpus_key = ? AND bundle_id = ?",
                                     (corpus_key, bundle_id)).fetchone()
        return None if row is None else dict(row)

    def start(self, corpus_key, bundle_id):
        """Claim one bundle; a crashed `running` row is reclaimed with attempts + 1."""
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """INSERT INTO runs (corpus_key, bundle_id, state, attempts, started_ns)
                   VALUES (?, ?, 'running', 1, ?)
                   ON CONFLICT (corpus_key, bundle_id) DO UPDATE SET
                     state = 'running', attempts = attempts + 1, started_ns = excluded.started_ns,
                     finished_ns = NULL, seconds = NULL, bench_exit = NULL, stage = NULL,
                     detail = NULL, receipt_sha256 = NULL""",
                (corpus_key, bundle_id, time.time_ns()))

    def finish(self, corpus_key, bundle_id, state, *, seconds, bench_exit=None, stage=None, detail=None,
               receipt_sha256=None):
        if state not in FINAL:
            raise ValueError(f"not a final ledger state: {state}")
        with closing(self._connect()) as connection, connection:
            updated = connection.execute(
                """UPDATE runs SET state = ?, finished_ns = ?, seconds = ?, bench_exit = ?, stage = ?,
                          detail = ?, receipt_sha256 = ?
                   WHERE corpus_key = ? AND bundle_id = ? AND state = 'running'""",
                (state, time.time_ns(), seconds, bench_exit, stage, detail, receipt_sha256,
                 corpus_key, bundle_id)).rowcount
        if updated != 1:
            raise RuntimeError(f"ledger row for {bundle_id} is not running")

    def summary(self, corpus_key):
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT state, COUNT(*) AS n, SUM(seconds) AS s FROM runs WHERE corpus_key = ? GROUP BY state",
                (corpus_key,)).fetchall()
        return {row["state"]: {"bundles": row["n"], "seconds": row["s"] or 0.0} for row in rows}

    def get_meta(self, key):
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else row["value"]

    def set_meta(self, key, value):
        with closing(self._connect()) as connection, connection:
            connection.execute("INSERT INTO meta (key, value) VALUES (?, ?) "
                               "ON CONFLICT (key) DO UPDATE SET value = excluded.value", (key, value))
