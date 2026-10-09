"""Open, validate and initialize the SQLite schema."""

from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Iterator

from universe.store.records import EvidenceConflict


SCHEMA_VERSION = 6
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schema" / "schema.sql"
REBUILD_INSTRUCTION = (
    "remove the rebuildable SQLite file and run backfill from the immutable archive"
)


class Connection:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version == 0:
                objects = self._schema_objects(connection)
                if not objects:
                    self._execute_schema_transaction(connection, SCHEMA_PATH)
                else:
                    raise EvidenceConflict(
                        f"Event Universe schema v{SCHEMA_VERSION} requires a fresh database; "
                        f"{REBUILD_INSTRUCTION}"
                    )
            elif version != SCHEMA_VERSION:
                raise EvidenceConflict(
                    f"unsupported Event Universe schema version {version}; "
                    f"{REBUILD_INSTRUCTION}"
                )
            self._validate_schema(connection)

    @staticmethod
    def _schema_objects(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
        return {
            (str(row[0]), str(row[1])): " ".join(str(row[2]).split())
            for row in connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE type IN ('table', 'index', 'view', 'trigger') AND sql IS NOT NULL "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }

    @classmethod
    def _expected_schema(cls) -> dict[tuple[str, str], str]:
        with closing(sqlite3.connect(":memory:")) as expected:
            expected.execute("PRAGMA foreign_keys = ON")
            expected.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            return cls._schema_objects(expected)

    @classmethod
    def _validate_schema(cls, connection: sqlite3.Connection) -> None:
        actual = cls._schema_objects(connection)
        expected = cls._expected_schema()
        if actual != expected:
            raise EvidenceConflict(
                f"database contains an invalid Event Universe schema v{SCHEMA_VERSION}; "
                f"{REBUILD_INSTRUCTION}"
            )

    @staticmethod
    def _execute_statements(connection: sqlite3.Connection, path: Path) -> None:
        # `executescript` parses SQL rather than splitting text on ";", so a
        # semicolon inside a comment or a literal cannot truncate the statement
        # it sits in. Splitting silently produced a table missing every column
        # after such a comment.
        connection.executescript(path.read_text(encoding="utf-8"))

    @classmethod
    def _execute_schema_transaction(
        cls, connection: sqlite3.Connection, *paths: Path
    ) -> None:
        try:
            connection.execute("BEGIN IMMEDIATE")
            for path in paths:
                cls._execute_statements(connection, path)
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def connect(self, *, readonly: bool = False) -> sqlite3.Connection:
        if readonly:
            connection = sqlite3.connect(
                f"file:{self.path}?mode=ro", uri=True, timeout=30.0
            )
            connection.execute("PRAGMA query_only = ON")
        else:
            connection = sqlite3.connect(self.path, timeout=30.0)
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA wal_autocheckpoint = 1000")
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
