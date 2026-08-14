#!/usr/bin/env python3
"""Operate the Event Universe store as explicit one-shot jobs and a read API."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from archive.storage.base import ObjectStore
from archive.storage.factory import add_store_arguments, build_store
from encoder import StoredIdentity
from universe.api import serve
from universe.backfill import backfill_segment_universe, receipt_inventory
from universe.store import SQLITE_CONTENT_TYPE, UniverseStore, file_sha256
from universe.sync import UniverseSync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init", help="create or migrate the SQLite database")
    commands.add_parser("status", help="print schema and row counts")

    sync = commands.add_parser("sync", help="ingest newly committed archive evidence once")
    add_store_arguments(sync)

    backfill = commands.add_parser(
        "backfill-controls",
        help="derive historical control sidecars from retained production receipts",
    )
    backfill.add_argument("--receipt-root", action="append", type=Path, default=[])
    backfill.add_argument("--receipt-inventory", action="append", type=Path, default=[])
    backfill.add_argument("--temp-root", type=Path, default=None)
    add_store_arguments(backfill)

    backup = commands.add_parser("backup", help="create a consistent SQLite backup")
    backup.add_argument("--output", required=True, type=Path)
    backup.add_argument(
        "--object-key",
        default=None,
        help="also publish the backup immutably to the configured object store",
    )
    add_store_arguments(backup)

    server = commands.add_parser("serve", help="serve the read-only JSON API")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8080)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    database = UniverseStore(arguments.database)
    if arguments.command == "init":
        database.initialize()
        _print(database.status())
        return 0
    database.initialize()
    if arguments.command == "status":
        _print(database.status())
        return 0
    if arguments.command == "serve":
        if not 1 <= arguments.port <= 65535:
            raise SystemExit("--port must be between 1 and 65535")
        serve(database, arguments.host, arguments.port)
        return 0

    if arguments.command == "sync":
        objects = _build_store(arguments, database)
        result = UniverseSync(database, objects).sync()
        _print(result.as_record())
        return 1 if result.failures else 0
    if arguments.command == "backfill-controls":
        if not arguments.receipt_root and not arguments.receipt_inventory:
            raise SystemExit(
                "backfill-controls requires --receipt-root or --receipt-inventory"
            )
        inventory = receipt_inventory(
            arguments.receipt_root, arguments.receipt_inventory
        )
        objects = _build_store(arguments, database)
        result = backfill_segment_universe(
            receipt_paths=inventory,
            objects=objects,
            database=database,
            temp_root=arguments.temp_root,
        )
        _print(result.as_record())
        return 1 if result.failures else 0
    if arguments.command == "backup":
        path = database.backup(arguments.output)
        digest, byte_length = file_sha256(path)
        record: dict[str, object] = {
            "path": str(path),
            "sha256": digest,
            "byte_length": byte_length,
        }
        if arguments.object_key is not None:
            objects = _build_store(arguments, database)
            with path.open("rb") as source:
                metadata = objects.put_immutable(
                    arguments.object_key,
                    source,
                    StoredIdentity(sha256=digest, byte_length=byte_length),
                    content_type=SQLITE_CONTENT_TYPE,
                )
            if metadata.sha256 != digest or metadata.byte_length != byte_length:
                raise SystemExit("published backup failed identity verification")
            record["object"] = {
                "key": metadata.key,
                "store": objects.store_id,
                "sha256": metadata.sha256,
                "byte_length": metadata.byte_length,
                "created_at": datetime.now(timezone.utc).isoformat().replace(
                    "+00:00", "Z"
                ),
            }
        _print(record)
        return 0
    raise AssertionError(f"unhandled command: {arguments.command}")


def _build_store(arguments: argparse.Namespace, database: UniverseStore) -> ObjectStore:
    # The shared factory uses spool_root only to reject a false independent
    # local durability claim. The universe does not own a spool, so its own
    # durable volume is the conservative comparison point.
    arguments.spool_root = database.path.parent
    arguments.spool_root.mkdir(parents=True, exist_ok=True)
    return build_store(arguments)


def _print(document: object) -> None:
    print(json.dumps(document, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
