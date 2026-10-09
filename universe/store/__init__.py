"""Durable SQLite projection of committed Targeter v3 selection history.

``UniverseStore`` composes the connection, ingest-transaction and read mixins;
its public methods and this module's public names are unchanged.
"""

from __future__ import annotations

from universe.store.backup import Backup, SQLITE_CONTENT_TYPE, file_sha256
from universe.store.connection import Connection, REBUILD_INSTRUCTION, SCHEMA_PATH, SCHEMA_VERSION
from universe.store.ingest_tx import IngestTransactions
from universe.store.limits import (
    DETAIL_ROW_LIMIT,
    EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES,
    STALE_AFTER_SECONDS,
    TARGETER_RUN_INTERVAL_SECONDS,
)
from universe.store.reads.bundles import BundleReads
from universe.store.reads.claims import ClaimReads
from universe.store.reads.events import EventReads
from universe.store.reads.health import HealthReads
from universe.store.reads.markets import MarketReads
from universe.store.reads.runs import RunReads
from universe.store.records import (
    BundleEventConflict,
    DetailTooLarge,
    EvidenceConflict,
    _occurrence,
)


class UniverseStore(
    Connection,
    IngestTransactions,
    HealthReads,
    RunReads,
    EventReads,
    MarketReads,
    ClaimReads,
    BundleReads,
    Backup,
):
    """The Event Universe SQLite store."""


__all__ = [
    "BundleEventConflict",
    "DETAIL_ROW_LIMIT",
    "DetailTooLarge",
    "EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES",
    "EvidenceConflict",
    "REBUILD_INSTRUCTION",
    "SCHEMA_PATH",
    "SCHEMA_VERSION",
    "SQLITE_CONTENT_TYPE",
    "STALE_AFTER_SECONDS",
    "TARGETER_RUN_INTERVAL_SECONDS",
    "UniverseStore",
    "_occurrence",
    "file_sha256",
]
