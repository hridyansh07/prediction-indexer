"""One-shot, externally scheduled retired-event pulls. Never executes Replay."""
import argparse
import fcntl
import math
import os
import re
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit

from archive.storage.factory import build_store
from gamestate import kalshi

# Retried with backoff until MAX_TRIES; every other outcome is final.
RETRY = frozenset({"incomplete", "fetch_failed", "unavailable"})
OUTCOMES = RETRY | {"complete", "complete_inconsistent", "timeline_failed",
                    "no_kalshi_events", "no_milestone", "multiple_milestones", "ticker_not_related"}
MAX_TRIES = 5

LEDGER_VERSION = 2
LEDGER_SCHEMA = """
    CREATE TABLE IF NOT EXISTS fetch_attempts(
        event_id TEXT, bundle_id TEXT NOT NULL,
        attempted_at_ns INTEGER NOT NULL, outcome TEXT NOT NULL,
        milestone_id TEXT, prefix TEXT, error TEXT);
    CREATE INDEX IF NOT EXISTS event_attempts ON fetch_attempts(event_id, attempted_at_ns);
    CREATE INDEX IF NOT EXISTS bundle_attempts ON fetch_attempts(bundle_id, attempted_at_ns);
    CREATE TABLE IF NOT EXISTS bundle_events(
        bundle_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, recorded_at_ns INTEGER NOT NULL);
    CREATE TRIGGER IF NOT EXISTS attempts_no_update BEFORE UPDATE ON fetch_attempts
        BEGIN SELECT RAISE(ABORT, 'append-only attempts'); END;
    CREATE TRIGGER IF NOT EXISTS attempts_no_delete BEFORE DELETE ON fetch_attempts
        BEGIN SELECT RAISE(ABORT, 'append-only attempts'); END;
    CREATE TRIGGER IF NOT EXISTS bundle_events_no_update BEFORE UPDATE ON bundle_events
        BEGIN SELECT RAISE(ABORT, 'append-only bundle events'); END;
    CREATE TRIGGER IF NOT EXISTS bundle_events_no_delete BEFORE DELETE ON bundle_events
        BEGIN SELECT RAISE(ABORT, 'append-only bundle events'); END;
    PRAGMA user_version=2;
"""


class Ledger:
    """Rebuildable operational state: attempt history and the immutable bundle → event map.

    A final outcome here is the skip authority, so finished bundles cost no network or
    archive call. Losing the ledger only costs one mapping pass; the pull itself still
    skips milestones the archive already holds complete.
    """

    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        try:
            version = self.connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, LEDGER_VERSION):
                raise ValueError("gamestate_ledger_version")
            def objects(connection):
                return {(kind, name): " ".join(sql.split()) for kind, name, sql in connection.execute(
                    "SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'")}
            actual = objects(self.connection)
            with closing(sqlite3.connect(":memory:")) as expected:
                expected.executescript(LEDGER_SCHEMA)
                if (version == 0 and actual) or (version == LEDGER_VERSION and actual != objects(expected)):
                    raise ValueError("gamestate_ledger_schema")
            if version == 0:
                self.connection.executescript("BEGIN IMMEDIATE;" + LEDGER_SCHEMA + "COMMIT;")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute("PRAGMA journal_mode=WAL")
        except BaseException:
            self.connection.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.connection.close()

    def event_for(self, bundle_id):
        row = self.connection.execute(
            "SELECT event_id FROM bundle_events WHERE bundle_id=?", (kalshi.identifier(bundle_id),)).fetchone()
        return None if row is None else row[0]

    def record_event(self, bundle_id, event_id, at):
        known = self.event_for(bundle_id)
        kalshi.event_identity(event_id)
        if known is not None:
            if known != event_id:
                raise ValueError("bundle_event_changed")
            return
        with self.connection:
            self.connection.execute("INSERT INTO bundle_events VALUES(?,?,?)", (bundle_id, event_id, at))

    def due(self, now_ns, *, event_id=None, bundle_id=None):
        """Event-keyed once the event is known; bundle-keyed for failures before that."""
        if event_id is not None:
            where, key = "event_id=?", kalshi.event_identity(event_id)
        else:
            where, key = "event_id IS NULL AND bundle_id=?", kalshi.identifier(bundle_id)
        last = self.connection.execute(
            f"SELECT attempted_at_ns,outcome FROM fetch_attempts WHERE {where} ORDER BY rowid DESC LIMIT 1",
            (key,)).fetchone()
        if last is None:
            return True
        if last[1] not in RETRY:
            return False
        attempts = self.connection.execute(
            f"SELECT COUNT(*) FROM fetch_attempts WHERE {where} AND outcome IN ({','.join('?' * len(RETRY))})",
            (key, *sorted(RETRY))).fetchone()[0]
        wait = min(24, 2 ** (attempts - 1)) * 3600 * 10**9
        return attempts < MAX_TRIES and now_ns >= last[0] + wait

    def append(self, event_id, bundle_id, at, outcome, milestone_id, prefix, error):
        if event_id is not None:
            kalshi.event_identity(event_id)
        kalshi.identifier(bundle_id)
        if outcome not in OUTCOMES or type(at) is not int or at < 0:
            raise ValueError("attempt_schema")
        with self.connection:
            self.connection.execute("INSERT INTO fetch_attempts VALUES(?,?,?,?,?,?,?)",
                                    (event_id, bundle_id, at, outcome, milestone_id, prefix, error))


def config(value):
    fields = {"version", "universe_base_url", "universe_spacing_seconds", "kalshi_spacing_seconds",
              "settle_delay_seconds", "max_bundles_per_run", "ledger_path", "archive"}
    if type(value) is not dict or set(value) != fields or type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("closed_gamestate_config")
    url = urlsplit(value["universe_base_url"])
    if url.scheme not in ("http", "https") or not url.netloc or url.username or url.password or url.query or url.fragment:
        raise ValueError("universe_base_url")
    for field in ("universe_spacing_seconds", "kalshi_spacing_seconds", "settle_delay_seconds"):
        number = value[field]
        if type(number) not in (int, float) or not math.isfinite(number) or not 0 <= number <= 86400:
            raise ValueError(field)
    maximum = value["max_bundles_per_run"]
    if type(maximum) is not int or not 1 <= maximum <= 1000 or type(value["ledger_path"]) is not str:
        raise ValueError("gamestate_limits")
    archive = value["archive"]
    if type(archive) is not dict or set(archive) != {"backend_env", "root_env", "bucket_env", "durability_env", "store_id_env"}:
        raise ValueError("closed_archive_config")
    if any(type(name) is not str or not re.fullmatch(r"[A-Z][A-Z0-9_]*", name) for name in archive.values()):
        raise ValueError("archive_env_names")
    return value


def eligible(history, now_ns, delay):
    retired = {kalshi.timestamp(row["retirement"]["retired_at"])
               for row in history if row.get("retirement") is not None}
    if not retired:
        raise ValueError("retirement_missing")
    return now_ns >= min(retired) + int(delay * 10**9)


def bundles(client, base):
    """Stream every bundle; only cursors are retained, to reject a cursor loop."""
    cursor, seen = None, set()
    while True:
        query = {"limit": 50, **({"cursor": cursor} if cursor else {})}
        page = client.get(base.rstrip("/") + "/v1/bundles?" + urlencode(query))
        if set(page) != {"bundles", "next_cursor"} or type(page["bundles"]) is not list or len(page["bundles"]) > 50:
            raise ValueError("bundles_page")
        for row in page["bundles"]:
            if type(row) is not dict or row.get("lifecycle") not in ("active", "retired"):
                raise ValueError("bundles_shape")
            yield row
        cursor = page["next_cursor"]
        if cursor is None:
            return
        if type(cursor) is not str or not 0 < len(cursor) <= 4096 or cursor in seen:
            raise ValueError("bundles_cursor")
        seen.add(cursor)


def resolve_event(universe, base, bundle, now_ns, delay):
    """``None`` until the settle delay has elapsed; then the bundle's immutable event id."""
    path = f"/v1/bundles/{quote(bundle, safe='')}"
    if not eligible(kalshi.pages(universe, base, path + "/history"), now_ns, delay):
        return None
    outcomes = universe.get(base.rstrip("/") + path + "/outcomes")
    if outcomes.get("version") != 1 or outcomes.get("bundle_id") != bundle:
        raise ValueError("outcomes_identity")
    return kalshi.event_identity(outcomes.get("event_id"))


def pull_outcome(report):
    """Ledger outcome, milestone, prefix and error for one single-bundle pull report."""
    mapped = report["bundles"][0]
    milestone = mapped["milestone_id"]
    if mapped["reason"] is not None:
        return mapped["reason"], milestone, None, None
    if report["milestones_skipped"]:
        return "complete", milestone, None, None  # already archived complete
    fetch = report["fetches"][0] if report["fetches"] else None
    if report["failures"]:
        reason = report["failures"][0]["reason"]
        if reason == "timeline_failed" and fetch is not None:
            return "timeline_failed", milestone, fetch["prefix"], reason
        return "fetch_failed", milestone, None, reason
    if fetch["status"] == "incomplete":
        return "incomplete", milestone, fetch["prefix"], fetch["errors"][0]["reason"] if fetch["errors"] else None
    return ("complete_inconsistent" if fetch["disqualified"] else "complete"), milestone, fetch["prefix"], None


def execute(document, store, ledger, client_factory, *, now_ns, activation=None, pull=kalshi.run):
    """One pass over every retired bundle. A bundle that fails is recorded and the pass continues."""
    document = config(document)
    base = document["universe_base_url"]
    result = {"version": 1, "eligible": 0, "attempted": 0, "failed": 0}
    universe = client_factory(records=kalshi.Discard())
    try:
        for row in bundles(universe, base):
            if row["lifecycle"] != "retired":
                continue
            if activation is not None and not activation[0] <= kalshi.timestamp(row["activation_at"]) < activation[1]:
                continue
            try:
                bundle = kalshi.identifier(row["bundle_id"])
            except ValueError:
                result["failed"] += 1  # not ledger-addressable; visible in the exit status
                continue
            event = ledger.event_for(bundle)
            if not ledger.due(now_ns, event_id=event, bundle_id=bundle):
                continue
            outcome = milestone = prefix = error = None
            try:
                if event is None:
                    resolved = resolve_event(universe, base, bundle, now_ns, document["settle_delay_seconds"])
                    if resolved is None:
                        continue
                    ledger.record_event(bundle, resolved, now_ns)
                    event = resolved
                    if not ledger.due(now_ns, event_id=event):
                        continue  # another bundle of this event already settled it
                result["eligible"] += 1
                fetch = client_factory()
                try:
                    report = pull(fetch, store, base, [bundle], event_id=event, universe=universe)
                finally:
                    fetch.close()
                outcome, milestone, prefix, error = pull_outcome(report)
            except (ValueError, KeyError, TypeError, OSError, kalshi.ObjectStoreError) as failure:
                outcome, error = "unavailable", (str(failure) or type(failure).__name__)[:512]
            ledger.append(event, bundle, now_ns, outcome, milestone, prefix, error)
            result["attempted"] += 1
            result["failed"] += int(outcome in RETRY or outcome == "timeline_failed")
            if activation is None and result["attempted"] >= document["max_bundles_per_run"]:
                break
    finally:
        universe.close()
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", nargs="?", default="configs/gamestate.json", type=Path)
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--activation-start")
    parser.add_argument("--activation-end")
    args = parser.parse_args(argv)
    if args.backfill != bool(args.activation_start and args.activation_end) or bool(args.activation_start) != bool(args.activation_end):
        parser.error("backfill requires both activation bounds")
    document = config(kalshi.loads(args.config.read_bytes()))
    activation = None if not args.backfill else (kalshi.timestamp(args.activation_start), kalshi.timestamp(args.activation_end))
    if activation is not None and activation[0] >= activation[1]:
        parser.error("activation-start must precede activation-end")
    keys = {"backend_env": "ARCHIVE_BACKEND", "root_env": "ARCHIVE_ROOT", "bucket_env": "ARCHIVE_GCS_BUCKET",
            "durability_env": "ARCHIVE_DURABILITY", "store_id_env": "ARCHIVE_STORE_ID"}
    environment = {target: os.environ.get(document["archive"][source], "") for source, target in keys.items()}
    environment["ARCHIVE_BACKEND"] = environment["ARCHIVE_BACKEND"] or "local"
    environment["ARCHIVE_DURABILITY"] = environment["ARCHIVE_DURABILITY"] or "conformance"
    path = Path(document["ledger_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with Ledger(path) as ledger:
            store = build_store([path.parent], environ=environment)
            factory = lambda records=None: kalshi.Client(universe_spacing=document["universe_spacing_seconds"],
                                                         kalshi_spacing=document["kalshi_spacing_seconds"],
                                                         records=records)
            result = execute(document, store, ledger, factory, now_ns=time.time_ns(), activation=activation)
    print(kalshi.dumps(result).decode(), end="")
    return int(result["failed"] > 0)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, kalshi.ObjectStoreError):
        print("game-state scheduled job failed; retained evidence unchanged", file=sys.stderr)
        raise SystemExit(1)
