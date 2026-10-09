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

OUTCOMES = {"complete", "incomplete", "fetch_failed", "timeline_failed",
            "no_kalshi_events", "no_milestone", "multiple_milestones", "ticker_not_related"}

LEDGER_SCHEMA = """
    CREATE TABLE IF NOT EXISTS fetch_attempts(
        event_id TEXT NOT NULL, bundle_id TEXT NOT NULL,
        attempted_at_ns INTEGER NOT NULL, outcome TEXT NOT NULL,
        milestone_id TEXT, prefix TEXT, error TEXT);
    CREATE INDEX IF NOT EXISTS event_attempts ON fetch_attempts(event_id, attempted_at_ns);
    CREATE TRIGGER IF NOT EXISTS attempts_no_update BEFORE UPDATE ON fetch_attempts
        BEGIN SELECT RAISE(ABORT, 'append-only attempts'); END;
    CREATE TRIGGER IF NOT EXISTS attempts_no_delete BEFORE DELETE ON fetch_attempts
        BEGIN SELECT RAISE(ABORT, 'append-only attempts'); END;
    PRAGMA user_version=1;
"""


class Ledger:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        try:
            version = self.connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError("gamestate_ledger_version")
            def objects(connection):
                return {(kind, name): " ".join(sql.split()) for kind, name, sql in connection.execute(
                    "SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'")}
            actual = objects(self.connection)
            with closing(sqlite3.connect(":memory:")) as expected:
                expected.executescript(LEDGER_SCHEMA)
                if (version == 0 and actual) or (version == 1 and actual != objects(expected)):
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

    def due(self, event_id, now_ns):
        kalshi.event_identity(event_id)
        last = self.connection.execute(
            "SELECT attempted_at_ns,outcome FROM fetch_attempts WHERE event_id=? ORDER BY rowid DESC LIMIT 1",
            (event_id,)).fetchone()
        if last is None or last[1] in ("complete", "timeline_failed"):
            # The archive, never this rebuildable ledger, is skip authority.
            return True
        if last[1] not in ("incomplete", "fetch_failed"):
            return False
        attempts = self.connection.execute(
            "SELECT COUNT(*) FROM fetch_attempts WHERE event_id=? AND outcome IN ('incomplete','fetch_failed')",
            (event_id,)).fetchone()[0]
        wait = min(24, 2 ** (attempts - 1)) * 3600 * 10**9
        return attempts < 5 and now_ns >= last[0] + wait

    def append(self, event_id, bundle_id, at, outcome, milestone_id, prefix, error):
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
    cursor, seen, count = None, set(), 0
    for _ in range(2000):
        query = {"limit": 50, **({"cursor": cursor} if cursor else {})}
        page = client.get(base.rstrip("/") + "/v1/bundles?" + urlencode(query))
        if set(page) != {"bundles", "next_cursor"} or type(page["bundles"]) is not list or len(page["bundles"]) > 50:
            raise ValueError("bundles_page")
        for row in page["bundles"]:
            count += 1
            if count > 100000 or type(row) is not dict or row.get("lifecycle") not in ("active", "retired"):
                raise ValueError("bundles_bound_or_shape")
            yield row
        cursor = page["next_cursor"]
        if cursor is None:
            return
        if type(cursor) is not str or not 0 < len(cursor) <= 4096 or cursor in seen:
            raise ValueError("bundles_cursor")
        seen.add(cursor)
    raise ValueError("bundles_page_limit")


def execute(document, store, ledger, client_factory, *, now_ns, activation=None, pull=kalshi.run):
    document = config(document)
    base, attempted, failed, considered = document["universe_base_url"], 0, 0, 0
    seen_events = set()
    client = client_factory()
    try:
        for row in bundles(client, base):
            if row["lifecycle"] != "retired":
                continue
            if activation is not None and not activation[0] <= kalshi.timestamp(row["activation_at"]) < activation[1]:
                continue
            bundle = kalshi.identifier(row["bundle_id"])
            history = list(kalshi.pages(client, base, f"/v1/bundles/{quote(bundle, safe='')}/history"))
            if not eligible(history, now_ns, document["settle_delay_seconds"]):
                continue
            considered += 1
            outcomes = client.get(base.rstrip("/") + f"/v1/bundles/{quote(bundle, safe='')}/outcomes")
            if outcomes.get("version") != 1 or outcomes.get("bundle_id") != bundle:
                raise ValueError("outcomes_identity")
            event = kalshi.event_identity(outcomes.get("event_id"))
            if event in seen_events or not ledger.due(event, now_ns):
                continue
            seen_events.add(event)
            from gamestate.timeline import latest
            prior = latest(store, event, require_timeline=False)
            if prior["state"] == "ok":
                continue
            fetch_client = client_factory()
            try:
                report = pull(fetch_client, store, base, [bundle], event_id=event)
                mapped = report["bundles"][0]
                fetch = next(iter(report["fetches"]), None)
                outcome = mapped["reason"] or (fetch["status"] if fetch else "complete")
                if report["failures"]:
                    outcome = "timeline_failed" if fetch and fetch["status"] == "complete" else "fetch_failed"
                ledger.append(event, bundle, now_ns, outcome, mapped["milestone_id"],
                              fetch["prefix"] if fetch else None,
                              report["failures"][0]["reason"] if report["failures"] else None)
                failed += int(outcome in ("incomplete", "fetch_failed", "timeline_failed"))
                attempted += 1
            finally:
                fetch_client.close()
            if activation is None and attempted >= document["max_bundles_per_run"]:
                break
    finally:
        client.close()
    return {"version": 1, "eligible": considered, "attempted": attempted, "failed": failed}


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
            factory = lambda: kalshi.Client(universe_spacing=document["universe_spacing_seconds"],
                                           kalshi_spacing=document["kalshi_spacing_seconds"])
            result = execute(document, store, ledger, factory, now_ns=time.time_ns(), activation=activation)
    print(kalshi.dumps(result).decode(), end="")
    return int(result["failed"] > 0)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, kalshi.ObjectStoreError):
        print("game-state scheduled job failed; retained evidence unchanged", file=sys.stderr)
        raise SystemExit(1)
