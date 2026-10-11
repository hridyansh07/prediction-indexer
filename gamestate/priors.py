"""Read-only archive statistics; exact quantiles use temporary disk, not a corpus buffer."""

import argparse
from contextlib import closing
from decimal import Decimal
import os
from pathlib import Path
import re
import sqlite3
import tempfile

from gamestate.timeline import read_fetch
from archive.common.durable import write_json_durable

GAMES = {"cs2": "counter_strike_2", "lol": "league_of_legends", "dota2": "dota_2"}
METRICS = ("segment_duration_ns", "between_segments_ns", "settlement_delay_ns")


def statistics(store):
    """Stream every verified ok fetch; retain only one timeline at a time."""
    with tempfile.TemporaryDirectory(prefix="game-priors-") as temporary:
        with closing(sqlite3.connect(str(Path(temporary) / "samples.sqlite3"))) as db:
            db.execute("CREATE TABLE samples (game TEXT, metric TEXT, ns TEXT)")
            counts = {"ok": 0, "incomplete": 0}
            for key in store.list_keys("gamestate/source=kalshi/"):
                if not key.endswith("/receipt.json"):
                    continue
                selected = read_fetch(store, key.removesuffix("/receipt.json"))
                counts[selected["state"]] += 1
                if selected["state"] != "ok":
                    continue
                timeline = selected["timeline"]
                vendor_game = timeline["game"]["value"]
                game = GAMES.get(vendor_game, vendor_game)
                if not isinstance(game, str) or not game:
                    raise ValueError("priors game missing")

                def add(metric, value):
                    if value is not None:
                        if type(value) is not int or value < 0:
                            raise ValueError("priors invalid interval")
                        db.execute("INSERT INTO samples VALUES (?, ?, ?)", (game, metric, str(value)))

                previous_end = None
                for segment in timeline["maps"]:
                    start, end, settled = (segment[k] for k in ("derived_start_ns", "close_ns", "settlement_ns"))
                    if start is not None and end is not None:
                        add(METRICS[0], end - start)
                    if start is not None and previous_end is not None:
                        add(METRICS[1], start - previous_end)
                    if end is not None and settled is not None:
                        add(METRICS[2], settled - end)
                    previous_end = end
            db.execute("CREATE INDEX samples_order ON samples(game, metric, length(ns), ns)")
            result = {}
            for game, in db.execute("SELECT DISTINCT game FROM samples ORDER BY game"):
                report = result[game] = {}
                for metric in METRICS:
                    count = db.execute("SELECT count(*) FROM samples WHERE game=? AND metric=?", (game, metric)).fetchone()[0]

                    def at(index):
                        return int(db.execute("SELECT ns FROM samples WHERE game=? AND metric=? ORDER BY length(ns), ns LIMIT 1 OFFSET ?",
                                              (game, metric, index)).fetchone()[0])

                    median = None if not count else format(Decimal(at((count - 1) // 2) + at(count // 2)) / 2, "f")
                    report[metric] = {"count": count, "median": median,
                                      "p10": str(at(max(0, (count + 9) // 10 - 1))) if count else None,
                                      "p90": str(at(max(0, (9 * count + 9) // 10 - 1))) if count else None}
            return {"version": 1, "unit": "ns", "quantiles": "nearest_rank", "fetches": counts, "games": result}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-env", default="ARCHIVE", help="prefix of exported archive variable names; no dotenv is read")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", args.archive_env) is None:
        parser.error("archive environment prefix must name exported variables")
    if args.output.exists() or args.output.is_symlink():
        parser.error("priors output already exists")
    from archive.storage.factory import build_store
    names = ("BACKEND", "ROOT", "GCS_BUCKET", "DURABILITY", "STORE_ID")
    environ = {"ARCHIVE_" + suffix: os.environ[args.archive_env + "_" + suffix]
               for suffix in names if args.archive_env + "_" + suffix in os.environ}
    report = statistics(build_store(primary_roots=[], environ=environ))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json_durable(args.output, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
