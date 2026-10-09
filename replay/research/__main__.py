"""Offline CLI. Publication, orchestration and live HTTP serving are deliberately absent."""
import argparse
import os
import sys

from archive.storage.factory import build_store
from replay.research.build import audit, build
from replay.research.inputs import configuration
from replay.research.io import encoded
from replay.research.query import query
from replay.research.verify.runs import verify


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("verify", "build"):
        p = commands.add_parser(command)
        p.add_argument("--config", default="configs/research.json")
        p.add_argument("--runs", required=True)
        p.add_argument("--verification", required=True)
        if command == "build":
            p.add_argument("--output", required=True)
    p = commands.add_parser("audit")
    p.add_argument("directory")
    p = commands.add_parser("query")
    p.add_argument("directory")
    p.add_argument("table", choices=("events", "episodes"))
    p.add_argument("--event-id")
    p.add_argument("--lens")
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    if args.command == "audit":
        result = audit(args.directory)
    elif args.command == "query":
        result = query(args.directory, args.table, event_id=args.event_id, lens=args.lens, offset=args.offset, limit=args.limit)
    else:
        archive = configuration(args.config)["archive"]
        store = None
        if archive is not None:
            names = {"backend_env": "ARCHIVE_BACKEND", "root_env": "ARCHIVE_ROOT", "bucket_env": "ARCHIVE_GCS_BUCKET",
                     "durability_env": "ARCHIVE_DURABILITY", "store_id_env": "ARCHIVE_STORE_ID"}
            store = build_store([], environ={names[k]: os.environ.get(v, "") for k, v in archive.items()})
        result = (verify(args.config, args.runs, args.verification, store=store) if args.command == "verify"
                  else build(args.config, args.runs, args.verification, args.output, store=store))
    sys.stdout.buffer.write(encoded(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
