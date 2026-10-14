"""``python -m universe {serve,sync,backfill,backup}``: configured commands, no options."""

import sys

from universe import commands

COMMANDS = {
    "serve": commands.serve,
    "sync": commands.sync,
    "backfill": commands.backfill,
    "backup": commands.backup,
}


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "migrate":
        from universe.ingest.migrations import main as migrate
        return migrate(argv[1:])
    if len(argv) != 1 or argv[0] not in COMMANDS:
        print("usage: python -m universe {" + ",".join(COMMANDS) + ",migrate}", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]]() or 0


if __name__ == "__main__":
    raise SystemExit(main())
