"""python -m replay.research {verify,build} KEY_DIR [KEY_DIR ...] (docs/specs/RESEARCH_SITE_V1.md)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from replay.research import build as builder
from replay.research import config as research_config

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "research.json"


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m replay.research", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("verify", "build"):
        command = commands.add_parser(name)
        command.add_argument("key_dirs", nargs="+", type=Path,
                             help="results_root/<corpus key[:16]> directories; verified commits win")
        command.add_argument("--prefer-later", action="store_true",
                             help="allow multiple commits per bundle, choosing the last committed input")
        command.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    commands.choices["build"].add_argument("--out", type=Path, required=True)
    commands.choices["build"].add_argument("--game-state", type=Path, default=None,
                                           help="a local Part A game-state archive root (fallback source)")
    args = parser.parse_args(argv)
    args.key_dirs = [path.resolve() for path in args.key_dirs]
    config_bytes = args.config.read_bytes()
    config = research_config.load(args.config)
    if args.command == "verify":
        report = builder.verify_only(args.key_dirs, config, prefer_later=args.prefer_later)
        print(json.dumps(report, indent=1, sort_keys=True))
        return 0 if all(check["pass"] for check in report["checks"]) else 1
    version, failures = builder.build(args.key_dirs, args.out, config, config_bytes=config_bytes,
                                      game_state_root=args.game_state,
                                      prefer_later=args.prefer_later,
                                      log=lambda line: print(line, file=sys.stderr))
    print(json.dumps({"version": version, "out": str(args.out / version), "pack_failures": failures}, indent=1))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
