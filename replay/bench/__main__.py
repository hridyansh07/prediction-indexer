"""Agent-agnostic local replay bench command surface."""

import argparse
import json
from pathlib import Path
import sys

from replay.streams.protocol import ProtocolError
from .compare import compare_contexts, compare_runs
from .docker import DockerError, run_bench
from .fees import build_fees
from .prepare import PreparationEnvironmentError, prepare_context
from .common import read_json


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prepare = commands.add_parser('prepare')
    prepare.add_argument('config', type=Path); prepare.add_argument('out_dir', type=Path)
    prepare.add_argument('--warm-timeout-s', type=float, default=120)
    prepare.add_argument('--allow-outcomes-unavailable', action='store_true')
    game_state = commands.add_parser('prepare-game-state')
    game_state.add_argument('context_dir', type=Path); game_state.add_argument('out_dir', type=Path)
    context = commands.add_parser('compare-context')
    context.add_argument('a', type=Path); context.add_argument('b', type=Path)
    context.add_argument('--expect-only', default='')
    fees = commands.add_parser('fees')
    fees.add_argument('spec', type=Path); fees.add_argument('out_dir', type=Path)
    run = commands.add_parser('run')
    run.add_argument('spec', type=Path); run.add_argument('out_dir', type=Path)
    run.add_argument('--keep-image', action='store_true'); run.add_argument('--prune-build-cache', action='store_true')
    run.add_argument('--label', default='bench')
    compare = commands.add_parser('compare-runs')
    compare.add_argument('a', type=Path); compare.add_argument('b', type=Path)
    compare.add_argument('--expect', type=Path)
    try:
        args = parser.parse_args(argv)
    except SystemExit as error:
        return 0 if error.code == 0 else 1
    try:
        if args.command == 'prepare-game-state':
            from archive.storage.factory import build_store
            from replay.prepare_game_state import prepare_game_state
            pin = prepare_game_state(args.context_dir, args.out_dir, store=build_store(primary_roots=[args.context_dir]))
            report, code = {'game_state_sha256': pin}, 0
        elif args.command == 'prepare':
            code, report = prepare_context(args.config, args.out_dir, warm_timeout_s=args.warm_timeout_s,
                                           allow_outcomes_unavailable=args.allow_outcomes_unavailable)
        elif args.command == 'fees':
            report, code = build_fees(args.spec, args.out_dir), 0
        elif args.command == 'compare-context':
            paths = args.expect_only.split(',') if args.expect_only else ()
            code, report = compare_contexts(args.a, args.b, expect_only=paths)
        elif args.command == 'compare-runs':
            code, report = compare_runs(args.a, args.b, expect=read_json(args.expect) if args.expect else None)
        else:
            return run_bench(args.spec, args.out_dir, keep_image=args.keep_image,
                             prune_build_cache=args.prune_build_cache, label=args.label)
        print(json.dumps(report, sort_keys=True, indent=2, allow_nan=False))
        return code
    except (PreparationEnvironmentError, DockerError) as error:
        print(str(error), file=sys.stderr)
        return 3
    except (ValueError, TypeError, KeyError, OSError, ProtocolError) as error:
        print('invalid bench input or refused operation: ' + str(error), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 3


if __name__ == '__main__':
    raise SystemExit(main())
