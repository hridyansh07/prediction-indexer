"""Private container entry: imports, one supervisor attempt, readers and checks."""

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import importlib
from pathlib import Path
import sys
import time

from replay.preparation import load_snapshot
from replay.fees.artifacts import load_catalog
from replay.streams.protocol import obj, require
from replay.strategy_sdk import plain
from replay import supervisor
from .common import closed_check, error_document, read_json, sha, write_json
from .specs import supervisor_config, validate_image, validate_resolved


def import_callable(reference):
    module, attribute = reference.split(':')
    function = getattr(importlib.import_module(module), attribute)
    require(callable(function), 'import must resolve to a callable')
    return function


def validate_result(value):
    obj(value, 'version label status started_at run_seconds image context fee_catalog_identity groups error')
    require(type(value['version']) is int and value['version'] == 1, 'result version')
    require(value['status'] in {'SUCCESS', 'CHECKS_FAILED', 'FAILED'}, 'result status')
    require(type(value['label']) is str and bool(value['label']), 'result label')
    require(type(value['started_at']) is str and datetime.fromisoformat(value['started_at']).tzinfo is not None, 'result timestamp')
    require(type(value['run_seconds']) in (int, float) and value['run_seconds'] >= 0, 'result duration')
    validate_image(value['image'], allow_unresolved=value['status'] == 'FAILED')
    context = obj(value['context'], 'snapshot_sha256 outcomes_provider')
    sha(context['snapshot_sha256'])
    require(context['outcomes_provider'] in (None, 'universe'), 'context provider')
    if value['fee_catalog_identity'] is not None:
        sha(value['fee_catalog_identity'])
    require(type(value['groups']) is list, 'result groups')
    names = []
    for group in value['groups']:
        obj(group, 'name factory receipt checks')
        names.append(group['name'])
        require(type(group['receipt']) is dict or group['receipt'] is None, 'reader receipt')
        require(type(group['checks']) is dict and all(type(v) is bool for v in group['checks'].values()), 'check flags')
    require(len(names) == len(set(names)), 'duplicate result group')
    if value['error'] is not None:
        obj(value['error'], 'type message trace_tail')
        require(all(type(v) is str for v in value['error'].values()), 'result error strings')
    return value


def initial_result(spec):
    runtime = spec['runtime']
    return {'version': 1, 'label': runtime['label'], 'status': 'FAILED',
            'started_at': datetime.now(timezone.utc).isoformat(), 'run_seconds': 0,
            'image': runtime['image'], 'context': runtime['context'],
            'fee_catalog_identity': runtime['fee_catalog_identity'],
            'groups': [{'name': g['name'], 'factory': g['factory'], 'receipt': None, 'checks': {}} for g in spec['groups']],
            'error': None}


def execute(spec, output=Path('/bench'), *, redis_url, supervisor_run=supervisor.run,
            importer=import_callable, snapshot_loader=load_snapshot, catalog_loader=load_catalog):
    """Injection seams keep offline tests independent of Redis and publishers."""
    validate_resolved(spec)
    root = Path(output)
    require(root.is_dir() and not (root / 'input.json').exists() and not (root / 'result.json').exists(), 'inside output already used')
    result, start, code = initial_result(spec), time.monotonic(), 3
    readers, checks = {}, {}
    try:
        # Import all references BEFORE the supervisor starts. An import failure
        # is invalid input (exit 1), even though a failure result is preserved.
        try:
            for group in spec['groups']:
                importer(group['factory'])
                readers[group['name']] = importer(group['reader'])
                checks[group['name']] = [(reference, importer(reference)) for reference in group['checks']]
        except Exception:
            code = 1
            raise
        snapshot = snapshot_loader(root / 'context', expected_sha256=spec['runtime']['context']['snapshot_sha256'])
        require(snapshot.get('outcomes', {}).get('provider') == spec['runtime']['context']['outcomes_provider'], 'outcomes provider pin mismatch')
        if spec['runtime']['fee_catalog_identity'] is not None:
            require(catalog_loader(root / 'fees' / spec['runtime']['fee_catalog_identity']).identity == spec['runtime']['fee_catalog_identity'], 'catalog pin mismatch')
        config = supervisor_config(spec)
        for group in spec['groups']:
            (root / 'groups' / group['name'] / 'work').mkdir(parents=True)
        write_json(root / 'input.json', config)
        completed = supervisor_run(config, root / 'run', redis_url)
        all_passed = True
        for group, entry in zip(spec['groups'], result['groups']):
            directory = root / 'groups' / group['name']
            returned = plain(readers[group['name']](root / 'run', group['name']))
            require(type(returned) is dict and type(returned.get('receipt')) is dict, 'reader must return a receipt')
            entry['receipt'] = returned['receipt']
            write_json(directory / 'receipt.json', returned['receipt'])
            if 'summary' in returned:
                write_json(directory / 'summary.json', returned['summary'])
            for reference, check in checks[group['name']]:
                try:
                    checked = closed_check(check(run_directory=root / 'run', group=group['name'],
                                                 output_directory=root / 'run' / completed['outputs'][group['name']], context_directory=root / 'context'))
                except Exception as error:
                    checked = {'passed': False, 'details': {'error': {'type': type(error).__name__, 'message': str(error)[:2048]}}}
                # Full import names avoid collisions between same-named functions
                # from different strategy-owned modules.
                write_json(directory / 'checks' / (reference.replace(':', '.') + '.json'), checked)
                entry['checks'][reference] = checked['passed']
                all_passed = all_passed and checked['passed']
        result['status'] = 'SUCCESS' if all_passed else 'CHECKS_FAILED'
        code = 0 if all_passed else 2
    except (Exception, KeyboardInterrupt) as error:
        result['status'], result['error'] = 'FAILED', error_document(error)
    finally:
        result['run_seconds'] = max(0, time.monotonic() - start)
        write_json(root / 'result.json', validate_result(result))
    return code, result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('resolved_spec', type=Path)
    try:
        args = parser.parse_args(argv)
    except SystemExit as error:
        return 0 if error.code == 0 else 1
    try:
        spec = read_json(args.resolved_spec)
        code, _ = execute(spec, args.resolved_spec.parent, redis_url='redis://redis:6379/0')
        return code
    except Exception as error:
        print('invalid bench input: ' + type(error).__name__, file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
