"""Closed V1 run and resolved-spec validation. Strategy configs remain opaque."""

from copy import deepcopy
from pathlib import Path, PurePosixPath
import re

from replay.preparation import load_snapshot
from replay.fees.artifacts import load_catalog
from replay.streams.protocol import obj, require
from replay.strategy_sdk import plain
from .common import import_name, name, path, positive, read_json, sha

LIMITS = {'attempts', 'no_progress', 'progress_margin', 'stall_seconds',
          'attempt_seconds', 'run_seconds', 'poll_seconds', 'stop_seconds', 'state_bytes'}
TOKEN = re.compile(r'\{([^{}]*)\}')
TOKENS = {'context', 'snapshot_sha256', 'fees', 'catalog_identity', 'output'}
FIELDS = 'version image redis base_run_config context_directory fee_catalog_directory mounts limits groups'


def substitute(value, replacements):
    if type(value) is dict:
        return {key: substitute(item, replacements) for key, item in value.items()}
    if type(value) is list:
        return [substitute(item, replacements) for item in value]
    if type(value) is str:
        def replace(match):
            token = match[1]
            require(token in TOKENS, 'unknown config token')
            require(replacements[token] is not None, 'fee token requires a catalog')
            return replacements[token]
        return TOKEN.sub(replace, value)
    return value


def validate_run_spec(spec, *, check_paths=True):
    obj(spec, FIELDS)
    require(type(spec['version']) is int and spec['version'] == 1, 'bench spec version')
    image = obj(spec['image'], 'build_context dockerfile reuse_id')
    path(image['build_context'], directory=True, exists=check_paths)
    dockerfile = image['dockerfile']
    require(type(dockerfile) is str and bool(dockerfile) and not PurePosixPath(dockerfile).is_absolute()
            and '..' not in PurePosixPath(dockerfile).parts, 'dockerfile must be relative to build context')
    if check_paths:
        path(str(Path(image['build_context']) / dockerfile), directory=False)
    require(image['reuse_id'] is None or (type(image['reuse_id']) is str
            and re.fullmatch(r'sha256:[0-9a-f]{64}', image['reuse_id']) is not None), 'reuse_id must pin an image ID')
    redis = obj(spec['redis'], 'image maxmemory')
    require(type(redis['image']) is str and re.fullmatch(r'redis:(?:8\.[2-9]|8\.[1-9][0-9]+|(?:9|[1-9][0-9]+)\.[0-9]+)(?:\.[0-9]+)?(?:-[A-Za-z0-9_.-]+)?', redis['image']) is not None,
            'Redis image must declare version >= 8.2')
    require(type(redis['maxmemory']) is str and re.fullmatch(r'[1-9][0-9]*(?:[kKmMgG][bB]?)?', redis['maxmemory']) is not None, 'positive Redis maxmemory required')
    path(spec['base_run_config'], directory=False, exists=check_paths)
    path(spec['context_directory'], directory=True, exists=check_paths)
    if spec['fee_catalog_directory'] is not None:
        path(spec['fee_catalog_directory'], directory=True, exists=check_paths)
    require(type(spec['mounts']) is list, 'mounts array required')
    destinations = []
    for mount in spec['mounts']:
        obj(mount, 'host container')
        path(mount['host'], exists=check_paths)
        dest = path(mount['container'], exists=False)
        require(str(dest) == mount['container'] and '..' not in dest.parts, 'normalized container path required')
        # The /bench output, fixed context/fees and bench evidence artifacts
        # cannot be shadowed. User mounts must also be disjoint from one another.
        for reserved in ('/bench/context', '/bench/fees', '/bench/run', '/bench/groups',
                         '/bench/spec.resolved.json', '/bench/input.json', '/bench/result.json',
                         '/bench/logs', '/bench/orchestration.json', '/bench/image.id'):
            base = Path(reserved)
            require(dest != base and not dest.is_relative_to(base) and not base.is_relative_to(dest), 'mount shadows a reserved container path')
        require(all(not dest.is_relative_to(other) and not other.is_relative_to(dest) for other in destinations), 'overlapping mounts')
        destinations.append(dest)
    require(type(spec['limits']) is dict and not set(spec['limits']) - LIMITS, 'closed limits overrides')
    for key, value in spec['limits'].items():
        positive(value)
        if key in {'attempts', 'no_progress', 'progress_margin', 'state_bytes'}:
            require(type(value) is int, 'integer limit required')
        if key == 'state_bytes':
            require(value <= 1024**3, 'state_bytes bound')
    require(type(spec['groups']) is list and 0 < len(spec['groups']) <= 128, 'groups required')
    names = []
    for group in spec['groups']:
        obj(group, 'name factory revision config reader checks compare')
        names.append(name(group['name']))
        import_name(group['factory']); import_name(group['reader'])
        require(type(group['revision']) is str and bool(group['revision']), 'strategy revision required')
        require(type(group['config']) is dict, 'group config object required')
        # Validate all recursive tokens before any output or Docker operation.
        substitute(group['config'], {token: '' for token in TOKENS})
        require(type(group['checks']) is list and len(group['checks']) <= 128, 'checks array required')
        for check in group['checks']:
            import_name(check)
        require(len(set(group['checks'])) == len(group['checks']), 'duplicate check')
        compare = obj(group['compare'], 'rows key')
        require(type(compare['rows']) is str and bool(compare['rows']), 'comparison rows path required')
        require(type(compare['key']) is list and bool(compare['key']) and len(set(compare['key'])) == len(compare['key'])
                and all(type(k) is str and bool(k) for k in compare['key']), 'comparison row key fields required')
    require(len(set(names)) == len(names), 'duplicate group names')
    return spec


def base_config(value):
    obj(value, 'version publisher python transport strategies limits')
    require(type(value['version']) is int and value['version'] == 1, 'base run version')
    require(type(value['transport']) is dict and type(value['strategies']) is dict, 'base run objects required')
    obj(value['limits'], ' '.join(sorted(LIMITS if 'state_bytes' in value['limits'] else LIMITS - {'state_bytes'})))
    for key, val in value['limits'].items():
        positive(val)
        if key in {'attempts', 'no_progress', 'progress_margin', 'state_bytes'}:
            require(type(val) is int, 'integer limit required')
        if key == 'state_bytes':
            require(val <= 1024**3, 'state_bytes bound')
    return value


def resolve_spec(spec, *, label, run_id, image):
    validate_run_spec(spec)
    snapshot_sha256 = sha(read_json(Path(spec['context_directory']) / 'receipt.json')['snapshot_sha256'])
    snapshot = load_snapshot(spec['context_directory'], expected_sha256=snapshot_sha256)
    catalog = load_catalog(spec['fee_catalog_directory']) if spec['fee_catalog_directory'] is not None else None
    result = deepcopy(spec)
    for group in result['groups']:
        group['config'] = substitute(group['config'], {
            'context': '/bench/context', 'snapshot_sha256': snapshot_sha256,
            'fees': '/bench/fees' if catalog else None,
            'catalog_identity': catalog.identity if catalog else None,
            'output': '/bench/groups/' + group['name'] + '/work',
        })
    result['runtime'] = {
        'label': label, 'run_id': run_id, 'image': image,
        'context': {'snapshot_sha256': snapshot_sha256,
                    'outcomes_provider': snapshot.get('outcomes', {}).get('provider')},
        'fee_catalog_identity': catalog.identity if catalog else None,
        'base_run_config': plain(base_config(read_json(spec['base_run_config']))),
    }
    return result


def validate_resolved(spec):
    obj(spec, FIELDS + ' runtime')
    validate_run_spec({k: v for k, v in spec.items() if k != 'runtime'}, check_paths=False)
    runtime = obj(spec['runtime'], 'label run_id image context fee_catalog_identity base_run_config')
    name(runtime['label']); name(runtime['run_id'])
    validate_image(runtime['image'])
    context = obj(runtime['context'], 'snapshot_sha256 outcomes_provider')
    sha(context['snapshot_sha256'])
    require(context['outcomes_provider'] in (None, 'universe'), 'outcomes provider')
    if runtime['fee_catalog_identity'] is not None:
        sha(runtime['fee_catalog_identity'])
    base_config(runtime['base_run_config'])
    return spec


def validate_image(image, *, allow_unresolved=False):
    obj(image, 'id built source_commit source_dirty dockerfile')
    require((allow_unresolved and image['id'] is None) or (type(image['id']) is str and re.fullmatch(r'sha256:[0-9a-f]{64}', image['id']) is not None), 'image ID required')
    require(type(image['built']) is bool and type(image['source_dirty']) is bool, 'image flags')
    require(type(image['source_commit']) is str and re.fullmatch(r'[0-9a-f]{40,64}', image['source_commit']) is not None, 'source commit')
    require(type(image['dockerfile']) is str and bool(image['dockerfile']), 'image dockerfile')
    return image


def supervisor_config(spec):
    validate_resolved(spec)
    config = deepcopy(spec['runtime']['base_run_config'])
    config['strategies'] = {g['name']: {k: g[k] for k in ('factory', 'revision', 'config')} for g in spec['groups']}
    config['transport']['groups'] = [g['name'] for g in spec['groups']]
    config['transport']['run_id'] = spec['runtime']['run_id']
    config['limits'].update(spec['limits'])
    config['limits']['attempts'] = 1
    return config
