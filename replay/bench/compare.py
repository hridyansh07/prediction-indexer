"""Read-only context and strategy-declared summary comparisons."""

from collections import Counter
from pathlib import Path

from replay.preparation import load_snapshot
from replay.strategy_sdk import plain
from replay.streams.protocol import obj, require
from .common import closed_check, identity, read_json, sha
from .inside import validate_result
from .specs import validate_resolved


def pinned_context(directory):
    directory = Path(directory)
    receipt = read_json(directory / 'receipt.json')
    return plain(load_snapshot(directory, expected_sha256=receipt['snapshot_sha256']))


def compare_contexts(a, b, *, expect_only=(), loader=pinned_context):
    left, right = loader(a), loader(b)
    fields = sorted(set(left) | set(right))
    allowed = set(expect_only)
    require(not allowed - (set(fields) | {'scopes.outcome_books'}), 'unknown expected context path')
    differences, top = [], {}
    for key in fields:
        top[key] = {'equal': left.get(key) == right.get(key)}
        if key != 'scopes' and not top[key]['equal']:
            differences.append(key)
    ls, rs = left.get('scopes', []), right.get('scopes', [])
    strip = lambda scopes: [{k: v for k, v in s.items() if k != 'outcome_books'} for s in scopes]
    scopes_equal = strip(ls) == strip(rs)
    if not scopes_equal:
        differences.append('scopes')
    books_equal = [s.get('outcome_books') for s in ls] == [s.get('outcome_books') for s in rs]
    if not books_equal:
        differences.append('scopes.outcome_books')
    count_diffs = []
    for index in range(max(len(ls), len(rs))):
        counts = []
        for scopes in (ls, rs):
            counts.append(Counter((row['status'], row.get('market_id')) for row in scopes[index].get('outcome_books', [])) if index < len(scopes) else Counter())
        for status, market in sorted(set(counts[0]) | set(counts[1]), key=lambda pair: (pair[0], pair[1] or '')):
            key = status, market
            count_diffs.append({'scope': index, 'status': status, 'market_id': market,
                                'a': counts[0][key], 'b': counts[1][key], 'delta': counts[1][key] - counts[0][key]})
    outcomes_a, outcomes_b = left.get('outcomes', {}), right.get('outcomes', {})
    unexpected = [p for p in differences if p not in allowed and not (p.startswith('scopes.') and 'scopes' in allowed)]
    report = {'version': 1, 'top_level': top, 'differences': differences, 'unexpected': unexpected,
              'scopes': {'equal_except_outcome_books': scopes_equal, 'outcome_books_equal': books_equal, 'counts': count_diffs},
              'outcomes': {'documents_equal': outcomes_a.get('document') == outcomes_b.get('document'),
                           'a': {k: outcomes_a.get(k) for k in ('provider', 'unavailable')},
                           'b': {k: outcomes_b.get(k) for k in ('provider', 'unavailable')}}}
    return (2 if unexpected else 0), report


def _rows(summary, declaration):
    rows = summary
    for part in declaration['rows'].split('.'):
        require(type(rows) is dict and part in rows, 'summary rows path missing')
        rows = rows[part]
    require(type(rows) is list, 'summary rows must be an array')
    indexed = {}
    for row in rows:
        require(type(row) is dict and all(k in row for k in declaration['key']), 'summary row key missing')
        key = [row[k] for k in declaration['key']]
        require(all(v is None or type(v) in (str, int, bool) for v in key), 'row keys must be JSON scalars')
        canonical_key = identity(key)
        require(canonical_key not in indexed, 'duplicate summary row key')
        indexed[canonical_key] = (key, row)
    return indexed


def _row_diff(left, right, declaration):
    a, b = _rows(left, declaration), _rows(right, declaration)
    added = [b[k][0] for k in sorted(set(b) - set(a))]
    removed = [a[k][0] for k in sorted(set(a) - set(b))]
    changed, identical = [], 0
    for key in sorted(set(a) & set(b)):
        la, rb = a[key][1], b[key][1]
        fields = [field for field in sorted(set(la) | set(rb)) if (field in la) != (field in rb) or la.get(field) != rb.get(field)]
        if fields:
            changed.append({'key': a[key][0], 'fields': fields})
        else:
            identical += 1
    return {'identical': identical, 'added': added, 'removed': removed, 'changed': changed}


def validate_expectation(value):
    obj(value, 'version groups')
    require(type(value['version']) is int and value['version'] == 1 and type(value['groups']) is dict, 'expectation version/groups')
    for group in value['groups'].values():
        obj(group, 'added removed changed')
        for rows in group.values():
            require(type(rows) is list, 'expected row keys array')
            require(all(type(row) is list and bool(row) and all(v is None or type(v) in (str, int, bool) for v in row) for row in rows), 'expected composite row keys')
            require(len({identity(row) for row in rows}) == len(rows), 'duplicate expected row key')
    return value


def _read_run(directory):
    root = Path(directory)
    result = validate_result(read_json(root / 'result.json'))
    spec = validate_resolved(read_json(root / 'spec.resolved.json'))
    require(result['status'] in {'SUCCESS', 'CHECKS_FAILED'}, 'comparison requires completed readers')
    runtime = spec['runtime']
    require(all(result[key] == runtime[key] for key in ('label', 'image', 'context', 'fee_catalog_identity')),
            'run result pin binding mismatch')
    require([g['name'] for g in spec['groups']] == [g['name'] for g in result['groups']], 'run group binding mismatch')
    artifacts = {}
    for declaration, entry in zip(spec['groups'], result['groups']):
        group = entry['name']
        require(entry['factory'] == declaration['factory'], 'run factory binding mismatch')
        directory = root / 'groups' / group
        receipt = read_json(directory / 'receipt.json')
        require(type(receipt) is dict and receipt == entry['receipt'], 'group receipt binding mismatch')
        if 'semantic_sha256' in receipt:
            sha(receipt['semantic_sha256'])
        # Receipts remain strategy-owned, so only check shared bindings when
        # supplied. Do not import or rerun economic readers on the host.
        for key, expected in (('group', group), ('run_id', runtime['run_id'])):
            require(key not in receipt or receipt[key] == expected, 'group receipt binding mismatch')
        require(set(entry['checks']) == set(declaration['checks']), 'group check binding mismatch')
        expected_files = {reference.replace(':', '.') + '.json': reference for reference in declaration['checks']}
        files = sorted((directory / 'checks').glob('*.json'))
        require({file.name for file in files} == set(expected_files), 'group check binding mismatch')
        checked = {}
        for file in files:
            value = closed_check(read_json(file))
            require(value['passed'] == entry['checks'][expected_files[file.name]], 'group check binding mismatch')
            checked[file.name] = value
        summary_file = directory / 'summary.json'
        artifacts[group] = {'receipt': receipt, 'checks': checked,
                            'summary': read_json(summary_file) if summary_file.exists() else None,
                            'has_summary': summary_file.exists()}
    passed = all(flag for entry in result['groups'] for flag in entry['checks'].values())
    require(passed == (result['status'] == 'SUCCESS'), 'run check status binding mismatch')
    return root, result, {g['name']: g for g in spec['groups']}, artifacts


def compare_runs(a, b, *, expect=None):
    expectation = validate_expectation(expect) if expect is not None else {'version': 1, 'groups': {}}
    left, right = _read_run(a), _read_run(b)
    all_groups = set(left[2]) | set(right[2])
    require(not set(expectation['groups']) - all_groups, 'expectation names an unknown group')
    report = {'version': 1, 'groups': {}, 'added_groups': sorted(set(right[2]) - set(left[2])),
              'removed_groups': sorted(set(left[2]) - set(right[2])), 'unexpected': []}
    if report['added_groups'] or report['removed_groups']:
        report['unexpected'].append('group membership')
    for group in sorted(set(left[2]) & set(right[2])):
        da, db = left[2][group]['compare'], right[2][group]['compare']
        require(da == db, 'comparison declarations differ')
        artifacts = [run[3][group] for run in (left, right)]
        receipts = [artifact['receipt'] for artifact in artifacts]
        presence = [artifact['has_summary'] for artifact in artifacts]
        if all(presence):
            rows = {'status': 'COMPARED', **_row_diff(*(artifact['summary'] for artifact in artifacts), da)}
        else:
            rows = {'status': 'NO_SUMMARY' if not any(presence) else 'SUMMARY_MISSING',
                    'identical': 0, 'added': [], 'removed': [], 'changed': []}
            if any(presence):
                report['unexpected'].append(group + '.summary')
        details = [artifact['checks'] for artifact in artifacts]
        flags = [key for key in sorted(set(details[0]) | set(details[1]))
                 if details[0].get(key) != details[1].get(key)]
        hashes = {kind: {'a': identity(receipts[0]) if kind == 'receipt' else receipts[0].get('semantic_sha256'),
                         'b': identity(receipts[1]) if kind == 'receipt' else receipts[1].get('semantic_sha256')} for kind in ('receipt', 'semantic')}
        for entry in hashes.values():
            entry['equal'] = entry['a'] == entry['b']
        report['groups'][group] = {'hashes': hashes, 'rows': rows, 'checks_changed': flags}
        expected = expectation['groups'].get(group, {'added': [], 'removed': [], 'changed': []})
        for kind in ('added', 'removed', 'changed'):
            actual = [row['key'] for row in rows[kind]] if kind == 'changed' else rows[kind]
            require(all(len(key) == len(da['key']) for key in expected[kind]), 'expected key arity mismatch')
            if {identity(key) for key in actual} != {identity(key) for key in expected[kind]}:
                report['unexpected'].append(group + '.' + kind)
        if not hashes['semantic']['equal'] and not any(expected[k] for k in ('added', 'removed', 'changed')):
            report['unexpected'].append(group + '.semantic')
        if flags:
            report['unexpected'].append(group + '.checks')
    return (2 if report['unexpected'] else 0), report
