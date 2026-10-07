"""Offline bench contracts: synthetic contexts, schedules, readers and Docker."""

from copy import deepcopy
import hashlib
import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from replay.bench.common import read_json, write_json
from replay.bench.compare import compare_contexts, compare_runs
from replay.bench.docker import DockerError, git_source, run_bench
from replay.bench.fees import build_fees, validate_fee_spec
from replay.bench.inside import execute, initial_result, validate_result
from replay.bench.prepare import PreparationEnvironmentError, prepare_context
from replay.bench.specs import resolve_spec, supervisor_config, validate_run_spec
from replay.fees.domain import tree
from replay.preparation import prepare, load_snapshot
from replay.streams.protocol import ProtocolError
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_outcomes import document
from tests.test_fee_sdk import SOURCE_BYTES, SOURCE, fill, schedule

IMAGE = 'sha256:' + 'c' * 64
OWNER = 'io.prediction-indexer.bench.owner'


def context(root, name='context', *, outcomes=True):
    directory = root / name
    prepare(config(), directory, universe=lambda *_: detail(), outcomes=(lambda _: document()) if outcomes else None)
    return directory


def run_spec(root, context_directory=None):
    source = root / 'source'
    source.mkdir(exist_ok=True)
    (source / 'Dockerfile').write_text('FROM synthetic\n')
    base = root / 'base.json'
    if not base.exists():
        write_json(base, {'version': 1, 'publisher': '/usr/local/bin/replay-publish', 'python': '/usr/local/bin/python',
                         'transport': {}, 'strategies': {},
                         'limits': {'attempts': 9, 'no_progress': 2, 'progress_margin': 1,
                                    'stall_seconds': 10, 'attempt_seconds': 20, 'run_seconds': 30,
                                    'poll_seconds': .1, 'stop_seconds': 1}})
    return {'version': 1, 'image': {'build_context': str(source), 'dockerfile': 'Dockerfile', 'reuse_id': IMAGE},
            'redis': {'image': 'redis:8.2-alpine', 'maxmemory': '150mb'}, 'base_run_config': str(base),
            'context_directory': str(context_directory or context(root)), 'fee_catalog_directory': None,
            'mounts': [], 'limits': {'attempts': 7, 'run_seconds': 40},
            'groups': [{'name': 'g', 'factory': 'synthetic.strategy:build', 'revision': 'test',
                        'config': {'context': '{context}', 'hash': '{snapshot_sha256}',
                                   'nested': [{'output': '{output}'}]},
                        'reader': 'synthetic.strategy:read_completed', 'checks': [],
                        'compare': {'rows': 'rows', 'key': ['id']}}]}


def resolved(spec):
    return resolve_spec(spec, label='test', run_id='bench-test-abc', image={
        'id': IMAGE, 'built': False, 'source_commit': 'a' * 40, 'source_dirty': True, 'dockerfile': 'Dockerfile'})


class FakeDocker:
    def __init__(self, *, fail=None, interrupt=False, collision=False, network_collision=False):
        self.calls = []
        self.fail, self.interrupt, self.collision = fail, interrupt, collision
        self.network_collision = network_collision
        self.objects = {}
        self.root = None

    def run(self, args, *, timeout=None, log=None):
        self.calls.append((list(args), log))
        if log is not None:
            Path(log).write_text('synthetic Docker log\n')
        if args[0] == 'build':
            if self.fail == 'build':
                raise DockerError('build failed')
            Path(args[args.index('--iidfile') + 1]).write_text(IMAGE)
            label = args[args.index('--label') + 1].split('=', 1)[1] if '--label' in args else ''
            self.objects[args[args.index('--tag') + 1]] = label
            return ''
        if args[:2] == ['image', 'inspect']:
            if args[-1] == IMAGE:
                return IMAGE if args[args.index('--format') + 1] == '{{.Id}}' else ''
            if args[-1] not in self.objects:
                raise DockerError('not found')
            return self.objects[args[-1]]
        if args[0] == 'inspect' or args[:2] == ['network', 'inspect']:
            if args[-1] not in self.objects:
                raise DockerError('not found')
            return self.objects[args[-1]]
        if args[:2] == ['network', 'create']:
            if self.network_collision:
                self.objects[args[-1]] = 'unrelated-owner'
                raise DockerError('network already exists')
            self.objects[args[-1]] = args[args.index('--label') + 1].split('=', 1)[1] if '--label' in args else ''
            return 'network-id'
        if args[0] == 'create':
            identifier = args[args.index('--name') + 1]
            if self.collision and identifier.endswith('-redis'):
                self.objects[identifier] = 'unrelated-owner'
                raise DockerError('name already exists')
            self.objects[identifier] = args[args.index('--label') + 1].split('=', 1)[1] if '--label' in args else ''
            if identifier.endswith('-runner'):
                mount = args[args.index('--mount') + 1]
                self.root = Path(mount.split('src=', 1)[1].split(',dst=', 1)[0])
                if self.fail == 'runner':
                    raise DockerError('runner create failed after creation')
                if self.interrupt:
                    raise KeyboardInterrupt()
            return identifier
        if args[0] == 'wait':
            spec = read_json(self.root / 'spec.resolved.json')
            result = initial_result(spec)
            result['status'] = 'SUCCESS'
            write_json(self.root / 'result.json', result)
            return '0'
        if args[0] == 'rm' or args[:2] == ['network', 'rm'] or args[:2] == ['image', 'rm']:
            self.objects.pop(args[-1], None)
        return ''


class BenchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.spec = run_spec(self.root)
        self.spec_path = self.root / 'spec.json'
        write_json(self.spec_path, self.spec)

    def test_closed_spec_paths_names_and_limits(self):
        validate_run_spec(self.spec)
        for mutate in (
            lambda s: s.update(extra=True), lambda s: s['image'].update(extra=True),
            lambda s: s['redis'].update(extra=True), lambda s: s['groups'][0].update(extra=True),
            lambda s: s['groups'][0]['compare'].update(extra=True), lambda s: s['limits'].update(extra=True),
            lambda s: s.update(context_directory='relative'), lambda s: s.update(base_run_config='/missing'),
            lambda s: s['groups'].append(deepcopy(s['groups'][0])),
            lambda s: s['groups'][0]['config'].update(value='{unknown}'),
            lambda s: s['redis'].update(image='redis:8.1-alpine'),
            lambda s: s['redis'].update(maxmemory='0mb'),
        ):
            spec = deepcopy(self.spec); mutate(spec)
            with self.subTest(spec=spec), self.assertRaises((ProtocolError, ValueError)):
                validate_run_spec(spec)
        r = resolved(self.spec)
        self.assertEqual(r['groups'][0]['config']['nested'][0]['output'], '/bench/groups/g/work')
        self.assertEqual(r['groups'][0]['config']['context'], '/bench/context')
        self.assertEqual(supervisor_config(r)['limits']['attempts'], 1)
        self.assertEqual(supervisor_config(r)['transport']['groups'], ['g'])

    def test_mounts_allow_caller_paths_and_refuse_actual_shadows(self):
        for destination in ('/bench/in/derivatives', '/app/replay/custom_check.py'):
            s = deepcopy(self.spec)
            s['mounts'] = [{'host': str(self.root / 'source'), 'container': destination}]
            validate_run_spec(s)
        for destination in ('/', '/bench', '/bench/context', '/bench/context/x', '/bench/run', '/bench/result.json'):
            s = deepcopy(self.spec)
            s['mounts'] = [{'host': str(self.root / 'source'), 'container': destination}]
            with self.subTest(destination=destination), self.assertRaises(ProtocolError):
                validate_run_spec(s)

    def test_inside_success_and_strategy_output_check_argument(self):
        r = resolved(self.spec)
        r['groups'][0]['checks'] = ['synthetic.check:verify']
        output = self.root / 'inside'; output.mkdir()
        actual = output / 'run' / 'attempt' / 'g' / 'output'
        actual.mkdir(parents=True)
        receipt = {'semantic_sha256': 'f' * 64, 'attempt_id': 'one'}
        reader = Mock(return_value={'receipt': receipt, 'summary': {'rows': []}})
        check = Mock(return_value={'passed': True, 'details': {'offline': True}})
        imports = {'synthetic.strategy:build': Mock(), 'synthetic.strategy:read_completed': reader, 'synthetic.check:verify': check}
        supervise = Mock(return_value={'outputs': {'g': 'attempt/g/output'}})
        code, result = execute(r, output, redis_url='redis://synthetic', supervisor_run=supervise,
                               importer=imports.__getitem__, snapshot_loader=Mock(return_value={'outcomes': {'provider': 'universe'}}))
        self.assertEqual(code, 0)
        self.assertEqual(result['status'], 'SUCCESS')
        validate_result(read_json(output / 'result.json'))
        self.assertEqual(supervise.call_args.args[0]['limits']['attempts'], 1)
        check.assert_called_once_with(run_directory=output / 'run', group='g', output_directory=actual, context_directory=output / 'context')
        self.assertEqual(read_json(output / 'groups/g/receipt.json'), receipt)

    def test_inside_reader_supervisor_import_and_check_failures(self):
        cases = [('reader', 3, 'FAILED'), ('supervisor', 3, 'FAILED'), ('import', 1, 'FAILED'),
                 ('check', 2, 'CHECKS_FAILED'), ('raising', 2, 'CHECKS_FAILED'), ('bad_check', 2, 'CHECKS_FAILED')]
        for case, expected, status in cases:
            r = resolved(self.spec); r['groups'][0]['checks'] = ['synthetic.check:verify']
            output = self.root / case; output.mkdir()
            reader = Mock(side_effect=ValueError('reader failure')) if case == 'reader' else Mock(return_value={'receipt': {'semantic_sha256': 'f' * 64}, 'summary': {'rows': []}})
            check = Mock(side_effect=RuntimeError('check failure')) if case == 'raising' else Mock(return_value={'passed': case != 'check', 'details': {}, **({'extra': True} if case == 'bad_check' else {})})
            imports = {'synthetic.strategy:build': Mock(), 'synthetic.strategy:read_completed': reader, 'synthetic.check:verify': check}
            importer = Mock(side_effect=ImportError('absent')) if case == 'import' else imports.__getitem__
            supervise = Mock(side_effect=RuntimeError('supervisor failure')) if case == 'supervisor' else Mock(return_value={'outputs': {'g': 'attempt/g/output'}})
            with self.subTest(case=case):
                code, result = execute(r, output, redis_url='redis://synthetic', supervisor_run=supervise,
                                       importer=importer, snapshot_loader=Mock(return_value={'outcomes': {'provider': 'universe'}}))
                self.assertEqual((code, result['status']), (expected, status))
                validate_result(read_json(output / 'result.json'))
                if case == 'import': supervise.assert_not_called()
                if case == 'raising':
                    checked = read_json(output / 'groups/g/checks/synthetic.check.verify.json')
                    self.assertEqual(checked['details']['error']['type'], 'RuntimeError')
        result = initial_result(resolved(self.spec)); result['extra'] = True
        with self.assertRaises(ProtocolError): validate_result(result)

    def _docker(self, output, fake, **kwargs):
        return run_bench(self.spec_path, output, docker=fake, source_reader=lambda _: ('a' * 40, True), **kwargs)

    def test_docker_owned_cleanup_and_readonly_mounts(self):
        names = []
        for i in range(2):
            fake = FakeDocker()
            self.assertEqual(self._docker(self.root / ('docker' + str(i)), fake), 0)
            creates = [args for args, _ in fake.calls if args[0] == 'create']
            names.extend(args[args.index('--name') + 1] for args in creates)
            runner = next(args for args in creates if args[args.index('--name') + 1].endswith('-runner'))
            mounts = [runner[i + 1] for i, arg in enumerate(runner) if arg == '--mount']
            self.assertEqual(sum('readonly' not in mount for mount in mounts), 1)
            self.assertIn('--read-only', runner)
            self.assertTrue(all('readonly' in m for m in mounts if 'dst=/bench/context' in m))
            self.assertEqual(fake.objects, {})
            self.assertFalse(any(args[:2] in (['image', 'rm'], ['builder', 'prune']) for args, _ in fake.calls))
        self.assertEqual(len(set(names)), 4)

    def test_cleanup_failure_interrupt_keeps_evidence(self):
        for label, fake in [('failure', FakeDocker(fail='runner')), ('interrupt', FakeDocker(interrupt=True))]:
            output = self.root / label
            self.assertEqual(self._docker(output, fake), 3)
            self.assertEqual(fake.objects, {})
            self.assertEqual(read_json(output / 'result.json')['status'], 'FAILED')
            self.assertTrue((output / 'logs/runner.log').exists())
            self.assertTrue((output / 'logs/redis.log').exists())
            self.assertFalse(any('--volumes' in args or '-v' in args for args, _ in fake.calls))

    def test_create_collision_does_not_remove_unowned_container(self):
        fake = FakeDocker(collision=True)
        self.assertEqual(self._docker(self.root / 'collision', fake), 3)
        self.assertEqual(list(fake.objects.values()), ['unrelated-owner'])

    def test_network_collision_preserves_unrelated_resource(self):
        fake = FakeDocker(network_collision=True)
        self.assertEqual(self._docker(self.root / 'network-collision', fake), 3)
        self.assertEqual(list(fake.objects.values()), ['unrelated-owner'])
        self.assertFalse(any(args[:2] == ['network', 'rm'] for args, _ in fake.calls))

    def test_cli_invalid_arguments_and_non_git_context(self):
        from replay.bench.__main__ import main
        from replay.bench.inside import main as inside_main
        with contextlib.redirect_stderr(io.StringIO()):
            for args in ([], ['unknown'], ['run'], ['prepare', 'a', 'b', '--warm-timeout-s', 'bad']):
                self.assertEqual(main(args), 1)
            self.assertEqual(inside_main([]), 1)
            self.assertEqual(main(['run', str(self.spec_path), str(self.root / 'non-git')]), 1)
        self.assertFalse((self.root / 'non-git').exists())
        with self.assertRaisesRegex(ValueError, 'Git checkout'):
            git_source(self.root / 'source')

    def test_fee_identity_directory_mount_and_inside_loader(self):
        fee_spec = self.root / 'empty-fees.json'
        write_json(fee_spec, {'version': 1, 'schedules': []})
        report = build_fees(fee_spec, self.root / 'catalogs')
        spec = deepcopy(self.spec)
        catalog = self.root / 'catalogs' / report['catalog_identity']
        spec['fee_catalog_directory'] = str(catalog)
        spec['groups'][0]['config']['fees'] = '{fees}/{catalog_identity}'
        self.spec_path.unlink(); write_json(self.spec_path, spec)
        fake = FakeDocker()
        self.assertEqual(self._docker(self.root / 'fee-mount', fake), 0)
        runner = next(args for args, _ in fake.calls if args[0] == 'create' and args[args.index('--name') + 1].endswith('-runner'))
        self.assertIn('type=bind,src=' + str(catalog.resolve()) + ',dst=/bench/fees/' + report['catalog_identity'] + ',readonly', runner)
        r = resolved(spec)
        self.assertEqual(r['groups'][0]['config']['fees'], '/bench/fees/' + report['catalog_identity'])
        output = self.root / 'fee-inside'; output.mkdir()
        loader = Mock(return_value=Mock(identity=report['catalog_identity']))
        imports = {'synthetic.strategy:build': Mock(), 'synthetic.strategy:read_completed': Mock(return_value={'receipt': {}})}
        code, _ = execute(r, output, redis_url='redis://synthetic', supervisor_run=Mock(return_value={}),
                          importer=imports.__getitem__, snapshot_loader=Mock(return_value={'outcomes': {'provider': 'universe'}}),
                          catalog_loader=loader)
        self.assertEqual(code, 0)
        loader.assert_called_once_with(output / 'fees' / report['catalog_identity'])

    def test_redis_does_not_create_anonymous_volume(self):
        fake = FakeDocker(); self._docker(self.root / 'redis-volume', fake)
        redis = next(args for args, _ in fake.calls if args[0] == 'create' and args[args.index('--name') + 1].endswith('-redis'))
        self.assertIn('/data:rw,nosuid,noexec,size=1m', redis)

    def test_built_image_keep_prune_and_unresolved_build_identity(self):
        spec = deepcopy(self.spec); spec['image']['reuse_id'] = None
        self.spec_path.unlink(); write_json(self.spec_path, spec)
        for index, keep, prune in [(0, False, False), (1, True, False), (2, False, True)]:
            fake = FakeDocker()
            code = self._docker(self.root / ('built' + str(index)), fake, keep_image=keep, prune_build_cache=prune)
            self.assertEqual(code, 0)
            self.assertEqual(any(args[:2] == ['image', 'rm'] for args, _ in fake.calls), not keep)
            self.assertEqual(any(args[:2] == ['builder', 'prune'] for args, _ in fake.calls), prune)
        fake = FakeDocker(fail='build')
        output = self.root / 'build-failure'
        self.assertEqual(self._docker(output, fake), 3)
        self.assertIsNone(read_json(output / 'result.json')['image']['id'])
        self.assertTrue((output / 'logs/build.log').exists())

    def test_all_writing_commands_refuse_existing_output(self):
        output = self.root / 'exists'; output.mkdir()
        marker = output / 'evidence'; marker.write_text('unchanged')
        with self.assertRaises((FileExistsError, ProtocolError)):
            self._docker(output, FakeDocker())
        fee_spec = self.root / 'fees-empty.json'; write_json(fee_spec, {'version': 1, 'schedules': []})
        with self.assertRaises((FileExistsError, ProtocolError)): build_fees(fee_spec, output)
        cfg = self.root / 'prepare.json'; write_json(cfg, config())
        with self.assertRaises((FileExistsError, ProtocolError)):
            prepare_context(cfg, output, environ={'UNIVERSE_BASE_URL': 'https://synthetic.invalid'})
        self.assertEqual(marker.read_text(), 'unchanged')

    def test_prepare_environment_only_warm_retries_and_outcome_policy(self):
        cfg = self.root / 'prepare.json'; write_json(cfg, config())
        warm = Mock(); warm.get.side_effect = [TimeoutError('https://private.invalid'), 200]
        source = Mock(); source.base_url = 'https://exported.invalid'
        source.side_effect = lambda *_: detail(); source.outcomes.return_value = document()
        factory = Mock(return_value=source)
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError): prepare_context(cfg, self.root / 'missing', warm_client=warm)
        code, report = prepare_context(cfg, self.root / 'prepared', environ={'UNIVERSE_BASE_URL': 'https://exported.invalid'},
                                       source_factory=factory, warm_client=warm)
        factory.assert_called_once_with(env_file=None, environ={'UNIVERSE_BASE_URL': 'https://exported.invalid'})
        self.assertEqual(code, 0); self.assertEqual(len(report['warm_up']), 2)
        self.assertNotIn('https://', (self.root / 'prepared/bench_prepare.json').read_text())
        source.outcomes.return_value = None
        warm.get.side_effect = None; warm.get.return_value = 200
        for allowed in (False, True):
            code, report = prepare_context(cfg, self.root / ('unavailable' + str(allowed)), source_factory=factory,
                                           warm_client=warm, allow_outcomes_unavailable=allowed)
            self.assertEqual(code, 0 if allowed else 3)
            self.assertIsNone(report['outcomes']['provider'])
        failing = Mock(side_effect=RuntimeError('private URL https://private.invalid/token'))
        with self.assertRaises(PreparationEnvironmentError) as error:
            prepare_context(cfg, self.root / 'source-fail', source_factory=factory, warm_client=warm, prepare_fn=failing)
        self.assertNotIn('https://', str(error.exception))
        self.assertNotIn('https://', (self.root / 'source-fail/bench_prepare.json').read_text())

    def test_no_dotenv_read_even_when_cwd_has_different_setting(self):
        # Only hand-authored test scratch; no repository secret is inspected.
        dotenv = self.root / '.env'; dotenv.write_text('UNIVERSE_BASE_URL=https://wrong.invalid\n')
        cfg = self.root / 'prepare.json'; write_json(cfg, config())
        real_open = Path.open
        def guarded(path, *args, **kwargs):
            if path.name == '.env': raise AssertionError('dotenv was read')
            return real_open(path, *args, **kwargs)
        source = Mock(); source.base_url = 'https://correct.invalid'; source.side_effect = lambda *_: detail(); source.outcomes.return_value = document()
        with patch.object(Path, 'open', guarded), patch('replay.prepare_context.UniverseHTTP', return_value=source) as construct:
            prepare_context(cfg, self.root / 'env-only', environ={'UNIVERSE_BASE_URL': source.base_url}, warm_client=Mock(get=Mock(return_value=200)))
        construct.assert_called_once_with('https://correct.invalid', timeout=10)

    def test_context_comparison_pinned_equal_outcomes_and_evidence(self):
        other = context(self.root, 'second')
        self.assertEqual(compare_contexts(self.spec['context_directory'], other)[0], 0)
        unavailable = context(self.root, 'without', outcomes=False)
        self.assertEqual(compare_contexts(unavailable, other)[0], 2)
        self.assertEqual(compare_contexts(unavailable, other, expect_only=['outcomes', 'scopes.outcome_books'])[0], 0)
        a = {'evidence': {'source': 1}, 'scopes': [{'outcome_books': [{'status': 'MASKED', 'market_id': 'm'}]}]}
        b = deepcopy(a); b['scopes'][0]['outcome_books'][0]['status'] = 'UNAVAILABLE'
        self.assertEqual(compare_contexts('a', 'b', loader=lambda p: a if p == 'a' else b)[0], 2)
        self.assertEqual(compare_contexts('a', 'b', expect_only=['scopes.outcome_books'], loader=lambda p: a if p == 'a' else b)[0], 0)
        b['evidence']['source'] = 2
        self.assertEqual(compare_contexts('a', 'b', expect_only=['scopes.outcome_books'], loader=lambda p: a if p == 'a' else b)[0], 2)

    def test_fees_determinism_hash_scale_and_closed_fields(self):
        src = self.root / 'source.txt'; src.write_bytes(SOURCE_BYTES)
        value = tree(schedule(fill()))
        value['sources'] = [{'path': str(src), 'sha256': SOURCE.sha256, 'url': SOURCE.url, 'retrieved_at': SOURCE.retrieved_at}]
        spec = {'version': 1, 'schedules': [value]}
        path = self.root / 'fees.json'; write_json(path, spec)
        one, two = build_fees(path, self.root / 'fees1'), build_fees(path, self.root / 'fees2')
        self.assertEqual(one['catalog_identity'], two['catalog_identity'])
        for mutate in (lambda s: s['schedules'][0].update(fee_scale=6),
                       lambda s: s['schedules'][0]['sources'][0].update(sha256='0' * 64),
                       lambda s: s.update(extra=True), lambda s: s['schedules'][0].update(extra=True)):
            invalid = deepcopy(spec); mutate(invalid)
            with self.assertRaises((ProtocolError, ValueError)): validate_fee_spec(invalid)

    def _comparison_run(self, directory, rows, attempt):
        directory.mkdir()
        r = resolved(self.spec); write_json(directory / 'spec.resolved.json', r)
        result = initial_result(r); result['status'] = 'SUCCESS'
        result['groups'][0]['receipt'] = {'semantic_sha256': 'f' * 64, 'attempt_id': attempt}
        write_json(directory / 'result.json', result)
        write_json(directory / 'groups/g/receipt.json', result['groups'][0]['receipt'])
        write_json(directory / 'groups/g/summary.json', {'rows': rows})
        return directory

    def test_run_comparison_optional_summaries(self):
        a = self._comparison_run(self.root / 'receipt-a', [], 'a')
        b = self._comparison_run(self.root / 'receipt-b', [], 'b')
        (a / 'groups/g/summary.json').unlink()
        code, report = compare_runs(a, b)
        self.assertEqual(code, 2)
        self.assertIn('g.summary', report['unexpected'])
        (b / 'groups/g/summary.json').unlink()
        code, report = compare_runs(a, b)
        self.assertEqual(code, 0)
        self.assertEqual(report['groups']['g']['rows']['status'], 'NO_SUMMARY')
        self.assertTrue(report['groups']['g']['hashes']['semantic']['equal'])

    def test_run_comparison_rejects_detached_receipts_and_checks(self):
        a = self._comparison_run(self.root / 'binding-a', [], 'a')
        b = self._comparison_run(self.root / 'binding-b', [], 'b')
        # Copied artifacts must match their recorded reader/check result.
        receipt = b / 'groups/g/receipt.json'
        receipt.unlink()
        write_json(receipt, {'semantic_sha256': 'e' * 64, 'attempt_id': 'b'})
        with self.assertRaisesRegex(ProtocolError, 'receipt binding'):
            compare_runs(a, b)
        receipt.unlink()
        write_json(receipt, read_json(b / 'result.json')['groups'][0]['receipt'])
        write_json(b / 'groups/g/checks/unbound.check.json', {'passed': True, 'details': {}})
        with self.assertRaisesRegex(ProtocolError, 'check binding'):
            compare_runs(a, b)

    def test_run_comparison_check_differences_and_binding(self):
        runs = [self._comparison_run(self.root / ('checks-' + letter), [], letter) for letter in ('a', 'b')]
        for directory, passed in zip(runs, (True, False)):
            spec_file, result_file = directory / 'spec.resolved.json', directory / 'result.json'
            spec, result = read_json(spec_file), read_json(result_file)
            spec['groups'][0]['checks'] = ['synthetic.check:verify']
            result['groups'][0]['checks'] = {'synthetic.check:verify': passed}
            result['status'] = 'SUCCESS' if passed else 'CHECKS_FAILED'
            spec_file.unlink(); result_file.unlink()
            write_json(spec_file, spec); write_json(result_file, result)
            write_json(directory / 'groups/g/checks/synthetic.check.verify.json', {'passed': passed, 'details': {}})
        code, report = compare_runs(*runs)
        self.assertEqual(code, 2)
        self.assertIn('g.checks', report['unexpected'])
        check = runs[1] / 'groups/g/checks/synthetic.check.verify.json'
        check.unlink(); write_json(check, {'passed': True, 'details': {}})
        with self.assertRaisesRegex(ProtocolError, 'check binding'):
            compare_runs(*runs)

    def test_run_comparison_semantic_change_without_row_change(self):
        a = self._comparison_run(self.root / 'semantic-a', [], 'a')
        b = self._comparison_run(self.root / 'semantic-b', [], 'b')
        result = read_json(b / 'result.json')
        result['groups'][0]['receipt']['semantic_sha256'] = 'e' * 64
        for file, value in ((b / 'result.json', result), (b / 'groups/g/receipt.json', result['groups'][0]['receipt'])):
            file.unlink(); write_json(file, value)
        code, report = compare_runs(a, b)
        self.assertEqual(code, 2)
        self.assertIn('g.semantic', report['unexpected'])

    def test_run_comparison_receipt_randomness_and_expected_row_changes(self):
        a = self._comparison_run(self.root / 'cmp-a', [{'id': 'same', 'count': 1}, {'id': 'old', 'count': 2}], 'a')
        b = self._comparison_run(self.root / 'cmp-b', [{'id': 'same', 'count': 1}, {'id': 'old', 'count': 2}], 'b')
        code, report = compare_runs(a, b)
        self.assertEqual(code, 0)
        self.assertFalse(report['groups']['g']['hashes']['receipt']['equal'])
        c = self._comparison_run(self.root / 'cmp-c', [{'id': 'same', 'count': 3}, {'id': 'new', 'count': 4}], 'c')
        self.assertEqual(compare_runs(a, c)[0], 2)
        expected = {'version': 1, 'groups': {'g': {'added': [['new']], 'removed': [['old']], 'changed': [['same']]}}}
        self.assertEqual(compare_runs(a, c, expect=expected)[0], 0)
        expected['groups']['g']['changed'] = []
        self.assertEqual(compare_runs(a, c, expect=expected)[0], 2)
        expected['extra'] = True
        with self.assertRaises(ProtocolError): compare_runs(a, c, expect=expected)
        write_json(b / 'groups/g/checks/synthetic.check.json', {'passed': False, 'details': {}})
        with self.assertRaisesRegex(ProtocolError, 'check binding'):
            compare_runs(a, b)


if __name__ == '__main__':
    unittest.main()
