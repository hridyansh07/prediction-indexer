"""Disposable Docker orchestration, with a command-level offline seam."""

from pathlib import Path
import os
import subprocess
import time
import uuid

from replay.streams.protocol import require
from .common import create_output, error_document, name, read_json, write_json
from .inside import initial_result, validate_result
from .specs import resolve_spec, validate_image, validate_run_spec


OWNER = 'io.prediction-indexer.bench.owner'


class DockerError(Exception):
    pass


class DockerCLI:
    def run(self, args, *, timeout=None, log=None):
        """Never use a shell or forward host environment/credentials to containers."""
        if log is not None:
            with Path(log).open('xb') as output:
                process = subprocess.run(['docker', *args], stdout=output, stderr=subprocess.STDOUT, timeout=timeout, check=False)
                output.flush(); os.fsync(output.fileno())
            result = ''
        else:
            process = subprocess.run(['docker', *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
            require(len(process.stdout) <= 2 * 1024 * 1024, 'Docker response limit')
            result = process.stdout.decode('utf-8').strip()
        if process.returncode:
            # Docker diagnostics are in owned logs where applicable. Commands
            # contain reviewed public paths and IDs, never environment values.
            raise DockerError('Docker ' + args[0] + ' failed')
        return result


def git_source(directory):
    try:
        return _git_source(directory)
    except (subprocess.CalledProcessError, OSError) as error:
        raise ValueError('build context must be an accessible Git checkout') from None


def _git_source(directory):
    commit = subprocess.run(['git', '-C', str(directory), 'rev-parse', 'HEAD'], capture_output=True, text=True, check=True).stdout.strip()
    dirty = bool(subprocess.run(['git', '-C', str(directory), 'status', '--porcelain'], capture_output=True, text=True, check=True).stdout)
    return commit, dirty


def _mount(host, container, *, readonly=True):
    require(',' not in str(host) and ',' not in container, 'commas are unsupported in mount paths')
    return ['--mount', 'type=bind,src=' + str(host) + ',dst=' + container + (',readonly' if readonly else '')]


def run_bench(spec_path, output, *, keep_image=False, prune_build_cache=False, label='bench',
              docker=None, source_reader=git_source, suffix_factory=lambda: uuid.uuid4().hex[:16]):
    spec = read_json(spec_path)
    validate_run_spec(spec)
    name(label)
    suffix = suffix_factory()
    resource = name('bench-' + label[:40] + '-' + suffix)
    image_tag = resource + ':local'
    source_commit, source_dirty = source_reader(spec['image']['build_context'])
    image = {'id': spec['image']['reuse_id'], 'built': spec['image']['reuse_id'] is None,
             'source_commit': source_commit, 'source_dirty': source_dirty, 'dockerfile': spec['image']['dockerfile']}
    # Validate and independently load all pinned inputs before Docker can run.
    resolved = resolve_spec(spec, label=label, run_id=resource, image=image)
    root = create_output(output).resolve()
    (root / 'logs').mkdir()
    docker = docker or DockerCLI()
    owned_containers, owned_network, owned_image = [], False, False
    failures = []
    code, started = 3, time.monotonic()
    runner, redis = resource + '-runner', resource + '-redis'
    owner = resource
    def ownership(kind, identifier):
        prefix = ['inspect'] if kind == 'container' else [kind, 'inspect']
        field = '.Labels' if kind == 'network' else '.Config.Labels'
        try:
            matched = docker.run([*prefix, '--format', '{{index ' + field + ' \"' + OWNER + '\"}}', identifier]) == owner
            if not matched:
                failures.append(error_document(DockerError('resource ownership mismatch; preserved')))
            return matched
        except (Exception, KeyboardInterrupt) as error:
            failures.append(error_document(error))
            return False

    try:
        if image['built']:
            # Claim only our random tag BEFORE build: Docker may leave that tag
            # on an interrupted/ambiguous successful build. Cleanup targets the
            # tag, never a reused ID or a shared untagged layer.
            try:
                docker.run(['image', 'inspect', '--format', '{{.Id}}', image_tag])
            except DockerError:
                pass
            else:
                raise DockerError('refusing existing image tag')
            owned_image = True
            docker.run(['build', '--label', OWNER + '=' + owner, '--iidfile', str(root / 'image.id'), '--tag', image_tag,
                        '--build-arg', 'REPLAY_IMAGE_REVISION=' + source_commit,
                        '--file', str(Path(spec['image']['build_context']) / spec['image']['dockerfile']),
                        spec['image']['build_context']], log=root / 'logs' / 'build.log')
            image['id'] = (root / 'image.id').read_text().strip()
        else:
            require(docker.run(['image', 'inspect', '--format', '{{.Id}}', image['id']]) == image['id'], 'reused image identity mismatch')
        validate_image(image)
        resolved['runtime']['image'] = image
        write_json(root / 'spec.resolved.json', resolved)
        # Claim names before operations so KeyboardInterrupt/timeout immediately
        # after Docker created a resource still follows the owned cleanup path.
        owned_network = True
        docker.run(['network', 'create', '--label', OWNER + '=' + owner, '--internal', resource])
        owned_containers.append(redis)
        docker.run(['create', '--label', OWNER + '=' + owner, '--name', redis, '--network', resource, '--network-alias', 'redis',
                    '--read-only', '--tmpfs', '/data:rw,nosuid,noexec,size=1m', spec['redis']['image'], 'redis-server', '--maxmemory', spec['redis']['maxmemory'],
                    '--maxmemory-policy', 'noeviction', '--save', '', '--appendonly', 'no'])
        docker.run(['start', redis])
        # Wait for actual readiness, without a shared host port or retrying replay.
        docker.run(['exec', redis, 'sh', '-c', 'for i in $(seq 1 100); do redis-cli ping && exit 0; sleep 0.1; done; exit 1'], timeout=20)
        args = ['create', '--label', OWNER + '=' + owner, '--name', runner, '--network', resource, '--read-only', '--cap-drop', 'ALL',
                '--user', str(os.getuid()) + ':' + str(os.getgid()),
                '--tmpfs', '/tmp:rw,nosuid,noexec,size=1g', '--entrypoint', 'python']
        args += _mount(root, '/bench', readonly=False)
        args += _mount(Path(spec['context_directory']).resolve(), '/bench/context')
        if spec['fee_catalog_directory'] is not None:
            args += _mount(Path(spec['fee_catalog_directory']).resolve(), '/bench/fees/' + resolved['runtime']['fee_catalog_identity'])
        for mount in spec['mounts']:
            args += _mount(Path(mount['host']).resolve(), mount['container'])
        args += [image['id'], '-m', 'replay.bench.inside', '/bench/spec.resolved.json']
        owned_containers.append(runner)
        docker.run(args)
        docker.run(['start', runner])
        seconds = resolved['runtime']['base_run_config']['limits']['run_seconds']
        seconds = spec['limits'].get('run_seconds', seconds)
        returned = docker.run(['wait', runner], timeout=seconds + 60)
        require(returned in {'0', '1', '2', '3'}, 'runner exit code outside bench contract')
        code = int(returned)
        # A process exit alone is insufficient: require the closed result artifact.
        result = validate_result(read_json(root / 'result.json'))
        require(result['image'] == image and result['context'] == resolved['runtime']['context']
                and result['fee_catalog_identity'] == resolved['runtime']['fee_catalog_identity']
                and result['label'] == label, 'runner result pin mismatch')
        require((code == 0 and result['status'] == 'SUCCESS') or
                (code == 2 and result['status'] == 'CHECKS_FAILED') or
                (code in {1, 3} and result['status'] == 'FAILED'), 'runner exit/result mismatch')
    except (Exception, KeyboardInterrupt) as error:
        failures.append(error_document(error))
        code = 3
        if not (root / 'result.json').exists():
            result = initial_result(resolved)
            result['error'] = error_document(error)
            result['run_seconds'] = max(0, time.monotonic() - started)
            write_json(root / 'result.json', validate_result(result))
    finally:
        # Independent cleanup operations all run, even if logging or an earlier
        # removal fails. No volume flag, prune, broad name scan or wildcard.
        for container in reversed(owned_containers):
            if not ownership('container', container):
                continue
            filename = 'runner.log' if container == runner else 'redis.log'
            try:
                docker.run(['logs', container], log=root / 'logs' / filename)
            except (Exception, KeyboardInterrupt) as error:
                failures.append(error_document(error))
            try:
                docker.run(['rm', '--force', container])
            except (Exception, KeyboardInterrupt) as error:
                failures.append(error_document(error))
        if owned_network and ownership('network', resource):
            try:
                docker.run(['network', 'rm', resource])
            except (Exception, KeyboardInterrupt) as error:
                failures.append(error_document(error))
        if owned_image and not keep_image and ownership('image', image_tag):
            try:
                docker.run(['image', 'rm', image_tag])
            except (Exception, KeyboardInterrupt) as error:
                failures.append(error_document(error))
        if prune_build_cache:
            try:
                docker.run(['builder', 'prune', '--force'], log=root / 'logs' / 'prune.log')
            except (Exception, KeyboardInterrupt) as error:
                failures.append(error_document(error))
        if failures:
            code = 3
        write_json(root / 'orchestration.json', {'version': 1, 'exit_code': code, 'errors': failures,
                                                'network': resource, 'containers': owned_containers,
                                                'image_tag': image_tag if owned_image else None,
                                                'image_kept': bool(owned_image and keep_image)})
    return code
