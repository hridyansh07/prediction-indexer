"""Outcome warm-up and preparation using exported configuration only."""

from collections import Counter
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import build_opener, ProxyHandler

from replay.preparation import MAX_BYTES, _NoRedirect, digest, load_snapshot, prepare, validate_config
from replay.prepare_context import universe_from_environment
from .common import create_output, positive, read_json, write_json


class PreparationEnvironmentError(Exception):
    """Sanitized failure; transport exception strings can contain private URLs."""


class WarmClient:
    """Separate long-timeout client; does not change UniverseHTTP's short bounds."""

    def get(self, url, *, timeout):
        deadline = time.monotonic() + timeout
        with build_opener(ProxyHandler({}), _NoRedirect()).open(url, timeout=timeout) as response:
            size = 0
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError('outcomes warm-up deadline')
                block = response.read1(min(65536, MAX_BYTES + 1 - size))
                if not block:
                    break
                size += len(block)
                if size > MAX_BYTES:
                    raise ValueError('outcomes warm-up body limit')
            return response.status


def warm_outcomes(source, bundle_id, *, client, timeout, clock=time.monotonic):
    positive(timeout)
    if timeout > 600:
        raise ValueError('warm timeout exceeds 600 seconds')
    url = source.base_url + '/v1/bundles/' + quote(bundle_id, safe='') + '/outcomes'
    attempts = []
    for _ in range(3):
        started = clock()
        status, error_type = None, None
        try:
            status = client.get(url, timeout=timeout)
        except HTTPError as error:
            status, error_type = error.code, type(error).__name__
        except Exception as error:
            # Never persist exception text, URL, response body or traceback here.
            error_type = type(error).__name__
        attempts.append({'status': status, 'seconds': max(0, clock() - started), 'error_type': error_type})
        if status == 200 or (status is not None and status not in {502, 503, 504}):
            break
    return attempts


def prepare_context(config_path, output, *, warm_timeout_s=120, allow_outcomes_unavailable=False,
                    warm_client=None, environ=None, prepare_fn=prepare, source_factory=universe_from_environment):
    config = read_json(config_path)
    validate_config(config)
    positive(warm_timeout_s)
    if warm_timeout_s > 600:
        raise ValueError('warm timeout exceeds 600 seconds')
    # No dotenv path reaches the existing CLI helper.
    source = source_factory(env_file=None, environ=environ)
    root = create_output(output)
    report = {'version': 1, 'config_sha256': digest(config), 'snapshot_sha256': None,
              'universe_base_url_set': True, 'warm_up': [], 'outcomes': None,
              'scope_outcome_books': [], 'error': None}
    try:
        report['warm_up'] = warm_outcomes(source, config['bundle_id'], client=warm_client or WarmClient(), timeout=warm_timeout_s)
        prepare_fn(config, root, universe=source)
        receipt = read_json(root / 'receipt.json')
        snapshot = load_snapshot(root, expected_sha256=receipt['snapshot_sha256'])
        report['snapshot_sha256'] = receipt['snapshot_sha256']
        outcomes = snapshot.get('outcomes', {})
        report['outcomes'] = {'provider': outcomes.get('provider'), 'unavailable': outcomes.get('unavailable')}
        for index, scope in enumerate(snapshot['scopes']):
            counts = Counter(book['status'] for book in scope.get('outcome_books', ()))
            report['scope_outcome_books'].append({'scope': index, 'statuses': dict(sorted(counts.items()))})
        if outcomes.get('provider') is None and not allow_outcomes_unavailable:
            report['error'] = {'type': 'OutcomesUnavailable', 'message': outcomes.get('unavailable', 'outcomes unavailable')}
            code = 3
        else:
            code = 0
    except Exception as error:
        report['error'] = {'type': type(error).__name__, 'message': 'Universe preparation failed'}
        write_json(root / 'bench_prepare.json', report)
        raise PreparationEnvironmentError('Universe preparation failed (' + type(error).__name__ + ')') from None
    write_json(root / 'bench_prepare.json', report)
    return code, report
