import test from 'node:test';
import assert from 'node:assert/strict';
import {
  findReplayJob,
  jobProgress,
  replayJobEvents,
  replayJobs,
  replayStatuses,
  replayStatusMeta,
  validateReplayForm,
} from '../src/client/replay-model.js';
import { buildSiweMessage } from '../src/client/universe-auth.js';
import {
  parseRetryAfter,
  UniverseApiError,
  UniverseClient,
  validateJobPage,
  validateStrategyCatalogue,
  type ReplayRequest,
} from '../src/client/universe-client.js';

const job = {
  job_id: '20260927T120000Z-0123456789abcdef',
  created_at_ns: '1',
  submitted_by: '0x7472005Ed1e68c8A82833cB5B251790eA6C0FB58',
  request_sha256: 'a'.repeat(64),
  status: 'queued',
  stage: null,
  stage_attempts: 0,
  next_attempt_at_ns: null,
  started_at_ns: null,
  updated_at_ns: '1',
  finished_at_ns: null,
  pending_outcome: null,
  reason_code: null,
  reason_detail: null,
  blocked_reason_code: null,
  blocked_at_ns: null,
  archive_receipt_key: null,
};

const response = (body: unknown, status = 200, headers: HeadersInit = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  });

test('fixtures cover every landed visible status with non-color labels', () => {
  assert.deepEqual(
    replayJobs.map((job) => job.status),
    replayStatuses,
  );
  for (const status of replayStatuses) {
    assert.ok(replayStatusMeta[status].label);
    assert.ok(replayStatusMeta[status].symbol);
  }
});

test('progress distinguishes queued, mid-run, blocked archive, and terminal jobs', () => {
  assert.equal(jobProgress(findReplayJob('missing', 'queued')), 0);
  const running = jobProgress(findReplayJob('missing', 'running'));
  assert.ok(running > 0 && running < 100);
  assert.equal(jobProgress(findReplayJob('missing', 'archive_blocked')), 92);
  assert.equal(jobProgress(findReplayJob('missing', 'succeeded')), 100);
});

test('job timeline keeps queued cancellation distinct from running work', () => {
  const queued = replayJobEvents(findReplayJob('missing', 'queued'));
  const cancelled = replayJobEvents(findReplayJob('missing', 'cancelled'));
  assert.equal(queued.length, 1);
  assert.equal(cancelled.at(-1)?.title, 'Cancelled');
  assert.equal(
    cancelled.some((event) => event.title === 'Runner claimed job'),
    false,
  );
});

test('new replay validation requires bundle, selected markets, and active strategy', () => {
  assert.deepEqual(
    validateReplayForm({
      bundleId: '',
      marketMode: 'selected',
      selectedMarkets: [],
      strategy: '',
    }),
    {
      bundleId: 'Choose a historical bundle.',
      markets: 'Choose at least one market or replay all markets.',
      strategy: 'Choose an active strategy.',
    },
  );
  assert.deepEqual(
    validateReplayForm({
      bundleId: 'bundle-a',
      marketMode: 'all',
      selectedMarkets: [],
      strategy: 'bundle_coverage',
    }),
    {},
  );
});

test('Replay codecs are closed and reject hostile nested catalogue and job shapes', () => {
  assert.throws(
    () =>
      validateStrategyCatalogue({
        version: 1,
        strategies: [],
        limits: [],
        injected: true,
      }),
    /unexpected fields/,
  );
  assert.throws(
    () =>
      validateStrategyCatalogue({
        version: 1,
        strategies: [
          {
            name: 'x',
            label: 'X',
            description: 'x',
            status: 'active',
            config_keys: [],
            factory: 'secret',
          },
        ],
        limits: [],
      }),
    /unexpected fields/,
  );
  assert.throws(
    () =>
      validateJobPage({
        jobs: [{ ...job, status: 'invented' }],
        next_cursor: null,
      }),
    /status is invalid/,
  );
  assert.throws(
    () =>
      validateJobPage({ jobs: [{ ...job, extra: true }], next_cursor: null }),
    /unexpected fields/,
  );
});

test('direct client preserves typed HTTP errors, Retry-After, and 401 revocation', async () => {
  let revoked = false;
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async () =>
      response({ error: 'rate limit exceeded' }, 429, {
        'retry-after': '7',
      })) as typeof fetch,
    onUnauthorized: () => {
      revoked = true;
    },
  });
  await assert.rejects(
    api.jobs(),
    (error) =>
      error instanceof UniverseApiError &&
      error.status === 429 &&
      error.retryAfterSeconds === 7,
  );
  const unauthorized = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async () =>
      response({ error: 'authentication required' }, 401)) as typeof fetch,
    getToken: () => 'redacted-test-token',
    onUnauthorized: () => {
      revoked = true;
    },
  });
  await assert.rejects(
    unauthorized.allowlist(),
    (error) => error instanceof UniverseApiError && error.status === 401,
  );
  assert.equal(revoked, true);
  assert.equal(
    parseRetryAfter(
      'Wed, 21 Oct 2026 07:28:00 GMT',
      Date.parse('Wed, 21 Oct 2026 07:27:58 GMT'),
    ),
    2,
  );
});

test('direct client preserves actionable 403, 404, 409, and 503 responses', async () => {
  for (const status of [403, 404, 409, 503]) {
    const api = new UniverseClient({
      baseUrl: 'https://universe.example',
      getToken: () => 'redacted-test-token',
      fetch: (async () =>
        response({ error: `status ${status}` }, status)) as typeof fetch,
    });
    await assert.rejects(
      api.cancel(job.job_id),
      (error) =>
        error instanceof UniverseApiError &&
        error.status === status &&
        error.message === `status ${status}`,
    );
  }
});

test('polling is bounded and stops immediately at a terminal job', async () => {
  let calls = 0;
  const request = {
    replay_request_version: 1,
    bundle_id: 'bundle-1',
    probe_markets: null,
    interval: null,
    strategy: { name: 'coverage', config: {} },
    limits: 'small',
  };
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async () => {
      calls += 1;
      return response({
        ...job,
        status: 'succeeded',
        stage: 'archive',
        finished_at_ns: '2',
        archive_receipt_key: `replay/jobs/${job.job_id}/job_receipt.json`,
        request,
      });
    }) as typeof fetch,
  });
  const updates: string[] = [];
  const result = await api.pollJob(
    job.job_id,
    (value) => updates.push(value.status),
    {
      attempts: 2,
      intervalMs: 1,
    },
  );
  assert.equal(result.status, 'succeeded');
  assert.equal(calls, 1);
  assert.deepEqual(updates, ['succeeded']);
});

test('caller cancellation remains an AbortError rather than a timeout failure', async () => {
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: ((_input, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () =>
          reject(new DOMException('Aborted', 'AbortError')),
        );
      })) as typeof fetch,
  });
  const controller = new AbortController();
  const pending = api.jobs(undefined, controller.signal);
  controller.abort();
  await assert.rejects(
    pending,
    (error) => error instanceof Error && error.name === 'AbortError',
  );
});

test('submission sends bearer auth without cookies and reuses the caller idempotency key', async () => {
  const seen: Array<{
    key: string | null;
    credentials: RequestCredentials | undefined;
    body: string;
  }> = [];
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    getToken: () => 'redacted-test-token',
    fetch: (async (_input, init) => {
      const headers = new Headers(init?.headers);
      assert.equal(headers.get('authorization'), 'Bearer redacted-test-token');
      seen.push({
        key: headers.get('idempotency-key'),
        credentials: init?.credentials,
        body: String(init?.body),
      });
      return response(
        { job_id: job.job_id, status: 'queued', replayed: seen.length > 1 },
        seen.length > 1 ? 200 : 201,
      );
    }) as typeof fetch,
  });
  const request: ReplayRequest = {
    replay_request_version: 1,
    bundle_id: 'bundle-1',
    probe_markets: null,
    interval: null,
    strategy: { name: 'coverage', config: {} },
    limits: 'small',
  };
  await api.submit(request, 'one-deliberate-run');
  await api.submit(request, 'one-deliberate-run');
  assert.deepEqual(
    seen.map((item) => item.key),
    ['one-deliberate-run', 'one-deliberate-run'],
  );
  assert.deepEqual(
    seen.map((item) => item.credentials),
    ['omit', 'omit'],
  );
  assert.equal(seen[0].body, seen[1].body);
});

test('SIWE message uses configured values rather than the current browser origin', () => {
  assert.equal(
    buildSiweMessage(
      '0x7472005Ed1e68c8A82833cB5B251790eA6C0FB58',
      '0123456789abcdef0123456789abcdef',
      '2026-09-27T12:00:00.000Z',
      {
        domain: 'prediction-indexer-targeter-ui-b8xg.vercel.app',
        uri: 'https://prediction-indexer-targeter-ui-b8xg.vercel.app',
        statement: 'Sign in to Prediction Indexer.',
        chainId: 1,
      },
    ),
    'prediction-indexer-targeter-ui-b8xg.vercel.app wants you to sign in with your Ethereum account:\n0x7472005Ed1e68c8A82833cB5B251790eA6C0FB58\n\nSign in to Prediction Indexer.\n\nURI: https://prediction-indexer-targeter-ui-b8xg.vercel.app\nVersion: 1\nChain ID: 1\nNonce: 0123456789abcdef0123456789abcdef\nIssued At: 2026-09-27T12:00:00.000Z',
  );
});
