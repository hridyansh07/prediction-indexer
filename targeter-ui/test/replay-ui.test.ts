import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import {
  findReplayJob,
  jobProgress,
  parseStrategyConfig,
  replayJobEvents,
  replayJobs,
  replayStatuses,
  replaySummaryHeading,
  replayStatusMeta,
  validateReplayDraft,
  validateReplayForm,
} from '../src/client/replay-model.js';
import {
  buildSiweMessage,
  normalizeWalletAddress,
  signInErrorMessage,
  validateSiweConfig,
} from '../src/client/universe-auth.js';
import {
  BUNDLE_PAGE_SIZE,
  bundleDisplayName,
  loadReplayDetailCycle,
  parseRetryAfter,
  startSerializedPolling,
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

const bundle = (id: string) => ({
  bundle_id: id,
  latest_run_id: '20260927T120000.000000Z',
  sport: 'esports',
  game: 'counter_strike_2',
  topology: 'best_of_series',
  participants: ['Alpha', 'Beta'],
  activation_at: '2026-09-27T12:00:00Z',
  capture_start_at: '2026-09-27T11:00:00Z',
  first_selected_at: '2026-09-27T10:00:00Z',
  last_selected_at: '2026-09-27T11:00:00Z',
  occurrence_count: 1,
  venues: ['kalshi', 'polymarket'],
  target_count: 2,
  lifecycle: 'retired',
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

test('Universe REST clients share one serialized request-start coordinator', async () => {
  let active = 0;
  let maximum = 0;
  const starts: number[] = [];
  const fetchImpl = (async (input) => {
    active += 1;
    maximum = Math.max(maximum, active);
    starts.push(Date.now());
    await new Promise((resolve) => setTimeout(resolve, 3));
    active -= 1;
    return new URL(String(input)).pathname.endsWith('/strategies')
      ? response({ version: 1, strategies: [], limits: [] })
      : response({ jobs: [], next_cursor: null });
  }) as typeof fetch;
  const first = new UniverseClient({
    baseUrl: 'https://rest-wide.example',
    fetch: fetchImpl,
    requestIntervalMs: 8,
  });
  const second = new UniverseClient({
    baseUrl: 'https://rest-wide.example',
    fetch: fetchImpl,
    requestIntervalMs: 8,
  });
  await Promise.all([first.jobs(), second.strategies()]);
  assert.equal(maximum, 1);
  assert.equal(starts.length, 2);
  assert.ok(starts[1] - starts[0] >= 8);
});

test('a REST-wide 429 cooldown delays the next route without retrying it', async () => {
  const starts: number[] = [];
  const api = new UniverseClient({
    baseUrl: 'https://rest-cooldown.example',
    requestIntervalMs: 10,
    fetch: (async (input) => {
      starts.push(Date.now());
      return new URL(String(input)).pathname.endsWith('/jobs')
        ? response({ error: 'rate limit exceeded' }, 429, {
            'retry-after': '0',
          })
        : response({ version: 1, strategies: [], limits: [] });
    }) as typeof fetch,
  });
  await assert.rejects(api.jobs(), UniverseApiError);
  await api.strategies();
  assert.equal(starts.length, 2);
  assert.ok(starts[1] - starts[0] >= 9);
});

test('a stale route aborts while queued in the shared REST coordinator', async () => {
  let finishFirst!: () => void;
  let fetches = 0;
  const api = new UniverseClient({
    baseUrl: 'https://rest-abort.example',
    requestIntervalMs: 1,
    fetch: (async (input) => {
      fetches += 1;
      if (new URL(String(input)).pathname.endsWith('/jobs'))
        await new Promise<void>((resolve) => {
          finishFirst = resolve;
        });
      return new URL(String(input)).pathname.endsWith('/strategies')
        ? response({ version: 1, strategies: [], limits: [] })
        : response({ jobs: [], next_cursor: null });
    }) as typeof fetch,
  });
  const first = api.jobs();
  while (!finishFirst) await new Promise((resolve) => setTimeout(resolve, 1));
  const controller = new AbortController();
  const queued = api.strategies(controller.signal);
  controller.abort();
  await assert.rejects(
    queued,
    (error) => error instanceof Error && error.name === 'AbortError',
  );
  finishFirst();
  await first;
  assert.equal(fetches, 1);
});

test('initial SIWE 401 does not expire a session that never existed', async () => {
  let revoked = 0;
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async () =>
      response({ error: 'authentication failed' }, 401)) as typeof fetch,
    onUnauthorized: () => {
      revoked += 1;
    },
  });
  await assert.rejects(
    api.signIn('message', 'signature'),
    (error) => error instanceof UniverseApiError && error.status === 401,
  );
  assert.equal(revoked, 0);
});

test('bundle discovery paginates serially and reduces its page size after 413', async () => {
  const requests: Array<{ cursor: string | null; limit: string | null }> = [];
  let active = 0;
  let maximum = 0;
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async (input) => {
      active += 1;
      maximum = Math.max(maximum, active);
      const url = new URL(String(input));
      const cursor = url.searchParams.get('cursor');
      const limit = url.searchParams.get('limit');
      requests.push({ cursor, limit });
      try {
        if (cursor === null)
          return response({
            bundles: [bundle('bundle-a')],
            next_cursor: 'next',
          });
        if (limit === String(BUNDLE_PAGE_SIZE))
          return response({ error: 'bundle context exceeds limit' }, 413);
        return response({ bundles: [bundle('bundle-b')], next_cursor: null });
      } finally {
        active -= 1;
      }
    }) as typeof fetch,
  });
  const result = await api.bundles(undefined, { paceMs: 0 });
  assert.deepEqual(
    result.bundles.map((item) => item.bundle_id),
    ['bundle-a', 'bundle-b'],
  );
  assert.deepEqual(requests, [
    { cursor: null, limit: '5' },
    { cursor: 'next', limit: '5' },
    { cursor: 'next', limit: '2' },
  ]);
  assert.equal(maximum, 1);
});

test('bundle discovery reports an actionable 413 at one bundle per page', async () => {
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async () =>
      response({ error: 'bundle context exceeds limit' }, 413)) as typeof fetch,
  });
  await assert.rejects(
    api.bundles(undefined, { paceMs: 0 }),
    (error) =>
      error instanceof UniverseApiError &&
      error.status === 413 &&
      error.message.includes('one bundle per page'),
  );
});

test('bundle discovery keeps safe earlier pages and its resume cursor after a later bundle is oversized', async () => {
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async (input) => {
      const cursor = new URL(String(input)).searchParams.get('cursor');
      return cursor === null
        ? response({ bundles: [bundle('bundle-a')], next_cursor: 'blocked' })
        : response({ error: 'bundle context exceeds limit' }, 413);
    }) as typeof fetch,
  });
  const result = await api.bundles(undefined, { paceMs: 0 });
  assert.deepEqual(
    result.bundles.map((item) => item.bundle_id),
    ['bundle-a'],
  );
  assert.match(
    result.warning ?? '',
    /Showing the bundles discovered before it/,
  );
  assert.equal(result.next_cursor, 'blocked');
});

test('bundle discovery stops on 429, preserves pages, and resumes at the blocked cursor', async () => {
  const requests: Array<string | null> = [];
  let allowResume = false;
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async (input) => {
      const cursor = new URL(String(input)).searchParams.get('cursor');
      requests.push(cursor);
      if (cursor === null)
        return response({
          bundles: [bundle('bundle-a')],
          next_cursor: 'page-b',
        });
      if (!allowResume)
        return response({ error: 'rate limit exceeded' }, 429, {
          'retry-after': '7',
        });
      return response({
        bundles: [bundle('bundle-b')],
        next_cursor: null,
      });
    }) as typeof fetch,
  });
  const partial = await api.bundles(undefined, { paceMs: 0 });
  assert.deepEqual(
    partial.bundles.map((item) => item.bundle_id),
    ['bundle-a'],
  );
  assert.equal(partial.next_cursor, 'page-b');
  assert.equal(partial.retry_after_seconds, 7);
  assert.match(partial.warning ?? '', /rate limit/i);

  allowResume = true;
  const resumed = await api.bundles(undefined, {
    cursor: partial.next_cursor,
    paceMs: 0,
  });
  assert.deepEqual(
    resumed.bundles.map((item) => item.bundle_id),
    ['bundle-b'],
  );
  assert.deepEqual(requests, [null, 'page-b', 'page-b']);
});

test('one detail refresh cycle reads job then one event page without overlap', async () => {
  const paths: string[] = [];
  let active = 0;
  let maximum = 0;
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
    fetch: (async (input) => {
      active += 1;
      maximum = Math.max(maximum, active);
      const path = new URL(String(input)).pathname;
      paths.push(path);
      await new Promise((resolve) => setTimeout(resolve, 1));
      active -= 1;
      return path.endsWith('/events')
        ? response({ events: [], next_cursor: null })
        : response({ ...job, request });
    }) as typeof fetch,
  });
  const result = await loadReplayDetailCycle(api, job.job_id);
  assert.equal(result.detail.job_id, job.job_id);
  assert.deepEqual(paths, [
    `/v1/replay/jobs/${job.job_id}`,
    `/v1/replay/jobs/${job.job_id}/events`,
  ]);
  assert.equal(maximum, 1);
});

test('job and event cursors use the server cursor query contract', async () => {
  const paths: string[] = [];
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async (input) => {
      const url = new URL(String(input));
      paths.push(`${url.pathname}${url.search}`);
      return url.pathname.endsWith('/events')
        ? response({ events: [], next_cursor: null })
        : response({ jobs: [], next_cursor: null });
    }) as typeof fetch,
  });
  await api.jobs('queued', 'job-cursor', undefined, 25);
  await api.jobEvents(job.job_id, 'event-cursor', undefined, 50);
  assert.deepEqual(paths, [
    '/v1/replay/jobs?limit=25&status=queued&cursor=job-cursor',
    `/v1/replay/jobs/${job.job_id}/events?limit=50&cursor=event-cursor`,
  ]);
});

test('serialized polling never overlaps and aborts the active request', async () => {
  let active = 0;
  let maximum = 0;
  let calls = 0;
  let observedAbort = false;
  let finishFirst: (() => void) | undefined;
  const stop = startSerializedPolling(async (signal) => {
    calls += 1;
    active += 1;
    maximum = Math.max(maximum, active);
    await new Promise<void>((resolve) => {
      if (calls === 1) finishFirst = resolve;
      signal.addEventListener(
        'abort',
        () => {
          observedAbort = true;
          resolve();
        },
        { once: true },
      );
    });
    active -= 1;
  }, 2);
  while (!finishFirst) await new Promise((resolve) => setTimeout(resolve, 1));
  finishFirst();
  for (let attempt = 0; calls < 2 && attempt < 100; attempt += 1)
    await new Promise((resolve) => setTimeout(resolve, 1));
  stop();
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(maximum, 1);
  assert.ok(calls >= 2);
  assert.equal(observedAbort, true);
});

test('polling pauses while hidden, resumes once, and terminal state stops it', async () => {
  let paused = true;
  let resume: (() => void) | undefined;
  let calls = 0;
  const stop = startSerializedPolling(
    async () => {
      calls += 1;
      return false;
    },
    1,
    {
      isPaused: () => paused,
      subscribe: (next) => {
        resume = next;
        return () => undefined;
      },
    },
  );
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(calls, 0);
  paused = false;
  resume?.();
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(calls, 1);
  stop();
});

test('polling honors Retry-After and backs off after other errors', async () => {
  const delays: number[] = [];
  let calls = 0;
  const stop = startSerializedPolling(
    async () => {
      calls += 1;
      if (calls === 1) throw new UniverseApiError('slow down', 429, 4);
      if (calls === 2) throw new Error('network');
      return false;
    },
    1_000,
    {
      random: () => 0.5,
      schedule: (callback, delay) => {
        delays.push(delay);
        return setTimeout(callback, 0);
      },
      clearSchedule: clearTimeout,
    },
  );
  for (let attempt = 0; calls < 3 && attempt < 100; attempt += 1)
    await new Promise((resolve) => setTimeout(resolve, 1));
  stop();
  assert.deepEqual(delays.slice(1), [4_000, 4_000]);
});

test('polling jitter cannot exceed the backoff cap', async () => {
  const delays: number[] = [];
  let calls = 0;
  const stop = startSerializedPolling(
    async () => {
      calls += 1;
      if (calls === 8) return false;
      throw new Error('network');
    },
    1_000,
    {
      random: () => 1,
      schedule: (callback, delay) => {
        delays.push(delay);
        return setTimeout(callback, 0);
      },
      clearSchedule: clearTimeout,
    },
  );
  for (let attempt = 0; calls < 8 && attempt < 100; attempt += 1)
    await new Promise((resolve) => setTimeout(resolve, 1));
  stop();
  assert.equal(Math.max(...delays), 60_000);
});

test('StrictMode-style immediate cleanup prevents a duplicate initial poll', async () => {
  let calls = 0;
  const task = async () => {
    calls += 1;
    return false;
  };
  const stopFirst = startSerializedPolling(task, 1);
  stopFirst();
  const stopSecond = startSerializedPolling(task, 1);
  await new Promise((resolve) => setTimeout(resolve, 5));
  stopSecond();
  assert.equal(calls, 1);
});

test('stopping polling aborts a stale route request', async () => {
  let aborted = false;
  const stop = startSerializedPolling(
    (signal) =>
      new Promise<boolean>((resolve) => {
        signal.addEventListener(
          'abort',
          () => {
            aborted = true;
            resolve(false);
          },
          { once: true },
        );
      }),
    1,
  );
  await new Promise((resolve) => setTimeout(resolve, 2));
  stop();
  await new Promise((resolve) => setTimeout(resolve, 2));
  assert.equal(aborted, true);
});

test('bundle display name prefers authoritative participants and falls back to ID', () => {
  assert.equal(
    bundleDisplayName(['A very long participant', 'Opponent'], 'opaque-id'),
    'A very long participant vs Opponent',
  );
  assert.equal(bundleDisplayName([], 'opaque-id'), 'opaque-id');
});

test('detail title resolves from one bounded Universe lookup and is cached', async () => {
  const paths: string[] = [];
  const source = {
    manifest_key: 'manifest',
    manifest_sha256: 'a'.repeat(64),
    report_key: 'report',
    report_sha256: 'b'.repeat(64),
  };
  const selection = {
    run_id: '20260927T120000.000000Z',
    generated_at: '2026-09-27T12:00:00Z',
    bundle_id: 'opaque-id',
    occurrence_kind: 'complete',
    continuity_selected: true,
    continuity_disposition: null,
    sport: 'esports',
    game: 'counter_strike_2',
    topology: 'best_of_series',
    activation_at: '2026-09-27T13:00:00Z',
    capture_start_at: '2026-09-27T12:00:00Z',
    retirement: null,
    source,
    origin: {
      ...source,
      run_id: '20260927T120000.000000Z',
      generated_at: '2026-09-27T12:00:00Z',
    },
  };
  const api = new UniverseClient({
    baseUrl: 'https://universe.example',
    fetch: (async (input) => {
      const path = `${new URL(String(input)).pathname}${new URL(String(input)).search}`;
      paths.push(path);
      if (path.includes('/history'))
        return response({
          selections: [selection],
          sort: 'selected',
          next_cursor: null,
        });
      return response({
        ...selection,
        context: {
          bundle_id: 'opaque-id',
          sport: 'esports',
          game: 'counter_strike_2',
          topology: 'best_of_series',
          participants: ['Alpha', 'Beta'],
          participant_keys: ['alpha', 'beta'],
          activation_at: selection.activation_at,
          capture_start_at: selection.capture_start_at,
          event_refs: [],
          markets: [],
          targets: [],
          relationships: [],
        },
      });
    }) as typeof fetch,
  });
  const first = await api.bundleIdentity('opaque-id', undefined, 0);
  const second = await api.bundleIdentity('opaque-id', undefined, 0);
  assert.equal(
    bundleDisplayName(first.participants, first.bundle_id),
    'Alpha vs Beta',
  );
  assert.deepEqual(second, first);
  assert.equal(paths.length, 2);
});

test('strategy config preserves catalogue keys and typed JSON values', () => {
  assert.deepEqual(
    parseStrategyConfig(['threshold', 'enabled'], {
      threshold: '0.75',
      enabled: 'true',
    }),
    { config: { threshold: 0.75, enabled: true }, errors: {} },
  );
  assert.deepEqual(
    parseStrategyConfig(['threshold'], { threshold: 'not-json' }).errors,
    { threshold: 'Enter a valid JSON value.' },
  );
});

test('new replay validation clears corrected fields and gates the summary', () => {
  const form = {
    bundleId: '',
    marketMode: 'all' as const,
    selectedMarkets: [] as string[],
    strategy: '',
  };
  let validation = validateReplayDraft(form, '', [], {});
  assert.deepEqual(Object.keys(validation.errors).sort(), [
    'bundleId',
    'limits',
    'strategy',
  ]);
  assert.equal(
    replaySummaryHeading(validation.errors),
    'Complete your request',
  );

  form.bundleId = 'bundle-a';
  validation = validateReplayDraft(form, '', [], {});
  assert.equal(validation.errors.bundleId, undefined);
  assert.deepEqual(Object.keys(validation.errors).sort(), [
    'limits',
    'strategy',
  ]);

  form.strategy = 'configured';
  validation = validateReplayDraft(form, '', ['threshold'], {});
  assert.equal(validation.errors.strategy, undefined);
  assert.equal(validation.errors['config.threshold'], 'Enter a JSON value.');

  validation = validateReplayDraft(form, '', ['threshold'], {
    threshold: '0.5',
  });
  assert.deepEqual(validation.errors, {
    limits: 'Choose a backend run preset.',
  });

  validation = validateReplayDraft(form, 'small', ['threshold'], {
    threshold: '0.5',
  });
  assert.deepEqual(validation, { errors: {}, config: { threshold: 0.5 } });
  assert.equal(replaySummaryHeading(validation.errors), 'Ready to replay');
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
  const pending = api.jobs(undefined, null, controller.signal);
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
  const address = normalizeWalletAddress(
    '0x7472005ed1e68c8a82833cb5b251790ea6c0fb58',
  );
  assert.equal(
    buildSiweMessage(
      address,
      '0123456789abcdef0123456789abcdef',
      '2026-09-27T12:00:00.000Z',
      validateSiweConfig({
        domain: 'prediction-indexer-targeter-ui-b8xg.vercel.app',
        uri: 'https://prediction-indexer-targeter-ui-b8xg.vercel.app/',
        statement: 'Sign in to Prediction Indexer.',
        chainId: 1,
      }),
    ),
    'prediction-indexer-targeter-ui-b8xg.vercel.app wants you to sign in with your Ethereum account:\n0x7472005Ed1e68c8A82833cB5B251790eA6C0FB58\n\nSign in to Prediction Indexer.\n\nURI: https://prediction-indexer-targeter-ui-b8xg.vercel.app/\nVersion: 1\nChain ID: 1\nNonce: 0123456789abcdef0123456789abcdef\nIssued At: 2026-09-27T12:00:00.000Z',
  );
});

test('SIWE configuration rejects a non-canonical URI and explains server mismatch', () => {
  assert.throws(
    () =>
      validateSiweConfig({
        domain: 'prediction-indexer-targeter-ui-b8xg.vercel.app',
        uri: 'https://prediction-indexer-targeter-ui-b8xg.vercel.app',
        statement: 'Sign in to Prediction Indexer.',
        chainId: 1,
      }),
    /canonical UI domain and URI.*trailing slash/,
  );
  assert.match(
    signInErrorMessage(new UniverseApiError('unauthorized', 401)),
    /SIWE domain\/URI build configuration may differ from Universe/,
  );
});

test('fixtures are build-gated and focus/search/layout regressions stay fixed', async () => {
  const [source, css] = await Promise.all([
    readFile(new URL('../src/client/replay.tsx', import.meta.url), 'utf8'),
    readFile(new URL('../src/client/style.css', import.meta.url), 'utf8'),
  ]);
  assert.match(source, /VITE_REPLAY_FIXTURES === 'true'/);
  assert.match(source, /if \(opener\?\.isConnected\) opener\.focus\(\)/);
  assert.doesNotMatch(source, /autoFocus/);
  assert.match(source, /\{filtered\.length\}/);
  assert.match(source, /Load older jobs/);
  assert.match(source, /Load more history/);
  assert.doesNotMatch(source, /jobPages\(/);
  assert.match(source, /aria-label="Copy bundle ID"/);
  assert.match(source, /navigator\.clipboard[\s\S]*\.writeText\(job\.bundle\)/);
  assert.match(
    css,
    /grid-template-columns: minmax\(0, 1fr\) 120px 124px 80px 12px/,
  );
});
