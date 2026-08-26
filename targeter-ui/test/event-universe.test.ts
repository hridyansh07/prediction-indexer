import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import express from 'express';
import {
  createEventUniverseRouter,
  EventUniverseClient,
  universePublicFailure,
  validateCadence,
  validateUniverseQuery,
} from '../src/server/event-universe.js';
import {
  occurrenceExplanation,
  retirementExplanation,
  universeFilterQuery,
} from '../src/client/event-universe-view-model.js';
import { targeterCadenceNeeded } from '../src/client/app-routing.js';
import {
  cadenceRunEmptyMessage,
  cadenceStatusLabel,
} from '../src/client/cadence-view-model.js';
import { handleEventUniverseProxy } from '../../api/event-universe-proxy.js';
import type {
  CadenceFreshnessState,
  UniverseCadence,
  UniverseCadenceRun,
  UniverseSelection,
  UniverseSelectionDetail,
} from '../src/event-universe.js';

const sha = 'a'.repeat(64);
const source = {
  manifest_key: 'targeter-v2/runs/date=2026-08-20/run=run/run_manifest.json',
  manifest_sha256: sha,
  report_key:
    'targeter-v2/runs/date=2026-08-20/run=run/selection_report.json.zst',
  report_sha256: sha,
};
const selection = (
  overrides: Partial<UniverseSelection> = {},
): UniverseSelection => ({
  run_id: '20260820T120000.000001Z',
  generated_at: '2026-08-20T12:00:00Z',
  bundle_id: 'bundle alpha',
  occurrence_kind: 'complete',
  continuity_selected: true,
  continuity_disposition: 'held_current_candidate',
  sport: 'esports',
  game: 'counter_strike_2',
  topology: 'series',
  activation_at: '2026-08-20T14:00:00Z',
  capture_start_at: '2026-08-20T13:00:00Z',
  retirement: null,
  source,
  origin: {
    ...source,
    run_id: '20260820T120000.000001Z',
    generated_at: '2026-08-20T12:00:00Z',
  },
  ...overrides,
});
const detail = (
  overrides: Partial<UniverseSelectionDetail> = {},
): UniverseSelectionDetail => ({
  ...selection(),
  context: {
    bundle_id: 'bundle alpha',
    sport: 'esports',
    game: 'counter_strike_2',
    topology: 'series',
    participants: ['Alpha', 'Beta'],
    participant_keys: ['alpha', 'beta'],
    activation_at: '2026-08-20T14:00:00Z',
    capture_start_at: '2026-08-20T13:00:00Z',
    event_refs: ['kalshi:event-a', 'polymarket:event-b'],
    markets: [
      { target_id: 'kalshi:market-a', venue: 'kalshi', selected: true },
      {
        target_id: 'polymarket:market-b',
        venue: 'polymarket',
        selected: true,
      },
    ],
    targets: [
      {
        target_id: 'kalshi:market-a',
        venue: 'kalshi',
        canonical_class: 'esports.series_moneyline',
        source_ref: 'kalshi:event-a',
        subscription_ids: ['asset-a'],
      },
      {
        target_id: 'polymarket:market-b',
        venue: 'polymarket',
        canonical_class: 'esports.series_moneyline',
        source_ref: 'polymarket:event-b',
        subscription_ids: ['asset-b-yes', 'asset-b-no'],
      },
    ],
    relationships: [
      {
        left: 'kalshi:market-a#claim=0',
        right: 'polymarket:market-b#claim=0',
        relationship: 'IDENTITY',
        scope: 'series',
        left_venue: 'kalshi',
        right_venue: 'polymarket',
        coverage: 'EXHAUSTIVE',
      },
    ],
  },
  ...overrides,
});
const run = (
  overrides: Partial<UniverseCadenceRun> = {},
): UniverseCadenceRun => ({
  run_id: '20260820T120000.000001Z',
  generated_at: '2026-08-20T12:00:00Z',
  generated_at_ns: 1,
  input_complete: true,
  report_version: 3,
  strategy_version: 2,
  manifest_key: source.manifest_key,
  manifest_sha256: sha,
  manifest_byte_length: 100,
  report_key: source.report_key,
  report_sha256: sha,
  report_byte_length: 80,
  report_decoded_sha256: sha,
  report_decoded_byte_length: 120,
  projection_version: 1,
  projection_sha256: sha,
  projection_row_count: 1,
  indexed_at_ns: 2,
  catalogs: [],
  discovery_failures: {},
  counts: {
    candidates: 1,
    eligible: 1,
    selected: 1,
    rejected: 0,
    retained: 0,
    retired: 0,
  },
  reason_summaries: {
    candidate_rejections: {},
    allocation_rejections: {},
    continuity_dispositions: {},
  },
  match_rejections: [],
  candidates: [],
  selected_targets: {},
  budget_used: {},
  continuity: { bundles: [], retained_bundle_ids: [], dispositions: {} },
  diagnostics: {
    continuity: [],
    continuity_degraded_base_run_id: null,
    target_records: {},
  },
  selections: [detail()],
  ...overrides,
});
const cadence = (
  state: CadenceFreshnessState = 'current',
  runs: UniverseCadenceRun[] = [run()],
): UniverseCadence => ({
  cadence_projection_version: 1,
  observed_at: '2026-08-20T12:05:18Z',
  freshness: {
    state,
    expected_run_seconds: 600,
    latest_run_age_seconds: state === 'unavailable' ? null : 318,
    latest_indexed_at: state === 'unavailable' ? null : '2026-08-20T12:01:00Z',
  },
  runs,
});
const json = (value: unknown, init: ResponseInit = {}) =>
  new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'content-type': 'application/json', ...init.headers },
    ...init,
  });

test('proxy allowlists one encoded value per supported query field', async () => {
  let requested = '';
  let authorization = '';
  const client = new EventUniverseClient({
    baseUrl: 'https://universe.internal/root/',
    authorization: 'Bearer server-secret',
    fetch: (async (input, init) => {
      requested = String(input);
      authorization = new Headers(init?.headers).get('authorization') ?? '';
      return json({ selections: [], sort: 'selected', next_cursor: null });
    }) as typeof fetch,
  });
  await client.bundleHistory(
    'bundle / unicode ✓'.replace('/', '-'),
    new URLSearchParams({
      venue: 'venue with spaces',
      sort: 'selected',
      cursor: 'opaque+/=',
    }),
  );
  const url = new URL(requested);
  assert.equal(
    decodeURIComponent(url.pathname),
    '/root/v1/bundles/bundle - unicode ✓/history',
  );
  assert.equal(url.searchParams.get('venue'), 'venue with spaces');
  assert.equal(url.searchParams.get('cursor'), 'opaque+/=');
  assert.equal(authorization, 'Bearer server-secret');

  assert.throws(() =>
    validateUniverseQuery(
      new URLSearchParams('venue=kalshi&venue=polymarket'),
      new Set(['venue']),
    ),
  );
  assert.throws(() =>
    validateUniverseQuery(
      new URLSearchParams({ raw_segments: 'true' }),
      new Set(['venue']),
    ),
  );
});

test('strictly validates selection detail and rejects schema drift', async () => {
  const valid = detail();
  const client = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () => json(valid)) as typeof fetch,
  });
  const result = await client.selection(valid.run_id, valid.bundle_id);
  assert.deepEqual(result.context.participants, ['Alpha', 'Beta']);
  assert.equal(result.context.targets[1].subscription_ids.length, 2);
  assert.equal(result.origin.manifest_sha256, sha);

  const invalid: any = structuredClone(valid);
  invalid.ended_at = '2026-08-20T15:00:00Z';
  const rejecting = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () => json(invalid)) as typeof fetch,
  });
  await assert.rejects(() =>
    rejecting.selection(valid.run_id, valid.bundle_id),
  );
});

test('bounds timeout, response bytes, content type, and upstream status', async () => {
  const timedOut = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    timeoutMs: 5,
    fetch: ((_input, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () =>
          reject(new DOMException('token must not leak', 'AbortError')),
        );
      })) as typeof fetch,
  });
  const timeout = await timedOut
    .cadence(new URLSearchParams())
    .catch((error) => error);
  assert.deepEqual(universePublicFailure(timeout), {
    status: 504,
    body: { error: 'Event Universe unavailable' },
  });

  const oversize = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    maxResponseBytes: 10,
    fetch: (async () =>
      json({ secret: 'upstream-body-must-not-leak' })) as typeof fetch,
  });
  const tooLarge = await oversize
    .cadence(new URLSearchParams())
    .catch((error) => error);
  assert.deepEqual(universePublicFailure(tooLarge), {
    status: 502,
    body: { error: 'Event Universe unavailable' },
  });

  for (const response of [
    new Response('not json', {
      status: 200,
      headers: { 'content-type': 'text/plain' },
    }),
    json({ error: 'private upstream failure' }, { status: 500 }),
  ]) {
    const failed = new EventUniverseClient({
      baseUrl: 'https://universe.internal',
      fetch: (async () => response) as typeof fetch,
    });
    const error = await failed
      .cadence(new URLSearchParams())
      .catch((reason) => reason);
    assert.deepEqual(universePublicFailure(error).body, {
      error: 'Event Universe unavailable',
    });
  }
});

test('view models preserve cursor filters and explain lifecycle without ended_at', () => {
  const query = universeFilterQuery(
    {
      activation_start: '2026-08-20T13:00',
      selected_end: '2026-08-21T00:00',
      venue: 'kalshi',
      sort: 'selected',
      limit: '25',
    },
    'opaque cursor',
  );
  assert.equal(query.get('activation_start'), '2026-08-20T13:00:00.000Z');
  assert.equal(query.get('selected_end'), '2026-08-21T00:00:00.000Z');
  assert.equal(query.get('cursor'), 'opaque cursor');

  assert.match(occurrenceExplanation(selection()), /Complete occurrence/);
  assert.match(
    occurrenceExplanation(
      selection({
        occurrence_kind: 'retained',
        continuity_disposition: 'retained',
      }),
    ),
    /Retained reference.*immutable origin/,
  );
  const terminal = selection({
    retirement: {
      retired_at: '2026-08-20T15:00:00Z',
      disposition: 'all_markets_terminal',
      terminal_observed_at: '2026-08-20T15:00:00Z',
      source: { ...source, run_id: 'retirement-run' },
    },
  });
  assert.match(retirementExplanation(terminal), /upper-bound observation/);
  assert.doesNotMatch(retirementExplanation(terminal), /ended_at/);
  assert.match(
    retirementExplanation(
      selection({
        retirement: {
          ...terminal.retirement!,
          disposition: 'terminal_clamp_elapsed',
          terminal_observed_at: null,
        },
      }),
    ),
    /not evidence that the match ended/,
  );
});

test('Event Universe routing is independent from Targeter cadence availability', () => {
  assert.equal(targeterCadenceNeeded('/'), false);
  assert.equal(targeterCadenceNeeded('/event-universe'), false);
  assert.equal(targeterCadenceNeeded('/operations'), true);
  assert.equal(targeterCadenceNeeded('/operations/selections'), true);
  assert.equal(targeterCadenceNeeded('/events'), false);
});

test('cadence proxy allows only a bounded limit and validates selection detail', async () => {
  let requested = '';
  let fetches = 0;
  const client = new EventUniverseClient({
    baseUrl: 'https://universe.internal/base',
    fetch: (async (input) => {
      fetches++;
      requested = String(input);
      return json(cadence());
    }) as typeof fetch,
  });
  const result = await client.cadence(new URLSearchParams({ limit: '5' }));
  assert.equal(
    requested,
    'https://universe.internal/base/v1/targeter/cadence?limit=5',
  );
  assert.equal(result.runs[0].selections[0].context.participants[0], 'Alpha');
  assert.equal(result.freshness.state, 'current');

  for (const query of [
    'limit=6',
    'limit=5&limit=4',
    'cursor=forbidden',
    'limit=0',
  ]) {
    assert.throws(() => client.cadence(new URLSearchParams(query)));
  }
  assert.equal(fetches, 1);
});

test('Express cadence proxy rejects non-GET refreshes before upstream access', async () => {
  let fetches = 0;
  const client = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () => {
      fetches++;
      return json(cadence());
    }) as typeof fetch,
  });
  const app = express();
  app.use('/api/event-universe', createEventUniverseRouter(client));
  const server = app.listen(0, '127.0.0.1');
  await new Promise<void>((resolve, reject) => {
    server.once('listening', resolve);
    server.once('error', reject);
  });
  try {
    const port = (server.address() as AddressInfo).port;
    const response = await fetch(
      `http://127.0.0.1:${port}/api/event-universe/v1/targeter/cadence?limit=5`,
      { method: 'POST' },
    );
    assert.equal(response.status, 405);
    assert.deepEqual(await response.json(), { error: 'Method not allowed' });
    assert.equal(fetches, 0);
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});

test('cadence schema and view model cover every freshness state and empty runs', () => {
  for (const state of ['current', 'late'] as const) {
    assert.equal(validateCadence(cadence(state)).freshness.state, state);
    assert.equal(cadenceStatusLabel(state), `CADENCE ${state.toUpperCase()}`);
  }
  const unavailable = cadence('unavailable', []);
  assert.deepEqual(validateCadence(unavailable).runs, []);
  assert.equal(cadenceStatusLabel('unavailable'), 'CADENCE UNAVAILABLE');

  const completeEmpty = run({ selections: [], projection_row_count: 0 });
  assert.match(cadenceRunEmptyMessage(completeEmpty), /complete indexed run/);
  assert.match(
    cadenceRunEmptyMessage(
      run({ input_complete: false, selections: [], projection_row_count: 0 }),
    ),
    /incomplete input/,
  );

  assert.throws(() =>
    validateCadence({
      ...cadence(),
      unexpected_decision_summary: {},
    }),
  );
  assert.throws(() =>
    validateCadence({
      ...cadence('unavailable', []),
      freshness: {
        ...cadence('unavailable', []).freshness,
        state: 'live',
      },
    }),
  );
  assert.throws(() =>
    validateCadence({
      ...cadence(),
      runs: [run({ selections: [detail({ run_id: 'wrong-run' })] })],
    }),
  );
});

test('Vercel proxy hydrates Universe only through server-side configuration', async () => {
  let upstreamUrl = '';
  let upstreamAuthorization = '';
  const response = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/selections&sort=selected&limit=25',
    ),
    {
      UNIVERSE_API_BASE_URL: 'https://universe.internal/base',
      UNIVERSE_API_AUTHORIZATION: 'Bearer server-only',
    },
    (async (input, init) => {
      upstreamUrl = String(input);
      upstreamAuthorization =
        new Headers(init?.headers).get('authorization') ?? '';
      return json({ selections: [], sort: 'selected', next_cursor: null });
    }) as typeof fetch,
  );
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    selections: [],
    sort: 'selected',
    next_cursor: null,
  });
  assert.equal(
    upstreamUrl,
    'https://universe.internal/base/v1/selections?sort=selected&limit=25',
  );
  assert.equal(upstreamAuthorization, 'Bearer server-only');
  assert.equal(response.headers.get('cache-control'), 'no-store');

  const cadenceResponse = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/targeter/cadence&limit=5',
    ),
    { UNIVERSE_API_BASE_URL: 'https://universe.internal' },
    (async (input) => {
      assert.equal(
        String(input),
        'https://universe.internal/v1/targeter/cadence?limit=5',
      );
      return json(cadence());
    }) as typeof fetch,
  );
  assert.equal(cadenceResponse.status, 200);
  assert.equal((await cadenceResponse.json()).cadence_projection_version, 1);

  const unconfigured = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/healthz',
    ),
    {},
    (async () => {
      throw new Error('must not fetch');
    }) as typeof fetch,
  );
  assert.equal(unconfigured.status, 503);

  const forbidden = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/segments',
    ),
    { UNIVERSE_API_BASE_URL: 'https://universe.internal' },
    (async () => {
      throw new Error('must not fetch');
    }) as typeof fetch,
  );
  assert.equal(forbidden.status, 404);
  assert.deepEqual(await forbidden.json(), {
    error: 'Event Universe route not found',
  });
});

test('cadence refresh is GET-only and Targeter UI has no direct archive dependency', async () => {
  const [client, server, packageDocument] = await Promise.all([
    readFile(new URL('../src/client/main.tsx', import.meta.url), 'utf8'),
    readFile(new URL('../src/server/index.ts', import.meta.url), 'utf8'),
    readFile(new URL('../package.json', import.meta.url), 'utf8'),
  ]);
  assert.match(client, /\/api\/event-universe\/v1\/targeter\/cadence\?limit=5/);
  assert.match(client, /method: 'GET'/);
  for (const removed of [
    '/api/refresh',
    '/api/snapshot',
    'TARGETER_UI_S3',
    'RustV1Decoder',
    'S3ReadOnlyObjectStore',
    'SnapshotService',
  ]) {
    assert.doesNotMatch(`${client}\n${server}`, new RegExp(removed));
  }
  const dependencies = JSON.parse(packageDocument).dependencies;
  assert.equal(
    dependencies['@prediction-indexer/read-only-object-store'],
    undefined,
  );
  assert.equal(dependencies['@prediction-indexer/rust-v1-decoder'], undefined);
});

test('Vercel builds only the client and routes Universe before the SPA fallback', async () => {
  const config = JSON.parse(
    await readFile(new URL('../../vercel.json', import.meta.url), 'utf8'),
  ) as {
    buildCommand: string;
    outputDirectory: string;
    rewrites: Array<{ source: string; destination: string }>;
  };
  assert.equal(
    config.buildCommand,
    'yarn workspace prediction-indexer-targeter-ui build:client',
  );
  assert.equal(config.outputDirectory, 'targeter-ui/dist');
  assert.deepEqual(config.rewrites, [
    {
      source: '/api/event-universe/:path*',
      destination: '/api/event-universe-proxy?__universe_path=/:path*',
    },
    { source: '/(.*)', destination: '/index.html' },
  ]);
});
