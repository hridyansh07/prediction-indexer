import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import {
  EventUniverseClient,
  universePublicFailure,
  validateUniverseQuery,
} from '../src/server/event-universe.js';
import {
  occurrenceExplanation,
  retirementExplanation,
  universeFilterQuery,
} from '../src/client/event-universe-view-model.js';
import { targeterSnapshotNeeded } from '../src/client/app-routing.js';
import { handleEventUniverseProxy } from '../../api/event-universe-proxy.js';
import type {
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
const detail = (): UniverseSelectionDetail => ({
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
  const timeout = await timedOut.health().catch((error) => error);
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
  const tooLarge = await oversize.health().catch((error) => error);
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
    const error = await failed.health().catch((reason) => reason);
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

test('Event Universe routing is independent from Targeter snapshot availability', () => {
  assert.equal(targeterSnapshotNeeded('/'), false);
  assert.equal(targeterSnapshotNeeded('/event-universe'), false);
  assert.equal(targeterSnapshotNeeded('/operations'), true);
  assert.equal(targeterSnapshotNeeded('/operations/events'), true);
  assert.equal(targeterSnapshotNeeded('/operations/config'), true);
  assert.equal(targeterSnapshotNeeded('/events'), false);
});

test('Vercel proxy hydrates Universe only through server-side configuration', async () => {
  let upstreamUrl = '';
  let upstreamAuthorization = '';
  const response = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/selections&sort=selected&limit=25',
    ),
    {
      TARGETER_UI_EVENT_UNIVERSE_URL: 'https://universe.internal/base',
      TARGETER_UI_EVENT_UNIVERSE_AUTHORIZATION: 'Bearer server-only',
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
    { TARGETER_UI_EVENT_UNIVERSE_URL: 'https://universe.internal' },
    (async () => {
      throw new Error('must not fetch');
    }) as typeof fetch,
  );
  assert.equal(forbidden.status, 404);
  assert.deepEqual(await forbidden.json(), {
    error: 'Event Universe route not found',
  });
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
