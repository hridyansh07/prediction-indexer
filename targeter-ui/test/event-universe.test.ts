import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import express from 'express';
import {
  createEventUniverseRouter,
  EventUniverseClient,
  universePublicFailure,
  validateTargeterRun,
  validateUniverseQuery,
} from '../src/server/event-universe.js';
import {
  boundedRenderPage,
  occurrenceExplanation,
  retirementExplanation,
  universeFilterQuery,
} from '../src/client/event-universe-view-model.js';
import { universeGet } from '../src/client/universe-api.js';
import { handleEventUniverseProxy } from '../../api/event-universe-proxy.js';
import type {
  UniverseRun,
  UniverseSelection,
  UniverseSelectionDetail,
  UniverseTargeterRunDetail,
  UniverseTargeterStatus,
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
const run = (overrides: Partial<UniverseRun> = {}): UniverseRun => ({
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
  ...overrides,
});
const status = (
  overrides: Partial<UniverseTargeterStatus> = {},
): UniverseTargeterStatus => ({
  status_projection_version: 1,
  observed_at: '2026-08-20T12:05:18Z',
  freshness: {
    state: 'current',
    expected_run_seconds: 600,
    latest_run_age_seconds: 318,
    latest_indexed_at: '2026-08-20T12:01:00Z',
  },
  latest_run: {
    run_id: run().run_id,
    generated_at: run().generated_at,
    input_complete: true,
    indexed_at: '2026-08-20T12:01:00Z',
  },
  current_complete_run: {
    run_id: run().run_id,
    generated_at: run().generated_at,
    input_complete: true,
    indexed_at: '2026-08-20T12:01:00Z',
  },
  current_complete_summary: {
    selected_bundles: 1,
    selected_targets: 2,
    venues: ['kalshi', 'polymarket'],
  },
  ...overrides,
});
const normalizedEventSummary = () => ({
  event_id: 'event-alpha',
  sport: 'esports',
  game: 'counter_strike_2',
  topology: 'series',
  activation_at: '2026-08-20T14:00:00Z',
  participants: ['Alpha', 'Beta'],
  participant_keys: ['alpha', 'beta'],
  first_seen_run_id: run().run_id,
  last_seen_run_id: run().run_id,
});
const normalizedRun = (): UniverseTargeterRunDetail => ({
  run: {
    run_id: run().run_id,
    generated_at: run().generated_at,
    input_complete: true,
    indexed_at: '2026-08-20T12:01:00Z',
  },
  source,
  counts: {
    candidates: 1,
    eligible: 1,
    selected_events: 1,
    selected_markets: 1,
    relations: 1,
  },
  decisions: [
    {
      event_id: 'event-alpha',
      bundle_id: 'bundle alpha',
      eligible: true,
      selected: true,
      score: 10,
      score_components: { venue_coverage: 1 },
      rejection_reasons: [],
      allocation_rejection: null,
      admission: {
        combined_moneyline_volume_usd: 30_000,
        minimum_moneyline_volume_usd: 25_000,
        moneyline_volume_usd_by_venue: { kalshi: 30_000 },
        moneyline_volume_usd_coverage: {},
      },
      market_exclusions: {},
      eligible_market_ids: ['market-alpha'],
    },
  ],
  events: [normalizedEventSummary()],
  selected_markets: [
    {
      event_id: 'event-alpha',
      bundle_id: 'bundle alpha',
      venue: 'kalshi',
      venue_market_id: 'K-ALPHA',
      market_id: 'market-alpha',
      market_template_version: 1,
      outcome_space_version: 1,
      canonical_class: 'esports.series_moneyline',
      continuity_score: 10,
      selection_reason: 'selected',
      origin_run_id: run().run_id,
    },
  ],
  relations: [
    {
      relation_id: 1,
      relation_type: 'IDENTITY',
      event_id: 'event-alpha',
      scope: 'series',
      coverage: 'EXHAUSTIVE',
      generation_version: 1,
      canonical_hash: sha,
    },
  ],
});
const normalizedEvent = () => ({
  event: normalizedEventSummary(),
  venue_events: [
    {
      venue: 'kalshi',
      venue_event_id: 'event-a',
      title: 'Alpha vs Beta',
      league: null,
      status: 'open',
      source_ref: 'kalshi:event-a',
      format: null,
      fragment_type: null,
      first_seen_run_id: run().run_id,
      last_seen_run_id: run().run_id,
    },
  ],
  markets: [
    {
      market_id: 'market-alpha',
      market_template_version: 1,
      outcome_space_version: 1,
      event_id: 'event-alpha',
      canonical_class: 'esports.series_moneyline',
      market_type: 'moneyline',
      scope: 'series',
      parameters: {},
      first_seen_run_id: run().run_id,
      last_seen_run_id: run().run_id,
      venue_market_count: 1,
      venues: ['kalshi'],
    },
  ],
  relations: [
    {
      relation_id: 1,
      relation_type: 'IDENTITY',
      scope: 'series',
      coverage: 'EXHAUSTIVE',
      generation_version: 1,
      canonical_hash: sha,
    },
  ],
  observations: [
    {
      run_id: run().run_id,
      generated_at: run().generated_at,
      bundle_id: 'bundle alpha',
    },
  ],
});
const relationshipTypes = () => ({
  relationship_type_catalog_version: 1,
  types: [
    { type: 'IDENTITY', directed: false, member_roles: ['member'] },
    { type: 'IMPLICATION', directed: true, member_roles: ['left', 'right'] },
    {
      type: 'REVERSE_IMPLICATION',
      directed: true,
      member_roles: ['left', 'right'],
    },
    { type: 'MUTUAL_EXCLUSION', directed: false, member_roles: ['member'] },
    { type: 'OVERLAP', directed: false, member_roles: ['member'] },
  ],
});
const normalizedMarket = () => ({
  market: normalizedEvent().markets[0],
  venue_markets: [
    {
      venue: 'kalshi',
      venue_market_id: 'K-ALPHA',
      venue_event_id: 'event-a',
      event_id: 'event-alpha',
      market_id: 'market-alpha',
      market_template_version: 1,
      outcome_space_version: 1,
      canonical_class: 'esports.series_moneyline',
      market_type: 'moneyline',
      scope: 'series',
      title: 'Alpha vs Beta',
      parameters: {},
      subscription_ids: ['K-ALPHA'],
      outcome_labels: ['Alpha', 'Beta'],
      status: 'open',
      accepting_orders: true,
      rules_hash: null,
      rule_template_id: null,
      source_ref: 'kalshi:event-a',
      created_at: null,
      volume_24h: null,
      volume_total: 30_000,
      volume_total_usd: 30_000,
      liquidity: null,
      first_seen_run_id: run().run_id,
      last_seen_run_id: run().run_id,
    },
  ],
  selections: [
    {
      run_id: run().run_id,
      generated_at: run().generated_at,
      bundle_id: 'bundle alpha',
      venue: 'kalshi',
      venue_market_id: 'K-ALPHA',
      continuity_score: 10,
      selection_reason: 'selected',
      origin_run_id: run().run_id,
    },
  ],
  relations: normalizedEvent().relations,
});
const normalizedRelation = (claimKey: unknown = '') => ({
  relation: {
    relation_id: 1,
    relation_type: 'IDENTITY',
    generation_version: 1,
    canonical_hash: sha,
  },
  members: [
    {
      venue: 'kalshi',
      venue_market_id: 'K-ALPHA',
      market_id: 'market-alpha',
      market_template_version: 1,
      outcome_space_version: 1,
      claim_key: claimKey,
      role: 'member',
    },
  ],
  observations: [
    {
      run_id: run().run_id,
      generated_at: run().generated_at,
      bundle_id: 'bundle alpha',
      event_id: 'event-alpha',
      scope: 'series',
      coverage: 'EXHAUSTIVE',
    },
  ],
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

test('bundle summaries are closed, grouped records', async () => {
  const bundle = {
    bundle_id: 'bundle alpha',
    latest_run_id: '20260820T120000.000001Z',
    sport: 'esports',
    game: 'counter_strike_2',
    topology: 'series',
    participants: ['Alpha', 'Beta'],
    activation_at: '2026-08-20T14:00:00Z',
    capture_start_at: '2026-08-20T13:00:00Z',
    first_selected_at: '2026-08-20T12:00:00Z',
    last_selected_at: '2026-08-20T12:10:00Z',
    occurrence_count: 2,
    venues: ['kalshi', 'polymarket'],
    target_count: 2,
    lifecycle: 'active',
  };
  const client = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () =>
      json({ bundles: [bundle], next_cursor: null })) as typeof fetch,
  });
  const result = await client.bundles(new URLSearchParams({ limit: '100' }));
  assert.equal(result.bundles[0].latest_run_id, bundle.latest_run_id);
  assert.equal(result.bundles[0].occurrence_count, 2);

  const rejecting = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () =>
      json({
        bundles: [{ ...bundle, raw_report: 'must not pass' }],
        next_cursor: null,
      })) as typeof fetch,
  });
  await assert.rejects(() =>
    rejecting.bundles(new URLSearchParams({ limit: '100' })),
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
    .status(new URLSearchParams())
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
    .status(new URLSearchParams())
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
      .status(new URLSearchParams())
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

test('status proxy allows only a bounded limit and validates selection detail', async () => {
  let requested = '';
  let fetches = 0;
  const client = new EventUniverseClient({
    baseUrl: 'https://universe.internal/base',
    fetch: (async (input) => {
      fetches++;
      requested = String(input);
      return json(
        requested.includes('/v1/targeter/runs/')
          ? normalizedRun()
          : requested.includes('/v1/events/')
            ? normalizedEvent()
            : status(),
      );
    }) as typeof fetch,
  });
  const result = await client.status(new URLSearchParams({ limit: '5' }));
  assert.equal(
    requested,
    'https://universe.internal/base/v1/targeter/status?limit=5',
  );
  assert.equal(result.current_complete_summary.selected_bundles, 1);
  assert.equal(result.current_complete_summary.selected_targets, 2);
  assert.equal(result.latest_run?.input_complete, true);
  assert.equal(result.freshness.state, 'current');

  for (const query of [
    'limit=6',
    'limit=5&limit=4',
    'cursor=forbidden',
    'limit=0',
  ]) {
    assert.throws(() => client.status(new URLSearchParams(query)));
  }
  assert.equal(fetches, 1);

  const detail = await client.targeterRun(run().run_id);
  assert.equal(
    requested,
    `https://universe.internal/base/v1/targeter/runs/${run().run_id}`,
  );
  assert.equal(detail.decisions[0].bundle_id, 'bundle alpha');
  assert.equal(detail.selected_markets[0].event_id, 'event-alpha');

  const event = await client.event('event-alpha');
  assert.equal(event.event.participants.join(' vs '), 'Alpha vs Beta');
  assert.equal(event.markets[0].market_id, 'market-alpha');
});

test('same-origin proxy dispatches every normalized collection and detail route', async () => {
  const requested: string[] = [];
  const upstreamFetch = (async (input) => {
    const url = new URL(String(input));
    requested.push(`${url.pathname}${url.search}`);
    if (url.pathname.endsWith('/v1/events'))
      return json({
        events: [
          {
            ...normalizedEventSummary(),
            venue_count: 1,
            market_count: 1,
            selected_run_count: 1,
          },
        ],
        next_cursor: 'opaque-events-cursor',
      });
    if (url.pathname.endsWith('/v1/events/event-alpha'))
      return json(normalizedEvent());
    if (url.pathname.endsWith('/v1/relationship-types'))
      return json(relationshipTypes());
    if (url.pathname.endsWith('/v1/markets/market-alpha'))
      return json(normalizedMarket());
    if (url.pathname.endsWith('/v1/relations/1'))
      return json(normalizedRelation());
    throw new Error(`unexpected route ${url.pathname}`);
  }) as typeof fetch;
  const environment = {
    UNIVERSE_API_BASE_URL: 'https://universe.internal/base',
    UNIVERSE_API_AUTHORIZATION: 'Bearer server-only',
  };

  for (const route of [
    '/v1/events?limit=25&cursor=opaque-events-cursor',
    '/v1/relationship-types',
    '/v1/markets/market-alpha?market_template_version=1&outcome_space_version=1',
    '/v1/relations/1',
  ]) {
    const [path, query = ''] = route.split('?');
    const response = await handleEventUniverseProxy(
      new Request(
        `https://ui.example/api/event-universe-proxy?__universe_path=${encodeURIComponent(path)}${query ? `&${query}` : ''}`,
      ),
      environment,
      upstreamFetch,
    );
    assert.equal(response.status, 200, route);
    if (route === '/v1/relations/1')
      assert.equal(
        response.headers.get('cache-control'),
        'private, max-age=300',
      );
  }
  assert.deepEqual(requested, [
    '/base/v1/events?limit=25&cursor=opaque-events-cursor',
    '/base/v1/relationship-types',
    '/base/v1/markets/market-alpha?market_template_version=1&outcome_space_version=1',
    '/base/v1/relations/1',
  ]);

  for (const route of [
    '/v1/events?limit=101',
    '/v1/relationship-types?cursor=nope',
    '/v1/markets/market-alpha?market_template_version=0',
    '/v1/relations/0',
  ]) {
    const [path, query = ''] = route.split('?');
    const response = await handleEventUniverseProxy(
      new Request(
        `https://ui.example/api/event-universe-proxy?__universe_path=${encodeURIComponent(path)}${query ? `&${query}` : ''}`,
      ),
      environment,
      upstreamFetch,
    );
    assert.equal(response.status, 400, route);
  }
  assert.equal(requested.length, 4);

  const eventDetail = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/events/event-alpha',
    ),
    environment,
    upstreamFetch,
  );
  assert.equal(eventDetail.status, 200);
  assert.equal(eventDetail.headers.get('cache-control'), 'no-store');
});

test('relation detail accepts an empty claim key but rejects schema drift', async () => {
  const valid = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () => json(normalizedRelation())) as typeof fetch,
  });
  assert.equal((await valid.relation('1')).members[0].claim_key, '');

  const invalid = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () => json(normalizedRelation(null))) as typeof fetch,
  });
  await assert.rejects(() => invalid.relation('1'));
});

test('run summaries avoid event-detail fan-out and render in bounded pages', async () => {
  const [source, historySource] = await Promise.all([
    readFile(
      new URL('../src/client/observability.tsx', import.meta.url),
      'utf8',
    ),
    readFile(
      new URL('../src/client/event-universe.tsx', import.meta.url),
      'utf8',
    ),
  ]);
  assert.doesNotMatch(
    source,
    /useEventDetails|Promise\.all\([^)]*\/v1\/events/s,
  );
  assert.match(source, /run\?\.events/);
  assert.match(source, /useEventDetail\(detailId\)/);
  assert.match(
    source,
    /document\.activeElement === element[\s\S]*event\.shiftKey \? last : first/,
  );
  assert.match(source, /opener\.current\?\.focus\(\)/);
  assert.doesNotMatch(historySource, /do \{|loadAllBundles/);
  assert.match(historySource, /limit: '100'/);

  const records = Array.from({ length: 1000 }, (_, index) => index);
  const first = boundedRenderPage(records, 0);
  const last = boundedRenderPage(records, 9);
  assert.equal(first.items.length, 100);
  assert.deepEqual(first.items.slice(0, 2), [0, 1]);
  assert.equal(last.items.length, 100);
  assert.deepEqual(last.items.slice(-2), [998, 999]);
  assert.equal(last.pageCount, 10);
  assert.throws(() =>
    validateTargeterRun({
      ...normalizedRun(),
      events: Array.from({ length: 1001 }, (_, index) => ({
        ...normalizedEventSummary(),
        event_id: `event-${index}`,
      })),
    }),
  );
  for (const field of ['bundles', 'selections', 'runs']) {
    const payload =
      field === 'bundles'
        ? {
            bundles: Array(101).fill((normalizedRun() as any).run),
            next_cursor: null,
          }
        : field === 'selections'
          ? {
              selections: Array(101).fill(detail()),
              sort: 'selected',
              next_cursor: null,
            }
          : {
              runs: Array(101).fill((normalizedRun() as any).run),
              next_cursor: null,
            };
    await assert.rejects(
      field === 'bundles'
        ? new EventUniverseClient({
            baseUrl: 'https://x',
            fetch: (async () => json(payload)) as typeof fetch,
          }).bundles(new URLSearchParams())
        : field === 'selections'
          ? new EventUniverseClient({
              baseUrl: 'https://x',
              fetch: (async () => json(payload)) as typeof fetch,
            }).selections(new URLSearchParams())
          : new EventUniverseClient({
              baseUrl: 'https://x',
              fetch: (async () => json(payload)) as typeof fetch,
            }).runs(new URLSearchParams()),
    );
  }
});

test('browser caching is bounded and does not retain event details', async () => {
  const originalFetch = globalThis.fetch;
  const requests: string[] = [];
  globalThis.fetch = (async (input) => {
    requests.push(String(input));
    return json({ ok: true });
  }) as typeof fetch;
  try {
    for (let index = 0; index < 9; index++)
      await universeGet(`/bounded-cache-${index}`);
    await universeGet('/bounded-cache-0');
    assert.equal(requests.length, 10, 'the oldest of nine entries was evicted');

    await universeGet('/v1/events/detail-not-cached');
    await universeGet('/v1/events/detail-not-cached');
    assert.equal(
      requests.filter((path) => path.endsWith('/v1/events/detail-not-cached'))
        .length,
      2,
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('signal-bound browser requests are never reused after abort', async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = ((input, init) => {
    calls++;
    if (calls === 1)
      return new Promise((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () =>
          reject(new DOMException('aborted', 'AbortError')),
        );
      });
    return Promise.resolve(json({ second: true }));
  }) as typeof fetch;
  try {
    const firstController = new AbortController();
    const first = universeGet('/retry-after-abort', {
      signal: firstController.signal,
      cache: false,
    });
    firstController.abort();
    await assert.rejects(first);
    const second = await universeGet<{ second: boolean }>(
      '/retry-after-abort',
      { cache: false },
    );
    assert.equal(second.second, true);
    assert.equal(calls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Express status proxy rejects non-GET refreshes before upstream access', async () => {
  let fetches = 0;
  const client = new EventUniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () => {
      fetches++;
      return json(status());
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
      `http://127.0.0.1:${port}/api/event-universe/v1/targeter/status?limit=5`,
      { method: 'POST' },
    );
    assert.equal(response.status, 405);
    assert.deepEqual(await response.json(), { error: 'Method not allowed' });
    assert.equal(fetches, 0);

    const detailClient = new EventUniverseClient({
      baseUrl: 'https://universe.internal',
      fetch: (async () => json(normalizedEvent())) as typeof fetch,
    });
    const detailApp = express();
    detailApp.use(
      '/api/event-universe',
      createEventUniverseRouter(detailClient),
    );
    const detailServer = detailApp.listen(0, '127.0.0.1');
    await new Promise<void>((resolve, reject) => {
      detailServer.once('listening', resolve);
      detailServer.once('error', reject);
    });
    try {
      const detailPort = (detailServer.address() as AddressInfo).port;
      const detailResponse = await fetch(
        `http://127.0.0.1:${detailPort}/api/event-universe/v1/events/event-alpha`,
      );
      assert.equal(detailResponse.status, 200);
      assert.equal(detailResponse.headers.get('cache-control'), 'no-store');
    } finally {
      await new Promise<void>((resolve, reject) =>
        detailServer.close((error) => (error ? reject(error) : resolve())),
      );
    }
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});

test('Vercel proxy hydrates Universe only through server-side configuration', async () => {
  let upstreamUrl = '';
  let upstreamAuthorization = '';
  let upstreamFetches = 0;
  const upstreamFetch = (async (input, init) => {
    upstreamFetches++;
    upstreamUrl = String(input);
    upstreamAuthorization =
      new Headers(init?.headers).get('authorization') ?? '';
    return json({ selections: [], sort: 'selected', next_cursor: null });
  }) as typeof fetch;
  const response = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/selections&sort=selected&limit=25',
    ),
    {
      UNIVERSE_API_BASE_URL: 'https://universe.internal/base',
      UNIVERSE_API_AUTHORIZATION: 'Bearer server-only',
    },
    upstreamFetch,
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
  assert.equal(response.headers.get('cache-control'), 'private, max-age=300');

  const cachedResponse = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/selections&sort=selected&limit=25',
    ),
    {
      UNIVERSE_API_BASE_URL: 'https://universe.internal/base',
      UNIVERSE_API_AUTHORIZATION: 'Bearer server-only',
    },
    upstreamFetch,
  );
  assert.equal(cachedResponse.status, 200);
  assert.equal(upstreamFetches, 1);

  const statusResponse = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/targeter/status&limit=5',
    ),
    { UNIVERSE_API_BASE_URL: 'https://universe.internal' },
    (async (input) => {
      assert.equal(
        String(input),
        'https://universe.internal/v1/targeter/status?limit=5',
      );
      return json(status());
    }) as typeof fetch,
  );
  assert.equal(statusResponse.status, 200);
  assert.equal((await statusResponse.json()).status_projection_version, 1);

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

test('live routes use the Universe proxy without cadence or archive dependencies', async () => {
  const [client, apiClient, server, proxy, packageDocument] = await Promise.all(
    [
      readFile(new URL('../src/client/main.tsx', import.meta.url), 'utf8'),
      readFile(
        new URL('../src/client/universe-api.ts', import.meta.url),
        'utf8',
      ),
      readFile(new URL('../src/server/index.ts', import.meta.url), 'utf8'),
      readFile(
        new URL('../src/server/event-universe.ts', import.meta.url),
        'utf8',
      ),
      readFile(new URL('../package.json', import.meta.url), 'utf8'),
    ],
  );
  assert.match(client, /universeGet<UniverseTargeterStatus>/);
  assert.match(client, /\/v1\/targeter\/status\?limit=5/);
  assert.match(client, /path="\/"[\s\S]*<TargetsPage/);
  assert.match(client, /path="\/targets" element={<Navigate to="\/" replace/);
  assert.doesNotMatch(client, /StatusPage|>Status</);
  assert.doesNotMatch(proxy, /validateCadence|UniverseCadence|CADENCE_QUERY/);
  assert.match(apiClient, /method: 'GET'/);
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
  const apiPackage = JSON.parse(
    await readFile(new URL('../../api/package.json', import.meta.url), 'utf8'),
  ) as { type?: string };
  const config = JSON.parse(
    await readFile(new URL('../../vercel.json', import.meta.url), 'utf8'),
  ) as {
    buildCommand: string;
    outputDirectory: string;
    rewrites: Array<{ source: string; destination: string }>;
  };
  assert.equal(apiPackage.type, 'module');
  assert.equal(
    config.buildCommand,
    'yarn workspace prediction-indexer-targeter-ui build:client',
  );
  assert.equal(config.outputDirectory, 'targeter-ui/dist');
  // The group name is `universePath` rather than a generic `path` because Vercel
  // echoes it into the destination query, where the proxy has to delete it by
  // name; keeping the two in step is what stops every route 400ing. See the
  // regression test below.
  assert.deepEqual(config.rewrites, [
    {
      source: '/api/event-universe/:universePath*',
      destination: '/api/event-universe-proxy?__universe_path=/:universePath*',
    },
    { source: '/(.*)', destination: '/index.html' },
  ]);
});

test('Vercel proxy drops the rewrite group the platform echoes into the query', async () => {
  // The vercel.json rewrite both substitutes `:universePath*` into the
  // destination and echoes it as its own query parameter, so a real request for
  // `/api/event-universe/healthz` arrives carrying `universePath=healthz`
  // alongside `__universe_path=/healthz`. Every other proxy test builds the
  // idealised URL by hand and so never sees it; forwarding it made every route
  // fail as `Invalid Event Universe request`, because `requireNoQuery` rejects
  // any key at all and the per-route allow-lists reject unknown ones.
  const health = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/healthz&universePath=healthz',
    ),
    { UNIVERSE_API_BASE_URL: 'https://universe.internal' },
    (async (input) => {
      assert.equal(String(input), 'https://universe.internal/healthz');
      return json({
        status: 'ok',
        schema_version: 4,
        latest_run: null,
        counts: {
          targeter_runs: 653,
          selection_occurrences: 3718,
          bundle_retirements: 152,
          bundle_contexts: 153,
          context_targets: 2125,
          umbrella_events: 804,
          canonical_markets: 1912,
          venue_markets: 3618,
          relations: 733,
        },
      });
    }) as typeof fetch,
  );
  assert.equal(health.status, 200);
  assert.equal((await health.clone().json()).schema_version, 4);

  // A route with its own allow-listed parameters must keep them and still shed
  // the echo, rather than the proxy stripping everything indiscriminately.
  const runs = await handleEventUniverseProxy(
    new Request(
      'https://ui.example/api/event-universe-proxy?__universe_path=/v1/runs&universePath=v1/runs&limit=20',
    ),
    { UNIVERSE_API_BASE_URL: 'https://universe.internal' },
    (async (input) => {
      assert.equal(String(input), 'https://universe.internal/v1/runs?limit=20');
      return json({ runs: [], next_cursor: null });
    }) as typeof fetch,
  );
  assert.equal(runs.status, 200);
});
