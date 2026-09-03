import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { fileURLToPath } from 'node:url';
import { InfiniteQueryObserver, QueryObserver } from '@tanstack/react-query';
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
import {
  bundlesQuery,
  createUniverseQueryClient,
  eventDetailQuery,
  MAX_BUNDLE_PAGES,
  QUERY_GC_MS,
  targeterRunQuery,
  universeKeys,
} from '../src/client/universe-queries.js';
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
  event_refs: ['kalshi:event-a', 'polymarket:event-b'],
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
      observed_activation_at: '2026-08-20T14:00:00Z',
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

type ContractCase = { name: string; path: string; body: any };

function realUniverseContract(): ContractCase[] {
  const repository = new URL('../..', import.meta.url);
  const document = JSON.parse(
    execFileSync(
      fileURLToPath(new URL('.venv/bin/python', repository)),
      ['-m', 'tests.generate_event_universe_contract'],
      { cwd: fileURLToPath(repository), encoding: 'utf8' },
    ),
  );
  assert.equal(document.fixture_version, 1);
  assert.equal(document.schema_version, 4);
  return document.cases;
}

async function proxyContractCase(contract: ContractCase) {
  const [pathname, query = ''] = contract.path.split('?');
  return handleEventUniverseProxy(
    new Request(
      `https://ui.example/api/event-universe-proxy?__universe_path=${encodeURIComponent(pathname)}${query ? `&${query}` : ''}`,
    ),
    { UNIVERSE_API_BASE_URL: 'https://universe.internal' },
    (async () => json(contract.body)) as typeof fetch,
  );
}

test('real UniverseApplication responses satisfy all eight proxy contracts', async () => {
  const contracts = realUniverseContract();
  assert.deepEqual(
    contracts.map(({ name }) => name),
    [
      'health_ok',
      'targeter_status',
      'runs',
      'selections',
      'bundles',
      'events',
      'event_detail',
      'targeter_run',
      'health_degraded',
    ],
  );
  for (const contract of contracts) {
    const response = await proxyContractCase(contract);
    assert.equal(response.status, 200, contract.name);
    assert.deepEqual(await response.json(), contract.body, contract.name);
  }
  const degraded = contracts.find(({ name }) => name === 'health_degraded')!;
  assert.equal(degraded.body.status, 'degraded');
  assert.equal(degraded.body.sync.pending_failures, 1);
});

test('real Universe contracts remain closed to key-set drift', async () => {
  const contracts = new Map(
    realUniverseContract().map((contract) => [contract.name, contract]),
  );
  const mutations: Array<[string, (body: any) => void]> = [
    ['health_ok', (body) => (body.unexpected = true)],
    ['health_degraded', (body) => (body.sync.pending_failures = 0)],
    ['events', (body) => delete body.events[0].event_refs],
    ['event_detail', (body) => (body.observations[0].unexpected = true)],
    ['targeter_run', (body) => delete body.events[0].event_refs],
  ];
  for (const [name, mutate] of mutations) {
    const contract = structuredClone(contracts.get(name)!);
    mutate(contract.body);
    const response = await proxyContractCase(contract);
    assert.equal(response.status, 502, name);
  }
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
  const [source, historySource, querySource] = await Promise.all([
    readFile(
      new URL('../src/client/observability.tsx', import.meta.url),
      'utf8',
    ),
    readFile(
      new URL('../src/client/event-universe.tsx', import.meta.url),
      'utf8',
    ),
    readFile(
      new URL('../src/client/universe-queries.ts', import.meta.url),
      'utf8',
    ),
  ]);
  assert.doesNotMatch(
    source,
    /useEventDetails|Promise\.all\([^)]*\/v1\/events/s,
  );
  assert.match(source, /run\?\.events/);
  assert.match(source, /useEventDetail\(detailId\)/);
  assert.match(source, /detail\.event\.event_refs\.map/);
  assert.match(source, /observation\.observed_activation_at/);
  assert.match(
    source,
    /document\.activeElement === element[\s\S]*event\.shiftKey \? last : first/,
  );
  assert.match(source, /opener\.current\?\.focus\(\)/);
  assert.doesNotMatch(historySource, /do \{|loadAllBundles/);
  assert.match(querySource, /BUNDLE_PAGE_SIZE = 100/);
  assert.match(querySource, /enabled: Boolean\(eventId\)/);

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

test('query keys are stable, typed by endpoint, and independent', () => {
  assert.deepEqual(universeKeys.status(5), universeKeys.status(5));
  assert.deepEqual(universeKeys.bundles(100), [
    'event-universe',
    'bundles',
    { limit: 100 },
  ]);
  assert.notDeepEqual(universeKeys.status(5), universeKeys.status(4));
  assert.notDeepEqual(
    universeKeys.targeterRun('run-a'),
    universeKeys.targeterRun('run-b'),
  );
  assert.notDeepEqual(
    universeKeys.event('event-a'),
    universeKeys.selection('run-a', 'event-a'),
  );
  const client = createUniverseQueryClient();
  assert.equal(client.getDefaultOptions().queries?.gcTime, QUERY_GC_MS);
  assert.equal(eventDetailQuery('event-a').gcTime, 0);
  assert.equal(bundlesQuery().maxPages, MAX_BUNDLE_PAGES);
  client.clear();
});

test('React Query deduplicates requests and retries one failed attempt', async () => {
  const originalFetch = globalThis.fetch;
  let requests = 0;
  let release: (() => void) | undefined;
  globalThis.fetch = (async () => {
    requests++;
    await new Promise<void>((resolve) => {
      release = resolve;
    });
    return json(normalizedRun());
  }) as typeof fetch;
  const client = createUniverseQueryClient();
  try {
    const first = client.fetchQuery(targeterRunQuery('run-a'));
    const second = client.fetchQuery(targeterRunQuery('run-a'));
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(requests, 1);
    release?.();
    assert.deepEqual(await first, await second);
  } finally {
    client.clear();
    globalThis.fetch = originalFetch;
  }

  let attempts = 0;
  globalThis.fetch = (async () => {
    attempts++;
    return attempts === 1
      ? json({ error: 'temporary' }, { status: 503 })
      : json(normalizedRun());
  }) as typeof fetch;
  const retryingClient = createUniverseQueryClient();
  retryingClient.setDefaultOptions({
    queries: { gcTime: 300_000, retry: 1, retryDelay: 0 },
  });
  try {
    assert.equal(
      (await retryingClient.fetchQuery(targeterRunQuery('run-retry'))).run
        .run_id,
      run().run_id,
    );
    assert.equal(attempts, 2);

    attempts = 0;
    globalThis.fetch = (async () => {
      attempts++;
      return json({ error: 'still unavailable' }, { status: 503 });
    }) as typeof fetch;
    await assert.rejects(() =>
      retryingClient.fetchQuery(targeterRunQuery('run-error')),
    );
    assert.equal(attempts, 2);
    assert.equal(
      retryingClient.getQueryState(universeKeys.targeterRun('run-error'))
        ?.status,
      'error',
    );
  } finally {
    retryingClient.clear();
    globalThis.fetch = originalFetch;
  }
});

test('query cancellation aborts the supplied request signal', async () => {
  const originalFetch = globalThis.fetch;
  let aborted = false;
  globalThis.fetch = ((_input, init) => {
    return new Promise((_resolve, reject) => {
      init?.signal?.addEventListener('abort', () => {
        aborted = true;
        reject(new DOMException('aborted', 'AbortError'));
      });
    });
  }) as typeof fetch;
  const client = createUniverseQueryClient();
  try {
    const request = client.fetchQuery(eventDetailQuery('event-a'));
    await client.cancelQueries({ queryKey: universeKeys.event('event-a') });
    await assert.rejects(request);
    assert.equal(aborted, true);
  } finally {
    client.clear();
    globalThis.fetch = originalFetch;
  }
});

test('switching detail observers aborts stale data and exposes only the new key', async () => {
  const originalFetch = globalThis.fetch;
  let firstAborted = false;
  globalThis.fetch = ((input, init) => {
    const eventId = String(input).split('/').at(-1);
    if (eventId === 'event-a')
      return new Promise((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => {
          firstAborted = true;
          reject(new DOMException('aborted', 'AbortError'));
        });
      });
    return Promise.resolve(
      json({
        ...normalizedEvent(),
        event: { ...normalizedEvent().event, event_id: eventId },
      }),
    );
  }) as typeof fetch;
  const client = createUniverseQueryClient();
  const observer = new QueryObserver(client, eventDetailQuery('event-a'));
  const observed: string[] = [];
  const unsubscribe = observer.subscribe((result) => {
    if (result.data) observed.push(result.data.event.event_id);
  });
  try {
    observer.setOptions(eventDetailQuery('event-b'));
    const result = await observer.refetch();
    assert.equal(result.data?.event.event_id, 'event-b');
    assert.equal(firstAborted, true);
    assert.deepEqual(observed, ['event-b']);
  } finally {
    unsubscribe();
    await new Promise((resolve) => setTimeout(resolve, 5));
    assert.equal(client.getQueryData(universeKeys.event('event-b')), undefined);
    client.clear();
    globalThis.fetch = originalFetch;
  }
});

test('bundle infinite query preserves opaque cursors and bounds retained pages', async () => {
  const originalFetch = globalThis.fetch;
  const requested: string[] = [];
  globalThis.fetch = (async (input) => {
    const url = new URL(String(input), 'https://ui.example');
    requested.push(url.searchParams.get('cursor') ?? 'first');
    const page = requested.length;
    return json({
      bundles: [],
      next_cursor: page <= MAX_BUNDLE_PAGES ? `opaque-${page}` : null,
    });
  }) as typeof fetch;
  const client = createUniverseQueryClient();
  const observer = new InfiniteQueryObserver(client, bundlesQuery());
  try {
    await observer.refetch();
    for (let page = 1; page <= MAX_BUNDLE_PAGES; page++)
      await observer.fetchNextPage();
    const result = observer.getCurrentResult();
    assert.equal(result.data?.pages.length, MAX_BUNDLE_PAGES);
    assert.deepEqual(requested, [
      'first',
      ...Array.from(
        { length: MAX_BUNDLE_PAGES },
        (_, index) => `opaque-${index + 1}`,
      ),
    ]);
    assert.deepEqual(result.data?.pageParams, [
      'opaque-1',
      'opaque-2',
      'opaque-3',
      'opaque-4',
      'opaque-5',
      'opaque-6',
      'opaque-7',
      'opaque-8',
    ]);
  } finally {
    client.clear();
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
  const [client, apiClient, queryClient, server, proxy, packageDocument] =
    await Promise.all([
      readFile(new URL('../src/client/main.tsx', import.meta.url), 'utf8'),
      readFile(
        new URL('../src/client/universe-api.ts', import.meta.url),
        'utf8',
      ),
      readFile(
        new URL('../src/client/universe-queries.ts', import.meta.url),
        'utf8',
      ),
      readFile(new URL('../src/server/index.ts', import.meta.url), 'utf8'),
      readFile(
        new URL('../src/server/event-universe.ts', import.meta.url),
        'utf8',
      ),
      readFile(new URL('../package.json', import.meta.url), 'utf8'),
    ]);
  assert.match(client, /QueryClientProvider client={queryClient}/);
  assert.match(queryClient, /universeGet<UniverseTargeterStatus>/);
  assert.match(queryClient, /\/v1\/targeter\/status\?limit=\$\{limit\}/);
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
        sync: { pending_failures: 0 },
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
