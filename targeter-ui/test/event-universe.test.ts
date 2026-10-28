import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { InfiniteQueryObserver, QueryObserver } from '@tanstack/react-query';
import { universeGet } from '../src/client/universe-api.js';
import {
  UniverseApiError,
  UniverseClient,
} from '../src/client/universe-client.js';
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
import type {
  UniverseSelection,
  UniverseSelectionDetail,
  UniverseTargeterRunDetail,
} from '../src/event-universe.js';

const sha = 'a'.repeat(64);
const otherSha = 'b'.repeat(64);
const shapeSha = 'c'.repeat(64);
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
const run = () => ({
  run_id: '20260820T120000.000001Z',
  generated_at: '2026-08-20T12:00:00Z',
});
const normalizedEventSummary = () => ({
  event_id: 'event-alpha',
  identity_version: 1 as const,
  identity_activation_date: '2026-08-20',
  identity_ordinal: 0,
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
});
const claimSummary = () => ({
  claim_id: sha,
  space_shape_id: shapeSha,
  scope: 'series',
  coverage: 'EXHAUSTIVE',
  outcome_key_count: 3,
  market_count: 2,
  venue_count: 2,
  first_seen_run_id: run().run_id,
  last_seen_run_id: run().run_id,
});
const claimRelation = () => ({
  space_shape_id: shapeSha,
  left_claim_id: sha,
  right_claim_id: otherSha,
  relation_type: 'IMPLICATION',
  antecedent_scope: 'series',
  antecedent_coverage: 'EXHAUSTIVE',
  consequent_scope: 'series',
  consequent_coverage: 'EXHAUSTIVE',
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
  claims: [claimSummary()],
  relations: [claimRelation()],
  observations: [
    {
      run_id: run().run_id,
      generated_at: run().generated_at,
      bundle_id: 'bundle alpha',
      observed_activation_at: '2026-08-20T14:00:00Z',
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
  return document.cases;
}

async function readContractCase(contract: ContractCase) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input, init) => {
    const requested = new URL(String(input));
    assert.equal(`${requested.pathname}${requested.search}`, contract.path);
    assert.equal(init?.credentials, 'omit');
    assert.equal(new Headers(init?.headers).get('authorization'), null);
    return json(contract.body);
  }) as typeof fetch;
  try {
    return await universeGet(contract.path);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test('browser transport reads real UniverseApplication responses', async () => {
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
      'market_detail',
      'claim_detail',
      'claim_markets',
      'health_degraded',
    ],
  );
  for (const contract of contracts) {
    assert.deepEqual(
      await readContractCase(contract),
      contract.body,
      contract.name,
    );
  }
  const degraded = contracts.find(({ name }) => name === 'health_degraded')!;
  assert.equal(degraded.body.status, 'degraded');
  assert.equal(degraded.body.sync.pending_failures, 1);
});

test('browser bundle discovery validates the real Universe bundle schema', async () => {
  const contract = realUniverseContract().find(
    ({ name }) => name === 'bundles',
  )!;
  const api = (body: unknown) =>
    new UniverseClient({
      baseUrl: 'https://universe.example',
      fetch: (async () => json(body)) as typeof fetch,
    });
  assert.deepEqual(
    (await api(contract.body).bundles()).bundles,
    contract.body.bundles,
  );
  for (const mutate of [
    (body: any) => {
      body.unexpected = true;
    },
    (body: any) => {
      delete body.bundles[0].participants;
    },
    (body: any) => {
      body.bundles[0].unexpected = true;
    },
  ]) {
    const body = structuredClone(contract.body);
    mutate(body);
    await assert.rejects(() => api(body).bundles(), { kind: 'contract' });
  }
});

test('strictly validates selection detail and rejects schema drift', async () => {
  const valid = detail();
  const client = new UniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () => json(valid)) as typeof fetch,
  });
  const result = await client.selection(valid.run_id, valid.bundle_id);
  assert.deepEqual(result.context.participants, ['Alpha', 'Beta']);
  assert.equal(result.context.targets[1].subscription_ids.length, 2);
  assert.equal(result.origin.manifest_sha256, sha);

  const invalid: any = structuredClone(valid);
  invalid.ended_at = '2026-08-20T15:00:00Z';
  const rejecting = new UniverseClient({
    baseUrl: 'https://universe.internal',
    fetch: (async () => json(invalid)) as typeof fetch,
  });
  await assert.rejects(() =>
    rejecting.selection(valid.run_id, valid.bundle_id),
  );
});

test('browser transport bounds timeout and response bytes', async () => {
  const timedOut = new UniverseClient({
    baseUrl: 'https://universe.example',
    timeoutMs: 5,
    fetch: ((_input, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () =>
          reject(new DOMException('aborted', 'AbortError')),
        );
      })) as typeof fetch,
  });
  await assert.rejects(
    () => timedOut.get('/v1/targeter/status', (value) => value),
    (error: unknown) =>
      error instanceof UniverseApiError && error.kind === 'timeout',
  );
  for (const response of [
    json({}, { headers: { 'content-length': '1750001' } }),
    json({ oversized: 'x'.repeat(1_750_000) }),
  ]) {
    const api = new UniverseClient({
      baseUrl: 'https://universe.example',
      fetch: (async () => response) as typeof fetch,
    });
    await assert.rejects(
      () => api.get('/v1/targeter/status', (value) => value),
      (error: unknown) =>
        error instanceof UniverseApiError && error.kind === 'contract',
    );
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

test('browser selection reads preserve dense contexts and nullable fields', async () => {
  const valid = detail({ game: null, topology: null });
  valid.context.game = null;
  valid.context.topology = null;
  valid.context.relationships = Array.from({ length: 3240 }, () => ({
    ...valid.context.relationships[0],
  }));
  let requested = '';
  const api = new UniverseClient({
    baseUrl: 'https://universe.example/base',
    fetch: (async (input) => {
      requested = String(input);
      return json(valid);
    }) as typeof fetch,
  });
  const result = await api.selection(valid.run_id, valid.bundle_id);
  assert.equal(
    new URL(requested).pathname,
    `/base/v1/runs/${valid.run_id}/selections/bundle%20alpha`,
  );
  assert.deepEqual(result, valid);
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

  // Claim detail and its paged markets are reachable, not merely implemented.
  // Both endpoints were fully validated and contract-tested while the drawer
  // rendered the claim digest as inert text with nothing to click.
  assert.match(querySource, /enabled: Boolean\(claimId\)/);
  assert.match(source, /useClaimDetail\(claimId\)/);
  assert.match(source, /useClaimMarkets\(claimId\)/);
  assert.match(source, /onClick=\{\(\) => openClaim\(claim\.claim_id\)\}/);
  assert.match(source, /openClaim=\{setClaimId\}/);
  // A relation names the claims it relates rather than showing two digests.
  assert.match(source, /claimName\(relation\.left_claim_id, detail\.claims\)/);

  const records = Array.from({ length: 1000 }, (_, index) => index);
  const first = boundedRenderPage(records, 0);
  const last = boundedRenderPage(records, 9);
  assert.equal(first.items.length, 100);
  assert.deepEqual(first.items.slice(0, 2), [0, 1]);
  assert.equal(last.items.length, 100);
  assert.deepEqual(last.items.slice(-2), [998, 999]);
  assert.equal(last.pageCount, 10);
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

test('live routes use the browser API client and the server serves static files', async () => {
  const [client, apiClient, queryClient, server, packageDocument] =
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
      readFile(new URL('../package.json', import.meta.url), 'utf8'),
    ]);
  assert.match(client, /QueryClientProvider client={queryClient}/);
  assert.match(queryClient, /universeGet<UniverseTargeterStatus>/);
  assert.match(queryClient, /\/v1\/targeter\/status\?limit=\$\{limit\}/);
  assert.match(client, /path="\/"[\s\S]*<TargetsPage/);
  assert.match(client, /path="\/targets" element={<Navigate to="\/" replace/);
  assert.doesNotMatch(client, /StatusPage|>Status</);
  assert.match(server, /express\.static\(web\)/);
  assert.doesNotMatch(
    server,
    /EventUniverseClient|createEventUniverseRouter|api\/event-universe/,
  );
  assert.match(apiClient, /UniverseClient/);
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

test('Vercel builds only the client with no API proxy', async () => {
  const config = JSON.parse(
    await readFile(new URL('../../vercel.json', import.meta.url), 'utf8'),
  ) as {
    buildCommand: string;
    outputDirectory: string;
    env: Record<string, string>;
    rewrites: Array<{ source: string; destination: string }>;
  };
  assert.equal(
    config.buildCommand,
    'yarn workspace prediction-indexer-targeter-ui build:client',
  );
  assert.equal(config.outputDirectory, 'targeter-ui/dist');
  assert.deepEqual(config.env, {
    VITE_UNIVERSE_API_BASE_URL: 'https://34-182-18-247.sslip.io',
    VITE_REPLAY_SIWE_DOMAIN: 'prediction-indexer-targeter-ui-b8xg.vercel.app',
    VITE_REPLAY_SIWE_URI:
      'https://prediction-indexer-targeter-ui-b8xg.vercel.app/',
    VITE_REPLAY_SIWE_STATEMENT: 'Sign in to Prediction Indexer.',
    VITE_REPLAY_SIWE_CHAIN_ID: '1',
    VITE_REPLAY_FIXTURES: 'false',
  });
  assert.deepEqual(config.rewrites, [
    { source: '/(.*)', destination: '/index.html' },
  ]);
});
