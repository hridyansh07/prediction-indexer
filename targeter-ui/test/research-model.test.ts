import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  chooseTier,
  displayLabel,
  filterEvents,
  fixedYRange,
  formatOffset,
  gameMarks,
  gameMarkTitle,
  gameSummaryAt,
  gridsToChart,
  IdentityError,
  knownAt,
  legendGroups,
  lensMarks,
  maskAfter,
  mergeEpisodes,
  nextMoment,
  parseLayers,
  rawToChart,
  resolveCorpusBase,
  sortEvents,
  tilesInRange,
  valuesAt,
  verifyIdentity,
  withLayers,
  type EventRow,
  type GameDocument,
  type GridDocument,
  type IntervalItem,
  type PackBook,
  type PackScope,
  type RawTile,
} from '../src/client/research/research-model.js';

// A hand-authored pack: capture starts at T0; scope 0 is [0, 900) s, scope 1 [1200, 1800) s.
const T0 = 1_791_000_000_000_000_000n;
const ns = (seconds: number) => String(T0 + BigInt(Math.round(seconds * 1e9)));

const BOOKS: PackBook[] = [
  {
    book: 0,
    instrument: 'kalshi:K',
    orientation: 'outcome',
    venue: 'kalshi',
    market_id: 'kalshi:K',
    market_type: 'series_moneyline',
    label: 'series_moneyline · Cloud9',
    claim_id: 'c1',
    price_scale: 4,
    quantity_scale: 2,
  },
  {
    book: 1,
    instrument: 'polymarket:1',
    orientation: 'outcome',
    venue: 'polymarket',
    market_id: 'polymarket:9',
    market_type: 'series_moneyline',
    label: 'series_moneyline · Cloud9',
    claim_id: 'c1',
    price_scale: 4,
    quantity_scale: 6,
  },
  {
    book: 2,
    instrument: 'polymarket:2',
    orientation: 'outcome',
    venue: 'polymarket',
    market_id: 'polymarket:9',
    market_type: 'series_moneyline',
    label: 'series_moneyline · LYON',
    claim_id: 'c2',
    price_scale: 4,
    quantity_scale: 6,
  },
  {
    book: 3,
    instrument: 'kalshi:M',
    orientation: 'outcome',
    venue: 'kalshi',
    market_id: 'kalshi:M',
    market_type: null,
    label: 'kalshi:M',
    claim_id: null,
    price_scale: 4,
    quantity_scale: 2,
  },
];
const SCOPES: PackScope[] = [
  { start_ns: ns(0), end_ns: ns(900), run_id: 'r0', scheduled_start_ns: null },
  {
    start_ns: ns(1200),
    end_ns: ns(1800),
    run_id: 'r1',
    scheduled_start_ns: null,
  },
];

function series(last: (number | null)[]) {
  return { min: last, max: last, last };
}

const OVERVIEW: GridDocument = {
  start_ns: ns(0),
  end_ns: ns(1800),
  bucket_ns: String(600n * 1_000_000_000n),
  buckets: 3,
  books: [
    {
      book: 0,
      bid: series([4000, 3900, 6000]),
      ask: series([4200, 4300, 6100]),
      usable_ppm: [1_000_000, 0, 500_000],
      trades: [0, 0, 1],
      trade_qty: [0, 0, 500],
    },
    {
      book: 1,
      bid: series([5000, null, 5500]),
      ask: series([5100, null, 5600]),
      usable_ppm: [1_000_000, 0, 1_000_000],
      trades: [1, 0, 0],
      trade_qty: [2_000_000, 0, 0],
    },
    {
      book: 2,
      bid: series([null, null, null]),
      ask: series([null, null, null]),
      usable_ppm: [0, 0, 0],
      trades: [0, 0, 0],
      trade_qty: [0, 0, 0],
    },
    {
      book: 3,
      bid: series([1000, 1000, 1000]),
      ask: series([null, null, null]),
      usable_ppm: [1, 1, 1],
      trades: [0, 0, 0],
      trade_qty: [0, 0, 0],
    },
  ],
};

test('bucket paths break where a book has no usable time', () => {
  const chart = gridsToChart([OVERVIEW], BOOKS, T0);
  assert.deepEqual(chart.x, [300, 900, 1500]);
  assert.deepEqual(chart.books[0].bid, [40, null, 60]);
  assert.deepEqual(chart.books[0].mid, [41, null, 60.5]);
  assert.deepEqual(chart.books[1].mid, [50.5, null, 55.5]);
  assert.deepEqual(chart.books[2].mid, [null, null, null]);
  assert.deepEqual(chart.books[3].mid, [null, null, null]); // one-sided book: no mid, bid stays
  assert.deepEqual(chart.books[3].bid, [10, 10, 10]);
  assert.equal(chart.stepped, false);
});

test('raw tiles become step lines that break on invalidation and at scope end', () => {
  const tiles: RawTile[] = [
    {
      start_ns: ns(600),
      end_ns: ns(1200),
      initial: [
        {
          book: 0,
          scope: 0,
          since_ns: ns(61),
          validity: 'usable',
          bid: ['3900', '1'],
          ask: ['4300', '1'],
        },
      ],
      rows: [
        {
          type: 'top',
          scope: 0,
          book: 0,
          t_ns: ns(700),
          validity: 'unusable:connection_closed',
          bid: null,
          ask: null,
        },
        {
          type: 'top',
          scope: 0,
          book: 1,
          t_ns: ns(800),
          validity: 'usable',
          bid: ['5000', '1'],
          ask: ['5200', '1'],
        },
        {
          type: 'trade',
          scope: 0,
          book: 1,
          t_ns: ns(801),
          price: '5200',
          qty: '1',
        },
      ],
    },
  ];
  const chart = rawToChart(tiles, BOOKS, SCOPES, T0);
  assert.deepEqual(chart.x, [600, 700, 800, 900]);
  assert.deepEqual(chart.books[0].mid, [41, null, null, null]);
  assert.deepEqual(chart.books[1].mid, [null, null, 51, null]); // breaks at scope end (900 s)
  assert.equal(chart.stepped, true);
  assert.deepEqual(valuesAt(chart, 850)[1], { bid: 50, ask: 52, mid: 51 });
  assert.deepEqual(valuesAt(chart, 10)[1], { bid: null, ask: null, mid: null });
  assert.deepEqual(maskAfter(chart, 700).books[1].mid, [
    null,
    null,
    null,
    null,
  ]);
});

test('zoom tier follows the visible span', () => {
  assert.equal(chooseTier(3 * 3600), 'overview');
  assert.equal(chooseTier(2 * 3600), '1s');
  assert.equal(chooseTier(601), '1s');
  assert.equal(chooseTier(600), 'raw');
  assert.equal(chooseTier(5), 'raw');
  assert.deepEqual(tilesInRange(590, 1250, 10, ['0', '10', '20']), [
    '0',
    '10',
    '20',
  ]);
  assert.deepEqual(tilesInRange(600, 1199, 10, ['0', '10', '20']), ['10']);
  assert.deepEqual(tilesInRange(901, 1199, 10, ['0', '20']), []);
});

test('legend groups cross-venue twins under one claim', () => {
  const groups = legendGroups(BOOKS);
  assert.deepEqual(
    groups.map((g) => [g.label, g.books.map((b) => b.book)]),
    [
      ['series_moneyline · Cloud9', [0, 1]],
      ['series_moneyline · LYON', [2]],
      ['kalshi:M', [3]],
    ],
  );
  const noContext = BOOKS.map((b) => ({ ...b, claim_id: null }));
  assert.equal(legendGroups(noContext).length, 4);
});

test('a hidden book never changes the y range', () => {
  const chart = gridsToChart([OVERVIEW], BOOKS, T0);
  const range = fixedYRange(chart);
  assert.deepEqual(range, [7, 64]);
  assert.deepEqual(fixedYRange({ x: [], books: [], stepped: false }), [0, 100]);
  // Zoomed to the last bucket only: the range follows the visible span.
  assert.deepEqual(fixedYRange(chart, [0, 1000]), [7, 54]);
});

const GAME: GameDocument = {
  state: 'ok',
  reason: null,
  source: { name: 'kalshi' },
  competitors: {
    home: { label: 'Cloud9', participant: 0 },
    away: { label: 'LYON', participant: 1 },
  },
  windows: { status: 'provisional', bases: {} },
  events: [
    {
      kind: 'scheduled_start',
      segment: null,
      label: 'Scheduled start',
      time_basis: 'scheduled',
      source_ns: ns(100),
      earliest_ns: ns(100),
      latest_ns: ns(100),
      exact: false,
      known_at_ns: ns(100),
      window_status: null,
      detail: {},
    },
    {
      kind: 'segment_end',
      segment: 1,
      label: 'Map 1 end · Cloud9',
      time_basis: 'kalshi_market_close',
      source_ns: ns(500),
      earliest_ns: ns(498),
      latest_ns: ns(505),
      exact: false,
      known_at_ns: ns(505),
      window_status: 'provisional',
      detail: {},
    },
    {
      kind: 'settlement',
      segment: 1,
      label: 'Map 1 settled',
      time_basis: 'market_settlement',
      source_ns: ns(600),
      earliest_ns: ns(600),
      latest_ns: ns(600),
      exact: true,
      known_at_ns: ns(600),
      window_status: null,
      detail: {},
    },
  ],
};

test('game marks: bracket for a window, diamond only when exact, dashed scheduled start', () => {
  const marks = gameMarks(GAME.events, T0);
  assert.deepEqual(
    marks.map((m) => [m.shape, m.x0, m.x1]),
    [
      ['scheduled', 100, 100],
      ['bracket', 498, 505],
      ['diamond', 600, 600],
    ],
  );
});

test('as-of game state uses known_at, not the source time', () => {
  assert.deepEqual(knownAt(GAME.events, T0 + 504n * 1_000_000_000n), []);
  assert.equal(knownAt(GAME.events, T0 + 505n * 1_000_000_000n).length, 1);
  assert.equal(
    gameSummaryAt(GAME, T0 + 550n * 1_000_000_000n),
    'Map 1 end · Cloud9',
  );
  assert.equal(gameSummaryAt(GAME, T0), 'Before the first known game fact');
  assert.equal(
    gameSummaryAt(
      { ...GAME, state: 'no_source', reason: 'no_context', events: [] },
      T0,
    ),
    'No game-state source (no_context)',
  );
});

function item(id: string, start: number, end: number): IntervalItem {
  return {
    id,
    start_ns: ns(start),
    end_ns: ns(end),
    books: [0, 1],
    label: 'net',
    censored: false,
    end_reason: 'KILL_PRICE',
    metrics: [
      { name: 'duration', value: String((end - start) * 1000), unit: 'ms' },
    ],
    detail: { kind: 'net' },
  };
}

test('sub-pixel episodes collapse into a density strip with counts', () => {
  const items = [
    item('a', 10, 10.001),
    item('b', 10.002, 10.003),
    item('c', 100, 200),
    item('d', 5000, 5001),
  ];
  const marks = lensMarks(items, T0, [0, 1800], 900);
  assert.deepEqual(
    marks.map((m) =>
      m.kind === 'item' ? ['item', m.item.id] : ['density', m.count, m.x0],
    ),
    [
      ['item', 'c'],
      ['density', 2, 0],
    ],
  );
  const zoomed = lensMarks(items, T0, [9.999, 10.004], 1000);
  assert.deepEqual(
    zoomed.map((m) => m.kind),
    ['item', 'item'],
  );
});

test('lens layers come from ?lenses=, the older ?lens=, or default to cross', () => {
  const available = ['complement', 'cross', 'multi'];
  const layers = (query: string, from = available) =>
    parseLayers(new URLSearchParams(query), from);
  assert.deepEqual(layers(''), ['cross']);
  assert.deepEqual(layers('', ['multi', 'complement']), ['multi']);
  assert.deepEqual(layers('', []), []);
  // Order-independent, returned in lens order; unknown or unavailable names ignored.
  assert.deepEqual(layers('lenses=multi,cross'), ['cross', 'multi']);
  assert.deepEqual(layers('lenses=multi,bogus,cover_same'), ['multi']);
  assert.deepEqual(layers('lenses='), []);
  assert.deepEqual(layers('lens=multi'), ['multi']);
  assert.deepEqual(layers('lens='), []);
  assert.deepEqual(layers('lens=bogus'), []);
  // ?lenses= wins over ?lens=, and writing layers drops ?lens= and keeps the rest.
  assert.deepEqual(layers('lens=complement&lenses=multi'), ['multi']);
  const next = withLayers(new URLSearchParams('lens=cross&t=12'), [
    'cross',
    'multi',
  ]);
  assert.equal(next.toString(), 't=12&lenses=cross%2Cmulti');
  assert.deepEqual(parseLayers(next, available), ['cross', 'multi']);
  assert.deepEqual(
    parseLayers(withLayers(next, []), available),
    [],
    'turning every layer off is a shareable empty set',
  );
});

test('episodes of every active layer merge in time order, tagged with their lens', () => {
  const cross = [item('x1', 10, 20), item('x2', 50, 60), item('x3', 50, 51)];
  const multi = [item('m1', 5, 6), item('m2', 50, 55)];
  const merged = mergeEpisodes([
    { lens: 'cross', items: cross },
    { lens: 'multi', items: multi },
  ]);
  assert.deepEqual(
    merged.map((e) => e.key),
    ['multi:m1', 'cross:x1', 'cross:x2', 'cross:x3', 'multi:m2'],
  );
  assert.equal(merged[0].item, multi[0]);
  // Turning a layer off removes exactly its episodes.
  const crossOnly = mergeEpisodes([{ lens: 'cross', items: cross }]);
  assert.deepEqual(
    crossOnly.map((e) => e.key),
    ['cross:x1', 'cross:x2', 'cross:x3'],
  );
  assert.deepEqual(mergeEpisodes([]), []);
  // The same episode id in two lenses stays two distinct rows.
  const twice = mergeEpisodes([
    { lens: 'cross', items: [item('same', 1, 2)] },
    { lens: 'multi', items: [item('same', 1, 2)] },
  ]);
  assert.deepEqual(
    twice.map((e) => e.key),
    ['cross:same', 'multi:same'],
  );
});

test('next moment finds the next game fact, episode start or availability change', () => {
  const items = [item('a', 50, 51)];
  const availability = {
    state: 'ok' as const,
    books: [
      {
        book: 0,
        intervals: [
          {
            start_ns: ns(0),
            end_ns: ns(700),
            scope: 0,
            state: 'usable',
            reason: null,
          },
          {
            start_ns: ns(700),
            end_ns: ns(900),
            scope: 0,
            state: 'unusable',
            reason: 'connection_closed',
          },
        ],
      },
    ],
  };
  assert.equal(nextMoment(0, T0, GAME.events, items, availability), 50);
  assert.equal(nextMoment(50, T0, GAME.events, items, availability), 100);
  assert.equal(nextMoment(600, T0, GAME.events, items, availability), 700);
  assert.equal(nextMoment(700, T0, GAME.events, items, availability), null);
});

function event(
  bundle: string,
  title: string,
  game: string | null,
  cross: string,
  start: string | null,
): EventRow {
  return {
    bundle_id: bundle,
    event_id: null,
    corpus_key: null,
    state: 'ok',
    message: null,
    pack: 'ok',
    pack_error: null,
    context: 'missing',
    game,
    title,
    capture_start_ns: start,
    duration_s: 1,
    venues: ['kalshi'],
    book_count: 3,
    scope_count: 1,
    game_state: 'no_source',
    bench_status: 'SUCCESS',
    cross_state: cross,
    cross_headline: 1,
  } as EventRow;
}

test('event filters report how many rows they hid', () => {
  const rows = [
    event(
      'bundle_a',
      'Cloud9 vs LYON',
      'league_of_legends',
      'ok',
      '1791000000000000000',
    ),
    event('bundle_b', 'bundle_b', null, 'failed', null),
    event('bundle_c', 'G2 vs M8', 'valorant', 'ok', '1790000000000000000'),
  ];
  const base = { query: '', game: null, lens: 'cross', lensState: null };
  const count = (f: typeof base | Record<string, unknown>) => {
    const { shown, hidden } = filterEvents(
      rows,
      f as Parameters<typeof filterEvents>[1],
    );
    return [shown.map((r) => r.bundle_id), hidden];
  };
  assert.deepEqual(count(base), [['bundle_a', 'bundle_b', 'bundle_c'], 0]);
  assert.deepEqual(count({ ...base, lensState: 'failed' }), [['bundle_b'], 2]);
  assert.deepEqual(count({ ...base, game: 'valorant' }), [['bundle_c'], 2]);
  assert.deepEqual(count({ ...base, query: 'lyon' }), [['bundle_a'], 2]);
  assert.deepEqual(
    sortEvents(rows, 'capture_start_ns', false).map((r) => r.bundle_id),
    ['bundle_c', 'bundle_a', 'bundle_b'],
  );
  assert.deepEqual(
    sortEvents(rows, 'capture_start_ns', true).map((r) => r.bundle_id),
    ['bundle_a', 'bundle_c', 'bundle_b'],
  );
});

test('manifest identity check rejects wrong length and wrong hash', async () => {
  const bytes = new TextEncoder().encode('{"a":1}\n');
  const sha256 = createHash('sha256').update(bytes).digest('hex');
  await verifyIdentity(
    bytes,
    { sha256, byte_length: bytes.byteLength },
    'x.json',
  );
  await assert.rejects(
    verifyIdentity(bytes, { sha256, byte_length: 3 }, 'x.json'),
    IdentityError,
  );
  await assert.rejects(
    verifyIdentity(
      bytes,
      { sha256: '0'.repeat(64), byte_length: bytes.byteLength },
      'x.json',
    ),
    /does not match the manifest/,
  );
});

test('offsets format as +hh:mm:ss', () => {
  assert.equal(formatOffset(3725), '+01:02:05');
  assert.equal(formatOffset(0.25), '+00:00:00.250');
});

test('long instrument ids are abbreviated for display only', () => {
  const token = 'polymarket:' + '1'.repeat(30) + '987654';
  assert.equal(
    displayLabel({ ...BOOKS[1], label: token }),
    'polymarket:111111…987654',
  );
  assert.equal(displayLabel(BOOKS[0]), 'series_moneyline · Cloud9');
  assert.equal(
    displayLabel({ ...BOOKS[0], label: 'kalshi:KXLOLGAME-X' }),
    'kalshi:KXLOLGAME-X',
  );
});

test('an empty VITE_CORPUS_BASE_URL falls back to /corpus in dev only', () => {
  assert.equal(resolveCorpusBase('', true), '/corpus');
  assert.equal(resolveCorpusBase(undefined, true), '/corpus');
  assert.equal(resolveCorpusBase('', false), '');
  assert.equal(resolveCorpusBase(undefined, false), '');
  assert.equal(
    resolveCorpusBase('https://cdn.example/corpus/', false),
    'https://cdn.example/corpus',
  );
});

test('a provisional game window says so in its hover text', () => {
  const [, bracket, diamond] = gameMarks(GAME.events, T0);
  assert.match(
    gameMarkTitle(bracket, T0),
    /provisional starting value, not calibrated/,
  );
  assert.doesNotMatch(gameMarkTitle(diamond, T0), /window/);
});
