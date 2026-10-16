// Opt-in end-to-end test of the research site against a real research build.
//
//   RESEARCH_CORPUS_DIR=<replay.research build --out dir> yarn e2e:research
//
// It builds the production client (vite build), serves it with gzip from a small
// static server together with RESEARCH_CORPUS_DIR at /corpus and a local fixture
// Event Universe at /universe-api, and drives the system Chrome through
// playwright-core. Nothing contacts a remote host. Screenshots and report.json go
// to E2E_OUTPUT (default: <tmp>/research-e2e). Exit status 1 on any failure.
import { spawn, type ChildProcess } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import http from 'node:http';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import { chromium, type Browser, type Page } from 'playwright-core';
import { build } from 'vite';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const UI = path.resolve(HERE, '..');
const REPO = path.resolve(UI, '..');
const CORPUS = process.env.RESEARCH_CORPUS_DIR
  ? path.resolve(process.env.RESEARCH_CORPUS_DIR)
  : '';
const OUTPUT = path.resolve(
  process.env.E2E_OUTPUT ?? path.join(tmpdir(), 'research-e2e'),
);
const PORT = Number(process.env.E2E_PORT ?? 4317);
const UNIVERSE_PORT = PORT + 1;
const BASE = `http://localhost:${PORT}`;
const BUDGET_BYTES = 3 * 1024 * 1024;

type Json = Record<string, any>;
const readJson = (file: string): Json => JSON.parse(readFileSync(file, 'utf8'));
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ------------------------------------------------------------------ server

function contentType(file: string) {
  if (file.endsWith('.html')) return 'text/html; charset=utf-8';
  if (file.endsWith('.js')) return 'text/javascript; charset=utf-8';
  if (file.endsWith('.css')) return 'text/css; charset=utf-8';
  if (file.endsWith('.json')) return 'application/json';
  if (file.endsWith('.svg')) return 'image/svg+xml';
  if (file.endsWith('.md')) return 'text/markdown; charset=utf-8';
  return 'application/octet-stream';
}

function startServer(dist: string): Promise<http.Server> {
  const gzipCache = new Map<string, Buffer>();
  const serveFile = (
    request: http.IncomingMessage,
    response: http.ServerResponse,
    file: string,
  ) => {
    const type = contentType(file);
    const compressible = /json|javascript|css|html|svg|markdown/.test(type);
    response.setHeader('Content-Type', type);
    response.setHeader('Cache-Control', 'no-cache');
    if (
      compressible &&
      /\bgzip\b/.test(String(request.headers['accept-encoding'] ?? ''))
    ) {
      let body = gzipCache.get(file);
      if (!body) {
        body = gzipSync(readFileSync(file), { level: 6 });
        gzipCache.set(file, body);
      }
      response.setHeader('Content-Encoding', 'gzip');
      response.setHeader('Content-Length', String(body.length));
      return response.end(request.method === 'HEAD' ? undefined : body);
    }
    const body = readFileSync(file);
    response.setHeader('Content-Length', String(body.length));
    response.end(request.method === 'HEAD' ? undefined : body);
  };
  const inside = (root: string, relative: string) => {
    const file = path.resolve(root, '.' + relative);
    return file === root || file.startsWith(root + path.sep) ? file : null;
  };
  const server = http.createServer((request, response) => {
    const url = new URL(request.url ?? '/', BASE);
    if (url.pathname.startsWith('/universe-api/')) {
      const upstream = http.request(
        {
          host: '127.0.0.1',
          port: UNIVERSE_PORT,
          method: request.method,
          path: url.pathname.slice('/universe-api'.length) + url.search,
          headers: { ...request.headers, host: `127.0.0.1:${UNIVERSE_PORT}` },
        },
        (reply) => {
          response.writeHead(reply.statusCode ?? 502, reply.headers);
          reply.pipe(response);
        },
      );
      upstream.on('error', () => {
        response.statusCode = 502;
        response.end();
      });
      request.pipe(upstream);
      return;
    }
    if (url.pathname.startsWith('/corpus/')) {
      const file = inside(
        CORPUS,
        decodeURIComponent(url.pathname.slice('/corpus'.length)),
      );
      if (!file || !existsSync(file) || !statSync(file).isFile()) {
        response.statusCode = 404;
        return response.end();
      }
      return serveFile(request, response, file);
    }
    const file = inside(dist, decodeURIComponent(url.pathname));
    if (file && existsSync(file) && statSync(file).isFile())
      return serveFile(request, response, file);
    return serveFile(request, response, path.join(dist, 'index.html')); // SPA fallback
  });
  return new Promise((resolve) =>
    server.listen(PORT, '127.0.0.1', () => resolve(server)),
  );
}

async function startUniverse(): Promise<ChildProcess> {
  const python = path.join(REPO, '.venv', 'bin', 'python');
  const child = spawn(
    python,
    [path.join(HERE, 'universe_fixture_server.py'), String(UNIVERSE_PORT)],
    {
      cwd: REPO,
      env: { ...process.env, PYTHONPATH: REPO },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  let log = ''; // the access log is kept quiet and shown only if the server fails
  child.stderr!.on(
    'data',
    (chunk) => (log = (log + String(chunk)).slice(-4000)),
  );
  await new Promise<void>((resolve, reject) => {
    child.stdout!.on(
      'data',
      (chunk) => String(chunk).includes('listening') && resolve(),
    );
    child.on('exit', (code) =>
      reject(new Error(`fixture Universe exited ${code}\n${log}`)),
    );
  });
  return child;
}

// ------------------------------------------------------------------ helpers

interface Report {
  page: string;
  checks: { name: string; pass: boolean; detail?: unknown }[];
  problems: string[];
  [key: string]: unknown;
}

function watch(page: Page, report: Report) {
  page.on('console', (message) => {
    if (message.type() === 'error')
      report.problems.push(`console: ${message.text()}`);
  });
  page.on('pageerror', (error) =>
    report.problems.push(`pageerror: ${error.message}`),
  );
  page.on('requestfailed', (request) =>
    report.problems.push(
      `requestfailed: ${request.url()} ${request.failure()?.errorText ?? ''}`,
    ),
  );
  page.on('response', (response) => {
    if (response.status() >= 400)
      report.problems.push(`HTTP ${response.status()} ${response.url()}`);
  });
}

function check(report: Report, name: string, pass: boolean, detail?: unknown) {
  report.checks.push({
    name,
    pass,
    ...(detail === undefined ? {} : { detail }),
  });
  if (!pass)
    console.log(
      `  FAIL ${report.page} · ${name} ${detail === undefined ? '' : JSON.stringify(detail)}`,
    );
}

async function noErrorState(page: Page) {
  return (await page.locator('.error-state').count()) === 0;
}

function cents(atoms: number | null, scale: number): number | null {
  if (atoms === null) return null;
  const shift = scale - 2;
  return shift >= 0 ? atoms / 10 ** shift : atoms * 10 ** -shift;
}

function offset(ns: string | bigint, start: bigint) {
  return Number(BigInt(ns) - start) / 1e9;
}

async function setCursor(page: Page, t: number): Promise<number> {
  await page.evaluate((value) => {
    const input = document.querySelector<HTMLInputElement>(
      'input[aria-label="Cursor"]',
    )!;
    const setter = Object.getOwnPropertyDescriptor(
      HTMLInputElement.prototype,
      'value',
    )!.set!;
    setter.call(input, String(value));
    input.dispatchEvent(new Event('input', { bubbles: true }));
  }, t);
  await sleep(150);
  return cursorValue(page);
}

async function cursorValue(page: Page): Promise<number> {
  return Number(
    await page
      .locator('input[aria-label="Cursor"]')
      .getAttribute('data-cursor'),
  );
}

async function readLayers(page: Page) {
  return page.$$eval('.research-layers .lens-chip-wrap', (wraps) =>
    wraps.map((wrap) => {
      const button = wrap.querySelector('button')!;
      return {
        lens: wrap.getAttribute('data-lens') ?? '',
        disabled: button.disabled,
        on: button.getAttribute('aria-pressed') === 'true',
        text: (wrap as HTMLElement).innerText,
      };
    }),
  );
}

/** Click chips until exactly ``wanted`` layers are on. */
async function setLayers(page: Page, wanted: string[]) {
  for (const chip of await readLayers(page)) {
    if (chip.on !== wanted.includes(chip.lens))
      await page
        .locator(`.lens-chip-wrap[data-lens="${chip.lens}"] button`)
        .click();
  }
  await page.waitForFunction(
    (names) =>
      [...document.querySelectorAll('.lens-track')]
        .map((t) => t.getAttribute('data-lens'))
        .join(',') === names.join(','),
    wanted,
    { timeout: 15_000 },
  );
  await sleep(150);
}

async function trackLenses(page: Page) {
  return page.$$eval('.lens-track', (tracks) =>
    tracks.map((t) => t.getAttribute('data-lens') ?? ''),
  );
}

async function listedEpisodes(page: Page) {
  return page.$$eval('.episode-list button', (rows) =>
    rows.map(
      (r) => `${r.getAttribute('data-lens')}:${r.getAttribute('data-episode')}`,
    ),
  );
}

/** The expected merged order: start time, then layer (pack lens) order, then file order. */
function mergedKeys(lenses: string[], overlays: Record<string, Json[]>) {
  return lenses
    .flatMap((lens, layer) =>
      overlays[lens].map((item) => ({
        key: `${lens}:${item.id}`,
        start: BigInt(item.start_ns),
        layer,
      })),
    )
    .sort((a, b) =>
      a.start !== b.start ? (a.start < b.start ? -1 : 1) : a.layer - b.layer,
    )
    .map((e) => e.key);
}

async function readout(page: Page) {
  return page.$$eval('table.readout tbody tr', (rows) =>
    rows.map((row) => ({
      book: Number(row.getAttribute('data-book')),
      bid:
        row.getAttribute('data-bid') === ''
          ? null
          : Number(row.getAttribute('data-bid')),
      ask:
        row.getAttribute('data-ask') === ''
          ? null
          : Number(row.getAttribute('data-ask')),
    })),
  );
}

const near = (a: number | null, b: number | null) =>
  a === null || b === null ? a === b : Math.abs(a - b) < 1e-9;

/** The overview point at or before t, computed from overview.json alone. */
function overviewExpected(
  overview: Json,
  books: Json[],
  start: bigint,
  t: number,
) {
  const bucket = Number(BigInt(overview.bucket_ns)) / 1e9;
  const s0 = offset(overview.start_ns, start);
  const e0 = offset(overview.end_ns, start);
  let index = -1;
  for (let i = 0; i < overview.buckets; i += 1) {
    const low = s0 + i * bucket;
    if ((low + Math.min(low + bucket, e0)) / 2 <= t) index = i;
  }
  return overview.books.map((series: Json) => {
    if (index < 0 || series.usable_ppm[index] === 0)
      return { book: series.book, bid: null, ask: null };
    const scale = books[series.book].price_scale;
    return {
      book: series.book,
      bid: cents(series.bid.last[index], scale),
      ask: cents(series.ask.last[index], scale),
    };
  });
}

/** The raw state as of t from the raw tile JSON alone (initial state plus top rows). */
function rawExpected(
  tiles: Json[],
  books: Json[],
  scopes: Json[],
  start: bigint,
  t: number,
) {
  const state = new Map<
    number,
    {
      t: number;
      bid: number | null;
      ask: number | null;
      usable: boolean;
      end: number;
    }
  >();
  const ends = scopes.map((s) => offset(s.end_ns, start));
  const apply = (book: number, at: number, row: Json, scope: number) =>
    state.set(book, {
      t: at,
      bid: row.bid ? cents(Number(row.bid[0]), books[book].price_scale) : null,
      ask: row.ask ? cents(Number(row.ask[0]), books[book].price_scale) : null,
      usable: row.validity === 'usable',
      end: ends[scope],
    });
  tiles.forEach((tile, position) => {
    if (position === 0)
      for (const initial of tile.initial)
        apply(
          initial.book,
          offset(tile.start_ns, start),
          initial,
          initial.scope,
        );
    for (const row of tile.rows) {
      const at = offset(row.t_ns, start);
      if (row.type === 'top' && at <= t) apply(row.book, at, row, row.scope);
    }
  });
  return books.map((book) => {
    const s = state.get(book.book);
    const live = s && s.usable && t < s.end;
    return {
      book: book.book,
      bid: live ? s!.bid : null,
      ask: live ? s!.ask : null,
    };
  });
}

function compareReadout(actual: Json[], expected: Json[]) {
  const byBook = new Map(expected.map((e) => [e.book, e]));
  const wrong = actual.filter((a) => {
    const e = byBook.get(a.book);
    return !e || !near(a.bid, e.bid) || !near(a.ask, e.ask);
  });
  return {
    rows: actual.length,
    wrong: wrong
      .slice(0, 3)
      .map((w) => ({ ...w, expected: byBook.get(w.book) })),
  };
}

async function canvasInk(page: Page) {
  return page.evaluate(() => {
    const canvas = document.querySelector<HTMLCanvasElement>(
      '.research-chart canvas',
    );
    if (!canvas) return 0;
    const context = canvas.getContext('2d')!;
    const { data } = context.getImageData(0, 0, canvas.width, canvas.height);
    let colourful = 0;
    for (let i = 0; i < data.length; i += 16) {
      const [r, g, b, a] = [data[i], data[i + 1], data[i + 2], data[i + 3]];
      if (a > 0 && Math.max(r, g, b) - Math.min(r, g, b) > 60) colourful += 1;
    }
    return colourful;
  });
}

// ------------------------------------------------------------------ pages

async function corpusPage(
  browser: Browser,
  version: string,
  events: Json,
): Promise<Report> {
  const report: Report = { page: '/research', checks: [], problems: [] };
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
  });
  watch(page, report);
  await page.goto(`${BASE}/research`);
  await page.waitForSelector('.research-table .row:not(.head)', {
    timeout: 30_000,
  });
  await page.waitForLoadState('networkidle');
  const rows = await page.locator('.research-table .row:not(.head)').count();
  check(
    report,
    'events table lists every event',
    rows === events.events.length,
    { rows },
  );
  check(
    report,
    'version shown',
    (await page.locator('.corpus-meta').innerText()).includes(version),
  );
  check(
    report,
    'headline stats render',
    (await page.locator('.stat').count()) === 5,
  );
  check(
    report,
    'four histograms render',
    (await page.locator('.histogram svg').count()) === 4,
  );
  await page.locator('button', { hasText: 'All durations' }).click();
  await page.locator('button', { hasText: 'Deduplicated' }).click();
  await page.locator('input.wide').fill('no-such-event');
  const hidden = await page
    .locator('.research-filters .muted.small')
    .last()
    .innerText();
  check(
    report,
    'filter reports hidden rows',
    hidden.includes(`${events.events.length} hidden`),
    { hidden },
  );
  await page.locator('input.wide').fill('');
  await page.screenshot({
    path: path.join(OUTPUT, 'research.png'),
    fullPage: true,
  });
  check(report, 'no error state', await noErrorState(page));
  await page.locator('.research-table .row.link').first().click();
  await page.waitForURL(/\/research\/events\//);
  check(report, 'row opens the terminal', true);
  await page.close();
  return report;
}

async function eventPage(
  browser: Browser,
  version: string,
  bundle: string,
): Promise<Report> {
  const packDir = path.join(CORPUS, version, 'events', bundle);
  const pack = readJson(path.join(packDir, 'manifest.json'));
  const overview = readJson(path.join(packDir, 'overview.json'));
  const start = BigInt(pack.capture_start_ns);
  const report: Report = {
    page: `/research/events/${bundle}`,
    checks: [],
    problems: [],
  };
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1100 },
  });
  watch(page, report);
  const transfers: { url: string; encoded: number; decoded: number }[] = [];
  let firstPaintDone = false;
  const rawResponses: { url: string; status: number }[] = [];
  page.on('response', (response) => {
    if (response.url().includes('/tiles/raw/'))
      rawResponses.push({ url: response.url(), status: response.status() });
  });
  page.on('requestfinished', async (request) => {
    if (firstPaintDone || !request.url().includes('/corpus/')) return;
    const sizes = await request.sizes().catch(() => null);
    const name = decodeURIComponent(new URL(request.url()).pathname).split(
      `/events/${bundle}/`,
    )[1];
    transfers.push({
      url: request.url(),
      encoded: sizes ? sizes.responseBodySize + sizes.responseHeadersSize : 0,
      decoded: name ? (pack.files[name]?.byte_length ?? 0) : 0,
    });
  });

  // a) b) j): first paint.
  await page.goto(`${BASE}/research/events/${bundle}`);
  await page.waitForSelector('.research-chart canvas', { timeout: 30_000 });
  await page.waitForLoadState('networkidle');
  await sleep(300);
  firstPaintDone = true;
  const encoded = transfers.reduce((sum, t) => sum + t.encoded, 0);
  const decoded = transfers.reduce((sum, t) => sum + t.decoded, 0);
  report.firstPaint = {
    requests: transfers.length,
    transferBytes: encoded,
    decodedPackBytes: decoded,
  };
  check(
    report,
    'first paint under 3 MB transferred',
    encoded < BUDGET_BYTES,
    report.firstPaint,
  );
  check(report, 'no error state after load', await noErrorState(page));
  const ink = await canvasInk(page);
  check(report, 'chart canvas is not blank', ink > 200, {
    colourfulSamples: ink,
  });
  const legend = await page.locator('.legend-entry').count();
  check(
    report,
    'legend count equals book count',
    legend === pack.books.length,
    { legend, books: pack.books.length },
  );
  await page.screenshot({
    path: path.join(OUTPUT, `${bundle}-wide.png`),
    fullPage: true,
  });

  // c) lens layers: one chip per lens; ok lenses switch on, the rest show their state.
  const chips = await readLayers(page);
  const lenses = Object.entries(pack.lenses as Record<string, Json>).filter(
    ([name]) => name !== 'profile',
  );
  const okNames = lenses
    .filter(([, l]) => l.state === 'ok' && l.overlay)
    .map(([n]) => n);
  const overlays: Record<string, Json[]> = Object.fromEntries(
    okNames.map((n) => [
      n,
      readJson(path.join(packDir, 'overlays', `${n}.json`)).items as Json[],
    ]),
  );
  const enabled = chips.filter((c) => !c.disabled).map((c) => c.lens);
  check(
    report,
    'a chip per lens; exactly the ok lenses can be switched on',
    chips.length === lenses.length &&
      JSON.stringify(enabled) === JSON.stringify(okNames),
    { chips: chips.map((c) => c.lens), enabled, okNames },
  );
  const defaults = okNames.includes('cross') ? ['cross'] : okNames.slice(0, 1);
  check(
    report,
    'default layers: cross only',
    JSON.stringify(chips.filter((c) => c.on).map((c) => c.lens)) ===
      JSON.stringify(defaults) &&
      JSON.stringify(await trackLenses(page)) === JSON.stringify(defaults),
    { on: chips.filter((c) => c.on).map((c) => c.lens) },
  );
  for (const [name, state] of lenses.filter(([, l]) => l.state !== 'ok')) {
    const chip = chips.find((c) => c.lens === name);
    const reason = String(state.error ?? state.reason ?? '').slice(0, 40);
    check(
      report,
      `${name} chip disabled with its state and reason`,
      !!chip &&
        chip.disabled &&
        chip.text.toLowerCase().includes(state.state.replace('_', ' ')) &&
        chip.text.includes(reason),
      { chip },
    );
  }

  // Context labels: real title, market labels and the context's market status.
  if (pack.context === 'ok') {
    const heading = await page.locator('.research-heading h1').innerText();
    const labels = await page.$$eval('.legend-entry .legend-label', (nodes) =>
      nodes.map((n) => n.textContent ?? ''),
    );
    check(
      report,
      'title and book labels come from the context, not the degrade path',
      heading === pack.title &&
        !heading.includes(bundle) &&
        pack.books.every((b: Json) => b.label !== b.instrument) &&
        labels.some((l) => pack.books.some((b: Json) => l === b.label)),
      { heading },
    );
    const statuses = await page.$$eval('.legend-entry', (entries) =>
      entries.map(
        (e) => e.querySelector('.market-status')?.textContent ?? null,
      ),
    );
    const wanted = pack.books
      .map((b: Json) => b.market_status)
      .filter((v: string | null) => v)
      .sort();
    check(
      report,
      "legend shows every book's market status",
      JSON.stringify(statuses.filter((v) => v).sort()) ===
        JSON.stringify(wanted),
      { statuses: [...new Set(statuses)] },
    );
  }

  // h) explicit states.
  const gameTrack = await page
    .locator('.research-tracks .track')
    .first()
    .innerText();
  if (pack.game_state.state !== 'ok')
    check(
      report,
      'game state shown explicitly',
      /No game-state source|Game state incomplete/.test(gameTrack),
      { gameTrack },
    );
  if (pack.context !== 'ok')
    check(
      report,
      'missing context notice shown',
      (await page.locator('.notice').count()) === 1,
    );

  // f) cursor readout against overview.json at a few times.
  const end = offset(pack.capture_end_ns, start);
  for (const fraction of [0.1, 0.5, 0.9]) {
    const t = await setCursor(page, end * fraction);
    const result = compareReadout(
      await readout(page),
      overviewExpected(overview, pack.books, start, t),
    );
    check(
      report,
      `overview readout at ${(fraction * 100).toFixed(0)}%`,
      result.rows === pack.books.length && result.wrong.length === 0,
      result,
    );
  }

  // d) e) each ok lens alone: first and last episode; raw zoom around a short one.
  let zoomShot = false;
  for (const name of okNames) {
    const items = mergedKeys([name], overlays).map(
      (key) => overlays[name].find((i) => `${name}:${i.id}` === key)!,
    );
    await setLayers(page, [name]);
    if (!items.length) {
      check(
        report,
        `${name}: a layer with no episodes can be switched on`,
        (await listedEpisodes(page)).length === 0,
      );
      continue;
    }
    await page.waitForSelector('.episode-nav', { timeout: 15_000 });
    for (const [label, item] of [
      ['First', items[0]],
      ['Last', items[items.length - 1]],
    ] as const) {
      await page.locator('.episode-nav button', { hasText: label }).click();
      await page.waitForSelector('dl.episode');
      const text = await page.locator('dl.episode').innerText();
      const missing = item.metrics
        .filter((m: Json) => !text.includes(`${m.value} ${m.unit}`))
        .map((m: Json) => m.name);
      check(
        report,
        `${name}: ${label.toLowerCase()} episode shows every metric with its unit`,
        missing.length === 0,
        { id: item.id.slice(0, 12), missing },
      );
    }
    // A short episode near the start, reached with the Next button, zooms below 10 min.
    const index = items
      .slice(0, 40)
      .findIndex((i) => Number(BigInt(i.end_ns) - BigInt(i.start_ns)) < 60e9);
    if (index >= 0) {
      const target = items[index];
      await page.locator('.episode-nav button', { hasText: 'First' }).click();
      for (let i = 0; i < index; i += 1)
        await page.locator('.episode-nav button', { hasText: 'Next' }).click();
      await page
        .waitForSelector('.research-chart[data-tier="raw"]', {
          timeout: 30_000,
        })
        .catch(() => null);
      const tier = await page
        .locator('.research-chart')
        .getAttribute('data-tier');
      const stepped = await page
        .locator('.research-chart')
        .getAttribute('data-stepped');
      const raw = rawResponses.filter((r) => r.status === 200);
      check(
        report,
        `${name}: zoom under 10 min uses raw tiles drawn as steps`,
        tier === 'raw' && stepped === 'true' && raw.length > 0,
        { tier, stepped, rawTiles: raw.length },
      );
      check(
        report,
        `${name}: raw tiles verified (no error state)`,
        await noErrorState(page),
      );
      const tileNames: string[] = pack.tiles.raw;
      const t = await cursorValue(page);
      const span =
        Number(BigInt(target.end_ns) - BigInt(target.start_ns)) / 1e9;
      const pad = Math.max(2, span * 2);
      const view = [Math.max(0, t - pad), t + span + pad];
      const needed = tileNames.filter(
        (n) => Number(n) * 60 < view[1] && view[0] < Number(n) * 60 + 600,
      );
      const docs = needed.map((n) =>
        readJson(path.join(packDir, 'tiles', 'raw', `${n}.json`)),
      );
      const result = compareReadout(
        await readout(page),
        rawExpected(docs, pack.books, pack.scopes, start, t),
      );
      check(
        report,
        `${name}: raw readout at the episode start`,
        result.wrong.length === 0,
        result,
      );
      if (!zoomShot) {
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.screenshot({
          path: path.join(OUTPUT, `${bundle}-zoomed.png`),
          fullPage: false,
        });
        zoomShot = true;
      }
      await page.locator('button', { hasText: 'Reset zoom' }).click();
      await page.waitForSelector('.research-chart[data-tier="overview"]', {
        timeout: 10_000,
      });
      check(report, `${name}: zoom back out to the overview`, true);
    }
  }

  // k) layers stack and come off: cross + another lens, then cross off.
  const withEpisodes = okNames.filter((n) => overlays[n].length > 0);
  const second = withEpisodes.includes('multi')
    ? 'multi'
    : withEpisodes.find((n) => n !== 'cross');
  if (withEpisodes.includes('cross') && second) {
    const pair = okNames.filter((n) => n === 'cross' || n === second);
    await setLayers(page, pair);
    await page.locator('button', { hasText: 'Reset zoom' }).click();
    const listed = await listedEpisodes(page);
    const expectedPair = mergedKeys(pair, overlays);
    check(
      report,
      `cross + ${second}: one track per layer, every episode listed in time order`,
      JSON.stringify(await trackLenses(page)) === JSON.stringify(pair) &&
        JSON.stringify(listed) === JSON.stringify(expectedPair),
      { tracks: await trackLenses(page), listed: listed.length },
    );
    check(
      report,
      `cross + ${second}: shareable URL`,
      new URL(page.url()).searchParams.get('lenses') === pair.join(','),
      { url: page.url() },
    );
    await page
      .locator(`.episode-list button[data-lens="${second}"]`)
      .first()
      .click();
    await page.waitForSelector('dl.episode');
    const panel = await page.locator('dl.episode').innerText();
    check(
      report,
      `${second}: clicking a listed episode selects it`,
      panel.includes(pack.lenses[second].label),
    );
    await page.locator('button', { hasText: 'Reset zoom' }).click();
    await page.evaluate(() => window.scrollTo(0, 0));
    await sleep(300);
    await page.screenshot({
      path: path.join(OUTPUT, `${bundle}-layers.png`),
      fullPage: true,
    });
    // Turning the second layer off removes its track, its episodes and its selection.
    await setLayers(page, ['cross']);
    const afterOff = await listedEpisodes(page);
    check(
      report,
      `${second} off: its track, episodes and selection disappear`,
      JSON.stringify(await trackLenses(page)) === '["cross"]' &&
        afterOff.length === overlays.cross.length &&
        afterOff.every((k) => k.startsWith('cross:')) &&
        (await page.locator('dl.episode').count()) === 0,
      { afterOff: afterOff.length },
    );
    // Cross off with the other on: the cross overlay and episodes are gone.
    await setLayers(page, [second]);
    const crossOff = await listedEpisodes(page);
    check(
      report,
      `cross off: only ${second} remains`,
      JSON.stringify(await trackLenses(page)) === JSON.stringify([second]) &&
        crossOff.length === overlays[second].length &&
        crossOff.every((k) => k.startsWith(`${second}:`)),
      { crossOff: crossOff.length },
    );
    await page.evaluate(() => window.scrollTo(0, 0));
    await sleep(300);
    await page.screenshot({
      path: path.join(OUTPUT, `${bundle}-cross-off.png`),
      fullPage: true,
    });
    // A shared link restores the set, order-independent; the older ?lens= still works.
    const shared = await browser.newPage({
      viewport: { width: 1440, height: 1100 },
    });
    watch(shared, report);
    for (const [query, wanted] of [
      [`lenses=${[...pair].reverse().join(',')},bogus`, pair],
      [`lens=${second}`, [second]],
      ['lenses=', []],
    ] as const) {
      await shared.goto(`${BASE}/research/events/${bundle}?${query}`);
      await shared.waitForSelector('.research-layers', { timeout: 30_000 });
      await shared
        .waitForFunction(
          (names) =>
            [...document.querySelectorAll('.lens-track')]
              .map((t) => t.getAttribute('data-lens'))
              .join(',') === names.join(','),
          [...wanted],
          { timeout: 15_000 },
        )
        .catch(() => null);
      const on = (await readLayers(shared))
        .filter((c) => c.on)
        .map((c) => c.lens);
      check(
        report,
        `?${query} opens with ${wanted.join(',') || 'no layer'}`,
        JSON.stringify(on) === JSON.stringify(wanted) &&
          JSON.stringify(await trackLenses(shared)) === JSON.stringify(wanted),
        { on },
      );
    }
    await shared.close();
    await setLayers(page, ['cross']);
  }

  // g) playback.
  await page.locator('.research-playback label.check input').uncheck();
  const before = await setCursor(page, end * 0.2);
  await page.locator('.research-playback button', { hasText: 'Play' }).click();
  await sleep(1200);
  const playing =
    (await page.locator('.research-playback button').first().innerText()) ===
    'Pause';
  await page.locator('.research-playback button', { hasText: 'Pause' }).click();
  const paused = await cursorValue(page);
  await sleep(500);
  const still = await cursorValue(page);
  await page
    .locator('.research-playback button', { hasText: 'Next moment' })
    .click();
  await sleep(200);
  const jumped = await cursorValue(page);
  check(
    report,
    'playback starts, pauses, and next moment advances',
    playing && paused > before && still === paused && jumped > still,
    { before, paused, still, jumped },
  );

  // i) phone width.
  await page.setViewportSize({ width: 390, height: 844 });
  await sleep(500);
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth - window.innerWidth,
  );
  const chartWidth = await page
    .locator('.research-chart canvas')
    .evaluate((c) => c.getBoundingClientRect().width);
  const playVisible = await page
    .locator('.research-playback button')
    .first()
    .isVisible();
  check(
    report,
    '390 px: no horizontal page scroll, chart and controls usable',
    overflow <= 0 && chartWidth > 200 && chartWidth <= 390 && playVisible,
    { overflow, chartWidth, playVisible },
  );
  await page.screenshot({
    path: path.join(OUTPUT, `${bundle}-phone.png`),
    fullPage: false,
  });

  check(
    report,
    'no console errors, exceptions or failed requests',
    report.problems.length === 0,
    report.problems.slice(0, 5),
  );
  await page.close();
  return report;
}

async function existingRoutes(browser: Browser): Promise<Report[]> {
  const reports: Report[] = [];
  for (const route of ['/', '/history', '/decisions', '/replay']) {
    const report: Report = { page: route, checks: [], problems: [] };
    const page = await browser.newPage({
      viewport: { width: 1440, height: 1000 },
    });
    watch(page, report);
    await page.goto(`${BASE}${route}`);
    await page.waitForLoadState('networkidle');
    await sleep(1500);
    check(
      report,
      'renders',
      (await page.locator('main').innerText()).trim().length > 0,
    );
    check(
      report,
      'no console errors, exceptions or failed requests',
      report.problems.length === 0,
      report.problems.slice(0, 5),
    );
    await page.close();
    reports.push(report);
  }
  return reports;
}

async function phoneCorpus(browser: Browser): Promise<Report> {
  const report: Report = {
    page: '/research (390 px)',
    checks: [],
    problems: [],
  };
  const page = await browser.newPage({ viewport: { width: 390, height: 844 } });
  watch(page, report);
  await page.goto(`${BASE}/research`);
  await page.waitForSelector('.research-table .row:not(.head)', {
    timeout: 30_000,
  });
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth - window.innerWidth,
  );
  check(report, 'no horizontal page scroll', overflow <= 0, { overflow });
  await page.screenshot({
    path: path.join(OUTPUT, 'research-phone.png'),
    fullPage: false,
  });
  check(
    report,
    'no console errors, exceptions or failed requests',
    report.problems.length === 0,
    report.problems.slice(0, 5),
  );
  await page.close();
  return report;
}

// ------------------------------------------------------------------ main

async function main() {
  if (!CORPUS || !existsSync(path.join(CORPUS, 'current.json'))) {
    console.error(
      'Set RESEARCH_CORPUS_DIR to a replay.research build output (it must hold current.json).',
    );
    process.exit(2);
  }
  mkdirSync(OUTPUT, { recursive: true });
  const pointer = readJson(path.join(CORPUS, 'current.json'));
  const manifestBytes = readFileSync(path.join(CORPUS, pointer.manifest_key));
  if (
    createHash('sha256').update(manifestBytes).digest('hex') !==
    pointer.manifest_sha256
  )
    throw new Error('corpus manifest does not match current.json');
  const version: string = pointer.version;
  const events = readJson(path.join(CORPUS, version, 'events.json'));
  const bundles = events.events
    .filter((e: Json) => e.pack === 'ok')
    .map((e: Json) => e.bundle_id as string);

  const dist = mkdtempSync(path.join(tmpdir(), 'research-dist-'));
  process.env.VITE_UNIVERSE_API_BASE_URL = `${BASE}/universe-api`;
  process.env.VITE_CORPUS_BASE_URL = '/corpus';
  process.env.VITE_REPLAY_SIWE_DOMAIN = `localhost:${PORT}`;
  process.env.VITE_REPLAY_SIWE_URI = `${BASE}/`;
  process.env.VITE_REPLAY_SIWE_STATEMENT = 'Sign in to Prediction Indexer.';
  process.env.VITE_REPLAY_SIWE_CHAIN_ID = '1';
  await build({
    root: UI,
    configFile: path.join(UI, 'vite.config.ts'),
    logLevel: 'warn',
    build: { outDir: dist, emptyOutDir: true },
  });

  const universe = await startUniverse();
  const server = await startServer(dist);
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  const reports: Report[] = [];
  try {
    console.log(
      `research e2e · version ${version} · ${bundles.length} events · output ${OUTPUT}`,
    );
    reports.push(await corpusPage(browser, version, events));
    reports.push(await phoneCorpus(browser));
    for (const bundle of bundles) {
      console.log(`event ${bundle}`);
      reports.push(await eventPage(browser, version, bundle));
    }
    reports.push(...(await existingRoutes(browser)));
  } finally {
    await browser.close();
    server.close();
    universe.kill();
  }
  writeFileSync(
    path.join(OUTPUT, 'report.json'),
    JSON.stringify(reports, null, 1),
  );
  let failed = 0;
  for (const report of reports) {
    const bad = report.checks.filter((c) => !c.pass);
    failed += bad.length;
    console.log(
      `${bad.length ? 'FAIL' : 'ok  '} ${report.page} · ${report.checks.length - bad.length}/${report.checks.length} checks${report.firstPaint ? ` · first paint ${JSON.stringify(report.firstPaint)}` : ''}`,
    );
  }
  const largest = reports
    .filter((r) => r.firstPaint)
    .sort(
      (a, b) =>
        (b.firstPaint as Json).transferBytes -
        (a.firstPaint as Json).transferBytes,
    )[0];
  if (largest)
    console.log(
      `largest first paint: ${largest.page} ${JSON.stringify(largest.firstPaint)}`,
    );
  console.log(failed ? `${failed} checks FAILED` : 'ALL CHECKS PASSED');
  process.exit(failed ? 1 : 0);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
