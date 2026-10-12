// Pure view models for the research site (docs/specs/RESEARCH_SITE_V1.md §5).
// Nothing here computes economics or book state: it reshapes built files for display.

export interface Identity {
  sha256: string;
  byte_length: number;
}

export interface CurrentPointer {
  version: string;
  manifest_key: string;
  manifest_sha256: string;
  manifest_byte_length: number;
}

export interface CorpusManifest {
  corpus_manifest_version: number;
  version: string;
  config_sha256: string;
  inputs: { key_prefix: string; corpus_key: string | null; bundles: number }[];
  superseded: {
    bundle_id: string;
    key_prefix: string;
    corpus_key: string | null;
    by: string;
  }[];
  events: Record<string, Identity & { pack: string }>;
  files: Record<string, Identity>;
}

export type LensStateName = 'ok' | 'failed' | 'excluded' | 'not_run';

export interface LensState {
  state: LensStateName;
  label: string;
  error?: string | null;
  reason?: string;
  overlay?: string;
  episodes?: number;
  semantic_sha256?: string | null;
}

export interface PackBook {
  book: number;
  instrument: string;
  orientation: string;
  venue: string;
  market_id: string | null;
  market_type: string | null;
  label: string;
  claim_id: string | null;
  price_scale: number;
  quantity_scale: number;
}

export interface PackScope {
  start_ns: string;
  end_ns: string;
  run_id: string | null;
  scheduled_start_ns: string | null;
}

export interface PackManifest {
  pack_version: number;
  event_id: string | null;
  bundle_id: string;
  game: string | null;
  title: string;
  participants: string[];
  context: 'ok' | 'missing' | 'invalid';
  capture_start_ns: string;
  capture_end_ns: string;
  scopes: PackScope[];
  books: PackBook[];
  lenses: Record<string, LensState>;
  unmapped_groups: string[];
  game_state: {
    state: 'ok' | 'incomplete' | 'no_source';
    reason: string | null;
    file: string;
  };
  provenance: Record<string, string | boolean | null>;
  overview: { bucket_ns: string; buckets: number };
  tiles: { '1s': string[]; raw: string[] };
  files: Record<string, Identity>;
}

export type Quote = [string, string] | null;

export interface QuoteSeries {
  min: (number | null)[];
  max: (number | null)[];
  last: (number | null)[];
}

export interface GridBook {
  book: number;
  bid: QuoteSeries;
  ask: QuoteSeries;
  usable_ppm: number[];
  trades: number[];
  trade_qty: number[];
}

export interface GridDocument {
  start_ns: string;
  end_ns: string;
  bucket_ns: string;
  buckets: number;
  books: GridBook[];
}

export interface RawRow {
  type: 'top' | 'trade';
  scope: number;
  book: number;
  t_ns: string;
  validity?: string;
  bid?: Quote;
  ask?: Quote;
  cause?: string;
  price?: string;
  qty?: string;
}

export interface RawTile {
  start_ns: string;
  end_ns: string;
  initial: {
    book: number;
    scope: number;
    since_ns: string;
    validity: string;
    bid: Quote;
    ask: Quote;
  }[];
  rows: RawRow[];
}

export interface AvailabilityDocument {
  state: 'ok' | 'missing';
  books: {
    book: number;
    intervals: {
      start_ns: string;
      end_ns: string;
      scope: number;
      state: string;
      reason: string | null;
    }[];
  }[];
}

export interface GameEvent {
  kind:
    | 'scheduled_start'
    | 'segment_start'
    | 'segment_end'
    | 'settlement'
    | 'match_end';
  segment: number | null;
  label: string;
  time_basis: string;
  source_ns: string;
  earliest_ns: string;
  latest_ns: string;
  exact: boolean;
  known_at_ns: string;
  /** 'provisional' until windows are calibrated from the corpus; null without a window. */
  window_status: 'provisional' | 'calibrated' | null;
  detail: Record<string, unknown>;
}

export interface GameDocument {
  state: 'ok' | 'incomplete' | 'no_source';
  reason: string | null;
  source: Record<string, unknown> | null;
  competitors: Record<
    string,
    { label: string | null; participant: number | null } | null
  > | null;
  windows: {
    status: 'provisional' | 'calibrated';
    bases: Record<string, unknown>;
  };
  events: GameEvent[];
}

export interface Metric {
  name: string;
  value: string;
  unit: string;
}

export interface IntervalItem {
  id: string;
  start_ns: string;
  end_ns: string;
  books: number[];
  label: string;
  censored: boolean;
  end_reason: string;
  metrics: Metric[];
  detail: Record<string, unknown>;
}

export interface OverlayDocument {
  overlay_version: number;
  lens: string;
  kind: 'intervals' | 'series' | 'markers';
  label: string;
  semantic_sha256: string | null;
  items: IntervalItem[];
}

// ---------------------------------------------------------------- configuration

/**
 * The research base URL. An unset or empty VITE_CORPUS_BASE_URL (Vite turns
 * `VITE_CORPUS_BASE_URL=` into "") means /corpus in development and "not
 * configured" in production.
 */
export function resolveCorpusBase(
  configured: string | undefined,
  dev: boolean,
): string {
  return (configured || (dev ? '/corpus' : '')).replace(/\/+$/, '');
}

// ---------------------------------------------------------------- identity

export class IdentityError extends Error {}

export function hex(buffer: ArrayBuffer): string {
  return [...new Uint8Array(buffer)]
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

/** Throws unless ``bytes`` have exactly the manifest's length and SHA-256. */
export async function verifyIdentity(
  bytes: Uint8Array,
  expected: Identity,
  label: string,
): Promise<void> {
  if (bytes.byteLength !== expected.byte_length) {
    throw new IdentityError(
      `${label}: ${bytes.byteLength} bytes, manifest says ${expected.byte_length}`,
    );
  }
  const digest = hex(
    await crypto.subtle.digest('SHA-256', bytes as BufferSource),
  );
  if (digest !== expected.sha256) {
    throw new IdentityError(
      `${label}: SHA-256 ${digest.slice(0, 12)}… does not match the manifest`,
    );
  }
}

// ---------------------------------------------------------------- time

export const NS_PER_SECOND = 1_000_000_000n;

/** Seconds since capture start, as a float for the chart axis. */
export function offsetSeconds(
  ns: string | bigint,
  captureStartNs: bigint,
): number {
  const value = typeof ns === 'bigint' ? ns : BigInt(ns);
  return Number(value - captureStartNs) / 1e9;
}

export function formatOffset(seconds: number): string {
  const sign = seconds < 0 ? '−' : '+';
  let rest = Math.abs(seconds);
  const hours = Math.floor(rest / 3600);
  rest -= hours * 3600;
  const minutes = Math.floor(rest / 60);
  rest -= minutes * 60;
  const whole = Math.floor(rest);
  const fraction = rest - whole;
  const base = `${sign}${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(whole).padStart(2, '0')}`;
  return fraction >= 0.0005
    ? `${base}.${String(Math.floor(fraction * 1000)).padStart(3, '0')}`
    : base;
}

export function formatUtc(captureStartNs: bigint, seconds: number): string {
  const ms = Number(captureStartNs / 1_000_000n) + seconds * 1000;
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', ' UTC');
}

// ---------------------------------------------------------------- zoom tiers

export type Tier = 'overview' | '1s' | 'raw';

export const OVERVIEW_ABOVE_SECONDS = 2 * 3600;
export const RAW_AT_OR_BELOW_SECONDS = 10 * 60;

/** Above 2 h the overview; from 2 h down to (not including) 10 min the 1 s tiles; 10 min or less raw. */
export function chooseTier(spanSeconds: number): Tier {
  if (spanSeconds > OVERVIEW_ABOVE_SECONDS) return 'overview';
  if (spanSeconds > RAW_AT_OR_BELOW_SECONDS) return '1s';
  return 'raw';
}

/** Tile names (minutes from capture start) overlapping ``[x0, x1)`` seconds, among those built. */
export function tilesInRange(
  x0: number,
  x1: number,
  tileMinutes: number,
  available: string[],
): string[] {
  const length = tileMinutes * 60;
  return available.filter((name) => {
    const start = Number(name) * 60;
    return start < x1 && x0 < start + length;
  });
}

// ---------------------------------------------------------------- chart data

export interface ChartBook {
  bid: (number | null)[];
  ask: (number | null)[];
  mid: (number | null)[];
}

export interface ChartData {
  x: number[];
  books: ChartBook[];
  stepped: boolean;
}

/** Price atoms to cents per $1. */
export function cents(
  atoms: number | string | null | undefined,
  priceScale: number,
): number | null {
  if (atoms === null || atoms === undefined) return null;
  const shift = priceScale - 2;
  return shift >= 0
    ? Number(atoms) / 10 ** shift
    : Number(atoms) * 10 ** -shift;
}

function mid(bid: number | null, ask: number | null): number | null {
  return bid === null || ask === null ? null : (bid + ask) / 2;
}

/**
 * Bucket grids (overview or 1 s tiles) as chart series, one point per bucket at
 * its midpoint. A bucket with no usable time is null, so the line breaks there.
 */
export function gridsToChart(
  grids: GridDocument[],
  books: PackBook[],
  captureStartNs: bigint,
): ChartData {
  const x: number[] = [];
  const out: ChartBook[] = books.map(() => ({ bid: [], ask: [], mid: [] }));
  for (const grid of grids) {
    const start = offsetSeconds(grid.start_ns, captureStartNs);
    const end = offsetSeconds(grid.end_ns, captureStartNs);
    const bucket = Number(BigInt(grid.bucket_ns)) / 1e9;
    for (let i = 0; i < grid.buckets; i += 1) {
      const low = start + i * bucket;
      x.push((low + Math.min(low + bucket, end)) / 2);
      grid.books.forEach((series, index) => {
        const scale = books[index].price_scale;
        const usable = series.usable_ppm[i] > 0;
        const bid = usable ? cents(series.bid.last[i], scale) : null;
        const ask = usable ? cents(series.ask.last[i], scale) : null;
        out[index].bid.push(bid);
        out[index].ask.push(ask);
        out[index].mid.push(mid(bid, ask));
      });
    }
  }
  return { x, books: out, stepped: false };
}

interface BookState {
  bid: number | null;
  ask: number | null;
  usable: boolean;
  scopeEnd: number;
}

/**
 * Raw tiles as step series: one x per distinct change time across books, each
 * book's state carried forward as of that time. A state is drawn only while the
 * book is usable and in scope; the line breaks at an unusable state and at a
 * scope's end, never interpolated.
 */
export function rawToChart(
  tiles: RawTile[],
  books: PackBook[],
  scopes: PackScope[],
  captureStartNs: bigint,
): ChartData {
  const scopeEnds = scopes.map((s) => offsetSeconds(s.end_ns, captureStartNs));
  const changes: { t: number; book: number; state: BookState | null }[] = [];
  const ordered = [...tiles].sort((a, b) =>
    BigInt(a.start_ns) < BigInt(b.start_ns) ? -1 : 1,
  );
  ordered.forEach((tile, position) => {
    if (position === 0) {
      for (const initial of tile.initial) {
        changes.push({
          t: offsetSeconds(tile.start_ns, captureStartNs),
          book: initial.book,
          state: {
            bid: cents(initial.bid?.[0], books[initial.book].price_scale),
            ask: cents(initial.ask?.[0], books[initial.book].price_scale),
            usable: initial.validity === 'usable',
            scopeEnd: scopeEnds[initial.scope],
          },
        });
      }
    }
    for (const row of tile.rows) {
      if (row.type !== 'top') continue;
      changes.push({
        t: offsetSeconds(row.t_ns, captureStartNs),
        book: row.book,
        state: {
          bid: cents(row.bid?.[0], books[row.book].price_scale),
          ask: cents(row.ask?.[0], books[row.book].price_scale),
          usable: row.validity === 'usable',
          scopeEnd: scopeEnds[row.scope],
        },
      });
    }
  });
  // Scope ends become explicit breaks.
  const breaks = new Set<number>();
  for (const change of changes)
    if (change.state) breaks.add(change.state.scopeEnd);
  const times = new Set<number>(changes.map((c) => c.t));
  for (const t of breaks) times.add(t);
  const x = [...times].sort((a, b) => a - b);
  const out: ChartBook[] = books.map(() => ({ bid: [], ask: [], mid: [] }));
  const current: (BookState | null)[] = books.map(() => null);
  const byTime = new Map<number, typeof changes>();
  for (const change of changes) {
    const list = byTime.get(change.t) ?? [];
    list.push(change);
    byTime.set(change.t, list);
  }
  for (const t of x) {
    for (const change of byTime.get(t) ?? [])
      current[change.book] = change.state;
    current.forEach((state, book) => {
      const live = state !== null && state.usable && t < state.scopeEnd;
      const bid = live ? state.bid : null;
      const ask = live ? state.ask : null;
      out[book].bid.push(bid);
      out[book].ask.push(ask);
      out[book].mid.push(mid(bid, ask));
    });
  }
  return { x, books: out, stepped: true };
}

/** Values at the last point at or before ``t`` (the cursor readout). */
export function valuesAt(
  data: ChartData,
  t: number,
): { bid: number | null; ask: number | null; mid: number | null }[] {
  let lo = 0;
  let hi = data.x.length - 1;
  let found = -1;
  while (lo <= hi) {
    const m = (lo + hi) >> 1;
    if (data.x[m] <= t) {
      found = m;
      lo = m + 1;
    } else hi = m - 1;
  }
  return data.books.map((b) =>
    found < 0
      ? { bid: null, ask: null, mid: null }
      : { bid: b.bid[found], ask: b.ask[found], mid: b.mid[found] },
  );
}

/** Hide everything after the cursor ("Full history" off). */
export function maskAfter(data: ChartData, t: number): ChartData {
  const keep = (values: (number | null)[]) =>
    values.map((v, i) => (data.x[i] <= t ? v : null));
  return {
    ...data,
    books: data.books.map((b) => ({
      bid: keep(b.bid),
      ask: keep(b.ask),
      mid: keep(b.mid),
    })),
  };
}

/**
 * A y range in cents across every book (hidden or not) within the visible span, so
 * hiding a book never rescales the chart, while zooming in does.
 */
export function fixedYRange(
  data: ChartData,
  view?: [number, number],
): [number, number] {
  let lo = Infinity;
  let hi = -Infinity;
  for (const book of data.books) {
    for (const values of [book.bid, book.ask]) {
      for (let i = 0; i < values.length; i += 1) {
        const v = values[i];
        if (
          v === null ||
          (view && (data.x[i] < view[0] || data.x[i] > view[1]))
        )
          continue;
        lo = Math.min(lo, v);
        hi = Math.max(hi, v);
      }
    }
  }
  if (!Number.isFinite(lo)) return [0, 100];
  const pad = Math.max(1, (hi - lo) * 0.05);
  return [
    Math.max(0, Math.floor(lo - pad)),
    Math.min(100, Math.ceil(hi + pad)),
  ];
}

// ---------------------------------------------------------------- legend

export interface LegendGroup {
  key: string;
  label: string;
  claimId: string | null;
  books: PackBook[];
}

/** Cross-venue twins (books sharing a non-null claim id) sit together under one claim label. */
export function legendGroups(books: PackBook[]): LegendGroup[] {
  const groups: LegendGroup[] = [];
  const byClaim = new Map<string, LegendGroup>();
  for (const book of books) {
    if (book.claim_id !== null) {
      const existing = byClaim.get(book.claim_id);
      if (existing) {
        existing.books.push(book);
        continue;
      }
      const group = {
        key: `claim:${book.claim_id}`,
        label: book.label,
        claimId: book.claim_id,
        books: [book],
      };
      byClaim.set(book.claim_id, group);
      groups.push(group);
    } else {
      groups.push({
        key: `book:${book.book}`,
        label: book.label,
        claimId: null,
        books: [book],
      });
    }
  }
  return groups;
}

/** A display label: long instrument ids (78-digit Polymarket tokens) are abbreviated; the full id stays in the manifest. */
export function displayLabel(book: PackBook): string {
  const label = book.label;
  const match = /^([a-z]+):(\d{20,})$/.exec(label);
  return match
    ? `${match[1]}:${match[2].slice(0, 6)}…${match[2].slice(-6)}`
    : label;
}

export function twinCount(books: PackBook[]): number {
  return legendGroups(books).filter((g) => g.books.length > 1).length;
}

// ---------------------------------------------------------------- game track

export interface GameMark {
  shape: 'bracket' | 'diamond' | 'scheduled';
  x0: number;
  x1: number;
  event: GameEvent;
}

/** A bracket spans the window; a diamond only when exact; a scheduled start is a dashed line. */
/** The hover text for a game mark; a provisional window says it is not calibrated. */
export function gameMarkTitle(mark: GameMark, captureStartNs: bigint): string {
  const event = mark.event;
  const lines = [
    event.label,
    `${event.time_basis} · source ${formatOffset(offsetSeconds(event.source_ns, captureStartNs))}`,
  ];
  if (event.window_status !== null) {
    lines.push(
      `window ${formatOffset(mark.x0)} – ${formatOffset(mark.x1)}` +
        (event.window_status === 'provisional'
          ? ' (provisional starting value, not calibrated from the corpus)'
          : ' (calibrated)'),
    );
  }
  if (event.detail && Object.keys(event.detail).length)
    lines.push(JSON.stringify(event.detail));
  return lines.join('\n');
}

export function gameMarks(
  events: GameEvent[],
  captureStartNs: bigint,
): GameMark[] {
  return events.map((event) => ({
    shape:
      event.kind === 'scheduled_start'
        ? 'scheduled'
        : event.exact
          ? 'diamond'
          : 'bracket',
    x0: offsetSeconds(event.earliest_ns, captureStartNs),
    x1: offsetSeconds(event.latest_ns, captureStartNs),
    event,
  }));
}

/** Facts known as of the cursor: ``known_at_ns`` at or before it. */
export function knownAt(events: GameEvent[], cursorNs: bigint): GameEvent[] {
  return events.filter(
    (event) =>
      event.kind !== 'scheduled_start' && BigInt(event.known_at_ns) <= cursorNs,
  );
}

export function gameSummaryAt(
  document: GameDocument,
  cursorNs: bigint,
): string {
  if (document.state !== 'ok') {
    return document.state === 'incomplete'
      ? `Game state incomplete${document.reason ? ` (${document.reason})` : ''}`
      : `No game-state source${document.reason ? ` (${document.reason})` : ''}`;
  }
  const known = knownAt(document.events, cursorNs);
  if (!known.length) return 'Before the first known game fact';
  return known[known.length - 1].label;
}

// ---------------------------------------------------------------- lens track

export type LensMark =
  | { kind: 'item'; item: IntervalItem; x0: number; x1: number }
  | { kind: 'density'; x0: number; x1: number; count: number };

/**
 * Intervals at pixel positions for a view of ``width`` px over ``[v0, v1]`` s. Items
 * narrower than ``minPx`` collapse into density bins of ``binPx`` with counts.
 */
export function lensMarks(
  items: IntervalItem[],
  captureStartNs: bigint,
  view: [number, number],
  width: number,
  minPx = 2,
  binPx = 6,
): LensMark[] {
  const [v0, v1] = view;
  const scale = width / (v1 - v0);
  const marks: LensMark[] = [];
  const bins = new Map<number, number>();
  for (const item of items) {
    const s = offsetSeconds(item.start_ns, captureStartNs);
    const e = offsetSeconds(item.end_ns, captureStartNs);
    if (e < v0 || s > v1) continue;
    const x0 = (s - v0) * scale;
    const x1 = (e - v0) * scale;
    if (x1 - x0 >= minPx) {
      marks.push({ kind: 'item', item, x0, x1 });
    } else {
      const bin = Math.floor(Math.max(0, x0) / binPx);
      bins.set(bin, (bins.get(bin) ?? 0) + 1);
    }
  }
  for (const [bin, count] of [...bins.entries()].sort((a, b) => a[0] - b[0])) {
    marks.push({
      kind: 'density',
      x0: bin * binPx,
      x1: (bin + 1) * binPx,
      count,
    });
  }
  return marks;
}

export function metric(item: IntervalItem, name: string): Metric | undefined {
  return item.metrics.find((m) => m.name === name);
}

// ---------------------------------------------------------------- moments

/** The next game fact, episode start or availability change strictly after ``t`` seconds. */
export function nextMoment(
  t: number,
  captureStartNs: bigint,
  game: GameEvent[],
  items: IntervalItem[],
  availability: AvailabilityDocument | null,
): number | null {
  let best: number | null = null;
  const consider = (value: number) => {
    if (value > t + 1e-9 && (best === null || value < best)) best = value;
  };
  for (const event of game)
    consider(offsetSeconds(event.known_at_ns, captureStartNs));
  for (const item of items)
    consider(offsetSeconds(item.start_ns, captureStartNs));
  for (const book of availability?.books ?? []) {
    for (const interval of book.intervals)
      consider(offsetSeconds(interval.start_ns, captureStartNs));
  }
  return best;
}

// ---------------------------------------------------------------- corpus index

export interface HistogramSpec {
  edges: number[];
  counts: number[];
}

export interface LensStats {
  events: number;
  episodes: number;
  dedup_episodes: number;
  events_with_one: number;
  share_with_one: number | null;
  per_event_mean: number | null;
  per_event_median: number | null;
  dedup_per_event_mean: number | null;
  dedup_per_event_median: number | null;
  net_total: number;
  net_max: number | null;
  histograms: Record<
    'per_event' | 'duration_ms' | 'net' | 'quantity',
    HistogramSpec
  >;
}

export interface LensSummary {
  label: string;
  adapter: string;
  states: Record<LensStateName, number>;
  headline_kind?: string;
  value_unit?: string;
  all?: LensStats;
  min_100ms?: LensStats;
}

export interface CorpusSummary {
  summary_version: number;
  min_duration_ms: number;
  events: Record<string, number>;
  packs: number;
  lenses: Record<string, LensSummary>;
}

export type EventRow = {
  bundle_id: string;
  event_id: string | null;
  corpus_key: string | null;
  state: string;
  message: string | null;
  pack: string | null;
  pack_error: string | null;
  context: string;
  game: string | null;
  title: string | null;
  capture_start_ns: string | null;
  duration_s: number | null;
  venues: string[] | null;
  book_count: number | null;
  scope_count: number | null;
  game_state: string | null;
  bench_status: string | null;
} & Record<string, string | number | string[] | null>;

export interface EventsIndex {
  events_version: number;
  lenses: { lens: string; label: string; adapter: string }[];
  events: EventRow[];
}

export interface EventFilter {
  query: string;
  game: string | null;
  lens: string;
  lensState: LensStateName | null;
}

/** The display filter over the events table: rows shown and how many it hid (never silently). */
export function filterEvents(
  rows: EventRow[],
  filter: EventFilter,
): { shown: EventRow[]; hidden: number } {
  const query = filter.query.trim().toLowerCase();
  const shown = rows.filter(
    (row) =>
      (filter.game === null || row.game === filter.game) &&
      (filter.lensState === null ||
        row[`${filter.lens}_state`] === filter.lensState) &&
      (!query ||
        `${row.title ?? ''} ${row.bundle_id} ${row.game ?? ''}`
          .toLowerCase()
          .includes(query)),
  );
  return { shown, hidden: rows.length - shown.length };
}

/** Stable sort with nulls last in either direction; nanosecond strings compare as integers. */
export function sortEvents(
  rows: EventRow[],
  key: string,
  desc: boolean,
): EventRow[] {
  const value = (row: EventRow) => {
    const v = row[key];
    if (v === null || v === undefined) return null;
    if (key.endsWith('_ns') && typeof v === 'string') return BigInt(v);
    return Array.isArray(v) ? v.join(',') : v;
  };
  return rows
    .map((row, index) => ({ row, index, v: value(row) }))
    .sort((a, b) => {
      if (a.v === null || b.v === null)
        return a.v === b.v ? a.index - b.index : a.v === null ? 1 : -1;
      const order = a.v < b.v ? -1 : a.v > b.v ? 1 : 0;
      return (desc ? -order : order) || a.index - b.index;
    })
    .map((entry) => entry.row);
}
