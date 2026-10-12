import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useQueries } from '@tanstack/react-query';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import {
  packFile,
  researchKeys,
  useCorpus,
  usePack,
  usePackFile,
  type Corpus,
} from './research-data';
import {
  ResearchChart,
  type BookStyle,
  type PlotLayout,
} from './research-chart';
import {
  chooseTier,
  displayLabel,
  fixedYRange,
  formatOffset,
  formatUtc,
  gameMarks,
  gameSummaryAt,
  gridsToChart,
  knownAt,
  legendGroups,
  lensMarks,
  maskAfter,
  metric,
  nextMoment,
  offsetSeconds,
  rawToChart,
  tilesInRange,
  valuesAt,
  type AvailabilityDocument,
  type ChartData,
  type GameDocument,
  type GridDocument,
  type IntervalItem,
  type OverlayDocument,
  type PackBook,
  type PackManifest,
  type RawTile,
} from './research-model';
import './research.css';

// Reference palette, dark steps, in fixed order (dataviz references/palette.md).
const HUES = [
  '#3987e5',
  '#d95926',
  '#199e70',
  '#c98500',
  '#d55181',
  '#008300',
  '#9085e9',
  '#e66767',
];
const VENUE_DASH: Record<string, number[]> = {
  kalshi: [6, 4],
  polymarket: [],
  limitless: [2, 3],
};
const TILE_MINUTES = 10;
const SPEEDS = [1, 10, 60, 300];

export function bookStyles(books: PackBook[]): BookStyle[] {
  const styles: BookStyle[] = books.map(() => ({
    color: HUES[0],
    dash: [],
    width: 1.5,
  }));
  legendGroups(books).forEach((group, index) => {
    for (const book of group.books) {
      // Past eight groups the hue repeats with a thinner line (composite encoding); the
      // legend always names every book.
      styles[book.book] = {
        color: HUES[index % HUES.length],
        dash: VENUE_DASH[book.venue] ?? [1, 2],
        width: index < HUES.length ? 1.75 : 1,
      };
    }
  });
  return styles;
}

function cents(value: number | null): string {
  return value === null ? '—' : `${value.toFixed(value % 1 ? 2 : 0)}¢`;
}

function Swatch({ style }: { style: BookStyle }) {
  return (
    <svg className="swatch" width="26" height="10" aria-hidden="true">
      <line
        x1="1"
        y1="5"
        x2="25"
        y2="5"
        stroke={style.color}
        strokeWidth={2}
        strokeDasharray={style.dash.join(' ') || undefined}
      />
    </svg>
  );
}

function StateBadge({
  state,
  title,
}: {
  state: string;
  title?: string | null;
}) {
  return (
    <span
      className={`state-badge state-${state.replace(/[^a-z_]/g, '')}`}
      title={title ?? undefined}
    >
      {state.replace('_', ' ')}
    </span>
  );
}

export function ResearchEventPage() {
  const { bundleId = '' } = useParams();
  const corpus = useCorpus();
  const pack = usePack(corpus.data, bundleId);
  const error = corpus.error ?? pack.error;
  return (
    <div className="research-page">
      <div className="research-crumbs">
        <Link to="/research">Research</Link> / <span>{bundleId}</span>
      </div>
      {error ? (
        <div className="error-state" role="alert">
          {(error as Error).message}
        </div>
      ) : !corpus.data || !pack.data ? (
        <div className="empty-state">
          Loading and verifying the research pack…
        </div>
      ) : (
        <Terminal corpus={corpus.data} pack={pack.data} />
      )}
    </div>
  );
}

function useTiles(
  corpus: Corpus,
  pack: PackManifest,
  kind: '1s' | 'raw' | null,
  names: string[],
) {
  return useQueries({
    queries: (kind === null ? [] : names).map((name) => ({
      queryKey: researchKeys.file(
        corpus.pointer.version,
        pack.bundle_id,
        `tiles/${kind}/${name}.json`,
      ),
      queryFn: () =>
        packFile<GridDocument | RawTile>(
          corpus,
          pack,
          `tiles/${kind}/${name}.json`,
        ),
      staleTime: Number.POSITIVE_INFINITY,
      gcTime: 5 * 60_000,
      retry: false,
    })),
  });
}

function Terminal({ corpus, pack }: { corpus: Corpus; pack: PackManifest }) {
  const start = BigInt(pack.capture_start_ns);
  const end = offsetSeconds(pack.capture_end_ns, start);
  const full: [number, number] = useMemo(() => [0, end], [end]);
  const okLenses = Object.entries(pack.lenses).filter(
    ([, l]) => l.state === 'ok' && l.overlay,
  );
  const [params] = useSearchParams();
  const [lens, setLens] = useState<string>(() => {
    const asked = params.get('lens');
    if (asked !== null && (asked === '' || okLenses.some(([n]) => n === asked)))
      return asked;
    return (okLenses.find(([n]) => n === 'cross') ?? okLenses[0])?.[0] ?? '';
  });
  const [view, setViewState] = useState<[number, number]>(full);
  // ?t=<seconds since capture start> sets the cursor, so a moment can be linked.
  const [cursor, setCursor] = useState<number | null>(() => {
    const t = Number(params.get('t'));
    return params.get('t') !== null && Number.isFinite(t)
      ? Math.max(0, Math.min(end, t))
      : null;
  });
  const [hover, setHover] = useState<number | null>(null);
  const [hidden, setHidden] = useState<Set<number>>(() => new Set());
  const [midOnly, setMidOnly] = useState(false);
  const [fullHistory, setFullHistory] = useState(true);
  const [selected, setSelected] = useState<IntervalItem | null>(null);
  const [layout, setLayout] = useState<PlotLayout>({ left: 52, width: 800 });
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(60);
  const [autoPause, setAutoPause] = useState(true);

  const overview = usePackFile<GridDocument>(corpus, pack, 'overview.json');
  const availability = usePackFile<AvailabilityDocument>(
    corpus,
    pack,
    'availability.json',
  );
  const game = usePackFile<GameDocument>(corpus, pack, 'game.json');
  const overlay = usePackFile<OverlayDocument>(
    corpus,
    pack,
    lens ? (pack.lenses[lens].overlay ?? null) : null,
  );

  const setView = useCallback(
    ([a, b]: [number, number]) => {
      const span = Math.max(0.002, Math.min(end, b - a));
      let lo = Math.max(0, Math.min(a, end - span));
      lo = Number.isFinite(lo) ? lo : 0;
      setViewState([lo, lo + span]);
    },
    [end],
  );

  const span = view[1] - view[0];
  const tier = chooseTier(span);
  const tileKind = tier === 'overview' ? null : tier;
  const tileNames = tileKind
    ? tilesInRange(view[0], view[1], TILE_MINUTES, pack.tiles[tileKind])
    : [];
  const tiles = useTiles(corpus, pack, tileKind, tileNames);
  const tilesReady = tiles.every((t) => t.data) && tiles.length > 0;
  const tileError = tiles.find((t) => t.error)?.error as Error | undefined;

  const overviewChart = useMemo(
    () =>
      overview.data ? gridsToChart([overview.data], pack.books, start) : null,
    [overview.data, pack.books, start],
  );
  const tileData = tiles.map((t) => t.data);
  const tileKey = tileNames.join(',') + tileKind + tilesReady;
  const detailChart = useMemo<ChartData | null>(() => {
    if (!tilesReady || !tileKind) return null;
    const docs = tileData as (GridDocument | RawTile)[];
    return tileKind === 'raw'
      ? rawToChart(docs as RawTile[], pack.books, pack.scopes, start)
      : gridsToChart(docs as GridDocument[], pack.books, start);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tileKey]);
  const baseChart = detailChart ?? overviewChart;
  const chart = useMemo(
    () =>
      baseChart && !fullHistory && cursor !== null
        ? maskAfter(baseChart, cursor)
        : baseChart,
    [baseChart, fullHistory, cursor],
  );
  const yRange = useMemo<[number, number]>(
    () => (baseChart ? fixedYRange(baseChart, view) : [0, 100]),
    [baseChart, view],
  );
  const styles = useMemo(() => bookStyles(pack.books), [pack.books]);
  const highlight = useMemo(
    () => (selected ? new Set(selected.books) : null),
    [selected],
  );
  const items = useMemo(() => overlay.data?.items ?? [], [overlay.data]);
  const unusableEverywhere = useMemo(() => {
    const set = new Set<number>();
    overview.data?.books.forEach(
      (b) => b.usable_ppm.every((v) => v === 0) && set.add(b.book),
    );
    return set;
  }, [overview.data]);

  // Playback: the cursor advances in event time; auto-pause stops at the next moment.
  const frame = useRef<number | null>(null);
  useEffect(() => {
    if (!playing) return;
    let last = performance.now();
    let stopAt =
      autoPause && cursor !== null
        ? nextMoment(
            cursor,
            start,
            game.data?.events ?? [],
            items,
            availability.data ?? null,
          )
        : null;
    const tick = (now: number) => {
      const dt = ((now - last) / 1000) * speed;
      last = now;
      let reached = false;
      setCursor((c) => {
        let next = (c ?? view[0]) + dt;
        if (stopAt !== null && next >= stopAt) {
          next = stopAt;
          reached = true;
        }
        if (next >= end) {
          next = end;
          reached = true;
        }
        return next;
      });
      if (reached) {
        setPlaying(false);
        stopAt = null;
        return;
      }
      frame.current = requestAnimationFrame(tick);
    };
    frame.current = requestAnimationFrame(tick);
    return () => {
      if (frame.current !== null) cancelAnimationFrame(frame.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [playing, speed, autoPause]);

  // Keep the cursor visible while playing.
  useEffect(() => {
    if (playing && cursor !== null && (cursor > view[1] || cursor < view[0])) {
      setView([cursor - span * 0.1, cursor + span * 0.9]);
    }
  }, [playing, cursor, view, span, setView]);

  const selectEpisode = (item: IntervalItem) => {
    setSelected(item);
    const s = offsetSeconds(item.start_ns, start);
    const e = offsetSeconds(item.end_ns, start);
    const pad = Math.max(2, (e - s) * 2);
    setView([s - pad, e + pad]);
    setCursor(s);
  };

  const jumpNext = () => {
    const next = nextMoment(
      cursor ?? view[0],
      start,
      game.data?.events ?? [],
      items,
      availability.data ?? null,
    );
    if (next !== null) {
      setCursor(next);
      if (next > view[1] || next < view[0])
        setView([next - span / 2, next + span / 2]);
    }
  };

  const selectedIndex = selected
    ? items.findIndex((item) => item.id === selected.id)
    : -1;
  const stepEpisode = (index: number) => {
    if (items.length)
      selectEpisode(items[Math.max(0, Math.min(items.length - 1, index))]);
  };

  const readoutAt = hover ?? cursor;
  const readout =
    chart && readoutAt !== null ? valuesAt(chart, readoutAt) : null;
  const toPx = (t: number) => ((t - view[0]) / span) * layout.width;
  const lensState = lens ? pack.lenses[lens] : null;
  const groups = legendGroups(pack.books);
  const firstPaintError =
    overview.error ?? availability.error ?? game.error ?? overlay.error;

  return (
    <>
      <div className="page-heading research-heading">
        <span className="eyebrow">
          EVENT RESEARCH · {pack.game ?? 'game unknown'}
        </span>
        <h1 className={pack.title === pack.bundle_id ? 'untitled' : undefined}>
          {pack.title}
        </h1>
        <p>
          {pack.books.length} books ·{' '}
          {new Set(pack.books.map((b) => b.venue)).size} venues ·{' '}
          {formatOffset(end).slice(1)} recorded from {formatUtc(start, 0)}
        </p>
        {pack.context !== 'ok' && (
          <div className="notice" role="note">
            This result was committed without its prepared context. Books are
            labelled by instrument id, the title is the bundle id, and
            cross-venue twins are unknown.
          </div>
        )}
      </div>

      <div className="research-toolbar">
        <label>
          Research lens{' '}
          <select
            value={lens}
            onChange={(e) => {
              setLens(e.target.value);
              setSelected(null);
            }}
          >
            <option value="">No lens</option>
            {Object.entries(pack.lenses)
              .filter(([name]) => name !== 'profile')
              .map(([name, state]) => (
                <option key={name} value={name} disabled={state.state !== 'ok'}>
                  {state.label}
                  {state.state === 'ok'
                    ? ` (${state.episodes ?? 0})`
                    : ` — ${state.state.replace('_', ' ')}${state.error || state.reason ? `: ${(state.error ?? state.reason ?? '').slice(0, 80)}` : ''}`}
                </option>
              ))}
          </select>
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={midOnly}
            onChange={(e) => setMidOnly(e.target.checked)}
          />{' '}
          Mid only
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={fullHistory}
            onChange={(e) => setFullHistory(e.target.checked)}
          />{' '}
          Full history
        </label>
        <span className="tier" title="Data source for the visible span">
          {tier === 'overview'
            ? 'Overview'
            : tier === '1s'
              ? '1 s tiles'
              : 'Raw rows'}
          {tileKind && !tilesReady && !tileError ? ' · loading…' : ''}
        </span>
        <button type="button" onClick={() => setView(full)}>
          Reset zoom
        </button>
      </div>

      {firstPaintError || tileError ? (
        <div className="error-state" role="alert">
          {((firstPaintError ?? tileError) as Error).message}
        </div>
      ) : null}

      <section className="research-plot" aria-label="Price history">
        {chart ? (
          <ResearchChart
            data={chart}
            view={view}
            yRange={yRange}
            styles={styles}
            hidden={hidden}
            highlight={highlight}
            midOnly={midOnly}
            cursor={cursor}
            onView={setView}
            onCursor={setCursor}
            onReset={() => setView(full)}
            onLayout={setLayout}
            onHover={setHover}
            tier={tileKind && tilesReady ? tier : 'overview'}
          />
        ) : (
          <div className="empty-state">Loading the overview…</div>
        )}
        <Tracks
          left={layout.left}
          width={layout.width}
          toPx={toPx}
          view={view}
          start={start}
          game={game.data}
          lensLabel={lensState?.label ?? null}
          items={items}
          selected={selected}
          onSelect={selectEpisode}
          onZoom={setView}
          availability={availability.data}
          books={pack.books}
          hidden={hidden}
          cursor={cursor}
        />
        <div className="axis-caption">
          Time since capture start ·{' '}
          {hover !== null
            ? `${formatOffset(hover)} · ${formatUtc(start, hover)}`
            : 'hover for UTC'}
        </div>
      </section>

      <div className="research-playback">
        <button
          type="button"
          onClick={() => {
            if (cursor === null) setCursor(view[0]);
            setPlaying((p) => !p);
          }}
        >
          {playing ? 'Pause' : 'Play'}
        </button>
        <input
          type="range"
          min={0}
          max={end}
          step={Math.max(0.001, end / 5000)}
          value={cursor ?? 0}
          data-cursor={cursor ?? ''}
          aria-label="Cursor"
          onChange={(e) => setCursor(Number(e.target.value))}
        />
        <span className="mono">
          {cursor === null ? '—' : formatOffset(cursor)}
        </span>
        <select
          value={speed}
          onChange={(e) => setSpeed(Number(e.target.value))}
          aria-label="Playback speed"
        >
          {SPEEDS.map((s) => (
            <option key={s} value={s}>
              {s}×
            </option>
          ))}
        </select>
        <button type="button" onClick={jumpNext}>
          Next moment
        </button>
        <label className="check">
          <input
            type="checkbox"
            checked={autoPause}
            onChange={(e) => setAutoPause(e.target.checked)}
          />{' '}
          Auto-pause
        </label>
      </div>

      <div className="research-legend">
        {groups.map((group) => (
          <div
            key={group.key}
            className={`legend-group${group.books.length > 1 ? ' twins' : ''}`}
          >
            {group.books.length > 1 && (
              <div className="legend-claim">{group.label} · twins</div>
            )}
            {group.books.map((book) => (
              <label key={book.book} className="legend-entry">
                <input
                  type="checkbox"
                  checked={!hidden.has(book.book)}
                  onChange={() =>
                    setHidden((h) => {
                      const next = new Set(h);
                      if (next.has(book.book)) next.delete(book.book);
                      else next.add(book.book);
                      return next;
                    })
                  }
                />
                <Swatch style={styles[book.book]} />
                <span>
                  <span className="legend-label" title={book.instrument}>
                    {group.books.length > 1
                      ? (book.market_id ?? displayLabel(book))
                      : displayLabel(book)}
                  </span>
                  <span className="legend-venue">
                    {book.venue}
                    {unusableEverywhere.has(book.book) && (
                      <StateBadge state="no_usable_time" />
                    )}
                  </span>
                </span>
              </label>
            ))}
          </div>
        ))}
        <div className="legend-actions">
          <button type="button" onClick={() => setHidden(new Set())}>
            Show all
          </button>
          <button
            type="button"
            onClick={() => setHidden(new Set(pack.books.map((b) => b.book)))}
          >
            Hide all
          </button>
        </div>
      </div>

      <div className="research-details">
        <section>
          <h3>At the cursor</h3>
          <p className="mono">
            {readoutAt === null
              ? 'Click the chart or scrub to set the cursor.'
              : `${formatOffset(readoutAt)} · ${formatUtc(start, readoutAt)}`}
          </p>
          <p>
            Game state:{' '}
            {game.data
              ? gameSummaryAt(
                  game.data,
                  start + BigInt(Math.round((readoutAt ?? 0) * 1e9)),
                )
              : '…'}
          </p>
          {readout && (
            <table className="readout">
              <thead>
                <tr>
                  <th>Book</th>
                  <th>Venue</th>
                  <th>Bid</th>
                  <th>Ask</th>
                  <th>Mid</th>
                </tr>
              </thead>
              <tbody>
                {pack.books
                  .filter((b) => !hidden.has(b.book))
                  .map((b) => (
                    <tr
                      key={b.book}
                      className={highlight?.has(b.book) ? 'highlight' : ''}
                      data-book={b.book}
                      data-bid={readout[b.book].bid ?? ''}
                      data-ask={readout[b.book].ask ?? ''}
                    >
                      <td title={b.instrument}>
                        <Swatch style={styles[b.book]} /> {displayLabel(b)}
                      </td>
                      <td>{b.venue}</td>
                      <td>{cents(readout[b.book].bid)}</td>
                      <td>{cents(readout[b.book].ask)}</td>
                      <td>{cents(readout[b.book].mid)}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          )}
        </section>
        <section>
          <h3>
            {selected
              ? 'Selected episode'
              : lensState
                ? lensState.label
                : 'No lens'}
          </h3>
          {lensState && items.length > 0 && (
            <div className="episode-nav" role="group" aria-label="Episodes">
              <button type="button" onClick={() => stepEpisode(0)}>
                First
              </button>
              <button
                type="button"
                disabled={selectedIndex <= 0}
                onClick={() => stepEpisode(selectedIndex - 1)}
              >
                Previous
              </button>
              <span className="mono small">
                {selectedIndex >= 0 ? selectedIndex + 1 : '—'} / {items.length}
              </span>
              <button
                type="button"
                disabled={selectedIndex >= items.length - 1}
                onClick={() => stepEpisode(selectedIndex + 1)}
              >
                Next
              </button>
              <button
                type="button"
                onClick={() => stepEpisode(items.length - 1)}
              >
                Last
              </button>
            </div>
          )}
          {selected ? (
            <EpisodePanel
              item={selected}
              lens={lens}
              pack={pack}
              corpus={corpus}
              start={start}
            />
          ) : lensState ? (
            <p className="muted">
              {items.length} episodes, in time order. Click one in the lens
              track or step through them. Net values are modeled apparent edge
              at open, not returns.
            </p>
          ) : (
            <p className="muted">Choose a lens to overlay its episodes.</p>
          )}
          <LensStates pack={pack} />
        </section>
      </div>
    </>
  );
}

function LensStates({ pack }: { pack: PackManifest }) {
  return (
    <dl className="lens-states">
      {Object.entries(pack.lenses).map(([name, state]) => (
        <div key={name}>
          <dt>{state.label}</dt>
          <dd>
            <StateBadge
              state={state.state}
              title={state.error ?? state.reason}
            />
            {state.state === 'ok' &&
              state.episodes !== undefined &&
              ` ${state.episodes} episode${state.episodes === 1 ? '' : 's'}`}
            {(state.error || state.reason) && (
              <span className="reason">{state.error ?? state.reason}</span>
            )}
          </dd>
        </div>
      ))}
      {pack.unmapped_groups.length > 0 && (
        <div>
          <dt>Not a lens</dt>
          <dd className="muted">{pack.unmapped_groups.join(', ')}</dd>
        </div>
      )}
    </dl>
  );
}

function EpisodePanel(props: {
  item: IntervalItem;
  lens: string;
  pack: PackManifest;
  corpus: Corpus;
  start: bigint;
}) {
  const { item, pack, start } = props;
  const s = offsetSeconds(item.start_ns, start);
  const detail = item.detail as Record<string, unknown>;
  return (
    <dl className="episode">
      <div>
        <dt>Lens</dt>
        <dd>{pack.lenses[props.lens]?.label}</dd>
      </div>
      <div>
        <dt>Kind</dt>
        <dd>{item.label}</dd>
      </div>
      <div>
        <dt>Start</dt>
        <dd className="mono">
          {formatOffset(s)} · {formatUtc(start, s)}
        </dd>
      </div>
      <div>
        <dt>End reason</dt>
        <dd>
          {item.end_reason}
          {item.censored ? ' (censored)' : ''}
        </dd>
      </div>
      {item.metrics.map((m) => (
        <div key={m.name}>
          <dt>{m.name === 'net' ? 'Net (modeled)' : m.name}</dt>
          <dd className="mono">
            {m.value} {m.unit}
          </dd>
        </div>
      ))}
      <div>
        <dt>Books</dt>
        <dd>
          {item.books
            .map((b) => (pack.books[b] ? displayLabel(pack.books[b]) : b))
            .join(' + ')}
        </dd>
      </div>
      {Object.entries(detail)
        .filter(([k]) => k !== 'legs' && k !== 'end_books' && k !== 'kind')
        .map(([k, v]) => (
          <div key={k}>
            <dt>{k}</dt>
            <dd className="mono">
              {typeof v === 'string' ? v : JSON.stringify(v)}
            </dd>
          </div>
        ))}
      <div>
        <dt>Provenance</dt>
        <dd className="mono small">
          corpus {String(pack.provenance.corpus_key ?? '').slice(0, 16)} ·
          semantic{' '}
          {String(pack.lenses[props.lens]?.semantic_sha256 ?? '').slice(0, 12)}{' '}
          · snapshot{' '}
          {String(pack.provenance.snapshot_sha256 ?? '').slice(0, 12)} · version{' '}
          {props.corpus.pointer.version}
        </dd>
      </div>
      {metric(item, 'net') === undefined && (
        <p className="muted">No fill: this episode carries duration only.</p>
      )}
    </dl>
  );
}

interface TrackProps {
  left: number;
  width: number;
  toPx: (t: number) => number;
  view: [number, number];
  start: bigint;
  game: GameDocument | undefined;
  lensLabel: string | null;
  items: IntervalItem[];
  selected: IntervalItem | null;
  onSelect: (item: IntervalItem) => void;
  onZoom: (view: [number, number]) => void;
  availability: AvailabilityDocument | undefined;
  books: PackBook[];
  hidden: Set<number>;
  cursor: number | null;
}

const ROW = 22;

function Tracks(props: TrackProps) {
  const { left, width, toPx, start, view } = props;
  const marks = props.game ? gameMarks(props.game.events, start) : [];
  const lens = lensMarks(props.items, start, view, width);
  const known =
    props.game && props.cursor !== null
      ? new Set(
          knownAt(
            props.game.events,
            start + BigInt(Math.round(props.cursor * 1e9)),
          ),
        )
      : null;
  const gameState = props.game?.state;
  const unusable = (props.availability?.books ?? [])
    .filter((b) => !props.hidden.has(b.book))
    .flatMap((b) =>
      b.intervals
        .filter((i) => i.state !== 'usable')
        .map((i) => ({ ...i, book: b.book })),
    );
  const clip = (x: number) => Math.max(0, Math.min(width, x));
  return (
    <div className="research-tracks" style={{ paddingLeft: left }}>
      <div className="track">
        <span className="track-label" style={{ left: -left, width: left }}>
          Game
        </span>
        <svg width={width} height={ROW} role="img" aria-label="Game events">
          <defs>
            <pattern
              id="hatch"
              width="4"
              height="4"
              patternUnits="userSpaceOnUse"
              patternTransform="rotate(45)"
            >
              <line
                x1="0"
                y1="0"
                x2="0"
                y2="4"
                stroke="#9595a3"
                strokeWidth="1"
              />
            </pattern>
          </defs>
          {gameState !== 'ok' && (
            <text x={6} y={15} className="track-state">
              {gameState === 'incomplete'
                ? 'Game state incomplete'
                : 'No game-state source'}
              {props.game?.reason ? ` (${props.game.reason})` : ''}
            </text>
          )}
          {marks.map((mark, i) => {
            const x0 = toPx(mark.x0);
            const x1 = toPx(mark.x1);
            if (x1 < 0 || x0 > width) return null;
            const title = `${mark.event.label}\n${mark.event.time_basis} · source ${formatOffset(offsetSeconds(mark.event.source_ns, start))}\nwindow ${formatOffset(mark.x0)} – ${formatOffset(mark.x1)}${mark.event.detail && Object.keys(mark.event.detail).length ? `\n${JSON.stringify(mark.event.detail)}` : ''}`;
            const faded =
              known !== null &&
              mark.shape !== 'scheduled' &&
              !known.has(mark.event);
            const opacity = faded ? 0.4 : 1;
            if (mark.shape === 'scheduled') {
              return (
                <line
                  key={i}
                  x1={x0}
                  x2={x0}
                  y1={2}
                  y2={ROW - 2}
                  stroke="#a9a7df"
                  strokeDasharray="3 3"
                >
                  <title>{title}</title>
                </line>
              );
            }
            if (mark.shape === 'diamond') {
              return (
                <path
                  key={i}
                  d={`M${x0} 4 L${x0 + 6} 11 L${x0} 18 L${x0 - 6} 11 Z`}
                  fill="#e7e7ee"
                  opacity={opacity}
                >
                  <title>{title}</title>
                </path>
              );
            }
            const w = Math.max(2, x1 - x0);
            return (
              <g key={i} opacity={opacity}>
                <path
                  d={`M${x0} 5 L${x0} 17 M${x0} 11 L${x0 + w} 11 M${x0 + w} 5 L${x0 + w} 17`}
                  stroke="#e7e7ee"
                  strokeWidth={1.5}
                  fill="none"
                />
                <rect
                  x={x0 - 4}
                  y={0}
                  width={w + 8}
                  height={ROW}
                  fill="transparent"
                >
                  <title>{title}</title>
                </rect>
              </g>
            );
          })}
        </svg>
      </div>
      <div className="track">
        <span className="track-label" style={{ left: -left, width: left }}>
          Lens
        </span>
        <svg width={width} height={ROW} role="img" aria-label="Lens episodes">
          {props.lensLabel === null && (
            <text x={6} y={15} className="track-state">
              No lens selected
            </text>
          )}
          {lens.map((mark, i) =>
            mark.kind === 'item' ? (
              <rect
                key={mark.item.id}
                x={clip(mark.x0)}
                y={4}
                width={Math.max(2, clip(mark.x1) - clip(mark.x0))}
                height={14}
                rx={2}
                className={`episode-mark${props.selected?.id === mark.item.id ? ' selected' : ''}${mark.item.detail.kind === 'gross' ? ' gross' : ''}`}
                onClick={() => props.onSelect(mark.item)}
              >
                <title>{`${mark.item.label} · ${mark.item.metrics.map((m) => `${m.name} ${m.value} ${m.unit}`).join(' · ')}`}</title>
              </rect>
            ) : (
              <g key={`d${i}`}>
                <rect
                  x={mark.x0}
                  y={6}
                  width={mark.x1 - mark.x0}
                  height={10}
                  className="density"
                  opacity={Math.min(1, 0.3 + mark.count / 10)}
                  onClick={() =>
                    props.onZoom([
                      view[0] + (mark.x0 / width) * (view[1] - view[0]) - 1,
                      view[0] + (mark.x1 / width) * (view[1] - view[0]) + 1,
                    ])
                  }
                >
                  <title>{`${mark.count} episodes too short to draw at this zoom; click to zoom in`}</title>
                </rect>
              </g>
            ),
          )}
        </svg>
      </div>
      <div className="track">
        <span className="track-label" style={{ left: -left, width: left }}>
          Books
        </span>
        <svg
          width={width}
          height={ROW}
          role="img"
          aria-label="Unusable book intervals"
        >
          {props.availability?.state === 'missing' && (
            <text x={6} y={15} className="track-state">
              Availability not recorded by this profile run
            </text>
          )}
          {unusable.map((interval, i) => {
            const x0 = clip(toPx(offsetSeconds(interval.start_ns, start)));
            const x1 = clip(toPx(offsetSeconds(interval.end_ns, start)));
            if (x1 <= x0) return null;
            return (
              <rect
                key={i}
                x={x0}
                y={3}
                width={Math.max(1, x1 - x0)}
                height={16}
                fill="url(#hatch)"
              >
                <title>{`${props.books[interval.book]?.label} (${props.books[interval.book]?.venue}): ${interval.state}${interval.reason ? ` · ${interval.reason}` : ''}`}</title>
              </rect>
            );
          })}
        </svg>
      </div>
    </div>
  );
}
