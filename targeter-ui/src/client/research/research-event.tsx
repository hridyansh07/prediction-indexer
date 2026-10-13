import {
  memo,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from 'react';
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
  gameMarkTitle,
  gameSummaryAt,
  gridsToChart,
  knownAt,
  legendGroups,
  lensMarks,
  maskAfter,
  mergeEpisodes,
  metric,
  nextMoment,
  offsetSeconds,
  parseLayers,
  rawToChart,
  tilesInRange,
  valuesAt,
  withLayers,
  type AvailabilityDocument,
  type ChartData,
  type GameDocument,
  type GridDocument,
  type IntervalItem,
  type LayerEpisode,
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
// One fixed colour per lens layer, from the same reference ramps; layers are drawn on
// their own labelled rows, never on the price lines.
const LENS_COLORS: Record<string, string> = {
  complement: '#c98500',
  cross: '#199e70',
  multi: '#3987e5',
  cover_same: '#d55181',
  cover_cross: '#9085e9',
};
const LENS_SHORT: Record<string, string> = {
  complement: 'Compl.',
  cross: 'Cross',
  multi: 'Multi',
  cover_same: 'Cov. S',
  cover_cross: 'Cov. X',
};
const lensColor = (lens: string) => LENS_COLORS[lens] ?? '#9595a3';
const lensShort = (lens: string) => LENS_SHORT[lens] ?? lens.slice(0, 6);
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

function useOverlays(corpus: Corpus, pack: PackManifest, layers: string[]) {
  return useQueries({
    queries: layers.map((lens) => {
      const name = pack.lenses[lens].overlay ?? '';
      return {
        queryKey: researchKeys.file(
          corpus.pointer.version,
          pack.bundle_id,
          name,
        ),
        queryFn: () => packFile<OverlayDocument>(corpus, pack, name),
        staleTime: Number.POSITIVE_INFINITY,
        gcTime: 10 * 60_000,
        retry: false,
      };
    }),
  });
}

function Terminal({ corpus, pack }: { corpus: Corpus; pack: PackManifest }) {
  const start = BigInt(pack.capture_start_ns);
  const end = offsetSeconds(pack.capture_end_ns, start);
  const full: [number, number] = useMemo(() => [0, end], [end]);
  const available = useMemo(
    () =>
      Object.entries(pack.lenses)
        .filter(([, l]) => l.state === 'ok' && l.overlay)
        .map(([name]) => name),
    [pack.lenses],
  );
  // The URL holds the active layers (?lenses=a,b), so a view can be shared.
  const [params, setParams] = useSearchParams();
  const layersKey = parseLayers(params, available).join(',');
  const layers = useMemo(
    () => (layersKey ? layersKey.split(',') : []),
    [layersKey],
  );
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
  const [selected, setSelected] = useState<LayerEpisode | null>(null);
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
  const overlays = useOverlays(corpus, pack, layers);
  const overlayError = overlays.find((o) => o.error)?.error as
    | Error
    | undefined;
  const overlayStamp = overlays.map((o) => o.dataUpdatedAt).join(',');
  const episodes = useMemo(
    () =>
      mergeEpisodes(
        layers.map((lens, i) => ({
          lens,
          items: overlays[i]?.data?.items ?? [],
        })),
      ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [layersKey, overlayStamp],
  );
  const items = useMemo(() => episodes.map((e) => e.item), [episodes]);
  const toggleLayer = (lens: string) => {
    const on = layers.includes(lens);
    const next = available.filter((l) =>
      l === lens ? !on : layers.includes(l),
    );
    if (on && selected?.lens === lens) setSelected(null);
    setParams(withLayers(params, next), { replace: true });
  };

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
    () => (selected ? new Set(selected.item.books) : null),
    [selected],
  );
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

  const selectEpisode = useCallback(
    (episode: LayerEpisode) => {
      setSelected(episode);
      const s = offsetSeconds(episode.item.start_ns, start);
      const e = offsetSeconds(episode.item.end_ns, start);
      const pad = Math.max(2, (e - s) * 2);
      setView([s - pad, e + pad]);
      setCursor(s);
    },
    [start, setView],
  );

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
    ? episodes.findIndex((e) => e.key === selected.key)
    : -1;
  const stepEpisode = (index: number) => {
    if (episodes.length)
      selectEpisode(
        episodes[Math.max(0, Math.min(episodes.length - 1, index))],
      );
  };

  const readoutAt = hover ?? cursor;
  const readout =
    chart && readoutAt !== null ? valuesAt(chart, readoutAt) : null;
  const toPx = (t: number) => ((t - view[0]) / span) * layout.width;
  const groups = legendGroups(pack.books);
  const firstPaintError =
    overview.error ?? availability.error ?? game.error ?? overlayError;
  const trackLayers = layers.map((lens, i) => ({
    lens,
    label: pack.lenses[lens].label,
    items: overlays[i]?.data?.items ?? [],
  }));

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

      <div
        className="research-layers"
        role="group"
        aria-label="Research lens layers"
      >
        <span className="layers-title">Lens layers</span>
        {Object.entries(pack.lenses)
          .filter(([name]) => name !== 'profile')
          .map(([name, state]) => {
            const usable = available.includes(name);
            const on = layers.includes(name);
            const why = state.error ?? state.reason ?? null;
            return (
              <span key={name} className="lens-chip-wrap" data-lens={name}>
                <button
                  type="button"
                  className={`lens-chip${on ? ' on' : ''}`}
                  aria-pressed={on}
                  disabled={!usable}
                  title={state.label}
                  style={{ '--lens': lensColor(name) } as CSSProperties}
                  onClick={() => toggleLayer(name)}
                >
                  <span className="lens-dot" aria-hidden="true" />
                  {state.label}
                  {usable && (
                    <span className="lens-count">{state.episodes ?? 0}</span>
                  )}
                </button>
                {state.state !== 'ok' && (
                  <>
                    <StateBadge state={state.state} title={why} />
                    {why && (
                      <span className="lens-why" title={why}>
                        {why.length > 60 ? `${why.slice(0, 60)}…` : why}
                      </span>
                    )}
                  </>
                )}
              </span>
            );
          })}
      </div>

      <div className="research-toolbar">
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
          layers={trackLayers}
          selectedKey={selected?.key ?? null}
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
                    {book.market_status && (
                      <span
                        className="market-status"
                        data-status={book.market_status}
                        title="Market status in the committed context snapshot"
                      >
                        {book.market_status}
                      </span>
                    )}
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
            Episodes{' '}
            <span className="muted small">
              {layers.length === 0
                ? '· no layer on'
                : `· ${episodes.length} across ${layers.map((l) => lensShort(l)).join(', ')}`}
            </span>
          </h3>
          {episodes.length > 0 && (
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
                {selectedIndex >= 0 ? selectedIndex + 1 : '—'} /{' '}
                {episodes.length}
              </span>
              <button
                type="button"
                disabled={selectedIndex >= episodes.length - 1}
                onClick={() => stepEpisode(selectedIndex + 1)}
              >
                Next
              </button>
              <button
                type="button"
                onClick={() => stepEpisode(episodes.length - 1)}
              >
                Last
              </button>
            </div>
          )}
          {layers.length === 0 ? (
            <p className="muted">
              Switch on a lens layer to overlay its episodes.
            </p>
          ) : (
            <EpisodeList
              episodes={episodes}
              selectedKey={selected?.key ?? null}
              start={start}
              onSelect={selectEpisode}
            />
          )}
          {selected ? (
            <EpisodePanel
              episode={selected}
              pack={pack}
              corpus={corpus}
              start={start}
            />
          ) : (
            layers.length > 0 && (
              <p className="muted">
                Click an episode in a lens row or the list, or step through
                them. Net values are modeled apparent edge at open, not returns.
              </p>
            )
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

const EpisodeList = memo(function EpisodeList(props: {
  episodes: LayerEpisode[];
  selectedKey: string | null;
  start: bigint;
  onSelect: (episode: LayerEpisode) => void;
}) {
  const list = useRef<HTMLOListElement>(null);
  // Keep the selected row in view inside the list, without scrolling the page.
  useEffect(() => {
    const box = list.current;
    const row = box?.querySelector<HTMLElement>('[aria-current="true"]');
    if (!box || !row) return;
    if (row.offsetTop < box.scrollTop) box.scrollTop = row.offsetTop;
    else if (
      row.offsetTop + row.offsetHeight >
      box.scrollTop + box.clientHeight
    )
      box.scrollTop = row.offsetTop + row.offsetHeight - box.clientHeight;
  }, [props.selectedKey]);
  return (
    <ol className="episode-list" ref={list} aria-label="Episodes in time order">
      {props.episodes.map((episode) => {
        const s = offsetSeconds(episode.item.start_ns, props.start);
        const net = metric(episode.item, 'net');
        const current = episode.key === props.selectedKey;
        return (
          <li key={episode.key}>
            <button
              type="button"
              aria-current={current ? 'true' : undefined}
              data-lens={episode.lens}
              data-episode={episode.item.id}
              onClick={() => props.onSelect(episode)}
            >
              <span
                className="lens-tag"
                style={{ '--lens': lensColor(episode.lens) } as CSSProperties}
              >
                {lensShort(episode.lens)}
              </span>
              <span className="mono">{formatOffset(s)}</span>
              <span className="episode-kind">{episode.item.label}</span>
              <span className="mono episode-net">
                {net ? `${net.value} ${net.unit}` : '—'}
              </span>
            </button>
          </li>
        );
      })}
    </ol>
  );
});

function EpisodePanel(props: {
  episode: LayerEpisode;
  pack: PackManifest;
  corpus: Corpus;
  start: bigint;
}) {
  const { pack, start } = props;
  const { item, lens } = props.episode;
  const s = offsetSeconds(item.start_ns, start);
  const detail = item.detail as Record<string, unknown>;
  return (
    <dl className="episode">
      <div>
        <dt>Lens</dt>
        <dd>
          <span
            className="lens-tag"
            style={{ '--lens': lensColor(lens) } as CSSProperties}
          >
            {lensShort(lens)}
          </span>{' '}
          {pack.lenses[lens]?.label}
        </dd>
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
            .map((b) => {
              const book = pack.books[b];
              if (!book) return String(b);
              const status = book.market_status
                ? ` (${book.market_status})`
                : '';
              return `${displayLabel(book)} · ${book.venue}${status}`;
            })
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
          {String(pack.lenses[lens]?.semantic_sha256 ?? '').slice(0, 12)} ·
          snapshot {String(pack.provenance.snapshot_sha256 ?? '').slice(0, 12)}{' '}
          · version {props.corpus.pointer.version}
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
  layers: { lens: string; label: string; items: IntervalItem[] }[];
  selectedKey: string | null;
  onSelect: (episode: LayerEpisode) => void;
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
            const title = gameMarkTitle(mark, start);
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
      {props.layers.length === 0 && (
        <div className="track">
          <span className="track-label" style={{ left: -left, width: left }}>
            Lens
          </span>
          <svg width={width} height={ROW} role="img" aria-label="Lens layers">
            <text x={6} y={15} className="track-state">
              No lens layer on
            </text>
          </svg>
        </div>
      )}
      {props.layers.map((layer) => (
        <LensTrack
          key={layer.lens}
          {...layer}
          left={left}
          width={width}
          view={view}
          start={start}
          selectedKey={props.selectedKey}
          onSelect={props.onSelect}
          onZoom={props.onZoom}
        />
      ))}
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

function LensTrack(props: {
  lens: string;
  label: string;
  items: IntervalItem[];
  left: number;
  width: number;
  view: [number, number];
  start: bigint;
  selectedKey: string | null;
  onSelect: (episode: LayerEpisode) => void;
  onZoom: (view: [number, number]) => void;
}) {
  const { lens, left, width, view } = props;
  const marks = lensMarks(props.items, props.start, view, width);
  const clip = (x: number) => Math.max(0, Math.min(width, x));
  const color = lensColor(lens);
  return (
    <div className="track lens-track" data-lens={lens}>
      <span
        className="track-label"
        style={{ left: -left, width: left }}
        title={props.label}
      >
        {lensShort(lens)}
      </span>
      <svg
        width={width}
        height={ROW}
        role="img"
        aria-label={`${props.label} episodes`}
      >
        {marks.map((mark, i) => {
          if (mark.kind === 'density') {
            return (
              <rect
                key={`d${i}`}
                x={mark.x0}
                y={6}
                width={mark.x1 - mark.x0}
                height={10}
                className="density"
                fill={color}
                opacity={Math.min(1, 0.3 + mark.count / 10)}
                onClick={() =>
                  props.onZoom([
                    view[0] + (mark.x0 / width) * (view[1] - view[0]) - 1,
                    view[0] + (mark.x1 / width) * (view[1] - view[0]) + 1,
                  ])
                }
              >
                <title>{`${mark.count} ${props.label} episodes too short to draw at this zoom; click to zoom in`}</title>
              </rect>
            );
          }
          const key = `${lens}:${mark.item.id}`;
          const selected = key === props.selectedKey;
          return (
            <rect
              key={key}
              x={clip(mark.x0)}
              y={4}
              width={Math.max(2, clip(mark.x1) - clip(mark.x0))}
              height={14}
              rx={2}
              className={`episode-mark${selected ? ' selected' : ''}`}
              fill={selected ? undefined : color}
              opacity={mark.item.detail.kind === 'gross' && !selected ? 0.5 : 1}
              onClick={() => props.onSelect({ key, lens, item: mark.item })}
            >
              <title>{`${props.label} · ${mark.item.label} · ${mark.item.metrics.map((m) => `${m.name} ${m.value} ${m.unit}`).join(' · ')}`}</title>
            </rect>
          );
        })}
      </svg>
    </div>
  );
}
