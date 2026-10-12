import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useNavigate } from 'react-router-dom';
import { corpusFile, useCorpus, type Corpus } from './research-data';
import {
  filterEpisodes,
  histogram,
  median,
  perEventCounts,
  type EpisodeRow,
} from './research-model';
import './research.css';

interface HistogramSpec {
  edges: number[];
  counts: number[];
}

interface LensSummary {
  label: string;
  adapter: string;
  states: Record<string, number>;
  headline_kind?: string;
  value_unit?: string;
  all?: { histograms: Record<string, HistogramSpec> };
}

interface Summary {
  min_duration_ms: number;
  events: Record<string, number>;
  packs: number;
  lenses: Record<string, LensSummary>;
}

type EventRow = Record<string, string | number | null | string[]>;

const decoder = new TextDecoder();

async function loadTables(corpus: Corpus) {
  const summary = JSON.parse(
    decoder.decode(await corpusFile(corpus, 'summary.json')),
  ) as Summary;
  const { openTables } = await import('./research-duckdb');
  const tables = await openTables(corpus);
  const events = await tables.query<EventRow>(
    'SELECT * FROM events ORDER BY capture_start_ns NULLS LAST, bundle_id',
  );
  return { summary, tables, events };
}

export function ResearchCorpusPage() {
  const corpus = useCorpus();
  const loaded = useQuery({
    queryKey: ['research', corpus.data?.pointer.version ?? '', 'tables'],
    queryFn: () => loadTables(corpus.data!),
    enabled: corpus.data !== undefined,
    staleTime: Number.POSITIVE_INFINITY,
    retry: false,
  });
  const error = corpus.error ?? loaded.error;
  return (
    <div className="research-page">
      <div className="page-heading">
        <span className="eyebrow">RESEARCH · CORPUS</span>
        <h1>Events × lenses</h1>
        <p>
          Every committed corpus result, every lens, from verified files.
          Filters change the display only; the backend records every episode.
        </p>
      </div>
      {error ? (
        <div className="error-state" role="alert">
          {(error as Error).message}
        </div>
      ) : !corpus.data || !loaded.data ? (
        <div className="empty-state">
          Loading and verifying the corpus tables…
        </div>
      ) : (
        <CorpusView corpus={corpus.data} {...loaded.data} />
      )}
    </div>
  );
}

const fmt = (value: number | null | undefined, digits = 2) =>
  value === null || value === undefined || Number.isNaN(value)
    ? '—'
    : value.toLocaleString(undefined, { maximumFractionDigits: digits });

function CorpusView(props: {
  corpus: Corpus;
  summary: Summary;
  tables: Awaited<ReturnType<typeof loadTables>>['tables'];
  events: EventRow[];
}) {
  const { corpus, summary, events } = props;
  const lenses = Object.entries(summary.lenses).filter(
    ([, l]) => l.adapter === 'sdk_episodes',
  );
  const [lens, setLens] = useState(
    lenses.find(([n]) => n === 'cross')?.[0] ?? lenses[0]?.[0] ?? '',
  );
  const [minDuration, setMinDuration] = useState(summary.min_duration_ms);
  const [minNet, setMinNet] = useState<string>('');
  const [game, setGame] = useState<string>('');
  const [venuePair, setVenuePair] = useState<string>('');
  const [dedup, setDedup] = useState(false);
  const [sort, setSort] = useState<{ key: string; desc: boolean }>({
    key: 'capture_start_ns',
    desc: false,
  });
  const navigate = useNavigate();
  const lensInfo = summary.lenses[lens];
  const kind = lensInfo?.headline_kind ?? 'net';

  const episodes = useQuery({
    queryKey: ['research', corpus.pointer.version, 'episodes', lens, kind],
    queryFn: () =>
      props.tables.query<EpisodeRow & { dedup_group: number }>(
        'SELECT bundle_id, lens, kind, duration_ms, net, quantity, game, venue_pair, dedup_group FROM episodes WHERE lens = ? AND kind = ?',
        lens,
        kind,
      ),
    staleTime: Number.POSITIVE_INFINITY,
    enabled: lens !== '',
  });

  const games = [
    ...new Set(
      events
        .map((e) => e.game)
        .filter((g): g is string => typeof g === 'string'),
    ),
  ].sort();
  const pairs = [
    ...new Set((episodes.data ?? []).map((e) => e.venue_pair)),
  ].sort();
  const counted = events
    .filter(
      (e) =>
        e.pack === 'ok' &&
        e[`${lens}_state`] === 'ok' &&
        (game === '' || e.game === game),
    )
    .map((e) => e.bundle_id as string);
  const filtered = useMemo(
    () =>
      filterEpisodes(episodes.data ?? [], {
        lens,
        kind,
        minDurationMs: minDuration,
        minNet: minNet === '' ? null : Number(minNet),
        game: game || null,
        venuePair: venuePair || null,
      }),
    [episodes.data, lens, kind, minDuration, minNet, game, venuePair],
  );
  const shown = filtered.shown as (EpisodeRow & { dedup_group: number })[];
  const unit = dedup
    ? [
        ...new Map(
          shown.map((r) => [`${r.bundle_id}:${r.dedup_group}`, r]),
        ).values(),
      ]
    : shown;
  const perEvent = perEventCounts(unit, counted);
  const edges = lensInfo?.all?.histograms ?? {};
  const nets = shown.map((r) => r.net).filter((n): n is number => n !== null);

  const sorted = [...events].sort((a, b) => {
    const x = a[sort.key];
    const y = b[sort.key];
    const order =
      x === y
        ? 0
        : x === null || x === undefined
          ? 1
          : y === null || y === undefined
            ? -1
            : x < y
              ? -1
              : 1;
    return sort.desc ? -order : order;
  });
  const header = (key: string, label: string) => (
    <button
      type="button"
      className="sort"
      onClick={() =>
        setSort((s) => ({ key, desc: s.key === key ? !s.desc : true }))
      }
    >
      {label}
      {sort.key === key ? (sort.desc ? ' ↓' : ' ↑') : ''}
    </button>
  );

  return (
    <>
      <p className="muted small">
        Version <code>{corpus.pointer.version}</code> · inputs{' '}
        {corpus.manifest.inputs
          .map((i) => `${i.key_prefix} (${i.bundles})`)
          .join(', ')}
        {corpus.manifest.superseded.length > 0 &&
          ` · ${corpus.manifest.superseded.length} superseded results`}{' '}
        · events{' '}
        {Object.entries(summary.events)
          .map(([k, v]) => `${k} ${v}`)
          .join(', ')}{' '}
        · packs {summary.packs}
      </p>
      <div className="research-filters" role="group" aria-label="Filters">
        <label>
          Lens{' '}
          <select value={lens} onChange={(e) => setLens(e.target.value)}>
            {lenses.map(([name, l]) => (
              <option key={name} value={name}>
                {l.label}
              </option>
            ))}
          </select>
        </label>
        <label>
          Game{' '}
          <select value={game} onChange={(e) => setGame(e.target.value)}>
            <option value="">All</option>
            {games.map((g) => (
              <option key={g}>{g}</option>
            ))}
          </select>
        </label>
        <label>
          Venues{' '}
          <select
            value={venuePair}
            onChange={(e) => setVenuePair(e.target.value)}
          >
            <option value="">All</option>
            {pairs.map((p) => (
              <option key={p}>{p}</option>
            ))}
          </select>
        </label>
        <label>
          Min duration (ms){' '}
          <input
            type="number"
            min={0}
            value={minDuration}
            onChange={(e) => setMinDuration(Number(e.target.value) || 0)}
          />
        </label>
        <label>
          Min net{' '}
          <input
            type="number"
            step="0.01"
            value={minNet}
            placeholder="none"
            onChange={(e) => setMinNet(e.target.value)}
          />
        </label>
        <div className="segmented" role="group" aria-label="Count">
          <button
            type="button"
            className={!dedup ? 'active' : ''}
            onClick={() => setDedup(false)}
          >
            Per route
          </button>
          <button
            type="button"
            className={dedup ? 'active' : ''}
            onClick={() => setDedup(true)}
          >
            Deduplicated
          </button>
        </div>
      </div>

      <div className="stat-row">
        <Stat
          label="Events counted"
          value={String(counted.length)}
          note={`lens ok and pack built · states ${Object.entries(
            lensInfo?.states ?? {},
          )
            .map(([k, v]) => `${k} ${v}`)
            .join(', ')}`}
        />
        <Stat
          label={`Episodes (${kind}${dedup ? ', dedup' : ''})`}
          value={String(unit.length)}
          note={`${filtered.hidden} hidden by filter`}
        />
        <Stat
          label="Mean per event"
          value={fmt(
            perEvent.length
              ? perEvent.reduce((a, b) => a + b, 0) / perEvent.length
              : null,
          )}
          note={`median ${fmt(median(perEvent))}`}
        />
        <Stat
          label="Events with ≥1"
          value={
            perEvent.length
              ? `${Math.round((perEvent.filter((n) => n > 0).length / perEvent.length) * 100)}%`
              : '—'
          }
          note={`${perEvent.filter((n) => n > 0).length} of ${perEvent.length}`}
        />
        <Stat
          label={`Net total (modeled, ${lensInfo?.value_unit ?? ''})`}
          value={fmt(nets.reduce((a, b) => a + b, 0))}
          note={`largest ${fmt(nets.length ? Math.max(...nets) : null)}`}
        />
      </div>

      <div className="histograms">
        <Histogram
          title="Episodes per event"
          spec={edges.per_event}
          values={perEvent}
        />
        <Histogram
          title="Duration (ms)"
          spec={edges.duration_ms}
          values={shown.map((r) => r.duration_ms)}
          marker={100}
        />
        <Histogram
          title={`Net (${lensInfo?.value_unit ?? ''}, modeled)`}
          spec={edges.net}
          values={nets}
        />
        <Histogram
          title="Quantity (contracts)"
          spec={edges.quantity}
          values={shown
            .map((r) => r.quantity)
            .filter((q): q is number => q !== null)}
        />
      </div>

      <div className="research-table" role="table" aria-label="Events">
        <div className="row head" role="row">
          <span>{header('title', 'Event')}</span>
          <span>{header('game', 'Game')}</span>
          <span>{header('capture_start_ns', 'Date (UTC)')}</span>
          <span>Venues · books</span>
          <span>Context · game</span>
          {lenses.map(([name, l]) => (
            <span key={name} title={l.label}>
              {header(`${name}_headline`, name.replace('_', ' '))}
            </span>
          ))}
        </div>
        {sorted.map((e) => {
          const open = e.pack === 'ok';
          return (
            <div
              key={e.bundle_id as string}
              role="row"
              className={`row${open ? ' link' : ''}`}
              onClick={() =>
                open && navigate(`/research/events/${e.bundle_id}`)
              }
            >
              <span>
                {open ? (
                  <Link
                    to={`/research/events/${e.bundle_id}`}
                    onClick={(ev) => ev.stopPropagation()}
                  >
                    {e.title ?? e.bundle_id}
                  </Link>
                ) : (
                  (e.title ?? e.bundle_id)
                )}
                {e.state !== 'ok' || e.pack !== 'ok' ? (
                  <span className="reason">
                    {String(e.state)}
                    {e.pack ? ` · ${e.pack}` : ''}
                    {e.message || e.pack_error
                      ? `: ${e.message ?? e.pack_error}`
                      : ''}
                  </span>
                ) : null}
              </span>
              <span>{e.game ?? '—'}</span>
              <span className="mono">
                {typeof e.capture_start_ns === 'number'
                  ? new Date(e.capture_start_ns / 1e6)
                      .toISOString()
                      .slice(0, 16)
                      .replace('T', ' ')
                  : '—'}
              </span>
              <span>
                {Array.isArray(e.venues) ? e.venues.join(', ') : '—'} ·{' '}
                {e.book_count ?? '—'}
              </span>
              <span>
                <span className={`state-badge state-${e.context}`}>
                  {String(e.context)}
                </span>{' '}
                <span className={`state-badge state-${e.game_state ?? 'none'}`}>
                  {String(e.game_state ?? '—')}
                </span>
              </span>
              {lenses.map(([name]) => {
                const state = e[`${name}_state`] as string;
                return (
                  <span key={name}>
                    {state === 'ok' ? (
                      <>
                        {e[`${name}_headline`] ?? '—'}
                        {typeof e[`${name}_max_net`] === 'number' ? (
                          <span className="muted">
                            {' '}
                            · max {fmt(e[`${name}_max_net`] as number)}
                          </span>
                        ) : null}
                      </>
                    ) : (
                      <span
                        className={`state-badge state-${state}`}
                        title={(e[`${name}_error`] as string) ?? undefined}
                      >
                        {state.replace('_', ' ')}
                      </span>
                    )}
                  </span>
                );
              })}
            </div>
          );
        })}
      </div>
    </>
  );
}

function Stat({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note?: string;
}) {
  return (
    <div className="stat">
      <span className="stat-label">{label}</span>
      <strong>{value}</strong>
      {note && <span className="muted small">{note}</span>}
    </div>
  );
}

function edgeLabel(value: number): string {
  return value >= 1000 ? `${value / 1000}k` : String(value);
}

function Histogram({
  title,
  spec,
  values,
  marker,
}: {
  title: string;
  spec?: HistogramSpec;
  values: number[];
  marker?: number;
}) {
  if (!spec) return null;
  const counts = histogram(values, spec.edges);
  const max = Math.max(1, ...counts);
  const width = 320;
  const height = 110;
  const bar = width / counts.length;
  const labels = counts.map((_, i) =>
    i === 0
      ? `< ${edgeLabel(spec.edges[0])}`
      : i === spec.edges.length
        ? `≥ ${edgeLabel(spec.edges[i - 1])}`
        : `${edgeLabel(spec.edges[i - 1])}–${edgeLabel(spec.edges[i])}`,
  );
  const markerIndex = marker === undefined ? -1 : spec.edges.indexOf(marker);
  return (
    <figure className="histogram">
      <figcaption>{title}</figcaption>
      <svg
        viewBox={`0 0 ${width} ${height + 18}`}
        role="img"
        aria-label={title}
      >
        {counts.map((count, i) => {
          const h = (count / max) * height;
          return (
            <g key={i}>
              <rect
                x={i * bar + 1}
                y={height - h}
                width={bar - 2}
                height={Math.max(h, count ? 1 : 0)}
                rx={2}
                className="bar"
              >
                <title>{`${labels[i]}: ${count}`}</title>
              </rect>
              <rect
                x={i * bar}
                y={0}
                width={bar}
                height={height}
                fill="transparent"
              >
                <title>{`${labels[i]}: ${count}`}</title>
              </rect>
            </g>
          );
        })}
        {markerIndex >= 0 && (
          <line
            x1={(markerIndex + 1) * bar}
            x2={(markerIndex + 1) * bar}
            y1={0}
            y2={height}
            className="marker"
          >
            <title>{`${marker} ms`}</title>
          </line>
        )}
        <line x1={0} x2={width} y1={height} y2={height} className="baseline" />
        <text x={0} y={height + 14} className="axis">
          {labels[0]}
        </text>
        <text x={width} y={height + 14} className="axis" textAnchor="end">
          {labels[labels.length - 1]}
        </text>
      </svg>
      <span className="muted small">{values.length} values</span>
    </figure>
  );
}
