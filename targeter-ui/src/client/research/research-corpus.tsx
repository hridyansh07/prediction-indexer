import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useNavigate } from 'react-router-dom';
import { corpusFile, useCorpus, type Corpus } from './research-data';
import {
  filterEvents,
  sortEvents,
  type CorpusSummary,
  type EventsIndex,
  type HistogramSpec,
  type LensStateName,
  type LensStats,
} from './research-model';
import './research.css';

const decoder = new TextDecoder();

async function loadIndex(corpus: Corpus) {
  const [summary, index] = await Promise.all([
    corpusFile(corpus, 'summary.json'),
    corpusFile(corpus, 'events.json'),
  ]);
  return {
    summary: JSON.parse(decoder.decode(summary)) as CorpusSummary,
    index: JSON.parse(decoder.decode(index)) as EventsIndex,
  };
}

export function ResearchCorpusPage() {
  const corpus = useCorpus();
  const loaded = useQuery({
    queryKey: ['research', corpus.data?.pointer.version ?? '', 'index'],
    queryFn: () => loadIndex(corpus.data!),
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
          Every committed corpus result and every lens, from verified files. The
          backend records every episode; the 100 ms view is a display filter.
        </p>
      </div>
      {error ? (
        <div className="error-state" role="alert">
          {(error as Error).message}
        </div>
      ) : !corpus.data || !loaded.data ? (
        <div className="empty-state">Loading and verifying the corpus…</div>
      ) : (
        <CorpusView corpus={corpus.data} {...loaded.data} />
      )}
    </div>
  );
}

const fmt = (value: number | null | undefined, digits = 2) =>
  value === null || value === undefined || Number.isNaN(value)
    ? '—'
    : value.toLocaleString('en-US', { maximumFractionDigits: digits });

function utcDate(ns: string | null): string {
  if (ns === null) return '—';
  return new Date(Number(BigInt(ns) / 1_000_000n))
    .toISOString()
    .slice(0, 16)
    .replace('T', ' ');
}

function CorpusView(props: {
  corpus: Corpus;
  summary: CorpusSummary;
  index: EventsIndex;
}) {
  const { corpus, summary, index } = props;
  const lenses = index.lenses.filter((l) => l.adapter === 'sdk_episodes');
  const [lens, setLens] = useState(
    lenses.find((l) => l.lens === 'cross')?.lens ?? lenses[0]?.lens ?? '',
  );
  const [minimum, setMinimum] = useState(true);
  const [dedup, setDedup] = useState(false);
  const [query, setQuery] = useState('');
  const [game, setGame] = useState('');
  const [lensState, setLensState] = useState<LensStateName | ''>('');
  const [sort, setSort] = useState<{ key: string; desc: boolean }>({
    key: 'capture_start_ns',
    desc: false,
  });
  const navigate = useNavigate();
  const info = summary.lenses[lens];
  const stats: LensStats | undefined = minimum ? info?.min_100ms : info?.all;
  const games = [
    ...new Set(
      index.events
        .map((e) => e.game)
        .filter((g): g is string => typeof g === 'string'),
    ),
  ].sort();
  const filtered = useMemo(
    () =>
      filterEvents(index.events, {
        query,
        game: game || null,
        lens,
        lensState: lensState || null,
      }),
    [index.events, query, game, lens, lensState],
  );
  const rows = sortEvents(filtered.shown, sort.key, sort.desc);
  const header = (key: string, label: string, title?: string) => (
    <button
      type="button"
      className="sort"
      title={title}
      onClick={() =>
        setSort((s) => ({ key, desc: s.key === key ? !s.desc : true }))
      }
    >
      {label}
      {sort.key === key ? (sort.desc ? ' ↓' : ' ↑') : ''}
    </button>
  );
  const episodes = stats ? (dedup ? stats.dedup_episodes : stats.episodes) : 0;
  const before = dedup ? info?.all?.dedup_episodes : info?.all?.episodes;
  const mean = stats
    ? dedup
      ? stats.dedup_per_event_mean
      : stats.per_event_mean
    : null;
  const median = stats
    ? dedup
      ? stats.dedup_per_event_median
      : stats.per_event_median
    : null;

  return (
    <>
      <p className="muted small corpus-meta">
        Version <code>{corpus.pointer.version}</code> · inputs{' '}
        {corpus.manifest.inputs
          .map((i) => `${i.key_prefix} (${i.bundles})`)
          .join(', ')}
        {corpus.manifest.superseded.length > 0 &&
          ` · ${corpus.manifest.superseded.length} superseded results`}{' '}
        · events{' '}
        {Object.entries(summary.events)
          .filter(([, v]) => v > 0)
          .map(([k, v]) => `${k.replace('_', ' ')} ${v}`)
          .join(', ')}{' '}
        · packs {summary.packs}
      </p>

      <div className="research-filters" role="group" aria-label="Lens">
        <label>
          Lens{' '}
          <select value={lens} onChange={(e) => setLens(e.target.value)}>
            {lenses.map((l) => (
              <option key={l.lens} value={l.lens}>
                {l.label}
              </option>
            ))}
          </select>
        </label>
        <div className="segmented" role="group" aria-label="Duration filter">
          <button
            type="button"
            className={minimum ? 'active' : ''}
            onClick={() => setMinimum(true)}
          >
            ≥ {summary.min_duration_ms} ms
          </button>
          <button
            type="button"
            className={!minimum ? 'active' : ''}
            onClick={() => setMinimum(false)}
          >
            All durations
          </button>
        </div>
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
            title="Connected components of overlapping episodes that buy a common leg: a conservative lower bound, not a count of distinct opportunities"
            onClick={() => setDedup(true)}
          >
            Deduplicated
          </button>
        </div>
      </div>

      {stats && info ? (
        <>
          <div className="stat-row">
            <Stat
              label="Events counted"
              value={String(stats.events)}
              note={`lens ok and pack built · ${Object.entries(info.states)
                .filter(([, v]) => v > 0)
                .map(([k, v]) => `${k.replace('_', ' ')} ${v}`)
                .join(', ')}`}
            />
            <Stat
              label={`${info.headline_kind} episodes${dedup ? ' (dedup)' : ''}`}
              value={String(episodes)}
              note={
                minimum
                  ? `${(before ?? 0) - episodes} hidden by the ${summary.min_duration_ms} ms filter`
                  : 'no duration filter'
              }
            />
            <Stat
              label="Per event"
              value={fmt(mean)}
              note={`mean · median ${fmt(median)}`}
            />
            <Stat
              label="Events with ≥1"
              value={
                stats.share_with_one === null
                  ? '—'
                  : `${Math.round(stats.share_with_one * 100)}%`
              }
              note={`${stats.events_with_one} of ${stats.events}`}
            />
            <Stat
              label={`Net total · ${info.value_unit ?? ''}`}
              value={fmt(stats.net_total)}
              note={`modeled at open · largest ${fmt(stats.net_max)}`}
            />
          </div>
          <div className="histograms">
            <Histogram
              title="Episodes per event"
              spec={stats.histograms.per_event}
            />
            <Histogram
              title="Duration (ms)"
              spec={stats.histograms.duration_ms}
              marker={summary.min_duration_ms}
            />
            <Histogram
              title={`Net (${info.value_unit ?? ''}, modeled)`}
              spec={stats.histograms.net}
            />
            <Histogram
              title="Quantity (contracts)"
              spec={stats.histograms.quantity}
            />
          </div>
        </>
      ) : (
        <div className="empty-state">This lens has no statistics.</div>
      )}

      <div className="research-filters" role="group" aria-label="Events filter">
        <label>
          Search{' '}
          <input
            className="wide"
            value={query}
            placeholder="title or bundle"
            onChange={(e) => setQuery(e.target.value)}
          />
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
          Lens state{' '}
          <select
            value={lensState}
            onChange={(e) => setLensState(e.target.value as LensStateName | '')}
          >
            <option value="">Any</option>
            {(['ok', 'failed', 'excluded', 'not_run'] as const).map((s) => (
              <option key={s} value={s}>
                {s.replace('_', ' ')}
              </option>
            ))}
          </select>
        </label>
        <span className="muted small">
          {rows.length} shown
          {filtered.hidden > 0 && ` · ${filtered.hidden} hidden by filter`}
        </span>
      </div>

      <div className="research-table-wrap">
        <div className="research-table" role="table" aria-label="Events">
          <div className="row head" role="row">
            <span>{header('title', 'Event')}</span>
            <span>{header('game', 'Game')}</span>
            <span>{header('capture_start_ns', 'Start (UTC)')}</span>
            <span>{header('book_count', 'Books')}</span>
            <span>Context · game state</span>
            {lenses.map((l) => (
              <span key={l.lens}>
                {header(
                  `${l.lens}_headline`,
                  l.lens.replace('_', ' '),
                  l.label,
                )}
              </span>
            ))}
          </div>
          {rows.map((e) => {
            const open = e.pack === 'ok';
            const problem =
              e.state !== 'ok' || e.pack !== 'ok'
                ? `${e.state}${e.pack ? ` · ${e.pack}` : ''}${e.message || e.pack_error ? `: ${e.message ?? e.pack_error}` : ''}`
                : null;
            return (
              <div
                key={e.bundle_id}
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
                  {e.title !== e.bundle_id && (
                    <span className="reason mono">{e.bundle_id}</span>
                  )}
                  {problem && <span className="reason">{problem}</span>}
                </span>
                <span>{e.game ?? '—'}</span>
                <span className="mono">{utcDate(e.capture_start_ns)}</span>
                <span>
                  {e.book_count ?? '—'}
                  {e.venues && (
                    <span className="reason">{e.venues.join(', ')}</span>
                  )}
                </span>
                <span>
                  <span className={`state-badge state-${e.context}`}>
                    context {e.context}
                  </span>
                  <span
                    className={`state-badge state-${e.game_state ?? 'none'}`}
                  >
                    {(e.game_state ?? '—').replace('_', ' ')}
                  </span>
                </span>
                {lenses.map((l) => {
                  const state = String(e[`${l.lens}_state`]);
                  const maxNet = e[`${l.lens}_max_net`];
                  return (
                    <span key={l.lens}>
                      {state === 'ok' ? (
                        <>
                          {e[`${l.lens}_headline`] ?? '—'}
                          {typeof maxNet === 'number' && (
                            <span className="reason">max {fmt(maxNet)}</span>
                          )}
                        </>
                      ) : (
                        <span
                          className={`state-badge state-${state}`}
                          title={String(e[`${l.lens}_error`] ?? '')}
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
      </div>
      <p className="muted small">
        Lens columns count the headline episode kind per event, all durations.
        Hover a failed or excluded badge for its reason.
      </p>
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
  if (value >= 1_000_000) return `${value / 1_000_000}M`;
  if (value >= 1000) return `${value / 1000}k`;
  return String(value);
}

function Histogram({
  title,
  spec,
  marker,
}: {
  title: string;
  spec?: HistogramSpec;
  marker?: number;
}) {
  if (!spec) return null;
  const { counts, edges } = spec;
  const max = Math.max(1, ...counts);
  const width = 320;
  const height = 110;
  const bar = width / counts.length;
  const labels = counts.map((_, i) =>
    i === 0
      ? `< ${edgeLabel(edges[0])}`
      : i === edges.length
        ? `≥ ${edgeLabel(edges[i - 1])}`
        : `${edgeLabel(edges[i - 1])}–${edgeLabel(edges[i])}`,
  );
  const markerIndex = marker === undefined ? -1 : edges.indexOf(marker);
  const total = counts.reduce((a, b) => a + b, 0);
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
                width={Math.max(1, bar - 2)}
                height={Math.max(h, count ? 1 : 0)}
                rx={2}
                className="bar"
              />
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
            x1={markerIndex * bar + bar}
            x2={markerIndex * bar + bar}
            y1={0}
            y2={height}
            className="marker"
          />
        )}
        <line x1={0} x2={width} y1={height} y2={height} className="baseline" />
        <text x={0} y={height + 14} className="axis">
          {labels[0]}
        </text>
        <text x={width} y={height + 14} className="axis" textAnchor="end">
          {labels[labels.length - 1]}
        </text>
      </svg>
      <span className="muted small">{total} values · hover a bar</span>
    </figure>
  );
}
