import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  BrowserRouter,
  Navigate,
  NavLink,
  Route,
  Routes,
  useLocation,
} from 'react-router-dom';
import type {
  UniverseCadence,
  UniverseCadenceRun,
  UniverseSelectionDetail,
} from '../event-universe';
import {
  occurrenceExplanation,
  retirementExplanation,
} from './event-universe-view-model';
import {
  cadenceRunEmptyMessage,
  cadenceStatusLabel,
} from './cadence-view-model';
import { EventUniversePage } from './event-universe';
import { targeterCadenceNeeded } from './app-routing';
import './style.css';

const CADENCE_ENDPOINT = '/api/event-universe/v1/targeter/cadence?limit=5';

const date = (value: string | null | undefined) => {
  if (!value) return '—';
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? value : parsed.toLocaleString();
};

const relativeTime = (value: string) => {
  const timestamp = new Date(value).valueOf();
  if (Number.isNaN(timestamp)) return value;
  const seconds = (timestamp - Date.now()) / 1000;
  const absolute = Math.abs(seconds);
  if (absolute < 60) return 'just now';
  const [divisor, unit] =
    absolute < 3600
      ? [60, 'minute']
      : absolute < 86400
        ? [3600, 'hour']
        : [86400, 'day'];
  return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(
    Math.round(seconds / divisor),
    unit as Intl.RelativeTimeFormatUnit,
  );
};

const label = (value: string | null | undefined) =>
  value
    ? value
        .replaceAll('_', ' ')
        .replace(/\b\w/g, (character) => character.toUpperCase())
    : '—';

async function loadCadence() {
  const response = await fetch(CADENCE_ENDPOINT, {
    method: 'GET',
    headers: { accept: 'application/json' },
  });
  if (!response.ok) throw new Error('Targeter cadence is unavailable.');
  return (await response.json()) as UniverseCadence;
}

function App() {
  const location = useLocation();
  const needsCadence = targeterCadenceNeeded(location.pathname);
  const [cadence, setCadence] = useState<UniverseCadence | null>(null);
  const [error, setError] = useState('');
  const [refreshing, setRefreshing] = useState(false);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    try {
      setCadence(await loadCadence());
      setError('');
    } catch (reason) {
      setError(
        reason instanceof Error
          ? reason.message
          : 'Targeter cadence is unavailable.',
      );
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    if (!needsCadence) return;
    void refresh();
    const interval = window.setInterval(() => void refresh(), 60_000);
    return () => window.clearInterval(interval);
  }, [needsCadence, refresh]);

  return (
    <>
      <header>
        <div>
          <span className="eyebrow">PREDICTION INDEXER</span>
          <h1>{needsCadence ? 'Targeter Cadence' : 'Event Universe'}</h1>
        </div>
        <nav aria-label="Main navigation">
          <NavLink to="/" end>
            Universe
          </NavLink>
          <NavLink to="/operations" end>
            Cadence
          </NavLink>
          <NavLink to="/operations/selections">Selections</NavLink>
        </nav>
        {needsCadence && (
          <button onClick={() => void refresh()} disabled={refreshing}>
            ↻ Refresh cadence
          </button>
        )}
      </header>
      {needsCadence && error && cadence && (
        <div className="alert" role="alert">
          {error} The last successful cadence projection remains displayed.
        </div>
      )}
      <main>
        <Routes>
          <Route path="/" element={<EventUniversePage />} />
          <Route
            path="/operations"
            element={
              <CadenceRoute cadence={cadence} error={error}>
                {(projection) => <CadenceOverview cadence={projection} />}
              </CadenceRoute>
            }
          />
          <Route
            path="/operations/selections"
            element={
              <CadenceRoute cadence={cadence} error={error}>
                {(projection) => <SelectionExplorer cadence={projection} />}
              </CadenceRoute>
            }
          />
          <Route path="/event-universe" element={<Navigate to="/" replace />} />
          <Route
            path="/operations/events"
            element={<Navigate to="/operations/selections" replace />}
          />
          <Route
            path="/operations/config"
            element={<Navigate to="/operations" replace />}
          />
          <Route
            path="/events"
            element={<Navigate to="/operations/selections" replace />}
          />
          <Route
            path="/config"
            element={<Navigate to="/operations" replace />}
          />
        </Routes>
      </main>
      {needsCadence && cadence && <CadenceFooter cadence={cadence} />}
    </>
  );
}

function CadenceRoute({
  cadence,
  error,
  children,
}: {
  cadence: UniverseCadence | null;
  error: string;
  children: (projection: UniverseCadence) => React.ReactNode;
}) {
  if (cadence) return children(cadence);
  if (error)
    return (
      <section className="operations-unavailable">
        <span className="eyebrow">UNIVERSE CADENCE API</span>
        <h2>Targeter cadence is unavailable</h2>
        <p>
          The historical Event Universe explorer remains independent and
          available from the Universe page.
        </p>
      </section>
    );
  return <div className="loading">Loading Targeter cadence…</div>;
}

function CadenceOverview({ cadence }: { cadence: UniverseCadence }) {
  const [selectedRunId, setSelectedRunId] = useState(
    cadence.runs[0]?.run_id ?? '',
  );
  useEffect(() => {
    if (!cadence.runs.some((run) => run.run_id === selectedRunId))
      setSelectedRunId(cadence.runs[0]?.run_id ?? '');
  }, [cadence.runs, selectedRunId]);
  const run =
    cadence.runs.find((candidate) => candidate.run_id === selectedRunId) ??
    cadence.runs[0];

  return (
    <div className="stack">
      <section>
        <Title
          title="Universe-indexed run timeline"
          sub="Up to five Targeter runs, newest first"
        />
        <div className="timeline">
          {cadence.runs.map((candidate, index) => (
            <button
              key={candidate.run_id}
              className={`${index === 0 ? 'latest' : ''} ${candidate.run_id === run?.run_id ? 'selected' : ''}`}
              aria-pressed={candidate.run_id === run?.run_id}
              onClick={() => setSelectedRunId(candidate.run_id)}
            >
              <span>{index === 0 ? 'LATEST' : 'RUN'}</span>
              <b>{candidate.run_id}</b>
              <small>{date(candidate.generated_at)}</small>
              <em className={candidate.input_complete ? 'ok' : 'warn'}>
                {candidate.input_complete
                  ? 'Complete input'
                  : 'Incomplete input'}
              </em>
            </button>
          ))}
        </div>
      </section>
      {run ? (
        <>
          <RunMetrics
            run={run}
            latest={run.run_id === cadence.runs[0]?.run_id}
          />
          <RunProvenance run={run} />
          <RunSelections run={run} />
        </>
      ) : (
        <section className="empty">
          No Targeter runs are indexed. Cadence state is unavailable.
        </section>
      )}
    </div>
  );
}

function RunMetrics({
  run,
  latest,
}: {
  run: UniverseCadenceRun;
  latest: boolean;
}) {
  const targets = run.selections.reduce(
    (count, selection) => count + selection.context.targets.length,
    0,
  );
  const venues = new Set(
    run.selections.flatMap((selection) =>
      selection.context.targets.map((target) => target.venue),
    ),
  );
  return (
    <section>
      <Title
        title={latest ? 'Latest indexed run' : 'Selected indexed run'}
        sub={relativeTime(run.generated_at)}
      />
      <div className="metrics">
        <Metric n={run.selections.length} label="Selected bundles" />
        <Metric n={run.counts.candidates} label="Candidates" />
        <Metric n={run.counts.rejected} label="Rejected" />
        <Metric
          n={
            run.selections.filter(
              (selection) => selection.occurrence_kind === 'retained',
            ).length
          }
          label="Retained bundles"
        />
        <Metric n={targets} label="Selected targets" />
        <Metric n={venues.size} label="Selected venues" />
        <Metric n={run.projection_row_count} label="Projected rows" />
      </div>
    </section>
  );
}

function RunProvenance({ run }: { run: UniverseCadenceRun }) {
  return (
    <section>
      <Title
        title="Run provenance"
        sub={`Report v${run.report_version} · Strategy v${run.strategy_version}`}
      />
      <div className="run-proof">
        <b className={run.input_complete ? 'ok' : 'warn'}>
          {run.input_complete ? 'COMPLETE INPUT' : 'INCOMPLETE INPUT'}
        </b>
        <span>Projection v{run.projection_version}</span>
        <code>{run.manifest_key}</code>
        <code>manifest {run.manifest_sha256}</code>
        <code>report {run.report_sha256}</code>
      </div>
    </section>
  );
}

function RunSelections({ run }: { run: UniverseCadenceRun }) {
  return (
    <section>
      <Title
        title="Selected bundles"
        sub="Verified selected lifecycle detail from the cadence projection"
      />
      {run.selections.length ? (
        <div className="cards">
          {run.selections.map((selection) => (
            <SelectionCard
              key={`${selection.run_id}-${selection.bundle_id}`}
              selection={selection}
            />
          ))}
        </div>
      ) : (
        <p className="empty">{cadenceRunEmptyMessage(run)}</p>
      )}
    </section>
  );
}

function SelectionCard({ selection }: { selection: UniverseSelectionDetail }) {
  const context = selection.context;
  const participants = context.participants.join(' vs ') || selection.bundle_id;
  const venues = [
    ...new Set(context.targets.map((target) => target.venue)),
  ].sort();
  return (
    <article
      className={`bundle ${selection.occurrence_kind === 'retained' ? 'retained' : ''}`}
    >
      <div className="row">
        <span className="tag">{selection.occurrence_kind.toUpperCase()}</span>
        <strong>{label(selection.continuity_disposition)}</strong>
      </div>
      <h3>{participants}</h3>
      <code>{selection.bundle_id}</code>
      <div className="muted">{venues.join(' · ') || 'No selected venues'}</div>
      <dl>
        <dt>Sport / game</dt>
        <dd>
          {label(selection.sport)} · {label(selection.game)}
        </dd>
        <dt>Topology</dt>
        <dd>{label(selection.topology)}</dd>
        <dt>Activation</dt>
        <dd>{date(selection.activation_at)}</dd>
        <dt>Capture starts</dt>
        <dd>{date(selection.capture_start_at)}</dd>
        <dt>Origin run</dt>
        <dd>
          <code>{selection.origin.run_id}</code>
        </dd>
        <dt>Retirement</dt>
        <dd>
          {selection.retirement
            ? label(selection.retirement.disposition)
            : 'Not observed'}
        </dd>
      </dl>
      <p className="note">{occurrenceExplanation(selection)}</p>
      {selection.retirement && (
        <p className="retirement-copy">{retirementExplanation(selection)}</p>
      )}
      <MarketDetails selection={selection} />
      <RelationshipDetails selection={selection} />
      <details className="detail-group">
        <summary>Immutable source and origin</summary>
        <div className="identity-proof">
          <b>Occurrence source · {selection.run_id}</b>
          <code>{selection.source.manifest_key}</code>
          <code>manifest {selection.source.manifest_sha256}</code>
          <code>report {selection.source.report_sha256}</code>
          <b>Origin · {selection.origin.run_id}</b>
          <code>{selection.origin.manifest_key}</code>
          <code>manifest {selection.origin.manifest_sha256}</code>
          <code>report {selection.origin.report_sha256}</code>
        </div>
      </details>
    </article>
  );
}

function MarketDetails({ selection }: { selection: UniverseSelectionDetail }) {
  return (
    <details className="markets">
      <summary>{selection.context.markets.length} markets</summary>
      <ul>
        {selection.context.markets.map((market) => (
          <li key={market.target_id}>
            <span className="market-venue">{market.venue}</span>
            <span>{market.selected ? 'Selected' : 'Sibling'}</span>
            <code>{market.target_id}</code>
          </li>
        ))}
      </ul>
      <div className="target-list">
        {selection.context.targets.map((target) => (
          <div className="target-proof" key={target.target_id}>
            <b>
              {target.venue} · {label(target.canonical_class)}
            </b>
            <code>{target.target_id}</code>
            <span>
              Source <code>{target.source_ref}</code>
            </span>
            <span>
              Subscriptions: {target.subscription_ids.join(', ') || 'None'}
            </span>
          </div>
        ))}
      </div>
    </details>
  );
}

function RelationshipDetails({
  selection,
}: {
  selection: UniverseSelectionDetail;
}) {
  return (
    <details className="detail-group">
      <summary>{selection.context.relationships.length} relationships</summary>
      <div>
        {selection.context.relationships.map((relationship, index) => (
          <div
            className="relationship-proof"
            key={`${relationship.left}-${relationship.right}-${index}`}
          >
            <b>
              {label(relationship.relationship)} ·{' '}
              {label(relationship.coverage)}
            </b>
            <code>{relationship.left}</code>
            <span>↔</span>
            <code>{relationship.right}</code>
          </div>
        ))}
      </div>
    </details>
  );
}

function SelectionExplorer({ cadence }: { cadence: UniverseCadence }) {
  const [query, setQuery] = useState('');
  const [occurrence, setOccurrence] = useState('all');
  const [runId, setRunId] = useState('all');
  const [venue, setVenue] = useState('all');
  const rows = useMemo(
    () =>
      cadence.runs.flatMap((run) =>
        run.selections.map((selection) => ({ run, selection })),
      ),
    [cadence],
  );
  const venues = [
    ...new Set(
      rows.flatMap(({ selection }) =>
        selection.context.targets.map((target) => target.venue),
      ),
    ),
  ].sort();
  const shown = rows.filter(({ run, selection }) => {
    const haystack = [
      selection.bundle_id,
      selection.sport,
      selection.game,
      selection.topology,
      ...selection.context.participants,
      ...selection.context.event_refs,
      ...selection.context.targets.flatMap((target) => [
        target.venue,
        target.target_id,
      ]),
    ]
      .join(' ')
      .toLowerCase();
    return (
      haystack.includes(query.toLowerCase()) &&
      (occurrence === 'all' || occurrence === selection.occurrence_kind) &&
      (runId === 'all' || runId === run.run_id) &&
      (venue === 'all' ||
        selection.context.targets.some((target) => target.venue === venue))
    );
  });

  return (
    <section>
      <Title
        title="Recent selected bundles"
        sub={`${shown.length} of ${rows.length} selections across indexed cadence runs`}
      />
      <div className="filters">
        <label>
          Search
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Team, venue, bundle, target…"
          />
        </label>
        <label>
          Occurrence
          <select
            value={occurrence}
            onChange={(event) => setOccurrence(event.target.value)}
          >
            <option value="all">All</option>
            <option value="complete">Complete</option>
            <option value="retained">Retained</option>
          </select>
        </label>
        <label>
          Run
          <select
            value={runId}
            onChange={(event) => setRunId(event.target.value)}
          >
            <option value="all">All five</option>
            {cadence.runs.map((run) => (
              <option key={run.run_id} value={run.run_id}>
                {run.run_id}
              </option>
            ))}
          </select>
        </label>
        <label>
          Venue
          <select
            value={venue}
            onChange={(event) => setVenue(event.target.value)}
          >
            <option value="all">All</option>
            {venues.map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </label>
      </div>
      {shown.length ? (
        <div className="eventlist">
          {shown.map(({ selection }) => (
            <SelectionCard
              key={`${selection.run_id}-${selection.bundle_id}`}
              selection={selection}
            />
          ))}
        </div>
      ) : (
        <p className="empty">No indexed selections match these filters.</p>
      )}
    </section>
  );
}

function CadenceFooter({ cadence }: { cadence: UniverseCadence }) {
  const state = cadence.freshness.state;
  const latest = cadence.runs[0];
  return (
    <footer className="status-footer">
      <div className={state === 'current' ? 'good' : 'bad'}>
        <span className={`dot ${state === 'current' ? 'good' : 'bad'}`} />
        <strong>{cadenceStatusLabel(state)}</strong>
      </div>
      <span>
        Latest run {latest ? relativeTime(latest.generated_at) : 'unavailable'}
      </span>
      <span>Indexed {date(cadence.freshness.latest_indexed_at)}</span>
      <small>
        Universe observation · expected every{' '}
        {cadence.freshness.expected_run_seconds}s · does not verify current.json
        or splice health
      </small>
    </footer>
  );
}

function Metric({ n, label: metricLabel }: { n: number; label: string }) {
  return (
    <div className="metric">
      <strong>{n}</strong>
      <span>{metricLabel}</span>
    </div>
  );
}

function Title({ title, sub }: { title: string; sub: string }) {
  return (
    <div className="title">
      <h2>{title}</h2>
      <span>{sub}</span>
    </div>
  );
}

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
