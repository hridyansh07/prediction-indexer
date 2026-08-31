import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import type {
  UniverseCadenceCandidate,
  UniverseCadenceRun,
  UniverseHealth,
  UniverseSelection,
  UniverseSelectionDetail,
  UniverseTargeterStatus,
} from '../event-universe';
import { Chevron, EventIcon, gameName, SearchIcon, VenueStack } from './icons';
import { universeGet } from './universe-api';

const date = (value: string | null | undefined) =>
  value ? new Date(value).toLocaleString() : 'Unavailable';

const relative = (value: string | null | undefined) => {
  if (!value) return 'Unavailable';
  const seconds = Math.round((new Date(value).valueOf() - Date.now()) / 1000);
  const absolute = Math.abs(seconds);
  const [divisor, unit] =
    absolute < 60
      ? [1, 'second']
      : absolute < 3600
        ? [60, 'minute']
        : [3600, 'hour'];
  return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(
    Math.round(seconds / divisor),
    unit as Intl.RelativeTimeFormatUnit,
  );
};

const label = (value: string | null | undefined) =>
  value
    ?.replaceAll('_', ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase()) ?? '—';

async function loadTargeterRun(runId: string): Promise<UniverseCadenceRun> {
  return universeGet<UniverseCadenceRun>(
    `/v1/targeter/runs/${encodeURIComponent(runId)}`,
  );
}

function useTargeterRun(runId: string | null) {
  const [run, setRun] = useState<UniverseCadenceRun | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(Boolean(runId));
  useEffect(() => {
    if (!runId) {
      setRun(null);
      setLoading(false);
      return;
    }
    let active = true;
    setRun(null);
    setError('');
    setLoading(true);
    void loadTargeterRun(runId)
      .then((nextRun) => {
        if (active) setRun(nextRun);
      })
      .catch(() => {
        if (active) setError('Targeter run diagnostics are unavailable.');
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [runId]);
  return { run, error, loading };
}

function PageHeading({
  eyebrow,
  title,
  copy,
}: {
  eyebrow: string;
  title: string;
  copy: string;
}) {
  return (
    <div className="page-heading">
      <span className="eyebrow">{eyebrow}</span>
      <h1>{title}</h1>
      <p>{copy}</p>
    </div>
  );
}

function StateDot({ state }: { state: 'live' | 'warn' | 'unknown' }) {
  return <span className={`state-dot ${state}`} aria-hidden="true" />;
}

export function StatusPage({
  health,
  status,
  healthError,
  statusError,
  refreshing,
  refresh,
}: {
  health: UniverseHealth | null;
  status: UniverseTargeterStatus | null;
  healthError: string;
  statusError: string;
  refreshing: boolean;
  refresh: () => Promise<void>;
}) {
  const targeterLive = status?.freshness.state === 'current';
  const checking = !health && !status && !healthError && !statusError;
  return (
    <div className="status-page">
      <section className="status-intro">
        <div>
          <span className="eyebrow">SYSTEM STATUS</span>
          <h1>
            {checking
              ? 'Checking indexer health.'
              : health && targeterLive
                ? 'Server and cadence are on track.'
                : 'Indexing needs attention.'}
          </h1>
          <p>
            A concise view of the Event Universe server and Targeter cadence.
          </p>
        </div>
        <button
          className="quiet-button"
          onClick={() => void refresh()}
          disabled={refreshing}
        >
          {refreshing ? 'Refreshing…' : '↻ Refresh'}
        </button>
      </section>
      <section className="status-grid" aria-label="Service health">
        <article className="status-card">
          <div className="status-card-title">
            <StateDot state={health ? 'live' : 'warn'} />
            <span>EVENT UNIVERSE</span>
          </div>
          <strong>{health ? 'Server live' : 'Unavailable'}</strong>
          <p>
            {healthError ||
              (health?.latest_run
                ? `Latest evidence ${relative(health.latest_run.generated_at)}`
                : 'No indexed runs yet')}
          </p>
        </article>
        <article className="status-card featured">
          <div className="status-card-title">
            <StateDot state={targeterLive ? 'live' : 'warn'} />
            <span>TARGETER CADENCE</span>
          </div>
          <strong>
            {targeterLive
              ? 'On cadence'
              : label(status?.freshness.state ?? 'Unavailable')}
          </strong>
          <p>
            {statusError ||
              `Expected every ${Math.round((status?.freshness.expected_run_seconds ?? 600) / 60)} minutes`}
          </p>
        </article>
        <article className="status-card unverified">
          <div className="status-card-title">
            <StateDot state="unknown" />
            <span>CAPTURE</span>
          </div>
          <strong>Unverified</strong>
          <p>
            Cadence evidence does not verify live splice or frame capture
            health.
          </p>
        </article>
      </section>
      <section className="current-summary">
        <div>
          <span className="eyebrow">CURRENT COMPLETE TARGET SET</span>
          <h2>
            {status?.current_complete_run
              ? `${status.current_complete_summary.selected_bundles} bundles across ${status.current_complete_summary.venues.length} venues`
              : 'No complete run available'}
          </h2>
          <p>
            {status?.current_complete_run
              ? `${status.current_complete_summary.selected_targets} selected targets · run ${status.current_complete_run.run_id}`
              : 'Waiting for complete Targeter evidence.'}
          </p>
        </div>
        <VenueStack venues={status?.current_complete_summary.venues ?? []} />
        <Link className="primary-link" to="/targets">
          View current targets <Chevron />
        </Link>
        <Link className="secondary-link" to="/decisions">
          View run diagnostics <Chevron />
        </Link>
      </section>
      <p className="mobile-truth">
        Capture status remains unverified until a splice-health projection
        exists.
      </p>
    </div>
  );
}

function EmptyPage({ error, loading }: { error: string; loading: string }) {
  return (
    <div className={error ? 'error-state' : 'empty-state'}>
      {error || loading}
    </div>
  );
}

export function MobileDetailNotice() {
  return (
    <div className="mobile-only">
      <h1>Desktop detail view</h1>
      <p>This compact mobile UI focuses on server and cadence health.</p>
      <Link className="primary-link" to="/">
        View status
      </Link>
    </div>
  );
}

function EventFilters({
  games,
  selected,
  setSelected,
}: {
  games: Array<{ game: string | null; sport: string }>;
  selected: string;
  setSelected: (value: string) => void;
}) {
  return (
    <div
      className="event-filters"
      role="group"
      aria-label="Filter by event type"
    >
      <button
        className={!selected ? 'active' : ''}
        onClick={() => setSelected('')}
        aria-label="Show all event types"
      >
        All
      </button>
      {games.map(({ game, sport }) => {
        const key = game ?? sport;
        return (
          <button
            key={key}
            className={selected === key ? 'active' : ''}
            onClick={() => setSelected(key)}
            aria-label={`Show ${gameName(game, sport)}`}
          >
            <EventIcon game={game} sport={sport} labelled />
          </button>
        );
      })}
    </div>
  );
}

export function TargetsPage({
  status,
  error,
}: {
  status: UniverseTargeterStatus | null;
  error: string;
}) {
  const runId = status?.current_complete_run?.run_id ?? null;
  const loaded = useTargeterRun(runId);
  const run = loaded.run;
  const [query, setQuery] = useState('');
  const [lifecycle, setLifecycle] = useState<'all' | 'current' | 'retained'>(
    'all',
  );
  const [event, setEvent] = useState('');
  const [detail, setDetail] = useState<UniverseSelectionDetail | null>(null);
  const games = useMemo(() => {
    const values = new Map<string, { game: string | null; sport: string }>();
    for (const selection of run?.selections ?? [])
      values.set(selection.game ?? selection.sport, {
        game: selection.game,
        sport: selection.sport,
      });
    return [...values.values()];
  }, [run]);
  const shown = (run?.selections ?? []).filter((selection) => {
    const text =
      `${selection.bundle_id} ${selection.context.participants.join(' ')}`.toLowerCase();
    return (
      text.includes(query.toLowerCase()) &&
      (!event || event === (selection.game ?? selection.sport)) &&
      (lifecycle === 'all' ||
        (lifecycle === 'retained') ===
          (selection.occurrence_kind === 'retained'))
    );
  });
  if (loaded.loading || !run)
    return (
      <EmptyPage
        error={error || loaded.error}
        loading="Loading the current complete target set…"
      />
    );
  return (
    <div className="desktop-page">
      <MobileDetailNotice />
      <PageHeading
        eyebrow="CURRENT TARGETS"
        title="Everything being indexed."
        copy={`The full selection set from the newest complete run · ${date(run.generated_at)}`}
      />
      <div className="compact-toolbar">
        <label className="search-field">
          <SearchIcon />
          <span className="sr-only">Search targets</span>
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search bundles or participants"
          />
        </label>
        <div className="segmented" role="group" aria-label="Lifecycle filter">
          {(['all', 'current', 'retained'] as const).map((value) => (
            <button
              key={value}
              className={lifecycle === value ? 'active' : ''}
              onClick={() => setLifecycle(value)}
            >
              {label(value)}
            </button>
          ))}
        </div>
        <EventFilters games={games} selected={event} setSelected={setEvent} />
      </div>
      <div className="bundle-table">
        <div className="bundle-table-head">
          <span>Event</span>
          <span>Venues</span>
          <span>Activation</span>
          <span>Targets</span>
          <span />
        </div>
        {shown.map((selection) => (
          <BundleRow
            key={selection.bundle_id}
            selection={selection}
            open={() => setDetail(selection)}
          />
        ))}
      </div>
      {!shown.length && (
        <div className="empty-state">
          No current targets match these controls.
        </div>
      )}
      <BundleDrawer detail={detail} run={run} close={() => setDetail(null)} />
    </div>
  );
}

function BundleRow({
  selection,
  open,
}: {
  selection: UniverseSelectionDetail;
  open: () => void;
}) {
  const venues = selection.context.targets.map((target) => target.venue);
  return (
    <button className="bundle-row" onClick={open}>
      <span className="event-cell">
        <EventIcon game={selection.game} sport={selection.sport} />
        <span>
          <b>
            {selection.context.participants.join(' vs ') || selection.bundle_id}
          </b>
          <small>
            {label(selection.topology)} ·{' '}
            {selection.occurrence_kind === 'retained'
              ? 'Retained'
              : 'Current candidate'}
          </small>
        </span>
      </span>
      <VenueStack venues={venues} />
      <span className="date-cell">{date(selection.activation_at)}</span>
      <strong className="target-count">
        {selection.context.targets.length}
      </strong>
      <Chevron />
    </button>
  );
}

export function BundleDrawer({
  detail,
  run,
  history = [],
  close,
}: {
  detail: UniverseSelectionDetail | null;
  run?: UniverseCadenceRun;
  history?: UniverseSelection[];
  close: () => void;
}) {
  const [tab, setTab] = useState<'markets' | 'relationships' | 'evidence'>(
    'markets',
  );
  useEffect(() => {
    if (!detail) return;
    setTab('markets');
    const keydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') close();
    };
    window.addEventListener('keydown', keydown);
    return () => window.removeEventListener('keydown', keydown);
  }, [detail, close]);
  if (!detail) return null;
  const candidate = run?.candidates.find(
    (item) => item.bundle_id === detail.bundle_id,
  );
  return (
    <div className="drawer-layer">
      <button
        className="drawer-backdrop"
        onClick={close}
        aria-label="Close bundle detail"
      />
      <div
        className="bundle-drawer"
        role="dialog"
        aria-modal="true"
        aria-label="Bundle detail"
      >
        <div className="drawer-header">
          <div>
            <span className="eyebrow">BUNDLE DETAIL</span>
            <h2>{detail.context.participants.join(' vs ')}</h2>
            <code>{detail.bundle_id}</code>
          </div>
          <button
            className="close-button"
            onClick={close}
            aria-label="Close"
            autoFocus
          >
            ×
          </button>
        </div>
        <div className="drawer-facts">
          <span>
            <b>{detail.context.targets.length}</b> targets
          </span>
          <span>
            <b>
              {
                new Set(detail.context.targets.map((target) => target.venue))
                  .size
              }
            </b>{' '}
            venues
          </span>
          <span>
            <b>{detail.context.relationships.length}</b> relationships
          </span>
        </div>
        <div className="drawer-tabs" role="tablist">
          {(['markets', 'relationships', 'evidence'] as const).map((value) => (
            <button
              role="tab"
              aria-selected={tab === value}
              className={tab === value ? 'active' : ''}
              onClick={() => setTab(value)}
              key={value}
            >
              {label(value)}
            </button>
          ))}
        </div>
        <div className="drawer-body">
          {tab === 'markets' && <MarketList detail={detail} />}
          {tab === 'relationships' &&
            (detail.context.relationships.length ? (
              detail.context.relationships.map((relationship, index) => (
                <div
                  className="proof-card"
                  key={`${relationship.left}-${index}`}
                >
                  <b>
                    {label(relationship.relationship)} ·{' '}
                    {label(relationship.coverage)}
                  </b>
                  <code>{relationship.left}</code>
                  <span>↔</span>
                  <code>{relationship.right}</code>
                </div>
              ))
            ) : (
              <p className="muted">
                No structured relationships in this bundle.
              </p>
            ))}
          {tab === 'evidence' && (
            <div className="evidence-list">
              <span>Occurrence: {label(detail.occurrence_kind)}</span>
              <span>Continuity: {label(detail.continuity_disposition)}</span>
              {candidate && (
                <span>
                  Admission volume: $
                  {candidate.admission.combined_moneyline_volume_usd.toLocaleString()}
                </span>
              )}
              <code>{detail.source.manifest_key}</code>
              <code>Manifest {detail.source.manifest_sha256}</code>
              <code>Report {detail.source.report_sha256}</code>
              {history.length > 0 && (
                <>
                  <h3>Selection timeline</h3>
                  <ol className="history-timeline">
                    {history.map((occurrence) => (
                      <li key={occurrence.run_id}>
                        <b>{date(occurrence.generated_at)}</b>
                        <span>
                          {label(occurrence.occurrence_kind)} ·{' '}
                          {label(occurrence.continuity_disposition)}
                        </span>
                        <code>{occurrence.run_id}</code>
                      </li>
                    ))}
                  </ol>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function MarketList({ detail }: { detail: UniverseSelectionDetail }) {
  return (
    <div className="market-list">
      {detail.context.markets.map((market) => (
        <div className="market-row" key={market.target_id}>
          <VenueStack venues={[market.venue]} />
          <span>
            <b>{label(market.target_id.split(':').at(-1))}</b>
            <code>{market.target_id}</code>
          </span>
          <em>{market.selected ? 'Selected' : 'Sibling'}</em>
        </div>
      ))}
    </div>
  );
}

export function DecisionsPage({
  status,
  error,
}: {
  status: UniverseTargeterStatus | null;
  error: string;
}) {
  const runId = status?.current_complete_run?.run_id ?? null;
  const loaded = useTargeterRun(runId);
  const run = loaded.run;
  const [query, setQuery] = useState('');
  if (loaded.loading || !run)
    return (
      <EmptyPage
        error={error || loaded.error}
        loading="Loading Targeter decisions…"
      />
    );
  const candidates = run.candidates.filter((candidate) =>
    `${candidate.bundle_id} ${candidate.participants.join(' ')} ${candidate.rejection_reasons.join(' ')}`
      .toLowerCase()
      .includes(query.toLowerCase()),
  );
  return (
    <div className="desktop-page decisions-page">
      <MobileDetailNotice />
      <PageHeading
        eyebrow="LATEST COMPLETE RUN"
        title="How Targeter chose."
        copy="The decision funnel stays separate from the bundles that are currently indexed."
      />
      <section className="decision-funnel">
        <FunnelStep count={run.counts.candidates} label="Candidates" />
        <Chevron />
        <FunnelStep count={run.counts.eligible} label="Eligible" />
        <Chevron />
        <FunnelStep count={run.counts.selected} label="Selected" accent />
      </section>
      <label className="search-field decisions-search">
        <SearchIcon />
        <span className="sr-only">Search decisions</span>
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search candidates or reasons"
        />
      </label>
      <section className="decision-list">
        {candidates.map((candidate) => (
          <CandidateRow key={candidate.bundle_id} candidate={candidate} />
        ))}
      </section>
      {!candidates.length && (
        <div className="empty-state">
          No candidate decisions match this search.
        </div>
      )}
    </div>
  );
}

function FunnelStep({
  count,
  label: text,
  accent = false,
}: {
  count: number;
  label: string;
  accent?: boolean;
}) {
  return (
    <div className={accent ? 'funnel-step accent' : 'funnel-step'}>
      <strong>{count}</strong>
      <span>{text}</span>
    </div>
  );
}

function CandidateRow({ candidate }: { candidate: UniverseCadenceCandidate }) {
  const state = candidate.selected
    ? 'Selected'
    : candidate.eligible
      ? 'Not allocated'
      : 'Rejected';
  return (
    <details className="candidate-row">
      <summary>
        <EventIcon game={candidate.game} sport={candidate.sport} />
        <span>
          <b>{candidate.participants.join(' vs ') || candidate.bundle_id}</b>
          <small>
            {candidate.rejection_reasons.map(label).join(' · ') ||
              label(candidate.allocation_rejection) ||
              'Passed all admission gates'}
          </small>
        </span>
        <strong className={candidate.selected ? 'ok' : 'warn'}>{state}</strong>
        <Chevron />
      </summary>
      <div className="candidate-detail">
        <span>Score {candidate.score.toLocaleString()}</span>
        <span>
          Known combined moneyline volume $
          {candidate.admission.combined_moneyline_volume_usd.toLocaleString()}
        </span>
        <span>{candidate.eligible_market_ids.length} eligible markets</span>
        <code>{candidate.bundle_id}</code>
      </div>
    </details>
  );
}
