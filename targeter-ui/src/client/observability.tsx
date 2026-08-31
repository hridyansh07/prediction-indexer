import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import type {
  UniverseEventDetail,
  UniverseHealth,
  UniverseSelectedMarket,
  UniverseSelection,
  UniverseSelectionDetail,
  UniverseTargeterDecision,
  UniverseTargeterRunDetail,
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

async function loadTargeterRun(
  runId: string,
): Promise<UniverseTargeterRunDetail> {
  return universeGet<UniverseTargeterRunDetail>(
    `/v1/targeter/runs/${encodeURIComponent(runId)}`,
  );
}

function useTargeterRun(runId: string | null) {
  const [run, setRun] = useState<UniverseTargeterRunDetail | null>(null);
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

function useEventDetails(eventIds: string[]) {
  const key = [...new Set(eventIds)].sort().join('\n');
  const [events, setEvents] = useState<Record<string, UniverseEventDetail>>({});
  const [error, setError] = useState('');
  useEffect(() => {
    const ids = key ? key.split('\n') : [];
    if (!ids.length) {
      setEvents({});
      return;
    }
    let active = true;
    setError('');
    void Promise.all(
      ids.map((eventId) =>
        universeGet<UniverseEventDetail>(
          `/v1/events/${encodeURIComponent(eventId)}`,
        ),
      ),
    )
      .then((details) => {
        if (active)
          setEvents(
            Object.fromEntries(
              details.map((detail) => [detail.event.event_id, detail]),
            ),
          );
      })
      .catch(() => {
        if (active) setError('Normalized event detail is unavailable.');
      });
    return () => {
      active = false;
    };
  }, [key]);
  return { events, error };
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
              ? `${status.current_complete_summary.selected_targets} selected markets · run ${status.current_complete_run.run_id}`
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
  const normalized = useEventDetails(
    run?.selected_markets.map((market) => market.event_id) ?? [],
  );
  const [query, setQuery] = useState('');
  const [lifecycle, setLifecycle] = useState<'all' | 'current' | 'retained'>(
    'all',
  );
  const [event, setEvent] = useState('');
  const [detailId, setDetailId] = useState<string | null>(null);
  const grouped = useMemo(() => {
    const groups = new Map<string, UniverseSelectedMarket[]>();
    for (const market of run?.selected_markets ?? [])
      groups.set(market.event_id, [
        ...(groups.get(market.event_id) ?? []),
        market,
      ]);
    return [...groups].map(([eventId, markets]) => ({
      detail: normalized.events[eventId],
      markets,
    }));
  }, [run, normalized.events]);
  const games = useMemo(() => {
    const values = new Map<string, { game: string | null; sport: string }>();
    for (const { detail } of grouped)
      if (detail)
        values.set(detail.event.game ?? detail.event.sport, {
          game: detail.event.game,
          sport: detail.event.sport,
        });
    return [...values.values()];
  }, [grouped]);
  const shown = grouped.filter(({ detail, markets }) => {
    if (!detail) return false;
    const record = detail.event;
    const retained = markets.every(
      (market) => market.selection_reason === 'retained',
    );
    const text =
      `${record.event_id} ${record.participants.join(' ')}`.toLowerCase();
    return (
      text.includes(query.toLowerCase()) &&
      (!event || event === (record.game ?? record.sport)) &&
      (lifecycle === 'all' || (lifecycle === 'retained') === retained)
    );
  });
  if (loaded.loading || !run)
    return (
      <EmptyPage
        error={error || loaded.error}
        loading="Loading the current complete target set…"
      />
    );
  if (normalized.error)
    return (
      <EmptyPage
        error={normalized.error}
        loading="Loading normalized events…"
      />
    );
  return (
    <div className="desktop-page">
      <MobileDetailNotice />
      <PageHeading
        eyebrow="CURRENT TARGETS"
        title="Everything being indexed."
        copy={`Normalized selected markets from the newest complete run · ${date(run.run.generated_at)}`}
      />
      <div className="compact-toolbar">
        <label className="search-field">
          <SearchIcon />
          <span className="sr-only">Search targets</span>
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search events or participants"
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
          <span>Markets</span>
          <span />
        </div>
        {shown.map(({ detail, markets }) => (
          <NormalizedTargetRow
            key={detail.event.event_id}
            detail={detail}
            markets={markets}
            open={() => setDetailId(detail.event.event_id)}
          />
        ))}
      </div>
      {!shown.length && (
        <div className="empty-state">
          {grouped.some(({ detail }) => !detail)
            ? 'Loading normalized event details…'
            : 'No current targets match these controls.'}
        </div>
      )}
      <NormalizedTargetDrawer
        detail={detailId ? normalized.events[detailId] : null}
        markets={
          detailId
            ? (grouped.find(({ detail }) => detail?.event.event_id === detailId)
                ?.markets ?? [])
            : []
        }
        close={() => setDetailId(null)}
      />
    </div>
  );
}

function NormalizedTargetRow({
  detail,
  markets,
  open,
}: {
  detail: UniverseEventDetail;
  markets: UniverseSelectedMarket[];
  open: () => void;
}) {
  const event = detail.event;
  const retained = markets.every(
    (market) => market.selection_reason === 'retained',
  );
  return (
    <button className="bundle-row" onClick={open}>
      <span className="event-cell">
        <EventIcon game={event.game} sport={event.sport} />
        <span>
          <b>{event.participants.join(' vs ') || event.event_id}</b>
          <small>
            {label(event.topology)} ·{' '}
            {retained ? 'Retained' : 'Current candidate'}
          </small>
        </span>
      </span>
      <VenueStack venues={markets.map((market) => market.venue)} />
      <span className="date-cell">{date(event.activation_at)}</span>
      <strong className="target-count">{markets.length}</strong>
      <Chevron />
    </button>
  );
}

function NormalizedTargetDrawer({
  detail,
  markets,
  close,
}: {
  detail: UniverseEventDetail | null;
  markets: UniverseSelectedMarket[];
  close: () => void;
}) {
  useEffect(() => {
    if (!detail) return;
    const keydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') close();
    };
    window.addEventListener('keydown', keydown);
    return () => window.removeEventListener('keydown', keydown);
  }, [detail, close]);
  if (!detail) return null;
  return (
    <div className="drawer-layer">
      <button
        className="drawer-backdrop"
        onClick={close}
        aria-label="Close event detail"
      />
      <div
        className="bundle-drawer"
        role="dialog"
        aria-modal="true"
        aria-label="Normalized event detail"
      >
        <div className="drawer-header">
          <div>
            <span className="eyebrow">NORMALIZED EVENT</span>
            <h2>{detail.event.participants.join(' vs ')}</h2>
            <code>{detail.event.event_id}</code>
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
            <b>{markets.length}</b> selected markets
          </span>
          <span>
            <b>{new Set(markets.map((market) => market.venue)).size}</b> venues
          </span>
          <span>
            <b>{detail.relations.length}</b> relations
          </span>
        </div>
        <div className="drawer-body market-list">
          {markets.map((market) => (
            <div
              className="market-row"
              key={`${market.venue}:${market.venue_market_id}`}
            >
              <VenueStack venues={[market.venue]} />
              <span>
                <b>{label(market.canonical_class)}</b>
                <code>{market.market_id}</code>
              </span>
              <em>{label(market.selection_reason)}</em>
            </div>
          ))}
          {detail.relations.map((relation) => (
            <div className="proof-card" key={relation.relation_id}>
              <b>
                {label(relation.relation_type)} · {label(relation.coverage)}
              </b>
              <code>Relation {relation.relation_id}</code>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function BundleDrawer({
  detail,
  history = [],
  close,
}: {
  detail: UniverseSelectionDetail | null;
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
  const normalized = useEventDetails(
    run?.decisions.map((decision) => decision.event_id) ?? [],
  );
  const [query, setQuery] = useState('');
  if (loaded.loading || !run)
    return (
      <EmptyPage
        error={error || loaded.error}
        loading="Loading Targeter decisions…"
      />
    );
  const candidates = run.decisions.filter((candidate) => {
    const participants =
      normalized.events[candidate.event_id]?.event.participants.join(' ') ?? '';
    return `${candidate.bundle_id} ${participants} ${candidate.rejection_reasons.join(' ')}`
      .toLowerCase()
      .includes(query.toLowerCase());
  });
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
        <FunnelStep
          count={run.counts.selected_events}
          label="Selected events"
          accent
        />
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
          <CandidateRow
            key={candidate.bundle_id}
            candidate={candidate}
            detail={normalized.events[candidate.event_id]}
          />
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

function CandidateRow({
  candidate,
  detail,
}: {
  candidate: UniverseTargeterDecision;
  detail?: UniverseEventDetail;
}) {
  const state = candidate.selected
    ? 'Selected'
    : candidate.eligible
      ? 'Not allocated'
      : 'Rejected';
  return (
    <details className="candidate-row">
      <summary>
        <EventIcon
          game={detail?.event.game ?? null}
          sport={detail?.event.sport ?? 'event'}
        />
        <span>
          <b>
            {detail?.event.participants.join(' vs ') || candidate.bundle_id}
          </b>
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
