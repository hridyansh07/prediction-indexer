import React, { useEffect, useMemo, useRef, useState } from 'react';
import type {
  UniverseEvent,
  UniverseEventDetail,
  UniverseSelectedMarket,
  UniverseSelection,
  UniverseSelectionDetail,
  UniverseTargeterDecision,
} from '../event-universe';
import { Chevron, EventIcon, gameName, SearchIcon, VenueStack } from './icons';
import { boundedRenderPage } from './event-universe-view-model';
import {
  useEventDetail,
  useTargeterRun,
  useTargeterStatus,
} from './universe-queries';

const date = (value: string | null | undefined) =>
  value ? new Date(value).toLocaleString() : 'Unavailable';

const label = (value: string | null | undefined) =>
  value
    ?.replaceAll('_', ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase()) ?? '—';

function useDrawerFocus(
  open: boolean,
  close: () => void,
  opener: React.MutableRefObject<HTMLElement | null>,
) {
  const dialog = useRef<HTMLDivElement>(null);
  const closeRef = useRef(close);
  closeRef.current = close;
  useEffect(() => {
    if (!open) return;
    const element = dialog.current;
    if (!element) return;
    element.focus();
    const focusables = () =>
      [
        ...element.querySelectorAll<HTMLElement>(
          'button, [href], input, [tabindex]:not([tabindex="-1"])',
        ),
      ].filter((item) => !item.hasAttribute('disabled'));
    const keydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closeRef.current();
        return;
      }
      if (event.key !== 'Tab') return;
      const items = focusables();
      if (!items.length) return;
      const first = items[0];
      const last = items[items.length - 1];
      if (document.activeElement === element) {
        event.preventDefault();
        (event.shiftKey ? last : first).focus();
        return;
      }
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    element.addEventListener('keydown', keydown);
    return () => {
      element.removeEventListener('keydown', keydown);
      opener.current?.focus();
      opener.current = null;
    };
  }, [open, opener]);
  return dialog;
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
      <p>The Event Universe explorer is currently desktop-first.</p>
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

function ResultPagination({
  currentPage,
  pageCount,
  setPage,
}: {
  currentPage: number;
  pageCount: number;
  setPage: (page: number) => void;
}) {
  if (pageCount <= 1) return null;
  return (
    <nav className="result-pagination" aria-label="Result pages">
      <button
        className="quiet-button"
        disabled={currentPage === 0}
        onClick={() => setPage(currentPage - 1)}
      >
        Previous
      </button>
      <span aria-live="polite">
        Page {currentPage + 1} of {pageCount}
      </span>
      <button
        className="quiet-button"
        disabled={currentPage + 1 === pageCount}
        onClick={() => setPage(currentPage + 1)}
      >
        Next
      </button>
    </nav>
  );
}

interface TargetEventGroup {
  event: UniverseEvent;
  markets: UniverseSelectedMarket[];
}

export function TargetsPage() {
  const statusQuery = useTargeterStatus();
  const status = statusQuery.data;
  const runId = status?.current_complete_run?.run_id ?? null;
  const loaded = useTargeterRun(runId);
  const run = loaded.data;
  const [query, setQuery] = useState('');
  const [lifecycle, setLifecycle] = useState<'all' | 'current' | 'retained'>(
    'all',
  );
  const [event, setEvent] = useState('');
  const [page, setPage] = useState(0);
  const [detailId, setDetailId] = useState<string | null>(null);
  const opener = useRef<HTMLElement | null>(null);
  const selectedDetail = useEventDetail(detailId);
  const grouped = useMemo(() => {
    const groups = new Map<string, UniverseSelectedMarket[]>();
    const events = new Map(
      (run?.events ?? []).map((event) => [event.event_id, event]),
    );
    for (const market of run?.selected_markets ?? [])
      groups.set(market.event_id, [
        ...(groups.get(market.event_id) ?? []),
        market,
      ]);
    return [...groups]
      .map(([eventId, markets]) => ({
        event: events.get(eventId),
        markets,
      }))
      .filter((group): group is TargetEventGroup => group.event !== undefined);
  }, [run]);
  const games = useMemo(() => {
    const values = new Map<string, { game: string | null; sport: string }>();
    for (const { event } of grouped)
      if (event)
        values.set(event.game ?? event.sport, {
          game: event.game,
          sport: event.sport,
        });
    return [...values.values()];
  }, [grouped]);
  const shown = grouped.filter(({ event: record, markets }) => {
    if (!record) return false;
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
  const rendered = boundedRenderPage(shown, page);
  useEffect(() => setPage(0), [query, lifecycle, event, runId]);
  if (statusQuery.isPending || loaded.isPending || !run)
    return (
      <EmptyPage
        error={
          statusQuery.isError
            ? 'Targeter status is unavailable.'
            : loaded.isError
              ? 'Targeter run diagnostics are unavailable.'
              : ''
        }
        loading="Loading the current complete target set…"
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
        {rendered.items.map(({ event, markets }) => (
          <NormalizedTargetRow
            key={event.event_id}
            event={event}
            markets={markets}
            open={() => {
              opener.current = document.activeElement as HTMLElement | null;
              setDetailId(event.event_id);
            }}
          />
        ))}
      </div>
      <ResultPagination
        currentPage={rendered.currentPage}
        pageCount={rendered.pageCount}
        setPage={setPage}
      />
      {!shown.length && (
        <div className="empty-state">
          No current targets match these controls.
        </div>
      )}
      {selectedDetail.isError && (
        <div className="error-state" role="alert">
          Normalized event detail is unavailable.
        </div>
      )}
      {detailId && selectedDetail.isPending && (
        <div className="empty-state" role="status">
          Loading event detail…
        </div>
      )}
      <NormalizedTargetDrawer
        detail={selectedDetail.data ?? null}
        markets={
          detailId
            ? (grouped.find(({ event }) => event?.event_id === detailId)
                ?.markets ?? [])
            : []
        }
        close={() => setDetailId(null)}
        opener={opener}
      />
    </div>
  );
}

function NormalizedTargetRow({
  event,
  markets,
  open,
}: {
  event: UniverseEvent;
  markets: UniverseSelectedMarket[];
  open: () => void;
}) {
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
  opener,
}: {
  detail: UniverseEventDetail | null;
  markets: UniverseSelectedMarket[];
  close: () => void;
  opener: React.MutableRefObject<HTMLElement | null>;
}) {
  const dialog = useDrawerFocus(Boolean(detail), close, opener);
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
        tabIndex={-1}
        ref={dialog}
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
  opener,
}: {
  detail: UniverseSelectionDetail | null;
  history?: UniverseSelection[];
  close: () => void;
  opener: React.MutableRefObject<HTMLElement | null>;
}) {
  const [tab, setTab] = useState<'markets' | 'relationships' | 'evidence'>(
    'markets',
  );
  const dialog = useDrawerFocus(Boolean(detail), close, opener);
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
        tabIndex={-1}
        ref={dialog}
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

export function DecisionsPage() {
  const statusQuery = useTargeterStatus();
  const status = statusQuery.data;
  const runId = status?.current_complete_run?.run_id ?? null;
  const loaded = useTargeterRun(runId);
  const run = loaded.data;
  const [query, setQuery] = useState('');
  const [page, setPage] = useState(0);
  useEffect(() => setPage(0), [query, runId]);
  if (statusQuery.isPending || loaded.isPending || !run)
    return (
      <EmptyPage
        error={
          statusQuery.isError
            ? 'Targeter status is unavailable.'
            : loaded.isError
              ? 'Targeter run diagnostics are unavailable.'
              : ''
        }
        loading="Loading Targeter decisions…"
      />
    );
  const events = new Map(run.events.map((event) => [event.event_id, event]));
  const candidates = run.decisions.filter((candidate) => {
    const participants =
      events.get(candidate.event_id)?.participants.join(' ') ?? '';
    return `${candidate.bundle_id} ${participants} ${candidate.rejection_reasons.join(' ')}`
      .toLowerCase()
      .includes(query.toLowerCase());
  });
  const rendered = boundedRenderPage(candidates, page);
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
        {rendered.items.map((candidate) => (
          <CandidateRow
            key={candidate.bundle_id}
            candidate={candidate}
            event={events.get(candidate.event_id)}
          />
        ))}
      </section>
      <ResultPagination
        currentPage={rendered.currentPage}
        pageCount={rendered.pageCount}
        setPage={setPage}
      />
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
  event,
}: {
  candidate: UniverseTargeterDecision;
  event?: UniverseEvent;
}) {
  const state = candidate.selected
    ? 'Selected'
    : candidate.eligible
      ? 'Not allocated'
      : 'Rejected';
  return (
    <details className="candidate-row">
      <summary>
        <EventIcon game={event?.game ?? null} sport={event?.sport ?? 'event'} />
        <span>
          <b>{event?.participants.join(' vs ') || candidate.bundle_id}</b>
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
