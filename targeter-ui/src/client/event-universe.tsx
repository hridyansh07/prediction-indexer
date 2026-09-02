import React, { useEffect, useMemo, useRef, useState } from 'react';
import type {
  UniverseBundle,
  UniverseBundlePage,
  UniverseSelection,
  UniverseSelectionDetail,
  UniverseSelectionPage,
} from '../event-universe';
import { EventIcon, gameName, SearchIcon, VenueStack, Chevron } from './icons';
import { BundleDrawer, MobileDetailNotice } from './observability';
import { universeGet } from './universe-api';

const CURSOR_HISTORY_LIMIT = 8;

async function loadBundles(cursor: string | null) {
  const query = new URLSearchParams({ limit: '100' });
  if (cursor) query.set('cursor', cursor);
  return universeGet<UniverseBundlePage>(`/v1/bundles?${query}`);
}

const displayDate = (value: string) =>
  new Date(value).toLocaleDateString(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });

export function EventUniversePage() {
  const [bundles, setBundles] = useState<UniverseBundle[]>([]);
  const [query, setQuery] = useState('');
  const [lifecycle, setLifecycle] = useState<'all' | 'active' | 'retired'>(
    'all',
  );
  const [event, setEvent] = useState('');
  const [detail, setDetail] = useState<UniverseSelectionDetail | null>(null);
  const [history, setHistory] = useState<UniverseSelection[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [previousCursors, setPreviousCursors] = useState<Array<string | null>>(
    [],
  );
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [pageNumber, setPageNumber] = useState(1);
  const detailRequest = useRef<AbortController | null>(null);
  const detailRequestId = useRef(0);
  const opener = useRef<HTMLElement | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError('');
    void loadBundles(cursor)
      .then((page) => {
        if (!active) return;
        setBundles(page.bundles);
        setNextCursor(page.next_cursor);
      })
      .catch(() => setError('Historical bundle summaries are unavailable.'))
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [cursor]);

  useEffect(
    () => () => {
      detailRequest.current?.abort();
      detailRequest.current = null;
    },
    [],
  );

  const games = useMemo(() => {
    const values = new Map<string, { game: string | null; sport: string }>();
    for (const bundle of bundles)
      values.set(bundle.game ?? bundle.sport, {
        game: bundle.game,
        sport: bundle.sport,
      });
    return [...values.values()];
  }, [bundles]);
  const shown = bundles.filter(
    (bundle) =>
      `${bundle.bundle_id} ${bundle.participants.join(' ')}`
        .toLowerCase()
        .includes(query.toLowerCase()) &&
      (lifecycle === 'all' || lifecycle === bundle.lifecycle) &&
      (!event || event === (bundle.game ?? bundle.sport)),
  );

  const open = async (bundle: UniverseBundle) => {
    detailRequest.current?.abort();
    const controller = new AbortController();
    detailRequest.current = controller;
    const requestId = ++detailRequestId.current;
    setError('');
    setDetail(null);
    setHistory([]);
    try {
      const bundleId = encodeURIComponent(bundle.bundle_id);
      const [nextDetail, historyPage] = await Promise.all([
        universeGet<UniverseSelectionDetail>(
          `/v1/runs/${encodeURIComponent(bundle.latest_run_id)}/selections/${bundleId}`,
          { signal: controller.signal, cache: false },
        ),
        universeGet<UniverseSelectionPage>(
          `/v1/bundles/${bundleId}/history?sort=selected&limit=100`,
          { signal: controller.signal, cache: false },
        ),
      ]);
      if (requestId !== detailRequestId.current) return;
      setDetail(nextDetail);
      setHistory(historyPage.selections);
    } catch {
      if (!controller.signal.aborted && requestId === detailRequestId.current)
        setError('Bundle detail is unavailable.');
    }
  };

  const closeDetail = () => {
    detailRequest.current?.abort();
    detailRequest.current = null;
    ++detailRequestId.current;
    setDetail(null);
  };

  return (
    <div className="desktop-page history-page">
      <MobileDetailNotice />
      <div className="page-heading">
        <span className="eyebrow">BUNDLE HISTORY</span>
        <h1>One event, one row.</h1>
        <p>
          Browse bundles Targeter has selected without repeating every retained
          occurrence.
        </p>
      </div>
      <div className="compact-toolbar history-toolbar">
        <label className="search-field">
          <SearchIcon />
          <span className="sr-only">Search history</span>
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search this page"
          />
        </label>
        <div className="segmented" role="group" aria-label="Lifecycle filter">
          {(['all', 'active', 'retired'] as const).map((value) => (
            <button
              key={value}
              className={lifecycle === value ? 'active' : ''}
              onClick={() => setLifecycle(value)}
            >
              {value[0].toUpperCase() + value.slice(1)}
            </button>
          ))}
        </div>
        <div
          className="event-filters"
          role="group"
          aria-label="Filter by event type"
        >
          <button
            className={!event ? 'active' : ''}
            onClick={() => setEvent('')}
          >
            All
          </button>
          {games.map(({ game, sport }) => (
            <button
              key={game ?? sport}
              className={event === (game ?? sport) ? 'active' : ''}
              onClick={() => setEvent(game ?? sport)}
              aria-label={`Show ${gameName(game, sport)}`}
            >
              <EventIcon game={game} sport={sport} labelled />
            </button>
          ))}
        </div>
      </div>
      {error && (
        <div className="error-state" role="alert">
          {error}
        </div>
      )}
      {loading ? (
        <div className="empty-state">Loading bundle history…</div>
      ) : (
        <div className="bundle-table history-table">
          <div className="bundle-table-head">
            <span>Event</span>
            <span>Venues</span>
            <span>Last selected</span>
            <span>Selections</span>
            <span />
          </div>
          {shown.map((bundle) => (
            <button
              className="bundle-row"
              key={bundle.bundle_id}
              onClick={(event) => {
                opener.current = event.currentTarget;
                void open(bundle);
              }}
            >
              <span className="event-cell">
                <EventIcon game={bundle.game} sport={bundle.sport} />
                <span>
                  <b>{bundle.participants.join(' vs ') || bundle.bundle_id}</b>
                  <small>
                    {bundle.lifecycle === 'active' ? 'Active' : 'Retired'} ·{' '}
                    {bundle.target_count} targets
                  </small>
                </span>
              </span>
              <VenueStack venues={bundle.venues} />
              <span className="date-cell">
                {displayDate(bundle.last_selected_at)}
              </span>
              <strong className="target-count">
                {bundle.occurrence_count}
              </strong>
              <Chevron />
            </button>
          ))}
        </div>
      )}
      {!loading && !shown.length && (
        <div className="empty-state">
          No historical bundles match these controls.
        </div>
      )}
      {!loading && (previousCursors.length > 0 || nextCursor) && (
        <nav className="result-pagination" aria-label="Bundle pages">
          <button
            className="quiet-button"
            disabled={!previousCursors.length}
            onClick={() => {
              const previous = previousCursors.at(-1) ?? null;
              setPreviousCursors((values) => values.slice(0, -1));
              setCursor(previous);
              setPageNumber((value) => Math.max(1, value - 1));
            }}
          >
            Previous
          </button>
          <span aria-live="polite">Page {pageNumber}</span>
          <button
            className="quiet-button"
            disabled={!nextCursor}
            onClick={() => {
              if (!nextCursor) return;
              setPreviousCursors((values) =>
                [...values, cursor].slice(-CURSOR_HISTORY_LIMIT),
              );
              setCursor(nextCursor);
              setPageNumber((value) => value + 1);
            }}
          >
            Next
          </button>
        </nav>
      )}
      <BundleDrawer
        detail={detail}
        history={history}
        close={closeDetail}
        opener={opener}
      />
    </div>
  );
}
