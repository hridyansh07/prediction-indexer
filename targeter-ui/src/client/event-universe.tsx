import React, { useMemo, useRef, useState } from 'react';
import type { UniverseBundle } from '../event-universe';
import { EventIcon, gameName, SearchIcon, VenueStack, Chevron } from './icons';
import { BundleDrawer, MobileDetailNotice } from './observability';
import {
  useBundleHistory,
  useBundles,
  useSelectionDetail,
} from './universe-queries';

const EMPTY_BUNDLES: UniverseBundle[] = [];

const displayDate = (value: string) =>
  new Date(value).toLocaleDateString(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });

export function EventUniversePage() {
  const bundlePages = useBundles();
  const [pageIndex, setPageIndex] = useState(0);
  const pages = bundlePages.data?.pages ?? [];
  const bundles = pages[pageIndex]?.bundles ?? EMPTY_BUNDLES;
  const [query, setQuery] = useState('');
  const [lifecycle, setLifecycle] = useState<'all' | 'active' | 'retired'>(
    'all',
  );
  const [event, setEvent] = useState('');
  const [selectedBundle, setSelectedBundle] = useState<UniverseBundle | null>(
    null,
  );
  const [pageNumber, setPageNumber] = useState(1);
  const opener = useRef<HTMLElement | null>(null);
  const detail = useSelectionDetail(
    selectedBundle?.latest_run_id ?? null,
    selectedBundle?.bundle_id ?? null,
  );
  const history = useBundleHistory(selectedBundle?.bundle_id ?? null);
  const loading = bundlePages.isPending;
  const listError =
    bundlePages.isError || bundlePages.isFetchNextPageError
      ? 'Historical bundle summaries are unavailable.'
      : '';
  const detailError = detail.isError || history.isError;

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

  const closeDetail = () => setSelectedBundle(null);

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
      {listError && (
        <div className="error-state" role="alert">
          {listError}
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
                setSelectedBundle(bundle);
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
      {!loading &&
        (pageIndex > 0 ||
          pageIndex + 1 < pages.length ||
          Boolean(pages.at(-1)?.next_cursor)) && (
          <nav className="result-pagination" aria-label="Bundle pages">
            <button
              className="quiet-button"
              disabled={pageIndex === 0}
              onClick={() => {
                setPageIndex((value) => Math.max(0, value - 1));
                setPageNumber((value) => Math.max(1, value - 1));
              }}
            >
              Previous
            </button>
            <span aria-live="polite">Page {pageNumber}</span>
            <button
              className="quiet-button"
              disabled={
                bundlePages.isFetchingNextPage ||
                (pageIndex + 1 === pages.length && !bundlePages.hasNextPage)
              }
              onClick={async () => {
                if (pageIndex + 1 < pages.length) {
                  setPageIndex((value) => value + 1);
                  setPageNumber((value) => value + 1);
                  return;
                }
                const result = await bundlePages.fetchNextPage();
                if (!result.data || result.isError) return;
                setPageIndex((value) =>
                  Math.min(value + 1, result.data.pages.length - 1),
                );
                setPageNumber((value) => value + 1);
              }}
            >
              {bundlePages.isFetchingNextPage ? 'Loading…' : 'Next'}
            </button>
          </nav>
        )}
      {detailError && (
        <div className="error-state" role="alert">
          Bundle detail is unavailable.
        </div>
      )}
      {selectedBundle && (detail.isPending || history.isPending) && (
        <div className="empty-state" role="status">
          Loading bundle detail…
        </div>
      )}
      <BundleDrawer
        detail={detail.data && history.data ? detail.data : null}
        history={history.data?.selections ?? []}
        close={closeDetail}
        opener={opener}
      />
    </div>
  );
}
