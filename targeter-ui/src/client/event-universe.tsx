import React, { useEffect, useMemo, useState } from 'react';
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

async function loadAllBundles() {
  const bundles: UniverseBundle[] = [];
  let cursor: string | null = null;
  do {
    const query = new URLSearchParams({ limit: '100' });
    if (cursor) query.set('cursor', cursor);
    const page = await universeGet<UniverseBundlePage>(`/v1/bundles?${query}`);
    bundles.push(...page.bundles);
    cursor = page.next_cursor;
  } while (cursor);
  return bundles;
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
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    void loadAllBundles()
      .then(setBundles)
      .catch(() => setError('Historical bundle summaries are unavailable.'))
      .finally(() => setLoading(false));
  }, []);

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
    setError('');
    setDetail(null);
    setHistory([]);
    try {
      const bundleId = encodeURIComponent(bundle.bundle_id);
      const [nextDetail, historyPage] = await Promise.all([
        universeGet<UniverseSelectionDetail>(
          `/v1/runs/${encodeURIComponent(bundle.latest_run_id)}/selections/${bundleId}`,
        ),
        universeGet<UniverseSelectionPage>(
          `/v1/bundles/${bundleId}/history?sort=selected&limit=100`,
        ),
      ]);
      setDetail(nextDetail);
      setHistory(historyPage.selections);
    } catch {
      setError('Bundle detail is unavailable.');
    }
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
            placeholder="Search event or bundle"
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
              onClick={() => void open(bundle)}
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
      <BundleDrawer
        detail={detail}
        history={history}
        close={() => setDetail(null)}
      />
    </div>
  );
}
