import React, { useCallback, useEffect, useState } from 'react';
import type {
  UniverseFilters,
  UniverseHealth,
  UniverseRun,
  UniverseRunDetail,
  UniverseRunPage,
  UniverseSelection,
  UniverseSelectionDetail,
  UniverseSelectionPage,
} from '../event-universe';
import {
  occurrenceExplanation,
  retirementExplanation,
  universeFilterQuery,
  universeLabel,
} from './event-universe-view-model';

const ROOT = '/api/event-universe';
const initialFilters: UniverseFilters = { sort: 'activation', limit: '25' };

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${ROOT}${path}`, {
    headers: { accept: 'application/json' },
  });
  if (!response.ok)
    throw new Error('Historical Event Universe is unavailable.');
  return response.json() as Promise<T>;
}

export function EventUniversePage() {
  const [health, setHealth] = useState<UniverseHealth | null>(null);
  const [runs, setRuns] = useState<UniverseRun[]>([]);
  const [runNextCursor, setRunNextCursor] = useState<string | null>(null);
  const [runCursor, setRunCursor] = useState<string | undefined>();
  const [runPrevious, setRunPrevious] = useState<Array<string | undefined>>([]);
  const [runDetail, setRunDetail] = useState<UniverseRunDetail | null>(null);
  const [selectedRun, setSelectedRun] = useState('');
  const [draft, setDraft] = useState<UniverseFilters>(initialFilters);
  const [filters, setFilters] = useState<UniverseFilters>(initialFilters);
  const [page, setPage] = useState<UniverseSelectionPage | null>(null);
  const [cursor, setCursor] = useState<string | undefined>();
  const [previous, setPrevious] = useState<Array<string | undefined>>([]);
  const [detail, setDetail] = useState<UniverseSelectionDetail | null>(null);
  const [history, setHistory] = useState<UniverseSelection[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [detailError, setDetailError] = useState('');

  const loadSelections = useCallback(
    async (
      activeFilters: UniverseFilters,
      runId: string,
      activeCursor?: string,
    ) => {
      setLoading(true);
      setError('');
      try {
        const query = universeFilterQuery(activeFilters, activeCursor);
        const root = runId
          ? `/v1/runs/${encodeURIComponent(runId)}/selections`
          : '/v1/selections';
        setPage(await get<UniverseSelectionPage>(`${root}?${query}`));
      } catch (reason) {
        setError(
          reason instanceof Error
            ? reason.message
            : 'Unable to load Event Universe.',
        );
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  useEffect(() => {
    void Promise.all([
      get<UniverseHealth>('/healthz'),
      get<UniverseRunPage>('/v1/runs?limit=20'),
    ])
      .then(([nextHealth, runPage]) => {
        setHealth(nextHealth);
        setRuns(runPage.runs);
        setRunNextCursor(runPage.next_cursor);
      })
      .catch((reason) =>
        setError(
          reason instanceof Error
            ? reason.message
            : 'Unable to load Event Universe.',
        ),
      );
    void loadSelections(initialFilters, '');
  }, [loadSelections]);

  const loadRuns = async (targetCursor?: string) => {
    try {
      const query = new URLSearchParams({ limit: '20' });
      if (targetCursor) query.set('cursor', targetCursor);
      const runPage = await get<UniverseRunPage>(`/v1/runs?${query}`);
      setRuns(runPage.runs);
      setRunNextCursor(runPage.next_cursor);
      setRunCursor(targetCursor);
    } catch {
      setError('Run navigation is unavailable.');
    }
  };

  const chooseRun = async (runId: string) => {
    setSelectedRun(runId);
    setCursor(undefined);
    setPrevious([]);
    setDetail(null);
    setRunDetail(null);
    if (runId) {
      try {
        setRunDetail(
          await get<UniverseRunDetail>(`/v1/runs/${encodeURIComponent(runId)}`),
        );
      } catch {
        setError('Run provenance is unavailable.');
      }
    }
    await loadSelections(filters, runId);
  };

  const openDetail = async (selection: UniverseSelection) => {
    setDetail(null);
    setHistory([]);
    setDetailError('');
    try {
      const runId = encodeURIComponent(selection.run_id);
      const bundleId = encodeURIComponent(selection.bundle_id);
      const [nextDetail, nextHistory] = await Promise.all([
        get<UniverseSelectionDetail>(
          `/v1/runs/${runId}/selections/${bundleId}`,
        ),
        get<UniverseSelectionPage>(
          `/v1/bundles/${bundleId}/history?sort=selected&limit=20`,
        ),
      ]);
      setDetail(nextDetail);
      setHistory(nextHistory.selections);
    } catch {
      setDetailError('Bundle detail or history is unavailable.');
    }
  };

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    setFilters(draft);
    setCursor(undefined);
    setPrevious([]);
    setDetail(null);
    void loadSelections(draft, selectedRun);
  };
  const next = () => {
    if (!page?.next_cursor) return;
    setPrevious((values) => [...values, cursor]);
    setCursor(page.next_cursor);
    void loadSelections(filters, selectedRun, page.next_cursor);
  };
  const back = () => {
    const target = previous.at(-1);
    setPrevious((values) => values.slice(0, -1));
    setCursor(target);
    void loadSelections(filters, selectedRun, target);
  };

  return (
    <div className="universe stack">
      <section className="universe-hero">
        <div>
          <span className="eyebrow">HISTORICAL SELECTED EVENTS</span>
          <h2>Event Universe</h2>
          <p>
            Navigate indexed Targeter selections and provenance. Recent
            operational decisions remain on the Overview page.
          </p>
        </div>
        <UniverseHealthSummary health={health} />
      </section>

      <section>
        <div className="title">
          <h2>Run navigation</h2>
          <span>
            Ascending generated time · choose one or search across history
          </span>
        </div>
        <div className="universe-runs">
          <button
            className={!selectedRun ? 'selected' : ''}
            onClick={() => void chooseRun('')}
          >
            <b>ALL HISTORY</b>
            <small>Cross-run selection index</small>
          </button>
          {runs.map((run) => (
            <button
              key={run.run_id}
              className={selectedRun === run.run_id ? 'selected' : ''}
              onClick={() => void chooseRun(run.run_id)}
            >
              <b>{run.run_id}</b>
              <small>{new Date(run.generated_at).toLocaleString()}</small>
              <span className={run.input_complete ? 'ok' : 'warn'}>
                {run.input_complete ? 'Complete input' : 'Incomplete input'} ·{' '}
                {run.projection_row_count} projected
              </span>
            </button>
          ))}
        </div>
        <div className="run-pagination">
          <button
            disabled={!runPrevious.length}
            onClick={() => {
              const target = runPrevious.at(-1);
              setRunPrevious((values) => values.slice(0, -1));
              void loadRuns(target);
            }}
          >
            ← Earlier runs
          </button>
          <span>Run page {runPrevious.length + 1}</span>
          <button
            disabled={!runNextCursor}
            onClick={() => {
              if (!runNextCursor) return;
              setRunPrevious((values) => [...values, runCursor]);
              void loadRuns(runNextCursor);
            }}
          >
            Later runs →
          </button>
        </div>
        {runDetail && (
          <div className="run-proof">
            <b>
              Run projection {runDetail.audit.ok ? 'verified' : 'audit failed'}
            </b>
            <span>Strategy v{runDetail.strategy_version}</span>
            <code>{runDetail.manifest_key}</code>
            <span>
              {runDetail.audit.selection_row_count} selections ·{' '}
              {runDetail.audit.retirement_row_count} retirements
            </span>
          </div>
        )}
      </section>

      <section>
        <div className="title">
          <h2>Selected bundle history</h2>
          <span>Half-open activation and selection-time filters</span>
        </div>
        <UniverseFilterForm draft={draft} setDraft={setDraft} submit={submit} />
        {error && (
          <div className="universe-error" role="alert">
            {error}
          </div>
        )}
        {loading ? (
          <div className="loading">Loading historical selections…</div>
        ) : page?.selections.length ? (
          <div className="universe-layout">
            <div className="universe-results">
              {page.selections.map((selection) => (
                <SelectionCard
                  key={`${selection.run_id}-${selection.bundle_id}`}
                  selection={selection}
                  venueFilter={filters.venue}
                  selected={
                    detail?.run_id === selection.run_id &&
                    detail.bundle_id === selection.bundle_id
                  }
                  open={() => void openDetail(selection)}
                />
              ))}
              <div className="pagination">
                <button disabled={!previous.length} onClick={back}>
                  ← Previous
                </button>
                <span>Cursor page {previous.length + 1}</span>
                <button disabled={!page.next_cursor} onClick={next}>
                  Next →
                </button>
              </div>
            </div>
            <SelectionDetailPanel
              detail={detail}
              history={history}
              error={detailError}
              openHistory={(selection) => void openDetail(selection)}
            />
          </div>
        ) : (
          <div className="empty">
            No selected occurrences match these filters. Empty complete runs
            remain visible in run navigation.
          </div>
        )}
      </section>
    </div>
  );
}

function UniverseHealthSummary({ health }: { health: UniverseHealth | null }) {
  if (!health)
    return (
      <div className="universe-health muted">Universe status unavailable</div>
    );
  const latest = health.latest_run;
  return (
    <div className="universe-health">
      <span className={latest && !latest.stale ? 'ok' : 'warn'}>
        <i className="dot" /> {latest && !latest.stale ? 'FRESH' : 'STALE'}
      </span>
      <b>{health.counts.selection_occurrences.toLocaleString()} selections</b>
      <span>{health.counts.targeter_runs.toLocaleString()} indexed runs</span>
      <small>
        {latest
          ? `Latest ${new Date(latest.generated_at).toLocaleString()} · ${Math.round(latest.age_seconds / 60)}m old`
          : 'No indexed runs'}
      </small>
    </div>
  );
}

function UniverseFilterForm({
  draft,
  setDraft,
  submit,
}: {
  draft: UniverseFilters;
  setDraft: React.Dispatch<React.SetStateAction<UniverseFilters>>;
  submit: (event: React.FormEvent) => void;
}) {
  const set = (key: keyof UniverseFilters, value: string) =>
    setDraft((current) => ({ ...current, [key]: value }));
  return (
    <form className="universe-filters" onSubmit={submit}>
      <label>
        Activation from
        <input
          type="datetime-local"
          value={draft.activation_start ?? ''}
          onChange={(e) => set('activation_start', e.target.value)}
        />
      </label>
      <label>
        Activation until
        <input
          type="datetime-local"
          value={draft.activation_end ?? ''}
          onChange={(e) => set('activation_end', e.target.value)}
        />
      </label>
      <label>
        Selected from
        <input
          type="datetime-local"
          value={draft.selected_start ?? ''}
          onChange={(e) => set('selected_start', e.target.value)}
        />
      </label>
      <label>
        Selected until
        <input
          type="datetime-local"
          value={draft.selected_end ?? ''}
          onChange={(e) => set('selected_end', e.target.value)}
        />
      </label>
      <label>
        Venue
        <input
          value={draft.venue ?? ''}
          placeholder="kalshi, polymarket…"
          onChange={(e) => set('venue', e.target.value.trim())}
        />
      </label>
      <label>
        Sort
        <select
          value={draft.sort}
          onChange={(e) => set('sort', e.target.value)}
        >
          <option value="activation">Activation time</option>
          <option value="selected">Selection time</option>
        </select>
      </label>
      <label>
        Page size
        <select
          value={draft.limit}
          onChange={(e) => set('limit', e.target.value)}
        >
          <option>10</option>
          <option>25</option>
          <option>50</option>
          <option>100</option>
        </select>
      </label>
      <button type="submit">Apply filters</button>
    </form>
  );
}

function SelectionCard({
  selection,
  venueFilter,
  selected,
  open,
}: {
  selection: UniverseSelection;
  venueFilter?: string;
  selected: boolean;
  open: () => void;
}) {
  return (
    <button
      className={`universe-card ${selected ? 'selected' : ''}`}
      onClick={open}
    >
      <div className="row">
        <span
          className={`decision ${selection.occurrence_kind === 'retained' ? 'warn' : 'ok'}`}
        >
          {selection.occurrence_kind === 'retained' ? 'RETAINED' : 'COMPLETE'}
        </span>
        <span>{universeLabel(selection.continuity_disposition)}</span>
      </div>
      <h3>
        {selection.game
          ? universeLabel(selection.game)
          : universeLabel(selection.sport)}
      </h3>
      <code>{selection.bundle_id}</code>
      <dl>
        <dt>Sport / topology</dt>
        <dd>
          {universeLabel(selection.sport)} · {universeLabel(selection.topology)}
        </dd>
        <dt>Activation</dt>
        <dd>{new Date(selection.activation_at).toLocaleString()}</dd>
        <dt>Capture starts</dt>
        <dd>{new Date(selection.capture_start_at).toLocaleString()}</dd>
        <dt>Selected</dt>
        <dd>{new Date(selection.generated_at).toLocaleString()}</dd>
        <dt>Venue</dt>
        <dd>
          {venueFilter
            ? `Includes ${venueFilter}`
            : 'Open detail for selected venues'}
        </dd>
        <dt>Retirement</dt>
        <dd>
          {selection.retirement
            ? universeLabel(selection.retirement.disposition)
            : 'Not observed'}
        </dd>
      </dl>
      <small>{occurrenceExplanation(selection)}</small>
    </button>
  );
}

function SelectionDetailPanel({
  detail,
  history,
  error,
  openHistory,
}: {
  detail: UniverseSelectionDetail | null;
  history: UniverseSelection[];
  error: string;
  openHistory: (selection: UniverseSelection) => void;
}) {
  if (error)
    return <aside className="universe-detail universe-error">{error}</aside>;
  if (!detail)
    return (
      <aside className="universe-detail empty">
        Choose a bundle to inspect its immutable context and history.
      </aside>
    );
  const context = detail.context;
  return (
    <aside className="universe-detail">
      <span className="eyebrow">BUNDLE DETAIL</span>
      <h2>{context.participants.join(' vs ')}</h2>
      <code>{detail.bundle_id}</code>
      <p className="retirement-copy">{retirementExplanation(detail)}</p>
      <DetailGroup title="Participants & keys">
        {context.participants.map((participant, index) => (
          <div className="proof-row" key={context.participant_keys[index]}>
            <b>{participant}</b>
            <code>{context.participant_keys[index]}</code>
          </div>
        ))}
      </DetailGroup>
      <DetailGroup title="Event references">
        {context.event_refs.map((reference) => (
          <code className="block" key={reference}>
            {reference}
          </code>
        ))}
      </DetailGroup>
      <DetailGroup title={`Markets (${context.markets.length})`}>
        {context.markets.map((market) => (
          <div className="proof-row" key={market.target_id}>
            <span className={market.selected ? 'ok' : 'muted'}>
              {market.selected ? 'SELECTED' : 'SIBLING'}
            </span>
            <b>{market.venue}</b>
            <code>{market.target_id}</code>
          </div>
        ))}
      </DetailGroup>
      <DetailGroup title={`Selected targets (${context.targets.length})`} open>
        {context.targets.map((target) => (
          <div className="target-proof" key={target.target_id}>
            <b>
              {target.venue} · {universeLabel(target.canonical_class)}
            </b>
            <code>{target.target_id}</code>
            <span>
              Source <code>{target.source_ref}</code>
            </span>
            <span>Subscriptions: {target.subscription_ids.join(', ')}</span>
          </div>
        ))}
      </DetailGroup>
      <DetailGroup title={`Relationships (${context.relationships.length})`}>
        {context.relationships.map((relationship, index) => (
          <div
            className="relationship-proof"
            key={`${relationship.left}-${relationship.right}-${index}`}
          >
            <b>
              {universeLabel(relationship.relationship)} ·{' '}
              {universeLabel(relationship.coverage)}
            </b>
            <code>{relationship.left}</code>
            <span>↔</span>
            <code>{relationship.right}</code>
          </div>
        ))}
      </DetailGroup>
      <DetailGroup title="Immutable source & origin">
        <div className="identity-proof">
          <b>Occurrence source · {detail.run_id}</b>
          <code>{detail.source.manifest_key}</code>
          <code>manifest {detail.source.manifest_sha256}</code>
          <code>report {detail.source.report_sha256}</code>
          <b>Origin · {detail.origin.run_id}</b>
          <code>{detail.origin.manifest_key}</code>
          <code>manifest {detail.origin.manifest_sha256}</code>
          <code>report {detail.origin.report_sha256}</code>
        </div>
      </DetailGroup>
      <DetailGroup title={`Bundle history (${history.length})`} open>
        <ol className="bundle-history">
          {history.map((occurrence) => (
            <li key={occurrence.run_id}>
              <button onClick={() => openHistory(occurrence)}>
                <b>{new Date(occurrence.generated_at).toLocaleString()}</b>
                <span>
                  {universeLabel(occurrence.occurrence_kind)} ·{' '}
                  {universeLabel(occurrence.continuity_disposition)}
                </span>
                <code>{occurrence.run_id}</code>
              </button>
            </li>
          ))}
        </ol>
      </DetailGroup>
    </aside>
  );
}

function DetailGroup({
  title,
  children,
  open = false,
}: {
  title: string;
  children: React.ReactNode;
  open?: boolean;
}) {
  return (
    <details className="detail-group" open={open}>
      <summary>{title}</summary>
      <div>{children}</div>
    </details>
  );
}
