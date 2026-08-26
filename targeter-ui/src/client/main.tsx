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
  UniverseCadenceCandidate,
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

const score = (value: number | undefined) =>
  value === undefined
    ? '—'
    : value.toLocaleString(undefined, { maximumFractionDigits: 2 });

const compactCurrency = (value: number | undefined) =>
  value === undefined
    ? '—'
    : `$${value.toLocaleString('en-US', {
        notation: 'compact',
        maximumFractionDigits: 1,
      })}`;

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
          <NavLink to="/operations/selections">Decisions</NavLink>
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
          <CatalogObservability run={run} />
          <ContinuityObservability run={run} />
          <RunProvenance run={run} />
          <RunSelections run={run} />
          <RejectionSummary run={run} />
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

function CatalogObservability({ run }: { run: UniverseCadenceRun }) {
  const failures = Object.entries(run.discovery_failures);
  return (
    <section>
      <Title
        title="Discovery inputs"
        sub={`${run.catalogs.filter((catalog) => catalog.complete).length}/${run.catalogs.length} catalogues complete`}
      />
      <div className="catalog-grid">
        {run.catalogs.map((catalog) => (
          <article className="catalog" key={catalog.venue}>
            <div className="row">
              <b>{catalog.venue}</b>
              <span className={catalog.complete ? 'ok' : 'warn'}>
                {catalog.complete ? 'COMPLETE' : 'INCOMPLETE'}
              </span>
            </div>
            <span>
              {catalog.events} events · {catalog.markets} markets ·{' '}
              {catalog.requests} requests
            </span>
            {!!catalog.classification_diagnostic_count && (
              <small className="warn">
                {catalog.classification_diagnostic_count} classification
                diagnostics
              </small>
            )}
            {catalog.diagnostics.map((diagnostic) => (
              <small key={diagnostic}>{diagnostic}</small>
            ))}
          </article>
        ))}
      </div>
      {!!failures.length && (
        <ul className="diagnostics">
          {failures.map(([venue, failure]) => (
            <li key={venue}>
              {venue}: {failure}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function ContinuityObservability({ run }: { run: UniverseCadenceRun }) {
  const legitimateRetirement =
    run.selections.length === 0 &&
    run.continuity.bundles.length > 0 &&
    run.continuity.bundles.every((bundle) =>
      ['all_markets_terminal', 'terminal_clamp_elapsed'].includes(
        bundle.disposition,
      ),
    );
  const budgetTrimmed =
    run.selections.length === 0 &&
    run.continuity.bundles.length > 0 &&
    run.continuity.bundles.every(
      (bundle) => bundle.disposition === 'continuity_budget_trimmed',
    );
  return (
    <section className="continuity-panel">
      <Title
        title="Continuity decisions"
        sub={`${run.continuity.bundles.length} prior bundles observed`}
      />
      <p className="truth-note">
        Archived run evidence only. This projection does not verify current.json
        publication or splice health.
      </p>
      <div className="continuity-metrics">
        <Metric n={run.counts.retained} label="Bundles retained" />
        <Metric n={run.counts.retired} label="Terminal/clamp retirements" />
        <Metric n={run.diagnostics.continuity.length} label="Diagnostics" />
      </div>
      {legitimateRetirement && (
        <div className="continuity-state ok">
          Legitimate empty generation: every prior bundle was retired by
          all-terminal evidence or the safety clamp.
        </div>
      )}
      {budgetTrimmed && (
        <div className="continuity-state warn">
          Empty generation: every prior bundle was continuity-budget trimmed.
        </div>
      )}
      {run.diagnostics.continuity_degraded_base_run_id && (
        <div className="continuity-state warn">
          DEGRADED BASE RUN{' '}
          <code>{run.diagnostics.continuity_degraded_base_run_id}</code>
        </div>
      )}
      {!!run.diagnostics.continuity.length && (
        <ul className="diagnostics">
          {run.diagnostics.continuity.map((diagnostic) => (
            <li key={diagnostic}>{diagnostic}</li>
          ))}
        </ul>
      )}
      <div className="continuity-list">
        {[...run.continuity.bundles]
          .sort(
            (left, right) =>
              right.score - left.score ||
              left.bundle_id.localeCompare(right.bundle_id),
          )
          .map((bundle) => (
            <details
              key={bundle.bundle_id}
              open={bundle.targets.some(
                (target) => target.terminal_probe.state === 'unknown',
              )}
            >
              <summary>
                <span
                  className={`decision ${bundle.disposition === 'retained' ? 'ok' : 'warn'}`}
                >
                  {label(bundle.disposition)}
                </span>
                <b>{bundle.bundle_id}</b>
                <span>
                  Score {score(bundle.score)} · base {bundle.base_run_id} ·
                  origin {bundle.origin_run_id}
                </span>
              </summary>
              <ul className="probe-list">
                {bundle.targets.map((target) => (
                  <li key={target.target_id}>
                    <span className={`probe ${target.terminal_probe.state}`}>
                      {target.terminal_probe.state.toUpperCase()}
                    </span>
                    <b>{target.venue}</b>
                    <code>{target.target_id}</code>
                    <span>{target.terminal_probe.reason}</span>
                  </li>
                ))}
              </ul>
            </details>
          ))}
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
      <div className="run-proof">
        <span>
          Budget used:{' '}
          {Object.entries(run.budget_used)
            .map(([venue, used]) => `${venue} ${used}`)
            .join(' · ') || 'none'}
        </span>
        {Object.entries(run.diagnostics.target_records).map(
          ([venue, diagnostics]) =>
            diagnostics.map((diagnostic) => (
              <span className="warn" key={`${venue}-${diagnostic}`}>
                {venue} target records: {diagnostic}
              </span>
            )),
        )}
      </div>
    </section>
  );
}

function RunSelections({ run }: { run: UniverseCadenceRun }) {
  const evidence = new Map(
    run.candidates.map((candidate) => [candidate.bundle_id, candidate]),
  );
  const continuityScores = new Map(
    run.continuity.bundles.map((bundle) => [bundle.bundle_id, bundle.score]),
  );
  const selections = [...run.selections].sort((left, right) => {
    const leftScore =
      evidence.get(left.bundle_id)?.score ??
      continuityScores.get(left.bundle_id) ??
      Number.NEGATIVE_INFINITY;
    const rightScore =
      evidence.get(right.bundle_id)?.score ??
      continuityScores.get(right.bundle_id) ??
      Number.NEGATIVE_INFINITY;
    return (
      rightScore - leftScore || left.bundle_id.localeCompare(right.bundle_id)
    );
  });
  return (
    <section>
      <Title
        title="Selected bundles"
        sub="Verified selected lifecycle detail from the cadence projection"
      />
      {selections.length ? (
        <div className="cards">
          {selections.map((selection) => (
            <SelectionCard
              key={`${selection.run_id}-${selection.bundle_id}`}
              selection={selection}
              candidate={evidence.get(selection.bundle_id)}
              decisionScore={
                evidence.get(selection.bundle_id)?.score ??
                continuityScores.get(selection.bundle_id)
              }
            />
          ))}
        </div>
      ) : (
        <p className="empty">{cadenceRunEmptyMessage(run)}</p>
      )}
    </section>
  );
}

function RejectionSummary({ run }: { run: UniverseCadenceRun }) {
  const reasons = Object.entries({
    ...run.reason_summaries.candidate_rejections,
    ...run.reason_summaries.allocation_rejections,
  }).sort(
    (left, right) => right[1] - left[1] || left[0].localeCompare(right[0]),
  );
  return (
    <section>
      <Title
        title="Rejected and unallocated"
        sub={`${run.counts.rejected} admission rejections · ${run.match_rejections.length} match rejections`}
      />
      {reasons.length ? (
        <div className="reasonbar">
          {reasons.map(([reason, count]) => (
            <div key={reason}>
              <b>{count}</b>
              <span>{label(reason)}</span>
            </div>
          ))}
        </div>
      ) : (
        <p className="muted">No candidate or allocation rejection reasons.</p>
      )}
      {!!run.match_rejections.length && (
        <details className="detail-group">
          <summary>
            {run.match_rejections.length} unmatched event groups
          </summary>
          <ul className="diagnostics">
            {run.match_rejections.map((rejection, index) => (
              <li key={`${rejection.reason}-${index}`}>
                {rejection.participant_keys.join(' vs ')} ·{' '}
                {label(rejection.reason)}
              </li>
            ))}
          </ul>
        </details>
      )}
    </section>
  );
}

function CandidateDecisionCard({
  run,
  candidate,
  state,
}: {
  run: UniverseCadenceRun;
  candidate: UniverseCadenceCandidate;
  state: string;
}) {
  const targetEntries = Object.entries(run.selected_targets).flatMap(
    ([venue, targets]) =>
      targets
        .filter((target) => target.bundle_id === candidate.bundle_id)
        .map((target) => ({ venue, target })),
  );
  return (
    <details className="decision-card">
      <summary>
        <span className={`decision ${state === 'rejected' ? 'warn' : 'ok'}`}>
          {label(state)}
        </span>
        <b>{candidate.participants?.join(' vs ') || candidate.bundle_id}</b>
        <span>
          Score {score(candidate.score)} · {label(candidate.event_status)} ·{' '}
          {run.run_id}
        </span>
      </summary>
      <div className="detail">
        <h4>Decision reasons</h4>
        <pre>
          {JSON.stringify(
            [
              ...(candidate.rejection_reasons ?? []),
              ...(candidate.allocation_rejection
                ? [candidate.allocation_rejection]
                : []),
            ],
            null,
            2,
          )}
        </pre>
        <h4>Admission</h4>
        <pre>
          {JSON.stringify(
            candidate.admission
              ? {
                  combined_moneyline_volume_usd:
                    candidate.admission.combined_moneyline_volume_usd,
                  minimum_moneyline_volume_usd:
                    candidate.admission.minimum_moneyline_volume_usd,
                }
              : {},
            null,
            2,
          )}
        </pre>
        <h4>Markets</h4>
        <pre>
          {JSON.stringify(candidate.eligible_market_ids ?? [], null, 2)}
        </pre>
        <h4>Selected targets</h4>
        <pre>
          {JSON.stringify(
            targetEntries.map(({ venue, target }) => ({
              venue,
              target_id: target.target_id,
              subscriptions: target.subscription_ids,
              continuity_score: target.continuity_score,
            })),
            null,
            2,
          )}
        </pre>
        <h4>Relationships</h4>
        <pre>
          {JSON.stringify(
            candidate.relationship_analysis.relationships ?? [],
            null,
            2,
          )}
        </pre>
      </div>
    </details>
  );
}

function SelectionCard({
  selection,
  candidate,
  decisionScore,
}: {
  selection: UniverseSelectionDetail;
  candidate?: UniverseCadenceCandidate;
  decisionScore?: number;
}) {
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
        <strong className="score">Score {score(decisionScore)}</strong>
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
        <dt>Volume gate</dt>
        <dd>
          {candidate?.admission
            ? `${compactCurrency(candidate.admission.combined_moneyline_volume_usd)} / ${compactCurrency(candidate.admission.minimum_moneyline_volume_usd)}`
            : 'Retained from prior committed targets'}
        </dd>
      </dl>
      <p className="muted">
        Continuity: {label(selection.continuity_disposition)}
      </p>
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
  const [decision, setDecision] = useState('all');
  const [runId, setRunId] = useState('all');
  const [venue, setVenue] = useState('all');
  const rows = useMemo(
    () =>
      cadence.runs.flatMap((run) =>
        run.candidates.map((candidate) => ({
          run,
          candidate,
          state: candidate.selected
            ? 'selected'
            : candidate.eligible
              ? 'not-selected'
              : 'rejected',
        })),
      ),
    [cadence],
  );
  const venues = [
    ...new Set(
      rows.flatMap(({ run, candidate }) =>
        Object.entries(run.selected_targets)
          .filter(([, targets]) =>
            targets.some((target) => target.bundle_id === candidate.bundle_id),
          )
          .map(([value]) => value),
      ),
    ),
  ].sort();
  const shown = rows
    .filter(({ run, candidate, state }) => {
      const haystack = [
        candidate.bundle_id,
        candidate.sport,
        candidate.game,
        candidate.topology,
        ...(candidate.participants ?? []),
        ...(candidate.event_refs ?? []),
        ...(candidate.rejection_reasons ?? []),
        ...Object.entries(run.selected_targets)
          .filter(([, targets]) =>
            targets.some((target) => target.bundle_id === candidate.bundle_id),
          )
          .flatMap(([targetVenue, targets]) => [
            targetVenue,
            ...targets
              .filter((target) => target.bundle_id === candidate.bundle_id)
              .map((target) => target.target_id),
          ]),
      ]
        .join(' ')
        .toLowerCase();
      return (
        haystack.includes(query.toLowerCase()) &&
        (decision === 'all' || decision === state) &&
        (runId === 'all' || runId === run.run_id) &&
        (venue === 'all' ||
          Object.entries(run.selected_targets).some(
            ([targetVenue, targets]) =>
              targetVenue === venue &&
              targets.some(
                (target) => target.bundle_id === candidate.bundle_id,
              ),
          ))
      );
    })
    .sort(
      (left, right) =>
        (right.candidate.score ?? Number.NEGATIVE_INFINITY) -
          (left.candidate.score ?? Number.NEGATIVE_INFINITY) ||
        right.run.run_id.localeCompare(left.run.run_id) ||
        left.candidate.bundle_id.localeCompare(right.candidate.bundle_id),
    );

  return (
    <section>
      <Title
        title="Targeter decisions"
        sub={`${shown.length} of ${rows.length} candidate decisions across indexed cadence runs`}
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
          Decision
          <select
            value={decision}
            onChange={(event) => setDecision(event.target.value)}
          >
            <option value="all">All</option>
            <option value="selected">Selected</option>
            <option value="not-selected">Eligible, not allocated</option>
            <option value="rejected">Rejected</option>
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
          {shown.map(({ run, candidate, state }) => (
            <CandidateDecisionCard
              key={`${run.run_id}-${candidate.bundle_id}`}
              run={run}
              candidate={candidate}
              state={state}
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
