import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  BrowserRouter,
  NavLink,
  Route,
  Routes,
  useLocation,
} from 'react-router-dom';
import type { ContinuityTarget, RunView, Snapshot } from '../shared';
import {
  isContinuityBundleV3,
  isContinuityReport,
  legitimateEmptyGenerationReason,
  selectedBundleViews,
  type SelectedBundleView,
} from './view-model';
import { EventUniversePage } from './event-universe';
import { targeterSnapshotNeeded } from './app-routing';
import './style.css';

const val = (x: any, fallback = '—') =>
  x === undefined || x === null || x === '' ? fallback : String(x);
const list = (x: any): any[] => (Array.isArray(x) ? x : []);
const date = (x: any) => {
  const d = new Date(x);
  return Number.isNaN(d.valueOf()) ? val(x) : d.toLocaleString();
};
const relativeTime = (x: any) => {
  const timestamp = new Date(x).valueOf();
  if (Number.isNaN(timestamp)) return val(x);
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
const numericScore = (x: any) => {
  const n = Number(x);
  return Number.isFinite(n) ? n : Number.NEGATIVE_INFINITY;
};
const scoreLabel = (x: any) => {
  const n = numericScore(x);
  return Number.isFinite(n)
    ? n.toLocaleString(undefined, { maximumFractionDigits: 2 })
    : '—';
};
const compactCurrency = (x: any) => {
  const n = Number(x);
  return Number.isFinite(n)
    ? `$${n.toLocaleString('en-US', {
        notation: 'compact',
        maximumFractionDigits: 1,
      })}`
    : '—';
};
const relationshipSummary = (candidate: any) => {
  const counts = new Map<string, number>();
  for (const item of list(candidate.relationship_analysis?.relationships)) {
    if (typeof item?.relationship !== 'string' || !item.relationship) continue;
    counts.set(item.relationship, (counts.get(item.relationship) ?? 0) + 1);
  }
  return [...counts.entries()].sort(
    ([left, leftCount], [right, rightCount]) =>
      rightCount - leftCount || left.localeCompare(right),
  );
};
const relationshipLabel = (x: string) => {
  const label = x.toLowerCase().replaceAll('_', ' ');
  return label.charAt(0).toUpperCase() + label.slice(1);
};
function App() {
  const location = useLocation();
  const needsSnapshot = targeterSnapshotNeeded(location.pathname);
  const [s, setS] = useState<Snapshot | null>(null);
  const [error, setError] = useState('');
  const load = useCallback(async (method = 'GET') => {
    try {
      const r = await fetch(
        method === 'GET' ? '/api/snapshot' : '/api/refresh',
        { method },
      );
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setS(await r.json());
      setError('');
    } catch (e) {
      setError(String(e));
    }
  }, []);
  useEffect(() => {
    if (!needsSnapshot) return;
    void load();
    const id = setInterval(() => void load(), 15000);
    return () => clearInterval(id);
  }, [load, needsSnapshot]);
  return (
    <>
      <header>
        <div>
          <span className="eyebrow">PREDICTION INDEXER</span>
          <h1>Targeter Operations</h1>
        </div>
        <nav aria-label="Main navigation">
          <NavLink to="/">Overview</NavLink>
          <NavLink to="/events">Events</NavLink>
          <NavLink to="/event-universe">Event Universe</NavLink>
          <NavLink to="/config">Config</NavLink>
        </nav>
        {needsSnapshot && (
          <button onClick={() => void load('POST')} disabled={s?.refreshing}>
            ↻ Refresh archive
          </button>
        )}
      </header>
      {needsSnapshot && (error || s?.lastRefreshError) && (
        <div className="alert" role="alert">
          Snapshot stale/error: {error || s?.lastRefreshError}. Last successful
          data is retained.
        </div>
      )}
      <main>
        <Routes>
          <Route
            path="/"
            element={
              <SnapshotRoute
                s={s}
                render={(snapshot) => <Overview s={snapshot} />}
              />
            }
          />
          <Route
            path="/events"
            element={
              <SnapshotRoute
                s={s}
                render={(snapshot) => <Events s={snapshot} />}
              />
            }
          />
          <Route path="/event-universe" element={<EventUniversePage />} />
          <Route
            path="/config"
            element={
              <SnapshotRoute
                s={s}
                render={(snapshot) => <Config s={snapshot} />}
              />
            }
          />
        </Routes>
      </main>
      {needsSnapshot && s && <StatusFooter s={s} />}
    </>
  );
}
function SnapshotRoute({
  s,
  render,
}: {
  s: Snapshot | null;
  render: (snapshot: Snapshot) => React.ReactNode;
}) {
  return s ? (
    render(s)
  ) : (
    <div className="loading">Loading archive snapshot…</div>
  );
}
function Overview({ s }: { s: Snapshot }) {
  const [selectedRunId, setSelectedRunId] = useState(s.runs[0]?.runId ?? '');
  useEffect(() => {
    if (!s.runs.some((run) => run.runId === selectedRunId))
      setSelectedRunId(s.runs[0]?.runId ?? '');
  }, [s.runs, selectedRunId]);
  const run = s.runs.find((item) => item.runId === selectedRunId) ?? s.runs[0];
  return (
    <div className="stack">
      <section>
        <Title
          title="Committed run timeline"
          sub="Up to five, ordered by parsed run ID — never S3 LastModified"
        />
        <div className="timeline">
          {s.runs.map((r, i) => (
            <button
              key={r.runId}
              className={`${i === 0 ? 'latest' : ''} ${r.runId === run?.runId ? 'selected' : ''}`}
              aria-pressed={r.runId === run?.runId}
              onClick={() => setSelectedRunId(r.runId)}
            >
              <span>{i === 0 ? 'LATEST' : 'RUN'}</span>
              <b>{r.runId}</b>
              <small>{date(r.generatedAt)}</small>
              <em className={r.inputComplete ? 'ok' : 'warn'}>
                {r.inputComplete ? 'Complete' : 'Incomplete'}
              </em>
            </button>
          ))}
        </div>
      </section>
      {run ? (
        <>
          <Metrics run={run} latest={run.runId === s.runs[0]?.runId} />
          <ContinuityObservability run={run} />
          <Bundles run={run} />
          <Rejections run={run} />
        </>
      ) : (
        <section className="empty">No committed manifests found.</section>
      )}
    </div>
  );
}
function Metrics({ run, latest }: { run: RunView; latest: boolean }) {
  const m = run.summary;
  const retained = isContinuityReport(run.report)
    ? run.report.continuity.retained_bundle_ids.length
    : 0;
  return (
    <section>
      <Title
        title={latest ? 'Latest run' : 'Selected run'}
        sub={relativeTime(run.generatedAt)}
      />
      <div className="metrics">
        <Metric n={m.selected} label="Selected bundles" />
        <Metric n={retained} label="Retained bundles" />
        <Metric n={m.targets} label="Capture targets" />
        <Metric n={m.candidates} label="Candidates" />
        <Metric
          n={`${m.catalogsComplete}/${m.catalogsTotal}`}
          label="Catalogues complete"
        />
      </div>
    </section>
  );
}
function ContinuityObservability({ run }: { run: RunView }) {
  if (!isContinuityReport(run.report)) {
    return (
      <section className="continuity-panel legacy">
        <Title
          title="Continuity"
          sub="Report v1 — continuity evidence is unavailable"
        />
        <p className="muted">
          This archived run predates continuity holds and terminal probes.
          Subscription truth remains the generation committed by current.json.
        </p>
      </section>
    );
  }
  const { continuity } = run.report;
  const emptyReason = legitimateEmptyGenerationReason(run.report);
  const retired = Object.values(continuity.dispositions).filter((value) =>
    ['all_markets_terminal', 'terminal_clamp_elapsed'].includes(value),
  ).length;
  return (
    <section className="continuity-panel">
      <Title
        title="Continuity"
        sub={`Report v${run.report.report_version} · ${continuity.bundles.length} prior bundles observed`}
      />
      <p className="truth-note">
        Run decision evidence only. Live subscription truth remains the
        immutable generation selected by current.json.
      </p>
      <div className="continuity-metrics">
        <Metric
          n={continuity.retained_bundle_ids.length}
          label="Exact bundles retained"
        />
        <Metric n={retired} label="Terminal/clamp retirements" />
        <Metric
          n={run.report.continuity_diagnostics.length}
          label="Diagnostics"
        />
      </div>
      {emptyReason && (
        <div className="continuity-state ok">
          {emptyReason === 'retirement'
            ? 'Legitimate empty retirement: every prior bundle is evidenced terminal or past its terminal clamp.'
            : 'Legitimate empty generation: every prior bundle was explicitly continuity-budget trimmed.'}
        </div>
      )}
      {run.report.continuity_degraded_base_run_id && (
        <div className="continuity-state warn">
          DEGRADED BASE RUN{' '}
          <code>{run.report.continuity_degraded_base_run_id}</code>
        </div>
      )}
      {!!run.report.continuity_diagnostics.length && (
        <ul className="diagnostics">
          {run.report.continuity_diagnostics.map((diagnostic, index) => (
            <li key={`${index}-${diagnostic}`}>{diagnostic}</li>
          ))}
        </ul>
      )}
      {!continuity.bundles.length &&
        !run.report.continuity_degraded_base_run_id && (
          <p className="muted">No prior committed generation was observed.</p>
        )}
      <div className="continuity-list">
        {continuity.bundles.map((bundle) => {
          const origin = isContinuityBundleV3(bundle) ? bundle : null;
          return (
            <details
              key={bundle.bundle_id}
              open={bundle.targets.some(
                (target) => target.terminal_probe.state === 'unknown',
              )}
            >
              <summary>
                <span
                  className={`decision ${continuity.dispositions[bundle.bundle_id] === 'retained' ? 'ok' : 'warn'}`}
                >
                  {relationshipLabel(continuity.dispositions[bundle.bundle_id])}
                </span>
                <b>{bundle.bundle_id}</b>
                <span>
                  Score {scoreLabel(bundle.score)} · base {bundle.base_run_id}
                  {origin ? ` · origin ${origin.origin_run_id}` : ''}
                </span>
              </summary>
              {origin && (
                <div className="origin-evidence">
                  <span>
                    Origin report <code>{origin.origin_report_sha256}</code>
                  </span>
                  <span>
                    Origin manifest{' '}
                    <code>{origin.origin_archive_manifest_key}</code>
                  </span>
                  <span>
                    Manifest SHA{' '}
                    <code>{origin.origin_archive_manifest_sha256}</code>
                  </span>
                </div>
              )}
              <TerminalProbes targets={bundle.targets} />
            </details>
          );
        })}
      </div>
    </section>
  );
}
function TerminalProbes({ targets }: { targets: ContinuityTarget[] }) {
  return (
    <ul className="probe-list">
      {targets.map((target) => (
        <li key={target.target_id}>
          <span className={`probe ${target.terminal_probe.state}`}>
            {target.terminal_probe.state.toUpperCase()}
          </span>
          <b>{target.venue}</b>
          <code>{target.venue_market_id}</code>
          <span>{target.terminal_probe.reason}</span>
        </li>
      ))}
    </ul>
  );
}
function StatusFooter({ s }: { s: Snapshot }) {
  const latest = s.runs[0];
  const age = latest
    ? (Date.now() - new Date(latest.generatedAt).valueOf()) / 1000
    : Infinity;
  const live = !!latest && age <= s.expectedRunSeconds * 2;
  return (
    <footer className="status-footer">
      <div className={live ? 'good' : 'bad'}>
        <span className={`dot ${live ? 'good' : 'bad'}`} />
        <strong>{live ? 'LIVE' : 'NOT LIVE'}</strong>
        <span>Targeter cadence</span>
      </div>
      <span>
        Latest run {latest ? relativeTime(latest.generatedAt) : 'unavailable'}
      </span>
      <span>
        Archive {s.stale ? 'STALE' : 'CURRENT'} · {s.source.toUpperCase()}
      </span>
      <small>
        Heuristic: latest run within 2× {s.expectedRunSeconds}s cadence
      </small>
    </footer>
  );
}
function Metric({ n, label }: { n: any; label: string }) {
  return (
    <div className="metric">
      <strong>{n}</strong>
      <span>{label}</span>
    </div>
  );
}
function Bundles({ run }: { run: RunView }) {
  const bundles = selectedBundleViews(run.report);
  const emptyReason = legitimateEmptyGenerationReason(run.report);
  return (
    <section>
      <Title
        title="Run-selected bundles"
        sub="Decision evidence; current.json remains subscription truth"
      />
      <div className="cards">
        {bundles.map((bundle) => {
          const candidate = bundle.candidate;
          const activation =
            candidate?.activation_at ??
            bundle.continuity?.activation_at ??
            bundle.targets[0]?.activation_at;
          const capture =
            candidate?.capture_start_at ?? bundle.targets[0]?.capture_start_at;
          const venues = [
            ...new Set(bundle.targets.map((target) => target.venue)),
          ];
          return (
            <article
              className={`bundle ${bundle.retained ? 'retained' : ''}`}
              key={bundle.bundleId}
            >
              <div className="row">
                <span className="tag">
                  {bundle.retained ? 'RETAINED' : val(candidate?.sport)}
                </span>
                <strong className="score">
                  {isContinuityReport(run.report)
                    ? 'Continuity score'
                    : 'Score'}{' '}
                  {scoreLabel(bundle.score)}
                </strong>
              </div>
              <h3>
                {list(candidate?.participants).join(' vs ') || bundle.bundleId}
              </h3>
              <div className="muted">
                {venues.join(' · ') || list(candidate?.venues).join(' · ')}
              </div>
              <dl>
                <dt>Activation</dt>
                <dd>{date(activation)}</dd>
                <dt>Capture</dt>
                <dd>{date(capture)}</dd>
                <dt>Continuity</dt>
                <dd>
                  {bundle.disposition
                    ? relationshipLabel(bundle.disposition)
                    : 'Report v1'}
                </dd>
                <dt>Occurrence</dt>
                <dd>{relationshipLabel(bundle.occurrenceKind)}</dd>
                <dt>Base run</dt>
                <dd>
                  <code>{val(bundle.continuityBaseRunId)}</code>
                </dd>
                <dt>Origin run</dt>
                <dd>
                  <code>{val(bundle.continuityOriginRunId)}</code>
                </dd>
                <dt>{bundle.retained ? 'Admission' : 'Volume gate'}</dt>
                <dd>
                  {bundle.retained
                    ? 'Exact prior committed targets'
                    : `${compactCurrency(candidate?.admission?.combined_moneyline_volume_usd)} / ${compactCurrency(candidate?.admission?.minimum_moneyline_volume_usd)}`}
                </dd>
              </dl>
              <MarketList bundle={bundle} />
              {candidate && <RelationshipSummary candidate={candidate} />}
            </article>
          );
        })}
      </div>
      {!bundles.length && (
        <p className={`empty ${emptyReason ? 'ok' : ''}`}>
          {emptyReason === 'retirement'
            ? 'No bundles selected: all prior bundles were legitimately retired by terminal evidence or the clamp.'
            : emptyReason === 'budget_trimmed'
              ? 'No bundles selected: every prior bundle was explicitly continuity-budget trimmed.'
              : 'No bundles selected in this run. This report alone does not change live subscriptions.'}
        </p>
      )}
    </section>
  );
}
function MarketList({ bundle }: { bundle: SelectedBundleView }) {
  const targets = bundle.targets.map((item) => ({
    venue: item.venue,
    id: String(item.target_id ?? item.source_ref ?? ''),
    type: String(item.canonical_class ?? ''),
    probe: bundle.continuity?.targets.find(
      (target) => target.target_id === item.target_id,
    )?.terminal_probe,
  }));
  const markets = (
    targets.length
      ? targets
      : list(bundle.candidate?.eligible_market_ids).map((id) => {
          const [venue, ...rest] = String(id).split(':');
          return {
            venue,
            id: String(id),
            type: rest.length ? '' : 'market',
            probe: undefined,
          };
        })
  ).sort(
    (a, b) =>
      a.venue.localeCompare(b.venue) ||
      a.type.localeCompare(b.type) ||
      a.id.localeCompare(b.id),
  );
  return (
    <details className="markets">
      <summary>{markets.length} markets</summary>
      <ul>
        {markets.map((market) => (
          <li key={market.id}>
            <span className="market-venue">{market.venue}</span>
            <span>
              {market.type
                ? relationshipLabel(market.type.split('.').at(-1)!)
                : 'Market'}
            </span>
            <code>{market.id.replace(`${market.venue}:`, '')}</code>
            {market.probe && (
              <small className={`probe-reason ${market.probe.state}`}>
                {market.probe.state}: {market.probe.reason}
              </small>
            )}
          </li>
        ))}
      </ul>
    </details>
  );
}
function RelationshipSummary({ candidate }: { candidate: any }) {
  const entries = relationshipSummary(candidate);
  const total = entries.reduce((sum, [, count]) => sum + count, 0);
  return (
    <div className="relationships">
      <b>{total ? `${total} relationships` : 'No relationships recorded'}</b>
      {!!entries.length && (
        <div className="relationship-types">
          {entries.map(([type, count]) => (
            <span key={type}>
              <strong>{count}</strong> {relationshipLabel(type)}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
function Rejections({ run }: { run: RunView }) {
  return (
    <section>
      <Title
        title="Rejected candidates"
        sub={`${run.summary.rejected} rejected in latest run`}
      />
      <div className="reasonbar">
        {Object.entries(run.summary.rejectionReasons)
          .sort((a, b) => b[1] - a[1])
          .map(([k, v]) => (
            <div key={k}>
              <b>{v}</b>
              <span>{k}</span>
            </div>
          ))}
      </div>
    </section>
  );
}
function Events({ s }: { s: Snapshot }) {
  const [q, setQ] = useState('');
  const [state, setState] = useState('all');
  const [run, setRun] = useState('all');
  const [reason, setReason] = useState('all');
  const rows = useMemo(
    () =>
      s.runs.flatMap((r) => {
        const selected = new Set(list(r.report.selection?.bundle_ids));
        const allocations = r.report.selection?.allocation_rejections ?? {};
        const selectedViews = selectedBundleViews(r.report);
        const selectedByBundle = new Map(
          selectedViews.map((bundle) => [bundle.bundleId, bundle]),
        );
        const candidates = list(r.report.candidates).map((c) => {
          const admissionReasons = list(c.rejection_reasons).map(String);
          const allocationReason = allocations[c.bundle_id];
          const bundle = selectedByBundle.get(c.bundle_id);
          const decision = bundle?.retained
            ? 'retained'
            : selected.has(c.bundle_id)
              ? 'selected'
              : admissionReasons.length
                ? 'rejected'
                : 'not-selected';
          return {
            r,
            c: bundle?.continuity
              ? {
                  ...c,
                  continuity: bundle.continuity,
                  continuity_base_run_id: bundle.continuityBaseRunId,
                }
              : c,
            decision,
            reasons: bundle?.disposition
              ? [bundle.disposition]
              : selected.has(c.bundle_id)
                ? ['selected']
                : admissionReasons.length
                  ? admissionReasons
                  : allocationReason
                    ? [String(allocationReason)]
                    : ['eligible_not_selected'],
          };
        });
        const retained = selectedViews
          .filter((bundle) => bundle.retained && !bundle.candidate)
          .map((bundle) => ({
            r,
            c: {
              bundle_id: bundle.bundleId,
              score: bundle.score,
              activation_at: bundle.continuity?.activation_at,
              venues: [
                ...new Set(bundle.targets.map((target) => target.venue)),
              ],
              event_status: 'RETAINED',
              continuity: bundle.continuity,
              continuity_base_run_id: bundle.continuityBaseRunId,
            },
            decision: 'retained',
            reasons: [bundle.disposition ?? 'retained'],
          }));
        return [...candidates, ...retained];
      }),
    [s],
  );
  const reasons = [...new Set(rows.flatMap((x) => x.reasons))];
  const shown = rows
    .filter((x) => {
      const hay = JSON.stringify([
        x.c.participants,
        x.c.bundle_id,
        x.c.venues,
        x.c.event_status,
        x.reasons,
      ]).toLowerCase();
      return (
        hay.includes(q.toLowerCase()) &&
        (state === 'all' || state === x.decision) &&
        (run === 'all' || run === x.r.runId) &&
        (reason === 'all' || x.reasons.includes(reason))
      );
    })
    .sort(
      (a, b) =>
        numericScore(b.c.score) - numericScore(a.c.score) ||
        b.r.runId.localeCompare(a.r.runId) ||
        String(a.c.bundle_id).localeCompare(String(b.c.bundle_id)),
    );
  return (
    <section>
      <Title
        title="Event explorer"
        sub={`${shown.length} of ${rows.length} candidates across retained runs`}
      />
      <div className="filters">
        <label>
          Search
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Team, venue, bundle ID…"
          />
        </label>
        <label>
          Decision
          <select value={state} onChange={(e) => setState(e.target.value)}>
            <option value="all">All</option>
            <option value="selected">Selected</option>
            <option value="retained">Retained from committed generation</option>
            <option value="rejected">Admission rejected</option>
            <option value="not-selected">Eligible, not allocated</option>
          </select>
        </label>
        <label>
          Run
          <select value={run} onChange={(e) => setRun(e.target.value)}>
            <option value="all">All five</option>
            {s.runs.map((r) => (
              <option key={r.runId}>{r.runId}</option>
            ))}
          </select>
        </label>
        <label>
          Reason
          <select value={reason} onChange={(e) => setReason(e.target.value)}>
            <option value="all">All reasons</option>
            {reasons.map((r) => (
              <option key={r}>{r}</option>
            ))}
          </select>
        </label>
      </div>
      <div className="eventlist">
        {shown.map(({ r, c, decision, reasons }) => (
          <details key={`${r.runId}-${c.bundle_id}`}>
            <summary>
              <span
                className={`decision ${['selected', 'retained'].includes(decision) ? 'ok' : 'warn'}`}
              >
                {decision === 'selected'
                  ? 'SELECTED'
                  : decision === 'retained'
                    ? 'RETAINED'
                    : decision === 'rejected'
                      ? 'REJECTED'
                      : 'NOT ALLOCATED'}
              </span>
              <b>{list(c.participants).join(' vs ') || c.bundle_id}</b>
              <span>
                Score {scoreLabel(c.score)} · {val(c.event_status)} · {r.runId}
              </span>
            </summary>
            <div className="detail">
              <h4>Decision reasons</h4>
              <pre>{JSON.stringify(reasons, null, 2)}</pre>
              <h4>Admission evidence</h4>
              <pre>{JSON.stringify(c.admission ?? {}, null, 2)}</pre>
              <h4>Market exclusions</h4>
              <pre>{JSON.stringify(c.market_exclusions ?? [], null, 2)}</pre>
              <h4>Relationships</h4>
              <pre>
                {JSON.stringify(c.relationship_analysis ?? [], null, 2)}
              </pre>
              <h4>Rule evidence</h4>
              <pre>{JSON.stringify(c.rule_assessment ?? {}, null, 2)}</pre>
              <h4>Identifiers & score</h4>
              <pre>
                {JSON.stringify(
                  {
                    bundle_id: c.bundle_id,
                    eligible_market_ids: c.eligible_market_ids,
                    score: c.score,
                    score_components: c.score_components,
                  },
                  null,
                  2,
                )}
              </pre>
              {c.continuity && (
                <>
                  <h4>Continuity evidence</h4>
                  <pre>{JSON.stringify(c.continuity, null, 2)}</pre>
                </>
              )}
            </div>
          </details>
        ))}
      </div>
    </section>
  );
}
function Config({ s }: { s: Snapshot }) {
  return (
    <section>
      <Title title="Current checkout strategy" sub={s.config.label} />
      <div className="configgrid">
        <div>
          <h3>Version comparison</h3>
          {s.runs.map((r) => (
            <div className="comparison" key={r.runId}>
              <code>{r.runId}</code>
              <span>archived v{val(r.strategyVersion)}</span>
              <b
                className={
                  s.config.versionMatchesRunIds.includes(r.runId)
                    ? 'ok'
                    : 'warn'
                }
              >
                {s.config.versionMatchesRunIds.includes(r.runId)
                  ? 'VERSION MATCH'
                  : 'DIFFERS / UNKNOWN'}
              </b>
            </div>
          ))}
          <p className="note">
            The archive records a strategy version and source path, but not the
            complete historical config bytes. A version match is useful
            evidence, not proof that every historical setting equals this
            checkout.
          </p>
        </div>
        <div>
          <h3>Readable groups</h3>
          {Object.entries(s.config.value as object).map(([k, v]) => (
            <details key={k}>
              <summary>{k}</summary>
              <pre>{JSON.stringify(v, null, 2)}</pre>
            </details>
          ))}
        </div>
      </div>
      <h3>Raw JSON</h3>
      <pre className="raw">{JSON.stringify(s.config.value, null, 2)}</pre>
    </section>
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
