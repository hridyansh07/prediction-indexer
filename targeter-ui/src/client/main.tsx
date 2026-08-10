import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom';
import type { RunView, Snapshot } from '../shared';
import './style.css';

const val = (x: any, fallback = '—') =>
  x === undefined || x === null || x === '' ? fallback : String(x);
const list = (x: any): any[] => (Array.isArray(x) ? x : []);
const date = (x: any) => {
  const d = new Date(x);
  return Number.isNaN(d.valueOf()) ? val(x) : d.toLocaleString();
};
function App() {
  const [s, setS] = useState<Snapshot | null>(null);
  const [error, setError] = useState('');
  const load = async (method = 'GET') => {
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
  };
  useEffect(() => {
    void load();
    const id = setInterval(() => void load(), 15000);
    return () => clearInterval(id);
  }, []);
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
          <NavLink to="/config">Config</NavLink>
        </nav>
        <button onClick={() => void load('POST')} disabled={s?.refreshing}>
          ↻ Refresh archive
        </button>
      </header>
      {(error || s?.lastRefreshError) && (
        <div className="alert" role="alert">
          Snapshot stale/error: {error || s?.lastRefreshError}. Last successful
          data is retained.
        </div>
      )}
      <main>
        {s ? (
          <Routes>
            <Route path="/" element={<Overview s={s} />} />
            <Route path="/events" element={<Events s={s} />} />
            <Route path="/config" element={<Config s={s} />} />
          </Routes>
        ) : (
          <div className="loading">Loading archive snapshot…</div>
        )}
      </main>
    </>
  );
}
function Overview({ s }: { s: Snapshot }) {
  const run = s.runs[0];
  const age = run
    ? (Date.now() - new Date(run.generatedAt).valueOf()) / 1000
    : Infinity;
  const stalled = age > s.expectedRunSeconds * 2;
  return (
    <div className="stack">
      <section className="hero">
        <div>
          <span className={`dot ${stalled ? 'bad' : 'good'}`} />
          <span className="eyebrow">TARGETER HEARTBEAT · HEURISTIC</span>
          <h2>
            {stalled
              ? 'Run cadence appears stalled'
              : 'Run cadence appears active'}
          </h2>
          <p>
            Latest committed run age is compared with 2× the expected run
            cadence ({s.expectedRunSeconds}s). This is an observability
            heuristic, not scheduler proof.
          </p>
        </div>
        <div className="health">
          <b>{s.stale ? 'STALE' : 'CURRENT'}</b>
          <span>Archive snapshot</span>
          <small>Updated {date(s.lastSuccessfulRefresh)}</small>
          <small>Source: {s.source.toUpperCase()}</small>
        </div>
      </section>
      <section>
        <Title
          title="Committed run timeline"
          sub="Latest five, ordered by parsed run ID — never S3 LastModified"
        />
        <div className="timeline">
          {s.runs.map((r, i) => (
            <article key={r.runId} className={i === 0 ? 'latest' : ''}>
              <span>{i === 0 ? 'LATEST' : 'RUN'}</span>
              <b>{r.runId}</b>
              <small>{date(r.generatedAt)}</small>
              <em className={r.inputComplete ? 'ok' : 'warn'}>
                {r.inputComplete ? 'Complete' : 'Incomplete'}
              </em>
            </article>
          ))}
        </div>
      </section>
      {run ? (
        <>
          <Metrics run={run} />
          <Bundles run={run} />
          <Rejections run={run} />
        </>
      ) : (
        <section className="empty">No committed manifests found.</section>
      )}
    </div>
  );
}
function Metrics({ run }: { run: RunView }) {
  const m = run.summary;
  return (
    <section>
      <Title title="Latest run" sub={run.runId} />
      <div className="metrics">
        <Metric n={m.selected} label="Selected bundles" />
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
function Metric({ n, label }: { n: any; label: string }) {
  return (
    <div className="metric">
      <strong>{n}</strong>
      <span>{label}</span>
    </div>
  );
}
function Bundles({ run }: { run: RunView }) {
  const selected = new Set(list(run.report.selection?.bundle_ids));
  const cs = list(run.report.candidates).filter((c) =>
    selected.has(c.bundle_id),
  );
  return (
    <section>
      <Title
        title="Selected bundles"
        sub="Capture surface and admission evidence"
      />
      <div className="cards">
        {cs.map((c) => (
          <article className="bundle" key={c.bundle_id}>
            <div className="row">
              <span className="tag">{val(c.sport)}</span>
              <span className="muted">{list(c.venues).join(' · ')}</span>
            </div>
            <h3>{list(c.participants).join(' vs ') || c.bundle_id}</h3>
            <dl>
              <dt>Activation</dt>
              <dd>{date(c.activation_at)}</dd>
              <dt>Capture</dt>
              <dd>{date(c.capture_start_at)}</dd>
              <dt>Volume gate</dt>
              <dd>
                {val(c.admission?.combined_moneyline_volume_usd)} /{' '}
                {val(c.admission?.minimum_moneyline_volume_usd)} USD
              </dd>
              <dt>Markets</dt>
              <dd>{list(c.eligible_market_ids).length}</dd>
            </dl>
            <p>
              <b>Relationships:</b>{' '}
              {list(c.relationship_analysis?.relationships)
                .map((x: any) => x.relationship)
                .filter(Boolean)
                .join(', ') || 'None recorded'}
            </p>
            <p>
              <b>Why chosen:</b> Passed event admission gates and fit the
              configured allocation limits. Diagnostic score: {val(c.score)}.
            </p>
          </article>
        ))}
      </div>
      {!cs.length && <p className="empty">No bundles selected in this run.</p>}
    </section>
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
        return list(r.report.candidates).map((c) => {
          const admissionReasons = list(c.rejection_reasons).map(String);
          const allocationReason = allocations[c.bundle_id];
          const decision = selected.has(c.bundle_id)
            ? 'selected'
            : admissionReasons.length
              ? 'rejected'
              : 'not-selected';
          return {
            r,
            c,
            decision,
            reasons: admissionReasons.length
              ? admissionReasons
              : allocationReason
                ? [String(allocationReason)]
                : ['eligible_not_selected'],
          };
        });
      }),
    [s],
  );
  const reasons = [...new Set(rows.flatMap((x) => x.reasons))];
  const shown = rows.filter((x) => {
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
  });
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
                className={`decision ${decision === 'selected' ? 'ok' : 'warn'}`}
              >
                {decision === 'selected'
                  ? 'SELECTED'
                  : decision === 'rejected'
                    ? 'REJECTED'
                    : 'NOT ALLOCATED'}
              </span>
              <b>{list(c.participants).join(' vs ') || c.bundle_id}</b>
              <span>
                {val(c.event_status)} · {r.runId}
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
