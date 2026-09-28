import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  Link,
  NavLink,
  useNavigate,
  useLocation,
  useParams,
  useSearchParams,
} from 'react-router-dom';
import {
  findReplayJob,
  jobProgress,
  parseStrategyConfig,
  replayJobEvents,
  replayJobs,
  replayStages,
  replayStatusMeta,
  type ReplayForm,
  type ReplayJob,
  type ReplayStatus,
  validateReplayForm,
} from './replay-model';
import { useUniverseAuth } from './universe-auth';
import {
  UniverseApiError,
  type AllowlistMember,
  type ReplayJobDetail,
  type ReplayJobEventRecord,
  type ReplayJobRecord,
  type ReplayRequest,
  type ReplayStrategy,
  startSerializedPolling,
} from './universe-client';

const replayFixturesEnabled = import.meta.env.VITE_REPLAY_FIXTURES === 'true';

const displayDate = (value: string | null) =>
  value
    ? new Date(value).toLocaleString(undefined, {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
      })
    : '—';

const nsDate = (value: string | null) =>
  value === null
    ? null
    : new Date(Number(BigInt(value) / 1_000_000n)).toISOString();

const shortAddress = (address: string) =>
  `${address.slice(0, 6)}…${address.slice(-4)}`;

const errorMessage = (error: unknown) => {
  if (error instanceof UniverseApiError) {
    if (error.status === 429 && error.retryAfterSeconds !== null)
      return `${error.message} Try again in ${error.retryAfterSeconds} seconds.`;
    return error.message;
  }
  return 'The request could not be completed.';
};

const label = (value: string) =>
  value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase());

function useModalFocus(open: boolean, close: () => void) {
  const dialog = useRef<HTMLDivElement>(null);
  const closeRef = useRef(close);
  closeRef.current = close;
  useEffect(() => {
    if (!open || !dialog.current) return;
    const opener =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    const element = dialog.current;
    element.focus();
    const keydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closeRef.current();
        return;
      }
      if (event.key !== 'Tab') return;
      const focusable = [
        ...element.querySelectorAll<HTMLElement>(
          'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])',
        ),
      ];
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable.at(-1)!;
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
      if (opener?.isConnected) opener.focus();
    };
  }, [open]);
  return dialog;
}

function MobileReplayNotice() {
  return (
    <div className="replay-mobile-notice">
      <span className="brand-mark" aria-hidden="true">
        ◇
      </span>
      <h1>Replay on mobile — Coming soon</h1>
      <p>Use a desktop browser to create replays and inspect job evidence.</p>
    </div>
  );
}

function AuthState({ state }: { state: 'entry' | 'expired' }) {
  const { signIn, signingIn, error } = useUniverseAuth();
  return (
    <section className="replay-auth-card" aria-labelledby="replay-auth-title">
      <span className="auth-glyph" aria-hidden="true">
        {state === 'expired' ? '↻' : '⌁'}
      </span>
      <span className="eyebrow">REPLAY ACCESS</span>
      <h1 id="replay-auth-title">
        {state === 'expired' ? 'Your session expired' : 'Sign in to use Replay'}
      </h1>
      <p>
        {state === 'expired'
          ? 'Sign the wallet message again to continue. Your jobs and archived evidence are unchanged.'
          : 'Connect an allowlisted wallet and sign a message. No transaction or wallet mutation is requested.'}
      </p>
      <button
        className="primary-button"
        type="button"
        disabled={signingIn}
        onClick={() => void signIn().catch(() => undefined)}
      >
        {signingIn
          ? 'Waiting for wallet…'
          : state === 'expired'
            ? 'Sign in again'
            : 'Connect wallet'}
      </button>
      {error && <small role="alert">{error}</small>}
      <small>No transaction or wallet mutation is requested.</small>
    </section>
  );
}

function ReplayLocalNav() {
  const { session } = useUniverseAuth();
  const [params] = useSearchParams();
  const fixtureAdmin =
    replayFixturesEnabled &&
    params.has('state') &&
    params.get('state') !== 'forbidden';
  return (
    <nav className="replay-local-nav" aria-label="Replay navigation">
      <NavLink to="/replay" end>
        Jobs
      </NavLink>
      {(session?.role === 'admin' || fixtureAdmin) && (
        <NavLink
          to={fixtureAdmin ? '/replay/admin?state=ready' : '/replay/admin'}
        >
          Access
        </NavLink>
      )}
    </nav>
  );
}

export function ReplayLayout({ children }: { children: React.ReactNode }) {
  const [params] = useSearchParams();
  const auth = params.get('auth');
  const { expired } = useUniverseAuth();
  return (
    <div className="replay-route">
      <MobileReplayNotice />
      <div className="replay-desktop">
        <ReplayLocalNav />
        {auth === 'entry' ||
        (replayFixturesEnabled && auth === 'expired') ||
        expired ? (
          <AuthState
            state={
              (replayFixturesEnabled && auth === 'expired') || expired
                ? 'expired'
                : 'entry'
            }
          />
        ) : (
          children
        )}
      </div>
    </div>
  );
}

function StatusPill({ status }: { status: ReplayStatus }) {
  const meta = replayStatusMeta[status];
  return (
    <span className={`replay-status ${meta.tone}`}>
      <span aria-hidden="true">{meta.symbol}</span>
      {meta.label}
    </span>
  );
}

function ReplayOutputControl() {
  return (
    <span className="coming-soon-wrap">
      <button
        className="output-control"
        type="button"
        aria-describedby="output-coming-soon"
      >
        Replay output
      </button>
      <span className="coming-soon-tip" id="output-coming-soon" role="tooltip">
        Coming soon
      </span>
    </span>
  );
}

function LoadingRows() {
  return (
    <div
      className="replay-job-table"
      aria-busy="true"
      aria-label="Loading replay jobs"
    >
      {[0, 1, 2].map((row) => (
        <div className="replay-job-row skeleton-row" key={row}>
          <span />
          <span />
          <span />
          <span />
        </div>
      ))}
    </div>
  );
}

function JobRow({ job }: { job: ReplayJob }) {
  return (
    <Link className="replay-job-row" to={`/replay/jobs/${job.id}`}>
      <span className="job-event-cell">
        <b>{job.event}</b>
        <small>{job.bundle}</small>
      </span>
      <StatusPill status={job.status} />
      <span>
        <b>{job.strategyLabel}</b>
        <small>
          {job.markets === 'all'
            ? 'All markets'
            : `${job.markets.length} markets`}
        </small>
      </span>
      <span className="job-created">{displayDate(job.createdAt)}</span>
      <span className="chevron" aria-hidden="true">
        ›
      </span>
    </Link>
  );
}

function LiveJobRow({ job }: { job: ReplayJobRecord }) {
  return (
    <Link className="replay-job-row" to={`/replay/jobs/${job.job_id}`}>
      <span className="job-event-cell">
        <b>{job.job_id}</b>
        <small>Submitted by {shortAddress(job.submitted_by)}</small>
      </span>
      <StatusPill status={job.status} />
      <span>
        <b>{job.stage ? label(job.stage) : 'Waiting'}</b>
        <small>{job.stage_attempts} stage attempts</small>
      </span>
      <span className="job-created">
        {displayDate(nsDate(job.created_at_ns))}
      </span>
      <span className="chevron" aria-hidden="true">
        ›
      </span>
    </Link>
  );
}

export function ReplayJobsPage() {
  const [params] = useSearchParams();
  const view = replayFixturesEnabled ? params.get('view') : null;
  const { client: api } = useUniverseAuth();
  const [status, setStatus] = useState('all');
  const [jobs, setJobs] = useState<ReplayJobRecord[] | null>(null);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [loadedPages, setLoadedPages] = useState(1);
  const [loadingMore, setLoadingMore] = useState(false);
  const [pageFailure, setPageFailure] = useState<string | null>(null);
  const [failure, setFailure] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const load = useCallback(
    async (signal?: AbortSignal) => {
      setFailure(null);
      try {
        const merged: ReplayJobRecord[] = [];
        let after: string | null = null;
        let pagesRead = 0;
        while (pagesRead < loadedPages) {
          const page = await api.jobs(
            status === 'all' ? undefined : (status as ReplayStatus),
            after,
            signal,
          );
          merged.push(...page.jobs);
          after = page.next_cursor;
          pagesRead += 1;
          if (!after) break;
        }
        if (signal?.aborted) return;
        setJobs(uniqueJobs(merged));
        setNextCursor(after);
        setPageFailure(null);
      } catch (error) {
        if (!(error instanceof Error && error.name === 'AbortError'))
          setFailure(errorMessage(error));
      }
    },
    [api, loadedPages, status],
  );
  useEffect(() => {
    setJobs(null);
    setNextCursor(null);
    setLoadedPages(1);
    setFailure(null);
    setPageFailure(null);
  }, [status]);
  useEffect(() => {
    if (view) return;
    return startSerializedPolling(load, 10_000);
  }, [load, refreshKey, view]);
  const loadMore = async () => {
    if (!nextCursor || loadingMore) return;
    setLoadingMore(true);
    setPageFailure(null);
    try {
      const page = await api.jobs(
        status === 'all' ? undefined : (status as ReplayStatus),
        nextCursor,
      );
      setJobs((current) => uniqueJobs([...(current ?? []), ...page.jobs]));
      setNextCursor(page.next_cursor);
      setLoadedPages((current) => current + 1);
    } catch (error) {
      setPageFailure(errorMessage(error));
    } finally {
      setLoadingMore(false);
    }
  };
  const fixtureVisible =
    status === 'all'
      ? replayJobs
      : replayJobs.filter((job) => job.status === status);
  const count = view ? fixtureVisible.length : (jobs?.length ?? 0);
  return (
    <ReplayLayout>
      <div className="replay-page-head split-heading">
        <div>
          <span className="eyebrow">REPLAY WORKSPACE</span>
          <h1>Historical replay jobs</h1>
          <p>
            Run strategies against immutable captured evidence and follow every
            durable transition.
          </p>
        </div>
        <Link className="primary-button" to="/replay/new">
          New replay
        </Link>
      </div>
      <div className="replay-toolbar">
        <label>
          <span className="sr-only">Filter by status</span>
          <select
            value={status}
            onChange={(event) => setStatus(event.target.value)}
          >
            <option value="all">All statuses</option>
            {Object.entries(replayStatusMeta).map(([value, meta]) => (
              <option value={value} key={value}>
                {meta.label}
              </option>
            ))}
          </select>
        </label>
        <span>{count} jobs</span>
      </div>
      {view === 'loading' || (!view && jobs === null && !failure) ? (
        <LoadingRows />
      ) : view === 'error' || (failure && jobs === null) ? (
        <div className="replay-state-card negative" role="alert">
          <b>Replay jobs could not be loaded</b>
          <span>
            {failure ?? 'The request failed before any job data was changed.'}
          </span>
          <button
            className="quiet-button"
            type="button"
            onClick={() => {
              setJobs(null);
              setRefreshKey((current) => current + 1);
            }}
          >
            Try again
          </button>
        </div>
      ) : view === 'empty' ||
        (view ? !fixtureVisible.length : !jobs?.length) ? (
        <div className="replay-state-card">
          <span className="state-glyph" aria-hidden="true">
            ◇
          </span>
          <b>No replay jobs yet</b>
          <span>
            Create a replay from a retired historical bundle to begin.
          </span>
          <Link className="quiet-button" to="/replay/new">
            Create replay
          </Link>
        </div>
      ) : (
        <>
          {failure && (
            <div className="inline-error" role="status">
              Refresh failed: {failure}
            </div>
          )}
          <div className="replay-job-table">
            <div className="replay-job-head" aria-hidden="true">
              <span>Event</span>
              <span>Status</span>
              <span>{view ? 'Strategy' : 'Stage'}</span>
              <span>Created</span>
              <span />
            </div>
            {view
              ? fixtureVisible.map((job) => <JobRow job={job} key={job.id} />)
              : jobs?.map((job) => <LiveJobRow job={job} key={job.job_id} />)}
          </div>
          {!view && (nextCursor || pageFailure) && (
            <div className="pagination-stack">
              {pageFailure && (
                <span className="field-error" role="alert">
                  {pageFailure}
                </span>
              )}
              {nextCursor && (
                <button
                  className="quiet-button"
                  type="button"
                  disabled={loadingMore}
                  onClick={() => void loadMore()}
                >
                  {loadingMore ? 'Loading…' : 'Load older jobs'}
                </button>
              )}
            </div>
          )}
        </>
      )}
    </ReplayLayout>
  );
}

function uniqueJobs(jobs: ReplayJobRecord[]) {
  const seen = new Set<string>();
  return jobs.filter((job) => {
    if (seen.has(job.job_id)) return false;
    seen.add(job.job_id);
    return true;
  });
}

function JobProgress({ job }: { job: ReplayJob }) {
  const progress = jobProgress(job);
  return (
    <section
      className="replay-panel progress-panel"
      aria-labelledby="progress-heading"
    >
      <div className="panel-heading">
        <div>
          <span className="eyebrow">PROGRESS</span>
          <h2 id="progress-heading">Replay pipeline</h2>
        </div>
        <strong>{progress}%</strong>
      </div>
      <div
        className="progress-track"
        role="progressbar"
        aria-valuenow={progress}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label="Replay progress"
      >
        <span style={{ width: `${progress}%` }} />
      </div>
      <ol className="stage-list">
        {replayStages.map((stage, index) => {
          const current = job.stage === stage && job.status === 'running';
          const complete =
            progress === 100 ||
            (job.stage ? index < replayStages.indexOf(job.stage) : false);
          return (
            <li
              className={current ? 'current' : complete ? 'complete' : ''}
              key={stage}
            >
              <span aria-hidden="true">
                {complete ? '✓' : current ? '↻' : index + 1}
              </span>
              <b>{label(stage)}</b>
              <small>
                {current ? 'In progress' : complete ? 'Complete' : 'Waiting'}
              </small>
            </li>
          );
        })}
      </ol>
    </section>
  );
}

function JobEvents({
  job,
  events,
}: {
  job: ReplayJob;
  events?: ReplayJobEventRecord[];
}) {
  const visibleEvents = events
    ? events.map((event) => ({
        id: Number(event.event_id),
        title: label(event.event_type),
        detail: `${replayStatusMeta[event.status].label}${event.stage ? ` · ${label(event.stage)}` : ''}`,
        at: nsDate(event.created_at_ns)!,
        tone: replayStatusMeta[event.status].tone,
      }))
    : replayJobEvents(job);
  return (
    <section className="replay-panel" aria-labelledby="events-heading">
      <div className="panel-heading">
        <div>
          <span className="eyebrow">EVENTS</span>
          <h2 id="events-heading">Job history</h2>
        </div>
      </div>
      <ol className="job-event-list">
        {visibleEvents.map((event) => (
          <li key={event.id} className={event.tone}>
            <span className="event-dot" aria-hidden="true" />
            <div>
              <b>{event.title}</b>
              <p>{event.detail}</p>
            </div>
            <time dateTime={event.at}>{displayDate(event.at)}</time>
          </li>
        ))}
      </ol>
    </section>
  );
}

export function ReplayJobPage() {
  const { jobId = '' } = useParams();
  const [params] = useSearchParams();
  const fixtureState = replayFixturesEnabled ? params.get('state') : null;
  const { client: api, session } = useUniverseAuth();
  const [detail, setDetail] = useState<ReplayJobDetail | null>(null);
  const [events, setEvents] = useState<ReplayJobEventRecord[]>([]);
  const [eventCursor, setEventCursor] = useState<string | null>(null);
  const [eventPages, setEventPages] = useState(1);
  const [loadingEvents, setLoadingEvents] = useState(false);
  const [eventFailure, setEventFailure] = useState<string | null>(null);
  const [catalogue, setCatalogue] = useState<ReplayStrategy[]>([]);
  const [failure, setFailure] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [cancelOpen, setCancelOpen] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);
  const cancelDialog = useModalFocus(cancelOpen, () => setCancelOpen(false));
  const load = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const next = await api.job(jobId, signal);
        const merged: ReplayJobEventRecord[] = [];
        let cursor: string | null = null;
        let pagesRead = 0;
        while (pagesRead < eventPages) {
          const history = await api.jobEvents(jobId, cursor, signal);
          merged.push(...history.events);
          cursor = history.next_cursor;
          pagesRead += 1;
          if (!cursor) break;
        }
        if (signal?.aborted) return;
        setDetail(next);
        setEvents(uniqueEvents(merged));
        setEventCursor(cursor);
        setEventFailure(null);
        setFailure(null);
      } catch (error) {
        if (!(error instanceof Error && error.name === 'AbortError'))
          setFailure(errorMessage(error));
      }
    },
    [api, eventPages, jobId],
  );
  useEffect(() => {
    if (fixtureState) return;
    return startSerializedPolling(load, 5_000);
  }, [fixtureState, load, refreshKey, session?.address]);
  useEffect(() => {
    if (fixtureState) return;
    const controller = new AbortController();
    api
      .strategies(controller.signal)
      .then((next) => {
        if (!controller.signal.aborted) setCatalogue(next.strategies);
      })
      .catch(() => undefined);
    return () => controller.abort();
  }, [api, fixtureState, jobId, session?.address]);
  const loadMoreEvents = async () => {
    if (!eventCursor || loadingEvents) return;
    setLoadingEvents(true);
    setEventFailure(null);
    try {
      const page = await api.jobEvents(jobId, eventCursor);
      setEvents((current) => uniqueEvents([...current, ...page.events]));
      setEventCursor(page.next_cursor);
      setEventPages((current) => current + 1);
    } catch (error) {
      setEventFailure(errorMessage(error));
    } finally {
      setLoadingEvents(false);
    }
  };
  if (!fixtureState && failure && !detail)
    return (
      <ReplayLayout>
        <div className="replay-state-card negative" role="alert">
          <b>Replay job could not be loaded</b>
          <span>{failure}</span>
          <button
            className="quiet-button"
            type="button"
            onClick={() => {
              setDetail(null);
              setRefreshKey((current) => current + 1);
            }}
          >
            Try again
          </button>
        </div>
      </ReplayLayout>
    );
  if (!fixtureState && !detail)
    return (
      <ReplayLayout>
        <LoadingRows />
      </ReplayLayout>
    );
  const strategyName = detail?.request.strategy.name ?? 'bundle_coverage';
  const strategyLabel =
    catalogue.find((strategy) => strategy.name === strategyName)?.label ??
    label(strategyName);
  const job: ReplayJob = fixtureState
    ? findReplayJob(jobId, fixtureState)
    : {
        id: detail!.job_id,
        bundle: detail!.request.bundle_id,
        event: detail!.request.bundle_id,
        status: detail!.status,
        stage: detail!.stage,
        pendingOutcome: detail!.pending_outcome,
        strategy: strategyName,
        strategyLabel,
        markets: detail!.request.probe_markets ?? 'all',
        submittedBy: detail!.submitted_by,
        createdAt: nsDate(detail!.created_at_ns)!,
        startedAt: nsDate(detail!.started_at_ns),
        finishedAt: nsDate(detail!.finished_at_ns),
        attempts: detail!.stage_attempts,
        reasonCode: detail!.reason_code,
        reasonDetail: detail!.reason_detail,
        archiveReceipt: detail!.archive_receipt_key,
      };
  const meta = replayStatusMeta[job.status];
  const canCancel =
    job.status === 'queued' &&
    Boolean(
      session &&
        (session.role === 'admin' ||
          session.address.toLowerCase() === job.submittedBy.toLowerCase()),
    );
  const cancel = async () => {
    try {
      await api.cancel(job.id);
      setCancelOpen(false);
      setRefreshKey((current) => current + 1);
    } catch (error) {
      setCancelError(errorMessage(error));
    }
  };
  return (
    <ReplayLayout>
      <Link className="back-link" to="/replay">
        ← All jobs
      </Link>
      <div className="replay-page-head job-detail-head">
        <div>
          <span className="eyebrow">REPLAY JOB</span>
          <h1>{job.event}</h1>
          <code>{job.id}</code>
        </div>
        <div className="job-actions">
          <ReplayOutputControl />
          {canCancel && (
            <button
              className="danger-button"
              type="button"
              onClick={() => setCancelOpen(true)}
            >
              Cancel replay
            </button>
          )}
        </div>
      </div>
      <section
        className={`job-status-hero ${meta.tone}`}
        aria-label={`Status: ${meta.label}`}
      >
        <span className="status-symbol" aria-hidden="true">
          {meta.symbol}
        </span>
        <div>
          <StatusPill status={job.status} />
          <h2>{meta.description}</h2>
          {job.pendingOutcome && (
            <p>
              Pending outcome:{' '}
              <b>{replayStatusMeta[job.pendingOutcome].label}</b>
            </p>
          )}
          {job.reasonDetail && <p>{job.reasonDetail}</p>}
        </div>
      </section>
      <div className="replay-detail-grid">
        <div className="detail-main">
          {failure && (
            <div className="inline-error" role="status">
              Refresh failed: {failure}
            </div>
          )}
          <JobProgress job={job} />
          <JobEvents job={job} events={fixtureState ? undefined : events} />
          {!fixtureState && (eventCursor || eventFailure) && (
            <div className="pagination-stack">
              {eventFailure && (
                <span className="field-error" role="alert">
                  {eventFailure}
                </span>
              )}
              {eventCursor && (
                <button
                  className="quiet-button"
                  type="button"
                  disabled={loadingEvents}
                  onClick={() => void loadMoreEvents()}
                >
                  {loadingEvents ? 'Loading…' : 'Load more history'}
                </button>
              )}
            </div>
          )}
        </div>
        <aside className="detail-side" aria-label="Replay job metadata">
          <section className="replay-panel">
            <span className="eyebrow">REQUEST</span>
            <h2>Replay request</h2>
            <dl className="fact-list">
              <div>
                <dt>Bundle</dt>
                <dd>{job.bundle}</dd>
              </div>
              <div>
                <dt>Markets</dt>
                <dd>
                  {job.markets === 'all'
                    ? 'All bundle markets'
                    : job.markets.join(', ')}
                </dd>
              </div>
              <div>
                <dt>Strategy</dt>
                <dd>{job.strategyLabel}</dd>
              </div>
              <div>
                <dt>Time</dt>
                <dd>Full bundle history</dd>
              </div>
            </dl>
          </section>
          <section className="replay-panel">
            <span className="eyebrow">AUDIT</span>
            <h2>Durable metadata</h2>
            <dl className="fact-list">
              <div>
                <dt>Submitted by</dt>
                <dd>
                  <code>{job.submittedBy}</code>
                </dd>
              </div>
              <div>
                <dt>Created</dt>
                <dd>{displayDate(job.createdAt)}</dd>
              </div>
              <div>
                <dt>Attempts</dt>
                <dd>{job.attempts}</dd>
              </div>
              <div>
                <dt>Reason code</dt>
                <dd>
                  <code>{job.reasonCode ?? '—'}</code>
                </dd>
              </div>
              <div>
                <dt>Receipt</dt>
                <dd>
                  <code>{job.archiveReceipt ?? 'Pending'}</code>
                </dd>
              </div>
            </dl>
          </section>
        </aside>
      </div>
      {cancelOpen && (
        <div className="modal-layer">
          <button
            className="modal-backdrop"
            aria-label="Close cancellation dialog"
            onClick={() => setCancelOpen(false)}
          />
          <div
            ref={cancelDialog}
            className="confirm-dialog"
            role="dialog"
            aria-modal="true"
            aria-labelledby="cancel-title"
            tabIndex={-1}
          >
            <span className="warning-icon" aria-hidden="true">
              !
            </span>
            <h2 id="cancel-title">Cancel this queued replay?</h2>
            <p>
              The request will still be archived with a Cancelled outcome. No
              replay work has started.
            </p>
            {cancelError && (
              <p className="field-error" role="alert">
                {cancelError}
              </p>
            )}
            <div className="dialog-actions">
              <button
                className="quiet-button"
                type="button"
                onClick={() => setCancelOpen(false)}
              >
                Keep queued
              </button>
              <button
                className="danger-button"
                type="button"
                onClick={() => void cancel()}
              >
                Cancel replay
              </button>
            </div>
          </div>
        </div>
      )}
    </ReplayLayout>
  );
}

function uniqueEvents(events: ReplayJobEventRecord[]) {
  const seen = new Set<string>();
  return events.filter((event) => {
    if (seen.has(event.event_id)) return false;
    seen.add(event.event_id);
    return true;
  });
}

type BundleOption = {
  id: string;
  event: string;
  detail: string;
  markets: string[];
  latestRunId?: string;
};

const fixtureBundles: BundleOption[] = [
  {
    id: 'worlds-2026-quarterfinal-2',
    event: 'Gen.G vs Bilibili Gaming',
    detail: 'League of Legends · Oct 18, 2026',
    markets: [
      'Polymarket · Match winner',
      'Kalshi · Series winner',
      'Polymarket · Map 1 winner',
    ],
  },
  {
    id: 'ti-2026-lower-bracket-final',
    event: 'Team Spirit vs PARIVISION',
    detail: 'Dota 2 · Sep 12, 2026',
    markets: ['Kalshi · Series winner', 'Polymarket · Match winner'],
  },
  {
    id: 'iem-cologne-2026-final',
    event: 'Team Vitality vs MOUZ',
    detail: 'Counter-Strike 2 · Aug 23, 2026',
    markets: [
      'Polymarket · Match winner',
      'Kalshi · Match winner',
      'Polymarket · Map handicap',
    ],
  },
];

export function NewReplayPage() {
  const { client: api, session } = useUniverseAuth();
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const fixture = replayFixturesEnabled && params.get('fixture') === 'true';
  const [bundleOptions, setBundleOptions] = useState<BundleOption[]>(
    fixture ? fixtureBundles : [],
  );
  const [strategies, setStrategies] = useState<ReplayStrategy[]>([]);
  const [limits, setLimits] = useState<string[]>([]);
  const [loading, setLoading] = useState(!fixture);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadWarning, setLoadWarning] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [limitPreset, setLimitPreset] = useState('');
  const [strategyValues, setStrategyValues] = useState<Record<string, string>>(
    {},
  );
  const retryRef = useRef<{ fingerprint: string; key: string } | null>(null);
  const [form, setForm] = useState<ReplayForm>({
    bundleId: '',
    marketMode: 'all',
    selectedMarkets: [],
    strategy: '',
  });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState(false);
  useEffect(() => {
    if (fixture) {
      setStrategies([
        {
          name: 'bundle_coverage',
          label: 'Bundle coverage',
          description: 'Evaluates historical coverage for the selected bundle.',
          status: 'active',
          config_keys: [],
        },
      ]);
      setLimits(['small']);
      return;
    }
    const controller = new AbortController();
    Promise.all([
      api.bundles(controller.signal),
      api.strategies(controller.signal),
    ])
      .then(([bundlePage, catalogue]) => {
        setBundleOptions(
          bundlePage.bundles
            .filter((bundle) => bundle.lifecycle === 'retired')
            .map((bundle) => ({
              id: bundle.bundle_id,
              event: bundle.participants.join(' vs ') || bundle.bundle_id,
              detail: `${label(bundle.game ?? bundle.sport)} · ${displayDate(bundle.activation_at)}`,
              latestRunId: bundle.latest_run_id,
              markets: [] as string[],
            })),
        );
        setStrategies(catalogue.strategies);
        setLimits(catalogue.limits);
        setLoadError(null);
        setLoadWarning(bundlePage.warning);
      })
      .catch((error) => setLoadError(errorMessage(error)))
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [api, fixture]);
  const selectedBundle = bundleOptions.find(
    (bundle) => bundle.id === form.bundleId,
  );
  useEffect(() => {
    if (fixture || !selectedBundle?.latestRunId) return;
    const controller = new AbortController();
    api
      .selection(
        selectedBundle.latestRunId,
        selectedBundle.id,
        controller.signal,
      )
      .then((selection) => {
        const markets = selection.context.targets
          .map((target) => target.target_id)
          .sort();
        setBundleOptions((current) =>
          current.map((bundle) =>
            bundle.id === selectedBundle.id ? { ...bundle, markets } : bundle,
          ),
        );
      })
      .catch((error) => setLoadError(errorMessage(error)));
    return () => controller.abort();
  }, [api, fixture, selectedBundle?.id, selectedBundle?.latestRunId]);
  const update = (next: Partial<ReplayForm>) =>
    setForm((current) => ({ ...current, ...next }));
  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    const nextErrors = validateReplayForm(form);
    if (!limitPreset) nextErrors.limits = 'Choose a backend run preset.';
    const selectedStrategy = strategies.find(
      (strategy) => strategy.name === form.strategy,
    );
    const parsedConfig = parseStrategyConfig(
      selectedStrategy?.config_keys ?? [],
      strategyValues,
    );
    for (const [key, message] of Object.entries(parsedConfig.errors))
      nextErrors[`config.${key}`] = message;
    setErrors(nextErrors);
    if (Object.keys(nextErrors).length || !session) {
      if (!session)
        setSubmitError(
          'Sign in with an allowlisted wallet to create a replay.',
        );
      return;
    }
    const request: ReplayRequest = {
      replay_request_version: 1,
      bundle_id: form.bundleId,
      probe_markets:
        form.marketMode === 'all' ? null : [...form.selectedMarkets].sort(),
      interval: null,
      strategy: { name: form.strategy, config: parsedConfig.config },
      limits: limitPreset,
    };
    const fingerprint = JSON.stringify(request);
    if (!retryRef.current || retryRef.current.fingerprint !== fingerprint)
      retryRef.current = { fingerprint, key: crypto.randomUUID() };
    setSubmitting(true);
    setSubmitError(null);
    try {
      const result = await api.submit(request, retryRef.current.key);
      setSubmitted(true);
      retryRef.current = null;
      navigate(`/replay/jobs/${result.job_id}`);
    } catch (error) {
      setSubmitError(errorMessage(error));
    } finally {
      setSubmitting(false);
    }
  };
  if (loading)
    return (
      <ReplayLayout>
        <LoadingRows />
      </ReplayLayout>
    );
  if (loadError)
    return (
      <ReplayLayout>
        <div className="replay-state-card negative" role="alert">
          <b>Replay options could not be loaded</b>
          <span>{loadError}</span>
        </div>
      </ReplayLayout>
    );
  return (
    <ReplayLayout>
      <Link className="back-link" to="/replay">
        ← All jobs
      </Link>
      <div className="replay-page-head">
        <span className="eyebrow">NEW REPLAY</span>
        <h1>Configure historical replay</h1>
        <p>
          Choose evidence and an active backend strategy. Compatibility is
          checked when the request is submitted.
        </p>
      </div>
      {loadWarning && (
        <div className="inline-warning" role="status">
          {loadWarning}
        </div>
      )}
      <form className="new-replay-layout" onSubmit={submit} noValidate>
        <div className="new-replay-sections">
          <fieldset
            className="replay-form-section"
            aria-describedby={errors.bundleId ? 'bundle-error' : undefined}
          >
            <legend>
              <span>1</span>
              <b>Historical bundle</b>
            </legend>
            <p>Choose one retired Event Universe bundle.</p>
            <label className="field-label" htmlFor="bundle">
              Bundle
            </label>
            <select
              id="bundle"
              value={form.bundleId}
              aria-invalid={Boolean(errors.bundleId)}
              onChange={(event) =>
                update({ bundleId: event.target.value, selectedMarkets: [] })
              }
            >
              <option value="">Select a historical bundle</option>
              {bundleOptions.map((bundle) => (
                <option value={bundle.id} key={bundle.id}>
                  {bundle.event} — {bundle.detail}
                </option>
              ))}
            </select>
            {errors.bundleId && (
              <span className="field-error" id="bundle-error">
                {errors.bundleId}
              </span>
            )}
            {selectedBundle && (
              <div className="selection-preview">
                <b>{selectedBundle.event}</b>
                <span>{selectedBundle.detail}</span>
                <code>{selectedBundle.id}</code>
              </div>
            )}
          </fieldset>
          <fieldset className="replay-form-section">
            <legend>
              <span>2</span>
              <b>Markets</b>
            </legend>
            <p>
              Use every listed market, or select a smaller probe set. The
              strategy decides compatibility.
            </p>
            <div className="choice-grid">
              <label
                className={
                  form.marketMode === 'all'
                    ? 'choice-card selected'
                    : 'choice-card'
                }
              >
                <input
                  type="radio"
                  name="market-mode"
                  checked={form.marketMode === 'all'}
                  onChange={() => update({ marketMode: 'all' })}
                />
                <span>
                  <b>All markets</b>
                  <small>Replay every market in the bundle</small>
                </span>
              </label>
              <label
                className={
                  form.marketMode === 'selected'
                    ? 'choice-card selected'
                    : 'choice-card'
                }
              >
                <input
                  type="radio"
                  name="market-mode"
                  checked={form.marketMode === 'selected'}
                  onChange={() => update({ marketMode: 'selected' })}
                />
                <span>
                  <b>Selected markets</b>
                  <small>Choose a focused probe set</small>
                </span>
              </label>
            </div>
            {form.marketMode === 'selected' && (
              <div
                className="market-checks"
                aria-describedby={errors.markets ? 'markets-error' : undefined}
              >
                {(selectedBundle?.markets ?? []).map((market) => (
                  <label key={market}>
                    <input
                      type="checkbox"
                      checked={form.selectedMarkets.includes(market)}
                      onChange={(event) =>
                        update({
                          selectedMarkets: event.target.checked
                            ? [...form.selectedMarkets, market]
                            : form.selectedMarkets.filter(
                                (item) => item !== market,
                              ),
                        })
                      }
                    />
                    {market}
                  </label>
                ))}
                {!selectedBundle && (
                  <span className="muted">
                    Choose a bundle to see its markets.
                  </span>
                )}
              </div>
            )}
            {errors.markets && (
              <span className="field-error" id="markets-error">
                {errors.markets}
              </span>
            )}
          </fieldset>
          <fieldset
            className="replay-form-section"
            aria-describedby={errors.strategy ? 'strategy-error' : undefined}
          >
            <legend>
              <span>3</span>
              <b>Strategy</b>
            </legend>
            <p>
              Active strategies are supplied by the Replay backend catalogue.
            </p>
            {strategies
              .filter((strategy) => strategy.status === 'active')
              .map((strategy) => (
                <label
                  className={
                    form.strategy === strategy.name
                      ? 'strategy-card selected'
                      : 'strategy-card'
                  }
                  key={strategy.name}
                >
                  <input
                    type="radio"
                    name="strategy"
                    value={strategy.name}
                    checked={form.strategy === strategy.name}
                    onChange={(event) => {
                      update({ strategy: event.target.value });
                      setStrategyValues({});
                    }}
                  />
                  <span className="strategy-icon" aria-hidden="true">
                    ⌁
                  </span>
                  <span>
                    <b>{strategy.label}</b>
                    <small>{strategy.description}</small>
                  </span>
                  <em>Active</em>
                </label>
              ))}
            {strategies
              .find((strategy) => strategy.name === form.strategy)
              ?.config_keys.map((key) => (
                <label className="field-label strategy-config" key={key}>
                  {label(key)}
                  <input
                    value={strategyValues[key] ?? ''}
                    aria-invalid={Boolean(errors[`config.${key}`])}
                    onChange={(event) =>
                      setStrategyValues((current) => ({
                        ...current,
                        [key]: event.target.value,
                      }))
                    }
                    placeholder="JSON value"
                  />
                  {errors[`config.${key}`] && (
                    <span className="field-error">
                      {errors[`config.${key}`]}
                    </span>
                  )}
                </label>
              ))}
            {errors.strategy && (
              <span className="field-error" id="strategy-error">
                {errors.strategy}
              </span>
            )}
            <label className="field-label" htmlFor="limit-preset">
              Backend run preset
            </label>
            <select
              id="limit-preset"
              value={limitPreset}
              aria-invalid={Boolean(errors.limits)}
              onChange={(event) => setLimitPreset(event.target.value)}
            >
              <option value="">Select a preset</option>
              {limits.map((preset) => (
                <option value={preset} key={preset}>
                  {preset}
                </option>
              ))}
            </select>
            {errors.limits && (
              <span className="field-error">{errors.limits}</span>
            )}
          </fieldset>
        </div>
        <aside className="request-review">
          <span className="eyebrow">REQUEST SUMMARY</span>
          <h2>Ready to replay</h2>
          <dl className="fact-list">
            <div>
              <dt>Bundle</dt>
              <dd>{selectedBundle?.event ?? 'Not selected'}</dd>
            </div>
            <div>
              <dt>Markets</dt>
              <dd>
                {form.marketMode === 'all'
                  ? 'All bundle markets'
                  : `${form.selectedMarkets.length} selected`}
              </dd>
            </div>
            <div>
              <dt>Strategy</dt>
              <dd>
                {strategies.find((strategy) => strategy.name === form.strategy)
                  ?.label ?? 'Not selected'}
              </dd>
            </div>
            <div>
              <dt>Time</dt>
              <dd>Full bundle history</dd>
            </div>
            <div>
              <dt>Preset</dt>
              <dd>{limitPreset || 'Not selected'}</dd>
            </div>
          </dl>
          <div className="fixed-note">
            <span aria-hidden="true">i</span>
            <p>
              <b>Time is fixed</b>The complete bundle interval is replayed.
              Runtime limits come only from the selected backend preset.
            </p>
          </div>
          <button
            className="primary-button full-button"
            type="submit"
            disabled={submitting || !limits.length}
          >
            {submitting ? 'Creating replay…' : 'Create replay'}
          </button>
          <p className="submit-note">
            Creates a durable queued request. An uncertain retry keeps the same
            idempotency key.
          </p>
          {submitError && (
            <div className="field-error" role="alert">
              {submitError}
            </div>
          )}
          {submitted && (
            <div className="form-success" role="status">
              ✓ Replay request is valid and ready to queue.
            </div>
          )}
        </aside>
      </form>
    </ReplayLayout>
  );
}

type Member = AllowlistMember & { added: string };
const initialMembers: Member[] = [
  {
    address: '0x7A421D4888A61E5D3945A7B6EAA973CBDBA819F2',
    note: 'Research team',
    added: 'Sep 18, 2026',
  },
  {
    address: '0x31D892040A09C4E52D91F1C0274BCE4C43A88A04',
    note: 'Strategy review',
    added: 'Sep 21, 2026',
  },
  {
    address: '0xB810996E320AD19B2F0077C8A995E7E43E80C216',
    note: 'Data operations',
    added: 'Sep 25, 2026',
  },
];

export function ReplayAdminPage() {
  const [params] = useSearchParams();
  const state = replayFixturesEnabled ? params.get('state') : null;
  const { client: api, session } = useUniverseAuth();
  const fixture = state !== null;
  const [members, setMembers] = useState(fixture ? initialMembers : []);
  const [membersLoaded, setMembersLoaded] = useState(fixture);
  const [failure, setFailure] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [query, setQuery] = useState('');
  const [address, setAddress] = useState('');
  const [note, setNote] = useState('');
  const [remove, setRemove] = useState<Member | null>(null);
  const removeDialog = useModalFocus(Boolean(remove), () => setRemove(null));
  const filtered = members.filter((member) =>
    `${member.address} ${member.note}`
      .toLowerCase()
      .includes(query.toLowerCase()),
  );
  const validAddress = /^0x[0-9a-fA-F]{40}$/.test(address);
  const loadMembers = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const result = await api.allowlist(signal);
        setMembers(
          result.members.map((member) => ({ ...member, added: 'Allowlisted' })),
        );
        setMembersLoaded(true);
        setFailure(null);
      } catch (error) {
        setFailure(errorMessage(error));
      }
    },
    [api],
  );
  useEffect(() => {
    if (fixture || session?.role !== 'admin') return;
    const controller = new AbortController();
    void loadMembers(controller.signal);
    return () => controller.abort();
  }, [fixture, loadMembers, session?.role]);
  const addMember = async () => {
    setBusy(true);
    try {
      await api.addMember(address, note.trim());
      setAddress('');
      setNote('');
      await loadMembers();
    } catch (error) {
      setFailure(errorMessage(error));
    } finally {
      setBusy(false);
    }
  };
  const removeMember = async () => {
    if (!remove) return;
    setBusy(true);
    try {
      await api.removeMember(remove.address);
      setRemove(null);
      await loadMembers();
    } catch (error) {
      setFailure(errorMessage(error));
    } finally {
      setBusy(false);
    }
  };
  if (state === 'forbidden' || (!fixture && session?.role !== 'admin'))
    return (
      <ReplayLayout>
        <div className="replay-state-card negative">
          <b>Admin access required</b>
          <span>
            Your wallet can use Replay jobs but cannot manage the allowlist.
          </span>
          <Link className="quiet-button" to="/replay">
            Return to jobs
          </Link>
        </div>
      </ReplayLayout>
    );
  return (
    <ReplayLayout>
      <div className="replay-page-head">
        <span className="eyebrow">ADMIN · ACCESS</span>
        <h1>Replay allowlist</h1>
        <p>Manage wallets that can authenticate and submit Replay jobs.</p>
      </div>
      <div className="admin-grid">
        <section
          className="replay-panel admin-list"
          aria-labelledby="members-heading"
        >
          <div className="panel-heading">
            <div>
              <h2 id="members-heading">Allowed wallets</h2>
              <p>
                {filtered.length} {filtered.length === 1 ? 'member' : 'members'}
                {query ? ` shown · ${members.length} total` : ''}
              </p>
            </div>
            <label className="admin-search">
              <span aria-hidden="true">⌕</span>
              <span className="sr-only">Search allowed wallets</span>
              <input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search address or note"
              />
            </label>
          </div>
          {state === 'loading' || (!fixture && !membersLoaded && !failure) ? (
            <LoadingRows />
          ) : state === 'error' || failure ? (
            <div className="replay-state-card negative" role="alert">
              <b>Allowlist unavailable</b>
              <span>{failure ?? 'Existing access has not changed.'}</span>
            </div>
          ) : filtered.length ? (
            <div className="member-list">
              {filtered.map((member) => (
                <div className="member-row" key={member.address}>
                  <span className="wallet-identicon" aria-hidden="true">
                    ◆
                  </span>
                  <span>
                    <code>{member.address}</code>
                    <small>
                      {member.note} · Added {member.added}
                    </small>
                  </span>
                  <button
                    className="text-danger"
                    type="button"
                    onClick={() => setRemove(member)}
                  >
                    Remove
                  </button>
                </div>
              ))}
            </div>
          ) : (
            <div className="replay-state-card">
              <b>No wallets match</b>
              <span>Try another address or note.</span>
            </div>
          )}
        </section>
        <aside className="replay-panel add-access">
          <span className="eyebrow">ADD MEMBER</span>
          <h2>Allow a wallet</h2>
          <p>
            Members can sign in, submit replay jobs, and cancel only their own
            queued jobs.
          </p>
          <label className="field-label" htmlFor="wallet-address">
            Wallet address
          </label>
          <input
            id="wallet-address"
            value={address}
            onChange={(event) => setAddress(event.target.value)}
            placeholder="0x…"
            aria-invalid={Boolean(address && !validAddress)}
          />
          {address && !validAddress && (
            <span className="field-error">
              Enter a 40-character Ethereum address.
            </span>
          )}
          <label className="field-label" htmlFor="access-note">
            Note
          </label>
          <input
            id="access-note"
            value={note}
            onChange={(event) => setNote(event.target.value)}
            placeholder="Team or purpose"
          />
          <button
            className="primary-button full-button"
            type="button"
            disabled={!validAddress || !note.trim() || busy}
            onClick={() =>
              fixture
                ? (setMembers((current) => [
                    ...current,
                    { address, note, added: 'Today' },
                  ]),
                  setAddress(''),
                  setNote(''))
                : void addMember()
            }
          >
            Add to allowlist
          </button>
          <hr />
          <div className="coming-soon-line">
            <span>
              <b>Access history</b>
              <small>Allowlist changes and session events</small>
            </span>
            <em>Coming soon</em>
          </div>
        </aside>
      </div>
      {remove && (
        <div className="modal-layer">
          <button
            className="modal-backdrop"
            aria-label="Close remove member dialog"
            onClick={() => setRemove(null)}
          />
          <div
            ref={removeDialog}
            className="confirm-dialog"
            role="alertdialog"
            aria-modal="true"
            aria-labelledby="remove-title"
            tabIndex={-1}
          >
            <span className="warning-icon" aria-hidden="true">
              !
            </span>
            <h2 id="remove-title">Remove wallet access?</h2>
            <p>
              <code>{remove.address}</code> will be removed from the allowlist.{' '}
              <b>
                All active sessions for this wallet will be revoked immediately.
              </b>
            </p>
            <div className="dialog-actions">
              <button
                className="quiet-button"
                type="button"
                onClick={() => setRemove(null)}
              >
                Keep access
              </button>
              <button
                className="danger-button"
                type="button"
                disabled={busy}
                onClick={() =>
                  fixture
                    ? (setMembers((current) =>
                        current.filter(
                          (member) => member.address !== remove.address,
                        ),
                      ),
                      setRemove(null))
                    : void removeMember()
                }
              >
                Remove and revoke
              </button>
            </div>
          </div>
        </div>
      )}
    </ReplayLayout>
  );
}

export function ReplayOutputPage() {
  const { jobId = '' } = useParams();
  return (
    <ReplayLayout>
      <Link className="back-link" to={`/replay/jobs/${jobId}`}>
        ← Back to job
      </Link>
      <div className="replay-state-card output-reserved">
        <span className="state-glyph" aria-hidden="true">
          ◇
        </span>
        <b>Replay output — Coming soon</b>
        <span>
          Results are not embedded in this design stage. The archived receipt
          remains the durable deliverable.
        </span>
      </div>
    </ReplayLayout>
  );
}

export function AccountMenu() {
  const location = useLocation();
  const { session, signOut } = useUniverseAuth();
  const [open, setOpen] = useState(false);
  const button = useRef<HTMLButtonElement>(null);
  const menu = useRef<HTMLDivElement>(null);
  const params = new URLSearchParams(location.search);
  const fixtureSignedIn =
    replayFixturesEnabled &&
    params.has('state') &&
    params.get('state') !== 'forbidden';
  const signedOut = !session && !fixtureSignedIn;
  const isAdmin = session?.role === 'admin' || fixtureSignedIn;
  const address =
    session?.address ?? '0x7A421D4888A61E5D3945A7B6EAA973CBDBA819F2';
  useEffect(() => {
    if (open)
      menu.current?.querySelector<HTMLElement>('[role="menuitem"]')?.focus();
  }, [open]);
  if (signedOut)
    return (
      <Link className="header-sign-in" to="/replay?auth=entry">
        Sign in
      </Link>
    );
  return (
    <div
      className="account-menu"
      onKeyDown={(event) => {
        if (event.key === 'Escape' && open) {
          setOpen(false);
          button.current?.focus();
        }
      }}
    >
      <button
        ref={button}
        className="account-trigger"
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
      >
        <span className="wallet-identicon" aria-hidden="true">
          ◆
        </span>
        <span>{shortAddress(address)}</span>
        <span aria-hidden="true">⌄</span>
      </button>
      {open && (
        <div ref={menu} className="account-popover" role="menu">
          <div className="account-summary">
            <span className="wallet-identicon" aria-hidden="true">
              ◆
            </span>
            <span>
              <b>{shortAddress(address)}</b>
              <small>{isAdmin ? 'Administrator' : 'Member'}</small>
            </span>
          </div>
          {isAdmin && (
            <NavLink
              role="menuitem"
              to="/replay/admin"
              onClick={() => setOpen(false)}
            >
              Access
            </NavLink>
          )}
          <button
            role="menuitem"
            type="button"
            onClick={() => {
              void navigator.clipboard.writeText(address);
              setOpen(false);
            }}
          >
            Copy address
          </button>
          <button
            role="menuitem"
            type="button"
            onClick={() => {
              setOpen(false);
              void signOut();
            }}
          >
            Sign out
          </button>
        </div>
      )}
    </div>
  );
}
