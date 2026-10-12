import React, { lazy, Suspense } from 'react';
import { QueryClientProvider } from '@tanstack/react-query';
import { createRoot } from 'react-dom/client';
import {
  BrowserRouter,
  Navigate,
  NavLink,
  Route,
  Routes,
} from 'react-router-dom';
import { EventUniversePage } from './event-universe';
import { DecisionsPage, TargetsPage } from './observability';
import {
  AccountMenu,
  NewReplayPage,
  ReplayAdminPage,
  ReplayJobPage,
  ReplayJobsPage,
  ReplayOutputPage,
} from './replay';
import { UniverseAuthProvider } from './universe-auth';
import { createUniverseQueryClient } from './universe-queries';
import './style.css';

const queryClient = createUniverseQueryClient();
const ResearchCorpusPage = lazy(() =>
  import('./research/research-corpus').then((m) => ({
    default: m.ResearchCorpusPage,
  })),
);
const ResearchEventPage = lazy(() =>
  import('./research/research-event').then((m) => ({
    default: m.ResearchEventPage,
  })),
);
const researchFallback = (
  <div className="empty-state">Loading the research site…</div>
);

function App() {
  return (
    <div className="app-shell">
      <header className="app-header">
        <NavLink
          className="brand"
          to="/"
          aria-label="Prediction Indexer targets"
        >
          <span className="brand-mark" aria-hidden="true">
            ◇
          </span>
          <span>PREDICTION INDEXER</span>
        </NavLink>
        <nav aria-label="Main navigation">
          <NavLink to="/" end>
            Targets
          </NavLink>
          <NavLink to="/history">History</NavLink>
          <NavLink to="/decisions">Decisions</NavLink>
          <NavLink to="/replay">Replay</NavLink>
          <NavLink to="/research">Research</NavLink>
        </nav>
        <AccountMenu />
      </header>
      <main>
        <Routes>
          <Route path="/" element={<TargetsPage />} />
          <Route path="/targets" element={<Navigate to="/" replace />} />
          <Route path="/history" element={<EventUniversePage />} />
          <Route path="/decisions" element={<DecisionsPage />} />
          <Route
            path="/research"
            element={
              <Suspense fallback={researchFallback}>
                <ResearchCorpusPage />
              </Suspense>
            }
          />
          <Route
            path="/research/events/:bundleId"
            element={
              <Suspense fallback={researchFallback}>
                <ResearchEventPage />
              </Suspense>
            }
          />
          <Route path="/replay" element={<ReplayJobsPage />} />
          <Route path="/replay/new" element={<NewReplayPage />} />
          <Route path="/replay/admin" element={<ReplayAdminPage />} />
          <Route path="/replay/jobs/:jobId" element={<ReplayJobPage />} />
          <Route
            path="/replay/jobs/:jobId/output"
            element={<ReplayOutputPage />}
          />
          <Route path="/operations" element={<Navigate to="/" replace />} />
          <Route
            path="/operations/selections"
            element={<Navigate to="/decisions" replace />}
          />
          <Route
            path="/event-universe"
            element={<Navigate to="/history" replace />}
          />
          <Route
            path="/events"
            element={<Navigate to="/decisions" replace />}
          />
          <Route path="/config" element={<Navigate to="/" replace />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <UniverseAuthProvider>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </UniverseAuthProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
