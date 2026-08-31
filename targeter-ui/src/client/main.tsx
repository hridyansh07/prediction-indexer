import React, { useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  BrowserRouter,
  Navigate,
  NavLink,
  Route,
  Routes,
} from 'react-router-dom';
import type { UniverseHealth, UniverseTargeterStatus } from '../event-universe';
import { EventUniversePage } from './event-universe';
import { DecisionsPage, StatusPage, TargetsPage } from './observability';
import { universeGet } from './universe-api';
import './style.css';

function App() {
  const [health, setHealth] = useState<UniverseHealth | null>(null);
  const [status, setStatus] = useState<UniverseTargeterStatus | null>(null);
  const [healthError, setHealthError] = useState('');
  const [statusError, setStatusError] = useState('');
  const [refreshing, setRefreshing] = useState(false);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    const [nextHealth, nextStatus] = await Promise.allSettled([
      universeGet<UniverseHealth>('/healthz'),
      universeGet<UniverseTargeterStatus>('/v1/targeter/status?limit=5'),
    ]);
    if (nextHealth.status === 'fulfilled') {
      setHealth(nextHealth.value);
      setHealthError('');
    } else {
      setHealthError('Server status is unavailable.');
    }
    if (nextStatus.status === 'fulfilled') {
      setStatus(nextStatus.value);
      setStatusError('');
    } else {
      setStatusError('Targeter status is unavailable.');
    }
    setRefreshing(false);
  }, []);

  useEffect(() => {
    void refresh();
    const interval = window.setInterval(() => void refresh(), 60_000);
    return () => window.clearInterval(interval);
  }, [refresh]);

  return (
    <div className="app-shell">
      <header className="app-header">
        <NavLink
          className="brand"
          to="/"
          aria-label="Prediction Indexer status"
        >
          <span className="brand-mark" aria-hidden="true">
            ◇
          </span>
          <span>PREDICTION INDEXER</span>
        </NavLink>
        <nav aria-label="Main navigation">
          <NavLink to="/" end>
            Status
          </NavLink>
          <NavLink to="/targets">Targets</NavLink>
          <NavLink to="/history">History</NavLink>
          <NavLink to="/decisions">Decisions</NavLink>
        </nav>
      </header>
      <main>
        <Routes>
          <Route
            path="/"
            element={
              <StatusPage
                health={health}
                status={status}
                healthError={healthError}
                statusError={statusError}
                refreshing={refreshing}
                refresh={refresh}
              />
            }
          />
          <Route
            path="/targets"
            element={<TargetsPage status={status} error={statusError} />}
          />
          <Route path="/history" element={<EventUniversePage />} />
          <Route
            path="/decisions"
            element={<DecisionsPage status={status} error={statusError} />}
          />
          <Route
            path="/operations"
            element={<Navigate to="/targets" replace />}
          />
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
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
