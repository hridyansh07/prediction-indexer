import React, { useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  BrowserRouter,
  Navigate,
  NavLink,
  Route,
  Routes,
} from 'react-router-dom';
import type { UniverseCadence, UniverseHealth } from '../event-universe';
import { EventUniversePage } from './event-universe';
import { DecisionsPage, StatusPage, TargetsPage } from './observability';
import './style.css';

const ROOT = '/api/event-universe';

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${ROOT}${path}`, {
    method: 'GET',
    headers: { accept: 'application/json' },
  });
  if (!response.ok) throw new Error('Event Universe is unavailable.');
  return response.json() as Promise<T>;
}

function App() {
  const [health, setHealth] = useState<UniverseHealth | null>(null);
  const [cadence, setCadence] = useState<UniverseCadence | null>(null);
  const [healthError, setHealthError] = useState('');
  const [cadenceError, setCadenceError] = useState('');
  const [refreshing, setRefreshing] = useState(false);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    const [nextHealth, nextCadence] = await Promise.allSettled([
      get<UniverseHealth>('/healthz'),
      get<UniverseCadence>('/v1/targeter/status?limit=5'),
    ]);
    if (nextHealth.status === 'fulfilled') {
      setHealth(nextHealth.value);
      setHealthError('');
    } else {
      setHealthError('Server status is unavailable.');
    }
    if (nextCadence.status === 'fulfilled') {
      setCadence(nextCadence.value);
      setCadenceError('');
    } else {
      setCadenceError('Targeter cadence is unavailable.');
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
                cadence={cadence}
                healthError={healthError}
                cadenceError={cadenceError}
                refreshing={refreshing}
                refresh={refresh}
              />
            }
          />
          <Route
            path="/targets"
            element={<TargetsPage cadence={cadence} error={cadenceError} />}
          />
          <Route path="/history" element={<EventUniversePage />} />
          <Route
            path="/decisions"
            element={<DecisionsPage cadence={cadence} error={cadenceError} />}
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
