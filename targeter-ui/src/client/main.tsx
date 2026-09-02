import React, { useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  BrowserRouter,
  Navigate,
  NavLink,
  Route,
  Routes,
} from 'react-router-dom';
import type { UniverseTargeterStatus } from '../event-universe';
import { EventUniversePage } from './event-universe';
import { DecisionsPage, TargetsPage } from './observability';
import { universeGet } from './universe-api';
import './style.css';

function App() {
  const [status, setStatus] = useState<UniverseTargeterStatus | null>(null);
  const [statusError, setStatusError] = useState('');

  const refresh = useCallback(async () => {
    try {
      setStatus(
        await universeGet<UniverseTargeterStatus>(
          '/v1/targeter/status?limit=5',
        ),
      );
      setStatusError('');
    } catch {
      setStatusError('Targeter status is unavailable.');
    }
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
        </nav>
      </header>
      <main>
        <Routes>
          <Route
            path="/"
            element={<TargetsPage status={status} error={statusError} />}
          />
          <Route path="/targets" element={<Navigate to="/" replace />} />
          <Route path="/history" element={<EventUniversePage />} />
          <Route
            path="/decisions"
            element={<DecisionsPage status={status} error={statusError} />}
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
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
