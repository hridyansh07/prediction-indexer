import React from 'react';
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
import { createUniverseQueryClient } from './universe-queries';
import './style.css';

const queryClient = createUniverseQueryClient();

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
        </nav>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<TargetsPage />} />
          <Route path="/targets" element={<Navigate to="/" replace />} />
          <Route path="/history" element={<EventUniversePage />} />
          <Route path="/decisions" element={<DecisionsPage />} />
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
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>,
);
