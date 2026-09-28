# Targeter observability UI

Read-only React/Vite UI for current Targeter selections, historical Event
Universe bundles, and recent Targeter decision evidence. The current targets
explorer is the landing page; detailed views are desktop-first.

The browser hydrates every view directly from the public Universe HTTPS API.
The UI does not list an archive, download
Targeter reports, decode Zstandard, stage private files, or hold cloud-storage
credentials. Event Universe owns report verification and lifecycle projection.

## Public build configuration

[`targeter-ui/.env.example`](.env.example) contains the non-secret examples:

```text
VITE_UNIVERSE_API_BASE_URL=https://34-182-18-247.sslip.io
VITE_REPLAY_SIWE_DOMAIN=<the exact domain in Universe replay.auth.siwe_domain>
VITE_REPLAY_SIWE_URI=<the exact canonical URI in Universe replay.auth.siwe_uri, including its trailing slash>
VITE_REPLAY_SIWE_STATEMENT=Sign in to Prediction Indexer.
VITE_REPLAY_SIWE_CHAIN_ID=1
VITE_REPLAY_FIXTURES=false # local visual-regression builds only
PORT=3000 # optional local static server
```

These values are public and embedded at build time. They must contain no token,
wallet key, signature, or other secret. The browser client validates closed
response schemas, bounds response sizes and polling, handles `Retry-After`, and
sends bearer sessions with `credentials: omit`. Sessions remain in memory and
are lost on refresh; they are never written to local or session storage.

The targets and decisions views resolve the newest complete run through:

```text
GET /api/event-universe/v1/targeter/status?limit=5
```

TanStack React Query deduplicates in-flight browser requests. Status is fresh for
15 seconds and polls every minute; immutable run, bundle, selection, and history
responses are fresh for five minutes. Inactive list/run queries are garbage
collected after five minutes, while drawer-only details are discarded as soon as
the drawer closes. Nothing is persisted to browser storage.

Refreshes are GET-only. The status response contains only card state and the
current complete target summary. Current targets and decisions fetch the full
run on demand from:

```text
GET /api/event-universe/v1/targeter/runs/<run_id>
```

They use `current_complete_run.run_id` and render the run's compact embedded
event summaries. Full `GET /v1/events/<event_id>` detail is requested only when
the corresponding target drawer opens; a newer incomplete run cannot replace
the current target set. Indexed status is not proof of
`current.json` publication or splice/frame capture health; capture therefore
remains explicitly unverified. The UI uses the server's semantic counts and
never reinterprets raw Targeter reports.

History pages through grouped bundle summaries from `GET /v1/bundles`, retaining
at most eight 100-row pages, then loads the latest immutable detail and occurrence
timeline only when a bundle opens. Targets and decisions render at most 100 rows
per client-side page. Full event and bundle drawer details are not retained after
their drawer closes.

## Routes

- `/` — normalized events and selected markets from the newest complete run
- `/targets` — compatibility redirect to `/`
- `/history` — one grouped row per historically selected bundle
- `/decisions` — latest complete run's candidate decision funnel
- `/replay` — public Replay jobs workspace
- `/replay/jobs/:jobId` — public Replay progress, events, request, and audit detail
- `/replay/new` — authenticated desktop Replay request workflow
- `/replay/admin` — admin-only allowlist management
- `/replay/jobs/:jobId/output` — reserved Coming soon route

Replay production paths use Universe directly. URL fixture controls remain for
design regression coverage: `?auth=entry`, `?auth=expired`, list
`?view=loading|empty|error`, detail `?state=<job-status>`, new replay
`?fixture=true`, and admin `?state=loading|error|forbidden|ready`. Except for the
real sign-in entry route, these controls are disabled unless the explicit
test-only `VITE_REPLAY_FIXTURES=true` build flag is present. Never set that flag
in a production or Vercel environment.

Legacy Event Universe and operations paths redirect to the corresponding new
routes.

## Vercel

Create the Vercel project from the repository root. [`vercel.json`](../vercel.json)
builds only the Vite client, serves `targeter-ui/dist`, and supplies the SPA
fallback. It has no API rewrite or function. The same file supplies the public
`VITE_UNIVERSE_API_BASE_URL` and `VITE_REPLAY_SIWE_*` production build values;
keep them exactly aligned with Universe when either deployment changes. No AWS,
S3, OIDC, archive-prefix, staging, decoder, or private-key variables belong in
Vercel.

## Local and orb operation

```sh
yarn install --frozen-lockfile
yarn workspace prediction-indexer-targeter-ui build
yarn workspace prediction-indexer-targeter-ui start
```

The optional Express server serves only `dist/` and `/healthz` on `PORT`; API
traffic still goes directly from the browser to Universe.

From the repository root, use `yarn lint`, `yarn typecheck`, `yarn test`, and
`yarn build`. Repository setup configures `.githooks/pre-commit`; it runs the
root lint gate before each commit.
