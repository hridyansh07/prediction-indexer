# Targeter observability UI

Read-only React/Vite UI for current Targeter selections, historical Event
Universe bundles, and recent Targeter decision evidence. The current targets
explorer is the landing page; detailed views are desktop-first.

The browser hydrates every view exclusively through the same-origin
`/api/event-universe/...` proxy. The UI does not list an archive, download
Targeter reports, decode Zstandard, stage private files, or hold cloud-storage
credentials. Event Universe owns report verification and lifecycle projection.

## Server configuration

[`targeter-ui/.env.example`](.env.example) contains the non-secret examples:

```text
UNIVERSE_API_BASE_URL=https://universe.example.com
UNIVERSE_API_AUTHORIZATION=Bearer replace-with-server-side-token  # optional
UNIVERSE_API_TIMEOUT_MS=5000                                     # optional
UNIVERSE_API_MAX_RESPONSE_BYTES=1750000                          # optional
PORT=3000                                                        # Express only
```

All Universe variables are server-only. Do not prefix them with `VITE_`; doing
so would expose them to browser JavaScript. The proxy accepts only documented
paths and query fields, validates closed response schemas, applies bounded
timeouts and response sizes, and returns generic failures without upstream
bodies or credentials.

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
- `/api/event-universe/...` — narrow same-origin Universe proxy

Legacy Event Universe and operations paths redirect to the corresponding new
routes.

## Vercel

Create the Vercel project from the repository root. [`vercel.json`](../vercel.json)
builds only the Vite client, serves `targeter-ui/dist`, forwards the same-origin
Universe routes through the Vercel function, and supplies the SPA fallback.
Configure the same `UNIVERSE_API_*` server-side variables in Vercel. No AWS,
S3, OIDC, archive-prefix, staging, or decoder variables belong in Vercel.

## Local and orb operation

```sh
yarn install --frozen-lockfile
yarn workspace prediction-indexer-targeter-ui build
yarn workspace prediction-indexer-targeter-ui start
```

The production Express server serves `dist/`, `/healthz`, and the same strict
Universe proxy on `PORT`. In an Amp orb, register `UNIVERSE_API_BASE_URL` and any
optional authorization as project secrets, then run `amp orb services ensure`.

From the repository root, use `yarn lint`, `yarn typecheck`, `yarn test`, and
`yarn build`. Repository setup configures `.githooks/pre-commit`; it runs the
root lint gate before each commit.
