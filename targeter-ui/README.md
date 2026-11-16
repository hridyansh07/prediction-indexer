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
PORT=3000 # optional local static server
```

These values are public and embedded at build time. They must contain no token,
key, or other secret. The browser client validates closed response schemas,
bounds response sizes and polling, and sends read-only GETs with
`credentials: omit`. The UI has no sign-in and holds no session.

The targets and decisions views resolve the newest complete run through:

```text
GET /v1/targeter/status?limit=5
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
GET /v1/targeter/runs/<run_id>
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

Legacy Event Universe and operations paths redirect to the corresponding new
routes.

## Vercel

Create the Vercel project from the repository root. [`vercel.json`](../vercel.json)
builds only the Vite client, serves `targeter-ui/dist`, and supplies the SPA
fallback. It has no API rewrite or function. The same file supplies the public
`VITE_UNIVERSE_API_BASE_URL` production build value; keep it aligned with
Universe when either deployment changes. No AWS,
S3, OIDC, archive-prefix, staging, decoder, or private-key variables belong in
Vercel.

## Local operation

```sh
yarn install --frozen-lockfile
yarn workspace prediction-indexer-targeter-ui dev
```

`dev` runs the Vite dev server; API traffic goes directly from the browser to
Universe.

From the repository root, use `yarn lint`, `yarn typecheck`, `yarn test`, and
`yarn build`. The `.githooks/pre-commit` hook (see the root README) runs the
root lint gate before each commit.
