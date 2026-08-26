# Event Universe and Targeter cadence UI

Read-only React/Vite UI for historical Event Universe exploration and recent
Targeter cadence observability. Event Universe is the primary experience at
`/`; the bounded five-run cadence projection is available at `/operations`.

The browser hydrates both views exclusively through the same-origin
`/api/event-universe/...` proxy. The UI does not list an archive, download
Targeter reports, decode Zstandard, stage private files, or hold cloud-storage
credentials. Event Universe owns report verification and lifecycle projection.

## Server configuration

[`targeter-ui/.env.example`](.env.example) contains the non-secret examples:

```text
UNIVERSE_API_BASE_URL=https://universe.example.com
UNIVERSE_API_AUTHORIZATION=Bearer replace-with-server-side-token  # optional
UNIVERSE_API_TIMEOUT_MS=5000                                     # optional
UNIVERSE_API_MAX_RESPONSE_BYTES=2097152                          # optional
PORT=3000                                                        # Express only
```

All Universe variables are server-only. Do not prefix them with `VITE_`; doing
so would expose them to browser JavaScript. The proxy accepts only documented
paths and query fields, validates closed response schemas, applies bounded
timeouts and response sizes, and returns generic failures without upstream
bodies or credentials.

The cadence dashboard consumes only:

```text
GET /api/event-universe/v1/targeter/cadence?limit=5
```

Refreshes are GET-only. The status is exactly `CADENCE CURRENT`, `CADENCE LATE`,
or `CADENCE UNAVAILABLE`, as supplied by Universe. It is an indexed cadence
observation, not proof of `current.json` publication or splice health. The
API includes selected occurrence detail plus bounded catalogue, candidate,
rejection, admission, continuity, terminal-probe, and diagnostic projections.
The UI uses the server's semantic counts and never reinterprets raw Targeter
reports.

## Routes

- `/` — historical selected-event explorer
- `/operations` — five-run Targeter cadence timeline and selected bundles
- `/operations/selections` — filters across selections in those cadence runs
- `/api/event-universe/...` — narrow same-origin Universe proxy

Legacy Event Universe and operations paths redirect to these routes.

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
