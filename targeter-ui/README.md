# Event Universe and Targeter observability UI

Read-only React/Vite UI for historical Event Universe exploration and recent Targeter operations. Event Universe is the primary experience at `/`; the existing Targeter cadence dashboard is secondary at `/operations`.

The browser hydrates Event Universe exclusively through the same-origin `/api/event-universe/...` contract. It never receives Universe credentials or talks to S3. The historical explorer uses run and selection indexes, immutable source/origin provenance, bundle detail, and bundle history rather than rescanning Targeter reports. It intentionally does not expose catalogues, current subscription state, receipts, raw files, replay coverage, or inferred event-end times.

The long-running Express server can additionally populate `/operations` from the five latest committed Targeter v2 S3 runs. It has no database or persistent data sink and uses the AWS SDK v3 default credential provider chain, including OIDC/web identity. A separate orb service refreshes the short-lived Amp ID-token file consumed by that standard chain.

The dashboard reads selection report v1, v2, and v3. For v2 and v3 it validates and displays committed-generation continuity evidence, including exact retained bundles, disposition, terminal probes, degraded-base diagnostics, and continuity provenance. Report v3 also shows the immutable origin run and archive identities that distinguish a complete occurrence from a retained reference. A retained bundle does not need to exist in current discovery candidates or catalogues: its exact targets come from the validated committed generation. All-terminal and terminal-clamp retirement—and v3's explicitly validated all-budget-trimmed result—are shown as legitimate empty decisions. These archived shadow reports are observability evidence only; `targeter-v2/current.json` and the immutable generation it names remain the sole live subscription truth.

Universe loading, errors, and freshness are independent from the recent Targeter S3 snapshot. Legacy `/event-universe`, `/events`, and `/config` paths redirect to `/`, `/operations/events`, and `/operations/config` respectively.

## Configuration

[`targeter-ui/.env.example`](.env.example) contains the complete server and Amp OIDC variable set with non-secret example values. Register these variables in the orb environment; the service does not automatically source the example file.

```sh
export AWS_OIDC_AUDIENCE=sts.amazonaws.com
export AWS_ROLE_ARN=arn:aws:iam::123456789012:role/prediction-indexer-targeter-ui
export AWS_ROLE_SESSION_NAME=prediction-indexer-targeter-ui
export TARGETER_UI_S3_BUCKET=example-archive
export TARGETER_UI_AWS_REGION=us-east-1
export TARGETER_UI_S3_EXPECTED_OWNER=123456789012
export TARGETER_UI_DECODER_PATH="$PWD/encoder/rust/target/release/prediction-decode-v1"
export TARGETER_UI_EVENT_UNIVERSE_URL=https://event-universe.internal.example
# optional: TARGETER_UI_S3_PREFIX=targeter-v2/runs
# optional: TARGETER_UI_REFRESH_SECONDS=60 TARGETER_UI_EXPECTED_RUN_SECONDS=600 PORT=3000
yarn install --frozen-lockfile
cargo build --release --manifest-path encoder/rust/Cargo.toml --bin prediction-decode-v1
yarn build
yarn workspace prediction-indexer-targeter-ui start
```

`TARGETER_UI_MAX_RUNS` defaults to and is fixed at `5`. AWS credentials are never returned by the API or rendered. For local visual work only, `TARGETER_UI_FIXTURE_PATH=/absolute/reports.json` replaces S3; the file is an array of decoded selection report v1–v3 objects (or `{ "runs": [...] }`). Fixture mode is never automatic.

`TARGETER_UI_EVENT_UNIVERSE_URL` is optional so the operational dashboard can run without the historical service. When set, Express exposes only the documented Universe health, run, selected-occurrence, detail, and bundle-history routes below `/api/event-universe`; arbitrary upstream paths and query fields are rejected. The proxy validates closed response schemas, applies a 5-second timeout and 2 MiB response cap by default, and returns generic errors without upstream bodies. `TARGETER_UI_EVENT_UNIVERSE_AUTHORIZATION` may hold a complete server-side Authorization header for an authenticated network proxy. It is never returned to the browser. Override the bounds with `TARGETER_UI_EVENT_UNIVERSE_TIMEOUT_MS` and `TARGETER_UI_EVENT_UNIVERSE_MAX_RESPONSE_BYTES`.

## Vercel deployment

Vercel hosts only the static Vite application and a thin same-origin Event Universe proxy. Event Universe and Targeter remain on their existing servers. Create the Vercel project from the repository root; the checked-in [`vercel.json`](../vercel.json) selects the client-only build, serves `targeter-ui/dist`, forwards `/api/event-universe/...` to the proxy function, and supplies the SPA fallback.

Configure only these server-side Vercel variables:

```text
TARGETER_UI_EVENT_UNIVERSE_URL=https://universe.example.com
TARGETER_UI_EVENT_UNIVERSE_AUTHORIZATION=Bearer replace-with-server-side-token  # optional
TARGETER_UI_EVENT_UNIVERSE_TIMEOUT_MS=5000                                     # optional
TARGETER_UI_EVENT_UNIVERSE_MAX_RESPONSE_BYTES=2097152                          # optional
```

Do not prefix them with `VITE_`: that would expose them to browser JavaScript. Do not configure AWS, S3 bucket, OIDC, web-identity-token, staging-root, or Rust decoder variables in Vercel. Those remain specific to the long-running operations server.

On Vercel, `/` is the fully functional Event Universe explorer. `/operations` clearly reports that Targeter cadence is not connected because direct S3 snapshot hydration does not run there. Moving cadence behind a provider-neutral server API is intentionally deferred; storage credentials and switchable storage adapters do not belong in the browser.

From the repository root, use `yarn lint`, `yarn lint:fix`, `yarn test`, `yarn typecheck`, and `yarn build`. The production Express server serves `dist/` and its API on `PORT`.

Repository setup configures `.githooks/pre-commit` through the local `core.hooksPath`. The hook runs the root `yarn lint` gate and blocks commits containing unformatted or lint-invalid Node/UI code. Developers who do not run `.agents/setup` can enable it with `git config core.hooksPath .githooks`.

## IAM / OIDC

The workload identity needs only `s3:ListBucket` on the bucket constrained with `s3:prefix` to `targeter-v2/runs/*`, and `s3:GetObject` on `arn:aws:s3:::BUCKET/targeter-v2/runs/*`. Its OIDC trust policy should constrain the provider subject/audience to this workload. Do not grant Put/Delete. Every list/get includes the configured expected bucket owner.

The `aws-identity` orb service runs [`scripts/refresh-amp-aws-token`](../scripts/refresh-amp-aws-token) immediately and every 45 minutes. The token path is derived as `<repository>/.amp/runtime/aws-oidc-token`, so `AWS_WEB_IDENTITY_TOKEN_FILE` does not need to be configured. The script asks Amp for a one-hour ID token, writes it with mode `0600`, and atomically replaces that file; it never prints or persists the token anywhere else. The AWS SDK reads the exported path and uses `AWS_ROLE_ARN` to call `AssumeRoleWithWebIdentity`. The UI service waits for the first non-empty token file before starting. A transient mint failure leaves the previous token file untouched and retries after 30 seconds instead of exiting into the service manager's rapid-restart limit. On orb resume, `.agents/resume` hydrates missing project environment entries and mints a fresh token synchronously before reconciling the declared services, so the UI cannot be restarted against a stale, merely non-empty token file.

## Identity and bounds

Only `date=YYYY-MM-DD/run=<run_id>/run_manifest.json` keys are commit markers. Listing is paginated and latest runs are chosen by validated microsecond UTC run ID, never `LastModified`. Manifest v2 and report v1–v3 structures are checked. The shared staging package verifies stored SHA-256/length. For compressed reports, the Rust protocol-v1 decoder verifies the strict Zstandard profile and logical SHA-256/length/LF count before the UI parses its output. `TARGETER_UI_DECODER_PATH` is required in S3 mode and must be an absolute path; fixture mode does not require it. Selection reports alone may be buffered after validation: stored size is limited to **16 MiB**, decoded size to **64 MiB**, and manifest to **1 MiB**. Catalogues are never downloaded. The cache is memory-only, refreshes at startup/on interval/API request, coalesces overlapping refreshes, and retains the last successful snapshot with a stale/error flag.

The displayed strategy is `configs/targeter_v2.json` from the **current checkout**. Archives do not embed the complete historical config. The UI compares report and checkout strategy versions, but correctly labels a match as evidence rather than byte-level proof of the historical settings.

## Amp orb

Register the required AWS and archive environment variables in the Amp project before creating the orb, then run `amp orb services ensure`. Project variables added later are available to newly created orbs, not an already-running orb. `.agents/setup` builds `prediction-decode-v1`; `.amp/services.yaml` passes that binary's exact repository path to the UI, starts the token refresher, and exposes port 3000 through an Amp portal. The token lives only under the gitignored `.amp/runtime/` directory; no AWS access key or web-identity token belongs in Git or `.env.example`.
