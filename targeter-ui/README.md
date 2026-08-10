# Targeter v2 observability UI

Read-only React/Vite + Express dashboard for the five latest committed Targeter v2 S3 runs. It has no database, writes no files, and uses the AWS SDK v3 default credential provider chain (including ECS/EC2 roles and OIDC/web-identity); it contains no custom token or credential handling.

## Configuration

```sh
export TARGETER_UI_S3_BUCKET=example-archive
export TARGETER_UI_AWS_REGION=us-east-1
export TARGETER_UI_S3_EXPECTED_OWNER=123456789012
# optional: TARGETER_UI_S3_PREFIX=targeter-v2/runs
# optional: TARGETER_UI_REFRESH_SECONDS=60 TARGETER_UI_EXPECTED_RUN_SECONDS=600 PORT=3000
yarn install --frozen-lockfile
yarn dev
```

`TARGETER_UI_MAX_RUNS` defaults to and is fixed at `5`. AWS credentials are never returned by the API or rendered. For local visual work only, `TARGETER_UI_FIXTURE_PATH=/absolute/reports.json` replaces S3; the file is an array of decoded selection report v1 objects (or `{ "runs": [...] }`). Fixture mode is never automatic.

Commands: `yarn lint`, `yarn lint:fix`, `yarn test`, `yarn typecheck`, `yarn build`, and `yarn start` (after build). The production Express server serves `dist/` and its API on `PORT`.

Repository setup configures `.githooks/pre-commit` through the local `core.hooksPath`. The hook runs `yarn --cwd targeter-ui lint` and blocks commits containing unformatted or lint-invalid UI code. Developers who do not run `.agents/setup` can enable it with `git config core.hooksPath .githooks`.

## IAM / OIDC

The workload identity needs only `s3:ListBucket` on the bucket constrained with `s3:prefix` to `targeter-v2/runs/*`, and `s3:GetObject` on `arn:aws:s3:::BUCKET/targeter-v2/runs/*`. Its OIDC trust policy should constrain the provider subject/audience to this workload. Do not grant Put/Delete. Every list/get includes the configured expected bucket owner.

## Identity and bounds

Only `date=YYYY-MM-DD/run=<run_id>/run_manifest.json` keys are commit markers. Listing is paginated and latest runs are chosen by validated microsecond UTC run ID, never `LastModified`. Manifest v2 and report v1 structure are checked. Stored SHA-256/length are verified before decoding; the system `zstd` decoder validates the checksum while a bounded child stream and frame parser enforce one frame, no trailing data, and the logical output limit. Logical SHA-256/length/LF count are checked before UTF-8 JSON parsing. Selection reports alone may be buffered: stored size is limited to **16 MiB**, decoded size to **64 MiB**, and manifest to **1 MiB**. Catalogues are never downloaded. The cache is memory-only, refreshes at startup/on interval/API request, coalesces overlapping refreshes, and retains the last successful snapshot with a stale/error flag.

The displayed strategy is `configs/targeter_v2.json` from the **current checkout**. Archives do not embed the complete historical config. The UI compares report and checkout strategy versions, but correctly labels a match as evidence rather than byte-level proof of the historical settings.

## Amp orb

Set the required environment variables in the orb's service environment, then run `amp orb services ensure`. `.amp/services.yaml` exposes port 3000 through an Amp portal. It intentionally stores no credentials; use an attached role/OIDC identity or provide environment configuration outside Git.
