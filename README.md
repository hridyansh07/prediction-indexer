# Prediction Indexer

Records prediction-market order books across venues verbatim, then replays them
to measure whether logically related contracts (the same outcome on two venues,
or outcomes that must jointly pay a fixed amount) were ever mispriced by more
than fees, and for how long.

- **Capture:** Targeter v2 selects multi-venue sports and esports events. One
  splice per lane records every venue delivery. The Rust ingester orders and
  seals the deliveries into canonical windows, and the archiver stores them
  immutably in an object store.
- **Universe:** an event store over the committed Targeter runs, with bundle
  history, outcomes and a Replay job control plane behind an HTTP API.
- **Replay:** materializes verified normalized books from canonical windows and
  runs economic strategies on them through a shared SDK. Each strategy's
  independent reader verifies its output.

Start with [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Layout

```
splices/             venue adapters: auth, subscribe, reconnect, record verbatim
targeter/            Targeter v2: discovery, matching, selection, run archive, publication
ingester/            Rust: sealed segments, ingest store, canonical windows, continuity
encoder/             the shared Zstandard codec (Python, Rust, Node)
archive/             raw/canonical archivers, local and GCS object stores, receipts, reapers
universe/            event store, bundle history, outcomes, Replay jobs API
engine/              Rust: Replay domain, venue normalizers, derivatives, risk, Redis transport
replay/              current strategy runtime, SDKs, bench and jobs
  strategies/        per-strategy code, readers, configs and docs
  legacy/            original raw-envelope replay and five-gate audits
analysis/            outcome spaces, masks and claims shared by targeter, universe, replay
targeter-ui/         web UI for the Universe and Replay
configs/             Targeter strategy, replay runner, bench and strategy examples
docker/              Dockerfiles, Caddyfile
scripts/             archive probe, codec fixtures, coverage backfill, bundle runner
docs/                deployment, runbook, pending specs (docs/specs/)
```

## Running

Use the project virtual environment for Python.

```bash
# select events once; shadow writes a run without publishing
.venv/bin/python targeter/run_v2.py --mode shadow --no-response-cache \
  --strategy configs/targeter_v2.json \
  --cache-root data/targeter-v2-monitor-state --output-root data/targeter-v2-shadow

# record one feed (long-lived)
.venv/bin/python splices/run.py polymarket --targets <targets_polymarket.json>
.venv/bin/python splices/run.py kalshi --targets <targets_kalshi.json>   # needs Kalshi credentials

# build the ingester and finalizer
cargo build --release --manifest-path ingester/Cargo.toml
```

Production runs under Docker Compose:

- `compose.yaml`: capture. The targeter, splices, ingester, finalizer, archivers
  and reapers. The `kalshi`, `reference` and `ops` profiles.
- `compose.targeter-v2.yaml`: Targeter run archiver, run reaper and integrity
  audit (the `ops` profile).
- `compose.universe.yaml`: Universe, Replay runner, Redis and Caddy.

See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) and
[`docs/RUNBOOK.md`](docs/RUNBOOK.md). Required environment variable names are
in `.env.example`.

Replay strategies run locally through the bench:

```bash
.venv/bin/python -m replay.bench --help
```

See [`replay/README.md`](replay/README.md).

## Tests

```bash
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m unittest discover -s replay/tests
cargo test --manifest-path ingester/Cargo.toml --workspace
cargo test --manifest-path engine/Cargo.toml --workspace
cargo test --manifest-path encoder/rust/Cargo.toml
yarn install && yarn test          # targeter-ui and the Node decoder
```

`scripts/archive_probe.py` archives and decodes real captured bytes and reports
the compression ratio and decode ceiling.

## Documents

| Area | Document |
|---|---|
| System map | [`ARCHITECTURE.md`](ARCHITECTURE.md) |
| Agent working rules | [`AGENTS.md`](AGENTS.md) |
| Splices, envelope | [`splices/README.md`](splices/README.md) |
| Ingester, canonical windows | [`ingester/README.md`](ingester/README.md), [`ingester/FORMATS.md`](ingester/FORMATS.md) |
| Codec | [`encoder/README.md`](encoder/README.md) |
| Archive, receipts, reapers | [`archive/README.md`](archive/README.md), [`archive/FORMATS.md`](archive/FORMATS.md) |
| Targeter v2 | [`targeter/README.md`](targeter/README.md), [`targeter/v2/SELECTION.md`](targeter/v2/SELECTION.md), [`targeter/v2/DELIVERY.md`](targeter/v2/DELIVERY.md) |
| Universe | [`universe/README.md`](universe/README.md) |
| Normalizers, derivatives, risk | [`engine/README.md`](engine/README.md), [`engine/DERIVATIVES.md`](engine/DERIVATIVES.md) |
| Replay and strategies | [`replay/README.md`](replay/README.md) |
| Outcome spaces, masks, claims | [`analysis/README.md`](analysis/README.md) |
| UI | [`targeter-ui/README.md`](targeter-ui/README.md) |
| Deployment, operations | [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md), [`docs/RUNBOOK.md`](docs/RUNBOOK.md) |
| Pending specifications | [`docs/specs/`](docs/specs/) |
