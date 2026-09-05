# Prediction Indexer

Finding and testing logical relationships between prediction-market instruments
across venues — whether two contracts that must settle identically are priced
identically, and whether the gap is ever large enough to trade.

Two halves. The **analysis half** is complete for a first pass and produced a null
result. The **capture half** is being built now, because the null turned out to be
a measurement problem rather than an answer.

## Where things stand

The relationship engine works. 204/204 derived masks agreed with actual
settlement, 0 disagreements. Kalshi prices equivalent routes to within 0.50¢ of
itself. State conditioning is load-bearing — 0 identities exist pre-match, 56
emerge once map results land.

It has not found any money. The cross-venue crossing funnel ran 3,756 bars → 932
gross crossings → 807 cross-venue → 372 net of fees → 91 with fresh quotes, median
**0.32¢**. Every headline number in this project has been larger before its
controls than after.

The crypto ladders remain a useful control because their masks derive from
strikes. The current discovery direction is nevertheless sports-first and
multi-venue: Targeter v2 looks for mature event bundles with reusable moneyline,
spread, total, score, map, and series relationships. Phases 1–5 are shadow-only;
phases 6–10 now provide immutable run archival, atomic multi-venue publication,
splice handoff, one-shot scheduling, and a production audit. The base Compose
file still runs v1; v2 is an explicit deployment override until a real S3-backed
rollout passes its audit gate.

The motivation, event-first selection model, local monitoring workflow, and
rollout rule are summarized in [`targeter/README.md`](targeter/README.md).

## Layout

```
splices/             venue adapters — auth, subscribe, reconnect, record verbatim
  common/              envelope, spool, shared connection loop
  polymarket/          market channel, plus sports and RTDS reference feeds
  limitless/           built, verified live
  kalshi/              built, plug-and-play once an API key is set
targeter/            v2 motivation and sports discovery/archive/publication; legacy v1
ingester/            Rust: daily fact-store partitions, sequencing, continuity, retention
encoder/             the shared Zstandard codec — Python and Rust, streaming only
archive/             raw/canonical archiver, immutable object store, dual-receipt reaper
engine/              Rust Replay domain/numeric boundary (S2; no venue adapters)
analysis/             masks, outcome space, void policy, partition sums
scripts/             analysis pipelines and historical pulls
docs/                cross-cutting specs, deployment guide, venue API notes
data/                pulled data, manifests, spools
```

## The rule that shapes the capture half

> Capture decisions are irreversible; analysis decisions are not.

So a splice records every message verbatim and filters nothing — not heartbeats,
not frames it cannot parse. Filtering is analysis, and analysis belongs on the
reversible side of the tape.

Not fastidiousness. The live wire has contradicted published venue documentation
three times: Polymarket's schema is flat snake_case where the docs describe wrapped
camelCase; Polymarket carries a book `hash` on every price change; Limitless
carries a `version` field its reference says does not exist. A splice normalising
against any of those documents would have written confident, wrong output — and by
the time anyone noticed, the frames would be gone.

Three processes with a file between them. Splices fsync NDJSON into per-venue
spools; the ingester tails them and assigns the one global order. **The file is the
protocol** — a socket would be lossy exactly when the ingester is down, which is
when it matters.

### Running capture

Use the project venv — it has `websockets`, `python-socketio` and `cryptography`:

```bash
# discover what to watch — long-lived loop, driven by configs/capture_manifest.json
.venv/bin/python targeter/run.py
.venv/bin/python targeter/run.py --once --venue kalshi    # one cycle, one venue

# v2 is one-shot: shadow locally, or archive+publish with an independent store
.venv/bin/python targeter/run_v2.py --mode shadow

# record — one process per feed, long-lived by default
.venv/bin/python splices/run.py polymarket
.venv/bin/python splices/run.py limitless --stop-after-seconds 60   # probe, same path
.venv/bin/python splices/run.py kalshi                             # needs an API key

# reference feeds — no targets, no subscription, they broadcast everything
.venv/bin/python splices/run.py polymarket-sports
.venv/bin/python splices/run.py polymarket-rtds

# recovery points — polls full books for whatever targets_polymarket.json holds.
# The websocket sends one book per epoch and then deltas only, so without this a
# six-hour connection has one anchor and six hours of unverified chain.
.venv/bin/python splices/run.py polymarket-snapshots --poll-seconds 60

# sequence and classify continuity
ingester/…/indexer-ingest data/spool data/ingest-store --watch-interval-seconds 5

# one-shot integrity verification
ingester/…/indexer-ingest data/spool data/ingest-store --check-integrity

# audit closed ingest partitions older than 24 hours (use --mode delete deliberately)
ingester/…/indexer-store-reap data/ingest-store --retention-hours 24
```

Kalshi needs `KALSHI_API_KEY_ID` and a private key (see `.env.example`); nothing
else does, including the Kalshi targeter, which uses the public catalogue.

Measured: 20 Polymarket assets produce **6.2M records/day, 6.8 GB/day**
uncompressed.

### Docker Compose deployment

The default deployment starts the targeter, separate Polymarket and Limitless
splice containers, the Polymarket recovery-snapshot lane, and a continuously
tailing Rust ingester:

```bash
test -e .env || cp .env.example .env
docker compose build
docker compose up -d
docker compose ps
```

Targeter v2 is opt-in and scheduled as repeated one-shot containers; see
[`docs/TARGETER_V2_PHASES_6_10.md`](docs/TARGETER_V2_PHASES_6_10.md) and the
`compose.targeter-v2.yaml` override before switching splice target paths.

Kalshi remains opt-in until its credentials are tested:

```bash
docker compose --profile kalshi up -d
```

Sports and RTDS reference lanes use the `reference` profile. The raw tape and
daily derived-store partitions are bind-mounted beneath `CAPTURE_DATA_ROOT`. The
`ingest-store-reaper` ops service is a one-shot command intended for host cron;
see
[`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) for Linux ownership, secret mounts,
profiles, integrity checks, storage sizing, and operations.

---

## The historical half

Discovers venue-native market identifiers into durable, reviewable files before any
orderbook download begins.

### Secret configuration

Copy `.env.example` to `.env` and place the Oddpool key there:

```dotenv
ODDPOOL_API_KEY=oddpool_...
```

The real `.env` and the generated `data/` directory are gitignored.

### Durable request behaviour

Every successful JSON response is cached beneath
`data/cache/http/<host>/<sha256-of-canonical-url>.json`, with an adjacent
`.meta.json` recording the URL and fetch time. Repeating an identical command uses
the cache and makes zero network requests unless `--refresh` is supplied.

Rate-limit state is written to disk and locked across processes. Historical Oddpool
pulls use a conservative two-second request-start interval plus retries for `429`,
timeout, and truncated-response errors.

### Discovery

```bash
python3 scripts/discover_polymarket.py --query "EWC 2026"
python3 scripts/discover_polymarket.py --event-id 630352 --event-id 630364
```

Kalshi puts individual esports matches in generic recurring series, so scan the
series and filter locally against the saved market rules:

```bash
python3 scripts/discover_kalshi.py \
  --series-ticker KXDOTA2GAME --status settled \
  --min-close 2026-07-15T00:00:00Z --max-close 2026-07-20T23:59:59Z \
  --contains "Esports World Cup 2026"

python3 scripts/discover_kalshi_event.py \
  "https://kalshi.com/markets/kxdota2game/dota-2-game/kxdota2game-26jul181030parity"
```

See [`scripts/GAME_DISCOVERY.md`](scripts/GAME_DISCOVERY.md) for other game-level series
and event grouping.

### Matched playoff history

```bash
python3 scripts/build_playoff_manifest.py \
  --kalshi-events data/discovery/kalshi/<job-id>/events.ndjson \
  --kalshi-markets data/discovery/kalshi/<job-id>/markets.ndjson \
  --polymarket-events data/discovery/polymarket/<job-id>/events.ndjson \
  --polymarket-markets data/discovery/polymarket/<job-id>/markets.ndjson

python3 scripts/pull_oddpool_history.py --manifest data/manifests/ewc_dota_playoffs.json
```

Each page is appended to NDJSON before its cursor checkpoint advances, so a
completed job re-runs with zero network requests. Validate with
`scripts/validate_oddpool_history.py`.

### Sibling market datasets (free venue APIs)

The playoff manifest is match-winner moneylines only, which admits no non-trivial
masks. Two sibling datasets fix that, both from free endpoints:

```bash
python3 scripts/build_sibling_datasets.py
python3 scripts/pull_free_history.py --manifest data/manifests/ewc_dota_siblings.json
python3 scripts/validate_sibling_history.py --manifest data/manifests/ewc_dota_siblings.json
```

For the World Cup pull candlesticks first — they are what the mask engine needs and
they land in ~30 minutes, whereas the trade tape takes hours. Both phases share one
checkpointed job directory, so the second command resumes rather than restarts:

```bash
python3 scripts/pull_free_history.py --manifest data/manifests/wc_knockout_2026.json \
  --skip-trades --timeout-seconds 40
python3 scripts/pull_free_history.py --manifest data/manifests/wc_knockout_2026.json \
  --skip-polymarket --timeout-seconds 40
```

Keep `--timeout-seconds` near 40. Candlestick responses are 1–6 MB and normally
complete in 6–10 s; with the 120 s default a single hung connection burns five
retries and stalls one market for up to 20 minutes.

#### Sources and what they can answer

| Source | Endpoint | Gives | Depth |
|---|---|---|---|
| Kalshi candlesticks | `/series/{s}/markets/{t}/candlesticks` | 1m `yes_bid`/`yes_ask` OHLC, price, volume, OI | **no** |
| Kalshi trades | `/markets/trades` | executed size, price, taker direction | **no** |
| Polymarket CLOB | `clob.polymarket.com/prices-history` | single price series per token | **no** |
| Oddpool | `/historical/{venue}/orderbook` | full L2 ladders | yes |

Neither venue serves historical book depth for free. Kalshi has no historical
orderbook endpoint at all and Polymarket's `/book` serves live markets only. So
these datasets support the mask engine, relationship derivation and top-of-book
partition sums, but **not** the size-adjusted VWAP sweep in
[`analysis/PARTITION_SUM_TEST_SPEC.md`](analysis/PARTITION_SUM_TEST_SPEC.md) §2–4. Manifest targets
record this as `depth_available: false`.

This limitation is the direct reason the capture half exists.

The candlestick endpoint rejects windows longer than 5,000 periods, so
`candlestick_windows()` chunks long-lived markets. The account-scoped
`/historical/{fills,orders,positions}` endpoints return only the authenticated
member's own activity and are deliberately unused.

### Partition-sum economic gate

Frozen in `configs/partition_sum_v1.json` and described in
[`analysis/PIPELINE_SPEC.md`](analysis/PIPELINE_SPEC.md).

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e .
.venv/bin/python scripts/run_partition_sum_test.py
.venv/bin/python scripts/run_partition_sum_test.py --offline   # verify zero network
```

Every run is content-addressed beneath `data/analysis/partition_sum/`: normalized
books and observations in Parquet, validation and fee records, stage manifests, a
machine-readable summary, and the Markdown report. Oddpool is never contacted here.

### Outputs

```text
data/discovery/<venue>/<job-id>/
  request.json  events.ndjson  markets.ndjson  event_bundles.ndjson  run.json
```

`markets.ndjson` carries the IDs Oddpool needs — Kalshi's `oddpool_market_id` is
the market ticker; Polymarket's is the condition ID, with `token_ids` holding the
optional `asset_id` values. `event_bundles.ndjson` is the grouped view. The raw
response cache is the source of record: generated NDJSON rebuilds from it without
re-querying.

---

## Tests

```bash
python3 -m unittest discover -s tests -q
python3 -m unittest discover -s replay/tests -q
cargo test --manifest-path ingester/Cargo.toml --workspace
cargo test --manifest-path encoder/rust/Cargo.toml
```

Against real captured bytes rather than generated ones:

```bash
python3 scripts/archive_probe.py     # archive, decode, ratio, decode ceiling, peak RSS
```

## Reading order

| Document | Covers |
|---|---|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | **Start here.** How the system fits together and why each boundary sits where it does |
| [`analysis/MARKET_RELATIONSHIP_GRAPH.md`](analysis/MARKET_RELATIONSHIP_GRAPH.md) | The original thesis: conditions, masks, relationship derivation |
| [`docs/CAPTURE_SPEC.md`](docs/CAPTURE_SPEC.md) | The capture system; §11 records what's been decided and measured since |
| [`splices/common/ENVELOPE.md`](splices/common/ENVELOPE.md) | The wire contract — start here for the capture half |
| [`splices/SPLICE.md`](splices/SPLICE.md) | Venue adapters and the no-filtering rule |
| [`targeter/TARGETER.md`](targeter/TARGETER.md) | Subscription management |
| [`docs/TARGETER_V2_PHASES_1_5.md`](docs/TARGETER_V2_PHASES_1_5.md) | Sports-first multi-venue discovery, matching, relationship selection, and the shadow-run contract |
| [`ingester/INGESTER.md`](ingester/INGESTER.md) | Sequencing and continuity |
| [`docs/SEALED_CAPTURE_PIPELINE_V1.md`](docs/SEALED_CAPTURE_PIPELINE_V1.md) | The staged capture-pipeline design; refined by the two docs below |
| [`encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md`](encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md) | The shared Zstandard codec contract |
| [`archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md) | Raw archiver, immutable object store, dual-receipt reaper |
| [`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md`](archive/S3_RAW_ARCHIVE_ADAPTER_V1.md) | The production AWS S3 backend for the archiver/reaper |
| [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) | Docker Compose topology and Linux operations |
| [`analysis/PARTITION_SUM_TEST_SPEC.md`](analysis/PARTITION_SUM_TEST_SPEC.md) | The first experiment and its staleness controls |
| [`analysis/PIPELINE_SPEC.md`](analysis/PIPELINE_SPEC.md) | The frozen partition-sum pipeline contract |
| [`analysis/CORRELATION_PIPELINE_REVIEW_V1.md`](analysis/CORRELATION_PIPELINE_REVIEW_V1.md) | Review of the correlation candidate pipeline; required reading before resuming that work |
