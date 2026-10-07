# Local replay bench examples

Replace all `<...>` placeholders with reviewed inputs. These templates contain no
retained fixture identities, endpoints, or rates. Numeric fee placeholders must
be replaced with JSON integers. Review economics, scales, applicability and source
claims under the [Fee SDK](../../replay/fees/README.md); null effective dates assert
current evidence, and do not establish historical applicability.

- [coverage bench example](../../replay/strategies/bundle_coverage/bench.example.json) runs the existing coverage factory/reader. In the base
  supervisor config, derivative paths must resolve under `/bench/in/derivatives`,
  matching the readonly mount. Use image executable paths, normally
  `/usr/local/bin/replay-publish` and `/usr/local/bin/python`.
- `run.example.json` demonstrates arbitrary strategy-owned config, checks and fee
  tokens. Replace it with the selected strategy's actual closed config. The bench
  does not interpret those fields. Only the selected identity-named catalog is
  mounted, at `/bench/fees/<catalog_identity>`.
- `fees.example.json` uses the SDK's tagged schedule encoding with bench-local
  pinned sources. Supply public evidence bytes and their SHA-256; no download or
  implicit schedule extraction is performed.
- `expect.example.json` declares composite added/removed/changed row keys in the
  order of `compare.key`. Omitted groups expect identical rows; `changed` lists
  keys, not field names. For a receipt-only reader, both missing summaries are
  reported explicitly; a one-sided missing summary fails comparison.

Preparation config uses the existing
[preparation contract](../../docs/STRATEGY_PREPARATION_V1.md). Export
`UNIVERSE_BASE_URL` before invoking bench preparation; it never loads dotenv.
Use fresh output directories on every writing invocation:

```bash
python -m replay.bench prepare /absolute/<prepare-config>.json /absolute/<context-a>
python -m replay.bench prepare /absolute/<prepare-config>.json /absolute/<context-b>
python -m replay.bench compare-context /absolute/<context-a> /absolute/<context-b>
python -m replay.bench fees /absolute/<fee-spec>.json /absolute/<fees-output>
python -m replay.bench run /absolute/<run-spec>.json /absolute/<run-a> --label local
python -m replay.bench run /absolute/<run-spec>.json /absolute/<run-b> --label local
python -m replay.bench compare-runs /absolute/<run-a> /absolute/<run-b>
python -m replay.bench compare-runs /absolute/<run-a> /absolute/<changed-run> --expect /absolute/<expect>.json
```

The angle-bracket placeholders above must be replaced before use in a shell.
Failed outputs remain evidence. Resource cleanup failures appear in
`orchestration.json` with exit 3. `--keep-image` retains only the image created
for that run; cache pruning is opt-in with `--prune-build-cache`.
See the full [bench contract](../../docs/LOCAL_REPLAY_BENCH_V1.md).
