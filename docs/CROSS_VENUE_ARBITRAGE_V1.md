# Cross-venue arbitrage research V1

Implemented bounded V1, based on reconciled `a11bbf0`. The default eight-size
sweep passed one retained Procyon fixture through Linux Redis/publisher execution
and independent completed reading. Broader corpus and live execution remain unverified.

V1 measures two-leg all-BUY taker complete sets inside one pinned bundle scope.
`outcome_scope` supplies static masks: legs must be MASKED, from different venues,
and partition one EXHAUSTIVE space. No names or prices establish equivalence.
Every cross-venue book pair is admitted or rejected visibly. Uncaptured members
receive their own NOT_CAPTURED denominators. Enumeration is sorted and capped at
4,096 candidate pairs per scope before the configured size sweep; excess fails.

The SDK owns fills, trust, same-time staging, exact denominators, episodes,
quote slices, optional time-shift controls, and bounds. The strategy explicitly
opts into native scales; existing strategies keep homogeneous-scale admission.
Kalshi acquisitions consume the opposite orientation's projected bid fill, with
the desired orientation retained for masks and fee economics. Native scales are
never rewritten. Shared FeeBridge exposes per-order FeeEngine assessments;
basket payout arithmetic lives in this strategy.

The closed run config is {version:1, snapshot_directory, snapshot_sha256, fees,
policy, valuation}. Fees use the existing complement fee configuration. Policy
uses complement policy 2's sweep, thresholds, latency tiers, skew edges, audit and
optional profile conventions. Only time_shift controls are supported: cyclic
replacement could invalidate the mask proof and is rejected. The verdict policy
fields are preserved for configuration reuse but no complement verdict is emitted.
Valuation is {version:1, kind:"PARITY_SCENARIO", unit:"research_dollar", assets:[...]},
with exact native asset identities sorted by canonical JSON; every listed asset
is valued at one research dollar. Missing bindings, quote assets, or valuation
coverage are explicit ECONOMICS_UNKNOWN or VALUATION_UNKNOWN statuses, with no
fabricated zero cost. This is a scenario, not an FX or settlement compatibility claim.

Gross payout floor is N research dollars. Cost is the sum of exact native fill
costs under the scenario. Net uses each order's native collateral delta plus the
minimum received outcome quantity across the two exclusive masks. Token BUY fees
reduce payout; collateral fees reduce cash exactly once. Exact values are signed
integers at scale 36, preserving prices times quantities through scale 18+18.
Native costs, quote assets, received tokens, fees, assessment identities and fee
labels remain in payloads. Missing schedules yield FEE_UNKNOWN on gross-positive
time. Gross nonpositive time skips fee work under the fixed no-rebate/refund policy.

Output layout 2 has closed manifests and payloads. Cross-venue manifests and
summaries now use **format version 2**; the experiment identity pins
`entity_contract_version: 2`. The configuration remains version 1. Original
format-1 outputs require their original frozen reader or a fresh replay; this
reader rejects them rather than reinterpreting their denominator table.

Statically rejected pairs and uncaptured members use `size_contracts: null` and
are emitted once per scope, with the full scope duration. Admitted pairs retain
one entity per requested size and their original descriptors/identities. A null
size never reaches fill/economics evaluation. A rejected two-leg pair also has
one null-size row in each enabled time-shift control; singleton missing members
continue to have no control. Controls remain isolated from real results.

Summaries retain null-size rows separately from numeric-size rows, sorted before
numeric sizes. If the same route is rejected in one scope and admitted in another,
its null row covers the rejected scopes, while each size row covers only admitted
scopes. To inspect that route's requested denominator at a chosen size, add the
null row's time to that size row's time once. Do not sum across size rows to claim
elapsed event time or expand the null row in persisted output. Missing-capture,
unavailable masks and unsupported shapes keep their structured reasons and
scoped time; no real opportunity time is multiplied.

The shared SDK measures every real/control entity table at construction before
any stream is consumed, NDJSON `.open` file is created or profile initialized.
The preflight and final writer use the same canonical row/chunk encoder, counting
JSON wrappers, commas, empty scopes, UTF-8 bytes and final LF against the unchanged
8,388,608-byte limit. Each pass resolves one scope at a time without retaining a
cross-scope descriptor cache. Final tables are streamed, fsynced, renamed and
hash-verified by the independent reader. Oversized tables fail visibly with the
name, exact required bytes and limit; no sampling or metadata-limit increase.

Every manifest, summary,
entity and positive payload identifies settlement_model=normal_resolution_only
and the pinned outcomes provider. Independent reader hooks reconstruct entities,
re-walk native consumed quotes, check fee/cashflow/payout arithmetic and recompute
SDK episodes, slices, denominators and Q(L). Completed reading requires the real
supervisor SUCCESS and matching configured identities. Native balances and fee
model evidence remain writer-attested; the reader does not reexecute Risk or
prove historical fee applicability.

Q(L) and time-shift controls are retrospective diagnostics. No atomic execution,
realized profit, dynamic masks, void branches, source/settlement compatibility,
shorts, makers, inventory simulation, conversion or optimizer is implemented.

## Preparation endpoint

Use UNIVERSE_BASE_URL, the existing jobs universe_base_url naming convention.
The local preparation CLI loads that one setting from a caller-specified dotenv
file without shell execution or loading other credentials, then passes it to
UniverseHTTP for both selections and outcomes. Exported environment takes priority.
Missing or invalid URLs fail before preparation. Offline replay loads the pinned
snapshot and never instantiates a network source. Invocation and validation results
are recorded below after implementation.

## Runnable small-bundle recipe

Use a finite interval for one reviewed bundle and at least two captured venues.
Preparation JSON, derivative pins, occurrence provenance, authorities and native
scales follow [STRATEGY_PREPARATION_V1.md](STRATEGY_PREPARATION_V1.md). Store research
outputs in new directories outside retained evidence. Run from this implementation
checkout with the project virtual environment and this checkout on `PYTHONPATH`.

1. Put the current endpoint in your `.env` as `UNIVERSE_BASE_URL`. The placeholder
   in `.env.example` is not a server to contact. Prepare using the CLI:

```bash
.venv/bin/python -m replay.prepare_context /research/prepare.json /research/context \
  --env-file /absolute/project/.env
.venv/bin/python - /research/context <<'PYCODE'
import sys, json
from pathlib import Path
from replay.preparation import load_snapshot
root = Path(sys.argv[1])
snapshot = load_snapshot(root)
receipt = json.loads((root / 'receipt.json').read_bytes())
print('snapshot_sha256:', receipt['snapshot_sha256'])
print('outcomes_provider:', snapshot.get('outcomes', {}).get('provider'))
PYCODE
```

2. Copy `configs/cross_venue_arbitrage_v1.example.json` into `/research/cross.json`.
   Replace the snapshot hash, reviewed fee catalog directory/identity and reference
   nanoseconds. Supply reviewed native `assets` and `instrument_bindings` using
   [the existing fee contract](SAME_VENUE_COMPLEMENT_V1.md#1-factory-and-closed-configuration).
   Add every required quote asset, including its ledger/token identity, to
   `valuation.assets`, sorted by canonical JSON. Empty bindings or valuation
   coverage produce explicit unknown economics, not a net research result.
   The template deliberately contains no invented catalog, native asset or timestamp.
   Policy uses the existing 1/10/25/50/100/250/500/1000-contract sweep. Start with a
   small pinned interval, then preregister any size/latency changes before the corpus.

3. Build a direct supervisor config `/research/cross-run.json` under
   [REPLAY_SUPERVISOR_V1.md](REPLAY_SUPERVISOR_V1.md), with absolute Python/publisher
   and derivative paths. Transport pins/bounds/plans must equal the prepared
   context; scales must also match the typed normalizer descriptor. Use groups
   `["coverage", "cross"]`, `replay.bundle_coverage:build` for coverage, and:

```json
{"factory":"replay.cross_venue_arbitrage:build",
 "revision":"<frozen implementation revision and source digest>",
 "config":"<replace this string with the complete /research/cross.json object>"}
```

   This is the strategy entry template; `config` is a JSON object in the final
   supervisor file. There is no new jobs registry or infrastructure. A convenient
   assembly step for an already reviewed supervisor template is:

```bash
.venv/bin/python - /research/cross-run-template.json /research/cross.json /research/cross-run.json <<'PYCODE'
import sys
from pathlib import Path
from replay.preparation import MAX_BYTES, encoded
from replay.streams.protocol import decode
def read(path):
    with open(path, 'rb') as stream:
        return decode(stream.read(MAX_BYTES + 1), MAX_BYTES)
run, strategy = read(sys.argv[1]), read(sys.argv[2])
assert run['strategies']['cross']['factory'] == 'replay.cross_venue_arbitrage:build'
run['strategies']['cross']['config'] = strategy
with Path(sys.argv[3]).open('xb') as stream:
    stream.write(encoded(run) + b'\n')
PYCODE
```

4. Validate before launching, on the later authorized Linux host with prebuilt
   publisher and a supplied disposable dedicated Redis >=8.2 (positive maxmemory,
   noeviction). No service startup is performed by this recipe:

```bash
.venv/bin/python - /research/cross-run.json <<'PYCODE'
import sys
from replay.supervisor import read, validate, initial
from replay.cross_venue_arbitrage import CrossVenueArbitrage
from replay.economic_sdk.entity_tables import preflight
from replay.streams.protocol import freeze
c = validate(read(sys.argv[1]))
strategy = CrossVenueArbitrage(c['strategies']['cross']['config'])
print('entity_table_bytes:', preflight(strategy))
strategy.bind(freeze(initial(c)))
print('metadata and snapshot/transport binding verified')
PYCODE
.venv/bin/python -m replay.supervisor /research/cross-run.json /research/cross-run
.venv/bin/python - /research/cross-run <<'PYCODE'
import sys, json
from replay.cross_venue_output import read_completed
from replay.coverage_output import read_completed as coverage
coverage(sys.argv[1], 'coverage')
result = read_completed(sys.argv[1], 'cross')
print(json.dumps(result['summary'], sort_keys=True, indent=2))
PYCODE
```

Set `REDIS_URL` separately in the environment, never in JSON. Preserve failed
attempts and both completed readers' semantic identities. For uncommitted work,
freeze a copy and record source-file SHA-256 values in addition to the base commit;
a revision string alone does not establish installed code identity. This recipe
does not authorize or demonstrate a retained-data, cloud or service run.

The default latency tiers are explicitly planning scenarios: 250 ms, 1 s, 5 s.
When reviewing venue-specific matching delay evidence, preregister combined
decision/send/matching budgets as additional `latency_tiers_ns` (up to eight).
Time-shift controls are optional sensitivity measurements; Q(L) never proves two
venues could execute atomically. The summary uses basket-direction-nanoseconds,
not elapsed event time or earned dollars; routes may share displayed liquidity.

## Server sizing measurements for the later pilot

Measure, without extrapolating the unit-test timings: entries/cuts per second by
phase and venue; wall and CPU time (decode, SDK views, fees, reader/finalization);
peak RSS per publisher/strategy/Redis plus temporary disk; read/write bytes and
IO throughput; output bytes per bundle/event-hour, size sweep and control mode;
queue/backpressure and slowest-group progress; and throughput/RSS/IO versus
concurrent independent event/bundle workers. Record counts of planned books,
candidate/admitted routes, scope durations, episodes and slices, fee resolution
cache behavior and unknown-input cohorts. Event/bundle concurrency must not mix
evidence identities or reuse liquidity as simulated fills. No corpus benchmark
or server size is claimed by this offline implementation.

## Offline validation report (2026-10-05)

Base: `a11bbf0ba4f78af7b27b382b913dbd71a41ea3f4`, verified against the remote
`feat/same-venue-complement` tip before work. Branch:
`codex/cross-venue-arbitrage-v1`. Managed worktree:
`/Users/hridyansh/.codex/worktrees/cross-venue-arbitrage/Prediction Indexer`.
Changes are unstaged and uncommitted. The main checkout and untracked research
memo are preserved. The code/config source identity is recorded in
[CROSS_VENUE_ARBITRAGE_V1_SOURCE.json](CROSS_VENUE_ARBITRAGE_V1_SOURCE.json); it lists
SHA-256 values for every changed executable/configuration file and supporting SDK
documentation. This report itself is not part of the source digest.

Changed files:

- `replay/cross_venue_contract.py`: closed config, masks, deterministic bounded routes.
- `replay/cross_venue_arbitrage.py`: SDK factory and exact native economics.
- `replay/cross_venue_output.py`: independent provisional and completed readers.
- `replay/complement_fees.py`: reusable per-order assessments, native economics lookup,
  typed unrepresentable notional condition; legacy accountant is preserved.
- `replay/economic_sdk/{__init__,entities}.py`: explicit native-scale admission opt-in.
- `replay/prepare_context.py`, `.env.example`: endpoint-only dotenv preparation.
- `replay/tests/test_cross_venue_arbitrage.py`, `replay/tests/test_prepare_context.py`:
  offline integrated contracts, arithmetic, bounds, receipt and configuration tests.
- `configs/cross_venue_arbitrage_v1.example.json`: reviewed-input configuration template.
- This contract, its source identity, `docs/ECONOMIC_STRATEGY_SDK_V1.md`,
  `docs/STRATEGY_PREPARATION_V1.md`, `replay/README.md`: contract and invocation docs.

All commands ran with the project's virtual environment interpreter:
`/Users/hridyansh/Personal Projects/Prediction Indexer/.venv/bin/python`.
From the managed worktree, the final focused gate passed **113 tests**:

```bash
"/Users/hridyansh/Personal Projects/Prediction Indexer/.venv/bin/python" -m unittest \
  replay.tests.test_cross_venue_arbitrage replay.tests.test_prepare_context \
  replay.tests.test_complement_fees replay.tests.test_economic_sdk \
  replay.tests.test_economic_sdk_port replay.tests.test_economic_sdk_outcomes \
  replay.tests.test_same_venue_complement replay.tests.test_complement_output \
  replay.tests.test_preparation replay.tests.test_preparation_outcomes \
  replay.tests.test_market_profile -q
```

The tests include offline Universe-shaped outcomes through actual preparation v2,
mask-backed baskets, real in-place Decoder books, the shared SDK runtime and
independent readers. The completed reader uses the real SUCCESS/participant
reader with synthetic supervisor attestations; only external Rust metadata
preflight is stubbed. That is not a Rust/Risk/Redis end-to-end acceptance claim.
Existing seven complement V1 golden scenarios retain every pinned output byte.

Broader gates:

| Command (same interpreter) | Result on macOS |
|---|---|
| `-m unittest discover -s tests -q` | 1,026 tests; one failure in existing stale retrieval cleanup. |
| `-m unittest discover -s replay/tests -q` | Final run: 376 tests; 33 skipped, two failing signal subtests and one error in existing bundle-runner Linux process-containment tests. |
| `git diff --check` | Passed. |

Every broader failure reproduced in the user's untouched main checkout. The
failing test/implementation files are unchanged between that checkout's
`99c67e3` and the reconciled base. Retrieval cleanup relies on `/proc/{pid}`;
bundle-runner containment relies on Linux `prctl`, unavailable on this macOS host.
No acceptance thresholds were weakened or platform failures hidden. Redis
integration and Linux process containment remain unverified here. No Rust code
changed, so Rust gates were not run. No Compose/deployment or cloud/live step ran.

Hand-checked witnesses at one contract per leg:

| Witness | Mask and native economics | Research result |
|---|---|---|
| Positive | Kalshi home mask and PM away mask partition the six Bo3 keys. Kalshi opposite bid .60 projects to BUY .40 USD; PM BUY .57 USDC. With the explicit parity scenario, supported synthetic Kalshi quadratic multiplier 1/non-direct fees debit .02 USD; synthetic evidenced PM zero fee. | Gross floor 1 minus .97 = **.03**. Net native cash: -.42 USD and -.57 USDC. Net normal-resolution floor 1; scenario edge **.01**. |
| Mask rejection | Kalshi home and PM home both pay on `{seq:AHH, seq:HAH, seq:HH}`. Their overlap leaves the away outcomes uncovered. | `UNSUPPORTED_SHAPE`, structured `overlap` reason; full scoped denominator retained. |
| Fee rejection | Limitless home BUY .40, PM away BUY .58; configured Limitless 3% BUY fee debits .03 outcome contracts. | Gross **+.02**, received payout floor **.97**, native cost **.98**, net **-.01**; gross episode, no net episode. |

Every witness is synthetic and labelled `normal_resolution_only`, provider
`universe`, with explicit native asset identities and `PARITY_SCENARIO`. No
settlement/source compatibility, historical fee applicability or realized profit
is established. Unrepresentable scale-18 partial-fill notionals preserve exact
scale-36 gross arithmetic and become FEE_UNKNOWN; there is no rounding workaround.

For this local worktree, invoke preparation with the actual existing interpreter
and your main project's dotenv path (after creating reviewed preparation JSON):

```bash
cd "/Users/hridyansh/.codex/worktrees/cross-venue-arbitrage/Prediction Indexer"
"/Users/hridyansh/Personal Projects/Prediction Indexer/.venv/bin/python" \
  -m replay.prepare_context /research/prepare.json /research/context \
  --env-file "/Users/hridyansh/Personal Projects/Prediction Indexer/.env"
```

This invocation was tested with temporary synthetic dotenv data and mocked HTTP;
it was not run against the user's real dotenv file or Universe server. Preserve
this checkout on Python's import path when launching supervisor subprocesses.
Reviewed real fee evidence/bindings, real capture coverage, endpoint reachability,
retained-data replay, Linux Redis/publisher acceptance, actual execution latency
and all server-sizing metrics remain unverified. No corpus throughput, peak RSS,
IO, output-byte or bundle-parallelism benchmark is invented.


## Metadata overflow correction (2026-10-06)

The reported 14-scope default sweep failed only at final metadata emission.
Before changing production code, a hand-authored identity-valid fixture with
10 books per venue, 14 scopes and the actual eight-size sweep reproduced
`Runtime.finish -> _tables -> _write_table -> LineWriter.append`, raising
`ProtocolError: output line budget`. Its canonical table (including LF) was
18,282,977 bytes for 11,312 entities. A second regression showed construction
accepted it and opened outputs instead of rejecting before consumption.

The compact contract now retains 90 unsupported routes, one uncaptured member
and 80 admitted size-specific entities per scope (171 total), with every scoped
status denominator intact. Seven new tests cover full-sweep completion,
four/eight shared-size time and episodes, optional controls, exact 8 MiB success
and one-byte-over failure for real/control tables, empty output on construction
rejection, deterministic ordering, rejection-to-admission scope transitions,
strict output-format rejection and completed reading. The scope-transition case
uses canonical subscriptions and actual preparation validation; it does not
weaken outcome token alignment.

The local retained context was found under the main checkout's documented
`.bench/cross/context`. The default eight-size preflight measures **2,798,865
bytes**, versus the reported original 13,149,792 bytes. That reduction comes from
one static rejection entity per route/scope, without increasing any bound.
The original byte count excludes the final LF: reconstructing the original
size-expanded table gives exactly 13,149,792 JSON bytes, or **13,149,793 persisted
bytes**. The compact table has 1,918 entities rather than 9,072: per scope,
72 null-size unsupported pairs, one null-size missing member, and 64 admitted
entities (eight routes at eight sizes).


### Retained fixture acceptance and equality

Both sweeps ran sequentially using existing local image
`sha256:3b31e4a7a2ee77021452ff5d7352aa50bcb28cff647a2284e5a4a6dc8a8de515`,
the same frozen Python source, read-only retained context/derivatives and a
separate disposable Redis 8.2 container on an internal task network. No image
pull, cloud request, credential access or production-service mutation occurred.
The pinned context is
`28f924a9106312a8123a5e338db549f836ca6ad119002a7e3be83e64b7bd7b66`,
bundle `bundle_e8a92effa246b9548571c907`, window
`1790548805943960000–1790557204235443000`, 14 scopes, 18 plans and terminal
sequence **2,008,580**. Both runs used the empty fee catalog, explicit native
research bindings/parity scenario, no controls and one supervisor attempt.

| Sweep | Result | Exact entity table bytes | Local harness wall time |
|---|---|---:|---:|
| 1/10/25/50/100/250/500/1000 | SUCCESS; both completed readers passed | 2,798,865 | 231.0 s |
| 1/10/100/1000 | SUCCESS; both completed readers passed | 2,138,513 | 140.2 s |

Wall times include completed rereading and reflect shared local resources. They
are acceptance observations, not corpus throughput or server-sizing estimates.
An earlier successful eight-size run took 205.7 s before the usage interruption;
its temporary output disappeared. Only its log is preserved. The two durable
runs above provide the independently reread evidence.

The final offline comparison independently read both supervisor SUCCESS results
again, without a network, and proved all **105** common summary rows byte-for-byte
equal, including every route's status/class time, gross-positive time, episode
and slice counts, Q, skew totals and quantiles. Every common scoped entity's
descriptor and hash also matched. The best historical route
`8cae068212e56302c9a83cbfb9233b5cc137422e85e684787d9ca24bbd370c5e`
at size 1 retains exactly **233,812,572,914 gross-positive ns** (233.812572914 s)
and **118 gross episodes**. Fees are unknown; this is not a net opportunity claim.

Full cross semantic identities:

- Eight sizes: `dac3e1713c3a23b8f0838c6f1225b0570cf544b2bdc66efecf335295afc4b33d`.
- Four sizes: `5931292e5563514617ab09784571b74dfff54df97752035d1291e77544af89b0`.
- Coverage, both runs: `0b933bbd2242d7628bb18e6b8444e73dd2a50446d28a281daf4f268f7d39bf1a`,
  unchanged from the user's historical reference.

Eight-size attempt `a73726017ae44e0d8cdd25046349ff77`, run
`cross-metadata-eight`; four-size attempt `14c9ab704fc94d1c8808dd0c9841b412`,
run `cross-metadata-four`. Complete receipt bindings and supervisor result files
are retained with the outputs. The user's original cross format-1 hash
`9cfa818e…` is historical evidence; it is not reused as a format-2 identity.

### Final gates, source identities and durable artifacts

The final focused command above, plus `replay.tests.test_cross_venue_metadata`,
passed **120 tests**. The seven pinned complement V1 synthetic scenarios remain
byte-identical. The new default-sweep regression passed after failing before the
fix at final table emission. Exact-bound success and one-byte-over rejection
used the actual 8 MiB limit, independently for real and control tables.

Recovered full gates (project virtual environment):

- `-m unittest discover -s tests -q`: **1,026 tests**, one existing macOS
  stale-retrieval cleanup failure (`/proc`); no socket-permission errors after
  allowing the tests' disposable loopback listeners.
- `-m unittest discover -s replay/tests -q`: **383 tests**, 33 skipped,
  two failing signal subtests and one error in existing Linux-only bundle-runner
  containment tests (`prctl`). These are the same failures reproduced in the
  untouched checkout during initial implementation.
- `git diff --check`: passed; index empty. No Rust code changed, so Rust and
  deployment gates were not run. Linux Redis/publisher acceptance is now verified
  for this one retained fixture; the host platform-only unit failures remain.

Metadata-fix source snapshot SHA-256:
`9d437d908f66965400dda211570d127542b51f981c6e3536634786aac034ec98`.
At that acceptance revision, all **19** listed source/config/document hashes
were verified and all **105** runner-frozen Python files matched the worktree;
each acceptance process checked their hashes before running. In addition to the
original implementation paths, this fix changes
`replay/economic_sdk/{entity_tables,runtime,aggregate_reader}.py`,
`replay/complement_output.py`, `replay/tests/test_cross_venue_metadata.py`,
the SDK/cross-venue contracts, subsystem README and source manifest.

Durable task-owned evidence is under
`/Users/hridyansh/.codex/worktrees/cross-venue-arbitrage/Prediction Indexer/.bench/cross-metadata-20261006-01a10d17/`:

- `out/fixed-eight/` and `out/fixed-four/`: supervisor configs, immutable run
  outputs, content receipts, SUCCESS, completed-read summaries and result reports;
- `out/comparison.json`: exhaustive shared-size equality and full receipts;
- `out/metadata-comparison.json`: original/compact counts and exact byte accounting;
- `acceptance.py`, `compare.py`, `source/`, `source-hashes.json`,
  `source-verification.json`, `task-artifacts.json`: runnable offline harness,
  frozen source and provenance;
- `logs/`: final focused/root/replay gates, both retained runs, comparison and
  the recovered prior eight-size log.

The task's Redis, runner containers and internal network are removed after
preserving evidence; no unrelated container is stopped or changed. The main
checkout, research memo, retained `.bench` inputs and old user outputs are
preserved. The metadata-fix handoff left everything unstaged and uncommitted.
No two-month corpus,
reviewed real fee model, live trading latency, throughput/RSS/IO benchmark,
server sizing or production rollout is claimed.


## Class-boundary correction and push validation (2026-10-06)

A subsequent user-supplied SDK fix prevents a zero-duration class entry when a
value-class change and consumed-quote slice close occur at the same instant.
`Runtime._charge_class` charges only nonempty intervals and is shared by class
changes and slice closes. Layout-1 accounting is unchanged. The regression drives
NET_NONPOSITIVE → NET_POSITIVE → NET_NONPOSITIVE through actual Decoder books,
the SDK writer and independent reader; the short positive slice qualifies at
1 ns, but contributes no class entry at the 5 ns tier.

Validation temporarily restored the two original accounting methods from the
frozen metadata-acceptance source, without editing production files. The new
regression failed at the independent reader with `ProtocolError: zero duration
entry`, then passed on the corrected methods. Final focused checks passed
**121 tests**, including all seven complement V1 byte goldens. Full replay ran
**384 tests**, with 33 skipped and the same two failing signal subtests and one
error in the existing macOS/Linux process-containment tests. Root's prior full
1,026-test gate remains applicable; this correction touches no root subsystem.
`git diff --check` passed. Rust and deployment gates were not rerun.

The retained eight/four acceptance above belongs to the preceding frozen runtime;
this class-boundary correction was not rerun on that retained fixture. Its
validation is the falsifying class-transition regression and the broader offline
gates. Earlier acceptance artifacts and source hashes remain immutable and are
not relabelled as the corrected source. Validation logs are preserved under
`.bench/cross-boundary-validation-20261006/`.

The refreshed source manifest additionally covers
`replay/tests/test_economic_sdk.py` and the corrected runtime; it now lists
20 source/config/document files. This validation supersedes the earlier
unstaged-only instruction: the user requested committing and pushing the branch
to GitHub. The main checkout, retained evidence and research memo remain preserved.
