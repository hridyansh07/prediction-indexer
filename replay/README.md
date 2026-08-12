# Standalone replay

Replay accepts only named immutable byte objects through `ByteStreamer`. Storage
selection is therefore an adapter decision:

- NFS/local: `DirectoryByteStreamer`
- tests and exact fixtures: `MemoryByteStreamer`
- receipt-committed raw S3 objects: `archive.ArchivedSegmentByteStreamer`
- receipt-committed Targeter records: `targeter.v2.replay_stream.ArchivedTargetRecordByteStreamer`

`CompositeByteStreamer` snapshots disjoint adapters into one lexically ordered
dataset and rejects duplicate logical keys. Gate 1 can therefore consume raw
segments and decoded `target_records_<venue>.ndjson` together without learning
about S3, Zstandard, or either receipt protocol.

No capture, ingester, targeter, or legacy-analysis module is imported.

The package is included in the installed distribution, including the frozen
terminal policy. Storage adapters provide bytes through `ByteStreamer`; none of
the replay, trust, economics, or execution code changes.

Target-record run selection includes every run in the half-open capture window
and the latest run strictly before it. Production run archive receipts remain
the authority: prefix listings are not accepted as commit evidence. Each read
freshly verifies the receipted remote manifest and selected object, fully stages
and verifies the decoded logical identity, and only then yields bytes.

## Ordered exit gates

Work advances only after the preceding gate is demonstrated against real venue
bytes.

1. `python -m replay.gate1 DATASET_ROOT` must pass every irreversible capture
   check. A report is content-addressed to its complete input object manifest.
2. Every analysis output must embed interval trust, coverage percentage,
   Polymarket hash-match rate, and leg-skew strata. Bare numeric output is a type
   error.
3. The economic headline is deployable-ticket VWAP net of conservative fees.
   Episode counts and uncapped gaps are diagnostics only. The subset LP ships
   together with its matched-leg placebo null.
4. Live replay reports quote lifetime and an explicitly named fill estimator;
   a detected opportunity is never labelled captured or filled.
5. Thresholds, controls, placebo construction, and resolution reconciliation
   are hashed before observations are evaluated. The terminal verdict includes
   `NO`.

Gate 1 is intentionally strict. A failed check is a capture-side gap and blocks
all later analysis; it is not an invitation to filter the fixture until it passes.

Run the complete sequence:

```bash
python -m replay.gate1 DATASET_ROOT --output gate1.json
python -m replay.gate2 DATASET_ROOT --output gate2.json
python -m replay.gate3 DATASET_ROOT --output gate3.json
python -m replay.gate4 DATASET_ROOT --output gate4.json
python -m replay.gate5 DATASET_ROOT --output gate5.json
```

Gate 5 validates and hashes `policy.json` before evaluating any tape bytes.
Its negative label is deliberately scoped:
`NO_DEPLOYABLE_EDGE_IN_FIXTURE`, never a claim that no edge can exist elsewhere.

## Trust and recovery semantics

- Polymarket state hashes may repeat across several frames in one logical
  update. Replay applies the complete hash run before checking the state.
- An independent snapshot mismatch opens `UNTRUSTED`; its full book recovers
  the chain, and later hashes are evaluated from that recovery point.
- Limitless full books are exact observations, but its non-dense monotonic
  version cannot prove that no update was dropped. Its completeness verdict is
  therefore `UNKNOWN`.
- Conflicting resolution fields are retained and labelled. In the live fixture,
  Limitless ETH records carried a stale `chainlinkPair=BTC/USD` alongside
  ETH/USD in the title, symbol, URL, and rules. Redundant-field consensus
  displays ETH/USD, while `CONFLICT` prevents exact cross-venue identity.

## Economic and execution semantics

The economic headline is a same-condition, share-matched long basket at 100
contracts. Each leg walks displayed depth and uses the captured fee curve with
conservative per-leg rounding. The symbolic LP solves the payout-cover problem
at the same ticket VWAP. Its matched placebo replaces one leg with the
nearest-time different condition while retaining the same symbolic incidence
matrix; the placebo is a null, never a locked basket.

Execution uses the named `DISPLAYED_DEPTH_SURVIVAL_100MS` estimator. It measures
how long the exact ladder slice required for the ticket remains unchanged. It
does not observe queue position, order acknowledgements, trades, or fills, and
cannot label a quote as captured or filled.
