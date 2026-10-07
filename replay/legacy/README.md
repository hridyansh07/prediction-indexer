# Legacy replay gates

This package contains the original raw-envelope replay pipeline: Gates 1–5,
their normalizers, book reconstruction, trust audit, economic analysis,
execution estimator and resolution reconciliation. The frozen terminal policy
is [policy.json](policy.json), and the capture-audit findings are documented in
[GATE1_FAILURES.md](GATE1_FAILURES.md).

Gate 1 remains available for immutable capture-fixture audits. The later gates
retain the older binary-market metadata and fixed-ticket assumptions described
below. This package preserves their behavior and report identities.

Current strategies live in [../strategies](../strategies/README.md). For current
preparation, SDK, streams, supervisor, bench and jobs entry points, start with
the [replay README](../README.md).

Imports and module commands now use `replay.legacy.<module>` in place of the
earlier `replay.<module>` paths. The five command examples below show the updated
invocations. Gate 5 finds its default policy beside its own module, including in
an installed distribution.

Shared byte adapters, lane ranking and target-record projection remain in
`replay.stream`, `replay.lanes` and `replay.catalog`; current archive and Targeter
code also consumes those contracts.

## Ordered exit gates

Work advances only after the preceding gate is demonstrated against real venue
bytes.

1. `python -m replay.legacy.gate1 DATASET_ROOT` must pass every irreversible capture
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
python -m replay.legacy.gate1 DATASET_ROOT --output gate1.json
python -m replay.legacy.gate2 DATASET_ROOT --output gate2.json
python -m replay.legacy.gate3 DATASET_ROOT --output gate3.json
python -m replay.legacy.gate4 DATASET_ROOT --output gate4.json
python -m replay.legacy.gate5 DATASET_ROOT --output gate5.json
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
