# Strategy scope preparation V1

Implemented preparation only. No coverage algorithm, economic strategy, scheduler,
query language, new reconstruction policy, or strategy output writer is included.
`replay.preparation` resolves one historical bundle or an explicit market subset
before replay starts. Runtime consumes a saved immutable snapshot without network
access. There is no shared cache, Redis cache, or cross-run response reuse.

## Python API and closed configuration

```python
from pathlib import Path
from replay.preparation import UniverseHTTP, prepare, load_snapshot
from replay.preparation_sources import ArchivedSelections
from archive.storage.factory import build_store
from targeter.v2.run_archive import read_run_archive_receipt

# Configuration JSON is caller-owned; decode with the strict replay JSON decoder.
# Receipts include every requested run and any retained occurrence's complete origin.
fallback = ArchivedSelections(
    build_store(primary_roots=[Path("/capture")]),
    [read_run_archive_receipt(path) for path in receipt_paths],
)
snapshot = prepare(config, Path("/research/run-1/context"),
    universe=UniverseHTTP(universe_base_url, timeout=10), fallback=fallback)

# On retry/offline replay, never instantiate network sources:
snapshot = load_snapshot(Path("/research/run-1/context"),
                         expected_sha256=pinned_context_sha256)
```

The Universe URL is directly configurable, HTTP(S), with no proxy requirement or
proxy-environment use. The client requests only
`GET /v1/runs/{run_id}/selections/{bundle_id}`; it never uses current-era market or
claim membership as historical evidence. For an in-process read, supply
`universe=lambda occurrence, bundle: store.selection_detail(occurrence['run_id'], bundle)`.

All fields below are required; unknown fields and duplicate JSON keys fail. Version
is integer 1. Times/scales use the existing Replay unsigned decimal **strings**;
pins and `lower_bound` have the existing supervisor/Risk meaning. Digest placeholders
must be replaced with 64 lowercase hex characters.

```json
{
  "version": 1,
  "pins": [{"derivative_address": "<sha256>", "receipt_sha256": "<sha256>"}],
  "start_ns": "100", "end_ns": "300", "lower_bound": "clip",
  "bundle_id": "<targeter-bundle-id>",
  "market_namespace": "targeter_target_id",
  "probe_markets": null,
  "occurrences": [{
    "run_id": "20260920T120000.000000Z", "start_ns": "100", "end_ns": "300",
    "source": {
      "manifest_key": "targeter-v2/runs/date=2026-09-20/run=20260920T120000.000000Z/run_manifest.json",
      "manifest_sha256": "<stored manifest sha256>",
      "report_key": "targeter-v2/runs/date=2026-09-20/run=20260920T120000.000000Z/selection_report.json.zst",
      "report_sha256": "<stored report sha256>"
    }
  }],
  "authorities": [
    {"venue": "kalshi", "lane": "caller-chosen-primary", "price_scale": "2", "quantity_scale": "0"},
    {"venue": "polymarket", "lane": "caller-chosen-pm", "price_scale": "3", "quantity_scale": "6"},
    {"venue": "limitless", "lane": "caller-chosen-ll", "price_scale": "3", "quantity_scale": "6"}
  ]
}
```

The tiny times above illustrate the schema, not a real historical occurrence.
`probe_markets: null` means **every listed market**, not only capture-selected
targets. A subset is a nonempty sorted unique list of `venue:native-market-id`
Targeter IDs. It must belong to every specified occurrence. Universe canonical
market/claim IDs, PM token IDs used as market IDs, and arbitrary ad-hoc markets are
not accepted namespaces. At most one explicit authority per venue is supported;
missing required authority fails. Preparation resolves native books and emits
`plans`; callers do not need to provide already-resolved instrument IDs. Unused
venue authorities are harmless and preserved as configuration, not inferred roles.

## Historical intervals are expectations, not recorded socket membership

The ordered `occurrences` must partition the entire requested half-open interval
without gaps or overlaps. Each interval independently pins a run and its report.
At a shared boundary the new scope applies; nothing applies at the final end.
Repeated references to one run must produce identical evidence. Distinct reports
may change listed, selected, and requested members across boundaries.

These intervals are **caller declarations**, not selection/publication/handoff
times inferred by preparation. Snapshots always state
`membership_basis: "caller_pinned_expectations"` and `history_complete: false`.
A selection occurrence proves neither actual socket subscription nor retirement.
No latest-state extrapolation, predecessor heuristic, or automatic history scan
is implemented. Thus there is no pagination to truncate or silently omit. Obtain
and explicitly pin the relevant occurrences before preparing; automatic complete
history resolution is deferred. Any Universe retirement information is preserved
as evidence only. Fallback does not query later reports to invent retirement.

## Source precedence and native mapping

Universe is primary. Only a missing selection (`None`/HTTP 404), explicit
`SourceUnavailable`, HTTP 502/503/504, or a network timeout/connection failure may
select Targeter fallback. Other HTTP errors, malformed data, unsupported schema,
pin conflicts, ambiguous mapping, and resource-limit failures abort. Empty valid
relationships do **not** trigger fallback. Universe is a trusted projection
service: supplied source/origin hashes bind its asserted provenance; preparation
does not also download the archive to authenticate every successful Universe read.

`ArchivedSelections` uses the existing `ArchivedTargeterRunByteStreamer` over the
shared ObjectStore (`ARCHIVE_BACKEND=s3` or `gcs`), preserving production receipt,
provider checksum, stored/logical hash, exact Zstandard decoding, and manifest
verification. It adds no cloud client and performs no listings, discovery, venue
requests, or Targeter reruns. It supports version-2 manifests and version-3 reports.
It reuses Universe's selected-bundle projection and context validation. A retained
occurrence requires the receipted complete origin and exact target/timing equality;
an absent origin or conflicting report fails. Ordinary retained local unreceipted
files are not a fallback authority. Tests use a disposable independent-conformance
ObjectStore with real compressed bytes, not live cloud services.

Mapping follows the current normalizers, independently of sport/product:

| Evidence | Required native BookKeys |
|---|---|
| PM subscription token IDs | one `polymarket:<token>`, `outcome`, per token |
| Kalshi ticker (must match target ID suffix) | `kalshi:<ticker>`, both `outcome` and `complement` |
| Limitless single subscription slug | `limitless:<slug>`, `outcome`, even when market ID differs |
| Listed but unselected market | retained member with `uncaptured_mapping_unknown`, no guessed books |

Missing/duplicate subscriptions on selected targets, one native book mapped to
multiple listed markets, or malformed IDs fail even when probing a subset. No implicit 1−p ladder, economic
complement projection, outcome masks, or fee inference is performed.

## Snapshot and stage-2 consumption contract

`context.json` is closed schema version 1 with:

- `config`: the full pinned configuration above;
- `evidence`: one `{provider: "universe"|"targeter", detail: <selection-detail>}`
  per occurrence, including full context, relationships, source and origin hashes;
- `plans`: ordered union of native Replay book plans with caller-specified
  `lane`, `venue`, `price_scale`, and `quantity_scale`;
- `scopes`: ordered half-open expectation intervals. Each contains `start_ns`,
  `end_ns`, `run_id`, `bundle_id`, `context_sha256`, `listed_market_ids`,
  `capture_selected_market_ids`, `members`, `required_books`, and
  `unresolved_market_ids`;
- `membership_basis` and `history_complete` as described above.

Each member is `{market_id, capture_selected, mapping_status, books}`. Each book
is `{instrument, orientation}`. The bundle ID names the single expected group for
that interval. `members` is the requested denominator; `required_books` is only
the native reconstruction scope. **Do not drop unresolved members or report a
fully covered group when `unresolved_market_ids` is nonempty.** An all-uncaptured
probe has no native plans but retains all requested members; downstream code must
report unavailable input rather than treating an empty conjunction as coverage.

Stage 2 should bind the snapshot's SHA-256 in its strategy configuration, load it
offline, assert pins/interval/lower-bound/plans equal its independently supplied
transport configuration, and consume scope changes in order. It must not use
those changes to mutate Risk policy or infer lifecycle evidence. Publisher groups,
derivative filesystem paths, and executable identity remain supervisor concerns.

`receipt.json` (version 1) is the commit marker, written last after fsync/rename
of context. It records `snapshot_sha256` over exact file bytes,
`snapshot_byte_length`, and `config_sha256` over canonical UTF-8 JSON (sorted keys,
compact separators, no ASCII escaping). `context_sha256` uses the same canonical
encoding for the historical context, matching Universe's context hashing.
`load_snapshot` verifies hashes and the closed schema and independently resolves
the evidence again, rejecting altered plans/scopes even with a rehashed receipt.
It returns deeply immutable mappings/tuples. For an externally pinned identity,
pass `expected_sha256`; a colocated receipt alone is not an authenticity signature.

The same directory/config loads without source calls. Changed config fails; a
context without receipt is uncommitted and requires a new directory. A local flock
prevents concurrent preparation. Files are immutable by ownership contract, as
with supervisor attempts. This is one run's reproducibility input, not a cache or
strategy result receipt. A new run may explicitly consume an existing pinned
snapshot but must not silently refresh it or share a mutable cache.

## Bounds and verification

Limits: 8 MiB each config, HTTP body, context, manifest, stored/decoded report;
8 MiB aggregate retained evidence; 128 intervals; 4096 markets per context;
8192 native books per scope and union plan; 4096 derivative pins; 256 archive receipts and 16384
receipt objects. The final serialized snapshot must also fit 8 MiB. Resource
exhaustion fails closed. HTTP has no redirects/retries, a 0–60 second finite
socket timeout (exclusive lower bound), and a checked response deadline (one
blocking read may extend it by the socket timeout). Cloud transport limits remain
owned by the existing ObjectStore adapters. Large reports require a separately
reviewed limit change, not truncation.

```bash
.venv/bin/python -m unittest replay.tests.test_preparation
.venv/bin/python -m unittest tests.test_targeter_replay_stream tests.test_event_universe_store \
  replay.tests.test_streams replay.tests.test_supervisor
```

Live Universe/S3/GCS and Redis execution are not required for these offline
tests and were not used to validate this preparation stage.
