"""The venue's own record for every market a run subscribes to.

`CanonicalMarket.as_record()` writes the normalized 22-field interpretation and
drops `raw`, so the record the venue actually published is fetched and then
discarded. Replay cannot interpret a tape without it: which token is YES, what
the market resolves against, the minimum order size, when it fixes, and what the
fee is all live in the venue's record and nowhere else.

Scope is the *selected* markets, not the whole catalogue. Only selected markets
are ever subscribed, and only subscribed assets appear in the tape, so a record
for anything else is evidence for a question that cannot be asked. In a real
shadow run that is 12 markets against 5,301 catalogued.

The record is stored **verbatim**. Trimming it to the fields
`replay.catalog._instrument` reads today would save tens of kilobytes per run and
cost every field nobody has thought of yet, which is the mistake this module
exists to undo.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from replay.catalog import (
    CAPTURED,
    canonical_sha256,
    projection_id,
    projection_sha256,
)
from targeter.v2.domain import CanonicalMarket, CatalogSnapshot

__all__ = [
    "TARGET_RECORD_VERSION",
    "artifact_stem",
    "target_record_rows",
]

TARGET_RECORD_VERSION = 1


def artifact_stem(venue: str) -> str:
    return f"target_records_{venue}"


def target_record_rows(
    *,
    run_id: str,
    observed_at: str,
    venue: str,
    catalogs: Iterable[CatalogSnapshot],
    targets: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Rows for one venue, plus a diagnostic per selected market with no record.

    Returns rows in `target_id` order so the artifact's bytes are a function of
    its content and not of dictionary iteration order.
    """
    markets: dict[str, CanonicalMarket] = {}
    for catalog in catalogs:
        if catalog.venue != venue:
            continue
        for market in catalog.markets:
            markets[market.target_id] = market

    rows: list[dict[str, Any]] = []
    diagnostics: list[str] = []
    for target in sorted(targets, key=lambda item: str(item.get("target_id"))):
        target_id = str(target.get("target_id") or "")
        market = markets.get(target_id)
        if market is None:
            # The selection named a market this run's catalogue does not carry.
            # Reported rather than skipped: a subscribed asset with no record is
            # precisely what makes a leg unanalysable, and it must not be
            # discoverable only by its absence.
            diagnostics.append(f"{target_id}: selected but absent from the catalogue")
            continue
        record = dict(market.raw)
        if not record:
            diagnostics.append(f"{target_id}: catalogue market carries no raw record")
            continue
        rows.append(
            {
                "version": TARGET_RECORD_VERSION,
                "run_id": run_id,
                "venue": venue,
                "target_id": target_id,
                "subscription_ids": list(target.get("subscription_ids") or ()),
                "observed_at": observed_at,
                "provenance": CAPTURED,
                "projection_id": projection_id(venue),
                "projection_sha256": projection_sha256(venue, record),
                "record_sha256": canonical_sha256(record),
                "record": record,
            }
        )
    return rows, diagnostics
