#!/usr/bin/env python3
"""Does `seq` stay continuous across a snapshot the poller asked for?

Reads captured Kalshi spool segments and answers the one question the poller
leaves open. The trust layer reads `seq` as proof that no message was dropped;
if asking for a snapshot silently restarts that sequence, every solicited
snapshot would read as a gap, and the poller would manufacture exactly the
doubt it exists to remove.

Read-only. Point it at a spool tree; it opens nothing else and writes nothing
back.

    python scripts/kalshi_snapshot_continuity.py \\
        /var/lib/prediction-indexer/spool --date 2026-08-12

Solicited snapshots are found through the `orderbook_reconciliation_request`
control records the poller writes, not by guessing from timing. That is the
whole reason those records exist: a requested snapshot and a handshake snapshot
are otherwise identical on the tape.

Natural snapshots are measured alongside as a control. If `seq` is continuous
across both, the assumption holds and the trust layer can read a solicited
snapshot exactly as it reads a handshake one. If it is continuous across natural
ones and not solicited ones, `get_snapshot` restarts the sequence, and the trust
layer needs a `kalshi_seq_reset_at_snapshot` reason class before it can tell
that apart from a real drop.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

BOOK_STREAM = "public_book"
REQUEST_EVENT = "orderbook_reconciliation_request"


def iter_records(root: Path, date: str | None) -> Iterator[dict[str, Any]]:
    """Every envelope in the Kalshi lane, oldest segment first.

    Handles both spellings a spool carries: `.ndjson` for a sealed segment and
    `.ndjson.zst` for a materialized derivative beside it. A segment present in
    both forms is read once.
    """
    lanes = sorted(root.rglob("lane=kalshi/date=*"))
    if date is not None:
        lanes = [item for item in lanes if item.name == f"date={date}"]
    if not lanes:
        raise SystemExit(f"no lane=kalshi/date=* directories under {root}")

    for directory in lanes:
        seen: set[str] = set()
        for path in sorted(directory.iterdir()):
            name = path.name
            stem = name.removesuffix(".zst").removesuffix(".ndjson")
            if not name.endswith((".ndjson", ".ndjson.zst")) or stem in seen:
                continue
            seen.add(stem)
            if name.endswith(".zst"):
                try:
                    import zstandard
                except ImportError:
                    raise SystemExit("zstandard is required to read .ndjson.zst segments")
                with path.open("rb") as handle:
                    reader = zstandard.ZstdDecompressor().stream_reader(handle)
                    for line in reader:
                        if line.strip():
                            yield json.loads(line)
            else:
                with path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if line.strip():
                            yield json.loads(line)


@dataclass
class Request:
    command_id: int | None
    tickers: tuple[str, ...]
    visible_ns: int
    fulfilled: dict[str, int] = field(default_factory=dict)


@dataclass
class Snapshot:
    ticker: str | None
    solicited: bool
    continuous: bool | None       # None when either side carried no usable seq
    previous_last: int | None
    last_seen: int | None
    visible_ns: int
    byte_length: int


def analyse(records: Iterator[dict[str, Any]]) -> tuple[list[Snapshot], list[Request]]:
    # `seq` is dense per subscription, not per connection, so continuity is
    # tracked per (epoch, stream) exactly as the ingester keys it. Comparing
    # across streams is what once scored 431 phantom `cursor_went_backwards`.
    last_seq: dict[tuple[str, str], int] = {}
    pending: dict[int, Request] = {}
    requests: list[Request] = []
    snapshots: list[Snapshot] = []
    outstanding: dict[str, Request] = {}

    for record in records:
        stream = record.get("stream")
        epoch = str(record.get("connection_epoch"))
        payload_text = record.get("raw_payload") or ""

        if stream == "process":
            try:
                control = json.loads(payload_text)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(control, dict) or control.get("event") != REQUEST_EVENT:
                continue
            request = Request(
                command_id=control.get("command_id"),
                tickers=tuple(control.get("market_tickers") or ()),
                visible_ns=int(record.get("visible_ns") or 0),
            )
            requests.append(request)
            if request.command_id is not None:
                pending[request.command_id] = request
            # A ticker is solicited from the request until its snapshot lands.
            for ticker in request.tickers:
                outstanding[ticker] = request
            continue

        if stream != BOOK_STREAM:
            continue

        try:
            payload = json.loads(payload_text)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(payload, dict):
            continue

        kind = payload.get("type")
        if kind not in ("orderbook_snapshot", "orderbook_delta"):
            # Control acknowledgements default onto the book stream with no
            # cursor. They are not book state and must not move `last_seq`.
            continue

        cursor = record.get("source_cursor") or {}
        key = (epoch, str(stream))
        previous_last = cursor.get("previous_last")
        seen = last_seq.get(key)

        if kind == "orderbook_snapshot":
            body = payload.get("msg") if isinstance(payload.get("msg"), dict) else payload
            ticker = body.get("market_ticker") or payload.get("market_ticker")
            request = outstanding.pop(ticker, None) if ticker else None
            continuous: bool | None
            if not isinstance(previous_last, int) or seen is None:
                continuous = None
            else:
                continuous = previous_last == seen
            snapshots.append(
                Snapshot(
                    ticker=ticker,
                    solicited=request is not None,
                    continuous=continuous,
                    previous_last=previous_last if isinstance(previous_last, int) else None,
                    last_seen=seen,
                    visible_ns=int(record.get("visible_ns") or 0),
                    byte_length=len(payload_text),
                )
            )
            if request is not None and ticker:
                request.fulfilled[ticker] = int(record.get("visible_ns") or 0)

        if isinstance(cursor.get("last"), int):
            last_seq[key] = cursor["last"]

    return snapshots, requests


def report(snapshots: list[Snapshot], requests: list[Request]) -> int:
    def summarize(items: list[Snapshot], label: str) -> tuple[int, int]:
        checked = [s for s in items if s.continuous is not None]
        broken = [s for s in checked if not s.continuous]
        print(f"\n{label}: {len(items)} snapshot(s), {len(checked)} with a checkable seq")
        if checked:
            print(f"  continuous: {len(checked) - len(broken)}/{len(checked)}")
        for item in broken[:10]:
            print(
                f"  BREAK {item.ticker}: previous_last={item.previous_last} "
                f"last_seen={item.last_seen} delta={(item.previous_last or 0) - (item.last_seen or 0)}"
            )
        return len(checked), len(broken)

    solicited = [s for s in snapshots if s.solicited]
    natural = [s for s in snapshots if not s.solicited]

    print(f"reconciliation requests: {len(requests)}")
    print(f"snapshots seen: {len(snapshots)}  solicited: {len(solicited)}  natural: {len(natural)}")

    _, natural_broken = summarize(natural, "NATURAL (control)")
    solicited_checked, solicited_broken = summarize(solicited, "SOLICITED (the question)")

    if requests:
        asked = sum(len(r.tickers) for r in requests)
        got = sum(len(r.fulfilled) for r in requests)
        print(f"\nfulfilment: {got}/{asked} requested tickers received a snapshot")
        latencies = sorted(
            (t - r.visible_ns) / 1e6
            for r in requests
            for t in r.fulfilled.values()
            if t >= r.visible_ns
        )
        if latencies:
            mid = latencies[len(latencies) // 2]
            print(f"request -> snapshot latency: p50 {mid:.0f} ms  max {latencies[-1]:.0f} ms")

    if solicited:
        sol_bytes = sum(s.byte_length for s in solicited) / len(solicited)
        print(f"mean solicited snapshot payload: {sol_bytes:,.0f} bytes")

    print("\n--- verdict ---")
    if not solicited:
        print("NO SOLICITED SNAPSHOTS FOUND.")
        print("The poller either never ran or never fired. Check that")
        print("KALSHI_SNAPSHOT_MAX_AGE_SECONDS is non-zero in the running container.")
        return 2
    if solicited_broken:
        print("seq is NOT continuous across solicited snapshots.")
        print("The trust layer cannot read a solicited snapshot as a handshake one:")
        print("it needs a `kalshi_seq_reset_at_snapshot` reason class to tell this")
        print("apart from a real drop.")
        return 1
    print(f"seq is continuous across all {solicited_checked} checkable solicited snapshots.")
    if natural_broken:
        print(f"NOTE: {natural_broken} natural snapshot(s) broke continuity -- investigate,")
        print("this is unrelated to the poller.")
    print("A solicited snapshot reads exactly like a handshake one.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("spool_root", type=Path)
    parser.add_argument("--date", help="UTC date partition, e.g. 2026-08-12. Default: every date present.")
    arguments = parser.parse_args()
    snapshots, requests = analyse(iter_records(arguments.spool_root, arguments.date))
    return report(snapshots, requests)


if __name__ == "__main__":
    raise SystemExit(main())
