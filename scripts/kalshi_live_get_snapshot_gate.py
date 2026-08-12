#!/usr/bin/env python3
"""The live exit gate for the Kalshi snapshot poller. **It talks to the real venue.**

Run by a human, deliberately, with real credentials — never from a test, never
from CI, and never against the production capture process.

**This has since been answered a stronger way, and the script kept.** The design
rested on an AsyncAPI document and on tape containing no solicited snapshot at
all, because the command had never been sent from this codebase. It has now been
sent, from a shadow deployment against the live venue: every solicited snapshot
carried a continuous `seq`, no subscription was displaced, and the request-to-
snapshot latency sat around 80 ms. That is a broader sample than this script
takes and it was taken on the real command path, so the question this was written
to settle is settled.

What it remains good for is a *changed* command shape — a venue schema revision,
a different `action`, an added parameter — where the same six assertions want
checking on a connection whose loss costs no tape. It is otherwise dead weight.

It opens **its own connection**. The production splice must not be the place a
new command shape is tried for the first time: a rejected command that drops that
connection costs tape, and tape is the one thing here that cannot be recovered.

```sh
python scripts/kalshi_live_get_snapshot_gate.py \
    --ticker KXBTCD-26JUL2904-T72299.99 \
    --ticker KXBTCD-26JUL2904-T71299.99
```

Procedure, from §5: subscribe to one to three markets with `send_initial_snapshot`
using the production command shape, let deltas accumulate a run of `seq`, send one
`update_subscription` with `action: get_snapshot`, keep recording, and write every
frame verbatim with the cursor the splice would have given it.

Six assertions, all required to pass:

```text
1  seq is continuous across the solicited snapshot
2  no new sid appears, and the existing sid keeps its sequence
3  a snapshot arrives for every requested ticker, and for no others
4  the connection survives and keeps delivering deltas afterwards
5  no error reply, and specifically no code 27
6  the solicited snapshot is the same shape as a handshake one
```

Three measurements are taken alongside them, because an authenticated connection
is the cheapest place to get them: command-to-snapshot latency, snapshot bytes
against delta bytes, and what `get_snapshot` does for a market that has had no
deltas since its last snapshot.

**Keep the recorded frames.** They are the fixture the trust-side unit tests want
when the reader learns to read a solicited snapshot, which otherwise have to
invent one. The output file holds venue payloads and no credential material.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from splices.common.spool import Spool
from splices.kalshi.splice import (
    BOOK_CHANNEL,
    DEMO_WEBSOCKET_URL,
    RATE_LIMIT_ERROR_CODE,
    WEBSOCKET_URL,
    KalshiSplice,
    get_snapshot_command,
)
from targeter.targets import Target, TargetSet, target_digest, target_metadata_digest

#: §5 says one to three markets. The gate's subject is the venue's behaviour, and
#: a wide subscription buys nothing while making a rejected command more expensive
#: and the recorded evidence harder to read.
MAX_TICKERS = 3

#: Matches the capture loop's own read timeout, so this drains the socket the way
#: production does rather than in one large blocking read.
READ_TIMEOUT_SECONDS = 1.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ticker", action="append", required=True, metavar="MARKET_TICKER",
                        help=f"A real Kalshi market ticker. Repeat up to {MAX_TICKERS} times.")
    parser.add_argument("--pre-seconds", type=float, default=60.0,
                        help="How long to let deltas flow before requesting snapshots. "
                             "A minute is plenty to accumulate a run of seq.")
    parser.add_argument("--post-seconds", type=float, default=60.0,
                        help="How long to keep recording after the request, which is what "
                             "assertion 4 reads.")
    parser.add_argument("--output-dir", type=Path,
                        default=PROJECT_ROOT / "data" / "live_gate" / "kalshi_get_snapshot",
                        help="Where the recorded frames and the verdict are written.")
    parser.add_argument("--demo", action="store_true",
                        help="Run against Kalshi's demo socket instead of production. Still a "
                             "real venue and still a real credential.")
    parser.add_argument("--url", default=None,
                        help="Override the WebSocket URL entirely.")
    return parser.parse_args()


def _target_set(tickers: list[str]) -> TargetSet:
    """The subscription the production `send_subscription` will be handed.

    Built here rather than read from `data/live/targets_kalshi.json` so the gate
    subscribes to exactly the markets a human named, and cannot accidentally open
    a second full-ladder subscription beside the capture process.
    """
    targets = tuple(Target(ticker) for ticker in tickers)
    return TargetSet(
        venue="kalshi",
        targets=targets,
        digest=target_digest("kalshi", targets),
        source_path="<kalshi_live_get_snapshot_gate>",
        metadata_digest=target_metadata_digest("kalshi", targets),
        metadata_path=None,
    )


def _payload(frame: dict[str, Any]) -> dict[str, Any]:
    try:
        parsed = json.loads(frame["raw_payload"])
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _ticker_of(payload: dict[str, Any]) -> str | None:
    message = payload.get("msg")
    ticker = message.get("market_ticker") if isinstance(message, dict) else None
    if not isinstance(ticker, str) or not ticker:
        ticker = payload.get("market_ticker")
    return ticker if isinstance(ticker, str) and ticker else None


def _error_code(payload: dict[str, Any]) -> Any:
    nested = payload.get("msg")
    if isinstance(nested, dict):
        return nested.get("code", payload.get("code"))
    return payload.get("code")


async def record(arguments: argparse.Namespace, splice: KalshiSplice,
                 targets: TargetSet) -> dict[str, Any]:
    """Connects, subscribes, requests, and returns everything that arrived.

    Connection and subscription both go through the production splice object.
    A gate that reimplemented the signed handshake or the subscribe command would
    prove that its own copy works, which is not the question.
    """
    frames: list[dict[str, Any]] = []
    subscriptions: dict[str, int] = {}
    connection_error: str | None = None
    command_id: int | None = None
    command_sent_ns: int | None = None
    command: str | None = None

    async def drain(until: float, phase: str) -> None:
        nonlocal connection_error
        while time.monotonic() < until:
            try:
                message = await asyncio.wait_for(transport.recv(), timeout=READ_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                continue
            except Exception as error:  # noqa: BLE001 - assertion 4 is exactly this
                connection_error = f"{type(error).__name__}: {error}"
                return
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            index = len(frames) + 1
            frames.append(
                {
                    "index": index,
                    "phase": phase,
                    "received_ns": time.time_ns(),
                    "monotonic_ns": time.monotonic_ns(),
                    "stream": splice.stream_for(message),
                    "source_cursor": splice.frame_cursor(index, message),
                    "byte_length": len(message.encode("utf-8")),
                    "raw_payload": message,
                }
            )
            payload = _payload(frames[-1])
            if payload.get("type") == "subscribed":
                nested = payload.get("msg")
                if isinstance(nested, dict) and isinstance(nested.get("sid"), int):
                    subscriptions[str(nested.get("channel"))] = int(nested["sid"])

    async with splice.open_connection() as transport:
        await splice.send_subscription(transport, targets)
        print(f"subscribed to {len(targets)} market(s); recording {arguments.pre_seconds:g}s")
        await drain(time.monotonic() + arguments.pre_seconds, "pre")

        book_sid = subscriptions.get(BOOK_CHANNEL)
        if connection_error is None and book_sid is not None:
            # The same bytes capture will send, from the same function, aimed at
            # the sid the venue itself named. A gate willing to guess a sid would
            # be testing a command production is forbidden to send.
            command_id = splice.next_command_id()
            command = get_snapshot_command(command_id, book_sid, list(targets.asset_ids()))
            await transport.send(command)
            command_sent_ns = time.time_ns()
            print(f"sent get_snapshot id={command_id} sid={book_sid}; "
                  f"recording {arguments.post_seconds:g}s")
            await drain(time.monotonic() + arguments.post_seconds, "post")
        elif connection_error is None:
            print(f"no `subscribed` acknowledgement for {BOOK_CHANNEL}; sending nothing",
                  file=sys.stderr)

    return {
        "frames": frames,
        "subscriptions": subscriptions,
        "book_sid": subscriptions.get(BOOK_CHANNEL),
        "connection_error": connection_error,
        "command": command,
        "command_id": command_id,
        "command_sent_ns": command_sent_ns,
    }


def evaluate(session: dict[str, Any], requested: list[str]) -> dict[str, Any]:
    """The six assertions of §5, each answered from the recorded frames alone."""
    frames: list[dict[str, Any]] = session["frames"]
    book_sid = session["book_sid"]
    assertions: list[dict[str, Any]] = []

    def assert_that(number: int, name: str, passed: bool, detail: Any) -> None:
        assertions.append({"n": number, "assertion": name, "passed": bool(passed),
                           "detail": detail})

    solicited = [
        frame for frame in frames
        if frame["phase"] == "post" and _payload(frame).get("type") == "orderbook_snapshot"
    ]
    handshake = [
        frame for frame in frames
        if frame["phase"] == "pre" and _payload(frame).get("type") == "orderbook_snapshot"
    ]

    # One walk over the book subscription's sequence, in arrival order. Every
    # frame's cursor already carries the venue's own claim about its predecessor
    # (`previous_last = seq - 1`), so continuity is that claim against what
    # actually arrived — which is precisely the check replay will run.
    solicited_indices = {frame["index"] for frame in solicited}
    breaks: list[dict[str, Any]] = []
    solicited_continuous: list[dict[str, Any]] = []
    last_seen: int | None = None
    for frame in frames:
        payload = _payload(frame)
        if payload.get("sid") != book_sid:
            continue
        cursor = frame["source_cursor"] or {}
        if cursor.get("type") != "update_range":
            continue
        previous_last = cursor["previous_last"]
        continuous = last_seen is None or previous_last == last_seen
        if not continuous:
            breaks.append({"index": frame["index"], "type": payload.get("type"),
                           "previous_last": previous_last, "last_seen": last_seen})
        if frame["index"] in solicited_indices:
            solicited_continuous.append(
                {"index": frame["index"], "ticker": _ticker_of(payload),
                 "seq": cursor["last"], "previous_last": previous_last,
                 "last_seen": last_seen, "continuous": continuous}
            )
        last_seen = cursor["last"]

    assert_that(
        1, "seq is continuous across the solicited snapshot",
        bool(solicited_continuous) and all(row["continuous"] for row in solicited_continuous),
        solicited_continuous or "no solicited snapshot arrived",
    )

    sids_before = {_payload(f).get("sid") for f in frames if f["phase"] == "pre"}
    sids_after = {_payload(f).get("sid") for f in frames if f["phase"] == "post"}
    new_sids = sorted(s for s in sids_after - sids_before if s is not None)
    assert_that(
        2, "no new sid appears, and the existing sid keeps its sequence",
        not new_sids and not breaks and book_sid in sids_after,
        {"book_sid": book_sid, "new_sids": new_sids,
         "sids_before": sorted(s for s in sids_before if s is not None),
         "sids_after": sorted(s for s in sids_after if s is not None),
         "continuity_breaks": breaks},
    )

    solicited_tickers = sorted({t for t in (_ticker_of(_payload(f)) for f in solicited) if t})
    assert_that(
        3, "a snapshot arrives for every requested ticker and for no others",
        solicited_tickers == sorted(requested),
        {"requested": sorted(requested), "received": solicited_tickers},
    )

    last_solicited = max((frame["index"] for frame in solicited), default=0)
    deltas_after = [
        frame for frame in frames
        if frame["index"] > last_solicited and _payload(frame).get("type") == "orderbook_delta"
    ]
    assert_that(
        4, "the connection survives and keeps delivering deltas afterwards",
        session["connection_error"] is None and bool(deltas_after),
        {"connection_error": session["connection_error"],
         "deltas_after_last_solicited_snapshot": len(deltas_after),
         "note": "requires at least one of the requested markets to be trading"},
    )

    errors = [_payload(frame) for frame in frames if _payload(frame).get("type") == "error"]
    rate_limited = [
        error for error in errors if str(_error_code(error)) == str(RATE_LIMIT_ERROR_CODE)
    ]
    assert_that(
        5, "no error reply, and specifically no code 27",
        not errors,
        {"errors": errors, "rate_limit_replies": len(rate_limited)},
    )

    shapes: list[dict[str, Any]] = []
    for ticker in sorted(requested):
        before = next((f for f in handshake if _ticker_of(_payload(f)) == ticker), None)
        after = next((f for f in solicited if _ticker_of(_payload(f)) == ticker), None)
        if before is None or after is None:
            continue
        first, second = _payload(before), _payload(after)
        shapes.append({
            "ticker": ticker,
            "same_top_level_keys": sorted(first) == sorted(second),
            "same_msg_keys": sorted(first.get("msg") or {}) == sorted(second.get("msg") or {}),
            "handshake_keys": sorted(first),
            "solicited_keys": sorted(second),
        })
    assert_that(
        6, "the solicited snapshot is the same shape as a handshake one",
        bool(shapes) and all(row["same_top_level_keys"] and row["same_msg_keys"]
                             for row in shapes),
        shapes or "no ticker produced both a handshake and a solicited snapshot",
    )

    return {"assertions": assertions,
            "passed": all(row["passed"] for row in assertions),
            "measurements": measure(session, requested, solicited, handshake)}


def measure(session: dict[str, Any], requested: list[str], solicited: list[dict[str, Any]],
            handshake: list[dict[str, Any]]) -> dict[str, Any]:
    """The numbers §5 asks for while the connection is open and paid for."""
    frames: list[dict[str, Any]] = session["frames"]
    command_sent_ns = session["command_sent_ns"]

    latencies = [
        round((frame["received_ns"] - command_sent_ns) / 1e6, 1)
        for frame in solicited
        if command_sent_ns is not None
    ]
    snapshot_bytes = [f["byte_length"] for f in frames
                      if _payload(f).get("type") == "orderbook_snapshot"]
    delta_bytes = [f["byte_length"] for f in frames
                   if _payload(f).get("type") == "orderbook_delta"]

    quiet: list[dict[str, Any]] = []
    for ticker in sorted(requested):
        first_snapshot = next(
            (f["index"] for f in handshake if _ticker_of(_payload(f)) == ticker), None
        )
        deltas_before_command = [
            f for f in frames
            if f["phase"] == "pre"
            and _payload(f).get("type") == "orderbook_delta"
            and _ticker_of(_payload(f)) == ticker
            and (first_snapshot is None or f["index"] > first_snapshot)
        ]
        answered = next((f for f in solicited if _ticker_of(_payload(f)) == ticker), None)
        quiet.append({
            "ticker": ticker,
            "deltas_since_its_last_snapshot": len(deltas_before_command),
            "was_answered": answered is not None,
            "solicited_snapshot_bytes": answered["byte_length"] if answered else None,
        })

    def summary(values: list[float]) -> dict[str, Any]:
        if not values:
            return {"count": 0}
        return {"count": len(values), "min": min(values),
                "median": round(statistics.median(values), 1), "max": max(values)}

    return {
        "command_to_snapshot_ms": summary(latencies),
        "snapshot_bytes": summary(snapshot_bytes),
        "delta_bytes": summary(delta_bytes),
        # Open question 4: quiet markets dominate the ladder and would be polled
        # forever at the staleness threshold while producing identical books.
        "quiet_market_behaviour": quiet,
        "frames_recorded": len(frames),
    }


def write_outputs(directory: Path, session: dict[str, Any],
                  verdict: dict[str, Any]) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    frames_path = directory / f"{stamp}-frames.ndjson"
    verdict_path = directory / f"{stamp}-verdict.json"
    with frames_path.open("w", encoding="utf-8") as handle:
        for frame in session["frames"]:
            handle.write(json.dumps(frame, ensure_ascii=False, separators=(",", ":")) + "\n")
    verdict_path.write_text(json.dumps(verdict, indent=2, ensure_ascii=False) + "\n",
                            encoding="utf-8")
    return frames_path, verdict_path


def main() -> int:
    arguments = parse_args()
    tickers = list(dict.fromkeys(arguments.ticker))
    if not 1 <= len(tickers) <= MAX_TICKERS:
        print(f"name between 1 and {MAX_TICKERS} distinct tickers", file=sys.stderr)
        return 2

    url = arguments.url or (DEMO_WEBSOCKET_URL if arguments.demo else WEBSOCKET_URL)
    targets = _target_set(tickers)
    # The production splice, holding a spool that is never started: nothing here
    # writes through the capture tape, and this process must not own a lane the
    # capture process may also be writing. The object exists for its connection,
    # its subscribe command, and its cursor.
    splice = KalshiSplice(
        Spool(arguments.output_dir / "unused-spool", "kalshi_live_gate"),
        None,
        url=url,
        dotenv_path=PROJECT_ROOT / ".env",
        snapshot_max_age_seconds=0,
    )

    print(f"connecting to {url} for {', '.join(tickers)}")
    session = asyncio.run(record(arguments, splice, targets))
    verdict = evaluate(session, tickers)
    verdict["url"] = url
    verdict["market_tickers"] = tickers
    verdict["subscriptions"] = session["subscriptions"]
    verdict["command"] = session["command"]

    frames_path, verdict_path = write_outputs(arguments.output_dir, session, verdict)
    for row in verdict["assertions"]:
        print(f"{'PASS' if row['passed'] else 'FAIL'}  {row['n']}. {row['assertion']}")
    print(json.dumps(verdict["measurements"], indent=2))
    print(f"frames:  {frames_path}")
    print(f"verdict: {verdict_path}")
    if session["command"] is None:
        print("no command was sent, so the gate is unanswered rather than failed",
              file=sys.stderr)
        return 2
    return 0 if verdict["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
