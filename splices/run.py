#!/usr/bin/env python3
"""Runs one venue splice.

One process per venue, deliberately. A single process multiplexing every venue
would make one venue's outage everyone's outage, and the network-facing part is
exactly where outages happen.

Long-lived by default. `--stop-after-seconds` exists for probes and smoke runs and
takes the identical code path, so a short run proves something about the long one.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from splices.common.base import BackoffPolicy
from splices.common.segment import DEFAULT_SEGMENT_SECONDS
from splices.common.spool import DEFAULT_FSYNC_INTERVAL_SECONDS, Spool
from splices.common.writer import DEFAULT_QUEUE_CAPACITY

#: What can be run, and what each one needs.
#:
#: Keyed by *feed* rather than venue because Polymarket now offers three: the
#: market channel, the sports reference channel, and the RTDS price channel. Each
#: gets its own process and its own spool lane, since `delivery_index` is dense
#: across a single splice's lifetime and two processes sharing a lane would
#: interleave two counters into one set of files.
#: `targets_venue` is the venue whose targets file the feed reads, which is not
#: the feed name once a venue has more than one lane. The snapshot poller must
#: poll exactly what the market splice subscribes to — a recovery point for an
#: asset nobody is streaming anchors nothing — so both read `targets_polymarket`.
FEEDS = {
    "polymarket": {"lane": "polymarket", "targets": "polymarket"},
    "limitless": {"lane": "limitless", "targets": "limitless"},
    "kalshi": {"lane": "kalshi", "targets": "kalshi"},
    "polymarket-sports": {"lane": "polymarket_sports", "targets": None},
    "polymarket-rtds": {"lane": "polymarket_rtds", "targets": None},
    "polymarket-snapshots": {"lane": "polymarket_snapshots", "targets": "polymarket"},
}

VENUES = tuple(FEEDS)


def build_splice(feed: str, spool: Spool, targets: Path | None, *,
                 poll_seconds: float = 60.0, **kwargs):
    """Imported lazily so a venue whose optional dependency is missing does not
    stop the others from running."""
    if feed == "polymarket":
        from splices.polymarket.splice import PolymarketSplice

        return PolymarketSplice(spool, targets, **kwargs)
    if feed == "limitless":
        from splices.limitless.splice import LimitlessSplice

        return LimitlessSplice(spool, targets, **kwargs)
    if feed == "kalshi":
        from splices.kalshi.splice import KalshiSplice

        # Credentials are read lazily at connect time, so an unconfigured Kalshi
        # never blocks constructing a splice for a venue that is configured.
        return KalshiSplice(spool, targets, dotenv_path=PROJECT_ROOT / ".env", **kwargs)
    if feed == "polymarket-sports":
        from splices.polymarket.sports import PolymarketSportsSplice

        return PolymarketSportsSplice(spool, targets, **kwargs)
    if feed == "polymarket-rtds":
        from splices.polymarket.rtds import PolymarketRtdsSplice

        return PolymarketRtdsSplice(spool, targets, **kwargs)
    if feed == "polymarket-snapshots":
        from splices.polymarket.snapshots import PolymarketSnapshotSplice

        return PolymarketSnapshotSplice(spool, targets, poll_seconds=poll_seconds, **kwargs)
    raise SystemExit(f"unknown feed: {feed}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("feed", choices=VENUES, metavar="feed",
                        help=f"one of: {', '.join(VENUES)}")
    parser.add_argument("--targets", type=Path, default=None,
                        help="Defaults to data/live/targets_<venue>.json. "
                             "Reference feeds broadcast everything and take none.")
    parser.add_argument("--spool-root", type=Path, default=PROJECT_ROOT / "data" / "spool")
    parser.add_argument("--stop-after-seconds", type=float, default=None)
    parser.add_argument("--max-connections", type=int, default=None)
    parser.add_argument("--target-poll-seconds", type=float, default=30.0)
    parser.add_argument("--fsync-interval-seconds", type=float,
                        default=DEFAULT_FSYNC_INTERVAL_SECONDS)
    parser.add_argument("--backoff-max-seconds", type=float, default=60.0)
    parser.add_argument("--segment-seconds", type=int, default=DEFAULT_SEGMENT_SECONDS,
                        help="Segment window length. Must divide 86400 evenly so windows "
                             "tile the UTC day. Short values are how rotation is tested.")
    parser.add_argument("--writer-queue-capacity", type=int, default=DEFAULT_QUEUE_CAPACITY,
                        help="Records buffered between the socket and the writer thread. "
                             "A full queue applies backpressure; it never drops and never "
                             "reconnects.")
    parser.add_argument("--poll-seconds", type=float, default=60.0,
                        help="Snapshot poller only: how often a full book cycle runs. "
                             "This is the worst-case unverified window it can bound.")
    return parser.parse_args()


async def main_async(arguments: argparse.Namespace) -> int:
    spool = Spool(arguments.spool_root, FEEDS[arguments.feed]["lane"],
                  fsync_interval_seconds=arguments.fsync_interval_seconds,
                  segment_seconds=arguments.segment_seconds,
                  queue_capacity=arguments.writer_queue_capacity)
    splice = build_splice(
        arguments.feed, spool, arguments.targets,
        poll_seconds=arguments.poll_seconds,
        backoff=BackoffPolicy(maximum_seconds=arguments.backoff_max_seconds),
        target_poll_seconds=arguments.target_poll_seconds,
    )

    task = asyncio.ensure_future(
        splice.run(stop_after_seconds=arguments.stop_after_seconds,
                   max_connections=arguments.max_connections)
    )

    loop = asyncio.get_running_loop()
    for received in (signal.SIGINT, signal.SIGTERM):
        # A cancelled task still runs its `finally`, which writes the closing
        # record and fsyncs — so an operator stopping a splice leaves a tape that
        # ends with a stated reason rather than simply stopping mid-stream.
        loop.add_signal_handler(received, task.cancel)

    try:
        summary = await task
    except asyncio.CancelledError:
        summary = splice.summary()
        summary["stopped_by"] = "signal"
    finally:
        # Idempotent: `run()` already seals in its own `finally`. This covers the
        # case where constructing or starting the splice failed before that.
        spool.close()

    print(json.dumps(summary, indent=2))
    return 0


def main() -> int:
    arguments = parse_args()
    targets_venue = FEEDS[arguments.feed]["targets"]
    if targets_venue:
        if arguments.targets is None:
            arguments.targets = PROJECT_ROOT / "data" / "live" / f"targets_{targets_venue}.json"
        if not arguments.targets.exists():
            print(f"targets file not found: {arguments.targets}", file=sys.stderr)
            print("run: python3 targeter/run.py --once "
                  f"--venue {targets_venue}", file=sys.stderr)
            return 2
    elif arguments.targets is not None:
        # Refused rather than ignored. A reference feed broadcasts everything, so
        # a targets file passed here would be read by nobody while looking, to the
        # operator who passed it, exactly like a subscription that took effect.
        print(f"{arguments.feed} broadcasts every event and takes no targets file",
              file=sys.stderr)
        return 2

    # Checked before connecting so a missing key produces setup instructions
    # rather than a 401 buried in a reconnect loop.
    if arguments.feed == "kalshi":
        from splices.kalshi.auth import KalshiCredentialsError, load_credentials

        try:
            load_credentials(dotenv_path=PROJECT_ROOT / ".env")
        except KalshiCredentialsError as error:
            print(str(error), file=sys.stderr)
            return 2

    return asyncio.run(main_async(arguments))


if __name__ == "__main__":
    raise SystemExit(main())
