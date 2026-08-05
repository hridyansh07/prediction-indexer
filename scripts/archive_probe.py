#!/usr/bin/env python3
"""§9.5's real-data gate: archive a real captured segment and measure it.

```sh
python scripts/archive_probe.py                      # the committed capture fixture
python scripts/archive_probe.py --source <file.ndjson>
```

Five things, against real bytes rather than generated ones:

```text
1  archive through the local conformance adapter
2  decode the stored frame and compare exact bytes, length, LF count and digest
3  report compressed/uncompressed lengths and the compression ratio
4  demonstrate that decode cannot exceed the seal's declared length
5  record peak RSS while processing a segment far larger than the codec buffer
```

**The source is never modified.** A sealed segment is built in a temporary spool
by feeding the fixture's own lines through the real capture writer, so the seal
under test is one `splices/common/segment.py` produced rather than one this
script invented.

No credentials and no venue payloads are committed by this script. It names the
fixture path and the identities it measured, and writes nothing outside its
temporary directory.
"""

from __future__ import annotations

import argparse
import json
import resource
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from archive.archiver import ARCHIVED, Archiver  # noqa: E402
from archive.storage.local import LocalObjectStore  # noqa: E402
from archive.common.receipts import read_archive_receipt  # noqa: E402
from archive.common.verify import decode_archived_segment  # noqa: E402
from encoder import DecodeLimitExceeded, logical_identity_of  # noqa: E402
from splices.common.segment import Record, SegmentWriter  # noqa: E402

#: A real capture, committed for replay's tests. Sports reference data rather
#: than a private venue book, which is why it can live in the repository.
DEFAULT_SOURCE = PROJECT_ROOT / "replay" / "tests" / "fixtures" / "polymarket_sports_20260730.ndjson"

WINDOW_SECONDS = 1800
NANOSECONDS = 1_000_000_000
BASE_NS = 1_785_369_600_000_000_000


def peak_rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports kilobytes, macOS bytes.
    return usage if sys.platform == "darwin" else usage * 1024


def build_segment(spool: Path, lines: list[bytes], lane: str, segment_id: str) -> Path:
    """Feeds real captured lines through the real segment writer."""
    writer = SegmentWriter(
        spool, lane, BASE_NS, segment_seconds=WINDOW_SECONDS, segment_id=segment_id
    )
    batch = [
        Record(
            line=line,
            visible_ns=BASE_NS + index + 1,
            delivery_index=index + 1,
            epoch="probe0001",
        )
        for index, line in enumerate(lines)
    ]
    # In batches, so the writer's own accumulator path is what produces the
    # seal rather than one enormous single write.
    for start in range(0, len(batch), 1000):
        writer.write_batch(batch[start : start + 1000])
    writer.seal("probe")
    return writer.data_path


def read_lines(path: Path) -> list[bytes]:
    with path.open("rb") as handle:
        return [line for line in handle if line.endswith(b"\n")]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument(
        "--large-multiple",
        type=int,
        default=256,
        help="how many times to repeat the source for the bounded-memory probe. The "
        "default puts the segment two orders of magnitude above the 1 MiB codec buffer, "
        "which is the comparison that matters — peak RSS should track the buffer.",
    )
    arguments = parser.parse_args(argv)

    source = arguments.source.resolve()
    if not source.is_file():
        raise SystemExit(f"no such capture: {source}")
    lines = read_lines(source)
    if not lines:
        raise SystemExit(f"{source} holds no complete NDJSON records")

    report: dict[str, object] = {
        "source": str(source.relative_to(PROJECT_ROOT) if source.is_relative_to(PROJECT_ROOT) else source),
        "source_records": len(lines),
    }

    with tempfile.TemporaryDirectory(prefix="archive-probe-") as directory:
        root = Path(directory)
        spool = root / "spool"
        store = LocalObjectStore(root / "archive")
        archiver = Archiver(spool, store)

        # -- 1. archive the real segment ---------------------------------
        segment = build_segment(spool, lines, "polymarket_sports", "probe001")
        started = time.monotonic()
        outcome = archiver.archive_segment("polymarket_sports", segment)
        if outcome.status != ARCHIVED:
            raise SystemExit(f"archiving failed: {outcome.detail}")
        elapsed = time.monotonic() - started

        receipt = read_archive_receipt(outcome.receipt_path)
        report["segment"] = segment.name
        report["logical"] = receipt.source.as_record()
        report["stored"] = receipt.data_stored.as_record()
        report["data_key"] = receipt.data_key
        report["seal_key"] = receipt.seal_key
        report["archive_seconds"] = round(elapsed, 3)

        # -- 2. decode and compare against the source, byte for byte ------
        restored = root / "restored.ndjson"
        decoded = decode_archived_segment(store, receipt, restored)
        with segment.open("rb") as handle:
            original = logical_identity_of(handle)
        report["decoded_matches_source"] = (
            restored.read_bytes() == segment.read_bytes()
            and decoded == original == receipt.source
        )

        # -- 3. compression ratio ----------------------------------------
        ratio = receipt.source.byte_length / receipt.data_stored.byte_length
        report["compression"] = {
            "uncompressed_bytes": receipt.source.byte_length,
            "compressed_bytes": receipt.data_stored.byte_length,
            "ratio": round(ratio, 3),
            "percent_of_original": round(100 / ratio, 2),
        }

        # -- 4. the decode ceiling ---------------------------------------
        #
        # Two things are checked: the abort happened, and the destination name
        # never appeared. A staged decode is removed on failure, so "the file is
        # at most `limit` bytes" would now be checking a file that must not
        # exist at all.
        clipped = root / "clipped.ndjson"
        limit = max(1, receipt.source.byte_length // 2)
        try:
            decode_archived_segment(store, receipt, clipped, max_decoded_bytes=limit)
            report["decode_limit_enforced"] = False
        except DecodeLimitExceeded:
            report["decode_limit_enforced"] = not clipped.exists()
            report["decode_limit_bytes"] = limit
            report["decode_limit_left_no_output"] = list(root.glob("*clipped*")) == []

        # -- 5. peak RSS over a much larger segment -----------------------
        large_spool = root / "large-spool"
        repeated = lines * max(1, arguments.large_multiple)
        large = build_segment(large_spool, repeated, "polymarket_sports", "probe002")
        large_store = LocalObjectStore(root / "large-archive")
        before_rss = peak_rss_bytes()
        started = time.monotonic()
        large_outcome = Archiver(large_spool, large_store).archive_segment(
            "polymarket_sports", large
        )
        if large_outcome.status != ARCHIVED:
            raise SystemExit(f"large archiving failed: {large_outcome.detail}")
        large_receipt = read_archive_receipt(large_outcome.receipt_path)
        decode_archived_segment(large_store, large_receipt, root / "large-restored.ndjson")
        report["large_segment"] = {
            "uncompressed_bytes": large_receipt.source.byte_length,
            "compressed_bytes": large_receipt.data_stored.byte_length,
            "ratio": round(
                large_receipt.source.byte_length / large_receipt.data_stored.byte_length, 3
            ),
            "seconds": round(time.monotonic() - started, 3),
            "peak_rss_bytes_before": before_rss,
            "peak_rss_bytes_after": peak_rss_bytes(),
            "peak_rss_growth_bytes": peak_rss_bytes() - before_rss,
        }

    print(json.dumps(report, indent=2, sort_keys=True))
    failures = [
        key
        for key in ("decoded_matches_source", "decode_limit_enforced")
        if not report.get(key)
    ]
    if failures:
        print(f"FAILED: {', '.join(failures)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
