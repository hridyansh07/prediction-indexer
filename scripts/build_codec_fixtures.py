#!/usr/bin/env python3
"""Regenerates the cross-language codec fixtures in `encoder/fixtures/`.

Three artifacts prove that the Python and Rust codecs are one format rather than
two implementations that happen to agree today:

```text
roundtrip_v1.ndjson             the logical payload, committed verbatim
roundtrip_v1.python.ndjson.zst  the frame python-zstandard produces
roundtrip_v1.rust.ndjson.zst    the frame zstd-rs produces
roundtrip_v1.json               identities and the encoder that made each frame
```

Each language decodes the *other* language's frame and compares the result with
`roundtrip_v1.ndjson` byte for byte. Neither test shells out to the other
toolchain, so a broken interop story fails in CI rather than on a machine that
happens to have both installed.

Regeneration is deliberately a two-step manual act, because a fixture that any
test can rewrite proves nothing:

```sh
python scripts/build_codec_fixtures.py            # payload + Python frame
cargo test --manifest-path encoder/rust/Cargo.toml -- --ignored regenerate
python scripts/build_codec_fixtures.py            # records the Rust identity
```

The payload is generated rather than captured: a fixture committed to a public
repository must not carry venue payloads, and PHASE_4_RAW_ARCHIVE_REAPER_V1 §9.5
keeps real segments out of the tree. Its *shape* is an envelope so the
compression ratio is representative.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from encoder import (  # noqa: E402
    DEFAULT_ZSTD_LEVEL,
    LogicalIdentity,
    encode_stream,
    encoder_version,
    logical_identity_of,
    stored_identity_of,
)

FIXTURES = PROJECT_ROOT / "encoder" / "fixtures"
PAYLOAD = FIXTURES / "roundtrip_v1.ndjson"
PYTHON_FRAME = FIXTURES / "roundtrip_v1.python.ndjson.zst"
RUST_FRAME = FIXTURES / "roundtrip_v1.rust.ndjson.zst"
METADATA = FIXTURES / "roundtrip_v1.json"

LANES = ("polymarket", "kalshi", "limitless")
#: Non-ASCII on purpose. The codec moves bytes, and a fixture that is entirely
#: ASCII cannot catch an implementation that quietly re-encodes text.
TITLES = ("Će rain?", "Will BTC ≥ $100k?", "Champion — decided?")


def payload_bytes() -> bytes:
    """256 envelope-shaped records, deterministic to the byte."""
    lines = []
    for index in range(256):
        record = {
            "envelope_version": 2,
            "lane_id": LANES[index % len(LANES)],
            "venue": LANES[index % len(LANES)].split("_")[0],
            "delivery_index": index + 1,
            "visible_ns": 1_769_000_000_000_000_000 + index * 1_250_000,
            "monotonic_ns": 900_000_000_000 + index * 1_250_000,
            "connection_epoch": f"c8f2a1{index // 64:02d}",
            "record_id": f"{index:064x}",
            "title": TITLES[index % len(TITLES)],
            "payload": {
                "asset_id": f"0x{index:040x}",
                "bids": [[0.01 * level, 25 * (level + 1)] for level in range(1, 6)],
                "asks": [[0.99 - 0.01 * level, 25 * (level + 1)] for level in range(1, 6)],
            },
        }
        lines.append(json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return ("\n".join(lines) + "\n").encode("utf-8")


def main() -> int:
    FIXTURES.mkdir(parents=True, exist_ok=True)
    PAYLOAD.write_bytes(payload_bytes())

    with PAYLOAD.open("rb") as source, PYTHON_FRAME.open("wb") as sink:
        result = encode_stream(source, sink, level=DEFAULT_ZSTD_LEVEL)

    with PAYLOAD.open("rb") as source:
        logical: LogicalIdentity = logical_identity_of(source)
    if logical != result.logical:  # pragma: no cover - would mean the codec disagrees with itself
        raise SystemExit("logical identity differs between encode and a plain hash")

    frames = {
        "python": {
            "file": PYTHON_FRAME.name,
            "encoder": encoder_version(),
            "stored": result.stored.as_record(),
        }
    }
    if RUST_FRAME.exists():
        with RUST_FRAME.open("rb") as source:
            frames["rust"] = {
                "file": RUST_FRAME.name,
                # Recorded by the Rust regeneration test; preserved across a
                # Python-only run so the two halves do not overwrite each other.
                "encoder": _previous_rust_encoder(),
                "stored": stored_identity_of(source).as_record(),
            }

    METADATA.write_text(
        json.dumps(
            {
                "fixture_version": 1,
                "payload": {"file": PAYLOAD.name, "logical": logical.as_record()},
                "compression": {
                    "algorithm": "zstd",
                    "level": DEFAULT_ZSTD_LEVEL,
                    "frame_checksum": True,
                    "dictionary": None,
                    "frame_count": 1,
                },
                "frames": frames,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"payload  {logical.byte_length} bytes, {logical.line_count} lines")
    for language, frame in sorted(frames.items()):
        print(f"{language:8} {frame['stored']['byte_length']} bytes  {frame['encoder']}")
    if "rust" not in frames:
        print("rust frame absent: run the ignored `regenerate` test, then this script again")
    return 0


def _previous_rust_encoder() -> str:
    if not METADATA.exists():
        return "unknown"
    document = json.loads(METADATA.read_text(encoding="utf-8"))
    frame = document.get("frames", {}).get("rust", {})
    return str(frame.get("encoder", "unknown"))


if __name__ == "__main__":
    raise SystemExit(main())
