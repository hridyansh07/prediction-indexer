from __future__ import annotations

import unittest

from replay.stream import (
    CompositeByteStreamer,
    MemoryByteStreamer,
    StreamError,
    TruncatedObject,
    build_input_manifest,
    iter_ndjson_lines,
)


class ByteStreamerTests(unittest.TestCase):
    def test_chunk_boundaries_do_not_change_lines_or_dataset_identity(self) -> None:
        objects = {
            "spool/venue=x/date=2026-01-01/a.ndjson": b'{"a":1}\n{"b":2}\n',
            "live/coverage.json": b'{"sightings":[]}\n',
        }
        tiny = MemoryByteStreamer(objects, chunk_size=1)
        wide = MemoryByteStreamer(objects, chunk_size=10_000)
        self.assertEqual(
            [line.data for line in iter_ndjson_lines(tiny)],
            [b'{"a":1}', b'{"b":2}'],
        )
        self.assertEqual(
            build_input_manifest(tiny).dataset_sha256,
            build_input_manifest(wide).dataset_sha256,
        )

    def test_torn_ndjson_is_rejected_instead_of_partially_replayed(self) -> None:
        streamer = MemoryByteStreamer({"spool/venue=x/a.ndjson": b'{"a":1}'})
        with self.assertRaises(TruncatedObject):
            list(iter_ndjson_lines(streamer))

    def test_composite_streamer_sorts_and_dispatches_child_objects(self) -> None:
        composite = CompositeByteStreamer(
            MemoryByteStreamer({"z.ndjson": b'{"z":1}\n'}),
            MemoryByteStreamer({"a.json": b'{"a":1}\n'}),
        )

        self.assertEqual(composite.object_keys(), ("a.json", "z.ndjson"))
        self.assertEqual(b"".join(composite.iter_bytes("z.ndjson")), b'{"z":1}\n')

    def test_composite_streamer_rejects_collisions_and_unknown_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate composite object key"):
            CompositeByteStreamer(
                MemoryByteStreamer({"same.ndjson": b"first\n"}),
                MemoryByteStreamer({"same.ndjson": b"second\n"}),
            )
        composite = CompositeByteStreamer(MemoryByteStreamer({"known": b"value"}))
        with self.assertRaises(StreamError):
            next(composite.iter_bytes("missing"))


if __name__ == "__main__":
    unittest.main()
