"""§9.1 — the shared Zstandard codec.

Every rejection test builds the unsafe object first and asserts the codec
refuses it. `archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md` §9 is explicit that a
production change begins with a test demonstrating the missing or unsafe
behaviour, and for a decoder that means the corrupt frame has to exist.
"""

from __future__ import annotations

import io
import json
import tempfile
import tracemalloc
import unittest
from pathlib import Path

import zstandard

from encoder import (
    DEFAULT_ZSTD_LEVEL,
    CodecError,
    DecodeLimitExceeded,
    IdentityMismatch,
    LogicalIdentity,
    StoredIdentity,
    decode_stream,
    encode_stream,
    encoder_version,
    logical_identity_of,
    stored_identity_of,
)
from encoder.whole_buffer import compress_bytes, decompress_bytes, encode_identity

FIXTURES = Path(__file__).parents[1] / "encoder" / "fixtures"
#: sha256 of zero bytes — what an empty segment's logical identity must be.
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def fixture_metadata() -> dict:
    return json.loads((FIXTURES / "roundtrip_v1.json").read_text(encoding="utf-8"))


def fixture_payload() -> bytes:
    return (FIXTURES / "roundtrip_v1.ndjson").read_bytes()


def recorded_logical() -> LogicalIdentity:
    return LogicalIdentity.from_record(fixture_metadata()["payload"]["logical"])


class CrossLanguageFixtureTests(unittest.TestCase):
    def test_python_decodes_the_committed_rust_frame_to_exact_bytes(self) -> None:
        frame = (FIXTURES / "roundtrip_v1.rust.ndjson.zst").read_bytes()
        decoded = decompress_bytes(frame, expected_logical=recorded_logical())
        self.assertEqual(decoded, fixture_payload())

    def test_python_frame_matches_its_recorded_stored_identity(self) -> None:
        document = fixture_metadata()
        result = encode_identity(fixture_payload())
        self.assertEqual(result.logical, recorded_logical())
        self.assertEqual(
            result.stored,
            StoredIdentity.from_record(document["frames"]["python"]["stored"]),
        )
        self.assertEqual(
            compress_bytes(fixture_payload()),
            (FIXTURES / "roundtrip_v1.python.ndjson.zst").read_bytes(),
        )

    def test_metadata_records_both_encoders_and_the_v1_compression_contract(self) -> None:
        document = fixture_metadata()
        self.assertEqual(
            document["compression"],
            {
                "algorithm": "zstd",
                "level": DEFAULT_ZSTD_LEVEL,
                "frame_checksum": True,
                "dictionary": None,
                "frame_count": 1,
            },
        )
        self.assertEqual(document["frames"]["python"]["encoder"], encoder_version())
        self.assertTrue(document["frames"]["rust"]["encoder"].startswith("zstd-rs/"))


class RoundTripTests(unittest.TestCase):
    def test_empty_ndjson_produces_and_decodes_a_valid_frame(self) -> None:
        result = encode_identity(b"")
        frame = compress_bytes(b"")
        self.assertGreater(len(frame), 0)
        self.assertEqual(result.logical, LogicalIdentity(EMPTY_SHA256, 0, 0))
        self.assertEqual(
            decompress_bytes(
                frame, expected_logical=result.logical, expected_stored=result.stored
            ),
            b"",
        )

    def test_logical_identity_counts_lf_bytes_as_records(self) -> None:
        payload = b'{"a":1}\n{"b":2}\n{"c":3}\n'
        result = encode_identity(payload)
        self.assertEqual(result.logical.line_count, 3)
        self.assertEqual(result.logical.byte_length, len(payload))
        self.assertEqual(
            result.logical, logical_identity_of(io.BytesIO(payload))
        )

    def test_a_payload_without_a_final_newline_is_refused(self) -> None:
        with self.assertRaisesRegex(CodecError, "does not end in LF"):
            encode_identity(b'{"a":1}')

    def test_encode_refuses_a_buffer_above_the_streaming_ceiling(self) -> None:
        with self.assertRaisesRegex(ValueError, "streaming ceiling"):
            encode_stream(io.BytesIO(b"x\n"), io.BytesIO(), buffer_bytes=4 * 1024 * 1024)


class StrictDecodingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.payload = b'{"one":1}\n{"two":2}\n'
        self.result = encode_identity(self.payload)
        self.frame = compress_bytes(self.payload)

    def decode(self, frame: bytes, **overrides) -> bytes:
        arguments = {"expected_logical": self.result.logical}
        arguments.update(overrides)
        return decompress_bytes(frame, **arguments)

    def test_a_truncated_frame_is_rejected(self) -> None:
        with self.assertRaisesRegex(CodecError, "truncated"):
            self.decode(self.frame[:-3])

    def test_trailing_bytes_after_the_frame_are_rejected(self) -> None:
        with self.assertRaisesRegex(CodecError, "trailing bytes"):
            self.decode(self.frame + b"junk")

    def test_concatenated_frames_are_rejected(self) -> None:
        with self.assertRaisesRegex(CodecError, "trailing bytes"):
            self.decode(self.frame + self.frame)

    def test_a_corrupt_frame_checksum_is_rejected(self) -> None:
        corrupt = bytearray(self.frame)
        corrupt[-1] ^= 0xFF
        with self.assertRaises(CodecError):
            self.decode(bytes(corrupt))

    def test_a_frame_without_a_checksum_is_rejected(self) -> None:
        unchecksummed = zstandard.ZstdCompressor(
            level=DEFAULT_ZSTD_LEVEL, write_checksum=False
        ).compress(self.payload)
        with self.assertRaisesRegex(CodecError, "no checksum"):
            self.decode(unchecksummed)

    def test_a_dictionary_dependent_frame_is_rejected(self) -> None:
        samples = [json.dumps({"k": index, "v": "x" * 48}).encode() for index in range(256)]
        dictionary = zstandard.train_dictionary(4096, samples)
        framed = zstandard.ZstdCompressor(
            level=DEFAULT_ZSTD_LEVEL, dict_data=dictionary, write_checksum=True
        ).compress(self.payload)
        with self.assertRaisesRegex(CodecError, "dictionary"):
            self.decode(framed)

    def test_input_that_is_not_a_frame_is_rejected(self) -> None:
        with self.assertRaises(CodecError):
            self.decode(b"not-a-zstd-frame-at-all")

    def test_a_wrong_stored_identity_fails_independently_of_the_logical_one(self) -> None:
        wrong = StoredIdentity(sha256="0" * 64, byte_length=self.result.stored.byte_length)
        with self.assertRaisesRegex(IdentityMismatch, "stored sha256"):
            self.decode(self.frame, expected_stored=wrong)

        short = StoredIdentity(
            sha256=self.result.stored.sha256, byte_length=self.result.stored.byte_length - 1
        )
        with self.assertRaisesRegex(IdentityMismatch, "stored byte length"):
            self.decode(self.frame, expected_stored=short)

    def test_a_wrong_logical_hash_length_or_line_count_each_fail(self) -> None:
        logical = self.result.logical
        cases = {
            "decoded sha256": LogicalIdentity("0" * 64, logical.byte_length, logical.line_count),
            "decoded byte length": LogicalIdentity(
                logical.sha256, logical.byte_length - 1, logical.line_count
            ),
            "decoded line count": LogicalIdentity(
                logical.sha256, logical.byte_length, logical.line_count + 1
            ),
        }
        for expected_message, identity in cases.items():
            with self.subTest(expected_message):
                with self.assertRaises(IdentityMismatch) as raised:
                    self.decode(
                        self.frame,
                        expected_logical=identity,
                        max_decoded_bytes=len(self.payload),
                    )
                self.assertIn(expected_message, str(raised.exception))

    def test_the_limit_aborts_before_the_first_byte_beyond_it(self) -> None:
        sink = io.BytesIO()
        with self.assertRaises(DecodeLimitExceeded):
            decode_stream(
                io.BytesIO(self.frame),
                sink,
                expected_logical=self.result.logical,
                max_decoded_bytes=8,
            )
        self.assertLessEqual(len(sink.getvalue()), 8)


class ShortWriteSink:
    """A sink that takes one byte fewer than it was offered, and says so.

    `write` returning less than it was given is ordinary, documented behaviour —
    an unbuffered file does it under memory or device pressure. This is the
    smallest sink that exercises it.
    """

    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, data) -> int:
        payload = bytes(data)
        taken = payload[:-1] if len(payload) > 1 else payload
        self.buffer.extend(taken)
        return len(taken)

    def flush(self) -> None:
        pass


class ShortWriteTests(unittest.TestCase):
    """An identity may only describe bytes that actually reached the sink."""

    def test_the_encoder_writes_every_byte_of_the_frame(self) -> None:
        sink = ShortWriteSink()
        result = encode_stream(io.BytesIO(b'{"a":1}\n'), sink)
        self.assertEqual(len(sink.buffer), result.stored.byte_length)
        self.assertEqual(
            stored_identity_of(io.BytesIO(bytes(sink.buffer))).sha256, result.stored.sha256
        )

    def test_the_decoder_writes_every_decoded_byte(self) -> None:
        payload = b'{"k":"v"}\n' * 800
        frame = compress_bytes(payload)
        sink = ShortWriteSink()
        result = decode_stream(
            io.BytesIO(frame), sink, expected_logical=encode_identity(payload).logical
        )
        self.assertEqual(bytes(sink.buffer), payload)
        self.assertEqual(len(sink.buffer), result.logical.byte_length)

    def test_a_sink_that_makes_no_progress_is_an_error_not_a_loop(self) -> None:
        class StalledSink:
            def write(self, data) -> int:
                return 0

            def flush(self) -> None:
                pass

        with self.assertRaisesRegex(CodecError, "no bytes"):
            encode_stream(io.BytesIO(b'{"a":1}\n'), StalledSink())


class CompressionLevelTests(unittest.TestCase):
    """§4.1 pins level 3, and receipts state it — so nothing else may be used."""

    def test_any_level_other_than_three_is_refused(self) -> None:
        for level in (1, 9, 19, -3):
            with self.subTest(level), self.assertRaisesRegex(ValueError, "V1 level"):
                encode_stream(io.BytesIO(b'{"a":1}\n'), io.BytesIO(), level=level)

    def test_level_three_is_accepted(self) -> None:
        result = encode_stream(io.BytesIO(b'{"a":1}\n'), io.BytesIO(), level=DEFAULT_ZSTD_LEVEL)
        self.assertEqual(result.logical.line_count, 1)


class FrameWalkTests(unittest.TestCase):
    """The frame walk is what makes single-pass strictness possible.

    Everything in `StrictDecodingTests` rests on knowing exactly where the frame
    ended, so the walk is checked directly against frames this codec did not
    produce: other levels, other content-size settings, and payloads that make
    libzstd emit raw and run-length blocks rather than compressed ones.
    """

    def frames(self):
        payloads = {
            "empty": b"",
            "tiny": b"a\n",
            "run_length": b"z" * (2 * 1024 * 1024),
            "incompressible": bytes(range(256)) * 4096,
            "text": b'{"k":"v"}\n' * 20000,
        }
        for level in (1, 3, 19):
            for name, payload in payloads.items():
                for content_size in (True, False):
                    compressor = zstandard.ZstdCompressor(
                        level=level, write_checksum=True, write_content_size=content_size
                    )
                    if content_size:
                        frame = compressor.compress(payload)
                    else:
                        sink = io.BytesIO()
                        with compressor.stream_writer(sink, closefd=False) as writer:
                            writer.write(payload)
                        frame = sink.getvalue()
                    yield f"{name}/level={level}/content_size={content_size}", frame

    def test_the_walk_finds_the_exact_end_of_every_frame_shape(self) -> None:
        from encoder.compression import _FrameBoundedReader

        for label, frame in self.frames():
            with self.subTest(label):
                reader = _FrameBoundedReader(io.BytesIO(frame))
                handed = bytearray()
                while chunk := reader.read(64 * 1024):
                    handed.extend(chunk)
                self.assertTrue(reader.frame_complete)
                self.assertEqual(reader.frame_length, len(frame))
                self.assertEqual(bytes(handed), frame)
                self.assertFalse(reader.has_trailing_bytes())

    def test_the_walk_withholds_trailing_bytes_from_the_decoder(self) -> None:
        from encoder.compression import _FrameBoundedReader

        frame = compress_bytes(b"one\n")
        for label, appended in (("junk", b"XYZ"), ("second frame", frame)):
            with self.subTest(label):
                reader = _FrameBoundedReader(io.BytesIO(frame + appended))
                handed = bytearray()
                while chunk := reader.read(4096):
                    handed.extend(chunk)
                self.assertEqual(bytes(handed), frame)
                self.assertTrue(reader.has_trailing_bytes())


class BoundedMemoryTests(unittest.TestCase):
    def test_memory_does_not_scale_with_input_size(self) -> None:
        """16 MiB through a 1 MiB encode buffer and a 128 KiB decode buffer."""
        line = json.dumps({"filler": "z" * 200}).encode() + b"\n"
        target = 16 * 1024 * 1024

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "large.ndjson"
            with source.open("wb") as handle:
                written = 0
                while written < target:
                    handle.write(line)
                    written += len(line)

            tracemalloc.start()
            try:
                with source.open("rb") as reader, (root / "large.zst").open("wb") as writer:
                    result = encode_stream(reader, writer)
                _, encode_peak = tracemalloc.get_traced_memory()
                tracemalloc.reset_peak()
                with (root / "large.zst").open("rb") as reader, (
                    root / "large.decoded"
                ).open("wb") as writer:
                    decode_stream(
                        reader,
                        writer,
                        expected_logical=result.logical,
                        expected_stored=result.stored,
                    )
                _, decode_peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()

            self.assertEqual(source.stat().st_size, (root / "large.decoded").stat().st_size)
            self.assertEqual(result.logical.byte_length, source.stat().st_size)
            # Generously above the buffers and far below the 16 MiB payload:
            # the assertion is that peak tracks the buffer, not the file.
            self.assertLess(encode_peak, 6 * 1024 * 1024, "encode allocated with the file size")
            self.assertLess(decode_peak, 6 * 1024 * 1024, "decode allocated with the file size")


if __name__ == "__main__":
    unittest.main()
