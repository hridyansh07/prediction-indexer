from __future__ import annotations

import json
import unittest

from splices.common.envelope import (
    ENVELOPE_FIELDS,
    ENVELOPE_FIELDS_V1,
    ENVELOPE_VERSION,
    KIND_VENUE_FRAME,
    STREAM_PUBLIC_BOOK,
    VENUE_POLYMARKET,
    EnvelopeError,
    build_envelope,
    encode_envelope,
    unsequenced_cursor,
)


def _envelope(**overrides):
    base = dict(
        delivery_index=1,
        record_id="pm-abc-1",
        visible_ns=1_700_000_000_000_000_000,
        monotonic_ns=123_456_789,
        venue=VENUE_POLYMARKET,
        stream=STREAM_PUBLIC_BOOK,
        connection_epoch="abc",
        local_counter=1,
        kind=KIND_VENUE_FRAME,
        raw_payload='{"event_type":"book"}',
        source_cursor=unsequenced_cursor(1),
    )
    base.update(overrides)
    return build_envelope(**base)


class EnvelopeTests(unittest.TestCase):
    def test_field_set_matches_the_ingester_exactly(self) -> None:
        """The Rust parser rejects unknown fields and requires all ten, so drift
        here makes every spool line unreadable rather than partially readable."""
        self.assertEqual(tuple(_envelope()), ENVELOPE_FIELDS)
        self.assertEqual(_envelope()["envelope_version"], ENVELOPE_VERSION)

    def test_v1_remains_the_original_closed_shape(self) -> None:
        record = _envelope(envelope_version=1, monotonic_ns=None)
        self.assertEqual(tuple(record), ENVELOPE_FIELDS_V1)
        self.assertNotIn("envelope_version", record)
        self.assertNotIn("monotonic_ns", record)

    def test_v2_requires_monotonic_time(self) -> None:
        with self.assertRaises(EnvelopeError):
            _envelope(monotonic_ns=None)

    def test_source_cursor_is_present_but_null_for_control_records(self) -> None:
        record = _envelope(source_cursor=None)
        self.assertIn("source_cursor", record)
        self.assertIsNone(record["source_cursor"])
        self.assertIn('"source_cursor":null', encode_envelope(record).decode())

    def test_identifiers_reject_what_the_borrowing_parser_cannot_read(self) -> None:
        """`record_id` and `connection_epoch` are read as raw byte borrows between
        the quotes, without unescaping."""
        for bad in ('pm"1', "pm\\1", "pm\x011", "pm-é"):
            with self.subTest(bad=bad), self.assertRaises(EnvelopeError):
                _envelope(record_id=bad)

    def test_booleans_are_not_accepted_as_counters(self) -> None:
        """isinstance(True, int) holds, and `true` fails the ASCII digit parser a
        whole layer away from the cause."""
        with self.assertRaises(EnvelopeError):
            _envelope(delivery_index=True)

    def test_negative_counter_is_refused(self) -> None:
        with self.assertRaises(EnvelopeError):
            _envelope(local_counter=-1)

    def test_payload_is_carried_verbatim_not_reparsed(self) -> None:
        payload = '{"unknown_event":"something new","n":[1,2,3]}'
        line = encode_envelope(_envelope(raw_payload=payload))
        self.assertEqual(json.loads(line)["raw_payload"], payload)

    def test_encoding_is_deterministic_and_single_line(self) -> None:
        record = _envelope()
        self.assertEqual(encode_envelope(record), encode_envelope(dict(record)))
        self.assertEqual(encode_envelope(record).count(b"\n"), 1)


if __name__ == "__main__":
    unittest.main()
