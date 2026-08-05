from __future__ import annotations

import unittest

from replay.events import ConnectionClosed, ConnectionOpened, FullBook
from replay.output import encode_analysis_output
from replay.trust import Verdict, audit_trust


def _base(index: int) -> dict[str, object]:
    return {
        "venue": "limitless",
        "lane": "limitless",
        "epoch": "epoch",
        "record_id": f"record-{index}",
        "delivery_index": index,
        "order_ns": index,
        "visible_ns": index,
        "event_index": 0,
    }


class OutputContractTests(unittest.TestCase):
    def test_bare_numeric_analysis_output_is_a_type_error(self) -> None:
        with self.assertRaises(TypeError):
            encode_analysis_output(42)


class TrustTests(unittest.TestCase):
    def test_equal_limitless_versions_are_not_a_false_regression(self) -> None:
        opened = ConnectionOpened(
            **_base(1), asset_ids=("market",), delivers_deltas=False
        )
        first = FullBook(
            **_base(2),
            market_id="market",
            asset_id="market",
            bids=(),
            asks=(),
            state_hash=None,
            source_time=2,
            source_version=7,
            independent_snapshot=True,
        )
        repeated = FullBook(
            **_base(3),
            market_id="market",
            asset_id="market",
            bids=(),
            asks=(),
            state_hash=None,
            source_time=3,
            source_version=7,
            independent_snapshot=True,
        )
        closed = ConnectionClosed(**_base(4))

        audit = audit_trust([opened, first, repeated, closed], ())

        self.assertEqual(len(audit.markets), 1)
        self.assertNotIn(
            Verdict.UNTRUSTED,
            {interval.verdict for interval in audit.markets[0].intervals},
        )


if __name__ == "__main__":
    unittest.main()
