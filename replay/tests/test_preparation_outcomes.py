"""Small, identity-valid documents; no live Universe or retained fixture."""

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from analysis.claims import claim_id, space_shape_id
from analysis.outcome_space import build_series_space
from replay.outcome_model import outcome_books, validate_document
from replay.preparation import (
    SourceUnavailable,
    UniverseHTTP,
    build_snapshot,
    digest,
    encoded,
    load_snapshot,
    prepare,
)
from replay.streams.protocol import ProtocolError
from replay.tests.test_preparation import config, detail


def document():
    space = build_series_space("bundle-1", best_of=3, home="Alpha", away="Beta")
    shape = space_shape_id(space)
    home = sorted(space.select(lambda p: p["winner_side"] == "home"))
    away = sorted(space.keys - set(home))
    claims = [
        {
            "claim_id": claim_id(keys, shape),
            "space_shape_id": shape,
            "outcome_keys": keys,
        }
        for keys in (home, away)
    ]
    markets = []
    for mid, subs, labels, indexes in [
        ("kalshi:series", ["series"], ["Alpha"], [0]),
        ("polymarket:series", ["123", "987"], ["Alpha", "Beta"], [0, 1]),
        ("limitless:internal", ["slug"], ["Alpha"], [0]),
    ]:
        markets.append(
            {
                "market_id": mid,
                "venue": mid.split(":")[0],
                "market_type": "series_moneyline",
                "market_status": "open",
                "subscription_ids": subs,
                "outcome_labels": labels,
                "mask_status": "MASKED",
                "reason": None,
                "claims": [
                    {"claim_key": f"claim={i}", "claim_id": claims[index]["claim_id"]}
                    for i, index in enumerate(indexes)
                ],
                "tokens": [
                    {"subscription_id": s, "claim_key": f"claim={i}", "negated": False}
                    for i, s in enumerate(subs)
                ],
            }
        )
    return {
        "version": 1,
        "bundle_id": "bundle-1",
        "event_id": "event:d1:" + "a" * 64,
        "identities": {"claim_identity_version": 2, "claim_algebra_version": 1},
        "status": "complete",
        "diagnostics": [],
        "participants": ["Alpha", "Beta"],
        "spaces": [
            {
                "space_shape_id": shape,
                "scope": "series",
                "coverage": "EXHAUSTIVE",
                "best_of": 3,
                "outcome_keys": sorted(space.keys),
            }
        ],
        "claims": sorted(claims, key=lambda c: c["claim_id"]),
        "markets": sorted(markets, key=lambda m: m["market_id"]),
    }


class PreparationOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_mapping_and_reload(self):
        d = detail()
        d["context"]["markets"].insert(
            1,
            {"target_id": "limitless:internal", "venue": "limitless", "selected": True},
        )
        d["context"]["targets"].insert(
            1,
            {
                "target_id": "limitless:internal",
                "venue": "limitless",
                "subscription_ids": ["slug"],
                "canonical_class": "esports.series_moneyline",
                "source_ref": "/test",
            },
        )
        source = Mock(return_value=document())
        snapshot = prepare(
            config(d), self.root / "run", universe=lambda *_: d, outcomes=source
        )
        source.assert_called_once_with("bundle-1")
        books = {
            (b["instrument"], b["orientation"]): b
            for b in snapshot["scopes"][0]["outcome_books"]
        }
        self.assertEqual(snapshot["version"], 2)
        self.assertTrue(books["kalshi:series", "complement"]["negated"])
        self.assertFalse(books["kalshi:series", "outcome"]["negated"])
        self.assertEqual(len(books), 5)
        self.assertEqual(snapshot, load_snapshot(self.root / "run"))
        prepare(
            config(d),
            self.root / "again",
            universe=lambda *_: d,
            outcomes=lambda _: document(),
        )
        self.assertEqual(
            (self.root / "run/context.json").read_bytes(),
            (self.root / "again/context.json").read_bytes(),
        )

    def test_statuses_and_probe(self):
        for i, status in enumerate(
            ("NOT_IN_MODEL", "SUBSCRIPTION_MISMATCH", "VOID_UNSUPPORTED")
        ):
            doc = document()
            if status == "NOT_IN_MODEL":
                doc["markets"].pop(0)
            elif status == "SUBSCRIPTION_MISMATCH":
                doc["markets"][0]["subscription_ids"] = ["different"]
                doc["markets"][0]["tokens"][0]["subscription_id"] = "different"
            else:
                doc["markets"][0].update(mask_status=status, claims=[], tokens=[])
            c = config()
            c["probe_markets"] = ["kalshi:series"]
            s = prepare(
                c,
                self.root / str(i),
                universe=lambda *_: detail(),
                outcomes=lambda _, doc=doc: doc,
            )
            entries = s["scopes"][0]["outcome_books"]
            self.assertEqual(len(entries), 2)
            self.assertTrue(
                all(e["status"] == status and e["negated"] is None for e in entries)
            )

    def test_subscription_ids_compare_independent_of_order(self):
        # Universe and the Targeter may list the same Polymarket tokens in a
        # different order; only a different id multiset is a mismatch.
        scope = {
            "members": [
                {
                    "market_id": "polymarket:series",
                    "books": [
                        {"instrument": "polymarket:" + s, "orientation": "outcome"}
                        for s in ("123", "987")
                    ],
                }
            ]
        }
        doc = document()
        market = next(m for m in doc["markets"] if m["market_id"] == "polymarket:series")
        outcomes = {"provider": "universe", "document": doc}
        for target_ids, status in (
            (["987", "123"], "MASKED"),
            (["987", "456"], "SUBSCRIPTION_MISMATCH"),
            (["123"], "SUBSCRIPTION_MISMATCH"),
        ):
            d = detail()
            d["context"]["targets"][1]["subscription_ids"] = target_ids
            entries = outcome_books(scope, d, outcomes)
            self.assertEqual([e["status"] for e in entries], [status, status])
            if status == "MASKED":
                self.assertEqual(
                    [e["claim_id"] for e in entries],
                    [ref["claim_id"] for ref in market["claims"]],
                )

    def test_unavailable_and_fatal_errors(self):
        for i, source in enumerate(
            (None, Mock(return_value=None), Mock(side_effect=SourceUnavailable()))
        ):
            s = prepare(
                config(),
                self.root / str(i),
                universe=lambda *_: detail(),
                outcomes=source,
            )
            self.assertEqual(s["outcomes"]["provider"], None)
            self.assertTrue(
                all(
                    b["status"] == "OUTCOMES_UNAVAILABLE"
                    for b in s["scopes"][0]["outcome_books"]
                )
            )
        for i, source in enumerate(
            (
                Mock(side_effect=HTTPError("url", 409, "conflict", {}, None)),
                Mock(return_value={"bad": 1}),
            )
        ):
            with self.assertRaises((HTTPError, ProtocolError)):
                prepare(
                    config(),
                    self.root / f"bad{i}",
                    universe=lambda *_: detail(),
                    outcomes=source,
                )

    def test_closed_validation_and_tampering(self):
        validate_document(document(), "bundle-1")
        mutations = [
            lambda d: d["claims"][0]["outcome_keys"].pop(),
            lambda d: d["spaces"][0]["outcome_keys"].pop(),
            lambda d: d["claims"].reverse(),
            lambda d: d["markets"][2]["tokens"].pop(),
            lambda d: d["markets"][0].update(reason="guessed"),
            lambda d: d.update(extra=True),
            lambda d: d["markets"][0]["claims"][0].update(claim_key="claim=1"),
        ]
        for mutate in mutations:
            doc = document()
            mutate(doc)
            with self.assertRaises(ProtocolError):
                validate_document(doc, "bundle-1")

    def test_version_one_loads_without_new_fields(self):
        c, d = config(), detail()
        s = build_snapshot(c, [{"provider": "universe", "detail": d}])
        self.assertEqual(s["version"], 1)
        root = self.root / "v1"
        root.mkdir()
        payload = encoded(s)
        (root / "context.json").write_bytes(payload)
        (root / "receipt.json").write_bytes(
            encoded(
                {
                    "version": 1,
                    "snapshot_sha256": hashlib.sha256(payload).hexdigest(),
                    "snapshot_byte_length": len(payload),
                    "config_sha256": digest(c),
                }
            )
        )
        loaded = load_snapshot(root)
        self.assertNotIn("outcomes", loaded)
        self.assertNotIn("outcome_books", loaded["scopes"][0])

    def test_http_outcomes_uses_shared_transport(self):
        source = UniverseHTTP("https://universe.example", timeout=2)
        with patch.object(source, "_get", return_value=document()) as get:
            source.outcomes("bundle/one")
        get.assert_called_once_with(
            "https://universe.example/v1/bundles/bundle%2Fone/outcomes"
        )
        opener = Mock()
        for status in (404, 502, 503, 504, 409):
            opener.open.side_effect = HTTPError("url", status, "error", {}, None)
            with (
                patch("replay.preparation.build_opener", return_value=opener),
                self.assertRaises(HTTPError if status == 409 else SourceUnavailable),
            ):
                source.outcomes("bundle-1")
