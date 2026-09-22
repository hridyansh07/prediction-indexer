import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from archive.storage.base import INDEPENDENT, JSON_CONTENT_TYPE, VerificationFailure
from archive.storage.local import LocalObjectStore
from encoder import encode_stream
from replay.preparation import (
    SourceUnavailable,
    UniverseHTTP,
    encoded,
    load_snapshot,
    prepare,
    validate_config,
)
from replay.preparation_sources import ArchivedSelections
from replay.streams.protocol import ProtocolError
from targeter.v2.run_archive import parse_run_archive_receipt
from tests.test_event_universe_store import (
    G1,
    G2,
    R1,
    R2,
    _retained_report,
    _selection_report,
)
from tests.test_targeter_replay_stream import _compression, _entry, _stored
from universe.projection import project_selected_bundles
from universe.store import EvidenceConflict
from universe.sync import _complete_context


def report():
    result = _selection_report(R1, G1)
    result["selection"]["targets"]["kalshi"][0]["subscription_ids"] = ["series"]
    result["selection"]["targets"]["polymarket"][0]["subscription_ids"] = ["123", "987"]
    result["candidates"][0]["relationship_analysis"]["relationships"] = []
    return result


def detail():
    row = project_selected_bundles(report())[0]
    context = _complete_context(row)
    prefix = f"targeter-v2/runs/date=2026-01-01/run={R1}"
    source = {
        "manifest_key": prefix + "/run_manifest.json",
        "manifest_sha256": "a" * 64,
        "report_key": prefix + "/selection_report.json.zst",
        "report_sha256": "b" * 64,
    }
    return {
        **{
            f: row[f]
            for f in (
                "run_id",
                "generated_at",
                "bundle_id",
                "occurrence_kind",
                "continuity_selected",
                "continuity_disposition",
            )
        },
        **{
            f: context[f]
            for f in ("sport", "game", "topology", "activation_at", "capture_start_at")
        },
        "source": source,
        "origin": {**source, "run_id": R1, "generated_at": G1},
        "retirement": None,
        "context": context,
    }


def config(d=None):
    d = d or detail()
    return {
        "version": 1,
        "pins": [{"derivative_address": "1" * 64, "receipt_sha256": "2" * 64}],
        "start_ns": "10",
        "end_ns": "40",
        "lower_bound": "clip",
        "bundle_id": "bundle-1",
        "market_namespace": "targeter_target_id",
        "probe_markets": None,
        "occurrences": [
            {
                "run_id": d["run_id"],
                "start_ns": "10",
                "end_ns": "40",
                "source": d["source"],
            }
        ],
        "authorities": [
            {
                "venue": v,
                "lane": "explicit-" + v,
                "price_scale": "3",
                "quantity_scale": "6",
            }
            for v in ("kalshi", "polymarket", "limitless")
        ],
    }


def archive_report(store, value):
    """Small compressed contract through real codec/store/receipt readers."""
    prefix = f"targeter-v2/runs/date=2026-01-01/run={value['run_id']}"
    sink = io.BytesIO()
    identities = encode_stream(io.BytesIO(encoded(value) + b"\n"), sink)
    report_entry = _entry(
        name="selection_report.json.zst",
        key=prefix + "/selection_report.json.zst",
        stored=identities.stored,
        logical=identities.logical,
        content_type=JSON_CONTENT_TYPE,
        content_encoding="zstd",
    )
    metadata = encoded(
        {
            "decoded": identities.logical.as_record(),
            "stored": identities.stored.as_record(),
        }
    )
    metadata_entry = _entry(
        name="selection_report.meta.json",
        key=prefix + "/selection_report.meta.json",
        stored=_stored(metadata),
        content_type=JSON_CONTENT_TYPE,
        content_encoding=None,
    )
    manifest = {
        "targeter_run_manifest_version": 2,
        "run_id": value["run_id"],
        "generated_at": value["generated_at"],
        "input_complete": True,
        "files": [
            {
                "file": report_entry["file"],
                "content_type": JSON_CONTENT_TYPE,
                "content_encoding": "zstd",
                "stored": identities.stored.as_record(),
                "decoded": identities.logical.as_record(),
                "compression": _compression(),
            },
            {
                k: metadata_entry[k]
                for k in (
                    "file",
                    "content_type",
                    "content_encoding",
                    "byte_length",
                    "sha256",
                )
            },
        ],
    }
    manifest_bytes = encoded(manifest) + b"\n"
    manifest_entry = _entry(
        name="run_manifest.json",
        key=prefix + "/run_manifest.json",
        stored=_stored(manifest_bytes),
        content_type=JSON_CONTENT_TYPE,
        content_encoding=None,
    )
    for entry, payload in (
        (report_entry, sink.getvalue()),
        (metadata_entry, metadata),
        (manifest_entry, manifest_bytes),
    ):
        store.put_immutable(
            entry["key"],
            io.BytesIO(payload),
            _stored(payload),
            content_type=JSON_CONTENT_TYPE,
            content_encoding=entry["content_encoding"],
        )
    receipt = parse_run_archive_receipt(
        {
            "targeter_run_archive_receipt_version": 2,
            "run_id": value["run_id"],
            "bucket": "test-archive",
            "prefix": prefix,
            "archived_at_ns": 1,
            "manifest": manifest_entry,
            "objects": [report_entry, metadata_entry, manifest_entry],
            "durability": "independent_durable",
            "authorizes_publication": True,
        },
        path=Path(value["run_id"]) / "archive_receipt.json",
    )
    source = {
        "manifest_key": manifest_entry["key"],
        "manifest_sha256": manifest_entry["sha256"],
        "report_key": report_entry["key"],
        "report_sha256": report_entry["sha256"],
    }
    return receipt, source


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_whole_bundle_preserves_uncaptured_and_native_books(self):
        d = detail()
        d["context"]["markets"].insert(
            1,
            {
                "target_id": "limitless:internal-id",
                "venue": "limitless",
                "selected": True,
            },
        )
        d["context"]["targets"].insert(
            1,
            {
                "target_id": "limitless:internal-id",
                "venue": "limitless",
                "subscription_ids": ["socket-market-slug"],
                "canonical_class": "any.product",
                "source_ref": "/market",
            },
        )
        fallback = Mock(
            side_effect=AssertionError("must not fall back on empty relationships")
        )
        snapshot = prepare(
            config(d), self.root / "run", universe=lambda *_: d, fallback=fallback
        )
        scope = snapshot["scopes"][0]
        self.assertEqual(scope["unresolved_market_ids"], ("polymarket:map-one",))
        self.assertEqual(len(scope["listed_market_ids"]), 4)
        self.assertEqual(len(scope["capture_selected_market_ids"]), 3)
        self.assertEqual(
            [(b["instrument"], b["orientation"]) for b in scope["required_books"]],
            [
                ("kalshi:series", "complement"),
                ("kalshi:series", "outcome"),
                ("limitless:socket-market-slug", "outcome"),
                ("polymarket:123", "outcome"),
                ("polymarket:987", "outcome"),
            ],
        )
        self.assertEqual(snapshot["plans"][0]["lane"], "explicit-kalshi")
        self.assertFalse(snapshot["history_complete"])
        self.assertEqual(snapshot["membership_basis"], "caller_pinned_expectations")
        with self.assertRaises(TypeError):
            snapshot["scopes"][0]["members"][0]["capture_selected"] = False
        fallback.assert_not_called()

    def test_subset_and_missing_member(self):
        c = config()
        c["probe_markets"] = ["polymarket:series"]
        snapshot = prepare(c, self.root / "subset", universe=lambda *_: detail())
        self.assertEqual(len(snapshot["plans"]), 2)
        self.assertEqual(snapshot["scopes"][0]["unresolved_market_ids"], ())
        c["probe_markets"] = ["polymarket:map-one"]
        snapshot = prepare(c, self.root / "uncaptured", universe=lambda *_: detail())
        self.assertEqual(snapshot["plans"], ())
        self.assertEqual(len(snapshot["scopes"][0]["members"]), 1)
        c["probe_markets"] = ["polymarket:absent"]
        with self.assertRaisesRegex(ProtocolError, "probe absent"):
            prepare(c, self.root / "absent", universe=lambda *_: detail())

    def test_fallback_only_for_absence_or_unavailability(self):
        for i, universe in enumerate(
            (Mock(return_value=None), Mock(side_effect=SourceUnavailable()))
        ):
            fallback = Mock(return_value=detail())
            snapshot = prepare(
                config(), self.root / str(i), universe=universe, fallback=fallback
            )
            self.assertEqual(snapshot["evidence"][0]["provider"], "targeter")
            fallback.assert_called_once()
        for i, mutate in enumerate(
            (
                lambda d: d.update(bundle_id="wrong"),
                lambda d: d["source"].update(report_sha256="f" * 64),
                lambda d: d["context"].update(relationships=None),
                lambda d: d.update(continuity_selected=True),
                lambda d: d.update(unrecognized=True),
            )
        ):
            d = detail()
            mutate(d)
            fallback = Mock(side_effect=AssertionError("conflict must not fall back"))
            with self.assertRaises((ProtocolError, EvidenceConflict)):
                prepare(
                    config(),
                    self.root / f"invalid-{i}",
                    universe=Mock(return_value=d),
                    fallback=fallback,
                )
            fallback.assert_not_called()

    def test_mapping_errors_and_explicit_authority(self):
        for i, subscriptions in enumerate(([], ["123", "123"], ["not-a-token"])):
            d = detail()
            d["context"]["targets"][1]["subscription_ids"] = subscriptions
            with self.assertRaises((ProtocolError, EvidenceConflict)):
                prepare(config(), self.root / str(i), universe=Mock(return_value=d))
        c = config()
        c["authorities"] = [c["authorities"][0]]
        with self.assertRaisesRegex(ProtocolError, "source authority"):
            prepare(c, self.root / "authority", universe=lambda *_: detail())

    def test_subset_cannot_hide_ambiguous_token_ownership(self):
        d = detail()
        d["context"]["markets"][1]["selected"] = True
        duplicate = copy.deepcopy(d["context"]["targets"][1])
        duplicate["target_id"] = "polymarket:map-one"
        d["context"]["targets"].insert(1, duplicate)
        c = config()
        c["probe_markets"] = ["polymarket:series"]
        with self.assertRaisesRegex(ProtocolError, "multiple markets"):
            prepare(c, self.root / "ambiguous", universe=lambda *_: d)

    def test_explicit_history_partition_and_boundary_membership(self):
        c = config()
        d = detail()
        c["occurrences"][0]["end_ns"] = "23"
        second = copy.deepcopy(c["occurrences"][0])
        second.update(start_ns="23", end_ns="40")
        c["occurrences"].append(second)
        later = copy.deepcopy(d)
        later["context"]["markets"] = [
            m for m in later["context"]["markets"] if m["selected"]
        ]
        # Same pinned identity may not return changed historical context.
        with self.assertRaisesRegex(ProtocolError, "occurrence evidence changed"):
            prepare(c, self.root / "conflict", universe=Mock(side_effect=[d, later]))
        snapshot = prepare(c, self.root / "valid", universe=lambda *_: d)
        self.assertEqual(
            [(s["start_ns"], s["end_ns"]) for s in snapshot["scopes"]],
            [("10", "23"), ("23", "40")],
        )
        changed_config = copy.deepcopy(c)
        later["run_id"], later["generated_at"] = R2, G2
        later["source"] = {k: v.replace(R1, R2) for k, v in d["source"].items()}
        later["source"]["report_sha256"] = "c" * 64
        later["origin"] = {**later["source"], "run_id": R2, "generated_at": G2}
        changed_config["occurrences"][1].update(run_id=R2, source=later["source"])
        changed = prepare(
            changed_config, self.root / "changed", universe=Mock(side_effect=[d, later])
        )
        self.assertEqual(
            changed["scopes"][0]["unresolved_market_ids"], ("polymarket:map-one",)
        )
        self.assertEqual(changed["scopes"][1]["unresolved_market_ids"], ())
        for boundary in ("22", "24"):
            invalid = copy.deepcopy(c)
            invalid["occurrences"][1]["start_ns"] = boundary
            with self.assertRaisesRegex(ProtocolError, "partition"):
                validate_config(invalid)
        c["occurrences"].pop()
        with self.assertRaisesRegex(ProtocolError, "incomplete explicit history"):
            validate_config(c)

    def test_repeated_preparation_offline_and_receipt_tampering(self):
        c = config()
        first = prepare(c, self.root / "run", universe=lambda *_: detail())
        original = (self.root / "run/context.json").read_bytes()
        offline = Mock(side_effect=AssertionError("network access"))
        self.assertEqual(prepare(c, self.root / "run", universe=offline), first)
        self.assertEqual(
            load_snapshot(
                self.root / "run", expected_sha256=hashlib.sha256(original).hexdigest()
            ),
            first,
        )
        prepare(c, self.root / "independent", universe=lambda *_: detail())
        self.assertEqual(
            (self.root / "independent/context.json").read_bytes(), original
        )
        c["authorities"][0]["lane"] = "other"
        with self.assertRaisesRegex(ProtocolError, "config changed"):
            prepare(c, self.root / "run", universe=offline)
        path = self.root / "run/context.json"
        path.write_bytes(original.replace(b"explicit-kalshi", b"tampered-kalshi"))
        with self.assertRaisesRegex(ProtocolError, "identity mismatch"):
            load_snapshot(path.parent)
        # Even a freshly re-hashed receipt cannot authorize incorrect resolved scope.
        changed = json.loads(original)
        changed["scopes"][0]["required_books"].pop()
        payload = encoded(changed)
        path.write_bytes(payload)
        receipt_path = path.parent / "receipt.json"
        receipt = json.loads(receipt_path.read_bytes())
        receipt.update(
            snapshot_sha256=hashlib.sha256(payload).hexdigest(),
            snapshot_byte_length=len(payload),
        )
        receipt_path.write_bytes(encoded(receipt))
        with self.assertRaisesRegex(ProtocolError, "resolved snapshot conflict"):
            load_snapshot(path.parent)

    def test_closed_schema_and_uncommitted_snapshot(self):
        c = config()
        c["latest"] = True
        with self.assertRaises(ProtocolError):
            validate_config(c)
        root = self.root / "incomplete"
        root.mkdir()
        (root / "context.json").write_text("{}")
        with self.assertRaises(FileNotFoundError):
            load_snapshot(root)
        with self.assertRaisesRegex(ProtocolError, "uncommitted"):
            prepare(config(), root, universe=lambda *_: detail())

    def test_compressed_targeter_streaming_fallback_and_corruption(self):
        store = LocalObjectStore(
            self.root / "archive", store_id="test-archive", durability=INDEPENDENT
        )
        receipt, source = archive_report(store, report())
        c = config()
        c["occurrences"][0]["source"] = source
        fallback = ArchivedSelections(store, [receipt], temp_root=self.root)
        snapshot = prepare(
            c, self.root / "run", universe=lambda *_: None, fallback=fallback
        )
        self.assertEqual(snapshot["evidence"][0]["detail"]["source"], source)
        self.assertEqual(len(snapshot["plans"]), 4)
        # Corrupt real stored compressed bytes; existing archive verification must fail.
        (self.root / "archive" / source["report_key"]).write_bytes(b"corrupt")
        with self.assertRaises(VerificationFailure):
            prepare(
                c, self.root / "corrupt", universe=lambda *_: None, fallback=fallback
            )

    def test_retained_targeter_origin_is_verified_not_current_membership(self):
        store = LocalObjectStore(
            self.root / "archive", store_id="test-archive", durability=INDEPENDENT
        )
        origin_receipt, origin_source = archive_report(store, report())
        retained = _retained_report(R2, G2, {**origin_source, "run_id": R1})
        for venue, targets in retained["selection"]["targets"].items():
            for target in targets:
                target["subscription_ids"] = (
                    ["series"] if venue == "kalshi" else ["123", "987"]
                )
        for target in retained["continuity"]["bundles"][0]["targets"]:
            target["subscription_ids"] = (
                ["series"] if target["venue"] == "kalshi" else ["123", "987"]
            )
        receipt, source = archive_report(store, retained)
        c = config()
        c["occurrences"][0].update(run_id=R2, source=source)
        missing_origin = ArchivedSelections(store, [receipt], temp_root=self.root)
        with self.assertRaisesRegex(ProtocolError, "origin receipt required"):
            prepare(
                c,
                self.root / "missing",
                universe=lambda *_: None,
                fallback=missing_origin,
            )
        fallback = ArchivedSelections(
            store, [origin_receipt, receipt], temp_root=self.root
        )
        snapshot = prepare(
            c, self.root / "retained", universe=lambda *_: None, fallback=fallback
        )
        self.assertEqual(snapshot["evidence"][0]["detail"]["origin"]["run_id"], R1)
        self.assertEqual(
            snapshot["scopes"][0]["unresolved_market_ids"], ("polymarket:map-one",)
        )
        self.assertEqual(snapshot["evidence"][0]["detail"]["retirement"], None)

    def test_http_url_bounds_and_malformed_response(self):
        source = UniverseHTTP("https://universe.example/base", timeout=2)
        response = Mock(status=200)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read1.side_effect = [b'{"x":1,"x":2}', b""]
        opener = Mock()
        opener.open.return_value = response
        with (
            patch("replay.preparation.build_opener", return_value=opener),
            self.assertRaises(ProtocolError),
        ):
            source(config()["occurrences"][0], "bundle/one")
        self.assertEqual(
            opener.open.call_args.args[0],
            f"https://universe.example/base/v1/runs/{R1}/selections/bundle%2Fone",
        )
        self.assertEqual(opener.open.call_args.kwargs, {"timeout": 2})

    def test_http_absence_not_conflict_fallback_and_byte_limit(self):
        from urllib.error import HTTPError

        source = UniverseHTTP("https://universe.example")
        opener = Mock()
        for status in (404, 503, 500, 413):
            opener.open.side_effect = HTTPError(
                "https://universe.example", status, "error", {}, None
            )
            with (
                patch("replay.preparation.build_opener", return_value=opener),
                self.assertRaises(
                    SourceUnavailable if status in (404, 503) else HTTPError
                ),
            ):
                source(config()["occurrences"][0], "bundle-1")
        response = Mock(status=200)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read1.side_effect = [b"12345"]
        opener.open.side_effect = None
        opener.open.return_value = response
        with (
            patch("replay.preparation.build_opener", return_value=opener),
            patch("replay.preparation.MAX_BYTES", 4),
            self.assertRaisesRegex(ProtocolError, "body budget"),
        ):
            source(config()["occurrences"][0], "bundle-1")


if __name__ == "__main__":
    unittest.main()
