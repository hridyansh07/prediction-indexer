"""Offline, minimal public-contract shapes; transports never use the network."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import pull_kalshi_game_state as pull
from archive.storage.local import LocalObjectStore
from archive.storage.gcs import GCSObjectStore
from tests.test_gcsstore import FakeClient


def setUpModule():
    unittest.enterModuleContext(
        patch(
            "socket.create_connection",
            side_effect=AssertionError("offline test attempted network"),
        )
    )


def milestone(tickers=("SERIES", "MAP1")):
    return {
        "id": "match-id",
        "type": "esports_match",
        "start_date": "2026-09-27T12:00:00Z",
        "end_date": None,
        "related_event_tickers": list(tickers),
        "primary_event_tickers": ["SERIES"],
        "details": {
            "game": "cs2",
            "status": "finished",
            "home_competitor_id": "h",
            "away_competitor_id": "a",
            "main_game_event_ticker": "SERIES",
        },
    }


class Fake:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.urls = []

    def __call__(self, url):
        self.urls.append(url)
        reply = next(self.replies)
        if isinstance(reply, Exception):
            raise reply
        if isinstance(reply, dict):
            return 200, {"Content-Type": "application/json"}, json.dumps(reply).encode()
        return reply


def client(replies):
    return pull.Client(Fake(replies), sleep=lambda _: None, records=[])


class MappingTests(unittest.TestCase):
    def test_mapping_reasons(self):
        cases = [
            ([], [], "no_kalshi_events"),
            (["SERIES"], [{"milestones": []}], "no_milestone"),
            (
                ["SERIES"],
                [{"milestones": [milestone(), milestone()]}],
                "multiple_milestones",
            ),
            (["OTHER"], [{"milestones": [milestone()]}], "ticker_not_related"),
            (["SERIES"], [(400, {}, b"bad")], "fetch_failed"),
        ]
        for tickers, replies, reason in cases:
            with self.subTest(reason=reason):
                self.assertEqual(pull.map_bundle(client(replies), tickers)[1], reason)

    def test_exact_same_milestone_and_type(self):
        m = milestone()
        self.assertEqual(
            pull.map_bundle(client([{"milestones": [m]}] * 2), ["MAP1", "SERIES"]),
            (m, None),
        )
        other = dict(m, id="different")
        self.assertEqual(
            pull.map_bundle(
                client([{"milestones": [m]}, {"milestones": [other]}]),
                ["MAP1", "SERIES"],
            )[1],
            "multiple_milestones",
        )
        self.assertEqual(
            pull.map_bundle(
                client([{"milestones": [dict(m, type="sport")]}]), ["SERIES"]
            )[1],
            "no_milestone",
        )


class HttpTests(unittest.TestCase):
    def test_retry_and_exact_bytes(self):
        c = client([(429, {"Retry-After": "2"}, b"wait"), {"ok": True}])
        self.assertEqual(c.get("https://example.test/x"), {"ok": True})
        self.assertEqual(c.retries, 1)
        self.assertEqual([r["status"] for r in c.records], [429, 200])
        self.assertEqual(pull.body_bytes(c.records[0]), b"wait")

    def test_exhaustion(self):
        c = client([TimeoutError()] * 5)
        with self.assertRaises(pull.FetchError):
            c.get("https://example.test/x")
        self.assertEqual(len(c.records), 5)
        self.assertEqual(c.retries, 4)

    def test_non_utf8_and_closed_record(self):
        c = client([(200, {}, b"\xff\x00")])
        with self.assertRaises(pull.FetchError):
            c.get("https://example.test/x")
        r = c.records[0]
        self.assertEqual(r["error"], "non_utf8_body")
        self.assertEqual(pull.body_bytes(r), b"\xff\x00")
        with self.assertRaises(ValueError):
            pull.body_bytes(dict(r, body="also"))

    def test_pagination_loop(self):
        c = client([{"selections": [], "next_cursor": "same"}] * 2)
        with self.assertRaises(pull.FetchError):
            list(pull.pages(c, "https://example.test", "/v1/selections"))

    def test_retry_after_and_global_rate(self):
        waits = []
        c = pull.Client(
            Fake([(503, {"Retry-After": "3"}, b"wait"), {"ok": True}]),
            sleep=waits.append,
            monotonic=lambda: 0,
            records=[],
        )
        c.get("https://example.test")
        self.assertEqual(waits, [0, 3, 0.2])
        c = client([(429, {"Retry-After": "999"}, b"wait")])
        with self.assertRaisesRegex(pull.FetchError, "retry_after_exceeds_budget"):
            c.get("https://example.test")
        self.assertEqual(c.retries, 0)

    def test_page_cursor_success_and_bounds(self):
        c = client(
            [
                {"selections": [{"run_id": "r1"}], "next_cursor": "c1"},
                {"selections": [{"run_id": "r2"}], "next_cursor": None},
            ]
        )
        self.assertEqual(
            [
                x["run_id"]
                for x in pull.pages(c, "https://example.test", "/v1/selections")
            ],
            ["r1", "r2"],
        )
        self.assertIn("cursor=c1", c.send.urls[1])
        with patch.object(pull, "MAX_PAGES", 1):
            with self.assertRaisesRegex(pull.FetchError, "page_limit"):
                list(
                    pull.pages(
                        client([{"selections": [], "next_cursor": "c"}]),
                        "https://example.test",
                        "/v1/selections",
                    )
                )
        for bad in (
            {"selections": [], "next_cursor": None, "unknown": 1},
            {"selections": [{}] * 51, "next_cursor": None},
        ):
            with self.assertRaises(pull.FetchError):
                list(
                    pull.pages(client([bad]), "https://example.test", "/v1/selections")
                )

    def test_bounds_and_strict_json(self):
        with patch.object(pull, "MAX_BODY", 2):
            c = client([(200, {}, b"long")])
            with self.assertRaisesRegex(pull.FetchError, "body_too_large"):
                c.get("https://example.test")
            self.assertEqual(len(c.records), 1)
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b"[]"):
            with self.assertRaisesRegex(pull.FetchError, "invalid_json"):
                client([(200, {}, raw)]).get("https://example.test")
        for bad in ("../bad", "a/b", "a%2Fb", ""):
            with self.assertRaises(ValueError):
                pull.identifier(bad)
        self.assertEqual(
            pull.timestamp("2026-09-27T18:30:00.123456789+05:30"), 1790514000123456789
        )

    def test_disk_journal(self):
        c = pull.Client(Fake([{"a": 1}, {"a": 2}]), sleep=lambda _: None)
        try:
            c.get("https://example.test/1")
            c.get("https://example.test/2")
            self.assertEqual([r["seq"] for r in c.records], [0, 1])
            self.assertEqual(pull.body_bytes(c.records[1]), b'{"a": 2}')
        finally:
            c.records.close()

    def test_closed_record_rejects_unknown_fields_types_and_wrong_hash(self):
        c = client([{"ok": True}])
        c.get("https://example.test")
        r = c.records[0]
        for updates in (
            {"extra": 1},
            {"record_version": True},
            {"seq": True},
            {"body_sha256": "0" * 64},
            {"status": 700},
            {"content_type": "x" * 1025},
        ):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                pull.body_bytes(dict(r, **updates))

    def test_retry_http_date_and_invalid_header(self):
        for header, expected in (("Thu, 01 Jan 1970 00:00:04 GMT", 4), ("invalid", 1)):
            waits = []
            c = pull.Client(
                Fake([(429, {"Retry-After": header}, b"wait"), {"ok": True}]),
                sleep=waits.append,
                clock=lambda: 0,
                monotonic=lambda: 0,
                records=[],
            )
            c.get("https://example.test")
            self.assertEqual(waits[1], expected)


def records(game="cs2", *, result="yes", extra_period=False):
    m = milestone()
    m["details"]["game"] = game
    live = {
        "home_score": 1,
        "away_score": 0,
        "home_periods": {"period_1": 13},
        "away_periods": {"period_1": 7},
        "home_stats": [
            {
                "period": "period_1",
                "stats": {"map_duration_seconds": 1234, "map_forfeit": 0, "winner": 1},
            }
        ],
        "away_stats": [
            {
                "period": "period_1",
                "stats": {"map_duration_seconds": 1234, "map_forfeit": 0, "winner": 0},
            }
        ],
    }
    if game in ("lol", "dota2"):
        live["home_stats"][0]["stats"].update(kills=27, towers=8)
        live["away_stats"][0]["stats"].update(kills=12, towers=3)
    if extra_period:
        for side in ("home", "away"):
            live[side + "_stats"].append(
                {"period": "period_2", "stats": dict(live[side + "_stats"][0]["stats"])}
            )
            live[side + "_periods"]["period_2"] = 5
    market = {
        "ticker": "MAP1-H",
        "yes_sub_title": "Home",
        "result": result,
        "custom_strike": {"esports_competitor": "h"},
        "close_time": "2026-09-27T13:00:00Z",
        "settlement_ts": "2026-09-27T13:02:00Z",
    }
    replies = [
        {"milestones": [m]},
        {"live_data": {"milestone_id": m["id"], "details": live}},
        {
            "event": {
                "event_ticker": "MAP1",
                "product_metadata": {"competition_scope": "Map 1 Winner"},
                "markets": [market],
            }
        },
        {
            "event": {
                "event_ticker": "SERIES",
                "markets": [dict(market, ticker="SERIES-H")],
            }
        },
    ]
    urls = [
        pull.KALSHI + "/milestones?limit=10&related_event_ticker=MAP1",
        pull.KALSHI + "/live_data/milestone/match-id",
        pull.KALSHI + "/events/MAP1?with_nested_markets=true",
        pull.KALSHI + "/events/SERIES?with_nested_markets=true",
    ]
    c = client(replies)
    for url in urls:
        c.get(url)
    return m, c.records


class ArchiveTests(unittest.TestCase):
    def test_exact_roundtrip_skip_refetch_and_regeneration(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            m, rows = records()
            c = client(
                [(200, {}, b"\xff\x00"), (200, {}, b'{ "x": "\xe2\x98\x83" }\n')]
            )
            for _ in range(2):
                try:
                    c.get("https://example.test/x")
                except pull.FetchError:
                    pass
            rows += c.records
            prefix = pull.archive_fetch(
                store,
                m,
                ["bundle_b", "bundle_a"],
                rows,
                1_800_000_000_123456000,
                "complete",
            )
            receipt, restored = pull.read_records(store, prefix)
            restored = list(restored)
            self.assertEqual(receipt["bundle_ids"], ["bundle_a", "bundle_b"])
            self.assertEqual(pull.body_bytes(restored[-2]), b"\xff\x00")
            self.assertEqual(
                pull.body_bytes(restored[-1]), b'{ "x": "\xe2\x98\x83" }\n'
            )
            self.assertTrue(pull.existing_complete(store, m["id"]))
            with self.assertRaises(ValueError):
                pull.archive_fetch(
                    store, m, ["bundle_a"], rows, 1_800_000_000_123456000, "complete"
                )
            with patch.object(pull, "transport", side_effect=AssertionError("network")):
                regenerated = pull.regenerate(
                    store, prefix, Path(root) / "rebuilt.json"
                )
            self.assertEqual(
                regenerated["maps"][0]["derived_start_ns"], 1790512766000000000
            )
            self.assertEqual(
                json.loads((Path(root) / "rebuilt.json").read_text()), regenerated
            )

    def test_receipt_last_and_provider_failure(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            m, rows = records()
            calls = []
            original = store.put_immutable

            def put(key, *args, **kwargs):
                calls.append(key.rsplit("/", 1)[-1])
                return original(key, *args, **kwargs)

            with patch.object(store, "put_immutable", side_effect=put):
                prefix = pull.archive_fetch(
                    store, m, ["bundle"], rows, 1_800_000_000_000000000, "complete"
                )
                pull.regenerate(store, prefix)
            self.assertEqual(
                calls, ["responses.ndjson.zst", "receipt.json", "timeline.json"]
            )
            with patch.object(
                store, "put_immutable", side_effect=pull.ObjectStoreError("offline")
            ):
                with self.assertRaises(pull.ObjectStoreError):
                    pull.archive_fetch(
                        store, m, ["bundle"], rows, 1_800_000_001_000000000, "complete"
                    )
            self.assertEqual(
                len(
                    [
                        k
                        for k in store.list_keys("gamestate/")
                        if k.endswith("receipt.json")
                    ]
                ),
                1,
            )

    def test_malformed_receipt_not_skipped(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            prefix = "gamestate/source=kalshi/date=2026-09-27/milestone=match-id/fetch=20260101T000000.000000Z"
            pull.put_json(store, prefix + "/receipt.json", {"status": "complete"})
            self.assertFalse(pull.existing_complete(store, "match-id"))

    def test_record_bound_includes_long_url_and_escaped_body(self):
        with tempfile.TemporaryDirectory() as root, patch.object(pull, "MAX_BODY", 32):
            store = LocalObjectStore(Path(root))
            c = client([(400, {}, b"\x00" * 32)])
            with self.assertRaises(pull.FetchError):
                c.get("https://example.test/" + "x" * 6000)
            prefix = pull.archive_fetch(
                store,
                milestone(),
                ["bundle"],
                c.records,
                1_800_000_000_000000000,
                "incomplete",
            )
            _, rows = pull.read_records(store, prefix)
            self.assertEqual(pull.body_bytes(next(rows)), b"\x00" * 32)
            self.assertEqual(list(rows), [])

    def test_closed_receipt_unknown_nested_fields_and_bounds(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            m, rows = records()
            prefix = pull.archive_fetch(
                store, m, ["bundle"], rows, 1_800_000_000_000000000, "complete"
            )
            receipt_path = Path(root) / prefix / "receipt.json"
            original = json.loads(receipt_path.read_text())
            mutations = [
                dict(original, extra=1),
                dict(original, script_version=True),
                dict(original, request_count=True),
                dict(original, logical=dict(original["logical"], extra=1)),
                dict(
                    original,
                    logical=dict(original["logical"], byte_length=pull.MAX_RAW + 1),
                ),
            ]
            for mutation in mutations:
                receipt_path.write_text(json.dumps(mutation))
                self.assertFalse(pull.existing_complete(store, "match-id"))

    def test_invalid_calendar_prefix_is_rejected(self):
        with self.assertRaises(ValueError):
            pull.prefix_valid(
                "gamestate/source=kalshi/date=2026-99-27/milestone=match-id/fetch=20261007T123000.000000Z"
            )

    def test_provider_neutral_gcs_listing_skip_and_regeneration(self):
        store = GCSObjectStore("test-only", client=FakeClient())
        m, rows = records()
        prefix = pull.archive_fetch(
            store, m, ["bundle"], rows, 1_800_000_000_000000000, "complete"
        )
        self.assertTrue(pull.existing_complete(store, "match-id"))
        self.assertEqual(
            pull.regenerate(store, prefix)["maps"][0]["winner_market"]["ticker"],
            "MAP1-H",
        )
        with patch.object(
            store, "list_keys", side_effect=pull.ObjectStoreError("list failed")
        ):
            with self.assertRaises(pull.ObjectStoreError):
                pull.existing_complete(store, "match-id")

    def test_receipt_failure_leaves_only_uncommitted_content(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            m, rows = records()
            original = store.put_immutable

            def put(key, *args, **kwargs):
                if key.endswith("receipt.json"):
                    raise pull.ObjectStoreError("receipt put failed")
                return original(key, *args, **kwargs)

            with patch.object(store, "put_immutable", side_effect=put):
                with self.assertRaises(pull.ObjectStoreError):
                    pull.archive_fetch(
                        store, m, ["bundle"], rows, 1_800_000_000_000000000, "complete"
                    )
            self.assertFalse(pull.existing_complete(store, "match-id"))
            self.assertEqual(
                [k.rsplit("/", 1)[-1] for k in store.list_keys("gamestate/")],
                ["responses.ndjson.zst"],
            )

    def test_tampered_frame_and_receipt_bounds(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            m, rows = records()
            prefix = pull.archive_fetch(
                store, m, ["bundle"], rows, 1_800_000_000_000000000, "complete"
            )
            path = Path(root) / prefix / "responses.ndjson.zst"
            path.write_bytes(path.read_bytes() + b"trailing")
            with self.assertRaises(pull.ObjectStoreError):
                pull.regenerate(store, prefix)
            self.assertFalse(pull.existing_complete(store, "match-id"))


def run_replies(bundles, *, exhausted=False, malformed=False):
    m, rows = records()
    responses = []
    for bundle in bundles:
        responses += [
            {
                "selections": [{"run_id": "run", "bundle_id": bundle}],
                "next_cursor": None,
            },
            {
                "bundle_id": bundle,
                "run_id": "run",
                "context": {"event_refs": ["kalshi:SERIES"]},
            },
            {"milestones": [m]},
        ]
    if exhausted:
        responses += [(503, {}, b"unavailable")] * 5
    else:
        responses.append(
            {"live_data": []} if malformed else pull.loads(pull.body_bytes(rows[1]))
        )
    responses += [
        pull.loads(pull.body_bytes(rows[2])),
        pull.loads(pull.body_bytes(rows[3])),
    ]
    return responses


class RunTests(unittest.TestCase):
    def test_shared_milestone_skip_and_refetch(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            bundles = ["bundle_a", "bundle_b"]
            c = client(run_replies(bundles))
            report = pull.run(c, store, "https://example.test", bundles)
            self.assertEqual(report["bundles_mapped"], 2)
            self.assertEqual(report["milestones_fetched"], 1)
            self.assertEqual(report["requests"], 9)
            prefix = report["fetches"][0]["prefix"]
            self.assertEqual(pull.read_receipt(store, prefix)["bundle_ids"], bundles)
            c = client(run_replies(bundles)[:-3])
            self.assertEqual(
                pull.run(c, store, "https://example.test", bundles)[
                    "milestones_skipped"
                ],
                1,
            )
            c = client(run_replies(bundles))
            refetch = pull.run(
                c, store, "https://example.test", bundles, skip_existing=False
            )
            self.assertNotEqual(refetch["fetches"][0]["prefix"], prefix)

    def test_exhausted_and_malformed_fetch_commits_incomplete(self):
        for options in ({"exhausted": True}, {"malformed": True}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as root:
                store = LocalObjectStore(Path(root))
                c = client(run_replies(["bundle"], **options))
                report = pull.run(c, store, "https://example.test", ["bundle"])
                self.assertEqual(report["milestones_incomplete"], 1)
                self.assertEqual(report["failures"], [])
                prefix = report["fetches"][0]["prefix"]
                self.assertEqual(
                    pull.read_receipt(store, prefix)["status"], "incomplete"
                )
                self.assertFalse(pull.existing_complete(store, "match-id"))

    def test_bo1_without_period_scores_is_complete_but_no_map_winner_guessed(self):
        replies = run_replies(["bundle"])
        live = replies[3]["live_data"]["details"]
        del live["home_periods"], live["away_periods"]
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            report = pull.run(
                client(replies), store, "https://example.test", ["bundle"]
            )
            self.assertEqual(report["milestones_incomplete"], 0)
            timeline = pull.regenerate(store, report["fetches"][0]["prefix"])
            self.assertEqual(len(timeline["maps"]), 1)
            self.assertIsNone(timeline["maps"][0]["scores"]["home"]["value"])

    def test_every_history_context_is_unioned(self):
        c = client(
            [
                {"selections": [{"run_id": "r1"}], "next_cursor": "c"},
                {"selections": [{"run_id": "r2"}], "next_cursor": None},
                {
                    "run_id": "r1",
                    "bundle_id": "b",
                    "context": {"event_refs": ["kalshi:SERIES", "polymarket:x"]},
                },
                {
                    "run_id": "r2",
                    "bundle_id": "b",
                    "context": {"event_refs": ["kalshi:MAP1", "kalshi:SERIES"]},
                },
            ]
        )
        self.assertEqual(
            pull.bundle_tickers(c, "https://example.test", "b"), ["MAP1", "SERIES"]
        )
        self.assertEqual(len(c.send.urls), 4)

    def test_timeline_provider_failure_retains_committed_receipt_and_counts(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root))
            original = store.put_immutable

            def put(key, *args, **kwargs):
                if key.endswith("timeline.json"):
                    raise pull.ObjectStoreError("timeline failed")
                return original(key, *args, **kwargs)

            with patch.object(store, "put_immutable", side_effect=put):
                report = pull.run(
                    client(run_replies(["bundle"])),
                    store,
                    "https://example.test",
                    ["bundle"],
                )
            self.assertEqual(report["milestones_fetched"], 1)
            self.assertEqual(report["failures"][0]["reason"], "timeline_failed")
            self.assertTrue(pull.existing_complete(store, "match-id"))


class CliTests(unittest.TestCase):
    def test_activation_inputs_and_atomic_report(self):
        with tempfile.TemporaryDirectory() as root:
            c = pull.Client(
                Fake(
                    [{"selections": [{"bundle_id": "bundle"}], "next_cursor": None}]
                    + run_replies(["bundle"])
                ),
                sleep=lambda _: None,
            )
            with (
                patch.object(pull, "Client", return_value=c),
                patch.dict(
                    "os.environ",
                    {"UNIVERSE_BASE_URL": "https://example.test"},
                    clear=True,
                ),
            ):
                self.assertEqual(
                    pull.main(
                        [
                            "--activation-start",
                            "2026-09-27T00:00:00Z",
                            "--activation-end",
                            "2026-09-28T00:00:00Z",
                            "--output-root",
                            root,
                        ]
                    ),
                    0,
                )
            report = json.loads((Path(root) / "report.json").read_text())
            self.assertEqual(report["requests"], 7)
            self.assertIn("activation_start=", report["request_attempts"][0]["url"])
            self.assertEqual(list(Path(root).glob("*.open")), [])

    def test_discovery_failure_still_reports_attempts(self):
        with tempfile.TemporaryDirectory() as root:
            c = pull.Client(Fake([(404, {}, b"missing")]), sleep=lambda _: None)
            with (
                patch.object(pull, "Client", return_value=c),
                patch.dict(
                    "os.environ",
                    {"UNIVERSE_BASE_URL": "https://example.test"},
                    clear=True,
                ),
            ):
                self.assertEqual(
                    pull.main(
                        [
                            "--activation-start",
                            "2026-09-27T00:00:00Z",
                            "--activation-end",
                            "2026-09-28T00:00:00Z",
                            "--output-root",
                            root,
                        ]
                    ),
                    1,
                )
            report = json.loads((Path(root) / "report.json").read_text())
            self.assertEqual(report["requests"], 1)
            self.assertEqual(
                report["failures"][0]["reason"], "selection_discovery_failed"
            )

    def test_offline_regeneration_cli(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalObjectStore(Path(root) / "archive")
            m, rows = records()
            prefix = pull.archive_fetch(
                store, m, ["bundle"], rows, 1_800_000_000_000000000, "complete"
            )
            output = Path(root) / "rebuilt.json"
            with patch.dict("os.environ", {}, clear=True):
                self.assertEqual(
                    pull.main(
                        [
                            "--regenerate",
                            prefix,
                            "--output-root",
                            root,
                            "--timeline-output",
                            str(output),
                        ]
                    ),
                    0,
                )
            self.assertEqual(
                json.loads(output.read_text())["maps"][0]["winner_market"]["ticker"],
                "MAP1-H",
            )


class DerivationTests(unittest.TestCase):
    def test_games_asymmetric_duration_and_objectives(self):
        for game in ("cs2", "lol", "dota2", "valorant"):
            with self.subTest(game=game):
                _, rows = records(game)
                timeline = pull.derive(
                    {"milestone_id": "match-id", "bundle_ids": ["bundle"]}, rows
                )
                one = timeline["maps"][0]
                self.assertEqual(one["winner_market"]["ticker"], "MAP1-H")
                self.assertEqual(one["derived_start_ns"], 1790512766000000000)
                self.assertEqual(
                    one["time_basis"],
                    {"end": "kalshi_market_close", "start": "close_minus_duration"},
                )
                self.assertEqual(one["scores"]["home"]["value"], 13)
                if game in ("lol", "dota2"):
                    self.assertEqual(one["scores"]["away_stats"]["value"]["kills"], 12)

    def test_unsettled_and_count_difference(self):
        _, rows = records(result="", extra_period=True)
        t = pull.derive({"milestone_id": "match-id", "bundle_ids": ["bundle"]}, rows)
        codes = {i["code"] for i in t["inconsistencies"]}
        self.assertIn("unsettled_map_market", codes)
        self.assertIn("map_count_difference", codes)
        self.assertIsNone(t["maps"][0]["winner_market"])

    def test_winner_disagreement_and_duration_disagreement(self):
        _, rows = records()
        rows = list(rows)
        live = pull.loads(pull.body_bytes(rows[1]))
        live["live_data"]["details"]["home_stats"][0]["stats"]["winner"] = 0
        live["live_data"]["details"]["away_stats"][0]["stats"].update(
            winner=1, map_duration_seconds=1400
        )
        c = client([live])
        c.get(rows[1]["url"])
        rows[1] = c.records[0]
        t = pull.derive({"milestone_id": "match-id", "bundle_ids": ["bundle"]}, rows)
        self.assertIn("winner_disagreement", {i["code"] for i in t["inconsistencies"]})
        self.assertIn(
            "duration_disagreement", {i["code"] for i in t["inconsistencies"]}
        )
        self.assertEqual(t["maps"][0]["winner_market"]["ticker"], "MAP1-H")
        self.assertIsNone(t["maps"][0]["derived_start_ns"])

    def test_partly_unsettled_market_is_visible_even_with_yes(self):
        _, rows = records()
        doc = pull.loads(pull.body_bytes(rows[2]))
        market = doc["event"]["markets"][0]
        doc["event"]["markets"].append(dict(market, ticker="MAP1-A", result=""))
        c = client([doc])
        c.get(rows[2]["url"])
        rows[2] = c.records[0]
        timeline = pull.derive(
            {"milestone_id": "match-id", "bundle_ids": ["bundle"]}, rows
        )
        self.assertIn(
            "unsettled_map_market", {i["code"] for i in timeline["inconsistencies"]}
        )

    def test_missing_bundle_map_ref_is_not_an_inconsistency(self):
        _, rows = records()
        timeline = pull.derive(
            {"milestone_id": "match-id", "bundle_ids": ["only_series_captured"]}, rows
        )
        self.assertEqual(timeline["inconsistencies"], [])

    def test_duplicate_map_identity_does_not_choose_first_event(self):
        _, rows = records()
        mapping = pull.loads(pull.body_bytes(rows[0]))
        mapping["milestones"][0]["related_event_tickers"].append("OTHER-MAP1")
        doc = pull.loads(pull.body_bytes(rows[2]))
        doc["event"]["event_ticker"] = "OTHER-MAP1"
        doc["event"]["markets"][0].update(ticker="OTHER-MAP1-A", yes_sub_title="Away")
        c = client([mapping, doc])
        c.get(rows[0]["url"])
        rows[0] = c.records[0]
        c.get(pull.KALSHI + "/events/OTHER-MAP1?with_nested_markets=true")
        timeline = pull.derive(
            {"milestone_id": "match-id", "bundle_ids": ["bundle"]}, rows + c.records[1:]
        )
        self.assertIsNone(timeline["maps"][0]["winner_market"])
        self.assertIsNone(timeline["maps"][0]["derived_start_ns"])
        self.assertIn(
            "map_event_shape", {i["code"] for i in timeline["inconsistencies"]}
        )


if __name__ == "__main__":
    unittest.main()
