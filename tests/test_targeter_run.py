from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from targeter import run as targeter_run
from targeter.coverage import CoverageLedger, Sighting, created_at_of
from targeter.manifest import ManifestError, load_manifest
from targeter.run import Targeter
from targeter.sources import Discovery, DiscoveryError, KalshiSource, LimitlessSource
from targeter.targets import Target, load_targets


def _manifest_document(**overrides):
    document = {
        "version": 1,
        "entries": [
            {
                "id": "btc",
                "structure_type": "LADDER",
                "discover_every_seconds": 60,
                "venues": {"kalshi": {"series": ["KXBTCD"]}, "limitless": {"underlyings": ["btc"]}},
            },
            {
                "id": "eth",
                "structure_type": "LADDER",
                "discover_every_seconds": 300,
                "venues": {"limitless": {"underlyings": ["eth"]}},
            },
        ],
    }
    document.update(overrides)
    return document


class _FakeSource:
    """Returns a scripted Discovery per selector, keyed by a marker in it."""

    def __init__(self, venue, script):
        self.venue = venue
        self.script = script
        self.calls = []

    def discover(self, selector):
        key = (selector.get("underlyings") or selector.get("series") or ["*"])[0]
        self.calls.append(key)
        return self.script.get(key, Discovery(venue=self.venue))


class ManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.path = self.root / "manifest.json"
        self.addCleanup(self._directory.cleanup)

    def _write(self, document) -> Path:
        self.path.write_text(json.dumps(document))
        return self.path

    def test_loads_entries_and_venues(self) -> None:
        manifest = load_manifest(self._write(_manifest_document()))
        self.assertEqual([e.id for e in manifest.entries], ["btc", "eth"])
        self.assertEqual(set(manifest.venues()), {"kalshi", "limitless"})
        self.assertEqual([e.id for e in manifest.entries_for("kalshi")], ["btc"])

    def test_an_unknown_venue_is_refused_by_name(self) -> None:
        """Otherwise it would simply never be discovered and the operator would
        see an empty targets file with no reason given."""
        document = _manifest_document()
        document["entries"][0]["venues"]["binance"] = {}
        with self.assertRaises(ManifestError) as caught:
            load_manifest(self._write(document))
        self.assertIn("binance", str(caught.exception))

    def test_an_unknown_structure_type_is_refused(self) -> None:
        document = _manifest_document()
        document["entries"][0]["structure_type"] = "SPAGHETTI"
        with self.assertRaises(ManifestError):
            load_manifest(self._write(document))

    def test_duplicate_ids_are_refused(self) -> None:
        document = _manifest_document()
        document["entries"][1]["id"] = "btc"
        with self.assertRaises(ManifestError):
            load_manifest(self._write(document))

    def test_disabled_entries_are_excluded(self) -> None:
        document = _manifest_document()
        document["entries"][1]["enabled"] = False
        manifest = load_manifest(self._write(document))
        self.assertEqual([e.id for e in manifest.active()], ["btc"])

    def test_a_non_positive_cadence_is_refused(self) -> None:
        document = _manifest_document()
        document["entries"][0]["discover_every_seconds"] = 0
        with self.assertRaises(ManifestError):
            load_manifest(self._write(document))


class CycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.live = self.root / "live"
        self.live.mkdir(parents=True)
        path = self.root / "manifest.json"
        path.write_text(json.dumps(_manifest_document()))
        self.manifest = load_manifest(path)
        self.addCleanup(self._directory.cleanup)
        self._original = dict(targeter_run.SOURCES)
        self.addCleanup(lambda: targeter_run.SOURCES.update(self._original))

    def _install(self, **sources):
        targeter_run.SOURCES.clear()
        targeter_run.SOURCES.update(sources)

    def _targeter(self, clock=lambda: 0.0):
        return Targeter(self.manifest, self.live, clock=clock)

    def test_targets_from_several_entries_merge_into_one_venue_file(self) -> None:
        """A splice holds one connection and one subscription set, so merging
        happens here rather than making the splice read several manifests."""
        self._install(
            kalshi=_FakeSource("kalshi", {"KXBTCD": Discovery("kalshi", [Target("K1")])}),
            limitless=_FakeSource("limitless", {
                "btc": Discovery("limitless", [Target("btc-1")]),
                "eth": Discovery("limitless", [Target("eth-1")]),
            }),
        )
        report = self._targeter().run_cycle(only=["btc", "eth"])

        self.assertEqual(report["venues"]["limitless"]["targets"], 2)
        loaded = load_targets(self.live / "targets_limitless.json", venue="limitless")
        self.assertEqual(set(loaded.asset_ids()), {"btc-1", "eth-1"})

    def test_an_asset_reachable_through_two_entries_is_one_subscription(self) -> None:
        """A duplicate would be refused by the targets writer and cost the whole
        file, so it is collapsed here."""
        self._install(
            kalshi=_FakeSource("kalshi", {}),
            limitless=_FakeSource("limitless", {
                "btc": Discovery("limitless", [Target("shared")]),
                "eth": Discovery("limitless", [Target("shared")]),
            }),
        )
        report = self._targeter().run_cycle(only=["btc", "eth"])
        self.assertEqual(report["venues"]["limitless"]["targets"], 1)

    def test_one_failed_entry_leaves_the_whole_venue_file_untouched(self) -> None:
        """Regression, from a real cycle: Limitless rate-limited the btc entry,
        the eth entry then succeeded, and the targets file was rewritten without
        btc. A partial set unsubscribes live markets for a reason unrelated to
        the venue's listings, and the hole is indistinguishable from a quiet
        market. Stale-but-complete beats fresh-but-truncated.
        """
        self._install(
            kalshi=_FakeSource("kalshi", {}),
            limitless=_FakeSource("limitless", {
                "btc": Discovery("limitless", [Target("btc-1")]),
                "eth": Discovery("limitless", [Target("eth-1")]),
            }),
        )
        good = self._targeter()
        good.run_cycle(only=["btc", "eth"])
        before = load_targets(self.live / "targets_limitless.json", venue="limitless").digest

        self._install(
            kalshi=_FakeSource("kalshi", {}),
            limitless=_FakeSource("limitless", {
                "btc": Discovery("limitless", error="HTTP 429"),
                "eth": Discovery("limitless", [Target("eth-1")]),
            }),
        )
        report = self._targeter().run_cycle(only=["btc", "eth"])

        self.assertEqual(report["venues"]["limitless"]["targets_file"], "unchanged")
        self.assertEqual(report["venues"]["limitless"]["failures"][0]["entry"], "btc")
        self.assertEqual(report["venues"]["limitless"]["would_have_written"], 1)
        after = load_targets(self.live / "targets_limitless.json", venue="limitless").digest
        self.assertEqual(before, after, "a truncated set must never replace a complete one")

    def test_a_raising_source_does_not_stop_the_other_venues(self) -> None:
        class Exploding:
            venue = "limitless"

            def discover(self, selector):
                raise RuntimeError("boom")

        self._install(
            kalshi=_FakeSource("kalshi", {"KXBTCD": Discovery("kalshi", [Target("K1")])}),
            limitless=Exploding(),
        )
        report = self._targeter().run_cycle(only=["btc", "eth"])
        self.assertEqual(report["venues"]["kalshi"]["targets"], 1)
        self.assertEqual(report["venues"]["limitless"]["targets_file"], "unchanged")

    def test_rejections_are_recorded_with_the_entry_that_produced_them(self) -> None:
        """A selector bug looks exactly like a venue not listing the market."""
        self._install(
            kalshi=_FakeSource("kalshi", {}),
            limitless=_FakeSource("limitless", {
                "btc": Discovery("limitless", [Target("btc-1")],
                                 rejections=[{"slug": "x", "reason": "wrong horizon"}]),
                "eth": Discovery("limitless", [Target("eth-1")]),
            }),
        )
        self._targeter().run_cycle(only=["btc", "eth"])
        written = json.loads((self.live / "rejected_limitless.json").read_text())
        self.assertEqual(written["rejected"][0]["entry"], "btc")
        self.assertEqual(written["rejected"][0]["reason"], "wrong horizon")

    def test_a_venue_is_not_written_until_every_entry_has_discovered_once(self) -> None:
        """On the first cycle an undiscovered entry is indistinguishable from a
        failed one, and either way the set would be incomplete."""
        self._install(
            kalshi=_FakeSource("kalshi", {}),
            limitless=_FakeSource("limitless", {"btc": Discovery("limitless", [Target("btc-1")])}),
        )
        report = self._targeter().run_cycle(only=["btc"])
        self.assertEqual(report["venues"]["limitless"]["targets_file"], "unchanged")
        self.assertFalse((self.live / "targets_limitless.json").exists())

    def test_an_entry_not_due_this_cycle_keeps_its_markets_subscribed(self) -> None:
        """Regression, from a real loop run: at t=60 only the 60-second entries
        were due and the Kalshi file was rewritten without the 120-second entry's
        ladder — silently unsubscribing 300 live markets until it next came round.

        A venue's file is the union of every active entry, due or not. A cached
        result is slightly stale for an entry that was not rediscovered, which is
        exactly what its cadence declares acceptable.
        """
        now = [0.0]
        self._install(
            kalshi=_FakeSource("kalshi", {}),
            limitless=_FakeSource("limitless", {
                "btc": Discovery("limitless", [Target("btc-1")]),
                "eth": Discovery("limitless", [Target("eth-1")]),
            }),
        )
        targeter = self._targeter(clock=lambda: now[0])
        targeter.run_cycle(only=["btc", "eth"])

        now[0] = 61.0            # btc due (60s), eth not (300s)
        self.assertEqual(targeter.due_entries(now[0]), ["btc"])
        report = targeter.run_cycle(only=["btc"])

        self.assertEqual(report["venues"]["limitless"]["targets"], 2)
        self.assertFalse(report["venues"]["limitless"]["changed"])
        loaded = load_targets(self.live / "targets_limitless.json", venue="limitless")
        self.assertEqual(set(loaded.asset_ids()), {"btc-1", "eth-1"})

    def test_creation_times_reach_the_coverage_ledger(self) -> None:
        """Discovery lag is only a measured number if the venue's creation time
        actually travels from the source to the ledger."""
        self._install(
            kalshi=_FakeSource("kalshi", {}),
            limitless=_FakeSource("limitless", {
                "btc": Discovery("limitless", [Target("btc-1")],
                                 created_at={"btc-1": "2026-07-29T00:00:00+00:00"}),
                "eth": Discovery("limitless", [Target("eth-1")]),
            }),
        )
        targeter = self._targeter()
        targeter.run_cycle(only=["btc", "eth"])
        summary = targeter.coverage.summary("limitless")
        self.assertEqual(summary["with_created_at"], 1)
        self.assertEqual(summary["without_created_at"], 1)

    def test_cadence_decides_which_entries_are_due(self) -> None:
        """A five-minute ladder can be rediscovered every 30s while a daily one is
        checked hourly, without either paying the other's request cost."""
        now = [0.0]
        self._install(
            kalshi=_FakeSource("kalshi", {"KXBTCD": Discovery("kalshi", [Target("K1")])}),
            limitless=_FakeSource("limitless", {}),
        )
        targeter = self._targeter(clock=lambda: now[0])

        self.assertEqual(set(targeter.due_entries(now[0])), {"btc", "eth"})
        targeter.run_cycle(only=["btc", "eth"])

        now[0] = 61.0   # btc cadence 60s elapsed, eth 300s has not
        self.assertEqual(targeter.due_entries(now[0]), ["btc"])
        now[0] = 301.0
        self.assertEqual(set(targeter.due_entries(now[0])), {"btc", "eth"})

    def test_an_unchanged_set_is_reported_as_unchanged(self) -> None:
        """The digest ignores ordering and annotation, so a cycle that finds the
        same markets must not force a reconnect and a book resync."""
        self._install(
            kalshi=_FakeSource("kalshi", {"KXBTCD": Discovery("kalshi", [Target("K1")])}),
            limitless=_FakeSource("limitless", {}),
        )
        targeter = self._targeter()
        first = targeter.run_cycle(only=["btc"])
        second = targeter.run_cycle(only=["btc"])
        self.assertTrue(first["venues"]["kalshi"]["changed"])
        self.assertFalse(second["venues"]["kalshi"]["changed"])


class SourceFailureTests(unittest.TestCase):
    def test_a_failed_kalshi_series_is_an_incomplete_discovery(self) -> None:
        """A catalogue failure must leave the venue file untouched, not replace
        one ladder with an empty successful result."""
        source = KalshiSource()
        with patch.object(
            source, "_open_markets", side_effect=DiscoveryError("HTTP 503")
        ):
            found = source.discover({"series": ["KXBTCD"]})

        self.assertEqual(found.error, "HTTP 503")

    def test_a_failed_later_limitless_page_is_an_incomplete_discovery(self) -> None:
        """One successful page followed by a failed page is still partial."""
        source = LimitlessSource()
        first_page = {
            "data": [
                {
                    "slug": "btc-up-or-down-5-min-1",
                    "tradeType": "clob",
                    "id": 1,
                }
            ]
        }
        with patch(
            "targeter.sources.get_json",
            side_effect=[first_page, DiscoveryError("HTTP 429")],
        ):
            found = source.discover(
                {
                    "pages": 2,
                    "horizons": ["5min"],
                    "underlyings": ["btc"],
                }
            )

        self.assertEqual(found.error, "HTTP 429")


class CoverageTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.path = Path(self._directory.name) / "coverage.json"
        self.addCleanup(self._directory.cleanup)

    def test_first_sighting_is_never_overwritten(self) -> None:
        """Overwriting would quietly improve every latency number on every rerun —
        a self-flattering metric that survives review by being unfalsifiable."""
        ledger = CoverageLedger(self.path)
        self.assertEqual(len(ledger.observe("kalshi", ["A"])), 1)
        self.assertEqual(ledger.observe("kalshi", ["A"]), [])
        first = ledger._sightings[("kalshi", "A")].first_seen_at

        ledger.save()
        reloaded = CoverageLedger(self.path)
        self.assertEqual(reloaded.observe("kalshi", ["A"]), [])
        self.assertEqual(reloaded._sightings[("kalshi", "A")].first_seen_at, first)

    def test_lag_is_the_gap_between_creation_and_first_sight(self) -> None:
        sighting = Sighting("A", "kalshi", "2026-07-29T00:05:00+00:00", "2026-07-29T00:00:00+00:00")
        self.assertEqual(sighting.discovery_lag_seconds(), 300.0)

    def test_unmeasurable_markets_are_counted_not_dropped(self) -> None:
        """Averaging over only the measurable ones would understate the lag
        exactly where it is least known."""
        ledger = CoverageLedger(self.path)
        ledger.observe("kalshi", ["A"], created_at={"A": "2026-07-29T00:00:00+00:00"})
        ledger.observe("kalshi", ["B"])
        summary = ledger.summary()
        self.assertEqual(summary["markets"], 2)
        self.assertEqual(summary["with_created_at"], 1)
        self.assertEqual(summary["without_created_at"], 1)

    def test_a_corrupt_ledger_does_not_stop_capture(self) -> None:
        self.path.write_text("{ not json")
        self.assertEqual(len(CoverageLedger(self.path)), 0)

    def test_created_at_reads_the_venue_spellings(self) -> None:
        self.assertIsNotNone(created_at_of({"createdAt": "2026-07-29T00:00:00Z"}))
        self.assertIsNotNone(created_at_of({"created_at": "2026-07-29T00:00:00Z"}))
        self.assertIsNone(created_at_of({"nothing": 1}), "a missing time must stay missing")

    def test_epoch_seconds_and_milliseconds_are_distinguished(self) -> None:
        seconds = created_at_of({"open_time": 1785312000})
        millis = created_at_of({"open_time": 1785312000000})
        self.assertEqual(seconds, millis)


if __name__ == "__main__":
    unittest.main()
