"""Deployment-shape regressions that do not require a Docker daemon."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ComposeArchiveCredentialTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")

    def service(self, name: str) -> str:
        marker = f"\n  {name}:\n"
        start = self.compose.index(marker) + len(marker)
        next_service = re.search(r"\n  [a-z][a-z0-9-]*:\n", self.compose[start:])
        end = start + next_service.start() if next_service else len(self.compose)
        return self.compose[start:end]

    def test_every_s3_facing_service_receives_the_standard_aws_environment(self) -> None:
        self.assertIn("x-aws-archive-environment: &aws-archive-environment", self.compose)
        for variable in (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ):
            self.assertIn(variable, self.compose)
        for service in (
            "archiver",
            "archiver-once",
            "archive-receipt-mirror",
            "reaper",
            "canonical-reaper",
            "canonical-reaper-once",
        ):
            with self.subTest(service=service):
                self.assertIn(
                    "environment: *aws-archive-environment", self.service(service)
                )

    def test_archiver_sweeps_the_canonical_root_as_well_as_the_raw_spool(self) -> None:
        for service in ("archiver", "archiver-once"):
            with self.subTest(service=service):
                configured = self.service(service)
                self.assertIn("--canonical-root", configured)
                self.assertIn("/var/lib/prediction-indexer/canonical", configured)

    def test_canonical_reaper_is_audit_first_with_an_eighteen_hour_floor(self) -> None:
        for service in ("canonical-reaper", "canonical-reaper-once"):
            with self.subTest(service=service):
                configured = self.service(service)
                self.assertIn("archive.reaper.canonical_cli", configured)
                self.assertIn("${CANONICAL_REAPER_MODE:-audit}", configured)
                self.assertIn("${CANONICAL_REAPER_RETENTION_HOURS:-18}", configured)
                self.assertIn("--canonical-root", configured)

    def test_ingest_store_reaper_is_one_shot_audit_first_and_uses_the_rust_image(self) -> None:
        service = self.service("ingest-store-reaper")
        self.assertIn("<<: *ingester-service", service)
        self.assertIn('restart: "no"', service)
        self.assertIn("indexer-store-reap", service)
        self.assertIn("${INGEST_STORE_REAPER_MODE:-audit}", service)
        self.assertNotIn("--interval-seconds", service)


class TargeterV2DeploymentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.override_path = ROOT / "compose.targeter-v2.yaml"

    def test_production_override_is_one_shot_and_switches_every_targeted_splice(self) -> None:
        document = self.override_path.read_text(encoding="utf-8")
        self.assertIn("targeter/run_v2.py", document)
        self.assertIn("--mode", document)
        self.assertIn("publish", document)
        self.assertIn('restart: "no"', document)
        self.assertNotIn("--interval-seconds", document)
        self.assertGreaterEqual(document.count("/live/targeter-v2/current.json"), 8)

    def test_documentation_supplies_a_periodic_one_shot_command_and_audit_gate(self) -> None:
        deployment = (ROOT / "docs" / "TARGETER_V2_PHASES_6_10.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("docker compose", deployment)
        self.assertIn("run --rm targeter", deployment)
        self.assertIn("cron", deployment.casefold())
        self.assertIn("targeter-v2-integrity", deployment)


class EventUniverseDeploymentTests(unittest.TestCase):
    def test_server_has_a_dedicated_default_image_and_no_capture_mount(self) -> None:
        compose = (ROOT / "compose.universe.yaml").read_text(encoding="utf-8")
        dockerfile = (ROOT / "docker" / "universe.Dockerfile").read_text(
            encoding="utf-8"
        )
        shared = (ROOT / "docker" / "python.Dockerfile").read_text(encoding="utf-8")
        self.assertIn("docker/universe.Dockerfile", compose)
        self.assertIn("configs/event_universe.json", compose)
        self.assertIn("EVENT_UNIVERSE_DATA_ROOT", compose)
        self.assertNotIn("CAPTURE_DATA_ROOT", compose)
        self.assertIn('CMD ["python", "-u", "universe/run_server.py"]', dockerfile)
        self.assertNotIn("COPY universe/", shared)
        server = compose.split("  event-universe:", 1)[1].split(
            "  event-universe-sync:", 1
        )[0]
        self.assertNotIn("AWS_ACCESS_KEY_ID", server)
        runtime = compose.split("x-universe-runtime:", 1)[1].split(
            "\nservices:", 1
        )[0]
        self.assertIn("environment: *universe-config-environment", runtime)
        self.assertEqual(
            compose.count("    environment: *universe-job-environment"), 3
        )

    def test_jobs_are_direct_configured_scripts_without_an_argument_parser(self) -> None:
        universe = ROOT / "universe"
        self.assertFalse((universe / "cli.py").exists())
        for name in ("run_server.py", "run_sync.py", "run_backfill.py", "run_backup.py"):
            source = (universe / name).read_text(encoding="utf-8")
            with self.subTest(name=name):
                self.assertIn("load_config()", source)
                self.assertNotIn("argparse", source)
        config = (ROOT / "configs" / "event_universe.json").read_text(
            encoding="utf-8"
        )
        self.assertIn('"event_universe_config_version": 1', config)
        mirror = (ROOT / "archive" / "run_receipt_mirror.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("load_config()", mirror)
        self.assertNotIn("argparse", mirror)
        self.assertTrue((ROOT / "configs" / "archive_receipt_mirror.json").is_file())

    def test_orb_setup_creates_and_installs_the_project_virtual_environment(self) -> None:
        setup = (ROOT / ".agents" / "setup").read_text(encoding="utf-8")
        self.assertIn('python3 -m venv "$REPO_ROOT/.venv"', setup)
        self.assertIn('"$REPO_ROOT/.venv/bin/python" -m pip install -e', setup)


if __name__ == "__main__":
    unittest.main()
