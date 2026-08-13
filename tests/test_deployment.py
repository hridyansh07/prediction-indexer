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
        for service in ("archiver", "archiver-once", "reaper"):
            with self.subTest(service=service):
                self.assertIn(
                    "environment: *aws-archive-environment", self.service(service)
                )

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


if __name__ == "__main__":
    unittest.main()
