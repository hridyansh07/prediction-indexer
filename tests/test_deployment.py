"""Deployment-shape regressions that do not require a Docker daemon."""

from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path
from unittest import mock

from universe import commands
from universe.ingest.sync import SyncResult

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

    def test_every_cloud_archive_service_receives_the_shared_environment(self) -> None:
        self.assertIn(
            "x-cloud-archive-environment: &cloud-archive-environment", self.compose
        )
        for variable in (
            "ARCHIVE_BACKEND",
            "ARCHIVE_ROOT",
            "ARCHIVE_STORE_ID",
            "ARCHIVE_DURABILITY",
            "ARCHIVE_GCS_BUCKET",
        ):
            self.assertIn(variable, self.compose)
        self.assertNotIn("ARCHIVE_S3_", self.compose)
        self.assertNotIn("AWS_", self.compose)
        for service in (
            "archiver",
            "archiver-once",
            "reaper",
            "reaper-once",
            "canonical-reaper",
            "canonical-reaper-once",
        ):
            with self.subTest(service=service):
                self.assertIn(
                    "environment: *cloud-archive-environment", self.service(service)
                )
                self.assertNotIn("--gcs-bucket", self.service(service))
                self.assertNotIn("--archive-backend", self.service(service))

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

    def test_ingest_store_reaper_is_one_shot_audit_first_and_uses_the_rust_image(
        self,
    ) -> None:
        service = self.service("ingest-store-reaper")
        self.assertIn("<<: *ingester-service", service)
        self.assertIn('restart: "no"', service)
        self.assertIn("indexer-store-reap", service)
        self.assertIn("${INGEST_STORE_REAPER_MODE:-audit}", service)
        self.assertNotIn("--interval-seconds", service)


class TargeterV2DeploymentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.override_path = ROOT / "compose.targeter-v2.yaml"

    def test_base_targeter_is_the_v2_one_shot(self) -> None:
        compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
        start = compose.index("\n  targeter:\n")
        end = compose.index("\n  splice-polymarket:\n")
        service = compose[start:end]
        self.assertIn("targeter/run_v2.py", service)
        self.assertIn("--mode", service)
        self.assertIn("publish", service)
        self.assertIn('restart: "no"', service)
        self.assertIn("environment: *cloud-archive-environment", service)
        self.assertNotIn("targeter/run.py", compose)
        self.assertNotIn("capture_manifest", compose)
        self.assertNotIn("--interval-seconds", service)
        self.assertNotIn("--tick-seconds", service)

    def test_base_splices_resolve_the_v2_generation_pointer(self) -> None:
        compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
        self.assertNotIn("targets_polymarket.json", compose)
        self.assertNotIn("targets_limitless.json", compose)
        self.assertNotIn("targets_kalshi.json", compose)
        self.assertGreaterEqual(compose.count("/live/targeter-v2/current.json"), 8)

    def test_override_holds_only_the_ops_services(self) -> None:
        document = self.override_path.read_text(encoding="utf-8")
        for service in ("targeter", "splice-polymarket", "splice-kalshi", "splice-limitless"):
            self.assertNotIn(f"\n  {service}:\n", document)
        self.assertNotIn("--interval-seconds", document)
        for service in (
            "targeter-v2-run-archiver",
            "targeter-v2-run-reaper",
            "targeter-v2-integrity",
        ):
            self.assertIn(f"\n  {service}:\n", document)

    def test_archive_provider_configuration_is_environment_only(self) -> None:
        document = self.override_path.read_text(encoding="utf-8")
        self.assertIn("environment: &targeter-v2-archive-environment", document)
        self.assertIn('ARCHIVE_BACKEND: "${ARCHIVE_BACKEND:-local}"', document)
        self.assertNotIn("TARGETER_ARCHIVE_BACKEND", document)
        for variable in (
            "ARCHIVE_BACKEND",
            "ARCHIVE_ROOT",
            "ARCHIVE_DURABILITY",
            "ARCHIVE_GCS_BUCKET",
        ):
            self.assertIn(variable, document)
        self.assertNotIn("ARCHIVE_S3_", document)
        self.assertNotIn("AWS_", document)
        for option in (
            "--archive-backend",
            "--archive-root",
            "--archive-durability",
            "--gcs-bucket",
        ):
            self.assertNotIn(option, document)

    def test_documentation_supplies_a_periodic_one_shot_command_and_audit_gate(
        self,
    ) -> None:
        deployment = (ROOT / "targeter" / "v2" / "DELIVERY.md").read_text(
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
        self.assertIn('CMD ["python", "-u", "-m", "universe", "serve"]', dockerfile)
        for command in ("sync", "backfill", "backup"):
            self.assertIn(f'command: ["python", "-u", "-m", "universe", "{command}"]', compose)
        self.assertIn('"eth-account>=0.13,<0.14"', dockerfile)
        config = (ROOT / "configs" / "event_universe.json").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            '"database_path": "/var/lib/replay/jobs.sqlite3"', config
        )
        self.assertIn(
            "COPY configs/replay_runner.json /etc/prediction-indexer/replay_runner.json",
            dockerfile,
        )
        pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('"eth-account>=0.13,<0.14"', pyproject)
        self.assertNotIn("COPY universe/", shared)
        server = compose.split("  event-universe:", 1)[1].split(
            "  event-universe-sync:", 1
        )[0]
        self.assertNotIn("AWS_ACCESS_KEY_ID", server)
        self.assertNotIn("ARCHIVE_S3_BUCKET", server)
        runtime = compose.split("x-universe-runtime:", 1)[1].split("\nservices:", 1)[0]
        self.assertNotIn("environment:", runtime)
        self.assertEqual(compose.count("    environment: *universe-job-environment"), 4)
        game = compose.split("  event-universe-game-state:", 1)[1].split("  replay-redis:", 1)[0]
        self.assertIn("GAMESTATE_DATA_ROOT", game)
        self.assertIn("gamestate.run_scheduled", game)
        self.assertNotIn("*replay-volume", game)
        self.assertNotIn("*universe-volume", game)

    def test_jobs_are_direct_configured_scripts_without_an_argument_parser(
        self,
    ) -> None:
        universe = ROOT / "universe"
        self.assertFalse((universe / "cli.py").exists())
        self.assertEqual(sorted(universe.glob("run_*.py")), [])
        # One command name selects a configured job; there are no options.
        for name in ("commands.py", "__main__.py"):
            self.assertNotIn("argparse", (universe / name).read_text(encoding="utf-8"))
        source = (universe / "commands.py").read_text(encoding="utf-8")
        self.assertEqual(source.count("config = load_config()"), 4)
        from universe.__main__ import COMMANDS, main
        self.assertEqual(sorted(COMMANDS), ["backfill", "backup", "serve", "sync"])
        with mock.patch("sys.stderr"):
            self.assertEqual(main([]), 2)
            self.assertEqual(main(["sync", "--extra"]), 2)
        config = (ROOT / "configs" / "event_universe.json").read_text(encoding="utf-8")
        self.assertIn('"event_universe_config_version": 4', config)
        self.assertIn('"generated_start": null', config)
        self.assertIn('"generated_end": null', config)
        self.assertFalse((ROOT / "archive" / "run_receipt_mirror.py").exists())
        self.assertFalse((ROOT / "configs" / "archive_receipt_mirror.json").exists())

    def test_backfill_job_fails_while_retry_ledger_is_pending(self) -> None:
        config = mock.Mock()
        config.backfill.generated_start = object()
        config.backfill.generated_end = object()
        result = SyncResult(completed=True, pending_failures=1)

        with (
            mock.patch.object(commands, "load_config", return_value=config),
            mock.patch.object(commands, "UniverseStore") as store,
            mock.patch.object(
                commands, "backfill_targeter_history", return_value=result
            ),
            mock.patch("builtins.print"),
        ):
            self.assertEqual(commands.backfill(), 1)
        store.return_value.initialize.assert_called_once_with()

    def test_schema_is_market_universe_without_raw_evidence_tables(self) -> None:
        schema_directory = ROOT / "universe" / "schema"
        sql_files = list(schema_directory.glob("*.sql"))
        self.assertEqual(
            sorted(path.name for path in sql_files),
            ["replay_auth.sql", "replay_jobs.sql", "schema.sql"],
        )
        schema = (schema_directory / "schema.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("CREATE TABLE selection_occurrences", schema)
        self.assertIn("CREATE TABLE bundle_contexts", schema)
        self.assertIn("CREATE TABLE bundle_retirements", schema)
        self.assertIn("CREATE TABLE umbrella_events", schema)
        self.assertIn("observed_activation_at", schema)
        self.assertIn("CREATE TABLE universe_sync_failures", schema)
        self.assertIn("CREATE TABLE canonical_markets", schema)
        self.assertIn("CREATE TABLE claim_classes", schema)
        self.assertIn("CREATE TABLE claim_relations", schema)
        self.assertIn("CREATE TABLE market_claims", schema)
        self.assertNotIn("CREATE TABLE relation_observations", schema)
        self.assertNotIn("CREATE TABLE cadence_runs", schema)
        for stale in ("segment_receipts", "control_records", "connection_epochs"):
            self.assertNotIn(stale, schema)

    def test_universe_runbook_has_safe_bounded_rebuild_order(self) -> None:
        deployment = (ROOT / "docs" / "DEPLOYMENT.md").read_text(encoding="utf-8")
        section = deployment.split("### Safe full rebuild ordering", 1)[1]
        self.assertLess(section.index("event-universe-backfill"), section.index("event-universe-sync"))
        for contract in (
            "backfill_batch",
            "range-specific SQLite checkpoint",
            "144 runs",
            "128 MiB/run",
            "There is no automatic pruning",
        ):
            self.assertIn(contract, section)

    def test_orb_setup_creates_and_installs_the_project_virtual_environment(
        self,
    ) -> None:
        setup = (ROOT / ".agents" / "setup").read_text(encoding="utf-8")
        self.assertIn('python3 -m venv "$REPO_ROOT/.venv"', setup)
        self.assertIn('"$REPO_ROOT/.venv/bin/python" -m pip install -e', setup)


class ReplayProductionDeploymentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compose = (ROOT / "compose.universe.yaml").read_text(encoding="utf-8")
        self.runner = (ROOT / "docker" / "replay-runner.Dockerfile").read_text(
            encoding="utf-8"
        )
        self.caddy = (ROOT / "docker" / "Caddyfile").read_text(encoding="utf-8")

    def service(self, name: str) -> str:
        marker = f"\n  {name}:\n"
        start = self.compose.index(marker) + len(marker)
        following = re.search(r"\n  [a-z][a-z0-9-]*:\n", self.compose[start:])
        return self.compose[start : start + following.start()] if following else self.compose[start:]

    def test_replay_services_are_private_hardened_and_one_shot(self) -> None:
        redis = self.service("replay-redis")
        runner = self.service("replay-runner")
        universe = self.service("event-universe")
        self.assertIn("redis:8.2", redis)
        self.assertIn("--maxmemory-policy", redis)
        self.assertIn("noeviction", redis)
        self.assertIn('--save', redis)
        self.assertIn('--appendonly', redis)
        self.assertIn('--protected-mode\n      - "no"', redis)
        self.assertIn('user: "${PUID:-1000}:${PGID:-1000}"', redis)
        self.assertNotIn("ports:", redis)
        self.assertNotIn("volumes:", redis)
        self.assertIn("REPLAY_REDIS_MAXMEMORY_BYTES", redis)
        self.assertIn('restart: "no"', runner)
        self.assertIn("python", runner)
        self.assertIn("replay.jobs", runner)
        self.assertIn("REPLAY_IMAGE_REVISION", runner)
        self.assertIn("REPLAY_RUNNER_IMAGE:-", runner)
        self.assertIn("*replay-volume", runner)
        self.assertIn("*universe-runtime", universe)
        self.assertIn("- *replay-volume", self.compose)
        self.assertIn("networks: [replay-private, replay-egress]", runner)
        for service in (runner, redis):
            self.assertIn("pids_limit:", service)
            self.assertIn("mem_limit:", service)
        self.assertNotIn("CAPTURE_DATA_ROOT", self.compose)
        self.assertNotIn("/var/run/docker.sock", self.compose)

    def test_runner_image_contains_release_tools_dependency_and_nonroot_runtime(self) -> None:
        self.assertIn("--release", self.runner)
        self.assertIn("--example materialize_range", self.runner)
        self.assertIn("replay-publish", self.runner)
        self.assertIn(".[replay-redis]", self.runner)
        self.assertIn("ARG REPLAY_IMAGE_REVISION", self.runner)
        self.assertIn("COPY replay/streams/attempt.lua replay/streams/attempt.lua", self.runner)
        self.assertNotIn("must be a full Git SHA", self.runner)
        self.assertIn("USER replay:replay", self.runner)
        self.assertIn("materialize_range --describe", self.runner)

    def test_caddy_limits_transport_but_universe_owns_rate_limiting(self) -> None:
        self.assertIn("REPLAY_PUBLIC_HOST", self.caddy)
        self.assertIn("reverse_proxy", self.caddy)
        self.assertIn("request_body", self.caddy)
        self.assertNotIn("rate_limit", self.caddy)
        self.assertIn("Strict-Transport-Security", self.caddy)
        self.assertIn("X-Content-Type-Options", self.caddy)
        self.assertIn("Referrer-Policy", self.caddy)
        self.assertIn("max_size 64KiB", self.caddy)
        self.assertIn("max_header_size 32KiB", self.caddy)
        auth = self.caddy.split("@auth path", 1)[1].split("@api path", 1)[0]
        self.assertNotIn("encode", auth)

    def test_caddy_grants_cors_to_any_origin_without_credentials(self) -> None:
        self.assertIn('Access-Control-Allow-Origin "*"', self.caddy)
        self.assertIn('Access-Control-Expose-Headers "Retry-After"', self.caddy)
        # "*" in Allow-Headers never covers Authorization, so it is listed.
        self.assertIn(
            'Access-Control-Allow-Headers "Authorization, Content-Type, Idempotency-Key"',
            self.caddy,
        )
        self.assertNotIn("Access-Control-Allow-Credentials", self.caddy)
        # Preflights are answered by Caddy before any proxy handler.
        self.assertIn("@cors_preflight method OPTIONS", self.caddy)
        self.assertLess(
            self.caddy.index("handle @cors_preflight"), self.caddy.index("handle @auth")
        )
        self.assertIn("sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d", self.compose)

    def test_non_replay_compose_renders_with_example_environment(self) -> None:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                ".env.example",
                "-f",
                "compose.universe.yaml",
                "config",
                "--quiet",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_replay_networks_keep_redis_private_and_localhost_universe_access(self) -> None:
        redis = self.service("replay-redis")
        universe = self.service("event-universe")
        caddy = self.service("caddy")
        self.assertIn("networks: [replay-private]", redis)
        self.assertNotIn("replay-edge", redis)
        self.assertIn("127.0.0.1", universe)
        self.assertIn("replay-private: {}", universe)
        self.assertIn("ipv4_address: 172.30.0.3", universe)
        self.assertNotIn("replay-private", caddy)
        self.assertIn("ipv4_address: 172.30.0.2", caddy)

    def test_scheduler_and_operations_are_documented_as_gated_one_shots(self) -> None:
        deployment = (ROOT / "docs" / "DEPLOYMENT.md").read_text(encoding="utf-8")
        replay = deployment.split("## Replay jobs production runtime", 1)[1]
        self.assertIn("--profile replay run --rm replay-preflight", replay)
        self.assertIn("--profile replay run --rm --no-deps replay-runner", replay)
        self.assertIn("disable the Replay scheduler", replay)
        self.assertIn("systemd", replay)
        self.assertIn("local_state_lost", replay)
        self.assertIn("resume-blocked", replay)
        self.assertIn("receipt-only", replay)
        self.assertIn("full-volume snapshot", replay)
        self.assertIn("in-process rate limiter", replay)

    def test_local_runner_build_revision_default_is_stable_identifier(self) -> None:
        self.assertIn(
            'REPLAY_IMAGE_REVISION: "${REPLAY_IMAGE_REVISION:-local}"',
            self.compose,
        )


if __name__ == "__main__":
    unittest.main()
