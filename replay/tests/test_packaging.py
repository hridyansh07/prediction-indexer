from __future__ import annotations

import tomllib
import unittest
from pathlib import Path


class PackagingTests(unittest.TestCase):
    def test_installed_distribution_contains_replay_and_frozen_policy(self) -> None:
        root = Path(__file__).resolve().parents[2]
        project = tomllib.loads(
            (root / "pyproject.toml").read_text(encoding="utf-8")
        )

        includes = project["tool"]["setuptools"]["packages"]["find"]["include"]
        package_data = project["tool"]["setuptools"]["package-data"]

        self.assertIn("replay*", includes)
        self.assertIn("policy.json", package_data["replay"])


if __name__ == "__main__":
    unittest.main()
