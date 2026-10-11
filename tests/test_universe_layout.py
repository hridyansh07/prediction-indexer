"""The Universe package layering: dependencies point one way."""

import ast
import unittest
from collections import defaultdict
from pathlib import Path

UNIVERSE = Path(__file__).resolve().parents[1] / "universe"

# Package -> the other universe packages it may import. `commands` and
# `__main__` compose everything and are not constrained.
ALLOWED = {
    "derive": set(),
    "claims": {"derive", "store"},  # store: only DetailTooLarge, imported lazily
    "store": {"derive", "claims"},
    "ingest": {"store", "derive", "claims"},
    "jobs": {"config"},
    "api": {"store", "claims", "derive", "jobs", "config"},
    "config": set(),
}


def imports_by_package():
    edges = defaultdict(set)
    for path in UNIVERSE.rglob("*.py"):
        package = path.relative_to(UNIVERSE).parts[0].removesuffix(".py")
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names = []
            if isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
                if node.module == "universe":
                    names += [f"universe.{alias.name}" for alias in node.names]
            elif isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            for name in names:
                parts = name.split(".")
                if parts[0] == "universe" and len(parts) > 1 and parts[1] != package:
                    edges[package].add((parts[1], str(path.relative_to(UNIVERSE))))
    return edges


class UniverseLayoutTests(unittest.TestCase):
    def test_every_package_imports_only_its_allowed_layers(self):
        edges = imports_by_package()
        for package, allowed in ALLOWED.items():
            with self.subTest(package=package):
                violations = sorted((target, where) for target, where in edges.get(package, ())
                                    if target not in allowed)
                self.assertEqual(violations, [])

    def test_store_and_claims_never_import_api_or_ingest(self):
        edges = imports_by_package()
        for package in ("store", "claims", "derive", "jobs"):
            targets = {target for target, _ in edges.get(package, ())}
            self.assertFalse(targets & {"api", "ingest", "commands"}, package)

    def test_every_module_belongs_to_a_known_layer(self):
        top = {p.relative_to(UNIVERSE).parts[0].removesuffix(".py") for p in UNIVERSE.rglob("*.py")}
        self.assertEqual(top - set(ALLOWED), {"__init__", "__main__", "commands"})


if __name__ == "__main__":
    unittest.main()
