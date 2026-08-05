"""§9.1's two absence gates: no SBE anywhere, no whole-buffer codec in production.

An absence is only a fact if something checks for it. Both of these are the kind
of removal that reappears — a helper is convenient, a generated codec is dropped
in beside the one it was meant to replace — so the check is a test rather than a
line in a document.

§4.5 also warns about the shape of the search itself: "Search results containing
ordinary words such as `misbehaving` are not SBE references and must not be
mechanically edited." The scan below matches identifiers, not substrings.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]

#: Where code lives. `docs/` is deliberately absent: what remains there after
#: the specification documents below moved beside their implementations still
#: records other decisions worth keeping unswept.
CODE_DIRECTORIES = (
    "encoder",
    "ingester",
    "live_tracker",
    "analysis",
    "archive",
    "replay",
    "scripts",
    "splices",
    "targeter",
    "tests",
)

CODE_SUFFIXES = (".py", ".rs", ".toml", ".yaml", ".yml", ".json", ".md", ".lock", ".hex")

SKIP_DIRECTORIES = {"__pycache__", "target", ".venv", "node_modules", ".git"}

#: Specification documents that legitimately discuss the rejected SBE design —
#: deleting that reasoning would lose why the codec has the shape it has.
#: Named individually rather than exempted by directory: both now live beside
#: the code they specify, inside directories this scan otherwise covers.
SBE_RATIONALE_DOCUMENTS = {
    "archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md",
    "encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md",
}

#: Identifier-shaped, so `misbehaving` does not match. Two alternatives because
#: SBE appeared both as a bare acronym and inside camel-case names — and the
#: camel-case one is deliberately case-*sensitive*: folding it makes `sbeh`
#: match, which is how `misbehaving` gets swept into a mechanical edit.
SBE_PATTERN = re.compile(r"(?<![A-Za-z0-9_])[Ss][Bb][Ee](?![A-Za-z0-9_])|Sbe[A-Z]")


def code_files() -> list[Path]:
    found: list[Path] = []
    for directory in CODE_DIRECTORIES:
        root = PROJECT_ROOT / directory
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in CODE_SUFFIXES:
                continue
            if SKIP_DIRECTORIES & set(path.relative_to(PROJECT_ROOT).parts):
                continue
            found.append(path)
    return found


class SbeRemovalTests(unittest.TestCase):
    def test_no_source_file_references_sbe(self) -> None:
        offenders = []
        for path in code_files():
            if path.name == Path(__file__).name:
                continue
            if path.relative_to(PROJECT_ROOT).as_posix() in SBE_RATIONALE_DOCUMENTS:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for number, line in enumerate(text.splitlines(), start=1):
                if SBE_PATTERN.search(line):
                    offenders.append(f"{path.relative_to(PROJECT_ROOT)}:{number}: {line.strip()}")
        self.assertEqual(offenders, [], "SBE survives in code")

    def test_the_sbe_module_and_its_fixtures_are_gone(self) -> None:
        for relative in (
            "encoder/sbe.py",
            "encoder/fixtures/probe_v1.hex",
            "encoder/fixtures/probe_v1.sbe.zst.hex",
        ):
            self.assertFalse((PROJECT_ROOT / relative).exists(), relative)

    def test_the_encoder_package_exports_only_the_streaming_codec(self) -> None:
        import encoder

        self.assertNotIn("encode_message", encoder.__all__)
        self.assertNotIn("compress_zstd", encoder.__all__)
        self.assertIn("encode_stream", encoder.__all__)
        self.assertIn("decode_stream", encoder.__all__)


class WholeBufferConfinementTests(unittest.TestCase):
    """§4.2 — production may not call an API whose value is a complete object."""

    #: Tests prove the codec; probes measure it. Neither is a production path,
    #: and both legitimately hold a small payload in memory.
    ALLOWED_PREFIXES = ("tests/", "scripts/", "encoder/whole_buffer.py")

    #: An import, not a mention: `encoder/compression.py` names the module in
    #: its docstring to say where the helpers went, which is the opposite of
    #: calling them.
    IMPORT_PATTERN = re.compile(r"^\s*(from\s+encoder\.whole_buffer|import\s+encoder\.whole_buffer)")

    def test_only_tests_and_probes_import_the_whole_buffer_helpers(self) -> None:
        importers = []
        for path in code_files():
            relative = path.relative_to(PROJECT_ROOT).as_posix()
            if relative.startswith(self.ALLOWED_PREFIXES) or path.suffix != ".py":
                continue
            text = path.read_text(encoding="utf-8")
            if any(self.IMPORT_PATTERN.match(line) for line in text.splitlines()):
                importers.append(relative)
        self.assertEqual(importers, [], "a production module reached for the test-only helpers")

    def test_the_archiver_and_reaper_stream(self) -> None:
        """A direct read of a whole segment is the failure this rules out."""
        for relative in (
            "archive/archiver/service.py",
            "archive/reaper/service.py",
            "archive/storage/local.py",
            "archive/common/verify.py",
        ):
            path = PROJECT_ROOT / relative
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            for forbidden in (".read_bytes()", ".read()\n"):
                self.assertNotIn(
                    forbidden,
                    text,
                    f"{relative} reads an object whole; §2.1 requires bounded streaming",
                )


if __name__ == "__main__":
    unittest.main()
