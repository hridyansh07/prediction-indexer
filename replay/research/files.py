"""Manifest-selected, identity-verified group evidence; one shared codec only."""

import hashlib
import io
import json
from pathlib import Path

from encoder import LogicalIdentity, StoredIdentity, decode_stream
from replay.economic_sdk import bounds
from replay.economic_sdk.profile_stream_io import stream_lines
from replay.preparation import digest
from replay.research.jsonio import require


class Files:
    def __init__(self, output, manifest=None):
        self.root = Path(output)
        self.manifest = manifest if manifest is not None else self._metadata("manifest.json", bounds.MAX_METADATA)

    def _regular(self, path):
        require(path.is_file() and not any(p.is_symlink() for p in (path, *path.parents)),
                f"{path.name}: regular output required")

    def _metadata(self, name, limit):
        path = self.root / name
        self._regular(path)
        with path.open("rb") as source:
            data = source.read(limit + 1)
        require(len(data) <= limit, f"{name}: document size limit {limit}")
        return json.loads(data)

    def entry(self, name):
        """None means genuine absence, never undeclared or missing evidence."""
        inventory = self.manifest.get("files", {})
        require(not (name in inventory and name + ".zst" in inventory), f"{name}: both forms declared")
        physical = name + ".zst" if name + ".zst" in inventory else name
        if physical not in inventory:
            require(not any(p.exists() or p.is_symlink() for p in
                            (self.root / name, self.root / (name + ".zst"))), f"{name}: undeclared evidence")
            return None
        path = self.root / physical
        require(path.exists() or path.is_symlink(), f"{physical}: declared file missing")
        self._regular(path)
        alternate = self.root / (name if physical != name else name + ".zst")
        require(not (alternate.exists() or alternate.is_symlink()), f"{name}: undeclared evidence (alternate form)")
        return inventory[physical]

    def _plain_identity(self, name, entry, limit):
        require(entry["byte_length"] <= limit, f"{name}: document/byte size limit {limit}")
        sha, length, records, last = hashlib.sha256(), 0, 0, b""
        with (self.root / name).open("rb") as source:
            while chunk := source.read(1024 * 1024):
                length += len(chunk)
                require(length <= limit, f"{name}: logical identity mismatch (byte size limit {limit})")
                sha.update(chunk)
                records += chunk.count(b"\n")
                last = chunk[-1:]
        require(length == entry["byte_length"] and sha.hexdigest() == entry["sha256"],
                f"{name}: logical identity mismatch (sha256/byte_length)")
        require(records == entry["records"], f"{name}: record count mismatch")
        require(not length or last == b"\n", f"{name}: line/truncation: unterminated line")

    def lines(self, name, *, max_line=bounds.MAX_LINE, max_bytes=None):
        entry = self.entry(name)
        require(entry is not None, f"{name}: not declared")
        compressed = name + ".zst" in self.manifest.get("files", {})
        logical = entry["logical"] if compressed else entry
        if max_bytes is not None:
            require(logical["byte_length"] <= max_bytes, f"{name}: research probes exceed byte budget")
        if compressed:
            lines = stream_lines(self.root / (name + ".zst"), logical, entry["stored"], max_line, name)
            try:
                for line in lines:
                    yield json.loads(line)
            finally:
                lines.close()
        else:
            self._plain_identity(name, entry, logical["byte_length"])
            with (self.root / name).open("rb") as source:
                while line := source.readline(max_line + 1):
                    require(len(line) <= max_line and line.endswith(b"\n"), f"{name}: research line bound")
                    yield json.loads(line)

    def document(self, name, limit):
        # These publication metadata files live outside the file inventory.
        # summary_sha256 hashes canonical JSON, not its physical serialization.
        if name in ("summary.json", "content_receipt.json"):
            document = self._metadata(name, limit)
            if name == "summary.json" and "summary_sha256" in self.manifest:
                require(digest(document) == self.manifest["summary_sha256"], "summary.json: logical sha256 mismatch")
            return document
        entry = self.entry(name)
        require(entry is not None, f"{name}: not declared")
        if name + ".zst" not in self.manifest.get("files", {}):
            self._plain_identity(name, entry, limit)
            return self._metadata(name, limit)
        logical, stored = entry["logical"], entry["stored"]
        require(logical["byte_length"] <= limit, f"{name}: document size limit {limit}")
        # Documents are explicitly bounded in memory; streams never are.
        with (self.root / (name + ".zst")).open("rb") as source, io.BytesIO() as sink:
            decode_stream(source, sink,
                expected_logical=LogicalIdentity(logical["sha256"], logical["byte_length"], logical["records"]),
                expected_stored=StoredIdentity(stored["sha256"], stored["byte_length"]), max_decoded_bytes=limit)
            sink.seek(0)
            return json.load(sink)
