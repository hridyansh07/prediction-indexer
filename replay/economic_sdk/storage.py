"""Closed economic storage selection and independent physical evidence boundary.

Semantic readers retain logical names. Only a validated manifest selects their
physical names; this module imports neither the runtime nor frame writers.
"""

from pathlib import Path

from replay.economic_sdk import bounds
from replay.economic_sdk.output import file_list
from replay.economic_sdk.profile_stream_io import stream_lines
from replay.preparation import sha
from replay.strategy_sdk import plain
from replay.streams.protocol import obj, require


def closed_config(value, fields):
    value = plain(value)
    require(type(value) is dict, "closed schema")
    return obj(value, fields + (" storage" if "storage" in value else ""))


def configured_storage(config, layout):
    if "storage" not in config:
        return None
    value = obj(plain(config["storage"]), "version economic")
    require(type(value["version"]) is int and value["version"] == 1, "economic storage version")
    require(value["economic"] == "zstd", "economic storage representation")
    require(layout == 2, "economic storage requires layout 2")
    return value


def stored_manifest(value, storage, version):
    return value if storage is None else {**value, "version": version, "storage": storage}


def manifest_storage(value, legacy, stored, layout=2):
    require(type(value["version"]) is int, "manifest version")
    storage = configured_storage(value, layout)
    require(value["version"] == (legacy if storage is None else stored), "manifest version")
    return storage


def evidence_identity(name, value, compressed):
    from replay.economic_sdk.reader import file_identity
    logical = value
    if compressed:
        obj(value, "logical stored")
        logical, stored = value["logical"], obj(value["stored"], "sha256 byte_length")
        sha(stored["sha256"])
        require(type(stored["byte_length"]) is int and stored["byte_length"] >= 0, "stored byte length")
    file_identity(logical, name)
    if name.endswith("entities.json") or name == "reasons.json":
        limit = bounds.MAX_ENTITY_TABLE if name.endswith("entities.json") else bounds.MAX_METADATA
        require(logical["records"] == 1 and logical["byte_length"] <= limit, "table/size")
    return logical


def logical_files(manifest, experiment):
    """Check exact physical manifest membership/identities, return logical-name map."""
    from replay.economic_sdk.profile_policy import FILES
    from replay.economic_sdk.reader import file_identity
    names = file_list(experiment)
    compressed = "storage" in manifest
    if compressed:
        configured_storage(manifest, experiment.layout)
    extras = FILES if experiment.profile is not None else ()
    expected = {name + ".zst" if compressed else name for name in names} | set(extras)
    files = manifest["files"]
    require(type(files) is dict and set(files) == expected, "output file set")
    result = {}
    for name in names:
        entry = files[name + ".zst" if compressed else name]
        evidence_identity(name, entry, compressed)
        result[name] = entry
    for name in extras:
        file_identity(files[name], name)
        result[name] = files[name]
    return result


def check_inventory(directory, files):
    """Reject alternate representations, staging, unknown paths and all symlinks."""
    root = Path(directory)
    require(root.is_dir() and not root.is_symlink(), "regular output directory required")
    metadata = {"summary.json", "manifest.json", "content_receipt.json"}
    metadata |= {str(Path(name).parent / "summary.json") for name in files
                 if name.startswith("controls/")}
    allowed = set(files) | metadata
    directories = {str(parent) for name in allowed for parent in Path(name).parents if str(parent) != "."}
    present = set()
    for path in root.rglob("*"):
        require(not path.is_symlink(), "regular output required")
        name = path.relative_to(root).as_posix()
        if path.is_dir():
            require(name in directories, "output file set")
        else:
            require(path.is_file() and name in allowed, "output file set")
            present.add(name)
    require(set(files) <= present, "output file set")


def stored_lines(root, name, identity, limit):
    evidence_identity(name, identity, True)
    path = Path(root) / (name + ".zst")
    require(path.is_file() and not any(p.is_symlink() for p in (path, *path.parents)),
            "regular output required")
    for other in (Path(root) / name, Path(root) / (name + ".open"), path.with_name(path.name + ".open")):
        require(not (other.exists() or other.is_symlink()), "output file set")
    yield from stream_lines(path, identity["logical"], identity["stored"], limit, name)
