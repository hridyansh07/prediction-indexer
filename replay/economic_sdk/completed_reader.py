"""Post-SUCCESS byte/configuration binding; explicit audits use the original reader.

Writers still run their independent semantic reader before terminal ACK. Only
known package readers use bound mode; caller-owned readers retain their contract.
No output, receipt or retained SDK state is changed by this dispatcher.
"""

import hashlib
import importlib
import json
from pathlib import Path

from replay.economic_sdk.reader import file_identity
from replay.economic_sdk.transitions_reader import identity_record
from replay.preparation import digest
from replay.strategies import canonical_reference, load_entrypoint
from replay.strategy_sdk import PreparedInput, plain
from replay.streams.protocol import freeze, obj, require
from replay import supervisor

# Package aliases resolve to these same callables, including output.py imports.
_READERS = (
    ("same_venue_complement", "complement", "complement factory binding"),
    ("cross_venue_arbitrage", "cross-venue result", "strategy factory binding"),
    ("same_venue_multi_market", "same-venue multi-market result", "strategy factory binding"),
    ("same_venue_implication_cover", "implication result", "strategy factory binding"),
    ("cross_venue_implication_cover", "implication result", "strategy factory binding"),
    ("market_profile", "profile", "profile factory binding"),
    ("bundle_coverage", None, "coverage factory binding"),
)


def _hash_file(root, name, identity):
    path = root / name
    require(not Path(name).is_absolute() and ".." not in Path(name).parts, "output file path")
    require(path.is_file() and not any(p.is_symlink() for p in
            (path, *(root / parent for parent in Path(name).parents))),
            "regular output required")
    checksum, size = hashlib.sha256(), 0
    with path.open("rb") as stream:
        while payload := stream.read(1 << 20):
            size += len(payload)
            require(size <= identity["byte_length"], f"output file length mismatch: {name}")
            checksum.update(payload)
    require(size == identity["byte_length"], f"output file length mismatch: {name}")
    require(checksum.hexdigest() == identity["sha256"], f"output file SHA-256 mismatch: {name}")


def _metadata_bytes(path, value):
    # The semantic digest canonicalizes JSON. Bind the writer's physical encoding
    # too, so a whitespace-only edit cannot escape the post-SUCCESS byte check.
    expected = (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode()
    with path.open("rb") as stream:
        require(stream.read(len(expected) + 1) == expected,
                f"output metadata encoding mismatch: {path.name}")


def read_completed(run_directory, group, *, reader, deep=False):
    """Same {receipt, manifest, summary}; deep delegates without altering the call."""
    if deep:
        return reader(run_directory, group)
    for package, completed_label, factory_message in _READERS:
        if reader is load_entrypoint(f"replay.strategies.{package}:read_completed"):
            break
    else:
        return reader(run_directory, group)

    root = Path(run_directory)
    if completed_label is not None:
        require((root / "run.json").is_file() and (root / "SUCCESS.json").is_file(),
                f"completed {completed_label} requires supervisor SUCCESS")
    success, config = supervisor.read_success(root), supervisor.read(root / "run.json")
    require(group in success["outputs"], "unknown strategy group")
    spec = config["strategies"][group]
    require(canonical_reference(spec["factory"]) == f"replay.strategies.{package}:build", factory_message)
    cfg = spec["config"]
    mode = None
    if package in ("market_profile", "bundle_coverage"):
        module = importlib.import_module(f"replay.strategies.{package}." +
                                        ("strategy" if package == "market_profile" else "output"))
        prepared = PreparedInput({k: cfg[k] for k in ("version", "snapshot_directory", "snapshot_sha256")})
        policy = module.profile_policy(cfg["policy"], standalone=True) if package == "market_profile" else None
        inputs = None
    else:
        if package.endswith("implication_cover"):
            mode = "same_venue" if package.startswith("same_venue") else "cross_venue"
            base = "replay.strategies._shared.implication_cover"
        else:
            base = f"replay.strategies.{package}"
        contract = importlib.import_module(base + ".contract")
        module = importlib.import_module(base + ".output")
        inputs = contract.Inputs(cfg, mode) if mode else contract.Inputs(cfg)
        prepared, policy = inputs.prepared, inputs.policy
    prepared.bind(freeze(supervisor.initial(config)))
    output = root / success["outputs"][group]
    read_metadata = module._json if package == "same_venue_complement" else module.read_json
    manifest = read_metadata(output / "manifest.json")
    if inputs is None:
        module._manifest(manifest, True)
    elif package == "same_venue_complement":
        module.check_manifest(manifest, True)
    elif mode:
        module.check_manifest(manifest, plain(prepared.snapshot), True, mode)
    else:
        module.check_manifest(manifest, plain(prepared.snapshot), True)
    require(manifest["snapshot_sha256"] == cfg["snapshot_sha256"], "snapshot binding")
    receipt = obj(read_metadata(output / "content_receipt.json"),
                  "version semantic_sha256 run_id attempt_id group identity terminal")
    version_message = ("semantic identity" if package == "market_profile" else "invalid wire value"
                       if package in ("same_venue_complement", "bundle_coverage") else "receipt version")
    require(type(receipt["version"]) is int and receipt["version"] == 1, version_message)
    require(receipt["semantic_sha256"] == digest(manifest), "semantic identity")
    summary = read_metadata(output / "summary.json")
    require(manifest["summary_sha256"] == digest(summary), "summary identity/schema")
    if policy is not None:
        require(manifest["policy"] == policy, "configured policy")
    if inputs is not None:
        expected = inputs.experiment_sha256 if package == "same_venue_complement" else inputs.identity
        require(manifest["experiment_sha256"] == expected
                and manifest["fee_engine_identity"] == inputs.bridge.engine_identity, "configured identity")
    require(type(receipt["terminal"]) is int
            and {k: receipt[k] for k in ("identity", "attempt_id", "group", "run_id", "terminal")} ==
            {"identity": success["identity"], "attempt_id": success["attempt"], "group": group,
             "run_id": config["transport"]["run_id"], "terminal": success["terminal"]},
            "supervisor/content binding")

    files = manifest.get("files", {"intervals.ndjson": manifest.get("intervals")})
    require(type(files) is dict, "output file set")
    for name, identity in files.items():
        if "stored" in identity:
            _, stored = identity_record(identity)
            _hash_file(output, name, stored)
        else:
            _hash_file(output, name, file_identity(identity))
    metadata = {"manifest.json": manifest, "summary.json": summary, "content_receipt.json": receipt}
    # Control summaries are emitted after semantic validation and are not listed
    # in manifest.files. They are bound by the receipted top-level summary.
    for name, control in summary.get("controls", {}).items():
        relative = f"controls/{name}/summary.json"
        require(read_metadata(output / relative) == control, "summary identity/schema")
        metadata[relative] = control
    for name, value in metadata.items():
        _metadata_bytes(output / name, value)
    present = {p.relative_to(output).as_posix() for p in output.rglob("*") if p.is_file()}
    require(present == set(files) | set(metadata), "output file set")
    return {"receipt": receipt, "manifest": manifest, "summary": summary}
