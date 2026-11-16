"""Read-only audits in the recorded image; no rebuild, fetch, cache lease or replay.

Only the original configured package readers run in the container. Prerequisites
are resolved before launching, and a missing environment is not an invalid result.
"""

import hashlib
import json
import os
from pathlib import Path

from replay.bench.common import read_json
from replay.bench.docker import DockerCLI, DockerError, _mount
from replay.fees.artifacts import load_catalog
from replay.jobs.bundle import BundleFailure, _check_pinned_files
from replay.preparation import load_snapshot, sha
from replay.streams.protocol import ProtocolError, require


class MissingPrerequisite(Exception):
    def __init__(self, kind, identity):
        super().__init__(f"deep: missing prerequisite {kind} {identity}")


def read_groups(run_directory, groups):
    """Inside the original image, invoke each reader unchanged and report all."""
    from replay.strategies import load_entrypoint
    reports = []
    for group in groups:
        try:
            load_entrypoint(group["reader"])(Path(run_directory), group["name"])
            reports.append({"group": group["name"], "status": "PASS"})
        except Exception as error:
            reports.append({"group": group["name"], "status": "FAIL",
                            "error": f"{type(error).__name__}: {error}"[:2048]})
    return reports


# Send only this entry point's source: old recorded images need no new module.
# It imports the existing loader and calls the existing readers without wrappers.
def _script():
    import inspect
    return ("import json\nimport sys\nfrom pathlib import Path\n" + inspect.getsource(read_groups)
            + "\nprint(json.dumps(read_groups('/bench/run', json.loads(sys.argv[1]))))\n")


def prerequisites(directory, spec, *, docker, catalog_loader=load_catalog):
    directory = Path(directory).resolve()
    result = read_json(directory / "result.json")
    resolved = read_json(directory / "spec.resolved.json")
    config = read_json(directory / "run" / "run.json")
    image_file = directory / "image.id"
    # Reused bench images have no image.id file; result.json always pins the ID.
    image = image_file.read_text().strip() if image_file.is_file() else result["image"]["id"]
    require(image == result["image"]["id"], "recorded image identity mismatch")
    if image is None:
        raise MissingPrerequisite("image", "unrecorded")
    try:
        present = docker.run(["image", "inspect", "--format", "{{.Id}}", image])
    except (DockerError, OSError):
        raise MissingPrerequisite("image", image) from None
    if present != image:
        raise MissingPrerequisite("image", image)

    groups = [g for g in resolved["groups"] if g["name"] in config["strategies"]]
    require({g["name"] for g in groups} == set(config["strategies"]), "deep reader group set")
    # Mount the run, not /bench itself: derivative mount points need not exist
    # in the committed result, and creating them there would mutate evidence.
    mounts = [(directory / "run", "/bench/run")]
    context = directory / "context"
    try:
        load_snapshot(context, expected_sha256=result["context"]["snapshot_sha256"])
    except (OSError, ValueError, ProtocolError):
        raise MissingPrerequisite("context", result["context"]["snapshot_sha256"]) from None
    mounts.append((context, "/bench/context"))
    catalog_identity = result["fee_catalog_identity"]
    if catalog_identity is not None:
        sha(catalog_identity)
        catalog = Path(spec["fees"]["template_catalog"]).resolve()
        try:
            matched = catalog_loader(catalog).identity == catalog_identity
        except (OSError, ValueError, ProtocolError):
            matched = False
        if not matched:
            raise MissingPrerequisite("fees", catalog_identity)
        mounts.append((catalog, "/bench/fees/" + catalog_identity))
    if "game_state_sha256" in result:
        game = directory / "game_state" / "game_state.json"
        if not game.is_file() or hashlib.sha256(game.read_bytes()).hexdigest() != result["game_state_sha256"]:
            raise MissingPrerequisite("game_state", result["game_state_sha256"])
        mounts.append((game, "/bench/game_state.json"))

    for pin in config["transport"]["inputs"]:
        address = pin["derivative_address"]
        sha(address)
        cache = spec.get("derivative_cache")
        if cache is None:
            raise MissingPrerequisite("derivative", address)
        source = Path(cache["root"]).resolve() / "derivatives" / address
        target = Path(pin["directory"])
        require(target.parent.as_posix() in ("/bench/in/derivatives", "/bench/in/private-derivatives")
                and target.name == address, "deep derivative mount layout")
        try:
            # Use the owning receipt/stored-byte reader, without acquiring a
            # cache lease or updating its mutable eviction bookkeeping.
            _check_pinned_files(source, pin["receipt_sha256"], "deep derivative")
        except (OSError, ValueError, BundleFailure):
            raise MissingPrerequisite("derivative", address) from None
        mounts.append((source, target.as_posix()))
    return image, mounts, groups, config["limits"]["run_seconds"]


def audit(directory, spec, *, docker=None, runner=None, catalog_loader=load_catalog):
    """One isolated bundle; return per-group reports or raise MissingPrerequisite."""
    docker = docker or DockerCLI()
    image, mounts, groups, seconds = prerequisites(directory, spec, docker=docker, catalog_loader=catalog_loader)
    args = ["run", "--rm", "--pull", "never", "--network", "none", "--read-only", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--user", f"{os.getuid()}:{os.getgid()}",
            "--tmpfs", "/tmp:rw,nosuid,noexec,size=1g", "--env", "PYTHONDONTWRITEBYTECODE=1",
            "--entrypoint", "python"]
    for host, target in mounts:
        args += _mount(host, target)
    args += [image, "-c", _script(), json.dumps([{"name": g["name"], "reader": g["reader"]} for g in groups])]
    reports = json.loads((runner or docker.run)(args, timeout=seconds + 60))
    require(type(reports) is list and [r["group"] for r in reports] == [g["name"] for g in groups],
            "deep audit report groups")
    require(all(r["status"] in ("PASS", "FAIL") for r in reports), "deep audit report status")
    return reports
