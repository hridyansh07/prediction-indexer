"""Closed run-list contracts, immutable contexts and exact completion bindings."""
import hashlib
import json
import re

from replay.preparation import load_snapshot
from replay.strategy_sdk import plain
from replay.strategies import canonical_reference
from replay.research.io import child, closed, digest, document, identity, need, safe_path

NAME = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,63}")
FACTORIES = {"profile": "market_profile", "cross_venue": "cross_venue_arbitrage",
             "multi_market": "same_venue_multi_market", "complement": "same_venue_complement",
             "implication_cover_same": "same_venue_implication_cover", "implication_cover_cross": "cross_venue_implication_cover"}


def configuration(path):
    value = closed(document(path), "version lenses game_time_windows archive")
    need(type(value["version"]) is int and value["version"] == 1, "research version")
    need(type(value["lenses"]) is dict and "profile" in value["lenses"] and len(value["lenses"]) <= 16, "research lenses")
    for name, lens in value["lenses"].items():
        need(NAME.fullmatch(name) is not None and name in FACTORIES, "lens name")
        closed(lens, "group adapter")
        need(NAME.fullmatch(lens["group"]) is not None, "group name")
        need(lens["adapter"] == ("profile" if name == "profile" else "sdk_episodes"), "closed adapter")
    windows = value["game_time_windows"]
    need(type(windows) is dict and set(windows) == {"kalshi_market_close", "close_minus_duration", "kalshi_milestone_end"}, "game windows")
    for window in windows.values():
        closed(window, "before_ms after_ms")
        need(all(type(n) is int and 0 <= n <= 86400000 for n in window.values()), "window bounds")
    archive = value["archive"]
    if archive is not None:
        closed(archive, "backend_env root_env bucket_env durability_env store_id_env")
        need(all(type(n) is str and re.fullmatch(r"[A-Z][A-Z0-9_]*", n) for n in archive.values()), "archive env names")
    return value


def run_list(path, config):
    value = closed(document(path), "runs_version events")
    need(type(value["runs_version"]) is int and value["runs_version"] == 1, "runs version")
    need(type(value["events"]) is list and 0 < len(value["events"]) <= 1000, "event bound")
    seen = set()
    for event in value["events"]:
        closed(event, "bundle_id context runs")
        need(type(event["bundle_id"]) is str and NAME.fullmatch(event["bundle_id"]) is not None
             and event["bundle_id"] not in seen, "bundle identity")
        seen.add(event["bundle_id"])
        need(type(event["runs"]) is dict and set(event["runs"]) <= set(config["lenses"]), "run lenses")
        for p in (event["context"], *event["runs"].values()):
            need(type(p) is str and safe_path(p).is_dir(), "run/context directory")
    return value


def context(directory, bundle, bindings):
    root = safe_path(directory)
    receipt = document(root / "receipt.json")
    snapshot = plain(load_snapshot(root, expected_sha256=receipt["snapshot_sha256"]))
    need(snapshot["config"]["bundle_id"] == bundle, "context bundle binding")
    event_id = snapshot.get("outcomes", {}).get("document", {}).get("event_id")
    need(type(event_id) is str and re.fullmatch(r"event:d1:[0-9a-f]{64}", event_id), "immutable event identity required")
    for name in ("context.json", "receipt.json"):
        p = child(root, name)
        bindings[str(p)] = identity(p)
    return snapshot, event_id, receipt["snapshot_sha256"]


def completed(directory, group, lens, snapshot_sha, bindings):
    """Recheck completion without resolving the container's original filesystem paths."""
    root = safe_path(directory)
    def read(path):
        bindings[str(path)] = identity(path)
        return document(path)
    result = closed(read(child(root, "result.json")), "version label status started_at run_seconds image context fee_catalog_identity groups error")
    need(result["status"] == "SUCCESS", "bench result: " + str(result.get("error") or result["status"])[:512])
    need(result["context"]["snapshot_sha256"] == snapshot_sha, "result snapshot binding")
    run = root / "run"
    configuration = closed(read(child(run, "run.json")), "version limits publisher python strategies transport")
    success = closed(read(child(run, "SUCCESS.json")), "version identity attempt terminal outputs")
    config_sha = hashlib.sha256(json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    need(configuration["version"] == 1 and success["version"] == 1 and success["identity"] == config_sha
         and re.fullmatch(r"[0-9a-f]{32}", success["attempt"]) is not None
         and type(success["terminal"]) is int and success["terminal"] >= 2, "supervisor identity")
    expected_outputs = {g: success["attempt"] + "/" + g + "/output" for g in configuration["transport"]["groups"]}
    need(success["outputs"] == expected_outputs, "supervisor output paths")
    attempt = read(child(run, success["attempt"] + "/result.json"))
    need(attempt == {"version": 1, "identity": config_sha, "attempt": success["attempt"], "outcome": "success",
                     "fatal": False, "progress": success["terminal"], "terminal": success["terminal"],
                     "participants": {g: 0 for g in ["publisher", *expected_outputs]}}, "attempt completion evidence")
    for g in expected_outputs:
        attest = read(child(run, success["attempt"] + "/" + g + "/complete.json"))
        need(attest == {"version": 1, "identity": config_sha, "attempt": success["attempt"], "group": g,
                       "terminal": success["terminal"]}, "group completion evidence")
    need(group in success["outputs"], "missing completed group")
    strategy = configuration["strategies"][group]
    need(canonical_reference(strategy["factory"]) == "replay.strategies." + FACTORIES[lens] + ":build", "factory/lens binding")
    need(strategy["config"]["snapshot_sha256"] == snapshot_sha, "configured snapshot binding")
    output = child(run, success["outputs"][group])
    manifest = read(output / "manifest.json")
    receipt = closed(read(output / "content_receipt.json"), "version semantic_sha256 run_id attempt_id group identity terminal")
    expected = {"version": 1, "semantic_sha256": digest(manifest), "run_id": configuration["transport"]["run_id"],
                "attempt_id": success["attempt"], "group": group, "identity": success["identity"], "terminal": success["terminal"]}
    need(receipt == expected and manifest["snapshot_sha256"] == snapshot_sha, "content/supervisor binding")
    selected = [g for g in result["groups"] if g["name"] == group]
    need(len(selected) == 1 and selected[0]["receipt"] == receipt and all(selected[0]["checks"].values()), "bench group binding")
    need(manifest["policy"] == strategy["config"]["policy"], "policy/configuration binding")
    need(digest(read(output / "summary.json")) == manifest["summary_sha256"], "summary binding")
    paths = [run / "run.json", run / "SUCCESS.json", child(run, success["attempt"] + "/result.json"),
             child(run, success["attempt"] + "/" + group + "/complete.json"), output / "manifest.json",
             output / "content_receipt.json", output / "summary.json"]
    for name, expected_file in manifest["files"].items():
        p = child(output, name)
        have = identity(p)
        bindings[str(p)] = have
        committed = expected_file.get("stored", expected_file)
        need(have == {k: committed[k] for k in ("sha256", "byte_length")}, "output file identity")
        paths.append(p)
    config = plain(strategy["config"])
    if "fees" in config:
        original = config["fees"]["catalog_directory"]
        if original.startswith("/bench/fees/"):
            original = str(child(root, "fees/" + config["fees"]["catalog_identity"]))
        fees = safe_path(original)
        # Catalogue manifest owns the finite input set; do not scan unrelated directories.
        catalog = document(fees / "manifest.json")
        for p in (fees / "manifest.json", fees / "receipt.json"):
            paths.append(p)
        for p in fees.iterdir():
            if p.name.startswith(("source-", "schedule-")) and p.suffix in (".blob", ".json"):
                paths.append(child(fees, p.name))
        need(len(paths) <= 12000 and type(catalog) is dict, "catalog input bound")
        config["fees"]["catalog_directory"] = str(fees)
    for p in paths:
        bindings[str(p)] = identity(p)
    return {"output": output, "manifest": manifest, "config": config, "source_commit": result["image"]["source_commit"]}
