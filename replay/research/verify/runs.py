"""Exact-input-bound verification report and fresh re-verification for every build."""
from gamestate.timeline import latest
from replay.research import inputs
from replay.streams.protocol import ProtocolError
from replay.research.io import document, identity, need, write
from replay.research.verify.profile import check_profile
from replay.research.verify.fills import check_episodes


def inspect(config_path, runs_path, *, store=None):
    config = inputs.configuration(config_path)
    runs = inputs.run_list(runs_path, config)
    bindings = {str(inputs.safe_path(p)): identity(p) for p in (config_path, runs_path)}
    report = {"research_verify_version": 1, "inputs": bindings, "archive_inputs": {}, "events": []}
    resolved, seen = {}, set()
    for entry in runs["events"]:
        snapshot, event_id, snapshot_sha = inputs.context(entry["context"], entry["bundle_id"], bindings)
        need(event_id not in seen, "duplicate immutable event")
        seen.add(event_id)
        event = {"event_id": event_id, "bundle_id": entry["bundle_id"], "lenses": {}}
        data = {"snapshot": snapshot, "snapshot_sha256": snapshot_sha, "lenses": {}}
        for lens, declaration in sorted(config["lenses"].items()):
            if lens not in entry["runs"]:
                event["lenses"][lens] = {"state": "not_run"}
                continue
            run = entry["runs"][lens]
            try:
                completed = inputs.completed(run, declaration["group"], lens, snapshot_sha, bindings)
                m, root = completed["manifest"], completed["output"]
                checks = (check_profile(root, snapshot, m["files"]) if lens == "profile"
                          else check_episodes(root, snapshot, m, completed["config"]))
                event["lenses"][lens] = {"state": "ok", "run": run, "semantic_sha256": inputs.digest(m),
                                         "checks": [{"check": k, "pass": True, "message": str(v)} for k, v in sorted(checks.items())]}
                data["lenses"][lens] = completed
            except (ValueError, OSError, KeyError, TypeError, ProtocolError) as error:
                event["lenses"][lens] = {"state": "failed", "run": run, "error": str(error)[:1024],
                                         "checks": [{"check": "verification", "pass": False, "message": str(error)[:1024]}]}
        game = latest(store, event_id) if store is not None else {"state": "no_source", "inputs": {}}
        report["archive_inputs"][event_id] = game["inputs"]
        event["game_state"] = game["state"]
        data["game"] = game
        resolved[event_id] = data
        report["events"].append(event)
    # A read concurrent with a producer must not bind different bytes to the result.
    need(all(identity(p) == expected for p, expected in bindings.items()), "inputs changed during verification")
    return report, resolved, config


def verify(config_path, runs_path, destination, *, store=None):
    report, _, _ = inspect(config_path, runs_path, store=store)
    write(destination, report)
    return report


def bound_report(config_path, runs_path, verification, *, store=None):
    recorded = document(verification)
    report, resolved, config = inspect(config_path, runs_path, store=store)
    need(recorded == report, "verify.json exact-input/check binding changed; verify again")
    return report, resolved, config
