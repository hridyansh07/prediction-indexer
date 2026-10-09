"""Offline event packs and backend records. No corpus orchestration or publication."""
from pathlib import Path

from archive.common.durable import fsync_directory
from archive.storage.base import ObjectExpectation
from encoder import StoredIdentity
from replay.economic_sdk.bounds import StateBudget, json_cost
from replay.research.charts import build_charts
from replay.research.io import child, closed, digest, document, encoded, identity, need, rows, safe_path, write, write_chunks
from replay.research.layout import effective_claim, layout
from replay.research.records import decimal, episodes, game_document, shared_sides
from replay.research.tables import write_table, verified_table
from replay.research.verify.runs import bound_report


def _array_chunks(header, field, records):
    # Canonical JSON order, including streaming arrays.
    keys = sorted([*header, field])
    yield b"{"
    for index, key in enumerate(keys):
        if index:
            yield b","
        yield encoded(key).rstrip(b"\n") + b":"
        if key != field:
            yield encoded(header[key]).rstrip(b"\n")
            continue
        yield b"["
        first = True
        for record in records:
            if not first:
                yield b","
            yield encoded(record).rstrip(b"\n")
            first = False
        yield b"]"
    yield b"}\n"


def build(config_path, runs_path, verification, destination, *, store=None):
    report, resolved, config = bound_report(config_path, runs_path, verification, store=store)
    root = safe_path(destination)
    need(not root.exists(), "immutable build destination exists")
    declarations = {safe_path(config_path), safe_path(runs_path)}
    need(not any(root.resolve().is_relative_to(safe_path(p).resolve().parent) for p in report["inputs"]
                 if safe_path(p) not in declarations), "output overlaps input evidence")
    verification_identity = identity(verification)
    write(root / "verify.json", report)
    files = {"verify.json": identity(root / "verify.json")}
    all_records, event_records = [], []
    budget = StateBudget()
    for event in sorted(report["events"], key=lambda e: e["event_id"]):
        event_id = event["event_id"]
        data = resolved[event_id]
        snapshot, lenses = data["snapshot"], event["lenses"]
        books, membership, _ = layout(snapshot)
        start, end = int(snapshot["scopes"][0]["start_ns"]), int(snapshot["scopes"][-1]["end_ns"])
        details = [e["detail"] for e in snapshot["evidence"] if "detail" in e]
        game = details[0].get("game") if details else None
        need(all(d.get("game") == game for d in details), "event game metadata changed")
        title = details[0].get("title") if details else None
        pack_name = "packs/" + event_id + "/manifest.json" if lenses["profile"]["state"] == "ok" else None
        event_row = {"event_id": event_id, "bundle_id": event["bundle_id"], "game": game, "title": title,
                     "capture_start_ns": str(start), "duration_s": decimal(end - start, 9),
                     "venues": sorted({b["venue"] for b in books}), "book_count": len(books), "scope_count": len(membership),
                     "game_state": event["game_state"], "pack": pack_name,
                     "pack_error": None if pack_name else lenses["profile"].get("error", "profile not_run"), "lenses": []}
        event_records.append(event_row)
        pack = None if pack_name is None else child(root, pack_name).parent
        pack_files = {}
        if pack is not None:
            pack.mkdir(parents=True)
            profile = data["lenses"]["profile"]
            pack_files = build_charts(pack, profile["output"], snapshot, profile["manifest"]["files"]["transitions.ndjson.zst"])
            game_data = game_document(data["game"], config["game_time_windows"], event_id)
            pack_files["game.json"] = write(pack / "game.json", game_data)
            ids = {"book:" + digest({"instrument": b["instrument"], "orientation": b["orientation"]}): b["book"] for b in books}
            availability = ({"book": ids[r["entity"]], **r} for r in rows(profile["output"], "availability.ndjson",
                            profile["manifest"]["files"]["availability.ndjson"]) if r["kind"] == "book" and r["entity"] in ids)
            pack_files["availability.json"] = write_chunks(pack / "availability.json", _array_chunks(
                {"availability_version": 1}, "intervals", availability))
        event_episodes = []
        pack_lenses = {}
        for lens, value in sorted(lenses.items()):
            state = value["state"]
            copied = {"state": state}
            if state == "failed":
                copied["error"] = value["error"]
            items = []
            if state == "ok":
                copied["semantic_sha256"] = value["semantic_sha256"]
                if lens != "profile":
                    for item, record in episodes(data["lenses"][lens], snapshot, event_id, lens, books):
                        budget.charge(json_cost(item) + json_cost(record) + len(record["sides"]) * 512,
                                      "research episode retained bound")
                        items.append(item)
                        event_episodes.append(record)
                    if pack is not None:
                        overlay = "overlays/" + lens + ".json"
                        pack_files[overlay] = write(pack / overlay, {"overlay_version": 1, "lens": lens, "kind": "intervals", "items": items})
                        copied["overlay"] = overlay
            event_row["lenses"].append({"lens": lens, "state": state, "error": value.get("error"), "episode_count": len(items)})
            pack_lenses[lens] = copied
        shared_sides(event_episodes)
        all_records.extend({k: v for k, v in record.items() if k != "sides"} for record in event_episodes)
        if pack is None:
            continue
        for b in books:
            key = b["instrument"], b["orientation"]
            claims = [{"scope": s, "claim": effective_claim(snapshot, s, key)} for s in range(len(membership)) if b["book"] in membership[s]]
            b["claims"] = claims
            common = claims[0]["claim"] if claims else None
            if any(c["claim"] != common for c in claims):
                common = None
            b["claim_id"] = None if common is None else common["claim_id"]
            b["effective_claim_id"] = None if common is None else common["effective_claim_id"]
        manifest = {"pack_version": 1, "event_id": event_id, "bundle_id": event["bundle_id"], "game": game, "title": title,
                    "participants": snapshot["outcomes"]["document"]["participants"], "capture_start_ns": str(start),
                    "capture_end_ns": str(end), "books": books, "lenses": pack_lenses,
                    "game_state": {"state": game_data["state"], "file": "game.json"},
                    "provenance": {"snapshot_sha256": data["snapshot_sha256"], "pins_sha256": digest(snapshot["config"]["pins"]),
                                   "source_commit": profile["source_commit"], "verify": verification_identity},
                    "files": {name: {**v, "stored": v} for name, v in sorted(pack_files.items())}}
        files[pack_name] = write(pack / "manifest.json", manifest)
        files.update({str((pack / name).relative_to(root)): v for name, v in pack_files.items()})
    files["events.parquet"] = write_table(root, "events", event_records)
    all_records.sort(key=lambda r: (r["event_id"], r["lens"], int(r["start_ns"]), r["episode_id"]))
    files["episodes.parquet"] = write_table(root, "episodes", all_records)
    need(identity(verification) == verification_identity and all(identity(p) == v for p, v in report["inputs"].items()),
         "exact inputs changed during build")
    receipt = {"research_build_version": 1, "verification": verification_identity, "files": dict(sorted(files.items()))}
    # Independently check every file before the receipt is the final commit marker.
    _audit_files(root, receipt)
    directories = {root}
    for name in files:
        directory = child(root, name).parent
        while directory != root:
            directories.add(directory)
            directory = directory.parent
    for directory in sorted(directories, key=lambda p: (-len(p.parts), str(p))):
        fsync_directory(directory)
    for inputs in report["archive_inputs"].values():
        for key, expected in inputs.items():
            framed = key.endswith("/responses.ndjson.zst")
            request = ObjectExpectation(key, StoredIdentity(**expected), None, None,
                                        "application/x-ndjson" if framed else "application/json",
                                        "zstd" if framed else None)
            with store.open_verified(request) as stream:
                while stream.read(1024**2):
                    pass
    write(root / "receipt.json", receipt)
    return audit(root)


def _audit_files(root, receipt):
    closed(receipt, "research_build_version verification files")
    need(type(receipt["research_build_version"]) is int and receipt["research_build_version"] == 1, "build receipt version")
    need(type(receipt["files"]) is dict and len(receipt["files"]) <= 200000, "build file bound")
    need({"verify.json", "events.parquet", "episodes.parquet"} <= set(receipt["files"]), "build required files")
    for name, expected in receipt["files"].items():
        closed(expected, "sha256 byte_length")
        need(identity(child(root, name)) == expected, "build file identity: " + name)
    need(receipt["files"]["verify.json"] == receipt["verification"], "verification identity binding")
    for name in ("events", "episodes"):
        verified_table(child(root, name + ".parquet"), name, receipt["files"][name + ".parquet"])
    for name in receipt["files"]:
        if name.startswith("packs/") and name.endswith("/manifest.json"):
            pack = document(child(root, name))
            closed(pack, "pack_version event_id bundle_id game title participants capture_start_ns capture_end_ns books lenses game_state provenance files")
            need(pack["pack_version"] == 1 and name == "packs/" + pack["event_id"] + "/manifest.json", "pack identity")
            for member, expected in pack["files"].items():
                closed(expected, "sha256 byte_length stored")
                key = str((child(root, name).parent / member).relative_to(root))
                child(root, key)
                need(expected["stored"] == receipt["files"].get(key) == {k: expected[k] for k in ("sha256", "byte_length")}, "pack file binding")
    return receipt


def audit(directory):
    root = safe_path(directory)
    return _audit_files(root, document(child(root, "receipt.json")))
