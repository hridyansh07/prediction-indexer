"""Build one content-addressed research version from committed corpus results (spec §2, §3)."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from replay.economic_sdk.availability_reader import book_id
from replay.research import game, metadata, overlays, series, tables
from replay.research.inputs import discover
from replay.research.jsonio import (ResearchError, dumps, file_identity, fsync_directory, identity, read_json,
                                    require, write_bytes)

PACK_VERSION = 1
CORPUS_MANIFEST_VERSION = 1


class Pack:
    """Writes one event's files and records their identities for its manifest."""

    def __init__(self, root):
        self.root, self.files = Path(root), {}

    def put(self, relative, document):
        data = dumps(document)
        self.files[relative] = write_bytes(self.root / relative, data)
        return data


def availability_document(output, books):
    path = Path(output) / "availability.ndjson"
    if not path.is_file():
        return {"availability_version": 1, "state": "missing", "books": []}
    index = {book_id({"instrument": b["instrument"], "orientation": b["orientation"]}): b["book"] for b in books}
    intervals = {b["book"]: [] for b in books}
    with path.open("rb") as handle:
        for line in handle:
            row = json.loads(line)
            book = index.get(row["entity"])
            if book is None:
                continue
            reason = row.get("reason")
            intervals[book].append({"start_ns": row["start_ns"], "end_ns": row["end_ns"], "scope": row["scope"],
                                    "state": row["state"],
                                    "reason": reason.get("kind") if type(reason) is dict else reason})
    return {"availability_version": 1, "state": "ok",
            "books": [{"book": book, "intervals": intervals[book]} for book in sorted(intervals)]}


def build_pack(event, root, config, game_state_root):
    """Write the pack; return ``(manifest bytes, lens episode rows, header facts)``."""
    pack = Pack(root)
    profile = event.output("profile")
    transition_books, transition_scopes = series.profile_tables(profile)
    books = metadata.book_rows(transition_books, event.context)
    header = metadata.event_header(event.bundle_id, event.context)
    scopes = metadata.scope_rows(transition_scopes)
    capture_start, capture_end = int(scopes[0]["start_ns"]), int(scopes[-1]["end_ns"])

    tiles_written = []

    def write_tile(kind, minute, document):
        tiles_written.append((kind, minute))
        pack.put(f"tiles/{kind}/{minute}.json", document)

    overview, tiles = series.build_series(
        series.transitions_rows(profile), books=transition_books, scopes=transition_scopes,
        max_buckets=config["overview_max_buckets"], tile_minutes=config["tile_minutes"], write_tile=write_tile)
    pack.put("overview.json", overview)
    pack.put("availability.json", availability_document(profile, books))
    document, how = game.prepared_document(event, game_state_root)
    game_json = game.game_json(document, how, config["game_time_windows"])
    pack.put("game.json", game_json)

    lenses, rows = {}, []
    for name, lens in config["lenses"].items():
        state = {k: v for k, v in event.lenses[name].items() if k != "output"}
        if state["state"] == "ok" and lens["adapter"] == "sdk_episodes":
            overlay = overlays.episode_intervals(event.output(name), name, lens, transition_books)
            pack.put(f"overlays/{name}.json", overlay)
            state["overlay"] = f"overlays/{name}.json"
            state["episodes"] = len(overlay["items"])
            rows += tables.episode_rows(event.bundle_id, name, overlay["items"], books=books,
                                        capture_end_ns=capture_end, game=header["game"])
        lenses[name] = state

    manifest = {
        "pack_version": PACK_VERSION, **header, "bundle_id": event.bundle_id, "context": event.context_state,
        "capture_start_ns": str(capture_start), "capture_end_ns": str(capture_end), "scopes": scopes,
        "books": books, "lenses": lenses, "unmapped_groups": event.unmapped_groups,
        "game_state": {"state": game_json["state"], "reason": game_json["reason"], "file": "game.json"},
        "provenance": {"corpus_key": event.corpus_key, "corpus_receipt_sha256": event.receipt_sha256,
                       "attempt": event.attempt, "snapshot_sha256": event.result["context"]["snapshot_sha256"],
                       "source_commit": event.result["image"].get("source_commit"),
                       "source_dirty": event.result["image"].get("source_dirty"),
                       "bench_status": event.result["status"]},
        "overview": {"bucket_ns": overview["bucket_ns"], "buckets": overview["buckets"]},
        "tiles": tiles, "files": dict(sorted(pack.files.items())),
    }
    data = dumps(manifest)
    write_bytes(Path(root) / "manifest.json", data)
    facts = {"capture_start_ns": capture_start, "duration_s": (capture_end - capture_start) / 1e9,
             "venues": sorted({b["venue"] for b in books}), "book_count": len(books),
             "scope_count": len(scopes), "game_state": game_json["state"], **header}
    return data, rows, facts


def event_row(event, config, pack_state, pack_error, facts, rows):
    header = facts or metadata.event_header(event.bundle_id, event.context)
    row = {"bundle_id": event.bundle_id, "event_id": header.get("event_id"), "corpus_key": event.corpus_key,
           "state": event.state, "message": event.message, "pack": pack_state, "pack_error": pack_error,
           "context": event.context_state, "game": header.get("game"), "title": header.get("title"),
           "capture_start_ns": None if facts is None else facts["capture_start_ns"],
           "duration_s": None if facts is None else facts["duration_s"],
           "venues": None if facts is None else facts["venues"],
           "book_count": None if facts is None else facts["book_count"],
           "scope_count": None if facts is None else facts["scope_count"],
           "game_state": None if facts is None else facts["game_state"],
           "bench_status": None if event.result is None else event.result["status"]}
    for name, lens in config["lenses"].items():
        state = event.lenses.get(name, {"state": "not_run"})
        mine = [r for r in rows if r["lens"] == name]
        headline = [r for r in mine if lens["adapter"] == "sdk_episodes" and r["kind"] == lens["headline_kind"]]
        nets = [r["net"] for r in headline if r["net"] is not None]
        row[f"{name}_state"] = state["state"]
        row[f"{name}_error"] = state.get("error") or state.get("reason")
        row[f"{name}_episodes"] = len(mine) if state["state"] == "ok" and lens["adapter"] == "sdk_episodes" else None
        row[f"{name}_headline"] = len(headline) if row[f"{name}_episodes"] is not None else None
        row[f"{name}_max_net"] = max(nets, default=None)
    return row


def build(key_dirs, out, config, *, config_bytes, game_state_root=None, log=print):
    """Build into ``out``; return ``(version, failures)``. ``current.json`` is replaced last."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    events, superseded, inputs = discover(key_dirs, config)
    staging = Path(tempfile.mkdtemp(prefix=".build-", dir=out))
    try:
        packs, event_rows, episodes, failures = {}, [], [], []
        for event in events:
            pack_state, pack_error, facts, rows = None, None, None, []
            if event.state == "ok":
                if event.lenses["profile"]["state"] != "ok":
                    pack_state = "no_profile"
                else:
                    try:
                        data, rows, facts = build_pack(event, staging / "events" / event.bundle_id, config,
                                                       game_state_root)
                        packs[event.bundle_id] = {"pack": f"events/{event.bundle_id}/manifest.json",
                                                  **identity(data)}
                        pack_state = "ok"
                    except Exception as error:   # recorded per event and reported; never silent
                        shutil.rmtree(staging / "events" / event.bundle_id, ignore_errors=True)
                        pack_state, pack_error, rows = "build_failed", f"{type(error).__name__}: {error}", []
                        failures.append((event.bundle_id, pack_error))
            log(f"{event.bundle_id} state={event.state} pack={pack_state}"
                + (f" error={pack_error}" if pack_error else ""))
            event_rows.append(event_row(event, config, pack_state, pack_error, facts, rows))
            episodes += rows
        checks = [check for event in events for check in event.checks]
        files = {}
        tables_out = {
            "events.parquet": tables.parquet_bytes(event_rows, tables.event_schema(list(config["lenses"]))),
            "episodes.parquet": tables.parquet_bytes(episodes, tables.EPISODE_SCHEMA),
            "verify.json": dumps({"verify_version": 1, "checks": checks}),
            "events.json": dumps(tables.events_index(config, event_rows)),
        }
        summary = tables.summarize(config, event_rows, episodes)
        tables_out["summary.json"] = dumps(summary)
        for name, data in tables_out.items():
            files[name] = write_bytes(staging / name, data)
        manifest = {"corpus_manifest_version": CORPUS_MANIFEST_VERSION,
                    "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
                    "inputs": inputs, "superseded": superseded, "events": packs, "files": files}
        # viability.md names the version, so it is listed after the version is fixed: the
        # version hashes everything else, and the report is derived only from what it hashes.
        version = hashlib.sha256(dumps(manifest)).hexdigest()[:16]
        report = tables.viability(config, version, inputs, superseded, event_rows, episodes, summary)
        manifest["files"]["viability.md"] = write_bytes(staging / "viability.md", report)
        manifest["version"] = version
        manifest_data = dumps(manifest)
        write_bytes(staging / "manifest.json", manifest_data)
        _sync_tree(staging)
        target = out / version
        if target.exists():
            _verify_existing(target, staging)
            shutil.rmtree(staging)
        else:
            os.rename(staging, target)
            fsync_directory(out)
        pointer = {"version": version, "manifest_key": f"{version}/manifest.json", **{
            "manifest_" + k: v for k, v in identity(manifest_data).items()}}
        temporary = out / ".current.json.tmp"
        temporary.unlink(missing_ok=True)
        write_bytes(temporary, dumps(pointer))
        os.replace(temporary, out / "current.json")
        fsync_directory(out)
        return version, failures
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def _sync_tree(root):
    for directory, _, _ in os.walk(root, topdown=False):
        fsync_directory(directory)


def _verify_existing(target, staging):
    """An existing version is reused only when it holds exactly the same bytes."""
    mine = {p.relative_to(staging).as_posix() for p in staging.rglob("*") if p.is_file()}
    theirs = {p.relative_to(target).as_posix() for p in target.rglob("*") if p.is_file()}
    require(mine == theirs, f"existing version {target.name} differs in its file set; refusing to rewrite")
    for relative in sorted(mine):
        if file_identity(staging / relative) != file_identity(target / relative):
            raise ResearchError(f"existing version {target.name} differs at {relative}; refusing to rewrite")


def verify_only(key_dirs, config):
    events, superseded, inputs = discover(key_dirs, config)
    return {"verify_version": 1, "inputs": inputs, "superseded": superseded,
            "checks": [check for event in events for check in event.checks]}


def read_current(out):
    pointer = read_json(Path(out) / "current.json")
    return pointer["version"]
