"""Prepare one immutable, hash-bound game file per pinned event, before replay."""

import argparse
import hashlib
from pathlib import Path

from gamestate import timeline
from gamestate.prepared import project
from replay.game_state import MAX_BYTES, align, context_header, load, validate
from replay.preparation import load_snapshot
from replay.streams.protocol import require
from replay.supervisor import write_json_durable, fsync_directory


def prepare_game_state(context_directory, output_directory, *, store):
    context = load_snapshot(context_directory)
    header, participants = context_header(context)
    document = {"version": 1, **header, "state": "unavailable", "reason": "no_source", "source": None,
                "competitors": align({"home": None, "away": None}, participants),
                "scheduled_start_ns": None, "segment_kind": None, "segments": [], "match": None}
    markets = context["outcomes"]["document"]["markets"]
    if any(row["venue"] == "kalshi" for row in markets):
        selected = timeline.latest(store, header["event_id"])
        if selected["state"] in ("no_source", "no_fetch"):
            document["reason"] = "incomplete" if selected.get("rejected_fetches", 0) else "no_fetch"
        elif selected["state"] != "ok":
            document["reason"] = "incomplete"
        else:
            source = project(store, selected)
            document.update(source=source["source"], competitors=align(source["labels"], participants),
                            scheduled_start_ns=source["scheduled_start_ns"], segment_kind=source["segment_kind"])
            if source["complete"]:
                document.update(state="ok", reason=None, segments=source["segments"], match=source["match"])
            else:
                document["reason"] = "incomplete"
    validate(document, context)
    root = Path(output_directory)
    require(not root.exists() and not root.is_symlink(), "game state output already exists")
    root.parent.mkdir(parents=True, exist_ok=True)
    root.mkdir()
    fsync_directory(root.parent)
    path = root / "game_state.json"
    write_json_durable(path, document)
    require(path.stat().st_size <= MAX_BYTES, "game state byte bound")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    load(path, digest, context)  # independent pinned reader, including unavailable results
    return digest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_dir", type=Path)
    parser.add_argument("out_dir", type=Path)
    args = parser.parse_args(argv)
    from archive.storage.factory import build_store
    try:
        digest = prepare_game_state(args.context_dir, args.out_dir,
                                    store=build_store(primary_roots=[args.context_dir]))
    except (ValueError, OSError) as error:
        parser.error(str(error))
    print(digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
