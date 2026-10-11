"""One prepared game input per fixture event, shared by every strategy line."""

import hashlib
from pathlib import Path

from archive.common.durable import confirm_durable
from archive.storage.factory import build_store
from replay.game_state import MAX_BYTES, load
from replay.preparation import load_snapshot
from replay.prepare_game_state import prepare_game_state
from replay.streams.protocol import require


def prepare_fixture_stage(context_directory, output_directory, *, store=None):
    """Resume only a verified existing file; never refresh prepared evidence."""
    root = Path(output_directory)
    path = root / "game_state.json"
    if root.exists():
        with path.open("rb") as stream:
            payload = stream.read(MAX_BYTES + 1)
        require(len(payload) <= MAX_BYTES, "game state byte bound")
        pin = hashlib.sha256(payload).hexdigest()
        load(path, pin, load_snapshot(context_directory))
        confirm_durable(path)
        return pin
    if store is None:
        store = build_store(primary_roots=[context_directory, root])
    return prepare_game_state(context_directory, root, store=store)
