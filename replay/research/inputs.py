"""Discover committed corpus results and classify every event and lens (spec §1, §4).

Nothing here interprets strategy output. It reads the corpus receipt, the bench
``result.json``, the supervisor's ``SUCCESS.json``, per-group ``failure.json`` and
the committed context, and records one explicit state for each of them.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path

from replay.corpus import results as corpus_results
from replay.research.jsonio import ResearchError, read_json

BUNDLE = re.compile(r"bundle_[0-9a-f]{24}")
ATTEMPT = re.compile(r"[0-9a-f]{32}")
MAX_MESSAGE = 2000


@dataclass
class Event:
    """One corpus result and everything the builder decided about it."""

    bundle_id: str
    root: Path
    key_prefix: str
    corpus_key: str | None = None
    state: str = "ok"              # ok | uncommitted | receipt_invalid | context_invalid
    message: str | None = None
    receipt: dict | None = None
    receipt_sha256: str | None = None
    result: dict | None = None
    attempt: str | None = None
    attempt_error: str | None = None
    context: dict | None = None
    context_state: str = "missing"  # ok | missing | invalid
    game_state_document: dict | None = None
    lenses: dict = field(default_factory=dict)
    unmapped_groups: list = field(default_factory=list)
    checks: list = field(default_factory=list)

    def check(self, name, passed, message=None):
        self.checks.append({"bundle_id": self.bundle_id, "check": name, "pass": bool(passed),
                            "message": None if message is None else str(message)[:MAX_MESSAGE]})

    def output(self, lens):
        state = self.lenses.get(lens)
        return None if state is None or state["state"] != "ok" else state["output"]


def _sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def discover(key_dirs, config, *, prefer_later=False):
    """Return (events sorted by bundle id, superseded, inputs).

    Verified commits win over incomplete attempts. Ambiguous commits require
    explicit preference; every losing directory remains visible.
    """
    candidates, chosen, superseded, inputs = {}, {}, [], []
    for key_dir in key_dirs:
        key_dir = Path(key_dir)
        if not key_dir.is_dir():
            raise ResearchError(f"not a corpus key directory: {key_dir}")
        bundles = sorted(p for p in key_dir.iterdir() if p.is_dir() and BUNDLE.fullmatch(p.name))
        corpus_keys = set()
        for root in bundles:
            event = classify(root, key_dir.name, config)
            if event.corpus_key is not None:
                corpus_keys.add(event.corpus_key)
            candidates.setdefault(event.bundle_id, []).append(event)
        if len(corpus_keys) > 1:
            raise ResearchError(f"{key_dir} holds results of several corpus keys: {sorted(corpus_keys)}")
        key = next(iter(corpus_keys), None)
        if key is not None and not key.startswith(key_dir.name):
            raise ResearchError(f"{key_dir} name does not prefix its corpus key {key}")
        # Incomplete attempts inherit only the key verified elsewhere in this
        # directory, never a fabricated 64-character identity from its prefix.
        for root in bundles:
            event = candidates[root.name][-1]
            if event.corpus_key is None:
                event.corpus_key = key
        inputs.append({"key_prefix": key_dir.name, "corpus_key": key, "bundles": len(bundles)})
    for bundle, events in candidates.items():
        committed = [e for e in events if e.receipt_sha256 is not None and e.state != "receipt_invalid"]
        if len(committed) > 1 and not prefer_later:
            raise ResearchError(f"{bundle}: multiple committed results; use --prefer-later: "
                                + ", ".join(e.key_prefix for e in committed))
        winner = (committed or events)[-1]
        chosen[bundle] = winner
        for event in events:
            if event is winner:
                continue
            row = {"bundle_id": bundle, "key_prefix": event.key_prefix,
                   "corpus_key": event.corpus_key, "by": winner.key_prefix}
            if event not in committed:
                row["state"] = "superseded_uncommitted"
            superseded.append(row)
    superseded.sort(key=lambda row: (row["bundle_id"], row["key_prefix"]))
    return [chosen[b] for b in sorted(chosen)], superseded, inputs


def classify(root, key_prefix, config):
    event = Event(bundle_id=root.name, root=root, key_prefix=key_prefix)
    if not (root / corpus_results.RECEIPT).is_file():
        event.state, event.message = "uncommitted", "no corpus_receipt.json"
        event.check("corpus_receipt", False, event.message)
        return _lenses_without_result(event, config, "result is uncommitted")
    try:
        event.receipt = corpus_results.verify(root)
    except Exception as error:   # any reader fault makes the result unusable, visibly
        event.state, event.message = "receipt_invalid", f"{type(error).__name__}: {error}"
        event.check("corpus_receipt", False, event.message)
        return _lenses_without_result(event, config, "corpus receipt is invalid")
    event.check("corpus_receipt", True)
    event.corpus_key = event.receipt["corpus_key"]
    event.receipt_sha256 = _sha256(root / corpus_results.RECEIPT)
    if event.receipt["bundle_id"] != event.bundle_id:
        event.state, event.message = "receipt_invalid", "receipt bundle_id differs from its directory"
        event.check("corpus_receipt", False, event.message)
        return _lenses_without_result(event, config, event.message)
    event.result = read_json(root / "result.json")
    status = event.result["status"]
    event.check("bench_status", status == "SUCCESS", status)
    _choose_attempt(event)
    _context(event)
    _game_state(event)
    _lens_states(event, config)
    return event


def _lenses_without_result(event, config, message):
    for name, lens in config["lenses"].items():
        event.lenses[name] = {"state": "failed", "label": lens["label"], "error": message}
    return event


def _choose_attempt(event):
    run = event.root / "run"
    marker = run / "SUCCESS.json"
    attempts = sorted(p.name for p in run.iterdir() if p.is_dir() and ATTEMPT.fullmatch(p.name)) \
        if run.is_dir() else []
    if marker.is_file():
        attempt = read_json(marker).get("attempt")
        if type(attempt) is str and attempt in attempts:
            event.attempt = attempt
        else:
            event.attempt_error = f"SUCCESS.json names a missing attempt: {attempt!r}"
    elif len(attempts) == 1:
        event.attempt = attempts[0]
    elif not attempts:
        event.attempt_error = "no attempt directory"
    else:
        event.attempt_error = "ambiguous attempts: " + ", ".join(attempts)
    event.check("attempt", event.attempt is not None, event.attempt_error or event.attempt)


def _context(event):
    directory = event.root / "context"
    if not (directory / "context.json").is_file():
        event.check("context_binding", True, "missing")
        return
    try:
        receipt = read_json(directory / "receipt.json")
        actual = _sha256(directory / "context.json")
        expected = event.result["context"]["snapshot_sha256"]
        if receipt.get("snapshot_sha256") != actual or actual != expected:
            raise ResearchError(f"context sha256 {actual} vs receipt {receipt.get('snapshot_sha256')} "
                                f"vs result {expected}")
        event.context = read_json(directory / "context.json")
        event.context_state = "ok"
        event.check("context_binding", True, "ok")
    except Exception as error:
        event.context_state = "invalid"
        event.state, event.message = "context_invalid", f"{type(error).__name__}: {error}"
        event.check("context_binding", False, event.message)


def _game_state(event):
    path = event.root / "game_state" / "game_state.json"
    if path.is_file():
        event.game_state_document = read_json(path)


def _failure_message(event, group):
    if event.attempt is not None:
        failure = event.root / "run" / event.attempt / group / "failure.json"
        if failure.is_file():
            try:
                errors = read_json(failure)["errors"]
                return " <- ".join(f"{e['type']}: {e['message']}" for e in errors)[:MAX_MESSAGE]
            except Exception as error:
                return f"unreadable failure.json: {type(error).__name__}: {error}"
    error = event.result.get("error")
    if error is not None:
        return f"{error['type']}: {error['message']}"[:MAX_MESSAGE]
    return None


def _lens_states(event, config):
    groups = {g["name"]: g for g in event.result["groups"]}
    excluded = {row["group"]: row["reason"] for row in event.receipt["excluded_groups"]}
    mapped = {lens["group"] for lens in config["lenses"].values()}
    event.unmapped_groups = sorted((set(groups) | set(excluded)) - mapped)
    readers = checks = True
    for name, lens in config["lenses"].items():
        group, base = lens["group"], {"label": lens["label"]}
        if group in excluded:
            event.lenses[name] = {**base, "state": "excluded", "reason": excluded[group]}
            continue
        if group not in groups:
            event.lenses[name] = {**base, "state": "not_run"}
            continue
        entry = groups[group]
        failed = [check for check, passed in entry["checks"].items() if not passed]
        readers = readers and entry["receipt"] is not None
        checks = checks and not failed
        if "unavailable" in entry:
            message = f"game_state_unavailable: {entry['unavailable']['reason']}"
        elif event.result["status"] == "FAILED" or entry["receipt"] is None:
            message = _failure_message(event, group) or "no reader receipt"
        elif failed:
            message = "checks failed: " + ", ".join(sorted(failed))
        elif event.attempt is None:
            message = event.attempt_error
        else:
            output = event.root / "run" / event.attempt / group / "output"
            event.lenses[name] = {**base, "state": "ok", "output": output,
                                  "semantic_sha256": entry["receipt"].get("semantic_sha256")}
            continue
        event.lenses[name] = {**base, "state": "failed", "error": message}
    event.check("group_readers", readers)
    event.check("group_checks", checks)
