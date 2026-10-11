"""Closed corpus run spec."""

from __future__ import annotations

import json
from pathlib import Path

FIELDS = {"version", "universe_base_url", "runner_config", "canonical_store", "work_root", "results_root",
          "ledger", "materializer", "image", "redis", "limits", "fees", "game_state", "bundles", "groups"}
GROUP_FIELDS = {"name", "factory", "reader", "config", "checks", "compare"}
CORPUS_TOKENS = {"@corpus:fees", "@corpus:valuation"}


def _closed(value, fields, label):
    if type(value) is not dict or set(value) != set(fields):
        raise ValueError(f"{label}: expected exactly {sorted(fields)}")
    return value


def load(path):
    spec = _closed(json.loads(Path(path).read_bytes()), FIELDS, "corpus spec")
    if spec["version"] != 1:
        raise ValueError("corpus spec version")
    for key in ("runner_config", "canonical_store", "materializer"):
        if not Path(spec[key]).exists():
            raise ValueError(f"corpus spec {key} does not exist: {spec[key]}")
    _closed(spec["image"], {"build_context", "dockerfile"}, "image")
    _closed(spec["redis"], {"image", "maxmemory"}, "redis")
    _closed(spec["limits"], {"attempt_seconds", "run_seconds", "stall_seconds"}, "limits")
    _closed(spec["fees"], {"template_catalog", "sources", "kalshi_member_class", "limitless_buy_bps",
                           "limitless_sell_bps"}, "fees")
    _closed(spec["game_state"], {"prepare"}, "game_state")
    if type(spec["game_state"]["prepare"]) is not bool:
        raise ValueError("game_state.prepare must be a boolean")
    _closed(spec["bundles"], {"only", "exclude"}, "bundles")
    for key in ("only", "exclude"):
        if spec["bundles"][key] is not None and not (type(spec["bundles"][key]) is list
                                                    and all(type(b) is str for b in spec["bundles"][key])):
            raise ValueError(f"bundles.{key} must be null or a list of bundle ids")
    names = set()
    if type(spec["groups"]) is not list or not spec["groups"]:
        raise ValueError("groups must be a nonempty list")
    for group in spec["groups"]:
        _closed(group, GROUP_FIELDS, "group")
        if group["name"] in names:
            raise ValueError(f"duplicate group {group['name']}")
        names.add(group["name"])
    return spec


def substitute(value, replacements):
    """Replace whole-string corpus tokens with per-bundle objects; bench tokens pass through."""
    if type(value) is dict:
        return {k: substitute(v, replacements) for k, v in value.items()}
    if type(value) is list:
        return [substitute(v, replacements) for v in value]
    if type(value) is str and value.startswith("@corpus:"):
        if value not in CORPUS_TOKENS:
            raise ValueError(f"unknown corpus token {value}")
        return replacements[value]
    return value
