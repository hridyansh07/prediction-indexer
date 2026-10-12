"""Closed research configuration (configs/research.json, version 1)."""

from __future__ import annotations

import json
import re
from pathlib import Path

from replay.research.jsonio import ResearchError, require

FIELDS = {"research_version", "lenses", "game_time_windows", "overview_max_buckets", "tile_minutes"}
PROFILE_FIELDS = {"group", "adapter", "label"}
EPISODE_FIELDS = PROFILE_FIELDS | {"headline_kind", "value_scale", "value_unit"}
WINDOW_BASES = ("kalshi_market_close", "close_minus_duration", "kalshi_milestone_end")
NAME = re.compile(r"[a-z][a-z0-9_]{0,31}")


def _closed(value, fields, label):
    require(type(value) is dict and set(value) == set(fields), f"{label}: expected exactly {sorted(fields)}")
    return value


def _count(value, label, low, high):
    require(type(value) is int and low <= value <= high, f"{label} must be an integer in [{low}, {high}]")
    return value


def _text(value, label):
    require(type(value) is str and 0 < len(value) <= 128, f"{label} must be a nonempty string")
    return value


def load(path):
    raw = Path(path).read_bytes()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ResearchError(f"research config is not JSON: {error}") from error
    return validate(value)


def validate(value):
    config = _closed(value, FIELDS, "research config")
    require(config["research_version"] == 1 and type(config["research_version"]) is int,
            "research_version must be 1")
    lenses = config["lenses"]
    require(type(lenses) is dict and lenses, "lenses must be a nonempty object")
    groups, profiles = set(), 0
    for name, lens in lenses.items():
        require(NAME.fullmatch(name) is not None, f"lens name {name!r}")
        require(type(lens) is dict and "adapter" in lens, f"lens {name}: adapter required")
        if lens["adapter"] == "profile":
            _closed(lens, PROFILE_FIELDS, f"lens {name}")
            profiles += 1
        elif lens["adapter"] == "sdk_episodes":
            _closed(lens, EPISODE_FIELDS, f"lens {name}")
            _text(lens["headline_kind"], f"lens {name} headline_kind")
            _count(lens["value_scale"], f"lens {name} value_scale", 0, 72)
            _text(lens["value_unit"], f"lens {name} value_unit")
        else:
            raise ResearchError(f"lens {name}: unknown adapter {lens['adapter']!r}")
        _text(lens["group"], f"lens {name} group")
        _text(lens["label"], f"lens {name} label")
        require(lens["group"] not in groups, f"group {lens['group']} mapped twice")
        groups.add(lens["group"])
    require(profiles == 1 and "profile" in lenses and lenses["profile"]["adapter"] == "profile",
            "exactly one profile lens, named profile")
    windows = _closed(config["game_time_windows"], WINDOW_BASES, "game_time_windows")
    for basis, window in windows.items():
        _closed(window, {"before_ms", "after_ms"}, f"game_time_windows.{basis}")
        _count(window["before_ms"], f"{basis}.before_ms", 0, 86_400_000)
        _count(window["after_ms"], f"{basis}.after_ms", 0, 86_400_000)
    _count(config["overview_max_buckets"], "overview_max_buckets", 16, 100_000)
    _count(config["tile_minutes"], "tile_minutes", 1, 1440)
    return config
