"""Pure game views, declared policy and a destructive in-memory release queue."""

from collections import deque
from dataclasses import dataclass, replace
from pathlib import Path

from replay.game_state import SIDES, natural, score
from replay.preparation import sha
from replay.strategy_sdk import plain
from replay.streams.protocol import freeze, obj, require, uint

ANCHORS = ("scheduled", "segment_start", "segment_end", "segment_settled", "match_end")
PHASES = ("unavailable", "pre_match", "in_segment", "between_segments", "finished")


class GameStateUnavailable(ValueError):
    code = "game_state_unavailable"

    def __init__(self, reason):
        self.reason = reason
        super().__init__(self.code + ":" + reason)


def game_policy(value):
    value = obj(plain(value), "required input windows segment_start priors")
    require(type(value["required"]) is bool, "game required flag")
    source = obj(value["input"], "path sha256")
    require(type(source["path"]) is str and Path(source["path"]).is_absolute(), "absolute game state path")
    require(not any(p.startswith(".env") or p.endswith((".key", ".pem")) or p in ("credentials", "secrets")
                    for p in Path(source["path"]).parts), "secret game paths forbidden")
    sha(source["sha256"])
    obj(value["windows"], "segment_start segment_end match_end")
    for window in value["windows"].values():
        obj(window, "after_ms"); natural(window["after_ms"])
    require(value["segment_start"] in ("estimated", "at_segment_end"), "game start mode")
    require(type(value["priors"]) is dict, "game priors")
    for name, prior in value["priors"].items():
        require(type(name) is str and bool(name), "game prior name")
        obj(prior, "segment_duration_ns between_segments_ns settlement_delay_ns")
        for amount in prior.values():
            uint(amount)
    return value


def binding(policy):
    value = game_policy(policy)
    return {"sha256": value["input"]["sha256"], "windows": value["windows"], "mode": value["segment_start"]}


def experiment_policy(policy):
    """Semantic experiment input: pin bytes and decisions, independent of mounts."""
    if "game" not in policy:
        return policy
    value = game_policy(policy["game"])
    return {**policy, "game": {**value, "input": {"sha256": value["input"]["sha256"]}}}


def check_binding(manifest):
    policy = manifest["policy"].get("game")
    require(("game_state" in manifest) == (policy is not None), "game state binding presence")
    if policy is not None:
        require(manifest["game_state"] == binding(policy), "game state manifest binding")


def manifest_fields(manifest):
    check_binding(manifest)
    return " game_state" if "game_state" in manifest else ""


@dataclass(frozen=True, slots=True)
class SegmentResult:
    index: int
    winner: str | None
    end_ns: int
    details: object


@dataclass(frozen=True, slots=True)
class GameView:
    phase: str
    segment: int | None
    phase_since_ns: int
    score: object
    last_segment: SegmentResult | None
    settled: int | None
    revision: int
    reason: str | None
    sport: str
    game: str | None
    segment_kind: str | None
    market_types: tuple
    competitors: object


@dataclass(frozen=True, slots=True)
class Fact:
    kind: str
    release_ns: int
    source_ns: int
    index: int | None = None
    value: object = None


class Timeline:
    """Only pending facts plus current state; no archived document is retained."""

    def __init__(self, document, policy, run_start):
        policy = game_policy(policy)
        available = document["state"] == "ok"
        if policy["required"] and not available:
            raise GameStateUnavailable(document["reason"])
        document = freeze(plain(document))
        self.view = GameView("pre_match" if available else "unavailable", None, run_start,
                             freeze({"home": 0, "away": 0}), None, None, 0, document["reason"],
                             document["sport"], document["game"], document["segment_kind"],
                             tuple(document["market_types"]), document["competitors"])
        self.time = None
        facts = []
        if available:
            scheduled = document["scheduled_start_ns"]
            facts.append(Fact("scheduled", run_start, run_start if scheduled is None else scheduled))
            for segment in document["segments"]:
                index, end = segment["index"], segment["end_ns"]
                end_release = end + policy["windows"]["segment_end"]["after_ms"] * 1_000_000
                if segment["start_ns"] is not None:
                    start = segment["start_ns"]
                    release = (end_release if policy["segment_start"] == "at_segment_end" else
                               start + policy["windows"]["segment_start"]["after_ms"] * 1_000_000)
                    facts.append(Fact("segment_start", release, start, index, segment["start_estimated"]))
                facts.append(Fact("segment_end", end_release, end, index,
                                  (segment["winner"], segment["details"])))
                if segment["settled_ns"] is not None:
                    facts.append(Fact("segment_settled", segment["settled_ns"], segment["settled_ns"], index))
            if document["match"] is not None:
                match = document["match"]
                release = match["end_ns"] + policy["windows"]["match_end"]["after_ms"] * 1_000_000
                facts.append(Fact("match_end", release, match["end_ns"], value=match))
        # Reject windows that would invert the known sequence of phases. Settlement
        # is independent and can precede a delayed segment-end release.
        phases = [f.release_ns for f in facts if f.kind in ("segment_start", "segment_end", "match_end")]
        require(phases == sorted(phases), "game release windows invert phases")
        require(all(f.release_ns <= 2**64 - 1 for f in facts), "game release time overflow")
        priority = {"scheduled": -1, "segment_start": 0, "segment_end": 1, "segment_settled": 2, "match_end": 3}
        self.facts = deque(sorted(facts, key=lambda f: (
            f.release_ns, 2**64 if f.kind == "match_end" else f.index or 0, priority[f.kind])))

    @property
    def next_time(self):
        return self.facts[0].release_ns if self.facts else None

    def advance(self, time):
        require(self.time is None or time >= self.time, "game release time order")
        applied = []
        while self.facts and self.facts[0].release_ns <= time:
            fact = self.facts.popleft()
            view = self.view
            changes = {"revision": view.revision + 1}
            if fact.kind == "segment_start":
                changes.update(phase="in_segment", segment=fact.index, phase_since_ns=fact.release_ns)
            elif fact.kind == "segment_end":
                scores = dict(view.score)
                winner, details = fact.value
                if winner is not None:
                    scores[winner] += 1
                changes.update(phase="between_segments", segment=fact.index, phase_since_ns=fact.release_ns,
                               score=freeze(scores), last_segment=SegmentResult(fact.index, winner, fact.source_ns, details))
            elif fact.kind == "segment_settled":
                changes["settled"] = max(view.settled or 0, fact.index)
            elif fact.kind == "match_end":
                changes.update(phase="finished", phase_since_ns=fact.release_ns, score=fact.value["score"])
            self.view = replace(view, **changes)
            applied.append(fact)
        self.time = time
        return applied


def episode_game(view):
    return {"phase": view.phase, "segment": view.segment, "score": dict(view.score), "revision": view.revision}


def check_episode_game(value):
    obj(value, "phase segment score revision")
    require(value["phase"] in PHASES, "episode game phase")
    natural(value["segment"], nullable=True); natural(value["revision"]); score(value["score"])
    require(value["segment"] is None or value["segment"] > 0, "episode game segment")
    require(value["phase"] not in ("pre_match", "unavailable") or value["segment"] is None,
            "episode game phase/segment")
    require(value["phase"] not in ("in_segment", "between_segments") or value["segment"] is not None,
            "episode game phase/segment")
    require(value["phase"] not in ("pre_match", "unavailable") or not any(value["score"].values()),
            "episode game phase/score")
    require(value["phase"] != "unavailable" or value["revision"] == 0, "episode game unavailable revision")


def elapsed_in_phase(view, now_ns):
    return None if view.phase == "unavailable" else max(0, now_ns - view.phase_since_ns)


def _prior(view, priors):
    return priors.get(view.game)


def expected_segment_end(view, priors):
    prior = _prior(view, priors)
    return (view.phase_since_ns + int(prior["segment_duration_ns"])
            if view.phase == "in_segment" and prior is not None else None)


def expected_settlement(view, priors):
    prior = _prior(view, priors)
    end = expected_segment_end(view, priors) if view.phase == "in_segment" else (
        view.last_segment.end_ns if view.last_segment is not None else None)
    return end + int(prior["settlement_delay_ns"]) if end is not None and prior is not None else None


def participant_score(view, participant):
    return next((view.score[s] for s in SIDES if view.competitors[s]["participant"] is not None
                 and view.competitors[s]["participant"] == participant), None)
