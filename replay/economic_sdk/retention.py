"""Wire-v3 retention requirements, shared by configuration and participants."""

from replay.strategy_sdk import plain
from replay.streams.protocol import require, retention_list, uint


def required(snapshot, policy=None, *, timeline=None, timers=()):
    """Fixed crossings and offsets; book-relative deadlines belong to Rust."""
    start = uint(snapshot["config"]["start_ns"])
    boundaries = {start, *(uint(scope["end_ns"]) for scope in snapshot["scopes"])}
    offsets = set()
    for control in (policy or {}).get("controls", ()):
        if control["kind"] == "time_shift":
            offsets.update(uint(value) for value in control["shift_ns"])
    if timeline is not None:
        for fact in timeline.facts:
            boundaries.add(max(start, fact.release_ns))
            for anchor, offset in timers:
                if anchor == fact.kind:
                    due = max(start, fact.release_ns, fact.source_ns + offset)
                    require(due <= 2**64 - 1, "game timer overflow")
                    boundaries.add(due)
    result = {"retain_boundaries_ns": [str(t) for t in sorted(boundaries)],
              "retain_shift_offsets_ns": [str(t) for t in sorted(offsets)]}
    retention_list(result["retain_boundaries_ns"], 100_000)
    retention_list(result["retain_shift_offsets_ns"], 128)
    return result


def configured(snapshot, config, *, timers=()):
    """A configured strategy's pre-run requirements, without output writers."""
    config = plain(config)
    policy = config.get("policy", {})
    timeline = None
    if "game" in policy:
        from replay.economic_sdk.game import Timeline, game_policy
        from replay.game_state import load
        game = game_policy(policy["game"])
        timeline = Timeline(load(game["input"]["path"], game["input"]["sha256"], snapshot),
                            game, uint(snapshot["config"]["start_ns"]))
    return required(snapshot, policy, timeline=timeline, timers=timers)


def for_groups(snapshot, groups):
    """Union the actual participating groups, not the base request strategy."""
    union = {"retain_boundaries_ns": set(), "retain_shift_offsets_ns": set()}
    for group in groups:
        config = group["config"]
        # SDK factories expose declarations for game timers. Other factories
        # still receive context scopes and conventional policy offsets.
        if "game" in config.get("policy", {}):
            from replay.strategies import load_entrypoint
            factory = load_entrypoint(group["factory"])
            requirements = getattr(factory, "retention", None)
        else:
            requirements = None
        value = requirements(config, snapshot) if requirements else configured(snapshot, config)
        for field in union:
            union[field].update(uint(t) for t in value[field])
    result = {field: [str(t) for t in sorted(times)] for field, times in union.items()}
    retention_list(result["retain_boundaries_ns"], 100_000)
    retention_list(result["retain_shift_offsets_ns"], 128)
    return result


def check(initial, required):
    for field, label in (("retain_boundaries_ns", "boundary"),
                         ("retain_shift_offsets_ns", "time_shift offset")):
        supplied = set(initial[field])
        for value in required[field]:
            require(value in supplied, f"v3 retention does not cover {label} {value}")
