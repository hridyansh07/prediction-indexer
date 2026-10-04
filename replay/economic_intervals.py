"""Shared clocks and small interval primitives for economic replay strategies."""

from replay.streams.protocol import require


def changed_keys(cut):
    """Return detached keys named by a cut's book transitions."""
    if cut.kind != "cut":
        return frozenset()
    return frozenset(
        (item["key"]["instrument"], item["key"]["orientation"])
        for item in cut.body["book_transitions"]
    )


class CutClock:
    """Validate Replay cut time/window contracts and yield crossed scope bounds."""

    def __init__(self, snapshot):
        self.start = int(snapshot["config"]["start_ns"])
        self.end = int(snapshot["config"]["end_ns"])
        self.lower_bound = snapshot["config"]["lower_bound"]
        self.scopes = snapshot["scopes"]
        self.time = self.start
        self.scope = 0
        self.window = None

    def observe(self, cut):
        # The cut body is callback-owned.  Inspect it in place and retain only the
        # three scalar fields needed to validate later group cuts; copying the
        # whole origin on every cut needlessly makes origin size part of the hot
        # path.
        origin = cut.body["origin"]
        raw = int(origin["start_ns"] if origin["kind"] == "window" else origin["visible_ns"])
        if origin["kind"] == "window":
            if self.window is None:
                require(raw <= self.start < int(origin["end_ns"]), "first window range")
            else:
                require(origin["start_ns"] == self.window[1], "window partition")
            self.window = (origin["start_ns"], origin["end_ns"], origin["pin"])
        else:
            require(self.window is not None and origin["pin"] == self.window[2], "group window")
            require(int(self.window[0]) <= raw < int(self.window[1]), "group range")
            require(raw >= self.start or self.lower_bound == "expand_to_window_start", "group before requested start")
        effective = max(raw, self.start)
        require(self.time <= effective < self.end, "economic time order")
        # Observation, rather than scope advancement, owns chronology.  Some
        # cuts neither cross a scope nor change a book.
        self.time = effective
        return raw, effective

    def advance(self, time):
        """Yield ``(old_scope, boundary, new_scope)`` in chronological order."""
        while self.scope + 1 < len(self.scopes) and int(self.scopes[self.scope]["end_ns"]) <= time:
            boundary = int(self.scopes[self.scope]["end_ns"])
            old = self.scope
            self.scope += 1
            yield old, boundary, self.scope
        self.time = time

    def terminal(self):
        require(self.window is not None and int(self.window[1]) >= self.end, "incomplete window coverage")
        return self.end


class EpisodeMath:
    """Exact survival/qualified-time arithmetic shared by economic trackers."""

    @staticmethod
    def close(start, end, tiers):
        require(type(start) is int and type(end) is int and start <= end)
        survival = end - start
        return survival, [tier for tier in tiers if survival >= int(tier)], {
            tier: str(max(0, survival - int(tier))) for tier in tiers
        }
