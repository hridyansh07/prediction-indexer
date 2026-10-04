"""Profiling factory wrapper for strategy groups (SDK test utility).

``profiled(build, directory)`` returns a supervisor factory whose callable runs
``cProfile`` around strategy callbacks only (not the consumer or decoder), and
dumps cumulative statistics every ``every`` callbacks and at ``finish``. Dumps
go to ``directory``, never into the strategy's own output directory, so the
semantic output and its hashes are unchanged.
"""

import cProfile
from pathlib import Path


class _Profiled:
    def __init__(self, strategy, directory, every):
        self.strategy, self.directory, self.every = strategy, Path(directory), every
        self.profile = cProfile.Profile()
        self.calls = 0

    def __call__(self, cut):
        self.profile.enable()
        try:
            return self.strategy(cut)
        finally:
            self.profile.disable()
            self.calls += 1
            if self.every and self.calls % self.every == 0:
                self.dump()

    def finish(self):
        self.profile.enable()
        try:
            return self.strategy.finish()
        finally:
            self.profile.disable()
            self.dump()

    def dump(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.profile.dump_stats(self.directory / "callbacks.prof")

    def __getattr__(self, name):
        return getattr(self.strategy, name)


def profiled(build, directory, every=100_000):
    def factory(context):
        return _Profiled(build(context), directory, every)

    return factory
