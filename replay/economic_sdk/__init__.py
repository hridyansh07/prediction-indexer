"""Economic strategy SDK (docs/ECONOMIC_STRATEGY_SDK_V1.md).

A strategy declares the book data it needs, the baskets it evaluates and a
pure ``evaluate``; the SDK owns time, staging, views, intervals, episodes,
slices, controls, bounded output and the independent reader.
"""

from replay.economic_sdk.types import (
    ADMISSIONS, CONTROL, DEPTH_LIMITED, EVALUATED, ONE_SIDED, REAL, SDK_STATUSES, UNUSABLE,
    Basket, BookRequirement, Context, Control, Experiment, Observation, Requirements,
)
from replay.economic_sdk.views import BookView

__all__ = [
    "ADMISSIONS", "CONTROL", "DEPTH_LIMITED", "EVALUATED", "ONE_SIDED", "REAL",
    "SDK_STATUSES", "UNUSABLE", "Basket", "BookRequirement", "BookView", "Context",
    "Control", "Experiment", "Observation", "Requirements", "Strategy", "factory",
]


class Strategy:
    """Base class. A configured instance sets ``experiment`` and ``snapshot``.

    Runtime instances are built from the supervisor configuration; reader
    instances (``for_reader``) are built from a manifest and never see fees or
    filesystem paths. Neither keeps state between callbacks.
    """

    # Explicit opt-in for strategies that price every leg in its native scales.
    native_scales = False
    experiment = None
    snapshot = None

    # -- runtime ---------------------------------------------------------------
    def bind(self, initial):
        raise NotImplementedError

    @property
    def bound(self):
        raise NotImplementedError

    def requirements(self, snapshot, policy):
        raise NotImplementedError

    def baskets(self, snapshot, policy, scope_index):
        raise NotImplementedError

    def control_descriptor(self, basket, control, replacement, admission):
        raise NotImplementedError

    def evaluate(self, entity, views, context):
        raise NotImplementedError

    def manifest(self, files, instantaneous):
        raise NotImplementedError

    def validate(self, directory, snapshot, manifest):
        raise NotImplementedError

    # -- reader hooks ----------------------------------------------------------
    def extra_files(self):
        return ()

    def check_measurement(self, row, entity):
        pass

    def open_facts(self, values, entity, kind):
        return None

    def check_open(self, values, facts, entity, kind, opening_class, where):
        pass

    def check_quotes(self, quotes, values, entity):
        pass

    def summary_key(self, entity):
        raise NotImplementedError

    def duration_label(self, status, value_class):
        return (value_class or status).lower() + "_ns"

    def summarize(self, aggregates, manifest, snapshot):
        raise NotImplementedError


def factory(strategy_class):
    """Wrap a ``Strategy`` subclass into a supervisor ``build(context)`` factory."""
    from replay.economic_sdk.runtime import Runtime

    def build(context):
        return Runtime(strategy_class(context["config"]), context)

    return build
