"""Economic strategy SDK (docs/ECONOMIC_STRATEGY_SDK_V1.md).

A strategy declares the book data it needs, the baskets it evaluates and a
pure ``evaluate``; the SDK owns time, staging, views, intervals, episodes,
slices, controls, bounded output and the independent reader.
"""

from replay.economic_sdk.types import (
    ADMISSIONS, CONTROL, DEPTH_LIMITED, EVALUATED, ONE_SIDED, REAL, SDK_STATUSES, UNUSABLE,
    Basket, BookRequirement, Context, Control, Experiment, FillPolicy, FillSizing, FillSpec,
    GameRequirement, Observation, ProbePolicy, Requirements,
)
from replay.economic_sdk.views import BookView

__all__ = [
    "ADMISSIONS", "CONTROL", "DEPTH_LIMITED", "EVALUATED", "ONE_SIDED", "REAL",
    "SDK_STATUSES", "UNUSABLE", "Basket", "BookRequirement", "BookView", "Context",
    "Control", "Experiment", "FillPolicy", "FillSizing", "FillSpec", "Observation",
    "GameRequirement", "ProbePolicy", "Requirements", "Strategy", "factory",
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
    probe_decision_evidence = False
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

    # -- fill mode (spec §13), runtime and reader -------------------------------
    def fill_spec(self, entity):
        """Static ``FillSpec`` of an admitted entity: per-leg ladder source and units."""
        raise NotImplementedError

    def fill_exits(self, entity):
        """Static per-leg ``(book_key, ladder)`` for excess unwind.

        The default preserves the reverse ladder on the entry source book.
        Strategies whose acquired book differs override this hook.
        """
        from replay.economic_sdk.probes import EXIT_SOURCE
        return tuple((key, EXIT_SOURCE[source])
                     for key, source in zip(entity.legs, self.fill_spec(entity).sources))

    def fill_value(self, entity, steps, legs, time):
        """Pure value of ``steps`` basket steps on per-leg ``Fill`` objects, or ``None``.

        ``time`` is the fill's open time (the episode's ``start_ns``), so a
        dated fee schedule can resolve. The value must be an integer that falls
        as any leg's price rises and depend only on these arguments (never on
        sequence, scope or other context): the reader recomputes it from the
        fill the episode carries and its start time.
        """
        raise NotImplementedError

    def fill_delays(self, entity, time_ns):
        """Pure per-leg taker delays in ns, or ``None`` when unknown.

        The default reads the pinned outcomes document. A strategy may override
        this hook; runtime and independent readers call the same pure contract.
        """
        from replay.economic_sdk.entities import Entity
        from replay.economic_sdk.execution_terms import execution_delays
        return execution_delays(self.snapshot, entity.legs, time_ns,
                                entity=entity if isinstance(entity, Entity) else None)

    def fill_loss(self, entity, native_losses, time_ns):
        """Value signed per-leg gross unwind losses in ``fill_value`` units.

        ``native_losses`` is a tuple in leg order, each at that leg's native
        price-times-quantity scale. The strategy supplies exact scale/asset
        conversion using only these arguments and pinned inputs. No unwind
        fees enter this hook. Unknown conversion fails instead of adding
        unlike units or assuming the strategy's value scale.
        """
        from replay.streams.protocol import require
        require(False, "probe loss valuation hook required")

    def fill_flags(self, entity, ages):
        """Optional closed flags for a fill episode; absent for legacy policies."""
        return None

    def check_fill_flags(self, value, entity, ages):
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

    def retention(config, snapshot):
        from replay.economic_sdk.retention import configured
        strategy = strategy_class(config)
        game = strategy.requirements(strategy.snapshot, strategy.experiment.policy).game
        return configured(snapshot, config, timers=game.timers if game is not None else ())

    def sections(config):
        return frozenset(("record", "events")) if config.get("policy", {}).get("profile") is not None else frozenset(("record",))

    build.sections = sections
    build.retention = retention
    return build
