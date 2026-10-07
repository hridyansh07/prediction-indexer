"""Output layouts.

Layout 1 is the frozen complement V1 wire (policy 1 only): a complete interval
partition in ``measurements.ndjson`` (real and placebo rows mixed), full
slices, and ``placebo_episodes.ndjson``.

Layout 2 is the SDK default. A strategy's result is its episodes; time per
status and value class per measured key is aggregated into denominators:

- ``entities.json`` and ``reasons.json``: tables that rows reference by index;
- ``denominators.ndjson``: one row per (scope, entity), writer-attested;
- ``episodes.ndjson`` and compact ``slices.ndjson``: real entities only;
- ``controls/<name>/``: per enabled control, its own entity table and
  denominators, plus episodes/slices only when the policy asks for them;
- ``audit/measurements.ndjson`` and ``audit/slices.ndjson``: the complete
  interval partition and full slices, only with ``audit_intervals``.
"""

from replay.economic_sdk.types import CONTROL, REAL
from replay.streams.protocol import require

_V1 = {("measurements", REAL): "measurements.ndjson", ("measurements", CONTROL): "measurements.ndjson",
       ("episodes", REAL): "episodes.ndjson", ("episodes", CONTROL): "placebo_episodes.ndjson",
       ("slices", REAL): "slices.ndjson", ("slices", CONTROL): "slices.ndjson"}


class Layout:
    """Layout 1 file map: (stream, entity class) -> file."""

    def __init__(self, experiment):
        require(experiment.layout == 1, "legacy layout")
        self.version = 1
        self.names = dict(_V1)
        self.files = tuple(sorted(set(self.names.values())))

    def name(self, stream, cls):
        return self.names[stream, cls]


def control_name(control):
    return control.kind if control.shift_ns is None else f"{control.kind}_{control.shift_ns}"


def group_of(entity):
    """Output group of an entity: ``""`` for real rows, else ``controls/<name>/``."""
    return "" if entity.cls == REAL else f"controls/{control_name(entity.control)}/"


def aggregate_files(experiment):
    """Every file layout 2 writes, by group, in a fixed order."""
    files = {"": ["entities.json", "reasons.json", "denominators.ndjson", "episodes.ndjson",
                  "slices.ndjson"]}
    if experiment.audit_intervals:
        files[""] += ["audit/measurements.ndjson", "audit/slices.ndjson"]
    for control in experiment.controls:
        names = ["entities.json", "denominators.ndjson"]
        if experiment.controls_episodes:
            names.append("episodes.ndjson")
        if experiment.controls_slices:
            names.append("slices.ndjson")
        files[f"controls/{control_name(control)}/"] = names
    return files


def file_list(experiment):
    return tuple(group + name for group, names in aggregate_files(experiment).items() for name in names)


def row_common(version, experiment, scope, entity, start, end):
    return {"version": version, "experiment_sha256": experiment, "scope": scope,
            "entity": entity, "start_ns": str(start), "end_ns": str(end)}
