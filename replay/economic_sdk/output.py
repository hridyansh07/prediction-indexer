"""Output file layouts and the common row header shared by every strategy."""

from replay.economic_sdk.types import CONTROL, REAL
from replay.streams.protocol import require

# Layout 1 is the frozen complement V1 wire: measurements and slices mix real
# and placebo rows, placebo episodes have their own file, all detail is full.
_V1 = {("measurements", REAL): "measurements.ndjson", ("measurements", CONTROL): "measurements.ndjson",
       ("episodes", REAL): "episodes.ndjson", ("episodes", CONTROL): "placebo_episodes.ndjson",
       ("slices", REAL): "slices.ndjson", ("slices", CONTROL): "slices.ndjson"}
_PREFIX = {REAL: "", CONTROL: "control_"}
_NEEDS = {"intervals": ("measurements",), "episodes": ("measurements", "episodes", "slices"),
          "slices": ("measurements", "episodes", "slices")}


class Layout:
    """Maps (stream, entity class) to a file; real and control never share a file in V2."""

    def __init__(self, experiment):
        self.version = experiment.layout
        classes = (REAL, CONTROL) if experiment.controls else (REAL,)
        if self.version == 1:
            for cls in (REAL, CONTROL):
                require(experiment.detail[cls] == "slices", "layout 1 is full detail only")
            self.names = dict(_V1)
        else:
            self.names = {(stream, cls): _PREFIX[cls] + stream + ".ndjson"
                          for cls in classes for stream in _NEEDS[experiment.detail[cls]]}
        self.files = tuple(sorted(set(self.names.values())))

    def name(self, stream, cls):
        return self.names[stream, cls]

    def streams(self, cls):
        return tuple(stream for (stream, c) in self.names if c == cls)


def row_common(version, experiment, scope, entity, start, end):
    return {"version": version, "experiment_sha256": experiment, "scope": scope,
            "entity": entity, "start_ns": str(start), "end_ns": str(end)}
