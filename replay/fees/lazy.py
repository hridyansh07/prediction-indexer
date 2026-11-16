"""Deferred assessment identities; no deferred representation enters the wire."""

from collections.abc import Sequence


class LazyAssessmentIds(Sequence):
    """Rebuild an immutable order through the full engine only when IDs are read.

    The request owns the exact inputs, not an earlier evaluation's identities.
    Equality between deferred handles compares those inputs without hashing.
    """

    __slots__ = ("request", "expected", "_ids")

    def __init__(self, request, expected=None):
        self.request, self.expected, self._ids = request, expected, None

    def materialize(self):
        if self._ids is None:
            ids = tuple(result.identity for result in self.request.assessments())
            if self.expected is not None and ids != self.expected:
                raise AssertionError("fee lazy identity crosscheck mismatch")
            self._ids = ids
        return list(self._ids)

    def __len__(self):
        return len(self.request.levels)

    def __getitem__(self, index):
        return self.materialize()[index]

    def __iter__(self):
        return iter(self.materialize())

    def __eq__(self, other):
        if isinstance(other, LazyAssessmentIds):
            return self.request == other.request
        if isinstance(other, (list, tuple)):
            return self.materialize() == list(other)
        return NotImplemented


def assessment_json(value, *, placeholders=False):
    """Return ordinary JSON containers, or exact-length IDs for a line preflight.

    SHA-256 IDs are always 64 ASCII characters. Placeholders therefore preserve
    encoded byte length without constructing or hashing any fee assessments.
    """
    if isinstance(value, LazyAssessmentIds):
        return ["0" * 64] * len(value) if placeholders else value.materialize()
    if type(value) is dict:
        return {k: assessment_json(v, placeholders=placeholders) for k, v in value.items()}
    if type(value) in (list, tuple):
        return [assessment_json(v, placeholders=placeholders) for v in value]
    return value
