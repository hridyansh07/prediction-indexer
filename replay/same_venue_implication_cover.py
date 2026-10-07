"""Same-venue routes using the shared implication-cover core."""

from functools import partial

from replay.economic_sdk import factory
from replay.implication_cover import ImplicationCover
from replay.implication_output import read_completed as _completed, read_provisional as _provisional


class SameVenueImplicationCover(ImplicationCover):
    name = "same_venue_implication_cover"
    mode = "same_venue"


build = factory(SameVenueImplicationCover)
read_completed = partial(_completed, mode="same_venue")
read_provisional = partial(_provisional, mode="same_venue")
