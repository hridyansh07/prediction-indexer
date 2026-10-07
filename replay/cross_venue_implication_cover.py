"""Cross-venue routes using the shared implication-cover core."""

from functools import partial

from replay.economic_sdk import factory
from replay.implication_cover import ImplicationCover
from replay.implication_output import read_completed as _completed, read_provisional as _provisional


class CrossVenueImplicationCover(ImplicationCover):
    name = "cross_venue_implication_cover"
    mode = "cross_venue"


build = factory(CrossVenueImplicationCover)
read_completed = partial(_completed, mode="cross_venue")
read_provisional = partial(_provisional, mode="cross_venue")
