"""Same venue implication cover factory and independent readers."""

STRATEGY_VERSION = 1

from .strategy import SameVenueImplicationCover, build, read_completed, read_provisional

__all__ = ['SameVenueImplicationCover', 'build', 'read_completed', 'read_provisional']
