"""Cross venue implication cover factory and independent readers."""

STRATEGY_VERSION = 1

from .strategy import CrossVenueImplicationCover, build, read_completed, read_provisional

__all__ = ['CrossVenueImplicationCover', 'build', 'read_completed', 'read_provisional']
