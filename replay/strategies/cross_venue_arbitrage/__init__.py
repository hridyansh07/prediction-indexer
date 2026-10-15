"""Cross venue arbitrage factory and independent readers."""

STRATEGY_VERSION = 1

from .strategy import CrossVenueArbitrage, build
from .output import read_completed, read_provisional, validate_content

__all__ = ['CrossVenueArbitrage', 'build', 'read_completed', 'read_provisional', 'validate_content']
