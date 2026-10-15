"""Bundle coverage factory and independent readers."""

STRATEGY_VERSION = 1

from .strategy import Coverage, build
from .output import read_completed, read_provisional, validate_content

__all__ = ['Coverage', 'build', 'read_completed', 'read_provisional', 'validate_content']
