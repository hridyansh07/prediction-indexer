"""Game-aware bounded minimum-payout optimizer, independent scenario accounts."""
from .strategy import build
from .output import read_provisional, read_completed, check

__all__ = ['build','read_provisional','read_completed','check']
