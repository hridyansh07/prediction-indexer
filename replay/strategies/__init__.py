"""Strategy packages and resolution of historical factory/reader references.

The loaders resolve old module names without rewriting a pinned run config.
Importing this package does not import any strategy implementation.
"""

import importlib

_LEGACY_MODULES = {
    'replay.bundle_coverage': 'replay.strategies.bundle_coverage',
    'replay.coverage_output': 'replay.strategies.bundle_coverage.output',
    'replay.same_venue_complement': 'replay.strategies.same_venue_complement',
    'replay.complement_contract': 'replay.strategies.same_venue_complement.contract',
    'replay.complement_output': 'replay.strategies.same_venue_complement.output',
    'replay.complement_fees': 'replay.strategies._shared.fee_bridge',
    'replay.cross_venue_arbitrage': 'replay.strategies.cross_venue_arbitrage',
    'replay.cross_venue_contract': 'replay.strategies.cross_venue_arbitrage.contract',
    'replay.cross_venue_output': 'replay.strategies.cross_venue_arbitrage.output',
    'replay.same_venue_multi_market': 'replay.strategies.same_venue_multi_market',
    'replay.same_venue_multi_market_contract': 'replay.strategies.same_venue_multi_market.contract',
    'replay.same_venue_multi_market_output': 'replay.strategies.same_venue_multi_market.output',
    'replay.same_venue_implication_cover': 'replay.strategies.same_venue_implication_cover',
    'replay.cross_venue_implication_cover': 'replay.strategies.cross_venue_implication_cover',
    'replay.implication_cover': 'replay.strategies._shared.implication_cover.strategy',
    'replay.implication_contract': 'replay.strategies._shared.implication_cover.contract',
    'replay.implication_output': 'replay.strategies._shared.implication_cover.output',
    'replay.market_profile': 'replay.strategies.market_profile',
}


def canonical_reference(reference):
    """Resolve a previously shipped module name; preserve the named attribute."""
    module, attribute = reference.split(":", 1)
    return _LEGACY_MODULES.get(module, module) + ":" + attribute


def load_entrypoint(reference):
    """Load a strategy factory, reader or check, including caller-owned modules."""
    module, attribute = canonical_reference(reference).split(":", 1)
    return getattr(importlib.import_module(module), attribute)
