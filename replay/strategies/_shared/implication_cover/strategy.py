"""One shared pure implication-cover evaluator for same- and cross-venue routes."""

from dataclasses import replace

from replay.strategies.cross_venue_arbitrage.strategy import CrossVenueArbitrage
from replay.strategies.cross_venue_arbitrage.contract import UNIT, SETTLEMENT, sizes
from replay.strategies._shared.implication_cover.contract import Inputs, payoff
from replay.strategies._shared.implication_cover.output import ImplicationReader, validate_content
from replay.streams.protocol import require


class ImplicationCover(ImplicationReader, CrossVenueArbitrage):
    """Reuse the existing two-leg BUY evaluator; price the full outcome vector.

    Each mask is nonempty and non-total. Strict implication gives three
    nonempty regions: A, B minus A, and not B. After token fees their payouts
    are received(B), received(B) + received(not A), and received(not A).
    Thus the shared evaluator's minimum-received floor is exact here too.
    """

    mode = None

    def __init__(self, config):
        self.inputs = Inputs(config, self.mode)
        self.snapshot = self.inputs.snapshot
        self.snapshot_sha256 = self.inputs.prepared.sha256
        ImplicationReader.__init__(self, self.inputs.policy, self.inputs.identity, self.inputs.valuation,
                                   self.inputs.bridge.semantic_config, self.snapshot, self.mode, self.inputs.bridge)
        self.sizes = tuple(int(s) for s in sizes(self.policy))

    def evaluate(self, entity, views, context):
        observation = super().evaluate(entity, views, context)
        if observation.payload is None:
            return observation
        values = dict(observation.payload)
        size, proof = int(entity.descriptor["size_contracts"]), entity.descriptor["implication"]
        gross = payoff(proof, (size * UNIT, size * UNIT))
        net = (payoff(proof, tuple(int(leg["received_e36"]) for leg in values["native_legs"]))
               if values["fee_status"] == "KNOWN" else None)
        values.update(payoffs_gross_e36=[str(v) for v in gross],
                      payoffs_net_e36=None if net is None else [str(v) for v in net],
                      payout_middle_gross_e36=str(max(gross)),
                      payout_middle_extra_gross_e36=str(max(gross) - min(gross)),
                      payout_middle_net_e36=None if net is None else str(max(net)),
                      payout_middle_extra_net_e36=None if net is None else str(max(net) - min(net)))
        return replace(observation, payload=values)

    def manifest(self, files, instantaneous):
        result = CrossVenueArbitrage.manifest(self, files, instantaneous)
        result.update(version=1, venue_mode=self.mode)
        return result

    def validate(self, directory, snapshot, manifest, *, state_bytes=128 * 1024**2):
        require(self.bound, "missing initial")
        require(manifest["venue_mode"] == self.mode and manifest["settlement_model"] == SETTLEMENT,
                "implication runtime mode")
        return validate_content(directory, snapshot, manifest, self.bridge, state_bytes=state_bytes)
