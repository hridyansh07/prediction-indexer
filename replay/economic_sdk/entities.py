"""Scope entities: strategy baskets plus SDK-constructed controls.

Shared by the runtime and the independent reader so both resolve the same
denominator. Controls never produce verdict inputs, only comparison rows.
"""

from __future__ import annotations

from replay.economic_sdk.types import ADMISSIONS, CONTROL, REAL, Basket
from replay.preparation import digest
from replay.streams.protocol import require


class Entity:
    __slots__ = ("id", "descriptor", "legs", "order", "admission", "admission_reasons",
                 "cls", "control", "shift", "basket")

    def __init__(self, descriptor, legs, order, admission, admission_reasons, cls,
                 control, shift, basket):
        self.id = digest(descriptor)
        self.descriptor, self.legs, self.order = descriptor, legs, order
        self.admission, self.admission_reasons = admission, admission_reasons
        self.cls, self.control, self.shift, self.basket = cls, control, shift, basket


def scale_admission(legs, plans, *, native_scales=False):
    require(type(native_scales) is bool, "native scale opt-in")
    if native_scales:
        return None
    scales = {(plans[key]["price_scale"], plans[key]["quantity_scale"]) for key in legs}
    return None if len(scales) == 1 else "UNSUPPORTED_SCALE"


def resolve(strategy, snapshot, scope_index, plans):
    """Return ``{entity_id: Entity}`` for one scope, real baskets first."""
    experiment = strategy.experiment
    baskets = strategy.baskets(snapshot, experiment.policy, scope_index)
    require(type(baskets) is tuple, "baskets must be a tuple")
    real = []
    for basket in baskets:
        admission = basket.admission
        require(type(basket) is Basket and type(basket.legs) is tuple
                and (basket.legs or admission is not None), "basket shape")
        require(admission is None or admission in ADMISSIONS[:3], "basket admission")
        reasons = basket.admission_reasons
        if admission is None:
            require(all(key in plans for key in basket.legs), "basket leg not planned")
            admission = scale_admission(basket.legs, plans, native_scales=getattr(strategy, "native_scales", False))
            reasons = ()
        real.append((basket, admission, reasons))

    result = {}

    def add(entity):
        require(entity.id not in result, "duplicate entity identity")
        result[entity.id] = entity

    peers = {}
    for basket, admission, _ in real:
        if admission is None and basket.control_leg is not None:
            peers.setdefault(basket.peer_group, []).append(basket)
    for group in peers.values():
        group.sort(key=lambda b: b.peer_order)

    for basket, admission, reasons in real:
        add(Entity(basket.descriptor, basket.legs, basket.order + (False, 0), admission,
                   reasons, REAL, None, None, basket))
        if basket.control_leg is None:
            continue
        for control in experiment.controls:
            legs, shift, replacement, control_admission, control_reasons = (
                basket.legs, None, None, admission, reasons)
            if control.kind == "cyclic_neighbor":
                if admission is None:
                    group = peers[basket.peer_group]
                    if len(group) < 2:
                        control_admission = "NO_MATCH"
                    else:
                        other = group[(group.index(basket) + 1) % len(group)]
                        index = basket.control_leg
                        replacement = other.descriptor["legs"][index]
                        legs = basket.legs[:index] + (other.legs[index],) + basket.legs[index + 1:]
                        control_admission = scale_admission(legs, plans, native_scales=getattr(strategy, "native_scales", False))
                    control_reasons = ()
                order = (True, 0)
            else:
                require(control.kind == "time_shift", "unknown control")
                if admission is None:
                    shift = (basket.control_leg, control.shift_ns)
                order = (True, control.shift_ns)
            descriptor = strategy.control_descriptor(basket, control, replacement,
                                                     control_admission)
            add(Entity(descriptor, legs, basket.order + order, control_admission,
                       control_reasons, CONTROL, control, shift, basket))
    return result
