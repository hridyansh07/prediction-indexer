"""Exact integer ladder walking shared by replay economic strategies."""

from __future__ import annotations

import sys
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Fill:
    filled_atoms: int
    cost: int
    depth_limited: bool
    taken: tuple[tuple[int, int], ...]
    consumed: tuple[tuple[int, int], ...]


def retained_size(fill: Fill) -> int:
    """Return a conservative shallow-graph size for one detached fill.

    Fill's schema is closed, so this avoids the much more expensive generic
    object traversal used by the strategy's retained-state accounting. Scalar
    objects are charged on every occurrence, which can only overcount sharing.
    """
    def pairs_size(values: tuple[tuple[int, int], ...]) -> int:
        return sys.getsizeof(values) + sum(
            sys.getsizeof(pair) + sys.getsizeof(pair[0]) + sys.getsizeof(pair[1])
            for pair in values
        )

    return (
        sys.getsizeof(fill)
        + sys.getsizeof(fill.filled_atoms)
        + sys.getsizeof(fill.cost)
        + sys.getsizeof(fill.depth_limited)
        + pairs_size(fill.taken)
        + pairs_size(fill.consumed)
    )


def walk(
    levels: tuple[tuple[int, int], ...], sizes_atoms: tuple[int, ...]
) -> tuple[Fill, ...]:
    """Walk ``levels`` once, returning an exact fill for each requested size.

    Levels are already best-first. Sizes must be positive, sorted, and unique;
    this makes the single traversal and result correspondence unambiguous.
    """
    if type(levels) is not tuple or type(sizes_atoms) is not tuple:
        raise TypeError("levels and sizes must be tuples")
    if not sizes_atoms or any(type(size) is not int or size <= 0 for size in sizes_atoms):
        raise ValueError("positive integer sizes required")
    if sizes_atoms != tuple(sorted(set(sizes_atoms))):
        raise ValueError("sizes must be sorted and unique")
    for level in levels:
        if (
            type(level) is not tuple
            or len(level) != 2
            or any(type(value) is not int for value in level)
            or level[0] < 0
            or level[1] <= 0
        ):
            raise ValueError("levels require nonnegative prices and positive quantities")

    results: list[Fill] = []
    level_index = 0
    remaining = levels[0][1] if levels else 0
    filled = cost = 0
    taken: list[tuple[int, int]] = []
    consumed: list[tuple[int, int]] = []
    for target in sizes_atoms:
        while filled < target and level_index < len(levels):
            price, displayed = levels[level_index]
            amount = min(remaining, target - filled)
            if remaining == displayed:
                taken.append((price, amount))
                consumed.append((price, displayed))
            else:
                prior_price, prior_quantity = taken[-1]
                taken[-1] = (prior_price, prior_quantity + amount)
            filled += amount
            cost += price * amount
            remaining -= amount
            if remaining == 0:
                level_index += 1
                if level_index < len(levels):
                    remaining = levels[level_index][1]
        results.append(
            Fill(
                filled_atoms=min(filled, target),
                cost=cost,
                depth_limited=filled < target,
                taken=tuple(taken),
                consumed=tuple(consumed),
            )
        )
    return tuple(results)
