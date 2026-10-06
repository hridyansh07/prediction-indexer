"""Exact integer ladder walking shared by replay economic strategies."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Fill:
    filled_atoms: int
    cost: int
    depth_limited: bool
    taken: tuple[tuple[int, int], ...]
    consumed: tuple[tuple[int, int], ...]


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

    # Only levels the walk touches can affect a fill, so only those are
    # validated; deep books are not scanned past the largest size.
    def level(index):
        value = levels[index]
        if (
            type(value) is not tuple
            or len(value) != 2
            or type(value[0]) is not int
            or type(value[1]) is not int
            or value[0] < 0
            or value[1] <= 0
        ):
            raise ValueError("levels require nonnegative prices and positive quantities")
        return value

    results: list[Fill] = []
    level_index = 0
    remaining = level(0)[1] if levels else 0
    filled = cost = 0
    taken: list[tuple[int, int]] = []
    consumed: list[tuple[int, int]] = []
    for target in sizes_atoms:
        while filled < target and level_index < len(levels):
            price, displayed = levels[level_index]  # validated on entry
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
                    remaining = level(level_index)[1]
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


FILL_STOPS = ("target", "edge", "book_exhausted", "level_cap", "value_unknown")


@dataclass(frozen=True, slots=True)
class BasketFill:
    """One priced basket fill on fixed ladders; the same shape in every mode.

    ``steps`` basket steps took ``legs[i].filled_atoms == steps * units[i]``
    atoms from each leg. ``value`` is the caller's valuation at ``steps``, or
    ``None`` for zero steps, no valuation, or an unknown value.
    """

    steps: int
    legs: tuple[Fill, ...]
    stop: str
    value: int | None


def walk_basket(
    ladders: tuple[tuple[tuple[int, int], ...], ...],
    units: tuple[int, ...],
    *,
    targets: tuple[int, ...] = (),
    value=None,
    edge: bool = False,
    max_levels: int | None = None,
) -> tuple[BasketFill, ...]:
    """Price basket fills, walking every leg's best-first ladder in lockstep.

    One basket step takes ``units[i]`` atoms from leg ``i``, which carries both
    the leg ratio and each leg's native quantity scale. ``targets`` are sorted,
    unique step counts to price. With ``edge`` the walk keeps adding depth
    while ``value(steps, legs)`` strictly increases and stops at the smallest
    step count that maximizes it, the point where the marginal edge is gone.

    ``value`` must be concave in ``steps`` for the edge result to be the
    maximum: true whenever each leg's marginal cost per unit (price plus fee)
    rises with price, as it does for best-first asks. Candidates are the step
    counts on either side of every leg level boundary, where a piecewise
    linear concave value attains its maximum. ``max_levels`` bounds the levels
    any leg may consume.

    Returns one result per target, in order, then the edge result when asked.
    """
    if type(ladders) is not tuple or type(units) is not tuple or type(targets) is not tuple:
        raise TypeError("ladders, units and targets must be tuples")
    if not units or len(ladders) != len(units):
        raise ValueError("one positive unit per ladder required")
    if any(type(unit) is not int or unit <= 0 for unit in units):
        raise ValueError("one positive unit per ladder required")
    if any(type(target) is not int or target <= 0 for target in targets):
        raise ValueError("positive integer targets required")
    if targets != tuple(sorted(set(targets))):
        raise ValueError("targets must be sorted and unique")
    if not targets and not edge:
        raise ValueError("a target or the edge walk is required")
    if edge and value is None:
        raise ValueError("the edge walk requires a value")
    if max_levels is not None and (type(max_levels) is not int or max_levels <= 0):
        raise ValueError("max_levels must be a positive integer")

    boundaries, limit, capped = [], None, False
    for ladder, unit in zip(ladders, units):
        if type(ladder) is not tuple:
            raise TypeError("ladders must be tuples")
        usable = ladder if max_levels is None else ladder[:max_levels]
        cumulative, points = 0, []
        for level in usable:
            if (type(level) is not tuple or len(level) != 2 or type(level[0]) is not int
                    or type(level[1]) is not int or level[0] < 0 or level[1] <= 0):
                raise ValueError("levels require nonnegative prices and positive quantities")
            cumulative += level[1]
            points.append(cumulative)
        boundaries.append(points)
        steps = cumulative // unit
        if limit is None or steps < limit:
            limit = steps
    # The cap binds only when a leg's uncapped depth would allow more steps.
    if max_levels is not None:
        full = min(sum(level[1] for level in ladder) // unit for ladder, unit in zip(ladders, units))
        capped = full > limit
    exhausted = "level_cap" if capped else "book_exhausted"

    empty = Fill(0, 0, False, (), ())

    def legs_at(steps):
        if steps == 0:
            return tuple(empty for _ in ladders)
        return tuple(walk(ladder, (steps * unit,))[0] for ladder, unit in zip(ladders, units))

    def valued(steps, legs):
        if value is None or steps == 0:
            return None
        result = value(steps, legs)
        if result is not None and type(result) is not int:
            raise TypeError("value must return an int or None")
        return result

    results = []
    for target in targets:
        steps, stop = (target, "target") if target <= limit else (limit, exhausted)
        legs = legs_at(steps)
        results.append(BasketFill(steps, legs, stop, valued(steps, legs)))
    if edge:
        candidates = {limit}
        for points, unit in zip(boundaries, units):
            for point in points:
                candidates.add(point // unit)
                candidates.add(-(-point // unit))
        # Zero steps is worth exactly zero; reaching ``limit`` while the value
        # still rises means depth or the level cap ended the walk, not the edge.
        best, best_value, best_legs, stop = 0, 0, legs_at(0), exhausted
        for steps in sorted(c for c in candidates if 0 < c <= limit):
            legs = legs_at(steps)
            current = valued(steps, legs)
            if current is None:
                best, best_value, best_legs, stop = steps, None, legs, "value_unknown"
                break
            if current <= best_value:
                stop = "edge"
                break
            best, best_value, best_legs = steps, current, legs
        results.append(BasketFill(best, best_legs, stop, best_value if best else None))
    return tuple(results)
