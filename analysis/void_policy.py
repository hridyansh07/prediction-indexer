"""Structured abnormal-resolution policy parsed from venue rules text.

A partition pays $1 only when the event resolves normally. When it does not —
cancellation, abandonment, forfeit, disqualification, indefinite postponement —
the venues do materially different things, and those differences decide whether
a basket is genuinely locked or merely locked-if-nothing-weird-happens.

The differences are not venue-wide. Observed in the rules text of the markets
in this repository:

* **Polymarket esports** pays ``50-50`` on cancellation, so an all-YES basket of
  ``n`` legs pays ``n * 0.50`` — $1 only when ``n == 2``. A three-leg basket is
  implicitly long a cancellation.
* **Polymarket soccer** resolves a cancelled game to **"No"**. Every leg of an
  all-YES basket pays $0, so the basket is a total loss, not a wash.
* **Kalshi** resolves to the *fair market price*, an exchange determination that
  cannot be known in advance and is therefore modelled as indeterminate.
* Two siblings on the *same* venue can diverge on the *same* trigger: for a
  match that begins and is abandoned after an opponent forfeits, Polymarket's
  ``Match Winner`` resolves to the winning team (a normal outcome) while its
  ``Games Total`` resolves 50-50.

These branches are never tradable legs. Their only job is to qualify a lock
claim.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Triggers — why the event failed to produce a normal result.
# ---------------------------------------------------------------------------
TRIGGER_CANCELLED_PRE_PLAY = "cancelled_pre_play"
TRIGGER_NOT_COMPLETED = "not_completed"
TRIGGER_NOT_COMPLETED_OPPONENT_FORFEIT = "not_completed_opponent_forfeit"
TRIGGER_POSTPONED_BEYOND_WINDOW = "postponed_beyond_window"
TRIGGER_POSTPONED_INDEFINITE = "postponed_indefinite"
TRIGGER_FORFEIT_PRE_PLAY = "forfeit_pre_play"
TRIGGER_FORFEIT_POST_PLAY = "forfeit_post_play"
TRIGGER_ENDS_IN_TIE = "ends_in_tie"
TRIGGER_AMBIGUOUS_IDENTITY = "ambiguous_identity"

# ---------------------------------------------------------------------------
# Payoffs — what one YES contract pays when the trigger fires.
# ---------------------------------------------------------------------------
PAYOFF_FIFTY_FIFTY = "fifty_fifty"
PAYOFF_FAIR_MARKET_PRICE = "fair_market_price"
PAYOFF_OFFICIAL_RESULT = "official_result"
PAYOFF_RESOLVES_TO_WINNER = "resolves_to_winner"
PAYOFF_COUNTS_AS_COMPLETED = "counts_as_completed"
PAYOFF_RESOLVE_NO = "resolve_no"
PAYOFF_RESOLVE_YES = "resolve_yes"
PAYOFF_REMAINS_OPEN = "remains_open"

# Per-contract payoff of one YES leg. ``None`` means the venue reserves
# discretion, so no lock claim can be made.
PAYOFF_PER_CONTRACT: Mapping[str, float | None] = {
    PAYOFF_FIFTY_FIFTY: 0.5,
    PAYOFF_FAIR_MARKET_PRICE: None,
    PAYOFF_RESOLVE_NO: 0.0,
    # A leg forced to YES pays the full unit. Polymarket's soccer three-way
    # uses this to keep the partition intact on cancellation: the two team legs
    # are forced to No and the draw leg to Yes, so the basket still pays $1.
    PAYOFF_RESOLVE_YES: 1.0,
    # The remaining rules fold back into an ordinary settlement: exactly one
    # leg of the partition pays $1, which is what a normal outcome does.
    PAYOFF_OFFICIAL_RESULT: 1.0,
    PAYOFF_RESOLVES_TO_WINNER: 1.0,
    PAYOFF_COUNTS_AS_COMPLETED: 1.0,
    PAYOFF_REMAINS_OPEN: 1.0,
}

# Payoffs that do not open a failure branch — the market still settles normally.
NORMAL_RESOLUTION_PAYOFFS = frozenset(
    {
        PAYOFF_OFFICIAL_RESULT,
        PAYOFF_RESOLVES_TO_WINNER,
        PAYOFF_COUNTS_AS_COMPLETED,
        PAYOFF_REMAINS_OPEN,
    }
)


@dataclass(frozen=True)
class VoidRule:
    trigger: str
    payoff: str
    window_hours: float | None
    evidence: str

    @property
    def payoff_per_contract(self) -> float | None:
        return PAYOFF_PER_CONTRACT.get(self.payoff)

    @property
    def opens_branch(self) -> bool:
        return self.payoff not in NORMAL_RESOLUTION_PAYOFFS


@dataclass(frozen=True)
class VoidPolicy:
    venue: str
    market_id: str
    rules: tuple[VoidRule, ...]
    parsed: bool
    unmatched_reason: str | None = None

    def rule_for(self, trigger: str) -> VoidRule | None:
        for rule in self.rules:
            if rule.trigger == trigger:
                return rule
        return None

    def branch_triggers(self) -> tuple[str, ...]:
        return tuple(
            sorted({rule.trigger for rule in self.rules if rule.opens_branch})
        )


_SENTENCE_SPLIT = re.compile(r"(?<=[.;])\s+")

# Payoff patterns, most specific first: "resolve to the team who wins" must be
# tested before the bare "resolve to ... No" style patterns.
_PAYOFF_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (PAYOFF_FIFTY_FIFTY, re.compile(r"resolve[sd]?\s+(?:to\s+)?50[-/\s]?50", re.I)),
    (
        PAYOFF_RESOLVES_TO_WINNER,
        re.compile(r"resolve\s+to\s+the\s+team\s+(?:who|that)\s+wins", re.I),
    ),
    (
        PAYOFF_COUNTS_AS_COMPLETED,
        re.compile(r"count\s+as\s+a\s+completed|counted\s+towards\s+the\s+total", re.I),
    ),
    (
        PAYOFF_REMAINS_OPEN,
        re.compile(r"remain\s+open\s+until", re.I),
    ),
    (
        PAYOFF_FAIR_MARKET_PRICE,
        re.compile(r"resolve\s+to\s+(?:the\s+|a\s+)?fair\s+(?:market\s+)?price", re.I),
    ),
    (
        PAYOFF_OFFICIAL_RESULT,
        re.compile(r"resolve\s+according\s+to\s+the\s+official\s+result", re.I),
    ),
    (
        PAYOFF_RESOLVE_YES,
        re.compile(r"resolve[sd]?\s+(?:to\s+)?[\"']?Yes[\"']?(?:\W|$)", re.I),
    ),
    (
        PAYOFF_RESOLVE_NO,
        re.compile(r"resolve[sd]?\s+(?:to\s+)?[\"']?No[\"']?(?:\W|$)", re.I),
    ),
)

# Trigger patterns, most specific first. A sentence may carry several.
_TRIGGER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        TRIGGER_NOT_COMPLETED_OPPONENT_FORFEIT,
        re.compile(
            r"begins?\b[^.;]*not\s+completed[^.;]*"
            r"(?:forfeit|disqualif|walkover|default)",
            re.I,
        ),
    ),
    (
        TRIGGER_FORFEIT_POST_PLAY,
        re.compile(
            r"(?:begins?\b[^.;]*subsequently\s+forfeit)"
            r"|(?:clinching\s+game\s+being\s+forfeited)",
            re.I,
        ),
    ),
    (
        TRIGGER_FORFEIT_PRE_PLAY,
        re.compile(
            r"(?:forfeit(?:ed)?\s+before\s+(?:any\s+)?play)"
            r"|(?:withdraws?\s+before\s+the\s+start)",
            re.I,
        ),
    ),
    (
        TRIGGER_POSTPONED_BEYOND_WINDOW,
        re.compile(
            r"(?:postponed|delayed|rescheduled)\b[^.;]*"
            r"(?:within|beyond|over|more\s+than|to\s+over)",
            re.I,
        ),
    ),
    (
        TRIGGER_POSTPONED_INDEFINITE,
        re.compile(r"(?:postponed|delayed)\b[^.;]*remain\s+open", re.I),
    ),
    (
        TRIGGER_AMBIGUOUS_IDENTITY,
        re.compile(r"cannot\s+be\s+unambiguously\s+determined|no\s+reasonable\s+connection", re.I),
    ),
    (TRIGGER_ENDS_IN_TIE, re.compile(r"ends?\s+in\s+a\s+tie", re.I)),
    (TRIGGER_CANCELLED_PRE_PLAY, re.compile(r"cancel(?:l?ed|lation)\b", re.I)),
    (
        TRIGGER_NOT_COMPLETED,
        re.compile(r"not\s+(?:be\s+)?complet(?:ed|e)\b|abandon", re.I),
    ),
)

# Triggers whose match should suppress the looser patterns later in the list,
# so "begins but is not completed … forfeiture" is not also read as a plain
# not-completed clause with the same payoff.
_SUPPRESSES_GENERIC = {
    TRIGGER_NOT_COMPLETED_OPPONENT_FORFEIT: {TRIGGER_NOT_COMPLETED},
    TRIGGER_FORFEIT_POST_PLAY: {TRIGGER_NOT_COMPLETED},
    TRIGGER_POSTPONED_INDEFINITE: {TRIGGER_POSTPONED_BEYOND_WINDOW},
}

_WINDOW_PATTERNS: tuple[tuple[re.Pattern[str], float], ...] = (
    (re.compile(r"(\d+(?:\.\d+)?)\s*hours?", re.I), 1.0),
    (re.compile(r"(\d+(?:\.\d+)?)\s*days?", re.I), 24.0),
    (re.compile(r"(\d+(?:\.\d+)?)\s*weeks?", re.I), 24.0 * 7),
)

_WORD_NUMBERS: Mapping[str, float] = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}
_WORD_WINDOW = re.compile(
    r"\b(" + "|".join(_WORD_NUMBERS) + r")\s+(hours?|days?|weeks?)\b", re.I
)


def _window_hours(sentence: str) -> float | None:
    for pattern, multiplier in _WINDOW_PATTERNS:
        match = pattern.search(sentence)
        if match:
            return float(match.group(1)) * multiplier
    match = _WORD_WINDOW.search(sentence)
    if match:
        unit = match.group(2).lower()
        multiplier = (
            1.0 if unit.startswith("hour")
            else 24.0 if unit.startswith("day")
            else 168.0
        )
        return _WORD_NUMBERS[match.group(1).lower()] * multiplier
    return None


def parse_void_policy(
    rules_text: str | None,
    *,
    venue: str,
    market_id: str,
) -> VoidPolicy:
    """Extract every abnormal-resolution clause from one market's rules text.

    A market with no recognised clause is reported ``parsed=False`` rather than
    given a default. Absence of a stated policy is a risk state, not a safe one.
    """
    text = (rules_text or "").strip()
    if not text:
        return VoidPolicy(
            venue=venue,
            market_id=market_id,
            rules=(),
            parsed=False,
            unmatched_reason="no_rules_text",
        )

    found: dict[str, VoidRule] = {}
    for sentence in _SENTENCE_SPLIT.split(text):
        payoff = next(
            (name for name, pattern in _PAYOFF_PATTERNS if pattern.search(sentence)),
            None,
        )
        if payoff is None:
            continue
        window = _window_hours(sentence)
        evidence = " ".join(sentence.split())[:400]

        matched: list[str] = []
        for trigger, pattern in _TRIGGER_PATTERNS:
            if pattern.search(sentence):
                matched.append(trigger)
        suppressed: set[str] = set()
        for trigger in matched:
            suppressed |= _SUPPRESSES_GENERIC.get(trigger, set())
        for trigger in matched:
            if trigger in suppressed or trigger in found:
                continue
            found[trigger] = VoidRule(
                trigger=trigger,
                payoff=payoff,
                window_hours=window,
                evidence=evidence,
            )

    rules = tuple(sorted(found.values(), key=lambda rule: rule.trigger))
    return VoidPolicy(
        venue=venue,
        market_id=market_id,
        rules=rules,
        parsed=bool(rules),
        unmatched_reason=None if rules else "no_recognised_clause",
    )


def basket_branch_payoff(
    policies: Sequence[VoidPolicy],
    trigger: str,
) -> tuple[float | None, str]:
    """Payoff of a one-contract all-YES basket if ``trigger`` fires.

    ``None`` means the basket cannot be called locked: either a venue reserves
    discretion, or at least one leg has no stated policy for this trigger and
    silence must not be read as "settles normally".
    """
    total = 0.0
    for policy in policies:
        rule = policy.rule_for(trigger)
        if rule is None:
            return None, "partial_policy_coverage"
        per_contract = rule.payoff_per_contract
        if per_contract is None:
            return None, "indeterminate"
        total += per_contract
    if not policies:
        return None, "no_legs"
    return total, "resolved"


def classify_basket_lock(
    policies: Sequence[VoidPolicy],
    *,
    unit_payout: float = 1.0,
    tolerance: float = 1e-9,
) -> dict[str, Any]:
    """Decide whether a basket is locked across every failure branch.

    ``LOCKED`` requires every branch to pay at least the unit payout. Anything
    else is ``LOCKED_CONDITIONAL``, naming the branches that break it, so the
    caveat travels with the claim rather than being dropped.
    """
    triggers = sorted({t for policy in policies for t in policy.branch_triggers()})
    branches: list[dict[str, Any]] = []
    breaking: list[str] = []
    indeterminate: list[str] = []

    for trigger in triggers:
        payoff, status = basket_branch_payoff(policies, trigger)
        branches.append(
            {
                "trigger": trigger,
                "payoff_per_contract": payoff,
                "status": status,
                "shortfall": (
                    None if payoff is None else round(payoff - unit_payout, 10)
                ),
            }
        )
        if payoff is None:
            indeterminate.append(trigger)
        elif payoff < unit_payout - tolerance:
            breaking.append(trigger)

    unknown_policy = [p.market_id for p in policies if not p.parsed]
    if unknown_policy:
        status = "LOCKED_CONDITIONAL"
    elif not triggers:
        status = "NO_POLICY"
    elif indeterminate or breaking:
        status = "LOCKED_CONDITIONAL"
    else:
        status = "LOCKED"

    return {
        "status": status,
        "leg_count": len(policies),
        "legs_without_policy": sorted(unknown_policy),
        "branches": branches,
        "breaking_triggers": breaking,
        "indeterminate_triggers": indeterminate,
    }
