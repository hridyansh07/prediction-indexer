"""Enumeration of terminal outcome spaces (Omega) for an event.

Omega is derived from the *competition format*, never from whichever markets a
venue happened to list. Those differ: in ``dota2-ty-pari-2026-07-18`` the two
listed map markets split 1-1 and PARIVISION still won the series, so a decider
was played that neither venue listed. Building Omega from listed markets would
have made that series look like a completed two-map Bo3 and every derived mask
would have been wrong.

Two scopes can coexist for one event. A World Cup fixture carries a regulation
full-time score space *and* a first-half score space; markets attach to whichever
one they are a function of, and the two are never mixed.

Per the current spec decision, uncovered scorelines are **not** members of
Omega. A space assembled from a listed correct-score ladder is therefore marked
``INCOMPLETE_COVERAGE`` and must not be used to claim a basket is locked.
Resolution-failure branches are handled separately in ``void_policy``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


SCOPE_REGULATION_FULLTIME = "regulation_fulltime"
SCOPE_FIRST_HALF = "first_half"
SCOPE_SERIES = "series"

COVERAGE_EXHAUSTIVE = "EXHAUSTIVE"
COVERAGE_INCOMPLETE = "INCOMPLETE_COVERAGE"

_SCORE_TICKER = re.compile(r"^([A-Z]+)(\d+)([A-Z]+)(\d+)$")
_BEST_OF = re.compile(r"\bBO\s?(\d+)\b", re.I)


@dataclass(frozen=True)
class Outcome:
    """One terminal state of the event."""

    key: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class OutcomeSpace:
    event_key: str
    scope: str
    outcomes: tuple[Outcome, ...]
    coverage: str
    coverage_note: str
    metadata: Mapping[str, Any]

    @property
    def keys(self) -> frozenset[str]:
        return frozenset(outcome.key for outcome in self.outcomes)

    def outcome(self, key: str) -> Outcome | None:
        for candidate in self.outcomes:
            if candidate.key == key:
                return candidate
        return None

    def select(self, predicate) -> frozenset[str]:
        """Every outcome key whose payload satisfies ``predicate``."""
        return frozenset(o.key for o in self.outcomes if predicate(o.payload))


# ---------------------------------------------------------------------------
# Soccer: score grids
# ---------------------------------------------------------------------------


def parse_score_ticker(ticker: str) -> tuple[str, int, str, int] | None:
    """``KXWCSCORE-26JUL19ESPARG-ESP1ARG0`` -> ``("ESP", 1, "ARG", 0)``."""
    suffix = str(ticker).rsplit("-", 1)[-1].strip().upper()
    match = _SCORE_TICKER.match(suffix)
    if not match:
        return None
    return match.group(1), int(match.group(2)), match.group(3), int(match.group(4))


def build_score_space(
    event_key: str,
    score_tickers: Iterable[str],
    *,
    scope: str = SCOPE_REGULATION_FULLTIME,
) -> OutcomeSpace:
    """Assemble a score space from a listed correct-score ladder."""
    outcomes: list[Outcome] = []
    seen: set[str] = set()
    home_code: str | None = None
    away_code: str | None = None
    unparsed: list[str] = []

    for ticker in sorted(score_tickers):
        parsed = parse_score_ticker(ticker)
        if parsed is None:
            unparsed.append(str(ticker))
            continue
        home, home_goals, away, away_goals = parsed
        home_code = home_code or home
        away_code = away_code or away
        key = f"score:{home_goals}-{away_goals}"
        if key in seen:
            continue
        seen.add(key)
        outcomes.append(
            Outcome(
                key=key,
                payload={
                    "home": home,
                    "away": away,
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                    "total_goals": home_goals + away_goals,
                    "goal_difference": home_goals - away_goals,
                },
            )
        )

    outcomes.sort(key=lambda o: (o.payload["home_goals"], o.payload["away_goals"]))
    note = (
        "Assembled from the listed correct-score ladder. Scorelines outside the "
        "listed grid are excluded from Omega by the current spec, so this space "
        "cannot support a locked-basket claim."
    )
    return OutcomeSpace(
        event_key=event_key,
        scope=scope,
        outcomes=tuple(outcomes),
        coverage=COVERAGE_INCOMPLETE,
        coverage_note=note,
        metadata={
            "home": home_code,
            "away": away_code,
            "listed_outcome_count": len(outcomes),
            "unparsed_tickers": sorted(unparsed),
            "max_home_goals": max(
                (o.payload["home_goals"] for o in outcomes), default=None
            ),
            "max_away_goals": max(
                (o.payload["away_goals"] for o in outcomes), default=None
            ),
        },
    )


# ---------------------------------------------------------------------------
# Series (Dota): map-winner sequences
# ---------------------------------------------------------------------------


def parse_best_of(text: str | None) -> int | None:
    match = _BEST_OF.search(str(text or ""))
    if not match:
        return None
    value = int(match.group(1))
    return value if value >= 1 and value % 2 == 1 else None


def series_sequences(best_of: int) -> tuple[str, ...]:
    """Every reachable map-winner sequence, stopping when a side clinches."""
    if best_of < 1 or best_of % 2 == 0:
        raise ValueError(f"best_of must be a positive odd number, got {best_of}")
    target = best_of // 2 + 1
    results: list[str] = []

    def walk(prefix: str, home_wins: int, away_wins: int) -> None:
        if home_wins == target or away_wins == target:
            results.append(prefix)
            return
        walk(prefix + "H", home_wins + 1, away_wins)
        walk(prefix + "A", home_wins, away_wins + 1)

    walk("", 0, 0)
    return tuple(sorted(results))


def build_series_space(
    event_key: str,
    *,
    best_of: int,
    home: str,
    away: str,
) -> OutcomeSpace:
    """A series space is exhaustive by construction: the format fixes it."""
    outcomes = []
    for sequence in series_sequences(best_of):
        home_wins = sequence.count("H")
        away_wins = sequence.count("A")
        outcomes.append(
            Outcome(
                key=f"seq:{sequence}",
                payload={
                    "sequence": sequence,
                    "home": home,
                    "away": away,
                    "home_wins": home_wins,
                    "away_wins": away_wins,
                    "maps_played": len(sequence),
                    "winner": home if home_wins > away_wins else away,
                    "winner_side": "home" if home_wins > away_wins else "away",
                    "margin": abs(home_wins - away_wins),
                },
            )
        )
    return OutcomeSpace(
        event_key=event_key,
        scope=SCOPE_SERIES,
        outcomes=tuple(outcomes),
        coverage=COVERAGE_EXHAUSTIVE,
        coverage_note=(
            "Enumerated from the series format, so it covers every reachable "
            "sequence regardless of which map markets the venues listed."
        ),
        metadata={"best_of": best_of, "home": home, "away": away},
    )


# ---------------------------------------------------------------------------
# State timeline
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StateTransition:
    map_index: int
    winner_side: str
    at_ms: int
    prefix: str


def build_state_timeline(
    map_results: Sequence[Mapping[str, Any]],
    *,
    home: str,
) -> tuple[StateTransition, ...]:
    """Reconstruct series state from settled map markets.

    ``map_results`` items need ``map_index``, ``winner`` and ``settled_at_ms``.
    Historical backtests need no live feed: a settled map market records both
    who won and when it closed.
    """
    ordered = sorted(map_results, key=lambda item: int(item["map_index"]))
    transitions: list[StateTransition] = []
    prefix = ""
    for item in ordered:
        side = "H" if str(item["winner"]) == home else "A"
        prefix += side
        transitions.append(
            StateTransition(
                map_index=int(item["map_index"]),
                winner_side="home" if side == "H" else "away",
                at_ms=int(item["settled_at_ms"]),
                prefix=prefix,
            )
        )
    return tuple(transitions)


def reachable_keys(space: OutcomeSpace, prefix: str) -> frozenset[str]:
    """Outcome keys still reachable once ``prefix`` has been played.

    State transitions never add outcomes; they remove them. Relationships are
    set operations over what is still reachable, which is why identities appear
    mid-series that do not hold before a ball is thrown.
    """
    if space.scope != SCOPE_SERIES:
        return space.keys
    if not prefix:
        return space.keys
    return frozenset(
        outcome.key
        for outcome in space.outcomes
        if str(outcome.payload["sequence"]).startswith(prefix)
    )


def settled_outcome_key(
    space: OutcomeSpace,
    *,
    home_goals: int | None = None,
    away_goals: int | None = None,
    sequence: str | None = None,
) -> str | None:
    """The single omega-star the event actually resolved to, if it is in Omega."""
    if space.scope == SCOPE_SERIES:
        if sequence is None:
            return None
        key = f"seq:{sequence}"
        return key if key in space.keys else None
    if home_goals is None or away_goals is None:
        return None
    key = f"score:{home_goals}-{away_goals}"
    return key if key in space.keys else None
