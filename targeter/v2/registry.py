"""Strict strategy and reusable venue-product registry loading."""

from __future__ import annotations

import json
import math
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from targeter.v2.models import canonical_participant


class StrategyError(ValueError):
    pass


# Key sets the loader accepts, shared with tests/test_targeter_v2_selection_config.py
# so configs/targeter_v2.schema.json can be checked against them.
TOP_LEVEL_KEYS = frozenset({
    "version", "sports", "selection", "scope", "ranking", "target_budgets",
    "polymarket_tags", "limitless_category_ids", "market_classes", "game_families",
    "known_rule_templates", "participant_aliases", "note",
})
SELECTION_KEYS = frozenset({
    "minimum_venues", "preferred_venues", "discovery_horizon_seconds",
    "pre_event_seconds", "run_interval_seconds", "subscription_guard_seconds",
    "event_time_tolerance_seconds", "minimum_market_age_seconds",
    "minimum_combined_moneyline_volume_usd", "post_start_retention_seconds",
    "terminal_clamp_seconds", "continuity_degraded_after_seconds",
    "continuity_hold_enabled", "maximum_bundles",
    "maximum_markets_per_bundle", "maximum_markets",
})
SCOPE_KEYS = frozenset({"sports", "games", "market_classes"})
RANKING_KEYS = frozenset({"keys", "priorities"})
RANKING_KEY_ITEM_KEYS = frozenset({"key", "order"})
PRIORITIES_KEYS = frozenset({"default", "game", "market_class", "venue_set"})
# The loader reads exactly these market class fields and ignores any others.
MARKET_CLASS_KEYS = frozenset({"id", "sport", "market_type", "scope", "venue_patterns"})
GAME_FAMILY_KEYS = frozenset({
    "id", "sport", "topology", "polymarket_game_tags", "venue_game_aliases", "venue_products",
})
PRODUCT_MAPPING_KEYS = frozenset({"field", "values", "patterns", "canonical_class"})
RANKING_KEY_NAMES = (
    "venue_coverage", "cross_venue_relationships", "market_class_breadth", "activity",
    "moneyline_volume_usd", "activation_time", "game_priority",
    "market_class_priority_max", "market_class_priority_sum", "venue_set_priority",
)
PRIORITY_RANKING_KEYS = frozenset({
    "game_priority", "market_class_priority_max", "market_class_priority_sum",
    "venue_set_priority",
})
_PRIORITY_VENUES = ("kalshi", "limitless", "polymarket")


@dataclass(frozen=True)
class ClassDefinition:
    id: str
    sport: str
    market_type: str
    scope: str
    venue_patterns: Mapping[str, Mapping[str, tuple[str, ...]]]

    def matches(self, venue: str, fields: Mapping[str, Any]) -> bool:
        patterns = self.venue_patterns.get(venue)
        if not patterns:
            return False
        for field_name, expressions in patterns.items():
            value = fields.get(field_name)
            values = value if isinstance(value, (list, tuple)) else (value,)
            for candidate in values:
                text = str(candidate or "")
                if any(re.search(expression, text, re.IGNORECASE) for expression in expressions):
                    return True
        return False


@dataclass(frozen=True)
class ProductMapping:
    field: str
    values: tuple[str, ...] | None
    patterns: tuple[str, ...] | None
    canonical_class: str


@dataclass(frozen=True)
class GameFamily:
    id: str
    sport: str
    topology: str
    polymarket_game_tags: tuple[str, ...]
    venue_game_aliases: Mapping[str, tuple[str, ...]]
    venue_products: Mapping[str, tuple[ProductMapping, ...]]


@dataclass(frozen=True)
class ScopeConfig:
    """Optional selection scope; `None` for a dimension means unrestricted."""

    sports: frozenset[str] | None = None
    games: frozenset[str] | None = None
    market_classes: frozenset[str] | None = None


@dataclass(frozen=True)
class RankingKey:
    key: str
    order: str


@dataclass(frozen=True)
class Priorities:
    default: int
    game: Mapping[str, int]
    market_class: Mapping[str, int]
    venue_set: Mapping[str, int]


@dataclass(frozen=True)
class RankingConfig:
    keys: tuple[RankingKey, ...]
    priorities: Priorities | None


@dataclass(frozen=True)
class Strategy:
    version: int
    sports: tuple[str, ...]
    minimum_venues: int
    preferred_venues: int
    discovery_horizon_seconds: int
    pre_event_seconds: int
    run_interval_seconds: int
    subscription_guard_seconds: int
    event_time_tolerance_seconds: int
    minimum_market_age_seconds: int
    minimum_combined_moneyline_volume_usd: float
    post_start_retention_seconds: int
    terminal_clamp_seconds: int
    continuity_degraded_after_seconds: int
    continuity_hold_enabled: bool
    maximum_bundles: int
    target_budgets: Mapping[str, int]
    polymarket_tags: tuple[str, ...]
    limitless_category_ids: tuple[int, ...]
    participant_aliases: Mapping[str, str]
    classes: tuple[ClassDefinition, ...]
    game_families: tuple[GameFamily, ...]
    known_rule_templates: frozenset[str]
    source_path: str
    # Selection config V1 (docs/specs/TARGETER_SELECTION_CONFIG_V1.md); None = not configured.
    scope: ScopeConfig | None = None
    maximum_markets_per_bundle: int | None = None
    maximum_markets: int | None = None
    ranking: RankingConfig | None = None

    @property
    def uses_selection_config(self) -> bool:
        return (
            self.scope is not None
            or self.maximum_markets_per_bundle is not None
            or self.maximum_markets is not None
            or self.ranking is not None
        )

    def market_class_priority(self, canonical_class: str) -> int:
        priorities = self.ranking.priorities if self.ranking else None
        if priorities is None:
            return 0
        return priorities.market_class.get(canonical_class, priorities.default)

    @property
    def selection_lookahead_seconds(self) -> int:
        return self.run_interval_seconds + self.subscription_guard_seconds


class MarketClassRegistry:
    def __init__(self, strategy: Strategy) -> None:
        self.strategy = strategy
        self.by_id = {definition.id: definition for definition in strategy.classes}
        self.family_by_id = {family.id: family for family in strategy.game_families}
        # Validate that all canonical_classes referenced in game_families actually exist
        for family in strategy.game_families:
            for venue, mappings in family.venue_products.items():
                for mapping in mappings:
                    if mapping.canonical_class not in self.by_id:
                        raise StrategyError(f"game_family {family.id} references unknown canonical class: {mapping.canonical_class}")

    def classify(self, venue: str, sport: str, fields: Mapping[str, Any]) -> ClassDefinition | None:
        for definition in self.strategy.classes:
            if definition.sport == sport and definition.matches(venue, fields):
                return definition
        return None

    def definition(self, identifier: str) -> ClassDefinition:
        try:
            return self.by_id[identifier]
        except KeyError as error:
            raise StrategyError(f"unknown canonical market class: {identifier}") from error

    def game_family(self, identifier: str) -> GameFamily | None:
        return self.family_by_id.get(identifier)


def _positive_integer(document: Mapping[str, Any], key: str) -> int:
    value = document.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise StrategyError(f"{key} must be a positive integer")
    return value


def _positive_number(document: Mapping[str, Any], key: str) -> float:
    value = document.get(key)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value <= 0
    ):
        raise StrategyError(f"{key} must be a positive finite number")
    return float(value)


def _normalized_alias(value: str) -> str:
    return re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", value).casefold(),
    ).strip()


def _load_game_families(document: Mapping[str, Any], sports: list[str]) -> tuple[GameFamily, ...]:
    raw_families = document.get("game_families", [])
    if not isinstance(raw_families, list):
        raise StrategyError("game_families must be an array")
    
    families: list[GameFamily] = []
    seen: set[str] = set()
    global_aliases: dict[str, str] = {}
    global_game_tags: dict[str, str] = {}

    for index, item in enumerate(raw_families):
        if not isinstance(item, dict):
            raise StrategyError(f"game_family {index} must be an object")
        unknown_family = sorted(set(item) - GAME_FAMILY_KEYS)
        if unknown_family:
            raise StrategyError(
                f"game_family {index} has unknown fields: {', '.join(unknown_family)}"
            )
        
        identifier = item.get("id")
        if not isinstance(identifier, str) or not re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", identifier):
            raise StrategyError(f"game_family {index} id must be lower snake case")
        if identifier in seen:
            raise StrategyError(f"duplicate game_family id: {identifier}")
        seen.add(identifier)

        sport = item.get("sport")
        if sport != "esports":
            raise StrategyError(f"game_family {identifier} sport must be 'esports'")
        
        topology = item.get("topology")
        if topology != "best_of_series":
            raise StrategyError(f"game_family {identifier} topology must be 'best_of_series'")

        raw_game_tags = item.get("polymarket_game_tags")
        if not isinstance(raw_game_tags, list) or not raw_game_tags or not all(isinstance(tag, str) for tag in raw_game_tags):
            raise StrategyError(f"game_family {identifier} polymarket_game_tags must be a non-empty string array")
        game_tags = tuple(_normalized_alias(tag) for tag in raw_game_tags)
        if len(set(game_tags)) != len(game_tags) or any(not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", tag) for tag in game_tags):
            raise StrategyError(f"game_family {identifier} has invalid or duplicate polymarket_game_tags")
        for tag in game_tags:
            if tag in global_game_tags:
                raise StrategyError(f"Polymarket game tag {tag!r} collides across game families")
            global_game_tags[tag] = identifier

        raw_aliases = item.get("venue_game_aliases")
        if not isinstance(raw_aliases, dict) or set(raw_aliases) != {
            "kalshi", "polymarket", "limitless"
        }:
            raise StrategyError(f"game_family {identifier} requires venue_game_aliases")
        
        venue_game_aliases: dict[str, tuple[str, ...]] = {}
        for venue, aliases in raw_aliases.items():
            if venue not in ("kalshi", "polymarket", "limitless"):
                raise StrategyError(f"game_family {identifier} aliases contains unknown venue {venue}")
            if not isinstance(aliases, list) or not aliases or not all(isinstance(a, str) for a in aliases):
                raise StrategyError(f"game_family {identifier} venue {venue} aliases must be a non-empty string array")
            
            normalized_aliases = tuple(_normalized_alias(a) for a in aliases)
            if any(not alias for alias in normalized_aliases):
                raise StrategyError(
                    f"game_family {identifier} venue {venue} aliases must remain non-empty"
                )
            if len(set(normalized_aliases)) != len(normalized_aliases):
                raise StrategyError(
                    f"game_family {identifier} venue {venue} has duplicate aliases"
                )
            for a in normalized_aliases:
                if a in global_aliases and global_aliases[a] != identifier:
                    raise StrategyError(f"alias {a!r} collides across game families")
                global_aliases[a] = identifier
            venue_game_aliases[venue] = normalized_aliases

        raw_products = item.get("venue_products")
        if not isinstance(raw_products, dict) or set(raw_products) != {
            "kalshi", "polymarket", "limitless"
        }:
            raise StrategyError(f"game_family {identifier} requires venue_products")

        venue_products: dict[str, tuple[ProductMapping, ...]] = {}
        for venue, products in raw_products.items():
            if venue not in ("kalshi", "polymarket", "limitless"):
                raise StrategyError(f"game_family {identifier} products contains unknown venue {venue}")
            if not isinstance(products, list):
                raise StrategyError(f"game_family {identifier} venue {venue} products must be an array")
            
            mappings: list[ProductMapping] = []
            exact_values: dict[tuple[str, str], str] = {}
            for m_index, mapping in enumerate(products):
                if not isinstance(mapping, dict):
                    raise StrategyError(f"game_family {identifier} product mapping {m_index} must be an object")
                unknown_mapping = sorted(set(mapping) - PRODUCT_MAPPING_KEYS)
                if unknown_mapping:
                    raise StrategyError(
                        f"game_family {identifier} product mapping {m_index} has unknown fields: "
                        + ", ".join(unknown_mapping)
                    )
                
                field = mapping.get("field")
                if not isinstance(field, str) or not field:
                    raise StrategyError(f"game_family {identifier} product mapping {m_index} missing field")
                
                # Check venue-specific field allowlist
                if venue == "kalshi" and field != "series_ticker":
                    raise StrategyError(f"game_family {identifier} kalshi mapping uses unallowed field {field}")
                if venue == "polymarket" and field != "group_title":
                    raise StrategyError(f"game_family {identifier} polymarket mapping uses unallowed field {field}")
                if venue == "limitless" and field != "metadata_market_type":
                    raise StrategyError(f"game_family {identifier} limitless mapping uses unallowed field {field}")

                canonical_class = mapping.get("canonical_class")
                if not isinstance(canonical_class, str) or not canonical_class:
                    raise StrategyError(f"game_family {identifier} product mapping {m_index} missing canonical_class")

                has_values = "values" in mapping
                has_patterns = "patterns" in mapping
                if has_values == has_patterns:
                    raise StrategyError(f"game_family {identifier} product mapping {m_index} must have exactly one of values or patterns")
                
                values: tuple[str, ...] | None = None
                patterns: tuple[str, ...] | None = None

                if has_values:
                    v_list = mapping["values"]
                    if not isinstance(v_list, list) or not v_list or not all(isinstance(v, str) for v in v_list):
                        raise StrategyError(f"game_family {identifier} product mapping {m_index} values must be a non-empty string array")
                    values = tuple(
                        value.strip()
                        if venue == "kalshi"
                        else _normalized_alias(value)
                        for value in v_list
                    )
                    if any(not value for value in values):
                        raise StrategyError(
                            f"game_family {identifier} product mapping {m_index} values must remain non-empty"
                        )
                    if len(set(values)) != len(values):
                        raise StrategyError(f"game_family {identifier} product mapping {m_index} has duplicate values")
                    for value in values:
                        key = (field, value)
                        previous = exact_values.get(key)
                        if previous is not None and previous != canonical_class:
                            raise StrategyError(
                                f"game_family {identifier} venue {venue} value {value!r} maps to multiple classes"
                            )
                        exact_values[key] = canonical_class
                else:
                    p_list = mapping["patterns"]
                    if not isinstance(p_list, list) or not p_list or not all(isinstance(p, str) for p in p_list):
                        raise StrategyError(f"game_family {identifier} product mapping {m_index} patterns must be a non-empty string array")
                    if venue == "polymarket":
                        for p in p_list:
                            if not p.startswith("^") or not p.endswith("$"):
                                raise StrategyError(f"game_family {identifier} polymarket pattern {p!r} must be anchored with ^ and $")
                    for p in p_list:
                        try:
                            re.compile(p)
                        except re.error as e:
                            raise StrategyError(f"game_family {identifier} invalid regex {p!r}: {e}")
                    patterns = tuple(p_list)

                mappings.append(ProductMapping(field, values, patterns, canonical_class))
            venue_products[venue] = tuple(mappings)
            
        if not any(venue_products.values()):
            raise StrategyError(f"game_family {identifier} must enable at least one venue")
        families.append(GameFamily(identifier, sport, topology, game_tags, venue_game_aliases, venue_products))
        
    return tuple(families)


def _optional_positive_integer(document: Mapping[str, Any], key: str) -> int | None:
    if key not in document:
        return None
    return _positive_integer(document, key)


def _unique_string_list(value: Any, name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise StrategyError(f"{name} must be a non-empty string array")
    if len(set(value)) != len(value):
        raise StrategyError(f"{name} must be unique")
    return tuple(value)


def _load_scope(
    raw: Any,
    sports: list[str],
    classes: list[ClassDefinition],
    game_families: tuple[GameFamily, ...],
) -> ScopeConfig | None:
    if raw is None:
        return None
    if not isinstance(raw, dict) or not raw:
        raise StrategyError("scope must be a non-empty object")
    unknown = sorted(set(raw) - SCOPE_KEYS)
    if unknown:
        raise StrategyError(f"unknown scope fields: {', '.join(unknown)}")
    known = {
        "sports": set(sports),
        "games": {family.id for family in game_families},
        "market_classes": {definition.id for definition in classes},
    }
    resolved: dict[str, frozenset[str]] = {}
    for field_name in ("sports", "games", "market_classes"):
        if field_name not in raw:
            continue
        values = _unique_string_list(raw[field_name], f"scope.{field_name}")
        unlisted = sorted(set(values) - known[field_name])
        if unlisted:
            raise StrategyError(
                f"scope.{field_name} names unknown entries: {', '.join(unlisted)}"
            )
        resolved[field_name] = frozenset(values)
    return ScopeConfig(**resolved)


def _priority_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StrategyError(f"{name} must be an integer")
    return value


def _load_priorities(
    raw: Any,
    classes: list[ClassDefinition],
    game_families: tuple[GameFamily, ...],
) -> Priorities:
    if not isinstance(raw, dict):
        raise StrategyError("ranking.priorities must be an object")
    unknown = sorted(set(raw) - PRIORITIES_KEYS)
    if unknown:
        raise StrategyError(f"unknown ranking.priorities fields: {', '.join(unknown)}")
    if "default" not in raw:
        raise StrategyError("ranking.priorities.default is required")
    default = _priority_integer(raw["default"], "ranking.priorities.default")
    groups: dict[str, dict[str, int]] = {}
    valid_ids = {
        "game": {family.id for family in game_families},
        "market_class": {definition.id for definition in classes},
    }
    for group in ("game", "market_class", "venue_set"):
        mapping = raw.get(group, {})
        if not isinstance(mapping, dict):
            raise StrategyError(f"ranking.priorities.{group} must be an object")
        resolved: dict[str, int] = {}
        for name, value in mapping.items():
            if group == "venue_set":
                venues = name.split("+")
                if (
                    len(venues) not in (2, 3)
                    or any(venue not in _PRIORITY_VENUES for venue in venues)
                    or venues != sorted(set(venues))
                ):
                    raise StrategyError(
                        f"ranking.priorities.venue_set key {name!r} must be 2 or 3 distinct "
                        "supported venues, sorted and joined by '+'"
                    )
            elif name not in valid_ids[group]:
                raise StrategyError(
                    f"ranking.priorities.{group} names unknown entry: {name}"
                )
            resolved[name] = _priority_integer(value, f"ranking.priorities.{group}.{name}")
        groups[group] = resolved
    return Priorities(default, groups["game"], groups["market_class"], groups["venue_set"])


def _load_ranking(
    raw: Any,
    classes: list[ClassDefinition],
    game_families: tuple[GameFamily, ...],
) -> RankingConfig | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise StrategyError("ranking must be an object")
    unknown = sorted(set(raw) - RANKING_KEYS)
    if unknown:
        raise StrategyError(f"unknown ranking fields: {', '.join(unknown)}")
    raw_keys = raw.get("keys")
    if not isinstance(raw_keys, list) or not raw_keys:
        raise StrategyError("ranking.keys must be a non-empty array")
    keys: list[RankingKey] = []
    for index, item in enumerate(raw_keys):
        if not isinstance(item, dict):
            raise StrategyError(f"ranking.keys[{index}] must be an object")
        unknown_item = sorted(set(item) - RANKING_KEY_ITEM_KEYS)
        if unknown_item:
            raise StrategyError(
                f"ranking.keys[{index}] has unknown fields: {', '.join(unknown_item)}"
            )
        name = item.get("key")
        if not isinstance(name, str) or name not in RANKING_KEY_NAMES:
            raise StrategyError(f"ranking.keys[{index}].key is not a supported ranking key")
        order = item.get("order")
        if order not in ("asc", "desc"):
            raise StrategyError(f"ranking.keys[{index}].order must be 'asc' or 'desc'")
        if any(existing.key == name for existing in keys):
            raise StrategyError(f"ranking.keys repeats {name}")
        keys.append(RankingKey(name, order))
    priorities = (
        _load_priorities(raw["priorities"], classes, game_families)
        if "priorities" in raw
        else None
    )
    if priorities is None and any(item.key in PRIORITY_RANKING_KEYS for item in keys):
        raise StrategyError("ranking uses a priority key but defines no ranking.priorities")
    return RankingConfig(tuple(keys), priorities)


def load_strategy(path: Path) -> Strategy:
    path = Path(path)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise StrategyError(f"strategy not found: {path}") from error
    except json.JSONDecodeError as error:
        raise StrategyError(f"strategy is not valid JSON: {error}") from error
    if not isinstance(document, dict):
        raise StrategyError("strategy must be a JSON object")

    unknown = sorted(set(document) - TOP_LEVEL_KEYS)
    if unknown:
        raise StrategyError(f"unknown strategy fields: {', '.join(unknown)}")

    sports = document.get("sports")
    if not isinstance(sports, list) or not sports or not all(isinstance(item, str) for item in sports):
        raise StrategyError("sports must be a non-empty string array")
    selection = document.get("selection")
    if not isinstance(selection, dict):
        raise StrategyError("selection must be an object")

    minimum_venues = _positive_integer(selection, "minimum_venues")
    preferred_venues = _positive_integer(selection, "preferred_venues")
    if minimum_venues < 2 or preferred_venues < minimum_venues or preferred_venues > 3:
        raise StrategyError("venue thresholds must satisfy 2 <= minimum <= preferred <= 3")

    unknown_selection = sorted(set(selection) - SELECTION_KEYS)
    if unknown_selection:
        raise StrategyError(f"unknown selection fields: {', '.join(unknown_selection)}")
    continuity_hold_enabled = selection.get("continuity_hold_enabled")
    if not isinstance(continuity_hold_enabled, bool):
        raise StrategyError("continuity_hold_enabled must be a boolean")
    terminal_clamp_seconds = _positive_integer(selection, "terminal_clamp_seconds")
    continuity_degraded_after_seconds = _positive_integer(
        selection, "continuity_degraded_after_seconds"
    )
    if continuity_degraded_after_seconds >= terminal_clamp_seconds:
        raise StrategyError(
            "continuity_degraded_after_seconds must be less than terminal_clamp_seconds"
        )

    raw_classes = document.get("market_classes")
    if not isinstance(raw_classes, list) or not raw_classes:
        raise StrategyError("market_classes must be a non-empty array")
    classes: list[ClassDefinition] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_classes):
        if not isinstance(item, dict):
            raise StrategyError(f"market class {index} must be an object")
        identifier = item.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in seen:
            raise StrategyError(f"market class {index} has a missing or duplicate id")
        seen.add(identifier)
        sport = item.get("sport")
        market_type = item.get("market_type")
        scope = item.get("scope")
        if sport not in sports or not isinstance(market_type, str) or not isinstance(scope, str):
            raise StrategyError(f"market class {identifier} has invalid sport/type/scope")
        raw_patterns = item.get("venue_patterns")
        if not isinstance(raw_patterns, dict) or (not raw_patterns and sport != "esports"):
            raise StrategyError(f"market class {identifier} needs venue_patterns")
        venue_patterns: dict[str, dict[str, tuple[str, ...]]] = {}
        for venue, fields in raw_patterns.items():
            if venue not in ("kalshi", "polymarket", "limitless") or not isinstance(fields, dict):
                raise StrategyError(f"market class {identifier} has invalid venue patterns")
            venue_patterns[venue] = {}
            for field_name, expressions in fields.items():
                if not isinstance(expressions, list) or not expressions or not all(
                    isinstance(expression, str) for expression in expressions
                ):
                    raise StrategyError(
                        f"market class {identifier} pattern {venue}.{field_name} must be a string array"
                    )
                for expression in expressions:
                    try:
                        re.compile(expression)
                    except re.error as error:
                        raise StrategyError(
                            f"market class {identifier} has invalid regex {expression!r}: {error}"
                        ) from error
                venue_patterns[venue][field_name] = tuple(expressions)
        classes.append(
            ClassDefinition(identifier, sport, market_type, scope, venue_patterns)
        )

    game_families = _load_game_families(document, sports)

    budgets = document.get("target_budgets")
    if not isinstance(budgets, dict) or set(budgets) != {"kalshi", "polymarket", "limitless"}:
        raise StrategyError("target_budgets must name kalshi, polymarket, and limitless")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in budgets.values()
    ):
        raise StrategyError("target budgets must be positive integers")
    target_budgets = dict(budgets)

    tags = document.get("polymarket_tags")
    if not isinstance(tags, list) or not tags or not all(isinstance(item, str) for item in tags):
        raise StrategyError("polymarket_tags must be a non-empty string array")
    raw_categories = document.get("limitless_category_ids", [])
    if not isinstance(raw_categories, list) or any(
        isinstance(item, bool) or not isinstance(item, int) or item <= 0
        for item in raw_categories
    ):
        raise StrategyError("limitless_category_ids must be an array of positive integers")
    if len(set(raw_categories)) != len(raw_categories):
        raise StrategyError("limitless_category_ids must be unique")
    limitless_category_ids = tuple(raw_categories)

    known = document.get("known_rule_templates", [])
    if not isinstance(known, list) or not all(isinstance(item, str) for item in known):
        raise StrategyError("known_rule_templates must be a string array")
    raw_aliases = document.get("participant_aliases", {})
    if not isinstance(raw_aliases, dict):
        raise StrategyError("participant_aliases must be an object")
    participant_aliases: dict[str, str] = {}
    for preferred, aliases in raw_aliases.items():
        preferred_key = canonical_participant(str(preferred))
        if not preferred_key or not isinstance(aliases, list) or not all(
            isinstance(alias, str) for alias in aliases
        ):
            raise StrategyError(
                "participant_aliases values must be string arrays under a usable preferred name"
            )
        for alias in (preferred, *aliases):
            alias_key = canonical_participant(alias)
            if not alias_key:
                raise StrategyError("participant aliases must remain non-empty after canonicalization")
            existing = participant_aliases.get(alias_key)
            if existing is not None and existing != preferred_key:
                raise StrategyError(f"participant alias {alias!r} maps to more than one preferred name")
            participant_aliases[alias_key] = preferred_key

    version = document.get("version", 1)
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise StrategyError("version must be a positive integer")

    scope = _load_scope(document.get("scope"), sports, classes, game_families)
    maximum_markets_per_bundle = _optional_positive_integer(selection, "maximum_markets_per_bundle")
    maximum_markets = _optional_positive_integer(selection, "maximum_markets")
    ranking = _load_ranking(document.get("ranking"), classes, game_families)

    return Strategy(
        version=version,
        sports=tuple(sports),
        minimum_venues=minimum_venues,
        preferred_venues=preferred_venues,
        discovery_horizon_seconds=_positive_integer(selection, "discovery_horizon_seconds"),
        pre_event_seconds=_positive_integer(selection, "pre_event_seconds"),
        run_interval_seconds=_positive_integer(selection, "run_interval_seconds"),
        subscription_guard_seconds=_positive_integer(selection, "subscription_guard_seconds"),
        event_time_tolerance_seconds=_positive_integer(selection, "event_time_tolerance_seconds"),
        minimum_market_age_seconds=_positive_integer(selection, "minimum_market_age_seconds"),
        minimum_combined_moneyline_volume_usd=_positive_number(
            selection, "minimum_combined_moneyline_volume_usd"
        ),
        post_start_retention_seconds=_positive_integer(selection, "post_start_retention_seconds"),
        terminal_clamp_seconds=terminal_clamp_seconds,
        continuity_degraded_after_seconds=continuity_degraded_after_seconds,
        continuity_hold_enabled=continuity_hold_enabled,
        maximum_bundles=_positive_integer(selection, "maximum_bundles"),
        target_budgets=target_budgets,
        polymarket_tags=tuple(tags),
        limitless_category_ids=limitless_category_ids,
        participant_aliases=participant_aliases,
        classes=tuple(classes),
        game_families=game_families,
        known_rule_templates=frozenset(known),
        source_path=str(path),
        scope=scope,
        maximum_markets_per_bundle=maximum_markets_per_bundle,
        maximum_markets=maximum_markets,
        ranking=ranking,
    )
