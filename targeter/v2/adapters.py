"""Current public sports catalogues translated into Targeter v2 records.

Adapters are vendor-scoped on purpose.  Everything after this boundary consumes
only :mod:`targeter.v2.domain`, so a vendor API change cannot leak into event
matching or relationship logic.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Protocol

from analysis.durable_http import DurableJsonClient, RetryingJsonClient
from targeter.v2.domain import (
    CanonicalEvent,
    CanonicalMarket,
    CatalogSnapshot,
    ClassificationEvidence,
    ActivationEvidence,
    TargeterDiagnostic,
    canonical_participant,
    finite_number,
    parse_timestamp,
    stable_id,
)
from targeter.v2.parsing import esports, products, text, traditional
from targeter.v2.registry import ClassDefinition, GameFamily, MarketClassRegistry, Strategy


KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"
POLYMARKET_API = "https://gamma-api.polymarket.com"
LIMITLESS_API = "https://api.limitless.exchange"



def _game_evidence(
    family: GameFamily,
    venue: str,
    source_field: str,
    observed_value: object,
) -> ClassificationEvidence:
    return ClassificationEvidence(
        f"{family.id}:{venue}:game:{source_field}",
        source_field,
        str(observed_value),
    )


def _kalshi_game_family(
    registry: MarketClassRegistry,
    series: Mapping[str, Any],
) -> tuple[GameFamily | None, ClassificationEvidence | None]:
    tags = {text.normalize_label(item) for item in (series.get("tags") or [])}
    if "esports" not in tags:
        return None, None
    ticker = str(series.get("ticker") or "").strip()
    for family in registry.strategy.game_families:
        if not family.venue_products.get("kalshi"):
            continue
        for mapping in family.venue_products.get("kalshi", ()):
            if mapping.field == "series_ticker" and mapping.values and ticker in mapping.values:
                return family, _game_evidence(family, "kalshi", "series_ticker", ticker)
    return None, None


def _polymarket_game_family(
    registry: MarketClassRegistry,
    title: object,
    tags: list[str],
) -> tuple[GameFamily | None, ClassificationEvidence | None, bool]:
    normalized_tags = {text.normalize_label(tag) for tag in tags}
    prefix_families = []
    tag_families = []
    for family in registry.strategy.game_families:
        if not family.venue_products.get("polymarket"):
            continue
        alias = esports.title_alias_prefix(
            title, family.venue_game_aliases.get("polymarket", ())
        )
        if alias is not None:
            prefix_families.append((family, alias))
        if normalized_tags.intersection(family.polymarket_game_tags):
            tag_families.append(family)
    if len(prefix_families) == 1 and len(tag_families) == 1:
        family, alias = prefix_families[0]
        if family.id == tag_families[0].id and "esports" in normalized_tags:
            game_tag = sorted(normalized_tags.intersection(family.polymarket_game_tags))[0]
            return family, _game_evidence(
                family, "polymarket", "event_tags_and_prefix", f"esports|{game_tag}|{alias}"
            ), False
    # Either half of the configured two-factor classification is evidence of
    # drift when the other half is absent or identifies another game.
    return None, None, bool(prefix_families or tag_families)


def _limitless_game_family(
    registry: MarketClassRegistry,
    record: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> tuple[GameFamily | None, ClassificationEvidence | None, bool]:
    structural_sports = text.normalize_label(
        record.get("automationType") or metadata.get("automationType")
    ) == "sports"
    if not structural_sports:
        return None, None, False
    structured: list[tuple[GameFamily, str, object]] = []
    for family in registry.strategy.game_families:
        if not family.venue_products.get("limitless"):
            continue
        aliases = family.venue_game_aliases.get("limitless", ())
        for field in ("esportTitle", "videogameSlug"):
            value = metadata.get(field)
            if value and text.normalize_label(value) in aliases:
                structured.append((family, field, value))
    title_matches = [
        (family, alias)
        for family in registry.strategy.game_families
        if family.venue_products.get("limitless")
        for alias in (esports.title_alias_prefix(record.get("title"), family.venue_game_aliases.get("limitless", ())),)
        if alias is not None
    ]
    structured_ids = {item[0].id for item in structured}
    if len(structured_ids) > 1:
        return None, None, True
    if structured:
        family, field, value = sorted(structured, key=lambda item: (item[0].id, item[1]))[0]
        if title_matches and any(item[0].id != family.id for item in title_matches):
            return None, None, True
        return family, _game_evidence(family, "limitless", field, value), False
    if len(title_matches) == 1:
        family, alias = title_matches[0]
        return family, _game_evidence(family, "limitless", "title_prefix", alias), False
    return None, None, len(title_matches) > 1


def _classify_market_by_family(
    venue: str,
    family: GameFamily,
    raw: Mapping[str, Any],
    parent_raw: Mapping[str, Any],
) -> tuple[str, ClassificationEvidence] | tuple[None, None]:
    mappings = family.venue_products.get(venue, ())
    matches: list[tuple[str, str, str]] = []
    for mapping in mappings:
        val = ""
        if venue == "kalshi" and mapping.field == "series_ticker":
            val = str(parent_raw.get("series_ticker") or "")
        elif venue == "polymarket" and mapping.field == "group_title":
            val = str(raw.get("groupItemTitle") or "")
        elif venue == "limitless" and mapping.field == "metadata_market_type":
            raw_metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
            parent_metadata = (
                parent_raw.get("metadata")
                if isinstance(parent_raw.get("metadata"), dict)
                else parent_raw
            )
            val = str(
                raw.get("metadata_market_type")
                or raw_metadata.get("marketType")
                or raw_metadata.get("binaryMarketType")
                or parent_metadata.get("marketType")
                or parent_metadata.get("binaryMarketType")
                or ""
            )

        if not val:
            continue
        comparison = val.strip() if venue == "kalshi" else text.normalize_label(val)
        if mapping.values and comparison in mapping.values:
            matches.append((mapping.canonical_class, mapping.field, val))
        if mapping.patterns:
            for pat in mapping.patterns:
                if re.search(pat, text.normalize_label(val), re.IGNORECASE):
                    matches.append((mapping.canonical_class, mapping.field, val))
                    break
    unique = {(canonical_class, field, value) for canonical_class, field, value in matches}
    if len({item[0] for item in unique}) != 1:
        return None, None
    if not unique:
        return None, None
    canonical_class, field, value = sorted(unique)[0]
    return canonical_class, ClassificationEvidence(
        f"{family.id}:{venue}:{canonical_class}:{field}",
        field,
        value,
    )

class JsonClient(Protocol):
    def get_json(
        self,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any: ...


def _data(response: Any) -> Any:
    return response.data if hasattr(response, "data") else response


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return [str(item) for item in parsed] if isinstance(parsed, list) else []
    return []


def _rules_hash(value: str | None) -> str | None:
    return hashlib.sha256(value.encode("utf-8")).hexdigest() if value else None


def _earliest_timestamp(*values: Any) -> datetime | None:
    parsed = [item for value in values for item in (parse_timestamp(value),) if item is not None]
    return min(parsed) if parsed else None


def _structured_activation(
    fields: tuple[tuple[str, tuple[Any, ...]], ...],
) -> tuple[datetime | None, ActivationEvidence | None, tuple[dict[str, object], ...]]:
    conflicts: list[dict[str, object]] = []
    for field, values in fields:
        instants = sorted(
            {
                parsed
                for value in values
                for parsed in (parse_timestamp(value),)
                if parsed is not None
            }
        )
        if len(instants) == 1:
            instant = instants[0]
            return (
                instant,
                ActivationEvidence(
                    instant=instant,
                    source_kind="structured",
                    source_field=field,
                    primary=True,
                ),
                tuple(conflicts),
            )
        if len(instants) > 1:
            conflicts.append(
                {
                    "source_field": field,
                    "instants": [_iso(item) for item in instants],
                }
            )
    return None, None, tuple(conflicts)


def _kalshi_rule_activation(
    markets: list[Mapping[str, Any]],
) -> tuple[ActivationEvidence | None, tuple[str, ...]]:
    instants: list[datetime] = []
    for market in markets:
        rules = "\n\n".join(
            str(value or "")
            for value in (market.get("rules_primary"), market.get("rules_secondary"))
            if value
        )
        instant = esports.parse_kalshi_originally_scheduled(rules)
        if instant is not None:
            instants.append(instant)
    unique = tuple(sorted(set(instants)))
    if len(unique) != 1:
        return None, tuple(_iso(item) or "" for item in unique)
    return (
        ActivationEvidence(
            instant=unique[0],
            source_kind="rule_template",
            source_field="nested_market_rules",
            primary=False,
            parser_id="kalshi_esports_originally_scheduled_v1",
        ),
        (),
    )


def _activation_evidence_sorted(
    values: list[ActivationEvidence],
) -> tuple[ActivationEvidence, ...]:
    return tuple(
        sorted(
            values,
            key=lambda item: (
                item.instant,
                item.source_kind,
                item.source_field,
                item.parser_id or "",
            ),
        )
    )


def _limitless_sport(record: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    explicit = str(metadata.get("sportType") or "").casefold()
    if explicit in {"football", "soccer"}:
        return "soccer"
    return text.sport_from_labels(
        record.get("categories"),
        record.get("tags"),
        metadata.get("esportTitle"),
        metadata.get("videogameSlug"),
        record.get("title"),
    )


def _kalshi_traded_usd(raw: Mapping[str, Any]) -> float | None:
    """Estimate the dollars that changed hands on one Kalshi market.

    Kalshi reports volume as a contract count (``volume_fp``) and does not
    publish a dollar volume; ``notional_value_dollars`` is a constant 1.0
    describing the $1 settlement value of a single contract, not a traded
    total.  A contract trades between $0 and $1, so contracts multiplied by
    price is the quantity comparable to Polymarket's USD ``volumeNum``.

    ``last_price_dollars`` stands in for the average trade price, which the
    catalogue does not expose.  That under-states a market whose price has
    since fallen, which is the safe direction for a minimum-volume gate.
    """
    contracts = finite_number(raw.get("volume_fp"))
    price = finite_number(raw.get("last_price_dollars"))
    if contracts is None or price is None or contracts < 0 or price < 0:
        return None
    return round(contracts * price, 6)


def _participants_usable(participants: tuple[str, str] | None) -> bool:
    if participants is None:
        return False
    keys = tuple(canonical_participant(item) for item in participants)
    return bool(keys[0] and keys[1] and keys[0] != keys[1])


@dataclass
class KalshiSportsAdapter:
    registry: MarketClassRegistry
    max_series: int | None = None
    max_pages: int | None = None

    venue: str = "kalshi"

    def discover(self, client: JsonClient, *, now: datetime) -> CatalogSnapshot:
        requests_before = getattr(client, "network_requests", 0) + getattr(client, "cache_hits", 0)
        payload = _data(client.get_json(KALSHI_API, "/series", params={"category": "Sports", "limit": 1000}))
        raw_series = payload.get("series", []) if isinstance(payload, dict) else []
        candidates: list[
            tuple[
                dict[str, Any],
                str,
                ClassDefinition | None,
                GameFamily | None,
                ClassificationEvidence | None,
            ]
        ] = []
        for series in raw_series:
            if not isinstance(series, dict) or str(series.get("category") or "").casefold() != "sports":
                continue
            family, evidence = _kalshi_game_family(self.registry, series)
            sport = family.sport if family else text.sport_from_labels(
                series.get("tags"), series.get("title")
            )
            if sport == "esports" and family is None:
                continue
            if sport not in self.registry.strategy.sports:
                continue
            definition = self.registry.classify(
                self.venue,
                sport,
                {"series_title": series.get("title"), "series_tags": series.get("tags") or []},
            )
            if (definition is not None or family is not None) and series.get("ticker"):
                candidates.append((series, sport, definition, family, evidence))
        candidates.sort(key=lambda item: str(item[0].get("ticker")))

        complete = True
        diagnostics: list[str] = []
        classification_diagnostics: list[TargeterDiagnostic] = []
        if self.max_series is not None and len(candidates) > self.max_series:
            complete = False
            diagnostics.append(
                f"probe limited Kalshi series from {len(candidates)} to {self.max_series}"
            )
            candidates = candidates[: self.max_series]
        definitions = {
            str(series["ticker"]): (series, sport, definition, family, evidence)
            for series, sport, definition, family, evidence in candidates
        }

        minimum_close = int((now - timedelta(seconds=self.registry.strategy.post_start_retention_seconds)).timestamp())
        maximum_close = int((now + timedelta(seconds=self.registry.strategy.discovery_horizon_seconds)).timestamp())
        events: list[CanonicalEvent] = []
        markets: list[CanonicalMarket] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        pages = 0
        while True:
            params: dict[str, Any] = {
                "status": "open",
                "limit": 200,
                "with_nested_markets": True,
                "min_close_ts": minimum_close,
            }
            if cursor:
                params["cursor"] = cursor
            page = _data(client.get_json(KALSHI_API, "/events", params=params))
            if not isinstance(page, dict) or not isinstance(page.get("events", []), list):
                raise ValueError("Kalshi events response is malformed")
            for raw_event in page.get("events", []):
                if not isinstance(raw_event, dict):
                    continue
                matched = definitions.get(str(raw_event.get("series_ticker") or ""))
                if matched is None:
                    continue
                series, sport, definition, family, evidence = matched
                raw_markets = [item for item in raw_event.get("markets", []) if isinstance(item, dict)]
                event_id = str(raw_event.get("event_ticker") or "")
                source_ref = f"/events/{event_id}" if event_id else "/events"
                activation_evidence: list[ActivationEvidence] = []
                if family:
                    activation, structured, activation_conflicts = _structured_activation(
                        (
                            ("strike_date", (raw_event.get("strike_date"),)),
                            (
                                "occurrence_datetime",
                                tuple(
                                    market.get("occurrence_datetime")
                                    for market in raw_markets
                                ),
                            ),
                            (
                                "expected_expiration_time",
                                tuple(
                                    market.get("expected_expiration_time")
                                    for market in raw_markets
                                ),
                            ),
                            (
                                "close_time",
                                tuple(market.get("close_time") for market in raw_markets),
                            ),
                        )
                    )
                    if structured is not None:
                        activation_evidence.append(structured)
                    for conflict in activation_conflicts:
                        classification_diagnostics.append(
                            TargeterDiagnostic(
                                "intra_event_activation_conflict",
                                self.venue,
                                source_ref,
                                "warning",
                                False,
                                conflict,
                            )
                        )
                    rule_evidence, conflicting_rule_times = _kalshi_rule_activation(
                        raw_markets
                    )
                    if rule_evidence is not None:
                        activation_evidence.append(rule_evidence)
                    if len(conflicting_rule_times) > 1:
                        classification_diagnostics.append(
                            TargeterDiagnostic(
                                "conflicting_rule_times",
                                self.venue,
                                source_ref,
                                "warning",
                                False,
                                {"instants": list(conflicting_rule_times)},
                            )
                        )
                    if activation is None and rule_evidence is not None:
                        activation = rule_evidence.instant
                else:
                    # Occurrence/strike is the event instant. Expiration and
                    # close are fallbacks only for the legacy soccer path.
                    activation = _earliest_timestamp(
                        raw_event.get("strike_date"),
                        *(market.get("occurrence_datetime") for market in raw_markets),
                    ) or _earliest_timestamp(
                        *(market.get("expected_expiration_time") for market in raw_markets),
                    ) or _earliest_timestamp(
                        *(market.get("close_time") for market in raw_markets),
                    )
                if (
                    activation is None
                    or activation.timestamp() < minimum_close
                    or activation.timestamp() > maximum_close
                ):
                    continue
                title = str(raw_event.get("title") or "")
                
                if family:
                    observed_formats = esports.parse_best_of_values(
                        title, raw_event.get("sub_title")
                    )
                    if len(observed_formats) > 1:
                        classification_diagnostics.append(
                            TargeterDiagnostic(
                                "intra_event_format_conflict", self.venue,
                                source_ref, "warning", False,
                                {"format_observed": list(observed_formats)},
                            )
                        )
                        continue
                    participants = esports.parse_participants(title, family.venue_game_aliases[self.venue])
                else:
                    participants = traditional.parse_participants(title)
                    
                if not _participants_usable(participants) or not event_id:
                    if family:
                        classification_diagnostics.append(
                            TargeterDiagnostic(
                                "participant_parse_failed",
                                self.venue,
                                source_ref,
                                "warning",
                                False,
                                {"title": title, "game": family.id},
                            )
                        )
                    elif event_id and participants is not None:
                        diagnostics.append(f"skipped Kalshi event {event_id}: invalid participants")
                    continue
                
                game_evidence = (evidence,) if evidence else ()
                
                event = CanonicalEvent(
                    venue=self.venue,
                    venue_event_id=event_id,
                    sport=family.sport if family else sport,
                    league=text.league_from_labels(
                        [*(series.get("tags") or []), (raw_event.get("product_metadata") or {}).get("competition")]
                    ),
                    title=title,
                    participants=participants,
                    activation_at=activation,
                    status="open",
                    source_ref=f"/events/{event_id}",
                    format=str(
                        esports.parse_best_of(title, raw_event.get("sub_title")) or ""
                    ) or None,
                    fragment_type=definition.id if definition else "group",
                    game=family.id if family else None,
                    topology=family.topology if family else None,
                    game_evidence=game_evidence,
                    activation_evidence=_activation_evidence_sorted(
                        activation_evidence
                    ),
                    raw=raw_event,
                )
                event_markets = [
                    self._market(event, raw_market, definition, family, raw_event)
                    for raw_market in raw_markets
                ]
                event_markets = [market for market in event_markets if market is not None]
                if event_markets:
                    events.append(event)
                    markets.extend(event_markets)
            pages += 1
            next_cursor = str(page.get("cursor") or "")
            if not next_cursor:
                break
            if next_cursor in seen_cursors:
                raise ValueError("Kalshi repeated cursor")
            if self.max_pages is not None and pages >= self.max_pages:
                complete = False
                diagnostics.append(f"probe stopped Kalshi after {pages} event pages")
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        requests_after = getattr(client, "network_requests", 0) + getattr(client, "cache_hits", 0)
        return CatalogSnapshot(
            self.venue,
            tuple(sorted(events, key=lambda item: item.venue_event_id)),
            tuple(sorted(markets, key=lambda item: item.target_id)),
            complete=complete,
            diagnostics=tuple(diagnostics),
            classification_diagnostics=tuple(classification_diagnostics),
            requests=requests_after - requests_before,
        )

    def _market(
        self,
        event: CanonicalEvent,
        raw: Mapping[str, Any],
        definition: ClassDefinition | None,
        family: GameFamily | None = None,
        parent_raw: Mapping[str, Any] | None = None,
    ) -> CanonicalMarket | None:
        ticker = str(raw.get("ticker") or "")
        if not ticker:
            return None
        rules = "\n\n".join(
            part for part in (str(raw.get("rules_primary") or "").strip(), str(raw.get("rules_secondary") or "").strip()) if part
        ) or None
        labels = tuple(
            str(value) for value in (raw.get("yes_sub_title"), raw.get("no_sub_title")) if value
        ) or ("Yes", "No")
        title = str(raw.get("title") or raw.get("subtitle") or ticker)
        canonical_class = None
        market_type = None
        scope = None
        classification_evidence = None
        
        if family:
            family_class, family_evidence = _classify_market_by_family(
                self.venue, family, raw, parent_raw or {}
            )
            if family_class:
                # ``definition`` raises on an unknown class, matching the other
                # venues. The registry validates every configured mapping at
                # load, so a miss here is a strategy bug, not a vendor quirk.
                family_definition = self.registry.definition(family_class)
                canonical_class = family_class
                classification_evidence = family_evidence
                market_type = family_definition.market_type
                scope = family_definition.scope

        if not canonical_class and definition:
            canonical_class = definition.id
            market_type = definition.market_type
            scope = definition.scope
            
        if not canonical_class:
            return None

        parameters = products.market_parameters(
            market_type,
            title=title,
            participants=event.participants,
            outcome_labels=labels,
            raw=raw,
        )
        if family and market_type == "map_winner" and "map_index" not in parameters:
            event_parameters = products.market_parameters(
                market_type,
                title=event.title,
                participants=event.participants,
                outcome_labels=labels,
                raw=raw,
            )
            if isinstance(event_parameters.get("map_index"), int):
                parameters["map_index"] = event_parameters["map_index"]
        if family and market_type in {"series_moneyline", "map_winner"}:
            affirmative = canonical_participant(
                str(raw.get("yes_sub_title") or raw.get("subtitle") or "")
            )
            participant_keys = [
                canonical_participant(participant) for participant in event.participants
            ]
            exact_hits = [
                index
                for index, participant_key in enumerate(participant_keys)
                if affirmative == participant_key
            ]
            if len(exact_hits) == 1:
                parameters["side"] = "home" if exact_hits[0] == 0 else "away"

        return CanonicalMarket(
            venue=self.venue,
            venue_market_id=ticker,
            venue_event_id=event.venue_event_id,
            canonical_class=canonical_class,
            market_type=market_type,
            scope=scope,
            classification_evidence=classification_evidence,
            title=title,
            parameters=parameters,
            subscription_ids=(ticker,),
            outcome_labels=labels,
            status=str(raw.get("status") or "open").casefold(),
            accepting_orders=str(raw.get("status") or "active").casefold() in {"active", "open"},
            rules_text=rules,
            rules_hash=_rules_hash(rules),
            created_at=parse_timestamp(raw.get("created_time")),
            volume_24h=finite_number(raw.get("volume_24h_fp")),
            volume_total=finite_number(raw.get("volume_fp")),
            volume_total_usd=_kalshi_traded_usd(raw),
            # Kalshi publishes no order-book depth on this endpoint:
            # ``liquidity_dollars`` is a constant 0.0 for every market, so
            # reading it only pretends to measure something. Leave it absent
            # rather than feed a constant into activity scoring.
            liquidity=None,
            source_ref=f"/markets/{ticker}",
            raw=raw,
        )


@dataclass
class PolymarketSportsAdapter:
    registry: MarketClassRegistry
    page_size: int = 500
    max_pages_per_tag: int | None = None

    venue: str = "polymarket"

    def discover(self, client: JsonClient, *, now: datetime) -> CatalogSnapshot:
        requests_before = getattr(client, "network_requests", 0) + getattr(client, "cache_hits", 0)
        minimum = now - timedelta(seconds=self.registry.strategy.post_start_retention_seconds)
        maximum = now + timedelta(seconds=self.registry.strategy.discovery_horizon_seconds)
        raw_events: dict[str, Mapping[str, Any]] = {}
        complete = True
        diagnostics: list[str] = []
        classification_diagnostics: list[TargeterDiagnostic] = []

        for tag in self.registry.strategy.polymarket_tags:
            cursor: str | None = None
            pages = 0
            seen_cursors: set[str] = set()
            while True:
                params: dict[str, Any] = {
                    "limit": self.page_size,
                    "closed": False,
                    "tag_slug": tag,
                    "end_date_min": _iso(minimum),
                    "end_date_max": _iso(maximum),
                    "ascending": True,
                    "order": "endDate",
                }
                if cursor:
                    params["after_cursor"] = cursor
                payload = _data(client.get_json(POLYMARKET_API, "/events/keyset", params=params))
                if not isinstance(payload, dict) or not isinstance(payload.get("events", []), list):
                    raise ValueError(f"Polymarket keyset response is malformed for tag {tag}")
                for event in payload.get("events", []):
                    if isinstance(event, dict) and event.get("id") is not None:
                        raw_events[str(event["id"])] = event
                pages += 1
                next_cursor = str(payload.get("next_cursor") or "")
                if not next_cursor or next_cursor == "LTE=":
                    break
                if next_cursor in seen_cursors:
                    raise ValueError(f"Polymarket repeated cursor for tag {tag}")
                if self.max_pages_per_tag is not None and pages >= self.max_pages_per_tag:
                    complete = False
                    diagnostics.append(f"probe stopped Polymarket tag {tag} after {pages} pages")
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor

        events: list[CanonicalEvent] = []
        markets: list[CanonicalMarket] = []
        for raw_event in raw_events.values():
            title = str(raw_event.get("title") or "")
            tags = [str(item.get("slug") or "") for item in raw_event.get("tags", []) if isinstance(item, dict)]
            
            family, evidence, classification_conflict = _polymarket_game_family(
                self.registry, title, tags
            )
            if classification_conflict:
                classification_diagnostics.append(
                    TargeterDiagnostic(
                        code="game_classification_conflict",
                        venue=self.venue,
                        source_ref=f"/events/{raw_event.get('id')}",
                        severity="error",
                        completeness_effect=True,
                        details={"title": title, "tags": sorted(tags)},
                    )
                )
                complete = False
                continue
            
            if family:
                participants = esports.parse_participants(title, family.venue_game_aliases[self.venue])
            else:
                participants = traditional.parse_participants(title)
                
            activation, structured_activation, _ = _structured_activation(
                (
                    ("eventStartTime", (raw_event.get("eventStartTime"),)),
                    ("startTime", (raw_event.get("startTime"),)),
                    ("endDate", (raw_event.get("endDate"),)),
                )
            )
            sport = family.sport if family else text.sport_from_labels(tags, title)
            if sport == "esports" and family is None:
                # Structured game-family classification is the only esports
                # authority. Never revive an unsupported game via registry
                # title patterns.
                classification_diagnostics.append(TargeterDiagnostic(
                    "unsupported_game", self.venue,
                    f"/events/{raw_event.get('id')}", "warning", False,
                    {"title": title, "tags": sorted(tags)},
                ))
                continue
            
            if (
                not _participants_usable(participants)
                or activation is None
                or not minimum <= activation <= maximum
                or (not family and sport not in self.registry.strategy.sports)
            ):
                if participants is not None and not _participants_usable(participants):
                    diagnostics.append(
                        f"skipped Polymarket event {raw_event.get('id')}: invalid participants"
                    )
                continue
            event_id = str(raw_event.get("id"))
            observed_formats = (
                esports.parse_best_of_values(title, raw_event.get("description"))
                if family
                else ()
            )
            if len(observed_formats) > 1:
                classification_diagnostics.append(
                    TargeterDiagnostic(
                        "intra_event_format_conflict", self.venue,
                        f"/events/{event_id}", "warning", False,
                        {"format_observed": list(observed_formats)},
                    )
                )
                continue
            event = CanonicalEvent(
                venue=self.venue,
                venue_event_id=event_id,
                sport=sport,
                league=text.league_from_labels(tags),
                title=title,
                participants=participants,
                activation_at=activation,
                status="open" if not raw_event.get("closed") else "closed",
                source_ref=f"/events/{event_id}",
                format=str(
                    esports.parse_best_of(title, raw_event.get("description")) or ""
                ) or None,
                fragment_type=_fragment_type(title),
                game=family.id if family else None,
                topology=family.topology if family else None,
                game_evidence=(evidence,) if evidence else (),
                activation_evidence=(
                    (structured_activation,) if family and structured_activation else ()
                ),
                raw=raw_event,
            )
            event_markets: list[CanonicalMarket] = []
            for raw_market in raw_event.get("markets", []):
                if not isinstance(raw_market, dict):
                    continue
                market = self._market(event, raw_market, family=family, parent_raw=raw_event)
                if market is not None:
                    event_markets.append(market)
                elif family:
                    canonical_class, _evidence = _classify_market_by_family(
                        self.venue, family, raw_market, raw_event
                    )
                    group_title = str(raw_market.get("groupItemTitle") or "")
                    if canonical_class is not None:
                        code, completeness_effect = "invalid_product_parameters", False
                    elif text.normalize_label(group_title) == "match winner":
                        code, completeness_effect = "unclassified_anchor_candidate", True
                    else:
                        code, completeness_effect = "unclassified_sibling_product", False
                    classification_diagnostics.append(TargeterDiagnostic(
                        code, self.venue,
                        f"/markets/{raw_market.get('id') or event_id}",
                        "error" if completeness_effect else "warning",
                        completeness_effect,
                        {"game": family.id, "group_title": group_title},
                    ))
            if event_markets:
                events.append(event)
                markets.extend(event_markets)

        requests_after = getattr(client, "network_requests", 0) + getattr(client, "cache_hits", 0)
        return CatalogSnapshot(
            self.venue,
            tuple(sorted(events, key=lambda item: item.venue_event_id)),
            tuple(sorted(markets, key=lambda item: item.target_id)),
            complete=complete,
            diagnostics=tuple(diagnostics),
            classification_diagnostics=tuple(classification_diagnostics),
            requests=requests_after - requests_before,
        )

    def _market(
        self,
        event: CanonicalEvent,
        raw: Mapping[str, Any],
        family: GameFamily | None = None,
        parent_raw: Mapping[str, Any] | None = None,
    ) -> CanonicalMarket | None:
        if event.fragment_type in {
            "halftime_result", "first_half_result", "second_half_result",
            "first_team_to_score", "total_corners",
        }:
            return None
        market_id = str(raw.get("id") or "")
        tokens = tuple(_json_list(raw.get("clobTokenIds")))
        if not market_id or not tokens:
            return None
        classification_evidence = None
        if family:
            canonical_class, classification_evidence = _classify_market_by_family(
                self.venue, family, raw, parent_raw or {}
            )
            if canonical_class is None:
                return None
            definition = self.registry.definition(canonical_class)
        else:
            fields = {
                "event_title": event.title,
                "group_title": raw.get("groupItemTitle"),
                "question": raw.get("question"),
            }
            definition = self.registry.classify(self.venue, event.sport, fields)
            if definition is None:
                return None
            canonical_class = definition.id
        market_type = definition.market_type
        # The broad event-title moneyline fallback must not relabel an unknown
        # child of a "More Markets" event as a match winner.
        group_title = str(raw.get("groupItemTitle") or "")
        if definition.market_type in {"moneyline_3way", "series_moneyline"}:
            participant_keys = {canonical_participant(part) for part in event.participants}
            group_key = canonical_participant(group_title)
            outcome_keys = {canonical_participant(label) for label in _json_list(raw.get("outcomes"))}
            named_outcomes = outcome_keys & (participant_keys | {"draw", "tie"})
            if (
                group_key not in participant_keys | {"draw", "tie", "match winner"}
                and len(named_outcomes) < 2
            ):
                return None
        outcomes = tuple(_json_list(raw.get("outcomes"))) or ("Yes", "No")
        title = str(raw.get("groupItemTitle") or raw.get("question") or market_id)
        rules = str(raw.get("description") or event.raw.get("description") or "").strip() or None
        parameters = products.market_parameters(
            market_type,
            title=title,
            participants=event.participants,
            outcome_labels=outcomes,
            raw=raw,
        )
        if family:
            if market_type == "series_moneyline":
                participant_keys = {
                    canonical_participant(participant)
                    for participant in event.participants
                }
                outcome_keys = {
                    canonical_participant(outcome) for outcome in outcomes
                }
                if len(tokens) != len(outcomes) or not participant_keys <= outcome_keys:
                    return None
            elif market_type == "map_winner" and not (
                isinstance(parameters.get("map_index"), int)
                and parameters["map_index"] > 0
            ):
                return None
            elif market_type == "total_maps":
                line = parameters.get("line")
                if not isinstance(line, (int, float)) or line <= 0 or (line * 2) % 1:
                    return None
            elif market_type == "map_handicap":
                line = parameters.get("line")
                if not isinstance(line, (int, float)) or not line:
                    return None
        return CanonicalMarket(
            venue=self.venue,
            venue_market_id=market_id,
            venue_event_id=event.venue_event_id,
            canonical_class=canonical_class,
            market_type=market_type,
            scope=definition.scope,
            classification_evidence=classification_evidence,
            title=title,
            parameters=parameters,
            subscription_ids=tokens,
            outcome_labels=outcomes,
            status="open" if raw.get("active", True) and not raw.get("closed") else "closed",
            accepting_orders=bool(raw.get("acceptingOrders", raw.get("active", False))),
            rules_text=rules,
            rules_hash=_rules_hash(rules),
            created_at=parse_timestamp(raw.get("createdAt")),
            volume_24h=finite_number(raw.get("volume24hr")),
            volume_total=finite_number(raw.get("volumeNum", raw.get("volume"))),
            volume_total_usd=finite_number(raw.get("volumeNum", raw.get("volume"))),
            liquidity=finite_number(raw.get("liquidityNum", raw.get("liquidity"))),
            source_ref=f"/markets/{market_id}",
            raw=raw,
        )


def _fragment_type(title: str) -> str:
    lowered = title.casefold()
    for fragment in (
        "more markets", "exact score", "correct score", "halftime result",
        "first half result", "second half result", "first team to score", "total corners",
    ):
        if lowered.endswith(fragment):
            return fragment.replace(" ", "_")
    return "base"


@dataclass
class LimitlessSportsAdapter:
    registry: MarketClassRegistry
    page_size: int = 25
    max_pages: int | None = None

    venue: str = "limitless"

    @staticmethod
    def _record_id(item: Mapping[str, Any]) -> str:
        return str(
            item.get("id")
            or item.get("slug")
            or stable_id("limitless_record", item)
        )

    def discover(self, client: JsonClient, *, now: datetime) -> CatalogSnapshot:
        requests_before = getattr(client, "network_requests", 0) + getattr(client, "cache_hits", 0)
        classification_diagnostics: list[TargeterDiagnostic] = []

        # ``automationType=sports`` serves the Sports category only; esports
        # lives under its own category and is invisible to that query even
        # though its records carry the same automationType. Each configured
        # category is therefore read as its own paginated source and merged by
        # vendor ID.
        sources: list[tuple[str, str, dict[str, Any]]] = [
            ("sports", "/markets/active", {"automationType": "sports"})
        ]
        for category_id in self.registry.strategy.limitless_category_ids:
            sources.append((f"category {category_id}", f"/markets/active/{category_id}", {}))

        records_by_id: dict[str, Mapping[str, Any]] = {}
        complete = True
        diagnostics: list[str] = []
        for label, path, extra in sources:
            found, source_diagnostics, source_complete = self._paginate_source(
                client, label=label, path=path, params=extra
            )
            records_by_id.update(found)
            diagnostics.extend(source_diagnostics)
            complete = complete and source_complete

        return self._build_catalog(
            client,
            now=now,
            records_by_id=records_by_id,
            complete=complete,
            diagnostics=diagnostics,
            classification_diagnostics=classification_diagnostics,
            requests_before=requests_before,
        )

    def _paginate_source(
        self,
        client: JsonClient,
        *,
        label: str,
        path: str,
        params: Mapping[str, Any],
    ) -> tuple[dict[str, Mapping[str, Any]], list[str], bool]:
        """Read one Limitless catalogue source, reconciled by stable vendor ID.

        This endpoint is a live page-number catalogue, not a snapshot.  A
        market inserted or removed during discovery can change the reported
        total and move a record across page boundaries.  Re-read once when
        the first pass cannot describe a stable catalogue, and reconcile the
        two observations by vendor ID.  A permanently inconsistent response
        remains fatal; an indefinitely moving catalogue does not cause an
        unbounded retry loop.
        """
        record_id = self._record_id
        records_by_id: dict[str, Mapping[str, Any]] = {}
        diagnostics: list[str] = []
        complete = True
        observed_totals: list[int] = []

        for pass_number in (1, 2):
            page = 1
            pass_ids: set[str] = set()
            pass_totals: list[int] = []
            probe_stopped = False

            while True:
                payload = _data(
                    client.get_json(
                        LIMITLESS_API,
                        path,
                        params={"page": page, "limit": self.page_size, **params},
                    )
                )
                if not isinstance(payload, dict) or not isinstance(payload.get("data", []), list):
                    raise ValueError(
                        f"Limitless {label} active markets response is malformed"
                    )
                items = [item for item in payload.get("data", []) if isinstance(item, dict)]
                raw_total = payload.get("totalMarketsCount")
                if isinstance(raw_total, bool) or not isinstance(raw_total, int) or raw_total < 0:
                    raise ValueError(
                        f"Limitless {label} response has an invalid totalMarketsCount"
                    )

                item_ids = [record_id(item) for item in items]
                page_ids = set(item_ids)
                if items and not (page_ids - pass_ids):
                    raise ValueError(
                        f"Limitless {label} repeated page at page {page} "
                        f"during pass {pass_number}"
                    )
                pass_ids.update(page_ids)
                pass_totals.append(raw_total)
                observed_totals.append(raw_total)
                for item, item_id in zip(items, item_ids, strict=True):
                    records_by_id[item_id] = item

                totals_changed = len(set(pass_totals)) > 1
                terminal_page = len(items) < self.page_size
                reached_stable_total = not totals_changed and len(pass_ids) == raw_total
                if terminal_page or reached_stable_total:
                    break
                if self.max_pages is not None and page >= self.max_pages:
                    complete = False
                    diagnostics.append(
                        f"probe stopped Limitless {label} after {page} pages "
                        f"of {raw_total} markets"
                    )
                    probe_stopped = True
                    break
                page += 1

            if probe_stopped:
                break

            first_total = pass_totals[0]
            final_total = pass_totals[-1]
            totals_changed = len(set(pass_totals)) > 1
            count_matches = len(pass_ids) == final_total
            if pass_number == 1 and (totals_changed or not count_matches):
                if totals_changed:
                    diagnostics.append(
                        f"Limitless {label} totalMarketsCount changed during pass 1 "
                        f"from {first_total} to {final_total}"
                    )
                else:
                    diagnostics.append(
                        f"Limitless {label} pagination count did not match its reported "
                        "total during pass 1; reconciling by stable ID"
                    )
                continue

            if pass_number == 2 and totals_changed:
                diagnostics.append(
                    f"Limitless {label} totalMarketsCount continued changing during "
                    "reconciliation; accepted the bounded two-pass stable-ID union"
                )
            elif not count_matches:
                if len(pass_ids) < final_total:
                    raise ValueError(
                        f"Limitless {label} pagination ended before reported total "
                        f"({len(pass_ids)} of {final_total})"
                    )
                raise ValueError(
                    f"Limitless {label} returned {len(pass_ids)} records for "
                    f"reported total {final_total}"
                )

            if pass_number == 2:
                diagnostics.append(
                    f"Limitless {label} pagination reconciled by stable ID across 2 "
                    f"passes ({len(records_by_id)} unique records; observed totals "
                    f"{min(observed_totals)}..{max(observed_totals)})"
                )
            break

        return records_by_id, diagnostics, complete

    def _build_catalog(
        self,
        client: JsonClient,
        *,
        now: datetime,
        records_by_id: Mapping[str, Mapping[str, Any]],
        complete: bool,
        diagnostics: list[str],
        classification_diagnostics: list[TargeterDiagnostic],
        requests_before: int,
    ) -> CatalogSnapshot:
        record_id = self._record_id
        raw_records = list(records_by_id.values())
        family_by_record: dict[
            str, tuple[GameFamily | None, ClassificationEvidence | None]
        ] = {}

        groups: list[tuple[Mapping[str, Any], str, tuple[str, str], datetime]] = []
        minimum = now - timedelta(seconds=self.registry.strategy.post_start_retention_seconds)
        maximum = now + timedelta(seconds=self.registry.strategy.discovery_horizon_seconds)
        for record in raw_records:
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            family, evidence, classification_conflict = _limitless_game_family(
                self.registry, record, metadata
            )
            if classification_conflict:
                classification_diagnostics.append(TargeterDiagnostic(
                    "game_classification_conflict", self.venue,
                    f"/markets/{record.get('slug') or record.get('id')}",
                    "error", True,
                    {"title": str(record.get("title") or ""), "metadata": {
                        key: metadata[key] for key in ("esportTitle", "videogameSlug") if metadata.get(key)
                    }},
                ))
                family, evidence = None, None
            family_by_record[record_id(record)] = (family, evidence)
            sport, participants, activation = self._derive(record, metadata, family)
            if (
                isinstance(record.get("markets"), list)
                and bool(record.get("markets"))
                and sport in self.registry.strategy.sports
                and _participants_usable(participants)
                and activation
                and minimum <= activation <= maximum
            ):
                groups.append((record, sport, participants, activation))

        group_by_key = {
            (
                tuple(sorted(canonical_participant(part) for part in participants)),
                int(record.get("expirationTimestamp") or 0),
            ): item
            for item in groups
            for record, _sport, participants, _activation in (item,)
        }
        event_by_id: dict[str, CanonicalEvent] = {}
        markets: list[CanonicalMarket] = []

        for record in raw_records:
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            nested = [item for item in record.get("markets", []) if isinstance(item, dict)]
            family, evidence = family_by_record.get(
                record_id(record), (None, None)
            )
            if nested:
                sport, participants, activation = self._derive(record, metadata, family)
                if sport == "esports" and family is None:
                    continue
                if (
                    sport not in self.registry.strategy.sports
                    or not _participants_usable(participants)
                    or activation is None
                    or not minimum <= activation <= maximum
                ):
                    continue
                event = self._event(record, sport, participants, activation, family=family, evidence=evidence)
                if event is None:
                    classification_diagnostics.append(
                        TargeterDiagnostic(
                            "intra_event_format_conflict", self.venue,
                            f"/markets/{record.get('slug') or record.get('id')}",
                            "warning", False,
                            {"format_observed": list(self._observed_formats(record))},
                        )
                    )
                    continue
                event_by_id[event.venue_event_id] = event
                for child in nested:
                    market = self._market(event, child, metadata, family=family)
                    if market is not None:
                        markets.append(market)
                continue

            participants = traditional.parse_prop_participants(str(record.get("title") or ""))
            if not _participants_usable(participants):
                continue
            key = (
                tuple(sorted(canonical_participant(part) for part in participants)),
                int(record.get("expirationTimestamp") or 0),
            )
            matched = group_by_key.get(key)
            if matched is None:
                continue
            group, sport, canonical_participants, activation = matched
            event_id = str(group.get("id"))
            group_family, group_evidence = family_by_record.get(
                record_id(group), (None, None)
            )
            event = event_by_id.get(event_id) or self._event(
                group,
                sport,
                canonical_participants,
                activation,
                family=group_family,
                evidence=group_evidence,
            )
            event_by_id[event_id] = event
            market = self._market(
                event, record, metadata, family=group_family
            )
            if market is not None:
                markets.append(market)

        # Drop groups whose recognised child set ended up empty.
        market_event_ids = {market.venue_event_id for market in markets}
        events = [event for event_id, event in event_by_id.items() if event_id in market_event_ids]
        requests_after = getattr(client, "network_requests", 0) + getattr(client, "cache_hits", 0)
        return CatalogSnapshot(
            self.venue,
            tuple(sorted(events, key=lambda item: item.venue_event_id)),
            tuple(sorted(markets, key=lambda item: item.target_id)),
            complete=complete,
            diagnostics=tuple(diagnostics),
            classification_diagnostics=tuple(classification_diagnostics),
            requests=requests_after - requests_before,
        )

    def _derive(
        self,
        record: Mapping[str, Any],
        metadata: Mapping[str, Any],
        family: GameFamily | None,
    ) -> tuple[str | None, tuple[str, str] | None, datetime | None]:
        """Resolve the sport, participants, and activation one record claims.

        Both discovery passes read a record the same way; only the filters they
        apply afterwards differ.
        """
        sport = family.sport if family else _limitless_sport(record, metadata)
        if metadata.get("homeTeam") and metadata.get("awayTeam"):
            participants = (str(metadata["homeTeam"]), str(metadata["awayTeam"]))
        elif family:
            participants = esports.parse_participants(
                str(record.get("title") or ""), family.venue_game_aliases[self.venue]
            )
        else:
            participants = traditional.parse_participants(str(record.get("title") or ""))
        return (
            sport,
            participants,
            parse_timestamp(metadata.get("startMatchTimestampInUTC")),
        )

    @staticmethod
    def _observed_formats(raw: Mapping[str, Any]) -> tuple[int, ...]:
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        text_values = esports.parse_best_of_values(raw.get("title"), raw.get("description"))
        games = finite_number(metadata.get("numberOfGames"))
        metadata_values = ((int(games),) if games is not None and int(games) == games and int(games) > 0 else ())
        return tuple(sorted(set(text_values + metadata_values)))

    def _event(
        self,
        raw: Mapping[str, Any],
        sport: str,
        participants: tuple[str, str],
        activation: datetime,
        family: GameFamily | None = None,
        evidence: ClassificationEvidence | None = None,
    ) -> CanonicalEvent | None:
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        event_id = str(
            metadata.get("eventId")
            or raw.get("id")
            or stable_id("limitless_event", [*participants, activation.isoformat()])
        )
        observed = self._observed_formats(raw)
        if len(observed) > 1:
            return None
        best_of = observed[0] if len(observed) == 1 else None
        return CanonicalEvent(
            venue=self.venue,
            venue_event_id=event_id,
            sport=sport,
            league=str(metadata.get("leagueKey") or metadata.get("leagueName") or "") or None,
            title=str(raw.get("title") or ""),
            participants=participants,
            activation_at=activation,
            status=str(raw.get("status") or "open").casefold(),
            source_ref=f"/markets/{raw.get('slug') or event_id}",
            format=str(best_of or "") or None,
            fragment_type="group",
            game=family.id if family else None,
            topology=family.topology if family else None,
            game_evidence=(evidence,) if evidence else (),
            activation_evidence=(
                ActivationEvidence(
                    activation,
                    "structured",
                    "metadata.startMatchTimestampInUTC",
                    True,
                ),
            )
            if family
            else (),
            raw=raw,
        )

    def _market(
        self,
        event: CanonicalEvent,
        raw: Mapping[str, Any],
        parent_metadata: Mapping[str, Any],
        family: GameFamily | None = None,
    ) -> CanonicalMarket | None:
        market_id = str(raw.get("id") or "")
        slug = str(raw.get("slug") or "")
        if not market_id or not slug:
            return None
        classification_evidence = None
        if family:
            canonical_class, classification_evidence = _classify_market_by_family(
                self.venue, family, raw, parent_metadata
            )
            if canonical_class is None:
                return None
            definition = self.registry.definition(canonical_class)
        else:
            fields = {
                "metadata_market_type": parent_metadata.get("marketType"),
                "market_title": raw.get("title"),
            }
            definition = self.registry.classify(self.venue, event.sport, fields)
            if definition is None:
                return None
            canonical_class = definition.id
        market_type = definition.market_type
        title = str(raw.get("title") or market_id)
        labels = tuple(str(item) for item in (raw.get("outcomeTokens") or [])) or ("Yes", "No")
        rules = str(raw.get("description") or "").strip() or None
        volume_total = finite_number(raw.get("volumeFormatted"))
        if volume_total is None:
            raw_volume = finite_number(raw.get("volume"))
            volume_total = raw_volume / 1_000_000.0 if raw_volume is not None else None
        return CanonicalMarket(
            venue=self.venue,
            venue_market_id=market_id,
            venue_event_id=event.venue_event_id,
            canonical_class=canonical_class,
            market_type=market_type,
            scope=definition.scope,
            classification_evidence=classification_evidence,
            title=title,
            parameters=products.market_parameters(
                market_type,
                title=title,
                participants=event.participants,
                outcome_labels=labels,
                raw=raw,
            ),
            subscription_ids=(slug,),
            outcome_labels=labels,
            status=str(raw.get("status") or "open").casefold(),
            accepting_orders=(
                str(raw.get("tradeType") or "").casefold() == "clob"
                and not bool(raw.get("expired"))
                and not bool(raw.get("hidden"))
            ),
            rules_text=rules,
            rules_hash=_rules_hash(rules),
            created_at=parse_timestamp(raw.get("createdAt")),
            volume_total=volume_total,
            volume_total_usd=volume_total,
            source_ref=f"/markets/{slug}",
            raw=raw,
        )


def live_adapters(
    strategy: Strategy,
    *,
    max_kalshi_series: int | None = None,
    max_kalshi_pages: int | None = None,
    max_polymarket_pages: int | None = None,
    max_limitless_pages: int | None = None,
) -> tuple[Any, ...]:
    registry = MarketClassRegistry(strategy)
    return (
        KalshiSportsAdapter(
            registry,
            max_series=max_kalshi_series,
            max_pages=max_kalshi_pages,
        ),
        PolymarketSportsAdapter(registry, max_pages_per_tag=max_polymarket_pages),
        LimitlessSportsAdapter(registry, max_pages=max_limitless_pages),
    )


def durable_client(
    cache_root: Any,
    *,
    force_refresh: bool = False,
    persist_responses: bool = True,
) -> DurableJsonClient:
    return RetryingJsonClient(
        cache_root,
        force_refresh=force_refresh,
        persist_responses=persist_responses,
        compress_responses=True,
    )
