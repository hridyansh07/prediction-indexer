from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping

from targeter.v2.continuity import TerminalProbe, TerminalState
from targeter.v2.models import (
    ActivationEvidence,
    CanonicalEvent,
    CanonicalMarket,
    CatalogSnapshot,
    ClassificationEvidence,
    TargeterDiagnostic,
    canonical_participant,
    finite_number,
    parse_timestamp,
    stable_id,
)
from targeter.v2.parsing import esports, products, text, traditional
from targeter.v2.registry import ClassDefinition, GameFamily, MarketClassRegistry
from .common import (
    JsonClient,
    _activation_evidence_sorted,
    _classify_market_by_family,
    _data,
    _earliest_timestamp,
    _game_evidence,
    _iso,
    _json_list,
    _participants_usable,
    _rules_hash,
    _structured_activation,
)

POLYMARKET_API = "https://gamma-api.polymarket.com"


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


@dataclass
class PolymarketSportsAdapter:
    registry: MarketClassRegistry
    page_size: int = 500
    max_pages_per_tag: int | None = None

    venue: str = "polymarket"

    def probe_terminal(
        self, client: JsonClient, market_ids: tuple[str, ...]
    ) -> dict[str, TerminalProbe]:
        result: dict[str, TerminalProbe] = {}
        for market_id in market_ids:
            try:
                raw = _data(client.get_json(POLYMARKET_API, f"/markets/{market_id}"))
            except Exception:  # noqa: BLE001 - 404 and transport failures retain capture
                result[market_id] = TerminalProbe(
                    TerminalState.UNKNOWN,
                    "terminal_probe_failed",
                )
                continue
            if not isinstance(raw, dict) or not isinstance(raw.get("acceptingOrders"), bool):
                result[market_id] = TerminalProbe(
                    TerminalState.UNKNOWN,
                    "terminal_shape_ambiguous",
                )
            elif raw["acceptingOrders"]:
                result[market_id] = TerminalProbe(
                    TerminalState.OPEN,
                    "accepting_orders",
                )
            else:
                # `active` is intentionally ignored: it remains true after the
                # CLOB has stopped accepting orders.
                result[market_id] = TerminalProbe(
                    TerminalState.TERMINAL,
                    "not_accepting_orders",
                )
        return result

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
