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

KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"


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


@dataclass
class KalshiSportsAdapter:
    registry: MarketClassRegistry
    max_series: int | None = None
    max_pages: int | None = None

    venue: str = "kalshi"

    def probe_terminal(
        self, client: JsonClient, market_ids: tuple[str, ...]
    ) -> dict[str, TerminalProbe]:
        if not market_ids:
            return {}
        unknown = {
            market_id: TerminalProbe(TerminalState.UNKNOWN, "terminal_probe_failed")
            for market_id in market_ids
        }
        try:
            payload = _data(
                client.get_json(
                    KALSHI_API,
                    "/markets",
                    params={"tickers": ",".join(sorted(market_ids)), "limit": len(market_ids)},
                )
            )
        except Exception:  # noqa: BLE001 - uncertainty must retain capture
            return unknown
        if not isinstance(payload, dict) or not isinstance(payload.get("markets"), list):
            return unknown
        result = dict(unknown)
        for raw in payload["markets"]:
            if not isinstance(raw, dict):
                continue
            ticker = str(raw.get("ticker") or "")
            if ticker not in result:
                continue
            status = str(raw.get("status") or "").casefold()
            close_time = parse_timestamp(raw.get("close_time"))
            expiration_time = parse_timestamp(raw.get("expiration_time"))
            if status == "finalized":
                result[ticker] = TerminalProbe(TerminalState.TERMINAL, "status_finalized")
            elif close_time is not None and expiration_time is not None and close_time < expiration_time:
                result[ticker] = TerminalProbe(
                    TerminalState.TERMINAL,
                    "close_before_expiration",
                )
            elif status in {"active", "open"} and (
                close_time is None
                or expiration_time is None
                or close_time == expiration_time
            ):
                result[ticker] = TerminalProbe(TerminalState.OPEN, "status_open")
            else:
                result[ticker] = TerminalProbe(
                    TerminalState.UNKNOWN,
                    "terminal_shape_ambiguous",
                )
        return result

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
