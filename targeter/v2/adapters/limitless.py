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

LIMITLESS_API = "https://api.limitless.exchange"


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


@dataclass
class LimitlessSportsAdapter:
    registry: MarketClassRegistry
    page_size: int = 25
    max_pages: int | None = None

    venue: str = "limitless"

    def probe_terminal(
        self, client: JsonClient, source_refs: Mapping[str, str]
    ) -> dict[str, TerminalProbe]:
        result: dict[str, TerminalProbe] = {}
        for target_id, source_ref in sorted(source_refs.items()):
            if not source_ref.startswith("/markets/"):
                result[target_id] = TerminalProbe(
                    TerminalState.UNKNOWN,
                    "terminal_source_ref_invalid",
                )
                continue
            try:
                raw = _data(client.get_json(LIMITLESS_API, source_ref))
            except Exception:  # noqa: BLE001 - 404 and transport failures retain capture
                result[target_id] = TerminalProbe(
                    TerminalState.UNKNOWN,
                    "terminal_probe_failed",
                )
                continue
            if not isinstance(raw, dict):
                result[target_id] = TerminalProbe(
                    TerminalState.UNKNOWN,
                    "terminal_shape_ambiguous",
                )
                continue
            status = str(raw.get("status") or "").casefold()
            expired = raw.get("expired")
            if expired is True or status == "resolved":
                result[target_id] = TerminalProbe(
                    TerminalState.TERMINAL,
                    "expired_or_resolved",
                )
            elif expired is False and status in {"funded", "open", "active"}:
                # `tradeType` remains `clob` after resolution and is not a
                # terminal discriminator.
                result[target_id] = TerminalProbe(
                    TerminalState.OPEN,
                    "funded_not_expired",
                )
            else:
                result[target_id] = TerminalProbe(
                    TerminalState.UNKNOWN,
                    "terminal_shape_ambiguous",
                )
        return result

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
