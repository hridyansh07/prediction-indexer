"""Content-addressed market metadata read through the byte-stream boundary."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from replay.stream import ByteStreamer, read_object


@dataclass(frozen=True)
class FeeTerms:
    fee_type: str
    rate: Decimal
    exponent: Decimal
    taker_only: bool
    source_record_hash: str

    def as_record(self) -> dict[str, object]:
        return {
            "fee_type": self.fee_type,
            "rate": str(self.rate),
            "exponent": str(self.exponent),
            "taker_only": self.taker_only,
            "source_record_hash": self.source_record_hash,
        }


@dataclass(frozen=True)
class InstrumentMetadata:
    venue: str
    subscription_asset_id: str
    condition_id: str
    market_id: str
    outcome: str | None
    outcome_index: int | None
    native_token_id: str | None
    size_scale: Decimal
    minimum_order_size: Decimal | None
    fee_terms: FeeTerms | None
    resolution_source: str | None
    observation_method: str | None
    fixing_time: str | int | None
    resolution_identity_status: str
    resolution_identity_conflicts: tuple[str, ...]
    catalogue_record_hash: str

    def as_record(self) -> dict[str, object]:
        return {
            "venue": self.venue,
            "subscription_asset_id": self.subscription_asset_id,
            "condition_id": self.condition_id,
            "market_id": self.market_id,
            "outcome": self.outcome,
            "outcome_index": self.outcome_index,
            "native_token_id": self.native_token_id,
            "size_scale": str(self.size_scale),
            "minimum_order_size": (
                str(self.minimum_order_size)
                if self.minimum_order_size is not None
                else None
            ),
            "fee_terms": (
                self.fee_terms.as_record() if self.fee_terms is not None else None
            ),
            "resolution_source": self.resolution_source,
            "observation_method": self.observation_method,
            "fixing_time": self.fixing_time,
            "resolution_identity_status": self.resolution_identity_status,
            "resolution_identity_conflicts": list(
                self.resolution_identity_conflicts
            ),
            "catalogue_record_hash": self.catalogue_record_hash,
        }


class MetadataCatalogue:
    def __init__(
        self,
        snapshots: dict[str, tuple[InstrumentMetadata, ...]],
    ) -> None:
        self.snapshots = snapshots
        all_instruments: dict[tuple[str, str], InstrumentMetadata] = {}
        for digest in sorted(snapshots):
            for item in snapshots[digest]:
                all_instruments[(item.venue, item.subscription_asset_id)] = item
        self.latest_by_asset = all_instruments

    @classmethod
    def from_streamer(cls, streamer: ByteStreamer) -> "MetadataCatalogue":
        snapshots: dict[str, tuple[InstrumentMetadata, ...]] = {}
        for key in streamer.object_keys():
            if "/metadata/" not in f"/{key}" or not key.endswith(".json"):
                continue
            document = json.loads(read_object(streamer, key))
            if not isinstance(document, dict):
                raise ValueError(f"metadata is not an object: {key}")
            digest = document.get("metadata_digest")
            venue = document.get("venue")
            targets = document.get("targets")
            if not isinstance(digest, str) or not isinstance(venue, str):
                raise ValueError(f"metadata identity missing: {key}")
            if not isinstance(targets, list):
                raise ValueError(f"metadata targets missing: {key}")
            snapshots[digest] = tuple(
                item
                for target in targets
                if isinstance(target, dict)
                for item in (_instrument(venue, target),)
                if item is not None
            )
        return cls(snapshots)

    def by_asset(
        self, venue: str, asset_id: str, digest: str | None = None
    ) -> InstrumentMetadata | None:
        if digest is not None:
            for item in self.snapshots.get(digest, ()):
                if (
                    item.venue == venue
                    and item.subscription_asset_id == asset_id
                ):
                    return item
        return self.latest_by_asset.get((venue, asset_id))


def _instrument(venue: str, target: dict[str, Any]) -> InstrumentMetadata | None:
    asset_id = target.get("asset_id")
    condition_id = target.get("condition_id")
    market_id = target.get("market_id")
    resolution = target.get("resolution")
    record = (
        resolution.get("catalogue_record")
        if isinstance(resolution, dict)
        else None
    )
    record_hash = (
        resolution.get("catalogue_record_hash")
        if isinstance(resolution, dict)
        else None
    )
    if (
        not isinstance(asset_id, str)
        or condition_id is None
        or market_id is None
        or not isinstance(record, dict)
        or not isinstance(record_hash, str)
    ):
        return None
    computed = hashlib.sha256(
        json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    if computed != record_hash:
        raise ValueError(f"catalogue record hash mismatch for {venue}/{asset_id}")

    if venue == "polymarket":
        tokens = _json_list(record.get("clobTokenIds"))
        outcomes = _json_list(record.get("outcomes"))
        try:
            outcome_index = [str(value) for value in tokens].index(asset_id)
        except ValueError:
            outcome_index = _note_index(target.get("note"))
        outcome = (
            str(outcomes[outcome_index])
            if outcome_index is not None and outcome_index < len(outcomes)
            else None
        )
        resolution_source, observation = _polymarket_resolution(record)
        return InstrumentMetadata(
            venue=venue,
            subscription_asset_id=asset_id,
            condition_id=str(condition_id),
            market_id=str(market_id),
            outcome=outcome,
            outcome_index=outcome_index,
            native_token_id=asset_id,
            size_scale=Decimal(1),
            minimum_order_size=_decimal_or_none(record.get("orderMinSize")),
            fee_terms=_fee_terms(record, record_hash),
            resolution_source=resolution_source,
            observation_method=observation,
            fixing_time=record.get("endDate"),
            resolution_identity_status=(
                "STRUCTURED"
                if resolution_source is not None and observation is not None
                else "RAW_TEXT_ONLY"
            ),
            resolution_identity_conflicts=(),
            catalogue_record_hash=record_hash,
        )

    if venue == "limitless":
        tokens = record.get("tokens")
        yes_token = tokens.get("yes") if isinstance(tokens, dict) else None
        collateral = record.get("collateralToken")
        decimals = collateral.get("decimals") if isinstance(collateral, dict) else 0
        try:
            size_scale = Decimal(10) ** int(decimals)
        except (TypeError, ValueError):
            size_scale = Decimal(1)
        oracle = record.get("priceOracleMetadata")
        resolution_source = None
        observation = None
        conflicts: tuple[str, ...] = ()
        identity_status = "RAW_TEXT_ONLY"
        if isinstance(oracle, dict):
            chart = oracle.get("chartSource")
            pair, conflicts = _limitless_pair(record, oracle)
            if chart is not None:
                resolution_source = (
                    f"{str(chart).lower()}:{pair}" if pair else str(chart).lower()
                )
            observation = "point_fixing"
            identity_status = (
                "CONFLICT"
                if conflicts
                else "STRUCTURED"
                if resolution_source is not None
                else "RAW_TEXT_ONLY"
            )
        settings = record.get("settings")
        minimum_native = (
            settings.get("minSize") if isinstance(settings, dict) else None
        )
        minimum = _decimal_or_none(minimum_native)
        if minimum is not None and size_scale:
            minimum /= size_scale
        return InstrumentMetadata(
            venue=venue,
            subscription_asset_id=asset_id,
            condition_id=str(condition_id),
            market_id=str(market_id),
            outcome="Yes",
            outcome_index=0,
            native_token_id=str(yes_token) if yes_token is not None else None,
            size_scale=size_scale,
            minimum_order_size=minimum,
            fee_terms=_fee_terms(record, record_hash),
            resolution_source=resolution_source,
            observation_method=observation,
            fixing_time=record.get("expirationTimestamp"),
            resolution_identity_status=identity_status,
            resolution_identity_conflicts=conflicts,
            catalogue_record_hash=record_hash,
        )
    return None


def _fee_terms(record: dict[str, Any], record_hash: str) -> FeeTerms | None:
    enabled = record.get("feesEnabled")
    if enabled is False:
        return FeeTerms("none", Decimal(0), Decimal(1), False, record_hash)
    schedule = record.get("feeSchedule")
    if isinstance(schedule, dict):
        rate = _decimal_or_none(schedule.get("rate"))
        exponent = _decimal_or_none(schedule.get("exponent"))
        if rate is not None and exponent is not None:
            return FeeTerms(
                fee_type=str(record.get("feeType") or "published_curve"),
                rate=rate,
                exponent=exponent,
                taker_only=bool(schedule.get("takerOnly")),
                source_record_hash=record_hash,
            )
    return None


def _polymarket_resolution(
    record: dict[str, Any],
) -> tuple[str | None, str | None]:
    text = str(record.get("description") or "").lower()
    source = None
    if "binance" in text:
        source = "binance"
    elif "chainlink" in text:
        source = "chainlink"
    elif "coinbase" in text:
        source = "coinbase"
    observation = None
    if "1 minute candle" in text or "one-minute candle" in text:
        observation = "one_minute_candle"
    elif "twap" in text:
        observation = "twap"
    elif source is not None:
        observation = "raw_rules_unspecified_fixing"
    return source, observation


def _limitless_pair(
    record: dict[str, Any], oracle: dict[str, Any]
) -> tuple[str | None, tuple[str, ...]]:
    candidates: list[str] = []
    for value in (oracle.get("chainlinkPair"), oracle.get("symbol")):
        pair = _normalize_pair(value)
        if pair is not None:
            candidates.append(pair)
    description = str(record.get("description") or "")
    for match in re.findall(
        r"chainlink[^A-Z0-9]{0,20}([A-Z]{2,10})/USD",
        description,
        flags=re.IGNORECASE,
    ):
        candidates.append(f"{match.upper()}/USD")
    title = str(record.get("title") or "")
    title_match = re.match(r"\s*([A-Za-z]{2,10})\s+Up or Down", title)
    if title_match:
        candidates.append(f"{title_match.group(1).upper()}/USD")
    if not candidates:
        return None, ()
    counts = Counter(candidates)
    pair = min(counts, key=lambda value: (-counts[value], value))
    unique = sorted(counts)
    conflicts = (
        (f"pair_fields_disagree:{'|'.join(unique)}",)
        if len(unique) > 1
        else ()
    )
    return pair, conflicts


def _normalize_pair(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).upper().removeprefix("CRYPTO.").replace("-", "/")
    if "/" not in text:
        return None
    base, quote = text.rsplit("/", 1)
    if not base or not quote:
        return None
    return f"{base}/{quote}"


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _note_index(value: Any) -> int | None:
    if not isinstance(value, str) or "#" not in value:
        return None
    try:
        return int(value.rsplit("#", 1)[1])
    except ValueError:
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None
