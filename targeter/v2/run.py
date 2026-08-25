"""Run one Targeter v2 discovery, archive, publication, or integrity pass.

The process is deliberately one-shot.  A host scheduler invokes ``publish``
periodically; the command never owns an internal sleep loop and therefore
cannot silently overlap or retain stale vendor state between runs.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from archive.common.durable import write_json_durable
from archive.storage.base import ObjectStoreError
from archive.storage.factory import build_store
from analysis.storage import write_json_zstd, write_ndjson, write_ndjson_zstd
from encoder import (
    DEFAULT_ZSTD_LEVEL,
    encoder_version,
    logical_identity_of,
    stored_identity_of,
)
from targeter.targets import TargetsError
from targeter.v2.adapters import durable_client, live_adapters
from targeter.v2.continuity import (
    ContinuityBundle,
    ContinuityError,
    TerminalProbe,
    TerminalState,
    load_continuity_bundles,
    target_ids_by_venue,
)
from targeter.v2.models import (
    CatalogSnapshot,
    SUPPORTED_VENUES,
    isoformat,
    parse_timestamp,
)
from targeter.v2.lease import TargeterRunLease
from targeter.v2.registry import Strategy, StrategyError, load_strategy
from targeter.v2.publication import (
    PublicationError,
    audit_current_publication,
    publication_pointer_path,
    read_publication_pointer,
    publish_run,
)
from targeter.v2.run_archive import RunArchiveError, archive_run, parse_run_id_ns
from targeter.v2.selection import SelectionResult, select_targets
from targeter.v2.target_records import artifact_stem, target_record_rows

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STRATEGY = PROJECT_ROOT / "configs" / "targeter_v2.json"
DEFAULT_CACHE = PROJECT_ROOT / "data" / "targeter-v2-cache"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "targeter-v2-shadow"
DEFAULT_LIVE = PROJECT_ROOT / "data" / "live"
ARTIFACT_FORMATS = ("zstd", "ndjson")
SELECTION_REPORT_ZSTD_FILE = "selection_report.json.zst"
SELECTION_REPORT_METADATA_FILE = "selection_report.meta.json"


@dataclass(frozen=True)
class ShadowRun:
    run_id: str
    directory: Path
    selection: SelectionResult
    discovery_failures: dict[str, str]
    # Decided once, from the catalogs themselves, and serialized into the
    # selection report. Publication trusts the serialized value, so nothing
    # downstream may re-derive it from a different view of the same run.
    input_complete: bool


def _run_id(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def _write_artifact(
    directory: Path,
    stem: str,
    rows: Iterable[dict[str, Any]],
    *,
    artifact_format: str,
) -> tuple[str, dict[str, Any]]:
    if artifact_format not in ARTIFACT_FORMATS:
        raise ValueError(
            f"artifact_format must be one of {', '.join(ARTIFACT_FORMATS)}"
        )
    name = f"{stem}.ndjson.zst" if artifact_format == "zstd" else f"{stem}.ndjson"
    path = directory / name
    if artifact_format == "zstd":
        result = write_ndjson_zstd(path, rows)
        logical = result.logical
        stored = result.stored
        content_encoding = "zstd"
    else:
        write_ndjson(path, rows)
        with path.open("rb") as handle:
            logical = logical_identity_of(handle)
        with path.open("rb") as handle:
            stored = stored_identity_of(handle)
        content_encoding = None
    return name, {
        "content_type": "application/x-ndjson",
        "content_encoding": content_encoding,
        "decoded": logical.as_record(),
        "stored": stored.as_record(),
        "compression": (
            {
                "algorithm": "zstd",
                "level": DEFAULT_ZSTD_LEVEL,
                "frame_checksum": True,
                "dictionary": None,
                "frame_count": 1,
                "encoder": encoder_version(),
            }
            if content_encoding == "zstd"
            else None
        ),
    }


def _continuity_for_run(
    *,
    live_root: Path,
    adapters: tuple[Any, ...],
    client: Any,
    strategy: Strategy,
    now: datetime,
) -> tuple[tuple[ContinuityBundle, ...], list[str], str | None]:
    if not strategy.continuity_hold_enabled:
        return (), [], None
    pointer = publication_pointer_path(live_root)
    if not pointer.exists():
        return (), [], None
    try:
        bundles = load_continuity_bundles(pointer)
    except (ContinuityError, TargetsError) as error:
        run_id = read_publication_pointer(live_root)
        run_ns = parse_run_id_ns(run_id)
        age_seconds = (
            (now.timestamp() * 1_000_000_000 - run_ns) / 1_000_000_000
            if run_ns is not None
            else -1
        )
        if age_seconds < strategy.continuity_degraded_after_seconds:
            raise
        return (), [f"continuity_degraded_after_timeout: {error}"], run_id

    by_venue = target_ids_by_venue(bundles)
    probes: dict[str, TerminalProbe] = {}
    adapters_by_venue = {str(adapter.venue): adapter for adapter in adapters}
    for venue, targets in by_venue.items():
        adapter = adapters_by_venue.get(venue)
        if adapter is None or not hasattr(adapter, "probe_terminal"):
            probes.update(
                {
                    target.target_id: TerminalProbe(
                        TerminalState.UNKNOWN,
                        "terminal_adapter_unavailable",
                    )
                    for target in targets
                }
            )
            continue
        if venue == "limitless":
            venue_probes = adapter.probe_terminal(
                client,
                {target.target_id: target.source_ref for target in targets},
            )
            probes.update(venue_probes)
        else:
            venue_probes = adapter.probe_terminal(
                client, tuple(target.venue_market_id for target in targets)
            )
            probes.update(
                {
                    target.target_id: venue_probes.get(
                        target.venue_market_id,
                        TerminalProbe(
                            TerminalState.UNKNOWN,
                            "terminal_probe_missing_record",
                        ),
                    )
                    for target in targets
                }
            )
    return tuple(bundle.with_probes(probes) for bundle in bundles), [], None


def run_shadow(
    *,
    strategy: Strategy,
    output_root: Path,
    cache_root: Path,
    live_root: Path | None = None,
    now: datetime | None = None,
    adapters: Iterable[Any] | None = None,
    client: Any | None = None,
    force_refresh: bool = True,
    persist_responses: bool = True,
    artifact_format: str = "zstd",
    max_kalshi_series: int | None = None,
    max_kalshi_pages: int | None = None,
    max_polymarket_pages: int | None = None,
    max_limitless_pages: int | None = None,
) -> ShadowRun:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    adapter_set = tuple(
        adapters
        or live_adapters(
            strategy,
            max_kalshi_series=max_kalshi_series,
            max_kalshi_pages=max_kalshi_pages,
            max_polymarket_pages=max_polymarket_pages,
            max_limitless_pages=max_limitless_pages,
        )
    )
    http = client or durable_client(
        cache_root,
        force_refresh=force_refresh,
        persist_responses=persist_responses,
    )
    continuity_bundles: tuple[ContinuityBundle, ...] = ()
    continuity_diagnostics: list[str] = []
    continuity_degraded_base_run_id: str | None = None
    if live_root is not None:
        (
            continuity_bundles,
            continuity_diagnostics,
            continuity_degraded_base_run_id,
        ) = _continuity_for_run(
            live_root=live_root,
            adapters=adapter_set,
            client=http,
            strategy=strategy,
            now=now,
        )
    if any(bundle.origin is None for bundle in continuity_bundles):
        raise ContinuityError("v3 continuity origin evidence is missing")
    catalogs: list[CatalogSnapshot] = []
    failures: dict[str, str] = {}
    for adapter in adapter_set:
        try:
            catalogs.append(adapter.discover(http, now=now))
        except Exception as error:  # noqa: BLE001 - one vendor must not hide the others
            failures[str(adapter.venue)] = f"{type(error).__name__}: {error}"

    selection = select_targets(
        catalogs,
        strategy=strategy,
        now=now,
        continuity_bundles=continuity_bundles,
    )
    run_id = _run_id(now)
    directory = Path(output_root) / run_id
    directory.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, dict[str, Any]] = {}
    for catalog in catalogs:
        name, identity = _write_artifact(
            directory,
            f"catalog_{catalog.venue}_events",
            (event.as_record() for event in catalog.events),
            artifact_format=artifact_format,
        )
        artifacts[name] = identity
        name, identity = _write_artifact(
            directory,
            f"catalog_{catalog.venue}_markets",
            (market.as_record() for market in catalog.markets),
            artifact_format=artifact_format,
        )
        artifacts[name] = identity
    # The venue's own record for every subscribed market, beside the normalized
    # view rather than in place of it. Written for every venue the strategy
    # budgets for, including one that selected nothing, so an empty artifact is
    # positive evidence that nothing was subscribed rather than a missing file.
    observed_at = isoformat(now)
    target_record_diagnostics: dict[str, list[str]] = {}
    for venue in SUPPORTED_VENUES:
        rows, diagnostics = target_record_rows(
            run_id=run_id,
            observed_at=observed_at,
            venue=venue,
            catalogs=catalogs,
            targets=selection.targets.get(venue, ()),
        )
        name, identity = _write_artifact(
            directory,
            artifact_stem(venue),
            rows,
            artifact_format=artifact_format,
        )
        artifacts[name] = identity
        if diagnostics:
            target_record_diagnostics[venue] = diagnostics

    record = selection.as_record()
    record["run_id"] = run_id
    record["strategy_source"] = strategy.source_path
    record["discovery_failures"] = dict(sorted(failures.items()))
    record["continuity_diagnostics"] = continuity_diagnostics
    record["continuity_degraded_base_run_id"] = continuity_degraded_base_run_id
    record["target_record_diagnostics"] = dict(
        sorted(target_record_diagnostics.items())
    )
    catalog_venues = [catalog.venue for catalog in catalogs]
    input_complete = (
        not failures
        and len(catalog_venues) == len(SUPPORTED_VENUES)
        and set(catalog_venues) == set(SUPPORTED_VENUES)
        and all(catalog.complete for catalog in catalogs)
    )
    record["input_complete"] = input_complete
    name, identity = _write_artifact(
        directory,
        "rule_templates",
        (
            template.as_record()
            for candidate in selection.candidates
            for template in candidate.rules.templates
        ),
        artifact_format=artifact_format,
    )
    artifacts[name] = identity
    name, identity = _write_artifact(
        directory,
        "rule_drift",
        (
            {"bundle_id": candidate.bundle.bundle_id, **drift}
            for candidate in selection.candidates
            for drift in candidate.rules.drift
        ),
        artifact_format=artifact_format,
    )
    artifacts[name] = identity
    record["artifact_format"] = artifact_format
    record["artifacts"] = dict(sorted(artifacts.items()))
    if artifact_format == "zstd":
        report_result = write_json_zstd(directory / SELECTION_REPORT_ZSTD_FILE, record)
        write_json_durable(
            directory / SELECTION_REPORT_METADATA_FILE,
            {
                "targeter_selection_report_metadata_version": 1,
                "run_id": run_id,
                "report": {
                    "file": SELECTION_REPORT_ZSTD_FILE,
                    "content_type": "application/json",
                    "content_encoding": "zstd",
                    "decoded": report_result.logical.as_record(),
                    "stored": report_result.stored.as_record(),
                    "compression": {
                        "algorithm": "zstd",
                        "level": DEFAULT_ZSTD_LEVEL,
                        "frame_checksum": True,
                        "dictionary": None,
                        "frame_count": 1,
                        "encoder": encoder_version(),
                    },
                },
            },
        )
    else:
        # Plain shadow/debug runs retain the original directly readable report.
        write_json_durable(directory / "selection_report.json", record)
    return ShadowRun(run_id, directory, selection, failures, input_complete)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("shadow", "archive", "publish", "audit"),
        default="shadow",
        help=(
            "shadow writes local evidence; archive also commits it to the object store; "
            "publish additionally replaces the live generation pointer; audit verifies the "
            "current pointer against its archived run without discovery"
        ),
    )
    parser.add_argument("--strategy", type=Path, default=DEFAULT_STRATEGY)
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--live-root", type=Path, default=DEFAULT_LIVE)
    parser.add_argument(
        "--artifact-format",
        choices=ARTIFACT_FORMATS,
        default="zstd",
        help=(
            "Normalized run artifact format. zstd is the bounded-disk default; "
            "ndjson preserves plain files for shadow inspection."
        ),
    )
    cache_mode = parser.add_mutually_exclusive_group()
    cache_mode.add_argument(
        "--reuse-cache",
        action="store_true",
        help="Offline/debug mode: reuse matching cached responses instead of querying live catalogs.",
    )
    cache_mode.add_argument(
        "--force-refresh",
        action="store_true",
        help="Compatibility flag; live refresh is already the default.",
    )
    cache_mode.add_argument(
        "--no-response-cache",
        action="store_true",
        help=(
            "Always query live APIs and retain only rate-limit state plus the normalized "
            "run artifacts; do not persist raw HTTP response bodies."
        ),
    )
    parser.add_argument(
        "--now", help="Deterministic ISO-8601 run time for probes/tests."
    )
    parser.add_argument("--max-kalshi-series", type=int)
    parser.add_argument("--max-kalshi-pages", type=int)
    parser.add_argument("--max-polymarket-pages", type=int)
    parser.add_argument("--max-limitless-pages", type=int)
    return parser.parse_args(argv)


def _optional_positive(value: int | None, name: str) -> None:
    if value is not None and value <= 0:
        raise ValueError(f"{name} must be positive")


def _configured_store(arguments: argparse.Namespace):
    return build_store((arguments.output_root,))


def main(argv: list[str] | None = None) -> int:
    arguments = parse_args(argv)
    lease: TargeterRunLease | None = None
    try:
        for value, name in (
            (arguments.max_kalshi_series, "--max-kalshi-series"),
            (arguments.max_kalshi_pages, "--max-kalshi-pages"),
            (arguments.max_polymarket_pages, "--max-polymarket-pages"),
            (arguments.max_limitless_pages, "--max-limitless-pages"),
        ):
            _optional_positive(value, name)
        strategy = load_strategy(arguments.strategy)
        now = parse_timestamp(arguments.now) if arguments.now else None
        if arguments.now and now is None:
            raise ValueError("--now must be a valid ISO-8601 timestamp")

        if arguments.mode == "audit":
            audited = audit_current_publication(
                live_root=arguments.live_root,
                output_root=arguments.output_root,
                store=_configured_store(arguments),
                strategy=strategy,
            )
            print(
                f"targeter-v2 audit {audited.run_id}: "
                + ", ".join(
                    f"{venue}={count}" for venue, count in audited.venue_counts.items()
                )
            )
            return 0

        lease = TargeterRunLease.acquire(arguments.output_root)
        result = run_shadow(
            strategy=strategy,
            output_root=arguments.output_root,
            cache_root=arguments.cache_root,
            live_root=arguments.live_root,
            now=now,
            force_refresh=not arguments.reuse_cache,
            persist_responses=not arguments.no_response_cache,
            artifact_format=arguments.artifact_format,
            max_kalshi_series=arguments.max_kalshi_series,
            max_kalshi_pages=arguments.max_kalshi_pages,
            max_polymarket_pages=arguments.max_polymarket_pages,
            max_limitless_pages=arguments.max_limitless_pages,
        )
        complete = result.input_complete
        if arguments.mode == "shadow":
            print(
                f"targeter-v2 shadow run {result.run_id}: "
                f"{len(result.selection.selected)} bundles -> {result.directory}"
            )
            return 0 if complete else 1

        store = _configured_store(arguments)
        receipt = archive_run(result.directory, store, now=now)
        if not complete:
            print(
                f"targeter-v2 {result.run_id}: archived incomplete discovery evidence; "
                "live publication was not changed"
            )
            return 1
        if arguments.mode == "archive":
            print(
                f"targeter-v2 archive {result.run_id}: {len(receipt.objects)} objects -> "
                f"{receipt.location}/{receipt.prefix}"
            )
            return 0

        generation = publish_run(
            result.directory,
            receipt,
            store,
            live_root=arguments.live_root,
            strategy=strategy,
            now=now,
        )
        print(
            f"targeter-v2 publish {result.run_id}: "
            + ", ".join(
                f"{venue}={count}" for venue, count in generation.venue_counts.items()
            )
            + f" -> {generation.pointer_path}"
        )
        return 0
    except (
        ObjectStoreError,
        OSError,
        PublicationError,
        RunArchiveError,
        StrategyError,
        ValueError,
    ) as error:
        print(str(error))
        return 2
    finally:
        if lease is not None:
            lease.close()


if __name__ == "__main__":
    raise SystemExit(main())
