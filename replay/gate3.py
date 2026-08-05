"""Gate 3: prove the economic result is size-, fee-, LP-, and null-honest."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from replay.catalog import MetadataCatalogue
from replay.economics import (
    HEADLINE_SIZE,
    EconomicAudit,
    audit_binary_economics,
    solve_cover_lp,
    walk_ladder,
)
from replay.gate1 import FAIL, PASS, gate1_object
from replay.gate2 import Gate2Auditor
from replay.output import AnalysisEvidence, AnalysisOutput, encode_analysis_output
from replay.pipeline import ReplayPipeline
from replay.stream import ByteStreamer, DirectoryByteStreamer, StreamError
from replay.trust import TrustAudit, Verdict, audit_trust


@dataclass(frozen=True)
class Gate3Check:
    name: str
    status: str
    requirement: str
    evidence: dict[str, Any]

    def as_record(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "requirement": self.requirement,
            "evidence": self.evidence,
        }


@dataclass(frozen=True)
class Gate3Report:
    gate2_report_sha256: str
    checks: tuple[Gate3Check, ...]
    output: AnalysisOutput

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.status == PASS for check in self.checks)

    def as_record(self) -> dict[str, Any]:
        body = {
            "gate": "GATE_3_ECONOMIC_HONESTY",
            "gate_version": 1,
            "passed": self.passed,
            "gate2_report_sha256": self.gate2_report_sha256,
            "checks": [check.as_record() for check in self.checks],
            "analysis_output": self.output.as_record(),
        }
        canonical = json.dumps(
            body,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        return {
            **body,
            "report_sha256": hashlib.sha256(
                b"replay-gate3-v1\0" + canonical
            ).hexdigest(),
        }


class Gate3Auditor:
    def audit(self, streamer: ByteStreamer) -> Gate3Report:
        gate2 = Gate2Auditor().audit(streamer)
        pipeline = ReplayPipeline(streamer)
        trust = audit_trust(
            pipeline.events(), pipeline.polymarket_anchor_checks()
        )
        catalogue = MetadataCatalogue.from_streamer(streamer)
        economics = audit_binary_economics(
            pipeline.events(), catalogue, trust
        )
        output = _analysis_output(trust, economics)
        checks = self._checks(gate2.passed, economics, output)
        return Gate3Report(
            gate2_report_sha256=str(gate2.as_record()["report_sha256"]),
            checks=tuple(checks),
            output=output,
        )

    def _checks(
        self,
        gate2_passed: bool,
        economics: EconomicAudit,
        output: AnalysisOutput,
    ) -> list[Gate3Check]:
        checks: list[Gate3Check] = []

        def add(
            name: str, passed: bool, requirement: str, **evidence: Any
        ) -> None:
            checks.append(
                Gate3Check(
                    name,
                    PASS if passed else FAIL,
                    requirement,
                    evidence,
                )
            )

        add(
            "gate_2_precondition",
            gate2_passed,
            "Economic analysis may run only after mandatory interval trust passes.",
            gate2_passed=gate2_passed,
        )
        exact = walk_ladder(
            (
                (Decimal("0.40"), Decimal(5)),
                (Decimal("0.50"), Decimal(5)),
            ),
            Decimal(10),
        )
        partial = walk_ladder(
            ((Decimal("0.40"), Decimal(5)),), Decimal(10)
        )
        add(
            "share_matched_vwap_depth",
            exact.vwap == Decimal("0.45")
            and not exact.depth_limited
            and partial.depth_limited
            and partial.filled == Decimal(5),
            "Every leg buys the same contract count, walks all displayed levels, and rejects partial depth.",
            exact_vwap=str(exact.vwap),
            partial_filled=str(partial.filled),
            partial_depth_limited=partial.depth_limited,
        )
        headline = [
            row
            for row in economics.rows
            if row["direction"] == "long"
            and row["size_contracts"] == HEADLINE_SIZE
            and row["headline_eligible"]
        ]
        complete = [
            row
            for row in headline
            if row["net_gap_conservative_per_contract"] is not None
            and all(row["fee_source_hashes"])
            and not row["depth_limited"]
        ]
        add(
            "deployable_ticket_net_of_conservative_fees",
            bool(headline) and len(headline) == len(complete),
            "The headline is a valid fixed-size VWAP ticket with contemporaneous fee hashes and conservative rounding.",
            headline_size_contracts=HEADLINE_SIZE,
            headline_rows=len(headline),
            fee_and_depth_complete=len(complete),
            positive_rows=economics.summary["headline_positive_rows"],
            median_net_gap=economics.summary["headline_median_net_gap"],
            max_net_gap=economics.summary["headline_max_net_gap"],
        )
        synthetic = solve_cover_lp(
            ("A", "B"),
            (frozenset({"A"}), frozenset({"B"})),
            (Decimal("0.49"), Decimal("0.49")),
        )
        lp_rows = [
            row
            for row in economics.rows
            if row["direction"] == "long"
            and row["exclusion_reason"] is None
        ]
        add(
            "symbolic_subset_cover_lp",
            synthetic.minimum_cost == Decimal("0.98")
            and bool(lp_rows)
            and all(row["subset_cover_lp"]["feasible"] for row in lp_rows),
            "A symbolic payout-incidence LP accompanies every valid long basket and recovers a known two-cent fixture exactly.",
            synthetic_minimum_cost=(
                str(synthetic.minimum_cost)
                if synthetic.minimum_cost is not None
                else None
            ),
            valid_long_rows=len(lp_rows),
            feasible_lp_rows=sum(
                row["subset_cover_lp"]["feasible"] for row in lp_rows
            ),
        )
        placebo_rows = [
            row
            for row in economics.rows
            if row.get("placebo_status") == "MATCHED"
        ]
        add(
            "matched_leg_placebo_null",
            bool(placebo_rows)
            and all(
                row["placebo_market_id"] != row["market_id"]
                and row["placebo_net_gap_conservative_per_contract"] is not None
                for row in placebo_rows
            ),
            "The null changes condition identity while matching venue, leg count, ticket size, direction, time proximity, and skew stratum.",
            matched_rows=len(placebo_rows),
            positive_rows=economics.placebo_summary["positive_rows"],
            median_net_gap=economics.placebo_summary["median_net_gap"],
        )
        add(
            "condition_identity_guard",
            economics.summary["cross_venue_baskets"] == 0,
            "No cross-venue basket is formed without exact resolution source, observation method, and fixing time identity.",
            cross_venue_baskets=economics.summary["cross_venue_baskets"],
            reason=economics.summary["cross_venue_reason"],
        )
        try:
            encoded = encode_analysis_output(output)
            valid_output = bool(encoded)
            error = None
        except (TypeError, ValueError) as caught:
            valid_output = False
            error = str(caught)
        add(
            "mandatory_economic_evidence",
            valid_output,
            "The economic result remains wrapped with interval trust, coverage, hash rate, and populated skew strata.",
            encoded_bytes=len(encoded) if valid_output else 0,
            error=error,
        )
        return checks


def _analysis_output(
    trust: TrustAudit, economics: EconomicAudit
) -> AnalysisOutput:
    intervals = tuple(interval.as_record() for interval in trust.intervals)
    coverage = tuple(
        {
            "venue": market.venue,
            "market_id": market.market_id,
            "trusted_percentage": market.trusted_percentage,
            "trusted_ns": market.duration(Verdict.TRUSTED),
            "untrusted_ns": market.duration(Verdict.UNTRUSTED),
            "unknown_ns": market.duration(Verdict.UNKNOWN),
            "total_ns": market.total_ns,
        }
        for market in trust.markets
    )
    strata: list[dict[str, Any]] = []
    for name, lower, upper in (
        ("lt_5s", 0, 5_000_000_000),
        ("5_to_15s", 5_000_000_000, 15_000_000_000),
        ("15_to_60s", 15_000_000_000, 60_000_000_001),
        ("gt_60s", 60_000_000_001, None),
    ):
        rows = [
            row
            for row in economics.rows
            if row["direction"] == "long"
            and row["leg_skew_stratum"] == name
        ]
        eligible = [row for row in rows if row["headline_eligible"]]
        positive = [
            row
            for row in eligible
            if row["net_gap_conservative_per_contract"] is not None
            and Decimal(row["net_gap_conservative_per_contract"]) > 0
        ]
        strata.append(
            {
                "name": name,
                "lower_inclusive_ns": lower,
                "upper_exclusive_ns": upper,
                "observations": len(rows),
                "headline_eligible": len(eligible),
                "positive": len(positive),
            }
        )
    return AnalysisOutput(
        analysis_kind="binary_partition_economics",
        payload={
            "summary": economics.summary,
            "placebo_null": economics.placebo_summary,
            "exclusions": economics.exclusions,
            "rows": list(economics.rows),
        },
        evidence=AnalysisEvidence(
            trust_intervals=intervals,
            coverage_percentage=coverage,
            polymarket_hash_match={
                "status": "MEASURED" if trust.polymarket_total else "NOT_PRESENT",
                "matched": trust.polymarket_matches,
                "total": trust.polymarket_total,
                "rate_percentage": trust.polymarket_hash_match_percentage,
            },
            leg_skew_strata=tuple(strata),
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset_root", type=Path)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    try:
        report = Gate3Auditor().audit(
            DirectoryByteStreamer(
                arguments.dataset_root, include=gate1_object
            )
        )
    except StreamError as error:
        parser.error(str(error))
    encoded = (
        json.dumps(
            report.as_record(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    )
    if arguments.output is not None:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
