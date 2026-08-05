"""Gate 4: measure displayed-depth lifetime without making a fill claim."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from replay.catalog import MetadataCatalogue
from replay.economics import audit_binary_economics
from replay.execution import (
    ESTIMATOR_NAME,
    MINIMUM_SURVIVAL_NS,
    DepthEpisode,
    ExecutionAudit,
    audit_execution,
    estimate_candidates,
)
from replay.gate1 import FAIL, PASS, gate1_object
from replay.gate3 import Gate3Auditor
from replay.output import AnalysisEvidence, AnalysisOutput, encode_analysis_output
from replay.pipeline import ReplayPipeline
from replay.stream import ByteStreamer, DirectoryByteStreamer, StreamError
from replay.trust import TrustAudit, Verdict, audit_trust


@dataclass(frozen=True)
class Gate4Check:
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
class Gate4Report:
    gate3_report_sha256: str
    checks: tuple[Gate4Check, ...]
    output: AnalysisOutput

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.status == PASS for check in self.checks)

    def as_record(self) -> dict[str, Any]:
        body = {
            "gate": "GATE_4_EXECUTION_REALITY",
            "gate_version": 1,
            "passed": self.passed,
            "gate3_report_sha256": self.gate3_report_sha256,
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
                b"replay-gate4-v1\0" + canonical
            ).hexdigest(),
        }


class Gate4Auditor:
    def audit(self, streamer: ByteStreamer) -> Gate4Report:
        gate3 = Gate3Auditor().audit(streamer)
        pipeline = ReplayPipeline(streamer)
        trust = audit_trust(
            pipeline.events(), pipeline.polymarket_anchor_checks()
        )
        catalogue = MetadataCatalogue.from_streamer(streamer)
        economics = audit_binary_economics(
            pipeline.events(), catalogue, trust
        )
        execution = audit_execution(
            pipeline.book_states(), catalogue, trust, economics
        )
        output = _analysis_output(trust, economics, execution)
        checks = self._checks(gate3.passed, economics.summary, execution, output)
        return Gate4Report(
            gate3_report_sha256=str(gate3.as_record()["report_sha256"]),
            checks=tuple(checks),
            output=output,
        )

    def _checks(
        self,
        gate3_passed: bool,
        economic_summary: dict[str, Any],
        execution: ExecutionAudit,
        output: AnalysisOutput,
    ) -> list[Gate4Check]:
        checks: list[Gate4Check] = []

        def add(
            name: str, passed: bool, requirement: str, **evidence: Any
        ) -> None:
            checks.append(
                Gate4Check(
                    name,
                    PASS if passed else FAIL,
                    requirement,
                    evidence,
                )
            )

        add(
            "gate_3_precondition",
            gate3_passed,
            "Execution diagnostics may run only after economic honesty passes.",
            gate3_passed=gate3_passed,
        )
        venues = {episode.venue for episode in execution.episodes}
        add(
            "venue_book_state_replay",
            {"polymarket", "limitless"} <= venues,
            "Size-specific displayed depth is reconstructed independently for every captured venue.",
            venues=sorted(venues),
            episodes=len(execution.episodes),
        )
        trusted_pm = [
            row
            for row in execution.lifetime_summary
            if row["venue"] == "polymarket"
            and row["direction"] == "long"
            and row["trust_verdict"] == "TRUSTED"
        ]
        add(
            "quote_lifetime_distribution",
            len(trusted_pm) == 1
            and trusted_pm[0]["uncensored_episodes"] > 0
            and all(
                trusted_pm[0][field] is not None
                for field in (
                    "p50_lifetime_ns",
                    "p90_lifetime_ns",
                    "p99_lifetime_ns",
                )
            ),
            "Execution reality reports uncensored p50/p90/p99 lifetime for the exact displayed ladder slice required by the ticket.",
            trusted_polymarket_long=trusted_pm[0] if trusted_pm else None,
        )
        synthetic_episodes = [
            DepthEpisode(
                venue="polymarket",
                market_id="synthetic",
                asset_id=asset,
                direction="long",
                size_contracts=100,
                start_ns=0,
                end_ns=MINIMUM_SURVIVAL_NS + 1,
                vwap=Decimal("0.49"),
                depth_limited=False,
                fingerprint=(("0.49", "100"),),
                trust_verdict=Verdict.TRUSTED,
                right_censored=False,
            )
            for asset in ("a", "b")
        ]
        synthetic_candidate = {
            "basket_id": "synthetic",
            "market_id": "synthetic",
            "observation_ns": 0,
            "size_contracts": 100,
            "net_gap_conservative_per_contract": "0.02",
            "leg_asset_ids": ["a", "b"],
        }
        synthetic = estimate_candidates(
            [synthetic_candidate], synthetic_episodes
        )[0]
        add(
            "explicit_non_fill_estimator",
            synthetic["estimator"] == ESTIMATOR_NAME
            and synthetic["not_a_fill_claim"] is True
            and all(
                estimate.get("not_a_fill_claim") is True
                and estimate.get("estimator") == ESTIMATOR_NAME
                for estimate in execution.candidate_estimates
            ),
            "Candidates are evaluated only by named displayed-depth survival; no candidate is labelled captured or filled.",
            estimator=ESTIMATOR_NAME,
            minimum_survival_ns=MINIMUM_SURVIVAL_NS,
            synthetic_status=synthetic["status"],
            detected_candidates=execution.detected_candidates,
            estimates=len(execution.candidate_estimates),
        )
        add(
            "candidate_accounting",
            execution.detected_candidates
            == economic_summary["headline_positive_rows"]
            and len(execution.candidate_estimates)
            == execution.detected_candidates,
            "Every positive, trusted, fee-complete headline quote receives exactly one survival estimate, including the valid zero-candidate case.",
            economic_positive_rows=economic_summary["headline_positive_rows"],
            detected_candidates=execution.detected_candidates,
            estimates=len(execution.candidate_estimates),
        )
        try:
            encoded = encode_analysis_output(output)
            valid_output = bool(encoded)
            error = None
        except (TypeError, ValueError) as caught:
            valid_output = False
            error = str(caught)
        add(
            "mandatory_execution_evidence",
            valid_output,
            "Execution output retains trust, coverage, hash rate, and leg-skew evidence.",
            encoded_bytes=len(encoded) if valid_output else 0,
            error=error,
        )
        return checks


def _analysis_output(
    trust: TrustAudit,
    economics: Any,
    execution: ExecutionAudit,
) -> AnalysisOutput:
    strata: list[dict[str, Any]] = []
    for name in ("lt_5s", "5_to_15s", "15_to_60s", "gt_60s"):
        rows = [
            row
            for row in economics.rows
            if row["direction"] == "long"
            and row["leg_skew_stratum"] == name
        ]
        strata.append(
            {
                "name": name,
                "observations": len(rows),
                "headline_eligible": sum(
                    row["headline_eligible"] for row in rows
                ),
            }
        )
    return AnalysisOutput(
        analysis_kind="execution_reality",
        payload={
            "economic_scope_summary": economics.summary,
            "quote_lifetime_summary": list(execution.lifetime_summary),
            "execution_estimator": {
                "name": ESTIMATOR_NAME,
                "minimum_survival_ns": MINIMUM_SURVIVAL_NS,
                "semantics": (
                    "Measures how long the exact displayed ladder slice remains "
                    "unchanged after detection. It does not observe queue position, "
                    "orders, acknowledgements, trades, captures, or fills."
                ),
                "detected_candidates": execution.detected_candidates,
                "candidate_estimates": list(execution.candidate_estimates),
            },
        },
        evidence=AnalysisEvidence(
            trust_intervals=tuple(
                interval.as_record() for interval in trust.intervals
            ),
            coverage_percentage=tuple(
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
            ),
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
        report = Gate4Auditor().audit(
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
