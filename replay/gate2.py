"""Gate 2: prove every replay result carries an auditable trust verdict."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from replay.gate1 import FAIL, PASS, Gate1Auditor, gate1_object
from replay.output import AnalysisEvidence, AnalysisOutput, encode_analysis_output
from replay.pipeline import ReplayPipeline
from replay.stream import ByteStreamer, DirectoryByteStreamer, StreamError
from replay.trust import TrustAudit, Verdict, audit_trust


@dataclass(frozen=True)
class Gate2Check:
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
class Gate2Report:
    gate1_report_sha256: str
    checks: tuple[Gate2Check, ...]
    output: AnalysisOutput

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.status == PASS for check in self.checks)

    def as_record(self) -> dict[str, Any]:
        body = {
            "gate": "GATE_2_MANDATORY_TRUST_VERDICT",
            "gate_version": 1,
            "passed": self.passed,
            "gate1_report_sha256": self.gate1_report_sha256,
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
                b"replay-gate2-v1\0" + canonical
            ).hexdigest(),
        }


class Gate2Auditor:
    MIN_BOOTSTRAP_ANCHORS = 100
    MIN_BOOTSTRAP_MATCH_RATE = 0.95

    def audit(self, streamer: ByteStreamer) -> Gate2Report:
        gate1 = Gate1Auditor().audit(streamer)
        gate1_record = gate1.as_record()
        pipeline = ReplayPipeline(streamer)
        anchor_checks = pipeline.polymarket_anchor_checks()
        trust = audit_trust(pipeline.events(), anchor_checks)
        output = _analysis_output(pipeline, trust)
        checks = self._checks(gate1.passed, trust, output)
        return Gate2Report(
            gate1_report_sha256=str(gate1_record["report_sha256"]),
            checks=tuple(checks),
            output=output,
        )

    def _checks(
        self, gate1_passed: bool, trust: TrustAudit, output: AnalysisOutput
    ) -> list[Gate2Check]:
        checks: list[Gate2Check] = []

        def add(
            name: str, passed: bool, requirement: str, **evidence: Any
        ) -> None:
            checks.append(
                Gate2Check(
                    name=name,
                    status=PASS if passed else FAIL,
                    requirement=requirement,
                    evidence=evidence,
                )
            )

        add(
            "gate_1_precondition",
            gate1_passed,
            "Trust analysis may run only on a tape that passed Gate 1.",
            gate1_passed=gate1_passed,
        )
        total = trust.polymarket_total
        matched = trust.polymarket_matches
        rate = matched / total if total else 0.0
        conflicting = sum(
            check.reason == "stream_hash_mapped_to_conflicting_reconstructed_states"
            for check in trust.polymarket_anchor_checks
        )
        add(
            "polymarket_serializer_bootstrap",
            total >= self.MIN_BOOTSTRAP_ANCHORS
            and rate >= self.MIN_BOOTSTRAP_MATCH_RATE
            and conflicting == 0,
            "The canonical replay must match at least 95% of 100 independent anchors, with no hash mapped to conflicting states.",
            anchors=total,
            exact_level_matches=matched,
            rate_percentage=trust.polymarket_hash_match_percentage,
            conflicting_hash_states=conflicting,
            minimum_anchors=self.MIN_BOOTSTRAP_ANCHORS,
            minimum_rate_percentage=f"{self.MIN_BOOTSTRAP_MATCH_RATE * 100:.6f}",
        )
        malformed = [
            f"{market.venue}/{market.market_id}"
            for market in trust.markets
            if not market.intervals
            or any(
                interval.start_ns >= interval.end_ns for interval in market.intervals
            )
            or any(
                left.end_ns != right.start_ns
                for left, right in zip(market.intervals, market.intervals[1:])
            )
            or sum(
                market.duration(verdict) for verdict in Verdict
            )
            != market.total_ns
        ]
        add(
            "complete_interval_partition",
            bool(trust.markets) and not malformed,
            "Every observed market is partitioned into non-overlapping TRUSTED, UNTRUSTED, or UNKNOWN intervals.",
            markets=len(trust.markets),
            malformed=malformed,
        )
        models = {market.venue: market.native_model for market in trust.markets}
        add(
            "venue_specific_trust_models",
            "polymarket" in models
            and "limitless" in models
            and models["polymarket"]
            != models["limitless"],
            "Venue trust is explicit and must not collapse unlike continuity mechanisms into one boolean.",
            models=dict(sorted(models.items())),
        )
        mismatches = [
            check for check in trust.polymarket_anchor_checks if not check.matched
        ]
        untrusted_intervals = sum(
            interval.verdict == Verdict.UNTRUSTED for interval in trust.intervals
        )
        recoveries_bounded = all(
            check.stream_order_ns is None
            or check.snapshot_receive_ns > check.stream_order_ns
            for check in mismatches
        )
        add(
            "mismatch_recovery_is_bounded",
            recoveries_bounded
            and (not mismatches or untrusted_intervals > 0),
            "Every detected Polymarket divergence becomes a labelled interval ending at independent snapshot recovery.",
            mismatches=len(mismatches),
            untrusted_intervals=untrusted_intervals,
            all_recoveries_after_detection=recoveries_bounded,
        )
        try:
            encoded = encode_analysis_output(output)
            output_valid = bool(encoded)
            error = None
        except (TypeError, ValueError) as caught:
            output_valid = False
            error = str(caught)
        add(
            "mandatory_analysis_evidence",
            output_valid,
            "Serialization rejects output lacking interval trust, coverage, Polymarket hash rate, or leg-skew strata.",
            encoded_bytes=len(encoded) if output_valid else 0,
            error=error,
        )
        return checks


def _analysis_output(pipeline: ReplayPipeline, trust: TrustAudit) -> AnalysisOutput:
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
    hash_match = {
        "status": "MEASURED" if trust.polymarket_total else "NOT_PRESENT",
        "matched": trust.polymarket_matches,
        "total": trust.polymarket_total,
        "rate_percentage": trust.polymarket_hash_match_percentage,
    }
    # Gate 2 evaluates no multi-leg hypothesis. Empty observation counts are
    # explicit in predeclared bands so later analyses cannot silently omit skew.
    leg_skew = tuple(
        {
            "lower_inclusive_ns": lower,
            "upper_exclusive_ns": upper,
            "observations": 0,
            "status": "NOT_EVALUATED_AT_GATE_2",
        }
        for lower, upper in (
            (0, 1_000_000),
            (1_000_000, 5_000_000),
            (5_000_000, 25_000_000),
            (25_000_000, 100_000_000),
            (100_000_000, None),
        )
    )
    return AnalysisOutput(
        analysis_kind="venue_trust_audit",
        payload={
            "ordering": pipeline.ordering.as_record(),
            "markets": [market.as_record() for market in trust.markets],
        },
        evidence=AnalysisEvidence(
            trust_intervals=intervals,
            coverage_percentage=coverage,
            polymarket_hash_match=hash_match,
            leg_skew_strata=leg_skew,
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset_root", type=Path)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    try:
        streamer = DirectoryByteStreamer(
            arguments.dataset_root, include=gate1_object
        )
        report = Gate2Auditor().audit(streamer)
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
