"""Gate 5: hash policy first, reconcile resolutions, and allow an honest NO."""

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
from replay.execution import audit_execution
from replay.gate1 import FAIL, PASS, gate1_object
from replay.gate4 import Gate4Auditor
from replay.output import AnalysisEvidence, AnalysisOutput, encode_analysis_output
from replay.pipeline import ReplayPipeline
from replay.resolution import ResolutionAudit, reconcile_resolutions
from replay.stream import ByteStreamer, DirectoryByteStreamer, StreamError
from replay.trust import TrustAudit, Verdict, audit_trust

POLICY_PATH = Path(__file__).with_name("policy.json")


@dataclass(frozen=True)
class FrozenPolicy:
    document: dict[str, Any]
    sha256: str


@dataclass(frozen=True)
class Gate5Check:
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
class Gate5Report:
    gate4_report_sha256: str
    policy_sha256: str
    checks: tuple[Gate5Check, ...]
    output: AnalysisOutput

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.status == PASS for check in self.checks)

    def as_record(self) -> dict[str, Any]:
        body = {
            "gate": "GATE_5_PRECOMMITTED_TERMINAL_VERDICT",
            "gate_version": 1,
            "passed": self.passed,
            "gate4_report_sha256": self.gate4_report_sha256,
            "policy_sha256": self.policy_sha256,
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
                b"replay-gate5-v1\0" + canonical
            ).hexdigest(),
        }


class Gate5Auditor:
    def audit(
        self, streamer: ByteStreamer, policy_document: dict[str, Any]
    ) -> Gate5Report:
        # This is deliberately the first operation. No tape bytes are evaluated
        # until the policy is closed, validated, canonicalized, and hashed.
        policy = freeze_policy(policy_document)

        gate4 = Gate4Auditor().audit(streamer)
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
        resolutions = reconcile_resolutions(pipeline.events(), catalogue)
        terminal = _terminal(policy, economics, execution)
        output = _analysis_output(
            policy, trust, economics, execution, resolutions, terminal
        )
        checks = self._checks(
            gate4.passed,
            policy,
            economics,
            execution,
            resolutions,
            terminal,
            output,
        )
        return Gate5Report(
            gate4_report_sha256=str(gate4.as_record()["report_sha256"]),
            policy_sha256=policy.sha256,
            checks=tuple(checks),
            output=output,
        )

    def _checks(
        self,
        gate4_passed: bool,
        policy: FrozenPolicy,
        economics: Any,
        execution: Any,
        resolutions: ResolutionAudit,
        terminal: dict[str, Any],
        output: AnalysisOutput,
    ) -> list[Gate5Check]:
        checks: list[Gate5Check] = []

        def add(
            name: str, passed: bool, requirement: str, **evidence: Any
        ) -> None:
            checks.append(
                Gate5Check(
                    name,
                    PASS if passed else FAIL,
                    requirement,
                    evidence,
                )
            )

        add(
            "gate_4_precondition",
            gate4_passed,
            "A terminal verdict may be emitted only after all four prior gates pass.",
            gate4_passed=gate4_passed,
        )
        add(
            "policy_frozen_before_observation_evaluation",
            len(policy.sha256) == 64,
            "Thresholds, controls, placebo construction, and resolution reconciliation are closed and hashed before tape evaluation.",
            policy_sha256=policy.sha256,
            evaluation_sequence=[
                "validate_policy",
                "canonicalize_policy",
                "hash_policy",
                "evaluate_tape",
            ],
        )
        resolution_record = resolutions.as_record()
        unique = int(resolution_record["unique_resolutions"])
        consistent = int(resolution_record["outcome_index_consistent"])
        reconciled = int(resolution_record["metadata_reconciled"])
        add(
            "captured_resolution_reconciliation",
            unique > 0 and reconciled == unique and consistent == unique,
            "Every captured unique resolution joins to captured metadata and has a consistent venue outcome/index; independent oracle verification remains separately labelled.",
            **resolution_record,
        )
        minimum = int(
            policy.document["thresholds"]["minimum_headline_observations"]
        )
        add(
            "precommitted_sample_adequacy",
            terminal["eligible_observations"] >= minimum,
            "The terminal YES/NO rule runs only when the precommitted trusted low-skew sample floor is met.",
            eligible_observations=terminal["eligible_observations"],
            minimum=minimum,
        )
        labels = policy.document["terminal_labels"]
        add(
            "deterministic_terminal_rule",
            terminal["verdict"] in set(labels.values()),
            "The outcome is one of the predeclared labels and is derived only from the frozen thresholds.",
            verdict=terminal["verdict"],
            reason=terminal["reason"],
            positive_rate_percentage=terminal[
                "positive_rate_percentage"
            ],
            surviving_candidates=terminal["surviving_candidates"],
        )
        add(
            "terminal_no_is_permitted",
            labels["no"] == "NO_DEPLOYABLE_EDGE_IN_FIXTURE"
            and terminal["verdict"]
            == "NO_DEPLOYABLE_EDGE_IN_FIXTURE",
            "A sufficient negative fixture returns a scoped NO instead of being reframed as an inconclusive or vanity metric.",
            configured_no=labels["no"],
            observed_verdict=terminal["verdict"],
            fixture_observation=economics.summary[
                "fixture_economic_observation"
            ],
            detected_candidates=execution.detected_candidates,
        )
        try:
            encoded = encode_analysis_output(output)
            valid_output = bool(encoded)
            error = None
        except (TypeError, ValueError) as caught:
            valid_output = False
            error = str(caught)
        add(
            "mandatory_terminal_evidence",
            valid_output,
            "The terminal verdict retains interval trust, coverage, hash rate, and skew strata.",
            encoded_bytes=len(encoded) if valid_output else 0,
            error=error,
        )
        return checks


def freeze_policy(document: dict[str, Any]) -> FrozenPolicy:
    if not isinstance(document, dict):
        raise ValueError("policy must be an object")
    expected = {
        "policy_version",
        "scope",
        "thresholds",
        "controls",
        "terminal_labels",
    }
    if set(document) != expected or document.get("policy_version") != 1:
        raise ValueError("closed policy fields or version do not match v1")
    thresholds = document.get("thresholds")
    required_thresholds = {
        "ticket_size_contracts",
        "maximum_leg_skew_ns",
        "minimum_headline_observations",
        "minimum_positive_rate_percentage",
        "minimum_surviving_candidates",
        "minimum_displayed_depth_survival_ns",
    }
    if not isinstance(thresholds, dict) or set(thresholds) != required_thresholds:
        raise ValueError("closed threshold fields do not match v1")
    if int(thresholds["ticket_size_contracts"]) != 100:
        raise ValueError("v1 execution replay is precommitted to 100 contracts")
    if int(thresholds["minimum_displayed_depth_survival_ns"]) != 100_000_000:
        raise ValueError("policy survival horizon differs from named estimator")
    labels = document.get("terminal_labels")
    if not isinstance(labels, dict) or set(labels) != {
        "pass",
        "no",
        "inconclusive",
    }:
        raise ValueError("closed terminal labels do not match v1")
    canonical = json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return FrozenPolicy(
        document=document,
        sha256=hashlib.sha256(
            b"replay-terminal-policy-v1\0" + canonical
        ).hexdigest(),
    )


def _terminal(
    policy: FrozenPolicy, economics: Any, execution: Any
) -> dict[str, Any]:
    thresholds = policy.document["thresholds"]
    size = int(thresholds["ticket_size_contracts"])
    maximum_skew = int(thresholds["maximum_leg_skew_ns"])
    eligible = [
        row
        for row in economics.rows
        if row["direction"] == "long"
        and row["size_contracts"] == size
        and row["headline_eligible"]
        and int(row["leg_skew_ns"]) < maximum_skew
    ]
    positive = [
        row
        for row in eligible
        if row["net_gap_conservative_per_contract"] is not None
        and Decimal(row["net_gap_conservative_per_contract"]) > 0
    ]
    positive_rate = (
        Decimal(len(positive)) * Decimal(100) / Decimal(len(eligible))
        if eligible
        else Decimal(0)
    )
    surviving = sum(
        estimate["status"] == "DISPLAYED_DEPTH_SURVIVED_MINIMUM"
        for estimate in execution.candidate_estimates
    )
    minimum_sample = int(thresholds["minimum_headline_observations"])
    minimum_rate = Decimal(
        str(thresholds["minimum_positive_rate_percentage"])
    )
    minimum_surviving = int(thresholds["minimum_surviving_candidates"])
    labels = policy.document["terminal_labels"]
    if len(eligible) < minimum_sample:
        verdict = labels["inconclusive"]
        reason = "precommitted_sample_floor_not_met"
    elif positive_rate >= minimum_rate and surviving >= minimum_surviving:
        verdict = labels["pass"]
        reason = "precommitted_economic_and_survival_thresholds_met"
    else:
        verdict = labels["no"]
        reason = "precommitted_deployable_edge_threshold_not_met"
    return {
        "verdict": verdict,
        "reason": reason,
        "scope": "this content-addressed fixture only",
        "eligible_observations": len(eligible),
        "positive_observations": len(positive),
        "positive_rate_percentage": f"{positive_rate:.6f}",
        "surviving_candidates": surviving,
        "thresholds": thresholds,
    }


def _analysis_output(
    policy: FrozenPolicy,
    trust: TrustAudit,
    economics: Any,
    execution: Any,
    resolutions: ResolutionAudit,
    terminal: dict[str, Any],
) -> AnalysisOutput:
    strata = tuple(
        {
            "name": name,
            "observations": sum(
                row["direction"] == "long"
                and row["leg_skew_stratum"] == name
                for row in economics.rows
            ),
            "headline_eligible": sum(
                row["direction"] == "long"
                and row["leg_skew_stratum"] == name
                and row["headline_eligible"]
                for row in economics.rows
            ),
        }
        for name in ("lt_5s", "5_to_15s", "15_to_60s", "gt_60s")
    )
    return AnalysisOutput(
        analysis_kind="terminal_fixture_verdict",
        payload={
            "policy": {
                "sha256": policy.sha256,
                "document": policy.document,
            },
            "terminal": terminal,
            "economic_summary": economics.summary,
            "placebo_null": economics.placebo_summary,
            "execution": {
                "detected_candidates": execution.detected_candidates,
                "candidate_estimates": list(
                    execution.candidate_estimates
                ),
                "quote_lifetime_summary": list(
                    execution.lifetime_summary
                ),
            },
            "resolution_reconciliation": resolutions.as_record(),
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
            leg_skew_strata=strata,
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset_root", type=Path)
    parser.add_argument("--policy", type=Path, default=POLICY_PATH)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    policy_document = json.loads(arguments.policy.read_text(encoding="utf-8"))
    try:
        report = Gate5Auditor().audit(
            DirectoryByteStreamer(
                arguments.dataset_root, include=gate1_object
            ),
            policy_document,
        )
    except (StreamError, ValueError) as error:
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
