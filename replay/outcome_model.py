"""Closed outcome document validation and deterministic preparation book mapping.

This boundary validates identities and alignment; only Universe derives masks.
"""

import re

from analysis.claims import (
    CLAIM_ALGEBRA_VERSION,
    CLAIM_IDENTITY_VERSION,
    claim_id,
    space_shape_id,
)
from analysis.outcome_space import Outcome, OutcomeSpace
from replay.streams.protocol import array, choice, obj, require, text

MASK_STATUSES = "MASKED REJECTED UNSUPPORTED NOT_A_FUNCTION DIFFERENT_SCOPE TAUTOLOGY NO_SPACE CLAIM_CONFLICT VOID_UNSUPPORTED"
DIAGNOSTICS = {
    "series_scope_missing_unambiguous_best_of_format",
    "unsupported_series_format",
}
UNAVAILABLE = {"provider": None, "unavailable": "universe_outcomes_unavailable"}


def _sha(value):
    require(type(value) is str and re.fullmatch("[0-9a-f]{64}", value) is not None)
    return value


def _ordered(rows, field=None):
    array(rows)
    values = [r[field] if field else text(r) for r in rows]
    require(values == sorted(set(values)), "outcome list must be sorted and unique")


def validate_document(doc, bundle_id):
    obj(
        doc,
        "version bundle_id event_id identities status diagnostics participants spaces claims markets",
    )
    require(type(doc["version"]) is int and doc["version"] == 1)
    require(doc["bundle_id"] == bundle_id, "outcome bundle identity")
    text(doc["event_id"])
    obj(doc["identities"], "claim_identity_version claim_algebra_version")
    require(all(type(v) is int for v in doc["identities"].values()))
    require(
        doc["identities"]
        == {
            "claim_identity_version": CLAIM_IDENTITY_VERSION,
            "claim_algebra_version": CLAIM_ALGEBRA_VERSION,
        },
        "outcome identity versions",
    )
    choice(doc["status"], "complete unreconstructed")
    _ordered(doc["diagnostics"])
    require(set(doc["diagnostics"]) <= DIAGNOSTICS)
    array(doc["participants"])
    for participant in doc["participants"]:
        text(participant)
    require(len(doc["participants"]) == len(set(doc["participants"])))
    spaces = {}
    array(doc["spaces"])
    require(len(doc["spaces"]) <= 8, "outcome space limit")
    for row in doc["spaces"]:
        obj(row, "space_shape_id scope coverage best_of outcome_keys")
        _sha(row["space_shape_id"])
        text(row["scope"])
        choice(row["coverage"], "EXHAUSTIVE INCOMPLETE_COVERAGE")
        require(
            (type(row["best_of"]) is int and row["best_of"] > 0)
            if row["scope"] == "series"
            else row["best_of"] is None
        )
        _ordered(row["outcome_keys"])
        require(0 < len(row["outcome_keys"]) <= 1024)
        space = OutcomeSpace(
            "",
            row["scope"],
            tuple(Outcome(k, {}) for k in row["outcome_keys"]),
            row["coverage"],
            "",
            {},
        )
        require(
            space_shape_id(space) == row["space_shape_id"], "outcome space identity"
        )
        spaces[row["space_shape_id"]] = frozenset(row["outcome_keys"])
    _ordered(doc["spaces"], "space_shape_id")
    claims = {}
    array(doc["claims"])
    require(len(doc["claims"]) <= 4096, "outcome claim limit")
    for row in doc["claims"]:
        obj(row, "claim_id space_shape_id outcome_keys")
        _sha(row["claim_id"])
        _sha(row["space_shape_id"])
        require(row["space_shape_id"] in spaces, "claim space absent")
        _ordered(row["outcome_keys"])
        require(
            bool(row["outcome_keys"])
            and set(row["outcome_keys"]) < spaces[row["space_shape_id"]],
            "outcome claim subset",
        )
        require(
            claim_id(row["outcome_keys"], row["space_shape_id"]) == row["claim_id"],
            "outcome claim identity",
        )
        claims[row["claim_id"]] = row
    _ordered(doc["claims"], "claim_id")
    array(doc["markets"])
    # Universe's owning bound; preparation's global snapshot bound still applies.
    require(len(doc["markets"]) <= 1000, "outcome market limit")
    referenced = set()
    for market in doc["markets"]:
        obj(
            market,
            "market_id venue market_type market_status subscription_ids outcome_labels mask_status reason claims tokens",
        )
        choice(market["venue"], "kalshi polymarket limitless")
        text(market["market_id"])
        text(market["market_type"])
        text(market["market_status"])
        require(
            market["market_id"].startswith(market["venue"] + ":")
            and bool(market["market_id"].split(":", 1)[1])
        )
        array(market["subscription_ids"])
        array(market["outcome_labels"])
        for subscription in market["subscription_ids"]:
            text(subscription)
        require(len(market["subscription_ids"]) == len(set(market["subscription_ids"])))
        require(all(type(label) is str for label in market["outcome_labels"]))
        status = choice(market["mask_status"], MASK_STATUSES)
        reason = market["reason"]
        if status == "REJECTED":
            choice(reason, "invalid_product_parameters product_outside_series_format")
        elif status == "CLAIM_CONFLICT":
            require(reason == "recorded_claim_mismatch")
        elif status == "UNSUPPORTED":
            require(reason is None or reason == "token_alignment")
        else:
            require(reason is None)
        refs = {}
        array(market["claims"])
        array(market["tokens"])
        for ref in market["claims"]:
            obj(ref, "claim_key claim_id")
            require(
                type(ref["claim_key"]) is str
                and re.fullmatch(r"claim=(0|[1-9][0-9]*)", ref["claim_key"]) is not None
            )
            require(ref["claim_id"] in claims, "market claim absent")
            refs[ref["claim_key"]] = ref["claim_id"]
            referenced.add(ref["claim_id"])
        _ordered(market["claims"], "claim_key")
        for token in market["tokens"]:
            obj(token, "subscription_id claim_key negated")
            require(
                token["subscription_id"] in market["subscription_ids"]
                and token["claim_key"] in refs,
                "token reference absent",
            )
            require(type(token["negated"]) is bool)
        if status != "MASKED":
            require(not refs and not market["tokens"], "unmasked market exposes claims")
            continue
        require(
            refs and len({claims[c]["space_shape_id"] for c in refs.values()}) == 1,
            "market spans spaces",
        )
        subs, labels = market["subscription_ids"], market["outcome_labels"]
        expected = []
        if market["venue"] in {"kalshi", "limitless"} and len(subs) == len(refs) == 1:
            expected = [
                {"subscription_id": subs[0], "claim_key": "claim=0", "negated": False}
            ]
        elif market["venue"] == "polymarket":
            meaningful = [
                s
                for s in labels
                if s.strip().casefold() not in {"", "yes", "no", "true", "false"}
            ]
            if len(meaningful) == len(labels) == len(subs) == len(refs) >= 2:
                expected = [
                    {"subscription_id": s, "claim_key": f"claim={i}", "negated": False}
                    for i, s in enumerate(subs)
                ]
            elif (
                len(subs) == len(labels) == 2
                and {s.casefold() for s in labels} == {"yes", "no"}
                and len(refs) == 1
            ):
                expected = [
                    {
                        "subscription_id": s,
                        "claim_key": "claim=0",
                        "negated": label.casefold() == "no",
                    }
                    for s, label in zip(subs, labels)
                ]
        require(expected and market["tokens"] == expected, "token alignment")
        require(
            set(refs) == {t["claim_key"] for t in expected}, "token claim alignment"
        )
    _ordered(doc["markets"], "market_id")
    require(referenced == set(claims), "unreferenced outcome claim")
    if doc["status"] == "unreconstructed":
        require(
            not spaces
            and not claims
            and all(m["mask_status"] == "NO_SPACE" for m in doc["markets"])
        )
    else:
        require(len(doc["participants"]) == 2)
    return doc


def validate_outcomes(value, bundle_id):
    require(type(value) is dict)
    if value.get("provider") is None:
        obj(value, "provider unavailable")
        require(value == UNAVAILABLE)
    else:
        obj(value, "provider document")
        require(value["provider"] == "universe")
        validate_document(value["document"], bundle_id)
    return value


def outcome_books(scope, detail, outcomes):
    doc = outcomes.get("document")
    markets = {m["market_id"]: m for m in doc["markets"]} if doc else {}
    claims = {c["claim_id"]: c for c in doc["claims"]} if doc else {}
    targets = {t["target_id"]: t for t in detail["context"]["targets"]}
    entries = []
    for member in scope["members"]:
        mid = member["market_id"]
        market = markets.get(mid)
        for book in member["books"]:
            status, reason, cid, shape, negated = (
                "OUTCOMES_UNAVAILABLE",
                None,
                None,
                None,
                None,
            )
            if doc is not None:
                if market is None:
                    status = "NOT_IN_MODEL"
                elif market["subscription_ids"] != targets[mid]["subscription_ids"]:
                    status = "SUBSCRIPTION_MISMATCH"
                elif market["mask_status"] != "MASKED":
                    status, reason = market["mask_status"], market["reason"]
                else:
                    native = book["instrument"].split(":", 1)[1]
                    token = next(
                        t for t in market["tokens"] if t["subscription_id"] == native
                    )
                    ref = next(
                        r
                        for r in market["claims"]
                        if r["claim_key"] == token["claim_key"]
                    )
                    status, cid = "MASKED", ref["claim_id"]
                    shape = claims[cid]["space_shape_id"]
                    negated = token["negated"] ^ (
                        market["venue"] == "kalshi"
                        and book["orientation"] == "complement"
                    )
            entries.append(
                {
                    **book,
                    "market_id": mid,
                    "status": status,
                    "reason": reason,
                    "space_shape_id": shape,
                    "claim_id": cid,
                    "negated": negated,
                }
            )
    return sorted(entries, key=lambda b: (b["instrument"], b["orientation"]))
