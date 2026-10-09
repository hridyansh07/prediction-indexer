"""One route table replacing the former if-chain: ``(method, pattern, handler, requires)``.

The first matching route wins, in exactly the former order. ``Exact`` compares the
whole path. ``Prefix`` tests ``startswith``/``endswith`` and hands the handler the
remainder after ``removeprefix``/``removesuffix``, the same string operations the
if-chain used. ``requires`` names an optional application component (``auth``,
``replay_jobs`` or ``runner_config``); when it is absent the route is skipped, as
the former ``and self.<component> is not None`` conditions did.
"""

from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Callable

from universe.api import replay_jobs as jobs
from universe.api.framing import _integer, _only, _optional_positive_integer, _path_value
from universe.store import BundleEventConflict

if TYPE_CHECKING:
    from universe.api.server import UniverseApplication

Response = tuple[int, dict[str, Any]]


@dataclass(frozen=True)
class Request:
    query: dict[str, list[str]]
    headers: Any
    value: str = ""  # the path remainder a Prefix pattern matched
    document: dict[str, Any] | None = None
    raw_body: bytes | None = None


@dataclass(frozen=True)
class Exact:
    path: str

    def match(self, path: str) -> str | None:
        return "" if path == self.path else None


@dataclass(frozen=True)
class Prefix:
    prefix: str
    suffix: str = ""

    def match(self, path: str) -> str | None:
        if not (path.startswith(self.prefix) and path.endswith(self.suffix)):
            return None
        return path.removeprefix(self.prefix).removesuffix(self.suffix)


@dataclass(frozen=True)
class Route:
    method: str
    pattern: Exact | Prefix
    handler: Callable[[UniverseApplication, Request], Response]
    requires: str | None = None


def _relationship_types() -> dict[str, Any]:
    """The relation types a claim pair can carry.

    IDENTITY is absent by construction: equal outcome subsets are one claim, so
    equivalence is shared membership rather than a relation. REVERSE_IMPLICATION
    is normalized to IMPLICATION with the antecedent named first. OVERLAP is the
    catch-all branch of the mask comparison rather than a finding, and the
    targeter's own scorer discards it, so it is never stored.
    """
    return {
        "relationship_type_catalog_version": 2,
        "types": [
            {
                "type": "IMPLICATION",
                "directed": True,
                "member_roles": ["antecedent", "consequent"],
            },
            {
                "type": "MUTUAL_EXCLUSION",
                "directed": False,
                "member_roles": ["member"],
            },
        ],
    }


def health(app: UniverseApplication, r: Request) -> Response:
    _only(r.query, set())
    return HTTPStatus.OK, app.database.status()


def runs(app: UniverseApplication, r: Request) -> Response:
    return HTTPStatus.OK, app._runs(r.query)


def selections(app: UniverseApplication, r: Request) -> Response:
    return HTTPStatus.OK, app._selections(r.query)


def bundles(app: UniverseApplication, r: Request) -> Response:
    return HTTPStatus.OK, app._bundles(r.query)


def events(app: UniverseApplication, r: Request) -> Response:
    return HTTPStatus.OK, app._events(r.query)


def relationship_types(app: UniverseApplication, r: Request) -> Response:
    _only(r.query, set())
    return HTTPStatus.OK, _relationship_types()


def targeter_status(app: UniverseApplication, r: Request) -> Response:
    _only(r.query, {"limit"})
    return HTTPStatus.OK, app.database.targeter_status_snapshot(
        limit=_integer(r.query, "limit", default=5)
    )


def targeter_run(app: UniverseApplication, r: Request) -> Response:
    _only(r.query, set())
    run_id = _path_value(r.value, "run id")
    detail = app.database.targeter_run_detail(run_id)
    if detail is None:
        return HTTPStatus.NOT_FOUND, {"error": "run not found"}
    return HTTPStatus.OK, detail


def event(app: UniverseApplication, r: Request) -> Response:
    _only(r.query, set())
    event_id = _path_value(r.value, "event id")
    detail = app.database.event_detail(event_id)
    if detail is None:
        return HTTPStatus.NOT_FOUND, {"error": "event not found"}
    return HTTPStatus.OK, detail


def market(app: UniverseApplication, r: Request) -> Response:
    query = r.query
    _only(query, {"market_template_version", "outcome_space_version"})
    market_id = _path_value(r.value, "market id")
    detail = app.database.market_detail(
        market_id,
        market_template_version=_optional_positive_integer(
            query, "market_template_version"
        ),
        outcome_space_version=_optional_positive_integer(
            query, "outcome_space_version"
        ),
    )
    if detail is None:
        return HTTPStatus.NOT_FOUND, {"error": "market not found"}
    return HTTPStatus.OK, detail


def claim_markets(app: UniverseApplication, r: Request) -> Response:
    _only(r.query, {"limit", "cursor"})
    claim_id = _path_value(r.value, "claim id")
    if not app.database.claim_exists(claim_id):
        return HTTPStatus.NOT_FOUND, {"error": "claim not found"}
    return HTTPStatus.OK, app._claim_markets(claim_id, r.query)


def claim(app: UniverseApplication, r: Request) -> Response:
    _only(r.query, set())
    claim_id = _path_value(r.value, "claim id")
    detail = app.database.claim_detail(claim_id)
    if detail is None:
        return HTTPStatus.NOT_FOUND, {"error": "claim not found"}
    return HTTPStatus.OK, detail


def bundle_outcomes(app: UniverseApplication, r: Request) -> Response:
    _only(r.query, set())
    bundle_id = _path_value(r.value, "bundle id")
    try:
        detail = app.database.bundle_outcomes(bundle_id)
    except BundleEventConflict as error:
        return HTTPStatus.CONFLICT, {"error": str(error)}
    if detail is None:
        return HTTPStatus.NOT_FOUND, {"error": "bundle not found"}
    return HTTPStatus.OK, detail


def bundle_history(app: UniverseApplication, r: Request) -> Response:
    bundle_id = _path_value(r.value, "bundle id")
    return HTTPStatus.OK, app._selections(
        r.query, bundle_id=bundle_id, default_sort="selected"
    )


def run_paths(app: UniverseApplication, r: Request) -> Response:
    query = r.query
    parts = r.value.split("/")
    run_id = _path_value(parts[0], "run id")
    if len(parts) == 1:
        _only(query, set())
        detail = app.database.run_detail(run_id)
        if detail is None:
            return HTTPStatus.NOT_FOUND, {"error": "run not found"}
        return HTTPStatus.OK, detail
    if len(parts) == 2 and parts[1] == "audit":
        _only(query, set())
        audit = app.database.audit_run(run_id)
        if audit is None:
            return HTTPStatus.NOT_FOUND, {"error": "run not found"}
        return HTTPStatus.OK, audit
    if len(parts) == 2 and parts[1] == "selections":
        return HTTPStatus.OK, app._selections(
            query, run_id=run_id, default_sort="activation"
        )
    if len(parts) == 3 and parts[1] == "selections":
        _only(query, set())
        bundle_id = _path_value(parts[2], "bundle id")
        detail = app.database.selection_detail(run_id, bundle_id)
        if detail is None:
            return HTTPStatus.NOT_FOUND, {"error": "selection not found"}
        return HTTPStatus.OK, detail
    return HTTPStatus.NOT_FOUND, {"error": "not found"}


ROUTES: tuple[Route, ...] = (
    Route("GET", Exact("/v1/auth/nonce"), jobs.nonce, "auth"),
    Route("GET", Exact("/v1/admin/allowlist"), jobs.list_members, "auth"),
    Route("GET", Exact("/v1/replay/jobs"), jobs.list_jobs, "replay_jobs"),
    Route("GET", Exact("/v1/replay/strategies"), jobs.strategies, "runner_config"),
    Route("GET", Prefix("/v1/replay/jobs/", "/events"), jobs.job_events, "replay_jobs"),
    Route("GET", Prefix("/v1/replay/jobs/"), jobs.job, "replay_jobs"),
    Route("GET", Exact("/healthz"), health),
    Route("GET", Exact("/v1/runs"), runs),
    Route("GET", Exact("/v1/selections"), selections),
    Route("GET", Exact("/v1/bundles"), bundles),
    Route("GET", Exact("/v1/events"), events),
    Route("GET", Exact("/v1/relationship-types"), relationship_types),
    Route("GET", Exact("/v1/targeter/status"), targeter_status),
    Route("GET", Prefix("/v1/targeter/runs/"), targeter_run),
    Route("GET", Prefix("/v1/events/"), event),
    Route("GET", Prefix("/v1/markets/"), market),
    Route("GET", Prefix("/v1/claims/", "/markets"), claim_markets),
    Route("GET", Prefix("/v1/claims/"), claim),
    Route("GET", Prefix("/v1/bundles/", "/outcomes"), bundle_outcomes),
    Route("GET", Prefix("/v1/bundles/", "/history"), bundle_history),
    Route("GET", Prefix("/v1/runs/"), run_paths),
    # POST and DELETE run only when auth is configured and the query is empty.
    Route("POST", Exact("/v1/auth/siwe"), jobs.siwe),
    Route("POST", Exact("/v1/auth/logout"), jobs.logout),
    Route("POST", Exact("/v1/admin/allowlist"), jobs.add_member),
    Route("POST", Exact("/v1/replay/jobs"), jobs.submit, "replay_jobs"),
    Route("POST", Prefix("/v1/replay/jobs/", "/cancel"), jobs.cancel, "replay_jobs"),
    Route("DELETE", Prefix("/v1/admin/allowlist/"), jobs.remove_member),
)
