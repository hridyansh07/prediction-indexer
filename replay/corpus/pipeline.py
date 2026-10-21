"""One bundle through every strategy group: stage, prepare, bench, commit, clean up."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import shutil
import time
import traceback
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import ProxyHandler, build_opener

from replay.corpus import results
from replay.corpus.fees import fee_inputs
from replay.corpus.overlay import OverlayStore, VerifyingSink
from replay.corpus.spec import substitute

PUBLISHER, PYTHON = Path("/usr/local/bin/replay-publish"), Path("/usr/local/bin/python3.13")
DERIVATIVES_MOUNT = "/bench/in/derivatives"
PRIVATE_DERIVATIVES_MOUNT = "/bench/in/private-derivatives"
OUTCOME_ATTEMPTS, OUTCOME_RETRY_SECONDS = 3, (5, 15)


class StageError(RuntimeError):
    def __init__(self, stage, error):
        super().__init__(f"{stage}: {type(error).__name__}: {error}")
        self.stage, self.error = stage, error


def retired_bundles(base_url, *, timeout=120, opener=None):
    """Every retired bundle from Universe, oldest capture first."""
    opener = opener or build_opener(ProxyHandler({}))
    rows, cursor = [], None
    while True:
        url = base_url.rstrip("/") + "/v1/bundles?limit=100" + ("" if cursor is None else "&cursor=" + quote(cursor))
        with opener.open(url, timeout=timeout) as response:
            page = json.loads(response.read())
        rows.extend(page["bundles"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    retired = [row for row in rows if row["lifecycle"] == "retired"]
    return [row["bundle_id"] for row in sorted(retired, key=lambda r: (r["capture_start_at"], r["bundle_id"]))]


def _strategy_class(factory_ref):
    module, attr = factory_ref.split(":")
    build = getattr(importlib.import_module(module), attr)
    return inspect.getclosurevars(build).nonlocals.get("strategy_class")


def preflight_groups(groups, resolve):
    """Split groups into runnable and excluded before the bench starts.

    A strategy that refuses at setup exits fatally and the supervisor stops every
    group of that replay, so one oversized entity table would cost the whole
    bundle. Economic-SDK groups are constructed on the host and run through the
    same entity-table preflight the runtime performs; refusals are excluded and
    recorded instead.
    """
    from replay.economic_sdk import entity_tables
    runnable, excluded = [], []
    for group in groups:
        strategy_class = _strategy_class(group["factory"])
        if strategy_class is None:
            runnable.append(group)
            continue
        try:
            entity_tables.preflight(strategy_class(resolve(group["config"])))
        except Exception as error:
            excluded.append({"group": group["name"], "reason": f"{type(error).__name__}: {error}"[:500]})
        else:
            runnable.append(group)
    return runnable, excluded


def _job_id(bundle_id):
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + hashlib.sha256(bundle_id.encode()).hexdigest()[:16]


def _stage(name, function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except Exception as error:   # recorded per bundle; one bundle never stops the corpus
        raise StageError(name, error) from error


class Pipeline:
    """Dependencies are injected so tests run without Universe, GCS, Rust or Docker."""

    def __init__(self, spec, *, corpus_key, config, canonical, history, game_store, image_id,
                 ensure_bundle, prepare_stage, supervisor_config, prepare_game_state, run_bench, sleep=time.sleep):
        self.spec, self.corpus_key, self.config = spec, corpus_key, config
        self.canonical, self.history, self.game_store = canonical, history, game_store
        self.image_id = image_id
        self.ensure_bundle, self.prepare_stage = ensure_bundle, prepare_stage
        self.supervisor_config, self.prepare_game_state, self.run_bench = supervisor_config, prepare_game_state, run_bench
        self.sleep = sleep
        self.derivative_cache, self.cache_diagnostics = None, None
        if "derivative_cache" in spec:
            from replay.jobs.derivative_cache import DerivativeCache
            self.derivative_cache = DerivativeCache(**spec["derivative_cache"])

    def results_dir(self, bundle_id):
        return Path(self.spec["results_root"]) / self.corpus_key[:16] / bundle_id

    def run(self, bundle_id):
        """Return (state, bench_exit, receipt_sha256, excluded_groups); raise StageError on a failed stage."""
        from replay.jobs.contracts import parse_request
        work = Path(self.spec["work_root"]) / bundle_id
        shutil.rmtree(work, ignore_errors=True)   # a crashed earlier attempt
        work.mkdir(parents=True)
        leases = ExitStack()
        self.cache_diagnostics = None
        try:
            job_id = _job_id(bundle_id)
            request = _stage("request", parse_request, json.dumps({
                "replay_request_version": 1, "bundle_id": bundle_id, "probe_markets": None, "interval": None,
                "strategy": {"name": "bundle_coverage", "config": {}}, "limits": "small"}).encode(), self.config)
            resolved = _stage("resolve", self.history.resolve, job_id, request, self.config)
            store = OverlayStore(self.canonical, VerifyingSink())
            for name in ("work", "derivatives", "job"):
                (work / name).mkdir()
            cache_options = {}
            if self.derivative_cache is not None:
                lease = _stage("materialize", leases.enter_context, self.derivative_cache.lease())
                cache_options["derivative_cache"] = lease
                self.cache_diagnostics = lease.diagnostics
            ready = _stage("materialize", self.ensure_bundle, bundle_id, resolved.window_interval, store=store,
                           work_root=work / "work", derivatives_root=work / "derivatives",
                           materializer=Path(self.spec["materializer"]),
                           window_seconds=self.config.canonical_window_seconds, **cache_options)
            if not hasattr(ready, "pins"):
                raise StageError("materialize", RuntimeError(f"bundle not ready: {ready}"))
            snapshot = self._prepare(work, request, resolved, ready)
            context = work / "job" / "context"
            game_path, game_state = None, None
            if self.spec["game_state"]["prepare"]:
                _stage("game_state", self.prepare_game_state, context, work / "game_state", store=self.game_store)
                game_path = work / "game_state" / "game_state.json"
                document = json.loads(game_path.read_bytes())
                game_state = {"state": document["state"], "reason": document["reason"]}
            catalog, fees, valuation = _stage("fees", fee_inputs, snapshot, self.spec["fees"], work / "fees")
            groups = [dict(g, config=substitute(g["config"], {"@corpus:fees": fees, "@corpus:valuation": valuation}))
                      for g in self.spec["groups"]]
            tokens = {"{context}": str(context),
                      "{snapshot_sha256}": json.loads((context / "receipt.json").read_bytes())["snapshot_sha256"],
                      "{fees}": str(Path(catalog).parent), "{catalog_identity}": Path(catalog).name}
            if game_path is not None:
                tokens["{game_state}"] = str(game_path)
                tokens["{game_state_sha256}"] = hashlib.sha256(game_path.read_bytes()).hexdigest()

            def resolve(config):
                text = json.dumps(config)
                for token, value in tokens.items():
                    text = text.replace(token, value)
                return json.loads(text)
            groups, excluded = _stage("preflight", preflight_groups, groups, resolve)
            if not groups:
                raise StageError("preflight", RuntimeError(f"every group refused: {excluded}"))
            document = _stage("run_config", self._run_config, job_id, request, ready, snapshot, context, work)
            from replay.economic_sdk.retention import for_groups
            document["transport"].update(for_groups(snapshot, [dict(g, config=resolve(g["config"])) for g in groups]))
            (work / "run.json").write_text(json.dumps(document, indent=1))
            bench_spec = {
                "version": 1,
                "image": {"build_context": self.spec["image"]["build_context"],
                          "dockerfile": self.spec["image"]["dockerfile"], "reuse_id": self.image_id},
                "redis": self.spec["redis"], "base_run_config": str(work / "run.json"),
                "context_directory": str(context), "fee_catalog_directory": str(catalog),
                "mounts": self._derivative_mounts(work),
                "limits": self.spec["limits"],
                "groups": [{"name": g["name"], "factory": g["factory"], "revision": self.corpus_key[:16],
                            "config": g["config"], "reader": g["reader"], "checks": g["checks"],
                            "compare": g["compare"]} for g in groups],
            }
            if game_path is not None:
                bench_spec["game_state_path"] = str(game_path)
            (work / "bench-spec.json").write_text(json.dumps(bench_spec, indent=1))
            exit_code = _stage("bench", self.run_bench, work / "bench-spec.json", work / "bench",
                               keep_image=True, label="corpus")
            if self.image_id is None and (work / "bench" / "image.id").exists():
                self.image_id = (work / "bench" / "image.id").read_text().strip()
            status = json.loads((work / "bench" / "result.json").read_bytes())["status"]
            kept = [("context/context.json", context / "context.json"),
                    ("context/receipt.json", context / "receipt.json")]
            if game_path is not None:
                kept.append(("game_state/game_state.json", game_path))
            receipt_sha = _stage("commit", results.commit, work / "bench", self.results_dir(bundle_id),
                                 corpus_key=self.corpus_key, bundle_id=bundle_id, status=status,
                                 bench_exit=exit_code, game_state=game_state, excluded_groups=excluded,
                                 inputs=kept)
            state = {"SUCCESS": "done", "GAME_STATE_UNAVAILABLE": "game_state_unavailable"}.get(status, "failed")
            return state, exit_code, receipt_sha, excluded
        finally:
            try:
                leases.close()
            finally:
                shutil.rmtree(work, ignore_errors=True)

    def _prepare(self, work, request, resolved, ready):
        """Prepare the context; outcomes must come from Universe.

        Preparation records UNAVAILABLE outcomes when Universe times out or errors,
        and every outcome-dependent strategy then admits nothing. That is not a
        result, so the context is discarded and prepared again, and the bundle
        fails at stage "outcomes" if Universe never answers.
        """
        for attempt in range(1, OUTCOME_ATTEMPTS + 1):
            snapshot = _stage("prepare", self.prepare_stage, work / "job", request, resolved, ready.receipt,
                              ready.pins, self.config)
            outcomes = snapshot.get("outcomes") or {}
            if outcomes.get("provider") == "universe":
                return snapshot
            shutil.rmtree(work / "job" / "context")   # a present receipt would be reused as-is
            if attempt < OUTCOME_ATTEMPTS:
                self.sleep(OUTCOME_RETRY_SECONDS[attempt - 1])
        raise StageError("outcomes", RuntimeError(
            f"{outcomes.get('unavailable', 'outcomes unavailable')} after {OUTCOME_ATTEMPTS} attempts"))

    def _derivative_mounts(self, work):
        if self.derivative_cache is None:
            return [{"host": str(work / "derivatives"), "container": DERIVATIVES_MOUNT}]
        return [{"host": str(self.derivative_cache.derivatives), "container": DERIVATIVES_MOUNT},
                {"host": str(work / "derivatives"), "container": PRIVATE_DERIVATIVES_MOUNT}]

    def _run_config(self, job_id, request, ready, snapshot, context, work):
        import replay.jobs.stages as stages
        # The publisher preflight is a Linux binary; the bench container repeats
        # the same validation, so skip it on the host (as the fixture builder does).
        original, stages.validate_supervisor = stages.validate_supervisor, (lambda document: None)
        try:
            document = self.supervisor_config(job_id, request, self.config, ready.receipt, ready.pins, snapshot,
                                              context, publisher=PUBLISHER, python=PYTHON,
                                              image_revision=self.corpus_key[:16])
        finally:
            stages.validate_supervisor = original
        for item in document["transport"]["inputs"]:
            directory = Path(item["directory"]).resolve()
            for mount in self._derivative_mounts(work):
                host = Path(mount["host"]).resolve()
                if directory.is_relative_to(host):
                    item["directory"] = str(Path(mount["container"]) / directory.relative_to(host))
                    break
            else:
                raise ValueError(f"derivative outside the bundle derivative mounts: {directory}")
        document["publisher"], document["python"] = str(PUBLISHER), str(PYTHON)
        return document


def run_corpus(pipeline, ledger, bundles, *, retry_failed=False, log=print):
    """Sequential, resumable: finished bundles are skipped; failures are recorded, never fatal.

    An interrupt is not a failure. It stops the corpus with the bundle's row still `running`
    and nothing committed, so the next run redoes that bundle.
    """
    done = skipped = 0
    for index, bundle_id in enumerate(bundles, 1):
        row = ledger.state(pipeline.corpus_key, bundle_id)
        if row is not None and (row["state"] in ("done", "game_state_unavailable")
                                or (row["state"] == "failed" and not retry_failed)):
            skipped += 1
            continue
        if row is not None and row["state"] == "failed":
            shutil.rmtree(pipeline.results_dir(bundle_id), ignore_errors=True)   # replaced by the retry
        ledger.start(pipeline.corpus_key, bundle_id)
        started = time.monotonic()
        try:
            state, exit_code, receipt, excluded = pipeline.run(bundle_id)
            detail = {"excluded_groups": excluded} if excluded else {}
            if pipeline.cache_diagnostics is not None:
                detail["derivative_cache"] = pipeline.cache_diagnostics
            ledger.finish(pipeline.corpus_key, bundle_id, state, seconds=time.monotonic() - started,
                          bench_exit=exit_code, receipt_sha256=receipt,
                          detail=json.dumps(detail) if detail else None)
            note = f" excluded={[e['group'] for e in excluded]}" if excluded else ""
            log(f"[{index}/{len(bundles)}] {bundle_id} {state} exit={exit_code} {time.monotonic() - started:.0f}s{note}")
        except StageError as error:
            detail = "".join(traceback.format_exception(error.error))[-4000:]
            if pipeline.cache_diagnostics is not None:
                detail = json.dumps({"error": detail, "derivative_cache": pipeline.cache_diagnostics})
            ledger.finish(pipeline.corpus_key, bundle_id, "failed", seconds=time.monotonic() - started,
                          stage=error.stage, detail=detail)
            log(f"[{index}/{len(bundles)}] {bundle_id} FAILED at {error.stage}: {error}")
        except KeyboardInterrupt as interrupt:
            interrupt.bundle_id = bundle_id
            raise
        done += 1
        if pipeline.image_id is not None:
            ledger.set_meta("image_id:" + pipeline.corpus_key, pipeline.image_id)
    return done, skipped
