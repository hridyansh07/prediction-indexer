"""One bundle through every strategy group: stage, prepare, bench, commit, clean up."""

from __future__ import annotations

import hashlib
import json
import shutil
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import ProxyHandler, build_opener

from archive.storage.local import LocalObjectStore
from replay.corpus import results
from replay.corpus.fees import fee_inputs
from replay.corpus.overlay import OverlayStore
from replay.corpus.spec import substitute

PUBLISHER, PYTHON = Path("/usr/local/bin/replay-publish"), Path("/usr/local/bin/python3.13")
DERIVATIVES_MOUNT = "/bench/in/derivatives"


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
                 ensure_bundle, prepare_stage, supervisor_config, prepare_game_state, run_bench):
        self.spec, self.corpus_key, self.config = spec, corpus_key, config
        self.canonical, self.history, self.game_store = canonical, history, game_store
        self.image_id = image_id
        self.ensure_bundle, self.prepare_stage = ensure_bundle, prepare_stage
        self.supervisor_config, self.prepare_game_state, self.run_bench = supervisor_config, prepare_game_state, run_bench

    def results_dir(self, bundle_id):
        return Path(self.spec["results_root"]) / self.corpus_key[:16] / bundle_id

    def run(self, bundle_id):
        """Return (state, bench_exit, receipt_sha256); raise StageError on a failed stage."""
        from replay.jobs.contracts import parse_request
        work = Path(self.spec["work_root"]) / bundle_id
        shutil.rmtree(work, ignore_errors=True)   # a crashed earlier attempt
        work.mkdir(parents=True)
        try:
            job_id = _job_id(bundle_id)
            request = _stage("request", parse_request, json.dumps({
                "replay_request_version": 1, "bundle_id": bundle_id, "probe_markets": None, "interval": None,
                "strategy": {"name": "bundle_coverage", "config": {}}, "limits": "small"}).encode(), self.config)
            resolved = _stage("resolve", self.history.resolve, job_id, request, self.config)
            store = OverlayStore(self.canonical, LocalObjectStore(work / "layer", store_id="corpus-layer"))
            for name in ("work", "derivatives", "job"):
                (work / name).mkdir()
            ready = _stage("materialize", self.ensure_bundle, bundle_id, resolved.window_interval, store=store,
                           work_root=work / "work", derivatives_root=work / "derivatives",
                           materializer=Path(self.spec["materializer"]),
                           window_seconds=self.config.canonical_window_seconds)
            if not hasattr(ready, "pins"):
                raise StageError("materialize", RuntimeError(f"bundle not ready: {ready}"))
            snapshot = _stage("prepare", self.prepare_stage, work / "job", request, resolved, ready.receipt,
                              ready.pins, self.config)
            context = work / "job" / "context"
            game_path, game_state = None, None
            if self.spec["game_state"]["prepare"]:
                _stage("game_state", self.prepare_game_state, context, work / "game_state", store=self.game_store)
                game_path = work / "game_state" / "game_state.json"
                document = json.loads(game_path.read_bytes())
                game_state = {"state": document["state"], "reason": document["reason"]}
            catalog, fees, valuation = _stage("fees", fee_inputs, snapshot, self.spec["fees"], work / "fees")
            document = _stage("run_config", self._run_config, job_id, request, ready, snapshot, context, work)
            (work / "run.json").write_text(json.dumps(document, indent=1))
            bench_spec = {
                "version": 1,
                "image": {"build_context": self.spec["image"]["build_context"],
                          "dockerfile": self.spec["image"]["dockerfile"], "reuse_id": self.image_id},
                "redis": self.spec["redis"], "base_run_config": str(work / "run.json"),
                "context_directory": str(context), "fee_catalog_directory": str(catalog),
                "mounts": [{"host": str(work / "derivatives"), "container": DERIVATIVES_MOUNT}],
                "limits": self.spec["limits"],
                "groups": [{"name": g["name"], "factory": g["factory"], "revision": self.corpus_key[:16],
                            "config": substitute(g["config"], {"@corpus:fees": fees, "@corpus:valuation": valuation}),
                            "reader": g["reader"], "checks": g["checks"], "compare": g["compare"]}
                           for g in self.spec["groups"]],
            }
            if game_path is not None:
                bench_spec["game_state_path"] = str(game_path)
            (work / "bench-spec.json").write_text(json.dumps(bench_spec, indent=1))
            exit_code = _stage("bench", self.run_bench, work / "bench-spec.json", work / "bench",
                               keep_image=True, label="corpus")
            if self.image_id is None and (work / "bench" / "image.id").exists():
                self.image_id = (work / "bench" / "image.id").read_text().strip()
            status = json.loads((work / "bench" / "result.json").read_bytes())["status"]
            receipt_sha = _stage("commit", results.commit, work / "bench", self.results_dir(bundle_id),
                                 corpus_key=self.corpus_key, bundle_id=bundle_id, status=status,
                                 bench_exit=exit_code, game_state=game_state)
            state = {"SUCCESS": "done", "GAME_STATE_UNAVAILABLE": "game_state_unavailable"}.get(status, "failed")
            return state, exit_code, receipt_sha
        finally:
            shutil.rmtree(work, ignore_errors=True)

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
        host = str((work / "derivatives").resolve())
        for item in document["transport"]["inputs"]:
            directory = str(Path(item["directory"]).resolve())
            if not directory.startswith(host):
                raise ValueError(f"derivative outside the bundle work root: {directory}")
            item["directory"] = DERIVATIVES_MOUNT + directory[len(host):]
        document["publisher"], document["python"] = str(PUBLISHER), str(PYTHON)
        return document


def run_corpus(pipeline, ledger, bundles, *, retry_failed=False, log=print):
    """Sequential, resumable: finished bundles are skipped; failures are recorded, never fatal."""
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
            state, exit_code, receipt = pipeline.run(bundle_id)
            ledger.finish(pipeline.corpus_key, bundle_id, state, seconds=time.monotonic() - started,
                          bench_exit=exit_code, receipt_sha256=receipt)
            log(f"[{index}/{len(bundles)}] {bundle_id} {state} exit={exit_code} {time.monotonic() - started:.0f}s")
        except StageError as error:
            ledger.finish(pipeline.corpus_key, bundle_id, "failed", seconds=time.monotonic() - started,
                          stage=error.stage, detail="".join(traceback.format_exception(error.error))[-4000:])
            log(f"[{index}/{len(bundles)}] {bundle_id} FAILED at {error.stage}: {error}")
        done += 1
        if pipeline.image_id is not None:
            ledger.set_meta("image_id:" + pipeline.corpus_key, pipeline.image_id)
    return done, skipped
