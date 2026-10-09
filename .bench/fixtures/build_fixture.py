"""Build one local replay fixture for a retired bundle, through the real job stages.

Usage (from a checkout, repo on PYTHONPATH, Universe reachable at UNIVERSE):
    build_fixture.py BUNDLE_ID NAME [START_NS END_NS SUFFIX]

Env FIXTURE_MATERIALIZER / FIXTURE_STORE / FIXTURE_DERIVATIVES / FIXTURE_SUFFIX
select another materializer, store, derivatives directory and job/run suffix.
With START_NS/END_NS the job covers that sub-interval (fewer scopes); its context
and run config go to job-SUFFIX/ and run-SUFFIX.json, sharing store and derivatives.

1. HistoryClient.resolve: the bundle interval and targeter occurrences.
2. Download every canonical window object the interval needs from GCS (gcloud CLI)
   into a LocalObjectStore under NAME/store, keeping GCS content type/encoding.
3. ensure_bundle: materialize verified derivatives with the host materializer.
4. prepare_stage: the pinned context (with Universe outcomes).
5. prepare_fixture_stage: one immutable game_state/game_state.json per event.
6. supervisor_config: the base run config, with container paths for the bench
   (derivatives at /bench/in/derivatives, image publisher and python).

Nothing here writes to GCS or Universe. Re-running resumes: present store objects,
derivatives and context are reused (and re-verified by the stages).
"""

import hashlib, json, os, subprocess, sys, tempfile, time
from datetime import datetime, timezone
from pathlib import Path

from archive.storage.local import LocalObjectStore
from archive.storage.factory import build_store
from encoder import stored_identity_of
from replay.jobs.bundle import BundleReady, ensure_bundle
from replay.jobs.contracts import canonical_window_keys, parse_request, parse_runner_config, window_bounds
from replay.jobs.stages import HistoryClient, prepare_stage, supervisor_config
from replay.bench.game_state import prepare_fixture_stage

UNIVERSE = os.environ.get("UNIVERSE", "http://127.0.0.1:18080")
GCLOUD = str(Path.home() / "google-cloud-sdk/bin/gcloud")
BUCKET = os.environ["FIXTURE_BUCKET"]
MATERIALIZER = Path(os.environ.get("FIXTURE_MATERIALIZER") or Path.home() / ".cargo/global-target/release/examples/materialize_range")
STORE_NAME = os.environ.get("FIXTURE_STORE", "store")
DERIVATIVES_NAME = os.environ.get("FIXTURE_DERIVATIVES", "derivatives")
ROOT = Path(__file__).resolve().parent
REPO = Path.cwd()

bundle_id, name = sys.argv[1], sys.argv[2]
interval = None if len(sys.argv) < 6 else {"start_ns": sys.argv[3], "end_ns": sys.argv[4]}
suffix = os.environ.get("FIXTURE_SUFFIX", "") if interval is None else "-" + sys.argv[5]
out = ROOT / name
out.mkdir(exist_ok=True)
log = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)

raw = json.loads((REPO / "configs/replay_runner.json").read_bytes())
raw["universe_base_url"] = UNIVERSE
config = parse_runner_config(json.dumps(raw).encode())
request = parse_request(json.dumps({
    "replay_request_version": 1, "bundle_id": bundle_id, "probe_markets": None, "interval": interval,
    "strategy": {"name": "bundle_coverage", "config": {}}, "limits": "small"}).encode(), config)
job_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + hashlib.sha256(bundle_id.encode()).hexdigest()[:16]

resolved = HistoryClient(UNIVERSE, timeout=120).resolve(job_id, request, config)
(out / f"resolved{suffix}.json").write_text(json.dumps({
    "bundle_interval": list(resolved.bundle_interval), "job_interval": list(resolved.job_interval),
    "window_interval": list(resolved.window_interval),
    "occurrences": [o.document() for o in resolved.occurrences]}, indent=1))
first, last, starts = window_bounds(*resolved.window_interval, config.canonical_window_seconds)
log(bundle_id, "windows", len(starts), "job", resolved.job_interval)

store = LocalObjectStore(out / STORE_NAME)
for start in starts:
    for key in canonical_window_keys(start):
        if store.head(key) is not None:
            continue
        url = f"gs://{BUCKET}/{key}"
        meta = json.loads(subprocess.run([GCLOUD, "storage", "objects", "describe", url, "--format=json"],
                                         capture_output=True, text=True, check=True).stdout)
        with tempfile.NamedTemporaryFile(dir=out, delete=False) as handle:
            temp = Path(handle.name)
        try:
            subprocess.run([GCLOUD, "storage", "cp", "-q", url, str(temp)], check=True, capture_output=True)
            with temp.open("rb") as reader:
                identity = stored_identity_of(reader)
            with temp.open("rb") as reader:
                store.put_immutable(key, reader, identity, content_type=meta.get("content_type"),
                                    content_encoding=meta.get("content_encoding"))
        finally:
            temp.unlink(missing_ok=True)
    log("window", start, "stored")

for directory in ("work" + suffix, DERIVATIVES_NAME, "job" + suffix):
    (out / directory).mkdir(exist_ok=True)
ready = ensure_bundle(bundle_id, resolved.window_interval, store=store, work_root=out / ("work" + suffix),
                      derivatives_root=out / DERIVATIVES_NAME, materializer=MATERIALIZER,
                      window_seconds=config.canonical_window_seconds)
if not isinstance(ready, BundleReady):
    raise SystemExit(f"bundle not ready: {ready}")
(out / f"bundle{suffix}.json").write_bytes(ready.receipt_bytes)
log("derivatives", len(ready.pins))

snapshot = prepare_stage(out / ("job" + suffix), request, resolved, ready.receipt, ready.pins, config)
context = out / ("job" + suffix) / "context"
log("context", json.loads((context / "receipt.json").read_bytes())["snapshot_sha256"])
game_directory = out / "game_state"
game_store = None if game_directory.exists() else build_store(primary_roots=[out], environ={
    "ARCHIVE_BACKEND": "gcs", "ARCHIVE_GCS_BUCKET": BUCKET})
game_pin = prepare_fixture_stage(context, game_directory, store=game_store)
log("game state", game_pin, game_directory / "game_state.json")

# The publisher preflight is a Linux binary; the supervisor repeats this
# validation inside the bench container, so skip it on the host.
import replay.jobs.stages as stages
stages.validate_supervisor = lambda document: None
document = supervisor_config(job_id, request, config, ready.receipt, ready.pins, snapshot, context,
                             publisher=Path("/usr/local/bin/replay-publish"),
                             python=Path("/usr/local/bin/python3.13"), image_revision="fixture")
host = str((out / DERIVATIVES_NAME).resolve())
for item in document["transport"]["inputs"]:
    assert item["directory"].startswith(host), item["directory"]
    item["directory"] = "/bench/in/derivatives" + item["directory"][len(host):]
document["publisher"], document["python"] = "/usr/local/bin/replay-publish", "/usr/local/bin/python3.13"
(out / f"run{suffix}.json").write_text(json.dumps(document, indent=1))
log("wrote", out / f"run{suffix}.json")
