"""python -m replay.corpus {run,status,verify} SPEC

run     replay every retired bundle (oldest first) through every spec group, resumably
status  ledger totals for this spec's corpus key
verify  re-hash every committed result against its corpus receipt
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from replay.corpus import results, spec as specs
from replay.corpus.ledger import Ledger


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def corpus_key(spec, *, source, materializer_describe, runner_config):
    """Every input that changes results; a different key is a separate corpus run."""
    fees = dict(spec["fees"])
    fees["template_catalog"] = Path(spec["fees"]["template_catalog"]).name
    fees["sources"] = sorted(_sha(p.read_bytes()) for p in Path(spec["fees"]["sources"]).iterdir() if p.is_file())
    identity = {"corpus_key_version": 1, "groups": spec["groups"], "image_source": source,
                "dockerfile": spec["image"]["dockerfile"], "materializer": _sha(materializer_describe),
                "runner_config": _sha(runner_config), "fees": fees, "game_state": spec["game_state"],
                "limits": spec["limits"]}
    return _sha(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())


def _context(spec, allow_dirty):
    from replay.bench.docker import git_source
    commit, dirty = git_source(spec["image"]["build_context"])
    if dirty and not allow_dirty:
        raise SystemExit("image build context has uncommitted changes; commit them or pass --allow-dirty")
    describe = subprocess.run([spec["materializer"], "--describe"], capture_output=True, check=True).stdout
    raw = json.loads(Path(spec["runner_config"]).read_bytes())
    raw["universe_base_url"] = spec["universe_base_url"]
    runner_config = json.dumps(raw, sort_keys=True).encode()
    key = corpus_key(spec, source={"commit": commit, "dirty": dirty}, materializer_describe=describe,
                     runner_config=runner_config)
    return key, runner_config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("run", "status", "verify"))
    parser.add_argument("spec", type=Path)
    parser.add_argument("--limit", type=int, help="process at most N pending bundles")
    parser.add_argument("--bundle", action="append", default=[], help="only these bundle ids (repeatable)")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--refresh-bundles", action="store_true",
                        help="re-list retired bundles from Universe instead of the cached list")
    args = parser.parse_args(argv)
    spec = specs.load(args.spec)
    key, runner_config = _context(spec, args.allow_dirty)
    ledger = Ledger(spec["ledger"])
    print("corpus key", key)
    if args.command == "status":
        print(json.dumps(ledger.summary(key), indent=1))
        return 0
    if args.command == "verify":
        root, bad, good = Path(spec["results_root"]) / key[:16], [], 0
        for directory in sorted(p for p in root.iterdir() if p.is_dir()) if root.exists() else []:
            try:
                results.verify(directory)
                good += 1
            except Exception as error:
                bad.append((directory.name, str(error)))
        print(f"verified {good}, failed {len(bad)}")
        for name, error in bad:
            print("  ", name, error)
        return 1 if bad else 0

    from archive.storage.factory import build_store
    from archive.storage.local import LocalObjectStore
    from replay.bench.docker import run_bench
    from replay.corpus.pipeline import Pipeline, retired_bundles, run_corpus
    from replay.jobs.bundle import ensure_bundle
    from replay.jobs.contracts import parse_runner_config
    from replay.jobs.stages import HistoryClient, prepare_stage, supervisor_config
    from replay.prepare_game_state import prepare_game_state

    config = parse_runner_config(runner_config)
    game_store = None
    if spec["game_state"]["prepare"]:
        Path(spec["work_root"]).mkdir(parents=True, exist_ok=True)
        game_store = build_store([Path(spec["work_root"])])
    cache = Path(spec["ledger"]).with_name("retired-bundles.json")
    if args.bundle:
        bundles = args.bundle
    elif cache.exists() and not args.refresh_bundles:
        bundles = json.loads(cache.read_bytes())
    else:
        # Listing pages through the whole selection history (minutes on the small
        # Universe server); the retired set is fixed while capture is off.
        bundles = retired_bundles(spec["universe_base_url"])
        cache.write_text(json.dumps(bundles, indent=1))
    if spec["bundles"]["only"] is not None:
        bundles = [b for b in bundles if b in set(spec["bundles"]["only"])]
    bundles = [b for b in bundles if b not in set(spec["bundles"]["exclude"] or ())]
    if args.limit is not None:
        pending = [b for b in bundles if (row := ledger.state(key, b)) is None
                   or row["state"] == "running" or (row["state"] == "failed" and args.retry_failed)]
        bundles = pending[:args.limit]
    pipeline = Pipeline(spec, corpus_key=key, config=config,
                        canonical=LocalObjectStore(spec["canonical_store"], store_id="corpus-canonical"),
                        history=HistoryClient(spec["universe_base_url"], timeout=120), game_store=game_store,
                        image_id=ledger.get_meta("image_id:" + key), ensure_bundle=ensure_bundle,
                        prepare_stage=prepare_stage, supervisor_config=supervisor_config,
                        prepare_game_state=prepare_game_state, run_bench=run_bench)
    print(f"{len(bundles)} bundles selected")
    processed, skipped = run_corpus(pipeline, ledger, bundles, retry_failed=args.retry_failed,
                                    log=lambda m: print(m, flush=True))
    print(f"processed {processed}, skipped {skipped}")
    print(json.dumps(ledger.summary(key), indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
