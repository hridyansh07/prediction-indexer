"""market_profile_v1: per-book trading data points beside any strategy.

Supervisor entry ``replay.strategies.market_profile:build`` with the closed configuration
``{version, snapshot_directory, snapshot_sha256, policy, storage?}``; ``policy`` is the
profile policy of ``replay.economic_sdk.profile``. Any SDK strategy can also
request the same output inside its own group through ``Requirements.profile``.
"""

from pathlib import Path

from replay.strategies import canonical_reference

from replay.economic_intervals import CutClock
from replay.economic_sdk.bounds import MAX_METADATA
from replay.economic_sdk.profile import (
    STRATEGY,
    TRANSITIONS_FILE,
    LEVELS_FILE,
    VOLATILITY_FILE,
    Collector,
    profile_files,
    profile_identity,
    profile_policy,
)
from replay.economic_sdk.profile_reader import validate_profile
from replay.economic_sdk.profile_policy import PROFILE_FILE, profile_storage
from replay.economic_sdk.reader import check_files, file_identity, read_json
from replay.economic_sdk.transitions_reader import identity_record
from replay.preparation import digest, encoded, load_snapshot, sha
from replay.strategy_sdk import PreparedInput, plain
from replay.streams.protocol import freeze, obj, require
from replay.supervisor import initial, read_success, write_json_durable
from replay.supervisor import read as read_run


def profile_config(value):
    value = plain(value)
    require(type(value) is dict, "closed schema")
    obj(value, "version snapshot_directory snapshot_sha256 policy"
        + (" storage" if "storage" in value else ""))
    require(len(encoded(value)) <= MAX_METADATA, "configuration budget")
    policy = profile_policy(value["policy"], standalone=True)
    if "storage" in value:
        profile_storage(value["storage"], policy, standalone=True)
    return value


class MarketProfile:
    def __init__(self, context):
        config = profile_config(context["config"])
        self.input = PreparedInput({k: config[k] for k in ("version", "snapshot_directory", "snapshot_sha256")})
        self.snapshot = plain(self.input.snapshot)
        self.policy = profile_policy(config["policy"], standalone=True)
        self.storage = config.get("storage")
        self.experiment_sha256 = profile_identity(self.input.sha256, self.policy)
        self.root = Path(context["output_directory"])
        require(self.root.is_dir() and not any(self.root.iterdir()), "output directory must be empty")
        self.binding = {k: context[k] for k in ("run_id", "attempt_id", "group", "identity")}
        self.clock = CutClock(self.snapshot)
        self.collector = Collector(self.policy, self.snapshot, self.input.sha256, self.root,
                                   self.experiment_sha256, standalone=True, storage=self.storage)
        self.sequence = -1
        self.terminal = self.finished = self.poisoned = False

    def __call__(self, cut):
        try:
            require(not self.poisoned and not self.terminal, "closed market profile")
            require(cut.sequence == self.sequence + 1, "profile sequence")
            self.sequence = cut.sequence
            if cut.kind == "initial":
                from replay.economic_sdk.retention import check, required
                check(cut.body, required(self.snapshot))
                self.input.bind(cut.body)
                self.collector.initial(cut, self.clock.start)
                return
            require(self.input.bound, "missing initial")
            if cut.kind == "terminal":
                self.collector.terminal(self.clock.terminal(), cut.risk_sequence)
                self.terminal = True
                return
            require(cut.kind == "cut")
            raw, time = self.clock.observe(cut)
            self.collector.cut(cut, raw, time)
        except Exception:
            self.poisoned = True
            raise

    def finish(self):
        try:
            require(self.terminal and not self.poisoned and not self.finished,
                    "missing terminal / failed market profile")
            files = self.collector.finish_files()
            manifest = {"version": manifest_version(self.policy, self.storage), "strategy": STRATEGY, "snapshot_sha256": self.input.sha256,
                        "policy": self.policy, "experiment_sha256": self.experiment_sha256,
                        "files": files}
            if self.storage is not None:
                manifest["storage"] = self.storage
            summary = validate_content(self.root, self.snapshot, manifest)
            if self.collector.volatility is not None:
                require(summary["volatility"] == self.collector.volatility.summary(),
                        "volatility collector/reader totals")
            write_json_durable(self.root / "summary.json", summary)
            manifest["summary_sha256"] = digest(summary)
            write_json_durable(self.root / "manifest.json", manifest)
            write_json_durable(self.root / "content_receipt.json", {
                "version": 1, "semantic_sha256": digest(manifest), **self.binding,
                "terminal": self.sequence + 1})
            self.finished = True
        except Exception:
            self.poisoned = True
            raise


def build(context):
    return MarketProfile(context)


def manifest_version(policy, storage=None):
    """Storage opt-in selects layout 3; legacy layouts follow the policy unchanged."""
    return policy["version"] if storage is None else 3


def _manifest(value, complete):
    require(type(value) is dict, "closed schema")
    obj(value, "version strategy snapshot_sha256 policy experiment_sha256 files"
        + (" storage" if value.get("version") == 3 else "")
        + (" summary_sha256" if complete else ""))
    require(value["strategy"] == STRATEGY)
    sha(value["snapshot_sha256"])
    policy = profile_policy(value["policy"], standalone=True)
    storage = profile_storage(value["storage"], policy, standalone=True) if "storage" in value else None
    require(type(value["version"]) is int and value["version"] == manifest_version(policy, storage),
            "profile manifest version")
    require(value["experiment_sha256"] == profile_identity(value["snapshot_sha256"], value["policy"]),
            "profile experiment identity")
    names, files = profile_files(policy, storage), value["files"]
    require(type(files) is dict and set(files) == set(names), "output file set")
    framed = (PROFILE_FILE, TRANSITIONS_FILE, LEVELS_FILE, VOLATILITY_FILE)
    check_files({k: v for k, v in files.items() if k not in framed},
                tuple(n for n in names if n not in framed))
    for name in framed:
        if name in files:
            logical, _ = identity_record(files[name])
            if name == PROFILE_FILE:
                file_identity(logical)
    if complete:
        sha(value["summary_sha256"])


def check_output_files(directory, files):
    """Only the selected representation and commit metadata may exist, even provisionally."""
    root = Path(directory)
    allowed = set(files) | {"summary.json", "manifest.json", "content_receipt.json"}
    present = {p.name for p in root.iterdir()}
    require(set(files) <= present <= allowed, "unexpected output file set")
    require(all(p.is_file() and not p.is_symlink() for p in root.iterdir()), "regular output required")


def validate_content(directory, snapshot, manifest):
    _manifest(manifest, "summary_sha256" in manifest)
    check_output_files(directory, manifest["files"])
    summary = validate_profile(directory, plain(snapshot), manifest["files"], manifest["policy"],
                               manifest["experiment_sha256"], manifest["snapshot_sha256"], standalone=True,
                               storage=manifest.get("storage"))
    require(len(encoded(summary)) <= MAX_METADATA, "summary budget")
    return summary


def read_provisional(directory, snapshot_directory, *, expected_sha256):
    root = Path(directory)
    snapshot = load_snapshot(snapshot_directory, expected_sha256=expected_sha256)
    manifest = read_json(root / "manifest.json")
    _manifest(manifest, True)
    require(manifest["snapshot_sha256"] == expected_sha256, "snapshot binding")
    receipt = obj(read_json(root / "content_receipt.json"),
                  "version semantic_sha256 run_id attempt_id group identity terminal")
    require(receipt["version"] == 1 and receipt["semantic_sha256"] == digest(manifest), "semantic identity")
    summary = validate_content(root, snapshot, manifest)
    require(encoded(read_json(root / "summary.json")) == encoded(summary)
            and manifest["summary_sha256"] == digest(summary), "summary identity/schema")
    return {"receipt": receipt, "manifest": manifest, "summary": summary}


def read_completed(run_directory, group):
    root = Path(run_directory)
    require((root / "run.json").is_file() and (root / "SUCCESS.json").is_file(),
            "completed profile requires supervisor SUCCESS")
    success = read_success(root)
    config = read_run(root / "run.json")
    require(group in success["outputs"], "unknown strategy group")
    spec = config["strategies"][group]
    require(canonical_reference(spec["factory"]) == "replay.strategies.market_profile:build", "profile factory binding")
    cfg = profile_config(spec["config"])
    prepared = PreparedInput({k: cfg[k] for k in ("version", "snapshot_directory", "snapshot_sha256")})
    prepared.bind(freeze(initial(config)))
    result = read_provisional(root / success["outputs"][group], cfg["snapshot_directory"],
                              expected_sha256=cfg["snapshot_sha256"])
    require(result["manifest"]["policy"] == cfg["policy"],
            "configured policy")
    require(result["manifest"].get("storage") == cfg.get("storage"), "configured storage")
    receipt = result["receipt"]
    require({k: receipt[k] for k in ("identity", "attempt_id", "group", "run_id", "terminal")}
            == {"identity": success["identity"], "attempt_id": success["attempt"], "group": group,
                "run_id": config["transport"]["run_id"], "terminal": success["terminal"]},
            "supervisor/content binding")
    return result
