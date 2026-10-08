"""Independent, streaming coverage content reader; completion is a separate fact.

The interval rules themselves live in ``replay.economic_sdk.availability_reader``
so that the market profile's ``availability`` group is read by the same code.
"""

from pathlib import Path

from replay.strategies import canonical_reference

from replay.economic_sdk.availability_reader import (  # noqa: F401  (re-exported)
    MAX_BYTES,
    MAX_ENTITIES,
    MAX_LINE,
    MAX_RECORDS,
    STATES,
    book_id,
    duration_rows,
    entities,
    validate_intervals,
)
from replay.preparation import digest, encoded, load_snapshot, sha
from replay.strategy_sdk import PreparedInput, plain
from replay.streams.protocol import (
    decode,
    freeze,
    obj,
    require,
)
from replay.supervisor import initial, read_success
from replay.supervisor import read as read_run

MAX_METADATA = 16 * 1024 * 1024
DISPOSITIONS = "observed applied duplicate not_authority invalidated"


def read_json(path):
    require(not path.is_symlink() and path.is_file(), "regular output required")
    with path.open("rb") as stream:
        return decode(stream.read(MAX_METADATA + 1), MAX_METADATA)


def _manifest(value, complete):
    obj(
        value,
        "version strategy snapshot_sha256 intervals trades nonduplicate_trade_observations"
        + (" summary_sha256" if complete else ""),
    )
    require(type(value["version"]) is int and value["version"] == 1)
    require(value["strategy"] == "bundle_coverage_v1")
    sha(value["snapshot_sha256"])
    if complete:
        sha(value["summary_sha256"])
    identity = obj(value["intervals"], "sha256 byte_length records")
    sha(identity["sha256"])
    for field, cap in (("byte_length", MAX_BYTES), ("records", MAX_RECORDS)):
        require(type(identity[field]) is int and 0 < identity[field] <= cap)
    obj(value["trades"], DISPOSITIONS)
    require(
        all(type(v) is int and 0 <= v <= 2**64 - 1 for v in value["trades"].values())
    )
    count = value["nonduplicate_trade_observations"]
    require(
        type(count) is int
        and count == sum(v for k, v in value["trades"].items() if k != "duplicate"),
        "trade count mismatch",
    )


def validate_content(directory, snapshot, manifest):
    """Validate provisional intervals; never asserts supervisor completion.

    One bounded line at a time; temporal cross-check uses a disposable disk index,
    not a retained tape. Returns a deterministic summary derived from intervals.
    """
    _manifest(manifest, "summary_sha256" in manifest)
    snapshot = plain(snapshot)
    durations = validate_intervals(
        Path(directory) / "intervals.ndjson", snapshot, manifest["intervals"]
    )
    summary = {
        "version": 1,
        "snapshot_sha256": manifest["snapshot_sha256"],
        "membership_basis": snapshot["membership_basis"],
        "history_complete": snapshot["history_complete"],
        "vendor_completeness": "NOT_PROVEN",
        "trades": manifest["trades"],
        "nonduplicate_trade_observations": manifest["nonduplicate_trade_observations"],
        "durations": duration_rows(durations),
    }
    require(len(encoded(summary)) <= MAX_METADATA, "summary budget")
    return summary


def read_provisional(directory, snapshot_directory, *, expected_sha256):
    """Content validation only. This API deliberately returns no committed flag."""
    root = Path(directory)
    snapshot = load_snapshot(snapshot_directory, expected_sha256=expected_sha256)
    manifest = read_json(root / "manifest.json")
    _manifest(manifest, True)
    require(manifest["snapshot_sha256"] == expected_sha256, "snapshot binding")
    receipt = obj(
        read_json(root / "content_receipt.json"),
        "version semantic_sha256 run_id attempt_id group identity terminal",
    )
    require(type(receipt["version"]) is int and receipt["version"] == 1)
    sha(receipt["identity"])
    sha(receipt["semantic_sha256"])
    require(receipt["semantic_sha256"] == digest(manifest), "semantic identity")
    for field in ("run_id", "attempt_id", "group"):
        require(type(receipt[field]) is str and 0 < len(receipt[field]) <= 128)
    require(type(receipt["terminal"]) is int and receipt["terminal"] >= 2)
    summary = validate_content(root, snapshot, manifest)
    require(
        encoded(read_json(root / "summary.json")) == encoded(summary)
        and manifest["summary_sha256"] == digest(summary),
        "summary identity/schema",
    )
    return {"receipt": receipt, "manifest": manifest, "summary": summary}


def read_completed(run_directory, group):
    """Only this reader combines content identity with whole-attempt success."""
    root = Path(run_directory)
    success = read_success(root)
    config = read_run(root / "run.json")
    require(group in success["outputs"], "unknown strategy group")
    spec = config["strategies"][group]
    require(
        canonical_reference(spec["factory"]) == "replay.strategies.bundle_coverage:build", "coverage factory binding"
    )
    strategy_config = obj(spec["config"], "version snapshot_directory snapshot_sha256")
    PreparedInput(strategy_config).bind(freeze(initial(config)))
    result = read_provisional(
        root / success["outputs"][group],
        strategy_config["snapshot_directory"],
        expected_sha256=strategy_config["snapshot_sha256"],
    )
    receipt = result["receipt"]
    require(
        {
            k: receipt[k]
            for k in ("identity", "attempt_id", "group", "run_id", "terminal")
        }
        == {
            "identity": success["identity"],
            "attempt_id": success["attempt"],
            "group": group,
            "run_id": config["transport"]["run_id"],
            "terminal": success["terminal"],
        },
        "supervisor/content binding",
    )
    return result
