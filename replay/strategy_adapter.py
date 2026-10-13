"""Strategy process entry point; factory(context) returns callable + finish()."""

import os
import signal
import sys
import time
import traceback
from pathlib import Path

from replay.streams import Consumer, TransportError
from replay.streams.consumer import DEFAULT_BATCH_BYTES
from replay.strategies import load_entrypoint
from replay.streams.protocol import freeze, require
from replay.supervisor import identity, initial, read, validate, write_json_durable


CONSUMER_BATCH_ENTRIES = 1024


class HookFailure(Exception):
    pass


def execute(root, attempt, group):
    config = validate(read(root / "run.json"))
    t = config["transport"]
    spec = config["strategies"][group]
    directory = root / attempt / group
    factory = load_entrypoint(spec["factory"])
    context = freeze(
        {
            "run_id": t["run_id"],
            "attempt_id": attempt,
            "group": group,
            "identity": identity(config),
            "config": spec["config"],
            "output_directory": str(directory / "output"),
        }
    )
    # Factory and finish exceptions are also local fatal errors, regardless of type.
    try:
        strategy = factory(context)
    except Exception as e:
        raise HookFailure() from e
    terminal = None

    def hook(cut):
        nonlocal terminal
        try:
            strategy(cut)
            if cut.kind == "terminal":
                strategy.finish()
                terminal = cut.sequence + 1
        except Exception as e:
            raise HookFailure() from e

    c = Consumer(
        os.environ["REDIS_URL"],
        scope=t["scope"],
        run_id=t["run_id"],
        attempt_id=attempt,
        group=group,
        initial=initial(config),
        timeout=t["command_timeout_ms"] / 1000,
        # Whole-entry batches: 1 GiB at the 1 MiB entry cap; one ACK each.
        batch_entries=CONSUMER_BATCH_ENTRIES,
        batch_bytes=max(DEFAULT_BATCH_BYTES, CONSUMER_BATCH_ENTRIES * t["max_entry_bytes"]),
    )
    deadline = time.monotonic() + config["limits"]["attempt_seconds"]
    try:
        while not c.terminal:
            if time.monotonic() >= deadline:
                raise TransportError("adapter deadline")
            c.poll(hook, block_ms=max(1, min(100, t["command_timeout_ms"] // 2)))
        # Validates the local terminal independently; all-group completion is
        # additionally required by the supervisor, not by a spinning child.
        c.finish()
        require(terminal is not None)
        write_json_durable(
            directory / "complete.json",
            {
                "version": 1,
                "identity": identity(config),
                "attempt": attempt,
                "group": group,
                "terminal": terminal,
            },
        )
    finally:
        c.close()


DIAGNOSTICS_ENV = "REPLAY_PARTICIPANT_DIAGNOSTICS"
MAX_DIAGNOSTIC_CHARS = 16384


def diagnose(directory, code, error):
    """Opt-in, bounded failure.json for a failed participant (the local bench sets it).

    Child stderr stays discarded (docs/REPLAY_SUPERVISOR_V1.md); this small file is
    for a person reading a failed run, never retry evidence. It must not change the
    exit code, so any problem writing it is ignored.
    """
    if os.environ.get(DIAGNOSTICS_ENV) != "1":
        return
    try:
        chain, seen = [], error
        while seen is not None and len(chain) < 8:
            chain.append({"type": type(seen).__name__, "message": str(seen)[:2000]})
            seen = seen.__cause__ or seen.__context__
        text = "".join(traceback.format_exception(error))
        write_json_durable(directory / "failure.json", {
            "version": 1, "exit": code, "errors": chain, "traceback": text[-MAX_DIAGNOSTIC_CHARS:]})
    except BaseException:
        pass


def main():
    signal.signal(signal.SIGTERM, lambda *_: os._exit(21))
    try:
        execute(Path(sys.argv[1]), sys.argv[2], sys.argv[3])
    except (TransportError, OSError) as error:
        diagnose(Path(sys.argv[1]) / sys.argv[2] / sys.argv[3], 21, error)
        return 21
    except BaseException as error:
        # Includes SystemExit from hooks: exit(0) is not successful completion.
        diagnose(Path(sys.argv[1]) / sys.argv[2] / sys.argv[3], 20, error)
        return 20
    return 0


if __name__ == "__main__":
    sys.exit(main())
