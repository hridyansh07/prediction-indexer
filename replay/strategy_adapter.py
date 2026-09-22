"""Strategy process entry point; factory(context) returns callable + finish()."""

import importlib
import os
import signal
import sys
import time
from pathlib import Path

from replay.streams import Consumer, TransportError
from replay.streams.protocol import freeze, require
from replay.supervisor import identity, initial, read, validate, write_json_durable


class HookFailure(Exception):
    pass


def execute(root, attempt, group):
    config = validate(read(root / "run.json"))
    t = config["transport"]
    spec = config["strategies"][group]
    directory = root / attempt / group
    module, name = spec["factory"].split(":")
    factory = getattr(importlib.import_module(module), name)
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
        batch_entries=1,
        batch_bytes=t["max_entry_bytes"],
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


def main():
    signal.signal(signal.SIGTERM, lambda *_: os._exit(21))
    try:
        execute(Path(sys.argv[1]), sys.argv[2], sys.argv[3])
    except (TransportError, OSError):
        return 21
    except BaseException:
        # Includes SystemExit from hooks: exit(0) is not successful completion.
        return 20
    return 0


if __name__ == "__main__":
    sys.exit(main())
