"""Receive clocks shared by every venue splice.

`time.time_ns()` correlates capture with venue and external timestamps but may
step when the host clock is corrected. `time.monotonic_ns()` never steps and is
the ordering/interval clock. On Linux it is system-wide within one boot, so the
kernel boot id scopes values that are comparable across venue processes.

The class is injectable: tests and bounded clock probes can provide deterministic
readers without teaching individual venue splices anything about clocks.
"""

from __future__ import annotations

import platform
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

LINUX_BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")


@dataclass(frozen=True)
class ClockSample:
    visible_ns: int
    monotonic_ns: int


@dataclass(frozen=True)
class ClockScope:
    #: The spool lane, which is one capture *process* — not the envelope's venue.
    #: Polymarket runs three lanes (market, sports, rtds) whose monotonic readings
    #: are only comparable under the conditions `scope` describes, so labelling
    #: this "venue" would invite a reader to join three distinct clocks into one.
    lane: str
    clock: str
    scope: str
    scope_id: str
    comparable_across_processes: bool
    platform: str

    def as_record(self) -> dict[str, object]:
        return {
            "lane": self.lane,
            "clock": self.clock,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "comparable_across_processes": self.comparable_across_processes,
            "platform": self.platform,
        }


class CaptureClock:
    """A lane-labelled pair of wall and monotonic clock readers."""

    def __init__(
        self,
        lane: str,
        *,
        wall_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        platform_name: str | None = None,
        boot_id_path: Path = LINUX_BOOT_ID_PATH,
        fallback_scope_id: str | None = None,
    ) -> None:
        self.lane = lane
        self._wall_ns = wall_ns
        self._monotonic_ns = monotonic_ns
        system = platform_name or platform.system()
        self.scope = _clock_scope(
            lane,
            system=system,
            boot_id_path=Path(boot_id_path),
            fallback_scope_id=fallback_scope_id,
        )

    def sample(self) -> ClockSample:
        return ClockSample(
            visible_ns=int(self._wall_ns()),
            monotonic_ns=int(self._monotonic_ns()),
        )


def _clock_scope(
    lane: str,
    *,
    system: str,
    boot_id_path: Path,
    fallback_scope_id: str | None,
) -> ClockScope:
    if system == "Linux":
        try:
            boot_id = boot_id_path.read_text(encoding="ascii").strip()
        except OSError as error:
            raise RuntimeError(f"Linux boot id unavailable at {boot_id_path}: {error}") from error
        if not boot_id or not boot_id.isascii():
            raise RuntimeError(f"Linux boot id is invalid at {boot_id_path}")
        return ClockScope(
            lane=lane,
            clock="CLOCK_MONOTONIC",
            scope="linux_boot",
            scope_id=boot_id,
            comparable_across_processes=True,
            platform=system,
        )

    # Development fallback only. A process-unique scope deliberately prevents a
    # reader from comparing monotonic values across macOS/Windows processes.
    return ClockScope(
        lane=lane,
        clock="monotonic",
        scope="process",
        scope_id=fallback_scope_id or uuid.uuid4().hex,
        comparable_across_processes=False,
        platform=system,
    )
