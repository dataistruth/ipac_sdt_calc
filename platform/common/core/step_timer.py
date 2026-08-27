"""Step-level timing for converted SP pipelines (start, end, elapsed seconds)."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator


@dataclass(frozen=True)
class StepRecord:
    step: str
    started_at: datetime
    ended_at: datetime
    elapsed_seconds: float


class StepTimer:
    """Track start, end, and elapsed seconds for named pipeline steps."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger
        self.steps: list[StepRecord] = []

    @contextmanager
    def step(self, name: str) -> Iterator[None]:
        t0 = time.perf_counter()
        started_at = datetime.now(timezone.utc)
        self._log(f"[timer] {name} START {started_at.isoformat()}")
        try:
            yield
        finally:
            ended_at = datetime.now(timezone.utc)
            elapsed = round(time.perf_counter() - t0, 3)
            self.steps.append(
                StepRecord(
                    step=name,
                    started_at=started_at,
                    ended_at=ended_at,
                    elapsed_seconds=elapsed,
                )
            )
            self._log(
                f"[timer] {name} END {ended_at.isoformat()} "
                f"elapsed={elapsed:.3f}s"
            )

    def _log(self, message: str) -> None:
        print(message)
        if self._logger:
            self._logger.info(message)

    def as_dict_list(self) -> list[dict[str, Any]]:
        return [
            {
                "step": rec.step,
                "started_at": rec.started_at.isoformat(),
                "ended_at": rec.ended_at.isoformat(),
                "elapsed_seconds": rec.elapsed_seconds,
            }
            for rec in self.steps
        ]

    def total_elapsed_seconds(self) -> float:
        return round(sum(rec.elapsed_seconds for rec in self.steps), 3)

    def print_summary(self, header: str = "Step timing summary") -> None:
        if not self.steps:
            return
        total = self.total_elapsed_seconds()
        self._log(
            f"[timer] {header} ({len(self.steps)} steps, tracked_total={total:.3f}s)"
        )
        for rec in self.steps:
            self._log(
                f"[timer]   {rec.step}: {rec.elapsed_seconds:.3f}s "
                f"({rec.started_at.isoformat()} -> {rec.ended_at.isoformat()})"
            )
