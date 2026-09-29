"""Bounded executor for independent, read-only plan builders."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

KNOWN_GROUPS = frozenset(
    {
        "independent_early_loads",
        "independent_inputs",
    }
)


def normalize_workers(max_threads=4, MaxThreads=None):
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        return max(1, min(int(raw), 4))
    except (TypeError, ValueError):
        return 4


def parse_enabled_groups(parallel_groups="all", ParallelGroups=None):
    raw = ParallelGroups if ParallelGroups is not None else parallel_groups
    text = str(raw or "all").strip().lower()
    if text in {"", "all"}:
        return set(KNOWN_GROUPS)
    if text == "none":
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


def run_parallel(tasks, workers, activity, label, enabled_groups=None):
    """Return results in task order. Observe every future before raising."""
    enabled = KNOWN_GROUPS if enabled_groups is None else enabled_groups
    sequential = (
        label not in enabled or int(workers) <= 1 or len(tasks) <= 1
    )
    pool_workers = 1 if sequential else max(1, min(int(workers), len(tasks), 4))
    pool_started = time.perf_counter()
    results = {}

    def invoke(name, fn):
        started = time.perf_counter()
        thread = threading.current_thread().name
        print(
            f"[parallel] START phase={label} task={name} thread={thread}"
        )
        status = "SUCCESS"
        try:
            result = fn()
        except Exception:
            status = "FAIL"
            print(
                f"[parallel] DONE phase={label} task={name} "
                f"status=fail elapsed={time.perf_counter() - started:.3f}s"
            )
            raise
        else:
            print(
                f"[parallel] DONE phase={label} task={name} "
                f"status=ok elapsed={time.perf_counter() - started:.3f}s"
            )
            return result
        finally:
            activity.append(
                {
                    "group": label,
                    "task": name,
                    "status": status,
                    "elapsed_seconds": round(
                        time.perf_counter() - started, 3
                    ),
                    "thread": thread,
                }
            )

    if pool_workers == 1:
        for name, fn in tasks:
            results[name] = invoke(name, fn)
    else:
        failures = []
        with ThreadPoolExecutor(
            max_workers=pool_workers, thread_name_prefix="k3-summary"
        ) as pool:
            futures = {
                pool.submit(invoke, name, fn): name for name, fn in tasks
            }
            for future, name in futures.items():
                try:
                    results[name] = future.result()
                except Exception as exc:
                    failures.append((name, exc))
        if failures:
            detail = [
                (name, f"{type(exc).__name__}: {exc}")
                for name, exc in failures
            ]
            error = RuntimeError(
                f"Parallel group {label!r} failed: {detail}"
            )
            raise error from failures[0][1]

    wall = round(time.perf_counter() - pool_started, 3)
    activity.append(
        {
            "group": label,
            "task": "__pool__",
            "status": "SUCCESS",
            "elapsed_seconds": wall,
            "thread": threading.current_thread().name,
        }
    )
    print(
        f"[parallel] {label}: tasks={len(tasks)} "
        f"workers={pool_workers} wall={wall:.3f}s critical=max-task"
    )
    return [results[name] for name, _ in tasks]


__all__ = [
    "KNOWN_GROUPS",
    "normalize_workers",
    "parse_enabled_groups",
    "run_parallel",
]
