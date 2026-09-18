"""Bounded executor for independent, read-only plan builders."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def normalize_workers(max_threads=4, MaxThreads=None):
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        return max(1, min(int(raw), 4))
    except (TypeError, ValueError):
        return 4


def run_parallel(tasks, workers, activity, label):
    """Return results in task order and propagate the first task failure."""
    pool_workers = max(1, min(int(workers), len(tasks), 4))
    pool_started = time.perf_counter()
    results = {}

    def invoke(name, fn):
        started = time.perf_counter()
        status = "SUCCESS"
        try:
            return fn()
        except Exception:
            status = "FAIL"
            raise
        finally:
            activity.append(
                {
                    "group": label,
                    "task": name,
                    "status": status,
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                    "thread": threading.current_thread().name,
                }
            )

    if pool_workers == 1:
        for name, fn in tasks:
            results[name] = invoke(name, fn)
    else:
        with ThreadPoolExecutor(
            max_workers=pool_workers, thread_name_prefix="lt-alloc-input"
        ) as pool:
            futures = {pool.submit(invoke, name, fn): name for name, fn in tasks}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception:
                    for pending in futures:
                        pending.cancel()
                    raise

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
        f"workers={pool_workers} wall={wall:.3f}s"
    )
    return [results[name] for name, _ in tasks]


__all__ = ["normalize_workers", "run_parallel"]
