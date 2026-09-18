"""Bounded helpers for independent preparation tasks."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def normalize_workers(max_threads=4, MaxThreads=None) -> int:
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 4))


def isolated_cfg(cfg):
    """Copy mutable orchestration state while sharing read-only DataFrames."""
    local = dict(cfg)
    local["_checkpoint_tables"] = cfg["_checkpoint_tables"]
    local["_checkpoint_paths"] = cfg["_checkpoint_paths"]
    local["_checkpoint_v2_activity"] = cfg["_checkpoint_v2_activity"]
    local["_plan_profile"] = cfg["_plan_profile"]
    local["_checkpoint_plan_profile"] = cfg["_checkpoint_plan_profile"]
    local["_action_plan_profile"] = cfg["_action_plan_profile"]
    return local


def run_parallel(tasks, workers, activity, label):
    """Run independent tasks and return results in declared task order."""
    pool_workers = max(1, min(int(workers), len(tasks), 4))
    pool_started = time.perf_counter()
    results = {}

    def invoke(name, fn):
        started = time.perf_counter()
        record = {
            "group": label,
            "task": name,
            "status": "SUCCESS",
            "thread": threading.current_thread().name,
        }
        try:
            return fn()
        except Exception:
            record["status"] = "FAIL"
            raise
        finally:
            record["elapsed_seconds"] = round(
                time.perf_counter() - started, 3
            )
            activity.append(record)

    if pool_workers == 1:
        for name, fn in tasks:
            results[name] = invoke(name, fn)
    else:
        with ThreadPoolExecutor(
            max_workers=pool_workers, thread_name_prefix="rounding-prep"
        ) as pool:
            futures = {
                pool.submit(invoke, name, fn): name for name, fn in tasks
            }
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
            "thread": threading.current_thread().name,
            "elapsed_seconds": wall,
        }
    )
    print(
        f"[parallel] {label}: tasks={len(tasks)} workers={pool_workers} "
        f"wall={wall:.3f}s"
    )
    return [results[name] for name, _ in tasks]


__all__ = ["isolated_cfg", "normalize_workers", "run_parallel"]
