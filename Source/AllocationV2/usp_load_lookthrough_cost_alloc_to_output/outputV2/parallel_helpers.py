"""Bounded parallel execution for independent Spark plan builders."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def normalize_workers(max_threads=4, MaxThreads=None):
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 4))


def isolated_cfg(cfg):
    local = dict(cfg)
    local["_checkpoint_tables"] = list(cfg.get("_checkpoint_tables", ()))
    local["_checkpoint_paths"] = list(cfg.get("_checkpoint_paths", ()))
    local["_checkpoint_v2_activity"] = []
    return local


def run_parallel(tasks, max_threads, activity, label):
    workers = max(1, min(max_threads, len(tasks)))
    started = time.perf_counter()
    if workers == 1:
        ordered = []
        for name, fn in tasks:
            task_started = time.perf_counter()
            ordered.append(fn())
            activity.append({
                "pool": label,
                "task": name,
                "elapsed_seconds": round(time.perf_counter() - task_started, 3),
            })
        return ordered

    results = {}
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix=label
    ) as pool:
        futures = {}
        task_starts = {}
        for name, fn in tasks:
            task_starts[name] = time.perf_counter()
            futures[pool.submit(fn)] = name
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()
            activity.append({
                "pool": label,
                "task": name,
                "elapsed_seconds": round(
                    time.perf_counter() - task_starts[name], 3
                ),
            })
    activity.append({
        "pool": label,
        "task": "__wall__",
        "workers": workers,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    })
    return [results[name] for name, _ in tasks]


def run_distinct_writers(tasks, activity):
    """Run two independent mutations on demonstrably distinct workers."""
    if len(tasks) != 2:
        raise ValueError("writer pool requires exactly two tasks")
    barrier = threading.Barrier(2)
    results = {}

    def invoke(name, fn):
        thread = threading.current_thread().name
        barrier.wait()
        started = time.perf_counter()
        result = fn()
        activity.append({
            "pool": "lookthrough-write",
            "task": name,
            "thread": thread,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        })
        return result

    with ThreadPoolExecutor(
        max_workers=2, thread_name_prefix="lookthrough-write"
    ) as pool:
        futures = {
            pool.submit(invoke, name, fn): name for name, fn in tasks
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    writer_threads = {
        item["thread"]
        for item in activity
        if item.get("pool") == "lookthrough-write"
        and item.get("task") != "__wall__"
    }
    if len(writer_threads) != 2:
        raise RuntimeError("output/input writes did not use distinct workers")
    return [results[name] for name, _ in tasks]


__all__ = [
    "isolated_cfg",
    "normalize_workers",
    "run_distinct_writers",
    "run_parallel",
]
