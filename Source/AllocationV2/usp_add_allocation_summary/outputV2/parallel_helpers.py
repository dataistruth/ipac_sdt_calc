"""Bounded, deterministic thread-pool helpers."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextvars import copy_context


def normalize_workers(max_threads=4, MaxThreads=None) -> int:
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 4))


def isolated_cfg(cfg):
    local = dict(cfg)
    local["_result_file_infos"] = []
    return local


def run_parallel(tasks, workers, label):
    workers = max(1, min(int(workers), 4, len(tasks)))
    started = time.time()
    if workers == 1:
        results = {name: fn() for name, fn in tasks}
    else:
        results = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(copy_context().run, fn): name
                for name, fn in tasks
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as exc:
                    raise RuntimeError(
                        f"Parallel summary task {name!r} failed"
                    ) from exc
    elapsed = round(time.time() - started, 3)
    print(
        f"[parallel] {label}: tasks={len(tasks)} workers={workers} "
        f"wall={elapsed:.3f}s"
    )
    return [(name, results[name]) for name, _ in tasks], {
        "group": label,
        "tasks": len(tasks),
        "workers": workers,
        "elapsed_seconds": elapsed,
    }
