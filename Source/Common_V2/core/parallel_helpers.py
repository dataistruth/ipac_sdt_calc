"""Bounded thread pools for independent SP builders and distinct-table writes.

Each SP declares its own ``KNOWN_GROUPS`` and passes them to
``parse_enabled_groups``. Pools never exceed ``MAX_WORKERS`` threads, and a
group runs sequentially when it is disabled, has one task, or workers <= 1.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

MAX_WORKERS = 4


def normalize_workers(max_threads=MAX_WORKERS, MaxThreads=None):
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        return max(1, min(int(raw), MAX_WORKERS))
    except (TypeError, ValueError):
        return MAX_WORKERS


def parse_enabled_groups(parallel_groups="all", ParallelGroups=None, known_groups=frozenset()):
    """Return the enabled group names: ``all`` -> ``known_groups``, ``none`` -> empty."""
    raw = ParallelGroups if ParallelGroups is not None else parallel_groups
    text = str(raw or "all").strip().lower()
    if text in {"", "all"}:
        return set(known_groups)
    if text == "none":
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


def isolated_cfg(cfg, fresh_results=True):
    """Shallow cfg copy for one worker task.

    With ``fresh_results`` the copy gets its own ``_parquet_results`` and
    ``_schema_cache`` so concurrent writers never mutate a shared dict; the
    caller merges them back. Without it, collected results stay shared.
    """
    local = dict(cfg)
    if fresh_results:
        local["_parquet_results"] = {}
        local["_schema_cache"] = dict(cfg.get("_schema_cache") or {})
    return local


def run_parallel(tasks, workers, activity, label, enabled_groups=None, thread_name_prefix=None):
    """tasks: list[(task_name, fn)]. Return results in task order.

    Every future is observed before raising so one failure cannot hide another.
    """
    enabled = {label} if enabled_groups is None else enabled_groups
    sequential = (
        label not in enabled or int(workers) <= 1 or len(tasks) <= 1
    )
    pool_workers = 1 if sequential else max(1, min(int(workers), len(tasks), MAX_WORKERS))
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
            max_workers=pool_workers,
            thread_name_prefix=thread_name_prefix or f"sp-{label}",
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


def run_phase(name, tasks, max_threads, enabled_groups):
    """tasks: list[(task_name, fn, args, kwargs)] in stable merge order.

    Returns ``[(task_name, result), ...]`` in task order.
    """
    if name not in enabled_groups or max_threads <= 1 or len(tasks) <= 1:
        return [
            (task_name, fn(*args, **kwargs))
            for task_name, fn, args, kwargs in tasks
        ]

    def _call(task_name, fn, args, kwargs):
        started = time.time()
        thread = threading.current_thread().name
        print(
            f"[parallel] START phase={name} task={task_name} thread={thread}"
        )
        try:
            result = fn(*args, **kwargs)
        except Exception:
            print(
                f"[parallel] DONE phase={name} task={task_name} "
                f"status=fail elapsed={time.time() - started:.2f}s"
            )
            raise
        print(
            f"[parallel] DONE phase={name} task={task_name} "
            f"status=ok elapsed={time.time() - started:.2f}s"
        )
        return result

    workers = max(1, min(max_threads, len(tasks), MAX_WORKERS))
    results = {}
    failures = []
    started = time.time()
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix=f"sp-{name}"
    ) as pool:
        futures = {
            pool.submit(_call, task_name, fn, args, kwargs): task_name
            for task_name, fn, args, kwargs in tasks
        }
        for future, task_name in futures.items():
            try:
                results[task_name] = future.result()
            except Exception as exc:
                failures.append((task_name, exc))
    if failures:
        detail = [(n, f"{type(e).__name__}: {e}") for n, e in failures]
        error = RuntimeError(f"Parallel group {name!r} failed: {detail}")
        raise error from failures[0][1]
    print(
        f"[parallel] {name}: tasks={len(tasks)} workers={workers} "
        f"wall={time.time() - started:.2f}s critical=max-task"
    )
    return [(task_name, results[task_name]) for task_name, *_ in tasks]


__all__ = [
    "MAX_WORKERS",
    "isolated_cfg",
    "normalize_workers",
    "parse_enabled_groups",
    "run_parallel",
    "run_phase",
]
