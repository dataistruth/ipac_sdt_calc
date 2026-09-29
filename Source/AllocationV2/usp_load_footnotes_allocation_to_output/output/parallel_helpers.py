"""Bounded parallel phases for footnote outputV2."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor


def parse_enabled_groups(parallel_groups="all", ParallelGroups=None):
    raw = ParallelGroups if ParallelGroups is not None else parallel_groups
    text = str(raw or "all").strip().lower()
    known = {"s3_s5_plans", "s13_writes"}
    if text in {"", "all"}:
        return set(known)
    if text == "none":
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


def run_phase(name, tasks, max_threads, enabled_groups):
    """tasks: list[(task_name, fn, args, kwargs)] in stable merge order."""
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

    workers = max(1, min(max_threads, len(tasks)))
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
