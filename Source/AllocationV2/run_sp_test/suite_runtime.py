"""Shared helpers for the 12-SP suite notebooks."""

from __future__ import annotations

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from AllocationV2.run_sp_test.sp_catalog import SP_SPECS


def all_specs():
    return list(SP_SPECS)


def batches(items, size):
    width = max(1, int(size or 1))
    for index in range(0, len(items), width):
        yield items[index : index + width]


def workspace_notebook_path(source_path, relative):
    root = (source_path or "").rstrip("/")
    if root.startswith("/Workspace/"):
        root = root[len("/Workspace") :]
    return f"{root}/{relative}"


def run_via_notebook(dbutils, source_path, relative, args, timeout_seconds=7200):
    path = workspace_notebook_path(source_path, relative)
    started = time.time()
    dbutils.notebook.run(path, timeout_seconds, args)
    return round(time.time() - started, 3)


def run_specs(specs, worker, parallel):
    rows = []
    for batch_no, batch in enumerate(batches(specs, parallel), start=1):
        print(
            f"[suite] batch {batch_no} workers={min(len(batch), max(1, int(parallel)))} "
            f"sps={[spec['name'] for spec in batch]}",
            flush=True,
        )
        if int(parallel) <= 1 or len(batch) == 1:
            for spec in batch:
                rows.append(worker(spec))
            continue
        with ThreadPoolExecutor(max_workers=int(parallel)) as pool:
            futures = {pool.submit(worker, spec): spec["name"] for spec in batch}
            for future in as_completed(futures):
                try:
                    rows.append(future.result())
                except Exception as exc:
                    name = futures[future]
                    rows.append(
                        {
                            "sp": name,
                            "status": "FAIL",
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    print(f"[suite] {name} failed\n{traceback.format_exc()}", flush=True)
    order = {spec["name"]: index for index, spec in enumerate(specs)}
    rows.sort(key=lambda row: order.get(row.get("sp"), 999))
    return rows
