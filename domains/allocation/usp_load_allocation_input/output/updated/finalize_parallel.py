"""
finalize_parallel.py — parallel variant of phase-9 result collection.

Prod phase 9 calls three collectors sequentially:

    write_allocation_input(spark, cfg, allocation_input_df)   # -> AllocationInput
    write_pfic_flowup(spark, cfg, pfic_flowup_df)             # -> PFICFootnoteFlowup(+TrackingKey)
    write_form_flowups(spark, cfg)                            # -> Form926/199A/8865/8886/...

Each one only *builds* DataFrames and accumulates them into ``cfg["_parquet_results"]``
via ``_collect_result``; the heavy shuffles are deferred to the actual write. The three
are independent (disjoint output table names, no cross-reads, no other cfg mutations),
but ``write_form_flowups`` does many latency-bound eager probes (``.isEmpty()`` /
``.first()`` + reference-table reads) to build the form outputs, so on a cluster with
free slots overlapping the three shortens wall time.

Correctness / parity: each collector runs against its own shallow copy of ``cfg`` with a
fresh, isolated ``_parquet_results`` dict — so there is no race on the shared dict. The
per-thread results are merged back into the real ``cfg["_parquet_results"]`` afterward
(union on the rare key collision). Because the three write disjoint keys and each builds
its DataFrames identically regardless of order, the merged result is identical to the
sequential path. Prod code is only *called*, never modified.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import DataFrame, SparkSession

from .parent import output_module

logger = logging.getLogger(__name__)

_ai_finalization = output_module("ai_finalization_service")


def collect_results_parallel(
    spark: SparkSession,
    cfg: dict,
    allocation_input_df: DataFrame,
    pfic_flowup_df: DataFrame,
    workers: int = 3,
) -> None:
    """Parallel drop-in for the three phase-9 collectors.

    Populates ``cfg["_parquet_results"]`` exactly as the sequential path would.
    """
    fin = _ai_finalization
    t0 = time.time()

    tasks = [
        ("allocation_input", lambda c: fin.write_allocation_input(spark, c, allocation_input_df)),
        ("pfic_flowup", lambda c: fin.write_pfic_flowup(spark, c, pfic_flowup_df)),
        ("form_flowups", lambda c: fin.write_form_flowups(spark, c)),
    ]

    def _run(name, fn):
        # Isolated cfg copy: shares nested read-only state, but its own
        # _parquet_results so concurrent collectors never race on the dict.
        local_cfg = dict(cfg)
        local_cfg["_parquet_results"] = {}
        t = time.time()
        fn(local_cfg)
        return name, local_cfg["_parquet_results"], time.time() - t

    base_results = cfg.setdefault("_parquet_results", {})

    def _merge(results: dict) -> None:
        for tbl, df in results.items():
            if tbl in base_results:
                base_results[tbl] = base_results[tbl].unionByName(df, allowMissingColumns=True)
            else:
                base_results[tbl] = df

    max_workers = max(1, min(int(workers or 1), len(tasks)))
    durations: dict[str, float] = {}
    if max_workers == 1:
        for name, fn in tasks:
            n, results, dt = _run(name, fn)
            _merge(results)
            durations[n] = dt
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_run, name, fn): name for name, fn in tasks}
            for fut in as_completed(futures):
                # re-raise any collector failure, matching sequential behaviour
                n, results, dt = fut.result()
                _merge(results)
                durations[n] = dt

    wall = time.time() - t0
    slowest = sorted(durations.items(), key=lambda kv: kv[1], reverse=True)
    per_task = ", ".join(f"{n}={dt:.2f}s" for n, dt in slowest)
    logger.info(
        "[finalize||] %d collectors on %d workers | wall=%.2fs | floor=%s (%.2fs) | %s",
        len(tasks),
        max_workers,
        wall,
        slowest[0][0] if slowest else "-",
        slowest[0][1] if slowest else 0.0,
        per_task,
    )
    print(
        f"[finalize||] wall={wall:.2f}s workers={max_workers} "
        f"floor={slowest[0][0]}({slowest[0][1]:.2f}s) | " + per_task
        if slowest
        else f"[finalize||] wall={wall:.2f}s workers={max_workers}"
    )
