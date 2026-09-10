"""Optimized orchestrator for usp_add_lookthrough_alloc_detail_step01.

Production flow (parent ``output`` module): read ``LookThroughAllocationOutput``
once into ``cfg['_base_lt_out']``, then run 12 independent section writers that
each filter/aggregate that base frame into a different detail table, collecting
the results and writing them via ``GenericResultStorer`` at the end.

This updated orchestrator keeps that flow and the exact business logic (the
section functions are imported unchanged from the parent module), but adds the
same three levers used by the prior optimized SPs:

* **checkpoint** the shared ``base_lt_out`` frame once (a plain scan+filter with
  no self-join, so ``local`` backend is safe by default) — every section reads
  the materialized break instead of re-deriving the read 12 times;
* a **thread pool** that builds the 12 independent section frames concurrently
  (each on an isolated ``cfg`` copy sharing the checkpointed base), then merges
  and writes them once;
* the shared **plan profiler** (``AllocationV2.plan_profiler.measure_plan``) to
  attribute logical-plan node-count / depth to each section — the largest
  growers are the best future checkpoint seams.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Iterator

from Common_V2.core.config import load_common_config
from Common_V2.core.generic_result_storer import GenericResultStorer
from Common_V2.core.helpers import tbl as _tbl
import pyspark.sql.functions as F

# Section writers + config alias step are imported unchanged from the parent
# production module so outputs stay bit-for-bit identical to the original.
from ..add_lookthrough_allocation_detail_step01 import (
    _load_config,
    _write_book,
    _write_book_k1_adjustment,
    _write_box_jkl,
    _write_cy_adjustment,
    _write_dated_transfer,
    _write_dated_transfer_without_adj,
    _write_k1_complete,
    _write_k1_text_allocation_detail,
    _write_m1_residual,
    _write_m1_sidepocket,
    _write_offset,
    _write_special_allocation,
)
from .checkpoint import (
    checkpoint,
    drop_checkpoints,
    normalize_checkpoint_backend,
    normalize_coalesce,
    normalize_local_denylist,
)
from .plan_profiler import measure_plan, plan_profile_report

logger = logging.getLogger(__name__)

# (section-name, writer-fn). All 12 are independent: each reads the shared
# ``cfg['_base_lt_out']`` (plus, for CY / K1-text, a few small side tables) and
# writes to a distinct detail table, so they parallelize cleanly.
_SECTIONS: tuple[tuple[str, Any], ...] = (
    ("m1_sidepocket", _write_m1_sidepocket),
    ("book", _write_book),
    ("book_k1_adjustment", _write_book_k1_adjustment),
    ("offset", _write_offset),
    ("dated_transfer", _write_dated_transfer),
    ("dated_transfer_without_adj", _write_dated_transfer_without_adj),
    ("special_allocation", _write_special_allocation),
    ("m1_residual", _write_m1_residual),
    ("box_jkl", _write_box_jkl),
    ("k1_complete", _write_k1_complete),
    ("cy_adjustment", _write_cy_adjustment),
    ("k1_text_allocation_detail", _write_k1_text_allocation_detail),
)


@contextmanager
def _timed(timings: list[dict[str, Any]], step: str) -> Iterator[None]:
    started = time.time()
    try:
        yield
    finally:
        timings.append(
            {"step": step, "elapsed_seconds": round(time.time() - started, 3)}
        )


def _run_section(spark, cfg: dict, name: str, writer) -> dict:
    """Run one production section writer on an isolated cfg copy.

    Returns the section's collected ``{table: DataFrame}`` plus timing. The cfg
    copy is shallow, so the checkpointed ``_base_lt_out`` frame and all scalars
    are shared (read-only) while ``_parquet_results`` stays thread-local.

    NOTE: plan measurement is intentionally NOT done here. ``measure_plan``'s
    Spark Connect fallback uses a process-wide ``redirect_stdout``, which is not
    thread-safe; running it in parallel workers can blank out the depth. Plans
    are measured in the main thread during the merge loop instead.
    """
    started = time.time()
    local_cfg = {**cfg, "_parquet_results": {}}
    writer(spark, local_cfg)
    produced = local_cfg.get("_parquet_results", {}) or {}
    return {
        "name": name,
        "produced": produced,
        "elapsed_seconds": round(time.time() - started, 3),
    }


def run_add_lookthrough_allocation_detail_step01(
    spark,
    cfg: dict = None,
    verbose: bool = False,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    CallFrom: str = None,
    ResultType: str = None,
    VolumePath: str = None,
    ExecutionID: str = None,
    **kwargs,
):
    """Load lookthrough allocated amounts into detail tables (S1-S13 semantics).

    UBTI/UBTI-DF logic is excluded, matching the production module.
    """
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName

    t0 = time.time()
    timings: list[dict[str, Any]] = []
    parallel_workers = max(1, min(int(kwargs.pop("parallel_workers", 4)), 8))
    # Plan-size profiler flags (default off; zero overhead unless enabled).
    profile_plan_kw = kwargs.pop("profile_plan", None)
    plan_threshold_kw = kwargs.pop("plan_checkpoint_threshold", None)
    # Checkpoint backend ("delta"/"local") + optional local-mode delta-denylist.
    checkpoint_backend_kw = kwargs.pop("checkpoint_backend", None)
    local_denylist_kw = kwargs.pop("local_delta_denylist", None)
    checkpoint_coalesce_kw = kwargs.pop("checkpoint_coalesce", None)
    section_pool = None

    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    status = {
        "sp_name": "uspAddLookThroughAllocationDetail_Step_01",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    try:
        with _timed(timings, "S1 config"):
            if cfg is None:
                cfg = load_common_config(
                    spark,
                    run_id=run_id,
                    entity_id=entity_id,
                    client_id=client_id,
                    tax_period_id=tax_period_id,
                    catalog=catalog,
                    schema=schema,
                    call_from=CallFrom,
                )
            elif CallFrom is not None:
                cfg["call_from"] = CallFrom

            # Invocation-local checkpoint/profiler/result state (thread safety).
            cfg = {
                **cfg,
                "_checkpoint_tables": [],
                "_plan_profile": [],
                "_parquet_results": {},
            }
            if ResultType is not None:
                cfg.setdefault("result_type", ResultType)
            if VolumePath is not None:
                cfg["volume_path"] = VolumePath
            if ExecutionID is not None:
                cfg["execution_id"] = ExecutionID

            # Resolve plan-profiler flags: explicit kwargs win, else cfg, else default.
            if profile_plan_kw is not None:
                cfg["profile_plan"] = bool(profile_plan_kw)
            else:
                cfg.setdefault("profile_plan", False)
            if plan_threshold_kw is not None:
                cfg["plan_checkpoint_threshold"] = int(plan_threshold_kw)
            else:
                cfg.setdefault("plan_checkpoint_threshold", 30)

            # Resolve checkpoint backend + denylist + coalesce (see checkpoint.py).
            cfg["_checkpoint_backend"] = normalize_checkpoint_backend(
                checkpoint_backend_kw
                if checkpoint_backend_kw is not None
                else cfg.get("_checkpoint_backend", cfg.get("checkpoint_backend"))
            )
            cfg["_local_delta_denylist"] = sorted(
                normalize_local_denylist(
                    local_denylist_kw
                    if local_denylist_kw is not None
                    else cfg.get("_local_delta_denylist")
                )
            )
            cfg["_checkpoint_coalesce"] = normalize_coalesce(
                checkpoint_coalesce_kw
                if checkpoint_coalesce_kw is not None
                else cfg.get("_checkpoint_coalesce")
            )

            status["run_id"] = cfg.get("run_id")
            status["entity_id"] = cfg.get("entity_id")

            _load_config(spark, cfg)

        if cfg.get("run_status") == "FAIL":
            logger.error(
                f"RunStatus=FAIL — aborting. RunID={cfg['run_id']}, "
                f"EntityID={cfg['entity_id']}"
            )
            status["status"] = "FAIL"
            status["error"] = "RunStatus=FAIL at entry"
            return status
        status["sections_completed"] = 1

        # PBI 377585: read LookThroughAllocationOutput once, then checkpoint it so
        # the 12 sections read a materialized lineage break instead of re-deriving
        # the scan+filter 12x.
        with _timed(timings, "S2 base_lt_out+checkpoint"):
            base_lt_out = _tbl(spark, "LookThroughAllocationOutput", cfg).filter(
                (F.col("RunID") == cfg["run_id"])
                & (F.col("ClientID") == cfg["client_id"])
            )
            base_lt_out = checkpoint(spark, base_lt_out, "base_lt_out", cfg)
            cfg["_base_lt_out"] = base_lt_out
            status["sections_completed"] = 2

        profile_plan = bool(cfg.get("profile_plan"))
        base_nodes = 0
        if profile_plan:
            base_metrics = measure_plan(base_lt_out)
            base_nodes = base_metrics["nodes"] if base_metrics else 0

        # Sections 3-13: build the 12 independent detail frames concurrently, then
        # merge (union same-named tables) into the master result set.
        with _timed(timings, "S3-S13 sections (parallel)"):
            section_pool = ThreadPoolExecutor(
                max_workers=parallel_workers,
                thread_name_prefix="ltdetail-step01",
            )
            futures = [
                section_pool.submit(_run_section, spark, cfg, name, writer)
                for name, writer in _SECTIONS
            ]
            section_results = [f.result() for f in futures]
            section_pool.shutdown(wait=True)
            section_pool = None

            # Merge + measure plans in the main thread (measure_plan is not
            # thread-safe on Spark Connect; it also runs no Spark job).
            master: dict[str, Any] = cfg["_parquet_results"]
            plan_records = cfg["_plan_profile"]
            for res in sorted(section_results, key=lambda r: r["name"]):
                timings.append(
                    {
                        "step": f"section:{res['name']}",
                        "elapsed_seconds": res["elapsed_seconds"],
                    }
                )
                produced = res["produced"]
                for table, df in produced.items():
                    if profile_plan:
                        metrics = measure_plan(df)
                        if metrics:
                            label = (
                                res["name"]
                                if len(produced) == 1
                                else f"{res['name']}:{table}"
                            )
                            plan_records.append(
                                {
                                    "func": label,
                                    "nodes": metrics["nodes"],
                                    "depth": metrics["depth"],
                                    "delta": metrics["nodes"] - base_nodes,
                                    "ops": metrics["ops"],
                                }
                            )
                    if table in master:
                        master[table] = master[table].unionByName(df)
                    else:
                        master[table] = df
            status["sections_completed"] = 13

        # Write collected DataFrames (drop empties first — parity with prod).
        with _timed(timings, "S13 result storage"):
            parquet_results = {
                k: v
                for k, v in cfg.get("_parquet_results", {}).items()
                if v.limit(1).first() is not None
            }
            if parquet_results:
                result_storer = GenericResultStorer(spark, None)
                status["result_value"] = result_storer.save_results(
                    result=parquet_results,
                    result_type=cfg.get("result_type", "deltalake"),
                    catalog_name=cfg["catalog"],
                    database_name=cfg["schema"],
                    run_id=cfg["run_id"],
                    client_id=cfg["client_id"],
                    entity_id=cfg["entity_id"],
                    execution_id=cfg.get("execution_id", "1"),
                    volume_path=cfg.get("volume_path", ""),
                    sql_url_path=None,
                    sql_username=None,
                    sql_password=None,
                )

    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error(f"[FAIL] {e}", exc_info=True)
        raise
    finally:
        if section_pool is not None:
            section_pool.shutdown(wait=True, cancel_futures=True)
        try:
            if isinstance(cfg, dict):
                drop_checkpoints(spark, cfg)
        finally:
            wall = round(time.time() - t0, 3)
            status["elapsed_seconds"] = round(wall, 1)
            checkpoint_timings = (
                list(cfg.get("_updated_checkpoint_timings", []))
                if isinstance(cfg, dict)
                else []
            )
            status["timings"] = timings + checkpoint_timings
            status["updated_wall_seconds"] = wall
            if isinstance(cfg, dict) and cfg.get("profile_plan"):
                try:
                    status["plan_profile"] = plan_profile_report(cfg)
                except Exception:
                    logger.warning("[PLAN] report failed", exc_info=True)
            resolved_backend = (
                cfg.get("_checkpoint_backend", "delta")
                if isinstance(cfg, dict)
                else "delta"
            )
            status["optimization_profile"] = {
                "checkpoint_backend": (
                    "local"
                    if resolved_backend == "local"
                    else "uc_delta_stats_off"
                ),
                "checkpoint_backend_mode": resolved_backend,
                "checkpoint_coalesce": (
                    cfg.get("_checkpoint_coalesce")
                    if isinstance(cfg, dict)
                    else None
                ),
                "local_delta_denylist": (
                    list(cfg.get("_local_delta_denylist", []))
                    if isinstance(cfg, dict)
                    else []
                ),
                "checkpoint_count": len(checkpoint_timings),
                "spark_session_tuning": "none",
                "parallel_workers": parallel_workers,
                "parallel_scopes": ["S3-S13 12 section-writer plans"],
                "broadcast_strategy": "none",
            }

    logger.info(
        f"[DONE] run_add_lookthrough_allocation_detail_step01 | "
        f"{status['elapsed_seconds']}s | RunID={cfg['run_id']} "
        f"EntityID={cfg['entity_id']}"
    )
    return status


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    status = run_add_lookthrough_allocation_detail_step01(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),  # noqa: F821
        EntityID=int(dbutils.widgets.get("entity_id")),  # noqa: F821
        ClientID=int(dbutils.widgets.get("client_id")),  # noqa: F821
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
        CatalogName=dbutils.widgets.get("catalog"),  # noqa: F821
        SchemaName=dbutils.widgets.get("schema"),  # noqa: F821
    )

    try:
        dbutils.notebook.exit(json.dumps(status))  # noqa: F821
    except Exception:
        print(json.dumps(status, indent=2))
