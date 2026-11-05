"""
uspAddAllocationSummary — Lakebase (PostgreSQL) execution.

Same public entry and status payload as ``output.add_allocation_summary``; the
work runs inside Lakebase as the PL/pgSQL procedure in
``sql/uspAddAllocationSummary.sql`` against the SQL Server tables in
``lakebase_schema``. Connection settings come from ``lakebase_*`` parameters
(see ``Common_V2.core.lakebase``), passed as keyword arguments or cfg keys.
Spark-only tuning arguments (MaxThreads, ExecutionProfile, ...) are accepted
and ignored.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from Common_V2.core.lakebase import COMMON_SQL_FILES, run_procedure

logger = logging.getLogger(__name__)

SP_NAME = "uspAddAllocationSummary"
SQL_FILES = COMMON_SQL_FILES + (Path(__file__).with_name("sql") / "uspAddAllocationSummary.sql",)


def _flag(value, default: bool = True) -> bool:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def _first(*values):
    for value in values:
        if value is not None and str(value).strip() != "":
            return value
    return None


def run_add_allocation_summary(
    spark=None,
    cfg: dict = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "None",
    VolumePath: str = None,
    ExecutionID: str = None,
    **kwargs,
) -> dict:
    """Main entry point for the uspAddAllocationSummary Lakebase port."""
    t0 = time.time()
    cfg = dict(cfg or {})
    params = {**{k: v for k, v in cfg.items() if str(k).startswith("lakebase_")}, **kwargs}
    entity_id = int(_first(EntityID, cfg.get("entity_id")))
    client_id = int(_first(ClientID, cfg.get("client_id")))
    tax_period_id = int(_first(TaxPeriodID, cfg.get("tax_period_id")))
    run_id = int(_first(RunID, cfg.get("run_id")))
    status = {
        "sp_name": SP_NAME,
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "elapsed_seconds": 0,
        "skip_reason": None,
    }
    result = run_procedure(
        params,
        SP_NAME,
        [
            (entity_id, "integer"),
            (client_id, "bigint"),
            (tax_period_id, "integer"),
            (run_id, "bigint"),
            (_flag(params.get("lakebase_write_log")), "boolean"),
            (None, "text"),
        ],
        SQL_FILES,
        spark=spark,
    )
    proc_status = (result["row"] or [None])[-1]
    if proc_status == "SKIPPED":
        logger.warning("Run status is FAIL — exiting early")
        status["status"] = "SKIPPED"
        status["skip_reason"] = "run_status_fail"
        status["reason"] = "RunStatus=FAIL"
    elif proc_status != "SUCCESS":
        raise RuntimeError(f"{SP_NAME} returned unexpected status {proc_status!r}")
    status["lakebase_schema"] = result["schema"]
    status["deployed"] = result["deployed"]
    status["elapsed_seconds"] = round(time.time() - t0, 1)
    logger.info("%s Lakebase %s in %.1fs", SP_NAME, status["status"], status["elapsed_seconds"])
    return status
