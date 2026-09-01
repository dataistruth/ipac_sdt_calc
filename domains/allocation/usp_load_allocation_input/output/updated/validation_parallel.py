"""
validation_parallel.py — parallel variant of ai_validation_service.run_validations.

The production ``run_validations`` (output/ai_validation_service.py) runs ~10 checks
sequentially. Each check is small and latency-bound (reference-table reads + a few
``.isEmpty()`` / ``.count()`` actions, each launching its own Spark job), so on a
cluster with free task slots the wall time is dominated by job-scheduling latency
rather than CPU.

This wrapper keeps the two *gating* checks sequential and first (they change control
flow), then runs the independent *warning* checks concurrently on a thread pool.

Behaviour parity with production:
  - Same gating decisions (run status FAIL, missing GP partner) in the same order.
  - Same set of warning checks, same messages written to AllocationRunErrors.
  - Only difference: warning rows may be appended in a non-deterministic order.

Thread-safety: each check calls the prod module-global ``_insert_run_error`` (a Delta
``append``). Concurrent Delta appends are safe under optimistic concurrency — an
append-only write to ``AllocationRunErrors`` does not conflict with other appends — so
no locking or patching of the prod module is required. This wrapper only *calls* the
existing prod functions; it never modifies prod code or its in-memory objects.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from .parent import output_module

logger = logging.getLogger(__name__)

_ai_validation = output_module("ai_validation_service")


def run_validations_parallel(
    spark: SparkSession,
    cfg: dict,
    lower_tier_df: DataFrame,
    workers: int = 8,
) -> bool:
    """Parallel drop-in for ``run_validations``. Returns True to continue, False to FAIL.

    Gating checks (run status, GP partner) run sequentially first; the remaining
    warning checks run concurrently on up to ``workers`` threads.
    """
    val = _ai_validation
    t0 = time.time()

    # --- Check 1 (gating): run already FAIL -> abort ---
    run_status = cfg.get("run_status")
    if run_status and str(run_status).upper() == "FAIL":
        logger.info("[validations||] run_status=FAIL, aborting")
        return False

    # --- Check 4 (gating): GP partner must exist when rounding logic = 'Plugged to GP' ---
    rounding_logic = cfg.get("rounding_logic")
    if rounding_logic and str(rounding_logic).lower() == "plugged to gp":
        gp_exists = not (
            val._entity_partner_rows(spark, cfg)
            .filter(F.upper(F.coalesce(F.col("GPorLP"), F.lit(""))) == "G")
            .isEmpty()
        )
        if not gp_exists:
            val._insert_run_error(
                spark,
                cfg,
                "GP Partner does not exist. Please select one of the Partner as GP.",
                "Error",
            )
            logger.info("[validations||] GP partner missing, aborting")
            return False

    # --- Independent warning checks: run in parallel ---
    tasks = [
        ("tax_capital", lambda: val._check_tax_capital_warning(spark, cfg)),
        ("extra_partners", lambda: val._check_extra_partners_warning(spark, cfg)),
        ("lower_tier_partner", lambda: val._check_lower_tier_partner_warnings(spark, cfg, lower_tier_df)),
        ("multiple_partner_flowup", lambda: val._check_multiple_partner_flowup(spark, cfg, lower_tier_df)),
        ("pcap_financial_mismatch", lambda: val._check_pcap_financial_mismatch(spark, cfg)),
        ("financial_partner_not_in_entity", lambda: val._check_financial_partner_not_in_entity(spark, cfg)),
        ("multiple_upper_tier_flowup", lambda: val._check_multiple_upper_tier_flowup(spark, cfg)),
        ("entity_relationship_unlinked", lambda: val._check_entity_relationship_unlinked(spark, cfg)),
    ]

    def _timed(name, fn):
        t = time.time()
        fn()
        return name, time.time() - t

    max_workers = max(1, min(int(workers or 1), len(tasks)))
    durations: dict[str, float] = {}
    if max_workers == 1:
        for name, fn in tasks:
            n, dt = _timed(name, fn)
            durations[n] = dt
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_timed, name, fn): name for name, fn in tasks}
            for fut in as_completed(futures):
                # re-raise any check failure, matching sequential behaviour
                n, dt = fut.result()
                durations[n] = dt

    wall = time.time() - t0
    slowest = sorted(durations.items(), key=lambda kv: kv[1], reverse=True)
    per_check = ", ".join(f"{n}={dt:.2f}s" for n, dt in slowest)
    logger.info(
        "[validations||] %d checks on %d workers | wall=%.2fs | floor=%s (%.2fs) | %s",
        len(tasks),
        max_workers,
        wall,
        slowest[0][0] if slowest else "-",
        slowest[0][1] if slowest else 0.0,
        per_check,
    )
    print(
        f"[validations||] wall={wall:.2f}s workers={max_workers} "
        f"floor={slowest[0][0]}({slowest[0][1]:.2f}s) | " + per_check
        if slowest
        else f"[validations||] wall={wall:.2f}s workers={max_workers}"
    )
    return True
