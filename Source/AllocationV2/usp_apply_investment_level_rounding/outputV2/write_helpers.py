"""Distinct-table writes after production summary flush.

K1/UBTI/Passive/BoxJKL/Adjustment LookThrough summaries stay inside
production ``write_final_summaries`` (one GenericResultStorer call).
``LookThroughOffsetUnRoundedLines`` and AllocationSummary tables are
independent of each other after that flush.
"""

from __future__ import annotations

from .parallel_helpers import isolated_cfg, run_parallel
from .parent import output_module

_write = output_module("write_service")


def flush_post_summary_writes(
    spark,
    cfg,
    k1_write_df,
    ubti_write_df,
    adj_write_df,
    workers,
    activity,
    enabled_groups,
):
    flag_cfg = isolated_cfg(cfg)
    summary_cfg = isolated_cfg(cfg)
    results = run_parallel(
        [
            (
                "update_is_rounded_flag",
                lambda: _write.update_is_rounded_flag(spark, flag_cfg),
            ),
            (
                "write_allocation_summaries",
                lambda: _write.write_allocation_summaries(
                    spark,
                    summary_cfg,
                    k1_write_df,
                    ubti_write_df,
                    adj_write_df,
                ),
            ),
        ],
        workers,
        activity,
        "output_writes",
        enabled_groups,
    )
    merged = dict(cfg.get("_parquet_results") or {})
    merged.update(flag_cfg.get("_parquet_results") or {})
    merged.update(summary_cfg.get("_parquet_results") or {})
    cfg["_parquet_results"] = merged
    return results[1]


__all__ = ["flush_post_summary_writes"]
