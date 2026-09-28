"""Sequential result writes matching production order.

LookThroughAllocationOutput is appended, then LookThroughAllocationInput
is overwritten. Do not run these on ThreadPoolExecutor: one Spark session
cannot safely commit both at once, and a parallel trial produced a
LookThroughAllocationOutput row-count mismatch (110 vs updated).
"""

from __future__ import annotations

from .parent import output_module

_prod = output_module("load_lt_footnote_effective_allocation_pct")


def flush_result_tables(
    spark,
    cfg,
    alloc_output_df,
    grouped_output_df,
    workers,
    activity,
    enabled_groups,
):
    del workers, activity, enabled_groups
    return_value = _prod.write_allocation_output(spark, cfg, alloc_output_df)
    _prod.update_allocation_input(spark, cfg, grouped_output_df)
    return return_value


__all__ = ["flush_result_tables"]
