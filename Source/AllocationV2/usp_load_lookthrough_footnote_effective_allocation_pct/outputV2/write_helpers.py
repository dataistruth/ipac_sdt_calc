"""Distinct-table writes: LookThroughAllocationOutput and LookThroughAllocationInput."""

from __future__ import annotations

from .parallel_helpers import isolated_cfg, run_parallel
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
    """Write independent tables concurrently. Writer is built inside each task."""
    holder = {"return_value": None}

    def write_output():
        holder["return_value"] = _prod.write_allocation_output(
            spark, isolated_cfg(cfg), alloc_output_df
        )

    def update_input():
        _prod.update_allocation_input(
            spark, isolated_cfg(cfg), grouped_output_df
        )

    run_parallel(
        [
            ("LookThroughAllocationOutput", write_output),
            ("LookThroughAllocationInput", update_input),
        ],
        workers,
        activity,
        "output_writes",
        enabled_groups,
    )
    return holder["return_value"]


__all__ = ["flush_result_tables"]
